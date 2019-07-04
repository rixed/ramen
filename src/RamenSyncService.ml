(* The actual running sync server daemon *)
open Batteries
open RamenHelpers
open RamenLog
open RamenSync
open RamenConsts
module Archivist = RamenArchivist
module Files = RamenFiles
module Processes = RamenProcesses
module FuncGraph = RamenFuncGraph
module Server = RamenSyncServer.Make (Value) (Selector)
module CltMsg = Server.CltMsg
module SrvMsg = Server.SrvMsg
module User = RamenSync.User
module Capa = RamenSync.Capacity
module C = RamenConf
module RC = C.Running
module FS = C.FuncStats
module F = C.Func
module P = C.Program
module T = RamenTypes
module O = RamenOperation
module Services = RamenServices

let u = User.internal

module DevNull =
struct
  let init srv =
    let on_set _ v =
      !logger.debug "Some clown wrote into DevNull: %a"
        Value.print v ;
      Some (Value.String "")
    in
    Server.register_callback
      srv srv.on_sets on_set (Globs.escape "devnull") ;
    let devnull = Value.String "Waldo" in
    Server.create_unlocked
      srv DevNull devnull ~r:Capa.nobody ~w:Capa.anybody ~s:true
end

module TargetConfig =
struct
  let init srv =
    let k = Key.TargetConfig
    and v = Value.TargetConfig []
    and r = Capa.anybody
    and w = Capa.anybody (*User.only_me u*)
    and s = true in
    Server.create_unlocked srv k v ~r ~w ~s
end

module Storage =
struct
  let last_read_user_conf = ref 0.

  let init srv =
    (* Create the minimal set of (sticky) keys: *)
    let r = Capa.anybody
    and w = Capa.anybody (* admin *)
    and s = true
    and total_size = Value.of_int 1073741824
    and recall_cost = Value.of_float 1e-6 in
    Server.create_unlocked srv (Storage TotalSize) total_size  ~r ~w ~s ;
    Server.create_unlocked srv (Storage RecallCost) recall_cost ~r ~w ~s
end

(*
 * The service: populate the initial conf and implement the message queue.
 * Also timeout last tuples.
 * TODO: Save the conf from time to time in a user friendly format.
 *)

let populate_init srv =
  !logger.info "Populating the configuration..." ;
  DevNull.init srv ;
  TargetConfig.init srv ;
  Storage.init srv

let last_tuples = Hashtbl.create 50

(* Process a single input message *)
let zock_step srv zock =
  let peel_multipart msg =
    let too_short l =
      Printf.sprintf "Invalid zmq message with only %d parts" l |>
      failwith in
    let rec look_for_delim l = function
      | [] -> too_short l
      | "" :: rest -> rest
      | _ :: rest -> look_for_delim (l + 1) rest in
    match msg with
      | [] -> too_short 0
      | peer :: rest ->
          peer, look_for_delim 1 rest
  in
  match Zmq.Socket.recv_all ~block:true zock with
  | exception Unix.Unix_error (Unix.EAGAIN, _, _) ->
      ()
  | parts ->
      !logger.info "0MQ: Received message %a" 
        (List.print String.print_quoted) parts ;
      (match peel_multipart parts with
      | peer, [ msg ] ->
          log_and_ignore_exceptions (fun () ->
            let u = User.of_socket peer in
            let m = CltMsg.of_string msg in
            Server.process_msg srv peer u m ;
            (* Special case: we automatically, and silently, prune old
             * entries under "lasts/" directories (only after a new entry has
             * successfully been added there). Clients are supposed to do the
             * same, at their own pace.
             * TODO: in theory, also monitor DelKey to update last_tuples
             * secondary hash. *)
            (match m with
            | _, CltMsg.NewKey (Key.(Tails (site, fq, LastTuple seq)), _) ->
                Hashtbl.modify_opt (site, fq) (function
                  | None ->
                      let seqs =
                        Array.init max_last_tuples (fun i ->
                          if i = 0 then seq else seq-1) in
                      Some (1, seqs)
                  | Some (n, seqs) ->
                      let to_del = Key.(Tails (site, fq, LastTuple seqs.(n))) in
                      !logger.info "Removing old tuple seq %d" seqs.(n) ;
                      Server.H.remove srv.Server.h to_del ;
                      seqs.(n) <- seq ;
                      Some ((n + 1) mod max_last_tuples, seqs)
                ) last_tuples
            | _ -> ())
          ) ()
      | _, parts ->
          Printf.sprintf "Invalid message with %d parts"
            (List.length parts) |>
          failwith)

let service_loop zock srv =
  Processes.until_quit (fun () ->
    zock_step srv zock ;
    true
  )

let send_msg zock ?block m sockets =
  let msg = SrvMsg.to_string m in
  Enum.iter (fun peer ->
    !logger.debug "0MQ: Sending message %S to %S" msg peer ;
    Zmq.Socket.send_all ?block zock [ peer ; "" ; msg ]
  ) sockets

let start _conf port =
  let ctx = Zmq.Context.create () in
  finally
    (fun () ->
      !logger.info "Terminating 0MQ" ;
      Zmq.Context.terminate ctx)
    (fun () ->
      let zock = Zmq.Socket.(create ctx router) in
      Zmq.Socket.set_send_high_water_mark zock 0 ;
      let send_msg = send_msg zock in
      let srv = Server.make ~send_msg in
      populate_init srv ;
      finally
        (fun () ->
          Zmq.Socket.close zock)
        (fun () ->
          (* (* For locally running tools: *)
             Zmq.Socket.bind zock "ipc://ramen_conf_server.ipc" ; *)
          (* For the rest of the world: *)
          let bind_to = "tcp://*:"^ string_of_int port in
          !logger.info "Listening to %s..." bind_to ;
          Zmq.Socket.bind zock bind_to ;
          service_loop zock srv
        ) ()
    ) ()
