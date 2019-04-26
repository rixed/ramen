(* The actual running sync server daemon *)
open Batteries
open RamenHelpers
open RamenLog
open RamenSync
module Archivist = RamenArchivist
module Files = RamenFiles
module Processes = RamenProcesses

module Server = RamenSyncServer.Make (Value) (Selector)
module CltMsg = Server.CltMsg
module SrvMsg = Server.SrvMsg
module User = RamenSync.User
module Capa = RamenSync.Capacity

let u = User.internal

let set_devnull_init srv =
  let on_set _ v =
    !logger.debug "Some clown wrote into DevNull: %a"
      Value.print v ;
    Some (Value.String "")
  in
  Server.register_callback srv srv.on_sets on_set (Globs.escape "devnull") ;
  let devnull = Value.String "Waldo" in
  Server.create_unlocked srv DevNull devnull
                         ~r:Capa.nobody ~w:Capa.anybody ~s:true

let storage_user_conf_dirty = ref false
let last_read_user_conf = ref 0.

let update_storage conf srv =
  let fname = Archivist.user_conf_file conf in
  let t = Files.mtime_def 0. fname in
  if t > !last_read_user_conf then (
    !logger.info "Updating storage configuration from %a"
      N.path_print fname ;
    last_read_user_conf := t ;
    let user_conf = Archivist.get_user_conf conf in
    Server.set srv u (Storage TotalSize)
               Value.(Int (Int64.of_int user_conf.Archivist.size_limit)) ;
    Server.set srv u (Storage RecallCost)
               Value.(Float user_conf.recall_cost) ;
    let r = Capa.Anybody and w = Capa.Admin in
    Hashtbl.iter (fun glob retention ->
      Server.create_or_update srv (Storage (RetentionsOverride glob))
                              Value.(Retention retention) ~r ~w ~s:false
      (* TODO: delete the left over from previous version *)
      (* TODO: transactions! *)
    ) user_conf.retentions
  ) else (
    (* in the other way around: if conf is dirty save the file.
     * TODO *)
  )

let set_storage_init srv =
  (* Create the minimal set of (sticky) keys: *)
  let r = Capa.Anybody
  and w = Capa.Admin
  and s = true in
  Server.create_unlocked srv (Storage TotalSize) Value.dummy ~r ~w ~s ;
  Server.create_unlocked srv (Storage RecallCost) Value.dummy ~r ~w ~s ;
  (* On any change down there, set the dirty flag: *)
  let set_dirty _ v =
    storage_user_conf_dirty := true ;
    Some v in
  let sel = Globs.escape "storage/*" in
  Server.register_callback srv srv.on_sets set_dirty sel ;
  Server.register_callback srv srv.on_news set_dirty sel ;
  Server.register_callback srv srv.on_dels set_dirty sel

let populate_init srv =
  !logger.info "Populating the configuration..." ;
  set_devnull_init srv ;
  set_storage_init srv

let sync_step conf srv =
  update_storage conf srv

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
  match Zmq.Socket.recv_all ~block:false zock with
  | exception Unix.Unix_error (Unix.EAGAIN, _, _) ->
      ()
  | parts ->
      !logger.info "0MQ: Received message %a" 
        (List.print String.print_quoted) parts ;
      (match peel_multipart parts with
      | peer, [ msg ] ->
          log_and_ignore_exceptions (fun () ->
            let u = User.of_zmq_id peer in
            let m = CltMsg.of_string msg in
            Server.process_msg srv u m
          ) ()
      | _, parts ->
          Printf.sprintf "Invalid message with %d parts"
            (List.length parts) |>
          failwith)

let service_loop conf zock srv =
  Processes.until_quit (fun () ->
    sync_step conf srv ;
    zock_step srv zock ;
    Unix.sleepf 0.3 ; (* FIXME *)
    true
  )

let send_msg zock m us =
  let msg = SrvMsg.to_string m in
  Enum.iter (fun u ->
    let peer = User.zmq_id u in
    !logger.debug "0MQ: Sending message %S to %S" msg peer ;
    Zmq.Socket.send_all ~block:false zock [ peer ; "" ; msg ]
  ) us

let start conf port =
  let ctx = Zmq.Context.create () in
  finally
    (fun () ->
      !logger.info "Terminating 0MQ" ;
      Zmq.Context.terminate ctx)
    (fun () ->
      let zock = Zmq.Socket.(create ctx router) in
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
          service_loop conf zock srv
        ) ()
    ) ()


(*
 * Test client
 *)

let test_client _conf url =
  let ctx = Zmq.Context.create () in
  finally
    (fun () -> Zmq.Context.terminate ctx)
    (fun () ->
      let zock = Zmq.Socket.(create ctx dealer) in
      finally
        (fun () -> Zmq.Socket.close zock)
        (fun () ->
          let connect_to = "tcp://"^ url in
          !logger.info "Connecting to %s..." connect_to ;
          Zmq.Socket.connect zock connect_to ;
          (* 1: send a wrong message and see what happen *)
          !logger.info "Sending a wrong auth..." ;
          Zmq.Socket.send_all zock [ "" ; "1 AU Waldo?" ] ;
          !logger.info "Waiting for an answer..." ;
          let answer = Zmq.Socket.recv_all zock in
          let print_msg fmt = List.print String.print_quoted fmt in
          !logger.info "Received %a" print_msg answer ;
          assert (String.sub (List.at answer 1) 0 3 = "AU ") ;
          !logger.info "Sending correct auth this time..." ;
          Zmq.Socket.send_all zock [ "" ; "1 AU tintin" ] ;
          !logger.info "There should be no answer..." ;
          Unix.sleepf 0.5 ;
          (match Zmq.Socket.recv_all ~block:false zock with
          | exception Unix.(Unix_error (EAGAIN, _, _)) ->
              !logger.info "Indeed, no answer."
          | s ->
              !logger.error "Got answer: %a" print_msg s ;
              assert false) ;
          !logger.info "Starting to sync..." ;
          Zmq.Socket.send_all zock [ "" ; "2 SS *" ] ;
          !logger.info "Receiving:" ;
          forever (fun () ->
            !logger.info "%a" print_msg (Zmq.Socket.recv_all zock)
          ) ()
        ) ()
    ) ()
