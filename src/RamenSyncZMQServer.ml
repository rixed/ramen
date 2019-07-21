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
module User = RamenSyncUser
module C = RamenConf
module RC = C.Running
module FS = C.FuncStats
module F = C.Func
module P = C.Program
module T = RamenTypes
module O = RamenOperation
module Services = RamenServices

let u = User.internal
let admin = Set.singleton User.Role.Admin
let anybody = Set.of_list User.Role.[ Admin ; User ]
let nobody = Set.empty

let create_unlocked srv k v ~can_read ~can_write ~can_del =
  Server.create srv User.internal k v ~lock_timeo:0.
                ~can_read ~can_write ~can_del

module DevNull =
struct
  let init srv =
    let on_set _ v =
      !logger.debug "Some clown wrote into DevNull: %a"
        Value.print v ;
      Some Value.dummy
    in
    Server.register_callback
      srv srv.on_sets on_set (Globs.escape "devnull") ;
    let can_read = nobody
    and can_write = anybody
    and can_del = nobody in
    create_unlocked srv DevNull Value.dummy ~can_read ~can_write ~can_del
end

module TargetConfig =
struct
  let init srv =
    let k = Key.TargetConfig
    and v = Value.TargetConfig []
    and can_read = anybody
    and can_write = anybody
    and can_del = nobody in
    create_unlocked srv k v ~can_read ~can_write ~can_del
end

module Storage =
struct
  let last_read_user_conf = ref 0.

  let init srv =
    (* Create the minimal set of (sticky) keys: *)
    let can_read = anybody
    and can_write = admin
    and can_del = nobody
    and total_size = Value.of_int 1073741824
    and recall_cost = Value.of_float 1e-6 in
    create_unlocked
      srv (Storage TotalSize) total_size ~can_read ~can_write ~can_del ;
    create_unlocked
      srv (Storage RecallCost) recall_cost ~can_read ~can_write ~can_del
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

(*
 * Snapshots
 *
 * The confserver saves on disc the whole tree from time to time and when
 * it terminates, in order to load it at next startup.
 *)

module Snapshot =
struct
  type t =
    V1 of (Key.t * Server.hash_value) list

  let file_name conf =
    N.path_cat [ conf.C.persist_dir ; N.path "confserver/snapshot" ]

  let load conf srv =
    let fname = file_name conf in
    try
      let fd = Files.safe_open fname [ O_RDONLY ] 0o640 in
      finally
        (fun () -> Files.safe_close fd)
        (fun () ->
          match Files.marshal_from_fd fname fd with
          | V1 lst ->
              !logger.info "Loading %d configuration keys from %a"
                (List.length lst)
                N.path_print fname ;
              List.iter (fun (k, hv) ->
                Server.H.replace srv.Server.h k hv ;
                !logger.debug "Loading configuration key %a" Key.print k ;
              ) lst ;
              true) ()
    with Unix.(Unix_error (ENOENT, _, _)) ->
          !logger.info
            "No previous configuration state, will start empty" ;
          false
       | e ->
          Files.move_aside fname ;
          !logger.error
            "Cannot read configuration initial state, will start empty: %s"
            (Printexc.to_string e) ;
          false

  let save conf srv =
    let fname = file_name conf in
    let what = "Saving confserver snapshot" in
    log_and_ignore_exceptions ~what (fun () ->
      let fd = Files.safe_open fname [ O_WRONLY ; O_CREAT ] 0o640 in
      finally
        (fun () -> Files.safe_close fd)
        (fun () ->
          let lst = Server.H.enum srv.Server.h |> List.of_enum in
          !logger.debug "Saving %d configuration keys into %a"
            (Server.H.length srv.Server.h)
            N.path_print fname ;
          Files.marshal_into_fd fd (V1 lst)
        ) ()
    ) ()

  let init conf =
    let fname = file_name conf in
    Files.mkdir_all ~is_file:true fname
end

(*
 * Synchronization Service
 *)

let last_tuples = Hashtbl.create 50

let is_ramen = function
  | User.Internal | User.Ramen _ -> true
  | User.Anonymous | User.Auth _ -> false

(* Process a single input message *)
let zock_step srv zock zock_idx =
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
      Server.timeout_all_locks srv
  | parts ->
      !logger.info "0MQ: Received message %a"
        (List.print String.print_quoted) parts ;
      (match peel_multipart parts with
      | peer, [ msg ] ->
          let socket = zock_idx, peer in
          log_and_ignore_exceptions (fun () ->
            let u = User.of_socket socket in
            let m = CltMsg.of_string msg in
            let clt_pub_key =
              match Zmq.Socket.get_mechanism zock with
              | `Null | `Plain ->
                  (if is_ramen u then !logger.debug else !logger.info)
                    "Cannot check client public key on an insecure channel" ;
                  ""
              | `Curve ->
                  (try
                    (* Check that it's not empty as it means unsecure for
                     * authenticate! *)
                    let s = Zmq.Socket.get_curve_publickey zock in
                    if s = "" then failwith "invalid public key" ;
                    s
                  with e ->
                    print_exception ~what:"Getting public key" e ;
                    failwith "Cannot get client public key") in
            Server.process_msg srv socket u clt_pub_key m ;
            (* Special case: we automatically, and silently, prune old
             * entries under "lasts/" directories (only after a new entry has
             * successfully been added there). Clients are supposed to do the
             * same, at their own pace.
             * TODO: in theory, also monitor DelKey to update last_tuples
             * secondary hash. *)
            (match m with
            | _, CltMsg.NewKey (Key.(Tails (site, fq, LastTuple seq)), _, _) ->
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

let service_loop conf zocks srv =
  Snapshot.init conf ;
  let save_rate = rate_limiter 1 5. in (* No more than 1 save every 5s *)
  let poll_mask =
    Array.map (fun zock -> zock, Zmq.Poll.In) zocks |>
    Zmq.Poll.mask_of in
  let timeout = 1000 (* ms *) in
  Processes.until_quit (fun () ->
    let ready = Zmq.Poll.poll ~timeout poll_mask in
    !logger.debug "out of poll..." ;
    Array.iteri (fun i m ->
      if m <> None then (
        !logger.debug "Zocket #%d is readable" i ;
        zock_step srv zocks.(i) i)
    ) ready ;
    if save_rate () then Snapshot.save conf srv ;
    true
  ) ;
  Snapshot.save conf srv

let send_msg zocks ?block m sockets =
  let msg = SrvMsg.to_string m in
  Enum.iter (fun (zock_idx, peer) ->
    !logger.debug "0MQ: Sending message %S to %S on zocket#%d"
      msg peer zock_idx ;
    let zock = zocks.(zock_idx) in
    Zmq.Socket.send_all ?block zock [ peer ; "" ; msg ]
  ) sockets

let create_new_server_keys srv_pub_key_file srv_priv_key_file =
  !logger.warning "Creating a new server pub/priv key pair into %a/%a"
    N.path_print srv_pub_key_file N.path_print srv_priv_key_file ;
  let srv_pub_key, srv_priv_key = Zmq.Curve.keypair () in
  Files.write_key false srv_pub_key_file srv_pub_key ;
  Files.write_key true srv_priv_key_file srv_priv_key ;
  srv_priv_key

(* [bind] can be a single number, in which case all local addresses
 * will be bound to that port (equivalent of "*:port"), or an "IP:port"
 * in which case only that IP will be bound. *)
let start conf port port_sec srv_pub_key_file srv_priv_key_file =
  let bind_to port =
    let bind =
      if string_is_numeric port then "*:"^ port else port in
    "tcp://"^ bind in
  let ctx = Zmq.Context.create () in
  let zocket use_curve bind =
    let zock = Zmq.Socket.(create ctx router) in
    Zmq.Socket.set_send_high_water_mark zock 0 ;
    if use_curve then (
      (* When using secure socket, the user *must* provide the path to
       * the server key files, even if it does not exist yet. they will
       * be created in that case. *)
      let srv_pub_key_file =
        if not (N.is_empty srv_pub_key_file) then srv_pub_key_file else
          C.default_srv_pub_key_file conf in
      let srv_priv_key_file =
        if not (N.is_empty srv_priv_key_file) then srv_priv_key_file else
          C.default_srv_priv_key_file conf in
      let srv_priv_key =
        try Files.read_key true srv_priv_key_file
        with Unix.(Unix_error (ENOENT, _, _)) | Sys_error _ ->
          create_new_server_keys srv_pub_key_file srv_priv_key_file in
      log_exceptions (fun () ->
        Zmq.Socket.set_curve_server zock true ;
        Zmq.Socket.set_curve_secretkey zock srv_priv_key)
    ) ;
    (* (* For locally running tools: *)
       Zmq.Socket.bind zock "ipc://ramen_conf_server.ipc" ; *)
    (* For the rest of the world: *)
    let bind_to = bind_to bind in
    !logger.info "Listening %sto %s..."
      (if use_curve then "securely " else "") bind_to ;
    log_exceptions (fun () ->
      Zmq.Socket.bind zock bind_to) ;
    zock in
  finally
    (fun () ->
      !logger.info "Terminating 0MQ" ;
      Zmq.Context.terminate ctx)
    (fun () ->
      let zocks =
        [| Option.map (zocket false) port ;
           Option.map (zocket true) port_sec |] |>
        Array.filter_map identity in
      let send_msg = send_msg zocks in
      finally
        (fun () ->
          Array.iter Zmq.Socket.close zocks)
        (fun () ->
          let srv = Server.make conf ~send_msg in
          if not (Snapshot.load conf srv) then populate_init srv ;
          service_loop conf zocks srv
        ) ()
    ) ()