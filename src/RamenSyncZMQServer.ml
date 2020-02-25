(* The actual running sync server daemon *)
open Batteries
open RamenHelpers
open RamenLog
open RamenSync
open RamenConsts
module Archivist = RamenArchivist
module Files = RamenFiles
module Processes = RamenProcesses
module Server = RamenSyncServer.Make (Value) (Selector)
module CltMsg = Server.CltMsg
module SrvMsg = Server.SrvMsg
module User = RamenSyncUser
module C = RamenConf
module FS = C.FuncStats
module T = RamenTypes
module O = RamenOperation
module Authn = RamenAuthn
module CompilConfig = RamenCompilConfig
module Paths = RamenPaths
module Services = RamenServices
module Versions = RamenVersions

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
    let can_read = nobody
    and can_write = anybody
    and can_del = nobody in
    create_unlocked srv DevNull Value.dummy ~can_read ~can_write ~can_del
end

module Time =
struct
  let init srv =
    let can_read = anybody
    and can_write = nobody
    and can_del = nobody in
    create_unlocked srv Time (Value.of_float 0.) ~can_read ~can_write ~can_del
end

module ConfVersions =
struct
  let init srv =
    let can_read = anybody
    and can_write = nobody
    and can_del = nobody in
    let set what value =
      create_unlocked srv (Versions what) (Value.of_string value)
                      ~can_read ~can_write ~can_del in
    set "release tag" Versions.release_tag ;
    set "running config format" Versions.rc ;
    set "code generator" Versions.codegen ;
    set "instrumentation format" Versions.instrumentation_tuple ;
    set "notifications format" Versions.notify_tuple ;
    set "alerting state" Versions.pending_notify ;
    set "ringbuffer format" Versions.ringbuf ;
    set "output references format" Versions.out_ref ;
    set "workers snapshot format" Versions.worker_state ;
    set "internal instrumentation format" Versions.binocle ;
    set "experiments configuration format" Versions.experiment ;
    set "archivist configuration format" Versions.archivist_conf ;
    set "factors format" Versions.factors ;
    set "services file format" Versions.services ;
    set "replays command format" Versions.replays ;
    set "synchronised configuration" Versions.sync_conf ;
    set "build date" CompilConfig.build_date ;
    set "build host" CompilConfig.build_host ;
    set "OCaml version" CompilConfig.ocaml_version
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

  let init srv total_size recall_cost =
    (* Create the minimal set of (sticky) keys: *)
    let can_read = anybody
    and can_write = admin
    and can_del = nobody
    and total_size = Value.of_int total_size
    and recall_cost = Value.of_float recall_cost in
    create_unlocked
      srv (Storage TotalSize) total_size ~can_read ~can_write ~can_del ;
    create_unlocked
      srv (Storage RecallCost) recall_cost ~can_read ~can_write ~can_del
end

module Sources =
struct
  let init srv =
    (* Add a few examples: *)
    let add_example name ext v =
      let can_read = anybody
      and can_write = nobody
      and can_del = admin
      and k = Key.Sources (N.src_path ("examples/"^ name), ext) in
      create_unlocked srv k v ~can_read ~can_write ~can_del in
    let add_ramen_example name text =
      let v = Value.of_string text in
      add_example name "ramen" v
    and add_alert_example name text =
      let a = PPP.of_string_exc RamenApi.alert_source_ppp_ocaml text in
      let v = Value.Alert (RamenApi.sync_value_of_alert a) in
      add_example name "alert" v
    in
    let open RamenSourceExamples in
    add_ramen_example "monitoring/network/security"
      Monitoring.Network.security ;
    add_ramen_example "monitoring/hosts"
      Monitoring.Network.hosts ;
    add_ramen_example "monitoring/network/traffic"
      Monitoring.Network.traffic ;
    add_ramen_example "monitoring/generated/logs"
      Monitoring.Generated.logs ;
    add_ramen_example "monitoring/generated/aggregated"
      Monitoring.Generated.aggregated ;
    add_alert_example "monitoring/generated/alerts/error_rate"
      Monitoring.Generated.error_rate
end

(*
 * The service: populate the initial conf and implement the message queue.
 * Also timeout last tuples.
 * TODO: Save the conf from time to time in a user friendly format.
 *)

let populate_init
    srv no_source_examples archive_total_size archive_recall_cost =
  !logger.info "Populating the configuration..." ;
  DevNull.init srv ;
  Time.init srv ;
  ConfVersions.init srv ;
  TargetConfig.init srv ;
  Storage.init srv archive_total_size archive_recall_cost ;
  if not no_source_examples then
    Sources.init srv

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
    N.path_cat [ conf.C.persist_dir ; N.path "confserver/snapshots" ;
                 N.path Versions.sync_conf ]

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
              List.iter (function
                | Key.Error _
                | Key.Versions _
                | Key.Tails (_, _, _, Subscriber _) as k, _ ->
                    !logger.debug "Skipping key %a"
                      Key.print k
                | k, hv ->
                    !logger.debug "Loading configuration key %a = %a"
                      Key.print k
                      Value.print hv.Server.v ;
                    Server.H.replace srv.Server.h k hv
              ) lst ;
              ConfVersions.init srv ;
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
    let must_save (k, _hv) =
      let open Key in
      match k with
      | DevNull | Time | Versions _ | Tails _ | Replays _ ->
          false
      | _ ->
          true in
    let fname = file_name conf in
    let what = "Saving confserver snapshot" in
    log_and_ignore_exceptions ~what (fun () ->
      let fd = Files.safe_open fname [ O_WRONLY ; O_CREAT ] 0o640 in
      finally
        (fun () -> Files.safe_close fd)
        (fun () ->
          let lst =
            Server.H.enum srv.Server.h //
            must_save |>
            List.of_enum in
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

open Binocle

let stats_num_sessions =
  IntGauge.make Metric.Names.sync_session_count
    "Number of currently connected sessions."

let stats_num_users =
  IntGauge.make Metric.Names.sync_user_count
    "Number of currently connected users."

let stats_num_subscriptions =
  IntGauge.make Metric.Names.sync_subscription_count
    "Number of currently active subscriptions."

let stats_sent_msgs =
  IntCounter.make Metric.Names.sync_sent_msgs
    "Total number of messages sent so far by the confserver."

let stats_sent_bytes =
  IntCounter.make Metric.Names.sync_sent_bytes
    "Total number of bytes sent so far by the confserver."

let stats_recvd_msgs =
  IntCounter.make Metric.Names.sync_recvd_msgs
    "Total number of messages received so far by the confserver."

let stats_recvd_bytes =
  IntCounter.make Metric.Names.sync_recvd_bytes
    "Total number of bytes received so far by the confserver."

let stats_bad_recvd_msgs =
  IntCounter.make Metric.Names.sync_bad_recvd_msgs
    "Total number of invalid messages received so far by the confserver."

(* Stores, per worker instance, the last max_last_tuples sequence numbers,
 * used to delete the oldest one when new ones are received. *)
let last_tuples = Hashtbl.create 100

let purge_old_tailed_tuples srv = function
  | CltMsg.NewKey (Key.(Tails (site, fq, instance, LastTuple seq)), _, _) ->
      Hashtbl.modify_opt (site, fq) (function
        | None ->
            let seqs =
              Array.init max_last_tuples (fun i ->
                if i = 0 then seq else seq-1) in
            Some (1, seqs)
        | Some (n, seqs) ->
            let to_del = Key.(Tails (site, fq, instance, LastTuple seqs.(n))) in
            !logger.debug "Removing old tuple seq %d" seqs.(n) ;
            Server.H.remove srv.Server.h to_del ;
            seqs.(n) <- seq ;
            Some ((n + 1) mod Array.length seqs, seqs)
      ) last_tuples
  | _ -> ()

let is_ramen = function
  | User.Internal | User.Ramen _ -> true
  | User.Anonymous | User.Auth _ -> false

type session =
  { socket : User.socket ;
    authn : Authn.session ;
    mutable timeout : float ;
    mutable user : User.t ;
    mutable last_used : float }

let sessions : (User.socket, session) Hashtbl.t = Hashtbl.create 90

let srv_pub_key = ref ""
let srv_priv_key = ref ""

let session_of_socket socket do_authn =
  try
    Hashtbl.find sessions socket
  with Not_found ->
    let session =
      { socket ;
        authn =
          if do_authn then
            Authn.(make_session None
              (pub_key_of_z85 !srv_pub_key)
              (priv_key_of_z85 !srv_priv_key))
          else
            Authn.make_clear_session () ;
        (* Will be set when the Auth is spotted: *)
        timeout = Default.sync_sessions_timeout ;
        user = User.Anonymous ;
        last_used = Unix.time () } in
    Hashtbl.add sessions socket session ;
    session

let send ?block zock peer msg =
  IntCounter.inc stats_sent_msgs ;
  IntCounter.add stats_sent_bytes (String.length msg) ;
  Zmq.Socket.send_all ?block zock [ peer ; "" ; msg ]

let send_msg zocks ?block msg_sockets =
  Enum.iter (fun ((zock_idx, peer as socket), msg) ->
    let zock, _do_authn = zocks.(zock_idx) in
    let session = Hashtbl.find sessions socket in
    !logger.debug "> Srv msg to %a on zocket %a: %a"
      User.print session.user
      User.print_socket socket
      SrvMsg.print msg ;
    let msg = SrvMsg.to_string msg in
    let msg = Authn.wrap session.authn msg in
    send ?block zock peer msg
  ) msg_sockets

let validate_cmd =
  let extension_is_known = function
    | "ramen" | "info" | "alert" -> true
    | _ -> false
  and path_is_valid p =
    not (
      N.is_empty p ||
      String.exists "./" (p : N.src_path :> string) ||
      String.starts_with "/" (p :> string) ||
      String.ends_with "/" (p :> string))
  in
  function
  (* Prevent altering DevNull. Still, writing into DevNull
   * is a good way to keep the session alive. *)
  | CltMsg.SetKey (DevNull, _)
  | CltMsg.UpdKey (DevNull, _) ->
      (* Although not an error, we want to prevent this to be written: *)
      raise Exit
  (* Forbids to create sources with empty name or unknown extension: *)
  | CltMsg.NewKey (Sources (path, _), _, _)
    when not (path_is_valid path) ->
      failwith "Source names must not be empty, contain any dot-path, \
                or start or end with a slash"
  | CltMsg.NewKey (Sources (_, ext), _, _)
    when not (extension_is_known ext) ->
      failwith ("Invalid extension '"^ ext ^"'")
  | _ -> ()

let delete_session srv session =
  let delete_session_errors session =
    if User.is_authenticated session.user then
      let k = Key.user_errs session.user session.socket in
      !logger.debug "Deleting error file %a" Key.print k ;
      Server.H.modify_opt k (fun hv_opt ->
        Option.may (fun hv ->
          Server.notify srv k hv.Server.prepared_key
                        (User.has_any_role hv.can_read)
                        (fun _ -> DelKey k)
        ) hv_opt ;
        None
      ) srv.Server.h
  and delete_session_subscriptions session =
    !logger.debug "Deleting all of %a's subscriptions"
      User.print session.user ;
    Hashtbl.filter_map_inplace (fun _sel_id (sel, map) ->
      let map' = Map.remove session.socket map in
      if Map.is_empty map' then None else Some (sel, map')
    ) srv.Server.subscriptions
  and delete_user_tails session =
    let uid = User.id session.user in
    Server.H.filteri_inplace (fun k _ ->
      match k with
      | Key.Tails (_, _, _, Subscriber u) when uid <> u ->
          !logger.debug "Deleting tail subscription %a" Key.print k ;
          false
      | _ ->
          true
    ) srv.Server.h
  in
  (* Delete subscriptions first so that the following deletions are not
   * mirrored to the disconnecting user: *)
  delete_session_subscriptions session ;
  delete_session_errors session ;
  delete_user_tails session

(* Process a single input message *)
let zock_step srv zock zock_idx do_authn =
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
      IntCounter.inc stats_recvd_msgs ;
      (match peel_multipart parts with
      | peer, [ msg ] ->
          IntCounter.add stats_recvd_bytes (String.length msg) ;
          let socket = zock_idx, peer in
          let session = session_of_socket socket do_authn in
          session.last_used <- Unix.time () ;
          (* Decrypt using the session auth: *)
          (match Authn.decrypt session.authn msg with
          | exception e ->
              IntCounter.inc stats_bad_recvd_msgs ;
              !logger.error "Cannot decrypt message: %s, ignoring"
                Printexc.(to_string e) ;
          | Bad errmsg ->
              IntCounter.inc stats_bad_recvd_msgs ;
              send zock peer errmsg
          | Ok str ->
              let msg = CltMsg.of_string str in
              let clt_pub_key =
                match session.authn with
                | Authn.Secure { peer_pub_key ; _ } ->
                    Option.map_default Authn.z85_of_pub_key
                                       "" peer_pub_key
                | Authn.Insecure ->
                    "" in
              !logger.debug "< Clt msg from %a with pubkey '%a': %a"
                User.print session.user
                User.print_pub_key clt_pub_key
                CltMsg.print msg ;
              (match validate_cmd msg.cmd with
              | exception Exit ->
                  !logger.debug "Ignoring"
              | exception Failure err ->
                  Server.set_user_err srv session.user socket msg.seq err
              | () ->
                  session.user <-
                    Server.process_msg srv socket session.user clt_pub_key msg ;
                  (* Manage user session:
                   * - Get the session timeout from Auth messages;
                   * - Handle Bye command. *)
                  (match msg.cmd with
                  | CltMsg.Auth (_, timeout) -> session.timeout <- timeout
                  | Bye ->
                      !logger.info "User %a disconnected" User.print session.user ;
                      delete_session srv session ;
                      Hashtbl.remove sessions socket
                  | _ -> ()) ;
                  (* Special case: we automatically, and silently, prune old
                   * entries under "lasts/" directories (only after a new entry has
                   * successfully been added there). Clients are supposed to do the
                   * same, at their own pace.
                   * TODO: in theory, also monitor DelKey to update last_tuples
                   * secondary hash. *)
                  purge_old_tailed_tuples srv msg.cmd))
      | _, parts ->
          IntCounter.inc stats_bad_recvd_msgs ;
          Printf.sprintf "Invalid message with %d parts"
            (List.length parts) |>
          failwith)

let timeout_sessions srv =
  let now = Unix.time () in
  (* TODO: list the sessions according to last_used *)
  Hashtbl.filter_inplace (fun session ->
    let oldest = now -. session.timeout in
    if session.last_used > oldest then true else (
      !logger.info "Timing out socket %a of user %a"
        User.print_socket session.socket
        User.print session.user ;
      delete_session srv session ;
      false
    )
  ) sessions

let update_stats srv =
  IntGauge.set stats_num_sessions (Hashtbl.length sessions) ;
  let uids =
    Hashtbl.fold (fun _ session uids ->
      let uid = User.id session.user in
      Set.add uid uids
    ) sessions Set.empty in
  IntGauge.set stats_num_users (Set.cardinal uids) ;
  let num_subscribtions = Hashtbl.length srv.Server.subscriptions in
  IntGauge.set stats_num_subscriptions num_subscribtions

let service_loop conf zocks srv =
  Snapshot.init conf ;
  let save_rate = rate_limiter 1 5. in (* No more than 1 save every 5s *)
  let clean_rate = rate_limiter 1 (Default.sync_sessions_timeout *. 0.5) in
  let poll_mask =
    Array.map (fun (zock, _) -> zock, Zmq.Poll.In) zocks |>
    Zmq.Poll.mask_of in
  let timeout = 1000 (* ms *) in
  Processes.until_quit (fun () ->
    (match Zmq.Poll.poll ~timeout poll_mask with
    | exception Unix.(Unix_error (EINTR, _, _)) ->
        ()
    | ready ->
        Array.iteri (fun i m ->
          let zock, do_authn = zocks.(i) in
          if m <> None then zock_step srv zock i do_authn
        ) ready ;
        if clean_rate () then (
          timeout_sessions srv ;
          update_stats srv
        )) ;
    if save_rate () then Snapshot.save conf srv ;
    (* Update current time: *)
    let now = Unix.time () in
    Server.set srv User.internal Key.Time (Value.of_float now) ;
    true
  ) ;
  Snapshot.save conf srv

let create_new_server_keys srv_pub_key_file srv_priv_key_file =
  !logger.warning "Creating a new server pub/priv key pair into %a/%a"
    N.path_print srv_pub_key_file N.path_print srv_priv_key_file ;
  let srv_pub_key, srv_priv_key = Zmq.Curve.keypair () in
  Files.write_key ~secure:false srv_pub_key_file srv_pub_key ;
  Files.write_key ~secure:true srv_priv_key_file srv_priv_key ;
  srv_priv_key

(* [bind] can be a single number, in which case all local addresses
 * will be bound to that port (equivalent of "*:port"), or an "IP:port"
 * in which case only that IP will be bound. *)
let start conf bound_addrs ports_sec srv_pub_key_file srv_priv_key_file
          no_source_examples archive_total_size archive_recall_cost =
  (* When using secure socket, the user *must* provide the path to
   * the server key files, even if it does not exist yet. They will
   * be created in that case. *)
  let srv_pub_key_file =
    if not (N.is_empty srv_pub_key_file) then srv_pub_key_file else
      Paths.default_srv_pub_key_file conf.C.persist_dir in
  let srv_priv_key_file =
    if not (N.is_empty srv_priv_key_file) then srv_priv_key_file else
      Paths.default_srv_priv_key_file conf.C.persist_dir in
  srv_priv_key :=
    if ports_sec = [] then "" else
    (try Files.read_key ~secure:true srv_priv_key_file
    with Unix.(Unix_error (ENOENT, _, _)) | Sys_error _ ->
      create_new_server_keys srv_pub_key_file srv_priv_key_file) ;
  srv_pub_key :=
    if ports_sec = [] then "" else
    Files.read_key ~secure:false srv_pub_key_file ;
  let bind_to port =
    let bind =
      if string_is_numeric port then "*:"^ port else port in
    "tcp://"^ bind in
  let ctx = Zmq.Context.create () in
  let zocket do_authn bind =
    let zock = Zmq.Socket.(create ctx router) in
    Zmq.Socket.set_send_high_water_mark zock 0 ;
    (* (* For locally running tools: *)
       Zmq.Socket.bind zock "ipc://ramen_conf_server.ipc" ; *)
    (* For the rest of the world: *)
    let bind_to = bind_to bind in
    !logger.info "Listening %sto %s..."
      (if do_authn then "securely " else "") bind_to ;
    log_exceptions (fun () ->
      Zmq.Socket.bind zock bind_to) ;
    zock, do_authn in
  finally
    (fun () ->
      !logger.info "Terminating ZMQ" ;
      Zmq.Context.terminate ctx)
    (fun () ->
      !logger.info "Create zockets..." ;
      let zocks =
        log_exceptions ~what:"Creating zockets" (fun () ->
          Enum.append
            (List.enum bound_addrs /@ zocket false)
            (List.enum ports_sec /@ zocket true) |>
          Array.of_enum) in
      let send_msg = send_msg zocks in
      finally
        (fun () ->
          Array.iter (Zmq.Socket.close % fst) zocks)
        (fun () ->
          let srv = Server.make conf.C.persist_dir ~send_msg in
          (* Not so easy: some values must be overwritten (such as server
           * versions, startup time...) *)
          if not (Snapshot.load conf srv) then
            populate_init srv no_source_examples archive_total_size
                          archive_recall_cost ;
          service_loop conf zocks srv
        ) ()
    ) ()
