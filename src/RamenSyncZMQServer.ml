(* The actual running sync server daemon *)
open Batteries
open Stdint

open RamenConsts
open RamenHelpersNoLog
open RamenHelpers
open RamenLog
open RamenSync
module Archivist = RamenArchivist
module Default = RamenConstsDefault
module ExitCodes = RamenConstsExitCodes
module Files = RamenFiles
module Metric = RamenConstsMetric
module Server = RamenSyncServer.Make (Value) (Selector)
module CltCmd = Sync_client_cmd.DessserGen
module CltMsg = Server.CltMsg
module SrvMsg = Server.SrvMsg
module User = RamenSyncUser
module C = RamenConf
module FS = C.FuncStats
module T = RamenTypes
module TcpSocket = RamenTcpSocket
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
  Server.create srv User.internal k v ~lock_timeo:0. ~recurs:false
                ~can_read ~can_write ~can_del ~echo:false

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
  let codegen_version_name = "code generator"

  let init srv =
    let can_read = anybody
    and can_write = nobody
    and can_del = nobody in
    let set what value =
      create_unlocked srv (Versions what) (Value.of_string value)
                      ~can_read ~can_write ~can_del in
    set "release tag" Versions.release_tag ;
    set codegen_version_name Versions.codegen ;
    set "ringbuffer format" Versions.ringbuf ;
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
    and v = Value.TargetConfig [||]
    and can_read = anybody
    and can_write = anybody
    and can_del = nobody in
    create_unlocked srv k v ~can_read ~can_write ~can_del
end

module Storage =
struct
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
      and can_write = admin
      and can_del = admin
      and k = Key.Sources (N.src_path ("examples/"^ name), ext) in
      create_unlocked srv k v ~can_read ~can_write ~can_del in
    let add_ramen_example name text =
      let v = Value.of_string text in
      add_example name "ramen" v
    and add_alert_example name text =
      let a = dessser_of_string Value.Alert.wrap_of_json text in
      let v = Value.Alert a in
      add_example name "alert" v
    and add_pivot_example name text =
      let p = dessser_of_string Value.Pivot.wrap_of_json text in
      let v = Value.Pivot p in
      add_example name "pivot" v
    in
    let open RamenSourceExamples in
    add_ramen_example "monitoring/network/security"
      Monitoring.Network.security ;
    add_ramen_example "monitoring/network/hosts"
      Monitoring.Network.hosts ;
    add_ramen_example "monitoring/network/traffic"
      Monitoring.Network.traffic ;
    add_pivot_example "monitoring/network/minutely"
      Monitoring.Network.minutely ;
    add_ramen_example "generators/network/logs"
      Generators.Network.logs ;
    add_ramen_example "generators/network/aggregated"
      Generators.Network.aggregated ;
    add_alert_example "generators/network/resp_time"
      Generators.Network.resp_time_alert ;
    add_ramen_example "generators/network/errors"
      Generators.Network.errors ;
    add_alert_example "generators/network/error_rate"
      Generators.Network.error_rate_alert ;
    add_ramen_example "generators/basic"
      Generators.basic;
end

(*
 * The service: populate the initial conf and implement the message queue.
 * Also timeout last tuples.
 *)

let populate_init
      conf srv no_source_examples archive_total_size archive_recall_cost =
  C.info_or_test conf "Populating the configuration..." ;
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
  (*$< Snapshot *)

  type t =
    V1 of (Key.t * Server.hash_value) list

  let file_name ?(version=Versions.sync_conf) conf =
    N.path_cat [ conf.C.persist_dir ; N.path "confserver/snapshots" ;
                 N.path version ]

  let previous_version_of v =
    if String.length v = 0 then None else
    let c = v.[0] in
    if c <> 'v' && c <> 'V' then None else
    match int_of_string (String.lchop v) - 1 with
    | exception _ -> None
    | v' -> Some (String.of_char c ^ string_of_int v')

  (*$= previous_version_of & ~printer:(function None -> "None" | Some s -> s)
     (Some "v41") (previous_version_of "v42")
     None (previous_version_of "")
     None (previous_version_of "x42")
     None (previous_version_of "v")
     None (previous_version_of "vfoo")
  *)

  (* When [fname] does not exist, look for an older version and try to
   * read it, snapshot it again as current version, and quit. The idea is
   * that only two behavior can happen:
   * - Either reading the old version will corrupt memory, and ramen will crash
   *   before saving is complete, or
   * - the values that have been read are actually "valid enough" and the new
   *   snapshot will be safe to read and its content will be close or
   *   identical to the previous snapshot.
   * Namely, it is believed that in no cases a snapshot will be written that
   * will be unsafe to read. *)
  let try_upgrade conf dst_fname =
    let rec upgrade_from ?(max_try=4) next_version =
      if max_try < 0 then
        !logger.info "No previous version, giving up"
      else
        let max_try = max_try - 1 in
        match previous_version_of next_version with
        | None ->
            !logger.error "Cannot find out previous version of %S, giving up"
              next_version
        | Some version ->
            let src_fname = file_name ~version conf in
            if Files.exists src_fname then (
              try
                !logger.warning "Trying to upgrade configuration from %S..."
                  version ;
                let V1 lst = Files.marshal_from_file src_fname in
                Files.marshal_into_file dst_fname (V1 lst) ;
                !logger.info "Upgraded from %S into %a, now restarting."
                  version N.path_print dst_fname ;
                exit ExitCodes.confserver_migrated
              with e ->
                !logger.error "Cannot upgrade configuration from %S: %s, \
                               giving up and restarting."
                  version
                  (Printexc.to_string e) ;
                (* Must ensure no upgrade will be attempted after restart: *)
                Files.touch dst_fname (Unix.time ()) ;
                exit ExitCodes.confserver_migrated
            ) else (
              !logger.warning "No previous version in %a, skipping."
                N.path_print src_fname ;
              upgrade_from ~max_try version
            )
    in
    upgrade_from Versions.sync_conf

  let load conf allow_upgrade srv no_source_examples =
    let fname = file_name conf in
    if allow_upgrade && not (Files.exists fname) then
      try_upgrade conf fname ;
    try
      let fd = Files.safe_open fname [ O_RDONLY ] 0o640 in
      finally
        (fun () -> Files.safe_close fd)
        (fun () ->
          (* Despite versioning of the file name, it may happen that we
           * end up with the wrong data type in the marshalled snapshot,
           * in which case the confserver is going to crash soon after
           * reading it. In case that happen, delete the file so it does
           * not dead-loop.
           * A new snapshot is going to be saved before long if the
           * confserver survives (or exit cleanly). *)
          Files.safe_unlink fname ;
          let V1 lst = Files.marshal_from_fd fname fd in
          let old_codegen_version =
            let k = Key.Versions ConfVersions.codegen_version_name in
            match (List.assoc k lst).v with
            | exception Not_found -> ""
            | Value.RamenValue (VString s) -> s
            | v ->
                err_sync_type k v "a string" ;
                "" in
          let skip_infos = old_codegen_version <> Versions.codegen in
          if skip_infos then
            !logger.info "Codegen version changed from %s to %s, \
                          will not load precompiled info keys"
              old_codegen_version Versions.codegen ;
          !logger.info "Loading %d configuration keys from %a"
            (List.length lst)
            N.path_print fname ;
          let now = Unix.gettimeofday () in
          let is_example n =
            String.starts_with (n : N.src_path :> string) "examples/" in
          List.iter (function
            | Key.Error _
            | Key.Versions _
            | Key.Tails (_, _, _, Subscriber _)
            (* Be wary of replay requests found at startup that could cause
             * crashloop, better delete them *)
            | Key.ReplayRequests as k, _ ->
                !logger.debug "Skipping key %a"
                  Key.print k
            | Key.Sources (n, "info") as k, _
              when skip_infos || (no_source_examples && is_example n) ->
                !logger.debug "Skipping key %a"
                  Key.print k
            | Key.Sources (n, "ramen") as k, _
              when no_source_examples && is_example n ->
                !logger.debug "Removing example program %a"
                  Key.print k
            | Key.PerSite (site, PerWorker (fq,
                PerInstance (_, QuarantineUntil))),
              Server.{ v = Value.RamenValue T.(VFloat t) ; _ } ->
                if t > now then
                  !logger.debug "Forgiving quarantined %a:%a"
                    N.site_print site
                    N.fq_print fq
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
      | DevNull | Time | Tails _ | Replays _ ->
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

  (*$>*)
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

let stats_key_count =
  IntGauge.make Metric.Names.sync_key_count
    "Current number of keys stored in the configuration tree."

(* Stores, per worker, the last max_last_tuples sequence numbers, used to
 * delete the oldest one when new one is received.
 * Notice the instance identifier of that worker does not matter: if the
 * worker is modified we still want to delete whatever tuple formed its
 * previous tail. *)
let last_tuples = Hashtbl.create 100

let update_last_tuples srv site fq instance seq =
  Hashtbl.modify_opt (site, fq) (function
    | None ->
        let seqs =
          Array.init max_last_tuples (fun i ->
            if i = 0 then seq else Uint32.pred seq) in
        Some (1, seqs)
    | Some (n, seqs) ->
        let to_del = Key.(Tails (site, fq, instance, LastTuple seqs.(n))) in
        !logger.debug "Removing old tuple seq %s" (Uint32.to_string seqs.(n)) ;
        Server.H.remove srv.Server.h to_del ;
        seqs.(n) <- seq ;
        Some ((n + 1) mod Array.length seqs, seqs)
  ) last_tuples

(* When a LastStateChangeNotif is received the whole alerting config
 * subtree is cleaned to keep only the most recent uuids. This take some time
 * so it is actually done only once in a while.
 * Alternatively, we could maintain a proper ordered list of all those uuids
 * but that kind of data structure is verbose (TODO). *)
let incidents_history_length = ref Default.incidents_history_length

let purge_incidents_history_ srv new_uuid =
  (* A hash with all keys per uuid: *)
  let uuid_keys = Hashtbl.create (10 * !incidents_history_length) in
  (* Then a map to store the LastStateChangeNotif per uuid: *)
  let uuid_times = ref Map.Float.empty in
  let uuid_num = ref 0 in (* To avoid the slow Map.cardinal *)
  Server.H.iter (fun k hv ->
    match k with
    | Key.(Incidents (uuid, (LastStateChangeNotif as alerting_k))) ->
        Hashtbl.add uuid_keys uuid alerting_k ;
        (match hv.Server.v with
        | Value.Notification notif ->
            let t = notif.Value.Alerting.Notification.sent_time in
            uuid_times := Map.Float.add t uuid !uuid_times ;
            incr uuid_num
        | v ->
            err_sync_type k v "a Notification")
    | Key.(Incidents (uuid, alerting_k)) ->
        Hashtbl.add uuid_keys uuid alerting_k
    | _ ->
        ()
  ) srv.Server.h ;
  (if !uuid_num > !incidents_history_length then !logger.warning
                                            else !logger.debug)
    "Expunging old incidents; now has %d/%d incidents"
    !uuid_num
    !incidents_history_length ;
  while !uuid_num > !incidents_history_length do
    decr uuid_num ;
    let (t, uuid), uuid_times' = Map.Float.pop !uuid_times in
    !logger.debug "Expunging ancient incident %s (from %a)"
      uuid
      print_as_date t ;
    uuid_times := uuid_times' ;
    (* Let's remove the new one under no circumstances: *)
    if uuid = new_uuid then
      !logger.info "Not expunging new incident %s" uuid
    else (
      Hashtbl.find_all uuid_keys uuid |>
      List.iter (fun alerting_k ->
        let k = Key.Incidents (uuid, alerting_k) in
        Server.H.remove srv.Server.h k)
    )
  done

let last_purged_incidents_history = ref 0

let purge_incidents_every = ref Default.purge_incidents_every

let purge_incidents_history srv new_uuid =
  let now = Unix.time () |> int_of_float in
  if now - !last_purged_incidents_history > !purge_incidents_every then (
    last_purged_incidents_history := now ;
    purge_incidents_history_ srv new_uuid
  )

let purge_old_keys srv = function
  | CltCmd.NewKey (Key.(Tails (site, fq, instance, LastTuple seq)), _, _, _)
  | SetKey (Key.(Tails (site, fq, instance, LastTuple seq)), _) ->
      update_last_tuples srv site fq instance seq
  | NewKey (Key.(Incidents (uuid, LastStateChangeNotif)), _, _, _)
  | SetKey (Key.(Incidents (uuid, LastStateChangeNotif)), _) ->
      purge_incidents_history srv uuid
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

let srv_pub_key = ref ""
let srv_priv_key = ref ""

let send peer bytes =
  IntCounter.inc stats_sent_msgs ;
  IntCounter.add stats_sent_bytes (Bytes.length bytes) ;
  TcpSocket.send peer bytes

(* Called with an enum of destination peer * message *)
let send_msg msg_sockets =
  (* FIXME: record peers with the keys directly *)
  Enum.iter (fun (peer, msg) ->
    !logger.debug "> %a: %a"
      User.print peer.TcpSocket.session.user
      SrvMsg.print msg ;
    let msg = SrvMsg.to_string msg in
    let msg = Authn.wrap peer.session.authn msg in
    send peer (Bytes.unsafe_of_string msg) (* FIXME *)
  ) msg_sockets

exception Ignore

let validate_cmd =
  let extension_is_known = function
    | "ramen" | "info" | "pivot" | "alert" -> true
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
  | CltCmd.SetKey (DevNull, _)
  | UpdKey (DevNull, _) ->
      (* Although not an error, we want to prevent this to be written: *)
      raise Ignore
  (* Forbids to create sources with empty name or unknown extension: *)
  | NewKey (Sources (path, _), _, _, _)
    when not (path_is_valid path) ->
      failwith "Source names must not be empty, contain any dot-path, \
                or start or end with a slash"
  | NewKey (Sources (_, ext), _, _, _)
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
            (fun _ -> DelKey { delKey_k = k ; uid = confserver_uid })
        ) hv_opt ;
        None
      ) srv.Server.h
  and delete_session_subscriptions session =
    !logger.debug "Deleting all of %a's subscriptions"
      User.print session.user ;
    Hashtbl.filter_map_inplace (fun _sel_id (sel, map) ->
      let map' = Server.MapOfSockets.remove session.socket map in
      if Server.MapOfSockets.is_empty map' then None else Some (sel, map')
    ) srv.Server.subscriptions
  and delete_user_tails session =
    let uid = User.id session.user in
    Server.H.filteri_inplace (fun k hv ->
      match k with
      | Key.Tails (_, _, _, Subscriber u) when uid = u ->
          !logger.debug "Deleting tail subscription %a belonging to %s"
            Key.print k u ;
          (* Notify other users that may be interested (notice that this user
           * subscriptions have been deleted beforehand, but it is still
           * excluded explicitly for extra caution) *)
          Server.notify srv k hv.Server.prepared_key
            (fun u -> User.has_any_role hv.can_read u &&
                      not (User.equal u session.user))
            (fun _ -> DelKey { delKey_k = k ; uid = confserver_uid }) ;
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

let fold_peers f u services =
  Array.fold_left (fun u service ->
    List.fold_left f u service.TcpSocket.Server.peers
  ) u services

let filter_peers f services =
  Array.iter (fun service ->
    service.TcpSocket.Server.peers <-
      List.filter f service.TcpSocket.Server.peers
  ) services

let timeout_sessions srv now services =
  filter_peers (fun peer ->
    let session = peer.TcpSocket.session in
    let oldest = now -. session.timeout in
    if session.last_used > oldest then true else (
      !logger.warning
        "Timing out socket %a of user %a (last used at %a while timeout is %a)"
        User.print_socket session.socket
        User.print session.user
        print_as_date session.last_used
        print_as_duration session.timeout ;
      delete_session srv session ;
      false
    )
  ) services

let update_stats srv services =
  let num_sessions, uids =
    fold_peers (fun (num_sessions, uids) peer ->
      num_sessions + 1,
      let uid = User.id peer.session.user in
      Set.add uid uids
    ) (0, Set.empty) services in
  IntGauge.set stats_num_sessions num_sessions ;
  IntGauge.set stats_num_users (Set.cardinal uids) ;
  let num_subscribtions = Hashtbl.length srv.Server.subscriptions in
  IntGauge.set stats_num_subscriptions num_subscribtions

let update_key_count srv =
  IntGauge.set stats_key_count (Server.H.length srv.Server.h)

(* Early cleaning of timed out locks is just for nicer visualisation in
 * clients but is not required for proper working of locks. *)
let timeout_all_locks =
  let last_timeout = ref 0. in
  fun srv ->
    let now = Unix.time () in
    if now -. !last_timeout >= 1. then (
      last_timeout := now ;
      (* FIXME: have a heap of locks *)
      Server.H.iter (Server.timeout_locks srv) srv.h
    )

let service_loop ~while_ conf srv services =
  Snapshot.init conf ;
  update_key_count srv ;
  let save_rate = rate_limiter 1 5. in (* No more than 1 save every 5s *)
  let clean_rate = rate_limiter 1 (Default.sync_sessions_timeout *. 0.5) in
  let timeout = 1. (* s *) in
  let last_time = ref (Unix.time ()) in
  let rec loop () =
    timeout_all_locks srv ;
    let handlers =
      Array.fold_left (fun handlers service ->
        service.TcpSocket.Server.handler :: handlers
      ) [] services in
    if handlers <> [] && while_ () then (
      TcpSocket.process_once ~timeout handlers ;
      update_key_count srv ;
      let now = Unix.time () in
      if clean_rate () then (
        timeout_sessions srv now services ;
        update_stats srv services) ;
      if save_rate () then Snapshot.save conf srv ;
      (* Update current time: *)
      if now <> !last_time then (
        last_time := now ;
        let echo = false in
        Server.set srv User.internal Key.Time (Value.of_float now) ~echo) ;
      loop ()
    ) in
  loop () ;
  Snapshot.save conf srv

(* Detect the case where the hostname of the confserver has changed since last
 * startup and emit a warning *)
let detect_hostname_change conf srv =
  (* As a groundwork we need iterators over sites in the configuration: *)
  let fold_sites f u =
    Server.H.fold (fun k _ u ->
      match k with
      | Key.PerSite (site, _)
      | Tails (site, _, _, _)
          -> f u site
      | _ -> u
    ) srv.Server.h u in
  let sites_1 = Services.all_sites conf in
  if Services.SetOfSites.is_singleton sites_1 then
  let sites_2 =
    fold_sites (fun s site -> Services.SetOfSites.add site s)
               Services.SetOfSites.empty in
  if Services.SetOfSites.is_singleton sites_2 then
  let site_1 = Services.SetOfSites.any sites_1
  and site_2 = Services.SetOfSites.any sites_2 in
  if N.compare site_1 site_2 <> 0 then
  !logger.warning "Localhost renamed from %a to %a!"
    N.site_print site_2
    N.site_print site_1

(* Clean a configuration that's just been reloaded from old, irrelevant
 * settings, esp. ancient sites that are not active any longer (the given
 * duration [oldest_site] is relative to the current site). *)
let clean_old_sites conf srv oldest_site =
  (* Hash from site names to most recent mtime: *)
  let sites = Hashtbl.create 10 in
  Server.H.iter (fun k hv ->
    match k with
    | Key.PerSite (site, _) ->
        Hashtbl.modify_opt site (function
          | None -> Some hv.Server.mtime
          | Some mt -> Some (max mt hv.mtime )
        ) sites
    | _ -> ()
  ) srv.Server.h ;
  let master_mtime =
    Hashtbl.find_default sites conf.C.site (Unix.time ()) in
  (* Remove from [sites] those we want to suppress: *)
  Hashtbl.filteri_inplace (fun site mtime ->
    if site = conf.C.site ||
       mtime >= master_mtime -. oldest_site then true
    else (
      !logger.info "Removing inactive site %a from the configuration"
        N.site_print site ;
      false
    )
  ) sites ;
  Server.H.filteri_inplace (fun k _ ->
    match k with
    | PerSite (site, _) -> Hashtbl.mem sites site
    | _ -> true
  ) srv.h

let fix_old_perms srv =
  Server.H.map_inplace (fun k v ->
    match k, v with
    (* Former versions of ramen let clients claim this vital key as theirs: *)
    | Notifications, _ ->
        let u = User.id v.Server.set_by in
        let can_read, can_write, can_del = Key.permissions u k in
        { v with can_read ; can_write ; can_del }
    | _ -> v
  ) srv.Server.h

let clean_old conf srv oldest =
  detect_hostname_change conf srv ;
  clean_old_sites conf srv oldest ;
  fix_old_perms srv

let create_new_server_keys srv_pub_key_file srv_priv_key_file =
  !logger.warning "Creating a new server pub/priv key pair into %a and %a"
    N.path_print srv_pub_key_file N.path_print srv_priv_key_file ;
  let srv_pub_key, srv_priv_key = Authn.random_keypair () in
  Files.write_key ~secure:false srv_pub_key_file srv_pub_key ;
  Files.write_key ~secure:true srv_priv_key_file srv_priv_key ;
  srv_priv_key

let start
      conf ~while_ ports ports_sec srv_pub_key_file srv_priv_key_file
      ignore_file_perms no_source_examples archive_total_size
      archive_recall_cost oldest_site incidents_history_length_
      purge_incidents_every_ allow_upgrade =
  incidents_history_length := incidents_history_length_ ;
  purge_incidents_every := purge_incidents_every_ ;
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
    (try Files.read_key ~secure:(not ignore_file_perms) srv_priv_key_file
    with (Unix.(Unix_error (ENOENT, _, _)) | Sys_error _) as e ->
      !logger.error "Cannot read server private key file %a: %s"
        N.path_print srv_priv_key_file
        (Printexc.to_string e) ;
      create_new_server_keys srv_pub_key_file srv_priv_key_file) ;
  srv_pub_key :=
    if ports_sec = [] then "" else
    Files.read_key ~secure:false srv_pub_key_file ;
  let srv = Server.make conf.C.users_dir ~send_msg in
  let make_session do_authn sockaddr =
    (* Shamefully go via string so that v4/v6 are handled
     * FIXME: User.socket_of_sockaddr *)
    let socket_str = string_of_sockaddr sockaddr in
    let socket = User.socket_of_string socket_str in
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
  let on_msg peer bytes =
    if Bytes.length bytes = 0 then (
      C.info_or_test conf "User %a disconnected"
        User.print peer.TcpSocket.session.user ;
      delete_session srv peer.session
    ) else (
      IntCounter.inc stats_recvd_msgs ;
      IntCounter.add stats_recvd_bytes (Bytes.length bytes) ;
      peer.session.last_used <- Unix.time () ;
      (* Decrypt using the session auth: *)
      (match Authn.decrypt peer.TcpSocket.session.authn
              (Bytes.unsafe_to_string bytes) with
      | exception e ->
          IntCounter.inc stats_bad_recvd_msgs ;
          !logger.error "Cannot decrypt message: %s, ignoring"
            Printexc.(to_string e)
      | Error errmsg ->
          IntCounter.inc stats_bad_recvd_msgs ;
          send peer (Bytes.unsafe_of_string errmsg)
      | Ok str ->
          let msg = CltMsg.of_string str in
          let clt_pub_key =
            match peer.session.authn with
            | Authn.Secure { peer_pub_key ; _ } ->
                Option.map_default Authn.z85_of_pub_key
                                   "" peer_pub_key
            | Authn.Insecure ->
                "" in
          !logger.debug "< %a with pubkey '%a': %a"
            User.print peer.session.user
            User.print_pub_key clt_pub_key
            CltMsg.print msg ;
          (match validate_cmd msg.cmd with
          | exception Ignore ->
              !logger.debug "Ignoring"
          | exception Failure err ->
              Server.set_user_err srv peer.session.user peer.session.socket
                                  msg.seq err
          | () ->
              peer.session.user <-
                Server.process_msg srv peer peer.session.user
                                   peer.session.socket clt_pub_key msg ;
              (* Manage user session:
               * - Get the session timeout from Auth messages;
               * - Handle Bye command. *)
              (match msg.cmd with
              | CltCmd.Auth (_, timeout) ->
                  !logger.debug "Setting timeout to %a for socket %a"
                    print_as_duration timeout
                    User.print_socket peer.session.socket ;
                  peer.session.timeout <- timeout
              | Bye ->   (* FIXME: remove that now useless command *)
                  ()
              | _ -> ()) ;
              (* At every addition some older keys are also automatically,
               * and silently, expunged from some parts of the configuration
               * tree. Clients are free to do keep them or to do the same at
               * their own pace, since those keys that are silently deleted
               * are never modified.
               * This is the case for the old tuples under "lasts/" and
               * oldest resolved incidents.
               * TODO: in theory, also monitor DelKey to update last_tuples
               * secondary hash. *)
              purge_old_keys srv msg.cmd))) in
  let make_service do_authn bind =
    (* [bind] is either a single port number or "bind_addr:port" *)
    C.info_or_test conf "Listening %sto %s..."
      (if do_authn then
        "securely using public key from "^ (srv_pub_key_file :> string) ^
        " and secret key from "^ (srv_priv_key_file :> string) ^" "
      else "") bind ;
    let bind_addr, service_name =
      match String.split ~by:":" bind with
      | exception Not_found ->
          Unix.inet_addr_any, bind
      | bind_addr, service_name ->
          let addr =
            if bind_addr = "*" then Unix.inet_addr_any else
            try Unix.inet_addr_of_string bind_addr
            with Failure m ->
              Printf.sprintf "Invalid binding address %S: %s" bind_addr m |>
              failwith in
          addr, service_name in
    TcpSocket.Server.make
      bind_addr service_name (make_session do_authn) on_msg in
  (* Load the configuration *before* listening to clients in order to avoid
   * initial turmoil: *)
  C.info_or_test conf "Loading latest configuration snapshot..." ;
  (* Not so easy: some values must be overwritten (such as server
   * versions, startup time...) *)
  if not conf.C.test &&
     Snapshot.load conf allow_upgrade srv no_source_examples then
    clean_old conf srv oldest_site
  else
    populate_init conf srv no_source_examples archive_total_size
                  archive_recall_cost ;
  C.info_or_test conf "Creating services..." ;
  let services =
    Enum.append
      (List.enum ports /@ make_service false)
      (List.enum ports_sec /@ make_service true) |>
    Array.of_enum in
  finally
    (fun () ->
      C.info_or_test conf "Shutting down services..." ;
      Array.iter TcpSocket.Server.shutdown services)
    (fun () ->
      service_loop ~while_ conf srv services
    ) ()
