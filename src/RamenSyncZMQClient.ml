(* A specific Client using ZMQ, for the values/keys defined in RamenSync. *)
open Batteries
open RamenConsts
open RamenLog
open RamenHelpersNoLog
open RamenHelpers
open RamenSync
module Files = RamenFiles
module Authn = RamenAuthn

module CltMsg = Client.CltMsg
module SrvMsg = Client.SrvMsg
module User = RamenSyncUser

open Binocle

let stats_num_sync_msgs_in =
  IntCounter.make Metric.Names.num_sync_msgs_in
    Metric.Docs.num_sync_msgs_in

let stats_num_sync_msgs_out =
  IntCounter.make Metric.Names.num_sync_msgs_out
    Metric.Docs.num_sync_msgs_out

let stats_resp_time =
  Histogram.make Metric.Names.sync_resp_time
    "Response times for any confserver commands."
    Histogram.powers_of_two

(* FIXME: We want to make all extra parameters (clt and zock) disappear in order
 * to simplify passing them to httpd router functions. But we also need clt
 * for inspecting the hash. Both zock and clt should go into the conf parameter
 * which role it is! So we need to reorder the conf type so that it can
 * have a SyncClient and a Zocket, or move Sync modules much higher up the
 * list of modules. This requires to rethink module dependencies though. *)

type session =
  { zock : [`Dealer] Zmq.Socket.t ;
    authn : Authn.session ;
    clt : Client.t ;
    timeout : float ;
    mutable last_sent : float ;
    (* to wait for end of sync: *)
    mutable is_synced : bool ;
    is_synced_cond : Condition.t ;
    wait_synced_lock : Mutex.t }

let wait_synced ~while_ session =
  with_lock session.wait_synced_lock (fun () ->
    !logger.debug "Waiting for ZMQ initial sync" ;
    while while_ () && not session.is_synced do
      Condition.wait session.is_synced_cond session.wait_synced_lock
    done ;
    !logger.debug "Finished waiting for ZMQ initial sync")

let retry_zmq ?while_ f =
  let on = function
    (* EWOULDBLOCK: According to 0mq documentation blocking is supposed
     * to be signaled with EAGAIN but... *)
    | Unix.(Unix_error ((EAGAIN|EWOULDBLOCK|EINTR), _, _)) -> true
    | _ -> false in
  retry ~on ~first_delay:0.3 ?while_ f

(* To help with RPC like interaction, we have two callbacks here that can be
 * set when calling send_cmd, and that are automatically called when the Error
 * message (that is automatically subscribed by the server) is updated. This is
 * a hash from command id to continuation.
 * FIXME: timeout after a while and replace the continuation with
 * `raise Timeout` in that case. *)
let on_oks : (int, unit -> unit) Hashtbl.t = Hashtbl.create 5
let on_kos : (int, unit -> unit) Hashtbl.t = Hashtbl.create 5
(* When we exec a command we might also want to have a callback when it's
 * actually effective (which is not the same as the command being accepted).
 * So here we register call backs that will be triggered automatically when a
 * given server command is received (regardless of mtime etc). *)
let on_dones : (Key.t, int * (SrvMsg.t -> bool) * (unit -> unit)) Hashtbl.t =
  Hashtbl.create 5

(* For response time measurements, a hash of command ids to timestamps which
 * is cleared whenever the "answer" is received (aka. whenever the error file
 * is updated): *)
let send_times = Hashtbl.create 99

let update_stats_resp_time start () =
  let resp_time = Unix.gettimeofday () -. start in
  Histogram.add stats_resp_time resp_time

(* Return the number of pending callbacks.
 * Also a good place to time them out. *)
let pending_callbacks () =
  Hashtbl.length on_oks + Hashtbl.length on_kos + Hashtbl.length on_dones

let log_done cmd_id =
  !logger.debug "Cmd %d: done!" cmd_id

let my_errors clt =
  Option.map (fun socket -> Key.Error (Some socket)) clt.Client.my_socket

(* Given a callback, return another one that intercept error messages and call
 * RPC continuations first: *)
let check_ok clt k v =
  let maybe_cb ~do_ ~dont cmd_id =
    (match Hashtbl.find do_ cmd_id with
    | exception Not_found ->
        if not (Hashtbl.is_empty do_) then
          !logger.debug "No callback pending for cmd #%d, only for %a"
            cmd_id
            (Enum.print Int.print) (Hashtbl.keys do_)
    | k ->
        !logger.debug "Calling back pending function for cmd #%d" cmd_id ;
        k () ;
        (* TODO: Hashtbl.take *)
        Hashtbl.remove do_ cmd_id) ;
    Hashtbl.remove dont cmd_id in
  if my_errors clt = Some k then (
    match v with
    | Value.(Error (_ts, cmd_id, err_msg)) ->
        Option.apply (hashtbl_take_option send_times cmd_id) () ;
        if err_msg = "" then (
          !logger.debug "Cmd %d: OK" cmd_id ;
          maybe_cb ~do_:on_oks ~dont:on_kos cmd_id
        ) else (
          !logger.error "Cmd %d: %s" cmd_id err_msg ;
          maybe_cb ~do_:on_kos ~dont:on_oks cmd_id
        )
    | v ->
        !logger.error "Error value is not an error: %a" Value.print v
  )

(* Replace the Client.t by the ZMQClient.session in the callbacks: *)
let check_new_cbs session_ref on_msg =
  fun clt k v uid mtime can_write can_del owner expiry ->
    check_ok clt k v ;
    (match Hashtbl.find on_dones k with
    | exception Not_found -> ()
    | cmd_id, filter, cb ->
        let srv_cmd = SrvMsg.NewKey { k ; v ; uid ; mtime ; can_write ; can_del ;
                                      owner ; expiry } in
        if filter srv_cmd then (
          Hashtbl.remove on_dones k ;
          !logger.debug "on_dones cb filter pass, calling back." ;
          log_done cmd_id ;
          cb ()
        ) else (
          !logger.debug "on_dones cb filter does not pass."
        )) ;
    let session = option_get "session_ref" __LOC__ !session_ref in
    on_msg session k v uid mtime can_write can_del owner expiry

let check_set_cbs session_ref on_msg =
  fun clt k v uid mtime ->
    check_ok clt k v ;
    (match Hashtbl.find on_dones k with
    | exception Not_found -> ()
    | cmd_id, filter, cb ->
        let srv_cmd = SrvMsg.SetKey { k ; v ; uid ; mtime } in
        if filter srv_cmd then (
          Hashtbl.remove on_dones k ;
          !logger.debug "on_dones cb filter pass, calling back." ;
          log_done cmd_id ;
          cb ()
        ) else (
          !logger.debug "on_dones cb filter does not pass."
        )) ;
    let session = option_get "session_ref" __LOC__ !session_ref in
    on_msg session k v uid mtime

let check_del_cbs session_ref on_msg =
  fun _clt k prev_v ->
    (match Hashtbl.find on_dones k with
    | exception Not_found -> ()
    | cmd_id, filter, cb ->
        let srv_cmd = SrvMsg.DelKey k in
        if filter srv_cmd then (
          Hashtbl.remove on_dones k ;
          log_done cmd_id ;
          cb ())) ;
    let session = option_get "session_ref" __LOC__ !session_ref in
    on_msg session k prev_v

let check_lock_cbs session_ref on_msg =
  fun _clt k owner expiry ->
    (* owner check is part of the filter *)
    (match Hashtbl.find on_dones k with
    | exception Not_found -> ()
    | cmd_id, filter, cb ->
        !logger.debug "Found a done filter for lock of %a"
          Key.print k ;
        let srv_cmd = SrvMsg.LockKey { k ; owner ; expiry } in
        if filter srv_cmd then (
          Hashtbl.remove on_dones k ;
          log_done cmd_id ;
          cb ())) ;
    let session = option_get "session_ref" __LOC__ !session_ref in
    on_msg session k owner expiry

let check_unlock_cbs session_ref on_msg =
  fun _clt k ->
    (match Hashtbl.find on_dones k with
    | exception Not_found -> ()
    | cmd_id, filter, cb ->
        let srv_cmd = SrvMsg.UnlockKey k in
        if filter srv_cmd then (
          Hashtbl.remove on_dones k ;
          log_done cmd_id ;
          cb ())) ;
    let session = option_get "session_ref" __LOC__ !session_ref in
    on_msg session k

(* As the same user might be sending commands at the same time, rather use
 * start from a random cmd_id so different client programs can tell their
 * errors apart; but wait for the random seed to have been set: *)
let next_id = ref None
let init_next_id () =
  next_id := Some (Random.int max_int_for_random)

let send_cmd session ?(eager=false) ?while_ ?on_ok ?on_ko ?on_done cmd =
  let my_uid =
    IO.to_string User.print_id session.clt.Client.my_uid in
  let seq =
    match !next_id with
    | None ->
        init_next_id () ;
        Option.get !next_id
    | Some i -> i in
  next_id := Some (seq + 1) ;
  let confirm_success = on_ok <> None || on_ko <> None in
  let msg = CltMsg.{ seq ; confirm_success ; cmd } in
  let crypted = Authn.is_crypted session.authn in
  !logger.debug "> Clt %s msg: %a"
    (if crypted then "crypt." else "CLEAR") CltMsg.print msg ;
  let save_cb h h_name cb =
    (* Callbacks can only be used once the error file is known: *)
    assert (session.clt.Client.my_socket <> None) ;
    let h_len = Hashtbl.length h in
    if h_len > 500 then
      Printf.sprintf "%d messages waiting for an answer, bailing out!" h_len |>
      failwith ;
    Hashtbl.add h seq cb ;
    (if h_len > 30 && h_len mod 10 = 0 then !logger.warning else !logger.debug)
      "%s size is now %d (%a...)"
      h_name h_len
      (Enum.print Int.print) (Hashtbl.keys h |> Enum.take 10) in
  let save_cb_opt h h_name cb =
    Option.may (save_cb h h_name) cb in
  let now = Unix.gettimeofday () in
  if confirm_success then (
    save_cb_opt on_oks "SyncZMQClient.on_oks" on_ok ;
    save_cb_opt on_kos "SyncZMQClient.on_kos" on_ko ;
    if session.clt.Client.my_socket <> None then
      save_cb send_times "SyncZMQClient.send_times" (update_stats_resp_time now)
  ) else
    assert (on_ok = None && on_ko = None) ;
  Option.may (fun cb ->
    let add_done_cb cb k filter =
      Hashtbl.add on_dones k (seq, filter, cb) ;
      !logger.debug "on_dones size is now %d (%a)"
        (Hashtbl.length on_dones)
        (Enum.print Key.print) (Hashtbl.keys on_dones) in
    match cmd with
    | CltMsg.Auth _
    | CltMsg.StartSync _ ->
        ()
    | CltMsg.SetKey (k, v)
    | CltMsg.NewKey (k, v, _)
    | CltMsg.UpdKey (k, v) ->
        add_done_cb cb k
          (function
          | SrvMsg.SetKey { v = v' ; _ }
          | SrvMsg.NewKey { v = v' ; _ } when Value.equal v v' -> true
          | _ -> false)
    | CltMsg.DelKey k ->
        add_done_cb cb k
          (function
          | SrvMsg.DelKey _ -> true
          | _ -> false)
    | CltMsg.LockKey (k, _) ->
        add_done_cb cb k
          (function
          | SrvMsg.LockKey { owner ; _ } when owner = my_uid -> true
          | SrvMsg.LockKey { owner ; _ } ->
              !logger.debug
                "Received a lock for key %a but for user %S instead of %S"
                Key.print k
                owner my_uid ;
              false
          | _ -> false)
    | CltMsg.LockOrCreateKey (k, _) ->
        add_done_cb cb k
          (function
          | SrvMsg.NewKey { owner ; _ }
          | SrvMsg.LockKey { owner ; _ } when owner = my_uid -> true
          | _ -> false)
    | CltMsg.UnlockKey k ->
        add_done_cb cb k
          (function
          | SrvMsg.UnlockKey _ -> true
          | _ -> false)
    | CltMsg.Bye -> ()
  ) on_done ;
  let msg = CltMsg.to_string msg |>
            Authn.wrap session.authn in
  session.last_sent <- Unix.time () ;
  (match while_ with
  | None ->
      Zmq.Socket.send_all session.zock [ "" ; msg ]
  | Some while_ ->
      retry_zmq ~while_
        (Zmq.Socket.send_all ~block:false session.zock) [ "" ; msg ]) ;
  IntCounter.inc stats_num_sync_msgs_out ;
  (* Create the entry in the conftree before (hopefully) receiving it
   * from the server, for clients in a hurry: *)
  let set_eagerly k v lock_timeo =
    let new_hv () =
      Client.{ value = v ;
               uid = session.clt.Client.my_uid ;
               mtime = now ;
               owner = if lock_timeo > 0. then session.clt.Client.my_uid
                       else "" ;
               expiry = 0. ; (* whatever *)
               eagerly = Created } in
    match Client.Tree.get session.clt.Client.h k with
    | exception Not_found ->
        let hv = new_hv () in
        session.clt.Client.h <- Client.Tree.add k hv session.clt.h ;
        Client.wait_is_over session.clt k hv
    | hv ->
        hv.value <- v ;
        hv.eagerly <- Overwritten in
  if eager then (
    match cmd with
    | SetKey (k, v) | UpdKey (k, v) ->
        set_eagerly k v 0.
    | NewKey (k, v, lock_timeo) ->
        set_eagerly k v lock_timeo
    | DelKey k ->
        (match Client.find session.clt k with
        | exception Not_found -> ()
        | hv ->
            hv.eagerly <- Deleted)
    | _ ->
        !logger.debug "Cannot do %a eagerly"
          CltMsg.print_cmd cmd
  )

let check_timeout clt = function
  | SrvMsg.DelKey k when Some k = my_errors clt ->
      !logger.error "Bummer! The server timed us out!"
  | _ -> ()

let recv_cmd session =
  (* Let's fail on EINTR and our caller retry_zmq which will do the right
   * thing: restart if no INT signal has been received. *)
  match Zmq.Socket.recv_all session.zock with
  | [ "" ; msg ] ->
      (* !logger.debug "srv message (raw): %S" msg ; *)
      (match Authn.decrypt session.authn msg with
      | Bad _ ->
          failwith "Decryption error" (* Clients keep errors for themselves *)
      | Ok msg ->
          let msg = SrvMsg.of_string msg in
          !logger.debug "< Srv msg: %a" SrvMsg.print msg ;
          IntCounter.inc stats_num_sync_msgs_in ;
          check_timeout session.clt msg ;
          msg)
  | m ->
      Printf.sprintf2 "Received unexpected message %a"
        (List.print String.print) m |>
      failwith

let unexpected_reply cmd =
  Printf.sprintf "Unexpected reply %s"
    (SrvMsg.to_string cmd) |>
  failwith

(* This locks keys one by one (waiting for the answer at every step.
 * FIXME: lock all then wait for all answers *)
let matching_keys session ?prefix f =
  Client.Tree.fold ?prefix session.clt.Client.h (fun k _ l ->
    if f k then k :: l else l
  ) []

(* Will fail it a lock is already owned, as needed (see
 * https://github.com/rixed/ramen/issues/1070) *)
let with_locked_matching
      ?while_ ?(lock_timeo=Default.sync_lock_timeout) session ?prefix f cb =
  let keys = matching_keys session ?prefix f in
  let rec loop unlock_all = function
    | [] ->
        !logger.debug "All keys locked, calling back user" ;
        cb () ;
        !logger.debug "...Back from user, unlocking everything" ;
        unlock_all ()
    | key :: rest ->
        let unlock_all' () =
          (* Keep going if unlock fails. Most of the time it's just because
           * that key has been deleted. TODO: UnlockIfExit command. *)
          send_cmd session ?while_ ~on_ok:unlock_all ~on_ko:unlock_all
            (CltMsg.UnlockKey key) in
        let on_ok () =
          loop unlock_all' rest in
        send_cmd session ?while_ ~on_ok ~on_ko:unlock_all
          (CltMsg.LockKey (key, lock_timeo))
  and last_unlock () =
    !logger.debug "All keys unlocked" in
  loop last_unlock keys

module Stage =
struct
  type t = | Conn | Auth | Sync | Config
  let to_string = function
    | Conn -> "Connection"
    | Config -> "Upload config"
    | Auth -> "Authentication"
    | Sync -> "Synchronization"
  let print oc s =
    String.print oc (to_string s)
end

module Status =
struct
  type t =
    | InitStart | InitOk | InitFail of string (* For the init stage *)
    | Ok of string | Fail of string
  let to_string = function
    | InitStart -> "Starting"
    | InitOk -> "Established"
    | InitFail s -> "Not established: "^ s
    | Ok s -> "Ok: "^ s
    | Fail s -> "Failed: "^ s
  let print oc s =
    String.print oc (to_string s)
end

let default_on_progress _session stage status =
  (match status with
  | Status.InitStart | InitOk -> !logger.debug
  | InitFail _ | Fail _ -> !logger.error
  | _ -> !logger.debug)
    "%a: %a" Stage.print stage Status.print status

let init_connect session ?while_ url on_progress =
  let connect_to = "tcp://"^ url in
  on_progress session Stage.Conn Status.InitStart ;
  try
    !logger.debug "Connecting to %s..." connect_to ;
    retry_zmq ?while_
      (Zmq.Socket.connect session.zock) connect_to ;
    on_progress session Stage.Conn Status.InitOk ;
    true
  with e ->
    on_progress session Stage.Conn Status.(InitFail (Printexc.to_string e)) ;
    false

let init_auth ?while_ session uid on_progress =
  on_progress session Stage.Auth Status.InitStart ;
  try
    send_cmd session ?while_ (CltMsg.Auth (uid, session.timeout)) ;
    match retry_zmq ?while_ recv_cmd session with
    | SrvMsg.AuthOk _ as msg ->
        Client.process_msg session.clt msg ;
        on_progress session Stage.Auth Status.InitOk ;
        true
    | SrvMsg.AuthErr s as msg ->
        Client.process_msg session.clt msg ;
        on_progress session Stage.Auth (Status.InitFail s) ;
        false
    | rep ->
        unexpected_reply rep
  with e ->
    on_progress session Stage.Auth Status.(InitFail (Printexc.to_string e)) ;
    false

let may_send_ping ?while_ session =
  let now = Unix.time () in
  if session.last_sent < now -. session.timeout *. 0.5 then (
    session.last_sent <- now ;
    !logger.info "Pinging the server to keep the session alive" ;
    let cmd = CltMsg.SetKey (Key.DevNull, Value.RamenValue T.VNull) in
    send_cmd session ?while_ cmd)

(* Receive and process incoming commands until timeout.
 * Returns the number of messages that have been read.
 * In case messages are incoming quicker than the timeout, use
 * [single] to force process_in out of the loop. *)
let process_in ?(while_=always) ?(single=false) session =
  let rec loop () =
    if while_ () then (
      may_send_ping ~while_ session ;
      match recv_cmd session with
      | exception Unix.(Unix_error ((EAGAIN|EINTR), _, _)) ->
          ()
      | msg ->
          Client.process_msg session.clt msg ;
          if not single then loop ()
    ) in
  loop ()

let process_until ~while_ session =
  while while_ () do
    process_in ~while_ session
  done

let init_sync ?while_ session topics on_progress =
  on_progress session Stage.Sync Status.InitStart ;
  let globs = List.map Globs.compile topics in
  let add_glob_for_key ?(is_dir=false) key globs =
    if List.exists (fun glob -> Globs.matches glob key) globs then (
      !logger.debug "subscribed topics already cover key %a, \
                     not subscribing separately"
        String.print_quoted key ;
      globs
    ) else
      let g = Globs.escape key in
      let g =
        if is_dir then Globs.concat g (Globs.compile "/*")
                  else g in
      g :: globs in
  (* Also subscribe to the error messages, unless it's covered already: *)
  (* Because we are authenticated: *)
  assert (session.clt.Client.my_socket <> None) ;
  let globs =
    add_glob_for_key (my_errors session.clt |> Option.get |> Key.to_string) globs |>
    add_glob_for_key ~is_dir:true
      ("clients/"^ (Option.get session.clt.my_socket |> User.string_of_socket)
                 ^"/response") in
  let rec loop = function
    | [] ->
        () (* Nothing to sync to -> nothing to wait for *)
    | [glob] ->
        (* Last command: wait until it's acked *)
        let set_synced () =
          with_lock session.wait_synced_lock (fun () ->
            session.is_synced <- true ;
            Condition.broadcast session.is_synced_cond) in
        send_cmd session ?while_ ~on_ok:set_synced (CltMsg.StartSync glob)
    | glob :: rest ->
        send_cmd session ?while_ (CltMsg.StartSync glob) ;
        loop rest in
  match loop globs with
  | exception e ->
      on_progress session Stage.Sync Status.(InitFail (Printexc.to_string e)) ;
      false
  | () ->
      on_progress session Stage.Sync Status.InitOk ;
      (* Wait until it's acked *)
      let while_ () =
        not session.is_synced &&
        (match while_ with Some f -> f () | None -> true) in
      while while_ () do
        process_in ~while_ session
      done ;
      true

(* Will be called by the C++ on a dedicated thread, never returns: *)
let start ?while_ ~url ~srv_pub_key ~username ~clt_pub_key ~clt_priv_key
          ?(topics=[]) ?(on_progress=default_on_progress)
          ?(on_sock=ignore1) ?(on_synced=ignore1)
          ?(on_new=ignore9) ?(on_set=ignore5) ?(on_del=ignore3)
          ?(on_lock=ignore4) ?(on_unlock=ignore2)
          ?(conntimeo=0.) ?(recvtimeo= ~-.1.) ?(sndtimeo= ~-.1.)
          ?(sesstimeo=Default.sync_sessions_timeout) sync_loop =
  !logger.debug "Starting ZMQ Client" ;
  let to_ms f =
    if f < 0. then -1 else int_of_float (f *. 1000.) in
  let ctx = Zmq.Context.create () in
  finally
    (fun () ->
      !logger.debug "Terminating ZMQ context..." ;
      Zmq.Context.terminate ctx)
    (fun () ->
      !logger.debug "Initializing ZMQ Client" ;
      (* Let the callbacks receive the session instead of the Client.t only.
       * This is OK because those callbacks won't be called before the initial
       * sync starts. *)
      let session = ref None in
      let on_new = check_new_cbs session on_new
      and on_set = check_set_cbs session on_set
      and on_del = check_del_cbs session on_del
      and on_lock = check_lock_cbs session on_lock
      and on_unlock = check_unlock_cbs session on_unlock in
      let clt =
        Client.make ~my_uid:username ~on_new ~on_set ~on_del ~on_lock ~on_unlock in
      !logger.debug "Create Zocket" ;
      let zock = Zmq.Socket.(create ctx dealer) in
      !logger.debug "Create session" ;
      let authn =
        if srv_pub_key = "" then
          Authn.make_clear_session ()
        else
          Authn.(make_session (Some (pub_key_of_z85 srv_pub_key))
            (pub_key_of_z85 clt_pub_key)
            (priv_key_of_z85 clt_priv_key)) in
      session := Some
        { zock ; authn ; clt ;
          timeout = sesstimeo ; last_sent = Unix.time () ;
          is_synced = false ;
          is_synced_cond = Condition.create () ;
          wait_synced_lock = Mutex.create () } ;
      let session = Option.get !session in
      finally
        (fun () -> Zmq.Socket.close zock)
        (fun () ->
          (* Timeouts must be in place before connect: *)
          (* Not implemented for some reasons, although there is a
           * ZMQ_CONNECT_TIMEOUT:
           * Zmq.Socket.set_connect_timeout zock conntimeo ; *)
          ignore conntimeo ;
          Zmq.Socket.set_receive_timeout zock (to_ms recvtimeo) ;
          Zmq.Socket.set_send_timeout zock (to_ms sndtimeo) ;
          Zmq.Socket.set_send_high_water_mark zock 0 ;
          on_sock clt ;
          let url =
            if String.contains url ':' then url
            else url ^":"^ string_of_int (
              if srv_pub_key = "" then Default.confserver_port
              else Default.confserver_port_sec) in
          if
            log_exceptions ~what:"init_connect"
              (fun () -> init_connect session ?while_ url on_progress) &&
            log_exceptions ~what:"init_auth"
              (fun () -> init_auth ?while_ session username on_progress) &&
            log_exceptions ~what:"init_sync"
              (fun () -> init_sync ?while_ session topics on_progress)
          then (
            on_synced session ;
            finally
              (fun () -> send_cmd session ?while_ CltMsg.Bye)
              (log_exceptions ~what:"sync_loop") (fun () -> sync_loop session)
          ) else failwith "Cannot initialize ZMQClient"
        ) ()
    ) ()
