(* A specific Client using TcpSocket for the values/keys defined in RamenSync. *)
open Batteries
open Stdint

open RamenConsts
open RamenLog
open RamenHelpersNoLog
open RamenHelpers
open RamenSync
module Authn = RamenAuthn
module Default = RamenConstsDefault
module Files = RamenFiles
module Metric = RamenConstsMetric

module CltMsg = Client.CltMsg
module SrvMsg = Client.SrvMsg
module TcpSocket = RamenTcpSocket
module User = RamenSyncUser

module CltCmd = Sync_client_cmd.DessserGen

open Binocle

let stats_num_sync_msgs_in =
  IntCounter.make Metric.Names.num_sync_msgs_in
    Metric.Docs.num_sync_msgs_in

let stats_num_sync_msgs_out =
  IntCounter.make Metric.Names.num_sync_msgs_out
    Metric.Docs.num_sync_msgs_out

let stats_resp_time =
  Histogram.make Metric.Names.sync_resp_time_client
    Metric.Docs.sync_resp_time_client
    Histogram.powers_of_two

type session =
  { socket : TcpSocket.Client.t ;
    authn : Authn.session ;
    mutable clt : Client.t option ; (* deferred initialization *)
    timeout : float ;
    mutable last_sent : float ;
    (* to wait for end of sync: *)
    mutable is_synced : bool ;
    is_synced_cond : Condition.t ;
    wait_synced_lock : Mutex.t ;
    (* To help with RPC like interaction, we have two callbacks here that can
     * be set when calling send_cmd, and that are automatically called when the
     * Error message (that is automatically subscribed by the server) is
     * updated. This is a hash from command id to continuation.
     * FIXME: timeout after a while and replace the continuation with
     * `raise Timeout` in that case. *)
    on_oks : (Uint32.t, unit -> unit) Hashtbl.t ;
    on_kos : (Uint32.t, unit -> unit) Hashtbl.t ;
    (* When we exec a command we might also want to have a callback when it's
     * actually effective (which is not the same as the command being accepted).
     * So here we register call backs that will be triggered automatically when
     * a given server command is received (regardless of mtime etc). *)
    on_dones :
      (Key.t, Uint32.t * (SrvMsg.t -> bool) * (unit -> unit)) Hashtbl.t }

let retry_socket ?while_ f =
  let on = function
    | Unix.(Unix_error ((EAGAIN|EWOULDBLOCK|EINTR), _, _)) ->
        true
    | TcpSocket.Client.Cannot_connect _ as e ->
        !logger.error "%s, will retry" (Printexc.to_string e) ;
        true
    | _ ->
        false in
  retry ~on ~first_delay:0.3 ?while_ f

(* For response time measurements, a hash of command ids to timestamps which
 * is cleared whenever the "answer" is received (aka. whenever the error file
 * is updated): *)
let send_times = Hashtbl.create 99

(* TODO: add command and status labels, as in RamenSyncServer. *)
let update_stats_resp_time start () =
  let resp_time = Unix.gettimeofday () -. start in
  Histogram.add stats_resp_time resp_time

let log_done cmd_id =
  !logger.debug "Cmd #%s: done!" (Uint32.to_string cmd_id)

let my_errors clt =
  Option.map (fun socket -> Key.Error (Some socket)) clt.Client.my_socket

(* Given a callback, return another one that intercept error messages and call
 * RPC continuations first: *)
let check_ok session k v =
  let maybe_cb ~do_ ~dont cmd_id =
    (match Hashtbl.find do_ cmd_id with
    | exception Not_found ->
        if not (Hashtbl.is_empty do_) then
          !logger.debug "No callback pending for cmd #%s, only for %a"
            (Uint32.to_string cmd_id)
            (Enum.print print_uint32) (Hashtbl.keys do_)
    | k ->
        !logger.debug "Calling back pending function for cmd #%s"
          (Uint32.to_string cmd_id) ;
        k () ;
        (* TODO: Hashtbl.take *)
        Hashtbl.remove do_ cmd_id) ;
    Hashtbl.remove dont cmd_id in
  let clt = option_get "check_ok" __LOC__ session.clt in
  if my_errors clt = Some k then (
    match v with
    | Value.(Error (_ts, cmd_id, err_msg)) ->
        Option.apply (hashtbl_take_option send_times cmd_id) () ;
        if err_msg = "" then (
          !logger.debug "Cmd #%s: OK" (Uint32.to_string cmd_id) ;
          maybe_cb ~do_:session.on_oks ~dont:session.on_kos cmd_id
        ) else (
          !logger.error "Cmd #%s: %s" (Uint32.to_string cmd_id) err_msg ;
          maybe_cb ~do_:session.on_kos ~dont:session.on_oks cmd_id
        )
    | v ->
        !logger.error "Error value is not an error: %a" Value.print v
  )

let check_new_cbs session on_msg =
  fun _clt k v newKey_uid mtime can_write can_del owner expiry ->
    check_ok session k v ;
    Hashtbl.find_all session.on_dones k |>
    List.iter (fun (cmd_id, filter, cb) ->
      let srv_cmd =
        SrvMsg.NewKey {
          newKey_k = k ; v ; newKey_uid ; mtime ; can_write ;
          can_del ; newKey_owner = owner ; newKey_expiry = expiry } in
      if filter srv_cmd then (
        Hashtbl.remove session.on_dones k ;
        !logger.debug "on_dones cb filter pass, calling back." ;
        log_done cmd_id ;
        cb ()
      ) else
        !logger.debug "on_dones cb filter does not pass.") ;
    on_msg session k v newKey_uid mtime can_write can_del owner expiry

let check_set_cbs session on_msg =
  fun _clt k v uid mtime ->
    check_ok session k v ;
    Hashtbl.find_all session.on_dones k |>
    List.iter (fun (cmd_id, filter, cb) ->
      let srv_cmd =
        SrvMsg.SetKey { setKey_k = k ; setKey_v = v ;
                        setKey_uid = uid ; setKey_mtime = mtime } in
      if filter srv_cmd then (
        Hashtbl.remove session.on_dones k ;
        !logger.debug "on_dones cb filter pass, calling back." ;
        log_done cmd_id ;
        cb ()
      ) else
        !logger.debug "on_dones cb filter does not pass.") ;
    on_msg session k v uid mtime

let check_del_cbs session on_msg =
  fun _clt k prev_v ->
    Hashtbl.find_all session.on_dones k |>
    List.iter (fun (cmd_id, filter, cb) ->
      let srv_cmd = SrvMsg.DelKey { delKey_k = k ; uid = "" } in
      if filter srv_cmd then (
        Hashtbl.remove session.on_dones k ;
        log_done cmd_id ;
        cb ())) ;
    on_msg session k prev_v

let check_lock_cbs session on_msg =
  fun _clt k owner expiry ->
    (* owner check is part of the filter *)
    Hashtbl.find_all session.on_dones k |>
    List.iter (fun (cmd_id, filter, cb) ->
      !logger.debug "Found a done filter for lock of %a"
        Key.print k ;
      let srv_cmd = SrvMsg.LockKey { k ; owner ; expiry } in
      if filter srv_cmd then (
        Hashtbl.remove session.on_dones k ;
        log_done cmd_id ;
        cb ())) ;
    on_msg session k owner expiry

let check_unlock_cbs session on_msg =
  fun _clt k ->
    Hashtbl.find_all session.on_dones k |>
    List.iter (fun (cmd_id, filter, cb) ->
      let srv_cmd = SrvMsg.UnlockKey k in
      if filter srv_cmd then (
        Hashtbl.remove session.on_dones k ;
        log_done cmd_id ;
        cb ())) ;
    on_msg session k

(* As the same user might be sending commands at the same time, rather use
 * start from a random cmd_id so different client programs can tell their
 * errors apart; but wait for the random seed to have been set: *)
let next_id = ref None
let init_next_id () =
  next_id := Some (Uint32.of_int (Random.int max_int_for_random))

let send_cmd
      session ?(eager=false) ?(echo=true) ?on_ok ?on_ko ?on_done cmd =
  let clt = option_get "send_cmd" __LOC__ session.clt in
  let my_uid =
    IO.to_string User.print_id clt.Client.my_uid in
  let seq =
    match !next_id with
    | None ->
        init_next_id () ;
        Option.get !next_id
    | Some i -> i in
  next_id := Some (Uint32.succ seq) ;
  let confirm_success = on_ok <> None || on_ko <> None in
  let msg = CltMsg.{ seq ; confirm_success ; echo ; cmd } in
  let crypted = Authn.is_crypted session.authn in
  !logger.debug "> %a (%s)"
    CltMsg.print msg
    (if crypted then "encrypted" else "CLEAR") ;
  let save_cb h h_name cb =
    (* Callbacks can only be used once the error file is known: *)
    assert (clt.Client.my_socket <> None) ;
    let h_len = Hashtbl.length h in
    if h_len > 500 then
      Printf.sprintf "%d messages waiting for an answer, bailing out!" h_len |>
      failwith ;
    Hashtbl.add h seq cb ;
    (if h_len > 30 && h_len mod 10 = 0 then !logger.warning else !logger.debug)
      "%s size is now %d (%a...)"
      h_name (h_len + 1)
      (Enum.print print_uint32) (Hashtbl.keys h |> Enum.take 10) in
  let save_cb_opt h h_name cb =
    Option.may (save_cb h h_name) cb in
  let now = Unix.gettimeofday () in
  if confirm_success then (
    save_cb_opt session.on_oks "SyncZMQClient.on_oks" on_ok ;
    save_cb_opt session.on_kos "SyncZMQClient.on_kos" on_ko ;
    if clt.Client.my_socket <> None then
      save_cb send_times "SyncZMQClient.send_times" (update_stats_resp_time now)
  ) else
    assert (on_ok = None && on_ko = None) ;
  Option.may (fun cb ->
    let add_done_cb cb k filter =
      Hashtbl.add session.on_dones k (seq, filter, cb) ;
      !logger.debug "SyncZMQClient.on_dones size is now %d (%a)"
        (Hashtbl.length session.on_dones)
        (Enum.print Key.print) (Hashtbl.keys session.on_dones) in
    match cmd with
    | CltCmd.Auth _
    | StartSync _ ->
        ()
    | SetKey (k, v)
    | NewKey (k, v, _, _)
    | UpdKey (k, v) ->
        add_done_cb cb k
          (function
          | SrvMsg.SetKey { setKey_v = v' ; _ }
          | SrvMsg.NewKey { v = v' ; _ } when Value.equal v v' -> true
          | _ -> false)
    | DelKey k ->
        add_done_cb cb k
          (function
          | SrvMsg.DelKey _ -> true
          | _ -> false)
    | LockKey (k, _, _) ->
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
    | LockOrCreateKey (k, _, _) ->
        add_done_cb cb k
          (function
          | SrvMsg.NewKey { newKey_owner ; _ } when newKey_owner = my_uid -> true
          | SrvMsg.LockKey { owner ; _ } when owner = my_uid -> true
          | _ -> false)
    | UnlockKey k ->
        add_done_cb cb k
          (function
          | SrvMsg.UnlockKey _ -> true
          | _ -> false)
    | Bye -> ()
  ) on_done ;
  let msg = CltMsg.to_string msg |>
            Authn.wrap session.authn in
  session.last_sent <- Unix.time () ;
  TcpSocket.Client.send session.socket (Bytes.unsafe_of_string msg) ; (* FIXME *)
  IntCounter.inc stats_num_sync_msgs_out ;
  (* Create the entry in the conftree before (hopefully) receiving it
   * from the server, for clients in a hurry: *)
  let set_eagerly k v lock_timeo =
    let new_hv () =
      Client.{ value = v ;
               uid = clt.Client.my_uid ;
               mtime = now ;
               owner = if lock_timeo > 0. then clt.Client.my_uid
                       else "" ;
               expiry = 0. ; (* whatever *)
               eagerly = Created } in
    match Client.Tree.get clt.Client.h k with
    | exception Not_found ->
        let hv = new_hv () in
        clt.Client.h <-
          Client.Tree.add k hv clt.h ;
        Client.wait_is_over clt k hv
    | hv ->
        hv.value <- v ;
        hv.eagerly <- Overwritten in
  if eager then (
    match cmd with
    | SetKey (k, v) | UpdKey (k, v) ->
        set_eagerly k v 0.
    | NewKey (k, v, lock_timeo, _recurs) ->
        set_eagerly k v lock_timeo
    | DelKey k ->
        (match Client.find clt k with
        | exception Not_found -> ()
        | hv ->
            hv.eagerly <- Deleted)
    | _ ->
        !logger.debug "Cannot do %a eagerly"
          CltMsg.print_cmd cmd
  )

let check_timeout clt = function
  | SrvMsg.DelKey { delKey_k ; _ } when Some delKey_k = my_errors clt ->
      !logger.error "Bummer! The server timed us out!"
  | _ -> ()

(* Process IO on [socket] until a message is received *)
let recv_cmd session =
  (* Let's fail on EAGAIN and our caller retry_socket which will do the right
   * thing: restart if no INT signal has been received. *)
  match TcpSocket.Client.get_msg session.socket with
  | None ->
      raise (Unix.Unix_error (EAGAIN, "get_msg", ""))
  | Some msg when Bytes.length msg = 0 ->
      !logger.warning "Disconnected from configuration server" ;
      raise Exit
  | Some msg ->
      (*!logger.debug "recv_cmd: received a message of %d bytes"
        (Bytes.length msg) ;*)
      let msg = Bytes.unsafe_to_string msg in (* FIXME *)
      (match Authn.decrypt session.authn msg with
      | Error _ ->
          failwith "Decryption error" (* Clients keep errors for themselves *)
      | Ok msg ->
          let clt = option_get "recv_cmd" __LOC__ session.clt in
          let msg = SrvMsg.of_string msg in
          !logger.debug "< %a" SrvMsg.print msg ;
          IntCounter.inc stats_num_sync_msgs_in ;
          check_timeout clt msg ;
          msg)

(* This locks keys one by one (waiting for the answer at every step.
 * FIXME: lock all then wait for all answers *)
let matching_keys session ?prefix f =
  let clt = option_get "matching_keys" __LOC__ session.clt in
  Client.Tree.fold ?prefix clt.Client.h (fun k _ l ->
    if f k then k :: l else l
  ) []

(* Will fail it a lock is already owned, as needed (see
 * https://github.com/rixed/ramen/issues/1070) *)
let with_locked_matching
      ?(lock_timeo=Default.sync_lock_timeout) session ?prefix f cb =
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
          send_cmd session ~on_ok:unlock_all ~on_ko:unlock_all
            (CltCmd.UnlockKey key) in
        let on_ok () =
          loop unlock_all' rest in
        send_cmd session ~on_ok ~on_ko:unlock_all
          (CltCmd.LockKey (key, lock_timeo, false))
  and last_unlock () =
    !logger.debug "All keys unlocked" in
  loop last_unlock keys

module Stage =
struct
  type t = | Conn | Auth | Sync
  let to_string = function
    | Conn -> "Connection"
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

let init_auth ?while_ session uid =
  let clt = option_get "init_auth" __LOC__ session.clt in
  try
    send_cmd session (CltCmd.Auth (uid, session.timeout)) ;
    match retry_socket ?while_ recv_cmd session with
    | SrvMsg.AuthOk _ as msg ->
        Client.process_msg clt msg ;
        true
    | SrvMsg.AuthErr s as msg ->
        !logger.error "Authentication error: %s" s ;
        Client.process_msg clt msg ;
        false
    | cmd->
        Printf.sprintf "Unexpected reply %s"
          (SrvMsg.to_string cmd) |>
        failwith
  with Exit ->
    let bt = Printexc.get_raw_backtrace () in
    Printexc.raise_with_backtrace Exit bt
  | e ->
    !logger.error "Failed to Auth: %s" (Printexc.to_string e) ;
    false

let may_send_ping session =
  let now = Unix.time () in
  if session.last_sent < now -. session.timeout *. 0.5 then (
    session.last_sent <- now ;
    !logger.info "Pinging the server to keep the session alive" ;
    let cmd = CltCmd.SetKey (Key.DevNull, Value.RamenValue Raql_value.VNull) in
    send_cmd session cmd)

(* Receive and process incoming commands until timeout.
 * In case messages are incoming quicker than the timeout, use
 * [max_count] to force process_in out of the loop.
 * Regardless of while_, it will exit after max recvtimeo. *)
let process_in ?(while_=always) ?(max_count=0) session =
  let stop =
    if session.socket.TcpSocket.Client.recvtimeo > 0. then
      Unix.gettimeofday () +. session.socket.recvtimeo
    else max_float in
  let clt = option_get "process_in" __LOC__ session.clt in
  let rec loop count =
    if while_ () && stop > Unix.gettimeofday () then (
      may_send_ping session ;
      match recv_cmd session with
      | exception Unix.(Unix_error ((EAGAIN|EINTR), _, _)) ->
          !logger.debug "No more configuration messages to read for now"
      | msg ->
          Client.process_msg clt msg ;
          if max_count <= 0 || count < max_count then loop (count + 1)
    ) in
  loop 1

let process_until ~while_ session =
  while while_ () do
    process_in ~while_ session
  done

let init_sync ?(while_=always) session topics =
  let clt = option_get "init_sync" __LOC__ session.clt in
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
  assert (clt.Client.my_socket <> None) ;
  let globs =
    add_glob_for_key (my_errors clt |> Option.get |> Key.to_string) globs |>
    add_glob_for_key ~is_dir:true
      ("clients/"^ (Option.get clt.my_socket |> User.string_of_socket)
                 ^"/response") in
  let rec loop globs =
    if while_ () then
      match globs with
      | [] ->
          () (* Nothing to sync to -> nothing to wait for *)
      | [ glob ] ->
          (* Last command: wait until it's acked *)
          let set_synced () =
            with_lock session.wait_synced_lock (fun () ->
              session.is_synced <- true ;
              Condition.broadcast session.is_synced_cond) in
          let sel = Globs.decompile glob in
          send_cmd session ~on_ok:set_synced (CltCmd.StartSync sel)
      | glob :: rest ->
          let sel = Globs.decompile glob in
          send_cmd session (CltCmd.StartSync sel) ;
          loop rest in
  match loop globs with
  | exception Exit ->
      let bt = Printexc.get_raw_backtrace () in
      Printexc.raise_with_backtrace Exit bt
  | exception e ->
      !logger.error "Failed to sync: %s" (Printexc.to_string e) ;
      false
  | () ->
      (* Wait until it's acked *)
      (* FIXME: make use of is_synced_cond *)
      let while_ () = not session.is_synced && while_ () in
      while while_ () do
        process_in ~while_ session
      done ;
      if session.is_synced then
        !logger.debug "Config client is now synchronized with the server" ;
      true

(* Will be called by the C++ on a dedicated thread, never returns: *)
let start ?while_ ~url ~srv_pub_key ~username ~clt_pub_key ~clt_priv_key
          ?(topics=[])
          ?(on_sock=ignore1) ?(on_synced=ignore1)
          ?(on_new=ignore9) ?(on_set=ignore5) ?(on_del=ignore3)
          ?(on_lock=ignore4) ?(on_unlock=ignore2)
          ?(conntimeo=0.) ?(recvtimeo= ~-.1.)
          ?(sesstimeo=Default.sync_sessions_timeout) sync_loop =
  let use_encryption =
    srv_pub_key <> "" && clt_pub_key <> "" && clt_priv_key <> "" in
  !logger.debug "Initializing configuration Client (%S, %S, %S -> %s)"
    srv_pub_key clt_pub_key clt_priv_key
    (if use_encryption then "use encryption" else "no encryption") ;
  let url = if url = "" then "localhost" else url in
  let url =
    if String.contains url ':' then url
    else url ^":"^ string_of_int (
      if use_encryption then Default.confserver_port_sec
      else Default.confserver_port) in
  let host, service_name =
    try String.split ~by:":" url
    with Not_found ->
      invalid_arg ("init_connect: invalid url "^ url) in
  !logger.debug "Connecting to configuration server at %s..." url ;
  match retry_socket ?while_ (TcpSocket.Client.make recvtimeo host)
                     service_name with
  | exception Exit ->
      let bt = Printexc.get_raw_backtrace () in
      Printexc.raise_with_backtrace Exit bt
  | exception e ->
      Printf.sprintf "Cannot connect to configuration server: %s"
        (Printexc.to_string e) |>
      failwith
  | socket ->
      finally
        (fun () ->
          !logger.debug "Closing connection to config server" ;
          TcpSocket.(Client.send socket empty_msg))
        (fun () ->
          (* Let the callbacks receive the session instead of the Client.t only.
           * This is OK because those callbacks won't be called before the initial
           * sync starts. *)
          let authn =
            if use_encryption then
              Authn.(make_session (Some (pub_key_of_z85 srv_pub_key))
                (pub_key_of_z85 clt_pub_key)
                (priv_key_of_z85 clt_priv_key))
            else
              Authn.make_clear_session () in
          let session =
            { socket ; authn ; clt = None ;
              timeout = sesstimeo ; last_sent = Unix.time () ;
              is_synced = false ;
              is_synced_cond = Condition.create () ;
              wait_synced_lock = Mutex.create () ;
              on_oks = Hashtbl.create 5 ;
              on_kos = Hashtbl.create 5 ;
              on_dones = Hashtbl.create 5 } in
          let on_new = check_new_cbs session on_new
          and on_set = check_set_cbs session on_set
          and on_del = check_del_cbs session on_del
          and on_lock = check_lock_cbs session on_lock
          and on_unlock = check_unlock_cbs session on_unlock in
          session.clt <-
            Some (Client.make ~my_uid:username ~on_new ~on_set ~on_del
                              ~on_lock ~on_unlock) ;
          ignore conntimeo ;
          on_sock (Option.get session.clt) ;
          if
            log_exceptions ~what:"init_auth"
              (fun () -> init_auth ?while_ session username) &&
            log_exceptions ~what:"init_sync"
              (fun () -> init_sync ?while_ session topics)
          then (
            on_synced session ;
            finally
              (fun () ->
                send_cmd session CltCmd.Bye ; (* FIXME: useless *)
                TcpSocket.Client.shutdown session.socket)
              (log_exceptions ~what:"sync_loop") (fun () -> sync_loop session)
          ) else failwith "Cannot initialize configuration client"
        ) ()
