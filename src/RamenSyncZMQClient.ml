(* A specific Client using ZMQ, for the values/keys defined in RamenSync. *)
open Batteries
open RamenConsts
open RamenLog
open RamenHelpers

module Value = RamenSync.Value
module Client = RamenSyncClient.Make (Value) (RamenSync.Selector)
module Key = RamenSync.Key

let retry_zmq ?while_ f =
  let on = function
    (* EWOULDBLOCK: According to 0mq documentation blocking is supposed
     * to be signaled with EAGAIN but... *)
    | Unix.(Unix_error ((EAGAIN|EWOULDBLOCK), _, _)) -> true
    | _ -> false in
  retry ~on ~first_delay:0.3 ?while_ f

(* To help with RPC like interaction, we have two callbacks here that can be set
 * when calling send_cmd, and that are automatically called when the Error message
 * (that is automatically subscribed by the server) is updated. This is a hash
 * from command id to continuation. *)
let on_oks : (int, unit -> unit) Hashtbl.t = Hashtbl.create 5
let on_kos : (int, unit -> unit) Hashtbl.t = Hashtbl.create 5
let my_login = ref ""

(* Given a callback, return another cllback that intercept error messages and call
 * RPC continuations first: *)
let check_err on_msg =
  let maybe_cb h cmd_id =
    Hashtbl.modify_opt cmd_id (function
      | None -> None
      | Some k ->
          !logger.debug "Calling back pending function for cmd %d" cmd_id ;
          k () ; None
    ) h in
  fun clt k v u mt ->
    (match k with
    | Key.Error (Some n) when n = !my_login ->
        (match v with
        | Value.(Error (_ts, cmd_id, err_msg)) ->
            if err_msg = "" then (
              !logger.debug "Cmd %d: OK" cmd_id ;
              maybe_cb on_oks cmd_id
            ) else (
              !logger.error "Cmd %d: %s" cmd_id err_msg ;
              maybe_cb on_kos cmd_id
            )
        | v ->
            !logger.error "Error value is not an error: %a" Value.print v)
    | _ -> ()) ;
    on_msg clt k v u mt

(* As the same user might be sending commands at the same time, rather use
 * start from a random cmd_id so different client programs can tell their
 * errors apart; but wait for the random seed to have been set: *)
let next_id = ref None
let init_next_id () =
  next_id := Some (Random.int max_int_for_random)

let send_cmd zock ?while_ ?on_ok ?on_ko cmd =
  let cmd_id =
    match !next_id with
    | None ->
        init_next_id () ;
        Option.get !next_id
    | Some i ->
        next_id := Some (i + 1) ;
        i in
  let msg = cmd_id, cmd in
  let rem h h_name cb =
    Option.may (fun cb ->
      Hashtbl.add h cmd_id cb ;
      let h_len = Hashtbl.length h in
      if h_len > 10 then !logger.error "%s size is not %d" h_name h_len
    ) cb in
  rem on_oks "SyncZMQClient.on_oks" on_ok ;
  rem on_kos "SyncZMQClient.on_kos" on_ko ;
  !logger.info "Sending command %a"
    Client.CltMsg.print msg ;
  let s = Client.CltMsg.to_string msg in
  (match while_ with
  | None ->
      Zmq.Socket.send_all zock [ "" ; s ]
  | Some while_ ->
      !logger.debug "send without blocking..." ;
      retry_zmq ~while_
        (Zmq.Socket.send_all ~block:false zock) [ "" ; s ]) ;
  !logger.debug "done sending"

let recv_cmd zock =
  match Zmq.Socket.recv_all zock with
  | [ "" ; s ] ->
      !logger.debug "srv message (raw): %S" s ;
      Client.SrvMsg.of_string s
  | m ->
      Printf.sprintf2 "Received unexpected message %a"
        (List.print String.print) m |>
      failwith

let unexpected_reply cmd =
  Printf.sprintf "Unexpected reply %s"
    (Client.SrvMsg.to_string cmd) |>
  failwith

let lock_matching clt f =
  (* TODO *)
  ignore clt ; ignore f

let unlock_matching clt f =
  (* TODO *)
  ignore clt ; ignore f

module Stage =
struct
  type t = | Conn | Auth | Sync
  let to_string = function
    | Conn -> "Connecting"
    | Auth -> "Authenticating"
    | Sync -> "Synchronizing"
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

let default_on_progress stage status =
  (match status with
  | Status.InitStart | InitOk -> !logger.info
  | InitFail _ | Fail _ -> !logger.error
  | _ -> !logger.debug)
    "%a: %a" Stage.print stage Status.print status

let init_connect ?while_ url zock on_progress =
  let url = if String.contains url ':' then url
            else url ^":"^ string_of_int Default.confserver_port in
  let connect_to = "tcp://"^ url in
  on_progress Stage.Conn Status.InitStart ;
  try
    !logger.info "Connecting to %s..." connect_to ;
    retry_zmq ?while_
      (Zmq.Socket.connect zock) connect_to ;
    on_progress Stage.Conn Status.InitOk
  with e ->
    on_progress Stage.Conn Status.(InitFail (Printexc.to_string e))

let init_auth ?while_ login zock on_progress =
  my_login := login ;
  on_progress Stage.Auth Status.InitStart ;
  try
    send_cmd zock ?while_ (Client.CltMsg.Auth login) ;
    match retry_zmq ?while_ recv_cmd zock with
    | Client.SrvMsg.Auth "" ->
        on_progress Stage.Auth Status.InitOk
    | Client.SrvMsg.Auth err ->
        failwith err
    | rep ->
        unexpected_reply rep
  with e ->
    on_progress Stage.Auth Status.(InitFail (Printexc.to_string e))

let init_sync ?while_ zock topics on_progress =
  on_progress Stage.Sync Status.InitStart ;
  let globs = List.map Globs.compile topics in
  (* Also subscribe to the error messages, unless it's covered already: *)
  let my_errors = Key.(Error (Some !my_login) |> to_string) in
  let globs =
    if List.exists (fun glob -> Globs.matches glob my_errors) globs then (
      !logger.debug "subscribed topics already cover error stream %S, \
                     not subscribing separately" my_errors ;
      globs
    ) else
      Globs.escape my_errors :: globs
  in
  try
    List.iter (fun glob ->
      send_cmd zock ?while_ (Client.CltMsg.StartSync glob)
    ) globs ;
    on_progress Stage.Sync Status.InitOk
  with e ->
    on_progress Stage.Sync Status.(InitFail (Printexc.to_string e))

(* Will be called by the C++ on a dedicated thread, never returns: *)
let start ?while_ url creds ?(topics=[])
          ?(on_progress=default_on_progress) ?(on_sock=ignore)
          ?(on_new=ignore6) ?(on_set=ignore6) ?(on_del=ignore4)
          ?(on_lock=ignore4) ?(on_unlock=ignore3)
          ?(conntimeo=0.) ?(recvtimeo= ~-.1.) ?(sndtimeo= ~-.1.) sync_loop =
  let to_ms f =
    if f < 0. then -1 else int_of_float (f *. 1000.) in
  let ctx = Zmq.Context.create () in
  finally
    (fun () -> Zmq.Context.terminate ctx)
    (fun () ->
      let zock = Zmq.Socket.(create ctx dealer) in
      finally
        (fun () -> Zmq.Socket.close zock)
        (fun () ->
          on_sock zock ;
          (* Timeouts must be in place before connect: *)
          (* Not implemented for some reasons, although there is a
           * ZMQ_CONNECT_TIMEOUT:
           * Zmq.Socket.set_connect_timeout zock conntimeo ; *)
          ignore conntimeo ;
          Zmq.Socket.set_receive_timeout zock (to_ms recvtimeo) ;
          Zmq.Socket.set_send_timeout zock (to_ms sndtimeo) ;
          Zmq.Socket.set_send_high_water_mark zock 0 ;
          log_exceptions ~what:"init_connect"
            (fun () -> init_connect ?while_ url zock on_progress) ;
          log_exceptions ~what:"init_auth"
            (fun () -> init_auth ?while_ creds zock on_progress) ;
          log_exceptions ~what:"init_sync"
            (fun () -> init_sync ?while_ zock topics on_progress) ;
          let on_new = check_err (on_new zock)
          and on_set = check_err (on_set zock)
          and on_del = on_del zock
          and on_lock = on_lock zock
          and on_unlock = on_unlock zock in
          let clt = Client.make ~on_new ~on_set ~on_del ~on_lock ~on_unlock in
          log_exceptions ~what:"sync_loop"
            (fun () -> sync_loop zock clt)
        ) ()
    ) ()

(* Receive and process incoming commands until timeout.
 * Returns the number of messages that have been read. *)
let process_in ?(while_=always) zock clt =
  let rec loop msg_count =
    if while_ () then
      match recv_cmd zock with
      | exception Unix.(Unix_error (EAGAIN, _, _)) ->
          msg_count
      | msg ->
          Client.process_msg clt msg ;
          loop (msg_count + 1)
    else
      msg_count in
  loop 0
