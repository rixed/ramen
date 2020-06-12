open Batteries
open RamenConsts
open RamenLog
open RamenHelpers
module T = RamenTypes
module ZMQClient = RamenSyncZMQClient
module Files = RamenFiles

let gc_debug = false

(*
 * Helpers to convert from/to T.value and string:
 *)

let value_of_string structure s =
  let typ = T.{ structure ; nullable = false } in
  match T.of_string ~typ s with
  | Result.Ok v -> v
  | Result.Bad msg ->
      !logger.error "Cannot convert %S into a value of type %a: %s"
        s T.print_structure structure msg ;
      VNull

(*
 * SyncConf client
 *)

module Value = RamenSync.Value
module Client = RamenSync.Client
module Key = RamenSync.Key
module SrvMsg = Client.SrvMsg

(* The idea is to do the networking work of connecting, sending/receiving
 * messages and maintaining the config in OCaml.
 * Then, at every message, in addition to maintaining the conf tree we also,
 * depending on the message, call a C wrapper to the Qt signal. *)

let unexpected_reply cmd =
  Printf.sprintf "Unexpected reply %s"
    (SrvMsg.to_string cmd) |>
  failwith

external signal_conn : string -> ZMQClient.Status.t -> unit = "signal_conn"
external signal_auth : ZMQClient.Status.t -> unit = "signal_auth"
external signal_sync : ZMQClient.Status.t -> unit = "signal_sync"

(* Will be set form the C++ thread when the sync thread should exit *)
external should_quit : unit -> bool = "should_quit"

type pending_req =
  | NoReq
  | New of string * Value.t * float
  | Set of string * Value.t
  | Lock of string * float
  | LockOrCreate of string * float
  | Unlock of string
  | Del of string

external next_pending_request : unit -> pending_req = "next_pending_request"

external conf_new_key :
  string -> Value.t -> string -> float -> bool -> bool -> string -> float -> unit =
    "no_use_for_bytecode" "conf_new_key"

external conf_sync_finished : unit -> unit = "conf_sync_finished"

let on_synced _session =
  if gc_debug then Gc.compact () ;
  conf_sync_finished () ;
  if gc_debug then Gc.compact ()

let on_new _session k v uid mtime can_write can_del owner expiry =
  if gc_debug then Gc.compact () ;
  conf_new_key (Key.to_string k) v uid mtime can_write can_del owner expiry ;
  if gc_debug then Gc.compact ()

external conf_set_key : string -> Value.t -> string -> float -> unit = "conf_set_key"

let on_set _session k v u mtime =
  if gc_debug then Gc.compact () ;
  conf_set_key (Key.to_string k) v u mtime ;
  if gc_debug then Gc.compact ()

external conf_del_key : string -> unit = "conf_del_key"

let on_del _session k _v =
  if gc_debug then Gc.compact () ;
  conf_del_key (Key.to_string k) ;
  if gc_debug then Gc.compact ()

external conf_lock_key : string -> string -> float -> unit = "conf_lock_key"

let on_lock _session k owner expiry =
  if gc_debug then Gc.compact () ;
  conf_lock_key (Key.to_string k) owner expiry ;
  if gc_debug then Gc.compact ()

external conf_unlock_key : string -> unit = "conf_unlock_key"

let on_unlock _session k =
  if gc_debug then Gc.compact () ;
  conf_unlock_key (Key.to_string k) ;
  if gc_debug then Gc.compact ()

let sync_loop session =
  if gc_debug then Gc.compact () ;
  let msg_count = ref 0 in
  let handle_msgs_in () =
    match ZMQClient.recv_cmd session with
    | exception Unix.(Unix_error ((EAGAIN|EINTR), _, _)) ->
        ()
    | msg ->
        (* The internal store of the sync client is needed only internally
         * to make sense of received messages. To limit its growth, let's
         * not store tail tuples: *)
        Client.process_msg session.clt msg ;
        (match msg with
        | SrvMsg.SetKey { k = Tails _ as k ; _ } ->
            session.clt.Client.h <- Client.Tree.rem k session.clt.h
        | _ -> ()) ;
        incr msg_count ;
        (*!logger.debug "received %d messages" !msg_count ;*)
        if !msg_count land 15 = 0 then (
          let status_msg =
            Printf.sprintf "%d messages, %d keys"
              !msg_count
              (Client.Tree.length session.clt.h) in
          if gc_debug then Gc.compact () ;
          signal_sync (Ok status_msg) ;
          if gc_debug then Gc.compact ()
        ) in
  let rec handle_msgs_out () =
    if gc_debug then Gc.compact () ;
    match next_pending_request () with
    | NoReq ->
        ()
    | New (k, v, t) ->
        (* TODO: also pass recurs *)
        ZMQClient.send_cmd
          session (Client.CltMsg.NewKey (Key.of_string k, v, t, false)) ;
        handle_msgs_out ()
    | Set (k, v) ->
        ZMQClient.send_cmd
          session (Client.CltMsg.SetKey (Key.of_string k, v)) ;
        handle_msgs_out ()
    | Lock (k, t) ->
        (* TODO: also pass recurs *)
        ZMQClient.send_cmd session
          (Client.CltMsg.LockKey (Key.of_string k, t, false)) ;
        handle_msgs_out ()
    | LockOrCreate (k, t) ->
        (* TODO: also pass recurs *)
        ZMQClient.send_cmd session
          (Client.CltMsg.LockOrCreateKey (Key.of_string k, t, false)) ;
        handle_msgs_out ()
    | Unlock k ->
        ZMQClient.send_cmd session
          (Client.CltMsg.UnlockKey (Key.of_string k)) ;
        handle_msgs_out ()
    | Del k ->
        ZMQClient.send_cmd session
          (Client.CltMsg.DelKey (Key.of_string k)) ;
        handle_msgs_out ()
  in
  while not (should_quit ()) do
    if gc_debug then Gc.compact () ;
    try
      ZMQClient.may_send_ping session ;
      handle_msgs_in () ;
      handle_msgs_out () ;
      if gc_debug then Gc.compact ()
    with e ->
      print_exception ~what:"sync loop" e ;
      signal_sync (Fail (Printexc.to_string e))
  done ;
  !logger.info "Flushing pending requests..." ;
  handle_msgs_out ()

external set_my_id : string -> string -> unit = "set_my_id"

let on_progress url session stage status =
  if stage = ZMQClient.Stage.Auth && status = ZMQClient.Status.InitOk then (
    let my_errors =
      ZMQClient.my_errors session.ZMQClient.clt |>
      option_get "my_errors" __LOC__ |> Key.to_string in
    let my_socket =
      session.clt.Client.my_socket |> option_get "my_socket" __LOC__ |>
      ZMQClient.User.string_of_socket in
    !logger.info "Setting my errors key to %S" my_errors ;
    set_my_id my_errors my_socket
  ) ;
  (match stage with
  | ZMQClient.Stage.Conn -> signal_conn url
  | ZMQClient.Stage.Auth -> signal_auth
  | ZMQClient.Stage.Sync -> signal_sync) status

(* Will be called by the C++ on a dedicated thread, never returns: *)
let start_sync url username srv_pub_key clt_pub_key clt_priv_key =
  Gc.compact () ;
  !logger.info "Will connect to %S using key %S" url srv_pub_key ;
  log_and_ignore_exceptions ~what:"Initializing config client" (fun () ->
    ZMQClient.start
      ~url ~srv_pub_key ~username ~clt_pub_key ~clt_priv_key
      ~topics:["*"] ~on_progress:(on_progress url)
      ~on_synced ~on_new ~on_set ~on_del ~on_lock ~on_unlock
      ~recvtimeo:0.1 sync_loop
  ) ()

let init =
  ignore (Thread.self ()) ;  (* Some say it's not useless *)
  init_logger (if Array.mem "--debug" Sys.argv then Debug else Normal) ;
  (* Register the functions that will be called from C++ *)
  ignore (Callback.register "start_sync" start_sync) ;
  ignore (Callback.register "value_of_string" value_of_string)
