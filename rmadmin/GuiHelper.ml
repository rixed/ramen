open Batteries
open RamenConsts
open RamenLog
open RamenHelpers
module ZMQClient = RamenSyncZMQClient

(*
 * SyncConf client
 *)

module Value = ZMQClient.Value
module Client = ZMQClient.Client
module Key = ZMQClient.Key

(* The idea is to do the networking work of connecting, sending/receiving
 * messages and maintaining the config in OCaml.
 * Then, at every message, in addition to maintaining the conf tree we also,
 * depending on the message, call a C wrapper to the Qt signal. *)

let recv_cmd zock =
  match Zmq.Socket.recv_all zock with
  | [ "" ; s ] ->
      let msg = Client.SrvMsg.of_string s in
      !logger.debug "srv message: %a"
        Client.SrvMsg.print msg ;
      msg
  | m ->
      Printf.sprintf2 "Received unexpected message %a"
        (List.print String.print) m |>
      failwith

let unexpected_reply cmd =
  Printf.sprintf "Unexpected reply %s"
    (Client.SrvMsg.to_string cmd) |>
  failwith

external signal_conn : string -> ZMQClient.Status.t -> unit = "signal_conn"
let init_connect url zock =
  let connect_to = "tcp://"^ url in
  signal_conn url InitStart ;
  try
    !logger.info "Connecting to %s..." connect_to ;
    Zmq.Socket.connect zock connect_to ;
    signal_conn url InitOk
  with e ->
    signal_conn url (InitFail (Printexc.to_string e))

external signal_auth : ZMQClient.Status.t -> unit = "signal_auth"
external signal_sync : ZMQClient.Status.t -> unit = "signal_sync"

(* Will be set form the C++ thread when the sync thread should exit *)
external should_quit : unit -> bool = "should_quit"

type pending_req =
  | NoReq
  | New of string * Value.t
  | Set of string * Value.t
  | Lock of string
  | LockOrCreate of string
  | Unlock of string
  | Del of string

external next_pending_request : unit -> pending_req = "next_pending_request"

external conf_new_key : string -> Value.t -> string -> unit = "conf_new_key"

let on_new _zock clt k v uid _mtime =
  ignore clt ;
  Gc.compact () ;
  conf_new_key (Key.to_string k) v uid ;
  Gc.compact ()

external conf_set_key : string -> Value.t -> unit = "conf_set_key"

let on_set _zock clt k v _u _mtime =
  ignore clt ;
  Gc.compact () ;
  conf_set_key (Key.to_string k) v ;
  Gc.compact ()

external conf_del_key : string -> unit = "conf_del_key"

let on_del _zock clt k _v =
  ignore clt ;
  Gc.compact () ;
  conf_del_key (Key.to_string k) ;
  Gc.compact ()

external conf_lock_key : string -> string -> unit = "conf_lock_key"

let on_lock _zock clt k uid =
  ignore clt ;
  Gc.compact () ;
  conf_lock_key (Key.to_string k) uid ;
  Gc.compact ()

external conf_unlock_key : string -> unit = "conf_unlock_key"

let on_unlock _zock clt k =
  ignore clt ;
  Gc.compact () ;
  conf_unlock_key (Key.to_string k) ;
  Gc.compact ()

let sync_loop zock clt =
  let msg_count = ref 0 in
  let handle_msgs_in () =
    match recv_cmd zock with
    | exception Unix.(Unix_error (EAGAIN, _, _)) ->
        ()
    | msg ->
        !logger.debug "Received: %a" Client.SrvMsg.print msg ;
        Client.process_msg clt msg ;
        incr msg_count ;
        (*!logger.debug "received %d messages" !msg_count ;*)
        if !msg_count mod 10 = 0 then
          let status_msg =
            Printf.sprintf "%d messages, %d keys"
              !msg_count
              (Client.H.length clt.h) in
          Gc.compact () ;
          signal_sync (Ok status_msg) ;
          Gc.compact () in
  let rec handle_msgs_out () =
    Gc.compact () ;
    match next_pending_request () with
    | NoReq -> ()
    | New (k, v) ->
        ZMQClient.send_cmd clt zock (Client.CltMsg.NewKey (Key.of_string k, v)) ;
        handle_msgs_out ()
    | Set (k, v) ->
        ZMQClient.send_cmd clt zock (Client.CltMsg.SetKey (Key.of_string k, v)) ;
        handle_msgs_out ()
    | Lock k ->
        ZMQClient.send_cmd clt zock (Client.CltMsg.LockKey (Key.of_string k)) ;
        handle_msgs_out ()
    | LockOrCreate k ->
        ZMQClient.send_cmd clt zock (Client.CltMsg.LockOrCreateKey (Key.of_string k)) ;
        handle_msgs_out ()
    | Unlock k ->
        ZMQClient.send_cmd clt zock (Client.CltMsg.UnlockKey (Key.of_string k)) ;
        handle_msgs_out ()
    | Del k ->
        ZMQClient.send_cmd clt zock (Client.CltMsg.DelKey (Key.of_string k)) ;
        handle_msgs_out ()
  in
  while not (should_quit ()) do
    Gc.compact () ;
    try
      handle_msgs_in () ;
      handle_msgs_out () ;
      Gc.compact ()
    with e ->
      print_exception ~what:"sync loop" e ;
      signal_sync (Fail (Printexc.to_string e))
  done

let on_progress url stage status =
  (match stage with
  | ZMQClient.Stage.Conn -> signal_conn url
  | ZMQClient.Stage.Auth -> signal_auth
  | ZMQClient.Stage.Sync -> signal_sync) status

let register_senders zock clt =
  let lock_from_cpp k =
    ZMQClient.send_cmd clt zock (Client.CltMsg.LockOrCreateKey k)
  and unlock_from_cpp k =
    ZMQClient.send_cmd clt zock (Client.CltMsg.UnlockKey k)
  in
  ignore (Callback.register "lock_from_cpp" lock_from_cpp) ;
  ignore (Callback.register "unlock_from_cpp" unlock_from_cpp)

(* Will be called by the C++ on a dedicated thread, never returns: *)
let start_sync url creds () =
  ZMQClient.start
    url creds ~topics:["*"] ~on_progress:(on_progress url) ~on_sock:register_senders
    ~on_new ~on_set ~on_del ~on_lock ~on_unlock
    ~recvtimeo:0.1 sync_loop

let init debug quiet url creds =
  if debug && quiet then
    failwith "Options --debug and --quiet are incompatible." ;
  let log_level =
    if debug then Debug else if quiet then Quiet else Normal in
  init_logger log_level ;
  (* Register the functions that will be called from C++ *)
  ignore (Callback.register "start_sync" (start_sync url creds)) ;
  !logger.info "Done with the command line"

(*
 * Command line options
 *)

open Cmdliner

let debug =
  let env = Term.env_info "RAMEN_DEBUG" in
  let i = Arg.info ~doc:CliInfo.debug
                   ~env [ "d"; "debug" ] in
  Arg.(value (flag i))

let quiet =
  let env = Term.env_info "RAMEN_QUIET" in
  let i = Arg.info ~doc:CliInfo.quiet
                   ~env [ "q"; "quiet" ] in
  Arg.(value (flag i))

let confserver_url =
  let env = Term.env_info "RAMEN_CONFSERVER" in
  let i = Arg.info ~doc:CliInfo.confserver_url
                   ~env [ "url" ] in
  let def = "localhost:"^ string_of_int Default.confserver_port in
  Arg.(value (opt string def i))

(* Run the program printing exceptions, and exit *)
let print_exn f =
  try f ()
  with Exit -> exit 0
     | Failure msg | Invalid_argument msg ->
         Printf.eprintf "%s\n" msg ;
         exit 1

let cli_parse_result =
  let version = RamenVersions.release_tag in
  let i = Term.info ~doc:"RamenAdmin" ~version "rmadmin" in
  match
    print_exn (fun () ->
      Term.eval ~catch:false
        Term.(const init $ debug $ quiet $ confserver_url $ const "admin", i))
  with `Error _ -> exit 1
     | `Version | `Help -> exit 0
     | `Ok () -> ()
