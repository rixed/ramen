open Batteries
open RamenConsts
open RamenLog
open RamenHelpers
module T = RamenTypes
module ZMQClient = RamenSyncZMQClient

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

(* The idea is to do the networking work of connecting, sending/receiving
 * messages and maintaining the config in OCaml.
 * Then, at every message, in addition to maintaining the conf tree we also,
 * depending on the message, call a C wrapper to the Qt signal. *)

let unexpected_reply cmd =
  Printf.sprintf "Unexpected reply %s"
    (Client.SrvMsg.to_string cmd) |>
  failwith

external signal_conn : string -> ZMQClient.Status.t -> unit = "signal_conn"
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

external conf_new_key :
  string -> Value.t -> string -> float -> string -> float -> unit =
    "no_use_for_bytecode" "conf_new_key"

let on_new clt k v uid mtime owner expiry =
  ignore clt ;
  Gc.compact () ;
  conf_new_key (Key.to_string k) v uid mtime owner expiry ;
  Gc.compact ()

external conf_set_key : string -> Value.t -> string -> float -> unit = "conf_set_key"

let on_set clt k v u mtime =
  ignore clt ;
  Gc.compact () ;
  conf_set_key (Key.to_string k) v u mtime ;
  Gc.compact ()

external conf_del_key : string -> unit = "conf_del_key"

let on_del clt k _v =
  ignore clt ;
  Gc.compact () ;
  conf_del_key (Key.to_string k) ;
  Gc.compact ()

external conf_lock_key : string -> string -> float -> unit = "conf_lock_key"

let on_lock clt k owner expiry =
  ignore clt ;
  Gc.compact () ;
  conf_lock_key (Key.to_string k) owner expiry ;
  Gc.compact ()

external conf_unlock_key : string -> unit = "conf_unlock_key"

let on_unlock clt k =
  ignore clt ;
  Gc.compact () ;
  conf_unlock_key (Key.to_string k) ;
  Gc.compact ()

let sync_loop clt =
  Gc.compact () ;
  let msg_count = ref 0 in
  let handle_msgs_in () =
    match ZMQClient.recv_cmd () with
    | exception Unix.(Unix_error (EAGAIN, _, _)) ->
        ()
    | msg ->
        Client.process_msg clt msg ;
        incr msg_count ;
        (*!logger.debug "received %d messages" !msg_count ;*)
        if !msg_count mod 10 = 0 then (
          let status_msg =
            Printf.sprintf "%d messages, %d keys"
              !msg_count
              (Client.H.length clt.h) in
          Gc.compact () ;
          signal_sync (Ok status_msg) ;
          Gc.compact ()
        ) in
  let rec handle_msgs_out () =
    Gc.compact () ;
    match next_pending_request () with
    | NoReq -> ()
    | New (k, v) ->
        ZMQClient.send_cmd clt (Client.CltMsg.NewKey (Key.of_string k, v, 0.)) ;
        handle_msgs_out ()
    | Set (k, v) ->
        ZMQClient.send_cmd clt (Client.CltMsg.SetKey (Key.of_string k, v)) ;
        handle_msgs_out ()
    | Lock k ->
        ZMQClient.send_cmd clt
          (Client.CltMsg.LockKey (Key.of_string k, Default.sync_gui_lock_timeout)) ;
        handle_msgs_out ()
    | LockOrCreate k ->
        ZMQClient.send_cmd clt
          (Client.CltMsg.LockOrCreateKey (Key.of_string k, Default.sync_gui_lock_timeout)) ;
        handle_msgs_out ()
    | Unlock k ->
        ZMQClient.send_cmd clt (Client.CltMsg.UnlockKey (Key.of_string k)) ;
        handle_msgs_out ()
    | Del k ->
        ZMQClient.send_cmd clt (Client.CltMsg.DelKey (Key.of_string k)) ;
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

external set_my_errors : string -> unit = "set_my_errors"

let on_progress url clt stage status =
  if stage = ZMQClient.Stage.Auth && status = ZMQClient.Status.InitOk then (
    let my_errors = option_get "my_errors" clt.Client.my_errors |>
                    Key.to_string in
    !logger.info "Setting my errors key to %S" my_errors ;
    set_my_errors my_errors
  ) ;
  (match stage with
  | ZMQClient.Stage.Conn -> signal_conn url
  | ZMQClient.Stage.Auth -> signal_auth
  | ZMQClient.Stage.Sync -> signal_sync) status

(* Will be called by the C++ on a dedicated thread, never returns: *)
let start_sync url creds () =
  Gc.compact () ;
  ZMQClient.start
    url creds ~topics:["*"] ~on_progress:(on_progress url)
    ~on_new ~on_set ~on_del ~on_lock ~on_unlock
    ~recvtimeo:0.1 sync_loop

external set_my_uid : string -> unit = "set_my_uid"

let init debug quiet url creds =
  if debug && quiet then
    failwith "Options --debug and --quiet are incompatible." ;
  let log_level =
    if debug then Debug else if quiet then Quiet else Normal in
  init_logger log_level ;
  set_my_uid creds ;
  (* Register the functions that will be called from C++ *)
  ignore (Callback.register "start_sync" (start_sync url creds)) ;
  ignore (Callback.register "value_of_string" value_of_string) ;
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
  ignore (Thread.self ()) ;
  let version = RamenVersions.release_tag in
  let i = Term.info ~doc:"RamenAdmin" ~version "rmadmin" in
  match
    print_exn (fun () ->
      Term.eval ~catch:false
        Term.(const init
          $ debug
          $ quiet
          $ confserver_url
          $ const "admin", i))
  with `Error _ -> exit 1
     | `Version | `Help -> exit 0
     | `Ok () -> ()
