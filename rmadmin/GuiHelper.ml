open Batteries
open RamenConsts
open RamenLog
open RamenHelpers

(*
 * SyncConf client
 *)

module Client = RamenSyncClient.Make (RamenSync.Value) (RamenSync.Selector)
module Key = Client.Key

(* The idea is to do the networking work of connecting, sending/receiving
 * messages and maintaining the config in OCaml.
 * Then, at every message, in addition to maintaining the conf tree we also,
 * depending on the message, call a C wrapper to the Qt signal. *)

let send_cmd () =
  let next_id = ref 0 in
  fun zock cmd ->
    let s = Client.CltMsg.to_string (!next_id, cmd) in
    incr next_id ;
    Zmq.Socket.send_all zock [ "" ; s ]

let recv_cmd zock =
  match Zmq.Socket.recv_all zock with
  | [ "" ; s ] ->
      !logger.info "srv message (raw): %S" s ;
      Client.SrvMsg.of_string s
  | m ->
      Printf.sprintf2 "Received unexpected message %a"
        (List.print String.print) m |>
      failwith

let unexpected_reply cmd =
  Printf.sprintf "Unexpected reply %s"
    (Client.SrvMsg.to_string cmd) |>
  failwith

type sync_status =
  | InitStart | InitOk | InitFail of string (* For the init stage *)
  | Ok of string | Fail of string

external signal_conn : string -> sync_status -> unit = "signal_conn"
let init_connect url zock =
  let connect_to = "tcp://"^ url in
  signal_conn url InitStart ;
  try
    !logger.info "Connecting to %s..." connect_to ;
    Zmq.Socket.connect zock connect_to ;
    signal_conn url InitOk
  with e ->
    signal_conn url (InitFail (Printexc.to_string e))

external signal_auth : sync_status -> unit = "signal_auth"

let init_auth zock =
  signal_auth InitStart ;
  try
    !logger.info "Sending auth..." ;
    send_cmd () zock (Client.CltMsg.Auth "tintin") ;
    match recv_cmd zock with
    | Auth "" ->
        signal_auth InitOk
    | Auth err ->
        failwith err
    | rep ->
        unexpected_reply rep
  with e ->
    signal_auth (InitFail (Printexc.to_string e))

external signal_sync : sync_status -> unit = "signal_sync"

let init_sync zock glob =
  signal_sync InitStart ;
  try
    !logger.info "Sending StartSync %s..." glob ;
    let glob = Globs.compile glob in
    send_cmd () zock (Client.CltMsg.StartSync glob) ;
    signal_sync InitOk
  with e ->
    signal_sync (InitFail (Printexc.to_string e))

let sync_loop clt zock =
  let msg_count = ref 0 in
  forever (fun () ->
    try
      !logger.debug "receiving message..." ;
      recv_cmd zock |> Client.process_msg clt ;
      incr msg_count ;
      !logger.debug "received %d messages" !msg_count ;
      if !msg_count mod 10 = 0 then
        let status_msg =
          Printf.sprintf "%d messages, %d keys"
            !msg_count
            (Client.H.length clt.h) in
        signal_sync (Ok status_msg)
    with e ->
      signal_sync (Fail (Printexc.to_string e))
  ) ()

let on_new clt k v =
  !logger.info "New key %a" Key.print k ;
  ignore clt ; ignore k ; ignore v

let on_set clt k v =
  !logger.info "Change key %a" Key.print k ;
  ignore clt ; ignore k ; ignore v

let on_del clt k =
  !logger.info "Del key %a" Key.print k ;
  ignore clt ; ignore k

let on_lock clt k =
  !logger.info "Lock key %a" Key.print k ;
  ignore clt ; ignore k

let on_unlock clt k =
  !logger.info "Unlock key %a" Key.print k ;
  ignore clt ; ignore k

(* Will be called by the C++ on a dedicated thread, never returns: *)
let start_sync url () =
  let ctx = Zmq.Context.create () in
  finally
    (fun () -> Zmq.Context.terminate ctx)
    (fun () ->
      let zock = Zmq.Socket.(create ctx dealer) in
      finally
        (fun () -> Zmq.Socket.close zock)
        (fun () ->
          log_exceptions ~what:"init_connect"
            (fun () -> init_connect url zock) ;
          log_exceptions ~what:"init_auth"
            (fun () -> init_auth zock) ;
          log_exceptions ~what:"init_sync"
            (fun () -> init_sync zock "*") ;
          let clt =
            Client.make ~on_new ~on_set ~on_del ~on_lock ~on_unlock in
          log_exceptions ~what:"sync_loop"
            (fun () -> sync_loop clt zock)
        ) ()
    ) ()

let init debug quiet url =
  if debug && quiet then
    failwith "Options --debug and --quiet are incompatible." ;
  let log_level =
    if debug then Debug else if quiet then Quiet else Normal in
  init_logger log_level ;
  (* Register the functions that will be called from C++ *)
  let _cb = Callback.register "start_sync" (start_sync url) in
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
        Term.(const init $ debug $ quiet $ confserver_url, i))
  with `Error _ -> false
     | `Version | `Help | `Ok () -> true
