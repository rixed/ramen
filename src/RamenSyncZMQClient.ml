(* A specific Client using ZMQ, for the values/keys defined in RamenSync. *)
open Batteries
open RamenConsts
open RamenLog
open RamenHelpers

module Value = RamenSync.Value
module Client = RamenSyncClient.Make (Value) (RamenSync.Selector)
module Key = Client.Key

let next_id = ref 0
let send_cmd zock cmd =
    let s = Client.CltMsg.to_string (!next_id, cmd) in
    !logger.info "Sending command %s" s ;
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

(* Receive and process incoming commands until timeout.
 * Returns the number of messages that have been read. *)
let process_in clt zock =
  let rec loop msg_count =
    match recv_cmd zock with
    | exception Unix.(Unix_error (EAGAIN, _, _)) ->
        msg_count
    | msg ->
        Client.process_msg clt msg ;
        loop (msg_count + 1) in
  loop 0

let unexpected_reply cmd =
  Printf.sprintf "Unexpected reply %s"
    (Client.SrvMsg.to_string cmd) |>
  failwith

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
    | Ok | Fail of string
  let to_string = function
    | InitStart -> "Starting"
    | InitOk -> "Established"
    | InitFail s -> "Not established: "^ s
    | Ok -> "Done"
    | Fail s -> "Failed: "^ s
  let print oc s =
    String.print oc (to_string s)
end

let retry_zmq ?while_ f =
  let on = function
    | Unix.(Unix_error (EAGAIN, _, _)) -> true
    | _ -> false in
  retry ~on ~first_delay:0.3 ?while_ f

let init_connect ?while_ url zock on_progress =
  let connect_to = "tcp://"^ url in
  on_progress Stage.Conn Status.InitStart ;
  try
    !logger.info "Conning to %s..." connect_to ;
    Zmq.Socket.connect zock connect_to ;
    on_progress Stage.Conn Status.InitOk
  with e ->
    on_progress Stage.Conn Status.(InitFail (Printexc.to_string e))

let init_auth ?while_ creds zock on_progress =
  on_progress Stage.Auth Status.InitStart ;
  try
    !logger.info "Sending auth..." ;
    send_cmd zock (Client.CltMsg.Auth creds) ;
    match retry_zmq ?while_ recv_cmd zock with
    | Client.SrvMsg.Auth "" ->
        on_progress Stage.Auth Status.InitOk
    | Client.SrvMsg.Auth err ->
        failwith err
    | rep ->
        unexpected_reply rep
  with e ->
    on_progress Stage.Auth Status.(InitFail (Printexc.to_string e))

let init_sync ?while_ zock glob on_progress =
  on_progress Stage.Sync Status.InitStart ;
  try
    !logger.info "Sending StartSync %s..." glob ;
    let glob = Globs.compile glob in
    send_cmd zock (Client.CltMsg.StartSync glob) ;
    on_progress Stage.Sync Status.InitOk
  with e ->
    on_progress Stage.Sync Status.(InitFail (Printexc.to_string e))

(* Will be called by the C++ on a dedicated thread, never returns: *)
let start ?while_ url creds topic on_progress
          ?(recvtimeo= -1) ?(sndtimeo= -1) k =
  let ctx = Zmq.Context.create () in
  finally
    (fun () -> Zmq.Context.terminate ctx)
    (fun () ->
      let zock = Zmq.Socket.(create ctx dealer) in
      finally
        (fun () -> Zmq.Socket.close zock)
        (fun () ->
          (* Timeouts must be in place before connect: *)
          Zmq.Socket.set_receive_timeout zock recvtimeo ;
          Zmq.Socket.set_send_timeout zock sndtimeo ;
          log_exceptions ~what:"init_connect"
            (fun () -> init_connect ?while_ url zock on_progress) ;
          log_exceptions ~what:"init_auth"
            (fun () -> init_auth ?while_ creds zock on_progress) ;
          log_exceptions ~what:"init_sync"
            (fun () -> init_sync ?while_ zock topic on_progress) ;
          log_exceptions ~what:"sync_loop"
            (fun () -> k zock)
        ) ()
    ) ()
