(* Start an HTTP(S) daemon to allow setting up the configuration graph. *)
open RamenLogger
open Batteries
open Cohttp
open Cohttp_lwt_unix
open Lwt

(* The function called for each HTTP request: *)
let callback _conn req body =
  let uri = Request.uri req |> Uri.to_string in
  let meth = Request.meth req |> Code.string_of_method in
  let headers = Request.headers req |> Header.to_string in
  let%lwt body_str = Cohttp_lwt_body.to_string body in
  let response_body =
    Printf.sprintf "Uri: %s\n\nMethod: %s\n\nHeaders:\n%s\n\nBody: %s\n"
      uri meth headers body_str in
  Server.respond_string ~status:`OK ~body:response_body ()

(* This will be called as a separate Lwt thread: *)
let start logger port cert_opt key_opt =
  let entry_point = Server.make ~callback () in
  let tcp_mode = `TCP (`Port port) in
  let t1 =
    let%lwt () = return (logger.info "Starting http server on port %d" port) in
    Server.create ~mode:tcp_mode entry_point in
  let t2 =
    match cert_opt, key_opt with
    | Some cert, Some key ->
      let port = port + 1 in
      let ssl_mode = `TLS (`Crt_file_path cert, `Key_file_path key, `No_password, `Port port) in
      let%lwt () = return (logger.info "Starting https server on port %d" port) in
      Server.create ~mode:ssl_mode entry_point
    | None, None ->
      return (logger.info "Not starting https server")
    | _ ->
      return (logger.info "Missing some of SSL configuration") in
  join [ t1 ; t2 ]
