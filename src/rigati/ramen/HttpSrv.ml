(* HTTP daemon running along ramen *)
open Batteries
open Cohttp
open Cohttp_lwt_unix

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
let start port =
  Server.create ~mode:(`TCP (`Port port)) (Server.make ~callback ())
