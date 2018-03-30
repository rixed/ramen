open RamenLog
open RamenHelpers

(* HTTP helpers: *)

open Cohttp
open Cohttp_lwt_unix
open Lwt
open Batteries

let http_notify url =
  let headers = Header.init_with "Connection" "close" in
  let%lwt resp, body = Client.get ~headers (Uri.of_string url) in
  let code = resp |> Response.status |> Code.code_of_status in
  if code <> 200 then (
    let%lwt body = Cohttp_lwt.Body.to_string body in
    !logger.error "Received code %d from %S (%S)" code url body ;
    return_unit
  ) else
    Cohttp_lwt.Body.drain_body body
