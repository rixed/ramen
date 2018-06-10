(* HTTP helpers:
 * This module groups several functions that make life easier for HTTP
 * servers. *)
open RamenLog
open RamenHelpers
open Cohttp
open Cohttp_lwt_unix
open Lwt
open Batteries

(*
 * Ramen can serve an API over HTTP
 * (for instance to impersonate Graphite).
 *)

exception HttpError of (int * string)
let () =
  Printexc.register_printer (function
    | HttpError (code, text) -> Some (
      Printf.sprintf "HttpError (%d, %S)" code text)
    | _ -> None)

let not_implemented msg = fail (HttpError (501, msg))
let bad_request msg = fail (HttpError (400, msg))
let not_found msg = fail (HttpError (404, msg))

exception BadPrefix

let list_of_prefix pfx =
  String.split_on_char '/' pfx |>
  List.filter ((<>) "")

let rec chop_prefix pfx path =
  match pfx, path with
  | [], path' -> path'
  | p1::pfx', p2::path' when p1 = p2 -> chop_prefix pfx' path'
  | _ -> raise BadPrefix

(* Case is significant for multipart boundaries *)
let get_content_type headers =
  Header.get headers "Content-Type" |? RamenConsts.ContentTypes.json

let get_accept headers =
  let h =
    Header.get headers "Accept" |? RamenConsts.ContentTypes.json |>
    String.lowercase in
  let h =
    try String.split ~by:";" h |> fst
    with Not_found -> h in
  String.split_on_char ',' h

let is_accepting_anything = List.mem "*/*"

let is_accepting content_type accept =
  is_accepting_anything accept || List.mem content_type accept

(* When the client cannot accept the response *)
let cant_accept accept =
  let msg =
    Printf.sprintf "{\"error\": \"Can't produce any of %s\"}\n"
      (IO.to_string (List.print ~first:"" ~last:"" ~sep:", " String.print)
                    accept) in
  fail (HttpError (406, msg))

let check_accept headers content_type =
  let accept = get_accept headers in
  if not (is_accepting content_type accept) then
    cant_accept accept
  else return_unit

let switch_accepted headers al =
  let accept = get_accept headers in
  match List.find (fun (ct, _) -> is_accepting ct accept) al with
  | exception Not_found -> cant_accept accept
  | _, k -> k ()

(* Helpers to deserialize an incoming json *)
let of_json_body what ppp body =
  try PPP.of_string_exc ppp body |> return
  with e ->
    let err = Printexc.to_string e in
    !logger.info "%s: Cannot parse received body: %S, Exception %s"
      what body err ;
    bad_request ("Can not parse body: "^ err)

let of_json headers what ppp body =
  let ct = get_content_type headers |> String.lowercase in
  if ct <> RamenConsts.ContentTypes.json then
    bad_request "Bad content type"
  else
    of_json_body what ppp body

(* List of condvars to signal to terminate all the HTTP servers: *)
(* FIXME: a single condvar that we broadcast to *)
let http_server_done = ref []

let stop_http_servers () =
  !logger.info "Stopping HTTP server..." ;
  List.iter (fun condvar ->
    Lwt_condition.signal condvar ()
  ) !http_server_done

open Binocle

let stats_count =
  IntCounter.make RamenConsts.MetricNames.requests_count
    "Number of HTTP requests, per response status"

let http_service port url_prefix router =
  set_signals Sys.[sigterm; sigint] (Signal_handle (fun s ->
    !logger.info "Received signal %s" (name_of_signal s) ;
    stop_http_servers ())) ;
  let dec s =
    (* As the name suggest, pct_decode only decode percent encoded values.
     * + signs are left untouched. As we are decoding the query part of the
     * URL we need to decode them: *)
    let s = Uri.pct_decode s in
    String.map (fun c -> if c = '+' then ' ' else c) s in
  let callback _conn req body =
    let path = Uri.path (Request.uri req) in
    (* Check path starts with url_prefix and chop off that prefix: *)
    let path =
      if String.starts_with path url_prefix then
        String.lchop ~n:(String.length url_prefix) path
      else (
        !logger.error "URL %S does not start with expected prefix (%S)"
          path url_prefix ;
        (* Keep going and hope for the best *)
        path
      ) in
    !logger.debug "Requested %S" req.Request.resource ;
    (* Make "/path" equivalent to "path" *)
    let path =
      let rec loop s =
        if String.starts_with s "/" then loop (String.lchop s) else s in
      loop path in
    (* Make "path/" equivalent to "path" for convenience. Beware that in
     * general "foo//bar" is not equivalent to "foo/bar" so not seemingly
     * spurious slashes can be omitted! *)
    let path =
      let rec loop s =
        if String.ends_with s "/" then loop (String.rchop s) else s in
      loop path in
    let path =
      String.nsplit path "/" |>
      List.map dec in
    let params = Hashtbl.create 7 in
    (match String.split ~by:"?" req.Request.resource with
    | exception Not_found -> ()
    | _, param_str ->
      String.nsplit ~by:"&" param_str |>
      List.iter (fun p ->
        match String.split ~by:"=" p with
        | exception Not_found -> ()
        | pn, pv -> Hashtbl.add params (dec pn) (dec pv))) ;
    let headers = Request.headers req in
    let%lwt body = Cohttp_lwt.Body.to_string body
    in
    try%lwt
      try router (Request.meth req) path params headers body
      with exn -> fail exn
    with HttpError (code, msg) as exn ->
           print_exception exn ;
           IntCounter.add ~labels:["status", string_of_int code] stats_count 1 ;
           let status = Code.status_of_code code in
           let headers =
             Header.init_with "Access-Control-Allow-Origin" "*" in
           let headers =
             Header.add headers "Content-Type" RamenConsts.ContentTypes.json in
           let body =
             Printf.sprintf "{\"success\": false, \"error\": %S}\n" msg in
           Server.respond_string ~headers ~status ~body ()
       | exn ->
           print_exception exn ;
           let code = 500 in
           IntCounter.add ~labels:["status", string_of_int code] stats_count 1 ;
           let status = Code.status_of_code code in
           let body = Printexc.to_string exn ^ "\n" in
           let headers = Header.init_with "Access-Control-Allow-Origin" "*" in
           Server.respond_error ~headers ~status ~body ()
  in
  let entry_point = Server.make ~callback () in
  let tcp_mode = `TCP (`Port port) in
  let on_exn = function
    | Unix.Unix_error (Unix.EPIPE, "write", _) ->
        !logger.warning "EPIPE while write, client probably closed its \
                         connection" ;
    | exn -> print_exception exn in
  let http_stop_thread () =
    let cond = Lwt_condition.create () in
    let stop = Lwt_condition.wait cond in
    http_server_done := cond :: !http_server_done ;
    stop in
  !logger.info "Starting http server on port %d" port ;
  let stop = http_stop_thread () in
  Server.create ~on_exn ~stop ~mode:tcp_mode entry_point

let respond_ok ~body ?(ct=RamenConsts.ContentTypes.json) () =
  let status = `Code 200 in
  let headers = Header.init_with "Content-Type" ct in
  let headers = Header.add headers "Access-Control-Allow-Origin" "*" in
  let body = body ^"\n" in
  Server.respond_string ~status ~headers ~body ()
