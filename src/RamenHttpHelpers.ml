open RamenLog
open Helpers

(* HTTP helpers: *)

open Cohttp
open Cohttp_lwt_unix
open Lwt
open Batteries

exception HttpError of (int * string)
let () =
  Printexc.register_printer (function
    | HttpError (code, text) -> Some (
      Printf.sprintf "HttpError (%d, %S)" code text)
    | _ -> None)

(* List of condvars to signal to terminate all the HTTP servers: *)
let http_server_done = ref []

let http_service port cert_opt key_opt router =
  let dec = Uri.pct_decode in
  let callback _conn req body =
    let path = Uri.path (Request.uri req) in
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
           let status = Code.status_of_code code in
           let headers =
             Header.init_with "Access-Control-Allow-Origin" "*" in
           let headers =
             Header.add headers "Content-Type" Consts.json_content_type in
           let body =
             Printf.sprintf "{\"success\": false, \"error\": %S}\n" msg in
           Server.respond_string ~headers ~status ~body ()
       | exn ->
           print_exception exn ;
           let body = Printexc.to_string exn ^ "\n" in
           let headers = Header.init_with "Access-Control-Allow-Origin" "*" in
           Server.respond_error ~headers ~body ()
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
  let t1 =
    !logger.info "Starting http server on port %d" port ;
    let stop = http_stop_thread () in
    Server.create ~on_exn ~stop ~mode:tcp_mode entry_point
  and t2 =
    match cert_opt, key_opt with
    | Some cert, Some key ->
      let port = port + 1 in
      let ssl_mode = `TLS (`Crt_file_path cert, `Key_file_path key, `No_password, `Port port) in
      !logger.info "Starting https server on port %d" port ;
      let stop = http_stop_thread () in
      Server.create ~on_exn ~stop ~mode:ssl_mode entry_point
    | None, None ->
      return (!logger.debug "Not starting https server")
    | _ ->
      return (!logger.info "Missing some of SSL configuration")
  in
  join [ t1 ; t2 ]

let ok_body = "{\"success\": true}"

let respond_ok ?(body=ok_body) ?(ct=Consts.json_content_type) () =
  let status = `Code 200 in
  let headers = Header.init_with "Content-Type" ct in
  let headers = Header.add headers "Access-Control-Allow-Origin" "*" in
  let body = body ^"\n" in
  Server.respond_string ~status ~headers ~body ()

let content_type_of_ext = function
  | "html" -> Consts.html_content_type
  | "js" -> Consts.js_content_type
  | "css" -> Consts.css_content_type
  | _ -> "I_dont_know/Good_luck"

let ext_of_file fname =
  let _, ext = String.rsplit fname ~by:"." in ext

let serve_raw_file ct fname =
  let headers = Header.init_with "Content-Type" ct in
  Server.respond_file ~headers ~fname ()

let serve_file path file replace =
  let fname = path ^"/"^ file in
  let%lwt body = lwt_read_whole_file fname in
  let%lwt body = replace body in
  let ct = content_type_of_ext (ext_of_file file) in
  respond_ok ~body ~ct ()

let not_implemented msg = fail (HttpError (501, msg))
let bad_request msg = fail (HttpError (400, msg))
let bad_request_exn msg = raise (HttpError (400, msg))

(* Case is significant for multipart boundaries *)
let get_content_type headers =
  Header.get headers "Content-Type" |? Consts.json_content_type

let get_accept headers =
  let h =
    Header.get headers "Accept" |? Consts.json_content_type |>
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
  if ct <> Consts.json_content_type then
    bad_request "Bad content type"
  else
    of_json_body what ppp body

(* Back-end version of JsHelpers.string_of_timestamp: *)
let string_of_timestamp t =
  let open Unix in
  let tm = localtime t in
  Printf.sprintf "%04d-%02d-%02d %02dh%02dm%02ds"
    (tm.tm_year + 1900) (tm.tm_mon + 1) tm.tm_mday
    tm.tm_hour tm.tm_min tm.tm_sec

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

(* Helpers for clients: *)

(* Retry an HTTP command reasonably *)
let exhort ?(err_ok=false) http_cmd =
  let on = function
    | Unix.Unix_error (Unix.ECONNREFUSED, "connect", "") -> return_true
    | _ -> return_false in
  let%lwt resp, body =
    Helpers.retry ~first_delay:1. ~on ~max_retry:3 http_cmd () in
  let code = resp |> Response.status |> Code.code_of_status in
  let%lwt body = Cohttp_lwt.Body.to_string body in
  if code <> 200 then (
    if err_ok then (
      !logger.debug "Response code: %d" code ;
      !logger.debug "Answer: %S" body
    ) else (
      !logger.error "Response code: %d" code ;
      !logger.error "Answer: %S" body
    ) ;
    fail_with ("Error HTTP "^ string_of_int code)
  ) else (
    !logger.debug "Response code: %d" code ;
    !logger.debug "Answer: %S" body ;
    return body
  )

(* Return the answered body *)
let http_do ?(cmd=Client.put) ?content_type ?body url =
  let headers = Header.init_with "Connection" "close" in
  let headers = match content_type with
    | Some ct -> Header.add headers "Content-Type" ct
    | None -> headers in
  !logger.debug "%S < %a" url (Option.print String.print) body ;
  let body = Option.map (fun s -> `String s) body in
  exhort (fun () -> cmd ~headers ?body (Uri.of_string (sure_is_http url)))

(* Return the answered body *)
let http_put_json url ppp msg =
  !logger.debug "HTTP PUT %s" url ;
  let body = PPP.to_string ppp msg in
  http_do ~content_type:Consts.json_content_type ~body url

let http_post_json url ppp msg =
  !logger.debug "HTTP POST %s" url ;
  let body = PPP.to_string ppp msg in
  http_do ~cmd:Client.post ~content_type:Consts.json_content_type ~body url

let http_get ?err_ok url =
  !logger.debug "HTTP GET %s" url ;
  exhort ?err_ok (fun () -> Client.get (Uri.of_string (sure_is_http url)))

let check_ok body =
  (* Yeah that's grand *)
  ignore body ;
  return_unit
