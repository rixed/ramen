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
      if String.starts_with path "/" then String.lchop path else path in
    (* Make "path/" equivalent to "path" for convenience. Beware that in
     * general "foo//bar" is not equivalent to "foo/bar" so not seemingly
     * spurious slashes can be omitted! *)
    let path =
      if String.ends_with path "/" then String.rchop path else path in
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
    catch
      (fun () ->
        try router (Request.meth req) path params headers body
        with exn -> fail exn)
      (function
        HttpError (code, msg) as exn ->
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
        Server.respond_error ~headers ~body ())
  in
  let entry_point = Server.make ~callback () in
  let tcp_mode = `TCP (`Port port) in
  let on_exn = print_exception in
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

let serve_file path file replace =
  let fname = path ^"/"^ file in
  let%lwt body = read_whole_file fname in
  let%lwt body = replace body in
  let ct = content_type_of_ext (ext_of_file file) in
  respond_ok ~body ~ct ()

let not_implemented msg = fail (HttpError (501, msg))
let bad_request msg = fail (HttpError (400, msg))
let bad_request_exn msg = raise (HttpError (400, msg))

let get_content_type headers =
  Header.get headers "Content-Type" |? Consts.json_content_type |>
  String.lowercase

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

(* Helper to deserialize an incoming json *)
let of_json headers what ppp body =
  if get_content_type headers <> Consts.json_content_type then
    bad_request "Bad content type"
  else (
    try PPP.of_string_exc ppp body |> return
    with e ->
      !logger.info "%s: Cannot parse received body: %S, Exception %s"
        what body (Printexc.to_string e) ;
      bad_request "Can not parse body")

(* Back-end version of JsHelpers.string_of_timestamp: *)
let string_of_timestamp t =
  let open Unix in
  let tm = localtime t in
  Printf.sprintf "%04d-%02d-%02d %02dh%02dm%02ds"
    (tm.tm_year + 1900) (tm.tm_mon + 1) tm.tm_mday
    tm.tm_hour tm.tm_min tm.tm_sec
