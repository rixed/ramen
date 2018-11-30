(* HTTP helpers:
 * This module groups several functions that make life easier for HTTP
 * servers. *)
open Batteries
open RamenLog
open RamenHelpers
open RamenConsts
module C = RamenConf

(*
 * Ramen can serve various API over HTTP
 * (for instance to impersonate Graphite).
 *)

exception HttpError of (int * string)
let () =
  Printexc.register_printer (function
    | HttpError (code, text) -> Some (
      Printf.sprintf "HttpError (%d, %S)" code text)
    | _ -> None)

let not_implemented msg = raise (HttpError (501, msg))
let bad_request msg = raise (HttpError (400, msg))
let not_found msg = raise (HttpError (404, msg))

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
  try List.assoc "Content-Type" headers
  with Not_found -> ContentTypes.json

let get_accept headers =
  let h =
    (try List.assoc "Accept" headers
     with Not_found -> ContentTypes.json) |>
    String.lowercase in
  let h =
    try String.split ~by:";" h |> fst
    with Not_found -> h in
  String.split_on_char ',' h

let is_accepting_everything = List.mem "*/*"

let is_accepting content_type accept =
  is_accepting_everything accept || List.mem content_type accept

(* When the client cannot accept the response *)
let cant_accept accept =
  let msg =
    Printf.sprintf "{\"error\": \"Can't produce any of %s\"}\n"
      (IO.to_string (List.print ~first:"" ~last:"" ~sep:", " String.print)
                    accept) in
  raise (HttpError (406, msg))

let check_accept headers content_type =
  let accept = get_accept headers in
  if not (is_accepting content_type accept) then
    cant_accept accept

open Binocle

let stats_count =
  RamenBinocle.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir
      Metric.Names.requests_count
      "Number of HTTP requests, per response status")

let stats_resp_time =
  RamenBinocle.ensure_inited (fun save_dir ->
    Histogram.make ~save_dir
      Metric.Names.http_resp_time
      "HTTP response time per URL" Histogram.powers_of_two)

(* Perf measurements (FIXME) *)
let times = Hashtbl.create 7
let with_timing n f =
  let start = Unix.gettimeofday () in
  let ret = f () in
  let stop = Unix.gettimeofday () in
  let dt = stop -. start in
  Hashtbl.modify_opt n (function
      | None -> Some (dt, 1)
      | Some (dt0, n) -> Some (dt0 +. dt, n+1)
    ) times ;
  ret

(* The HTTP protocol is usually a protocol one don't want to touch with a
 * 10 feet pole, so the preferred way to make ramen works over HTTP is to
 * use the CGI mode (TODO: `ramen cgi`). But for ease of use, ramen also
 * provide its own basic and slow HTTP server.
 * It runs in its own thread already thanks to [forking_server].
 * Parsing an average HTTP message takes about 5ms, which is still likely
 * considerably faster than what's going to happen next... *)
module ParserConfig = ParsersConfig.BlockList (ParsersConfig.FileReader)
module ParserConfigWithOffset = ParsersPositions.Offset (ParserConfig)
let make_stream fd = ParserConfig.make_stream fd, 0
module HttpParser = CodecHttp.MakeParser (ParserConfigWithOffset)

let respond fd msg =
  let s = CodecHttp.Msg.encode msg in
  Legacy.Unix.write_substring fd s 0 (String.length s) |> ignore

let kaputt str =
  CodecHttp.(Msg.{
    start_line = StartLine.Response StatusLine.{
      version = 1, 1 ;
      code = 500 ;
      msg = "Kaputt" } ;
    headers = [
      "Content-Length", String.length str |> string_of_int ;
      "Content-Type", "text/plain" ] ;
    body = str })

let http_msg ?(code=200) ?(content_type=ContentTypes.json)
             ?(headers=[]) body =
  let headers =
    ("Access-Control-Allow-Origin", "*") ::
    ("Content-Length", String.length body |> string_of_int) ::
    ("Content-Type", content_type) :: headers in
  CodecHttp.(Msg.{
    start_line = StartLine.Response StatusLine.{
      version = 1, 1 ;
      code ;
      msg = CodecHttp.text_of_code code } ;
    headers ; body })

let code_of_response = function
  | CodecHttp.(Msg.{
      start_line = StartLine.Response StatusLine.{ code ; _ } }) -> code
  | _ -> invalid_arg "code_of_response"

let on_all_http_msg conf url_prefix fault_injection_rate router fd msg =
  match msg.CodecHttp.Msg.start_line with
  | CodecHttp.StartLine.Response _ -> ()
  | CodecHttp.StartLine.Request r ->
      let url =
        r.CodecHttp.RequestLine.url |>
        CodecUrl.of_string ~force_absolute:true in
      !logger.debug "answering to %s..." (CodecUrl.to_string url) ;
      let params = CodecUrl.parse_query_of_url url in
      let path = url.CodecUrl.path in
      let path =
        if String.starts_with path url_prefix then
          String.lchop ~n:(String.length url_prefix) path
        else (
          !logger.error "URL %S does not start with expected prefix (%S)"
            path url_prefix ;
          (* Keep going and hope for the best *)
          path
        ) in
      !logger.debug "Requested %S" (CodecUrl.to_string url) ;
      (* Make "/path" equivalent to "path" *)
      let path =
        let rec loop s =
          if String.starts_with s "/" then loop (String.lchop s) else s in
        loop path in
      (* Make "path/" equivalent to "path" for convenience. Beware that in
       * general "foo//bar" is not equivalent to "foo/bar" so not every
       * seemingly spurious slashes can be omitted! *)
      let path =
        let rec loop s =
          if String.ends_with s "/" then loop (String.rchop s) else s in
        loop path in
      let path_lst =
        String.nsplit path "/" |>
        List.map CodecUrl.decode in
      let headers = msg.CodecHttp.Msg.headers in
      let body = msg.CodecHttp.Msg.body in
      let start_time = Unix.gettimeofday () in
      let method_ = r.CodecHttp.RequestLine.cmd in
      let labels = [ "method", CodecHttp.Command.to_string method_ ;
                     "path", path] in
      !logger.info "%s path %a"
        (CodecHttp.Command.to_string method_)
        (List.print String.print) path_lst ;
      (* Fake fault injection: *)
      let do_inject_fault =
        if List.mem_assoc "X-Keep-The-Ouistiti-At-Bay" headers then (
          !logger.debug "Ouistiti kept at bay" ;
          false
        ) else
          List.mem_assoc "X-Send-Me-Ouistiti" headers ||
          Random.float 1. <= fault_injection_rate in
      let resp =
        if do_inject_fault then (
          kaputt "ouistiti sapristi"
        ) else (
          try router method_ path_lst params headers body with
          | HttpError (code, msg) as exn ->
             print_exception exn ;
             let labels = ("status", string_of_int code) :: labels in
             IntCounter.inc ~labels (stats_count conf.C.persist_dir) ;
             http_msg ~code msg
         | exn ->
             print_exception exn ;
             let code = 500 in
             let labels = ("status", string_of_int code) :: labels in
             IntCounter.inc ~labels (stats_count conf.C.persist_dir) ;
             let body = Printexc.to_string exn ^ "\n" in
             let content_type = ContentTypes.text in
             http_msg ~code ~content_type body
        ) in
      let resp_time = Unix.gettimeofday () -. start_time in
      respond fd resp ;
      let labels =
        ("status", string_of_int (code_of_response resp)) :: labels in
      !logger.debug "Response time to %a: %f"
        (List.print (fun oc (n,v) -> Printf.fprintf oc "%s=%s" n v))
          labels resp_time ;
      Histogram.add (stats_resp_time conf.C.persist_dir) ~labels resp_time

let on_all_err err =
  !logger.error "Error: %a"
    (HttpParser.P.print_bad_result (Option.print CodecHttp.Msg.print)) err

let http_service conf port url_prefix router fault_injection_rate =
  (* This will run in another process: *)
  let srv fd =
    !logger.debug "New connection! I'm so excited!!" ;
    let on_all_http_msg =
      on_all_http_msg conf url_prefix fault_injection_rate router fd in
    let rec loop stream =
      let parser_res =
        let open HttpParser in
        let p = P.((p >>: fun m -> Some m) ||| (eof >>: fun () -> None)) in
        (p [] None Parsers.no_error_correction stream |> P.to_result) in
      !logger.debug "Received: %a"
        (HttpParser.P.print_result (Option.print CodecHttp.Msg.print))
          parser_res ;
      match parser_res with
      | Ok (Some msg, stream') ->
          with_timing "answering queries" (fun () -> on_all_http_msg msg) ;
          loop stream'
      | Ok (None, _) ->
          !logger.info "Client disconnected"
      | Bad err -> on_all_err err in
    try loop (make_stream fd) with
    | Unix.Unix_error (Unix.EPIPE, "write", _) ->
        !logger.warning "EPIPE while write, client probably closed its \
                         connection" ;
    | exn ->
        print_exception exn ;
        let str = Printexc.to_string exn ^"\n"^
                  Printexc.get_backtrace () in
        kaputt str |>
        respond fd
  in
  !logger.info "Starting HTTP server on port %d" port ;
  let inet = Unix.inet_addr_any in (* or: inet_addr_of_string "127.0.0.1" *)
  let addr = Unix.(ADDR_INET (inet, port)) in
  let while_ () = !RamenProcesses.quit = None in
  forking_server ~while_ addr srv
