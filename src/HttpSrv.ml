(* Start an HTTP(S) daemon to allow setting up the configuration graph. *)
open Cohttp
open Cohttp_lwt_unix
open Batteries
open BatOption.Infix
open Lwt
open RamenLog
open RamenSharedTypes
open Helpers
open RamenHttpHelpers
open Lang
module C = RamenConf
module N = RamenConf.Func
module L = RamenConf.Program
module SL = RamenSharedTypes.Info.Program
module SN = RamenSharedTypes.Info.Func

let hostname =
  let cached = ref "" in
  fun () ->
    if !cached <> "" then return !cached else
    let%lwt c =
      try%lwt
        run ~timeout:2. [| "hostname" |]
      with _ -> return "unknown host" in
    cached := c ;
    return c

let replace_placeholders ramen_url url_prefix s =
  let%lwt hostname = hostname () in
  let rep sub by str = String.nreplace ~str ~sub ~by in
  return (
    rep "$RAMEN_URL$" ramen_url s |>
    rep "$RAMEN_PATH_PREFIX$" url_prefix |>
    rep "$HOSTNAME$" hostname |>
    rep "$VERSION$" RamenVersions.release_tag)

let serve_string ramen_url url_prefix _headers body =
  let%lwt body = replace_placeholders ramen_url url_prefix body in
  respond_ok ~body ~ct:Consts.html_content_type ()

(*
    Add/Remove programs

    Programs and funcs within a program are referred to via name that can be
    anything as long as they are unique.  So the clients decide on the name.
    The server ensure uniqueness by forbidding creation of a new programs by the
    same name as one that exists already.

*)

let find_func_or_fail programs program_name func_name =
  match C.find_func programs program_name func_name with
  | exception Not_found ->
    bad_request ("Function "^ program_name ^"/"^ func_name ^" does not exist")
  | program, _func as both ->
    RamenProcesses.use_program (Unix.gettimeofday ()) program ;
    return both

let func_of_name programs program_name func_name =
  if func_name = "" then bad_request "Empty string is not a valid func name"
  else find_func_or_fail programs program_name func_name

let del_program conf _headers program_name =
  try%lwt
    let%lwt _ =
      RamenOps.del_program_by_name ~ok_if_running:false conf program_name in
    respond_ok ()
  with Not_found ->
    let e = "Program "^ program_name ^" does not exist" in
    bad_request e
  | e ->
    bad_request (Printexc.to_string e)

(*
    Whole graph operations: run/stop
*)

let run conf headers program_opt =
  try%lwt
    let%lwt () =
      C.with_wlock conf (fun programs ->
        let%lwt to_run = RamenProcesses.graph_programs programs program_opt in
        let to_run = L.order to_run in
        Lwt_list.iter_s (RamenProcesses.run conf programs) to_run) in
    switch_accepted headers [
      Consts.json_content_type, (fun () -> respond_ok ()) ]
  with SyntaxError _
     | Compiler.SyntaxErrorInFunc _
     | C.InvalidCommand _ as e ->
       bad_request (Printexc.to_string e)
     | x -> fail x

let stop conf headers program_opt =
  try%lwt
    let%lwt () =
      C.with_wlock conf (fun programs ->
        RamenProcesses.stop_programs conf programs program_opt) in
    switch_accepted headers [
      Consts.json_content_type, (fun () -> respond_ok ()) ]
  with C.InvalidCommand e -> bad_request e
     | x -> fail x

let ext_run conf headers body =
  let%lwt msg =
    of_json headers "Starting program" start_program_req_ppp_json body in
  let%lwt () = RamenOps.ext_start conf msg.program_name msg.bin_path
                                  msg.parameters msg.timeout in
  switch_accepted headers [
    Consts.json_content_type, (fun () -> respond_ok ()) ]

let shutdown _conf _headers =
  (* TODO: also log client info *)
  !logger.info "Asked to shut down" ;
  (* Hopefully cohttp will serve this answer before stopping. *)
  RamenProcesses.quit := true ;
  respond_ok ()

(*
    Data Upload
    Data is then written to tmp_input_dirname/uploads/suffix/$random
*)

let save_in_tmp_file dir body =
  mkdir_all dir ;
  let fname = random_string 10 in
  let path = dir ^"/"^ random_string 10 in
  Lwt_io.(with_file Output path (fun oc ->
    let%lwt () = write oc body in
    return (path, fname)))

let upload conf headers program func body =
  let%lwt _program, func =
    C.with_rlock conf (fun programs ->
      find_func_or_fail programs program func) in
  (* Look for the func handling this suffix: *)
  match func.N.operation with
  | ReadCSVFile { where = ReceiveFile ; _ } ->
    let dir = C.upload_dir_of_func conf.C.persist_dir func.N.program_name func.N.name func.N.in_type in
    let ct = get_content_type headers |> String.lowercase in
    let content =
      if ct = Consts.urlencoded_content_type then Uri.pct_decode body
      else if ct = Consts.text_content_type then body
      else (
        !logger.info "Don't know how to convert content type '%s' into \
                      CSV, trying no conversion." ct ;
        body) in
    let%lwt path, fname = save_in_tmp_file dir content in
    Lwt_unix.rename path (dir ^"/_"^ fname) >>=
    respond_ok
  | _ ->
    bad_request ("Function "^ N.fq_name func ^" does not accept uploads")

(* Start the HTTP server: *)
let router conf www_dir url_prefix =
  let lyr_opt = function
    | [] -> None
    | lst -> Some (String.concat "/" lst) in
  let lyr lst = match lyr_opt lst with
    | None -> bad_request_exn "Program name missing from URL"
    | Some lst -> lst in
  let lyr_func_of path =
    let rec loop ls = function
      | [] -> bad_request_exn "operation name missing from URL"
      | [x] ->
        if ls = [] then bad_request_exn "operation name missing from URL"
        else lyr (List.rev ls), x
      | l::rest ->
        loop (l :: ls) rest in
    loop [] path in
  (* The function called for each HTTP request: *)
  fun meth path params headers body ->
    match meth, path with
    (* Ramen API *)
    | `GET, (("run" | "start") :: programs) ->
      run conf headers (lyr_opt programs)
    | `POST, ["run" | "start"] -> (* Start from offline binary *)
      ext_run conf headers body
    | `GET, ("stop" :: programs) ->
      stop conf headers (lyr_opt programs)
    | `GET, ["shutdown"] ->
      shutdown conf headers
    (* Uploads of data files *)
    | (`POST|`PUT), ("upload" :: path) ->
      let program, func = lyr_func_of path in
      upload conf headers program func body
    (* Errors *)
    | `PUT, p | `GET, p | `DELETE, p ->
      let path = String.join "/" p in
      fail (HttpError (404, "Unknown resource "^ path))
    | _ ->
      fail (HttpError (405, "Method not implemented"))
