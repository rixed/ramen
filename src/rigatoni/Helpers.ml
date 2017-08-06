open Batteries
open Lwt
open Log

(* Small helper to return the ith entry of an array, capped to the last one.
 * Useful when we reach the last defined attempt while escalating an alert. *)
let get_cap a i =
  let len = Array.length a in
  assert (len > 0) ;
  a.(min i (len - 1))

let round_to_int f =
  int_of_float (Float.round f)

let retry ~on ?(first_delay=1.0) ?(min_delay=0.000001) ?(max_delay=10.0) ?(delay_adjust_ok=0.2) ?(delay_adjust_nok=1.1) f =
  let next_delay = ref first_delay in
  let rec loop x =
    (match%lwt f x with
    | exception e ->
      if on e then (
        let delay = !next_delay in
        let delay = min delay max_delay in
        let delay = max delay min_delay in
        next_delay := !next_delay *. delay_adjust_nok ;
        if delay > 1. then
          !logger.debug "Retryable error: %s, pausing %gs"
            (Printexc.to_string e) delay ;
        let%lwt () = Lwt_unix.sleep delay in
        loop x
      ) else (
        !logger.error "Non-retryable error: %s"
          (Printexc.to_string e) ;
        fail e
      )
    | r ->
      next_delay := !next_delay *. delay_adjust_ok ;
      return r)
  in
  loop

let shell_quote s =
  "'"^ String.nreplace s "'" "'\\''" ^"'"

let print_exception e =
  !logger.error "Exception: %s at\n%s"
    (Printexc.to_string e)
    (Printexc.get_backtrace ())

exception HttpError of (int * string)

let http_service port cert_opt key_opt router =
  let open Cohttp in
  let open Cohttp_lwt_unix in
  let callback _conn req body =
    let uri = Request.uri req in
    let path =
      String.nsplit (Uri.path uri) "/" |>
      List.filter (fun s -> String.length s > 0) |>
      List.map Uri.pct_decode in
    let params = Hashtbl.create 7 in
    (match String.split ~by:"?" req.Request.resource with
    | exception Not_found -> ()
    | _, param_str ->
      String.nsplit ~by:"&" param_str |>
      List.iter (fun p ->
        match String.split ~by:"=" p with
        | exception Not_found -> ()
        | pn, pv -> Hashtbl.add params pn pv)) ;
    let headers = Request.headers req in
    let%lwt body = Cohttp_lwt_body.to_string body
    in
    catch
      (fun () ->
        try router (Request.meth req) path params headers body
        with exn ->
          print_exception exn ;
          fail exn)
      (function
        | HttpError (code, body) ->
          let body = body ^ "\n" in
          let status = Code.status_of_code code in
          let headers = Header.init_with "Access-Control-Allow-Origin" "*" in
          Server.respond_error ~headers ~status ~body ()
        | exn ->
          let body = Printexc.to_string exn ^ "\n" in
          let headers = Header.init_with "Access-Control-Allow-Origin" "*" in
          Server.respond_error ~headers ~body ())
  in
  let entry_point = Server.make ~callback () in
  let tcp_mode = `TCP (`Port port) in
  let on_exn = print_exception in
  let t1 =
    !logger.info "Starting http server on port %d" port ;
    Server.create ~on_exn ~mode:tcp_mode entry_point
  and t2 =
    match cert_opt, key_opt with
    | Some cert, Some key ->
      let port = port + 1 in
      let ssl_mode = `TLS (`Crt_file_path cert, `Key_file_path key, `No_password, `Port port) in
      !logger.info "Starting https server on port %d" port ;
      Server.create ~on_exn ~mode:ssl_mode entry_point
    | None, None ->
      return (!logger.info "Not starting https server")
    | _ ->
      return (!logger.info "Missing some of SSL configuration")
  in
  join [ t1 ; t2 ]

let looks_like_true s =
  s = "1" || (
    String.length s > 1 &&
    let lc = Char.lowercase s.[0] in lc = 'y' || lc = 't')
