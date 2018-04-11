(*
 * Notifications:
 * To alleviate workers from the hassle to send HTTP notifications, those are
 * sent to Ramen via a ringbuffer. Advantages are many:
 * Workers do not need an HTTP client and are therefore smaller, faster to
 * link, and easier to port to another language. Also, other notification
 * mechanisms are easier to implement in a single location.
 *
 * TODO: quarantine workers when command fails?
 *)
open Batteries
open Lwt
open RamenLog
open RamenHelpers

let http_notify worker http =
  let open Cohttp in
  let open Cohttp_lwt_unix in
  let open RamenOperation in
  let headers =
    List.fold_left (fun h (n, v) ->
      Header.add h n v
    ) (Header.init_with "Connection" "close") http.headers in
  let url = Uri.of_string http.url in
  let thd =
    match http.method_ with
    | HttpCmdGet ->
      Client.get ~headers url
    | HttpCmdPost ->
      let headers = Header.add headers "Content-type"
                               RamenConsts.ContentTypes.urlencoded in
      let body = Uri.pct_encode http.body |> Cohttp_lwt.Body.of_string in
      Client.post ~headers ~body url
  in
  let%lwt resp, body = thd in
  let code = resp |> Response.status |> Code.code_of_status in
  if code <> 200 then (
    let%lwt body = Cohttp_lwt.Body.to_string body in
    !logger.error "Received code %d from %S (%S)" code http.url body ;
    return_unit
  ) else
    Cohttp_lwt.Body.drain_body body

let execute_cmd worker cmd =
  match%lwt run ~timeout:5. [| "/bin/sh"; "-c"; cmd |] with
  | exception e ->
      !logger.error "While executing command %S from %s: %s"
        cmd worker
        (Printexc.to_string e) ;
      return_unit
  | stdout, stderr ->
      if stdout <> "" then !logger.debug "cmd: %s" stdout ;
      if stderr <> "" then !logger.error "cmd: %s" stderr ;
      return_unit

let start conf rb =
  let while_ () = if !RamenProcesses.quit then return_false else return_true in
  RamenSerialization.read_notifs ~while_ rb (fun (worker, notif) ->
    !logger.info "Received execute instruction from %s: %s"
      worker notif ;
    match PPP.of_string_exc RamenOperation.notification_ppp_ocaml notif with
    | ExecuteCmd cmd -> execute_cmd worker cmd
    | HttpCmd http -> http_notify worker http)
