(*
 * Notifications:
 * To alleviate workers from the hassle to deal with external systems,
 * notifications are sent to Ramen notifier process via a ringbuffer.
 * Advantages are many:
 *
 * - Workers do not need so many libraries and are therefore smaller and easier
 * to port to another language;
 *
 * - New notification mechanisms are easier to implement in a single location;
 *
 * - Workers are easier to distribute.
 *)
open Batteries
open Lwt
open RamenLog
open RamenHelpers

let http_send http worker =
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

let execute_cmd cmd worker =
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

let syslog =
  try Some (Syslog.openlog "ramen")
  with _ -> None

let log_str str worker =
  let level = `LOG_ALERT in
  match syslog with
  | None ->
    fail_with "No syslog on this host"
  | Some slog ->
    wrap (fun () -> Syslog.syslog slog level str)

let sqllite_insert file insert_q create_q worker =
  let open Sqlite3 in
  let open SqliteHelpers in
  let handle = db_open file in
  let db_fail err q =
    !logger.error "Cannot %S into sqlite DB %S: %s"
      q file (Rc.to_string err) ;
    exit 1 in
  let exec_or_fail q =
    match exec handle q with
    | Rc.OK -> ()
    | err -> db_fail err q in
  (match exec handle insert_q with
  | Rc.OK -> ()
  | Rc.ERROR when create_q <> "" ->
    !logger.info "Creating table in sqlite DB %S" file ;
    exec_or_fail create_q ;
    exec_or_fail insert_q
  | err ->
    db_fail err insert_q) ;
  close ~max_tries:30 handle

(* Generic notification configuration.
 * The actual behavior in case of a NOTIFY is taken from the command line.
 * From the notification name the team is retrieved. Then depending on the
 * severity one of two contact is chosen. *)
module Contact = struct
  type t =
    | ViaHttp of RamenOperation.http_cmd
    | ViaExec of string
    | ViaSysLog of string
    | ViaSqlite of
        { file : string ;
          insert : string ;
          create : string [@ppp_default ""] }
    [@@ppp PPP_OCaml]
end

module Team = struct
  type t =
    { (* Team name is really nothing but a prefix for notification names: *)
      name : string ;
      deferrable_contacts : Contact.t list
        [@ppp_default []] ;
      urgent_contacts : Contact.t list
        [@ppp_default []] }
    [@@ppp PPP_OCaml]

  let find_in_charge teams name =
    try List.find (fun t -> String.starts_with name t.name) teams
    with Not_found ->
      !logger.warning "No team name found in notification %S, \
                       assigning to first team." name ;
      List.hd teams
end

type notify_config =
  { teams : Team.t list }
  [@@ppp PPP_OCaml]

let default_notify_conf =
  let send_to_prometheus =
    Contact.ViaHttp RamenOperation.{
      method_ = HttpCmdPost ;
      url = "http://localhost:9093/api/v1/alerts" ;
      headers = [ "Content-Type", "application/json" ] ;
      body =
        {|[{"labels":{"alertname":"${name}","summary":"${text}","severity":"critical"}}]|} }
  in
  { teams =
      Team.[
        { name = "" ;
          deferrable_contacts =
            [ Contact.ViaSysLog "${name}: ${text}" ] ;
          urgent_contacts =
            [ send_to_prometheus ] } ] }

(* Function to replace a map of keys by their values in a string.
 * Keys are delimited in the string with "${" "}". *)
let subst_dict =
  let open Str in
  let re =
    regexp "\\${\\([_a-zA-Z][-_a-zA-Z0-9]+\\)}" in
  fun dict ?(quote=identity) ?null text ->
    global_substitute re (fun s ->
      let var_name = matched_group 1 s in
      try List.assoc var_name dict |> quote
      with Not_found ->
        !logger.error "Unknown parameter %S!" var_name ;
        null |? "??"^ var_name ^"??"
    ) text

let contact_via notif worker contact =
  let dict =
    ("name", notif.RamenOperation.name) ::
    ("severity",
      PPP.to_string RamenOperation.severity_ppp_ocaml notif.severity) ::
    notif.parameters in
  let exp ?q ?n = subst_dict dict ?quote:q ?null:n in
  let open Contact in
  match contact with
  | ViaHttp http ->
      http_send
        { http with
          body = exp http.body ;
          url = exp ~q:Uri.pct_encode http.url ;
          headers = List.map (fun (n, v) -> exp n, exp v) http.headers }
        worker
  | ViaExec cmd -> execute_cmd (exp ~q:shell_quote cmd) worker
  | ViaSysLog str -> log_str (exp str) worker
  | ViaSqlite { file ; insert ; create } ->
      wrap (fun () ->
        let ins = exp ~q:sql_quote ~n:"NULL" insert in
        sqllite_insert (exp file) ins create worker)

let generic_notify conf notif worker =
  (* Find the team in charge of that alert name: *)
  let open RamenOperation in
  let team = Team.find_in_charge conf.teams notif.name in
  let contacts =
    match notif.severity with
    | Deferrable -> team.Team.deferrable_contacts
    | Urgent -> team.Team.urgent_contacts in
  Lwt_list.iter_p (contact_via notif worker) contacts

let start conf rb =
  !logger.info "Starting notifier" ;
  let while_ () =
    if !RamenProcesses.quit then return_false else return_true in
  RamenSerialization.read_notifs ~while_ rb (fun (worker, notif) ->
    !logger.info "Received execute instruction from %s: %s"
      worker notif ;
    match PPP.of_string_exc RamenOperation.notification_ppp_ocaml
                            notif with
    | ExecuteCmd cmd -> execute_cmd cmd worker
    | HttpCmd http -> http_send http worker
    | SysLog str -> log_str str worker
    | NotifyCmd notif -> generic_notify conf notif worker)

let check_conf_is_valid conf =
  if conf.teams = [] then
    failwith "Notification configuration must have at least one team."
