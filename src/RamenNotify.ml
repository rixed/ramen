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

  let compare = compare

  let print oc cmd =
    PPP.to_string t_ppp_ocaml cmd |>
    String.print oc
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
  { teams : Team.t list ;
    default_init_schedule_delay : float [@ppp_default 90.] }
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
            [ send_to_prometheus ] } ] ;
    default_init_schedule_delay = 90. }

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

(* A thread that notifies the external world and wait for a successful
 * confirmation, or fails. *)
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

(* Generic notification are also reliably sent, deduplicated and
 * non-flapping.
 * First, each notification has an identifier (here, the name),
 * a firing boolean (we merely look for a parameter named "firing"
 * and assume true if we can't find it), and an event time (that we
 * take equal to the reception time by default).
 * Then we are not going to send the notification right away, but
 * store it for later, to avoid flapping. We store this heap of
 * notifications to be delivered on disc.  *)

type pending_status = ToBeSent | Sent | SentThenCancelled | Cancelled

type pending_notification =
  { notif : RamenOperation.notify_cmd ;
    contact : Contact.t ;
    worker : string ;
    rcvd_time : float ;
    event_time : float option ;
    mutable attempts : int }

type scheduled_item  =
  { (* When we planned to send this notif: *)
    mutable schedule_time : float ;
    (* When it has been actually rescheduled to: *)
    mutable send_time : float ;
    (* The current delivery status: *)
    mutable status : pending_status ;
    (* The notification that's to be sent: *)
    mutable item : pending_notification }

module PendingSet = Set.Make (struct
  type t = scheduled_item
  let compare i1 i2 =
    let c = String.compare i1.item.notif.RamenOperation.name i2.item.notif.name in
    if c <> 0 then c else
    Contact.compare i1.item.contact i2.item.contact
end)

type pendings =
  { (* We keep only one notif per alert id * contact *)
    mutable set : PendingSet.t ;
    (* Delivery schedule is a heap ordered by schedule_time: *)
    mutable heap : scheduled_item RamenHeap.t ;
    mutable dirty : bool }

let pendings =
  { set = PendingSet.empty ;
    heap = RamenHeap.empty ;
    dirty = false }

let heap_pending_cmp i1 i2 =
  Float.compare i1.schedule_time i2.schedule_time

let save_pendings conf =
  let fname = RamenConf.pending_notifications_file conf in
  marshal_into_file fname pendings.set

let restore_pendings conf =
  let fname = RamenConf.pending_notifications_file conf in
  (try
    pendings.set <- marshal_from_file fname
  with Unix.(Unix_error (ENOENT, _, _)) -> ()) ;
  pendings.heap <-
    PendingSet.fold (RamenHeap.add heap_pending_cmp)
      pendings.set RamenHeap.empty ;
  pendings.dirty <- false

(* Used to find pending notif in the set: *)
let fake_pending_named name contact =
  { schedule_time = 0. ;
    send_time = 0. ;
    status = Cancelled ;
    item =
      { rcvd_time = 0. ;
        event_time = None ;
        attempts = 0 ;
        worker = "" ;
        contact ;
        notif = { name ; severity = Urgent ; parameters = [] } } }

(* When we receive a notification that an alert is no more firing, we must
 * cancel pending delivery. If the alert has been sent already then we
 * flag it as extinguished, to avoid looking too deep in the heap when the
 * ack is eventually received.
 * Can raise Not_found. *)
let extinguish_pending name contact =
  let p = PendingSet.find (fake_pending_named name contact) pendings.set in
  (match p.status with
  | ToBeSent -> p.status <- Cancelled
  | Sent -> p.status <- SentThenCancelled
  | _ -> ()) ;
  pendings.dirty <- true

(* When we receive a notification that an alert is firing, we must first
 * check if we have a pending notification with same name already and
 * reschedule it, or create a new one. *)
let put_alight notif_conf notif worker event_time rcvd_time contact =
  let new_pending =
    let schedule_time =
      rcvd_time +. jitter notif_conf.default_init_schedule_delay in
    { schedule_time ;
      send_time = schedule_time ;
      status = ToBeSent ;
      item = {
        attempts = 0 ;
        notif ; contact ; worker ; event_time ; rcvd_time } } in
  (match PendingSet.find new_pending pendings.set with
  | exception Not_found ->
      pendings.set <- PendingSet.add new_pending pendings.set ;
      pendings.heap <-
        RamenHeap.add heap_pending_cmp new_pending pendings.heap ;
  | p ->
      (match p.status with
      | ToBeSent (* No change *)
      | Sent (* Keep waiting for the ack then *) -> ()
      | SentThenCancelled (* Cancel the cancellation *) -> p.status <- Sent
      | Cancelled (* It's like it's not really there *) ->
          p.status <- ToBeSent ;
          p.send_time <- new_pending.send_time ;
          p.item <- new_pending.item)) ;
  pendings.dirty <- true

let ack name contact =
  let p = PendingSet.find (fake_pending_named name contact) pendings.set in
  (match p.status with
  | SentThenCancelled -> (* too bad *)
      !logger.info "Successfully sent notification %s to %a, \
                    which timed out" name Contact.print contact
  | Sent ->
      !logger.info "Successfully sent notification %s to %a"
        name Contact.print contact ;
      p.status <- Cancelled
  | _ -> assert false) ;
  pendings.dirty <- true

(* Returns the timeout, or 0,. if the is nothing to wait for *)
let do_notify i =
  if i.attempts >= 3 then (
    !logger.warning "Cannot deliver alert %s after %d attempt, \
                     giving up" i.notif.name i.attempts ;
    0.
  ) else (
    i.attempts <- i.attempts + 1 ;
    async (fun () ->
      match%lwt contact_via i.notif i.worker i.contact with
      | exception e -> return_unit (* let it timeout *)
      | () -> wrap (fun () -> ack i.notif.name i.contact)) ;
    0. (* TODO *)
  )

(* Returns true if there may still be notifications to be sent: *)
let send_next now =
  let reschedule_min time =
    let p, heap = RamenHeap.pop_min heap_pending_cmp pendings.heap in
    p.schedule_time <- time ;
    pendings.heap <- RamenHeap.add heap_pending_cmp p heap
  and del_min () =
    pendings.heap <- RamenHeap.del_min heap_pending_cmp pendings.heap
  in
  match RamenHeap.min pendings.heap with
  | exception Not_found -> false
  | p ->
      if p.schedule_time > now then false
      else (
        (match p.status with
        | ToBeSent ->
            assert (p.send_time >= p.schedule_time) ;
            if p.send_time <= now then (
              let timeout = do_notify p.item in
              if timeout > 0. then (
                p.status <- Sent ;
                reschedule_min (now +. timeout)
              ) else (
                (* We are done with this notification *)
                del_min ()
              )
            ) else (
              reschedule_min p.send_time
            )
        | Sent -> (* That's a timeout, reschedule *)
            assert (p.send_time <= p.schedule_time) ;
            (* Reschedule a few times: *)
            p.status <- ToBeSent ;
            p.send_time <- now ;
            reschedule_min now
        | SentThenCancelled ->
            assert (p.send_time <= p.schedule_time) ;
            (* Alert is flapping and the notification failed. Oh, well. *)
            del_min ()
        | Cancelled ->
            (* normal end of life *)
            assert (p.send_time <= p.schedule_time) ;
            (* TODO: a counter for those: *)
            if p.item.attempts = 0 then
              !logger.info "Cancelled flapping alert" ;
            del_min ()
      ) ;
      true
    )

let send_notifications conf =
  let rec loop () =
    let now = Unix.gettimeofday () in
    while send_next now do () done ;
    if pendings.dirty then (
      save_pendings conf ;
      pendings.dirty <- false) ;
    Lwt_unix.sleep 1. >>= loop in
  loop ()

let start conf notif_conf rb =
  !logger.info "Starting notifier" ;
  restore_pendings conf ;
  async (fun () ->
    restart_on_failure "send_notifications" send_notifications conf) ;
  let while_ () =
    if !RamenProcesses.quit then return_false else return_true in
  RamenSerialization.read_notifs ~while_ rb (fun (worker, notif) ->
    !logger.info "Received execute instruction from %s: %s"
      worker notif ;
    let event_time = None (* TODO, see #273 *)
    and now = Unix.gettimeofday () in
    match PPP.of_string_exc RamenOperation.notification_ppp_ocaml
                            notif with
    | ExecuteCmd cmd -> execute_cmd cmd worker
    | HttpCmd http -> http_send http worker
    | SysLog str -> log_str str worker
    | NotifyCmd notif ->
        (* Find the team in charge of that alert name: *)
        let open RamenOperation in
        let team = Team.find_in_charge notif_conf.teams notif.name in
        let contacts =
          match notif.severity with
          | Deferrable -> team.Team.deferrable_contacts
          | Urgent -> team.Team.urgent_contacts in
        List.iter (put_alight notif_conf notif worker event_time now) contacts ;
        return_unit)

let check_conf_is_valid notif_conf =
  if notif_conf.teams = [] then
    failwith "Notification configuration must have at least one team."
