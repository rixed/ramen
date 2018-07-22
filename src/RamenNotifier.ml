(*
 * Notifications:
 * To alleviate workers from the hassle to deal with external systems,
 * notifications are sent to Ramen notifier process via a ringbuffer.
 * Advantages are many:
 *
 * - Workers do not need so many libraries and are therefore smaller and
 *   easier to port to another language;
 *
 * - New notification mechanisms are easier to implement in a single
 *   location;
 *
 * - Workers are easier to distribute.
 *)
open Batteries
open Lwt
open RamenLog
open RamenHelpers
module C = RamenConf
open Stdint

(* Used to know if we must use normal schedule delay or schedule delay
 * after startup: *)
let startup_time = ref (Unix.gettimeofday ())

(* Used to build an id for each incident (aka sent alert) *)
type alert_id = uint64 [@@ppp PPP_OCaml]

let next_alert_id conf =
  let fname = conf.C.persist_dir ^"/notifier_state" in
  ensure_file_exists ~contents:"0" fname ;
  let get = ppp_of_file ~error_ok:true alert_id_ppp_ocaml in
  fun () ->
    let v = get fname in
    ppp_to_file fname alert_id_ppp_ocaml (Uint64.succ v) ;
    v

open Binocle

(* Number of notifications of each types: *)

let stats_count =
  IntCounter.make RamenConsts.MetricNames.notifs_count
    "Number of notifications sent, per channel."

let stats_send_fails =
  IntCounter.make RamenConsts.MetricNames.notifs_send_fails
    "Number of messages that could not be sent due to error."

let stats_rcv_errors =
  IntCounter.make RamenConsts.MetricNames.notifs_rcv_fails
    "Number of notifications that the notifier failed to read."

let stats_team_fallbacks =
  IntCounter.make RamenConsts.MetricNames.team_fallbacks
    "Number of times the default time was selected because the \
    configuration was not specific enough"

type http_cmd_method = HttpCmdGet | HttpCmdPost
  [@@ppp PPP_OCaml]

let http_send method_ url headers body worker =
  let open Cohttp in
  let open Cohttp_lwt_unix in
  let open RamenOperation in
  IntCounter.add ~labels:["via", "http"] stats_count 1 ;
  let headers =
    List.fold_left (fun h (n, v) ->
      Header.add h n v
    ) (Header.init_with "Connection" "close") headers in
  let url_ = Uri.of_string url in
  let thd =
    match method_ with
    | HttpCmdGet ->
      Client.get ~headers url_
    | HttpCmdPost ->
      let headers = Header.add headers "Content-type"
                               RamenConsts.ContentTypes.urlencoded in
      let body = Uri.pct_encode body |> Cohttp_lwt.Body.of_string in
      Client.post ~headers ~body url_
  in
  let%lwt resp, body = thd in
  let code = resp |> Response.status |> Code.code_of_status in
  if code <> 200 then (
    let%lwt body = Cohttp_lwt.Body.to_string body in
    !logger.error "Received code %d from %S (%S)" code url body ;
    IntCounter.add stats_send_fails 1 ;
    return_unit
  ) else
    Cohttp_lwt.Body.drain_body body

let execute_cmd cmd worker =
  IntCounter.add ~labels:["via", "execute"] stats_count 1 ;
  match%lwt run ~timeout:5. [| "/bin/sh"; "-c"; cmd |] with
  | exception e ->
      !logger.error "While executing command %S from %s: %s"
        cmd worker
        (Printexc.to_string e) ;
      IntCounter.add stats_send_fails 1 ;
      return_unit
  | stdout, stderr ->
      if stdout <> "" then !logger.debug "cmd: %s" stdout ;
      if stderr <> "" then !logger.error "cmd: %s" stderr ;
      return_unit

let log_str str worker =
  IntCounter.add ~labels:["via", "syslog"] stats_count 1 ;
  let level = `LOG_ALERT in
  match syslog with
  | None ->
    IntCounter.add stats_send_fails 1 ;
    fail_with "No syslog on this host"
  | Some slog ->
    Lwt.wrap (fun () -> Syslog.syslog slog level str)

let sqllite_insert file insert_q create_q worker =
  IntCounter.add ~labels:["via", "sqlite"] stats_count 1 ;
  let open Sqlite3 in
  let open SqliteHelpers in
  let handle = db_open file in
  let db_fail err q =
    let e = Printf.sprintf "Cannot %S into sqlite DB %S: %s"
              q file (Rc.to_string err) in
    IntCounter.add stats_send_fails 1 ;
    failwith e in
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
    | ViaHttp of
        { method_ : http_cmd_method
            [@ppp_rename "method"] [@ppp_default HttpCmdGet] ;
          url : string ;
          headers : (string * string) list
            [@ppp_default []] ;
          body : string [@ppp_default ""] }
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
      IntCounter.add stats_team_fallbacks 1 ;
      List.hd teams
end

type notify_config =
  { teams : Team.t list ;
    default_init_schedule_delay : float [@ppp_default 90.] ;
    default_init_schedule_delay_after_startup : float [@ppp_default 120.] }
  [@@ppp PPP_OCaml]

let default_notify_conf =
  let send_to_prometheus =
    Contact.ViaHttp {
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
    default_init_schedule_delay = 90. ;
    default_init_schedule_delay_after_startup = 120. }

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
        !logger.debug "Unknown parameter %S" var_name ;
        null |? "??"^ var_name ^"??"
    ) text

(* Generic notifications are also reliably sent, de-duplicated,
 * de-bounced and the ongoing incident is identified with an "alert-id"
 * that's usable in the notification template.
 *
 * First, each notification has an identifier (here, the name), a firing
 * boolean (we merely look for a parameter named "firing" and assume true if we
 * can't find it), and an event time (that we take equal to the reception time
 * by default). A notification might start or end an alert, which has an
 * identifier (integer), a start and an end time.  We keep only a set of unique
 * notifications (unique per notification name), until the ending notification
 * is received.
 *
 * We send a message to the user for each new starting notification and each
 * new ending notification.  But we do not send this message right away.
 * Instead, it is stored for later, to avoid flapping. *)

type pending_status =
  | StartToBeSent (* firing notification that is yet to be sent *)
  | StartToBeSentThenStopped (* notification that stopped before being sent *)
  | StartSent     (* firing notification that is yet to be acked *)
  | StartAcked (* firing notification that has been acked *)
  | StopToBeSent   (* non-firing notification that is yet to be sent *)
  | StopSent       (* non-firing notification that is yet to be acked *)
  | StopAcked
  [@@ppp PPP_OCaml]

let string_of_pending_status =
  PPP.to_string pending_status_ppp_ocaml

type pending_notification =
  { notif : RamenOperation.notification;
    contact : Contact.t ;
    worker : string ;
    rcvd_start : float ;
    mutable rcvd_stop : float option ;
    event_start : float option ;
    mutable event_stop : float option ;
    mutable attempts : int ;
    mutable alert_id : uint64 }

type scheduled_item  =
  { (* When we planned to send this notif: *)
    mutable schedule_time : float ;
    (* When it has been actually rescheduled to: *)
    mutable send_time : float ;
    (* The current delivery status: *)
    mutable status : pending_status ;
    (* Will we receive an end of alert notification? *)
    mutable wait_for_stop : bool ;
    (* The notification that's to be sent: *)
    mutable item : pending_notification }

module PendingSet = Set.Make (struct
  type t = scheduled_item
  let compare i1 i2 =
    let c = String.compare i1.item.notif.RamenOperation.notif_name
                           i2.item.notif.notif_name in
    if c <> 0 then c else
    Contact.compare i1.item.contact i2.item.contact
end)

type pendings =
  { (* We keep only one notif per alert id * contact *)
    mutable set : PendingSet.t ;
    (* Delivery schedule is a heap ordered by schedule_time. All notifications
     * that has to be sent or acked are in there: *)
    mutable heap : scheduled_item RamenHeap.t ;
    (* Set of all known alert ids. Notifications are removed from this set as
     * soon as the end of the alert is acked. *)
    mutable dirty : bool }

let pendings =
  { set = PendingSet.empty ;
    heap = RamenHeap.empty ;
    dirty = false }

let heap_pending_cmp i1 i2 =
  Float.compare i1.schedule_time i2.schedule_time

let save_pendings conf =
  let fname = C.pending_notifications_file conf in
  marshal_into_file fname pendings.set

let restore_pendings conf =
  let fname = C.pending_notifications_file conf in
  (try
    pendings.set <- marshal_from_file fname
  with Unix.(Unix_error (ENOENT, _, _)) -> ()) ;
  pendings.heap <-
    PendingSet.fold (RamenHeap.add heap_pending_cmp)
      pendings.set RamenHeap.empty ;
  pendings.dirty <- false

(* Used to find pending notif in the set: *)
let fake_pending_named notif_name contact =
  { schedule_time = 0. ;
    send_time = 0. ;
    status = StartToBeSent ;
    wait_for_stop = false ;
    item =
      { rcvd_start = 0. ;
        rcvd_stop = None ;
        event_start = None ;
        event_stop = None ;
        attempts = 0 ;
        alert_id = Uint64.zero ;
        worker = "" ;
        contact ;
        notif = { notif_name ; severity = Urgent ; parameters = [] } } }

(* When we receive a notification that an alert is no more firing, we must
 * cancel pending delivery or send the end of alert notification.
 * Can raise Not_found. *)
let extinguish_pending notif_conf name event_stop now contact =
  match PendingSet.find (fake_pending_named name contact) pendings.set with
  | exception Not_found ->
      !logger.warning "Cannot find pending notification %s to stop"
        name ;
  | p ->
      (match p.status with
      | StartToBeSent ->
          p.status <- StartToBeSentThenStopped ;
          p.item.event_stop <- event_stop ;
          p.item.rcvd_stop <- Some now ;
          pendings.dirty <- true
      | StartSent (* don't care about the ack any more *)
      | StartAcked ->
          p.status <- StopToBeSent ;
          p.item.event_stop <- event_stop ;
          p.item.rcvd_stop <- Some now ;
          p.item.attempts <- 0 ;
          (* reschedule *)
          p.send_time <-
            now +. jitter notif_conf.default_init_schedule_delay ;
          pendings.heap <-
            RamenHeap.add heap_pending_cmp p pendings.heap ;
          pendings.dirty <- true
      | StopSent | StopToBeSent | StartToBeSentThenStopped -> ()
      | StopAcked ->
          !logger.error "StopAcked alert found in the pending set!")

(* When we receive a notification that an alert is firing, we must first
 * check if we have a pending notification with same name already and
 * reschedule it, or create a new one. *)
let set_alight conf notif_conf notif worker event_start rcvd_start
               wait_for_stop contact =
  let new_pending =
    (* schedule_delay_after_startup is the minimum time we should wait
     * after startup to ever consider sending the notification. If we
     * are still that close to startup, then we delay the scheduling of
     * the send by at least what remains to be past startup.
     * If we are already past startup then we schedule the send for
     * init_schedule_delay: *)
    let init_delay_after_startup = notif_conf.default_init_schedule_delay_after_startup
    and init_delay = notif_conf.default_init_schedule_delay in
    let until_end_of_statup =
      init_delay_after_startup -. (rcvd_start -. !startup_time) in
    let init_delay =
      if until_end_of_statup > 0. then max until_end_of_statup init_delay
      else init_delay in
    let schedule_time =
      rcvd_start +. jitter init_delay in
    { schedule_time ;
      send_time = schedule_time ;
      status = StartToBeSent ;
      wait_for_stop ;
      item = {
        attempts = 0 ; alert_id = Uint64.zero ;
        notif ; contact ; worker ;
        event_start ; event_stop = None ;
        rcvd_start ; rcvd_stop = None } } in
  let queue_new_alert () =
    new_pending.item.alert_id <- next_alert_id conf () ;
    pendings.set <- PendingSet.add new_pending pendings.set ;
    pendings.heap <-
      RamenHeap.add heap_pending_cmp new_pending pendings.heap ;
    pendings.dirty <- true in
  match PendingSet.find new_pending pendings.set with
  | exception Not_found ->
      queue_new_alert ()
  | p ->
      (match p.status with
      | StartToBeSentThenStopped | StopSent ->
          p.status <- StartToBeSent ;
          p.wait_for_stop <- new_pending.wait_for_stop ;
          p.item.attempts <- 0 ;
          p.send_time <- new_pending.send_time ;
          pendings.dirty <- true
      | StopToBeSent ->
          p.status <- StartAcked ;
          pendings.dirty <- true
      | StartAcked | StartToBeSent | StartSent -> ()
      | StopAcked (* Those should not be in the set *) ->
          !logger.error "StopAcked alert found in the pending set!" ;
          queue_new_alert ())

let ack name contact =
  match PendingSet.find (fake_pending_named name contact) pendings.set with
  | exception Not_found ->
      !logger.warning "Received an Ack for unknown notification %s via %a, \
                       ignoring."
        name Contact.print contact
  | { status = StartSent ; _ } as p ->
      !logger.info "Successfully notified %a of alert %s starting"
        Contact.print contact name ;
      if p.wait_for_stop then
        p.status <- StartAcked
      else (
        (* That's already the end of this story *)
        p.status <- StopAcked ;
        pendings.set <- PendingSet.remove p pendings.set
      ) ;
      pendings.dirty <- true
  | { status = StopSent ; _ } as p ->
      !logger.info "Successfully notified %a of alert %s ending"
        Contact.print contact name ;
      p.status <- StopAcked ; (* So that we know we can ignore it when its scheduled again *)
      pendings.set <- PendingSet.remove p pendings.set ; (* So that we create a new one with a fresh alert_id if it fires again *)
      pendings.dirty <- true
  | p ->
      (if p.status = StopAcked then !logger.debug else !logger.warning)
        "Received an ACK for notification in status %s, ignoring"
        (string_of_pending_status p.status)

(* A thread that notifies the external world and wait for a successful
 * confirmation, or fails.
 * TODO: time this thread and add this to notifier instrumentation. *)
let contact_via item =
  let dict =
    [ "name", item.notif.RamenOperation.notif_name ;
      "severity",
        PPP.to_string RamenOperation.severity_ppp_ocaml item.notif.severity ;
      "alert_id", Uint64.to_string item.alert_id ;
      "start", string_of_float (item.event_start |? item.rcvd_start) ;
      "worker", item.worker ] in
  (* Add "stop" if we have it (or les it be NULL) *)
  let dict =
    match item.event_stop with
    | Some t -> ("stop", string_of_float t) :: dict
    | None ->
        (match item.rcvd_stop with
        | Some t -> ("stop", string_of_float t) :: dict
        | None -> dict) in
  let dict = List.rev_append dict item.notif.parameters in
  let dict = List.rev dict (* Allow parameters to overwrite builtins *) in
  let exp ?q ?n = subst_dict dict ?quote:q ?null:n in
  let open Contact in
  match item.contact with
  | ViaHttp http ->
      http_send http.method_
                (exp ~q:Uri.pct_encode http.url)
                (List.map (fun (n, v) -> exp n, exp v) http.headers)
                (exp http.body)
                item.worker
  | ViaExec cmd -> execute_cmd (exp ~q:shell_quote cmd) item.worker
  | ViaSysLog str -> log_str (exp str) item.worker
  | ViaSqlite { file ; insert ; create } ->
      Lwt.wrap (fun () ->
        let ins = exp ~q:sql_quote ~n:"NULL" insert in
        sqllite_insert (exp file) ins create item.worker)

(* Returns the timeout, or 0,. if the is nothing to wait for *)
let do_notify i =
  if i.attempts >= 3 then (
    !logger.warning "Cannot deliver alert %s after %d attempt, \
                     giving up" i.notif.notif_name i.attempts ;
    0.
  ) else (
    let timeout = 5. (* TODO *) in
    i.attempts <- i.attempts + 1 ;
    async (fun () ->
      match%lwt contact_via i with
      | exception e ->
          !logger.error "Cannot notify: %s" (Printexc.to_string e) ;
          return_unit (* let it timeout *)
      | () ->
          if timeout > 0. then
            Lwt.wrap (fun () -> ack i.notif.notif_name i.contact)
          else return_unit) ;
    timeout
  )

(* Returns true if there may still be notifications to be sent: *)
let send_next now =
  let reschedule_min time =
    let p, heap = RamenHeap.pop_min heap_pending_cmp pendings.heap in
    p.schedule_time <- time ;
    pendings.heap <- RamenHeap.add heap_pending_cmp p heap
  and del_min p =
    pendings.heap <- RamenHeap.del_min heap_pending_cmp pendings.heap ;
    pendings.set <- PendingSet.remove p pendings.set
  in
  match RamenHeap.min pendings.heap with
  | exception Not_found -> false
  | p ->
      if p.schedule_time > now then false
      else (
        (match p.status with
        | StartToBeSent | StopToBeSent ->
            if p.send_time <= now then (
              let timeout = do_notify p.item in
              p.status <-
                if p.status = StartToBeSent then StartSent else StopSent ;
              if timeout > 0. then (
                reschedule_min (now +. timeout)
              ) else (
                ack p.item.notif.notif_name p.item.contact ;
                del_min p  (* Acked alerts need not be in the scheduling heap *)
              ) ;
            ) else (
              reschedule_min p.send_time
            )
        | StartToBeSentThenStopped | StartAcked | StopAcked ->
            (* No need to reschedule this *)
            del_min p
        | StartSent | StopSent -> (* That's a timeout, resend *)
            assert (p.send_time <= p.schedule_time) ;
            !logger.warning "Timing out sending a notification to %a for %s of alert %s"
              Contact.print p.item.contact
              (if p.status = StartSent then "starting" else "stopping")
              p.item.notif.notif_name ;
            p.status <- (if p.status = StartSent then StartToBeSent else StopToBeSent) ;
            p.send_time <- now ;
            reschedule_min now
      ) ;
      pendings.dirty <- true ;
      true
    )

(* Avoid creating several instances of watchdogs even when thread crashes
 * and is restarted: *)
let watchdog = RamenWatchdog.make "notifier" RamenProcesses.quit

let send_notifications conf =
  let rec loop () =
    let now = Unix.gettimeofday () in
    while send_next now do () done ;
    if pendings.dirty then (
      save_pendings conf ;
      pendings.dirty <- false) ;
    RamenWatchdog.reset watchdog ;
    Lwt_unix.sleep 1. >>= loop in
  RamenWatchdog.run watchdog ;
  loop ()

let start conf notif_conf rb =
  !logger.info "Starting notifier" ;
  restore_pendings conf ;
  (* Better check if we can draw a new alert_id before we need it: *)
  let _alert_id = next_alert_id conf () in
  async (fun () ->
    restart_on_failure "send_notifications" send_notifications conf) ;
  let while_ () =
    if !RamenProcesses.quit <> None then return_false else return_true in
  RamenSerialization.read_notifs ~while_ rb (fun (worker, notif) ->
    !logger.info "Received message from %s: %s"
      worker notif ;
    match PPP.of_string_exc RamenOperation.notification_ppp_ocaml notif with
    | exception _ ->
        !logger.error "Cannot deserialize notification %S, skipping" notif ;
        IntCounter.add stats_rcv_errors 1 ;
        return_unit
    | notif ->
        let event_time =
          (* TODO: a real field, see #273 *)
          try
            Some (List.find (fun (n, _) -> n = "time") notif.parameters |>
                  snd |> float_of_string)
          with Not_found | Failure _ -> None
        and now = Unix.gettimeofday ()
        (* Find the team in charge of that alert name: *)
        and team = Team.find_in_charge notif_conf.teams notif.notif_name in
        let contacts =
          match notif.severity with
          | Deferrable -> team.Team.deferrable_contacts
          | Urgent -> team.Team.urgent_contacts in
        let action =
          (* TODO: a formal parameter, for now we get it from the list of template vars: *)
          match List.find (fun (n, _) -> n = "firing") notif.parameters with
          | exception Not_found ->
              set_alight conf notif_conf notif worker event_time now false
          | _, v ->
              if looks_like_true v then
                set_alight conf notif_conf notif worker event_time now true
              else
                extinguish_pending notif_conf notif.notif_name event_time now in
        List.iter action contacts ;
        return_unit)

let check_conf_is_valid notif_conf =
  if notif_conf.teams = [] then
    failwith "Notification configuration must have at least one team."
