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
open Stdint
open RamenLog
open RamenHelpers
open RamenNullable
module C = RamenConf

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
  RamenBinocle.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir
      RamenConsts.Metric.Names.notifs_count
      "Number of notifications sent, per channel.")

let stats_send_fails =
  RamenBinocle.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir
      RamenConsts.Metric.Names.notifs_send_fails
      "Number of messages that could not be sent due to error.")

let stats_team_fallbacks =
  RamenBinocle.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir
      RamenConsts.Metric.Names.team_fallbacks
      "Number of times the default team was selected because the \
      configuration was not specific enough")

let stats_notifs_cancelled =
  RamenBinocle.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir
      RamenConsts.Metric.Names.notifs_cancelled
      "Number of notifications not send, per reason")

let max_exec = Atomic.Counter.make 5 (* no more than 5 simultaneous execs *)

let execute_cmd conf cmd =
  IntCounter.inc ~labels:["via", "execute"] (stats_count conf.C.persist_dir) ;
  let cmd_name = "notifier exec" in
  match run_coprocess ~max_count:max_exec cmd_name cmd with
  | None ->
      !logger.error "Cannot execute %S" cmd ;
      IntCounter.inc (stats_send_fails conf.C.persist_dir)
  | Some (Unix.WEXITED 0) -> ()
  | Some status ->
      !logger.error "Cannot execute %S: %s"
        cmd (string_of_process_status status) ;
      IntCounter.inc (stats_send_fails conf.C.persist_dir)

let log_str conf str =
  IntCounter.inc ~labels:["via", "syslog"] (stats_count conf.C.persist_dir) ;
  let level = `LOG_ALERT in
  match syslog with
  | None ->
      IntCounter.inc (stats_send_fails conf.C.persist_dir) ;
      failwith "No syslog on this host"
  | Some slog ->
      Syslog.syslog slog level str

let sqllite_insert conf file insert_q create_q =
  IntCounter.inc ~labels:["via", "sqlite"] (stats_count conf.C.persist_dir) ;
  let open Sqlite3 in
  let open SqliteHelpers in
  let handle = db_open file in
  let db_fail err q =
    let e = Printf.sprintf "Cannot %S into sqlite DB %S: %s"
              q file (Rc.to_string err) in
    IntCounter.inc (stats_send_fails conf.C.persist_dir) ;
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
      contacts : Contact.t list [@ppp_default []] }
    [@@ppp PPP_OCaml]

  let find_in_charge conf teams name =
    try List.find (fun t -> String.starts_with name t.name) teams
    with Not_found ->
      !logger.warning "No team name found in notification %S, \
                       assigning to first team." name ;
      IntCounter.inc (stats_team_fallbacks conf.C.persist_dir) ;
      List.hd teams
end

type notify_config =
  { teams : Team.t list ;
    default_init_schedule_delay : float [@ppp_default 90.] ;
    default_init_schedule_delay_after_startup : float [@ppp_default 120.] }
  [@@ppp PPP_OCaml]

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
  | StartToBeSent  (* firing notification that is yet to be sent *)
  | StartToBeSentThenStopped (* notification that stopped before being sent *)
  | StartSent      (* firing notification that is yet to be acked *)
  | StartAcked     (* firing notification that has been acked *)
  | StopToBeSent   (* non-firing notification that is yet to be sent *)
  | StopSent       (* non-firing notification that is yet to be acked *)
  | StopAcked
  [@@ppp PPP_OCaml]

let string_of_pending_status =
  PPP.to_string pending_status_ppp_ocaml

(* Same as RamenOperation.notification, but with strings for name and
 * and parameters: *)
type notification =
  { worker : string ;
    notif_name : string ;
    event_time : float option ;
    sent_time : float ;
    rcvd_time : float ;
    firing : bool option ;
    certainty : float ;
    parameters : (string * string) list }

type pending_notification =
  { notif : notification;
    contact : Contact.t ;
    mutable rcvd_stop : float option ;
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
    let c = String.compare i1.item.notif.notif_name
                           i2.item.notif.notif_name in
    if c <> 0 then c else
    Contact.compare i1.item.contact i2.item.contact
end)

let max_last_sent = 100

type pendings =
  { (* We keep only one notif per alert id * contact *)
    mutable set : PendingSet.t ;
    (* Delivery schedule is a heap ordered by schedule_time. All notifications
     * that has to be sent or acked are in there: *)
    mutable heap : scheduled_item RamenHeap.t ;
    (* Set of the timestamp * certainties of the last last_max_sent
     * notifications that have been sent (not persisted to disk): *)
    mutable last_sent : (float * float) Deque.t ;
    (* Flag that tells us if this record has been modified since last save: *)
    mutable dirty : bool }

let pendings =
  { set = PendingSet.empty ;
    heap = RamenHeap.empty ;
    last_sent = Deque.empty ;
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
      { rcvd_stop = None ;
        event_stop = None ;
        attempts = 0 ;
        alert_id = Uint64.zero ;
        contact ;
        notif =
          { notif_name ; worker = "" ; parameters = [] ;
            firing = None ; certainty = 0. ;
            event_time = None ; sent_time = 0. ; rcvd_time = 0. } } }

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
let set_alight conf notif_conf notif wait_for_stop contact =
  let new_pending =
    (* schedule_delay_after_startup is the minimum time we should wait
     * after startup to ever consider sending the notification. If we
     * are still that close to startup, then we delay the scheduling of
     * the send by at least what remains to be past startup.
     * If we are already past startup then we schedule the send for
     * init_schedule_delay: *)
    let init_delay_after_startup =
      notif_conf.default_init_schedule_delay_after_startup
    and init_delay = notif_conf.default_init_schedule_delay in
    let until_end_of_statup =
      init_delay_after_startup -. (notif.rcvd_time -. !startup_time) in
    let init_delay =
      if until_end_of_statup > 0. then max until_end_of_statup init_delay
      else init_delay in
    let schedule_time =
      notif.rcvd_time +. jitter init_delay in
    { schedule_time ;
      send_time = schedule_time ;
      status = StartToBeSent ;
      wait_for_stop ;
      item = {
        attempts = 0 ; alert_id = Uint64.zero ;
        notif ; contact ; event_stop = None ;
        rcvd_stop = None } } in
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

(*
 * A thread that notifies the external world and wait for a successful
 * confirmation, or fails.
 *
 * TODO: time this thread and add this to notifier instrumentation.
 *)

(* When a notification is delivered to the user (or we abandon it).
 * Notice that "is delivered" depends on the channel: some may have
 * delivery acknowledgment while some may not. *)
let ack name contact now =
  match PendingSet.find (fake_pending_named name contact) pendings.set with
  | exception Not_found ->
      !logger.warning "Received an Ack for unknown notification %s via %a, \
                       ignoring."
        name Contact.print contact
  | { status = StartSent ; _ } as p ->
      !logger.info "Successfully notified %a of start of alert %S"
        Contact.print contact name ;
      if p.wait_for_stop then
        p.status <- StartAcked
      else (
        (* That's already the end of this story *)
        p.status <- StopAcked ;
        pendings.set <- PendingSet.remove p pendings.set
      ) ;
      (* Save this emission: *)
      pendings.last_sent <- Deque.cons (now, p.item.notif.certainty) pendings.last_sent ;
      if Deque.size pendings.last_sent > max_last_sent then
        pendings.last_sent <- fst (Option.get (Deque.rear pendings.last_sent)) ;
      pendings.dirty <- true
  | { status = StopSent ; _ } as p ->
      !logger.info "Successfully notified %a of ending of alert %S"
        Contact.print contact name ;
      p.status <- StopAcked ; (* So that we know we can ignore it when its scheduled again *)
      pendings.set <- PendingSet.remove p pendings.set ; (* So that we create a new one with a fresh alert_id if it fires again *)
      pendings.dirty <- true
  | p ->
      (if p.status = StopAcked then !logger.debug else !logger.warning)
        "Ignoring an ACK for notification in status %s"
        (string_of_pending_status p.status)

(* When we give up sending a notification *)
(* TODO: log *)
let cancel conf pending _now reason =
  !logger.info "Cancelling alert %S: %s"
    pending.item.notif.notif_name reason ;
  let labels = ["reason", reason] in
  IntCounter.inc ~labels (stats_notifs_cancelled conf.C.persist_dir) ;
  pending.status <- StopAcked ;
  pendings.set <- PendingSet.remove pending pendings.set ;
  pendings.dirty <- true

(* Returns the timeout (0. if there will be no delayed ack *)
let contact_via conf item =
  let dict =
    [ "name", item.notif.notif_name ;
      "alert_id", Uint64.to_string item.alert_id ;
      "start", string_of_float (item.notif.event_time |? item.notif.rcvd_time) ;
      "worker", item.notif.worker ] in
  (* Add "stop" if we have it (or let it be NULL) *)
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
  (match item.contact with
  | ViaExec cmd ->
      execute_cmd conf (exp ~q:shell_quote cmd)
  | ViaSysLog str ->
      log_str conf (exp str)
  | ViaSqlite { file ; insert ; create } ->
      let ins = exp ~q:sql_quote ~n:"NULL" insert in
      sqllite_insert conf (exp file) ins create) ;
  0. (* timeout *)

(* Returns the timeout, or fail *)
(* TODO: log *)
let do_notify conf pending _now =
  let i = pending.item in
  if i.attempts >= 3 then (
    !logger.warning "Cannot deliver alert %S after %d attempt, \
                     giving up" i.notif.notif_name i.attempts ;
    failwith "too many attempts"
  ) else (
    i.attempts <- i.attempts + 1 ;
    contact_via conf i
  )

let pass_fpr max_fpr now certainty =
  match Deque.rear pendings.last_sent with
  | None ->
      !logger.info "Max FPR test: pass due to first notification ever sent." ;
      true
  | Some (_, (oldest, _)) ->
      let dt = now -. oldest in
      let max_fp = Float.ceil (dt *. max_fpr) |> int_of_float in
      (* Compute the probability that we had more than max_fp fp already. *)
      if max_fp < 1 (* bogus dt *) then (
        !logger.info "Max FPR test: bogus DT" ;
        false
      ) else if max_fp > 1 + Deque.size pendings.last_sent then (
        !logger.info "Max FPR test: Haven't sent enough notif yet (%d) to \
                      send more false positives than %d"
          (Deque.size pendings.last_sent) max_fp ;
        true
      ) else (
        (* Actually, we are going to compute the probability that we sent
         * exactly 0 junk notif, exactly 1, etc up to max_fp, and then
         * take 1 - that probability.
         * Initially, the probability to have sent 0 is 1 and everything
         * else 0: *)
        let p_junks = Array.init (max_fp + 1) (fun i ->
          if i = 0 then 1. else 0.) in
        (* For each notification sent, update the probabilities to have sent N
         * false positives: *)
        let send certainty =
          let p_junk = 1. -. certainty in
          (* Probability to have sent N = probability to have sent N and not
           * send another one + probability to have sent N-1 and send a new
           * one: *)
          for i = Array.length p_junks - 1 downto 0 do
            p_junks.(i) <- p_junks.(i) *. certainty +.
              (if i > 0 then p_junks.(i-1) *. p_junk else 0.)
          done in
        Deque.iter (fun (_, certainty) -> send certainty) pendings.last_sent ;
        (* And then we also suppose we send that new one: *)
        send certainty ;
        (* The probability to have sent less or exactly max_fp is thus: *)
        let p_less_eq = Array.fold_left (+.) 0. p_junks in
        !logger.debug "After sent %a, Proba to send exactly 0..N: %a"
          (Deque.print (Tuple2.print Float.print Float.print)) pendings.last_sent
          (Array.print Float.print) p_junks ;
        (* So that the probability to have sent more than max_fp is: *)
        let p_more = 1. -. p_less_eq in
        !logger.info "Max FPR test: we have sent %d notifications since %.0f, \
                      probability to send more than %d false positive: %f."
          (Deque.size pendings.last_sent) oldest max_fp p_more ;
        p_more <= 0.5
      )

(* Returns true if there may still be notifications to be sent: *)
let send_next conf max_fpr now =
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
              if p.status = StopToBeSent ||
                 pass_fpr max_fpr now p.item.notif.certainty
              then (
                match do_notify conf p now with
                | exception Failure reason ->
                    cancel conf p now reason ;
                    del_min p
                | exception e ->
                    !logger.error "Cannot notify: %s"
                      (Printexc.to_string e)
                    (* and let it timeout *)
                | timeout ->
                  p.status <- if p.status = StartToBeSent then StartSent
                                                          else StopSent ;
                  if timeout > 0. then (
                    reschedule_min (now +. timeout)
                  ) else (
                    ack p.item.notif.notif_name p.item.contact now ;
                    (* Acked alerts need not be in the scheduling heap *)
                    del_min p
                  )
              ) else ( (* not pass_fpr *)
                cancel conf p now "too many false positives" ;
                del_min p
              )
            ) else ( (* p.send_time > now *)
              reschedule_min p.send_time
            )
        | StartToBeSentThenStopped | StartAcked | StopAcked ->
            (* No need to reschedule this *)
            del_min p
        | StartSent | StopSent -> (* That's a timeout, resend *)
            assert (p.send_time <= p.schedule_time) ;
            !logger.warning "Timing out sending a notification to %a for %s of alert %S"
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
let watchdog = ref None

let send_notifications max_fpr conf =
  if !watchdog = None then
    watchdog := Some (RamenWatchdog.make "notifier" RamenProcesses.quit) ;
  let watchdog = Option.get !watchdog in
  let rec loop () =
    let now = Unix.gettimeofday () in
    while send_next conf max_fpr now do () done ;
    if pendings.dirty then (
      save_pendings conf ;
      pendings.dirty <- false) ;
    RamenWatchdog.reset watchdog ;
    Unix.sleep 1 ;
    loop () in
  RamenWatchdog.enable watchdog ;
  loop ()

let check_conf_is_valid notif_conf =
  if notif_conf.teams = [] then
    failwith "Notification configuration must have at least one team."

let ensure_conf_file_exists notif_conf_file =
  (* Default content in case the configuration file is absent: *)
  let default_conf =
    let send_to_prometheus =
      Contact.ViaExec "\
        curl \
          -X POST \
          -H 'Content-Type: application/json' \
          --data-raw '[{\"labels\":{\
               \"alertname\":\"'${name}'\",\
               \"summary\":\"'${text}'\",\
               \"severity\":\"critical\"}}]' \
          'http://localhost:9093/api/v1/alerts'" in
    { teams =
        Team.[
          { name = "" ;
            contacts = [ send_to_prometheus ] } ] ;
      default_init_schedule_delay = 90. ;
      default_init_schedule_delay_after_startup = 120. } in
  let contents = PPP.to_string notify_config_ppp_ocaml default_conf in
  ensure_file_exists ~min_size:0 ~contents notif_conf_file

let load_config notif_conf_file =
  let notif_conf = ppp_of_file notify_config_ppp_ocaml notif_conf_file in
  check_conf_is_valid notif_conf ;
  notif_conf

let start conf notif_conf_file rb max_fpr =
  !logger.info "Starting notifier, using configuration file %s"
    notif_conf_file ;
  (* Check the configuration file is OK before waiting for the first
   * notification. Also, we will reload this ref, keeping the last
   * good version: *)
  let notif_conf = ref (load_config notif_conf_file) in
  restore_pendings conf ;
  (* Better check if we can draw a new alert_id before we need it: *)
  let _alert_id = next_alert_id conf () in
  Thread.create (
    restart_on_failure "send_notifications"
      (send_notifications max_fpr)) conf |> ignore ;
  let while_ () = !RamenProcesses.quit = None in
  RamenSerialization.read_notifs ~while_ rb
    (fun (worker, sent_time, event_time, notif_name,
          firing, certainty, parameters) ->
    let event_time = option_of_nullable event_time in
    let firing = option_of_nullable firing
    and parameters = Array.to_list parameters in
    let now = Unix.gettimeofday () in
    let notif =
      { worker ; sent_time ; rcvd_time = now ; event_time ;
        notif_name ; firing ; certainty ; parameters } in
    !logger.info "Received notification from %s: %S %s"
      worker notif_name (if firing = Some false then "ended" else "started") ;
    (* Each time we receive a notification we have to assign it to a team,
     * and then use the configured channel to notify it. We load the
     * configuration anew each time, relying on ppp_of_file caching
     * mechanism to cut down the work: *)
    (try notif_conf := load_config notif_conf_file
    with exn ->
      !logger.error "Cannot read notifier configuration file %s: %s"
        notif_conf_file (Printexc.to_string exn)) ;
    let team = Team.find_in_charge conf !notif_conf.teams notif_name in
    let action =
      match notif.firing with
      | None ->
          set_alight conf !notif_conf notif false
      | Some true ->
          set_alight conf !notif_conf notif true
      | Some false ->
          extinguish_pending !notif_conf notif.notif_name notif.event_time now
    in
    List.iter action team.Team.contacts)
