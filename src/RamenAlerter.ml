(*
 * To alleviate workers from the hassle to deal with external systems,
 * notifications are sent to Ramen alerter process via a ringbuffer.
 * Advantages are many:
 *
 * - Workers do not need so many libraries and are therefore smaller and
 *   easier to port to another language;
 *
 * - New notification mechanisms are easier to implement in a single
 *   location;
 *
 * - Workers are easier to distribute.
 *
 *
 * Glossary:
 *
 * A _notification_ is the message sent by a ramen worker to the alerter via
 * the "NOTIFY" keyword. Notifications must have a name that is used as an
 * identifier. Then, notification can signal either the start (firing true-ish)
 * or the end (firing false-ish) of an incident. Here, everything that's not
 * false-ish is true-ish.
 *
 * An _incident_ is what may be notified to an actual user. It's created as soon
 * as is received a firing notification which name was not already firing, and
 * ends because of a notification for that same name that is not firing, or
 * because no firing notification for that name has been received for longer
 * than a specified timeout. So, multiple notifications referring to the same
 * name will contribute to the same incident. That's _deduplication_.
 *
 * A _message_ is what is actually sent to the user. After a configurable
 * amount of time, a new incident is considered ripe for messaging. This delay
 * is there so that a flapping incident will not result in many messages. That's
 * _debouncing_. As many _dialogs_ as there are contacts will be created.
 *
 * A _dialog_ is created for each contact and live incident.
 * Any number of _messages_ will be sent and any number of acks may be
 * received per dialog.
 * The exact nature of a message depends on user configuration: it can be an
 * email, a pushed mobile phone message, an SNMP trap...
 *
 * An _outage_ is a set of incident. By default, every incident also creates its
 * own outage, but users can manually group incidents together to document that
 * a group of incidents refers to the same root cause.
 *
 * A _task_ is an action to progress along an incident workflow (typically, a
 * message has to be sent or to be timed out). Basically we have a heap of such
 * tasks with an associated timestamp and execute them all in order, which most
 * often than not yield to the task to be rescheduled with maybe a new
 * delivery_status. Most of these processing steps are logged in the incident's
 * _journal_.
 *)
open Batteries
open Stdint

open RamenConsts
open RamenHelpersNoLog
open RamenHelpers
open RamenLog
open RamenSync
open RamenSyncHelpers
module C = RamenConf
module Default = RamenConstsDefault
module Metric = RamenConstsMetric
module N = RamenName
module Files = RamenFiles
module Paths = RamenPaths
module StringExpansion = RamenStringExpansion
module VA = RamenSync.Value.Alerting
module VN = VA.Notification
module ZMQClient = RamenSyncZMQClient

(* Used to know if we must use normal schedule delay or schedule delay
 * after startup: *)
let startup_time = ref (Unix.gettimeofday ())

let timeout_idle_kafka_producers = ref Default.timeout_idle_kafka_producers

let debounce_delay = ref Default.debounce_delay

let max_incident_age = ref Default.max_incident_age

let for_test = ref false

let reschedule_clock = ref Default.reschedule_clock

(* We keep some info about the last [max_last_incidents_kept] incidents
 * created in [last_incidents] in order to fight false positives.
 * Not in the confserver though, although this could be inferred somewhat from
 * every incidents log: *)
let max_last_incidents_kept = ref Default.max_last_incidents_kept

open Binocle

(* Number of notifications of each types: *)

let stats_count =
  Files.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.messages_count
      "Number of messages sent, per channel.")

let stats_send_fails =
  Files.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.messages_send_fails
      "Number of messages that could not be sent due to error.")

let stats_team_fallbacks =
  Files.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.team_fallbacks
      "Number of times the default team was selected because the \
       configuration was not specific enough")

let stats_messages_cancelled =
  Files.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.messages_cancelled
      "Number of notifications not sent, per reason")

let stats_opened_incidents =
  IntGauge.make Metric.Names.opened_incidents
    "Number of opened incidents"

let stats_opened_dialogs =
  IntGauge.make Metric.Names.opened_dialogs
    "Number of opened dialogs"

(* Helpers to get configuration from the configuration tree *)

let get_key session k =
  let clt = option_get "get_key" __LOC__ session.ZMQClient.clt in
  (Client.find clt k).value

let incident_key incident_id k =
  Key.(Incidents (incident_id, k))

let dialog_key incident_id dialog_id k =
  Key.(Incidents (incident_id, Dialogs (dialog_id, k)))

let get_incident_key session incident_id k =
  get_key session (incident_key incident_id k)

let get_dialog_key session incident_id dialog_id k =
  get_key session (dialog_key incident_id dialog_id k)

let get_float_dialog_key session incident_id dialog_id k =
  let k = dialog_key incident_id dialog_id k in
  match get_key session k with
  | Value.RamenValue (VFloat t) -> t
  | v -> invalid_sync_type k v "a float"

let get_int_dialog_key session incident_id dialog_id k =
  let k = dialog_key incident_id dialog_id k in
  let v = get_key session k in
  match Value.to_int v with
  | Some n ->
      n
  | None ->
      err_sync_type k v "an integer" ;
      0

let set_key session k v =
  ZMQClient.send_cmd session ~eager:true ~echo:false (SetKey (k, v))

let set_dialog_key session incident_id dialog_id k v =
  let k = dialog_key incident_id dialog_id k in
  set_key session k v

let set_incident_key session incident_id k v =
  let k = incident_key incident_id k in
  set_key session k v

let log session incident_id time event =
  let r = Uint32.of_int (Random.bits ()) in
  let k = incident_key incident_id (Journal (time, r)) in
  let v = Value.IncidentLog event in
  set_key session k v

(* Retrieve the team name in charge of a given notification name (ie which name
 * is the longest possible prefix of the notification name). *)
let find_in_charge conf session name =
  let prefix = "alerting/teams/" in
  let def_team = ref None
  and best_team = ref None in
  let clt = option_get "find_in_charge" __LOC__ session.ZMQClient.clt in
  Client.iter clt ~prefix (fun k _hv ->
    match k with
    | Teams { name = t ; _ } ->
        if t = default_team_name ||
           !def_team = None then def_team := Some t ;
        (* Consider any team name if it prefixes the alert name, the longer the
         * better. *)
        let len = string_longest_prefix name (t :> string) in
        if len >= String.length (t :> string) &&
           match !best_team with
           | None -> true
           | Some (_, best_len) -> len > best_len
        then
          best_team := Some (t , len)
    | _ ->
        ()) ;
  match !best_team with
  | Some (best, _) ->
      !logger.info "Assigned to team %a" N.team_print best ;
      best
  | None ->
      IntCounter.inc (stats_team_fallbacks conf.C.persist_dir) ;
      (match !def_team with
      | None ->
          failwith "No teams configured, dropping notification!"
      | Some def ->
          !logger.warning "No team name found in notification %S, \
                           assigning to default team (%a)."
            name N.team_print def ;
          def
      )

(* Return the closest we have from the event time: *)
let notif_time notif =
  notif.VN.event_time |? notif.sent_time

type dialog =
  { (* The incident this task is about: *)
    incident : string ;
    (* When we plan to have another look at this task: *)
    mutable schedule_time : float ;
    (* When we plan to send the message: *)
    mutable send_time : float ;
    (* The current delivery status (see below): *)
    mutable status : VA.DeliveryStatus.t }

(* Set of all pending incidents identifiers indexed by notification name, so
 * we can quickly retrieve opened incidents: *)
module PendingMap = Map.String

let heap_pending_cmp (t1, _, _) (t2, _, _) =
  Float.compare t1 t2

type pendings =
  { (* The set of all incidents indexed by notification name: *)
    mutable incidents : string PendingMap.t ;
    (* The set of all dialogs as a heap ordered by schedule_time (Cf.
     * [heap_pending_cmp]).
     * All messages that have to be sent or acknowledged are in there: *)
    mutable dialogs :
      (float * string (*incident*) * string (*dialog*)) RamenHeap.t ;
    (* Set of the timestamp * certainties of the last max_last_incidents_kept
     * incidents that have been created. This is not persisted to disk.
     * Newest creations at the front: *)
    mutable last_incidents : (float * float) Deque.t }

let pendings =
  { incidents = PendingMap.empty ;
    dialogs = RamenHeap.empty ;
    last_incidents = Deque.empty }

(* From time to time, update stats_opened_incidents/dialogs: *)
let update_stats () =
  let num_incidents = PendingMap.cardinal pendings.incidents in
  IntGauge.set stats_opened_incidents num_incidents ;
  let num_dialogs = RamenHeap.length pendings.dialogs in
  IntGauge.set stats_opened_dialogs num_dialogs

let set_status session incident_id dialog_id prev_status new_status reason =
  !logger.info "incident %s, dialog %s, status change: %a -> %a (%s)"
    incident_id dialog_id
    VA.DeliveryStatus.print prev_status
    VA.DeliveryStatus.print new_status
    reason ;
  set_dialog_key session incident_id dialog_id Key.DeliveryStatus
                 (Value.DeliveryStatus new_status)

let new_incident_id =
  let seq = ref ~-1 in
  fun () ->
    if !for_test then (
      incr seq ;
      string_of_int !seq
    ) else (
      Uuidm.v4_gen (Random.State.make_self_init ()) () |>
      Uuidm.to_string
    )

let debounce_delay_for = function
  | None -> !debounce_delay
  | Some notif ->
      if notif.VN.debounce >= 0. then notif.debounce
      else !debounce_delay

let initial_sent_schedule session incident_id dialog_id now t =
  pendings.dialogs <-
    RamenHeap.add heap_pending_cmp (t, incident_id, dialog_id) pendings.dialogs ;
  set_dialog_key session incident_id dialog_id NextScheduled
                 (Value.RamenValue (VFloat t)) ;
  set_dialog_key session incident_id dialog_id NextSend
                 (Value.RamenValue (VFloat t)) ;
  log session incident_id now (NewNotification StartEscalation)

(* TODO: Also store [now] in the incident *)
let create_new_incident conf session notif _now =
  let clt = option_get "create_new_incident" __LOC__ session.ZMQClient.clt in
  let incident_id = new_incident_id () in
  !logger.info "Creating new incident %s for notification %S"
    incident_id notif.VN.name ;
  let set = set_incident_key session incident_id in
  let team_name =
    find_in_charge conf session notif.VN.name in
  set Key.FirstStartNotif (Value.Notification notif) ;
  set Key.LastStateChangeNotif (Value.Notification notif) ;
  set Key.Team (Value.RamenValue (VString (team_name :> string))) ;
  pendings.incidents <- PendingMap.add notif.name incident_id pendings.incidents ;
  let contacts =
    let prefix = "alerting/teams/"^ (team_name :> string) ^"/contacts/" in
    Client.fold clt ~prefix (fun k hv lst ->
      match k, hv.Client.value with
      | Key.Teams { info = Contacts dialog_id ; _ }, Value.AlertingContact _ ->
          dialog_id :: lst
      | _ ->
          lst
    ) [] in
  List.iter (fun dialog_id ->
    set (Key.Dialogs (dialog_id, DeliveryStatus))
        (Value.DeliveryStatus VA.DeliveryStatus.StartToBeSent)
  ) contacts ;
  incident_id

let fold_dialog session incident_id f u =
  let clt = option_get "fold_dialog" __LOC__ session.ZMQClient.clt in
  let prefix =
    "alerting/incidents/"^ incident_id ^"/dialogs/" in
  Client.fold clt ~prefix (fun k hv u ->
    match k, hv.Client.value with
    | Key.Incidents (_, Dialogs (dialog_id, DeliveryStatus)),
      Value.DeliveryStatus status ->
        f u dialog_id status
    | _ ->
        u
  ) u

let update_dialogs session notif now incident_id =
  let reason = "received notification of firing" in
  fold_dialog session incident_id (fun first dialog_id status ->
    let save_start_notif () =
      if first then (
        set_incident_key session incident_id LastStartNotif
                         (Value.Notification notif) ;
        set_incident_key session incident_id LastStateChangeNotif
                         (Value.Notification notif))
    in
    let schedule_time = now +. debounce_delay_for (Some notif) in
    (match status with
    | StartToBeSent ->
        let numDeliveryAttempts =
          try get_int_dialog_key session incident_id dialog_id NumDeliveryAttempts
          with Not_found -> 0 in
        if numDeliveryAttempts = 0 then
          initial_sent_schedule session incident_id dialog_id now schedule_time
        else
          log session incident_id now (NewNotification Duplicate)
    | StartToBeSentThenStopped | StopSent ->
        set_status session incident_id dialog_id status StartToBeSent reason ;
        set_dialog_key session incident_id dialog_id NextSend
                       (Value.RamenValue (VFloat schedule_time)) ;
        set_dialog_key session incident_id dialog_id NumDeliveryAttempts
                       (Value.RamenValue (VU8 Uint8.zero)) ;
        save_start_notif () ;
        log session incident_id now (NewNotification Duplicate)
    | StopToBeSent ->
        set_status session incident_id dialog_id status StartAcked reason ;
        save_start_notif () ;
        log session incident_id now (NewNotification Duplicate)
    | StartAcked | StartSent ->
        log session incident_id now (NewNotification Duplicate)) ;
    false
  ) true |> ignore

let pass_fpr max_fpr last_incidents certainty =
  if certainty >= 1. then true else
  let certainty = cap ~min:0. ~max:1. certainty in
  match Deque.rear last_incidents with
  | None ->
      !logger.debug "Max FPR test: pass due to first incident ever created." ;
      true
  | Some (_, (oldest, _)) ->
      (* Since alerts are processed by batches we do not use the [now] of the
       * caller, that might be the same for several alerts. Instead we ask
       * again for the current time: *)
      let now = Unix.gettimeofday () in
      let dt = now -. oldest in
      (* Max tolerable number of false positives in [last_incidents]: *)
      let max_fp = Float.ceil (dt *. max_fpr) |> int_of_float in
      (* Compute the probability that we had more than max_fp fp already. *)
      if max_fp < 1 (* bogus dt *) then (
        !logger.info "Max FPR test: bogus DT %f-%f=%f" now oldest dt ;
        false
      ) else if max_fp > 1 + Deque.size last_incidents then (
        !logger.debug "Max FPR test: Haven't created enough incidents yet (%d)
                       to have more false positives than %d"
          (Deque.size last_incidents) max_fp ;
        true
      ) else (
        (* Actually, we are going to compute the probability that we sent
         * exactly 0 junk notif, exactly 1, etc up to max_fp, and then
         * take 1 - that probability. That will be the probability that
         * we have sent more than max_fp if we sent this new alert.
         * Proba(n junk after m messages) =
         *     Proba((n-1) junk after (m-1) messages) * Proba(m is junk)
         *   + Proba(n junk after (m-1) messages) * (1 - Proba(m is junk))
         * Initially, before having sent any messages, the probability to
         * have sent 0 junk messages is 1 and the probability to have sent
         * more is 0: *)
        let p_junks = Array.init (max_fp + 1) (fun i ->
          if i = 0 then 1. else 0.) in
        (* For each notification sent, update the probabilities to have sent N
         * false positives: *)
        let send certainty =
          let certainty = cap ~min:0. ~max:1. certainty in
          let p_junk = 1. -. certainty in
          (* Probability to have sent N = probability to have sent N and not
           * send another one + probability to have sent N-1 and send a new
           * one: *)
          for i = Array.length p_junks - 1 downto 0 do
            p_junks.(i) <- p_junks.(i) *. certainty +.
              (if i > 0 then p_junks.(i-1) *. p_junk else 0.)
          done in
        (* Not in chronological order but does not change the result: *)
        Deque.iter (fun (_, certainty) -> send certainty) last_incidents ;
        (* And then we also suppose we send that new one: *)
        send certainty ;
        (* The probability to have sent less or exactly max_fp is thus: *)
        let p_less_eq = Array.fold_left (+.) 0. p_junks in
        !logger.debug "After having created %a and then (%f, %f), the \
                       probability to have exactly 0..N: %a"
          (Deque.print (pair_print Float.print Float.print)) last_incidents
          now certainty
          (Array.print Float.print) p_junks ;
        (* So that the probability to have sent more than max_fp is: *)
        let p_more = 1. -. p_less_eq in
        (if p_more > 0.5 then !logger.info else !logger.debug)
          "Max FPR test: %d incidents have been created in the last %a, \
           probability to have more than %d false positive: %f."
          (Deque.size last_incidents)
          RamenParsing.print_duration dt
          max_fp p_more ;
        p_more <= 0.5
      )

(*$R pass_fpr
  let oldify dt dq = Deque.map (fun (t, c) -> t -. dt, c) dq in
  (* Max FPR: 1 every 5 messages: *)
  let max_fpr = 1. /. 5. in
  (* From a past where only certain alerts were sent: *)
  let now = Unix.gettimeofday () in
  let last_incidents =
    Deque.of_list [ now -. 1., 1. ;
                    now -. 2., 1. ;
                    now -. 3., 1. ;
                    now -. 4., 1. ] in
  (* Any new message should pass, regardless of how bad: *)
  assert_bool "any test pass after all certain"
    (pass_fpr max_fpr last_incidents 0.) ;
  assert_bool "any test pass after all certain (2)"
    (pass_fpr max_fpr last_incidents 0.5) ;
  assert_bool "any test pass after all certain (3)"
    (pass_fpr max_fpr last_incidents 1.) ;
  let now = Unix.gettimeofday () in
  let last_incidents =
    Deque.of_list [ now -. 1., 0. ;
                    now -. 2., 0. ;
                    now -. 3., 0. ;
                    now -. 4., 0. ] in
  assert_bool "enough junk" (not (pass_fpr max_fpr last_incidents 0.)) ;
  assert_bool "one good is ok " (pass_fpr max_fpr last_incidents 1.) ;

  (* Add one junk alert and only certain alerts can go: *)
  let now = Unix.gettimeofday () in
  let last_incidents = Deque.cons (now, 0.) last_incidents in
  assert_bool "none shall pass" (not (pass_fpr max_fpr last_incidents 0.)) ;
  assert_bool "none shall pass (2)" (not (pass_fpr max_fpr last_incidents 0.9)) ;
  assert_bool "but that one" (pass_fpr max_fpr last_incidents 1.) ;

  (* Wait so that the oldest bad is 20+s old and a good enough alert can go: *)
  let last_incidents = oldify 18. last_incidents in
  assert_bool "unacceptably bad" (not (pass_fpr max_fpr last_incidents 0.)) ;
  assert_bool "acceptable" (pass_fpr max_fpr last_incidents 0.9) ;
*)

(* Remember that an incident have been created with that certainty.
 * Used to compute FRP. *)
let save_last_incidents certainty now =
  pendings.last_incidents <-
    Deque.cons (now, certainty) pendings.last_incidents ;
  if Deque.size pendings.last_incidents > !max_last_incidents_kept then
    pendings.last_incidents <- fst (Option.get (Deque.rear pendings.last_incidents))

(* When we receive a firing notification we must first check if we have a pending
 * incident for that notification already, and if so update it and reschedule it.
 * Otherwise create a new one with as many dialogs as the assigned team has
 * contacts. *)
let set_alight conf session notif now max_fpr =
  match PendingMap.find notif.VN.name pendings.incidents with
  | exception Not_found ->
      (* Test for FPR before creating a new incident: *)
      if notif.test || pass_fpr max_fpr pendings.last_incidents notif.certainty
      then (
        let incident_id = create_new_incident conf session notif now in
        if not notif.test then save_last_incidents notif.certainty now ;
        update_dialogs session notif now incident_id
      )
  | incident_id ->
      update_dialogs session notif now incident_id

let stop_pending session incident_id dialog_id status now notif_opt reason =
  match status with
  | VA.DeliveryStatus.StartToBeSent ->
      set_status session incident_id dialog_id status StartToBeSentThenStopped
                 reason
  | StartSent (* don't care about the ack any more *)
  | StartAcked ->
      set_status session incident_id dialog_id status StopToBeSent reason ;
      set_dialog_key session incident_id dialog_id NumDeliveryAttempts
                     (Value.RamenValue (VU8 Uint8.zero)) ;
      (* reschedule *)
      let t = now +. jitter (debounce_delay_for notif_opt) in
      set_dialog_key session incident_id dialog_id NextScheduled
                     (Value.RamenValue (VFloat t))
  | StopToBeSent | StopSent | StartToBeSentThenStopped ->
      ()

(* When we receive a notification that an alert is no longer firing, we must
 * cancel pending delivery or send the end of alert notification.
 * We do not do this from here though, but merely update the status and
 * let the scheduler do the rest.
 * Can raise Not_found. *)
let extinguish_pending session notif now =
  match PendingMap.find notif.VN.name pendings.incidents with
  | exception Not_found ->
      !logger.warning "Cannot find any ongoing pending incident for %s"
        notif.name
  | incident_id ->
      set_incident_key session incident_id LastStopNotif
                       (Value.Notification notif) ;
      let save_state_change state_changed =
        if not state_changed then
          set_incident_key session incident_id LastStateChangeNotif
                           (Value.Notification notif) ;
        true
      in
      log session incident_id now (Stop Notification) ;
      fold_dialog session incident_id (fun state_changed dialog_id status ->
        stop_pending session incident_id dialog_id status now (Some notif)
                     "no longer firing" ;
        match status with
        | StartToBeSent | StartSent | StartAcked ->
            save_state_change state_changed
        | _ ->
            state_changed
      ) false |> ignore

(*
 * Dialog scheduling, ie. messaging the external world and handling acks.
 *
 * TODO: time this thread and add this to alerter instrumentation.
 *)

let max_exec = Atomic.Counter.make 5 (* no more than 5 simultaneous execs *)

let execute_cmd conf cmd =
  IntCounter.inc ~labels:["via", "execute"] (stats_count conf.C.persist_dir) ;
  let cmd_name = "alerter exec" in
  match run_coprocess ~max_count:max_exec cmd_name cmd with
  | None ->
      IntCounter.inc (stats_send_fails conf.C.persist_dir) ;
      Printf.sprintf "Cannot execute %S" cmd
      |> failwith
  | Some (Unix.WEXITED 0) -> ()
  | Some status ->
      IntCounter.inc (stats_send_fails conf.C.persist_dir) ;
      Printf.sprintf "Cannot execute %S: %s"
        cmd (string_of_process_status status) |>
      failwith

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
    IntCounter.inc (stats_send_fails conf.C.persist_dir) ;
    Printf.sprintf "Cannot %S into sqlite DB %S: %s"
      q file (Rc.to_string err) |>
    failwith in
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

let kafka_publish =
  (* We keep kafka producers in a hash for some time *)
  let producers = Hashtbl.create 10 in
  fun conf options topic partition text ->
    let del_kafka_prod prod prod_topic =
      Kafka.destroy_topic prod_topic ;
      Kafka.destroy_handler prod in
    let get_or_create_kafka_producer () =
      let k = options, topic in
      let now = Unix.time () in
      let make_producer () =
        let topic_options, prod_options =
          List.partition (fun (n, _) ->
            String.starts_with n kafka_topic_option_prefix
          ) options in
        let prod = Kafka.new_producer prod_options in
        let prod_topic = Kafka.new_topic prod topic topic_options in
        prod, prod_topic, ref now in
      let _, prod_topic, last_used =
        hashtbl_find_option_delayed make_producer producers k in
      last_used := now ;
      (* Timeout producers *)
      Hashtbl.filter_inplace (fun (prod, prod_topic, last_used) ->
        if now -. !last_used >= !timeout_idle_kafka_producers then (
          del_kafka_prod prod prod_topic ;
          false
        ) else true
      ) producers ;
      prod_topic
    and kill_kafka_producer () =
      let k = options, topic in
      Hashtbl.modify_opt k (function
        | Some (prod, prod_topic, _) ->
            del_kafka_prod prod prod_topic ;
            None
        | None ->
            None
      ) producers
    in
    IntCounter.inc ~labels:["via", "kafka"] (stats_count conf.C.persist_dir) ;
    let prod = get_or_create_kafka_producer () in
    try
      Kafka.produce prod partition text
    with e ->
      kill_kafka_producer () ;
      Printf.sprintf "Cannot send alert via Kafka (topic %S): %s"
        topic (Printexc.to_string e) |>
      failwith

(* When a notification is delivered to the user (or we abandon it).
 * Notice that "is delivered" depends on the channel: some may have
 * delivery acknowledgment while some may not. *)
let ack session incident_id dialog_id name now =
  log session incident_id now (Ack dialog_id) ;
  let clt = option_get "ack" __LOC__ session.ZMQClient.clt in
  let k = Key.Incidents (incident_id, Dialogs (dialog_id, DeliveryStatus)) in
  match (Client.find clt k).Client.value with
  | exception Not_found ->
      !logger.warning
        "Received an Ack for incident %s, dialog %s which does not exist, ignoring."
        incident_id dialog_id ;
  | Value.DeliveryStatus (StartSent as old_status) ->
      !logger.debug "Alert %S acknowledged" name ;
      set_status session incident_id dialog_id old_status StartAcked "acked" ;
  | Value.DeliveryStatus status ->
      !logger.debug "Ignoring an ACK for dialog in status %a"
        VA.DeliveryStatus.print status
  | v ->
      err_sync_type k v "a DeliveryStatus"

(* Deliver the message (or raise).
 * An acknowledgment is supposed to be received via another channel. *)
let contact_via conf session incident_id dialog_id now status contact attempts =
  log session incident_id now (Outcry (dialog_id, Uint32.of_int attempts)) ;
  (* Fetch all the info we can possibly need: *)
  let first_start_notif =
    let k = incident_key incident_id FirstStartNotif in
    match get_key session k with
    | Value.Notification n -> n
    | v -> invalid_sync_type k v "a Notification"
  and last_stop_notif_opt =
    let k = incident_key incident_id LastStopNotif in
    match get_key session k with
    | exception Not_found -> None
    | Value.Notification n -> Some n
    | v -> err_sync_type k v "a Notification" ; None
  and last_state_change =
    let k = incident_key incident_id LastStateChangeNotif in
    match get_key session k with
    | Value.Notification n -> n
    | v -> invalid_sync_type k v "a Notification"
  and first_delivery_attempt =
    let k = dialog_key incident_id dialog_id FirstDeliveryAttempt in
    match get_key session k with
    | Value.RamenValue (VFloat t) -> t
    | v -> invalid_sync_type k v "a float"
  and last_delivery_attempt =
    let k = dialog_key incident_id dialog_id LastDeliveryAttempt in
    match get_key session k with
    | Value.RamenValue (VFloat t) -> t
    | v -> invalid_sync_type k v "a float"
  and firing =
    match status with
    | VA.DeliveryStatus.StartToBeSent -> true
    | StopToBeSent -> false
    | _ -> assert false in
  let dict =
    [ "name", first_start_notif.name ;
      "incident_id", incident_id ;
      "start", nice_string_of_float (notif_time first_start_notif) ;
      "now", nice_string_of_float now ;
      "first_sent", nice_string_of_float first_delivery_attempt ;
      "last_sent", nice_string_of_float last_delivery_attempt ;
      "site", (first_start_notif.site :> string) ;
      "worker", (first_start_notif.worker :> string) ;
      "test", string_of_bool first_start_notif.test ;
      "firing", string_of_bool firing ;
      "last_state_change", nice_string_of_float (notif_time last_state_change) ;
      "certainty", nice_string_of_float first_start_notif.certainty ;
      (* For convenience, before we can call actual functions
       * from the templates: *)
      "hostname", (getenv ~def:"?HOSTNAME?" "HOSTNAME") ] in
  (* Add "stop" if we have it (or let it be NULL) *)
  let dict =
    match last_stop_notif_opt with
    | Some notif ->
        let ts = notif_time notif in
        ("stop", nice_string_of_float ts) :: dict
    | None -> dict in
  (* Allow parameters to overwrite built-ins: *)
  let dict =
    List.rev_append
      (if firing then
        first_start_notif.parameters
      else match last_stop_notif_opt with
        (* Note that there already exist a field "timeout" which is the
         * timeout duration, set by the worker. *)
        | Some n -> ("timed-out", "false") :: n.parameters
        | None -> ("timed-out", "true") :: first_start_notif.parameters)
    dict in
  !logger.debug "Expand config with dict: %a"
    (List.print (pair_print String.print String.print)) dict ;
  let exp ?n = StringExpansion.subst_dict dict ?null:n in
  let open VA.Contact in
  match contact.via with
  | Ignore ->
      ()
  | Exec cmd ->
      execute_cmd conf (exp cmd)
  | SysLog str ->
      log_str conf (exp str)
  | Sqlite { file ; insert ; create } ->
      let ins = exp ~n:"NULL" insert in
      sqllite_insert conf (exp file) ins create
  | Kafka { options ; topic ; partition ; text } ->
      let text = exp ~n:"null" text in
      kafka_publish conf options topic (Uint16.to_int partition) text

let contact_of_incident session incident_id dialog_id =
  let team_name =
    let k = incident_key incident_id Team in
    match get_key session k with
    | Value.RamenValue (VString n) -> N.team n
    | v -> invalid_sync_type k v "a string" in
  let k = Key.Teams { name = team_name ; info = Contacts dialog_id } in
  match get_key session k with
  | Value.AlertingContact c -> c
  | v -> invalid_sync_type k v "a contact"

let do_notify conf session incident_id dialog_id now old_status start_notif =
  let clt = option_get "do_notify" __LOC__ session.ZMQClient.clt in
  let contact = contact_of_incident session incident_id dialog_id in
  let attempts =
    try get_int_dialog_key session incident_id dialog_id NumDeliveryAttempts
    with Not_found -> 0 in
  set_dialog_key session incident_id dialog_id NumDeliveryAttempts
                 (Value.RamenValue (VU32 (Uint32.of_int (attempts + 1)))) ;
  let k = dialog_key incident_id dialog_id FirstDeliveryAttempt in
  if not (Client.mem clt k) then
    set_key session k (Value.of_float now) ;
  set_dialog_key session incident_id dialog_id LastDeliveryAttempt
                 (Value.of_float now) ;
  contact_via conf session incident_id dialog_id now old_status contact
              attempts ;
  let new_status =
    let open VA.DeliveryStatus in
    match old_status with
    | StartToBeSent -> StartSent
    | StopToBeSent -> StopSent
    | _ -> assert false in
  set_status session incident_id dialog_id old_status new_status
             "successfully sent message" ;
  (* if timeout > 0 then an acknowledgment is supposed to be received via
   * another async channel and until then the message will be repeated at
   * regular intervals. If timeout is <=0 though, it means no acks is to be
   * expected and no repetition will occur. In that case it is acknowledged
   * immediately: *)
  if contact.timeout <= 0. then (
    !logger.debug "No ack to be expected so acking now." ;
    ack session incident_id dialog_id start_notif.VN.name now)

(* Returns true if there may still be notifications to be sent: *)
let send_next conf session now =
  let reschedule_min time =
    let (_, incident_id, dialog_id), dialogs =
      RamenHeap.pop_min heap_pending_cmp pendings.dialogs in
    !logger.debug "Rescheduling incident %s for %a"
      incident_id
      print_as_date time ;
    set_dialog_key session incident_id dialog_id Key.NextScheduled
                           (Value.RamenValue (VFloat time)) ;
    pendings.dialogs <-
      RamenHeap.add heap_pending_cmp (time, incident_id, dialog_id) dialogs
  and del_min_dialog () =
    pendings.dialogs <- RamenHeap.del_min heap_pending_cmp pendings.dialogs ;
  in
  let del_min notif_name =
    del_min_dialog () ;
    pendings.incidents <- PendingMap.remove notif_name pendings.incidents
  in
  let send_message incident_id dialog_id old_status start_notif =
    do_notify conf session incident_id dialog_id now old_status start_notif ;
    (* Keep rescheduling until stopped (or timed out): *)
    reschedule_min (now +. jitter !reschedule_clock)
  in
  (* When we give up sending a notification. *)
  let cancel incident_id dialog_id notif_name reason_msg reason_code =
    !logger.info "Cancelling dialog %s, %s for notification %S: %s"
      incident_id dialog_id notif_name reason_msg ;
    log session incident_id now (Cancel dialog_id) ;
    let labels = [ "reason", reason_code ] in
    IntCounter.inc ~labels (stats_messages_cancelled conf.C.persist_dir) ;
    del_min notif_name
  in
  (* Some incidents have no "recovery" condition and just stop firing
   * after a while: *)
  let timeout_pending incident_id dialog_id status =
    log session incident_id now (Stop (Timeout dialog_id)) ;
    stop_pending session incident_id dialog_id status now None "timed out"
  in
  match RamenHeap.min pendings.dialogs with
  | exception Not_found ->
      false
  | schedule_time, incident_id, dialog_id ->
      if schedule_time > now then false else
      (* Get the incident name *)
      let k = incident_key incident_id FirstStartNotif in
      (match get_key session k with
      | exception Not_found ->
          !logger.error
            "Cannot find FirstStartNotif for incident %s, deleting the dialog"
            incident_id ;
          del_min_dialog () ;
          true
      | Value.Notification start_notif ->
          if now -. start_notif.sent_time > !max_incident_age then (
            !logger.warning
              "Incident %s (event time = %a) is older than %a, cancelling!"
              incident_id
              print_as_date start_notif.sent_time
              print_as_duration !max_incident_age ;
            cancel incident_id dialog_id start_notif.name
                   "incident is too old" "too old"
          ) else (
            let k = dialog_key incident_id dialog_id DeliveryStatus in
            (match get_key session k with
            | exception Not_found ->
                !logger.error
                  "Cannot find delivery status for %s, %s, deleting the dialog"
                  incident_id dialog_id ;
                del_min_dialog ()
            | Value.DeliveryStatus status ->
                let k = dialog_key incident_id dialog_id NextSend in
                (match get_key session k with
                | exception Not_found ->
                    !logger.error
                      "Cannot find next_send for %s, %s, deleting the dialog"
                      incident_id dialog_id ;
                    del_min_dialog ()
                | Value.RamenValue (VFloat send_time) ->
                    !logger.debug
                      "Current dialog is about %s%s, %a, scheduled for %s"
                      start_notif.VN.name
                      (if start_notif.test then " (TEST)" else "")
                      VA.DeliveryStatus.print status
                      (string_of_time schedule_time) ;
                    (match status with
                    | StartToBeSent | StopToBeSent ->
                        if send_time <= now then (
                          try
                            send_message incident_id dialog_id status start_notif
                          with exn ->
                            let err_msg = Printexc.to_string exn in
                            cancel incident_id dialog_id start_notif.name err_msg
                                   "other" ;
                        ) else ( (* send_time > now *)
                          reschedule_min send_time
                        )
                    | StartToBeSentThenStopped | StopSent ->
                        (* No need to reschedule this *)
                        del_min start_notif.name
                    | StartSent -> (* Still missing the Ack, resend? *)
                        let contact =
                          contact_of_incident session incident_id dialog_id in
                        if contact.VA.Contact.timeout <= 0. then (
                          !logger.warning
                            "Contact has no timeout but incident %s is not \
                             acked, acking now"
                            incident_id ;
                          ack session incident_id dialog_id start_notif.name now
                        ) else (
                          let last_delivery_attempt =
                            get_float_dialog_key session incident_id dialog_id
                                           LastDeliveryAttempt in
                          if now -. last_delivery_attempt > contact.timeout then (
                            (* Resend the Start *)
                            !logger.info "%s, %s: Waited ack for too long"
                              incident_id dialog_id ;
                            set_status session incident_id dialog_id status
                                       StartToBeSent "still no ack" ;
                            set_dialog_key session incident_id dialog_id NextSend
                                           (Value.RamenValue (VFloat now)) ;
                            reschedule_min now
                          ) else (
                            reschedule_min (last_delivery_attempt +. contact.timeout)
                          )
                        )
                    | StartAcked -> (* Maybe timeout this alert? *)
                        if start_notif.test then (
                          (* Test alerts have no recovery or timeout and their lifespan
                           * does not extend beyond the ack. *)
                          pendings.incidents <-
                            PendingMap.remove start_notif.name pendings.incidents
                        ) else if start_notif.timeout > 0. &&
                                  now >= notif_time start_notif +. start_notif.timeout then (
                          timeout_pending incident_id dialog_id status ;
                          reschedule_min send_time
                        ) else (
                          (* Keep rescheduling as we may time it out or we may
                           * receive an ack or an end notification: *)
                          reschedule_min (now +. jitter !reschedule_clock)
                        )
                    )
                | v ->
                    err_sync_type k v "a float" ;
                    del_min_dialog ())
            | v ->
                err_sync_type k v "a DeliveryStatus" ;
                del_min_dialog ())
          ) ;
          true
      | v ->
          err_sync_type k v "a Notification" ;
          true)

(* Avoid creating several instances of watchdogs even when thread crashes
 * and is restarted: *)
let watchdog = ref None

(* Make sure we have a minimal default configuration of one team: *)
let ensure_minimal_conf session =
  (* Default content in case the configuration file is absent: *)
  let clt = option_get "ensure_minimal_conf" __LOC__ session.ZMQClient.clt in
  let prefix = "alerting/teams/" in
  if not (Client.exists clt ~prefix (fun k _hv ->
            match k with
            | Key.Teams { info = Contacts _ ; _ } -> true
            | _ -> false))
  then
    let k = Key.Teams { name = default_team_name ; info = Contacts "prometheus" }
    and v =
      Value.AlertingContact {
        via = VA.Contact.Exec "\
                curl \
                  --silent \
                  -X POST \
                  -H 'Content-Type: application/json' \
                  --data-raw '[{\"labels\":{\
                       \"alertname\":\"'${name|shell}'\",\
                       \"summary\":\"'${desc|shell}'\",\
                       \"severity\":\"critical\"}}]' \
                  'http://localhost:9093/api/v1/alerts'" ;
        timeout = 0. } in
    set_key session k v

let start conf max_fpr timeout_idle_kafka_producers_
          debounce_delay_ max_last_incidents_kept_ max_incident_age_
          for_test_ reschedule_clock_ =
  timeout_idle_kafka_producers := timeout_idle_kafka_producers_ ;
  debounce_delay := debounce_delay_ ;
  max_last_incidents_kept := max_last_incidents_kept_ ;
  max_incident_age := max_incident_age_ ;
  for_test := for_test_ ;
  reschedule_clock := reschedule_clock_ ;
  let topics = [ "alerting/*" ] in
  let while_ () = !RamenProcesses.quit = None in
  if !watchdog = None then
    (* More than 30s delay has been observed when trying to open an sqlite DB: *)
    let timeout = 60. in
    watchdog := Some (RamenWatchdog.make ~timeout "alerter" RamenProcesses.quit) ;
  let watchdog = Option.get !watchdog in
  let update_stats_rate_limited = rate_limit 1 in
  let sync_loop session =
    ensure_minimal_conf session ;
    RamenWatchdog.enable watchdog ;
    while while_ () do
      let now = Unix.gettimeofday () in
      while send_next conf session now do () done ;
      (* Avoid staying too long in process_in because of the watchdog: *)
      ZMQClient.process_in ~while_ ~max_count:100 session ;
      if not (update_stats_rate_limited now) then update_stats () ;
      RamenWatchdog.reset watchdog ;
    done in
  let on_set session k v _uid _mtime =
    let what = "Handling new notification" in
    log_and_ignore_exceptions ~what (fun () ->
      match k, v with
      | Key.Notifications, Value.Notification notif ->
          (* FIXME: if not synced, keep it for later: *)
          let now = Unix.gettimeofday () in
          if notif.VN.firing then (
            !logger.info
              "Received %snotification from %a:%a: %S started (%s certainty)"
              (if notif.VN.test then "TEST " else "")
              N.site_print notif.site N.fq_print notif.worker notif.name
              (nice_string_of_float notif.certainty) ;
            set_alight conf session notif now max_fpr
          ) else (
            !logger.info
              "Received %snotification from %a:%a: %S ended"
              (if notif.VN.test then "TEST " else "")
              N.site_print notif.site N.fq_print notif.worker notif.name ;
            extinguish_pending session notif now
          )
      | Key.Notifications, _ ->
          err_sync_type k v "a notification"
      | Key.(Incidents (incident_id, Dialogs (dialog_id, Ack))), _ ->
          (* start_notif is needed to ack: *)
          let k = incident_key incident_id FirstStartNotif in
          (match get_key session k with
          | exception Not_found ->
              !logger.warning "Invalid incident id %s has no FirstStartNotif"
                incident_id
          | Value.Notification start_notif ->
              let now = Unix.gettimeofday () in
              ack session incident_id dialog_id start_notif.name now
          | v ->
              invalid_sync_type k v "a Notification")
      | _ ->
          ()
    ) ()
  in
  let on_new session k v uid mtime _can_write _can_del _owner _expiry =
    (* Update incident/dialog indexes: *)
    match k, v with
    | Key.Incidents (incident_id, FirstStartNotif),
      Value.Notification notif ->
        !logger.debug "Restore pending incident %s for notification %s"
          incident_id notif.VN.name ;
        pendings.incidents <-
          PendingMap.add notif.name incident_id pendings.incidents
    | Key.Incidents (incident_id, Dialogs (dialog_id, NextScheduled)),
      Value.RamenValue (Raql_value.VFloat time) ->
        !logger.debug
          "Restore pending dialog %s for incident %s, next scheduled at %a"
          dialog_id incident_id print_as_date time ;
        pendings.dialogs <-
          RamenHeap.add heap_pending_cmp (time, incident_id, dialog_id)
                        pendings.dialogs
    | _ ->
        on_set session k v uid mtime
  in
  let on_del _session k v =
    match k, v with
    | Key.Incidents (incident_id, FirstStartNotif),
      Value.Notification notif ->
        !logger.debug "Delete pending incident %s for notification %s"
          incident_id notif.VN.name ;
        pendings.incidents <-
          PendingMap.remove notif.name pendings.incidents
    | _ ->
        (* No need to delete the dialogs, let them expire when scheduled *)
        ()
  in
  start_sync conf ~topics ~while_ ~recvtimeo:1. ~on_set ~on_new ~on_del sync_loop
