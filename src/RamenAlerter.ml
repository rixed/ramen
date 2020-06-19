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
open RamenNullable
open RamenSync
open RamenSyncHelpers
module C = RamenConf
module N = RamenName
module Files = RamenFiles
module Paths = RamenPaths
module StringExpansion = RamenStringExpansion
module VA = RamenSync.Value.Alerting
module ZMQClient = RamenSyncZMQClient

(* Used to know if we must use normal schedule delay or schedule delay
 * after startup: *)
let startup_time = ref (Unix.gettimeofday ())

let timeout_idle_kafka_producers = ref Default.timeout_idle_kafka_producers

let debounce_delay = ref Default.debounce_delay

let max_incident_age = ref Default.max_incident_age

(* We keep some info about the last [max_last_sent_kept] message sent in
 * [last_sent] in order to fight false positives.
 * Not in the confserver though, although this could be inferred to some extend
 * from incidents log: *)
let max_last_sent_kept = ref Default.max_last_sent_kept

let default_reschedule = ref Default.alerting_dialog_reschedule

open Binocle

(* Number of notifications of each types: *)

let stats_count =
  RamenWorkerStats.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.messages_count
      "Number of messages sent, per channel.")

let stats_send_fails =
  RamenWorkerStats.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.messages_send_fails
      "Number of messages that could not be sent due to error.")

let stats_team_fallbacks =
  RamenWorkerStats.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.team_fallbacks
      "Number of times the default team was selected because the \
      configuration was not specific enough")

let stats_messages_cancelled =
  RamenWorkerStats.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.messages_cancelled
      "Number of notifications not send, per reason")

(* Helpers to get configuration from the configuration tree *)

let get_key session k =
  (Client.find session.ZMQClient.clt k).value

let incident_key incident_id k =
  Key.(Incidents (incident_id, k))

let dialog_key incident_id dialog_id k =
  Key.(Incidents (incident_id, Dialogs (dialog_id, k)))

let get_incident_key session incident_id k =
  get_key session (incident_key incident_id k)

let get_dialog_key session incident_id dialog_id k =
  get_key session (dialog_key incident_id dialog_id k)

let get_int_dialog_key session incident_id dialog_id k =
  let k = dialog_key incident_id dialog_id k in
  let v = get_key session k in
  match Value.to_int v with
  | Some n ->
      n
  | None ->
      err_sync_type k v "an integer" ;
      0

let get_dialog_float_key session incident_id dialog_id k =
  let k = dialog_key incident_id dialog_id k in
  match get_key session k with
  | Value.RamenValue (VFloat v) ->
      v
  | v ->
      err_sync_type k v "a float" ;
      raise Not_found

let set_key session k v =
  ZMQClient.send_cmd session ~eager:true (SetKey (k, v))

let set_dialog_key session incident_id dialog_id k v =
  let k = dialog_key incident_id dialog_id k in
  set_key session k v

let set_incident_key session incident_id k v =
  let k = incident_key incident_id k in
  set_key session k v

let log session incident_id time event =
  let k = incident_key incident_id (Journal (time, Random.bits ())) in
  let v = Value.IncidentLog event in
  set_key session k v

(* Retrieve the team name in charge of a given notification name (ie which name
 * is the longest possible prefix of the notification name). *)
let find_in_charge conf session name =
  let prefix = "alerting/teams/"^ name in
  let def_team = ref None
  and best_team = ref None in
  Client.iter session.ZMQClient.clt ~prefix (fun k _hv ->
    match k with
    | Teams (t, _) ->
        if t = default_team_name ||
           !def_team = None then def_team := Some t ;
        if String.starts_with name (t :> string) &&
           (match !best_team with
           | None -> true
           | Some best -> N.length t > N.length best)
        then
          best_team := Some t
    | _ ->
        ()) ;
  match !best_team with
  | Some best -> best
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
  notif.VA.Notification.event_time |? notif.sent_time

(* Set of all pending incidents identifiers indexed by notification name, so
 * that live incidents can be quickly retrieved: *)
module PendingMap = Map.String

let heap_cmp (t1, _, _) (t2, _, _) =
  Float.compare t1 t2

type pendings =
  { (* The set of all live incidents indexed by notification name.
       "live" means that the incident will accept any new notification for
       its name as a continuation. Incidents are removed when their phase
       reach StopSent or StartToBeSentThenStopped and stays there for a little
       while. They are left in the confserver for even longer though
       (identified by UUID), until later removal at user convenience, to serve
       as historical records. *)
    mutable incidents : string PendingMap.t ;
    (* The heap of scheduled tasks ordered by schedule_time (Cf. [heap_cmp])
     * We schedule either whole incidents (for debouncing and sending recovery)
     * or individual dialogues (for repetitions) *)
    mutable schedule : (float * string * string option) RamenHeap.t ;
    (* Set of the timestamp * certainties of the last last_max_sent
     * notifications that have been sent (not persisted to disk), used
     * to compute false-positive rate (fpr): *)
    mutable last_sent : (float * float) Deque.t }

let pendings =
  { incidents = PendingMap.empty ;
    schedule = RamenHeap.empty ;
    last_sent = Deque.empty }

let get_phase session incident_id =
  let k = incident_key incident_id Phase in
  match get_key session k with
  | Value.IncidentPhase phase ->
      phase
  | v ->
      err_sync_type k v "an IncidentPhase" ;
      raise Not_found

let set_phase session incident_id new_phase reason =
  let do_set_phase () =
    set_incident_key session incident_id Key.Phase
                     (Value.IncidentPhase new_phase)
  in
  match get_phase session incident_id with
  | exception _ ->
      do_set_phase ()
  | prev_phase ->
      !logger.info "incident %s, phase change: %a -> %a (%s)"
        incident_id
        VA.Phase.print prev_phase
        VA.Phase.print new_phase
        reason ;
      if new_phase <> prev_phase then do_set_phase ()

let get_notif session incident_id k =
  let k = incident_key incident_id k in
  match get_key session k with
  | Value.Notification n -> n
  | v -> invalid_sync_type k v "a Notification"

let reschedule incident_id ?dialog_id time =
  !logger.debug "Scheduling incident_id %s%s for %a"
    incident_id
    (match dialog_id with None -> "" | Some id -> ", "^ id)
    print_as_date time ;
  pendings.schedule <-
    RamenHeap.add heap_cmp (time, incident_id, dialog_id) pendings.schedule

(* Scan the configuration tree after it's synchronized and build pending
 * incidents and schedule: *)
let load_pendings session =
  let prefix = "alerting/incidents/" in
  Client.iter session.ZMQClient.clt ~prefix (fun k hv ->
    match k, hv.value with
    (* Update incident/dialog indexes if still syncing.
     * Load only incidents which are not over yet: *)
    | Key.Incidents (incident_id, Phase),
      Value.IncidentPhase phase
      when phase <> VA.Phase.Finished ->
        log_and_ignore_exceptions ~what:"Loading pending incidents" (fun () ->
          let notif =
            get_notif session incident_id FirstStartNotif in
          !logger.debug "Restore pending incident %s for notification %s"
            incident_id notif.VA.Notification.name ;
          pendings.incidents <-
            PendingMap.add notif.name incident_id pendings.incidents ;
          (match phase with Debouncing (_, _, time) ->
            reschedule incident_id time
          | _ -> ())
        ) ()
    (* NextSend might be in the past or referring to message that must not be
     * sent any longer, but the scheduler will deal with it according to the
     * current phase. What's important is that all live incidents are scheduled
     * and that existing valid NextSend are preserved: *)
    | Key.Incidents (incident_id, Dialogs (dialog_id, NextSend)),
      Value.RamenValue (T.VFloat time) ->
        log_and_ignore_exceptions ~what:"Loading pending dialogues" (fun () ->
          match get_phase session incident_id with
          | exception Not_found ->
              !logger.warning "Ignoring incident %s with no phase" incident_id
          | phase when phase <> VA.Phase.Finished ->
              !logger.debug
                "Restore pending dialog %s for incident %s, next scheduled at %a"
                dialog_id incident_id print_as_date time ;
              reschedule incident_id ~dialog_id time
          | _ ->
              ()
        ) ()
    | _ ->
        ())

let new_incident_id () =
  Uuidm.v4_gen (Random.State.make_self_init ()) () |>
  Uuidm.to_string

let debounce_delay_for = function
  | None -> !debounce_delay
  | Some notif ->
      if notif.VA.Notification.debounce > 0. then notif.debounce
      else !debounce_delay

let fold_dialogs session incident_id f u =
  let prefix =
    "alerting/incidents/"^ incident_id ^"/dialogs/" in
  Client.fold session.ZMQClient.clt ~prefix (fun k hv u ->
    (* Call [f] only once per dialog: *)
    match k with
    | Key.Incidents (_, Dialogs (dialog_id, NumDeliveryAttempts)) ->
        let num_delivery_attempts = Value.to_int hv.value |? 0 in
        f u dialog_id num_delivery_attempts
    | _ ->
        u
  ) u

(* raises Not_found when team name is not set: *)
let fold_contacts session incident_id f u =
  let k = incident_key incident_id Key.Team in
  match get_key session k with
  | exception Not_found ->
      !logger.error "Cannot find team for incident %s!" incident_id
      (* won't fold anything! *)
  | Value.RamenValue (VString team_name) ->
      let prefix = "alerting/teams/"^ team_name ^"/contacts/" in
      Client.fold session.ZMQClient.clt ~prefix (fun k hv u ->
        match k, hv.Client.value with
        | Key.Teams (_, Contacts dialog_id), Value.AlertingContact _ ->
            f u dialog_id
        | _ ->
            u
      ) u
  | v ->
      invalid_sync_type k v "a string"

(* When we receive a firing notification we must first check if we have a pending
 * incident for that notification already, and if so update it and reschedule it.
 * Otherwise create a new one with as many dialogs as the assigned team has
 * contacts. *)
let rec set_alight conf session notif now =
  let debounce_time = now +. debounce_delay_for (Some notif) in
  (* TODO: Also store [now] in the incident *)
  let create_new_incident conf session notif _now =
    let incident_id = new_incident_id () in
    !logger.info "Creating new incident %s for notification %S"
      incident_id notif.VA.Notification.name ;
    let set = set_incident_key session incident_id in
    let team_name =
      find_in_charge conf session notif.VA.Notification.name in
    let phase = VA.Phase.Debouncing (None, true, debounce_time) in
    set Key.Phase (Value.IncidentPhase phase) ;
    set Key.FirstStartNotif (Value.Notification notif) ;
    set Key.LastStartNotif (Value.Notification notif) ;
    set Key.LastStateChangeNotif (Value.Notification notif) ;
    set Key.Team (Value.RamenValue (VString (team_name :> string))) ;
    pendings.incidents <- PendingMap.add notif.name incident_id pendings.incidents ;
    (* Schedule the incident for debouncing: *)
    reschedule incident_id debounce_time ;
    incident_id
  in
  match PendingMap.find notif.VA.Notification.name pendings.incidents with
  | exception Not_found ->
      let incident_id = create_new_incident conf session notif now in
      log session incident_id now (NewNotification StartEscalation)
  | incident_id ->
      let reason = "received firing=1 notification" in
      let save_start_notif () =
        set_incident_key session incident_id LastStartNotif
                         (Value.Notification notif) ;
        set_incident_key session incident_id LastStateChangeNotif
                         (Value.Notification notif)
      in
      match get_phase session incident_id with
      | exception Not_found ->
          (* Could happen if the confserver incident is deleted while we hold a
           * pending incident: *)
          !logger.error "Cannot find Phase for firing incident %s, \
                         deleting bogus pending incident"
            incident_id ;
          pendings.incidents <- PendingMap.remove notif.name pendings.incidents ;
          (* and retry: *)
          set_alight conf session notif now

      | Debouncing (_, true, _)
          (* Keep on debouncing, do not reset the time: *)
      | Contacting
          (* Ignore this duplicate and keep contacting as before: *)
      | Acknowledged ->
          (* Ignore this duplicate and assume oncallers are aware already *)
          log session incident_id now (NewNotification Duplicate)

      | Debouncing (init, false, _) ->
          (* Save that change of current state: *)
          let phase = VA.Phase.Debouncing (init, true, debounce_time) in
          set_phase session incident_id phase reason ;
          save_start_notif () ;
          log session incident_id now (NewNotification StartEscalation)
          (* No need to reschedule as we only extend the debounce time; the
           * current schedule will be bounced *)

      | Recovered
      | Finished as phase ->
          (* That incident was about to be forgotten but is revived; Rollback the
           * phase to Debouncing: *)
          let phase = VA.Phase.Debouncing (Some phase, true, debounce_time) in
          set_phase session incident_id phase reason ;
          reschedule incident_id debounce_time

(* When a notification that an alert is no longer firing is received,
 * cancel pending delivery or send the end of alert notification.
 * We do not do this from here though, but merely update the phase and
 * let the scheduler do the rest. *)
let extinguish_pending session notif now =
  let reason = "received firing=0 notification" in
  match PendingMap.find notif.VA.Notification.name pendings.incidents with
  | exception Not_found ->
      !logger.warning "Cannot find any ongoing pending incident for %s"
        notif.name
  | incident_id ->
      set_incident_key session incident_id LastStopNotif
                       (Value.Notification notif) ;
      let debounce_time = now +. debounce_delay_for (Some notif) in
      (match get_phase session incident_id with
      | exception Not_found ->
          !logger.error "Cannot find Phase for extinguished incident %s, \
                         deleting bogus pending incident"
            incident_id ;
          pendings.incidents <- PendingMap.remove notif.name pendings.incidents
      | Debouncing (_, false, _) ->
          (* Keep debouncing *)
          ()
      | Debouncing (init, true, _) ->
          (* Save this change in state: *)
          let phase = VA.Phase.Debouncing (init, false, debounce_time) in
          set_phase session incident_id phase reason ;
          set_incident_key session incident_id LastStateChangeNotif
                           (Value.Notification notif) ;
      | phase ->
          (* Start debouncing the recovery: *)
          let phase = VA.Phase.Debouncing (Some phase, false, debounce_time) in
          set_phase session incident_id phase reason ;
          set_incident_key session incident_id LastStateChangeNotif
                           (Value.Notification notif)) ;
      log session incident_id now (Stop Notification)

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

let get_notif_opt session incident_id k =
  let k = incident_key incident_id k in
  match get_key session k with
  | exception Not_found ->
      None
  | Value.Notification n ->
      Some n
  | v ->
      err_sync_type k v "a Notification" ;
      None

(* Deliver the message (or raise).
 * An acknowledgment is supposed to be received via another channel, TBD. *)
let contact_via conf session now incident_id dialog_id contact firing =
  log session incident_id now (Outcry dialog_id) ;
  (* Fetch all the info we can possibly need: *)
  let first_start_notif =
    get_notif session incident_id FirstStartNotif
  and last_stop_notif_opt =
    get_notif_opt session incident_id LastStopNotif
  and last_state_change =
    get_notif session incident_id LastStateChangeNotif
  and first_delivery_attempt =
    let k = dialog_key incident_id dialog_id FirstDeliveryAttempt in
    match get_key session k with
    | Value.RamenValue (VFloat t) -> t
    | v -> invalid_sync_type k v "a float"
  and last_delivery_attempt =
    let k = dialog_key incident_id dialog_id LastDeliveryAttempt in
    match get_key session k with
    | Value.RamenValue (VFloat t) -> t
    | v -> invalid_sync_type k v "a float" in
  let dict =
    [ "name", first_start_notif.name ;
      "incident_id", incident_id ;
      "start", nice_string_of_float (notif_time first_start_notif) ;
      "now", nice_string_of_float (Unix.time ()) ;
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
  (* Allow parameters to overwrite builtins: *)
  let dict = List.rev_append first_start_notif.parameters dict in
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
      kafka_publish conf options topic partition text

let pass_fpr max_fpr certainty =
  let certainty = cap ~min:0. ~max:1. certainty in
  match Deque.rear pendings.last_sent with
  | None ->
      !logger.info "Max FPR test: pass due to first notification ever sent." ;
      true
  | Some (_, (oldest, _)) ->
      (* Since we do not ask for the current time but work with time at start
       * of this batch, then if this alert belongs to the same batch than oldest
       * then we are going to skip it regardless of its certainty.
       * To work around this issue we do not use the batch start time but ask
       * again for the current time: * *)
      let now = Unix.gettimeofday () in
      let dt = now -. oldest in
      let max_fp = Float.ceil (dt *. max_fpr) |> int_of_float in
      (* Compute the probability that we had more than max_fp fp already. *)
      if max_fp < 1 (* bogus dt *) then (
        !logger.info "Max FPR test: bogus DT %f-%f=%f" now oldest dt ;
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
          (Deque.print (pair_print Float.print Float.print)) pendings.last_sent
          (Array.print Float.print) p_junks ;
        (* So that the probability to have sent more than max_fp is: *)
        let p_more = 1. -. p_less_eq in
        (if p_more > 0.5 then !logger.info else !logger.debug)
          "Max FPR test: we have sent %d notifications in the last %a, \
           probability to send more than %d false positive: %f."
          (Deque.size pendings.last_sent)
          RamenParsing.print_duration dt
          max_fp p_more ;
        p_more <= 0.5
      )

(* Returns true if there may still be tasks scheduled for now: *)
let scheduler conf session now =
  let del_incident incident_id =
    let first_start_notif =
      get_notif session incident_id FirstStartNotif in
    pendings.incidents <-
      PendingMap.remove first_start_notif.name pendings.incidents
  in
  let get_phase_or_delete incident_id =
    match get_phase session incident_id with
    | exception Not_found ->
        !logger.error "Cannot find phase of scheduled incident %s: deleting it"
          incident_id ;
        del_incident incident_id ;
        None
    | phase ->
        Some phase
  in
  (* Handle possible incident timeout (ie incident "solves" itself
   * after some time): *)
  let timeout_incident incident_id =
    let k = incident_key incident_id LastStartNotif in
    match get_key session k with
    | exception Not_found ->
        !logger.error
          "Cannot find last start notif for incident %s, forcing timeout"
          incident_id ;
        true
    | Value.Notification last_start ->
        let incident_age = now -. last_start.sent_time in
        if incident_age > !max_incident_age then (
          !logger.info "Timing out incident %s (age=%a > max-age=%a)"
            incident_id
            print_as_duration incident_age
            print_as_duration !max_incident_age ;
          true
        ) else if last_start.timeout > 0. &&
                  incident_age > last_start.timeout then (
          !logger.info "Timing out incident %s (age=%a > timeout=%a)"
            incident_id
            print_as_duration incident_age
            print_as_duration last_start.timeout ;
          true
        ) else false
    | v ->
        invalid_sync_type k v "a Notification"
  in
  let debounce_incident incident_id init current =
    (* [current] is the stable firing state at the end of the debouncing
     * period. If it is true, we must transition to `Contacting` phase,
     * whereas if it's not we must transition to `Recovered`.
     * [init] is the phase we started with. *)
    let set = set_incident_key session incident_id in
    let new_phase =
      match init, current with
      | Some (VA.Phase.Debouncing _), _ ->
          (* No debouncing is ever constructed with that init phase *)
          assert false
      | (None | Some (Recovered | Finished)), true ->
          fold_contacts session incident_id (fun () dialog_id ->
            set (Key.Dialogs (dialog_id, NumDeliveryAttempts))
                (Value.RamenValue (VU8 Uint8.zero)) ;
            set (Key.Dialogs (dialog_id, NextSend))
                (Value.RamenValue (VFloat now)) ;
            pendings.schedule <-
              RamenHeap.add heap_cmp (now, incident_id, Some dialog_id)
                            pendings.schedule
          ) () ;
          VA.Phase.Contacting
      | Some (Contacting | Acknowledged), false ->
          Recovered
      | Some init, _ ->
          init
      | None, false ->
          Finished in
    set_phase session incident_id new_phase "finished debouncing"
  in
  (* Send a fire notification (assuming phase is Contacting and time is due) *)
  let do_notify_fire incident_id dialog_id now contact =
    let attempts =
      try get_int_dialog_key session incident_id dialog_id NumDeliveryAttempts
      with Not_found -> 0 in
    if attempts >= contact.VA.Contact.max_attempts then
      !logger.info "Reached max delivery attempts (%d), won't retry"
        contact.max_attempts
    else (
      set_dialog_key session incident_id dialog_id NumDeliveryAttempts
                     (Value.RamenValue (VU32 (Uint32.of_int (attempts + 1)))) ;
      let k = dialog_key incident_id dialog_id FirstDeliveryAttempt in
      if not (Client.mem session.ZMQClient.clt k) then
        set_key session k (Value.of_float now) ;
      set_dialog_key session incident_id dialog_id LastDeliveryAttempt
                     (Value.of_float now) ;
      contact_via conf session now incident_id dialog_id contact true ;
      (* if pulse > 0 then a new delivery is supposed to take place after
       * that amount of time: *)
      if contact.pulse > 0. then (
        let t = now +. contact.pulse in
        set_dialog_key session incident_id dialog_id NextSend
                       (Value.of_float t) ;
        reschedule incident_id ~dialog_id t
      )
    )
  in
  (* Send a recovery notification (assuming phase is Recovery and time is due) *)
  let do_notify_recovery incident_id dialog_id now contact =
    contact_via conf session now incident_id dialog_id contact false ;
    (* Even if there are several dialogues it is OK to set the incident phase
     * as soone as the first dialog is sent a recovery message, because
     * recovries are notified for Finished incident as long as some dialogues
     * are still scheduled: *)
    set_phase session incident_id Finished "sent recovery notice"
    (* And do not reschedule this dialog *)
  in
  match RamenHeap.min pendings.schedule with
  | exception Not_found ->
      false
  | schedule_time, incident_id, None ->
      if schedule_time > now then false else (
        !logger.debug "Scheduling incident %s" incident_id ;
        pendings.schedule <-
          RamenHeap.del_min heap_cmp pendings.schedule ;
        if not (timeout_incident incident_id) then (
          match get_phase_or_delete incident_id with
          | Some (Debouncing (init, current, debounce_time)) ->
              if now >= debounce_time then
                debounce_incident incident_id init current
              else
                reschedule incident_id debounce_time
          | _ ->
              ()
        ) ;
        true
      )
  | schedule_time, incident_id, Some dialog_id ->
      if schedule_time > now then false else (
        !logger.debug "Scheduling incident %s, %s" incident_id dialog_id ;
        pendings.schedule <-
          RamenHeap.del_min heap_cmp pendings.schedule ;
        if not (timeout_incident incident_id) then
          (match get_dialog_float_key session incident_id dialog_id NextSend with
          | exception Not_found ->
              !logger.error
                "Cannot find next_send for %s, %s, deleting the incident"
                incident_id dialog_id ;
              del_incident incident_id
          | next_send ->
              if now >= next_send then (
                let team_name =
                  let k = incident_key incident_id Team in
                  match get_key session k with
                  | Value.RamenValue (VString n) -> N.team n
                  | v -> invalid_sync_type k v "a string" in
                let contact =
                  let k = Key.Teams (team_name, Contacts dialog_id) in
                  match get_key session k with
                  | Value.AlertingContact c -> c
                  | v -> invalid_sync_type k v "a contact" in
                match get_phase_or_delete incident_id with
                | Some Contacting ->
                    do_notify_fire incident_id dialog_id now contact
                | Some (Recovered | Finished) ->
                    do_notify_recovery incident_id dialog_id now contact
                | _ ->
                    (* reschedule for a later phase: *)
                    reschedule incident_id !default_reschedule
              ) else
                reschedule incident_id next_send) ;
          true
      )

(* Avoid creating several instances of watchdogs even when thread crashes
 * and is restarted: *)
let watchdog = ref None

(* Make sure we have a minimal default configuration of one team: *)
let ensure_minimal_conf session =
  (* Default content in case the configuration file is absent: *)
  let prefix = "alerting/teams/" in
  if not (Client.exists session.ZMQClient.clt ~prefix (fun k _hv ->
            match k with
            | Key.Teams (_, Contacts _) -> true
            | _ -> false))
  then
    let k = Key.Teams (default_team_name, Contacts "prometheus")
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
        pulse = 0. ;
        max_attempts = 1 } in
    set_key session k v

let start conf max_fpr timeout_idle_kafka_producers_
          debounce_delay_ max_last_sent_kept_ max_incident_age_ =
  timeout_idle_kafka_producers := timeout_idle_kafka_producers_ ;
  debounce_delay := debounce_delay_ ;
  max_last_sent_kept := max_last_sent_kept_ ;
  max_incident_age := max_incident_age_ ;
  let topics = [ "alerting/*" ] in
  let while_ () = !RamenProcesses.quit = None in
  if !watchdog = None then
    (* More than 30s delay has been observed when trying to open an sqlite DB: *)
    let timeout = 60. in
    watchdog := Some (RamenWatchdog.make ~timeout "alerter" RamenProcesses.quit) ;
  let watchdog = Option.get !watchdog in
  let sync_loop session =
    ensure_minimal_conf session ;
    load_pendings session ;
    RamenWatchdog.enable watchdog ;
    while while_ () do
      let now = Unix.gettimeofday () in
      while scheduler conf session now do () done ;
      ZMQClient.process_in ~while_ session ;
      RamenWatchdog.reset watchdog ;
    done in
  let on_set session k v _uid _mtime =
    match k, v with
    | Key.Notifications, Value.Notification notif ->
        (* FIXME: if not synced, keep it for later: *)
        let now = Unix.gettimeofday () in
        if notif.VA.Notification.firing then (
          !logger.info
            "Received %snotification from %a:%a: %S started (%s certainty)"
            (if notif.VA.Notification.test then "TEST " else "")
            N.site_print notif.site N.fq_print notif.worker notif.name
            (nice_string_of_float notif.certainty) ;
          (* A single global false positive rate is enforced here.
           * firing notif that fail the test are merely ignored. *)
          if notif.test || pass_fpr max_fpr notif.certainty then
            set_alight conf session notif now
        ) else (
          !logger.info
            "Received %snotification from %a:%a: %S ended"
            (if notif.VA.Notification.test then "TEST " else "")
            N.site_print notif.site N.fq_print notif.worker notif.name ;
          extinguish_pending session notif now
        )
    | Key.Notifications, _ ->
        err_sync_type k v "a notification"
    | _ ->
        ()
  in
  let on_new session k v uid mtime _can_write _can_del _owner _expiry =
    on_set session k v uid mtime
  in
  let on_del _session k v =
    match k, v with
    | Key.Incidents (incident_id, FirstStartNotif),
      Value.Notification notif ->
        !logger.debug "Delete pending incident %s for notification %s"
          incident_id notif.VA.Notification.name ;
        pendings.incidents <-
          PendingMap.remove notif.name pendings.incidents
    | _ ->
        (* No need to delete the dialogs, let them expire when scheduled *)
        ()
  in
  start_sync conf ~topics ~while_ ~recvtimeo:1. ~on_set ~on_new ~on_del sync_loop
