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
 * An _alert_ is what may be notified to an actual user. It's created as soon
 * as is received a firing notification which name was not already firing, and
 * ends because of a notification for that same name that is not firing, or
 * because no firing notification for that name has been received for longer
 * than a specified duration. So, multiple notifications referring to the same
 * name will contribute to the same alert. That's _deduplication_. Strictly
 * speaking, the key is actually alert name * contact, so that the same alert
 * will ring the next oncaller during the on-duty shift.
 *
 * A _message_ is what is actually sent to the user. After a configurable
 * amount of time, a new alert is considered ripe for messaging. This delay is
 * there so that a flapping alert will not result in many messages. That's
 * _debouncing_. When an alert is finished, another message will also be sent,
 * again after a debouncing delay.  The exact nature of a message depends on
 * user configuration: it can be an email, a pushed mobile phone message, an
 * SNMP trap... Users are supposed to acknowledge the start message (but not
 * the ending message), but this is currently not implemented.
 *
 * An _incident_ is a set of alerts. By default, every alert also creates its
 * own incident, but users can manually group alerts together to document that
 * a group of alert refers to the same route cause. Incidents are currently
 * not implemented.
 *
 * A _task_ is an action to progress along an alert workflow (typically, a
 * message has to be sent or to be timed out). Basically we have a heap of such
 * tasks with an associated timestamp and execute them all in order, which most
 * often than not yield to the task to be rescheduled with maybe a new
 * delivery_status.
 *)
open Batteries
open Stdint
open RamenLog
open RamenHelpers
open RamenNullable
open RamenConsts
module C = RamenConf
module N = RamenName
module Files = RamenFiles
module Paths = RamenPaths
module StringExpansion = RamenStringExpansion

(* Used to know if we must use normal schedule delay or schedule delay
 * after startup: *)
let startup_time = ref (Unix.gettimeofday ())

(* Used to build an unique integer for each new alert. This is an external,
 * non-temporal identifier useful for acknowledgment or logging. Still,
 * there can be only one live alert per notification name. *)
type alert_id = uint64 [@@ppp PPP_OCaml]

let next_alert_id =
  let ppp_of_file = Files.ppp_of_file ~default:"0" alert_id_ppp_ocaml in
  fun conf ->
    let fname =
      N.path_cat [ conf.C.persist_dir ; N.path "pending_alerts" ] in
    let v = ppp_of_file fname in
    Files.ppp_to_file fname alert_id_ppp_ocaml (Uint64.succ v) ;
    v

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

(* From the notification name the team is retrieved, and from there the
 * messaging channel. *)
module Contact = struct
  type t =
    | ViaExec of string
    | ViaSysLog of string
    | ViaSqlite of
        { file : string ;
          insert : string ;
          create : string [@ppp_default ""] }
    | ViaKafka of
        (* For now it's way simpler to have the connection configured
         * once and for all rather than dependent of the notification
         * options, as we can keep a single connection alive.
         * As per coutume, options starting with kafka_topic_option_prefix
         * ("topic.") are topic options, while others are producer options.
         * Mandatory options:
         * - metadata.broker.list
         * Interesting options:
         * - topic.message.timeout.ms *)
        { options : (string * string) list ;
          topic : string ;
          partition : int ;
          text : string }
    [@@ppp PPP_OCaml]

  let compare = compare

  let print ?abbrev oc cmd =
    let s = PPP.to_string t_ppp_ocaml cmd in
    let s = match abbrev with None -> s | Some l -> RamenHelpers.abbrev l s in
    String.print oc s

  let print_short = print ~abbrev:18
  let print = print ?abbrev:None
end

module Team = struct
  type t =
    { (* Team name is really nothing but a prefix for notification names: *)
      name : string [@ppp_default ""];
      (* TODO: with each contact have a boolean to disable messaging the
       * alert stops. *)
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
    default_init_schedule_delay : float
      [@ppp_default Default.init_schedule_delay] ;
    default_init_schedule_delay_after_startup : float [@ppp_default 120.] ;
    (* After a firing is received, automatically "close" the alert if it's not
     * been firing again for that long (timing out behaves the same as if a
     * firing=0 had been received, up to sending a termination message). *)
    default_alert_timeout : float [@ppp_default 10. *. 3600.] ;
    (* How long to keep idle kafka producers: *)
    timeout_idle_kafka_producers : float [@ppp_default 24. *. 3600.] }
  [@@ppp PPP_OCaml]

(* Same as RamenOperation.notification, but with strings for name and
 * parameters: *)
type notification =
  { site : string ;
    worker : string ;
    test : bool ;
    notif_name : string ;
    event_time : float option ;
    sent_time : float ;
    rcvd_time : float ;
    firing : bool option ;
    certainty : float ;
    parameters : (string * string) list }
  [@@ppp PPP_OCaml]

(* Return the closest we have from the event time: *)
let notif_time notif =
  notif.event_time |? notif.rcvd_time

type alert =
  { (* The notification that started this alert: *)
    first_start_notif : notification ;
    (* The last notification with firing=1 (useful for alert timeout: *)
    mutable last_start_notif : notification ;
    (* A contact is associated to each alert at creation time, and
     * therefore does not change from oncaller to oncaller when the shift
     * change, which is correct (incidents trump on-duty switches): *)
    contact : Contact.t ;
    (* If we received the firing=0 notification: *)
    mutable last_stop_notif : notification option ;
    (* Number of delivery attempt of the start or stop message: *)
    mutable attempts : int ;
    (* Timestamps of the first and last delivery attempt *)
    mutable first_delivery_attempt : float ;
    mutable last_delivery_attempt : float ;
    (* Duration after which the alert should be automatically closed: *)
    timeout : float ;
    (* Duration after which we can sent a message after a status change: *)
    debounce_delay : float ;
    (* Non-temporal alert integer identifier: *)
    alert_id : uint64 }
  [@@ppp PPP_OCaml]

type task =
  { (* The notification this task is about: *)
    mutable alert : alert ;
    (* When we plan to have another look at this task: *)
    mutable schedule_time : float ;
    (* When we plan to send the message: *)
    mutable send_time : float ;
    (* The current delivery status (see below): *)
    mutable status : delivery_status }
  [@@ppp PPP_OCaml]

and delivery_status =
  | StartToBeSent  (* firing notification that is yet to be sent *)
  | StartToBeSentThenStopped (* notification that stopped before being sent *)
  | StartSent      (* firing notification that is yet to be acked *)
  | StartAcked     (* firing notification that has been acked *)
  | StopToBeSent   (* non-firing notification that is yet to be sent *)
  | StopSent       (* we do not ack stop messages so this is all over *)
  [@@ppp PPP_OCaml]

let string_of_delivery_status =
  PPP.to_string delivery_status_ppp_ocaml

let make_task conf notif_conf start_notif schedule_time contact =
  { schedule_time ;
    send_time = schedule_time ;
    status = StartToBeSent ;
    alert =
      { attempts = 0 ;
        first_delivery_attempt = 0. ;
        last_delivery_attempt = 0. ;
        alert_id = next_alert_id conf ;
        first_start_notif = start_notif ;
        last_start_notif = start_notif ;
        last_stop_notif = None ;
        timeout = notif_conf.default_alert_timeout ;
        debounce_delay = notif_conf.default_init_schedule_delay ;
        contact } }

module PendingSet = Set.Make (struct
  type t = task
  let compare i1 i2 =
    let c = String.compare i1.alert.first_start_notif.notif_name
                           i2.alert.first_start_notif.notif_name in
    if c <> 0 then c else
    Contact.compare i1.alert.contact i2.alert.contact
end)

(* We keep some info about the last [max_last_sent] message sent in
 * [last_sent] in order to fight false positives: *)
let max_last_sent = 100

type pendings =
  { (* The set of all tasks. We keep only one alert per notif name * contact *)
    mutable set : PendingSet.t ;
    (* Delivery schedule is a heap ordered by schedule_time. All messages
     * that have to be sent or acknowledged are in there: *)
    mutable heap : task RamenHeap.t ;
    (* Set of the timestamp * certainties of the last last_max_sent
     * notifications that have been sent (not persisted to disk): *)
    mutable last_sent : (float * float) Deque.t ;
    (* Flag that tells us if this record has been modified since last save, to
     * spare us from serializing and saving the file if nothing has changed: *)
    mutable dirty : bool }

let pendings =
  { set = PendingSet.empty ;
    heap = RamenHeap.empty ;
    last_sent = Deque.empty ;
    dirty = false }

let heap_pending_cmp i1 i2 =
  Float.compare i1.schedule_time i2.schedule_time

let find_pending notif_name test contact =
  let fake_pending_named =
    let notif =
      { notif_name ; site = "" ; worker = "" ; parameters = [] ;
        test ; firing = None ; certainty = 0. ;
        event_time = None ; sent_time = 0. ; rcvd_time = 0. } in
    { schedule_time = 0. ;
      send_time = 0. ;
      status = StartToBeSent ;
      alert =
        { attempts = 0 ;
          first_delivery_attempt = 0. ;
          last_delivery_attempt = 0. ;
          timeout = 0. ;
          debounce_delay = 0. ;
          alert_id = Uint64.zero ;
          contact ;
          first_start_notif = notif ;
          last_start_notif = notif ;
          last_stop_notif = None } } in
  PendingSet.find fake_pending_named pendings.set

let set_status p status reason =
  if status <> p.status then (
    !logger.info "%s to %a status change: %s -> %s (%s)"
      p.alert.first_start_notif.notif_name Contact.print_short p.alert.contact
      (string_of_delivery_status p.status)
      (string_of_delivery_status status)
      reason ;
    p.status <- status ;
    pendings.dirty <- true)

(* The serialized type under which pending tasks are saved: *)
type saved_pendings = task list [@@ppp PPP_OCaml]

let save_pendings conf =
  let fname = Paths.pending_notifications_file conf.C.persist_dir in
  let lst = PendingSet.to_list pendings.set in
  Files.ppp_to_file fname saved_pendings_ppp_ocaml lst

let restore_pendings conf =
  let fname = Paths.pending_notifications_file conf.C.persist_dir in
  (match Files.ppp_of_file ~default:"[]" saved_pendings_ppp_ocaml fname with
  | exception (Unix.(Unix_error (ENOENT, _, _)) | Sys_error _) -> ()
  | lst ->
      pendings.set <- PendingSet.of_list lst) ;
  let heap, count, mi_ma =
    PendingSet.fold (fun p (heap, count, mi_ma) ->
      RamenHeap.add heap_pending_cmp p heap,
      count + 1,
      let t = p.schedule_time in
      match mi_ma with None -> Some (t, t)
      | Some (mi, ma) -> Some (min mi t, max ma t)
    ) pendings.set (RamenHeap.empty, 0, None) in
  pendings.heap <- heap ;
  pendings.dirty <- false ;
  if count > 0 then
    !logger.info "Start with %d tasks%s"
      count
      (match mi_ma with None -> ""
       | Some (mi, ma) -> " scheduled from "^ string_of_time mi
                          ^" to "^ string_of_time ma)

(* When we receive a notification that an alert is no more firing, we must
 * cancel pending delivery or send the end of alert notification.
 * We do not do this from here though, but merely update the status and
 * let the scheduler do the rest.
 * Can raise Not_found. *)
let stop_pending p notif_opt now reason =
  if notif_opt <> None then
    p.alert.last_stop_notif <- notif_opt ;
  match p.status with
  | StartToBeSent ->
      set_status p StartToBeSentThenStopped reason
  | StartSent (* don't care about the ack any more *)
  | StartAcked ->
      set_status p StopToBeSent reason ;
      p.alert.attempts <- 0 ;
      (* reschedule *)
      p.send_time <-
        now +. jitter p.alert.debounce_delay ;
      pendings.dirty <- true
  | StopToBeSent | StopSent | StartToBeSentThenStopped -> ()

let extinguish_pending notif now contact =
  match find_pending notif.notif_name false contact with
  | exception Not_found ->
      !logger.warning "Cannot find pending notification %s for %a to stop"
        notif.notif_name Contact.print contact ;
  | p ->
      stop_pending p (Some notif) now "no longer firing"

let timeout_pending p now =
  stop_pending p None now "timed out"

(* When we receive a notification that an alert is firing, we must first
 * check if we have a pending notification with same name already and
 * reschedule it, or create a new one. *)
let set_alight conf notif_conf notif contact =
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
  match find_pending notif.notif_name notif.test contact with
  | exception Not_found ->
      let new_pending = make_task conf notif_conf notif schedule_time contact in
      pendings.set <- PendingSet.add new_pending pendings.set ;
      pendings.heap <-
        RamenHeap.add heap_pending_cmp new_pending pendings.heap ;
      pendings.dirty <- true ;
      !logger.info "%s to %a status change : NEW -> %s"
        notif.notif_name Contact.print_short contact
        (string_of_delivery_status new_pending.status)
  | p ->
      let reason = "received notification of firing" in
      (match p.status with
      | StartToBeSentThenStopped | StopSent ->
          set_status p StartToBeSent reason ;
          p.alert.attempts <- 0 ;
          p.alert.last_start_notif <- notif ;
          p.send_time <- schedule_time ;
          pendings.dirty <- true
      | StopToBeSent ->
          set_status p StartAcked reason ;
          p.alert.attempts <- 0 ;
          p.alert.last_start_notif <- notif ;
          pendings.dirty <- true
      | StartAcked | StartToBeSent | StartSent -> ())

(*
 * A thread that notifies the external world and wait for a successful
 * confirmation, or fails.
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
  fun conf notif_conf options topic partition text ->
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
        if now -. !last_used >= notif_conf.timeout_idle_kafka_producers then (
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
let ack name test contact now =
  match find_pending name test contact with
  | exception Not_found ->
      !logger.warning "Received an Ack for unknown alert %s via %a, \
                       ignoring."
        name Contact.print contact
  | { status = StartSent ; _ } as p ->
      !logger.debug "Successfully messaged %a of start of alert %S"
        Contact.print contact name ;
      set_status p StartAcked "acked" ;
      (* Save this emission: *)
      pendings.last_sent <- Deque.cons (now, p.alert.first_start_notif.certainty) pendings.last_sent ;
      if Deque.size pendings.last_sent > max_last_sent then
        pendings.last_sent <- fst (Option.get (Deque.rear pendings.last_sent)) ;
      pendings.dirty <- true
  | p ->
      !logger.debug "Ignoring an ACK for alert in status %s"
        (string_of_delivery_status p.status)

(* When we give up sending a notification. Actually removing it from
 * the set of pending tasks is done separately. *)
(* TODO: log *)
let cancel conf pending _now reason =
  !logger.info "Cancelling alert %S: %s"
    pending.alert.first_start_notif.notif_name reason ;
  let labels = ["reason", reason] in
  IntCounter.inc ~labels (stats_messages_cancelled conf.C.persist_dir)

(* Deliver the message (or raise).
 * An acknowledgment is supposed to be received via another channel, TBD. *)
let contact_via conf notif_conf p =
  let alert = p.alert in
  let firing =
    match p.status with
    | StartToBeSent -> true
    | StopToBeSent -> false
    | _ -> assert false in
  let dict =
    [ "name", alert.first_start_notif.notif_name ;
      "alert_id", Uint64.to_string alert.alert_id ;
      "start", nice_string_of_float (notif_time alert.first_start_notif) ;
      "now", nice_string_of_float (Unix.time ()) ;
      "first_sent", nice_string_of_float alert.first_delivery_attempt ;
      "last_sent", nice_string_of_float alert.last_delivery_attempt ;
      "site", alert.first_start_notif.site ;
      "worker", alert.first_start_notif.worker ;
      "test", string_of_bool (alert.first_start_notif.test) ;
      "firing", string_of_bool firing ;
      "certainty", nice_string_of_float alert.first_start_notif.certainty ;
      (* Those are for convenience, before we can call actual functions
       * from the templates: *)
      "site", (conf.C.site :> string) ;
      "hostname", (getenv ~def:"?HOSTNAME?" "HOSTNAME") ;
      "certainty_percent", 100. *. alert.first_start_notif.certainty |>
                           round_to_int |> string_of_int ] in
  (* Add "stop" if we have it (or let it be NULL) *)
  let dict =
    match alert.last_stop_notif with
    | Some notif ->
        let ts = notif_time notif in
        ("stop", nice_string_of_float ts) :: dict
    | None -> dict in
  (* Allow parameters to overwrite builtins: *)
  let dict = List.rev_append alert.first_start_notif.parameters dict in
  let exp ?q ?n = StringExpansion.subst_dict dict ?quote:q ?null:n in
  let open Contact in
  match alert.contact with
  | ViaExec cmd ->
      execute_cmd conf (exp ~q:shell_quote cmd)
  | ViaSysLog str ->
      log_str conf (exp str)
  | ViaSqlite { file ; insert ; create } ->
      let ins = exp ~q:sql_quote ~n:"NULL" insert in
      sqllite_insert conf (exp file) ins create
  | ViaKafka { options ; topic ; partition ; text } ->
      (* FIXME: Here it is assumed that the text is a JSON template, and
       * that all notification parameters will be embedded as json strings.
       * It would be much better to give user control over the quotation with
       * explicit function calls from the template. *)
      let text = exp ~q:json_quote ~n:"null" text in
      kafka_publish conf notif_conf options topic partition text

(* TODO: log *)
let do_notify conf notif_conf p now =
  let i = p.alert in
  if i.attempts >= 3 then (
    !logger.warning "Cannot deliver alert %S after %d attempt, \
                     giving up" i.first_start_notif.notif_name i.attempts ;
    failwith "too many attempts"
  ) else (
    i.attempts <- i.attempts + 1 ;
    if i.first_delivery_attempt = 0. then
      i.first_delivery_attempt <- now ;
    i.last_delivery_attempt <- now ;
    contact_via conf notif_conf p
  )

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
          (Deque.print (Tuple2.print Float.print Float.print)) pendings.last_sent
          (Array.print Float.print) p_junks ;
        (* So that the probability to have sent more than max_fp is: *)
        let p_more = 1. -. p_less_eq in
        !logger.info "Max FPR test: we have sent %d notifications in the last %a, \
                      probability to send more than %d false positive: %f."
          (Deque.size pendings.last_sent)
          RamenParsing.print_duration dt
          max_fp p_more ;
        p_more <= 0.5
      )

let last_adv_task = ref None

(* Returns true if there may still be notifications to be sent: *)
let send_next conf notif_conf max_fpr now =
  (* Regardless of when the next step is supposed to happen, do not stay
   * longer than this duration without rescheduling a task, for it's status
   * can be changed assynchronically by received notifs or acks: *)
  let default_reschedule = 10. in
  let reschedule_min time =
    let p, heap = RamenHeap.pop_min heap_pending_cmp pendings.heap in
    p.schedule_time <- time ;
    pendings.heap <- RamenHeap.add heap_pending_cmp p heap ;
    pendings.dirty <- true
  and del_min p =
    pendings.heap <- RamenHeap.del_min heap_pending_cmp pendings.heap ;
    pendings.set <- PendingSet.remove p pendings.set ;
    pendings.dirty <- true
  in
  match RamenHeap.min pendings.heap with
  | exception Not_found -> false
  | p ->
      let start_notif = p.alert.first_start_notif in
      if p.schedule_time > now then (
        (match !last_adv_task with
        | Some p' when p' == p -> ()
        | _ ->
          last_adv_task := Some p ;
          !logger.debug "Next task will be about %s for %a%s, status %s, scheduled for %s, send at %s"
            start_notif.notif_name
            Contact.print_short p.alert.contact
            (if start_notif.test then " (TEST)" else "")
            (string_of_delivery_status p.status)
            (string_of_time p.schedule_time)
            (string_of_time p.send_time)
        ) ;
        false
      ) else (
        !logger.debug "Current task is about %s for %a%s, status %s, scheduled for %s, send at %s"
          start_notif.notif_name
          Contact.print_short p.alert.contact
          (if start_notif.test then " (TEST)" else "")
          (string_of_delivery_status p.status)
          (string_of_time p.schedule_time)
          (string_of_time p.send_time) ;
        (match p.status with
        | StartToBeSent | StopToBeSent ->
            if p.send_time <= now then (
              if p.status = StopToBeSent ||
                 start_notif.test ||
                 pass_fpr max_fpr start_notif.certainty
              then (
                try
                  do_notify conf notif_conf p now ;
                  let status = if p.status = StartToBeSent then StartSent
                                                           else StopSent in
                  set_status p status "successfully sent message" ;
                  (* Acknowledgments are supposed to be received via another
                   * async channel but this is still TBD. For now we ack at
                   * once. *)
                  ack start_notif.notif_name start_notif.test p.alert.contact now ;
                  (* Keep rescheduling until stopped (or timed out): *)
                  reschedule_min (now +. jitter default_reschedule)
                with Failure reason ->
                    cancel conf p now reason ;
                    del_min p
                   | e ->
                    let err_msg = Printexc.to_string e in
                    cancel conf p now err_msg ;
                    del_min p
              ) else ( (* not pass_fpr *)
                cancel conf p now "too many false positives" ;
                del_min p
              )
            ) else ( (* p.send_time > now *)
              reschedule_min p.send_time
            )
        | StartToBeSentThenStopped | StopSent ->
            (* No need to reschedule this *)
            del_min p
        | StartSent -> (* Still missing the Ack, resend *)
            assert (p.send_time <= p.schedule_time) ;
            !logger.info
              "Waited %a ack for too long regarding %s of alert %S"
              Contact.print p.alert.contact
              (if p.status = StartSent then "start" else "stop")
              start_notif.notif_name ;
            set_status p StartToBeSent "still no ack" ;
            p.send_time <- now ;
            reschedule_min now
        | StartAcked -> (* Maybe timeout this alert? *)
            let ts = notif_time p.alert.last_start_notif in
            (* Test alerts have no recovery or timeout end their lifespan
             * does not go beyond the ack. *)
            if start_notif.test ||
               now >= ts +. p.alert.timeout
            then (
              timeout_pending p now ;
              reschedule_min p.send_time
            ) else (
              (* Keep rescheduling as we may time it out or we may
               * receive an ack or an end notification: *)
              reschedule_min (now +. jitter default_reschedule)
            )
      ) ;
      true
    )

(* Avoid creating several instances of watchdogs even when thread crashes
 * and is restarted: *)
let watchdog = ref None

let send_notifications max_fpr conf notif_conf =
  if !watchdog = None then
    watchdog := Some (RamenWatchdog.make "alerter" RamenProcesses.quit) ;
  let watchdog = Option.get !watchdog in
  let rec loop () =
    let now = Unix.gettimeofday () in
    while send_next conf notif_conf max_fpr now do () done ;
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
               \"summary\":\"'${desc}'\",\
               \"severity\":\"critical\"}}]' \
          'http://localhost:9093/api/v1/alerts'" in
    { teams =
        Team.[
          { name = "" ;
            contacts = [ send_to_prometheus ] } ] ;
      default_init_schedule_delay = Default.init_schedule_delay ;
      default_init_schedule_delay_after_startup = 120. ;
      default_alert_timeout = 10. *. 3600. ;
      timeout_idle_kafka_producers = 24. *. 3600. } in
  let contents = PPP.to_string notify_config_ppp_ocaml default_conf in
  Files.ensure_exists ~contents notif_conf_file

let load_config =
  let ppp_of_file =
    Files.ppp_of_file ~default:"{}" notify_config_ppp_ocaml in
  fun notif_conf_file ->
    let notif_conf = ppp_of_file notif_conf_file in
    check_conf_is_valid notif_conf ;
    notif_conf

let start conf notif_conf_file rb max_fpr =
  !logger.debug "Using configuration file %a" N.path_print notif_conf_file ;
  (* Check the configuration file is OK before waiting for the first
   * notification. Also, we will reload this ref, keeping the last
   * good version: *)
  let notif_conf = ref (load_config notif_conf_file) in
  restore_pendings conf ;
  (* Better check if we can draw a new alert_id before we need it: *)
  let _alert_id = next_alert_id conf in
  Thread.create (
    restart_on_failure "send_notifications"
      (send_notifications max_fpr conf)) !notif_conf |> ignore ;
  let while_ () = !RamenProcesses.quit = None in
  RamenSerialization.read_notifs ~while_ rb
    (fun (site, worker, test, sent_time, event_time, notif_name,
          firing, certainty, parameters) ->
    let event_time = option_of_nullable event_time in
    (* From time to time worker can issue test notifications, which can be
     * ignored or used to periodically check connectivity.
     * Those messages are sent as normally as possible but does not start an
     * actual incident cycle. *)
    let firing =
      if test then Some true else option_of_nullable firing
    and parameters = Array.to_list parameters in
    let now = Unix.gettimeofday () in
    let notif =
      { site ; worker ; test ; sent_time ; rcvd_time = now ; event_time ;
        notif_name ; firing ; certainty ; parameters } in
    !logger.info "Received %snotification from %s%s: %S %s"
      (if test then "TEST " else "")
      (if site <> "" then site ^":" else "")
      worker notif_name
      (if firing = Some false then "ended"
       else ("started ("^ nice_string_of_float certainty ^" certainty)")) ;
    (* Each time we receive a notification we have to assign it to a team,
     * and then use the configured channel to notify it. We load the
     * configuration anew each time, relying on ppp_of_file caching
     * mechanism to cut down the work: *)
    (try notif_conf := load_config notif_conf_file
    with exn ->
      !logger.error "Cannot read alerter configuration file %a: %s"
        N.path_print notif_conf_file (Printexc.to_string exn)) ;
    let team = Team.find_in_charge conf !notif_conf.teams notif_name in
    let action =
      match notif.firing with
      | None | Some true ->
          set_alight conf !notif_conf notif
      | Some false ->
          extinguish_pending notif now
    in
    List.iter action team.Team.contacts)
