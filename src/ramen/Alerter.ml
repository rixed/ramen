(* Alert manager: receive alert signals from Ramen configuration and:
 * - deduplicate the alerts (same team, same name);
 * - find who to contact, and how (including cascading process);
 * - deliver the notifications;
 * - handle acknowledgments.
 *
 * This currently runs as part of the alerting configuration (aka provides
 * the implementation of the "alert" primitive operation). Later could be
 * made independent, although the simplest way to do that is probably via
 * Ramen choreographer.
 *)

let debug = true

(* All global (aka team-wide) inhibits, keyed by team name (value being
 * expiration of the inhibit *)
let team_inhibits = Hashtbl.create 11

(* All inhibited alerts, keyed by alert name*team *)
let alert_inhibits = Hashtbl.create 11

let is_inhibited name team =
  let in_inhibit_hash h k =
    match Hashtbl.find h k with
    | exception Not_found -> false
    | end_date ->
      if !Alarm.now < end_date then true else (
        Hashtbl.remove h k ;
        false
      ) in
  in_inhibit_hash team_inhibits team ||
  in_inhibit_hash alert_inhibits (name, team)

module Alert =
struct
  type t =
    { name : string ;
      team : string ;
      importance : int ;
      time : float ;
      title : string ;
      text : string }

  let make name team importance title text =
    { name ; team ; importance ; title ; text ;
      time = !Alarm.now }
end

module Incident =
struct
  type t =
    { team : string ;
      mutable alerts : Alert.t list }

  let ongoings = ref []

  let make alert =
    let t = { team : alert.Alert.team ; alerts = [ alert ] } in
    ongoings := t :: !ongoings

  let has_alert_with i cond =
    List.exists cond i.alerts

  (* Let's be conservative and create a new incident for each new alerts.
   * User could still manually merge incidents later on. *)
  let get_or_create alert =
    match List.find (fun i ->
            i.team = alert.Alert.team &&
            has_alert_with (fun a -> a.Alert.name = alert.Alert.name))
    with exception Not_found -> make alert, false
       | i -> i, has_alert_with (fun a -> a.Alert.name = alert.Alert.name &&
                                          a.Alert.title = alert.Alert.title)
end

module Sender =
struct
  let printer ~title ~text =
    Printf.fprintf "Title: %s\n\n%s\n%!" title text

  let get team step = printer
end

module Escalation =
struct
  type t =
    { name : string ;
      timeouts : float array ;
      mutable acknowledged : bool ;
      alarm : Alarm.t }

  (* All ongoing escalations, keyed by escalation, value = step we are
   * currently in and when we entered it. Note that strictly speaking
   * we do not need this hash since we already have all escalation in
   * alarms, but we'd like to save this from time to time on disk to help
   * not loosing track of what's happening in case of crash: *)
  let ongoings = Hashtbl.create 11

  let default_escalation () =
    (* First send type 0, then immediately send type 1 then
     * after 5 minutes send type 2 then after 10 minutes send type 3,
     * and repeat type 3 every 10 minutes: *)
    { name = "default" ; timeouts = [| 0.; 350.; 600.; 600. |] ;
      acknowledged = false ; alarm = Alarm.make () }

  let make team importance =
    let e = default_escalation ()

  (* Send that alert using message king for this step *)
  let send_alert step alert =
    let open Alert in
    let send = Sender.get alert.team step in
    send ~title:alert.title ~text:alert.text

  (* Send a message and prepare the escalation *)
  let rec send e step alert =
    let len = Array.length e.timeouts in
    assert (len > 0) ;
    let timeout = e.timeouts.[ min (len - 1) step ] in 
    send_alert step alert
    Alarm.at e.alarm (!Alarm.now +. timeout) (fun () ->
      if not e.acknowledged then (
        Hashtbl.modify e (fun (step, _) -> step+1, !Alarm.now) ongoings ;
        send e (step+1) alert
      ))

  let start e alert =
    Hashtbl.add ongoings e (0, !Alarm.now) ;
    send e 0 alert
end

(* Everything start by: we receive an alert with a name, a team name,
 * and a description (text + title). *)
let alert ~name ~team ~importance ~title ~text =
  (* First step: deduplication *)
  let alert = Alert.make name team importance title text in
  let incident, is_dup = Incident.get_or_create alert in
  let victim = who_is_oncall team in
  if is_dup then (
    log (NewAlert (alert, incident, Duplicate)) ;
  ) else if is_inhibited name team then (
    log (NewAlert (alert, incident, Inhibited)) ;
  ) else if victim.stfu then (
    log (NewAlert (alert, incident, STFU)) ;
  ) else (
    let esc = Escalation.make team importance in
    Escalation.start esc alert
  )
