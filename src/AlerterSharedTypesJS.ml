module Contact =
struct
  type t =
    | Console
    | SysLog
    | Email of { to_ : string [@ppp_rename "to"] ;
                 cc : string [@ppp_default ""] ;
                 bcc : string [@ppp_default ""] }
    | SMS of string
    | Sqlite of { file : string ;
                  (* $ID$, $STARTED$, $STOPPED$, $TEXT$, $TITLE$,
                   * $TEAM$ and $IMPORTANCE$ replaced by actual values *)
                  insert : string ;
                  create : string [@ppp_default ""] }
    [@@ppp PPP_JSON]

  let to_string = function
    | Console -> "console"
    | SysLog -> "syslog"
    | Email { to_ ; _ } -> to_
    | SMS num -> "SMS to "^ num
    | Sqlite { file ; _ } -> "sqlite DB "^ file
end

module OnCaller =
struct
  type t =
    { name : string ;
      (* Contacts ordered by preference, preferred one first: *)
      contacts : Contact.t array } [@@ppp PPP_JSON]
end

module Escalation =
struct
  type step =
    { timeout : float ;
      (* Who to ring from the oncallers squad. Can be empty (useful at the
       * end of the escalation to stop notifying). *)
      victims : int array } [@@ppp PPP_JSON]

  let string_of_victims a =
    let soi i =
      if i = 0 then "1st" else
      if i = 1 then "2nd" else
      if i = 2 then "3rd" else
      string_of_int i ^"th" in
    let len = Array.length a in
    let rec loop s i =
      if i = len then s ^" oncall"^ (if i > 1 then "s" else "") else
      loop
        (s ^(if i > 0 then (
               if i = len - 1 then " and " else ", "
             ) else "")^ soi i)
        (i + 1) in
    if len = 0 then "no one" else loop "" 0

  type t =
    { steps : step array ;
      (* index in the above array: *)
      mutable attempt : int ;
      mutable last_sent : float } [@@ppp PPP_JSON]
end

module Alert =
struct
  type notification_outcome =
    | Duplicate | Inhibited | STFU | StartEscalation
    [@@ppp PPP_JSON]

  type stop_source = Notification | Manual of string | Timeout [@@ppp PPP_JSON]

  type event =
    | NewNotification of notification_outcome
    | Escalate of Escalation.step
    | Outcry of (string * Contact.t)
    (* TODO: we'd like to know the origin of this ack. *)
    | Ack
    | Stop of stop_source [@@ppp PPP_JSON]

  let string_of_event = function
    | NewNotification Duplicate -> "Received duplicate notification"
    | NewNotification Inhibited -> "Received inhibited notification"
    | NewNotification STFU -> "Received notification for silenced incident"
    | NewNotification StartEscalation -> "Notified"
    | Escalate step ->
        "Escalated to "^ Escalation.string_of_victims step.victims
    | Outcry (name, contact) ->
        "Contacted "^ name ^" via "^ Contact.to_string contact
    | Ack -> "Acknowledged"
    | Stop Notification -> "Notified to stop"
    | Stop (Manual reason) -> "Manual stop: "^ reason
    | Stop Timeout -> "Timed out"

  type log =
    { current_time : float ;
      event_time : float ;
      event : event } [@@ppp PPP_JSON]

  type t =
    { id : int ; (* Used for acknowledgments *)
      name : string ;
      started_firing : float ; (* event time *)
      mutable stopped_firing : float option ; (* event time *)
      team : string ;
      title : string ;
      text : string ;
      (* The smaller the int, the most important the alert. 0 is the most
       * important one. *)
      importance : int ;
      (* When we _received_ the alert and started the escalation. Hopefully
       * not too long after [started_firing]. *)
      received : float ;
      mutable escalation : Escalation.t option ;
      (* Log for that alert, most recent first: *)
      mutable log : log list } [@@ppp PPP_JSON]

  type stop_req =
    { alert_id : int ; reason : string } [@@ppp PPP_JSON]
end

module Incident =
struct
  type t =
    { id : int ;
      mutable alerts : Alert.t list } [@@ppp PPP_JSON]

  let team_of i =
    match i.alerts with
    | [] -> ""
    | a :: _ -> a.Alert.team

  let started i =
    List.fold_left (fun mi a ->
        min mi a.Alert.started_firing
      ) max_float i.alerts

  let stopped i =
    List.fold_left (fun ma a ->
        match ma, a.Alert.stopped_firing with
        | None, x -> x
        | _, None -> ma
        | Some ma, Some ts -> Some (max ma ts)
      ) None i.alerts
end

module Inhibition =
struct
  type t =
    { (* Abstract identifier ; we want to be able to have several
         inhibits with the same prefix (to cover several time
         ranges). *)
      id : int [@ppp_default -1] ;
      mutable what : string ; (* all alerts starting with this prefix *)
      mutable start_date : float ;
      mutable stop_date : float ;
      who : string [@ppp_default "anonymous"] ;
      mutable why : string } [@@ppp PPP_JSON]

  type list_resp = (string * t list) list [@@ppp PPP_JSON]
end

module Team =
struct
  type escalation =
    { importance : int ;
      steps : Escalation.step list } [@@ppp PPP_JSON]

  type t =
    { name : string ;
      mutable members : string list ;
      escalations : escalation list ;
      mutable inhibitions : Inhibition.t list } [@@ppp PPP_JSON]

  type get_resp =
    { teams : t list ;
      default_team : string } [@@ppp PPP_JSON]

  type set_members = string list [@@ppp PPP_JSON]
end

module StaticConf =
struct
  (* Schedule: Who carries the pager when. The alerter needs to be provided
   * with just enough insight to be able to route the alerts that are coming
   * now and in the near future. Data on the past is unused. Ideally those
   * are provided by another tool, although an incorporated constraint solver
   * is supposed to do this at some point in the future.
   *
   * Notice that there is no team: if someone is in a team and is found to be
   * oncall at some time then he will be sent the alerts.  If you belong to
   * several teams but want to be oncall for only one then merely create
   * several identities. *)
  type schedule =
    { rank : int ;
      from : float ;
      oncaller : string } [@@ppp PPP_JSON]

  (* The part of the configuration that's static for the purpose of routing
   * alerts, and that can be easily exported/imported to backup/restore the
   * configuration. *)
  type t =
    { mutable oncallers : OnCaller.t list ;
      mutable teams : Team.t list ;
      (* The team that receives the alerts with no team set: *)
      mutable default_team: string ;
      schedule : schedule list } [@@ppp PPP_JSON]
end

(* Query returning ongoing incidents *)

module GetOngoing =
struct
  type resp = Incident.t list [@@ppp PPP_JSON]
end

(* Query for retrieving history of past alerting events *)

module GetHistory =
struct
  type time_range = LastSecs of float
                  | SinceUntil of (float * float) [@@ppp PPP_JSON]

  type req =
    { team : string option ;
      time_range : time_range } [@@ppp PPP_JSON]

  type resp = Incident.t list [@@ppp PPP_JSON]
end
