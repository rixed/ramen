module Contact =
struct
  type t =
    | Console
    | SysLog
    | Email of { to_ : string [@ppp_rename "to"] ;
                 cc : string [@ppp_default ""] ;
                 bcc : string [@ppp_default ""] }
    | SMS of string [@@ppp PPP_JSON]
end

module Escalation =
struct
  type step =
    { timeout : float ;
      (* Who to ring from the oncallers squad *)
      victims : int array } [@@ppp PPP_JSON]

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

  type event =
    | NewNotification of notification_outcome
    | Escalate of Escalation.step
    | Outcry of (string * Contact.t)
    (* TODO: we'd like to know the origin of this ack. *)
    | Ack
    | Stop [@@ppp PPP_JSON]

  type t =
    { id : int ; (* Used for acknowledgments *)
      name : string ;
      started_firing : float ;
      mutable stopped_firing : float option ;
      team : string ;
      title : string ;
      text : string ;
      (* The smaller the int, the most important the alert. 0 is the most
       * important one. *)
      importance : int ;
      (* When we _received_ the alert and started the escalation. Hopefully
       * not too long after [time]. *)
      received : float ;
      mutable escalation : Escalation.t option ;
      (* Log for that alert, most recent first: *)
      mutable log : (float * event) list } [@@ppp PPP_JSON]
end

module Incident =
struct
  type t =
    { id : int ;
      mutable alerts : Alert.t list ;
      mutable stfu : bool } [@@ppp PPP_JSON]

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
    { what: string ; (* all alerts starting with this prefix *)
      start_date : float ;
      end_date : float ;
      who : string ;
      why : string } [@@ppp PPP_JSON]
end

module GetTeam =
struct
  type team =
    { name : string ;
      members : string list ;
      inhibitions : Inhibition.t list } [@@ppp PPP_JSON]

  type resp = team list [@@ppp PPP_JSON]
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
