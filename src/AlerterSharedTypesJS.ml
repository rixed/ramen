module Alert =
struct
  type t =
    { name : string ;
      time : float ;
      team : string ;
      title : string ;
      text : string ;
      (* The smaller the int, the most important the alert. 0 is the most
       * important one. *)
      importance : int ;
      (* When we _received_ the alert and started the escalation. Hopefully
       * not too long after [time]. *)
      received : float ;
      mutable stopped : float option } [@@ppp PPP_JSON]
end

module Incident =
struct
  type t =
    { id : int ;
      mutable alerts : Alert.t list ;
      mutable stfu : bool } [@@ppp PPP_JSON]
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
  type date_range = LastSecs of float
                  | SinceUntil of (float * float) [@@ppp PPP_JSON]

  type req =
    { team : string option ;
      date_range : date_range } [@@ppp PPP_JSON]

  type resp = Incident.t list [@@ppp PPP_JSON]
end
