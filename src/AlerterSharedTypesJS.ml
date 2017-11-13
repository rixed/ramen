module Inhibition =
struct
  type t =
    { alert : string ; (* all alerts starting with this prefix *)
      end_date : float ;
      who : string ;
      why : string } [@@ppp PPP_JSON]
end

type team_resp =
  { team : string ;
    members : string list ;
    inhibitions : Inhibition.t list } [@@ppp PPP_JSON]

type teams_resp = team_resp list [@@ppp PPP_JSON]
