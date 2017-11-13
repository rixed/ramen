type team_resp =
  { team : string ; members : string list } [@@ppp PPP_JSON]

type teams_resp = team_resp list [@@ppp PPP_JSON]
