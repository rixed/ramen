open Js_of_ocaml
module Html = Dom_html
open Engine
open AlerterSharedTypesJS_noPPP

(* Conversions from to Js *)

let inhibition_of_js js =
  let what = of_field js "what" Js.to_string
  and start_date = of_field js "start_date" Js.to_float
  and end_date = of_field js "end_date" Js.to_float
  and who = of_field js "who" Js.to_string
  and why = of_field js "why" Js.to_string in
  Inhibition.{ what ; start_date ; end_date ; who ; why }

let team_of_js js =
  let name = of_field js "name" Js.to_string
  and members = of_field js "members" (list_of_js Js.to_string)
  and inhibitions =
    of_field js "inhibitions" (list_of_js inhibition_of_js) in
  GetTeam.{ name ; members ; inhibitions }

let contact_of_js =
  let open Contact in
  let email_of_js js =
    Email {
      to_ = of_field js "to" Js.to_string ;
      cc = of_opt_field js "cc" Js.to_string |> option_def "" ;
      bcc = of_opt_field js "bcc" Js.to_string |> option_def "" } in
  variant_of_js [
    "Console", (fun _ -> Console) ;
    "SysLog", (fun _ -> SysLog) ;
    "Email", email_of_js ;
    "SMS", (fun js -> SMS (Js.to_string js)) ]

let notif_outcome_of_js js =
  let open Alert in
  variant_of_js [
    "Duplicate", (fun _ -> Duplicate) ;
    "Inhibited", (fun _ -> Inhibited) ;
    "STFU", (fun _ -> STFU) ;
    "StartEscalation", (fun _ -> StartEscalation) ] js

let esc_step_of_js js =
  Escalation.{
    timeout = of_field js "timeout" Js.to_float ;
    victims = of_field js "victims" (array_of_js identity) }

let escalation_of_js js =
  Escalation.{
    steps = of_field js "steps" (array_of_js esc_step_of_js) ;
    attempt = of_field js "attempt" identity ;
    last_sent = of_field js "last_sent" Js.to_float }

let log_event_of_js =
  let open Alert in
  variant_of_js [
    "NewNotification",
      (fun js -> NewNotification (notif_outcome_of_js js)) ;
    "Escalate", (fun js -> Escalate (esc_step_of_js js)) ;
    "Outcry",
      (fun js -> Outcry (pair_of_js Js.to_string contact_of_js js)) ;
    "Ack", (fun _ -> Ack) ;
    "Stop", (fun _ -> Stop) ]

let log_entry_of_js js =
  pair_of_js Js.to_float log_event_of_js js

let alert_of_js js =
  Alert.{
    id = of_field js "id" identity ;
    name = of_field js "name" Js.to_string ;
    time = of_field js "time" Js.to_float ;
    team = of_field js "team" Js.to_string ;
    title = of_field js "title" Js.to_string ;
    text = of_field js "text" Js.to_string ;
    importance = of_field js "importance" identity ;
    received = of_field js "received" Js.to_float ;
    stopped = of_opt_field js "stopped" Js.to_float ;
    escalation = of_opt_field js "escalation" escalation_of_js ;
    log = of_field js "log" (list_of_js log_entry_of_js) }

let incident_of_js i =
  Incident.{
    id = of_field i "id" identity ;
    alerts = of_field i "alerts" (list_of_js alert_of_js) ;
    stfu = of_field i "stfu" Js.to_bool ;
    started = of_field i "started" Js.to_float ;
    stopped = of_opt_field i "stopped" Js.to_float }

(*
 * States
 *)

(* Alerter state *)

let teams : GetTeam.resp param = make_param "teams" []

let reload_teams () =
  http_get "/teams" (fun resp ->
    let roster = list_of_js team_of_js resp in
    set teams roster ;
    resync ())

(* Team Selection: all of them or just one *)

type team_selection = AllTeams | SingleTeam of string
let team_selection = make_param "team selection" AllTeams

let team_is_selected sel team =
  match sel with
  | AllTeams -> true
  | SingleTeam n -> team = n

let fold_teams teams selection init f =
  List.fold_left (fun prev team ->
      if team_is_selected selection team.GetTeam.name then
        f prev team
      else prev
    ) init teams

(* Main views of the application: *)

type page = PageLive | PageTeam | PageHandOver | PageHistory
let current_page = make_param "tab" PageLive

(* Incidents *)

let ongoing = make_param "ongoing" []

let reload_ongoing () =
  let url =
    match team_selection.value with
    | AllTeams -> "/ongoing"
    | SingleTeam t -> "/ongoing/"^ enc t in
  http_get url (fun resp ->
    let incidents = list_of_js incident_of_js resp in
    set ongoing incidents ;
    resync ())

let fold_incidents incidents team_selection init f =
  List.fold_left (fun prev incident ->
      match incident.Incident.alerts with
      | [] -> prev
      | a :: _ ->
        if team_is_selected team_selection a.Alert.team then
          f prev incident
        else prev
    ) init incidents

(* History *)

let histo_duration = make_param "history duration" (3. *. 3600.)

let history = make_param "history" []

let reload_history () =
  let url = match team_selection.value with
    | AllTeams -> "/history"
    | SingleTeam t -> "/history/"^ enc t in
  let date_range =
    "last="^ enc (string_of_float histo_duration.value) in
  let url = url ^"?"^ date_range in
  http_get url (fun resp ->
    let incidents = list_of_js incident_of_js resp in
    set history incidents)

(*
 * DOM
 *)

let todo what =
  p [ text ("TODO: "^ what) ]

let live_incidents =
  with_param team_selection (fun tsel ->
    let with_team_col = tsel = AllTeams in
    with_param ongoing (fun incidents ->
      let incident_rows =
        List.fold_left (fun prev i ->
          let alerts_txt, min_time, team =
            List.fold_left (fun (prev_txt, prev_time, _) a ->
                prev_txt ^(if prev_txt <> "" then ", " else "")^
                  a.Alert.name,
                min prev_time a.time,
                Some a.Alert.team
              ) ("", max_float, None) i.Incident.alerts in
          let team = option_get team in
          let row =
            (if with_team_col then td [ text team ] else group []) ::
            [ td [ text (date_of_ts min_time) ] ;
              td [ text alerts_txt ] ] |> tr in
          row :: prev) [] incidents in
      table
        [ clss "incidents" ;
          thead
            [ tr
              ((if with_team_col then th [ text "team" ] else group []) ::
              [ th [ text "since" ] ;
                th [ text "alerts" ] ]) ] ;
          tbody incident_rows ]))

let chronology =
  todo "chronology"

let incident_detail = todo "detail"

let page_live =
  div
    [ id "page-live" ;
      live_incidents ;
      chronology ;
      incident_detail ]

let inhibition i =
  let open Inhibition in
  text ("alert "^ i.what ^" up to "^ date_of_ts i.end_date ^
        " by "^ i.who ^" since "^ date_of_ts i.start_date ^
        " because: "^ i.why)

let page_team team =
  (* TODO: edit inhibitions *)
  let open GetTeam in
  div
    [ clss "team-info" ;
      h2 ("Team "^ team.name) ;
      h3 "Members" ;
      ul (List.map (fun m -> li [ text m ]) team.members) ;
      h3 "Inhibitions" ;
      ul (List.map (fun i -> li [ inhibition i ]) team.inhibitions) ]

let page_teams =
  with_param teams (fun teams ->
    with_param team_selection (fun sel ->
      div
        (clss "team-list" ::
         fold_teams teams sel [] (fun prev team ->
           page_team team :: prev))))

let page_hand_over = todo "hand over"

let page_history =
  with_param history (fun incidents ->
    let bars = List.map (fun i ->
      Chart.{
        start = Some i.Incident.started ;
        stop = i.stopped ;
        color = Color.random_of_string (Incident.team_of i) ;
        markers = [] }) incidents
    and margin_vert = 15. and margin_horiz = 5.
    and bar_height = 16.
    and svg_width = 800. in
    let svg_height =
      2. *. margin_vert +. 20. (* axis approx height *) +.
      float_of_int (List.length bars) *. bar_height in
    div
      [ time_selector histo_duration ;
        svg
          [ clss "chart" ;
            attr "style"
              ("width:"^ string_of_float svg_width ^
               "; height:"^ string_of_float svg_height ^
               "; min-height:"^ string_of_float svg_height ^";") ;
            with_param histo_duration (fun dur ->
              let n = now () in
              Chart.chronology ~svg_width:svg_width
                               ~svg_height:svg_height
                               ~margin_bottom:margin_vert
                               ~margin_top:margin_vert
                               ~margin_left:margin_horiz
                               ~margin_right:margin_horiz
                               bars (n -. dur) n) ] ])

let tab label page =
  div ~action:(fun _ -> set current_page page)
    [ with_param current_page (fun p ->
        if p = page then clss "tab selected"
                    else clss "tab actionable") ;
      p [ text label ] ]

let menu =
  div
    [ tab "Live" PageLive ;
      tab "Team" PageTeam ;
      tab "Hand Over" PageHandOver ;
      tab "History" PageHistory ]

let page =
  with_param current_page (function
    | PageLive -> page_live
    | PageTeam -> page_teams
    | PageHandOver -> page_hand_over
    | PageHistory -> page_history)

let dom =
  div [ menu ; page ]

let () =
  start dom ;
  reload_teams () ;
  reload_ongoing () ;
  reload_history ()
