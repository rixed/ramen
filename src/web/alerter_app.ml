open Js_of_ocaml
module Html = Dom_html
open Engine
open WebHelpers
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
    started_firing = of_field js "started_firing" Js.to_float ;
    team = of_field js "team" Js.to_string ;
    title = of_field js "title" Js.to_string ;
    text = of_field js "text" Js.to_string ;
    importance = of_field js "importance" identity ;
    received = of_field js "received" Js.to_float ;
    stopped_firing = of_opt_field js "stopped_firing" Js.to_float ;
    escalation = of_opt_field js "escalation" escalation_of_js ;
    log = of_field js "log" (list_of_js log_entry_of_js) }

let incident_of_js i =
  Incident.{
    id = of_field i "id" identity ;
    alerts = of_field i "alerts" (list_of_js alert_of_js) ;
    stfu = of_field i "stfu" Js.to_bool }

(*
 * States
 *)

(* Alerter state *)

let teams : GetTeam.resp param = make_param "teams" []

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

let event_time = ref 0.

let update_event_time_from_alert a =
  event_time :=
    List.fold_left (fun ma (t, _) ->
        max ma t
      ) !event_time a.Alert.log

let update_event_time_from_incident i =
  List.iter update_event_time_from_alert i.Incident.alerts

(* Whatever the page, all incidents are stored in here, so that all
 * references to incidents (selected_incident...) points here: *)
let known_incidents = make_param "ongoing" []

(* Individually selected incidents and alerts: *)
let selected_incident = make_param "selected incident" None
let selected_alert = make_param "selected alert" None

let set_known_incidents incidents =
  set known_incidents incidents ;
  option_may (fun sel ->
      match List.find (fun i ->
              i.Incident.id = sel.Incident.id) incidents with
      | exception Not_found ->
        set selected_incident None ;
        set selected_alert None
      | i ->
        set selected_incident (Some i) ;
        option_may (fun sel ->
            set selected_alert
              (match List.find (fun a ->
                       a.Alert.id = sel.Alert.id) i.Incident.alerts with
              | exception Not_found -> None
              | a -> Some a)
          ) selected_alert.value
    ) selected_incident.value

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

let histo_relto = make_param "history rel.to" true

(*
 * Reload
 *)

let reload_ongoing () =
  let url =
    match team_selection.value with
    | AllTeams -> "/ongoing"
    | SingleTeam t -> "/ongoing/"^ enc t in
  http_get url (fun resp ->
    let incidents = list_of_js incident_of_js resp in
    set_known_incidents incidents ;
    List.iter update_event_time_from_incident incidents ;
    resync ())

let reload_teams () =
  http_get "/teams" (fun resp ->
    let roster = list_of_js team_of_js resp in
    set teams roster ;
    resync ())

let reload_history () =
  let url = match team_selection.value with
    | AllTeams -> "/history"
    | SingleTeam t -> "/history/"^ enc t in
  let date_range =
    if histo_relto.value then (
      let until = !event_time in
      let since = until -. histo_duration.value in
      "range="^ enc (string_of_float since ^","^ string_of_float until)
    ) else
      "last="^ enc (string_of_float histo_duration.value) in
  let url = url ^"?"^ date_range in
  http_get url (fun resp ->
    let incidents = list_of_js incident_of_js resp in
    List.iter update_event_time_from_incident incidents ;
    set_known_incidents incidents)

let reload_for_current_page () =
  match current_page.value with
  | PageLive -> reload_ongoing ()
  | PageHistory -> reload_history ()
  | PageTeam -> reload_teams ()
  | _ -> ()

(*
 * DOM
 *)

let todo what =
  p [ text ("TODO: "^ what) ]

let live_incidents =
  with_param team_selection (fun tsel ->
    let with_team_col = tsel = AllTeams in
    with_param known_incidents (fun incidents ->
      let incident_rows =
        List.fold_left (fun prev i ->
          List.fold_left (fun prev a ->
            let alert_txt = a.Alert.name in
            let ack _ =
              http_get ("/ack/"^ string_of_int a.id) (fun _ ->
                reload_ongoing ()) in
            let need_ack = a.escalation <> None in
            let row =
              (if with_team_col then td [ text a.team ] else group []) ::
              [ td [ text (date_of_ts a.started_firing) ] ;
                td [ text alert_txt ] ;
                td [ if need_ack then
                       button ~action:ack
                         [ clss "icon actionable" ;
                           attr "title" "Acknowledge this alert" ;
                           text "Ack" ]
                     else group [] ] ] |> tr in
            row :: prev) prev i.Incident.alerts) [] incidents in
      table
        [ clss "incidents" ;
          thead
            [ tr
              ((if with_team_col then th [ text "team" ] else group []) ::
              [ th [ text "since" ] ;
                th [ text "alert" ] ;
                th [] ]) ] ;
          tbody incident_rows ]))

let selected_incident_detail =
  with_param selected_incident (function
  | None ->
    p [ text "Select an incident to see its composition" ]
  | Some i ->
    with_param selected_alert (fun sel_alert ->
      div
        [ div
            [ clss "alert_list" ;
              h2 "alerts" ;
              group
                (List.map (fun a ->
                    let c =
                      match sel_alert with
                      | Some sa when sa == a -> "selected"
                      | _ -> "actionable" in
                    p ~action:(fun _ ->
                        set selected_alert (Some a))
                      [ clss c ; text a.Alert.name ]
                  ) i.Incident.alerts) ] ;
          match sel_alert with
          | None ->
            p [ text "Select an alert to see its history" ]
          | Some a ->
            div
              [ clss "log_list" ;
                h2 "log" ;
                table
                  [ tbody
                      (List.rev a.Alert.log |>
                       List.map (fun (t, e) ->
                          tr [ th [ text (date_of_ts t) ] ;
                               td [ text (Alert.string_of_event e) ] ]
                        )) ] ] ]))

let chronology incidents dur relto_event =
  let marker_of_event (ts, ev) = ts, Alert.string_of_event ev in
  let markers_of_alert a =
    let m =
      (a.Alert.started_firing, "Started") ::
      List.map marker_of_event a.log in
    match a.stopped_firing with
    | None -> m
    | Some t -> (t, "Stopped") :: m in
  let bars = List.map (fun i ->
    Chart.{
      start = Some (Incident.started i) ;
      stop = Incident.stopped i ;
      color = Color.random_of_string (Incident.team_of i) ;
      markers = List.fold_left (fun prev a ->
          List.rev_append (markers_of_alert a) prev
        ) [] i.alerts }) incidents
  and margin_vert = 15.
  and bar_height = 36.
  and svg_width = 800. in
  let svg_height =
    2. *. margin_vert +. 20. (* axis approx height *) +.
    float_of_int (List.length bars) *. bar_height in
  div
    [ clss "chronology" ;
      div
        [ clss "wide" ;
          svg
            [ clss "chart" ;
              attr "style"
                ("width:"^ string_of_float svg_width ^
                 "; height:"^ string_of_float svg_height ^
                 "; min-height:"^ string_of_float svg_height ^";") ;
              let base_time =
                if relto_event && !event_time > 0. then !event_time
                else now () in
              Chart.chronology
                ~svg_width:svg_width ~svg_height:svg_height
                ~margin_bottom:(margin_vert+.10.) (* for the scrollbar *)
                ~margin_top:margin_vert
                ~margin_left:0. ~margin_right:70.
                ~click_on_bar:(fun i ->
                  let incident = List.nth incidents i in
                  set selected_incident (Some incident) ;
                  let first_alert =
                    try Some (List.hd incident.alerts)
                    with Failure _ -> None in
                  set selected_alert first_alert)
                bars (base_time -. dur) base_time ] ] ;
      selected_incident_detail ]

let page_live =
  div
    [ id "page-live" ;
      h2 "Opened incidents" ;
      live_incidents ;
      h2 "Chronology" ;
      with_param known_incidents (fun incidents ->
        let n = now () in
        let min_ts =
          List.fold_left (fun mi i ->
            min mi (Incident.started i)) n incidents in
        let dur = n -. min_ts in
        chronology incidents dur false) ]

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
  with_param known_incidents (fun incidents ->
    div
      [ h2 "Chronology" ;
        time_selector ~action:reload_history histo_duration histo_relto ;
        with_param histo_duration (fun dur ->
          with_param histo_relto (fun relto_event ->
            chronology incidents dur relto_event)) ])

let tab label page =
  div ~action:(fun _ ->
      set current_page page ;
      reload_for_current_page ())
    [ with_param current_page (fun p ->
        if p = page then clss "tab selected"
                    else clss "tab actionable") ;
      p [ text label ] ]

let menu =
  div
    [ clss "tabs" ;
      tab "Live" PageLive ;
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
  reload_for_current_page () ;
  Html.window##setInterval
    (Js.wrap_callback reload_for_current_page) 5_137. |>
  ignore ;
  start dom
