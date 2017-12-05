open Js_of_ocaml
module Html = Dom_html
open Engine
open RamenHtml
open WebHelpers
open JsHelpers
open AlerterSharedTypesJS_noPPP

(* Conversions from to Js *)

module Searchable =
struct
  let of_string s = (Js.string s)##toLowerCase

  type team =
    { name : string ;
      inhibitions : Inhibition.t list ;
      searchable_name : Js.js_string Js.t ;
      members : (string * Js.js_string Js.t) list }

  let of_team team =
    { name = team.GetTeam.name ;
      inhibitions = team.inhibitions ;
      searchable_name = of_string team.name ;
      members = List.map (fun m -> m, of_string m) team.members }
end

let inhibition_of_js js =
  let id = of_field js "id" identity
  and what = of_field js "what" Js.to_string
  and start_date = of_field js "start_date" Js.to_float
  and stop_date = of_field js "stop_date" Js.to_float
  and who = of_field js "who" Js.to_string
  and why = of_field js "why" Js.to_string in
  Inhibition.{ id ; what ; start_date ; stop_date ; who ; why }

let team_of_js js =
  let name = of_field js "name" Js.to_string
  and members = of_field js "members" (list_of_js Js.to_string)
  and inhibitions =
    of_field js "inhibitions" (list_of_js inhibition_of_js) in
  Searchable.of_team GetTeam.{ name ; members ; inhibitions }

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

let oncaller_of_js js =
  let name = of_field js "name" Js.to_string
  and contacts = of_field js "contacts" (array_of_js contact_of_js) in
  OnCaller.{ name ; contacts }

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

let log_event_of_js js =
  let open Alert in
  variant_of_js [
    "NewNotification",
      (fun js -> NewNotification (notif_outcome_of_js js)) ;
    "Escalate", (fun js -> Escalate (esc_step_of_js js)) ;
    "Outcry",
      (fun js -> Outcry (pair_of_js Js.to_string contact_of_js js)) ;
    "Ack", (fun _ -> Ack) ;
    "Stop", variant_of_js [
      "Notification", (fun _ -> Stop Notification) ;
      "Manual", (fun _ -> Stop Manual) ] ] js

let log_entry_of_js js =
  Alert.{
    current_time = of_field js "current_time" Js.to_float ;
    event_time = of_field js "event_time" Js.to_float ;
    event = of_field js "event" log_event_of_js }

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
    alerts = of_field i "alerts" (list_of_js alert_of_js) }

(*
 * States
 *)

(* Alerter state *)

let teams : Searchable.team list param = make_param "teams" []
let team_search = make_param "team search" ""

(* Team Selection: all of them or just one *)

type team_selection = AllTeams | SingleTeam of string
let team_selection = make_param "team selection" AllTeams

let is_team_selected sel team =
  match sel with
  | AllTeams -> true
  | SingleTeam n -> team = n

let fold_teams teams selection search_str init f =
  List.fold_left (fun prev team ->
      if is_team_selected selection team.Searchable.name &&
         (string_contains search_str team.searchable_name ||
          List.exists (fun (_, m) ->
              string_contains search_str m
            ) team.members)
      then
          f prev team
      else prev
    ) init teams

let fold_members team search_str init f =
  let team_match_search =
    string_contains search_str team.Searchable.searchable_name in
  List.fold_left (fun prev (member, searchable_member) ->
      if team_match_search || string_contains search_str searchable_member then
        f prev member
      else prev
    ) init team.members

let find_team teams name =
  List.find (fun t -> t.Searchable.name = name) teams

let oncallers : OnCaller.t option param Jstable.t = Jstable.create ()

let invalidate_oncaller name =
  let js_name = Js.string name in
  Jstable.remove oncallers js_name

let set_oncaller name oncaller_opt =
  let js_name = Js.string name in
  let previous = Jstable.find oncallers js_name in
  Js.Optdef.case previous
    (fun () ->
      let param_name = "oncaller "^ name in
      let param = make_param param_name oncaller_opt in
      Jstable.add oncallers js_name param ;
      param)
    (fun param ->
      set param oncaller_opt ;
      param)

(* Main views of the application: *)

type page = PageLive | PageTeam | PageHandOver | PageHistory | Reports
          | Configuration
let current_page = make_param "tab" PageLive

(* Incidents *)

let event_time = make_param "event time" 0.
let chart_now = make_param "current time" (now ())

let update_event_time_from_alert a =
  set event_time
    (List.fold_left (fun ma l ->
        max ma l.Alert.event_time
      ) event_time.value a.Alert.log)

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
        if is_team_selected team_selection a.Alert.team then
          f prev incident
        else prev
    ) init incidents

(* Inhibitions *)

let inhibit_what = make_param "inhibit what" ""
let inhibit_why = make_param "inhibit why" ""
let inhibit_start = make_param "inhibit start" ""
let inhibit_stop = make_param "inhibit stop" ""

let reset_inhibit_form () =
  set inhibit_what "" ;
  set inhibit_why "" ;
  set inhibit_start "" ;
  set inhibit_stop ""

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
    set chart_now (now ()) ;
    resync ())

let reload_oncaller name =
  let path = "/oncaller/"^ enc name in
  http_get path (fun resp ->
    let oncaller = oncaller_of_js resp in
    set_oncaller name (Some oncaller) |> ignore)

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
      let until = event_time.value in
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
  p [] [ text ("TODO: "^ what) ]

let live_incidents =
  with_param team_selection (fun tsel ->
    let with_team_col = tsel = AllTeams in
    with_param known_incidents (fun incidents ->
      let incident_rows =
        with_param selected_alert (fun sel_alert ->
          List.fold_left (fun prev i ->
            List.fold_left (fun prev a ->
              let with_action_cols = tsel = SingleTeam a.Alert.team
              and alert_txt = a.Alert.name in
              let ack _ =
                http_get ("/ack/"^ string_of_int a.id) (fun _ ->
                  reload_ongoing ())
              and stop _ =
                http_get ("/stop/"^ string_of_int a.id) (fun _ ->
                  reload_ongoing ()) in
              let need_ack = a.escalation <> None in
              let row =
                tr ~action:(fun _ ->
                    set selected_incident (Some i) ;
                    set selected_alert (Some a))
                  [ clss (
                    (match sel_alert with Some sa when sa == a -> "selected "
                                        | _ -> "") ^"actionable") ]
                  [ (if with_team_col then td [] [ text a.team ] else group []) ;
                    td [] [ text (date_of_ts a.started_firing) ] ;
                    td [] [ text alert_txt ] ;
                    td [] [
                      if need_ack then
                        if with_action_cols then
                           button ~action:ack
                             [ clss "icon actionable" ;
                               title "Acknowledge this alert" ]
                             [ text "Ack" ]
                        else
                          text "not acked"
                      else group [] ] ;
                    td [] [
                      if with_action_cols then
                        button ~action:stop
                          [ clss "icon actionable" ;
                            title "Terminate this alert" ]
                          [ text "Stop" ]
                      else group [] ] ] in
              row :: prev) prev i.Incident.alerts) [] incidents |>
              tbody []) in
      table
        [ clss "incidents" ]
        [ thead []
            [ tr []
              ((if with_team_col then th [] [ text "team" ] else group []) ::
              [ th [] [ text "since" ] ;
                th [] [ text "alert" ] ;
                th [] [] ]) ] ;
          incident_rows ]))

let selected_incident_detail =
  with_param selected_incident (function
  | None ->
    p [] [ text "Select an incident to see its composition" ]
  | Some i ->
    with_param selected_alert (fun sel_alert ->
      div []
        [ div
            [ clss "alert_list" ]
            [ h2 "alerts" ;
              group
                (List.map (fun a ->
                    let c =
                      match sel_alert with
                      | Some sa when sa == a -> "selected"
                      | _ -> "actionable" in
                    p ~action:(fun _ ->
                        set selected_alert (Some a))
                      [ clss c ] [ text a.Alert.name ]
                  ) i.Incident.alerts) ] ;
          match sel_alert with
          | None ->
            p [] [ text "Select an alert to see its history" ]
          | Some a ->
            div
              [ clss "log_list" ]
              [ h2 "log" ;
                table []
                  [ thead []
                      [ tr []
                        [ th [] [ text "sent" ] ;
                          th [] [ text "recvd" ] ;
                          th [] [ text "event" ] ] ] ;
                    tbody []
                      (List.rev a.Alert.log |>
                       List.map (fun l ->
                          tr []
                            [ td [] [ text (date_of_ts l.Alert.event_time) ] ;
                              td [] [ text (date_of_ts l.current_time) ] ;
                              td [] [ text (Alert.string_of_event l.event) ] ]
                        )) ] ] ]))

let chronology incidents dur relto_event =
  (* TODO: markers should have start/stop times *)
  let marker_of_event l =
    l.Alert.current_time, Alert.string_of_event l.event in
  let markers_of_alert a =
    let m =
      (a.Alert.started_firing, "Started") ::
      List.map marker_of_event a.log in
    match a.stopped_firing with
    | None -> m
    | Some t -> (t, "Stopped") :: m in
  let bars = List.map (fun i ->
    RamenChart.{
      start = Some (Incident.started i) ;
      stop = Incident.stopped i ;
      color = RamenColor.random_of_string (Incident.team_of i) ;
      markers = List.fold_left (fun prev a ->
          List.rev_append (markers_of_alert a) prev
        ) [] i.alerts }) incidents in
  (* Guess a good svg_width given the time range and min duration
   * between markers. *)
  let ts =
    List.fold_left (fun i bar ->
        List.fold_left (fun prev (t, _) -> t :: prev
          ) i bar.RamenChart.markers
      ) [] bars |>
    List.fast_sort compare in
  let past_marks = Array.create 4 0. in (* past 4 marks *)
  let fst_lst_mi =
    List.fold_left (fun (fst_lst_mi) t ->
      match fst_lst_mi with
      | None -> (* first mark *)
        Some (t, None, 0)
      | Some (fst, mi, idx) ->
        let idx' = (idx + 1) mod (Array.length past_marks) in
        let dt = t -. past_marks.(idx) in
        past_marks.(idx) <- t ;
        Some (
          fst,
          (if dt < min_float then mi else
           match mi with None -> Some dt
                       | Some mi -> Some (min mi dt)),
          idx')
      ) None ts in
  let min_svg_width = 800. and max_svg_width = 4000. and min_pix = 30. in
  let svg_width = match fst_lst_mi with
    | None | Some (_, None, _) -> 800. (* 0 or 1 mark? wtv. *)
    | Some (fst, Some min_sp, idx) ->
      let prev_idx =
        (idx - 1 + Array.length past_marks) mod (Array.length past_marks) in
      let lst = past_marks.(prev_idx) in
      let w = min_pix *. (lst -. fst) /. min_sp in
      if w < min_svg_width then min_svg_width else
      if w > max_svg_width then max_svg_width else w in
  let margin_vert = 15.
  and bar_height = 36. in
  let svg_height =
    2. *. margin_vert +. 20. (* axis approx height *) +.
    float_of_int (List.length bars) *. bar_height in
  div
    [ clss "chronology" ]
    [ div
        [ clss "wide" ]
        [ svg svg_width svg_height
            [ clss "chart" ]
            [ (if relto_event then with_param event_time
              else with_param chart_now) (fun base_time ->
                RamenChart.chronology
                  ~svg_width:svg_width ~svg_height:svg_height
                  ~margin_bottom:(margin_vert+.10.) (* for the scrollbar *)
                  ~margin_top:margin_vert
                  ~margin_left:0. ~margin_right:70.
                  ~string_of_t:RamenFormats.((timestamp string_of_timestamp).to_label)
                  (* TODO: Wouldn't that be easier if we had one bar per alert, grouped
                   * by incident? *)
                  ~click_on_bar:(fun i ->
                    let incident = List.nth incidents i in
                    set selected_incident (Some incident) ;
                    let first_alert =
                      try Some (List.hd incident.alerts)
                      with Failure _ -> None in
                    set selected_alert first_alert)
                  bars (base_time -. dur) base_time) ] ] ;
      selected_incident_detail ]

let stfu =
  with_param team_selection (function
    | AllTeams -> group []
    | SingleTeam team ->
      with_param teams (fun teams ->
        match find_team teams team with
        | exception Not_found -> (* better not ask *) group []
        | team ->
          let now = now () in
          match List.fold_left (fun o i ->
              if i.Inhibition.what = "" then
                match o with None -> Some i.stop_date
                           | Some t -> Some (max t i.stop_date)
              else o) None team.inhibitions with
          | Some t when t > now ->
             p [ clss "stfu" ]
               [ text ("Everything is silenced until "^ date_of_ts t ^
                       " for team "^ team.name) ]
          | _ ->
            button ~action:(fun _ ->
                let path = "/stfu/"^ enc team.name in
                http_get path (fun _ -> reload_teams ()))
              [ clss "actionable" ]
              [ text ("Silence EVERYTHING for team "^ team.name) ]))

let page_live =
  (* TODO: STFU button *)
  div
    [ id "page-live" ]
    [ h2 "Opened incidents" ;
      live_incidents ;
      stfu ;
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
  div
    [ clss "inhibition" ]
    (* TODO: if i.what = "" then display merely "STFU until ..." *)
    [ text ("alert "^ i.what ^" up to "^ date_of_ts i.stop_date ^
            " by "^ i.who ^" since "^ date_of_ts i.start_date ^
            " because: "^ i.why) ]

let input_text ~label ~size ~placeholder param =
  elmt "label" []
    [ text (label ^": ") ;
      input ~action:(fun v -> set param v ; resync ())
        [ attr "type" "text" ;
          attr "size" (string_of_int size) ;
          attr "placeholder" placeholder ;
          attr "value" param.value ] ]

let team_member teams team name =
  let js_name = Js.string name in
  let oncaller_p =
    let prev = Jstable.find oncallers js_name in
    Js.Optdef.case prev
      (fun () ->
        (* We want to grab the param right now so we can reference it
         * in our output: *)
        let param = set_oncaller name None in
        reload_oncaller name ;
        param)
      identity in
  with_param oncaller_p (function
    | None -> text ("loading "^ name ^"...")
    | Some oncaller ->
      let other_teams =
        List.filter (fun t ->
            t.Searchable.name <> team.Searchable.name &&
            List.exists (fun (m, _) -> m = name) t.members
          ) teams in
      div [ clss "oncaller-tile" ]
          [ p [ clss "oncaller-name" ] [ text name ] ;
            ul [ clss "oncaller-contacts" ]
               ( Array.fold_left (fun lst c ->
                     li [] [ text (Contact.to_string c) ] :: lst
                   ) [] oncaller.OnCaller.contacts ) ;
            if other_teams = [] then group [] else
            p [ clss "oncaller-other-teams" ]
              ( text "also in teams: " ::
                List.map (fun t ->
                    span [ clss "oncaller-other-team" ]
                         [ text t.Searchable.name ]
                  ) other_teams ) ])

let page_team teams search_str tsel team =
  let open Searchable in
  div
    [ clss "team-info" ]
    [ (let action =
        if tsel = SingleTeam team.name then
          (fun _ -> reset_inhibit_form () ;
                    set team_selection AllTeams)
        else
          (fun _ -> reset_inhibit_form () ;
                    set team_selection (SingleTeam team.name)) in
      h2 ~action ~attrs:[clss "actionable"] ("Team "^ team.name)) ;
      h3 "Members" ;
      ul [] (fold_members team search_str [] (fun prev m ->
        li [] [ team_member teams team m ] :: prev)) ;
      h3 "Inhibitions" ;
      (if tsel = SingleTeam team.name then
        let new_inhibition =
          with_param inhibit_what (fun _what ->
            with_param inhibit_why (fun _why ->
              with_param inhibit_start (fun _start ->
                with_param inhibit_stop (fun _stop ->
                    li [] [
                      input_text ~label:"alert" ~size:16 ~placeholder:"alert name (or prefix)" inhibit_what ;
                      input_text ~label:"reason" ~size:32 ~placeholder:"why should this be silenced?" inhibit_why ;
                      input_text ~label:"from" ~size:9 ~placeholder:"timestamp" inhibit_start ;
                      input_text ~label:"to" ~size:9 ~placeholder:"timestamp" inhibit_stop ;
                      button ~action:(fun _ ->
                          (* TODO: catch errors and display an err msg beside
                           * the field if not convertible: *)
                          let start = float_of_string inhibit_start.value
                          and stop = float_of_string inhibit_stop.value in
                          let req =
                            object%js
                              val what = Js.string inhibit_what.value
                              val why = Js.string inhibit_why.value
                              val start_date_ = start
                              val stop_date_ = stop
                            end in
                          let path = "/inhibit/add/"^ enc team.name in
                          http_post path req (fun _ ->
                            reset_inhibit_form () ;
                            reload_teams ()))
                        [ clss "actionable" ] [ text "inhibit" ] ])))) in
        ul [] (new_inhibition :: List.map (fun i ->
            li []
               [ inhibition i ;
                 let now = now () in
                 if i.stop_date > now then
                   button ~action:(fun _ ->
                       let req =
                         object%js
                           val id = i.id
                           val what = Js.string i.what
                           val why = Js.string "deleted from GUI"
                           val start_date_ = js_of_float i.start_date
                           val stop_date_ = now
                         end in
                       let path = "/inhibit/edit/"^ enc team.name in
                       http_post path req (fun _ -> reload_teams ()))
                     [ clss "actionable" ] [ text "delete" ]
                 else group [] ]
          ) team.inhibitions)
      else (* team is not selected *)
        ul [] (List.map (fun i ->
          li [] [ inhibition i ]) team.inhibitions)) ]

let up_down_conf =
  div [ id "up-down-conf" ]
    [ p []
        [ a [ href "/config.db" ]
            [ text "download database" ] ] ;
      p []
        [ elmt "form"
          [ attr "action" "/config.db" ;
            attr "method" "POST" ;
            attr "enctype" "multipart/form-data" ]
          [ input [ attr "type" "file" ;
                    attr "name" "config.db" ] ;
            button [ attr "type" "submit" ]
                   [ text "Upload" ] ] ] ]

let page_teams =
  with_param teams (fun teams ->
    with_param team_selection (fun tsel ->
      with_param team_search (fun search_str ->
        let search_str = Searchable.of_string search_str in
        group
          [ div
              [ clss "searchbox" ]
              [ input_text ~label:"🔍" ~size:8 ~placeholder:"search"
                           team_search ] ;
            div
              [ clss "team-list" ]
              (fold_teams teams tsel search_str [] (fun prev team ->
                 page_team teams search_str tsel team :: prev)) ;
              up_down_conf ])))

let page_hand_over = todo "hand over"

let page_reports = todo "reports"

let page_config =
  div [ id "configuration" ]
      [ up_down_conf ]

let page_history =
  with_param known_incidents (fun incidents ->
    div []
      [ h2 "Chronology" ;
        time_selector ~action:reload_history histo_duration histo_relto ;
        with_param histo_duration (fun dur ->
          with_param histo_relto (fun relto_event ->
            chronology incidents dur relto_event)) ])

let tab label page =
  with_param current_page (fun cp ->
    div ~action:(fun _ ->
        set current_page page ;
        reload_for_current_page ())
      [ if cp = page then clss "tab selected"
                     else clss "tab actionable" ]
      [ p [] [ text label ] ])

let menu =
  div
    [ clss "tabs" ]
    [ tab "Live" PageLive ;
      (*tab "Hand Over" PageHandOver ;*)
      tab "History" PageHistory ;
      with_param team_selection (function
        | AllTeams -> tab "Teams" PageTeam
        | SingleTeam t -> tab ("Team "^ t) PageTeam) ;
      (*tab "Reports" Reports ;*)
      (*tab "Configuration" Configuration*) ]

let page =
  with_param current_page (function
    | PageLive -> page_live
    | PageTeam -> page_teams
    | PageHandOver -> page_hand_over
    | PageHistory -> page_history
    | Reports -> page_reports
    | Configuration -> page_config)

let dom =
  div [] [ menu ; page ]

let () =
  reload_for_current_page () ;
  Html.window##setInterval
    (Js.wrap_callback reload_for_current_page) 5_137. |>
  ignore ;
  start dom
