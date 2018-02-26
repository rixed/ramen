open Js_of_ocaml
module Html = Dom_html
open Engine
open RamenHtml
open WebHelpers
open JsHelpers
open AlerterSharedTypesJS_noPPP
open Style

(* Conversions from to Js *)

(* Editable versions of some common types (with searchable strings) *)
module Editable =
struct
  let of_string s = (Js.string s)##toLowerCase

  module Team =
  struct
    type t =
      { name : string ;
        searchable_name : Js.js_string Js.t ;
        inhibitions : Inhibition.t list ;
        mutable members : (string * Js.js_string Js.t) list }

    let of_team team =
      { name = team.Team.name ;
        inhibitions = team.inhibitions ;
        searchable_name = of_string team.name ;
        members = List.map (fun m -> m, of_string m) team.members }
  end

  module OnCaller =
  struct
    type contact =
      { contact : Contact.t ;
        to_del : bool ;
        (* if None this is a new entry not backed up yet *)
        old : Contact.t option }

    type t =
      { name : string ;
        contacts : contact list }
  end
end

let inhibition_of_js js =
  let id = of_field js "id" identity
  and what = of_field js "what" Js.to_string
  and start_date = of_field js "start_date" Js.to_float
  and stop_date = of_field js "stop_date" Js.to_float
  and who = of_field js "who" Js.to_string
  and why = of_field js "why" Js.to_string in
  Inhibition.{ id ; what ; start_date ; stop_date ; who ; why }

let escalation_step_of_js js =
  Escalation.{
    timeout = of_field js "timeout" Js.to_float ;
    victims = of_field js "victims" (array_of_js identity) }

let escalation_of_js js =
  Team.{
    importance = of_field js "importance" identity ;
    steps = of_field js "steps" (list_of_js escalation_step_of_js) }

let team_of_js js =
  let name = of_field js "name" Js.to_string
  and members = of_field js "members" (list_of_js Js.to_string)
  and escalations =
    of_field js "escalations" (list_of_js escalation_of_js)
  and inhibitions =
    of_field js "inhibitions" (list_of_js inhibition_of_js) in
  Editable.Team.of_team Team.{
    name ; members ; escalations ; inhibitions }

let teams_get_of_js js =
  of_field js "teams" (list_of_js team_of_js),
  of_field js "default_team" Js.to_string

let contact_of_js =
  let open Contact in
  let email_of_js js =
    Email {
      to_ = of_field js "to" Js.to_string ;
      cc = of_opt_field js "cc" Js.to_string |> option_def "" ;
      bcc = of_opt_field js "bcc" Js.to_string |> option_def "" }
  and sqlite_of_js js =
    Sqlite {
      file = of_field js "file" Js.to_string ;
      insert = of_field js "insert" Js.to_string ;
      create = of_field js "create" Js.to_string } in
  variant_of_js [
    "Console", (fun _ -> Console) ;
    "SysLog", (fun _ -> SysLog) ;
    "Email", email_of_js ;
    "SMS", (fun js -> SMS (Js.to_string js)) ;
    "Sqlite", sqlite_of_js ]

let oncaller_of_js js =
  let name = of_field js "name" Js.to_string
  and contacts = of_field js "contacts" (list_of_js (fun js ->
    let open Editable.OnCaller in
    let contact = contact_of_js js in
    { to_del = false ; contact ; old = Some contact })) in
  Editable.OnCaller.{ name ; contacts }

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
      "Manual", (fun js -> Stop (Manual (Js.to_string js))) ] ] js

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

let js_of_contact = function
  | Contact.Console -> object%js
        val _Console = Js.def Js.null
        val _SysLog = Js.undefined
        val _Email = Js.undefined
        val _SMS = Js.undefined
        val _Sqlite = Js.undefined
      end
  | Contact.SysLog -> object%js
        val _Console = Js.undefined
        val _SysLog = Js.def Js.null
        val _Email = Js.undefined
        val _SMS = Js.undefined
        val _Sqlite = Js.undefined
      end
  | Contact.Email { to_ ; cc ; bcc } -> object%js
        val _Console = Js.undefined
        val _SysLog = Js.undefined
        val _Email = Js.def (object%js
          val _to = Js.string to_
          val cc = Js.string cc
          val bcc = Js.string bcc
        end)
        val _SMS = Js.undefined
        val _Sqlite = Js.undefined
      end
  | Contact.SMS to_ -> object%js
        val _Console = Js.undefined
        val _SysLog = Js.undefined
        val _Email = Js.undefined
        val _SMS = Js.def (Js.string to_)
        val _Sqlite = Js.undefined
      end
  | Contact.Sqlite { file ; insert ; create } -> object%js
        val _Console = Js.undefined
        val _SysLog = Js.undefined
        val _Email = Js.undefined
        val _SMS = Js.undefined
        val _Sqlite = Js.def (object%js
          val file = Js.string file
          val insert = Js.string insert
          val create = Js.string create
        end)
      end

(*
 * States
 *)

(* Alerter state *)

let teams = make_param "teams" ([], "") (* teams * default_team *)
let team_search = make_param "team search" ""

let find_team teams name =
  List.find (fun t -> t.Editable.Team.name = name) teams

(* Team Selection: all of them or just one *)

type team_selection = AllTeams | SingleTeam of string
let sel_team = make_param "team selection" AllTeams

let is_team_selected sel team =
  match sel with
  | AllTeams -> true
  | SingleTeam n -> team = n

let fold_teams teams selection search_str init f =
  List.fold_left (fun prev team ->
      if is_team_selected selection team.Editable.Team.name &&
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
    string_contains search_str team.Editable.Team.searchable_name in
  List.fold_left (fun prev (member, searchable_member) ->
      if team_match_search || string_contains search_str searchable_member then
        f prev member
      else prev
    ) init team.members

(* The option below is for having the param before we receive the
 * oncaller info from the server: *)
let oncallers : Editable.OnCaller.t option param Jstable.t =
  Jstable.create ()

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

let fold_incidents incidents sel_team init f =
  List.fold_left (fun prev incident ->
      match incident.Incident.alerts with
      | [] -> prev
      | a :: _ ->
        if is_team_selected sel_team a.Alert.team then
          f prev incident
        else prev
    ) init incidents

(* Each ongoing incident has a stop button that starts a dialog: *)
type stopping_alert_stage = AskWhy of string | Stopping
let stopping_alerts = make_param "stopping alerts" []

(* Inhibitions *)

type new_inhibition =
  { what : string ; why : string ; start : string ; stop : string }

let new_inhibition =
  make_param "new inhibit" { what = "" ; why = "" ; start = "" ; stop = "" }

let reset_new_inhibit () =
  set new_inhibition { what = "" ; why = "" ; start = "" ; stop = "" }

(* History *)

let histo_duration = make_param "history duration" (3. *. 3600.)

let histo_relto = make_param "history rel.to" true

(*
 * Reload
 *)

let reload_ongoing () =
  let url =
    match sel_team.value with
    | AllTeams -> "/ongoing"
    | SingleTeam t -> "/ongoing/"^ enc t in
  http_get url (fun resp ->
    let incidents = list_of_js incident_of_js resp in
    set_known_incidents incidents ;
    List.iter update_event_time_from_incident incidents ;
    set chart_now (now ()) ;
    (* purge stopping_alerts from non-ongoing alerts *)
    let exists_alert id =
      List.exists (fun i ->
        List.exists (fun a ->
          a.Alert.id = id) i.Incident.alerts) incidents in
    let rem_stoppings =
      List.filter (fun (id, _) -> exists_alert id) stopping_alerts.value in
    set stopping_alerts rem_stoppings ;
    resync ())

let reload_oncaller name =
  let path = "/oncaller/"^ enc name in
  http_get path (fun resp ->
    let oncaller = oncaller_of_js resp in
    set_oncaller name (Some oncaller) |> ignore)

let reload_teams () =
  http_get "/teams" (fun resp ->
    teams_get_of_js resp |>
    set teams ;
    resync ())

let reload_history () =
  let url = match sel_team.value with
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

let save_members name members =
  let path = "/members/"^ enc name in
  let content = List.map fst members |> js_of_list Js.string in
  let what = "Saved members of team "^ name in
  http_post path content ~what reload_teams

let save_default_team name =
  let path = "/set_default_team/"^ enc name in
  let what = "Set team "^ name ^" the default" in
  http_get path ~what (fun _ ->
    reload_teams ())

let confirm_stop id lst =
  let rec loop = function
    | [] ->
      (* Not stopping yet, start the process: *)
      (id, ref (AskWhy "")) :: lst
    | (id', stage) :: s' ->
      if id' = id then match !stage with
        | AskWhy reason ->
          http_post "/extinguish"
            (object%js
              val alert_id_ = id
              val reason = Js.string reason
             end)
            (fun _ -> reload_ongoing ()) ;
          stage := Stopping ;
          lst
        | Stopping -> lst
      else loop s' in
  loop lst

let update_reason id lst reason =
  let rec loop = function
    | [] -> assert false
    | (id', ({ contents = AskWhy _ } as stage)) :: _ when id = id' ->
        stage := AskWhy reason ;
        lst
    | _ :: s' -> loop s' in
  loop lst

(*
 * DOM
 *)

let todo what =
  p [] [ text ("TODO: "^ what) ]

let live_incidents =
  with_param sel_team (fun tsel ->
    let with_team_col = tsel = AllTeams in
    with_param known_incidents (fun incidents ->
      let incident_rows =
        with_param selected_alert (fun sel_alert ->
          List.fold_left (fun prev i ->
            List.fold_left (fun prev a ->
              let with_action_cols = tsel = SingleTeam a.Alert.team in
              let ack _ =
                http_get ("/ack/"^ string_of_int a.id) (fun _ ->
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
                    td [] [ text a.Alert.name ] ;
                    td [] [ text a.Alert.title ] ;
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
                        with_param stopping_alerts (fun stops ->
                          let param = stopping_alerts in
                          match List.assoc a.id stops with
                          | exception Not_found ->
                            (* merely display the stop button *)
                            Gui.button ~param
                              ~next_state:(confirm_stop a.id)
                              [ clss "icon actionable" ;
                                title "Terminate this alert" ]
                              [ text "Stop" ]
                          | { contents = AskWhy s } ->
                            group
                              [ Gui.input_text ~param
                                  ~next_state:(update_reason a.id)
                                  ~placeholder:"reason" [] s ;
                                Gui.button ~param
                                  ~next_state:(confirm_stop a.id)
                                  [ clss "icon actionable" ;
                                    title "Terminate this alert" ]
                                  [ text "Stop" ] ]
                          | { contents = Stopping } ->
                            text "stopping...")
                      else group [] ] ] in
              row :: prev) prev i.Incident.alerts) [] incidents |>
              tbody []) in
      table
        [ clss "incidents" ]
        [ thead []
            [ tr []
              ((if with_team_col then th [] [ text "team" ] else group []) ::
              [ th [] [ text "since" ] ;
                th [] [ text "alert name" ] ;
                th [] [ text "alert title" ] ;
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
            [ h2 [] [ text "alerts" ] ;
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
              [ h2 [] [ text "log" ] ;
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
  let past_marks = Array.make 4 0. in (* past 4 marks *)
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
  with_param sel_team (function
    | AllTeams -> group []
    | SingleTeam team ->
      with_param teams (fun (teams, _) ->
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
  div
    [ id "page-live" ]
    [ h2 [] [ text "Opened incidents" ] ;
      live_incidents ;
      stfu ;
      h2 [] [ text "Chronology" ] ;
      with_param known_incidents (fun incidents ->
        let n = now () in
        let min_ts =
          List.fold_left (fun mi i ->
            min mi (Incident.started i)) n incidents in
        let dur = n -. min_ts in
        chronology incidents dur false) ]

let inhibition i =
  let open Inhibition in
  p
    [ clss "inhibition" ]
    (* TODO: if i.what = "" then display merely "STFU until ..." *)
    [ text ("alert "^ i.what ^" up to "^ date_of_ts i.stop_date ^
            " by "^ i.who ^" since "^ date_of_ts i.start_date ^
            " because: "^ i.why) ]

(* Here param is the oncaller option that's being edited. For us it will
 * always be Some oncaller.
 * Returns the func and a boolean indicating whether anything have been
 * changed in this contact. *)
let contact_edit contact param idx =
  let open Contact in
  let open Editable.OnCaller in
  let map_contact f i c = if i = idx then f c else c in
  let map_contacts f contacts = List.mapi (map_contact f) contacts in
  let set_to_field oncaller to_ = option_map (fun oncaller ->
    { oncaller with
      contacts = map_contacts (fun c ->
        { c with contact = match c.contact with
        | Email { cc ; bcc ; _ } -> Email { cc ; bcc ; to_ }
        | _ -> assert false }) oncaller.contacts }) oncaller in
  let set_cc_field oncaller cc = option_map (fun oncaller ->
    { oncaller with
      contacts = map_contacts (fun c ->
        { c with contact = match c.contact with
        | Email { to_ ; bcc ; _ } -> Email { cc ; bcc ; to_ }
        | _ -> assert false }) oncaller.contacts }) oncaller in
  let set_bcc_field oncaller bcc = option_map (fun oncaller ->
    { oncaller with
      contacts = map_contacts (fun c ->
        { c with contact = match c.contact with
        | Email { to_ ; cc ; _ } -> Email { cc ; bcc ; to_ }
        | _ -> assert false }) oncaller.contacts }) oncaller in
  let set_sms_field oncaller v = option_map (fun oncaller ->
    { oncaller with
      contacts = map_contacts (fun c ->
        { c with contact = match c.contact with
        | SMS _ -> SMS v
        | _ -> assert false }) oncaller.contacts }) oncaller in
  let set_file_field oncaller file = option_map (fun oncaller ->
    { oncaller with
      contacts = map_contacts (fun c ->
        { c with contact = match c.contact with
        | Sqlite { insert ; create ; _ } ->
            Sqlite { file ; insert ; create }
        | _ -> assert false }) oncaller.contacts }) oncaller in
  let set_insert_field oncaller insert = option_map (fun oncaller ->
    { oncaller with
      contacts = map_contacts (fun c ->
        { c with contact = match c.contact with
        | Sqlite { file ; create ; _ } ->
            Sqlite { file ; insert ; create }
        | _ -> assert false }) oncaller.contacts }) oncaller in
  let set_create_field oncaller create = option_map (fun oncaller ->
    { oncaller with
      contacts = map_contacts (fun c ->
        { c with contact = match c.contact with
        | Sqlite { file ; insert ; _ } ->
            Sqlite { file ; insert ; create }
        | _ -> assert false }) oncaller.contacts }) oncaller in
  let del_contact oncaller = option_map (fun oncaller ->
    { oncaller with
      contacts = map_contacts (fun c ->
        { c with to_del = not c.to_del }) oncaller.contacts |>
                 List.filter (fun c ->
                   not (c.to_del && c.old = None)) }) oncaller in
  let undel_contact = del_contact in
  if contact.to_del then
    div [ clss "contact changed deleted" ]
        [ elmt "s" [] [ text (Contact.to_string contact.contact) ] ;
          Gui.button ~param ~next_state:undel_contact
                     [ clss "actionable icon" ] [text "⌦" ] ],
    true
  else
    let console = "console" and syslog = "syslog" and email = "email"
    and sms = "SMS" and sqlite = "sqlite" in
    let selected, inp = match contact.contact with
      | Console ->
        console,
        group [
          p [ clss "explanations" ]
            [ text "Alerts will appear on the console." ] ]
      | SysLog ->
        syslog,
        group [
          p [ clss "explanations" ]
            [ text "Alerts will be logged." ] ]
      | Email { to_ ; cc ; bcc } ->
        email,
        group [
          p [ clss "explanations" ]
            [ text "To be notified via emails enter your address below:" ] ;
          Gui.input_text ~label:"To:" ~placeholder:"recipient"
                         ~param ~next_state:set_to_field [] to_ ;
          Gui.input_text ~label:"Cc:" ~placeholder:"carbon-copy"
                         ~param ~next_state:set_cc_field [] cc ;
          Gui.input_text ~label:"Bcc:" ~placeholder:"blind carbon-copy"
                         ~param ~next_state:set_bcc_field [] bcc ]
      | SMS to_ ->
        sms,
        group [
          p [ clss "explanations" ]
            [ text "SMS are currently not implemented." ] ;
          Gui.input_text ~label:"To:" ~placeholder:"phone number"
                         ~param ~next_state:set_sms_field [] to_ ]
      | Sqlite { file ; insert ; create } ->
        let rep n w =
          li [] [ span [ clss "sqlite-placeholder" ]
                       [ text ("$"^ n ^"$") ] ;
                  p [ clss "sqlite-placeholder-help" ]
                    [ text w ] ] in
        sqlite,
        group [
          p [ clss "explanations" ]
            [ text "It is possible to write alerts into a Sqlite3 \
                    database. You are then free to check this database \
                    to deliver a page wherever float your boat." ] ;
          p [ clss "explanations" ]
            [ text "For this you must first specify the emplacement of \
                    a sqlite3 file. If this file does not exist yet then \
                    it will be created, and a single SQL query will be \
                    executed to create the required table (the query \
                    labelled 'create' below)." ] ;
          p [ clss "explanations" ]
            [ text "Then, each time the alerter will want to sent a \
                    message to this person it will instead execute an \
                    SQL query (the one labelled 'insert' below) on this \
                    database, supposedly to insert the alert. To help you \
                    in this task these special placeholders will be \
                    replaced by actual values if they are present in your \
                    'insert' query:" ] ;
          ul [] [
            rep "ID" "a unique identifier for the alert" ;
            rep "NAME" "the alert name" ;
            rep "STARTED" "timestamp when the alert started" ;
            rep "STOPPED" "... and stopped" ;
            rep "TITLE" "title of the alert" ;
            rep "TEXT" "text of the alert" ;
            rep "TEAM" "team name in charge of the alert" ;
            rep "IMPORTANCE" "integer, the higher the more important the \
                              alert" ;
            rep "VICTIM" "name of the oncaller in charge of the alert" ;
            rep "ATTEMPT" "how many times the alerter have tried to reach \
                           someone from this team about this alert" ] ;
          p [ clss "explanations" ]
            [ text "Note that the values will be already quoted after \
                    replacement to prevent the injection of malicious \
                    content." ] ;
          Gui.input_text ~label:"File:"
                         ~placeholder:"path to sqlite3 file"
                         ~param ~next_state:set_file_field [] file ;
          Gui.input_text ~width:40 ~height:5 ~label:"SQL for insertion:"
                         ~placeholder:"INSERT INTO table ..."
                         ~param ~next_state:set_insert_field [] insert ;
          Gui.input_text ~width:40 ~height:5
                         ~label:"SQL for table creation:"
                         ~placeholder:"CREATE TABLE table ..."
                         ~param ~next_state:set_create_field [] create ] in
    let set_contact_type oncaller v = option_map (fun oncaller ->
    { oncaller with
      contacts = map_contacts (fun c ->
        { c with contact =
          if v = console then Console else
          if v = syslog then SysLog else
          if v = email then Email { to_ = "" ; cc = "" ; bcc = "" } else
          if v = sms then SMS "" else
          if v = sqlite then
            Sqlite { file = "" ; insert = "" ; create = "" } else
          assert false }) oncaller.contacts }) oncaller in
    let typ = Gui.select_box ~param ~next_state:set_contact_type ~selected
                [ console, console  ; syslog, syslog ; email, email ;
                  sms, sms ; sqlite, sqlite ] in
    let changed = contact.old <> Some contact.contact in
    div [ clss ("contact"^ if changed then " changed" else "") ]
        [ typ ; inp ;
          Gui.button ~param ~next_state:del_contact
                     [ clss "actionable icon" ] [text "⌫" ] ],
    changed

let team_member team name =
  let open Editable in
  let open OnCaller in
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
      let add_contact oncaller = option_map (fun oncaller ->
        { oncaller with contacts =
            { old = None ; to_del = false ;
              contact = Console } :: oncaller.contacts }) oncaller in
      let save_oncaller _ =
        (* URL has the current name while the json has the new name
         * (although for now you cannot change it. TODO. *)
        let path = "/oncaller/"^ enc name in
        let contacts = list_map_filter (fun c ->
              if c.to_del then None else Some c.contact
            ) oncaller.contacts in
        let content =
          object%js
            val name = Js.string oncaller.name
            val contacts = js_of_list js_of_contact contacts
          end in
        let what = "Saved "^ oncaller.name in
        http_post path content ~what (fun _ ->
          reload_oncaller oncaller.name) in
      let leave_team _ =
        let members = List.filter (fun (m, _) ->
          m <> oncaller.name) team.Team.members in
        save_members team.Team.name members in
      let other_teams =
        List.filter (fun t ->
            t.Team.name <> team.Team.name &&
            List.exists (fun (m, _) -> m = name) t.members
          ) (fst teams.value) in
      let funcs, anything_changed =
        list_fold_lefti (fun i (lst, changed) c ->
            let func, c = contact_edit c oncaller_p i in
            li [] [ func ] :: lst, changed || c
          ) ([], false) oncaller.contacts in
      div [ clss "oncaller-tile" ]
          [ (* name being the identifier we cannot edit it. Shall we
             * want to make the name editable then we could add an id
             * in the oncaller type. *)
            p [ clss "oncaller-name" ] [ text name ] ;
            button ~action:leave_team
              [ clss "icon actionable" ]
              [ text "leave" ] ;
            ( if other_teams = [] then group [] else
              p [ clss "oncaller-other-teams" ]
                ( text "Also in teams: " ::
                  List.map (fun t ->
                      span [ clss "oncaller-other-team" ]
                           [ text t.Team.name ]
                    ) other_teams ) ) ;
            p [ clss "explanations" ]
              [ text "Contacts by order of preference:" ] ;
            ol [ clss "oncaller-contacts" ]
               [ group funcs ;
                 li []
                    [ Gui.button ~param:oncaller_p
                                 ~next_state:add_contact
                                 [ clss "icon actionable" ]
                                 [ text "+" ] ] ] ;
            if anything_changed then
              button ~action:save_oncaller
                [ clss "tile-save icon actionable" ]
                [ text "💾" ]
            else group [] ] )

let fold_oncaller teams init f =
  List.fold_left (fun prev t ->
      List.fold_left (fun prev (m, _) ->
          f prev m
        ) prev t.Editable.Team.members
    ) init teams

let other_team_mate = make_param "other team mate" ""
let new_team_mate = make_param "new team mate" ""

let new_team_member team =
  let open Editable in
  (* First, add oncaller from another team: *)
  let other_oncallers =
    fold_oncaller (fst teams.value) [] (fun prev c ->
      if List.exists (fun (m, _) -> m = c) team.Team.members
      then prev else (c, c) :: prev) |>
    List.sort_uniq compare in
  let set_new_team_mate _old new_name = new_name in
  let save_team_members _ =
    let n = new_team_mate.value in
    let n = if n = "" then other_team_mate.value else n in
    set other_team_mate "" ;
    if n <> "" then
      save_members team.name ((n, of_string n) :: team.members) in
  div [ clss "add-members" ]
    [ div [ clss "add-members-from" ]
        [ if other_oncallers <> [] then group [
            elmt "label" []
              [ Gui.select_box ~param:other_team_mate
                               ~next_state:set_new_team_mate
                               (("Add existing", "") :: other_oncallers) ] ;
            text " or " ] else group [] ;
          Gui.input_text ~placeholder:"Create a new team mate" ~width:25
                         ~param:new_team_mate
                         ~next_state:set_new_team_mate
                         [] new_team_mate.value ] ;
      button ~action:save_team_members
        [ clss "actionable icon" ] [ text "Add" ] ]

let page_team search_str tsel team is_default =
  let is_sel = tsel = SingleTeam team.Editable.Team.name in
  div
    [ clss "team-info" ]
    [ h2 []
         [ text "Team " ;
           span [ clss "team-name" ]
                [ text team.name ] ;
           (let help =
              "Click to "^ (if is_sel then "un" else "") ^"select" in
           button ~action:(
                if is_sel then
                  (fun _ -> reset_new_inhibit () ;
                            set sel_team AllTeams)
                else
                  (fun _ -> reset_new_inhibit () ;
                            set sel_team (SingleTeam team.name)))
             (icon_class ~extra_large:true ~help is_sel)
             [ text "select" ]) ;
           (let help =
              if is_default then "This team is the default team for alerts"
              else "Make this team the default team for alerts"
           and actionable = not is_default
           and action =
            if is_default then None else Some (fun _ ->
              save_default_team team.Editable.Team.name) in
           button ?action
             (icon_class ~extra_large:true ~actionable ~help is_default)
             [ text "default" ]) ] ;
      (if team.Editable.Team.members = [] then
        div [ clss "team-empty" ]
            [ text "This team is currently empty and can be " ;
              button ~action:(fun _ ->
                  let path = "/team/"^ enc team.Editable.Team.name
                  and what = "Deleting team "^ team.Editable.Team.name in
                  http_del path ~what (fun _ -> reload_teams ()))
                [ clss "actionable" ]
                [ text "deleted" ] ]
      else group [
        h3 [] [ text "Members" ] ;
        ul [ clss "team-members" ]
           (fold_members team search_str []
              (fun prev m -> li [] [ team_member team m ] :: prev)) ]) ;
      new_team_member team ;
      (if tsel = SingleTeam team.name then
        let new_inhibition_form =
          with_param new_inhibition (fun inhibition ->
            let param = new_inhibition in
            group [
              Gui.input_text ~label:"alert" ~width:16 ~param
                             ~placeholder:"alert name (or prefix)"
                             ~next_state:(fun i what -> { i with what })
                             [] inhibition.what ;
              Gui.input_text ~label:"reason" ~width:32 ~param
                             ~placeholder:"why should this be silenced?"
                             ~next_state:(fun i why -> { i with why })
                             [] inhibition.why ;
              Gui.input_text ~label:"from" ~width:9 ~placeholder:"timestamp"
                             ~param
                             ~next_state:(fun i start -> { i with start })
                             [] inhibition.start ;
              Gui.input_text ~label:"to" ~width:9 ~placeholder:"timestamp"
                             ~param
                             ~next_state:(fun i stop -> { i with stop })
                             [] inhibition.stop ;
              button ~action:(fun _ ->
                  (* TODO: catch errors and display an err msg beside
                   * the field if not convertible: *)
                  let i = new_inhibition.value in
                  let req =
                    object%js
                      val what = Js.string i.what
                      val why = Js.string i.why
                      val start_date_ = float_of_string i.start
                      val stop_date_ = float_of_string i.stop
                    end in
                  let path = "/inhibit/add/"^ enc team.name in
                  http_post path req (fun _ ->
                    reset_new_inhibit () ;
                    reload_teams ()))
                [ clss "actionable" ] [ text "inhibit" ] ]) in
        let cur_inhibitions = List.map (fun i ->
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
          ) team.inhibitions in
        div [ clss "inhibitions-outer" ]
            [ div [ clss "inhibitions" ]
                  [ h3 [] [ text "Inhibitions" ] ;
                    ul [] cur_inhibitions ;
                    new_inhibition_form ] ] ;
      else (* team is not selected *)
        let cur_inhibitions = List.map (fun i ->
            li [] [ inhibition i ]) team.inhibitions in
        if cur_inhibitions <> [] then div [ clss "inhibitions-outer" ] [
          div [ clss "inhibitions" ]
              [ h3 [] [ text "Inhibitions" ] ;
                ul [] cur_inhibitions ]
        ] else group []) ]

let new_team = make_param "new team" ""

let page_teams =
  let set_new_team _old new_name = new_name in
  let save_team _ =
    let path = "/team/"^ enc new_team.value in
    let what = "Saving team "^ new_team.value in
    http_put path "" ~what (fun _ -> reload_teams ()) in
  with_param teams (fun (teams, default_team) ->
    with_param sel_team (fun tsel ->
      let new_team_form =
        if tsel = AllTeams then
          div [ clss "new-team" ]
            [ Gui.input_text ~label:"Create a new team: "
                             ~placeholder:"enter a name" ~width:25
                             ~param:new_team ~next_state:set_new_team
                             [] new_team.value ;
              with_param new_team (fun nt ->
                if nt = "" then group [] else
                  button ~action:save_team
                    [ clss "icon actionable" ]
                    [ text "💾" ]) ]
        else group [] in
      with_param team_search (fun search_str ->
        let search_str = Editable.of_string search_str in
        group
          [ div
              [ clss "import-export" ]
              [ p []
                  [ a
                      [ href "$RAMEN_PATH_PREFIX$/alerting/configuration" ;
                        attr "target" "_blank" ]
                      [ text "download the configuration" ] ;
                    text "/" ;
                    elmt "form"
                      [ attr "action"
                             "$RAMEN_PATH_PREFIX$/alerting/configuration" ;
                        attr "method" "POST" ;
                        attr "enctype" "multipart/form-data" ]
                      [ input [ attr "type" "file" ;
                                attr "name" "config.json" ] ;
                        button [ attr "type" "submit" ]
                               [ text "Upload" ] ] ] ] ;
            div
              [ clss "searchbox" ]
              [ Gui.input_text ~label:"🔍" ~width:8 ~placeholder:"search"
                               ~param:team_search
                               ~next_state:(fun _ x -> x)
                               [] team_search.value ] ;
            div
              [ clss "team-list" ]
              (fold_teams teams tsel search_str [ new_team_form ]
                 (fun prev team ->
                    let is_default = team.name = default_team in
                    page_team search_str tsel team is_default :: prev)) ])))

let page_reports = todo "reports"

let page_history =
  with_param known_incidents (fun incidents ->
    div []
      [ h2 [] [ text "Chronology" ] ;
        time_selector ~action:reload_history histo_duration histo_relto ;
        with_param histo_duration (fun dur ->
          with_param histo_relto (fun relto_event ->
            chronology incidents dur relto_event)) ])
