(* Alert manager: receive alert signals from Ramen configuration and:
 * - deduplicate the alerts (same team, same name);
 * - find who to contact, and how (including cascading process);
 * - deliver the notifications;
 * - handle acknowledgments.
 *
 * Even when one want to manage several independent organizations it's better
 * to have one executable per organization, in order to simplify the data model
 * and also to make progressive rollouts possible.
 *)
open Batteries
open Helpers
open RamenHttpHelpers
open SqliteHelpers
open RamenLog
open Lwt
open AlerterSharedTypesJS
module C = RamenConf
module CA = C.Alerter

(* Purge inhibitions stopped for more than: *)
let max_inhibit_age = 24. *. 3600.

(* Delay received notifications before acting on them, to detect flapping: *)
let flapping_detect_delay = 90.

let syslog =
  try Some (Syslog.openlog "ramen")
  with _ -> None

(* We want to have a global state of the alert management situation that we
 * can save regularly and restore whenever we start in order to limit the
 * disruption in case of a crash: *)

let is_inhibited state name team now =
  match List.find (fun t -> t.Team.name = team) state.CA.static.teams with
  | exception Not_found -> false
  | team ->
    let open Inhibition in
    (* Expunge expired inhibitions while at it: *)
    let inhibitions, inhibited =
      List.fold_left (fun (inhibitions, inhibited) i ->
          (if i.stop_date > now -. max_inhibit_age
          then i :: inhibitions else inhibitions),
          inhibited ||
          i.stop_date > now && String.starts_with name i.what
        ) ([], false) team.Team.inhibitions in
    team.inhibitions <- inhibitions ;
    inhibited

exception Not_implemented

module IncidentOps =
struct
  open Incident (* type of an incident is shared with JS but not the ops *)

  let make state alert =
    !logger.debug "Creating an incident for alert %s"
      (PPP.to_string Alert.t_ppp alert) ;
    let i = { id = state.CA.next_incident_id ;
              alerts = [ alert ] } in
    state.CA.next_incident_id <- state.CA.next_incident_id + 1 ;
    Hashtbl.add state.CA.ongoing_incidents i.id i ;
    i

  let is_for_team t i = team_of i = t

  let stopped_after ts i =
    match stopped i with
    | None -> true
    | Some s -> s >= ts

  let started_before ts i =
    Incident.started i <= ts

  let find_open_alert state ~name ~team ~title =
    Hashtbl.values state.CA.ongoing_incidents |>
    Enum.find_map (fun i ->
      if (List.hd i.alerts).team <> team then None else
      match List.find (fun a ->
        a.Alert.stopped_firing = None &&
        a.name = name && a.title = title) i.alerts with
      | exception Not_found -> None
      | a -> Some (i, a))

  let find_open_alert_by_id state id =
    Hashtbl.values state.CA.ongoing_incidents |>
    Enum.find_map (fun i ->
      match List.find (fun a -> a.Alert.id = id) i.alerts with
    | exception Not_found -> None
    | a -> Some (i, a))

  (* Let's be conservative and create a new incident for each new alerts.
   * User could still manually merge incidents later on. *)
  let get_or_create state alert =
    match find_open_alert state alert.Alert.name alert.team alert.title with
    | exception Not_found -> make state alert, None
    | i, a -> i, Some a

  let fold_archived conf init f =
    List.fold_left f init conf.C.archived_incidents

  let fold_ongoing state init f =
    Hashtbl.fold (fun _id i prev ->
      f prev i) state.CA.ongoing_incidents init

  let fold conf init f =
    let init' = fold_ongoing conf.C.alerts init f in
    fold_archived conf init' f

  let find state i =
    Hashtbl.find state.CA.ongoing_incidents i

  let remove state i =
    Hashtbl.remove state.CA.ongoing_incidents i

  (* Move i1 alerts into i2 *)
  let merge_into state i1 i2 =
    let inc1, inc2 = find state i1, find state i2 in
    inc2.alerts <- List.rev_append inc1.alerts inc2.alerts ;
    remove state i1

  let archive conf i =
    remove conf.C.alerts i.id ;
    conf.C.archived_incidents <- i :: conf.C.archived_incidents
end

module AlertOps =
struct
  open Alert (* type of an alert is shared with JS but not the ops *)

  let make state name team importance title text started_firing now =
    let id = state.CA.next_alert_id in
    state.CA.next_alert_id <- state.CA.next_alert_id + 1 ;
    { id ; name ; team ; importance ; title ; text ; started_firing ;
      stopped_firing = None ; received = now ; escalation = None ;
      log = [] }

  let log alert event_time event =
    let current_time = Unix.gettimeofday () in
    !logger.info "Alert %s: %s"
      alert.name (PPP.to_string Alert.event_ppp event) ;
    alert.log <- { current_time ; event_time ; event } :: alert.log

  let stop conf i a source time =
    a.stopped_firing <- Some time ;
    a.escalation <- None ;
    log a time (Stop source) ;
    (* If no more alerts are firing, archive the incident *)
    let stopped_firing a = a.Alert.stopped_firing <> None in
    if List.for_all stopped_firing i.Incident.alerts then
      IncidentOps.archive conf i
end

module OnCallerOps =
struct
  open OnCaller

  let default_oncaller =
    { name = "default_oncall" ;
      contacts = [| Contact.Console |] }

  let get_contacts state oncaller_name =
    match List.find (fun c -> c.OnCaller.name = oncaller_name
            ) state.CA.static.oncallers with
    | exception Not_found -> [||]
    | oncaller -> oncaller.OnCaller.contacts

  let get state name =
    (* Notice this does not even check the name is in the DB at all,
     * but that's not really needed ATM. *)
    let contacts = get_contacts state name in
    { name ; contacts }

  let who_is_oncall state team rank time =
    match List.find (fun t -> t.Team.name = team) state.CA.static.teams with
    | exception Not_found ->
      default_oncaller
    | team ->
      (match
        List.fold_left (fun best_s s ->
            if s.StaticConf.rank <> rank || s.StaticConf.from > time ||
               not (List.exists ((=) s.StaticConf.oncaller) team.members)
            then best_s else
            match best_s with
            | None -> Some s
            | Some best ->
              if s.StaticConf.from >= best.StaticConf.from then
                Some s else best_s
          ) None state.CA.static.schedule with
      | None -> default_oncaller
      | Some s -> get state s.StaticConf.oncaller)

  let assign_unassigned state =
    (* All oncallers not member of any team will be added to the "slackers"
     * team, which will be created if it doesn't exists. *)
    let slackers = "Slackers" in
    let all_assigned =
      List.fold_left (fun s t ->
          if t.Team.name = slackers then s else
          List.fold_left (fun s o ->
            Set.add o s) s t.Team.members
        ) Set.empty state.CA.static.teams in
    let unassigned =
      List.fold_left (fun s o ->
          if Set.mem o.name all_assigned then s
          else Set.add o.name s
        ) Set.empty state.CA.static.oncallers in
    if Set.is_empty unassigned then (
      (* Delete the team of Slackers if it exists *)
      let changed, teams =
        List.fold_left (fun (changed, teams) t ->
            if t.Team.name = slackers then true, teams
            else changed, t::teams
          ) (false, []) state.CA.static.teams in
      if changed then (
        !logger.info "No more slackers!" ;
        state.CA.static.teams <- teams)
    ) else (
      !logger.info "Oncallers %a are slackers"
        (Set.print String.print) unassigned ;
      match List.find (fun t -> t.Team.name = slackers)
                      state.CA.static.teams with
      | exception Not_found ->
        state.CA.static.teams <-
          Team.{ name = slackers ; members = Set.to_list unassigned ;
                 escalations = [] ; inhibitions = [] } ::
            state.CA.static.teams
      | t ->
        t.members <- Set.to_list unassigned
    )
end

module Sender =
struct
  let string_of_alert alert attempt victim =
    Printf.sprintf "\
      Title: %s\n\
      Time: %s\n\
      Id: %d\n\
      Attempt: %d\n\
      Dest: %s\n\
      %s\n\n%!"
      alert.Alert.title (string_of_time alert.started_firing)
      alert.id attempt victim alert.text

  exception CannotSendAlert of string

  let send_mail ~subject ~cc ~bcc dest body =
    let cmd =
      ("", [|"mail"; "-s"; subject; "-c"; cc; "-b"; bcc; dest|]) in
    let%lwt status =
      run_coprocess "send mail" ~timeout:10. ~to_stdin:body cmd in
    if status = Unix.WEXITED 0 then (
      !logger.debug "Mail sent" ;
      return_unit
    ) else (
      let err = string_of_process_status status in
      !logger.error "Cannot send mail: %s" err ;
      fail (CannotSendAlert err)
    )

  let via_console alert attempt victim =
    Printf.printf "%s\n%!" (string_of_alert alert attempt victim) ;
    return_unit

  let via_syslog alert attempt victim =
    let level =
      match alert.Alert.importance with
      | 0 -> `LOG_EMERG | 1 -> `LOG_ALERT | 2 -> `LOG_CRIT
      | 3 -> `LOG_ERR | 4 -> `LOG_WARNING | 5 -> `LOG_NOTICE
      | _ -> `LOG_INFO in
    let msg =
      Printf.sprintf "Name=%s;Title=%s;Id=%d;Attempt=%d;Dest=%s;Text=%s"
        alert.name alert.title alert.id attempt victim alert.text in
    match syslog with
    | None ->
      fail_with "No syslog on this host"
    | Some slog ->
      wrap (fun () -> Syslog.syslog slog level msg)

  let via_email to_ cc bcc alert attempt victim =
    let body = string_of_alert alert attempt victim in
    (* TODO: there should be a link to an acknowledgement URL *)
    let subject = "ALERT: "^ alert.title in
    send_mail ~subject ~cc ~bcc to_ body

  let via_sqlite file insert_q create_q alert attempt victim =
    let open Sqlite3 in
    let%lwt handle = wrap (fun () -> db_open file) in
    let replacements =
      [ "$ID$", string_of_int alert.Alert.id ;
        "$NAME$", sql_quote alert.name ;
        "$STARTED$", string_of_float alert.started_firing ;
        "$STOPPED$", (match alert.stopped_firing with
                     | Some f -> string_of_float f
                     | None -> "NULL") ;
        "$TEXT$", sql_quote alert.text ;
        "$TITLE$", sql_quote alert.title ;
        "$TEAM$", sql_quote alert.team ;
        "$IMPORTANCE$", string_of_int alert.importance ;
        "$VICTIM$", sql_quote victim ;
        "$ATTEMPT$", string_of_int attempt ] in
    let q = List.fold_left (fun str (sub, by) ->
      String.nreplace ~str ~sub ~by) insert_q replacements in
    let db_fail err q =
      let msg =
        Printf.sprintf "Cannot %S into sqlite DB %S: %s"
          q file (Rc.to_string err) in
      fail (CannotSendAlert msg) in
    let exec_or_fail q =
      match exec handle q with
      | Rc.OK -> return_unit
      | err -> db_fail err q in
    let%lwt () =
      match exec handle q with
      | Rc.OK -> return_unit
      | Rc.ERROR when create_q <> "" ->
        !logger.info "Creating table in sqlite DB %S" file ;
        let%lwt () = exec_or_fail create_q in
        exec_or_fail q
      | err ->
        db_fail err q in
    close ~max_tries:30 handle

  let via_todo _alert _attempt _victim = fail Not_implemented

  let get =
    let open Contact in
    function
    | Console -> via_console
    | SysLog -> via_syslog
    | Email { to_ ; cc ; bcc } -> via_email to_ cc bcc
    | Sqlite { file ; insert ; create } -> via_sqlite file insert create
    | SMS _ -> via_todo
end

module EscalationOps =
struct
  open Escalation

  let default_escalation now =
    (* First send type 0, then after 5 minutes send type 1 then after 10
     * minutes send type 2, and repeat type 2 every 10 minutes: *)
    { steps = [|
        { timeout = 350. ; victims = [| 0 |] } ;
        { timeout = 600. ; victims = [| 0 ; 1 |] } ;
        { timeout = 600. ; victims = [| 0 ; 1 |] } |] ;
      attempt = 0 ; last_sent = now }

  let make state alert now =
    !logger.debug "Creating an escalation" ;
    (* We might not find a configuration corresponding to this alert
     * importance. In that case we take the configuration for the next
     * most important alerts: *)
    match List.find (fun t -> t.Team.name = alert.Alert.team
            ) state.CA.static.teams with
    | exception Not_found ->
      default_escalation now
    | team ->
      (match 
        List.fold_left (fun best esc ->
            if esc.Team.importance > alert.Alert.importance then best else
            match best with
            | None -> Some esc
            | Some best_e ->
              if esc.importance > best_e.importance then Some esc
              else best
          ) None team.escalations with
      | None -> default_escalation now
      | Some e ->
        Escalation.{
          steps = Array.of_list e.Team.steps ;
          attempt = 0 ; last_sent = now })

  (* Send a message to all victims *)
  let outcry_once state alert step attempt now =
    (* We figure out who the oncallers are at each attempt of each alert of an
     * incident. This means that if the incident happens during an oncall
     * shift the new oncallers get involved in the middle of the action while
     * the previous oncallers, still firefighting, stop to receive updates.
     * This is on purpose: we want the incident to be handed over so at this
     * point we expect both oncall squads are already collaborating. In case
     * the previous squad hopes to finish firefighting soon and to avoid
     * bothering next squad then they can easily silence the incident anyway.
     *
     * How we actually contact an oncaller depends on its personal preferences
     * so we now need to know who exactly are we going to bother. Note that
     * here [victims] is nothing but the rank of the oncallers to be
     * targeted. *)
    let _contacted, ths =
      Array.fold_left (fun (contacted, ths as prev) rank ->
          let open OnCallerOps in
          let victim = who_is_oncall state alert.Alert.team rank now in
          let contact = get_cap victim.contacts (attempt - 1) in
          !logger.debug "%d oncall is %s, contact for attempt %d is: %s"
            rank victim.name attempt (PPP.to_string Contact.t_ppp contact) ;
          if Set.mem contact contacted then (
              !logger.debug "Skipping %s since already contacted at this step"
                (Contact.to_string contact) ;
              prev
          ) else (
            let sender = Sender.get contact in
            AlertOps.log alert now (Alert.Outcry (victim.name, contact)) ;
            Set.add contact contacted,
            sender alert attempt victim.name :: ths)
        ) (Set.empty, []) step.victims in
    (* Keep trying to reach out other victims after one failed *)
    match%lwt
      Lwt_list.fold_left_s (fun exn th ->
        try%lwt
          let%lwt () = th in
          return exn
        with e -> return (Some e)) None ths with
    | None -> return_unit
    | Some exn -> fail exn

  (* Same as above, but escalate until delivery *)
  let outcry state alert esc now =
    let rec loop () =
      let step = get_cap esc.steps esc.attempt in
      esc.attempt <- esc.attempt + 1 ;
      AlertOps.log alert now (Alert.Escalate step) ;
      (try%lwt
        let%lwt () = outcry_once state alert step esc.attempt now in
        esc.last_sent <- now ;
        return_unit
      with exn ->
        !logger.error "Cannot reach oncaller(s): %s, escalating"
          (Printexc.to_string exn) ;
        if esc.attempt - 1 < Array.length esc.steps - 1 then (
          loop ()
        ) else (
          !logger.error "All escalation steps have failed, giving up" ;
          return_unit)) in
    loop ()

  let fold_ongoing state init f =
    IncidentOps.fold_ongoing state init (fun prev i ->
      List.fold_left (fun prev a ->
        match a.Alert.escalation with
        | Some esc ->
          assert (a.stopped_firing = None) ;
          f prev a esc
        | None -> prev) prev i.alerts)
end

let check_escalations state now =
  EscalationOps.fold_ongoing state [] (fun prev alert esc ->
      let timeout =
        (* If we have not tried yet there is no possible timeout,
         * but we still want to wait a bit to protect the victims
         * against flapping alerts: *)
        if esc.attempt = 0 then
          alert.received +. flapping_detect_delay
        else
          (* Check the timeout of the current attempt: *)
          let step = get_cap esc.steps (esc.attempt - 1) in
          esc.last_sent +. step.timeout in
      if now >= timeout then (
        EscalationOps.outcry state alert esc now :: prev
      ) else prev
    ) |>
  join

let check_static_conf static =
  (* All teams must have a distinct, non empty name: *)
  let team_names =
    List.fold_left (fun s t ->
        Set.add t.Team.name s
      ) Set.empty static.StaticConf.teams in
  if Set.cardinal team_names <> List.length static.teams then
    failwith "Team names must be unique" ;
  if Set.mem "" team_names then
    failwith "Team names must not be empty" ;
  if static.default_team = "" then
    failwith "Default team must be set" ;
  if not (Set.mem static.default_team team_names) then
    failwith "Default team does not exist"

(* Everything start by: we receive an alert with a name, a team name,
 * and a description (text + title). *)
let alert conf ~name ~team ~importance ~title ~text ~firing ~time ~now =
  if firing then (
    (* First step: deduplication *)
    let alert =
      AlertOps.make conf.C.alerts name team importance title text time now in
    match IncidentOps.get_or_create conf.C.alerts alert with
    | _i, Some orig_alert ->
      AlertOps.log orig_alert time (NewNotification Duplicate) ;
      return_unit
    | _i, None ->
      if is_inhibited conf.C.alerts name team now then (
        AlertOps.log alert time (NewNotification Inhibited) ;
        return_unit
      ) else (
        let esc = EscalationOps.make conf.C.alerts alert now in
        alert.escalation <- Some esc ;
        AlertOps.log alert time (NewNotification StartEscalation) ;
        return_unit
      )
  ) else (
    (* Retrieve the alert and close it *)
    (match IncidentOps.find_open_alert conf.C.alerts ~name ~team ~title with
    | exception Not_found ->
      (* Maybe we just started in non-firing position, or the incident
       * have been manually archived already. Better not ask too many
       * questions and just log. *)
      !logger.info "Unknown alert %s for team %s titled %S stopped firing."
        name team title
    | i, a ->
      AlertOps.stop conf i a Notification time) ;
    return_unit
  )

let acknowledge state id =
  (* TODO: the time when the ack was sent *)
  let now = Unix.gettimeofday () in
  try
    IncidentOps.fold_ongoing state () (fun () i ->
      List.iter (fun alert ->
          if alert.Alert.id = id then (
            AlertOps.log alert now Ack ;
            alert.Alert.escalation <- None ;
            raise Exit
          )
        ) i.Incident.alerts) ;
    !logger.error "Received an ack for unknown id %d!" id
  with Exit -> ()

let with_rlock conf f =
  RWLock.with_r_lock conf.C.alerts_lock f

let with_wlock conf f =
  RWLock.with_w_lock conf.C.alerts_lock (fun () ->
    let%lwt res = f () in
    (* Save the config only if f did not fail: *)
    CA.save_state conf.C.persist_dir conf.C.alerts ;
    Lwt.return res)

module Api =
struct
  let find_team state name =
    match List.find (fun t -> t.Team.name = name) state.CA.static.teams with
    | exception Not_found ->
      bad_request ("unknown team "^ name)
    | t -> return t

  let notify conf params =
    with_wlock conf (fun () ->
      let hg = Hashtbl.find_default params
      and now = Unix.gettimeofday () in
      match hg "name" "alert",
            hg "importance" "10" |> int_of_string,
            hg "time" (string_of_float now) |> float_of_string,
            hg "firing" "true" |> looks_like_true with
      | exception _ ->
        fail (HttpError (400, "importance and now must be numeric"))
      | name, importance, time, firing ->
        alert conf ~name ~importance ~now ~time ~firing
          ~team:(hg "team" conf.C.alerts.static.default_team)
          ~title:(hg "title" "")
          ~text:(hg "text" "")) >>=
    respond_ok ~body:""

  let get_teams conf =
    let%lwt body =
      with_rlock conf (fun () ->
        let resp =
          Team.{ teams = conf.C.alerts.static.teams ;
                 default_team = conf.C.alerts.static.default_team } in
        return (PPP.to_string Team.get_resp_ppp resp)) in
    respond_ok ~body ()

  let new_team conf _headers name =
    with_wlock conf (fun () ->
      let teams = conf.C.alerts.static.teams in
      match List.find (fun t -> t.Team.name = name) teams with
      | exception Not_found ->
        conf.C.alerts.static.teams <-
          Team.{ name ; members = [] ; escalations = [] ;
                 inhibitions = [] } :: teams ;
        return_unit
      | _ ->
        bad_request ("Team "^ name ^" already exists")) >>=
    respond_ok

  let del_team conf _headers name =
    with_wlock conf (fun () ->
      let teams = conf.C.alerts.static.teams in
      match List.find (fun t -> t.Team.name = name) teams with
      | exception Not_found ->
        bad_request ("Team "^ name ^" does not exist")
      | t ->
        if t.Team.members <> [] then
          bad_request ("Team "^ name ^" is not empty")
        else if conf.C.alerts.static.default_team = name then
          bad_request "Cannot delete the default team"
        else (
          !logger.info "Delete team %s" name ;
          conf.C.alerts.static.teams <-
            List.filter (fun t -> t.Team.name <> name) teams ;
          return_unit)) >>=
    respond_ok

  let get_oncaller conf _headers name =
    let%lwt body =
      with_rlock conf (fun () ->
        let oncaller = OnCallerOps.get conf.C.alerts name in
        return (PPP.to_string OnCaller.t_ppp oncaller)) in
    respond_ok ~body ()

  let get_ongoing conf team_opt =
    let%lwt body =
      with_rlock conf (fun () ->
        let incidents =
          Hashtbl.fold (fun _id incident prev ->
              match incident.Incident.alerts with
              | [] -> prev (* Should not happen *)
              | a :: _ ->
                let is_selected =
                  match team_opt with None -> true
                                    | Some t -> a.team = t in
                if is_selected then (
                  incident :: prev
                ) else prev
            ) conf.C.alerts.ongoing_incidents [] in
        return (PPP.to_string GetOngoing.resp_ppp incidents)) in
    respond_ok ~body ()

  let get_history conf _headers team time_range =
    let is_in_team =
      match team with
      | Some t -> fun i -> IncidentOps.is_for_team t i
      | None -> fun _ -> true
    and is_in_range =
      match time_range with
      | GetHistory.LastSecs s ->
          let min_ts = Unix.gettimeofday () -. s in
          IncidentOps.stopped_after min_ts
      | GetHistory.SinceUntil (s, u) ->
          fun i ->
            IncidentOps.stopped_after s i &&
            IncidentOps.started_before u i in
    let is_in i =
      is_in_team i && is_in_range i in
    let%lwt body =
      with_rlock conf (fun () ->
        let logs =
          IncidentOps.fold conf [] (fun prev i ->
            if is_in i then i :: prev else prev) in
        return (PPP.to_string GetHistory.resp_ppp logs)) in
    respond_ok ~body ()

  let get_history_post conf headers body =
    let%lwt req = of_json headers "Get History" GetHistory.req_ppp body in
    get_history conf headers req.team req.time_range

  let get_history_get conf headers team params =
    let%lwt time_range =
      match Hashtbl.find params "last" with
      | exception Not_found ->
        (match Hashtbl.find params "range" with
        | exception Not_found ->
          fail (HttpError (400, "Missing parameter: last or range"))
        | r ->
          (try
            let s, u = String.split ~by:"," r in
            GetHistory.SinceUntil (float_of_string (min s u),
                                   float_of_string (max s u)) |>
            return
          with Not_found | Failure _ ->
            let msg = "Invalid parameter: range must be \
                       timestamp,timestamp" in
            fail (HttpError (400, msg))))
      | l ->
        (try GetHistory.LastSecs (float_of_string l) |> return
        with Failure _ ->
          let msg = "Invalid parameter: last must be a numeric" in
          fail (HttpError (400, msg))) in
    get_history conf headers team time_range

  let alert_of_id state id =
    try IncidentOps.find_open_alert_by_id state id |> return
    with Not_found ->
      bad_request "No alert with that id in opened incidents"

  let get_ack conf id =
    with_wlock conf (fun () ->
      let%lwt _i, a = alert_of_id conf.C.alerts id in
      (* We record the ack regardless of stopped_firing *)
      AlertOps.log a (Unix.gettimeofday ()) Alert.Ack ;
      a.escalation <- None ;
      return_unit) >>=
    respond_ok

  (* Stops an alert as if a notification with firing=f have been received *)
  let stop_alert conf id reason =
    with_wlock conf (fun () ->
      let%lwt i, a = alert_of_id conf.C.alerts id in
      let now = Unix.gettimeofday () in
      AlertOps.stop conf i a (Manual reason) now ;
      return_unit) >>=
    respond_ok

  let get_stop conf id =
    stop_alert conf id "todo"

  let post_stop conf headers body =
    let%lwt req = of_json headers "Stop Alert" Alert.stop_req_ppp body in
    stop_alert conf req.alert_id req.reason

  let export_static_conf conf =
    let%lwt body =
      with_rlock conf (fun () ->
        return (PPP.to_string StaticConf.t_ppp conf.C.alerts.static)) in
    respond_ok ~body ()

  let set_static_conf conf static =
    match check_static_conf static with
    | exception Failure msg -> bad_request msg
    | () -> with_wlock conf (fun () ->
      conf.C.alerts.static <- static ;
      return_unit)

  let upload_static_conf conf headers body =
    let content_type = get_content_type headers in
    let%lwt data = match
      let open CodecMultipartFormData in
      parse_multipart_args content_type body |>
      List.find (fun (name, _) -> name = "config.json") with
      | exception Not_found -> bad_request "Cannot find config.json"
      | exception e -> bad_request (Printexc.to_string e)
      | _, { value ; content_type ; _ }  ->
        if content_type <> "application/json" then
          bad_request ("Bad content type for config.json file: expected \
                        application/json but got "^ content_type)
        else return value in
    let%lwt static =
      of_json_body "Static Configuration" StaticConf.t_ppp data in
    set_static_conf conf static
    (* Let HttpSrv answer the query *)

  let put_static_conf conf headers body =
    let%lwt static =
      of_json headers "Static Configuration" StaticConf.t_ppp body in
    set_static_conf conf static >>=
    respond_ok

  (* Inhibitions *)

  let user = "anonymous user"

  let add_inhibit_ state team inhibit =
    let open Inhibition in
    let%lwt team = find_team state team in
    let id = state.CA.next_inhibition_id in
    state.CA.next_inhibition_id <- state.CA.next_inhibition_id + 1 ;
    let inhibit = { inhibit with id } in
    (* TODO: lock the conf with a RW lock *)
    match List.find (fun i -> i.id = inhibit.id) team.Team.inhibitions with
    | exception Not_found ->
      team.inhibitions <- inhibit :: team.inhibitions ;
      return_unit
    | _ ->
      failwith "Found an inhibit with next id?"

  let edit_inhibit conf headers team body =
    let open Inhibition in
    let%lwt inhibit = of_json headers "Inhibit" Inhibition.t_ppp body in
    with_wlock conf (fun () ->
      let%lwt team = find_team conf.C.alerts team in
      (* Note: one do not delete inhibitions but one can set a past
       * stop_date. *)
      match List.find (fun i -> i.id = inhibit.id)
                      team.Team.inhibitions with
      | exception Not_found ->
        bad_request "No such inhibition"
      | i ->
        i.what <- inhibit.what ;
        i.start_date <- inhibit.start_date ;
        i.stop_date <- inhibit.stop_date ;
        i.why <- inhibit.why ;
        return_unit) >>=
    respond_ok

  let add_inhibit conf headers team body =
    let%lwt inhibit = of_json headers "Inhibit" Inhibition.t_ppp body in
    with_wlock conf (fun () ->
      add_inhibit_ conf.C.alerts team inhibit) >>=
    respond_ok (* TODO: why not returning the GetTeam.resp directly? *)

  let stfu conf _headers team =
    let open Inhibition in
    let now = Unix.gettimeofday () in
    let inhibit =
      { id = -1 ; what = "" ; start_date = now ; stop_date = now +. 3600. ;
        who = user ; why = "STFU!" } in
    with_wlock conf (fun () ->
      add_inhibit_ conf.C.alerts team inhibit) >>=
    respond_ok

  let edit_oncaller conf headers name body =
    let%lwt oncaller =
      of_json headers "Save Oncaller" OnCaller.t_ppp body in
    let rec list_replace prev = function
      | [] -> List.rev (oncaller :: prev) (* New one *)
      | c :: rest ->
        if c.OnCaller.name = name then
          List.rev_append prev (oncaller :: rest)
        else
          list_replace (c :: prev) rest in
    with_wlock conf (fun () ->
      conf.C.alerts.static.oncallers <-
        list_replace [] conf.C.alerts.static.oncallers ;
      return_unit) >>=
    respond_ok

  let edit_members conf headers name body =
    let%lwt members =
      of_json headers "Save members" Team.set_members_ppp body in
    with_wlock conf (fun () ->
      let%lwt team = find_team conf.C.alerts name in
      team.Team.members <- members ;
      OnCallerOps.assign_unassigned conf.C.alerts ;
      return_unit) >>=
    respond_ok

  let set_default_team conf _headers name =
    (* Sanity checks *)
    if name = "" then
      bad_request "Default team must be set"
    else with_wlock conf (fun () ->
      if not (List.exists (fun t -> t.Team.name = name)
                          conf.C.alerts.static.teams) then
        bad_request ("Team "^ name ^" does not exist")
      else (
        conf.C.alerts.static.default_team <- name ;
        return_unit)) >>=
    respond_ok
end

(* Alerting Daemon *)

let rec check_escalations_loop conf =
  let now = Unix.gettimeofday () in
  let%lwt () =
    try%lwt
      check_escalations conf.C.alerts now
    with e ->
      print_exception e ;
      return_unit in
  let%lwt () = Lwt_unix.sleep 0.5 in
  check_escalations_loop conf

let start ?initial_json conf =
  Option.may (fun fname ->
      let static =
        read_whole_file fname |>
        PPP.of_string_exc StaticConf.t_ppp in
      check_static_conf static ;
      conf.C.alerts.static <- static
    ) initial_json ;
  async (fun () -> restart_on_failure check_escalations_loop conf)
