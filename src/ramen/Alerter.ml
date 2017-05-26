(* Alert manager: receive alert signals from Ramen configuration and:
 * - deduplicate the alerts (same team, same name);
 * - find who to contact, and how (including cascading process);
 * - deliver the notifications;
 * - handle acknowledgments.
 *
 * This currently runs as part of the alerting configuration (aka provides
 * the implementation of the "alert" primitive operation). Later could be
 * made independent, although the simplest way to do that is probably via
 * Ramen choreographer.
 *)
open Batteries
open Helpers

let debug = true

(* We want to have a global state of the alert management situation that we
 * can save regularly and restore whenever we start in order to limit the
 * disruption in case of a crash: *)

type alert =
  { name : string ;
    team : string ;
    importance : int ;
    time : float ;
    title : string ;
    text : string }

type incident =
  { mutable alerts : alert list ;
    mutable stfu : bool ;
    mutable merged_with : incident option }

type escalation_step =
  { timeout : float ;
    victims : int array (* Who to ring from the oncallers squad *) }

type escalation =
  { steps : escalation_step array ;
    mutable acknowledged : bool ;
    alarm : Alarm.t }

(* The part of the internal state that we persist on disk: *)
type persisted_state = {
  mutable ongoing_incidents : incident list ;

  (* All ongoing escalations, keyed by escalation, value = step we are
   * currently in and when we entered it. Note that strictly speaking
   * we do not need this hash since we already have all escalation in
   * alarms, but we'd like to save this from time to time on disk to help
   * not loosing track of what's happening in case of crash: *)
  mutable ongoing_escalations : (escalation, int * float) Hashtbl.t ;

  (* All global (aka team-wide) inhibits, keyed by team name, value being
   * expiration of the inhibit *)
  team_inhibits : (string, float) Hashtbl.t ;

  (* All inhibited alerts, keyed by alert name*team *)
  alert_inhibits : (string * string, float) Hashtbl.t }

type state = {
  (* Where this state is saved *)
  save_file : string ;
  mutable dirty : bool ;

  (* Sqlite configuration for oncallers and escalations (will be created if
   * it does not exist) *)
  db : Sqlite3.db ;

  (* A few prepared statements *)
  stmt_get_oncall : Sqlite3.stmt ;
  stmt_get_contacts : Sqlite3.stmt ;
  stmt_get_escalation : Sqlite3.stmt ;

  persist : persisted_state }

let is_inhibited state name team =
  let in_inhibit_hash h k =
    match Hashtbl.find h k with
    | exception Not_found -> false
    | end_date ->
      if !Alarm.now < end_date then true else (
        Hashtbl.remove h k ;
        false
      ) in
  in_inhibit_hash state.persist.team_inhibits team ||
  in_inhibit_hash state.persist.alert_inhibits (name, team)

exception Not_implemented

module Contact =
struct
  type t = Console | Email of string | SMS of string [@@ppp PPP_OCaml]
end

module Alert =
struct
  let make name team importance title text =
    { name ; team ; importance ; title ; text ;
      time = !Alarm.now }
end

module Incident =
struct
  let make state alert =
    let i = { alerts = [ alert ] ;
              stfu = false ;
              merged_with = None } in
    state.persist.ongoing_incidents <- i :: state.persist.ongoing_incidents ;
    state.dirty <- true ;
    i

  let has_alert_with i cond =
    List.exists cond i.alerts

  (* Let's be conservative and create a new incident for each new alerts.
   * User could still manually merge incidents later on. *)
  let get_or_create state alert =
    match List.find (fun i ->
              (List.hd i.alerts).team = alert.team &&
              has_alert_with i (fun a -> a.name = alert.name)
            ) state.persist.ongoing_incidents
    with exception Not_found -> make state alert, false
       | i -> i, has_alert_with i (fun a -> a.name = alert.name &&
                                            a.title = alert.title)
end

module Log =
struct
  type alert_outcome =
    Duplicate | Inhibited | STFU | Notify of Contact.t
  type log_event = NewAlert of (alert * incident * alert_outcome)
  let add = ignore (* TODO *)
end

module OnCaller =
struct
  type t =
    { name : string ;
      contacts : Contact.t array }

  let default_oncaller =
    { name = "default_oncall" ;
      contacts = [| Contact.Console |] }

  (* Return the list of oncallers for a team at time t *)
  let who_is_oncall state team rank time =
    let open Sqlite3 in
    let stmt = state.stmt_get_oncall in
    reset stmt |> must_be_ok ;
    bind stmt 1 Data.(INT (Int64.of_int rank)) |> must_be_ok ;
    bind stmt 2 Data.(FLOAT time) |> must_be_ok ;
    bind stmt 3 Data.(TEXT team) |> must_be_ok ;
    match step stmt with
    | Rc.ROW ->
      let name = column stmt 0 |> to_string |> required in
      let stmt = state.stmt_get_contacts in
      reset stmt |> must_be_ok ;
      bind stmt 1 Data.(TEXT name) |> must_be_ok ;
      let contacts =
        step_all_fold stmt [] (fun prev ->
          (column stmt 0 |> to_string |> required |>
           PPP.of_string_exc Contact.t_ppp) :: prev) |>
        List.rev |>
        Array.of_list in
      { name ; contacts }
    | _ ->
      default_oncaller
end

module Sender =
struct
  let printer ~title ~text =
    Printf.printf "Title: %s\n\n%s\n%!" title text

  let get = function
    | Contact.Console -> printer
    | Contact.Email _ | Contact.SMS _ ->
      raise Not_implemented
end

module Escalation =
struct
  let default_escalation () =
    (* First send type 0, then immediately send type 1 then
     * after 5 minutes send type 2 then after 10 minutes send type 3,
     * and repeat type 3 every 10 minutes: *)
    { steps = [|
        { timeout = 0. ; victims = [| 0 |] } ;
        { timeout = 350. ; victims = [| 0 |] } ;
        { timeout = 600. ; victims = [| 0 ; 1 |] } ;
        { timeout = 600. ; victims = [| 0 ; 1 |] }
      |] ;
      acknowledged = false ; alarm = Alarm.make () }

  let rec make state team importance =
    let to_array mask =
      let rec loop prev bit =
        if bit >= 64 then Array.of_list prev else
        let prev' =
          if 0L = Int64.(logand mask (shift_left 1L bit)) then prev else
          bit :: prev in
        loop prev' (bit+1) in
      loop [] 0
    in
    if importance < 0 then ( (* end the recursion *)
      default_escalation ()
    ) else (
      let open Sqlite3 in
      let stmt = state.stmt_get_escalation in
      reset stmt |> must_be_ok ;
      bind stmt 1 Data.(TEXT team) |> must_be_ok ;
      bind stmt 2 Data.(INT (Int64.of_int importance)) |> must_be_ok ;
      let steps =
        step_all_fold stmt [] (fun prev ->
          let timeout = column stmt 1 |> to_float |> required
          and victims = column stmt 2 |> to_int64 |> required |> to_array
          in
          { timeout ; victims } :: prev) |>
        List.rev |>
        Array.of_list in
      if steps = [||] then make state team (importance - 1) else
      { steps ; acknowledged = false ; alarm = Alarm.make () }
    )

  (* Send that alert using message king for this step *)
  let send_alert state attempt alert incident victims =
    (* We figure out who the oncaller are at each attempt of each alert of an
     * incident. This means that if the incident happends during an oncall
     * shift the new oncallers get involved in the middle of the action while
     * the previous oncallers, still firefighting, stop to receive updates.
     * This is on purpose: we want the incident to be handed over so at this
     * point we expect both oncall squads are already collaborating. In case
     * the previous squad hopes to finish firefighting soon and to avoid
     * bothering next squad then they can easily silence the incident anyway.
     *
     * How we actually contact an oncaller depends on its personal preferences
     * so we now need to know who exactly are we going to bother: *)
    Array.iter (fun rank ->
        let open OnCaller in
        let victim = who_is_oncall state alert.team rank !Alarm.now in
        let contact = get_cap victim.contacts attempt in
        let send = Sender.get contact in
        Log.(add (NewAlert (alert, incident, Notify contact))) ;
        send ~title:alert.title ~text:alert.text
      ) victims

  (* Send a message and prepare the escalation *)
  let rec send state e attempt alert incident =
    let step = get_cap e.steps attempt in
    send_alert state attempt alert incident step.victims ;
    Alarm.at e.alarm (!Alarm.now +. step.timeout) (fun () ->
      if not e.acknowledged then (
        Hashtbl.modify e (fun (step, _) -> step+1, !Alarm.now)
                       state.persist.ongoing_escalations ;
        state.dirty <- true ;
        send state e (attempt + 1) alert incident
      ))

  let start state e alert incident =
    Hashtbl.add state.persist.ongoing_escalations e (0, !Alarm.now) ;
    state.dirty <- true ;
    send state e 0 alert incident
end

let open_config_db file =
  let open Sqlite3 in
  let ensure_db_table db create insert =
    exec db create |> must_be_ok ;
    exec db insert |> must_be_ok
  in
  try db_open ~mode:`READONLY file
  with Error err ->
    if debug then
      Printf.eprintf "Cannot open DB %S: %s, will create a new one.\n%!"
        file err ;
    let db = db_open file in
    ensure_db_table db
      "CREATE TABLE IF NOT EXISTS oncallers ( \
         name STRING PRIMARY KEY, \
         team STRING NOT NULL)"
      "INSERT INTO oncallers VALUES \
         (\"John Doe\", \"support\")" ;
    (* TODO: maybe in the future make the contact depending on day of
     * week and/or time of day? *)
    ensure_db_table db
      "CREATE TABLE IF NOT EXISTS contacts ( \
         oncaller STRING REFERENCES oncallers (name) ON DELETE CASCADE, \
         preferred INTEGER NOT NULL, \
         contact STRING NOT NULL)"
      "INSERT INTO contacts VALUES \
         (\"John Doe\", 1, \"Console\")" ;
    ensure_db_table db
      "CREATE TABLE IF NOT EXISTS schedule ( \
         oncaller STRING REFERENCES oncallers (name) ON DELETE SET NULL, \
         from INTEGER NOT NULL, \
         rank INTEGER NOT NULL)"
      "INSERT INTO schedule VALUES \
         (\"John Doe\", 0, 1)" ;
    ensure_db_table db
      "CREATE TABLE IF NOT EXISTS escalations ( \
         team STRING NOT NULL, \
         importance INTEGER NOT NULL, \
         attempt INTEGER NOT NULL, \
         timeout REAL NOT NULL, \
         victims INTEGER NOT NULL DEFAULT 1)"
      "INSERT INTO escalations VALUES \
         (\"support\", 0, 1, 350, 1), \
         (\"support\", 0, 2, 350, 3)" ;
    (* Reopen in read-only *)
    db_close db |> must_be string_of_bool true ;
    db_open ~mode:`READONLY file

let get_state save_file db_config_file =
  let persist =
    try
      File.with_file_in save_file (fun ic -> Marshal.input ic)
    with Sys_error err ->
      Printf.eprintf "Cannot read state from file %S: %s. Starting anew\n%!"
        save_file err ;
      { ongoing_incidents = [] ;
        ongoing_escalations = Hashtbl.create 11 ;
        team_inhibits = Hashtbl.create 11 ;
        alert_inhibits = Hashtbl.create 11 }
    in
  let db = open_config_db db_config_file in
  { save_file ; dirty = false ; db ;
    stmt_get_oncall =
      Sqlite3.prepare db
        "SELECT oncallers.name \
         FROM schedule NATURAL JOIN oncallers \
         WHERE schedule.rank <= ? AND schedule.from <= ? \
           AND oncallers.team = ? \
         ORDER BY schedule.from DESC, schedule.rank DESC LIMIT 1" ;
    stmt_get_contacts =
      Sqlite3.prepare db
        "SELECT contact FROM contacts \
         WHERE oncaller = ? ORDER BY preferred DESC" ;
    stmt_get_escalation =
      Sqlite3.prepare db
        "SELECT timeout, victims FROM escalations \
         WHERE team = ? AND importance = ? ORDER BY attempt ASC" ;
    persist }


let save_state state =
  if state.dirty then (
    if debug then
      Printf.eprintf "Saving state in file %S\n%!" state.save_file ;
    File.with_file_out ~mode:[`create; `trunc] state.save_file (fun oc ->
      Marshal.output oc ~sharing:false state) ;
    state.dirty <- false
  )

(* Everything start by: we receive an alert with a name, a team name,
 * and a description (text + title). *)
let alert state ~name ~team ~importance ~title ~text =
  (* First step: deduplication *)
  let alert = Alert.make name team importance title text in
  let incident, is_dup = Incident.get_or_create state alert in
  if is_dup then (
    Log.(add (NewAlert (alert, incident, Duplicate))) ;
  ) else if is_inhibited state name team then (
    Log.(add (NewAlert (alert, incident, Inhibited))) ;
  ) else if incident.stfu then (
    Log.(add (NewAlert (alert, incident, STFU))) ;
  ) else (
    let esc = Escalation.make state team importance in
    Escalation.start state esc alert incident ;
    save_state state
  )
