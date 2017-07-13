(* Helper to extract alerting configuration re. network link from a SQLite DB.
 *)
open Helpers
open Batteries
open Log

type db =
  { db : Sqlite3.db ;
    get_mtime : Sqlite3.stmt ;
    get_config : Sqlite3.stmt ;
    mutable last_updated : float ;
    mutable new_mtime : float }

type config_key =
  { (* Name used in the alert configuration: *)
    name : string ;
    (* Source of traffic covered by this rule: *)
    source : int option ;
    (* Destination of traffic covered by this rule: *)
    dest : int option ;
    (* Individual measurements are aggregated over that period of time
     * (typically 5 minutes), and we henceforth consider only the averages over
     * that duration. If incoming events are already aggregated (which they
     * certainly are), then this avg_window has to be longer that event
     * aggregation window. *)
    avg_window : float ;
    (* Then we take as many of those averages as to cover this observation
     * window (which thus has to be longer that avg_window: *)
    obs_window : float ;
    (* We then consider this percentile of the individual averages within that
     * observation window (or the closest one based on how many samples are
     * present): *)
    percentile : float ;
    (* And we check that this percentile traffic volume is between those
     * bounds (in bytes per seconds): *)
    min_bps : int option ;
    max_bps : int option ;
    (* Additionally, each time an average volume is above this
     * min_for_relevance... *)
    min_for_relevance : int ;
    (* We also check that the same percentile over the same obs_window of the
     * averaged round trip time and retransmission rate are below those limits:
     *)
    max_rtt : float option ;
    max_rr : float option }

let config_key_of_step db =
  let open Sqlite3 in
  { name = column db.get_config 0 |> to_string |> required ;
    source = column db.get_config 1 |> to_int ;
    dest = column db.get_config 2 |> to_int ;
    avg_window = column db.get_config 3 |> to_float |> required ;
    obs_window = column db.get_config 4 |> to_float |> required ;
    percentile = column db.get_config 5 |> to_float |> required ;
    min_bps = column db.get_config 6 |> to_int ;
    max_bps = column db.get_config 7 |> to_int ;
    min_for_relevance = column db.get_config 8 |> to_int |> default 0 ;
    max_rtt = column db.get_config 9 |> to_float ;
    max_rr = column db.get_config 10 |> to_float }

(* Query to get flow alert parameters.
 * For each alert we need:
 * - a unique but meaningful name
 * - netflow source
 * - netflow dest
 * - bandwidth averaging window
 * - time window in the past to consider
 * - percentile to consider
 * - min/max values for that percentile of those averages over that window,
 *   in bytes/secs
 * - min traffic for alerting on RTT/RR
 * - max RTT in us
 * - max RR in 0-1
 *)
let flow_alert_params_query =
  "SELECT zone_from || '-' || zone_to AS name, \
          zone_from AS source, \
          zone_to AS dest, \
          (5 * 60) AS avg_window, \
          (3600 * 2) AS obs_window, \
          0.95 AS percentile, \
          NULL AS \"min\", \
          bandwrate_alert_asc * bandw_available_asc / 100 AS \"max\", \
          bandw_min_asc AS \"relevancy\", \
          rtt_alert_asc AS \"max_rtt\", \
          rr_alert_asc AS \"max_rr\" \
   FROM bcnthresholds \
   WHERE \"max\" > 0 \
   UNION \
   SELECT zone_to || '-' || zone_from AS name, \
          zone_to AS source, \
          zone_from AS dest, \
          (5 * 60) AS avg_window, \
          (3600 * 2) AS obs_window, \
          0.95 AS percentile, \
          NULL AS \"min\", \
          bandwrate_alert_dsc * bandw_available_dsc / 100 AS \"max\", \
          bandw_min_dsc AS \"relevancy\", \
          rtt_alert_dsc AS \"max_rtt\", \
          rr_alert_dsc AS \"max_rr\" \
   FROM bcnthresholds \
   WHERE \"max\" > 0 AND NOT is_symmetric \
   UNION \
   SELECT zone_to || '-' || zone_from AS name, \
          zone_to AS source, \
          zone_from AS dest, \
          (5 * 60) AS avg_window, \
          (3600 * 2) AS obs_window, \
          0.95 AS percentile, \
          NULL AS \"min\", \
          bandwrate_alert_asc * bandw_available_asc / 100 AS \"max\", \
          bandw_min_asc AS \"relevancy\", \
          rtt_alert_asc AS \"max_rtt\", \
          rr_alert_asc AS \"max_rr\" \
    FROM bcnthresholds \
    WHERE \"max\" > 0 AND is_symmetric"

let last_mtime_query =
  "SELECT strftime('%s', MAX(date_modify)) as mtime \
   FROM bcnthresholds"

let get_db_mtime stmt =
  let open Sqlite3 in
  if reset stmt <> Rc.OK then
    failwith "Cannot reset prepared statement for get_mtime" ;
  match step stmt with
  | Rc.ROW ->
    let t = column stmt 0 |> to_float |> required in
    !logger.debug "Max mtime in DB: %g" t ;
    t
  | _ ->
    failwith "No idea what to do from this get_mtime result"

let get_db filename =
  !logger.debug "Opening DB %S" filename ;
  let open Sqlite3 in
  try (
    let db = db_open ~mode:`READONLY filename in
    !logger.debug "got db handler" ;
    let get_mtime = prepare db last_mtime_query in
    let get_config = prepare db flow_alert_params_query in
    !logger.debug "SQL statements have been prepared" ;
    { db ; get_mtime ; get_config ;
      last_updated = 0. ; new_mtime = get_db_mtime get_mtime }
  ) with exc -> (
    Printf.eprintf "Exception: %s" (Printexc.to_string exc) ;
    exit 1
  )

let check_config_changed db =
  try (
    let t = get_db_mtime db.get_mtime in
    if t > db.last_updated then (
      !logger.debug "DB mtime changed to %g (was %g)" t db.last_updated ;
      db.new_mtime <- t
    )
  ) with e ->
    Printf.eprintf "Cannot check_config_changed: %s, assuming no change.\n"
      (Printexc.to_string e)

let must_reload db =
  let ret = db.new_mtime > db.last_updated in
  if ret then !logger.info "Configuration is not up to date with DB." ;
  ret

let get_config db =
  !logger.debug "Building configuration from DB..." ;
  let open Sqlite3 in
  if reset db.get_config <> Rc.OK then
    failwith "Cannot reset prepared statement for get_config" ;
  let rec loop prev =
    match step db.get_config with
    | Rc.ROW ->
      let config_key = config_key_of_step db in
      !logger.debug "Read one DB row from zone %a to %a..."
        (Option.print Int.print) config_key.source (Option.print Int.print) config_key.dest ;
      loop (config_key :: prev)
    | Rc.DONE ->
      !logger.debug "Build a conf with %d alerts"
                      (List.length prev) ;
      prev
    | _ ->
      failwith "No idea what to do from this get_config result"
  in
  let conf = loop [] in
  db.last_updated <- db.new_mtime ;
  conf

let make filename =
  let db = get_db filename in
  !logger.debug "Building conf from DB %S" filename ;
  (*Alarm.every 1.0 (fun () -> check_config_changed db) ;*)
  db
