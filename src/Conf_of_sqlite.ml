(* Helper to extract alerting configuration re. network link from a SQLite DB.
 *)
open Batteries
open RamenLog
open SqliteHelpers

type db =
  { db : Sqlite3.db ;
    get_mtime : Sqlite3.stmt ;
    get_bcns : Sqlite3.stmt ;
    get_bcas : Sqlite3.stmt ;
    get_zones : Sqlite3.stmt ;
    mutable last_updated : float ;
    mutable new_mtime : float }

let zone_name_of_id = Hashtbl.create 37
let fold_zones f = Hashtbl.fold f zone_name_of_id []

(* Is z1 a subzone of z2? *)
let is_subzone zn1 = function
  | None -> false
  | Some zn2 ->
    (* z1 is a subzone of z2 iff z1 name starts with z2 name + "/" *)
    String.starts_with zn1 (zn2 ^ "/")
let (<<) = is_subzone

let add_subzones_down_to z' z_opt =
  let z'name = Option.map (Hashtbl.find zone_name_of_id) z' in
  match z_opt with
  | None -> []
  | Some z ->
    let z_name = Hashtbl.find zone_name_of_id z in
    (* The main zone must stay at head of the list for later we use List.hd for
     * naming the set *)
    z :: fold_zones (fun id name lst ->
      if name << Some z_name && not (name << z'name) then id::lst else lst)

module BCN =
struct
  type config =
    { (* Id used in the alert text: *)
      id : int ;
      (* Sources of traffic covered by this rule (empty list for any): *)
      source : int list ;
      (* Destinations of traffic covered by this rule (empty list for any): *)
      dest : int list ;
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
      (* ...then we also check that the same percentile over the same
       * obs_window of the averaged round trip time and retransmission rate
       * are below those limits: *)
      max_rtt : float option (* in seconds *) ;
      max_rr : float option (* fraction of retransmitted packets over packets
                               with payload *) }

  let of_step db =
    let main_source = with_field db.get_bcns 1 "zone_from" to_int
    and main_dest = with_field db.get_bcns 2 "zone_to" to_int in
    { id = with_field db.get_bcns 0 "id" (required % to_int) ;
      source = main_source |> add_subzones_down_to main_dest ;
      dest = main_dest |> add_subzones_down_to main_source ;
      avg_window = with_field db.get_bcns 3 "avg_window" (required % to_float) ;
      obs_window = with_field db.get_bcns 4 "obs_window" (required % to_float) ;
      percentile = with_field db.get_bcns 5 "percentile" (required % to_float) ;
      min_bps = with_field db.get_bcns 6 "min" to_int ;
      max_bps = with_field db.get_bcns 7 "max" to_int ;
      min_for_relevance = with_field db.get_bcns 8 "relevance" (default 0 % to_int) ;
      max_rtt = with_field db.get_bcns 9 "max_rtt" to_float ;
      max_rr = with_field db.get_bcns 10 "max_rr" to_float }
end

module BCA =
struct
  type config =
    { (* First fields are similar to BCN *)
      id : int ;
      service_id : int ;
      name : string ;
      avg_window : float ;
      obs_window : float ;
      percentile : float ;
      max_eurt : float ;
      (* Only relevant if there are at least that many SRT: *)
      min_srt_count : int }

  let of_step db =
    { id = with_field db.get_bcas 0 "bca_id" (required % to_int) ;
      service_id = with_field db.get_bcas 1 "service_id" (required % to_int) ;
      name = with_field db.get_bcas 2 "name" (required % to_string) ;
      avg_window = with_field db.get_bcas 3 "avg_window" (required % to_float) ;
      obs_window = with_field db.get_bcas 4 "obs_window" (required % to_float) ;
      percentile = with_field db.get_bcas 5 "percentile" (required % to_float) ;
      min_srt_count = with_field db.get_bcas 6 "min_handshake_count" (default 0 % to_int) ;
      (* ms in the DB *)
      max_eurt = 0.001 *. (with_field db.get_bcas 7 "threshold_alert" (required % to_float)) }
end

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
  "SELECT id, \
          zone_from AS source, \
          zone_to AS dest, \
          60 AS avg_window, \
          600 AS obs_window, \
          90. AS percentile, \
          NULL AS \"min\", \
          (bandwrate_alert_asc * bandw_available_asc / 100) / 8 AS \"max\", \
          bandw_min_asc / 8 AS \"relevance\", \
          rtt_alert_asc / 1000000.0 AS \"max_rtt\", \
          rr_alert_asc AS \"max_rr\" \
   FROM bcnthresholds \
   WHERE \"max\" > 0 \
   UNION \
   SELECT id, \
          zone_to AS source, \
          zone_from AS dest, \
          60 AS avg_window, \
          600 AS obs_window, \
          90. AS percentile, \
          NULL AS \"min\", \
          (bandwrate_alert_dsc * bandw_available_dsc / 100) / 8 AS \"max\", \
          bandw_min_dsc / 8 AS \"relevance\", \
          rtt_alert_dsc / 1000000.0 AS \"max_rtt\", \
          rr_alert_dsc AS \"max_rr\" \
   FROM bcnthresholds \
   WHERE \"max\" > 0 AND NOT is_symmetric \
   UNION \
   SELECT id, \
          zone_to AS source, \
          zone_from AS dest, \
          60 AS avg_window, \
          600 AS obs_window, \
          90. AS percentile, \
          NULL AS \"min\", \
          (bandwrate_alert_asc * bandw_available_asc / 100) / 8 AS \"max\", \
          bandw_min_asc / 8 AS \"relevance\", \
          rtt_alert_asc / 1000000.0 AS \"max_rtt\", \
          rr_alert_asc AS \"max_rr\" \
    FROM bcnthresholds \
    WHERE \"max\" > 0 AND is_symmetric"

(* Query to get the critical services: *)
(* Note: avg_window is 6 mins since the traffic to BCA is smaller and alerts
 * would flap if we considered 1 minute intervals. *)
let service_alert_params_query =
  "SELECT s.bca_id, \
          s.id, \
          s.name, \
          360 AS avg_window, \
          600 AS obs_window, \
          90. AS percentile, \
          a.min_handshake_count, \
          a.threshold_alert \
   FROM service s \
   JOIN bcathresholds a ON (s.bca_id=a.id) \
   WHERE \
    NOT s.is_deleted AND \
    s.bca_id > 0 AND \
    a.threshold_alert > 0"

let last_mtime_query =
  (* Note: there is no date_modify in bcathresholds. Not my problem. *)
  "SELECT strftime('%s', MAX(date_modify)) as mtime FROM \
   (SELECT date_modify FROM bcnthresholds UNION \
    SELECT date_modify FROM zone UNION \
    SELECT '1976-01-01' as date_modify)" (* <- fallback *)

let get_zones_query =
  "SELECT id, name FROM zone WHERE NOT is_deleted"

let get_db_mtime stmt =
  let open Sqlite3 in
  match step stmt with
  | Rc.ROW ->
    let t = with_field stmt 0 "mtime" (required % to_float) in
    !logger.debug "Max mtime in DB: %g" t ;
    reset stmt |> must_be_ok ;
    t
  | _ ->
    reset stmt |> ignore ;
    failwith "No idea what to do from this get_mtime result"

let get_db filename =
  !logger.debug "Opening DB %S" filename ;
  let open Sqlite3 in
  try (
    let db = db_open ~mode:`READONLY filename in
    !logger.debug "got db handler" ;
    let get_mtime = prepare db last_mtime_query in
    !logger.debug "SQL statements have been prepared" ;
    { db ; get_mtime ;
      get_bcns = prepare db flow_alert_params_query ;
      get_bcas = prepare db service_alert_params_query ;
      get_zones = prepare db get_zones_query ;
      last_updated = 0. ; new_mtime = get_db_mtime get_mtime }
  ) with exc -> (
    !logger.error "Exception: %s" (Printexc.to_string exc) ;
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
    !logger.error "Cannot check_config_changed: %s, assuming no change.\n"
      (Printexc.to_string e)

let must_reload db =
  let ret = db.new_mtime > db.last_updated in
  if ret then !logger.info "Configuration is not up to date with DB." ;
  ret

let get_config db =
  !logger.debug "Building configuration from DB..." ;
  let open Sqlite3 in
  (* First get the zone tree *)
  Hashtbl.clear zone_name_of_id ;
  let rec loop () =
    match step db.get_zones with
    | Rc.ROW ->
      let id = with_field db.get_zones 0 "id" (required % to_int)
      and name = with_field db.get_zones 1 "name" (required % to_string) in
      Hashtbl.add zone_name_of_id id name ;
      loop ()
    | Rc.DONE ->
      reset db.get_zones |> must_be_ok
    | _ ->
      reset db.get_zones |> ignore ;
      failwith "No idea what to do from this get_zones result" (* TODO: to_string *)
  in loop () ;
  (* Now the BCN *)
  let rec loop prev =
    match step db.get_bcns with
    | Rc.ROW ->
      let cfg = BCN.of_step db in
      !logger.debug "Read one BCN from zone %a to %a..."
        (List.print Int.print) cfg.source
        (List.print Int.print) cfg.dest ;
      loop (cfg :: prev)
    | Rc.DONE ->
      !logger.debug "Build a BCN config with %d alerts"
                      (List.length prev) ;
      reset db.get_bcns |> must_be_ok ;
      prev
    | _ ->
      reset db.get_bcns |> ignore ;
      failwith "No idea what to do from this get_bcns result"
  in
  let bcns = loop [] in
  let rec loop prev =
    match step db.get_bcas with
    | Rc.ROW ->
      let cfg = BCA.of_step db in
      !logger.debug "Read one BCA for %s..." cfg.BCA.name ;
      loop (cfg :: prev)
    | Rc.DONE ->
      !logger.debug "Build a BCA config with %d alerts"
                     (List.length prev) ;
      reset db.get_bcas |> must_be_ok ;
      prev
    | _ ->
      reset db.get_bcas |> ignore ;
      failwith "No idea what to do from this get_bcas result"
  in
  let bcas = loop [] in
  db.last_updated <- db.new_mtime ;
  bcns, bcas

let make filename =
  let db = get_db filename in
  !logger.debug "Building conf from DB %S" filename ;
  (*Alarm.every 1.0 (fun () -> check_config_changed db) ;*)
  db
