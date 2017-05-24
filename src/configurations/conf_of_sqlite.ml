(* For Ramen, this is a mere configuration file that is dynlinked and that
 * will register a configuration.
 * But actually, this will generate the configuration from a sqlite db,
 * and use the Alarm facility of Ramen to register a thread that is going to
 * monitor the DB for changes and update the configuration (TODO: an update
 * function to replace a running configuration). *)

let debug = false

type db =
  { db : Sqlite3.db ;
    get_mtime : Sqlite3.stmt ;
    get_config : Sqlite3.stmt ;
    mutable last_updated : float ;
    mutable up_to_date : bool }

type config_key =
  { id : int ;
    mtime : float ;
    name : string ;
    source : int option ;
    dest : int option ;
    avg_window : float ;
    obs_window : float ;
    percentile : float ;
    min : int option ;
    max : int option }

let to_int x =
  let open Sqlite3.Data in
  match x with
  | NONE | NULL | TEXT "" -> None
  | INT i -> Some (Int64.to_int i)
  | FLOAT f -> Some (int_of_float f)
  | TEXT s | BLOB s -> Some (int_of_string s)

let to_float x =
  let open Sqlite3.Data in
  match x with
  | NONE | NULL | TEXT "" -> None
  | INT i -> Some (Int64.to_float i)
  | FLOAT f -> Some f
  | TEXT s | BLOB s -> Some (float_of_string s)

let to_string x =
  let open Sqlite3.Data in
  match x with
  | NONE | NULL | TEXT "" -> None
  | INT i -> Some (Int64.to_string i)
  | FLOAT f -> Some (string_of_float f)
  | TEXT s | BLOB s -> Some s

let required = function
  | None -> failwith "Missing required value"
  | Some x -> x

let config_key_of_step db =
  let open Sqlite3 in
  { id = column db.get_config 0 |> to_int |> required ;
    mtime = column db.get_config 1 |> to_float |> required ;
    name = column db.get_config 2 |> to_string |> required ;
    source = column db.get_config 3 |> to_int ;
    dest = column db.get_config 4 |> to_int ;
    avg_window = column db.get_config 5 |> to_float |> required ;
    obs_window = column db.get_config 6 |> to_float |> required ;
    percentile = column db.get_config 7 |> to_float |> required ;
    min = column db.get_config 8 |> to_int ;
    max = column db.get_config 9 |> to_int }

(* Query to get flow alert parameters.
 * For each alert we need:
 * - a unique but meaningful name
 * - netflow source
 * - netflow dest
 * - bandwidth averaging window
 * - time window in the past to consider
 * - percentile to consider
 * - min/max values for that percentile of those averages over that window.
 *)
let flow_alert_params_query =
  "SELECT id, \
          strftime('%s', date_modify) as mtime, \
          zone_from || '-' || zone_to AS name, \
          zone_from AS source, \
          zone_to AS dest, \
          (5 * 60) AS avg_window, \
          (3600 * 12) AS obs_window, \
          0.95 AS percentile, \
          NULL AS \"min\", \
          bandwrate_alert_asc AS \"max\" \
   FROM bcnthresholds \
   WHERE \"max\" > 0 \
   UNION \
   SELECT id, \
          strftime('%s', date_modify) as mtime, \
          zone_to || '-' || zone_from AS name, \
          zone_to AS source, \
          zone_from AS dest, \
          (5 * 60) AS avg_window, \
          (3600 * 12) AS obs_window, \
          0.95 AS percentile, \
          NULL AS \"min\", \
          bandwrate_alert_dsc AS \"max\" \
   FROM bcnthresholds \
   WHERE \"max\" > 0"

let last_mtime_query =
  "SELECT strftime('%s', MAX(date_modify)) as mtime \
   FROM bcnthresholds"

let get_db filename =
  if debug then Printf.eprintf "Opening DB %S\n%!" filename ;
  let open Sqlite3 in
  try (
    let db = db_open ~mode:`READONLY filename in
    if debug then Printf.eprintf "got db handler\n%!" ;
    let get_mtime = prepare db last_mtime_query in
    let get_config = prepare db flow_alert_params_query in
    if debug then Printf.eprintf "SQL statements have been prepared\n%!" ;
    { db ; get_mtime ; get_config ;
      last_updated = 0. ; up_to_date = false }
  ) with exc -> (
    Printf.eprintf "Exception: %s\n%!" (Printexc.to_string exc) ;
    exit 1
  )

let check_config_changed db =
  let open Sqlite3 in
  try (
    if reset db.get_mtime <> Rc.OK then
      failwith "Cannot reset prepared statement for get_mtime" ;
    match step db.get_mtime with
    | Rc.ROW ->
      let t = column db.get_mtime 0 |> to_float |> required in
      if debug then Printf.eprintf "Max mtime in DB: %f\n%!" t ;
      if t > db.last_updated then (
        db.up_to_date <- false
      )
    | _ ->
      failwith "No idea what to do from this get_mtime result"
  ) with e ->
    Printf.eprintf "Cannot check_config_changed: %s, assuming no change.\n"
      (Printexc.to_string e)

let must_reload db =
  if debug && not db.up_to_date then
    Printf.eprintf "Configuration is not up to date with DB.\n%!" ;
  not db.up_to_date

let build_config alert_of_conf db =
  if debug then 
    Printf.eprintf "Building configuration from DB...\n%!" ;
  let open Sqlite3 in
  if reset db.get_config <> Rc.OK then
    failwith "Cannot reset prepared statement for get_config" ;
  let rec loop prev =
    match step db.get_config with
    | Rc.ROW ->
      if debug then Printf.eprintf "Read one DB row...\n%!" ;
      let config_key = config_key_of_step db in
      loop (alert_of_conf config_key :: prev)
    | Rc.DONE ->
      if debug then Printf.eprintf "Build a conf with %d alerts\n%!"
                      (List.length prev) ;
      prev
    | _ ->
      failwith "No idea what to do from this get_config result"
  in
  let conf = loop [] in
  db.up_to_date <- true ;
  conf

let make filename =
  let db = get_db filename in
  if debug then Printf.eprintf "Building conf from DB %S\n%!" filename ;
  Alarm.every 1.0 (fun () -> check_config_changed db) ;
  db
