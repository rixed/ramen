(* Used by run_test to build the alerting config DB. *)
open Batteries
open Helpers
open Sqlite3

let schema =
  "CREATE TABLE bcnthresholds ( \
     date_modify DATETIME DEFAULT (datetime('now','localtime')), \
     is_symmetric BOOLEAN NOT NULL DEFAULT 1, \
     zone_from INTEGER NOT NULL, \
     zone_to INTEGER NOT NULL, \
     bandwrate_alert_asc INTEGER DEFAULT 100, \
     bandwrate_alert_dsc INTEGER DEFAULT 100, \
     bandw_available_asc INTEGER, \
     bandw_available_dsc INTEGER, \
     bandw_min_asc FLOAT, \
     bandw_min_dsc FLOAT, \
     rtt_alert_asc INTEGER, \
     rtt_alert_dsc INTEGER, \
     rr_alert_asc INTEGER, \
     rr_alert_dsc INTEGER);"

let create ?(debug=false) fname entries =
  let open TestGen in
  ignore_exceptions Unix.unlink fname ;
  let db = db_open fname in
  if debug then Printf.eprintf "Creating alerting conf DB in %s\n" fname ;
  exec db schema |> must_be_ok ;
  (* Our entries are unidirectional, while the current DB does both
   * directions. No effort is made to try to merge entries into bidirectional
   * DB. TODO: improve current DB. *)
  let stmt =
    prepare db "INSERT INTO bcnthresholds (\
                  zone_from, zone_to, \
                  bandw_available_asc, bandw_available_dsc, \
                  bandw_min_asc, bandw_min_dsc, \
                  rtt_alert_asc, rtt_alert_dsc, \
                  rr_alert_asc, rr_alert_dsc \
                ) VALUES (\
                  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" in
  List.iter (fun e ->
    reset stmt |> must_be_ok ;
    bind stmt 1 (of_int e.zone_from) |> must_be_ok ;
    bind stmt 2 (of_int e.zone_to) |> must_be_ok ;
    bind stmt 3 (of_int_or_null e.max) |> must_be_ok ;
    bind stmt 4 (of_int_or_null e.max) |> must_be_ok ;
    bind stmt 5 (of_float_or_null (Option.map float_of_int e.relevancy)) |> must_be_ok ;
    bind stmt 6 (of_float_or_null (Option.map float_of_int e.relevancy)) |> must_be_ok ;
    bind stmt 7 (of_int_or_null e.max_rtt) |> must_be_ok ;
    bind stmt 8 (of_int_or_null e.max_rtt) |> must_be_ok ;
    bind stmt 9 (of_int_or_null e.max_rr) |> must_be_ok ;
    bind stmt 10 (of_int_or_null e.max_rr) |> must_be_ok ;
    if debug then Printf.eprintf "Inserting a tuple in %s\n" fname ;
    step stmt |> must_be_done) entries ;
  finalize stmt |> must_be_ok ;
  while not (db_close db) do
    Unix.sleep 1 ;
    Printf.eprintf "Waiting to close DB %s\n%!" fname
  done
