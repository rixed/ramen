(* JSONRPC API to interact with Ramen.
 *
 * Allows to query the running schema, extract timeseries, and create
 * new nodes.
 *)
open Batteries
open RamenLog
open RamenHelpers
open RamenHttpHelpers
module C = RamenConf
module F = C.Func
module P = C.Program

(* To help the client to make sense of the error we distinguish between those
 * kind of errors: *)
exception ParseError of exn (* When we cannot parse the query. *)
exception BadRequest of string (* When there is an error in the query. *)
(* Everything else will be reported as "internal error". *)

let bad_request s = raise (BadRequest s)

let () =
  Printexc.register_printer (function
    | ParseError e ->
        Some ("Parse error: "^ Printexc.to_string e)
    | BadRequest s ->
        Some ("Error in request: "^ s)
    | _ -> None)

let parse_error_with_context ctx f =
  try
    fail_with_context ctx f
  with exn ->
    raise (ParseError exn)

module JSONRPC =
struct
  (* Id will be copied verbatim regardless of the type as long as it's a
   * JS value: *)
  type any_json = string * int (* value and position in the query *)
  type req = { method_ : string ; id : any_json ; params : any_json }

  let err id msg =
    let msg = strip_control_chars msg in
    Printf.sprintf "{\"id\":%s,\"error\":%s}"
      id (PPP_JSON.json_encoded_string msg) |>
    (* Assuming jsonrpc does not mix transport errors with applicative errors: *)
    http_msg

  let wrap (id, _) f =
    match f () with
    | exception e ->
        print_exception ~what:("Answering request "^id) e ;
        err id (match e with
          | ParseError _ | BadRequest _ -> Printexc.to_string e
          | e -> "Internal error: "^ Printexc.to_string e)
    | s ->
        Printf.sprintf "{\"id\":%s,\"result\":%s}" id s |>
        http_msg

  (* We need to parse this by hand due to dispatching on the "method" field
   * and unspecified type for the "id" field. PPP provides some good helpers
   * though: *)

  (* PPP for the JSONRPC request, returning the params as a verbatim string: *)
  let req_ppp =
    let open PPP in
    let any_json : any_json t =
      fun () ->
      { printer = (fun o (v, _) -> o v) ;
        scanner = (fun i o ->
          match skip_any PPP_JSON.groupings PPP_JSON.delims i o with
          | None ->
              parse_error o "Cannot parse json blurb"
          | Some o' ->
              !logger.info "Found any-json at loc %d" o ;
              let str = i o (o'-o) in
              (* We return both the string and its location, that will be used
               * later to adjust parse error locations: *)
              Ok ((str, o), o')) ;
        descr = fun _ -> "some json blurb" }
    in
    PPP_JSON.(record (
      field "method" string <->
      field "id" any_json <->
      field ~default:("", 0) "params" any_json)) >>:
    ((fun { method_ ; id ; params } -> Some (Some method_, Some id), Some params),
     (function Some (Some method_, Some id), Some params -> { method_ ; id ; params }
             | Some (Some method_, Some id), None -> { method_ ; id ; params = "", 0 }
             | _ -> assert false))

  let parse =
    parse_error_with_context "Parsing JSON" (fun () ->
      PPP.of_string_exc req_ppp)

  let json_any_parse ?(what="the") ppp (str, loc) =
    let ctx = "parsing "^ what ^" request" in
    let str = String.make loc ' ' ^ str in
    parse_error_with_context ctx (fun () ->
      PPP.of_string_exc ppp str)
end

(*
 * Get Ramen version
 *)

type version_resp = string [@@ppp PPP_JSON]

let version () =
  PPP.to_string version_resp_ppp_json RamenVersions.release_tag

(*
 * Get list of available tables which name starts with a given prefix
 *)

type get_tables_req = { prefix : string } [@@ppp PPP_JSON]

type get_tables_resp = (string, string) Hashtbl.t [@@ppp PPP_JSON]

let get_tables conf msg =
  let req = JSONRPC.json_any_parse ~what:"get-tables" get_tables_req_ppp_json msg in
  let tables = Hashtbl.create 31 in
  C.with_rlock conf (fun programs ->
    Hashtbl.iter (fun _prog_name (mre, get_rc) ->
      if mre.C.status = C.MustRun then match get_rc () with
      | exception _ -> ()
      | prog ->
          List.iter (fun f ->
            let fqn = RamenName.string_of_fq (F.fq_name f) in
            if f.F.event_time <> None && String.starts_with fqn req.prefix
            then Hashtbl.add tables fqn f.F.doc
          ) prog.P.funcs
    ) programs) ;
  PPP.to_string get_tables_resp_ppp_json tables

(*
 * Get the schema of a given set of tables.
 * Schema being: list of columns and of threshold-based alerts.
 *)

let empty_units = Hashtbl.create 0

type get_columns_req = string list [@@ppp PPP_JSON]

type get_columns_resp = (string, columns_info) Hashtbl.t [@@ppp PPP_JSON]

and columns_info = (string, column_info) Hashtbl.t [@@ppp PPP_JSON]

and column_info =
  { type_ : string [@ppp_rename "type"] ;
    units : (string, float) Hashtbl.t [@ppp_default empty_units] ;
    doc : string [@ppp_default ""] ;
    factor : bool [@ppp_default false] ;
    group_key : bool [@ppp_rename "group-key"] [@ppp_default false] ;
    alerts : alert_info_v1 list }
  [@@ppp PPP_JSON]

and alert_info_v1 =
  { enabled : bool [@ppp_default true] ;
    where : simple_filter list [@ppp_default []] ;
    having : simple_filter list [@ppp_default []] ;
    threshold : float ;
    recovery : float ;
    duration : float [@ppp_default 0.] ;
    ratio : float [@ppp_default 1.] ;
    time_step : float [@ppp_rename "time-step"] [@ppp_default 60.] ;
    (* Unused, for the client purpose only *)
    id : string [@ppp_default ""] ;
    (* Desc to use when firing/recovering: *)
    desc_firing : string [@ppp_default ""] ;
    desc_recovery : string [@ppp_default ""] }
  [@@ppp PPP_JSON]
  [@@ppp PPP_OCaml]

and simple_filter =
  { lhs : string ;
    rhs : string ;
    op : string [@ppp_default "="] }
  [@@ppp PPP_JSON]
  [@@ppp PPP_OCaml]

(* Alerts are saved on disc under this format: *)
and alert_source =
  | V1 of { table : string ; column : string ; alert : alert_info_v1 }
  (* ... and so on *)
  [@@ppp PPP_OCaml]

type ext_type = Numeric | String | Other

let ext_type_of_typ =
  let open RamenTypes in
  function
  | TString | TEth | TIpv4 | TIpv6 | TIp
  | TCidrv4 | TCidrv6 | TCidr ->
      String
  | x ->
      if is_a_num x then Numeric else Other

let string_of_ext_type = function
  | Numeric -> "numeric"
  | String -> "string"
  | Other -> "other"

(* We look for all keys which are simple fields, then look for a output field
 * forwarding that field, and return its name (in theory not only fields but
 * any expression yielding the same results.) *)
let group_keys_of_operation =
  let open RamenOperation in
  function
  | Aggregate { fields ; key ; _ } ->
      let simple_keys =
        List.filter_map (function
          | RamenExpr.Field (_t, tuple_prefix, name) ->
              Some (tuple_prefix, name)
          | _ -> None
        ) key in
      List.filter_map (fun sf ->
        match sf.expr with
        | Field (_t, tuple_prefix, name) when
            List.mem (tuple_prefix, name) simple_keys ->
            Some sf.alias
        | _ -> None
      ) fields
  | _ -> []

let alert_info_of_alert_source enabled = function
  | V1 { alert ; _ } -> { alert with enabled }

let alerts_of_column conf programs func column =
  (* All files with extension ".alert" in this directory is supposed to be
   * an alert description: *)
  let func_path = F.path func in
  let dir = C.api_alerts_root conf ^"/"^ func_path ^"/"^ column in
  if is_directory dir then
    Sys.readdir dir |>
    Array.fold_left (fun lst f ->
      if String.ends_with f ".alert" then
        match ppp_of_file alert_source_ppp_ocaml (dir ^"/"^ f) with
        | exception e ->
            print_exception ~what:"Error while listing alerts" e ;
            lst
        | a ->
            let id = Filename.remove_extension f in
            let program_name =
              RamenName.program_of_string (func_path ^"/"^ column ^"/"^ id) in
            !logger.debug "Program implementing alert %s: %s"
              id (RamenName.string_of_program program_name) ;
            let enabled = Hashtbl.mem programs program_name in
            alert_info_of_alert_source enabled a :: lst
      else lst
    ) []
  else []

let units_of_column ft =
  match ft.RamenTuple.units with
  | None ->
      (* For simplicity, there is currently no way to distinguish no
       * units from dimensionless: *)
      empty_units
  | Some units ->
      let h = Hashtbl.create 3 in
      RamenUnits.MapUnit.iter (fun n (e, r) ->
        let n = if r then n ^"(rel)" else n in
        Hashtbl.add h n e
      ) units ;
      h

let columns_of_func conf programs func =
  let h = Hashtbl.create 11 in
  let group_keys = group_keys_of_operation func.F.operation in
  List.iter (fun ft ->
    if not (is_private_field ft.RamenTuple.typ_name) then
      let type_ = ext_type_of_typ ft.typ.structure in
      if type_ <> Other then
        Hashtbl.add h ft.typ_name {
          type_ = string_of_ext_type type_ ;
          units = units_of_column ft ;
          doc = ft.doc ;
          factor = List.mem ft.typ_name func.F.factors ;
          group_key = List.mem ft.typ_name group_keys ;
          alerts = alerts_of_column conf programs func ft.typ_name }
  ) func.F.out_type ;
  h

let columns_of_table conf table =
  (* A function is what is called here in baby-talk a "table": *)
  let prog_name, func_name =
    RamenName.(fq_of_string table |> fq_parse) in
  C.with_rlock conf (fun programs ->
    match Hashtbl.find programs prog_name with
    | exception _ -> None
    | _mre, get_rc ->
      (match get_rc () with
      | exception _ -> None
      | prog ->
          (match List.find (fun f -> f.F.name = func_name) prog.P.funcs with
          | exception Not_found -> None
          | func -> Some (columns_of_func conf programs func))))

let get_columns conf msg =
  let req = JSONRPC.json_any_parse ~what:"get-columns" get_columns_req_ppp_json msg in
  let h = Hashtbl.create 9 in
  List.iter (fun table ->
    match columns_of_table conf table with
    | None -> ()
    | Some c -> Hashtbl.add h table c
  ) req ;
  PPP.to_string get_columns_resp_ppp_json h

(*
 * Get a timeseries for a given set of columns.
 *)

type get_timeseries_req =
  { since : float ;
    until : float ;
    num_points : int [@ppp_default 100] ;
    data : (string, timeseries_data_spec) Hashtbl.t }
  [@@ppp PPP_JSON]

and timeseries_data_spec =
  { select : string list ;
    where : simple_filter list [@ppp_default []] ;
    factors : string list [@ppp_default []] }
  [@@ppp PPP_JSON]

type get_timeseries_resp =
  { times : float array ;
    values : (string (* table *), table_values) Hashtbl.t }
  [@@ppp PPP_JSON]

and table_values =
  { column_labels : string list array ;
    (* One optional float per selected field, per label, per time: *)
    column_values : float option array array array }
  [@@ppp PPP_JSON]

let get_timeseries conf msg =
  let req = JSONRPC.json_any_parse ~what:"get-timeseries"
                                   get_timeseries_req_ppp_json msg in
  let times = Array.make_float req.num_points in
  let times_inited = ref false in
  let values = Hashtbl.create 5 in
  Hashtbl.iter (fun table data_spec ->
    let fq = RamenName.fq_of_string table in
    let prog_name, func_name = RamenName.fq_parse fq in
    let filters =
      C.with_rlock conf (fun programs ->
        let _mre, get_rc = Hashtbl.find programs prog_name in
        let prog = get_rc () in
        let func = List.find (fun f -> f.F.name = func_name) prog.funcs in
        List.fold_left (fun filters where ->
          if is_private_field where.lhs then
            bad_request ("Cannot filter through private field "^ where.lhs) ;
          let open RamenSerialization in
          let _, ftyp = find_field func.F.out_type where.lhs in
          let v = value_of_string ftyp.typ where.rhs in
          (where.lhs, where.op, v) :: filters
        ) [] data_spec.where) in
    let column_labels, datapoints =
      RamenTimeseries.get conf req.num_points req.since req.until
                          filters data_spec.factors fq data_spec.select in
    (* [column_labels] is an array of labels (empty if no result).
     * Each label is a list of factors values. *)
    let column_labels =
      Array.map (List.map RamenTypes.to_string) column_labels in
    let column_values = Array.create req.num_points [||] in
    Hashtbl.add values table { column_labels ; column_values } ;
    Enum.iteri (fun ti (t, data) ->
      (* in data we have one column per column-label: *)
      assert (Array.length data = Array.length column_labels) ;
      column_values.(ti) <- data ;
      if not !times_inited then times.(ti) <- t
    ) datapoints ;
    times_inited := true
  ) req.data ;
  let resp = { times ; values } in
  PPP.to_string get_timeseries_resp_ppp_json resp

(*
 * Save the alerts
 *)

type set_alerts_req =
  (string, (string, alert_info_v1 list) Hashtbl.t) Hashtbl.t
  [@@ppp PPP_JSON]

(* Alert ids are used to uniquely identify alerts (for instance when
 * saving on disc). Alerts are identified by their defining properties,
 * which exclude "enabled" or other similar meta info, should we have
 * some. This is not to be confused with the field "id" from alert_info,
 * which is the id for the user and that, as far as we are concerned, need
 * not even be unique. But we have to save this user id as well even when
 * its the only thing that changed so it's easier to make it part of this
 * hash. *)
let alert_id column =
  let filterspec filter =
    IO.to_string
      (List.print ~first:"" ~last:"" ~sep:"-"
        (fun oc w -> Printf.fprintf oc "(%s %s %s)" w.op w.lhs w.rhs))
      filter in
  function
  | V1 { alert = { threshold ; where ; having ; recovery ; duration ; ratio ;
                   id ; _ } ; _ } ->
      Legacy.Printf.sprintf "V1-%s-%h-%h-%h-%h-%s-%s-%s"
         column threshold recovery duration ratio id
         (filterspec where)
         (filterspec having) |> md5

let is_enabled = function
  | V1 { alert = { enabled ; _ } ; _ } -> enabled

let func_of_table programs table =
  let pn, fn =
    RamenName.(fq_of_string table |> fq_parse) in
  match Hashtbl.find programs pn with
  | exception Not_found ->
      Printf.sprintf "Program %s does not exist"
        (RamenName.string_of_program pn) |>
      bad_request
  | _mre, get_rc ->
      let prog = get_rc () in
      (try List.find (fun f -> f.F.name = fn) prog.P.funcs
      with Not_found ->
        Printf.sprintf "No function %s in program %s"
          (RamenName.string_of_func fn)
          (RamenName.string_of_program pn) |>
        bad_request)

let field_typ_of_column programs table column =
  let open RamenTuple in
  let func = func_of_table programs table in
  try
    List.find (fun t -> t.typ_name = column) func.F.out_type
  with Not_found ->
    Printf.sprintf "No column %s in table %s" column table |>
    bad_request

let generate_alert programs src_file (V1 { table ; column ; alert = a }) =
  File.with_file_out ~mode:[`create; `text ; `trunc] src_file (fun oc ->
    Printf.fprintf oc
      "-- Alert automatically generated by ramen %s at %s\n\n"
      RamenVersions.release_tag (ctime (Unix.time ())) ;
    (* TODO: if this sticks make the URL a parameter: *)
    let desc_link =
      Printf.sprintf
        "See https://${env.HOSTNAME}/view_alert?id=%s for details"
        (CodecUrl.encode a.id) in
    let desc_firing =
      if a.desc_firing <> "" then a.desc_firing else
        Printf.sprintf "%s went %s the configured threshold %f.\n%s"
          column
          (if a.threshold >= a.recovery then "above" else "below")
          a.threshold
          desc_link
    and desc_recovery =
      if a.desc_recovery <> "" then a.desc_recovery else
        Printf.sprintf "The value of %s recovered.\n%s"
          column
          desc_link
    and print_filter oc filter =
      if filter = [] then String.print oc "true" else
      List.print ~first:"" ~sep:" AND " ~last:""
        (fun oc w ->
          let ft = field_typ_of_column programs table w.lhs in
          let v = RamenSerialization.value_of_string ft.RamenTuple.typ w.rhs in
          Printf.fprintf oc "(%s %s %a)"
            (ramen_quote w.lhs) w.op
            RamenTypes.print v)
        oc filter
    and default_aggr_of_field fn =
      let ft = field_typ_of_column programs table fn in
      ft.RamenTuple.aggr |? "avg"
    in
    (* First we need to resample the TS with the desired time step,
     * aggregating all values for the desired column: *)
    Printf.fprintf oc "-- Alerting program\n\n" ;
    Printf.fprintf oc "DEFINE resampled AS\n" ;
    Printf.fprintf oc "  FROM %s\n" (ramen_quote table) ;
    Printf.fprintf oc "  WHERE %a\n" print_filter a.where ;
    Printf.fprintf oc "  GROUP BY u32(floor(start / %f))\n" a.time_step ;
    Printf.fprintf oc "  SELECT\n" ;
    Printf.fprintf oc "    floor(start / %f) * %f AS start,\n"
      a.time_step a.time_step ;
    Printf.fprintf oc "    start + %f AS stop,\n"
      a.time_step ;
    (* Also select all the fields used in the HAVING filter: *)
    List.iter (fun h ->
      Printf.fprintf oc "    %s %s AS %s, -- for HAVING\n"
        (default_aggr_of_field h.lhs)
        (ramen_quote h.lhs) (ramen_quote h.lhs)
    ) a.having ;
    Printf.fprintf oc "    min %s AS min_value,\n" (ramen_quote column) ;
    Printf.fprintf oc "    max %s AS max_value,\n" (ramen_quote column) ;
    Printf.fprintf oc "    %s %s AS avg_value\n"
      (default_aggr_of_field column) (ramen_quote column) ;
    Printf.fprintf oc "  COMMIT AFTER in.start > out.start + 1.5 * %f;\n\n"
      a.time_step ;
    (* Then we want for each point to find out if it's within the acceptable
     * boundaries or not, using hysteresis: *)
    Printf.fprintf oc "DEFINE ok AS\n" ;
    Printf.fprintf oc "  FROM resampled\n" ;
    Printf.fprintf oc "  SELECT\n" ;
    Printf.fprintf oc "    min_value, max_value,\n" ;
    Printf.fprintf oc "    IF (%a) THEN avg_value AS filtered_avg_value,\n"
       print_filter a.having ;
    Printf.fprintf oc "    COALESCE(\n" ;
    Printf.fprintf oc "      HYSTERESIS (filtered_avg_value, %f, %f),\n"
      a.recovery a.threshold ;
    (* Be healthy when filtered_avg_value is NULL: *)
    Printf.fprintf oc "    true) AS ok;\n\n" ;
    (* Then we fire an alert if too many values are unhealthy: *)
    Printf.fprintf oc "DEFINE alert AS\n" ;
    Printf.fprintf oc "  FROM ok\n" ;
    Printf.fprintf oc "  SELECT\n" ;
    Printf.fprintf oc "    max_value, min_value,\n" ;
    Printf.fprintf oc "    moveavg(%d, float(not ok)) > %f AS firing\n"
      (round_to_int (a.duration /. a.time_step)) a.ratio ;
    Printf.fprintf oc "  NOTIFY \"%s is off!\" WITH\n" column ;
    Printf.fprintf oc "    firing AS firing,\n" ;
    Printf.fprintf oc "    1 AS certainty,\n" ;
    (* This cast to string can handle the NULL case: *)
    Printf.fprintf oc "    \"${min_value},${max_value}\" AS values,\n" ;
    Printf.fprintf oc "    %f AS thresholds,\n" a.threshold ;
    Printf.fprintf oc "    (IF firing THEN %S ELSE %S) AS desc\n"
      desc_firing desc_recovery ;
    (* TODO: a way to add zone, service, etc, if present in the
     * parent table *)
    Printf.fprintf oc "  AND KEEP ALL\n" ;
    Printf.fprintf oc "  AFTER firing <> COALESCE(previous.firing, false);\n")

(* Register a rule to turn an alert into a ramen source file: *)
let () =
  RamenMake.register "alert" "ramen"
    RamenMake.target_is_older
    (fun conf _prog_name src_file target_file ->
      let a = ppp_of_file alert_source_ppp_ocaml src_file in
      C.with_rlock conf (fun programs ->
        generate_alert programs target_file a))

let stop_alert conf program_name =
  let glob =
    Globs.(RamenName.string_of_program program_name |> escape |> compile) in
  let num_kills = RamenRun.kill conf [ glob ] in
  if num_kills < 0 || num_kills > 1 then
    !logger.error "When attempting to kill alert %s, got num_kill = %d"
      (RamenName.string_of_program program_name) num_kills

let same_file_exist fname new_alert =
  file_exists fname &&
  match ppp_of_file alert_source_ppp_ocaml fname with
  | exception e ->
      print_exception ~what:"Error while comparing with previous alert" e ;
      false
  | old_alert -> old_alert = new_alert

let save_alert conf program_name alert_info =
  let program_name = RamenName.program_of_string program_name in
  let basename =
    C.api_alerts_root conf ^"/"^ RamenName.path_of_program program_name in
  let src_file = basename ^".alert" in
  (* Avoid triggering a recompilation if it's unchanged: *)
  if same_file_exist src_file alert_info then
    !logger.debug "Alert %s preexist with same definition" src_file
  else (
    !logger.info "Saving new alert into %s" src_file ;
    ppp_to_file ~pretty:true src_file alert_source_ppp_ocaml alert_info ;
    if is_enabled alert_info then (
      try
        (* Compile right now so that we can report errors to the client and RamenRun.run
         * can check linkage errors: *)
        let exec_file = basename ^".x" in
        RamenMake.build conf program_name src_file exec_file ;
        let debug = conf.C.log_level = Debug in
        let params = Hashtbl.create 0 in
        RamenRun.run conf params true RamenConsts.Default.report_period
                     program_name ~src_file exec_file debug
      with e ->
        (* In case of error, do not leave the alert definition file so that the
         * client can retry, but keep it for later inspection: *)
        log_and_ignore_exceptions move_file_away src_file ;
        raise e
    ) else
      (* Won't do anything if it's not running *)
      stop_alert conf program_name)

let set_alerts conf msg =
  let req = JSONRPC.json_any_parse ~what:"set-alerts" set_alerts_req_ppp_json msg in
  (* In case the same table/column appear several times, build a single list
   * of all preexisting alert files for the mentioned tables/columns, and a
   * list of all that are set: *)
  let old_alerts = ref Set.String.empty
  and new_alerts = ref Set.String.empty in
  Hashtbl.iter (fun table columns ->
    Hashtbl.iter (fun column alerts ->
      (* All non listed alerts must be suppressed *)
      let parent =
        RamenName.program_of_string (table ^"/"^ column) in
      let dir =
        C.api_alerts_root conf ^"/"^
        RamenName.path_of_program parent in
      if is_directory dir then (
        Sys.readdir dir |>
        Array.iter (fun f ->
          if String.ends_with f ".alert" then
            let id = Filename.remove_extension f in
            let program_name =
              RamenName.string_of_program parent ^"/"^ id in
            old_alerts:= Set.String.add program_name !old_alerts)) ;
      List.iter (fun alert ->
        (* Check the alert: *)
        C.with_rlock conf (fun programs ->
          let ft = field_typ_of_column programs table column in
          if ext_type_of_typ ft.RamenTuple.typ.structure <> Numeric then
            Printf.sprintf "Column %s of table %s is not numeric"
              column table |>
            bad_request ;
          (* Also check that table has event time info: *)
          let func = func_of_table programs table in
          if func.event_time = None then
            Printf.sprintf "Table %s has no event time information" table |>
            bad_request) ;
        (* We receive only the latest version: *)
        let alert_source = V1 { table ; column ; alert } in
        let id = alert_id column alert_source in
        let program_name = table ^"/"^ column ^"/"^ id in
        new_alerts := Set.String.add program_name !new_alerts ;
        save_alert conf program_name alert_source
      ) alerts
    ) columns
  ) req ;
  let to_delete = Set.String.diff !old_alerts !new_alerts in
  if not (Set.String.is_empty to_delete) then (
    !logger.info "going to delete non mentioned alerts %a"
      (Set.String.print String.print) to_delete ;
    Set.String.iter (fun program_name ->
      let program_name = RamenName.program_of_string program_name in
      stop_alert conf program_name ;
      let fname =
        C.api_alerts_root conf ^"/"^
        RamenName.(path_of_program program_name)
        ^".alert" in
      safe_unlink fname
    ) to_delete)

(*
 * Dispatch queries
 *)

let router conf prefix =
  (* The function called for each HTTP request: *)
  fun _meth path _params _headers body ->
    let prefix = list_of_prefix prefix in
    let path = chop_prefix prefix path in
    if path <> [] then raise BadPrefix
    else
      let open JSONRPC in
      let req = parse body in
      wrap req.id (fun () ->
        match String.lowercase_ascii req.method_ with
        | "version" -> version ()
        | "get-tables" -> get_tables conf req.params
        | "get-columns" -> get_columns conf req.params
        | "get-timeseries" -> get_timeseries conf req.params
        | "set-alerts" -> set_alerts conf req.params ; "null"
        | m -> bad_request (Printf.sprintf "unknown method %S" m))
