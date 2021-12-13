(* JSONRPC API to interact with Ramen.
 *
 * Allows to query the running schema, extract time series, and create
 * new nodes.
 *)
open Batteries

open RamenLog
open RamenHelpersNoLog
open RamenHelpers
open RamenHttpHelpers
open RamenSyncHelpers
open RamenLang
open RamenConsts
open RamenSync
module C = RamenConf
module VA = Value.Alert
module VSI = Value.SourceInfo
module E = RamenExpr
module O = RamenOperation
module DT = DessserTypes
module T = RamenTypes
module Timeseries = RamenTimeseries
module N = RamenName
module Variable = RamenVariable
module ZMQClient = RamenSyncZMQClient

(* To help the client to make sense of the error we distinguish between those
 * kind of errors: *)
exception ParseError of exn (* When we cannot parse the query. *)
exception BadRequest of string (* When there is an error in the query. *)
exception RateLimited
(* Everything else will be reported as "internal error". *)

let bad_request s = raise (BadRequest s)

let () =
  Printexc.register_printer (function
    | ParseError e ->
        Some ("Parse error: "^ Printexc.to_string e)
    | BadRequest s ->
        Some ("Error in request: "^ s)
    | RateLimited ->
        Some "Rate limited"
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

  let wrap body (id, _) f =
    match f () with
    | exception e ->
        let what =
          Printf.sprintf "While answering request %S" (abbrev 200 body) in
        let is_internal =
          match e with ParseError _ | BadRequest _ | RateLimited -> true
                     | _ -> false in
        print_exception ~what ~with_backtrace:(not is_internal) e ;
        err id (
          if is_internal then
            Printexc.to_string e
          else
            "Internal error: "^ Printexc.to_string e)
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
              !logger.debug "Found any-json at loc %d" o ;
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

let while_ () = !RamenProcesses.quit = None

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

let get_tables table_prefix session msg =
  let req =
    JSONRPC.json_any_parse ~what:"get-tables" get_tables_req_ppp_json msg in
  let tables = Hashtbl.create 31 in
  let programs = get_programs session in
  !logger.debug "Programs: %a"
    (Enum.print N.program_print) (Hashtbl.keys programs) ;
  Hashtbl.iter (fun prog_name prog ->
    List.iter (fun f ->
      let fqn = (VSI.fq_name prog_name f :> string)
      and event_time =
        O.event_time_of_operation f.VSI.operation in
      if event_time <> None && String.starts_with fqn (table_prefix ^ req.prefix)
      then
        let fqn' = String.lchop ~n:(String.length table_prefix) fqn in
        Hashtbl.add tables fqn' f.VSI.doc
    ) prog.VSI.funcs
  ) programs ;
  PPP.to_string get_tables_resp_ppp_json tables

(* Alert definitions
 * See RamenSync.Value.Alert.t for explanations on the alerting fields
 *)

module AlertInfoV1 =
struct
  type simple_filter =
    { lhs : N.field ;
      rhs : string ;
      op : string [@ppp_default "="] }
    [@@ppp PPP_JSON]

  type t =
    { enabled : bool [@ppp_default true] ;
      where : simple_filter list [@ppp_default []] ;
      having : simple_filter list [@ppp_default []] ;
      threshold : float ;
      recovery : float ;
      duration : float [@ppp_default 0.] ;
      ratio : float [@ppp_default 1.] ;
      time_step : float [@ppp_rename "time-step"] [@ppp_default 0.] ;
      tops : N.field list [@ppp_default []] ;
      (* Renamed as "carry" for retro-compatibility with old HTTP clients
       * (FIXME: obsolete all this) *)
      carry_fields : N.field list
        [@ppp_rename "carry"] [@ppp_default []] ;
      carry_csts : (N.field * string) list
        [@ppp_rename "carry-csts"] [@ppp_default []] ;
      id : string [@ppp_default ""] ;
      desc_title : string [@ppp_rename "desc-title"] [@ppp_default ""] ;
      desc_firing : string [@ppp_rename "desc-firing"] [@ppp_default ""] ;
      desc_recovery : string [@ppp_rename "desc-recovery"] [@ppp_default ""] }
    [@@ppp PPP_JSON]
end

(*
 * Get the schema of a given set of tables.
 * Schema being: list of columns and of threshold-based alerts.
 *)

let empty_units = Hashtbl.create 0

type get_columns_req = string list [@@ppp PPP_JSON]

type get_columns_resp = (string, columns_info) Hashtbl.t [@@ppp PPP_JSON]

and columns_info = (N.field, column_info) Hashtbl.t [@@ppp PPP_JSON]

and column_info =
  { type_ : string [@ppp_rename "type"] ;
    units : (string, float) Hashtbl.t ;
    doc : string ;
    factor : bool ;
    group_key : bool [@ppp_rename "group-key"] ;
    alerts : AlertInfoV1.t list }
  [@@ppp PPP_JSON]

type ext_type = Numeric | String | Other

let ext_type_of_typ =
  let open RamenTypes in
  function
  | DT.TString
  | TUsr { name = "Eth"|"Ip4"|"Ip6"|"Ip"|"Cidr4"|"Cidr6"|"Cidr" ; _ } ->
      String
  | x ->
      if is_num x then Numeric else Other

let string_of_ext_type = function
  | Numeric -> "numeric"
  | String -> "string"
  | Other -> "other"

(* We look for all keys which are simple fields (but not start/stop), then look
 * for an output field forwarding that field, and return its name (in theory
 * not only fields but any expression yielding the same results.) *)
let group_keys_of_operation =
  let open Raql_path_comp.DessserGen in
  let open Raql_select_field.DessserGen in
  function
  | O.Aggregate { aggregate_fields ; key ; _ } ->
      let simple_keys =
        List.filter_map (fun e ->
          match e.E.text with
          | Stateless (SL0 (Path [ Name n ]))
            when n <> N.field "start" &&
                 n <> N.field "stop" ->
              Some (Variable.In, n)
          | Stateless (SL2 (
                Get, { text = Stateless (SL0 (Const (VString n))) ; _ },
                     { text = Stateless (SL0 (Variable pref)) ; _ }))
            when n <> "start" && n <> "stop" ->
              Some (pref, N.field n)
          | _ -> None
        ) key in
      List.filter_map (fun sf ->
        match sf.expr.text with
        | Stateless (SL0 (Path [ Name n ]))
          when List.mem (Variable.In, n) simple_keys ->
            Some sf.alias
        | Stateless (SL2 (
              Get, { text = Stateless (SL0 (Const (VString n))) ; _ },
                   { text = Stateless (SL0 (Variable pref)) ; _ }))
          when List.mem (pref, N.field n) simple_keys ->
            Some sf.alias
        | _ -> None
      ) aggregate_fields
  | _ -> []

(* For custom API, where to store alerting thresholds: *)
let api_alerts_root = N.src_path "api/set_alerts"

let src_path_of_alert_info alert =
  N.src_path_cat [ api_alerts_root ; N.src_path alert.VA.id ]

let units_of_column ft =
  match ft.RamenTuple.units with
  | None ->
      (* For simplicity, there is currently no way to distinguish no
       * units from dimensionless: *)
      empty_units
  | Some units ->
      let h = Hashtbl.create 3 in
      Array.iter (fun (n, (e, r)) ->
        let n = if r then n ^"(rel)" else n in
        Hashtbl.add h n e
      ) units ;
      h

(* Returns the alist of src_path * alerts for the given table and column: *)
let get_alerts session table column =
  let clt = option_get "get_alerts" __LOC__ session.ZMQClient.clt in
  let prefix = "sources/" in
  Client.fold clt ~prefix (fun k hv lst ->
    match k, hv.value with
    | Key.Sources (src_path, "alert"),
      Value.Alert alert
      when N.starts_with src_path api_alerts_root ->
        if table = alert.VA.table && column = alert.column then
          (src_path, alert) :: lst
        else
          lst
    | _ ->
        lst
  ) []

(* Returns alerts in V1 format only ; V2 alerts are ignored. *)
let alert_of_sync_value a =
  let simple_filter_of_sync f =
    AlertInfoV1.{
      lhs = f.VA.SimpleFilter.lhs ;
      rhs = f.rhs ;
      op = f.op } in
  let threshold, recovery =
    match a.VA.threshold with
    | VA.Constant threshold ->
        let recovery = threshold +. a.hysteresis in
        threshold, recovery
    | VA.Baseline _ ->
        (* Wait until we know how to improve the API before doing anything: *)
        !logger.error "Cannot convert baseline-based alerts into v1 API" ;
        0., 0. in
  AlertInfoV1.{
    enabled = a.enabled ;
    where = List.map simple_filter_of_sync a.where ;
    having = List.map simple_filter_of_sync a.having ;
    threshold ;
    recovery ;
    duration = a.duration ;
    ratio = a.ratio ;
    time_step = a.time_step ;
    tops = a.tops ;
    carry_fields = a.carry_fields ;
    carry_csts = List.map (fun cst -> cst.VA.name, cst.value) a.carry_csts ;
    id = a.id ;
    desc_title = a.desc_title ;
    desc_firing = a.desc_firing ;
    desc_recovery = a.desc_recovery }

let columns_of_func session prog_name func =
  let h = Hashtbl.create 11 in
  let fq = VSI.fq_name prog_name func in
  let group_keys = group_keys_of_operation func.VSI.operation in
  O.out_type_of_operation ~with_priv:true func.VSI.operation |>
  List.iter (fun ft ->
    if not (N.is_private ft.RamenTuple.name) then (
      let type_ = ext_type_of_typ ft.typ.DT.typ in
      if type_ <> Other then
        let factors =
          O.factors_of_operation func.operation in
        let alerts =
          List.map (alert_of_sync_value % snd)
                   (get_alerts session fq ft.name) in
        Hashtbl.add h ft.name {
          type_ = string_of_ext_type type_ ;
          units = units_of_column ft ;
          doc = ft.doc ;
          factor = List.mem ft.name factors ;
          group_key = List.mem ft.name group_keys ;
          alerts }
    )
  ) ;
  h

exception NoSuchProgram of N.program
exception NoSuchFunction of N.program * N.func
exception Return of VSI.compiled_program

let find_program programs prog_name =
  try Hashtbl.find programs prog_name
  with Not_found -> raise (NoSuchProgram prog_name)

let func_of_table programs table =
  let prog_name, func_name = N.(fq table |> fq_parse) in
  let prog =
    if String.ends_with (prog_name :> string) "#_" then (
      let prog_name = N.chop_suffix prog_name in
      !logger.debug "Looking for any table implementing %a"
        N.program_print prog_name ;
      try
        Hashtbl.iter (fun prog_name' prog ->
          let prog_name' = N.chop_suffix prog_name' in
          if prog_name' = prog_name then raise (Return prog)
        ) programs ;
        raise (NoSuchProgram prog_name)
      with Return prog ->
        prog
    ) else (
      find_program programs prog_name
    ) in
  try
    List.find (fun f -> f.VSI.name = func_name) prog.VSI.funcs
  with Not_found ->
    raise (NoSuchFunction (prog_name, func_name))

let func_of_table_or_bad_req programs table =
  try func_of_table programs table with
  | NoSuchProgram p ->
      Printf.sprintf2 "Program %a does not exist"
        N.program_print p |>
      bad_request
  | NoSuchFunction (p, f) ->
      Printf.sprintf2 "No function %a in program %a"
        N.func_print f
        N.program_print p |>
      bad_request

let columns_of_table session table =
  (* A function is what is called here in baby-talk a "table": *)
  let prog_name, _func_name = N.(fq table |> fq_parse) in
  let programs = get_programs session in
  match func_of_table programs table with
  | exception _ -> None
  | func -> Some (columns_of_func session prog_name func)

let get_columns table_prefix session msg =
  let req = JSONRPC.json_any_parse ~what:"get-columns"
                                   get_columns_req_ppp_json msg in
  let h = Hashtbl.create 9 in
  List.iter (fun table ->
    match columns_of_table session (table_prefix ^ table) with
    | None -> ()
    | Some c -> Hashtbl.add h table c
  ) req ;
  PPP.to_string get_columns_resp_ppp_json h

(*
 * Get a time series for a given set of columns.
 *)

type get_timeseries_req =
  { since : float ;
    until : float ;
    consolidation : string [@ppp_default ""] ; (* "" => default aggr *)
    bucket_time : string [@ppp_rename "bucket-time"] [@ppp_default "end"] ;
    (* One and only one of these two must be set (>0): *)
    num_points : int [@ppp_rename "num-points"] [@ppp_default 0] ;
    num_points_ : int [@ppp_rename "num_points"] [@ppp_default 0] ;
    time_step : float [@ppp_rename "time-step"] [@ppp_default 0.] ;
    data : (string, timeseries_data_spec) Hashtbl.t }
  [@@ppp PPP_JSON]

and timeseries_data_spec =
  { select : N.field list ;
    where : AlertInfoV1.simple_filter list [@ppp_default []] ;
    factors : N.field list [@ppp_default []] }
  [@@ppp PPP_JSON]

let check_get_timeseries_req req =
  if req.since > req.until then bad_request "since must come before until" ;
  if req.num_points <= 0 && req.time_step <= 0. then
    bad_request "must set either num-points or time-step" ;
  if req.num_points > 0 && req.time_step > 0. then
    bad_request "must set only one of num-points or time-step"

type get_timeseries_resp =
  { times : float array ;
    values : (string (* table *), table_values) Hashtbl.t }
  [@@ppp PPP_JSON]

and table_values =
  { column_labels : string array array ;
    (* One optional float per selected field, per label, per time: *)
    column_values : float option array array array }
  [@@ppp PPP_JSON]

let empty_values = Hashtbl.create 0

let get_timeseries conf table_prefix session msg =
  let req = JSONRPC.json_any_parse ~what:"get-timeseries"
                                   get_timeseries_req_ppp_json msg in
  (* Accept both "num-points" (new) and "num_points" (old): *)
  let req =
    if req.num_points <> 0 || req.num_points_ = 0 then req else
    { req with num_points = req.num_points_ } in
  check_get_timeseries_req req ;
  let num_points, since, until =
    Timeseries.compute_num_points
      req.time_step req.num_points req.since req.until in
  let times = Array.make_float num_points in
  let times_inited = ref false in
  let values = Hashtbl.create 5 in
  let programs = get_programs session in
  Hashtbl.iter (fun table data_spec ->
    let worker = N.worker (table_prefix ^ table) in
    let _site_name, prog_name, func_name = N.worker_parse worker in
    let filters =
      (* Even if the program has been killed we want to be able to output
       * its time series: *)
      let prog = find_program programs prog_name in
      let func = List.find (fun f -> f.VSI.name = func_name) prog.funcs in
      List.fold_left (fun filters where ->
        let open RamenSerialization in
        try
          let ser =
            O.out_record_of_operation ~with_priv:false func.VSI.operation in
          let _, mn = find_field ser where.AlertInfoV1.lhs in
          let v = value_of_string mn where.rhs in
          (where.lhs, where.op, v) :: filters
        with Failure msg -> bad_request msg
      ) [] data_spec.where in
    let column_labels, datapoints =
      let consolidation =
        let s = req.consolidation in
        if s = "" then None else Some s in
      let bucket_time =
        match String.lowercase_ascii req.bucket_time with
        | "begin" -> Timeseries.Begin
        | "middle" -> Timeseries.Middle
        | "end" -> Timeseries.End
        | _ -> bad_request "The only possible values for bucket_time are begin, \
                            middle and end" in
      Timeseries.get
        conf session num_points since until filters data_spec.factors
        ?consolidation ~bucket_time worker data_spec.select ~while_ in
    (* [column_labels] is an array of labels (empty if no result).
     * Each label is an array of factors values. *)
    let column_labels =
      Array.map (Array.map T.to_string) column_labels in
    let column_values = Array.create num_points [||] in
    Hashtbl.add values table { column_labels ; column_values } ;
    Enum.iteri (fun ti (t, data) ->
      (* in data we have one column per column-label: *)
      assert (Array.length data = Array.length column_labels) ;
      column_values.(ti) <- data ;
      if not !times_inited then times.(ti) <- t
    ) datapoints ;
    times_inited := true
  ) req.data ;
  let resp =
    if !times_inited then { times ; values }
    else { times = [||] ; values = empty_values } in
  PPP.to_string get_timeseries_resp_ppp_json resp

(*
 * Save the alerts
 *)

(* FIXME: do not use [programs] but instead RamenSync.function_of_fq *)
let field_typ_of_column programs table column =
  let open RamenTuple in
  let func = func_of_table_or_bad_req programs table in
  try
    O.out_type_of_operation ~with_priv:false func.VSI.operation |>
    List.find (fun ft -> ft.RamenTuple.name = column)
  with Not_found ->
    Printf.sprintf2 "No column %a in table %s"
      N.field_print column
      table |>
    bad_request

let stop_alert session src_path =
  (* Alerting programs have no suffixes: *)
  let program_name = (src_path : N.src_path :> string) in
  let glob = Globs.escape program_name in
  (* As we are also deleting the binary better purge the conf as per
   * https://github.com/rixed/ramen/issues/548 *)
  let num_kills = RamenRun.kill ~while_ ~purge:true session [ glob ] in
  if num_kills < 0 || num_kills > 1 then
    !logger.error "When attempting to kill alert %s, got num_kill = %d"
      program_name num_kills

(* Delete the source of an alert program that's been stopped already: *)
let delete_alert session src_path =
  let delete_ext ext =
    let src_k = Key.Sources (src_path, ext) in
    !logger.info "Deleting alert %a" Key.print src_k ;
    ZMQClient.send_cmd ~eager:true session (DelKey src_k) ;
  in
  List.iter delete_ext [ "alert" ; "ramen" ; "info" ]

let save_alert session src_path a =
  let clt = option_get "save_alert" __LOC__ session.ZMQClient.clt in
  let src_k = Key.Sources (src_path, "alert") in
  (* Avoid touching the source and recompiling for no reason: *)
  let is_new =
    match (Client.find clt src_k).value with
    | exception Not_found -> true
    | Value.Alert a' -> a' <> a
    | v ->
        err_sync_type src_k v "an Alert" ;
        true in
  if is_new then (
    ZMQClient.send_cmd ~eager:true session
      (SetKey (src_k, Value.Alert a)) ;
    !logger.info "Saved new alert into %a" Key.print src_k ;
  ) else (
    (* TODO: do nothing once the dust has settled down: *)
    !logger.info "Alert %a preexist with same definition"
      Key.print src_k
  ) ;
  (* Also make sure it is running *)
  !logger.info "Making sure the alert is running..." ;
  let debug = !logger.log_level = Debug
  and params = Hashtbl.create 0
  and on_sites = Globs.all (* TODO *)
  and replace = true in
  (* Alerts use no suffix: *)
  let program_name = N.program (src_path :> string) in
  RamenRun.do_run ~while_ session program_name
                  Default.report_period on_sites debug params replace

(* Returns the set of the src_paths of the alerts that are currently defined
 * for this table and column: *)
let get_alert_paths session table column =
  get_alerts session table column |>
  List.fold_left (fun s (src_path, _) ->
    Set.add src_path s
  ) Set.empty

let check_alert a =
  if a.VA.duration < 0. then
    bad_request "Duration must be positive" ;
  if a.ratio < 0. || a.ratio > 1. then
    bad_request "Ratio must be between 0 and 1" ;
  if a.time_step < 0. then
    bad_request "Time step must be greater than 0"

let check_column programs table column =
  let ft = field_typ_of_column programs table column in
  if ext_type_of_typ ft.RamenTuple.typ.DT.typ <> Numeric then
    Printf.sprintf2 "Column %a of %s is not numeric"
      N.field_print column
      table |>
    bad_request ;
  (* Also check that table has event time info: *)
  let func = func_of_table_or_bad_req programs table in
  if O.event_time_of_operation func.VSI.operation = None
  then
    Printf.sprintf2 "Table %s has no event time information"
      table |>
    bad_request

let delete_alerts session to_delete =
  !logger.debug "delete_alerts!" ;
  if not (Set.is_empty to_delete) then (
    !logger.info "Going to delete non mentioned alerts %a"
      (Set.print N.src_path_print) to_delete ;
    Set.iter (fun src_path ->
      (* Will also delete the sources: *)
      stop_alert session src_path
    ) to_delete)

type set_alerts_v1_req =
  (string, (N.field, AlertInfoV1.t list) Hashtbl.t) Hashtbl.t
  [@@ppp PPP_JSON]

let sync_value_of_alert_v1 table column a =
  let sync_value_of_simple_filter f =
    VA.SimpleFilter.{
      lhs = f.AlertInfoV1.lhs ;
      rhs = f.rhs ;
      op = f.op } in
  let hysteresis = a.AlertInfoV1.recovery -. a.threshold in
  VA.{
    table ; column ;
    enabled = a.enabled ;
    where = List.map sync_value_of_simple_filter a.where ;
    group_by = None ;
    having = List.map sync_value_of_simple_filter a.having ;
    threshold = VA.Constant a.threshold ;
    hysteresis ;
    duration = a.duration ;
    ratio = a.ratio ;
    time_step = a.time_step ;
    tops = a.tops ;
    carry_fields = a.carry_fields ;
    carry_csts = List.map (fun (name, value) -> VA.{ name ; value }) a.carry_csts ;
    id = a.id ;
    desc_title = a.desc_title ;
    desc_firing = a.desc_firing ;
    desc_recovery = a.desc_recovery }

let set_alerts table_prefix session msg =
  let req =
    JSONRPC.json_any_parse ~what:"set-alerts-v1" set_alerts_v1_req_ppp_json msg in
  (* In case the same table/column appear several times, build a single list
   * of all preexisting alert files for the mentioned tables/columns, and a
   * list of all that are set: *)
  let old_alerts = ref Set.empty
  and new_alerts = ref Set.empty in
  let programs = get_programs session in
  Hashtbl.iter (fun table columns ->
    !logger.debug "set-alerts-v1: table %s" table ;
    let table = table_prefix ^ table in
    let fq = N.fq table in
    Hashtbl.iter (fun column alerts ->
      !logger.debug "set-alerts-v1: column %a" N.field_print column ;
      check_column programs table column ;
      old_alerts :=
        Set.union !old_alerts (get_alert_paths session fq column) ;
      List.iter (fun alert ->
        let a = sync_value_of_alert_v1 fq column alert in
        check_alert a ;
        let src_path = src_path_of_alert_info a in
        new_alerts := Set.add src_path !new_alerts ;
        save_alert session src_path a
      ) alerts
    ) columns
  ) req ;
  let to_delete = Set.diff !old_alerts !new_alerts in
  delete_alerts session to_delete

(*
 * Dispatch queries
 *)

exception BadApiVersion of string
let current_api_version = 2

let () =
  Printexc.register_printer (function
    | BadApiVersion version -> Some (
        Printf.sprintf "Bad HTTP API version: %S (must be between 1 and %d)"
          version current_api_version)
    | _ ->
        None)

let router conf prefix table_prefix =
  (* The function called for each HTTP request: *)
  let set_alerts =
    let rate_limit = rate_limiter 10 10. in
    fun table_prefix session msg ->
      if rate_limit () then set_alerts table_prefix session msg
      else raise RateLimited in
  fun session _meth path _params _headers body ->
    let prefix = list_of_prefix prefix in
    let path = chop_prefix prefix path in
    let api_version =
      match path with
      | [] | [""] ->
          1 (* Backward compatibility *)
      | [ v ] ->
          let v =
            if v.[0] = 'v' || v.[0] = 'V' then String.lchop v else v in
          (try int_of_string v
          with Failure _ -> raise (BadApiVersion v))
      | _ ->
          raise (BadPrefix path) in
    if api_version <> 1 then
      raise (BadApiVersion (string_of_int api_version)) ;
    let open JSONRPC in
    let req = parse body in
    wrap body req.id (fun () ->
      match String.lowercase_ascii req.method_ with
      | "version" -> version ()
      | "get-tables" -> get_tables table_prefix session req.params
      | "get-columns" -> get_columns table_prefix session req.params
      | "get-timeseries" -> get_timeseries conf table_prefix session req.params
      | "set-alerts" ->
          set_alerts table_prefix session req.params ;
          "null"
      | m -> bad_request (Printf.sprintf "unknown method %S" m))
