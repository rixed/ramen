(* JSONRPC API to interact with Ramen.
 *
 * Allows to query the running schema, extract time series, and create
 * new nodes.
 *)
open Batteries
open RamenLog
open RamenHelpers
open RamenHttpHelpers
open RamenSyncHelpers
open RamenLang
open RamenConsts
module C = RamenConf
module RC = C.Running
module F = C.Func
module P = C.Program
module E = RamenExpr
module O = RamenOperation
module T = RamenTypes
module N = RamenName
module Files = RamenFiles
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
        let what = Printf.sprintf "Answering request %S" body in
        print_exception ~what e ;
        err id (match e with
          | ParseError _ | BadRequest _ | RateLimited ->
              Printexc.to_string e
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

let get_tables conf msg =
  let req = JSONRPC.json_any_parse ~what:"get-tables" get_tables_req_ppp_json msg in
  let tables = Hashtbl.create 31 in
  let programs = get_programs conf in
  Hashtbl.iter (fun _prog_name prog ->
    List.iter (fun f ->
      let fqn = (F.fq_name f :> string)
      and event_time =
        O.event_time_of_operation f.F.operation in
      if event_time <> None && String.starts_with fqn req.prefix
      then Hashtbl.add tables fqn f.F.doc
    ) prog.P.funcs
  ) programs ;
  PPP.to_string get_tables_resp_ppp_json tables

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
    units : (string, float) Hashtbl.t [@ppp_default empty_units] ;
    doc : string [@ppp_default ""] ;
    factor : bool [@ppp_default false] ;
    group_key : bool [@ppp_rename "group-key"] [@ppp_default false] ;
    alerts : alert_info_v1 list }
  [@@ppp PPP_JSON]

(* A disabled alert should not be a non-running alert, but an alert that
 * does a NOP (or that does not notify, at least). This would tremendously
 * simplify the handling of serialized alert files. *)
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
    desc_title : string [@ppp_rename "desc-title"] [@ppp_default ""] ;
    desc_firing : string [@ppp_rename "desc-firing"] [@ppp_default ""] ;
    desc_recovery : string [@ppp_rename "desc-recovery"] [@ppp_default ""] }
  [@@ppp PPP_JSON]
  [@@ppp PPP_OCaml]

and simple_filter =
  { lhs : N.field ;
    rhs : string ;
    op : string [@ppp_default "="] }
  [@@ppp PPP_JSON]
  [@@ppp PPP_OCaml]

(* Alerts are saved on disc under this format: *)
and alert_source =
  | V1 of { table : N.fq ; column : N.field ; alert : alert_info_v1 }
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

(* We look for all keys which are simple fields (but not start/stop), then look
 * for an output field forwarding that field, and return its name (in theory
 * not only fields but any expression yielding the same results.) *)
let group_keys_of_operation = function
  | O.Aggregate { fields ; key ; _ } ->
      let simple_keys =
        List.filter_map (fun e ->
          match e.E.text with
          | Stateless (SL0 (Path [ E.Name n ]))
            when n <> N.field "start" &&
                 n <> N.field "stop" ->
              Some (TupleIn, n)
          | Stateless (SL2 (Get, { text = Const (VString n) ; _ },
                                 { text = Variable pref ; _ }))
            when n <> "start" && n <> "stop" ->
              Some (pref, N.field n)
          | _ -> None
        ) key in
      List.filter_map (fun sf ->
        match sf.O.expr.text with
        | Stateless (SL0 (Path [ E.Name n ]))
          when List.mem (TupleIn, n) simple_keys ->
            Some sf.alias
        | Stateless (SL2 (Get, { text = Const (VString n) ; _ },
                               { text = Variable pref ; _ }))
          when List.mem (pref, N.field n) simple_keys ->
            Some sf.alias
        | _ -> None
      ) fields
  | _ -> []

let alert_info_of_alert_source = function
  | V1 { alert ; _ } -> alert

let alerts_of_column conf func (column : N.field) =
  (* All files with extension ".alert" in this directory is supposed to be
   * an alert description: *)
  let dir =
    N.path_cat [ C.api_alerts_root conf ;
                 N.path "alerts" ; F.path func ;
                 N.path (column :> string) ] in
  if Files.is_directory dir then
    Sys.readdir (dir :> string) |>
    Array.fold_left (fun lst f ->
      if String.ends_with f ".alert" then
        match Files.ppp_of_file alert_source_ppp_ocaml
                                (N.path_cat [ dir ; N.path f ]) with
        | exception e ->
            print_exception ~what:"Error while listing alerts" e ;
            lst
        | a ->
            alert_info_of_alert_source a :: lst
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

let columns_of_func conf func =
  let h = Hashtbl.create 11 in
  let group_keys = group_keys_of_operation func.F.operation in
  O.out_type_of_operation ~with_private:false func.F.operation |>
  List.iter (fun ft ->
    let type_ = ext_type_of_typ ft.RamenTuple.typ.structure in
    if type_ <> Other then
      let factors =
        O.factors_of_operation func.operation in
      Hashtbl.add h ft.name {
        type_ = string_of_ext_type type_ ;
        units = units_of_column ft ;
        doc = ft.doc ;
        factor = List.mem ft.name factors ;
        group_key = List.mem ft.name group_keys ;
        alerts = alerts_of_column conf func ft.name }) ;
  h

let columns_of_table conf table =
  (* A function is what is called here in baby-talk a "table": *)
  let prog_name, func_name =
    N.(fq table |> fq_parse) in
  let programs = get_programs conf in
  match Hashtbl.find programs prog_name with
  | exception _ -> None
  | prog ->
      (match List.find (fun f -> f.F.name = func_name) prog.P.funcs with
      | exception Not_found -> None
      | func -> Some (columns_of_func conf func))

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
    where : simple_filter list [@ppp_default []] ;
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

let get_timeseries conf msg =
  let req = JSONRPC.json_any_parse ~what:"get-timeseries"
                                   get_timeseries_req_ppp_json msg in
  (* Accept both "num-points" (new) and "num_points" (old): *)
  let req =
    if req.num_points <> 0 || req.num_points_ = 0 then req else
    { req with num_points = req.num_points_ } in
  check_get_timeseries_req req ;
  let open RamenTimeseries in
  let num_points, since, until =
    compute_num_points req.time_step req.num_points req.since req.until in
  let times = Array.make_float num_points in
  let times_inited = ref false in
  let values = Hashtbl.create 5 in
  let programs = get_programs conf in
  Hashtbl.iter (fun table data_spec ->
    let fq = N.fq table in
    let prog_name, func_name = N.fq_parse fq in
    let filters =
      (* Even if the program has been killed we want to be able to output
       * its time series: *)
      let prog = Hashtbl.find programs prog_name in
      let func = List.find (fun f -> f.F.name = func_name) prog.funcs in
      List.fold_left (fun filters where ->
        let open RamenSerialization in
        try
          let out_type =
            O.out_type_of_operation ~with_private:false
                                    func.F.operation in
          let _, ftyp = find_field out_type where.lhs in
          let v = value_of_string ftyp.typ where.rhs in
          (where.lhs, where.op, v) :: filters
        with Failure msg -> bad_request msg
      ) [] data_spec.where in
    let column_labels, datapoints =
      let consolidation =
        let s = req.consolidation in
        if s = "" then None else Some s in
      let bucket_time =
        match String.lowercase_ascii req.bucket_time with
        | "begin" -> Begin | "middle" -> Middle | "end" -> End
        | _ -> bad_request "The only possible values for bucket_time are begin, \
                            middle and end" in
      if conf.C.sync_url = "" then
        get_local conf num_points since until filters data_spec.factors
                  ?consolidation ~bucket_time fq data_spec.select
      else
        let _zock, clt = ZMQClient.get_connection () in
        get_sync conf num_points since until filters data_spec.factors
                 ?consolidation ~bucket_time fq data_spec.select ~while_ clt in
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

type set_alerts_req =
  (N.fq, (N.field, alert_info_v1 list) Hashtbl.t) Hashtbl.t
  [@@ppp PPP_JSON]

(* Alert ids are used to uniquely identify alerts (for instance when
 * saving on disc). Alerts are identified by their defining properties.
 * This is not to be confused with the field "id" from alert_info,
 * which is the id for the user and that, as far as we are concerned, need
 * not even be unique. But we have to save this user id as well even when
 * its the only thing that changed so it's easier to make it part of this
 * hash. *)
let alert_id (column : N.field) =
  let filterspec filter =
    IO.to_string
      (List.print ~first:"" ~last:"" ~sep:"-"
        (fun oc w ->
          Printf.fprintf oc "(%s %s %s)"
            w.op (w.lhs :> string) w.rhs))
      filter in
  function
  | V1 { alert = { threshold ; where ; having ; recovery ; duration ; ratio ;
                   id ; _ } ; _ } ->
      Legacy.Printf.sprintf "V1-%s-%h-%h-%h-%h-%s-%s-%s"
         (column :> string)
         threshold recovery duration ratio id
         (filterspec where)
         (filterspec having) |> N.md5

let func_of_table programs table =
  let pn, fn = N.fq_parse table in
  let no_such_program () =
    Printf.sprintf "Program %s does not exist"
      (pn :> string) |>
    bad_request in
  match Hashtbl.find programs pn with
  | exception Not_found -> no_such_program ()
  | rce, get_rc ->
      (match get_rc () with
      (* Best effort if the program is no longer running: *)
      | exception _ when rce.RC.status <> MustRun ->
          no_such_program ()
      | prog ->
        (try List.find (fun f -> f.F.name = fn) prog.P.funcs
        with Not_found ->
          Printf.sprintf "No function %s in program %s"
            (fn :> string)
            (pn :> string) |>
          bad_request))

let field_typ_of_column programs table column =
  let open RamenTuple in
  let func = func_of_table programs table in
  try
    O.out_type_of_operation ~with_private:false
                            func.F.operation |>
    List.find (fun t -> t.name = column)
  with Not_found ->
    Printf.sprintf2 "No column %a in table %a"
      N.field_print column
      N.fq_print table |>
    bad_request

let generate_alert programs (src_file : N.path)
                   (V1 { table ; column ; alert = a }) =
  File.with_file_out ~mode:[`create; `text ; `trunc]
                     (src_file :> string) (fun oc ->
    Printf.fprintf oc
      "-- Alert automatically generated by ramen %s at %s\n\n"
      RamenVersions.release_tag (string_of_time (Unix.time ())) ;
    (* TODO: if this sticks make the URL a parameter: *)
    let with_desc_link s =
      let t =
        Printf.sprintf "&id=%s" (CodecUrl.encode a.id) in
      Printf.sprintf
        "%S || COALESCE(\"See \" || env.VIEW_ALERT_URL || %S \
                                 || \" for details\", \
                        \"\")" s t in
    let desc_firing =
      if a.desc_firing <> "" then String.quote a.desc_firing else
        Printf.sprintf "%s went %s the configured threshold %f.\n"
          (column :> string)
          (if a.threshold >= a.recovery then "above" else "below")
          a.threshold |>
        with_desc_link
    and desc_recovery =
      if a.desc_recovery <> "" then String.quote a.desc_recovery else
        Printf.sprintf "The value of %s recovered.\n"
          (column :> string) |>
        with_desc_link
    and print_filter oc filter =
      if filter = [] then String.print oc "true" else
      List.print ~first:"" ~sep:" AND " ~last:""
        (fun oc w ->
          let ft = field_typ_of_column programs table w.lhs in
          let v = RamenSerialization.value_of_string ft.RamenTuple.typ w.rhs in
          Printf.fprintf oc "(%s %s %a)"
            (ramen_quote (w.lhs :> string)) w.op
            T.print v)
        oc filter
    and default_aggr_of_field fn =
      let ft = field_typ_of_column programs table fn in
      ft.RamenTuple.aggr |? "avg"
    in
    (* Do we need to reaggregate?
     * We need to if the where filter leaves us with several groups. *)
    let func = func_of_table programs table in
    let group_keys = group_keys_of_operation func.F.operation in
    let need_reaggr =
      List.for_all (fun k ->
         List.exists (fun w -> w.op = "=" && w.lhs = k) a.where
       ) group_keys |> not in
    Printf.fprintf oc "-- Alerting program\n\n" ;
    Printf.fprintf oc "DEFINE filtered AS\n" ;
    Printf.fprintf oc "  FROM %s\n" (ramen_quote (table :> string)) ;
    Printf.fprintf oc "  WHERE %a\n" print_filter a.where ;
    Printf.fprintf oc "  SELECT\n" ;
    if need_reaggr then (
      (* First we need to resample the TS with the desired time step,
       * aggregating all values for the desired column: *)
      Printf.fprintf oc "    floor(start / %f) * %f AS start,\n"
        a.time_step a.time_step ;
      Printf.fprintf oc "    start + %f AS stop,\n"
        a.time_step ;
      (* Also select all the fields used in the HAVING filter: *)
      List.iter (fun h ->
        Printf.fprintf oc "    %s %s AS %s, -- for HAVING\n"
          (default_aggr_of_field h.lhs)
          (ramen_quote (h.lhs :> string))
          (ramen_quote (h.lhs :> string))
      ) a.having ;
      Printf.fprintf oc "    min %s AS min_value,\n"
        (ramen_quote (column :> string)) ;
      Printf.fprintf oc "    max %s AS max_value,\n"
        (ramen_quote (column :> string)) ;
      Printf.fprintf oc "    %s %s AS value\n"
        (default_aggr_of_field column)
        (ramen_quote (column :> string)) ;
      Printf.fprintf oc "  GROUP BY u32(floor(start / %f))\n" a.time_step ;
      Printf.fprintf oc "  COMMIT AFTER in.start > out.start + 1.5 * %f;\n\n"
        a.time_step ;
    ) else (
      !logger.debug "All group keys are set to a unique value!" ;
      Printf.fprintf oc "    start, stop,\n" ;
      (* Also select all the fields used in the HAVING filter: *)
      List.iter (fun h ->
        Printf.fprintf oc "    %s, -- for HAVING\n"
          (ramen_quote (h.lhs :> string))
      ) a.having ;
      Printf.fprintf oc "    %s AS value;\n\n"
        (ramen_quote (column :> string)) ;
    ) ;
    (* Then we want for each point to find out if it's within the acceptable
     * boundaries or not, using hysteresis: *)
    Printf.fprintf oc "DEFINE ok AS\n" ;
    Printf.fprintf oc "  FROM filtered\n" ;
    Printf.fprintf oc "  SELECT\n" ;
    Printf.fprintf oc "    start, stop,\n";
    if need_reaggr then
      Printf.fprintf oc "    min_value, max_value,\n" ;
    Printf.fprintf oc "    IF (%a) THEN value AS filtered_value,\n"
       print_filter a.having ;
    Printf.fprintf oc "    COALESCE(\n" ;
    Printf.fprintf oc "      HYSTERESIS (filtered_value, %f, %f),\n"
      a.recovery a.threshold ;
    (* Be healthy when filtered_value is NULL: *)
    Printf.fprintf oc "    true) AS ok;\n\n" ;
    (* Then we fire an alert if too many values are unhealthy: *)
    if a.enabled then (
      Printf.fprintf oc "DEFINE alert AS\n" ;
      Printf.fprintf oc "  FROM ok\n" ;
      Printf.fprintf oc "  SELECT\n" ;
      Printf.fprintf oc "    start, stop,\n";
      if need_reaggr then
        Printf.fprintf oc "    max_value, min_value,\n" ;
      Printf.fprintf oc "    COALESCE(avg(last %d float(not ok)) >= %f, false)\n"
        (max 1 (round_to_int (a.duration /. a.time_step))) a.ratio ;
      Printf.fprintf oc "      AS firing,\n" ;
      Printf.fprintf oc "    1 AS certainty,\n" ;
      (* This cast to string can handle the NULL case: *)
      if need_reaggr then (
        Printf.fprintf oc "    string(min_value) || \",\" || string(max_value)\n" ;
        Printf.fprintf oc "      AS values,\n") ;
      Printf.fprintf oc "    %f AS thresholds,\n" a.threshold ;
      Printf.fprintf oc "    (IF firing THEN %s ELSE %s) AS desc\n"
        desc_firing desc_recovery ;
      Printf.fprintf oc "  NOTIFY %S || \" (\" || %S || \") triggered\" || %S,\n"
        (column :> string) (table :> string)
        (if a.desc_title = "" then "" else " on "^ a.desc_title) ;
      (* TODO: a way to add zone, service, etc, if present in the
       * parent table *)
      Printf.fprintf oc "  KEEP\n" ;
      Printf.fprintf oc "  AFTER CHANGED firing |? false;\n"))

(* Register a rule to turn an alert into a ramen source file: *)
let () =
  RamenMake.register "alert" "ramen"
    RamenMake.target_is_older
    (fun conf _get_parent _prog_name src_file target_file ->
      let a = Files.ppp_of_file alert_source_ppp_ocaml src_file in
      RC.with_rlock conf (fun programs ->
        generate_alert programs target_file a))

let stop_alert conf (program_name : N.program) =
  let glob = Globs.escape (program_name :> string) in
  (* As we are also deleting the binary better purge the conf as per
   * https://github.com/rixed/ramen/issues/548 *)
  let num_kills = RamenRun.kill ~purge:true conf [ glob ] in
  if num_kills < 0 || num_kills > 1 then
    !logger.error "When attempting to kill alert %s, got num_kill = %d"
      (program_name :> string) num_kills

let save_alert_local conf program_name alert_info =
  let basename = N.path_cat [ C.api_alerts_root conf ;
                              N.path_of_program program_name ] in
  let src_file = Files.add_ext basename "alert" in
  (* Avoid triggering a recompilation if it's unchanged.
   * To avoid comparing floats we compare the actual serialized result. *)
  let tmp_src_file = Files.add_ext src_file "tmp" in
  Files.ppp_to_file ~pretty:true tmp_src_file alert_source_ppp_ocaml
                    alert_info ;
  if Files.replace_if_different ~src:tmp_src_file ~dst:src_file then (
    !logger.info "Saved new alert into %a" N.path_print src_file ;
    try
      let bin_file = Files.add_ext basename "x" in
      let is_new_alert =
        RC.with_rlock conf (fun programs ->
          (* If this is a new alert we must compile it before we can add it to
           * the running-config. If it's already running though, we must leave
           * the recompilation to the supervisor (or we would race). *)
          if RC.is_program_running programs program_name then false else (
            let get_parent = RamenCompiler.parent_from_programs programs in
            RamenMake.build conf get_parent program_name src_file bin_file ;
            true)) in
      if is_new_alert then (
        let debug = conf.C.log_level = Debug in
        let params = Hashtbl.create 0 in
        RamenRun.run conf ~replace:true ~params ~src_file ~debug
                     bin_file (Some program_name))
    with e ->
      (* In case of error, do not leave the alert definition file so that the
       * client can retry, but keep it for later inspection: *)
      log_and_ignore_exceptions Files.move_away src_file ;
      raise e
  ) else
    (* TODO: demote back into debug once the dust has settled down: *)
    !logger.info "Alert %a preexist with same definition"
      N.path_print src_file

(* Program name is "alerts/table/column/id" *)
let save_alert_sync conf program_name (V1 { table ; column ; alert}) =
  let open RamenSync in
  let src_path = N.path_of_program program_name in
  let src_k = Key.Sources (src_path, "alert") in
  let conv_filter (f : simple_filter) =
    Value.Alert.{ lhs = f.lhs ; rhs = f.rhs ; op = f.op } in
  let a = Value.Alert.(V1 {
    table ; column ;
    enabled = alert.enabled ;
    where = List.map conv_filter alert.where ;
    having = List.map conv_filter alert.having ;
    threshold = alert.threshold ;
    recovery = alert.recovery ;
    duration = alert.duration ;
    ratio = alert.ratio ;
    time_step = alert.time_step ;
    id = alert.id ;
    desc_title = alert.desc_title ;
    desc_firing = alert.desc_firing ;
    desc_recovery = alert.desc_recovery }) in
  let _zock, clt = ZMQClient.get_connection () in
  (* Avoid touching the source and recompiling for no reason: *)
  let is_new =
    match (Client.find clt src_k).value with
    | exception Not_found -> true
    | Value.Alert a' -> a' <> a
    | v ->
        err_sync_type src_k v "an Alert" ;
        true in
  if is_new then (
    ZMQClient.send_cmd ~eager:true ~while_ clt
      (SetKey (src_k, Value.Alert a)) ;
    !logger.info "Saved new alert into %a" Key.print src_k ;
    (* Also run it *)
    if is_new then
      let debug = conf.C.log_level = Debug in
      let params = Hashtbl.create 0 in
      let on_sites = Globs.all in (* TODO *)
      RamenRun.run_sync src_path conf program_name true Default.report_period
                        on_sites debug params
  ) else
    (* TODO: demote back into debug once the dust has settled down: *)
    !logger.info "Alert %a preexist with same definition"
      Key.print src_k

let save_alert conf program_name alert_info =
  let program_name = N.program program_name in
  (if conf.C.sync_url = "" then save_alert_local else save_alert_sync)
    conf program_name alert_info

let get_alerts_local conf (table : N.fq) (column : N.field) =
  let alerts = ref Set.String.empty in
  (* All non listed alerts must be suppressed *)
  let parent =
    (* It's safer to anchor alerts in a different subtree
     * (for instance to avoid configurator "managing" them) *)
    N.program ("alerts/"^ (table :> string) ^"/"^ (column :> string)) in
  let dir =
    N.path_cat [ C.api_alerts_root conf ; N.path_of_program parent ] in
  if Files.is_directory dir then (
    Sys.readdir (dir :> string) |>
    Array.iter (fun f ->
      let f = N.path f in
      if Files.has_ext "alert" f then
        let id = Files.remove_ext f in
        let program_name =
          (parent :> string) ^"/"^ (id :> string) in
        alerts := Set.String.add program_name !alerts)) ;
  !alerts

let get_alerts_sync (table : N.fq) (column : N.field) =
  let _zock, clt = ZMQClient.get_connection () in
  let open RamenSync in
  let alerts = ref Set.String.empty in
  let parent =
    (* It's safer to anchor alerts managed by this API in a dedicated subtree,
     * although alerts source files could be given any name (what program they
     * apply to is written in the alert definition). Maybe a level of indirection
     * will be desirable in the future.
     * For now alert source path is given by the table name, column name and
     * hash of the alert definition (aka. id). *)
    N.program ("alerts/" ^ (table :> string) ^"/"^ (column :> string)) in
  let pref = N.path ((parent :> string) ^ "/") in
  Client.iter clt (fun k hv ->
    match k, hv.value with
    | Key.Sources (src_path, "alert"),
      Value.Alert _
      when String.starts_with (src_path :> string) (pref :> string) ->
        let id =
          String.lchop ~n:(String.length (pref :> string)) (src_path :> string) in
        let program_name =
           (parent :> string) ^"/"^ (id :> string) in
        alerts := Set.String.add program_name !alerts
    | _ -> ()) ;
  !alerts

(* Returns a set of program names that are currently running alerts *)
let get_alerts conf table column =
  if conf.C.sync_url = "" then
    get_alerts_local conf table column
  else
    get_alerts_sync table column

let set_alerts conf msg =
  let req = JSONRPC.json_any_parse ~what:"set-alerts" set_alerts_req_ppp_json msg in
  (* In case the same table/column appear several times, build a single list
   * of all preexisting alert files for the mentioned tables/columns, and a
   * list of all that are set: *)
  let old_alerts = ref Set.String.empty
  and new_alerts = ref Set.String.empty in
  Hashtbl.iter (fun table columns ->
    !logger.debug "set-alerts: table %a" N.fq_print table ;
    Hashtbl.iter (fun column alerts ->
      !logger.debug "set-alerts: column %a" N.field_print column ;
      old_alerts := Set.String.union !old_alerts (get_alerts conf table column) ;
      List.iter (fun alert ->
        (* Check the alert: *)
        if alert.duration < 0. then
          bad_request "Duration must be positive" ;
        if alert.ratio < 0. || alert.ratio > 1. then
          bad_request "Ratio must be between 0 and 1" ;
        if alert.time_step <= 0. then
          bad_request "Time step must be strictly greater than 0" ;
        RC.with_rlock conf (fun programs ->
          let ft = field_typ_of_column programs table column in
          if ext_type_of_typ ft.RamenTuple.typ.structure <> Numeric then
            Printf.sprintf2 "Column %a of table %a is not numeric"
              N.field_print column
              N.fq_print table |>
            bad_request ;
          (* Also check that table has event time info: *)
          let func = func_of_table programs table in
          if O.event_time_of_operation func.F.operation = None
          then
            Printf.sprintf2 "Table %a has no event time information"
              N.fq_print table |>
            bad_request) ;
        (* We receive only the latest version: *)
        let alert_source = V1 { table ; column ; alert } in
        let id = alert_id column alert_source in
        let program_name =
          "alerts/"^ (table :> string) ^"/"^ (column :> string) ^"/"^ id in
        new_alerts := Set.String.add program_name !new_alerts ;
        save_alert conf program_name alert_source
      ) alerts
    ) columns
  ) req ;
  let to_delete = Set.String.diff !old_alerts !new_alerts in
  if not (Set.String.is_empty to_delete) then (
    !logger.info "Going to delete non mentioned alerts %a"
      (Set.String.print String.print) to_delete ;
    Set.String.iter (fun program_name ->
      let program_name = N.program program_name in
      stop_alert conf program_name ;
      let fname ext =
        N.path_cat [ C.api_alerts_root conf ;
                     Files.add_ext (N.path_of_program program_name) ext ] in
      List.iter (fun ext -> Files.safe_unlink (fname ext))
        [ "alert" ; "ramen" ; "x" ]
    ) to_delete)

(*
 * Dispatch queries
 *)

let router conf prefix =
  (* The function called for each HTTP request: *)
  let set_alerts =
    let rate_limit = rate_limiter 10 10. in
    fun conf msg ->
      if rate_limit () then set_alerts conf msg
      else raise RateLimited in
  fun _meth path _params _headers body ->
    let prefix = list_of_prefix prefix in
    let path = chop_prefix prefix path in
    if path <> [] then raise BadPrefix
    else
      let open JSONRPC in
      let req = parse body in
      wrap body req.id (fun () ->
        match String.lowercase_ascii req.method_ with
        | "version" -> version ()
        | "get-tables" -> get_tables conf req.params
        | "get-columns" -> get_columns conf req.params
        | "get-timeseries" -> get_timeseries conf req.params
        | "set-alerts" -> set_alerts conf req.params ; "null"
        | m -> bad_request (Printf.sprintf "unknown method %S" m))
