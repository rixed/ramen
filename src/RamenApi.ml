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
module VSI = Value.SourceInfo
module E = RamenExpr
module O = RamenOperation
module T = RamenTypes
module N = RamenName
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
        let what = Printf.sprintf "Answering request %S" (abbrev 100 body) in
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

and alert_info_v1 =
  { enabled : bool [@ppp_default true] ;
    where : simple_filter list [@ppp_default []] ;
    having : simple_filter list [@ppp_default []] ;
    threshold : float ;
    recovery : float ;
    duration : float [@ppp_default 0.] ;
    ratio : float [@ppp_default 1.] ;
    time_step : float [@ppp_rename "time-step"] [@ppp_default 0.] ;
    (* Also build the list of top contributors for the selected column.
     * String here could be any expression. The list of top will be named
     * "top_$n" where $n is the rank in this list. *)
    tops : N.field list [@ppp_default []] ;
    (* When reaggregating it is too expensive to reaggregate all possible
     * fields but a short selection is OK: *)
    carry : N.field list [@ppp_default []] ;
    (* Supposed to be unique, used as a component in the src_path: *)
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
(* FIXME: allows table to use relative program names *)
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
              Some (In, n)
          | Stateless (SL2 (Get, { text = Const (VString n) ; _ },
                                 { text = Variable pref ; _ }))
            when n <> "start" && n <> "stop" ->
              Some (pref, N.field n)
          | _ -> None
        ) key in
      List.filter_map (fun sf ->
        match sf.O.expr.text with
        | Stateless (SL0 (Path [ E.Name n ]))
          when List.mem (In, n) simple_keys ->
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

let alert_id alert_source =
  (alert_info_of_alert_source alert_source).id

(* For custom API, where to store alerting thresholds: *)
let api_alerts_root = N.src_path "api/set_alerts"

let src_path_of_alert_info alert_source =
  N.src_path_cat [ api_alerts_root ;
                   N.src_path (alert_id alert_source) ]

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

(* Returns the alist of src_path * alert_sources for the given table and column: *)
let get_alerts session table column =
  let prefix = "sources/" in
  Client.fold session.ZMQClient.clt ~prefix (fun k hv lst ->
    match k, hv.value with
    | Key.Sources (src_path, "alert"),
      Value.Alert alert_source
      when N.starts_with src_path api_alerts_root ->
        let table', column' = Value.Alert.column_of_alert_source alert_source in
        if table = table' && column = column' then
          (src_path, alert_source) :: lst
        else
          lst
    | _ ->
        lst
  ) []

let alert_of_sync_value =
  let conv_filter (f : Value.Alert.simple_filter) =
    { lhs = f.lhs ; rhs = f.rhs ; op = f.op } in
  function
  | Value.Alert.V1 v1 ->
      V1 {
        table = v1.table ;
        column = v1.column ;
        alert =
          { enabled = v1.enabled ;
            where = List.map conv_filter v1.where ;
            having = List.map conv_filter v1.having ;
            threshold = v1.threshold ;
            recovery = v1.recovery ;
            duration = v1.duration ;
            ratio = v1.ratio ;
            time_step = v1.time_step ;
            tops = v1.tops ;
            carry = v1.carry ;
            id = v1.id ;
            desc_title = v1.desc_title ;
            desc_firing = v1.desc_firing ;
            desc_recovery = v1.desc_recovery } }

let columns_of_func session prog_name func =
  let h = Hashtbl.create 11 in
  let fq = VSI.fq_name prog_name func in
  let group_keys = group_keys_of_operation func.VSI.operation in
  O.out_type_of_operation ~with_private:false func.VSI.operation |>
  List.iter (fun ft ->
    let type_ = ext_type_of_typ ft.RamenTuple.typ.structure in
    if type_ <> Other then
      let factors =
        O.factors_of_operation func.operation in
      let alerts =
        List.map (alert_info_of_alert_source % alert_of_sync_value % snd)
                 (get_alerts session fq ft.name) in
      Hashtbl.add h ft.name {
        type_ = string_of_ext_type type_ ;
        units = units_of_column ft ;
        doc = ft.doc ;
        factor = List.mem ft.name factors ;
        group_key = List.mem ft.name group_keys ;
        alerts }
  ) ;
  h

let columns_of_table session table =
  (* A function is what is called here in baby-talk a "table": *)
  let prog_name, func_name = N.(fq table |> fq_parse) in
  let programs = get_programs session in
  match Hashtbl.find programs prog_name with
  | exception _ -> None
  | prog ->
      (match List.find (fun f -> f.VSI.name = func_name) prog.VSI.funcs with
      | exception Not_found -> None
      | func -> Some (columns_of_func session prog_name func))

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

let get_timeseries conf table_prefix session msg =
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
  let programs = get_programs session in
  Hashtbl.iter (fun table data_spec ->
    let worker = N.worker (table_prefix ^ table) in
    let _site_name, prog_name, func_name = N.worker_parse worker in
    let filters =
      (* Even if the program has been killed we want to be able to output
       * its time series: *)
      let prog = Hashtbl.find programs prog_name in
      let func = List.find (fun f -> f.VSI.name = func_name) prog.funcs in
      List.fold_left (fun filters where ->
        let open RamenSerialization in
        try
          let out_type =
            O.out_type_of_operation ~with_private:false
                                    func.VSI.operation in
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
      get conf session num_points since until filters data_spec.factors
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

type set_alerts_req =
  (string, (N.field, alert_info_v1 list) Hashtbl.t) Hashtbl.t
  [@@ppp PPP_JSON]

let func_of_table programs table =
  let pn, fn = N.fq_parse table in
  match Hashtbl.find programs pn with
  | exception Not_found ->
      Printf.sprintf "Program %s does not exist"
        (pn :> string) |>
      bad_request
  | prog ->
      (try List.find (fun f -> f.VSI.name = fn) prog.VSI.funcs
      with Not_found ->
        Printf.sprintf "No function %s in program %s"
          (fn :> string)
          (pn :> string) |>
        bad_request)

(* FIXME: do not use [programs] but instead RamenSync.function_of_fq *)
let field_typ_of_column programs table column =
  let open RamenTuple in
  let func = func_of_table programs table in
  try
    O.out_type_of_operation ~with_private:false
                            func.VSI.operation |>
    List.find (fun t -> t.name = column)
  with Not_found ->
    Printf.sprintf2 "No column %a in table %a"
      N.field_print column
      N.fq_print table |>
    bad_request

(* This function turns an alert into a ramen program. It is called by the
 * compiler (via RamenMake and not by the API HTTP server. As a consequence,
 * it must fail with Failure rather than BadRequest. *)
let generate_alert get_program (src_file : N.path)
                   (V1 { table ; column ; alert = a }) =
  let func_of_program fq =
    let pn, fn = N.fq_parse fq in
    match get_program pn with
    | exception Not_found ->
        Printf.sprintf "Program %s does not exist"
          (pn :> string) |>
        failwith
    | prog ->
        (try List.find (fun f -> f.VSI.name = fn) prog.VSI.funcs
        with Not_found ->
          Printf.sprintf "No function %s in program %s"
            (fn :> string)
            (pn :> string) |>
          failwith) in
  let func = func_of_program table in
  let field_type_of_column column =
    let open RamenTuple in
    try
      O.out_type_of_operation ~with_private:false
                              func.VSI.operation |>
      List.find (fun t -> t.name = column)
    with Not_found ->
      Printf.sprintf2 "No column %a in table %a"
        N.field_print column
        N.fq_print table |>
      failwith
  in
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
      List.print ~first:"" ~sep:" AND " ~last:"" (fun oc w ->
        (* Get the proper right-type according to left-type and operator: *)
        let lft = (field_type_of_column w.lhs).RamenTuple.typ in
        let rft = if w.op = "in" then T.(make (TList lft)) else lft in
        let v = RamenSerialization.value_of_string rft w.rhs in
        let s =
          Printf.sprintf2 "%s %s %a"
            (ramen_quote (w.lhs :> string))
            w.op
            T.print v in
        if lft.nullable then
          Printf.fprintf oc "COALESCE(%s, false)" s
        else
          String.print oc s
      ) oc filter
    and field_expr fn =
      match func.VSI.operation with
      | O.Aggregate { fields ; _ } ->
          (* We know it's there because of field_type_of_column: *)
          let sf = List.find (fun sf -> sf.O.alias = fn) fields in
          sf.expr
      | _ ->
          (* Should not happen that we have "same" then *)
          assert false in
    let default_aggr_of_field fn =
      let ft = field_type_of_column fn in
      let e = field_expr fn in
      let default =
        match e.E.text with
        (* If the field we re-aggregate is a min, then we can safely aggregate
         * it again with the min function. Same for max, and sum. *)
        | E.(Stateful (_, _, SF1 (AggrMin, _))) -> "min"
        | E.(Stateful (_, _, SF1 (AggrMax, _))) -> "max"
        | E.(Stateful (_, _, SF1 (AggrSum, _))) -> "sum"
        (* If the field was an average, then our last hope is to reuse the
         * same expression, hoping that the components will be available: *)
        | E.(Stateful (_, _, SF1 (AggrAvg, _))) -> "same"
        | _ ->
            (* Beware that "carry" fields need not be numeric: *)
            if T.is_numeric ft.RamenTuple.typ.structure then "sum"
                                                        else "first" in
      ft.RamenTuple.aggr |? default in
    let reaggr_field fn =
      let aggr = default_aggr_of_field fn in
      if aggr = "same" then (
        (* Alias field: supposedly, we can recompute it from it's expression.
         * This is only possible if all fields this one depends on are
         * available already. *)
        let e = field_expr fn in
        IO.to_string (E.print false) e
      ) else (
        aggr ^" "^ (ramen_quote (fn :> string))
      )
    in
    (* Do we need to reaggregate?
     * We need to if the where filter leaves us with several groups.
     * It is clear that a filter selecting only one group, corresponding
     * to a "group by thing, thung" followed by "where thing=42 and thung=43",
     * just plainly cancels the group-by.
     * Also, it is clear that if the filter does not cancel the group-by
     * entirely (for instance "group by thing, thung, thong"), then values
     * have to be re-aggregated grouped by time only.
     *
     * It is less clear what to do when the filter uses another operator
     * than "=". What's the intent of "thing>42"? Do we want to aggregate all
     * values for those things into one, and alert on the aggregate? Or do we
     * intend to alert on each metric grouped by thing, for those greater than
     * 42?
     *
     * In the following, we assume the later, so that the same alert can be
     * defined for many groups and run independently in a single worker.
     * So in that case, even if the group-by is fully cancelled we still need
     * to group-by the selected fields (but not necessarily by time), so that
     * we have one alerting context (hysteresis) per group. *)
    let group_keys = group_keys_of_operation func.VSI.operation in
    (* Reaggregation is needed if we set time_step: *)
    let need_reaggr = a.time_step > 0. in
    let need_reaggr, group_by =
      List.fold_left (fun (need_reaggr, group_by) group_key ->
        (* Reaggregation is also needed as soon as the group_keys have a field
         * which is not paired with a WHERE condition (remember
         * group_keys_of_operation leaves group by time aside): *)
        need_reaggr || not (
          List.exists (fun w ->
            w.op = "=" && w.lhs = group_key
          ) a.where
        ),
        (* group by any keys which is used in the where but not with an
         * equality. Used both when reaggregating and to have one alert
         * per group even when not reagregating *)
        List.fold_left (fun group_by w ->
          if w.op <> "=" && w.lhs = group_key then
            ramen_quote (w.lhs :> string) :: group_by
          else
            group_by
        ) group_by a.where
      ) (need_reaggr, []) group_keys in
    Printf.fprintf oc "-- Alerting program\n\n" ;
    (* TODO: get rid of 'filtered' if a.where is empty and not need_reaggr *)
    (* The first function, filtered, as the name suggest, performs the WHERE
     * filter; but it also reaggregate (if needed).
     * To reaggregate the parent we use either the specified default
     * aggregation functions or, if "same", reuse the expression as is, but
     * take care to also define (and aggregate) the fields that are depended
     * upon. So we start by building the set of all the fields that we need,
     * then we will need to output them in the same order as in the parent: *)
    let filtered_fields = ref Set.String.empty in
    let iter_in_order f =
      O.out_type_of_operation ~with_private:false func.operation |>
      List.iter (fun ft ->
        if Set.String.mem (ft.RamenTuple.name :> string) !filtered_fields then
          f ft.name) in
    let add_field fn =
      filtered_fields :=
        Set.String.add (fn : N.field :> string) !filtered_fields in
    add_field column ;
    List.iter (fun f -> add_field (N.field f)) group_by ;
    List.iter (fun f -> add_field f.lhs) a.having ;
    List.iter add_field a.tops ;
    List.iter add_field a.carry ;
    Printf.fprintf oc "DEFINE filtered AS\n" ;
    Printf.fprintf oc "  FROM %s\n" (ramen_quote (table :> string)) ;
    Printf.fprintf oc "  WHERE %a\n" print_filter a.where ;
    Printf.fprintf oc "  SELECT\n" ;
    if need_reaggr then (
      (* Return the fields from out in the given expression: *)
      let depended fn =
        let aggr = default_aggr_of_field fn in
        if aggr <> "same" then Set.String.empty else
        let e = field_expr fn in
        E.fold (fun _stack deps -> function
          | E.{ text = Stateless (SL2 (Get, { text = Const (VString fn) ; _ },
                                            { text = Variable Out ; })) } ->
              Set.String.add (fn :> string) deps
          | _ ->
              deps) [] Set.String.empty e
      in
      (* Recursively also add the dependencies for aggr using "same": *)
      let deps =
        Set.String.fold (fun fn deps ->
          Set.String.union deps (depended (N.field fn))
        ) !filtered_fields Set.String.empty in
      filtered_fields := Set.String.union deps !filtered_fields ;
      !logger.debug "List of fields required to reaggregate: %a"
        (Set.String.print String.print) !filtered_fields ;
      (* First we need to re-sample the TS with the desired time step,
       * aggregating all values for the desired column: *)
      Printf.fprintf oc "    TRUNCATE(#start, %a) AS start,\n"
        print_nice_float a.time_step ;
      Printf.fprintf oc "    start + %a AS stop,\n"
        print_nice_float a.time_step ;
      (* Then all fields that have been selected: *)
      let aggr_field field =
        Printf.fprintf oc "    %s AS %s,\n"
          (reaggr_field field)
          (ramen_quote (field :> string)) in
      iter_in_order aggr_field ;
      Printf.fprintf oc "    %s AS value,\n"
        (ramen_quote (column :> string)) ;
      Printf.fprintf oc "    min value,\n" ;
      Printf.fprintf oc "    max value\n" ;
      let group_by =
        (Printf.sprintf2 "#start // %a" print_nice_float a.time_step) ::
        group_by in
      Printf.fprintf oc "  GROUP BY %a\n"
        (List.print ~first:"" ~last:"" ~sep:", " String.print) group_by ;
      (* This wait for late points for half the time_step. Maybe too
       * conservative? *)
      Printf.fprintf oc "  COMMIT AFTER in.start > out.start + 1.5 * %a;\n\n"
        print_nice_float a.time_step ;
    ) else (
      !logger.debug "No need to reaggregate! List of required fields: %a"
        (Set.String.print String.print) !filtered_fields ;
      Printf.fprintf oc "    #start AS start, #stop AS stop," ;
      iter_in_order (fun fn ->
        Printf.fprintf oc "    %s AS %s,\n"
          (ramen_quote (fn :> string))
          (ramen_quote (fn :> string))) ;
      Printf.fprintf oc "    %s AS value;\n\n"
        (ramen_quote (column :> string))
    ) ;
    (* Then we want for each point to find out if it's within the acceptable
     * boundaries or not, using hysteresis: *)
    Printf.fprintf oc "DEFINE ok AS\n" ;
    Printf.fprintf oc "  FROM filtered\n" ;
    Printf.fprintf oc "  SELECT *,\n" ;
    (* For each top expression, compute the list of top contributing values *)
    List.iteri (fun i fn ->
      Printf.fprintf oc "    LIST TOP 10 %s LOCALLY BY value\n"
        (ramen_quote (fn : N.field :> string)) ;
      if a.duration > 0. then
        Printf.fprintf oc "      FOR THE LAST %a SECONDS"
          print_nice_float a.duration ;
      Printf.fprintf oc "      ABOVE 2 SIGMAS AS top_%d,\n" i
    ) a.tops ;
    if need_reaggr then
      Printf.fprintf oc "    min_value, max_value,\n" ;
    Printf.fprintf oc "    IF (%a) THEN value AS filtered_value,\n"
       print_filter a.having ;
    Printf.fprintf oc "    COALESCE(\n" ;
    Printf.fprintf oc "      HYSTERESIS (filtered_value, %a, %a),\n"
      print_nice_float a.recovery
      print_nice_float a.threshold ;
    (* Be healthy when filtered_value is NULL: *)
    Printf.fprintf oc "    true) AS ok\n" ;
    if group_by <> [] then (
      !logger.debug "Combined alert for group keys %a"
        (List.print String.print) group_by ;
      Printf.fprintf oc "  GROUP BY %a\n"
        (List.print ~first:"" ~last:"" ~sep:", " String.print) group_by ;
    ) ;
    (* The HYSTERESIS above use the local context and so regardless of
     * whether we group-by or not we want the keep the group intact from
     * tuple to tuple: *)
    Printf.fprintf oc "  KEEP;\n\n" ;
    (* Then we fire an alert if too many values are unhealthy: *)
    if a.enabled then (
      Printf.fprintf oc "DEFINE alert AS\n" ;
      Printf.fprintf oc "  FROM ok\n" ;
      Printf.fprintf oc "  SELECT *,\n" ;
      if need_reaggr then
        Printf.fprintf oc "    max_value, min_value,\n" ;
      if a.duration > 0. then
        Printf.fprintf oc
          "    COALESCE(AVG(PAST %a SECONDS OF float(not ok)) >= %a, false)\n"
          print_nice_float a.duration print_nice_float a.ratio
      else (* Look only at the last point: *)
        Printf.fprintf oc "    not ok\n" ;
      Printf.fprintf oc "      AS firing,\n" ;
      Printf.fprintf oc "    %S AS id,\n" a.id ;
      Printf.fprintf oc "    1 AS certainty,\n" ;
      (* This cast to string can handle the NULL case: *)
      if need_reaggr then (
        Printf.fprintf oc "    string(min_value) || \",\" || string(max_value)\n" ;
        Printf.fprintf oc "      AS values,\n") ;
      Printf.fprintf oc "    %S AS column,\n" (column :> string) ;
      Printf.fprintf oc "    %a AS thresholds,\n" print_nice_float a.threshold ;
      Printf.fprintf oc "    %a AS duration,\n" print_nice_float a.duration ;
      Printf.fprintf oc "    (IF firing THEN %s\n" desc_firing ;
      Printf.fprintf oc "     ELSE %s) AS desc\n" desc_recovery ;
      if group_by <> [] then (
        Printf.fprintf oc "  GROUP BY %a\n"
          (List.print ~first:"" ~last:"" ~sep:", " String.print) group_by ;
      ) ;
      Printf.fprintf oc "  AFTER CHANGED firing |? firing\n" ;
      Printf.fprintf oc "    NOTIFY %S || \" (\" || %S || \") triggered\" || %S\n"
        (column :> string) (table :> string)
        (if a.desc_title = "" then "" else " on "^ a.desc_title) ;
      (* TODO: a way to add zone, service, etc, if present in the
       * parent table *)
      Printf.fprintf oc "    AND KEEP;\n"))

let stop_alert conf src_path =
  (* Alerting programs have no suffixes: *)
  let program_name = (src_path : N.src_path :> string) in
  let glob = Globs.escape program_name in
  (* As we are also deleting the binary better purge the conf as per
   * https://github.com/rixed/ramen/issues/548 *)
  let num_kills = RamenRun.kill ~while_ ~purge:true conf [ glob ] in
  if num_kills < 0 || num_kills > 1 then
    !logger.error "When attempting to kill alert %s, got num_kill = %d"
      program_name num_kills

(* Delete the source of an alert program that's been stopped already: *)
let delete_alert session src_path =
  let delete_ext ext =
    let src_k = Key.Sources (src_path, ext) in
    !logger.info "Deleting alert %a" Key.print src_k ;
    ZMQClient.send_cmd ~eager:true ~while_ session (DelKey src_k) ;
  in
  List.iter delete_ext [ "alert" ; "ramen" ; "info" ]

(* Program name is "alerts/table/column/id" *)
let sync_value_of_alert (V1 { table ; column ; alert }) =
  let conv_filter (f : simple_filter) =
    Value.Alert.{ lhs = f.lhs ; rhs = f.rhs ; op = f.op } in
  Value.Alert.(V1 {
    table ; column ;
    enabled = alert.enabled ;
    where = List.map conv_filter alert.where ;
    having = List.map conv_filter alert.having ;
    threshold = alert.threshold ;
    recovery = alert.recovery ;
    duration = alert.duration ;
    ratio = alert.ratio ;
    time_step = alert.time_step ;
    tops = alert.tops ;
    carry = alert.carry ;
    id = alert.id ;
    desc_title = alert.desc_title ;
    desc_firing = alert.desc_firing ;
    desc_recovery = alert.desc_recovery })

let save_alert conf session src_path alert =
  let src_k = Key.Sources (src_path, "alert") in
  let a = sync_value_of_alert alert in
  (* Avoid touching the source and recompiling for no reason: *)
  let is_new =
    match (Client.find session.ZMQClient.clt src_k).value with
    | exception Not_found -> true
    | Value.Alert a' -> a' <> a
    | v ->
        err_sync_type src_k v "an Alert" ;
        true in
  if is_new then (
    ZMQClient.send_cmd ~eager:true ~while_ session
      (SetKey (src_k, Value.Alert a)) ;
    !logger.info "Saved new alert into %a" Key.print src_k ;
  ) else (
    (* TODO: do nothing once the dust has settled down: *)
    !logger.info "Alert %a preexist with same definition"
      Key.print src_k
  ) ;
  (* Also make sure it is running *)
  !logger.info "Making sure the alert is running..." ;
  let debug = conf.C.log_level = Debug
  and params = Hashtbl.create 0
  and on_sites = Globs.all (* TODO *) in
  (* Alerts use no suffix: *)
  let program_name = N.program (src_path :> string) in
  RamenRun.do_run ~while_ session program_name
                  Default.report_period on_sites debug params

(* Returns the set of the src_paths of the alerts that are currently defined
 * for this table and column: *)
let get_alert_paths session table column =
  get_alerts session table column |>
  List.fold_left (fun s (src_path, _) ->
    Set.add src_path s
  ) Set.empty

let set_alerts conf table_prefix session msg =
  let req = JSONRPC.json_any_parse ~what:"set-alerts" set_alerts_req_ppp_json msg in
  (* In case the same table/column appear several times, build a single list
   * of all preexisting alert files for the mentioned tables/columns, and a
   * list of all that are set: *)
  let old_alerts = ref Set.empty
  and new_alerts = ref Set.empty in
  Hashtbl.iter (fun table columns ->
    !logger.debug "set-alerts: table %s" table ;
    let fq = N.fq (table_prefix ^ table) in
    Hashtbl.iter (fun column alerts ->
      !logger.debug "set-alerts: column %a" N.field_print column ;
      old_alerts :=
        Set.union !old_alerts (get_alert_paths session fq column) ;
      List.iter (fun alert ->
        (* Check the alert: *)
        if alert.duration < 0. then
          bad_request "Duration must be positive" ;
        if alert.ratio < 0. || alert.ratio > 1. then
          bad_request "Ratio must be between 0 and 1" ;
        if alert.time_step < 0. then
          bad_request "Time step must be greater than 0" ;
        let programs = RamenSyncHelpers.get_programs session in
        let ft = field_typ_of_column programs fq column in
        if ext_type_of_typ ft.RamenTuple.typ.structure <> Numeric then
          Printf.sprintf2 "Column %a of table %s is not numeric"
            N.field_print column
            table |>
          bad_request ;
        (* Also check that table has event time info: *)
        let func = func_of_table programs fq in
        if O.event_time_of_operation func.VSI.operation = None
        then
          Printf.sprintf2 "Table %s has no event time information"
            table |>
          bad_request ;
        (* We receive only the latest version: *)
        let alert_source = V1 { table = fq ; column ; alert } in
        let src_path = src_path_of_alert_info alert_source in
        new_alerts := Set.add src_path !new_alerts ;
        save_alert conf session src_path alert_source
      ) alerts
    ) columns
  ) req ;
  let to_delete = Set.diff !old_alerts !new_alerts in
  if not (Set.is_empty to_delete) then (
    !logger.info "Going to delete non mentioned alerts %a"
      (Set.print N.src_path_print) to_delete ;
    Set.iter (fun src_path ->
      stop_alert conf src_path ;
      delete_alert session src_path
    ) to_delete)

(*
 * Dispatch queries
 *)

let router conf prefix table_prefix =
  (* The function called for each HTTP request: *)
  let set_alerts =
    let rate_limit = rate_limiter 10 10. in
    fun conf session msg ->
      if rate_limit () then set_alerts conf session msg
      else raise RateLimited in
  fun session _meth path _params _headers body ->
    let prefix = list_of_prefix prefix in
    let path = chop_prefix prefix path in
    if path <> [""] && path <> [] then raise BadPrefix
    else
      let open JSONRPC in
      let req = parse body in
      wrap body req.id (fun () ->
        match String.lowercase_ascii req.method_ with
        | "version" -> version ()
        | "get-tables" -> get_tables table_prefix session req.params
        | "get-columns" -> get_columns table_prefix session req.params
        | "get-timeseries" -> get_timeseries conf table_prefix session req.params
        | "set-alerts" -> set_alerts conf table_prefix session req.params ; "null"
        | m -> bad_request (Printf.sprintf "unknown method %S" m))
