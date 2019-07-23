(* Graphite impersonator.
 *
 * Implement (the most important bits of) Graphite API for Grafana.
 * This way, there is no need for a Grafana plugin. Just run
 * `ramen graphite --port=XYZ` and create a graphite datasource in
 * Grafana, and then basic charts of time series should just work.
 *
 * What is supported:
 * - Normal charts;
 * - Autocompletion of time series in chart editor;
 * - The "*" metric;
 * - All dates used by Grafana.
 *
 * Functions are currently not supported and should be disabled.
 * There seems to be a call to get the list of supported functions, maybe
 * investigate that?
 *)
open Batteries
open BatOption.Infix
open RamenLog
open RamenHelpers
open RamenHttpHelpers
open RamenSyncHelpers
module C = RamenConf
module RC = C.Running
module F = C.Func
module P = C.Program
module O = RamenOperation
module T = RamenTypes
module N = RamenName
module ZMQClient = RamenSyncZMQClient

let while_ () = !RamenProcesses.quit = None

type comp_section =
  | ProgPath
  | OpName of F.t
  | FactorAll of N.field
  | FactorValue of (N.field * T.value)
(*  | FactorIgnore TODO *)
  | DataField of N.field

type comp =
  { value : string ; (* The value as a raw string (unquoted) *)
    section : comp_section }

(* When building target names we may use scalar values in place of factor
 * fields. When those values are strings they are always quoted. But we'd
 * like to unquote them when possible. Also, some other values may be left
 * unquoted by RamenTypes.print but we do need to quote them if they
 * contain a ".": *)
let fix_quote s =
  let try_quote s =
    if s.[0] = '"' then s else "\""^ s ^"\""
  and try_unquote s =
    let l = String.length s in
    if s.[0] = '"' && s.[l - 1] = '"' then String.sub s 1 (l - 2) else s
  in
  if s = "" then "\"\"" else
  if String.contains s '.' then try_quote s
  else try_unquote s
(*$inject open Batteries *)
(*$= fix_quote & ~printer:identity
  "\"\"" (fix_quote "")
  "glop" (fix_quote "glop")
  "glop" (fix_quote "\"glop\"")
  "\"pas.glop\"" (fix_quote "pas.glop")
  "\"pas.glop\"" (fix_quote "\"pas.glop\"")
 *)

(* We might want to expand the last component of a metrics_find query: *)
type factor_expansion = No | OnlyLast | All

(* Need the map of prog_name -> P.t *)
let inverted_tree_of_programs
    conf ?since ?until ~only_with_event_time ~only_num_fields
    ~factor_expansion filters programs =
  (* [filters] is a list of filters, each being an array of globs. *)
  let max_filter_len =
    List.fold_left (fun ma flt ->
      max ma (Array.length flt)
    ) 0 filters in
  let end_of_filters flt_idx =
    flt_idx >= max_filter_len
  and filters_match flt_idx v =
    List.exists (fun flt ->
      flt_idx < Array.length flt && Globs.matches flt.(flt_idx) v
    ) filters
  and filter_is_last flt_idx =
    flt_idx = max_filter_len - 1 in
  let should_expand_factor flt_idx =
    factor_expansion = All ||
    factor_expansion = OnlyLast && filter_is_last flt_idx
  in
  let programs = Hashtbl.enum programs |> Array.of_enum in
  let cmp (n1, _) (n2, _) = N.compare n1 n2 in
  Array.fast_sort cmp programs ;
  (* For each of these programs, we will build an enumeration of lists from
   * leaves to root (empty if the filter is not matched): *)
  Array.enum programs /@
  (fun ((program_name : N.program), prog) ->
    let program_name = (program_name :> string) in
    (* Enumerate all data fields (numeric that is not a factor) as
     * leaves: *)
    let rec loop_data_field prev flt_idx operation =
      if end_of_filters flt_idx then Enum.singleton prev else
      let factors =
        O.factors_of_operation operation in
      let out_typ =
        O.out_type_of_operation ~with_private:false operation in
      (* TODO: sort alphabetically (only the remaining fields!) *)
      List.enum out_typ //@ (fun ft ->
        if (not only_num_fields || T.is_a_num ft.RamenTuple.typ.structure) &&
           not (List.mem ft.RamenTuple.name factors)
        then
          let value = (ft.RamenTuple.name :> string) in
          (* If we asked for some extra components then that's also a fail: *)
          if end_of_filters (flt_idx + 1) &&
             filters_match flt_idx value then
            Some ({ value ; section = DataField ft.name} :: prev)
          else None
        else None)
    (* For each factor in turn, start a new enum of leaves *)
    and loop_factor prev flt_idx func = function
      | [] -> loop_data_field prev flt_idx func.F.operation
      | factor :: factors ->
          if end_of_filters flt_idx then Enum.singleton prev else
          let comps =
            if should_expand_factor flt_idx then
              let pv = RamenTimeseries.possible_values conf ?since ?until
                                                       func factor in
              (* If there is no possible value, output an empty string that
               * will allow us to keep iterating and have a chance to see
               * the fields.  Grafana will skip that value and offer only
               * the "*", which is all good. *)
              if Set.is_empty pv then
                let value = "" in
                if filters_match flt_idx value then
                  Set.singleton
                    { value ; section = FactorAll factor }
                else Set.empty
              else
                Set.filter_map (fun v ->
                  let value = T.to_string ~quoting:false v in
                  if filters_match flt_idx value then
                    Some { value ; section = FactorValue (factor, v) }
                  else None
                ) pv
            else
              Set.singleton
                { value = "" ; section = FactorAll factor }
          in
          (* TODO: sort alphabetically *)
          Set.enum comps /@ (fun comp ->
            loop_factor (comp :: prev) (flt_idx + 1) func factors) |>
          Enum.flatten
    (* Loop over all functions of the program: *)
    and loop_func prev flt_idx =
      if end_of_filters flt_idx then
        Enum.singleton prev
      else
        (* TODO: sort alphabetically *)
        let funcs =
          if only_with_event_time then
            List.enum prog.P.funcs //
            (fun func ->
              O.event_time_of_operation func.F.operation <>
                None)
          else
            List.enum prog.P.funcs in
        funcs /@ (fun func ->
          let value = (func.F.name :> string) in
          if filters_match flt_idx value then
            let comp = { value ; section = OpName func } in
            let factors =
              O.factors_of_operation func.F.operation in
            loop_factor (comp :: prev) (flt_idx + 1) func factors else
          Enum.empty ()) |> Enum.flatten
    (* Loop over all path components of the program name: *)
    and loop_prog prev chr_idx flt_idx =
      if chr_idx >= String.length program_name then
        loop_func prev flt_idx else
      if end_of_filters flt_idx then Enum.singleton prev else
      let end_idx =
        try String.index_from program_name chr_idx '/'
        with Not_found -> String.length program_name in
      let value = String.sub program_name chr_idx (end_idx - chr_idx) in
      (* In theory "//" in program name is forbidden but: *)
      if value = "" then loop_prog prev (end_idx + 1) flt_idx else
      if filters_match flt_idx value then
        let comp = { value ; section = ProgPath } in
        loop_prog (comp :: prev) (end_idx + 1) (flt_idx + 1) else
      Enum.empty () in
    loop_prog [] 0 0) |>
  Enum.flatten

let rec find_quote_from s i =
  if i >= String.length s then raise Not_found ;
  if s.[i] = '\\' then find_quote_from s (i + 2)
  else if s.[i] = '"' then i
  else find_quote_from s (i + 1)
(*$= find_quote_from & ~printer:string_of_int
  4 (find_quote_from "glop\"gl\\\"op\"" 0)
  4 (find_quote_from "glop\"gl\\\"op\"" 4)
  11 (find_quote_from "glop\"gl\\\"op\"" 5)
 *)

let rec find_dot_from s i =
  if i >= String.length s then raise Not_found ;
  if s.[i] = '.' then i else find_dot_from s (i + 1)

(* query is composed of:
 * several components separated by a dots. Each components can be either a
 * plain word (optionally quoted, and then dots within that string are not
 * component separators) or a star but the last component can also be "*word*".
 * Splitting this string requires caution as "." can legitimately appear in
 * a string or float value. We assume all such problematic strings and
 * floats will be quoted.
 *
 * In order to be able to chop off some components from the original query
 * string all components must be preserved with quotes. *)
let split_query s =
  let rec extract_next prev i =
    (* TODO: Array.of_rev_list *)
    if i >= String.length s then List.rev prev |> Array.of_list else
    if s.[i] = '"' then (
      (* value extends to next non-escaped double quote *)
      match find_quote_from s (i+1) with
      | exception Not_found -> invalid_arg "split_query: bad quotes"
      | i' ->
          if i' < String.length s - 1 && s.[i' + 1] <> '.' then
            invalid_arg "split_query: bad quotes(2)" ;
          (* Keep the quotes: *)
          let prev' = String.sub s i (i' - i + 1) :: prev in
          extract_next prev' (i' + 2)
    ) else (
      (* value extends to the next dot or end *)
      let i' = try find_dot_from s i with Not_found -> String.length s in
      let prev' = String.sub s i (i' - i) :: prev in
      extract_next prev' (i' + 1)
    )
  in
  extract_next [] 0
(*$= split_query & ~printer:(IO.to_string (Array.print String.print))
  [|"monitoring"; "traffic"; "inbound"; "\"127.0.0.1:56687\""; "0"; "bytes"|] \
    (split_query "monitoring.traffic.inbound.\"127.0.0.1:56687\".0.bytes")
  [|"cpu"; "\"www.srv.com\""; "0"|] (split_query "cpu.\"www.srv.com\".0")
*)

(* Takes the result of the above function and build an array of Globs: *)
let filter_of_query =
  let unquote s =
    let l = String.length s in
    if l > 1 && s.[0] = '\"' && s.[l-1] = '\"' then
      String.sub s 1 (l - 2)
    else s in
  Array.map (Globs.compile % unquote)

(*
 * Answer graphite queries for /metrics/find, documented in
 * http://graphite-api.readthedocs.io/en/latest/api.html#the-metrics-api
 * but the actual graphite does nothing like that
 * (https://github.com/brutasse/graphite-api). It's a bit sad to have to
 * emulate a protocol that's both crap and not documented.
 *
 * id is the query with only the last part completed (other '*' are left
 * as we want to complete only the last component)
 * leaf is a flag set if that completed component has no further descendant,
 * text is the completed last component (same as id ending) and everything
 * else is redundant.
 *)

type metric =
  { text : string ; id : string ;
    (* These 3 are kind of synonymous ; apparently Grafana uses only
     * "expandable". *)
    expandable : int ; leaf : int ; allowChildren : int } [@@ppp PPP_JSON]
type metrics = metric list [@@ppp PPP_JSON]

let metrics_find conf ?since ?until query =
  let split_q = split_query query in
  (* We need to save the prefix that will be used for id: *)
  let prefix =
    let l = Array.length split_q in
    if l <= 1 then "" else
      String.rchop query ~n:(String.length split_q.(l-1)) in
  let filter = filter_of_query split_q in
  !logger.debug "metrics_find: filter = %a"
    (Array.print Globs.print) filter ;
  let programs = get_programs conf in
  (* We are going to compute all possible values for the query, expanding
   * also intermediary globs. But in here we want to return only one result
   * per terminal expansion. So we are going to uniquify the results.
   * Note: there is no way around it, as final completion depends on what
   * we find for intermediary ones. *)
  let uniq = uniquify () in
  inverted_tree_of_programs conf ?since ?until ~only_with_event_time:true
                            ~only_num_fields:true ~factor_expansion:OnlyLast
                            [ filter ] programs //@
  function
  | [] -> None
  | last :: _ ->
      (* It is important to also quote the label, as Grafana won't bother
       * quoting it for its next query: *)
      let text = fix_quote last.value
      and leaf = match last.section with DataField _ -> 1 | _ -> 0 in
      let expandable = leaf lxor 1
      and id = prefix ^ text in
      let res =
        { text ; id ; expandable ; leaf ; allowChildren = expandable } in
      if uniq res then Some res else None

let metrics_find_of_params conf params =
  let get_opt_ts n =
    try
      Hashtbl.find_option params n |>
      Option.map (fun s -> float_of_string (List.hd s))
    with _ -> None
  in
  let since = get_opt_ts "from"
  and until = get_opt_ts "until"
  and query = Hashtbl.find_default params "query" ["*"] |>
              List.hd in
  metrics_find conf ?since ?until query |>
  List.of_enum |>
  PPP.to_string metrics_ppp_json |>
  http_msg

(*
 * Render a selected metric (in JSON only, no actual picture is
 * generated).
 *
 * First the query is expanded fully; ie we build an exhaustive list of all
 * matching paths (unlike in metrics_find where only the last component is
 * expanded). Then for each path we get a time series and return it.
 *)

type render_metric =
  { target : string ;
    datapoints : (float option * int) array } [@@ppp PPP_JSON]
type render_resp = render_metric list [@@ppp PPP_JSON]

(* We output the field name and then a list of factor=value, to be simplified
 * later to remove all common values: *)
let target_name_of used_factors fvals =
  List.fold_lefti (fun res i f ->
    (f, fvals.(i)) :: res
  ) [] used_factors

(* Used to count the number of occurrences of names in labels: *)
module SimpleSet =
struct
  type 'a t = Empty | Single of 'a | Many
  let add t x =
    match t with
    | Empty -> Single x
    | Single y when x = y -> t
    | _ -> Many

  let has_many = function Many -> true | _ -> false

  let print p oc = function
    | Empty -> Printf.fprintf oc "<empty>"
    | Single v -> Printf.fprintf oc "{%a}" p v
    | Many -> Printf.fprintf oc "{...many!...}"
end

let targets_for_render conf ?since ?until queries =
  (* We start by expanding the query so that we have also an expansion
   * for field/function names with matches. *)
  let filters =
    List.map (filter_of_query % split_query) queries
  and programs = get_programs conf in
  inverted_tree_of_programs conf ?since ?until ~only_with_event_time:true
                            ~only_num_fields:true ~factor_expansion:All
                            filters programs //@
  (* Instead of a list of components, rather have func, fq, fvals
   * and data_field, where fvals None means all *)
  fun comps ->
    let func, fvals, data_field =
      List.fold_left (fun (func, fvals, data_field as prev) comp ->
        match comp.section with
        | OpName f ->
            assert (func = None) ;
            Some f, fvals, data_field
        | FactorAll factor ->
            func, Set.add (factor, None) fvals, data_field
        | FactorValue (factor, value) ->
            func, Set.add (factor, Some value) fvals, data_field
        | DataField field ->
            assert (data_field = None) ;
            func, fvals, Some field
        | ProgPath -> prev
      ) (None, Set.empty, None) comps in
    match func, data_field with
    | Some func, Some data_field ->
        let fq = F.fq_name func in
        Some (func, fq, fvals, data_field)
    | _ -> None (* ignore incomplete targets *)

let render conf headers body =
  let content_type = get_content_type headers in
  let open CodecMultipartFormData in
  let params = parse_multipart_args content_type body in
  let v x = x.value in
  let now = Unix.gettimeofday () in
  let (|>>) = Option.bind in
  let queries = Hashtbl.find_all params "target" |> List.map v
  (* From http://graphite-api.readthedocs.io/en/latest/api.html#from-until:
   *  "If from is omitted, it defaults to 24 hours ago If until is omitted,
   *   it defaults to the current time (now)" *)
  and since = Hashtbl.find_option params "from" |> Option.map v |>>
              time_of_graphite_time |? now -. 86400.
  and until = Hashtbl.find_option params "until" |> Option.map v |>>
              time_of_graphite_time |? now
  and max_data_points = Hashtbl.find_option params "maxDataPoints" |>
                        Option.map (int_of_string % v) |? 300
  and format = Hashtbl.find_option params "format" |>
               Option.map v |? "json" in
  if format <> "json" then failwith "only JSON format is supported" ;
  let targets =
    targets_for_render conf ~since ~until queries |>
    Array.of_enum in
  (* Now we need to decide, for each factor value, if we want it in a where
   * filter (it's the only value we want for this factor) or if we want to
   * get a time series for all possible values (in a single scan). For this
   * we merely count how many distinct values we are asking for: *)
  let wanted_factors = Hashtbl.create 9 in
  let print_wanted_factors =
    let fq_field_print oc (fq, field) =
      Printf.fprintf oc "%a.%a" N.fq_print fq N.field_print field in
    Hashtbl.print fq_field_print (Option.print T.print) in
  Array.iter (fun (_func, fq, fvals, _data_field) ->
    !logger.debug "Factors for %a: %a"
      N.fq_print fq
      (Set.print (fun oc (f, v_opt) ->
        Printf.fprintf oc "%a=%a" N.field_print f
          (Option.print T.print) v_opt)) fvals ;
    Set.iter (fun (factor, opt_val) ->
      match opt_val with
      | None ->
          (* None means high cardinality: *)
          Hashtbl.replace wanted_factors (fq, factor) None
      | Some fval ->
          Hashtbl.modify_opt (fq, factor) (function
            | None -> Some (Some fval)
            | Some None as prev -> prev
            | Some (Some v) as prev ->
                if v = fval then prev
                else Some None
          ) wanted_factors
    ) fvals
  ) targets ;
  !logger.debug "wanted factors: %a" print_wanted_factors wanted_factors ;

  (* Now we can decide on which scans to perform *)
  let scans = Hashtbl.create 9 in
  Array.iter (fun (func, fq, fvals, data_field) ->
    let where, factors =
      Set.fold (fun (factor, _) (where, factors) ->
        match Hashtbl.find wanted_factors (fq, factor) with
        | Some fval ->
            (* If we are interested in only one value, do not ask for this
             * factor but add a where filter: *)
            (factor, "=", fval) :: where,
            factors
        | None (* aka many *) ->
          (* We want several values for that factor, so we will take it as a
           * factor: *)
          where, Set.add factor factors
      ) fvals ([], Set.empty) in
    Hashtbl.modify_opt (fq, where) (function
      | None ->
          Some (Set.singleton data_field, factors, func)
      | Some (d, f, func) ->
          Some (Set.add data_field d, Set.union factors f, func)
    ) scans
  ) targets ;

  (* Now actually run the scans, one for each function/where pair, and start
   * building the result. For each columns we want one time series per data
   * field. *)
  let metrics_of_scan (fq, where) (data_fields, factors, _func) res =
    (* [columns] will be an array of the factors. [datapoints] is an
     * enumeration of arrays with one entry per factor, the entry being an
     * array of one time series per data_fields. *)
    let data_fields = Set.to_list data_fields
    and factors = Set.to_list factors in
    let columns, datapoints =
      if conf.C.sync_url = "" then
        RamenTimeseries.get_local conf max_data_points since until where
                                  factors fq data_fields
      else
        let _zock, _session, clt = ZMQClient.get_connection () in
        RamenTimeseries.get_sync conf max_data_points since until where
                                 factors fq data_fields ~while_ clt in
    let datapoints = Array.of_enum datapoints in
    (* datapoints.(time).(factor).(data_field) *)
    Array.fold_lefti (fun res col_idx column ->
      List.fold_lefti (fun res field_idx data_field ->
        let datapoints =
          Array.map (fun (t, v) ->
            (if Array.length v > 0 then v.(col_idx).(field_idx) else None),
            int_of_float t
          ) datapoints
        and target = target_name_of factors column in
        (data_field, target, datapoints) :: res
      ) res data_fields
    ) res columns
  in
  let resp = Hashtbl.fold metrics_of_scan scans [] in
  (* Build the render_metric list out of those values, simplifying
   * the label:
   * Map from factor name to values present in the label: *)
  let factor_values = Hashtbl.create 9 in
  let data_fields =
    List.fold_left (fun dfs (data_field, target, _) ->
      List.iter (fun (fact_name, fact_val) ->
        Hashtbl.modify_def SimpleSet.Empty fact_name (fun vals ->
          SimpleSet.add vals fact_val
        ) factor_values ;
      ) target ;
      SimpleSet.add dfs data_field
    ) SimpleSet.Empty resp in
  let with_data_field = SimpleSet.has_many data_fields
  and with_factors = (* Set of factor names we want to keep *)
    Hashtbl.enum factor_values //@ (fun (fact_name, fact_vals) ->
      !logger.debug "Factor %a has values %a"
        N.field_print fact_name
        (SimpleSet.print T.print) fact_vals ;
      if SimpleSet.has_many fact_vals then Some fact_name
      else None) |>
    Set.of_enum in
  !logger.debug "kept factors = %a"
    (Set.print N.field_print) with_factors ;
  let resp =
    List.map (fun ((data_field : N.field), target, datapoints) ->
      let factors =
        List.filter (fun (fact_name, _fact_val) ->
          Set.mem fact_name with_factors
        ) target in
      let target_field =
        if with_data_field || factors = [] then
          (data_field :> string) ^" "
        else ""
      and target_factors =
        match factors with
        | [] -> ""
        | [ _n, v ] -> IO.to_string T.print v
        | _ ->
            IO.to_string
              (List.print ~first:"" ~last:"" ~sep:" " (fun oc (n, v) ->
                Printf.fprintf oc "%a=%a"
                  N.field_print n
                  T.print v)) factors in
      let target =
        if target_field = "" then target_factors else
        if target_factors = "" then target_field else
        target_field ^" "^ target_factors in
      { target ; datapoints }
    ) resp in
  let body = PPP.to_string render_resp_ppp_json resp in
  !logger.debug "%d metrics from %d scans" (List.length resp) (Hashtbl.length scans) ;
  http_msg body

(*
 * All supported HTTP requests:
 *)

let version = http_msg "1.1.3"

let router conf prefix =
  (* The function called for each HTTP request: *)
  fun meth path params headers body ->
    let prefix = RamenHttpHelpers.list_of_prefix prefix in
    let path = RamenHttpHelpers.chop_prefix prefix path in
    match meth, path with
    (* Mimic Graphite for Grafana datasource *)
    | CodecHttp.Command.GET, ["metrics"; "find"] ->
      (* This is a query that's used to find the possible completions
       * of a path. It should answer with all possible single component
       * completions that are compatible with the query, but not expand
       * the globs present in the query. *)
      metrics_find_of_params conf params
    | CodecHttp.Command.POST, ["render"] ->
      render conf headers body
    | CodecHttp.Command.GET, ["version"] ->
      version
    | CodecHttp.Command.OPTIONS, _ ->
      let headers =
        [ "Access-Control-Allow-Methods", "POST" ;
          "Access-Control-Allow-Headers", "Content-Type" ] in
      http_msg ~headers ""
    (* Errors *)
    | CodecHttp.Command.(PUT|GET|DELETE), p ->
      let path = String.join "/" p in
      not_found (Printf.sprintf "Unknown resource %S" path)
    | _ ->
      not_implemented "Method not implemented"
