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
module C = RamenConf
module F = C.Func
module P = C.Program

(*
 * Answer graphite queries for /metrics/find, documented in
 * http://graphite-api.readthedocs.io/en/latest/api.html#the-metrics-api
 * but the actual graphite does nothing like that
 * (https://github.com/brutasse/graphite-api). It's a bit sad to have to
 * emulate a protocol that's both crap and not documented.
 *)

type graphite_metric =
  { text : string (* the name of the path component? *) ;
    (* These 3 are kind of synonymous ; apparently Grafana uses only "expandable". *)
    expandable : int ; leaf : int ; allowChildren : int ;
    id : string (* Not sure if used *) } [@@ppp PPP_JSON]
type graphite_metrics = graphite_metric list [@@ppp PPP_JSON]

let metric_of_worker_path (id, leaf, text, _targets) =
  { text ; id ; expandable = if leaf then 0 else 1 ;
    leaf = if leaf then 1 else 0 ;
    allowChildren = if leaf then 0 else 1 }

(* Tells if all the components of comps matches all globs
 * (as long as there are globs - extra comps are ok).
 * Returns the list of matched query chunks (as id), a flag telling if more
 * comps were present, and the completed name of the last one, and the
 * fully expanded matched target, or None: *)
let comp_matches globs comps =
  let id name = function
    | [] -> (* No id at all? *) name
    | _::ids ->
      List.rev (name :: ids) |> String.concat "." in
  let target lst =
    List.rev (lst) |> String.concat "." in
  let rec loop ids targets name globs comps =
    match globs, comps with
    | [], [] -> Some (id name ids, true, name, target targets)
    | [], _ -> Some (id name ids, false, name, target targets)
    | _, [] -> None
    | (q, g)::globs, c::comps ->
        if Globs.matches g c then
          loop (q :: ids) (c :: targets) c globs comps
        else None
  in
  loop [] [] "" globs comps

(* This ugly E is there to make the type non cyclic: *)
type 'a tree_enum = E of ('a * 'a tree_enum) List.t
let get (E x) = x

let tree_enum_is_empty = function E [] -> true | _ -> false

(* Fold over the tree. [node_init] will be carried from node to node while
 * [stack_init] will be popped when the iterator go forward in the tree.
 * Only return the value that's carried from node to node. *)
let rec tree_enum_fold f node_init stack_init te =
  List.fold_left (fun usr (n, te') ->
    let is_leaf = List.is_empty (get te') in
    let usr', stack' = f usr stack_init n is_leaf in
    tree_enum_fold f usr' stack' te') node_init (get te)

(* We are going to build mostly a string tree_enum, with the addition that we
 * also want to know in which part of the tree we are and also to keep any
 * scalar value we might have for a factor.  So we actually build a tree_enum
 * of: *)
type tree_enum_section =
  ProgPath | OpName | FactorField of RamenTypes.value option | DataField
type string_with_scalar =
  { value : string ; (* The value as a raw string (unquoted) *)
    section : tree_enum_section }

let string_of_tree_enum_section = function
  | ProgPath -> "ProgPath"
  | OpName -> "OpName"
  | FactorField (Some v) -> Printf.sprintf2 "FactorField %a" RamenTypes.print v
  | FactorField None -> "FactorField"
  | DataField -> "DataField"

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

(* Given a func, returns the tree_enum of fields that are not factors and
 * numeric *)
let tree_enum_of_fields only_num_fields func =
  let factors =
    RamenOperation.factors_of_operation func.F.operation in
  let ser =
    RamenOperation.out_type_of_operation func.F.operation |>
    RingBufLib.ser_tuple_typ_of_tuple_typ in
  E (List.filter_map (fun ft ->
       let n = ft.RamenTuple.name in
       if (not only_num_fields ||
           RamenTypes.is_a_num ft.RamenTuple.typ.structure) &&
          not (List.mem n factors)
       then
         Some ({ value = RamenName.string_of_field n ;
                 section = DataField }, E [])
       else
         None
     ) ser)

(* Given a functions and a list of factors (any part of func.F.factors),
 * return the tree_enum of the factors (only the branches going as far as
 * an actual non-factor fields tree) : *)
let rec tree_enum_of_factors conf cache ?since ?until only_num_fields func =
  function
  | [] -> tree_enum_of_fields only_num_fields func
  | factor :: factors' ->
      (* If there is no possible value, output an empty string that will
       * allow us to keep iterating and have a chance to see the fields.
       * Grafana will skip that value and offer only the "*", which is all
       * good. *)
      let fact_values =
        (* In theory we would have a where clause that would restrict this
         * search to factors selected earlier: *)
        let k = since, until, F.fq_name func, factor in
        try Hashtbl.find cache k
        with Not_found ->
          let pv = RamenTimeseries.possible_values conf ?since ?until func factor in
          Hashtbl.add cache k pv ;
          pv in
      let fact_values =
        if Set.is_empty fact_values then
          [ { value = "" ; section = FactorField None } ]
        else
          Set.map (fun v ->
            let value = RamenTypes.to_string ~quoting:false v in
            { value ; section = FactorField (Some v) }
          ) fact_values |>
          Set.to_list in
      E (List.filter_map (fun pv ->
          (* Skip over empty branches *)
          let sub_tree =
            tree_enum_of_factors conf cache ?since ?until only_num_fields
                                 func factors' in
          if tree_enum_is_empty sub_tree then None
          else Some (pv, sub_tree)
         ) fact_values)

(* Given a program, returns the tree_enum of its functions: *)
let tree_enum_of_program
      conf cache ?since ?until ~only_with_event_time ~only_num_fields prog =
  E (
    List.filter_map (fun func ->
      if only_with_event_time &&
         RamenOperation.event_time_of_operation func.F.operation = None
      then None else
        let factors =
          RamenOperation.factors_of_operation func.F.operation in
        let sub_tree =
          tree_enum_of_factors conf cache ?since ?until only_num_fields func
                               factors in
        if tree_enum_is_empty sub_tree then None else
          Some (
            let value = RamenName.string_of_func func.F.name in
            { value ; section = OpName }, sub_tree)
    ) prog.P.funcs)

(* Given the programs hashtable, return a tree_enum of the path components and
 * programs. Merely build a hashtbl of hashtbls.
 * The hash has a boolean alongside the prefix, indicating if the value is a
 * Prog or a Hash, so that we can have both under the same name. *)
type program_tree_item =
  | Prog of (RamenName.program * (unit -> P.t))
  | Hash of ((bool * string), program_tree_item) Hashtbl.t

let tree_enum_of_programs
    conf cache ?since ?until ~only_with_event_time ~only_num_fields
    ~only_running programs =
  let programs =
    Hashtbl.enum programs //
    (if only_running then
      (fun (_p, (mre, _get_rc)) -> mre.C.status = C.MustRun)
     else (fun _ -> true)) |>
    Array.of_enum in
  Array.fast_sort (fun (k1, _) (k2, _) ->
    String.compare (RamenName.string_of_program k1)
                   (RamenName.string_of_program k2)) programs ;
  let rec hash_for_prefix pref =
    let h = Hashtbl.create 11 in
    let pl = String.length pref in
    Array.iter (fun (program_name, (_mre, get_rc)) ->
      if String.starts_with
           (RamenName.string_of_program program_name) pref
      then
        (* Do not look for '/' in the parameter expansion suffix: *)
        let suf =
          String.lchop ~n:pl (RamenName.string_of_program program_name) in
        (* Get the next prefix *)
        match String.split suf ~by:"/" with
        | exception Not_found ->
            Hashtbl.add h (false, suf) (Prog (program_name, get_rc))
        | suf, _rest ->
            (* If we've done it already, continue: *)
            if not (Hashtbl.mem h (true, suf)) then
              let pref' = pref ^ suf ^"/" in
              Hashtbl.add h (true, suf) (Hash (hash_for_prefix pref'))
      (* TODO: exit when we stop matching as the array is ordered *)
    ) programs ;
    h in
  let h = hash_for_prefix "" in
  let rec tree_enum_of_h h =
    E (Hashtbl.enum h //@ (function
      | (_, name), Prog (_prog_name, get_rc) ->
        (match get_rc () with
        | exception _ -> None
        | prog ->
            let sub_tree =
              tree_enum_of_program conf cache ?since ?until
                                   ~only_with_event_time ~only_num_fields
                                   prog in
            if tree_enum_is_empty sub_tree then None else
              Some ({ value = name ; section = ProgPath }, sub_tree))
      | (_, name), Hash h ->
        let sub_tree = tree_enum_of_h h in
        if tree_enum_is_empty sub_tree then None else
          Some (
            { value = name ; section = ProgPath }, sub_tree)) |>
     List.of_enum) in
  tree_enum_of_h h

(* User pass a filter such as "foo.bar*.*.baz".
 * We make it a list of 4 filters: "foo", "bar*", "*" and "baz".
 * (This list is the argument [query] here). *)
let filters_of_query query =
  List.map (fun q ->
    let glob = Globs.compile q in
    fun pv -> Globs.matches glob pv.value
  ) query

(* Given a tree enumerator and a list of filters, return a tree enumerator
 * of the entries matching the filter: *)
let filter_tree ?anchor_right te flts =
  let skip cause =
    !logger.debug "Skip a subtree because %s" cause ; None in
  let rec loop te = function
    | [] ->
        if anchor_right = Some true && te <> E [] then
          skip "past right anchor"
        else Some te
    | flt :: flts ->
        Some (E (
          List.filter_map (fun (a, te') ->
            if flt a then
              Option.map (fun sub_tree -> a, sub_tree) (loop te' flts)
            else skip (a.value ^ " fail filter")
          ) (get te)))
  in loop te flts |? E []

let full_enum_tree_of_query
      conf cache ?since ?until ?anchor_right ?(only_running=false)
      ?(only_with_event_time=false) ?(only_num_fields=false) query =
    let programs = C.with_rlock conf identity in
    let te = tree_enum_of_programs conf cache ?since ?until
                                   ~only_with_event_time
                                   ~only_num_fields
                                   ~only_running programs in
    let filters = filters_of_query query in
    filter_tree ?anchor_right te filters

(* Specialized version for graphite, skipping functions with no time info
 * and cached (TODO): *)
let enum_tree_of_query
    conf cache ?since ?until ?anchor_right query =
  full_enum_tree_of_query
    conf cache ?since ?until ?anchor_right ~only_with_event_time:true
    ~only_num_fields:true ~only_running:false query

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
 * The answer is a list of graphite_metric which name field must be the
 * completed last component and which id field the query with the last
 * component completed. The idea is that id will then identify the time series
 * we want to display (once expanded).
 * Extracting factors value from this string requires caution as "." can
 * legitimately appear in a string or float value. We assume all such
 * problematic strings and floats will be quoted. *)
let split_query s =
  let rec extract_next prev i =
    if i >= String.length s then List.rev prev else
    if s.[i] = '"' then (
      (* value extends to last non-escaped double quote *)
      match find_quote_from s (i+1) with
      | exception Not_found -> invalid_arg "split_query: bad quotes"
      | i' ->
          if i' < String.length s - 1 && s.[i' + 1] <> '.' then
            invalid_arg "split_query: bad quotes(2)" ;
          let prev' = String.sub s (i + 1) (i' - i - 1) :: prev in
          extract_next prev' (i' + 2)
    ) else (
      (* value extends to last dot or end *)
      let i' = try find_dot_from s i with Not_found -> String.length s in
      let prev' = String.sub s i (i' - i) :: prev in
      extract_next prev' (i' + 1)
    )
  in
  extract_next [] 0
(*$= split_query & ~printer:(IO.to_string (List.print String.print))
  [ "monitoring"; "traffic"; "inbound"; "127.0.0.1:56687"; "0"; "bytes" ] \
    (split_query "monitoring.traffic.inbound.\"127.0.0.1:56687\".0.bytes")
  [ "cpu"; "www.srv.com"; "0" ] (split_query "cpu.\"www.srv.com\".0")
*)

(* Enumerate the (id, leaf, text, target) matching a query. Where:
 * id is the query with only the last part completed (used for autocompletion)
 * leaf is a flag set if that completed component has no further descendant,
 * text is the completed last component (same as id ending) and target
 * is the fully expanded query string matching id. *)
let expand_query_text conf ?since ?until query =
  let query = split_query query in
  let cache = Hashtbl.create 11 in
  let filtered = enum_tree_of_query conf cache ?since ?until query in
  let num_filters = List.length query in
  let prefix = (List.take (num_filters - 1) query |>
                String.concat ".") ^ "." in
  tree_enum_fold (fun node_res (depth, target) pv is_leaf ->
    (* depth starts at 0 *)
    let value = fix_quote pv.value in
    let target' =
      if target = "" then value else target ^"."^ value in
    (if depth = num_filters - 1 then
      (prefix ^ value, is_leaf, value, target') :: node_res
    else
      node_res),
    (depth + 1, target')
  ) [] (0, "") filtered

let expand_query_values conf cache ?since ?until ?anchor_right query =
  let query = split_query query in
  let depth0 = "", "", [], "" in
  let filtered =
    enum_tree_of_query conf cache ?since ?until ?anchor_right query in
  tree_enum_fold (fun node_res
                      (prog_name, func_name, factors, data_field)
                      pv is_leaf ->
    match pv.section with
    | ProgPath ->
        let prog_name = if prog_name = "" then pv.value
                        else prog_name ^"/"^ pv.value in
        let prog_name = fix_quote prog_name in
        node_res, (prog_name, func_name, factors, data_field)
    | OpName ->
        node_res,
        (prog_name, pv.value, factors, data_field)
    | FactorField v_opt ->
        node_res,
        (prog_name, func_name, v_opt :: factors, data_field)
    | DataField ->
        assert is_leaf ;
        (RamenName.program_of_string prog_name,
         RamenName.func_of_string func_name,
         List.rev factors,
         RamenName.field_of_string pv.value) :: node_res, depth0
  ) [] depth0 filtered

let complete_graphite_find conf params =
  let get_opt_ts n =
    try
      Hashtbl.find_option params n |>
      Option.map (fun s -> float_of_string (List.hd s))
    with _ -> None
  in
  let since = get_opt_ts "from"
  and until = get_opt_ts "until" in
  let expanded =
    Hashtbl.find_default params "query" ["*"] |>
    List.hd |>
    expand_query_text conf ?since ?until in
  let uniq = uniquify () in
  let resp =
    List.filter_map (fun r ->
      let r = metric_of_worker_path r in
      if uniq r then Some r else None
    ) expanded in
  let body = PPP.to_string graphite_metrics_ppp_json resp in
  http_msg body

(*
 * Render a selected metric (in JSON only, no actual picture is
 * generated).
 *)

type graphite_render_metric =
  { target : string ;
    datapoints : (float option * int) array } [@@ppp PPP_JSON]
type graphite_render_resp = graphite_render_metric list [@@ppp PPP_JSON]

let time_of_graphite_time s =
  let len = String.length s in
  if len = 0 then None
  else if s.[0] = '-' then time_of_reltime s
  else time_of_abstime s

(* We output the field name and then a list of factor=value, to be simplified
 * later to remove all common values: *)
let target_name_of used_factors fvals =
  List.fold_lefti (fun res i f ->
    (f, fvals.(i)) :: res
  ) [] used_factors

(* Used to cound number of occurrences of names in labels: *)
module SimpleSet =
struct
  type 'a t = Empty | Single of 'a | Many
  let add t x =
    match t with
    | Empty -> Single x
    | Single y when x = y -> t
    | _ -> Many

  let has_many = function Many -> true | _ -> false
end

let render_graphite conf headers body =
  let content_type = get_content_type headers in
  let open CodecMultipartFormData in
  let params = parse_multipart_args content_type body in
  let v x = x.value in
  let now = Unix.gettimeofday () in
  let (|>>) = Option.bind in
  let targets = Hashtbl.find_all params "target" |> List.map v
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
  assert (format = "json") ; (* FIXME *)
  (* We start by expanding the query so that we have also an expansion
   * for field/function names with matches. *)
  let targets =
    let cache = Hashtbl.create 11 in
    List.map (expand_query_values conf cache ~anchor_right:true ~since
                                  ~until) targets in
  (* Targets is now a list of enumerations of program + function name + factors
   * values + field name. Regardless of how many original query were sent, the
   * expected answer is a flat array of target name + time series.  We can
   * therefore flatten all the expanded targets and regroup by operation +
   * where filter, then scan data for each of those groups asking for all
   * possible factors. *)
  let targets = List.flatten targets in
  (* In addition to the prog and func names we'd like to have the func: *)
  let targets =
    C.with_rlock conf (fun programs ->
      List.filter_map (fun (prog_name, func_name, fvals, data_field) ->
        let fq = RamenName.fq prog_name func_name in
        match Hashtbl.find programs prog_name with
        | exception Not_found ->
            !logger.error "Program %s just disappeared?"
              (RamenName.string_of_program prog_name) ;
            None
        | _mre, get_rc ->
            (match get_rc () with
            | exception _ -> None
            | prog ->
                (match List.find (fun f ->
                         f.F.name = func_name) prog.P.funcs with
                | exception Not_found ->
                    !logger.error "Function %s just disappeared?"
                      (RamenName.string_of_fq fq) ;
                    None
                | func ->
                    let factors =
                      RamenOperation.factors_of_operation func.operation in
                    if List.length fvals <> List.length factors then (
                      !logger.error "Function %s just changed factors?"
                        (RamenName.string_of_fq fq) ;
                      None
                    ) else if List.mem data_field factors then (
                      !logger.error "Function %a just got %a as factor?"
                        RamenName.fq_print fq
                        RamenName.field_print data_field ;
                      None
                    ) else if not (
                              RamenOperation.out_type_of_operation
                                func.operation |>
                              List.exists (fun ft ->
                                ft.RamenTuple.name = data_field)
                            ) then (
                      !logger.error "Function %a just lost field %a?"
                        RamenName.fq_print fq
                        RamenName.field_print data_field ;
                      None
                    ) else (
                      Some (func, fq, fvals, data_field)
                    )))
      ) targets) in
  (* Now we need to decide, for each factor value, if we want it in a where
   * filter (it's the only value we want for this factor) or if we want to
   * get a time series for all possible values (in a single scan). For this
   * we merely count how many distinct values we are asking for: *)
  let factor_values = Hashtbl.create 9 in
  let count_factor_values (_func, fq, fvals, data_field) =
    !logger.debug "target = op:%s, fvals:%a, data:%a"
      (RamenName.string_of_fq fq)
      (List.print (Option.print RamenTypes.print)) fvals
      RamenName.field_print data_field ;
    List.iteri (fun i fval_opt ->
      match fval_opt with
      | None -> ()
      | Some fval ->
          Hashtbl.modify_opt (fq, i) (function
            | None -> Some (Set.singleton fval)
            | Some s -> Some (Set.add fval s)
          ) factor_values
    ) fvals in
  List.iter count_factor_values targets ;
  (* Now we can decide on which scans to perform *)
  let scans = Hashtbl.create 9 in
  let add_scans (func, fq, fvals, data_field) =
    let factors =
      RamenOperation.factors_of_operation func.F.operation in
    let where, factors, _ =
      List.fold_left2 (fun (where, factors, i) factor _fval ->
        let wanted =
          Hashtbl.find_default factor_values (fq, i) Set.empty in
        match Set.cardinal wanted with
        | 0 -> (* Can happen if there are no possible values. *)
            where, factors, i + 1
        | 1 ->
          (* If we are interested in only one value, do not ask for this
           * factor but add a where filter: *)
          (factor, "=", Set.min_elt wanted) :: where,
          factors, i + 1
        | _many ->
          (* We want several values for that factor, so we will take it as a
           * factor: *)
          where, Set.add factor factors, i + 1
      ) ([], Set.empty, 0) factors fvals in
    Hashtbl.modify_opt (fq, where) (function
      | None ->
          Some (Set.singleton data_field, factors, func)
      | Some (d, f, func) ->
          Some (Set.add data_field d, Set.union factors f, func)
    ) scans in
  List.iter add_scans targets ;
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
      RamenTimeseries.get conf max_data_points since until where factors
                          fq data_fields in
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
  (* Build the graphite_render_metric list out of those values, simplifying
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
      if SimpleSet.has_many fact_vals then Some fact_name
      else None) |>
    Set.of_enum in
  !logger.debug "kept factors = %a"
    (Set.print RamenName.field_print) with_factors ;
  let resp =
    List.map (fun (data_field, target, datapoints) ->
      let target =
        Printf.sprintf2 "%s%a"
          (if with_data_field then
            RamenName.string_of_field data_field ^" "
           else "")
          (List.print ~first:"" ~last:"" ~sep:" " (fun oc (n, v) ->
            Printf.fprintf oc "%a=%a"
              RamenName.field_print n
              RamenTypes.print v))
            (List.filter (fun (fact_name, _fact_val) ->
              Set.mem fact_name with_factors
             ) target) in
      { target ; datapoints }
    ) resp in
  let body = PPP.to_string graphite_render_resp_ppp_json resp in
  !logger.debug "%d metrics from %d scans" (List.length resp) (Hashtbl.length scans) ;
  http_msg body

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
      complete_graphite_find conf params
    | CodecHttp.Command.POST, ["render"] ->
      render_graphite conf headers body
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
