(* Graphite impersonator.
 *
 * Implement (the most important bits of) Graphite API for Grafana.
 * This way, there is no need for a Grafana plugin. Just run
 * `ramen graphite --port=XYZ` and create a graphite datasource in
 * Grafana, and then basic charts of timeseries should just work.
 *
 * What is supported:
 * - Normal charts;
 * - Autocompletion of timeseries in chart editor;
 * - The "*" metric;
 * - All dates used by Grafana.
 *
 * Functions are currently not supported and should be disabled.
 * There seems to be a call to get the list of supported functions, maybe
 * investigate that?
 *)
open Batteries
open BatOption.Infix
open Lwt
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
 * emulate a protocol that's both crap and non documented.
 *)

type graphite_metric =
  { text : string (* the name of the path component?*) ;
    (* These 3 are kind of synonymous ; apparently Grafana uses only "expandable". *)
    expandable : int ; leaf : int ; allowChildren : int ;
    id : string (* Not sure if used *) } [@@ppp PPP_JSON]
type graphite_metrics = graphite_metric list [@@ppp PPP_JSON]

let metric_of_worker_path (id, leaf, text, targets) =
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

(* Fold over the tree. [node_init] will be carried from node to node while
 * [stack_init] will be popped when the iterator go forward in the tree.
 * Only return the value that's carried from node to node. *)
let rec tree_enum_fold f node_init stack_init te =
  List.fold_left (fun usr (n, te') ->
    let is_leaf = List.is_empty (get te') in
    let usr', stack' = f usr stack_init n is_leaf in
    tree_enum_fold f usr' stack' te') node_init (get te)

(* Given a tree enumerator and a list of filters, return a tree enumerator
 * of the entries matching the filter: *)
let rec filter_tree te = function
  | [] -> te
  | flt :: flts ->
      E (List.filter_map (fun (a, te') ->
        if flt a then Some (a, filter_tree te' flts)
        else None) (get te))

(* We are going to build mostly a string tree_enum, with the addition that we
 * also want to know in which part of the tree we are and also to keep any
 * scalar value we might have for a factor.  So we actually build a tree_enum
 * of: *)
type tree_enum_section =
  ProgPath | OpName | FactorField of RamenTypes.value option | DataField
type string_with_scalar = string * tree_enum_section

(* Given a func, returns the tree_enum of fields that are not factors and
 * numeric *)
let tree_enum_of_fields func =
  let ser = RingBufLib.ser_tuple_typ_of_tuple_typ func.F.out_type in
  E (List.filter_map (fun ft ->
       let n = ft.RamenTuple.typ_name in
       if RamenTypes.is_a_num ft.RamenTuple.typ.structure &&
          not (List.mem n func.F.factors)
       then
         Some ((n, DataField), E [])
       else
         None
     ) ser)

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

(* Given a functions and a list of factors (any part of func.F.factors),
 * return the tree_enum of the factors: *)
let rec tree_enum_of_factors func = function
  | [] -> tree_enum_of_fields func
  | factor :: factors' ->
      (* If there is no possible value, output an empty string that will
       * allow us to keep iterating and have a chance to see the fields.
       * Grafana will skip that value and offer only the "*", which is all
       * good. *)
      let fact_values = RamenTimeseries.possible_values func factor in
      let fact_values =
        if Set.is_empty fact_values then [ "", FactorField None ]
        else
          Set.map (fun v ->
            RamenTypes.to_string v |> fix_quote,
            FactorField (Some v)
          ) fact_values |>
          Set.to_list in
      E (List.map (fun pv ->
           pv, tree_enum_of_factors func factors'
         ) fact_values)

(* Given a program, returns the tree_enum of its functions: *)
let tree_enum_of_program prog =
  E (List.filter_map (fun func ->
       if func.F.event_time <> None then
         Some ((RamenName.string_of_func func.F.name, OpName),
               tree_enum_of_factors func func.F.factors)
       else None
     ) prog.P.funcs)

(* Given the programs hashtable, return a tree_enum of the path components and
 * programs. Merely build a hashtbl of hashtbls.
 * The hash has a boolean alongside the prefix, indicating if the value is a
 * Prog or a Hash, so that we can have both under the same name. *)
type program_tree_item =
  | Prog of (RamenName.program * (unit -> string * P.t))
  | Hash of ((bool * string), program_tree_item) Hashtbl.t

let tree_enum_of_programs programs =
  let programs =
    Hashtbl.enum programs |>
    Array.of_enum in
  Array.fast_sort (fun (k1, _) (k2, _) ->
    String.compare (RamenName.string_of_program k1)
                   (RamenName.string_of_program k2)) programs ;
  let rec hash_for_prefix pref =
    let h = Hashtbl.create 11 in
    let pl = String.length pref in
    Array.iter (fun (program_name, get_rc as p) ->
      if String.starts_with
           (RamenName.string_of_program program_name) pref
      then
        (* Do not look for '/' in the parameter expansion suffix: *)
        let suf =
          String.lchop ~n:pl (RamenName.string_of_program program_name) in
        (* Get the next prefix *)
        match String.split suf ~by:"/" with
        | exception Not_found ->
            Hashtbl.add h (false, suf) (Prog p)
        | suf, rest ->
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
      | (_, name), Prog (program_name, get_rc) ->
        (match get_rc () with
        | exception _ -> None
        | _bin, prog ->
            Some ((name, ProgPath), tree_enum_of_program prog))
      | (_, name), Hash h ->
        Some ((name, ProgPath), tree_enum_of_h h)) |>
     List.of_enum) in
  tree_enum_of_h h

let filters_of_query query =
  List.map (fun q ->
    let glob = Globs.compile q in
    Globs.matches glob % fst) query

let enum_tree_of_query =
  let reread () conf =
    !logger.debug "Recomputing enum_tree for query expansion" ;
    let%lwt programs = C.with_rlock conf return in
    !logger.debug "Caching factors possible values..." ;
    let%lwt () = RamenTimeseries.cache_possible_values conf programs in
    !logger.debug "Building tree..." ;
    return (tree_enum_of_programs programs)
  and time () conf =
    C.last_conf_mtime conf
  in
  let get_tree = cached2 "enum_tree" reread time in
  fun conf query ->
    !logger.debug "Expanding query %a..."
      (List.print String.print) query ;
    let%lwt te = get_tree () conf in
    let filters = filters_of_query query in
    return (filter_tree te filters)

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
 * several components separated by a dots. Each non last components
 * can be either a plain word or a star. The last component can also
 * be "*word*".
 * The answer is a list of graphite_metric which name field must be the
 * completed last component and which id field the query with the last
 * component completed. The idea is that id will then identify the timeseries
 * we want to display (once expanded).
 * Extracting factors value from this string requires caution as "." can
 * legitimately appear in a string or float value. We assume all such
 * problematic strings and floats will be double-quoted. *)
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
 *)

(* Enumerate the (id, leaf, text, target) matching a query. Where:
 * id is the query with only the last part completed (used for autocompletion)
 * leaf is a flag set if that completed component has no further descendant,
 * text is the completed last component (same as id ending) and target
 * is the fully expanded query string matching id. *)
let expand_query_text conf query =
  let query = split_query query in
  let%lwt filtered = enum_tree_of_query conf query in
  let num_filters = List.length query in
  let prefix = (List.take (num_filters - 1) query |>
                String.concat ".") ^ "." in
  tree_enum_fold (fun node_res (depth, target) (n, section) is_leaf ->
    (* depth starts at 0 *)
    let target' =
      if target = "" then n else target ^"."^ n in
    (if depth = num_filters - 1 then
      (prefix ^ n, is_leaf, n, target') :: node_res
    else
      node_res),
    (depth + 1, target')
  ) [] (0, "") filtered |>
  return

let expand_query_values conf query =
  let query = split_query query in
  let%lwt filtered = enum_tree_of_query conf query in
  let depth0 = "", "", [], "" in
  tree_enum_fold (fun node_res
                      (prog_name, func_name, factors, data_field)
                      (n, section) is_leaf ->
    match section with
    | ProgPath ->
        let prog_name = if prog_name = "" then n else prog_name ^"/"^ n in
        node_res, (prog_name, func_name, factors, data_field)
    | OpName ->
        node_res, (prog_name, n, factors, data_field)
    | FactorField v_opt ->
        node_res, (prog_name, func_name, (n, v_opt) :: factors, data_field)
    | DataField ->
        assert is_leaf ;
        (RamenName.program_of_string prog_name,
         RamenName.func_of_string func_name,
         List.rev factors, n) :: node_res, depth0
  ) [] depth0 filtered |>
  return

let complete_graphite_find conf headers params =
  let%lwt expanded =
    Hashtbl.find_default params "query" "*" |>
    expand_query_text conf in
  let uniq = uniquify () in
  let resp =
    List.filter_map (fun r ->
      let r = metric_of_worker_path r in
      if uniq r then Some r else None
    ) expanded in
  let body = PPP.to_string graphite_metrics_ppp_json resp in
  respond_ok body

(*
 * Render a selected metric (in JSON only, no actual picture is
 * generated).
 *)

type graphite_render_metric =
  { target : string ;
    datapoints : (float option * int) array ;
    (* weight is only used when computing the top: *)
    mutable weight : float [@ppp_ignore 0.] } [@@ppp PPP_JSON]
type graphite_render_resp = graphite_render_metric list [@@ppp PPP_JSON]

(* Reduce the size of a graphite_render_resp by aggregating smaller
 * contributions: *)
let reduce_render_resp max_ts resp =
  let num_ts = List.length resp in
  if num_ts <= max_ts then resp else (
    (* Compute the total contribution (weight): *)
    List.iter (fun m ->
      m.weight <-
        Array.fold_left (fun w -> function
          | None, _ -> w
          | Some v, _ -> w +. abs_float v
        ) 0. m.datapoints
    ) resp ;
    (* Sort with smallest weight first: *)
    let resp =
      List.fast_sort (fun m1 m2 -> Float.compare m1.weight m2.weight) resp in
    (* Aggregate the smallest together: *)
    let num_aggr = (num_ts - max_ts) + 1 in
    let acc =
      { target = string_of_int num_aggr ^" others" ;
        datapoints = (List.hd resp).datapoints |>
                     Array.map (fun (vo, t) -> None, t) ;
        weight = 0. } in
    let rec loop n rest =
      if n <= 0 then acc :: rest else
      match rest with
      | [] -> acc :: rest
      | m :: rest ->
          Array.iter2i (fun i ad md ->
            match ad, md with
            | (Some v1, t1), (Some v2, t2) ->
                assert (t1 = t2) ;
                acc.datapoints.(i) <- Some (v1 +. v2), t1
            | (None, t1), (Some v, t2) | (Some v, t1), (None, t2) ->
                assert (t1 = t2) ;
                acc.datapoints.(i) <- Some v, t1
            | _ -> ()
          ) acc.datapoints m.datapoints ;
          loop (n - 1) rest in
    loop num_aggr resp)

let time_of_graphite_time s =
  let len = String.length s in
  if len = 0 then None
  else if s.[0] = '-' then time_of_reltime s
  else time_of_abstime s

(* Return the target name of that function, given the where filter and
 * used factors. [fvals] are the scalar values to use for the factored
 * fields (fields present in [factors], in same order): *)
let target_name_of func_name func_factors where used_factors fvals data_field =
  (* Return the name of field for the [i]th factor of the function,
   * ie the fvals if this field has been used as factor, or the
   * value from the where filter otherwise: *)
  let print_factor oc factor =
    (match List.findi (fun i f -> factor = f) used_factors with
    | exception Not_found -> (* take the value from the where filter *)
        List.assoc factor where
    | i, _ -> (* take the [i]th value *)
        List.nth fvals i) |>
    RamenTypes.to_string |> fix_quote |> String.print oc
  in
  Printf.sprintf2 "%s%s%a%s"
    (String.nreplace ~str:func_name ~sub:"/" ~by:".")
    (if func_factors = [] then "" else ".")
    (List.print ~first:"" ~last:"." ~sep:"." print_factor) func_factors
    data_field
(*$= target_name_of & ~printer:identity
  "a.df" (target_name_of "a" [] [] [] [] "df")
  "a.b.df" (target_name_of "a/b" [] [] [] [] "df")
  "a.b.v1.df" (target_name_of "a/b" ["f1"] ["f1", VString "v1"] [] [] "df")
  "a.b.v1.v2.df" \
    (target_name_of "a/b" ["f1";"f2"] ["f1", VString "v1"] \
                    ["f2"] [VString "v2"] "df")
  "a.\"3.14\".df" \
    (target_name_of "a" ["f1"] ["f1", VFloat 3.14] [] [] "df")
 *)

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
  and max_timeseries = Hashtbl.find_option params "maxTimeseries" |>
                       Option.map (int_of_string % v) |? 30
  and format = Hashtbl.find_option params "format" |>
               Option.map v |? "json" in
  assert (format = "json") ; (* FIXME *)
  (* We start by expanding the query so that we have also an expansion
   * for field/function names with matches. *)
  let%lwt targets =
    Lwt_list.map_s (expand_query_values conf) targets in
  (* Targets is now a list of enumerations of program + function name + factors
   * values + field name. Regardless of how many original query were sent, the
   * expected answer is a flat array of target name + timeseries.  We can
   * therefore flatten all the expanded targets and regroup by operation +
   * where filter, then scan data for each of those groups asking for all
   * possible factors. *)
  let targets = List.flatten targets in
  (* In addition to the prog and func names we'd like to have the func: *)
  let%lwt targets =
    C.with_rlock conf (fun programs ->
      Lwt_list.filter_map_s (fun (prog_name, func_name, fvals, data_field) ->
        let fq = RamenName.string_of_program prog_name ^"/"^
                 RamenName.string_of_func func_name in
        match Hashtbl.find programs prog_name () with
        | exception Not_found ->
            !logger.error "Program %s just disappeared?"
              (RamenName.string_of_program prog_name) ;
            return_none
        | exception e -> return_none
        | _bin, prog ->
            (match List.find (fun f -> f.F.name = func_name) prog.P.funcs with
            | exception Not_found ->
                !logger.error "Function %s just disappeared?" fq ;
                return_none
            | func ->
                if List.length fvals <> List.length func.factors then (
                  !logger.error "Function %s just changed factors?" fq ;
                  return_none
                ) else if List.mem data_field func.factors then (
                  !logger.error "Function %s just got %s as factor?"
                    fq data_field ;
                  return_none
                ) else if not (List.exists (fun ft ->
                               ft.RamenTuple.typ_name = data_field
                             ) func.out_type) then (
                  !logger.error "Function %s just lost field %s?"
                    fq data_field ;
                  return_none
                ) else (
                  return (Some (func, fq, List.map snd fvals, data_field))
                ))
      ) targets) in
  (* Now we need to decide, for each factor value, if we want it in a where
   * filter (it's the only value we want for this factor) or if we want to
   * get a timeseries for all possible values (in a single scan). For this
   * we merely count how many distinct values we are asking for: *)
  let factor_values = Hashtbl.create 9 in
  let count_factor_values (func, func_name, fvals, data_field) =
    !logger.debug "target = op:%s, fvals:%a, data:%s"
      func_name (List.print (Option.print RamenTypes.print)) fvals data_field ;
    List.iteri (fun i fval_opt ->
      match fval_opt with
      | None -> ()
      | Some fval ->
          Hashtbl.modify_opt (func_name, i) (function
            | None -> Some (Set.singleton fval)
            | Some s -> Some (Set.add fval s)
          ) factor_values
    ) fvals ;
    return_unit in
  let%lwt () = Lwt_list.iter_s count_factor_values targets in
  (* Now we can decide on which scans to perform *)
  let scans = Hashtbl.create 9 in
  let add_scans (func, func_name, fvals, data_field) =
    let where, factors, _ =
      List.fold_left2 (fun (where, factors, i) factor fval ->
        let wanted =
          Hashtbl.find_default factor_values (func_name, i) Set.empty in
        match Set.cardinal wanted with
        | 0 -> (* Can happen if there are no possible values. *)
            where, factors, i + 1
        | 1 ->
          (* If we are interested in only one value, do not ask for this factor
           * but add a where filter: *)
          (factor, Set.min_elt wanted) :: where,
          factors, i + 1
        | _many ->
          (* We want several values for that factor, so we will take it as a
           * factor: *)
          where, Set.add factor factors, i + 1
      ) ([], Set.empty, 0) func.F.factors fvals in
    Hashtbl.modify_opt (func_name, where) (function
      | None ->
          Some (Set.singleton data_field, factors, func)
      | Some (d, f, func) ->
          Some (Set.add data_field d, Set.union factors f, func)
    ) scans ;
    return_unit in
  let%lwt () = Lwt_list.iter_s add_scans targets in
  (* Now actually run the scans, one for each function/where pair, and start
   * building the result. For each columns we want one timeseries per data
   * field. *)
  let metrics_of_scan (func_name, where) (data_fields, factors, func) res_th =
    (* [columns] will be an array of the factors. [datapoints] is an
     * enumeration of arrays with one entry per factor, the entry being an
     * array of one timeseries per data_fields. *)
    let data_fields = Set.to_list data_fields
    and factors = Set.to_list factors in
    let%lwt columns, datapoints =
      RamenTimeseries.get conf max_data_points since until where factors
                          func_name data_fields in
    let datapoints = Array.of_enum datapoints in
    let%lwt res = res_th in
    (* datapoints.(time).(factor).(data_field) *)
    Array.fold_lefti (fun res colnum column ->
      List.fold_lefti (fun res fieldnum data_field ->
        let datapoints =
          Array.map (fun (t, v) ->
            (if Array.length v > 0 then v.(colnum).(fieldnum) else None),
            int_of_float t
          ) datapoints
        (* TODO: rebuild the target name from the list of values in column *)
        and target = target_name_of func_name func.F.factors where factors
                                    column data_field in
        { target ; datapoints ; weight = 0. } :: res
      ) res data_fields
    ) res columns |> return
  in
  let%lwt resp = Hashtbl.fold metrics_of_scan scans return_nil in
  let resp = reduce_render_resp max_timeseries resp in
  let body = PPP.to_string graphite_render_resp_ppp_json resp in
  !logger.debug "%d metrics from %d scans" (List.length resp) (Hashtbl.length scans) ;
  respond_ok body

let version _conf _headers _params =
  respond_ok "1.1.3"

let router conf prefix =
  (* The function called for each HTTP request: *)
  fun meth path params headers body ->
    let%lwt prefix =
      wrap (fun () -> RamenHttpHelpers.list_of_prefix prefix) in
    let path = RamenHttpHelpers.chop_prefix prefix path in
    match meth, path with
    (* Mimic Graphite for Grafana datasource *)
    | `GET, ["metrics"; "find"] ->
      complete_graphite_find conf headers params
    | `POST, ["render"] ->
      render_graphite conf headers body
    | `GET, ["version"] ->
      version conf headers params
    | `OPTIONS, _ ->
      let headers = Cohttp.Header.init_with "Access-Control-Allow-Origin" "*" in
      let headers =
        Cohttp.Header.add headers "Access-Control-Allow-Methods" "POST" in
      let headers =
        Cohttp.Header.add headers "Access-Control-Allow-Headers" "Content-Type" in
      Cohttp_lwt_unix.Server.respond_string ~status:(`Code 200) ~headers ~body:"" ()
    (* Errors *)
    | `PUT, p | `GET, p | `DELETE, p ->
      let path = String.join "/" p in
      not_found (Printf.sprintf "Unknown resource %S" path)
    | _ ->
      fail (HttpError (501, "Method not implemented"))
