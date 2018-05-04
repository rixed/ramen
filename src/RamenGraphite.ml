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
open Cohttp
open Cohttp_lwt_unix
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
    (* These 3 are kind of synonymous ; aparently grafana uses only "expandable". *)
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
type 'a tree_enum = E of ('a * 'a tree_enum) Enum.t
let get (E x) = x

(* Fold over the tree. [node_init] will be carried from node to node while
 * [stack_init] will be popped when the iterator go forward in the tree.
 * Only return the value that's carried from node to node. *)
let rec tree_enum_fold f node_init stack_init te =
  Enum.fold (fun usr (n, te') ->
    let is_leaf = Enum.is_empty (get te') in
    let usr', stack' = f usr stack_init n is_leaf in
    tree_enum_fold f usr' stack' te') node_init (get te)

(* Given a tree enumerator and a list of filters, return a tree enumerator
 * of the entries matching the filter: *)
let rec filter_tree te = function
  | [] -> te
  | flt :: flts ->
      E (Enum.filter_map (fun (a, te') ->
        if flt a then Some (a, filter_tree te' flts)
        else None) (get te))

(* Given a func, returns the tree_enum of fields that are not factors *)
let tree_enum_of_fields func =
  E (List.enum func.F.out_type.ser //@
     (fun ft ->
       let n = ft.RamenTuple.typ_name in
       if List.mem n func.F.factors then None
       else Some (n, E (Enum.empty ()))))

(* Given a functions and a list of factors (any part of func.F.factors),
 * return the tree_enum of the factors: *)
let rec tree_enum_of_factors func = function
  | [] -> tree_enum_of_fields func
  | factor :: factors' ->
      E (RamenTimeseries.possible_values func factor /@
         (fun pv -> pv, tree_enum_of_factors func factors'))

(* Given a program, returns the tree_enum of its functions: *)
let tree_enum_of_program (program_name, get_rc) =
  let _bin, funcs = get_rc () in
  E (List.enum funcs /@
     (fun func ->
       func.F.name, tree_enum_of_factors func func.F.factors))

(* Given the programs hashtable, return a tree_enum of the
 * path components and programs. Merely build a hashtbl of hashtbls. *)
type program_tree_item = Prog of (string * (unit -> string * P.t))
                       | Hash of (string, program_tree_item) Hashtbl.t

let tree_enum_of_programs programs =
  let programs =
    Hashtbl.enum programs |>
    Array.of_enum in
  Array.fast_sort (fun (k1, _) (k2, _) -> String.compare k1 k2) programs ;
  let rec hash_for_prefix pref =
    let h = Hashtbl.create 11 in
    let pl = String.length pref in
    Array.iter (fun (program_name, get_rc as p) ->
      if String.starts_with program_name pref then
        let suf = String.lchop ~n:pl program_name in
        (* Get the next prefix *)
        match String.split suf ~by:"/" with
        | exception Not_found ->
            Hashtbl.add h suf (Prog p)
        | suf, rest ->
            (* If we've done it already, continue: *)
            if not (Hashtbl.mem h suf) then
              let pref' = pref ^ suf ^"/" in
              Hashtbl.add h suf (Hash (hash_for_prefix pref'))
      (* TODO: exit when we stop matching as the array is ordered *)
    ) programs ;
    h in
  let h = hash_for_prefix "" in
  let rec tree_enum_of_h h =
    E (Hashtbl.enum h /@
       (function name, Prog p -> name, tree_enum_of_program p
               | name, Hash h -> name, tree_enum_of_h h)) in
  tree_enum_of_h h

let filters_of_query query =
  List.map (fun q ->
    let glob = Globs.compile q in
    Globs.matches glob) query

let enum_tree_of_query conf query =
  !logger.debug "Getting programs..." ;
  let%lwt programs = C.with_rlock conf Lwt.return in
  !logger.debug "Caching factors possible values..." ;
  let%lwt () = RamenTimeseries.cache_possible_values conf programs in
  !logger.debug "Building tree..." ;
  let te = tree_enum_of_programs programs in
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
 * legitimately appear in a string or float value. We assume all strings
 * and floats will be double-quoted (TODO for floats). *)
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
(*$inject open Batteries *)
(*$= split_query & ~printer:(IO.to_string (List.print String.print))
 [ "monitoring"; "traffic"; "inbound"; "127.0.0.1:56687"; "0"; "bytes" ] \
    (split_query "monitoring.traffic.inbound.\"127.0.0.1:56687\".0.bytes")
 *)

(* Enumerate the (id, leaf, text, target) matching a query. Where:
 * id is the query with only the last part completed (used for autocompletion)
 * leaf is a flag set if that completed component has no further descendant,
 * text is the completed last component (same as id ending) and targets
 * is the list of fully expanded targets (as a query string) matching id. *)
let expand_query conf query =
  let query = split_query query in
  let%lwt filtered = enum_tree_of_query conf query in
  let nb_filters = List.length query in
  let prefix = (List.take (nb_filters - 1) query |>
                String.concat ".") ^ "." in
  tree_enum_fold (fun node_res (depth, target) n is_leaf ->
    (* depth starts at 0 *)
    let target' =
      if target = "" then n else target ^"."^ n in
    (if depth = nb_filters - 1 then
      (prefix ^ n, is_leaf, n, target') :: node_res
    else
      node_res),
    (depth + 1, target')
  ) [] (0, "") filtered |>
  List.enum |>
  return

let complete_graphite_find conf headers params =
  let%lwt expanded =
    Hashtbl.find_default params "query" "*" |>
    expand_query conf in
  let resp = (expanded |> Enum.uniq) /@
             metric_of_worker_path |>
             List.of_enum in
  let body = PPP.to_string graphite_metrics_ppp_json resp in
  respond_ok ~body ()

(*
 * Render a selected metric (in JSON only, no actual picture is
 * generated).
 *)

type graphite_render_metric =
  { target : string ;
    datapoints : (float option * int) array } [@@ppp PPP_JSON]
type graphite_render_resp = graphite_render_metric list [@@ppp PPP_JSON]

(* All the time conversion functions below are taken from (my understanding of)
 * http://graphite-api.readthedocs.io/en/latest/api.html#from-until *)

let time_of_reltime s =
  let scale d s =
    try
      Some (
        Unix.gettimeofday () +. d *.
          (match s with
          | "s" -> 1.
          | "min" -> 60.
          | "h" -> 3600.
          | "d" -> 86400.
          | "w" -> 7. *. 86400.
          | "mon" -> 30. *. 86400.
          | "y" -> 365. *. 86400.
          | _ -> raise Exit))
    with Exit ->
      None
  in
  Scanf.sscanf s "%f%s%!" scale

let time_of_abstime s =
  let s = String.lowercase s in
  let scan fmt recv =
    try Some (Scanf.sscanf s fmt recv)
    with Scanf.Scan_failure _ -> None
  and eq str recv =
    if s = str then Some (recv ()) else None
  and (|||) o1 o2 =
    if o1 <> None then o1 else o2 in
  let open Unix in
  let is_past h m tm =
    h < tm.tm_hour || h = tm.tm_hour && m < tm.tm_min in
  let time_of_hh_mm h m am_pm =
    let h = match String.lowercase am_pm with
      | "am" | "" -> h
      | "pm" -> h + 12
      | _ -> raise (Scanf.Scan_failure ("Invalid AM/PM: "^ am_pm)) in
    let now = time () in
    let tm = localtime now in
    (* "If that time is already past, the next day is assumed" *)
    if is_past h m tm then now +. 86400. else now in
  let time_of_dd_mm_yyyy d m y =
    let y = if y < 100 then y + 2000 (* ? *) else y in
    let tm =
      { tm_sec = 0 ; tm_min = 0 ; tm_hour = 0 ;
        tm_mday = d ; tm_mon = m - 1 ; tm_year = y - 1900 ;
        (* ignored: *) tm_wday = 0 ; tm_yday = 0 ; tm_isdst = false } in
    mktime tm |> fst
  in
  (* Extracts are from `man 1 at`:
   *
   * "It accepts times of the form HHMM or HH:MM to run a job at a specific
   * time of day.  (If that time is already past, the next day is assumed.)
   * (...) and time-of-day may be suffixed with AM or PM for running in the
   * morning or the evening." *)
  (scan "%2d%2d%s%!" time_of_hh_mm) |||
  (scan "%2d:%2d%s%!" time_of_hh_mm) |||
  (scan "%2d:%2d%s%!" time_of_hh_mm) |||
  (* "As an alternative, the following keywords may be specified: midnight,
   * noon, or teatime (4pm) (...)." *)
  (eq "midnight" (fun () -> time_of_hh_mm 0 0 "")) |||
  (eq "noon" (fun () -> time_of_hh_mm 12 00 "")) |||
  (eq "teatime" (* fuck you! *) (fun () -> time_of_hh_mm 16 00 "")) |||
  (* Not specified but that's actually the first Grafana will send: *)
  (eq "now" time) |||
  (* Also not specified but mere unix timestamps are actually frequent: *)
  (scan "%f%!" (fun f ->
    if f > 946681200. && f < 2208985200. then f
    else raise (Scanf.Scan_failure "Doesn't look like a timestamp"))) |||
  (* "The day on which the job is to be run may also be specified by giving a
   * date in the form month-name day with an optional year," *)
  (* TODO *)
  (* "or giving a date of the forms DD.MM.YYYY, DD.MM.YY, MM/DD/YYYY, MM/DD/YY,
   * MMDDYYYY, or MMDDYY." *)
  (scan "%2d.%2d.%4d%!" time_of_dd_mm_yyyy) |||
  (scan "%2d/%2d/%4d%!" (fun m d y -> time_of_dd_mm_yyyy d m y)) |||
  (scan "%2d%2d%4d%!" (fun m d y -> time_of_dd_mm_yyyy d m y)) |||
  (* "The specification of a date must follow the specification of the time of
   * day.  Time can also be specified as: [now] + count time-units, where the
   * time-units can be minutes, hours, days, weeks, months or years and at may
   * be told to run the job today by suffixing the time with today and to run
   * the job tomorrow by suffixing the time with tomorrow.  The shortcut next
   * can be used instead of + 1." *)
  (* TODO *)
  None

(* mktime tm struct "is interpreted in the local time zone". Work around this
 * by dividing by 24h. *)
(*$= time_of_abstime & ~printer:(function None -> "None" | Some f -> string_of_float f)
 (Some 2218.) (BatOption.map (fun ts -> ceil (ts /. 86400.)) (time_of_abstime "28.01.1976"))
 (time_of_abstime "28.01.1976") (time_of_abstime "01/28/1976")
 (Some 1523052000.) (time_of_abstime "1523052000")
 *)

let time_of_graphite_time s =
  let len = String.length s in
  if len = 0 then None
  else if s.[0] = '-' then time_of_reltime s
  else time_of_abstime s

(* Return the target name of that function, given the where filter and
 * used factors. [fvals] are the scalar values to use for the factored
 * fields (fields present in [factors], in same order): *)
let target_name_of func where factors fvals data_field =
  (* Return the name of field for the [i]th factor of the function,
   * ie the fvals if this field has been used as factor, or the
   * value from the where filter otherwise: *)
  let print_factor oc factor =
    (match List.findi (fun i f -> factor = f) factors with
    | exception Not_found -> (* take the value from the where filter *)
        List.assoc factor where
    | i, _ -> (* take the [i]th value *)
        List.nth fvals i) |>
    RamenScalar.print oc
  in
  Printf.sprintf2 "%s%s%s%s%a%s"
    (String.nreplace ~str:func.F.program_name ~sub:"/" ~by:".")
    (if func.program_name <> "" then "." else "")
    func.name
    (if func.factors = [] then "" else ".")
    (List.print ~first:"" ~last:"." ~sep:"." print_factor) func.factors
    data_field
(* TODO: tests *)

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
  let%lwt targets =
    Lwt_list.map_s (expand_query conf) targets in
  (* Targets is now a list of enumerations of expanded program + function name
   * + factors values + field name, all separated by dots. Regardless of how
   * many original query were sent, the expected answer is a flat array of
   * target name + timeseries.  We can therefore flatten all the expanded
   * targets and regroup by operation + where filter, then scan data for each
   * of those groups asking for all possible factors. *)
  let targets =
    (List.enum targets |> Enum.flatten) /@
    (fun (_, _, _, target) -> target) |>
    List.of_enum in
  !logger.debug "targets = %a" (List.print String.print) targets ;
  (* Instead of those strings we'd like to have the target decomposed into a
   * func, function FQ name, the factors values, and the data field: *)
  let%lwt targets =
    C.with_rlock conf (fun programs ->
      Lwt_list.map_s (fun target ->
        (* FIXME: handle "#stats" and "stats" *)
        let rec search_func pref = function
          | [] | [_] -> bad_request ("bad target: "^ target)
          | pn :: (fn :: fields as rest) ->
              let prog_name =
                if pref = "" then pn else pref ^"/"^ pn in
              (* Beware that first program is not necessarily the
               * intended one! *)
              match Hashtbl.find programs prog_name with
              | exception Not_found -> search_func prog_name rest
              | get_rc ->
                  let _bin, funcs = get_rc () in
                  (match List.find (fun f -> f.F.name = fn) funcs with
                  | exception Not_found -> search_func prog_name rest
                  | func ->
                      let nb_fields = List.length fields in
                      let nb_factors = List.length func.factors in
                      if nb_fields = nb_factors + 1 then (
                        let factors, data_field =
                          List.split_at (nb_fields - 1) fields in
                        let data_field = List.hd data_field in
                        let full_name = prog_name ^"/"^ fn in
                        return (func, full_name, factors, data_field)
                      ) else (
                        !logger.debug "bad number of fields for %s/%s: \
                                       fields=%a but have %d factors"
                          prog_name fn
                          (List.print String.print) fields nb_factors ;
                        search_func prog_name rest
                      ))
        in
        search_func "" (split_query target)
      ) targets) in
  (* Now we need to decide, for each factor value, if we want it in a where
   * filter (it's the only value we want for this factor) or if we want to
   * get a timeseries for all possible values (in a single scan). For this
   * we merely count how many distinct values we are asking for: *)
  let factor_values = Hashtbl.create 9 in
  let count_factor_values (func, func_name, factors, data_field) =
    (* Target is actually the program + function name + factors values +
     * field name, all separated by dots. We need the fully qualified name
     * of the function, the factor values and the field name: *)
    List.iteri (fun i fval ->
      Hashtbl.add factor_values (func_name, i) fval
    ) factors ;
    return_unit in
  let%lwt () = Lwt_list.iter_s count_factor_values targets in
  (* Now we can decide on which scans to perform *)
  let scans = Hashtbl.create 9 in
  let add_scans (func, func_name, fvals, data_field) =
    let where, factors, _ =
      List.fold_left2 (fun (where, factors, i) factor fval ->
        if Hashtbl.find_all factor_values (func_name, i) |> List.length > 1
        then
          (* We want several values for that factor, so we will take it as a
           * factor: *)
          where, Set.add factor factors, i + 1
        else
          (* If we are interested in only one value, do not ask for this factor
           * but add a where filter: *)
          (* FIXME: we should have kept the scalar value in factors instead of
           * a string *)
          let ft =
            List.find (fun ft ->
              ft.RamenTuple.typ_name = factor
            ) func.F.out_type.ser in
          (factor,
           RamenScalar.value_of_string ft.RamenTuple.typ fval) :: where,
          factors, i + 1
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
        and target = target_name_of func where factors column data_field in
        { target ; datapoints } :: res
      ) res data_fields
    ) res columns |> return
  in
  let%lwt resp = Hashtbl.fold metrics_of_scan scans return_nil in
  let body = PPP.to_string graphite_render_resp_ppp_json resp in
  !logger.debug "%d metrics from %d scans" (List.length resp) (Hashtbl.length scans) ;
  respond_ok ~body ()

let version _conf _headers _params =
  respond_ok ~body:"1.1.3" ()

let router conf =
  (* The function called for each HTTP request: *)
  fun meth path params headers body ->
    match meth, path with
    (* Mimic Graphite for Grafana datasource *)
    | `GET, ["metrics"; "find"] ->
      complete_graphite_find conf headers params
    | `POST, ["render"] ->
      render_graphite conf headers body
    | `GET, ["version"] ->
      version conf headers params
    | `OPTIONS, _ ->
      let headers = Header.init_with "Access-Control-Allow-Origin" "*" in
      let headers =
        Header.add headers "Access-Control-Allow-Methods" "POST" in
      let headers =
        Header.add headers "Access-Control-Allow-Headers" "Content-Type" in
      Server.respond_string ~status:(`Code 200) ~headers ~body:"" ()
    (* Errors *)
    | `PUT, p | `GET, p | `DELETE, p ->
      let path = String.join "/" p in
      not_found ("Unknown resource "^ path)
    | _ ->
      fail (HttpError (501, "Method not implemented"))
