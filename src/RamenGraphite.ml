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

let find_components query all_comps =
  (* Break the query into components, and make each of them a glob: *)
  let query = String.nsplit ~by:"." query in
  let globs = List.map (fun q -> q, Globs.compile q) query in
  (all_comps //@ comp_matches globs)

(*$inject
  open Batteries
  let test_comps =
    [ [ "prog1"; "prog2"; "prog3"; "w1" ] ;
      [ "prog1"; "prog2"; "w5" ] ;
      [ "prog1"; "prog4"; "w6" ] ;
      [ "prog2"; "w8" ] ]
 *)
(*$= find_components & ~printer:dump
  [ "prog1.*.prog3", false,  "prog3", "prog1.prog2.prog3" ; \
    "prog1.*.w5",    true,   "w5",    "prog1.prog2.w5" ; \
    "prog1.*.w6",    true,   "w6",    "prog1.prog4.w6" ] \
    (find_components "prog1.*.*" (List.enum test_comps) |> List.of_enum)

  [ "prog1", false, "prog1", "prog1" ; \
    "prog1", false, "prog1", "prog1" ; \
    "prog1", false, "prog1", "prog1" ; \
    "prog2", false, "prog2", "prog2" ] \
    (find_components "*p*" (List.enum test_comps) |> List.of_enum)
 *)

(* Given the list of running programs, return the broken down list of
 * path components à la graphite: *)
let comps_of_programs programs =
  let comps_of = String.nsplit ~by:"/" in
  (* ((string, unit -> string * C.Program.t) Batteries.Hashtbl.t *)
  let programs =
    Hashtbl.enum programs |>
    Array.of_enum in
  Array.fast_sort (fun (k1, _) (k2,_) -> String.compare k1 k2) programs ;
  Array.enum programs /@
  (fun (program_name, get_rc) ->
    let _bin, program = get_rc () in
    List.enum program /@
    (fun func ->
      let fq_name = program_name ^"/"^ func.F.name in
      List.enum func.out_type.ser /@
      (fun ft ->
        comps_of (fq_name ^"/"^ ft.RamenTuple.typ_name))) |>
    Enum.flatten) |>
  Enum.flatten |>
  return

(* query is composed of:
 * several components separated by a dots. Each non last components
 * can be either a plain word or a star. The last component can also
 * be "*word*".
 * The answer must have the completed last component in name and
 * the query with the last component completed in id.
 * The idea is that id will then identify the timeseries we want to
 * display (once expanded) *)

(* Enumerate the (id, leaf, text, target) matching a query. Where:
 * id is the query with only the last part completed (used for autocompletion)
 * leaf is a flag set if that completed component has no further descendant,
 * text is the completed last component (same as id ending) and targets
 * is the list of fully expanded targets matching id. *)
let expand_query conf query =
  let%lwt all_comps =
    C.with_rlock conf comps_of_programs in
  find_components query all_comps |>
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

(*$= time_of_abstime & ~printer:(function None -> "None" | Some f -> string_of_float f)
 (Some 191631600.) (time_of_abstime "28.01.1976")
 (Some 191631600.) (time_of_abstime "01/28/1976")
 (Some 1523052000.) (time_of_abstime "1523052000")
 *)

let time_of_graphite_time s =
  let len = String.length s in
  if len = 0 then None
  else if s.[0] = '-' then time_of_reltime s
  else time_of_abstime s

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
  and where = [] (* TODO when we also have factors *)
  and max_data_points = Hashtbl.find_option params "maxDataPoints" |>
                        Option.map (int_of_string % v) |? 300
  and format = Hashtbl.find_option params "format" |>
               Option.map v |? "json" in
  assert (format = "json") ; (* FIXME *)
  let%lwt targets =
    Lwt_list.map_s (expand_query conf) targets in
  let targets =
    (List.enum targets |> Enum.flatten) /@
    (fun (_, _, _, target) -> target) |>
    List.of_enum in
  let metric_of_target target =
    (* Target is actually the program + function name + field name, all
     * separated by dots. We need the fully qualified name of the function
     * and the field name: *)
    let%lwt func_name, data_field =
      match String.rsplit ~by:"." target with
      | exception Not_found ->
          bad_request "bad target name"
      | func, field ->
          return (String.nreplace ~str:func ~sub:"." ~by:"/", field) in
    let%lwt columns, datapoints =
      RamenTimeseries.get conf max_data_points since until where []
                          func_name data_field in
    let datapoints =
      (* TODO: return all factor value combinations *)
      Enum.map (fun (t, v) -> v.(0), int_of_float t) datapoints |>
      Array.of_enum in
    return { target ; datapoints }
  in
  let%lwt resp = Lwt_list.map_p metric_of_target targets in
  let body = PPP.to_string graphite_render_resp_ppp_json resp in
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
