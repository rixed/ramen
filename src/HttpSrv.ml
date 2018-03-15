(* Start an HTTP(S) daemon to allow setting up the configuration graph. *)
open Cohttp
open Cohttp_lwt_unix
open Batteries
open BatOption.Infix
open Lwt
open RamenLog
open RamenSharedTypes
open Helpers
open RamenHttpHelpers
open Lang
module C = RamenConf
module N = RamenConf.Func
module L = RamenConf.Program
module SL = RamenSharedTypes.Info.Program
module SN = RamenSharedTypes.Info.Func

let hostname =
  let cached = ref "" in
  fun () ->
    if !cached <> "" then return !cached else
    let%lwt c =
      try%lwt
        run ~timeout:2. [| "hostname" |]
      with _ -> return "unknown host" in
    cached := c ;
    return c

let replace_placeholders ramen_url url_prefix s =
  let%lwt hostname = hostname () in
  let rep sub by str = String.nreplace ~str ~sub ~by in
  return (
    rep "$RAMEN_URL$" ramen_url s |>
    rep "$RAMEN_PATH_PREFIX$" url_prefix |>
    rep "$HOSTNAME$" hostname |>
    rep "$VERSION$" RamenVersions.release_tag)

let serve_string ramen_url url_prefix _headers body =
  let%lwt body = replace_placeholders ramen_url url_prefix body in
  respond_ok ~body ~ct:Consts.html_content_type ()

(*
    Returns the graph (as JSON, dot or mermaid representation)
*)

let dot_of_graph programs =
  let dot = IO.output_string () in
  Printf.fprintf dot "digraph g {\n" ;
  Printf.fprintf dot "\trankdir=LR\n" ;
  List.iter (fun program ->
    Hashtbl.iter (fun _ func ->
        Printf.fprintf dot "\t%S\n" (N.fq_name func)
      ) program.L.funcs
    ) programs ;
  Printf.fprintf dot "\n" ;
  List.iter (fun program ->
    Hashtbl.iter (fun _ func ->
        List.iter (fun (pl, pn) ->
            Printf.fprintf dot "\t%S -> %S\n"
              (pl ^"/"^ pn) (func.N.program ^"/"^ func.N.name)
          ) func.N.parents
      ) program.L.funcs
    ) programs ;
  Printf.fprintf dot "}\n" ;
  IO.close_out dot

let get_graph_dot _headers programs =
  let body = dot_of_graph programs in
  let status = `Code 200 in
  let headers = Header.init_with "Content-Type" Consts.dot_content_type in
  Server.respond_string ~headers ~status ~body ()

let mermaid_of_graph programs =
  (* Build unique identifier that are valid for mermaid: *)
  let is_alphanum c =
    Char.(is_letter c || is_digit c) in
  let mermaid_id n =
    "id_" ^ (* To make it start with an alpha *)
    String.replace_chars (fun c ->
      if is_alphanum c then String.of_char c
      else string_of_int (Char.code c)) n
  (* And valid labels *)
  and mermaid_label n =
    "\""^ String.nreplace n "\"" "#quot;" ^"\""
  in
  let txt = IO.output_string () in
  Printf.fprintf txt "graph LR\n" ;
  List.iter (fun program ->
    Hashtbl.iter (fun _ func ->
        Printf.fprintf txt "%s(%s)\n"
          (mermaid_id (N.fq_name func))
          (mermaid_label func.N.name)
      ) program.L.funcs
    ) programs ;
  Printf.fprintf txt "\n" ;
  List.iter (fun program ->
    Hashtbl.iter (fun _ func ->
        List.iter (fun (pl, pn) ->
            Printf.fprintf txt "\t%s-->%s\n"
              (mermaid_id (pl ^"/"^ pn))
              (mermaid_id (func.N.program ^"/"^ func.N.name))
          ) func.N.parents
      ) program.L.funcs
    ) programs ;
  IO.close_out txt

let get_graph_mermaid _headers programs =
  let body = mermaid_of_graph programs in
  let status = `Code 200 in
  let headers = Header.init_with "Content-Type" Consts.mermaid_content_type in
  let headers = Header.add headers "Access-Control-Allow-Origin" "*" in
  let headers = Header.add headers "Access-Control-Allow-Methods" "POST" in
  let headers = Header.add headers "Access-Control-Allow-Headers" "Content-Type" in
  Server.respond_string ~headers ~status ~body ()

let get_graph conf headers program_opt =
  let accept = get_accept headers in
  if is_accepting Consts.json_content_type accept then
    let%lwt graph = RamenOps.graph_info conf program_opt in
    let body = PPP.to_string get_graph_resp_ppp_json graph in
    respond_ok ~body ()
  else (
    (* For non-json we can release the lock sooner as we don't need the
     * children: *)
    let%lwt programs = C.with_rlock conf (fun programs ->
      RamenProcesses.graph_programs programs program_opt) in
    let programs = L.order programs in
    if is_accepting Consts.dot_content_type accept then
      get_graph_dot headers programs
    else if is_accepting Consts.mermaid_content_type accept then
      get_graph_mermaid headers programs
    else
      cant_accept accept)

(*
    Add/Remove programs

    Programs and funcs within a program are referred to via name that can be
    anything as long as they are unique.  So the clients decide on the name.
    The server ensure uniqueness by forbidding creation of a new programs by the
    same name as one that exists already.

*)

let find_func_or_fail programs program_name func_name =
  match C.find_func programs program_name func_name with
  | exception Not_found ->
    bad_request ("Function "^ program_name ^"/"^ func_name ^" does not exist")
  | program, _func as both ->
    RamenProcesses.use_program (Unix.gettimeofday ()) program ;
    return both

(* Caller must have a wlock on programs *)
let find_history_or_fail conf programs program_name func_name =
  let%lwt program, func =
    find_func_or_fail programs program_name func_name in
  (* We need the output type to find the history *)
  if not (L.is_typed program) then
    bad_request ("operation "^ func_name ^" is not typed (yet)")
  else (
    let%lwt history = RamenExport.get_or_start conf func in
    return (program, func, history))

let func_of_name programs program_name func_name =
  if func_name = "" then bad_request "Empty string is not a valid func name"
  else find_func_or_fail programs program_name func_name

let del_program conf _headers program_name =
  try%lwt
    let%lwt _ =
      RamenOps.del_program_by_name ~ok_if_running:false conf program_name in
    respond_ok ()
  with Not_found ->
    let e = "Program "^ program_name ^" does not exist" in
    bad_request e
  | e ->
    bad_request (Printexc.to_string e)

(* Returns info about a given operation *)
let op_info conf headers program_name func_name params =
  let bool_opt n =
    try Hashtbl.find params n |> bool_of_string |> return
    with Not_found -> return_true
       | e -> fail e in
  let%lwt with_stats = bool_opt "stats" in
  let%lwt with_code = bool_opt "code" in
  let%lwt () = check_accept headers Consts.json_content_type in
  let%lwt info =
    C.with_rlock conf (fun programs ->
      let%lwt _program, func =
        find_func_or_fail programs program_name func_name in
      RamenOps.func_info_of_func ~with_stats ~with_code programs func) in
  let body = PPP.to_string SN.info_ppp_json info in
  respond_ok ~body ()

let list_info conf headers program_opt =
  let%lwt resp =
    C.with_rlock conf (fun programs ->
      let get_info =
        RamenOps.func_info_of_func ~with_code:false ~with_stats:true programs in
      match program_opt with
      | None ->
        C.lwt_fold_funcs programs [] (fun lst _prog func ->
          let%lwt i = get_info func in
          return (i :: lst))
      | Some program_name ->
        match Hashtbl.find programs program_name with
        | exception Not_found ->
          bad_request ("Program "^ program_name ^" does not exist")
        | p ->
          Hashtbl.values p.L.funcs |> List.of_enum |>
          Lwt_list.fold_left_s (fun lst func ->
            let%lwt i = get_info func in
            return (i :: lst)
          ) []) in
  let body = PPP.to_string top_functions_resp_ppp_json resp in
  respond_ok ~body ()

(* FIXME: instead of stopping/starting for real we must build two sets of
 * programs to stop/start and perform with changing the processes only
 * when we are about to save the new configuration. *)

let put_program conf headers body =
  let%lwt msg =
    of_json headers "Uploading program" put_program_req_ppp_json body in
  try%lwt
    let%lwt program_name =
      RamenOps.set_program
        conf ~ok_if_running:msg.ok_if_running ~start:msg.start
        msg.name msg.program in
    let body =
      PPP.to_string put_program_resp_ppp_json
        { success = true ; program_name = Some program_name } in
    respond_ok ~body ()
  with exn ->
    bad_request (Printexc.to_string exn)

(*
    Serving normal files
*)

let serve_file_with_replacements ramen_url url_prefix _headers path file =
  serve_file path file (replace_placeholders ramen_url url_prefix)

let get_index www_dir ramen_url url_prefix headers =
  if www_dir = "" then
    serve_string ramen_url url_prefix headers RamenGui.without_link
  else
    serve_string ramen_url url_prefix headers RamenGui.with_links

(*
    Whole graph operations: compile/run/stop
*)

let compile conf headers program_opt =
  let%lwt program_names =
    C.with_rlock conf (fun programs ->
      let%lwt programs = RamenProcesses.graph_programs programs program_opt in
      List.map (fun p -> p.L.name) programs |>
      return) in
  let%lwt failures = RamenOps.compile_programs conf program_names in
  if failures = [] then
    switch_accepted headers [
      Consts.json_content_type, (fun () -> respond_ok ()) ]
  else
    let first_e, _ = List.hd failures in
    bad_request (Printexc.to_string first_e)

let run conf headers program_opt =
  try%lwt
    let%lwt () =
      C.with_wlock conf (fun programs ->
        let%lwt to_run = RamenProcesses.graph_programs programs program_opt in
        let to_run = L.order to_run in
        Lwt_list.iter_s (RamenProcesses.run conf programs) to_run) in
    switch_accepted headers [
      Consts.json_content_type, (fun () -> respond_ok ()) ]
  with SyntaxError _
     | Compiler.SyntaxErrorInFunc _
     | C.InvalidCommand _ as e ->
       bad_request (Printexc.to_string e)
     | x -> fail x

let stop_programs conf program_opt =
  C.with_wlock conf (fun programs  ->
    RamenProcesses.stop_programs conf programs program_opt)

let stop conf headers program_opt =
  try%lwt
    let%lwt () = stop_programs conf program_opt in
    switch_accepted headers [
      Consts.json_content_type, (fun () -> respond_ok ()) ]
  with C.InvalidCommand e -> bad_request e
     | x -> fail x

let shutdown _conf _headers =
  (* TODO: also log client info *)
  !logger.info "Asked to shut down" ;
  (* Hopefully cohttp will serve this answer before stopping. *)
  RamenProcesses.quit := true ;
  respond_ok ()

(*
    Exporting tuples
*)

let get_tuples ?since ?max_res ?(wait_up_to=0.)
               conf programs program_name func_name =
  (* Check that the func exists and exports *)
  let%lwt _program, _func, history =
    find_history_or_fail conf programs program_name func_name in
  let open RamenExport in
  let start = Unix.gettimeofday () in
  let get_values () =
    (* If since is < 0 here it means to take the last N tuples. *)
    let since =
      Option.map (fun s ->
        if s >= 0 then s
        else history.block_start + history.count + s
      ) since in
    let first, nb_values, values =
      fold_tuples_since
        ?since ?max_res history (None, 0, [])
          (fun _ seqnum tup (first, nbv, prev) ->
            let first =
              if first = None then Some seqnum else first in
            first, nbv+1, List.cons tup prev) in
    (* when is first None here? *)
    let first = first |? (since |? 0) in
    first, nb_values, values, Some history in
  let first, nb_values, values, history = get_values () in
  let dt = Unix.gettimeofday () -. start in
  if values = [] && dt < wait_up_to then (
    (* We cannot sleep with the lock so we fail with_r_lock and
     * ask it to retry us. That's ok because we haven't performed
     * any work yet. *)
    fail (C.RetryLater 0.5)
  ) else (
    !logger.debug "Exporting %d tuples" nb_values ;
    return (
      match history with
      | None -> 0, []
      | Some history ->
        first,
        export_columns_of_tuples
          history.ser_tuple_type values |>
        List.fast_sort (reorder_columns_to_user history) |>
        List.map (fun (typ, nullmask, column) ->
          typ, Option.map RamenBitmask.to_bools nullmask, column)))

let export conf headers program_name func_name body =
  let%lwt () = check_accept headers Consts.json_content_type in
  let%lwt req =
    if body = "" then return empty_export_req else
    of_json headers ("Exporting from "^ func_name) export_req_ppp_json body in
  let%lwt first, columns =
    C.with_rlock conf (fun programs ->
      get_tuples ?since:req.since ?max_res:req.max_results conf
        ~wait_up_to:req.wait_up_to programs program_name func_name) in
  let resp = { first ; columns } in
  let body = PPP.to_string export_resp_ppp_json resp in
  respond_ok ~body ()

(*
    Grafana Datasource: autocompletion of func/field names
*)

let complete_funcs conf headers body =
  let%lwt msg =
    of_json headers "Complete tables" complete_func_req_ppp_json body in
  let%lwt lst =
    C.with_rlock conf (fun programs ->
      C.complete_func_name programs msg.prefix |>
      return) in
  let body =
    PPP.to_string complete_resp_ppp_json lst
  in
  respond_ok ~body ()

let complete_fields conf headers body =
  let%lwt msg =
    of_json headers "Complete fields" complete_field_req_ppp_json body in
  let%lwt lst =
    C.with_rlock conf (fun programs ->
      C.complete_field_name programs msg.operation msg.prefix |>
      return) in
  let body =
    PPP.to_string complete_resp_ppp_json lst
  in
  respond_ok ~body ()

(*
    Grafana Datasource: data queries
*)

let timeseries conf headers body =
  let%lwt msg =
    of_json headers "time series query" timeseries_req_ppp_json body in
  let ts_of_func_field programs req program func data_field =
    let%lwt _program, func, history =
      find_history_or_fail conf programs program func in
    let open RamenExport in
    let consolidation =
      match String.lowercase req.consolidation with
      | "min" -> bucket_min | "max" -> bucket_max | _ -> bucket_avg in
    wrap (fun () ->
      try
        build_timeseries func history data_field msg.max_data_points
                         msg.since msg.until consolidation
      with FuncHasNoEventTimeInfo _ as e ->
        bad_request_exn (Printexc.to_string e))
  and create_temporary_func programs select_x select_y from where =
    (* First, we need to find out the name for this operation, and create it if
     * it does not exist yet. Name must be given by the operation and parent, so
     * that we do not create new funcs when not required (avoiding a costly
     * compilation and losing export history). This is not equivalent to the
     * signature: the signature identifies operation and types (aka the binary)
     * but not the data (since the same worker can be placed at several places
     * in the graph); while here we want to identify the data, that depends on
     * everything the user sent (aka operation text and parent name) but for the
     * formatting. We thus start by parsing and pretty-printing the operation: *)
    let%lwt parent_program, parent_name =
      try C.program_func_of_user_string from |> return
      with Not_found -> bad_request ("operation "^ from ^" does not exist") in
    let%lwt _program, parent =
      func_of_name programs parent_program parent_name in
    (* FIXME: this should be a program directly, so that we could get rid of parse_operation *)
    let%lwt op_text =
      if select_x = "" then (
        let open RamenOperation in
        match parent.N.operation with
        | Aggregate { event_time = Some ((start, scale), DurationConst dur) ; _ } ->
          Printf.sprintf
            "SELECT %s, %s AS data \
             FROM '%s' \
             EXPORT EVENT STARTING AT %s * %g WITH DURATION %g"
            start select_y
            parent.N.name
            start scale dur |> return
        | Aggregate { event_time = Some ((start, scale), DurationField (dur, scale2)) ; _ } ->
          Printf.sprintf
            "SELECT %s, %s AS data \
             FROM '%s' \
             EXPORT EVENT STARTING AT %s * %g WITH DURATION %s * %g"
            start select_y
            parent.N.name
            start scale dur scale2 |> return
        | Aggregate { event_time = Some ((start, scale), StopField (stop, scale2)) ; _ } ->
          Printf.sprintf
            "SELECT %s, %s, %s AS data \
             FROM '%s' \
             EXPORT EVENT STARTING AT %s * %g AND STOPPING AT %s * %g"
            start stop select_y
            parent.N.name
            start scale stop scale2 |> return
        | _ ->
          bad_request "This parent does not provide time information"
      ) else return (
        "SELECT "^ select_x ^" AS time, "
                 ^ select_y ^" AS data \
         FROM '"^ parent.N.name ^"' \
         EXPORT EVENT STARTING AT time") in
    let op_text =
      if where = "" then op_text else op_text ^" WHERE "^ where in
    let%lwt operation = wrap (fun () -> C.parse_operation op_text) in
    let reformatted_op = IO.to_string RamenOperation.print operation in
    let program_name = "temp/timeseries/"^ md5 reformatted_op
    and func_name = "operation" in
    (* So far so good. In all likelihood this program exists already: *)
    if Hashtbl.mem programs program_name then (
      !logger.debug "Program %S already there" program_name ;
      return (program_name, func_name, "data")
    ) else (
      (* Add this program to the running configuration: *)
      let%lwt funcs = wrap (fun () -> C.parse_program op_text) in
      C.make_program ~timeout:300. programs program_name op_text funcs |>
      ignore ;
      return (program_name, func_name, "data")
    )
  in
  try%lwt
    let%lwt resp = Lwt_list.map_s (fun req ->
        let%lwt program_name, func_name, data_field =
          match req.spec with
          | Predefined { operation ; data_field } ->
            let%lwt program, func =
              try C.program_func_of_user_string operation |> return
              with Not_found -> bad_request ("operation "^ operation ^" does not exist") in
            return (program, func, data_field)
          | NewTempFunc { select_x ; select_y ; from ; where } ->
            let%lwt (program_name, _func_name, _data_field as res) =
              C.with_wlock conf (fun programs ->
                create_temporary_func programs select_x select_y from where) in
            let%lwt () = RamenOps.compile_one conf program_name in
            let%lwt () = RamenOps.start_program_by_name conf program_name in
            return res in
        let%lwt times, values =
          C.with_rlock conf (fun programs ->
            ts_of_func_field programs req program_name func_name data_field) in
        return { id = req.id ; times ; values }
      ) msg.timeseries in
    let body = PPP.to_string timeseries_resp_ppp_json resp in
    respond_ok ~body ()
  with Failure err -> bad_request err
     | e -> fail e

let timerange_of_func func history =
  let open RamenSharedTypesJS in
  match RamenExport.hist_min_max history with
  | None -> NoData
  | Some (sta, sto) ->
    let oldest, latest =
      RamenExport.timerange_of_filenum func history sta in
    let latest =
      if sta = sto then latest
      else snd (RamenExport.timerange_of_filenum func history sto) in
    TimeRange (oldest, latest)

let get_timerange conf headers program_name func_name =
  let%lwt resp =
    C.with_rlock conf (fun programs ->
      let%lwt _program, func, history =
        find_history_or_fail conf programs program_name func_name in
      try return (timerange_of_func func history)
      with RamenExport.FuncHasNoEventTimeInfo _ as e ->
        bad_request (Printexc.to_string e)) in
  switch_accepted headers [
    Consts.json_content_type, (fun () ->
      let body = PPP.to_string time_range_resp_ppp_json resp in
      respond_ok ~body ()) ]

(*
   Obtaining an SVG plot for an exporting func
*)

let plot conf _headers program_name func_name params =
  (* Get all the parameters: *)
  let get ?def name conv =
    let lwt_conv s =
      try return (conv s)
      with Failure s -> bad_request ("Parameter "^ name ^": "^ s) in
    match Hashtbl.find params name with
    | exception Not_found ->
      (match def with
      | None -> bad_request ("Parameter "^ name ^" is mandatory")
      | Some def -> lwt_conv def)
    | x -> lwt_conv x in
  let to_relto s =
    match String.lowercase s with
    | "metric" -> true
    | "wallclock" -> false
    | _ -> failwith "rel must be 'metric' or 'wallclock'"
  and to_stacked s =
    let open RamenChart in
    match String.lowercase s with
    | "no" | "not" -> NotStacked
    | "yes" -> Stacked
    | "centered" -> StackedCentered
    | _ -> failwith "must be 'yes', 'no' or 'centered'"
  and to_float s =
    try float_of_string s
    with _ -> failwith "must be a float"
  and to_bool s =
    try bool_of_string s
    with _ -> failwith "must be 'false' or 'true'"
  in
  let%lwt fields = get "fields" identity in
  let fields = String.split_on_char ',' fields in
  let%lwt svg_width = get "width" ~def:"800" to_float in
  let%lwt svg_height = get "height" ~def:"400" to_float in
  let%lwt rel_to_metric = get "relto" ~def:"metric" to_relto in
  let%lwt duration = get "duration" ~def:"10800" to_float in
  let%lwt force_zero = get "force_zero" ~def:"false" to_bool in
  let%lwt stacked = get "stacked" ~def:"no" to_stacked in
  let%lwt single_field =
    match fields with
    | [] -> bad_request "You must supply at least one field"
    | [_] -> return_true
    | _ -> return_false in
  (* Fetch timeseries: *)
  let%lwt _program, func, history =
    C.with_rlock conf (fun programs ->
      find_history_or_fail conf programs program_name func_name) in
  let now = Unix.gettimeofday () in
  let%lwt until =
    if rel_to_metric then
      match timerange_of_func func history with
      | exception (RamenExport.FuncHasNoEventTimeInfo _ as e) ->
        bad_request (Printexc.to_string e)
      | RamenSharedTypesJS.NoData -> return now
      | RamenSharedTypesJS.TimeRange (_oldest, latest) -> return latest
    else return now in
  let since = until -. duration in
  let pen_of_field field_name =
    RamenChart.{
      label = field_name ; draw_line = true ; draw_points = true ;
      color = RamenColor.random_of_string field_name ;
      stroke_width = 1.5 ; opacity = 1. ;
      dasharray = None ; filled = true ; fill_opacity = 0.3 } in
  let%lwt data_points =
    wrap (fun () ->
      try
        List.map (fun data_field ->
            pen_of_field data_field,
            RamenExport.(
              build_timeseries func history data_field
                               (int_of_float svg_width + 1)
                               since until bucket_avg)
          ) fields
      with RamenExport.FuncHasNoEventTimeInfo _ as e ->
        bad_request_exn (Printexc.to_string e)) in
  let _fst_pen, (fst_times, _fst_data) = List.hd data_points in
  let nb_pts = Array.length fst_times in
  let shash = RamenChart.{
    create = (fun () -> Hashtbl.create 11) ;
    find = Hashtbl.find_option ;
    add = Hashtbl.add } in
  let open RamenHtml in
  let html =
    if nb_pts = 0 then svg svg_width svg_height [] [ svgtext "No data" ]
    else
    let vx_start = fst_times.(0) and vx_stop = fst_times.(nb_pts-1) in
    let fold = RamenChart.{ fold = fun f i ->
      List.fold_left (fun i (pen, (_times, data)) ->
        (* FIXME: xy_plout should handle None itself *)
        let getter j = data.(j) |? 0. in
        f i pen true getter) i data_points } in
    RamenChart.xy_plot
      ~svg_width ~svg_height ~force_show_0:force_zero ~stacked_y1:stacked
      ~string_of_x:RamenFormats.((timestamp string_of_timestamp).to_label)
      "time" (if single_field then List.hd fields else "")
      vx_start vx_stop nb_pts shash fold in
  let body = string_of_html html in
  let headers = Header.init_with "Content-Type" Consts.svg_content_type in
  let status = `Code 200 in
  Server.respond_string ~headers ~status ~body ()

(*
    Data Upload
    Data is then written to tmp_input_dirname/uploads/suffix/$random
*)

let save_in_tmp_file dir body =
  mkdir_all dir ;
  let fname = random_string 10 in
  let path = dir ^"/"^ random_string 10 in
  Lwt_io.(with_file Output path (fun oc ->
    let%lwt () = write oc body in
    return (path, fname)))

let upload conf headers program func body =
  let%lwt _program, func =
    C.with_rlock conf (fun programs ->
      find_func_or_fail programs program func) in
  (* Look for the func handling this suffix: *)
  match func.N.operation with
  | ReadCSVFile { where = ReceiveFile ; _ } ->
    let dir = C.upload_dir_of_func conf.C.persist_dir func in
    let ct = get_content_type headers |> String.lowercase in
    let content =
      if ct = Consts.urlencoded_content_type then Uri.pct_decode body
      else if ct = Consts.text_content_type then body
      else (
        !logger.info "Don't know how to convert content type '%s' into \
                      CSV, trying no conversion." ct ;
        body) in
    let%lwt path, fname = save_in_tmp_file dir content in
    Lwt_unix.rename path (dir ^"/_"^ fname) >>=
    respond_ok
  | _ ->
    bad_request ("Function "^ N.fq_name func ^" does not accept uploads")

(* Start the HTTP server: *)
let router conf www_dir url_prefix =
  let lyr_opt = function
    | [] -> None
    | lst -> Some (String.concat "/" lst) in
  let lyr lst = match lyr_opt lst with
    | None -> bad_request_exn "Program name missing from URL"
    | Some lst -> lst in
  let lyr_func_of path =
    let rec loop ls = function
      | [] -> bad_request_exn "operation name missing from URL"
      | [x] ->
        if ls = [] then bad_request_exn "operation name missing from URL"
        else lyr (List.rev ls), x
      | l::rest ->
        loop (l :: ls) rest in
    loop [] path in
  let alert_id_of_string s =
    match int_of_string s with
    | exception Failure _ ->
        bad_request "alert id must be numeric"
    | id -> return id in
  (* The function called for each HTTP request: *)
  fun meth path params headers body ->
    match meth, path with
    (* Ramen API *)
    | `GET, ("graph" :: programs) ->
      get_graph conf headers (lyr_opt programs)
    | `PUT, ["graph"] ->
      put_program conf headers body
    | `DELETE, ("graph" :: programs) ->
      del_program conf headers (lyr programs)
    | `GET, ("operation" :: path) ->
      let program, func = lyr_func_of path in
      op_info conf headers program func params
    | `GET, ("list" :: programs) ->
      list_info conf headers (lyr_opt programs)
    | `GET, ("compile" :: programs) ->
      compile conf headers (lyr_opt programs)
    | `GET, (("run" | "start") :: programs) ->
      run conf headers (lyr_opt programs)
    | `GET, ("stop" :: programs) ->
      stop conf headers (lyr_opt programs)
    | `GET, ["shutdown"] ->
      shutdown conf headers
    | (`GET|`POST), ("export" :: path) ->
      let program, func = lyr_func_of path in
      (* We must allow both POST and GET for that one since we have an
       * optional body (and some client won't send a body with a GET) *)
      export conf headers program func body
    | `GET, ("plot" :: path) ->
      let program, func = lyr_func_of path in
      plot conf headers program func params
    (* Grafana datasource plugin *)
    | `GET, ["grafana"] ->
      respond_ok ()
    | `POST, ["complete"; "operations"] ->
      complete_funcs conf headers body
    | `POST, ["complete"; "fields"] ->
      complete_fields conf headers body
    | `POST, ["timeseries"] ->
      timeseries conf headers body
    | `GET, ("timerange" :: path) ->
      let program, func = lyr_func_of path in
      get_timerange conf headers program func
    | `OPTIONS, _ ->
      let headers = Header.init_with "Access-Control-Allow-Origin" "*" in
      let headers =
        Header.add headers "Access-Control-Allow-Methods" "POST" in
      let headers =
        Header.add headers "Access-Control-Allow-Headers" "Content-Type" in
      Server.respond_string ~status:(`Code 200) ~headers ~body:"" ()
    (* Uploads of data files *)
    | (`POST|`PUT), ("upload" :: path) ->
      let program, func = lyr_func_of path in
      upload conf headers program func body
    (* Alerter API *)
    | `GET, ["notify"] ->
      RamenAlerter.Api.notify conf params
    | `GET, ["teams"] ->
      RamenAlerter.Api.get_teams conf
    | `PUT, ["team"; name] ->
      RamenAlerter.Api.new_team conf headers name
    | `DELETE, ["team"; name] ->
      RamenAlerter.Api.del_team conf headers name
    | `GET, ["oncaller"; name] ->
      RamenAlerter.Api.get_oncaller conf headers name
    | `GET, ["ongoing"] ->
      RamenAlerter.Api.get_ongoing conf None
    | `GET, ["ongoing"; team] ->
      RamenAlerter.Api.get_ongoing conf (Some team)
    | (`POST|`PUT), ["history"] ->
      RamenAlerter.Api.get_history_post conf headers body
    | `GET, ("history" :: team) ->
      let team = if team = [] then None else Some (List.hd team) in
      RamenAlerter.Api.get_history_get conf headers team params
    | `GET, ["ack" ; id] ->
      let%lwt id = alert_id_of_string id in
      RamenAlerter.Api.get_ack conf id
    | `GET, ["extinguish" ; id] ->
      let%lwt id = alert_id_of_string id in
      (* TODO: a parameter for reason *)
      RamenAlerter.Api.get_stop conf id
    | `POST, ["extinguish"] ->
      RamenAlerter.Api.post_stop conf headers body
    | `POST, ["inhibit"; "add"; team] ->
      RamenAlerter.Api.add_inhibit conf headers team body
    | `POST, ["inhibit"; "edit" ; team] ->
      RamenAlerter.Api.edit_inhibit conf headers team body
    | `GET, ["stfu" ; team] ->
      RamenAlerter.Api.stfu conf headers team
    | `POST, ["oncaller"; name] ->
      RamenAlerter.Api.edit_oncaller conf headers name body
    | `POST, ["members"; name] ->
      RamenAlerter.Api.edit_members conf headers name body
    | `GET, ["set_default_team"; name] ->
      RamenAlerter.Api.set_default_team conf headers name
    | `GET, ["alerting"; "configuration"] ->
      RamenAlerter.Api.export_static_conf conf
    | `PUT, ["alerting"; "configuration"] ->
      RamenAlerter.Api.put_static_conf conf headers body
      (* Same as above, but for multipart form-data: *)
    | `POST, ["alerting"; "configuration"] ->
      let%lwt () =
        RamenAlerter.Api.upload_static_conf conf headers body in
      switch_accepted headers [
        Consts.html_content_type, (fun () ->
          get_index www_dir conf.C.ramen_url url_prefix headers) ;
        Consts.json_content_type, (fun () -> respond_ok ()) ]
    (* Web UI *)
    | `GET, ([]|["index.html"]) ->
      get_index www_dir conf.C.ramen_url url_prefix headers
    | `GET, [ "style.css" | "ramen_script.js" as file ] ->
      serve_file_with_replacements conf.C.ramen_url url_prefix
                                   headers www_dir file
    (* Errors *)
    | `PUT, p | `GET, p | `DELETE, p ->
      let path = String.join "/" p in
      fail (HttpError (404, "Unknown resource "^ path))
    | _ ->
      fail (HttpError (405, "Method not implemented"))
