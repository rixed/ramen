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

let replace_placeholders conf s =
  let%lwt hostname = hostname () in
  let rep sub by str = String.nreplace ~str ~sub ~by in
  return (
    rep "$RAMEN_URL$" conf.C.ramen_url s |>
    rep "$HOSTNAME$" hostname |>
    rep "$VERSION$" RamenVersions.release_tag)

let serve_string conf _headers body =
  let%lwt body = replace_placeholders conf body in
  respond_ok ~body ~ct:Consts.html_content_type ()

(*
    Returns the graph (as JSON, dot or mermaid representation)
*)

let rec find_int_opt_metric metrics name =
  let open Binocle in
  match metrics with
  | [] -> None
  | { measure = MInt n ; _ } as m ::_ when m.name = name -> Some n
  | _::rest -> find_int_opt_metric rest name

let find_int_metric metrics name = find_int_opt_metric metrics name |? 0

let rec find_float_metric metrics name =
  let open Binocle in
  match metrics with
  | [] -> 0.
  | { measure = MFloat n ; _ } as m ::_ when m.name = name -> n
  | _::rest -> find_float_metric rest name

let func_info_of_func programs func =
  let%lwt stats = RamenProcesses.last_report (N.fq_name func) in
  let%lwt exporting = RamenExport.is_func_exporting func in
  return SN.{
    definition = {
      name = func.N.name ;
      operation = IO.to_string Operation.print func.N.operation } ;
    exporting ;
    signature = if func.N.signature = "" then None else Some func.N.signature ;
    pid = func.N.pid ;
    input_type = C.info_of_tuple_type func.N.in_type ;
    output_type = C.info_of_tuple_type func.N.out_type ;
    parents = List.map (fun (l, n) -> l ^"/"^ n) func.N.parents ;
    children = C.fold_funcs programs [] (fun children _l n ->
      N.fq_name n :: children) ;
    stats }

let program_info_of_program programs program =
  let%lwt operations =
    Hashtbl.values program.L.funcs |>
    List.of_enum |>
    Lwt_list.map_s (func_info_of_func programs) in
  return SL.{
    name = program.L.name ;
    program = program.L.program ;
    operations ;
    status = program.L.status ;
    last_started = program.L.last_started ;
    last_stopped = program.L.last_stopped }

let graph_programs programs = function
  | None ->
    Hashtbl.values programs |>
    List.of_enum |>
    return
  | Some l ->
    try Hashtbl.find programs l |>
        List.singleton |>
        return
    with Not_found -> bad_request ("Unknown program "^l)

let dot_of_graph programs =
  let dot = IO.output_string () in
  Printf.fprintf dot "digraph g {\n" ;
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
    let%lwt graph = C.with_rlock conf (fun programs ->
      let%lwt programs' = graph_programs programs program_opt in
      let programs' = L.order programs' in
      Lwt_list.map_s (program_info_of_program programs) programs') in
    let body = PPP.to_string get_graph_resp_ppp graph in
    respond_ok ~body ()
  else (
    (* For non-json we can release the lock sooner as we don't need the
     * children: *)
    let%lwt programs = C.with_rlock conf (fun programs ->
      graph_programs programs program_opt) in
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
    bad_request ("func "^ func_name ^" is not typed (yet)")
  else (
    let%lwt history = RamenExport.get_or_start conf func in
    return (program, func, history))

let func_of_name programs program_name func_name =
  if func_name = "" then bad_request "Empty string is not a valid func name"
  else find_func_or_fail programs program_name func_name

let del_program_ programs program =
  if program.L.status = Running then
    bad_request "Cannot delete a running program"
  else
    try
      C.del_program programs program ;
      return_unit
    with C.InvalidCommand e -> bad_request e

let del_program conf _headers program_name =
  C.with_wlock conf (fun programs  ->
    match Hashtbl.find programs program_name with
    | exception Not_found ->
      let e = "Program "^ program_name ^" does not exist" in
      bad_request e
    | program ->
      del_program_ programs program) >>=
  respond_ok

(* FIXME: instead of stopping/starting for real we must build two sets of
 * programs to stop/start and perform with changing the processes only
 * when we are about to save the new configuration. *)

let run_ conf programs program =
  try%lwt RamenProcesses.run conf programs program
  with RamenProcesses.AlreadyRunning -> return_unit

let run_by_name conf to_run =
  C.with_wlock conf (fun programs ->
    match Hashtbl.find programs to_run with
    | exception Not_found ->
        (* Cannot be an error since we are not passed programs. *)
        !logger.warning "Cannot run unknown program %s" to_run ;
        return_unit
    | p ->
        run_ conf programs p)

(* Compile one program and stop those that depended on it. *)
let compile_ conf program_name =
  (* Compilation taking a long time, we do a two phase approach:
   * First we take the wlock, read the programs and put their status
   * to compiling, and release the wlock. Then we compile everything
   * at our own peace. Then we take the wlock again, check that the
   * status is still compiling, and store the result of the
   * compilation. *)
  !logger.debug "Compiling phase 1: copy the program info" ;
  match%lwt
    C.with_wlock conf (fun programs ->
      match Hashtbl.find programs program_name with
      | exception Not_found ->
          !logger.warning "Cannot compile unknown node %s" program_name ;
          (* Cannot be a hard error since caller had no lock *)
          return_none
      | to_compile ->
          (match to_compile.L.status with
          | Edition _ ->
              (match
                Hashtbl.map (fun _func_name func ->
                  List.map (fun (par_prog, par_name) ->
                    match C.find_func programs par_prog par_name with
                    | exception Not_found ->
                      let e = UnknownFunc (par_prog ^"/"^ par_name) in
                      raise (Compiler.SyntaxErrorInFunc (func.N.name, e))
                    | _, par_func -> par_func
                  ) func.N.parents
                ) to_compile.L.funcs with
              | exception exn ->
                  L.set_status to_compile (Edition (Printexc.to_string exn)) ;
                  return_none
              | parents ->
                  L.set_status to_compile Compiling ;
                  return (Some (to_compile, parents)))
          | _ -> return_none)) with
  | None -> (* Nothing to compile *) return_unit
  | Some (to_compile, parents) ->
    (* Helper to retrieve to_compile: *)
    let with_compiled_program programs def f =
      match Hashtbl.find programs to_compile.L.name with
      | exception Not_found ->
          !logger.error "Compiled program %s disappeared"
            to_compile.L.name ;
          def
      | program ->
          if program.L.status <> Compiling then (
            !logger.error "Status of %s have been changed to %s \
                           during compilation (at %10.0f)!"
                program.L.name
                (SL.string_of_status program.L.status)
                program.L.last_status_change ;
            (* Doing nothing is probably the safest bet *)
            def
          ) else f program in
    (* From now on this program is our. Let's make sure we return it
     * if we mess up. *)
    !logger.debug "Compiling phase 2: compiling %s" to_compile.L.name ;
    match%lwt Compiler.compile conf parents to_compile with
    | exception exn ->
      !logger.error "Compilation of %s failed with %s"
        to_compile.L.name (Printexc.to_string exn) ;
      !logger.debug "Compiling phase 3: Returning the erroneous program" ;
      let%lwt () = C.with_wlock conf (fun programs ->
        with_compiled_program programs return_unit (fun program ->
          L.set_status program (Edition (Printexc.to_string exn)) ;
          return_unit)) in
      fail exn
    | () ->
      !logger.debug "Compiling phase 3: Returning the program" ;
      let%lwt to_restart =
        C.with_wlock conf (fun programs ->
          with_compiled_program programs return_nil (fun program ->
            L.set_status program Compiled ;
            program.funcs <- to_compile.funcs ;
            Compiler.stop_all_dependents conf programs program)) in
      (* Be lenient if some of those programs are not there anymore: *)
      Lwt_list.iter_s (run_by_name conf) to_restart

let put_program conf headers body =
  let%lwt msg =
    of_json headers "Uploading program" put_program_req_ppp body in
  let program_name = msg.name in
  (* Disallow anonymous programs for simplicity: *)
  if program_name = "" then
    bad_request "Programs must have non-empty names" else (
  let%lwt must_restart =
    C.with_wlock conf (fun programs ->
      (* Delete the program if it already exists. No worries the conf won't be
       * changed if there is any error. *)
      let%lwt stopped_it =
        match Hashtbl.find programs program_name with
        | exception Not_found -> return_false
        | program ->
          if program.L.status = Running then (
            if msg.ok_if_running then (
              let%lwt () = RamenProcesses.stop conf programs program in
              let%lwt () = del_program_ programs program in
              return_true
            ) else (
              bad_request ("Program "^ program_name ^" is running")
            )
          ) else (
            let%lwt () = del_program_ programs program in
            return_false
          ) in
      (* Create all the funcs *)
      let%lwt _program =
        try C.make_program programs program_name msg.program |>
            return
        with Invalid_argument x -> bad_request ("Invalid "^ x)
           | SyntaxError e -> bad_request (string_of_syntax_error e)
           | e -> fail e in
      return stopped_it) in
  let%lwt () =
    if must_restart || msg.start then (
      !logger.debug "Trying to (re)start program %s" program_name ;
      let%lwt () = compile_ conf program_name in
      run_by_name conf program_name
    ) else return_unit in
  respond_ok ())

(*
    Serving normal files
*)

let serve_file_with_replacements conf _headers path file =
  serve_file path file (replace_placeholders conf)

let get_index www_dir conf headers =
  if www_dir = "" then
    serve_string conf headers RamenGui.without_link
  else
    serve_string conf headers RamenGui.with_links

(*
    Whole graph operations: compile/run/stop
*)

let compile conf headers program_opt =
  (* Loop until all the given programs are compiled.
   * Return a list of programs that should be re-started. *)
  let rec compile_loop left_try failures to_retry = function
  | [] ->
      if to_retry = [] then return failures
      else compile_loop (left_try - 1) failures [] to_retry
  | to_compile :: rest ->
    !logger.debug "%d programs left to compile..."
      (List.length rest + 1 + List.length to_retry) ;
    if left_try < 0 then (
      let more_failure =
        SyntaxError (UnsolvableDependencyLoop { program = to_compile }),
        to_compile in
      return (more_failure :: failures)
    ) else (
      let open Compiler in
      try%lwt
        let%lwt () = compile_ conf to_compile in
        compile_loop (left_try - 1) failures to_retry rest
      with MissingDependency n ->
            !logger.debug "We miss func %s" (N.fq_name n) ;
            compile_loop (left_try - 1) failures
                         (to_compile :: to_retry) rest
         | exn ->
            compile_loop (left_try - 1) ((exn, to_compile) :: failures)
                         to_retry rest)
  in
  let%lwt failures = match program_opt with
  | Some to_compile ->
      compile_loop 1 [] [] [ to_compile ]
  | None ->
      (* In this case we want to compile *everything* *)
      let%lwt uncompiled = C.with_rlock conf (fun programs ->
        C.fold_programs programs [] (fun lst p ->
          match p.status with
          | Edition _ -> p.name :: lst
          | _ -> lst) |> return) in
      let len = List.length uncompiled in
      compile_loop (1 + len * (len - 1) / 2) [] [] uncompiled
  in
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
        let%lwt to_run = graph_programs programs program_opt in
        let to_run = L.order to_run in
        Lwt_list.iter_s (run_ conf programs) to_run) in
    switch_accepted headers [
      Consts.json_content_type, (fun () -> respond_ok ()) ]
  with SyntaxError _
     | Compiler.SyntaxErrorInFunc _
     | C.InvalidCommand _ as e ->
       bad_request (Printexc.to_string e)
     | x -> fail x

let stop_programs_ conf programs program_opt =
  let%lwt to_stop = graph_programs programs program_opt in
  Lwt_list.iter_p (RamenProcesses.stop conf programs) to_stop

let stop_programs conf program_opt =
  C.with_wlock conf (fun programs  ->
    stop_programs_ conf programs program_opt)

let stop conf headers program_opt =
  try%lwt
    let%lwt () = stop_programs conf program_opt in
    switch_accepted headers [
      Consts.json_content_type, (fun () -> respond_ok ()) ]
  with C.InvalidCommand e -> bad_request e
     | x -> fail x

let quit = ref false

let shutdown _conf _headers =
  (* TODO: also log client info *)
  !logger.info "Asked to shut down" ;
  (* Hopefully cohttp will serve this answer before stopping. *)
  quit := true ;
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
    of_json headers ("Exporting from "^ func_name) export_req_ppp body in
  let%lwt first, columns =
    C.with_rlock conf (fun programs ->
      get_tuples ?since:req.since ?max_res:req.max_results conf
        ~wait_up_to:req.wait_up_to programs program_name func_name) in
  let resp = { first ; columns } in
  let body = PPP.to_string export_resp_ppp resp in
  respond_ok ~body ()

(*
    Grafana Datasource: autocompletion of func/field names
*)

let complete_funcs conf headers body =
  let%lwt msg =
    of_json headers "Complete tables" complete_func_req_ppp body in
  let%lwt lst =
    C.with_rlock conf (fun programs ->
      C.complete_func_name programs msg.prefix |>
      return) in
  let body =
    PPP.to_string complete_resp_ppp lst
  in
  respond_ok ~body ()

let complete_fields conf headers body =
  let%lwt msg =
    of_json headers "Complete fields" complete_field_req_ppp body in
  let%lwt lst =
    C.with_rlock conf (fun programs ->
      C.complete_field_name programs msg.operation msg.prefix |>
      return) in
  let body =
    PPP.to_string complete_resp_ppp lst
  in
  respond_ok ~body ()

(*
    Grafana Datasource: data queries
*)

let timeseries conf headers body =
  let%lwt msg =
    of_json headers "time series query" timeseries_req_ppp body in
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
      with Not_found -> bad_request ("func "^ from ^" does not exist") in
    let%lwt _program, parent =
      func_of_name programs parent_program parent_name in
    (* FIXME: this should be a program directly, so that we could get rid of parse_operation *)
    let%lwt op_text =
      if select_x = "" then (
        let open Operation in
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
    let reformatted_op = IO.to_string Operation.print operation in
    let program_name = "temp/"^ md5 reformatted_op
    and func_name = "operation" in
    (* So far so good. In all likelihood this program exists already: *)
    if Hashtbl.mem programs program_name then (
      !logger.debug "Program %S already there" program_name
    ) else (
      (* Add this program to the running configuration: *)
      C.make_program ~timeout:300. programs program_name op_text |> ignore
    ) ;
    return (program_name, func_name, "data")
  in
  try%lwt
    let%lwt resp = Lwt_list.map_s (fun req ->
        let%lwt program_name, func_name, data_field =
          match req.spec with
          | Predefined { operation ; data_field } ->
            let%lwt program, func =
              try C.program_func_of_user_string operation |> return
              with Not_found -> bad_request ("func "^ operation ^" does not exist") in
            return (program, func, data_field)
          | NewTempFunc { select_x ; select_y ; from ; where } ->
            let%lwt (program_name, _func_name, _data_field as res) =
              C.with_wlock conf (fun programs ->
                create_temporary_func programs select_x select_y from where) in
            let%lwt () = compile_ conf program_name in
            let%lwt () = run_by_name conf program_name in
            return res in
        let%lwt times, values =
          C.with_rlock conf (fun programs ->
            ts_of_func_field programs req program_name func_name data_field) in
        return { id = req.id ; times ; values }
      ) msg.timeseries in
    let body = PPP.to_string timeseries_resp_ppp resp in
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
      let body = PPP.to_string time_range_resp_ppp resp in
      respond_ok ~body ()) ]

(* A thread that hunt for unused programs / imports *)
let rec timeout_programs conf =
  let%lwt () = C.with_wlock conf (fun programs ->
    RamenProcesses.timeout_programs conf programs) in
  (* No need for a lock on conf for that one: *)
  let%lwt () = RamenExport.timeout_exports conf in
  let%lwt () = Lwt_unix.sleep 7.1 in
  timeout_programs conf

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

let () =
  async_exception_hook := (fun exn ->
    !logger.error "Received exception %s\n%s"
      (Printexc.to_string exn)
      (Printexc.get_backtrace ()))

let cleanup_old_files persist_dir =
  (* Have a list of directories and regexps and current version,
   * Iter through this list for file matching the regexp and that are also directories.
   * If this direntry matches the current version, touch it.
   * If not, and if it hasn't been touched for X days, assume that's an old one and delete it.
   * Then sleep for one day and restart. *)
  let get_log_file () =
    Unix.gettimeofday () |> Unix.localtime |> RamenLog.log_file
  and touch_file fname =
    let now = Unix.gettimeofday () in
    Lwt_unix.utimes fname now now
  and delete_directory fname = (* TODO: should really delete *)
    Lwt_unix.rename fname (fname ^".todel")
  in
  let open Str in
  let cleanup_dir (dir, sub_re, current) =
    let dir = persist_dir ^"/"^ dir in
    !logger.debug "Cleaning directory %s" dir ;
    (* Error in there will be delivered to the stream reader: *)
    let files = Lwt_unix.files_of_directory dir in
    try%lwt
      Lwt_stream.iter_s (fun fname ->
        let full_path = dir ^"/"^ fname in
        if fname = current then (
          !logger.debug "Touching %s." full_path ;
          touch_file full_path
        ) else if string_match sub_re fname 0 &&
           is_directory full_path &&
           file_is_older_than (1. *. 86400.) fname (* TODO: should be 10 days *)
        then (
          !logger.info "Deleting old version %s." fname ;
          delete_directory full_path
        ) else return_unit
      ) files
    with Unix.Unix_error (Unix.ENOENT, _, _) ->
        return_unit
       | exn ->
        !logger.error "Cannot list %s: %s" dir (Printexc.to_string exn) ;
        return_unit
  in
  let date_regexp = regexp "^[0-9]+-[0-9]+-[0-9]+$"
  and v_regexp = regexp "v[0-9]+"
  and v1v2_regexp = regexp "v[0-9]+_v[0-9]+" in
  let rec loop () =
    let to_clean =
      [ "log", date_regexp, get_log_file () ;
        "alerting", v_regexp, RamenVersions.alerting_state ;
        "configuration", v_regexp, RamenVersions.graph_config ;
        "instrumentation_ringbuf", v1v2_regexp, (RamenVersions.instrumentation_tuple ^"_"^ RamenVersions.ringbuf) ;
        "workers/log", v_regexp, get_log_file () ;
        "workers/bin", v_regexp, RamenVersions.codegen ;
        "workers/history", v_regexp, RamenVersions.history ;
        "workers/ringbufs", v_regexp, RamenVersions.ringbuf ;
        "workers/out_ref", v_regexp, RamenVersions.out_ref ;
        "workers/src", v_regexp, RamenVersions.codegen ;
        "workers/tmp", v_regexp, RamenVersions.worker_state ]
    in
    !logger.info "Cleaning old unused files..." ;
    let%lwt () = Lwt_list.iter_s cleanup_dir to_clean in
    Lwt_unix.sleep 86400. >>= loop
  in
  loop ()

let start debug daemonize rand_seed no_demo to_stderr ramen_url www_dir
          persist_dir max_history_archives use_embedded_compiler bundle_dir
          port cert_opt key_opt alert_conf_json () =
  let demo = not no_demo in (* FIXME: in the future do not start demo by default? *)
  if to_stderr && daemonize then
    failwith "Options --daemonize and --to-stderr are incompatible." ;
  (match rand_seed with
  | None -> Random.self_init ()
  | Some seed -> Random.init seed) ;
  let logdir = if to_stderr then None else Some (persist_dir ^"/log") in
  Option.may mkdir_all logdir ;
  logger := make_logger ?logdir debug ;
  let conf =
    C.make_conf true ramen_url debug persist_dir 5 (* TODO *) max_history_archives use_embedded_compiler bundle_dir in
  (* Start the HTTP server: *)
  let lyr = function
    | [] -> bad_request_exn "Program name missing from URL"
    | lst -> String.concat "/" lst in
  let lyr_func_of path =
    let rec loop ls = function
      | [] -> bad_request_exn "func name missing from URL"
      | [x] ->
        if ls = [] then bad_request_exn "func name missing from URL"
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
  let router meth path params headers body =
    match meth, path with
    (* Ramen API *)
    | `GET, ["graph"] ->
      get_graph conf headers None
    | `GET, ("graph" :: programs) ->
      get_graph conf headers (Some (lyr programs))
    | `PUT, ["graph"] ->
      put_program conf headers body
    | `DELETE, ("graph" :: programs) ->
      del_program conf headers (lyr programs)
    | `GET, ["compile"] ->
      compile conf headers None
    | `GET, ("compile" :: programs) ->
      compile conf headers (Some (lyr programs))
    | `GET, ["run" | "start"] ->
      run conf headers None
    | `GET, (("run" | "start") :: programs) ->
      run conf headers (Some (lyr programs))
    | `GET, ["stop"] ->
      stop conf headers None
    | `GET, ("stop" :: programs) ->
      stop conf headers (Some (lyr programs))
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
          get_index www_dir conf headers) ;
        Consts.json_content_type, (fun () -> respond_ok ()) ]
    (* Web UI *)
    | `GET, ([]|["index.html"]) ->
      get_index www_dir conf headers
    | `GET, [ "style.css" | "ramen_script.js" as file ] ->
      serve_file_with_replacements conf headers www_dir file
    (* Errors *)
    | `PUT, p | `GET, p | `DELETE, p ->
      let path = String.join "/" p in
      fail (HttpError (404, "Unknown resource "^ path))
    | _ ->
      fail (HttpError (405, "Method not implemented"))
  in
  if daemonize then do_daemonize () ;
  (* When there is nothing to do, listen to collectd and netflow! *)
  let run_demo () =
    C.with_wlock conf (fun programs ->
      if demo && Hashtbl.is_empty programs then (
        !logger.info "Adding default funcs since we have nothing to do..." ;
        C.make_program programs "demo"
          "DEFINE collectd AS LISTEN FOR COLLECTD;\n\
           DEFINE netflow AS LISTEN FOR NETFLOW;" |> ignore) ;
      return_unit) in
  (* Install signal handlers *)
  Sys.(set_signal sigterm (Signal_handle (fun _ ->
    !logger.info "Received TERM" ;
    quit := true))) ;
  let rec monitor_quit () =
    let%lwt () = Lwt_unix.sleep 0.3 in
    if !quit then (
      !logger.info "Waiting for the wlock for quitting..." ;
      C.with_wlock conf (fun programs ->
        !logger.info "Stopping all workers..." ;
        let%lwt () = stop_programs_ conf programs None in
        List.iter (fun condvar ->
          !logger.info "Signaling condvar..." ;
          Lwt_condition.signal condvar ()) !http_server_done ;
        !logger.info "Quitting monitor_quit..." ;
        return_unit)
    ) else monitor_quit () in
  (* Prepare ringbuffers for reports and notifications: *)
  let rb_name = C.report_ringbuf conf in
  RingBuf.create rb_name RingBufLib.rb_default_words ;
  let reports_rb = RingBuf.load rb_name in
  let rb_name = C.notify_ringbuf conf in
  RingBuf.create rb_name RingBufLib.rb_default_words ;
  let notify_rb = RingBuf.load rb_name in
  Lwt_main.run (join
    [ (* TIL the hard way that although you can use async outside of
       * Lwt_main.run, the result will be totally unpredictable. *)
      (let%lwt () =
        Lwt_unix.sleep 1. in
        async (fun () -> restart_on_failure timeout_programs conf) ;
        async (fun () -> restart_on_failure cleanup_old_files conf.C.persist_dir) ;
        async (fun () -> restart_on_failure RamenProcesses.read_reports reports_rb) ;
        async (fun () -> restart_on_failure RamenProcesses.read_notifications notify_rb) ;
        RamenAlerter.start ?initial_json:alert_conf_json conf ;
        return_unit) ;
      run_demo () ;
      restart_on_failure monitor_quit () ;
      restart_on_failure (http_service port cert_opt key_opt) router ])
