open Batteries
open Lwt
open Helpers
open RamenLog
open RamenSharedTypes
module C = RamenConf
module L = RamenConf.Program
module N = RamenConf.Func
module SL = RamenSharedTypes.Info.Program
module SN = RamenSharedTypes.Info.Func

(* High level operations (uses LWT) *)

(* Delete (and optionally also stops) a program, and returns if the
 * program was actually running. *)
let del_program_by_name ~ok_if_running conf program_name =
  let had_stopped_it = ref false in
  let rec loop () =
    let%lwt finished =
      C.with_wlock conf (fun programs ->
        match Hashtbl.find programs program_name with
        | exception e -> fail e
        | program ->
          (match program.L.status with
          | Running ->
            if ok_if_running then (
              had_stopped_it := true ;
              let%lwt () = RamenProcesses.stop conf programs program in
              return_false
            ) else
              fail_with ("Cannot delete program "^ program_name ^
                         " which is running")
          | Stopping ->
            !logger.info "Cannot delete stopping program %s right away"
              program_name ;
            return_false
          | Compiled | Compiling | Edition _ ->
            let%lwt () =
              wrap (fun () -> C.del_program programs program) in
            return_true)) in
    if not finished then
      let delay = 1. +. Random.float 2. in
      Lwt_unix.sleep delay >>=
      loop
    else
      return_unit
  in
  let%lwt () = loop () in
  return !had_stopped_it

let start_program_by_name conf to_run =
  C.with_wlock conf (fun programs ->
    match Hashtbl.find programs to_run with
    | exception Not_found ->
        (* Cannot be an error since we are not passed programs. *)
        !logger.warning "Cannot run unknown program %s" to_run ;
        return_unit
    | p ->
        try%lwt RamenProcesses.run conf programs p
        with RamenProcesses.AlreadyRunning -> return_unit)

(* Compile one program and stop those that depended on it. *)
let rec compile_one conf program_name =
  (* Compilation taking a long time, we do a multi-stage approach:
   * First we take the wlock, read the programs and put their status
   * to compiling, and release the wlock. Then we compile everything
   * at our own peace. Then we take the wlock again, check that the
   * status is still compiling, and store the result of the
   * compilation.
   * Any dependent program will be stopped, returned to edition
   * (with a indicative error message), then restarted. *)
  !logger.debug "Compiling %s phase 1: copy the program info" program_name ;
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
                      let e = Lang.UnknownFunc (par_prog ^"/"^ par_name) in
                      raise (Compiler.SyntaxErrorInFunc (func.N.name, e))
                    | _, par_func -> par_func
                  ) func.N.parents
                ) to_compile.L.funcs with
              | exception exn ->
                  (* This is a parse error that must be reported *)
                  L.set_status to_compile (Edition (Printexc.to_string exn)) ;
                  fail exn
              | parents ->
                  L.set_status to_compile Compiling ;
                  return (Some (to_compile, parents)))
          | status ->
              !logger.info "Cannot compile program in status %s"
                (SL.string_of_status status) ;
              return_none)) with
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
    !logger.debug "Compiling %s phase 2: compiling at least" program_name ;
    match%lwt Compiler.compile conf parents to_compile with
    | exception exn ->
      !logger.error "Compilation of %s failed with %s"
        to_compile.L.name (Printexc.to_string exn) ;
      !logger.debug "Compiling %s phase 3: Returning the erroneous program" program_name ;
      let%lwt () = C.with_wlock conf (fun programs ->
        with_compiled_program programs return_unit (fun program ->
          L.set_status program (Edition (Printexc.to_string exn)) ;
          return_unit)) in
      fail exn
    | () ->
      (* Stop all dependents, waiting for them to reach the compiled state
       * and then demote them further to Edition so they go through typing
       * again. Wait until we reach this state where, with the wlock, all
       * our dependent are in Edition. *)
      !logger.debug "Compiling %s phase 3: Stopping all dependent programs" program_name ;
      let to_restart = ref Set.empty in
      let rec wait_stop count =
        if count > 60 then fail_with "Cannot stop dependent programs" else
        let%lwt finished =
          C.with_wlock conf (fun programs ->
            let%lwt all_stopped =
              C.lwt_fold_programs programs true (fun all_stopped program ->
                if program.L.name <> program_name &&
                   C.depends_on program program_name
                then (
                  match program.L.status with
                  | Stopping -> return_false (* Wait longer *)
                  | Compiled ->
                      (* Demote it further to Editable since it needs to endure
                       * typing again: *)
                      L.set_editable program ("Depends on "^ to_compile.L.name) ;
                      return all_stopped
                  | Edition _ ->
                      return all_stopped
                  | Running ->
                      !logger.info
                        "Program %s must be stopped due to compilation of %s"
                        program.L.name program_name ;
                      to_restart := Set.add program.L.name !to_restart ;
                      let%lwt () = RamenProcesses.stop conf programs program in
                      return_false
                  | Compiling ->
                      !logger.info
                        "Program %s must be stopped due to compilation of %s \
                         but is compiling. Will wait for it."
                        program.L.name program_name ;
                      return_false
                ) else return all_stopped) in
            if all_stopped then (
              !logger.debug "Compiling %s phase 4: Restarting the compiled program" program_name ;
              with_compiled_program programs return_false (fun program ->
                L.set_status program Compiled ;
                program.funcs <- to_compile.funcs ;
                return_true)
            ) else return_false) in
        if not finished then
          (* In case several clients are fighting to recompile many things it
           * is important to wait for a long and random duration: *)
          let delay = 1. +. Random.float (float_of_int (2 * count)) in
          let%lwt () = Lwt_unix.sleep delay in
          wait_stop (count + 1)
        else return_unit in
      let%lwt () = wait_stop 0 in
      (* The hard part is done. Now restart what we stopped, to be nice: *)
      !logger.debug "Compiling %s phase 5: Restarting the dependent programs" program_name ;
      (* Be lenient if some of those programs are not there anymore: *)
      let%lwt () = Lwt_list.iter_s (fun r ->
        let%lwt () = compile_one conf r in
        start_program_by_name conf r
      ) (Set.to_list !to_restart) in
      !logger.debug "Compiling %s phase 6: Victory!" program_name ;
      return_unit

(* Compile all the given programs, returning the names of those who've
 * failed *)
let compile_programs conf program_names =
  !logger.info "Going to compile %a"
    (List.print String.print) program_names ;
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
        Lang.SyntaxError (UnsolvableDependencyLoop { program = to_compile }),
        to_compile in
      return (more_failure :: failures)
    ) else (
      let open Compiler in
      try%lwt
        let%lwt () = compile_one conf to_compile in
        compile_loop (left_try - 1) failures to_retry rest
      with MissingDependency n ->
            !logger.debug "We miss func %s" (N.fq_name n) ;
            compile_loop (left_try - 1) failures
                         (to_compile :: to_retry) rest
         | exn ->
            compile_loop (left_try - 1) ((exn, to_compile) :: failures)
                         to_retry rest)
  in
  let%lwt uncompiled =
    C.with_rlock conf (fun programs ->
      List.filter_map (fun name ->
        match Hashtbl.find programs name with
        | exception Not_found ->
            (* Not a hard error since lock was released *)
            !logger.warning "Program %S does not exist" name ;
            None
        | p ->
            (match p.status with
            | Edition _ -> Some p.name
            | _ -> None)
      ) program_names |> return) in
  let len = List.length uncompiled in
  compile_loop (1 + len * (len - 1) / 2) [] [] uncompiled

(* Change the FROMs from program_name to new_program_name,
 * warn if time-related functions are used, and force the export. *)
let reprogram_for_test old_program_name new_program_name func =
  let open Lang in
  let rename s =
    if String.starts_with s old_program_name then
      let len = String.length old_program_name in
      if String.length s = len then new_program_name
      else if s.[len] = '/' then
        new_program_name ^ String.lchop ~n:len s
      else s
    else s
  in
  RamenOperation.iter_expr (function
    | RamenExpr.(StatelessFun1 (_, Age, _) |
                 StatelessFun0 (_, Now)) ->
        !logger.warning "Test uses time related functions"
    | _ -> ()) func.RamenProgram.operation ;
  match func.operation with
  | RamenOperation.Yield { every ; _ } ->
      if every > 0. then !logger.warning "Test uses YIELD EVERY" ;
      func
  | RamenOperation.ReadCSVFile _ | RamenOperation.ListenFor _ -> func
  | RamenOperation.Aggregate ({ from ; _ } as r) ->
      { func with operation =
          RamenOperation.Aggregate { r with from = List.map rename from ;
                                       force_export = true } }

(* TODO: Alternative: let the tester reprogram the code, and add a flag
 * in the func to prevent it from being started *)
(* Return the name of the program (not necessarily the same as input *)
let set_program ?test_id ?(ok_if_running=false) ?(start=false)
                conf name program =
  (* Disallow anonymous programs for simplicity: *)
  if name = "" then
    fail_with "Programs must have non-empty names" else
  if has_dotnames name then
    fail_with "Program names cannot include directory dotnames" else
  let%lwt funcs = wrap (fun () -> C.parse_program program) in
  let name, funcs =
    match test_id with
    | None -> name, funcs
    | Some id ->
      let new_name = "temp/tests/"^ id ^"/"^ name in
      new_name,
      List.map (reprogram_for_test name new_name) funcs in
  let%lwt must_restart =
    try%lwt del_program_by_name ~ok_if_running conf name
    with Not_found -> return_false in
  (* Now create it, which would be OK as long as nobody is competing with us
   * to create the same program under the same name: *)
  let%lwt _program =
    C.with_wlock conf (fun programs ->
      wrap (fun () ->
        C.make_program ?test_id programs name program funcs)) in
  let%lwt () =
    if must_restart || start then (
      !logger.debug "Trying to (re)start program %s" name ;
      let%lwt () = compile_one conf name in
      start_program_by_name conf name
    ) else return_unit in
  return name

let func_info_of_func ~with_code ~with_stats programs func =
  let%lwt exporting = RamenExport.is_func_exporting func in
  let operation =
    IO.to_string RamenOperation.print func.N.operation |>
    PPP_prettify.prettify in
  let%lwt stats =
    if with_stats then
      let%lwt report = RamenProcesses.last_report (N.fq_name func) in
      return (Some report)
    else return_none in
  let code =
    if with_code then
      Some (SN.{
        operation ;
        input_type = C.info_of_tuple_type func.N.in_type ;
        output_type = C.info_of_tuple_type func.N.out_type })
    else None in
  return SN.{
    program = func.N.program ;
    name = func.N.name ;
    exporting ;
    signature = if func.N.signature = "" then None else Some func.N.signature ;
    pid = func.N.pid ;
    last_exit = func.N.last_exit ;
    parents = List.map (fun (p, f) -> p ^"/"^ f) func.N.parents ;
    children =
      C.fold_funcs programs [] (fun children _l n ->
        if List.exists (fun (p, f) ->
             p = func.program && f = func.name
           ) n.parents
        then N.fq_name n :: children
        else children) ;
    code ;
    stats }

let func_info ?(with_code=true) ?(with_stats=true) conf program_name func_name =
  C.with_rlock conf (fun programs ->
    let%lwt func = wrap (fun () ->
      let prog = Hashtbl.find programs program_name in
      Hashtbl.find prog.L.funcs func_name) in
    func_info_of_func ~with_code ~with_stats programs func)

let program_info_of_program programs program =
  let operations =
    Hashtbl.keys program.L.funcs |>
    List.of_enum in
  return SL.{
    name = program.L.name ;
    program = program.L.program ;
    operations ;
    test_id = program.L.test_id ;
    status = program.L.status ;
    last_started = program.L.last_started ;
    last_stopped = program.L.last_stopped }

let graph_info conf program_opt =
  C.with_rlock conf (fun programs ->
    let%lwt programs' = RamenProcesses.graph_programs programs program_opt in
    let programs' = L.order programs' in
    Lwt_list.map_s (program_info_of_program programs) programs')

(* Async thread that recompiles programs which source have been changed on
 * disc *)
let recompile_dirty_source conf program_name =
  let cache, source =
    C.save_files_of_program conf.C.persist_dir program_name in
  if C.has_source_file source then
    let prog = C.load_program cache source in
    let test_id =
      if prog.test_id <> "" then Some prog.test_id else None in
    let%lwt _name =
      set_program ?test_id ~ok_if_running:true conf program_name prog.program in
    return_unit
  else
    (* No more valid source -> delete the program *)
    try
      let%lwt _ = del_program_by_name ~ok_if_running:true conf program_name in
      return_unit
    with Not_found ->
      (* del_program_by_name will load the config, which will set that program
       * on the dirty_sources list again, so we will try to delete it again...
       * That's fine (for now) *)
      return_unit

let recompile_dirty_sources conf =
  let rec loop () =
    let%lwt () =
      if not (Set.is_empty !C.dirty_sources) then
        (* With only lightweight threads no need to protect this: *)
        let program_names = Set.to_list !C.dirty_sources in
        C.dirty_sources := Set.empty ;
        !logger.info "Dealing with dirty programs %a..."
          (List.print String.print) program_names ;
        Lwt_list.iter_s (recompile_dirty_source conf) program_names
      else return_unit in
    Lwt_unix.sleep 5. >>= loop
  in
  loop ()
