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

let del_program programs program =
   if program.L.status = Running then
    fail_with "Cannot delete a running program"
  else
    wrap (fun () -> C.del_program programs program)

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
let compile_one conf program_name =
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
      Lwt_list.iter_s (start_program_by_name conf) to_restart

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
    fail_with "Program names cannot include directory dotnames" else (
  let%lwt funcs = wrap (fun () -> C.parse_program program) in
  let name, funcs =
    match test_id with
    | None -> name, funcs
    | Some id ->
      let new_name = "temp/tests/"^ id ^"/"^ name in
      new_name,
      List.map (reprogram_for_test name new_name) funcs in
  let%lwt must_restart =
    C.with_wlock conf (fun programs ->
      (* Delete the program if it already exists. No worries the conf won't be
       * changed if there is any error. *)
      let%lwt stopped_it =
        match Hashtbl.find programs name with
        | exception Not_found -> return_false
        | program ->
          if program.L.status = Running then (
            if ok_if_running then (
              let%lwt () = RamenProcesses.stop conf programs program in
              let%lwt () = del_program programs program in
              return_true
            ) else (
              fail_with ("Program "^ name ^" is running")
            )
          ) else (
            let%lwt () = del_program programs program in
            return_false
          ) in
      (* Create the program *)
      let%lwt _program =
        wrap (fun () -> C.make_program ?test_id programs name program funcs) in
      return stopped_it) in
  let%lwt () =
    if must_restart || start then (
      !logger.debug "Trying to (re)start program %s" name ;
      let%lwt () = compile_one conf name in
      start_program_by_name conf name
    ) else return_unit in
  return name)

let func_info_of_func programs func =
  let%lwt stats = RamenProcesses.last_report (N.fq_name func) in
  let%lwt exporting = RamenExport.is_func_exporting func in
  let operation =
    IO.to_string RamenOperation.print func.N.operation |>
    PPP_prettify.prettify in
  return SN.{
    name = func.N.name ;
    operation ;
    exporting ;
    signature = if func.N.signature = "" then None else Some func.N.signature ;
    pid = func.N.pid ;
    last_exit = func.N.last_exit ;
    input_type = C.info_of_tuple_type func.N.in_type ;
    output_type = C.info_of_tuple_type func.N.out_type ;
    parents = List.map (fun (p, f) -> p ^"/"^ f) func.N.parents ;
    children =
      C.fold_funcs programs [] (fun children _l n ->
        if List.exists (fun (p, f) ->
             p = func.program && f = func.name
           ) n.parents
        then N.fq_name n :: children
        else children) ;
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
    test_id = program.L.test_id ;
    status = program.L.status ;
    last_started = program.L.last_started ;
    last_stopped = program.L.last_stopped }

let graph_info conf program_opt =
  C.with_rlock conf (fun programs ->
    let%lwt programs' = RamenProcesses.graph_programs programs program_opt in
    let programs' = L.order programs' in
    Lwt_list.map_s (program_info_of_program programs) programs')
