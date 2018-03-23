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
 * program was actually running. Children are unaffected (beside,
 * obviously, losing the input). If a program with the same name ever
 * get recompiled then the children will be recompiled too, to account
 * for a possible change in the output type. *)
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

let stop_all_dependent_programs conf program_name f =
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
                  L.set_editable program ("Depends on "^ program_name) ;
                  return all_stopped
              | Edition _ ->
                  return all_stopped
              | Running ->
                  !logger.info
                    "Program %s must be stopped as it depends on %s"
                    program.L.name program_name ;
                  to_restart := Set.add program.L.name !to_restart ;
                  let%lwt () = RamenProcesses.stop conf programs program in
                  return_false
              | Compiling ->
                  !logger.info
                    "Program %s must be stopped as it depends on %s \
                     but is compiling. Will wait for it."
                    program.L.name program_name ;
                  return_false
            ) else return all_stopped) in
        if all_stopped then (
          let%lwt () = f programs in
          return_true
        ) else return_false) in
    if not finished then
      (* In case several clients are fighting to recompile many things it
       * is important to wait for a long and random duration: *)
      let delay = 1. +. Random.float (float_of_int (2 * count)) in
      let%lwt () = Lwt_unix.sleep delay in
      wait_stop (count + 1)
    else return !to_restart in
  wait_stop 0

(* Compile one program and stop those depending on it. *)
let rec compile_one conf program_name =
  (* Compilation taking a long time, we do a multi-stage approach:
   * First we take the wlock, read the programs and put their status
   * to compiling, and release the wlock. Then we compile everything
   * at our own peace. Then we take the wlock again, check that the
   * status is still compiling, and store the result of the
   * compilation.
   * Any dependent program will be stopped, returned to edition
   * (with a indicative error message), then recompiled. *)
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
      match Hashtbl.find programs program_name with
      | exception Not_found ->
          !logger.error "Compiled program %s disappeared"
            program_name ;
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
        program_name (Printexc.to_string exn) ;
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
      let%lwt to_restart = stop_all_dependent_programs conf program_name (fun programs ->
        !logger.debug "Compiling %s phase 4: Restarting the compiled program" program_name ;
        with_compiled_program programs return_unit (fun program ->
          L.set_status program Compiled ;
          program.funcs <- to_compile.funcs ;
          return_unit)) in
      (* The hard part is done. Now restart what we stopped, to be nice: *)
      !logger.debug "Compiling %s phase 5: Restarting the dependent programs" program_name ;
      (* Be lenient if some of those programs are not there anymore: *)
      let%lwt () = Lwt_list.iter_s (fun r ->
        let%lwt () = compile_one conf r in
        start_program_by_name conf r
      ) (Set.to_list to_restart) in
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

(* Return the name of the program (not necessarily the same as input) *)
(* FIXME: do not systematically delete everything but rather keep going
 * unaffected nodes and update the others, so they can legitimately reuse
 * their saved state. *)
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
    program = func.N.program_name ;
    name = func.N.name ;
    exporting ;
    signature = if func.N.signature = "" then None else Some func.N.signature ;
    pid = func.N.pid ;
    last_exit = func.N.last_exit ;
    parents = List.map (fun (p, f) -> p ^"/"^ f) func.N.parents ;
    children =
      C.fold_funcs programs [] (fun children _l n ->
        if List.exists (fun (p, f) ->
             p = func.program_name && f = func.name
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

(*
 * Simpler, offline, lwt-free version of set_program followed by
 * Compiler.compile, for `ramen comp`:
 *)

let exec_of_program root_path program_name =
  (* Use an extension wo we can still use the plain program_name for a
   * directory holding subprograms. Not using "exe" as it remind me of
   * that operating system, but rather "x" as in the x bit: *)
  root_path ^"/"^ program_name ^".x"

exception CannotFindBinary of string
exception LinkingError of
            { parent_program : string ; parent_function : string ;
              child_program : string ; child_function : string ;
              parent_out : C.typed_tuple ; child_in : C.typed_tuple ;
              msg : string }

let () =
  Printexc.register_printer (function
    | CannotFindBinary s ->
        Some (Printf.sprintf "Cannot find worker executable %s" s)
    | LinkingError { parent_program ; parent_function ; parent_out ;
                     child_program ; child_function ; child_in ; msg } ->
        Some (Printf.sprintf "Linking error from %s/%s (%s) to %s/%s (%s): %s"
                parent_program parent_function
                (IO.to_string C.print_typed_tuple parent_out)
                child_program child_function
                (IO.to_string C.print_typed_tuple child_in) msg)
    | _ -> None)

let runconf_of_bin fname : C.RunConf.Program.t =
  !logger.debug "Reading config from %s..." fname ;
  match Unix.open_process_in fname with
  | exception Unix.Unix_error (ENOENT, _, _) ->
      raise (CannotFindBinary fname)
  | ic ->
      let close () =
        match Unix.close_process_in ic with
        | Unix.WEXITED o -> ()
        | s ->
            !logger.error "Process %s exited with %s"
              fname
              (string_of_process_status s) in
      try
        finally close Marshal.from_channel ic
      with BatInnerIO.No_more_input ->
        raise (CannotFindBinary fname)

let runconf_of_program root_path program_name : C.RunConf.Program.t =
  let bin = exec_of_program root_path program_name in
  runconf_of_bin bin

let fake_operation = RamenOperation.Yield {
  fields = [] ; every = 0. ; event_time = None ;
  force_export = false }

let func_of_fq_name root_path fq_name =
  let program_name, func_name = C.program_func_of_user_string fq_name in
  let rc = runconf_of_program root_path program_name in
  List.find (fun func -> func.C.RunConf.Func.name = func_name) rc.functions

let ext_compile conf root_path program_name program_code =
  let program_name = L.sanitize_name program_name in
  (* List of temp files created, that must be deleted at the end: *)
  let temp_files = ref [] in
  let add_temp_file f = temp_files := f :: !temp_files in
  let add_temp_file f =
    add_temp_file (change_ext ".ml" f) ;
    add_temp_file (change_ext ".cmx" f) ;
    add_temp_file (change_ext ".cmi" f) ;
    add_temp_file (change_ext ".o" f) ;
    if conf.C.debug then add_temp_file (change_ext ".annot" f) in
  let del_temp_files () =
    if not conf.C.keep_temp_files then
      List.iter (fun fname ->
        !logger.debug "Deleting temp file %s" fname ;
        log_exceptions Unix.unlink fname
      ) !temp_files in
  finally del_temp_files (fun () ->
    (* Get a list of RamenProgram.fun: *)
    !logger.info "Parsing program %s" program_name ;
    let funcs = C.parse_program program_code in
    (* Typing *)
    (* Note: before we get rid of the "inline" compiler we'll have to fake
     * our knowledge of the program come from the inline configuration.
     *
     * So, the compiler needs parents to be a hash from child name to a list
     * of func (the parents), and it takes the functions as a hash of names
     * to C.Func.c. The resulting types will be stored in compiler_funcs: *)
    !logger.info "Typing program %s" program_name ;
    let compiler_funcs = Hashtbl.create 7 in
    List.iter (fun func ->
      let fq_name = program_name ^"/"^ func.RamenProgram.name in
      let me_func =
        C.make_func program_name func.RamenProgram.name func.RamenProgram.params func.RamenProgram.operation in
      !logger.debug "Found function %s" fq_name ;
      Hashtbl.add compiler_funcs fq_name me_func
    ) funcs ;
    let compiler_parents = Hashtbl.create 7 in
    List.iter (fun func ->
      let func_parents =
        RamenOperation.parents_of_operation func.RamenProgram.operation in
      let par_list =
        List.map (fun parent_name ->
          (* parent_name is the name as it appear in the source, can be FQ name
           * or just a local name. We need to build a set of funcs where all
           * involved funcs appear only once: *)
          let parent_fq_name =
            if String.contains parent_name '/' then parent_name
            else program_name ^"/"^ parent_name in
          match Hashtbl.find compiler_funcs parent_fq_name with
          | exception Not_found ->
              !logger.debug "Found external reference to function %s"
                parent_fq_name ;
              let pdef = func_of_fq_name root_path parent_fq_name in
              (* Build a fake C.Func.t from this RunConf.Fun.t: *)
              let program_name, name =
                C.program_func_of_user_string parent_fq_name in
              C.Func.{
                program_name ; name ; params = pdef.C.RunConf.Func.params ;
                operation = fake_operation ; signature = "" ; parents = [] ;
                in_type = TypedTuple pdef.C.RunConf.Func.in_type ;
                out_type = TypedTuple pdef.C.RunConf.Func.out_type ;
                pid = None ; last_exit = "" ; succ_failures = 0 ;
                force_export = false ; merge_inputs = false ;
                event_time = None }
          | f -> f
        ) func_parents in
      Hashtbl.add compiler_parents func.RamenProgram.name par_list ;
    ) funcs ;
    Compiler.set_all_types conf compiler_parents compiler_funcs ;
    (* Now compile all those funcs for real.
     *
     * We compile each functions into an object file with a single
     * entry point performing the operation. We then generate the dynamic part
     * of the ocaml casing (dynamic just because it needs to know the functions
     * name - we could find the various symbols in the executable itself but
     * doing this portably would be a lot of work). The casing will then pick a
     * function to run according to some envvar. It will pass this function a
     * few OCaml functions to call - but for now, given the functions are all
     * implemented in OCaml, we just call them directly.  *)
    (* Compile all functions locally, in parallel: *)
    !logger.info "Compiling program %s" program_name ;
    let obj_files = Lwt_main.run (
      Hashtbl.values compiler_funcs |> List.of_enum |>
      Lwt_list.map_p (fun func ->
        let obj_name =
          root_path ^"/"^ program_name ^"/M"^ func.N.signature ^".cmx" in
        let%lwt () =
          Compiler.compile_func conf program_name func.N.name func.N.params func.N.operation func.N.in_type func.N.out_type obj_name in
        add_temp_file obj_name ;
        return obj_name)) in
    (* Produce the casing: *)
    let exec_file = exec_of_program root_path program_name in
    let src_file =
      RamenOCamlCompiler.with_code_file_for ~allow_reuse:false exec_file conf
        (fun oc ->
        Printf.fprintf oc "(* Ramen Casing for program %s *)\n"
          program_name ;
        (* Embed in the binary all info required for running it: the program
         * name, the function names, their signature, input and output types,
         * force export and merge flags, and parameters. *)
        let runconf =
          let open C.RunConf in
          Program.{
            name = program_name ;
            functions =
              Hashtbl.values compiler_funcs |>
              Enum.map (fun func ->
                Func.{
                  name = func.N.name ;
                  params = func.N.params ;
                  in_type = C.typed_tuple_type func.N.in_type ;
                  out_type = C.typed_tuple_type func.N.out_type ;
                  signature = func.N.signature ;
                  parents = func.N.parents ;
                  force_export = RamenOperation.is_exporting func.operation ;
                  merge_inputs = RamenOperation.is_merging func.operation ;
                  event_time =
                    RamenOperation.event_time_of_operation func.operation }
              ) |>
              List.of_enum } in
        Printf.fprintf oc "let rc_str_ = %S\n"
          ((PPP.to_string C.RunConf.Program.t_ppp_ocaml runconf) |>
           PPP_prettify.prettify) ;
        Printf.fprintf oc "let rc_marsh_ = %S\n"
          (Marshal.(to_string runconf [])) ;
        (* Then call CodeGenLib.casing with all this: *)
        Printf.fprintf oc
          "let () = CodeGenLib.casing rc_str_ rc_marsh_ [\n" ;
        Hashtbl.iter (fun _ func ->
          Printf.fprintf oc"\t%S, M%s.%s ;\n"
            func.N.name func.N.signature Compiler.entry_point_name
        ) compiler_funcs ;
        Printf.fprintf oc "]\n") in
    add_temp_file src_file ;
    (* Compile the casing and link it with everything: *)
    Lwt_main.run (
      RamenOCamlCompiler.link conf program_name obj_files src_file exec_file)) ()

let check_is_subtype t1 t2 =
  (* For t1 to be a subtype of t2, all fields of t1 must be present and
   * public in t2. And since there is no more extension from scalar types at
   * this stage, those fields must have the exact same types. *)
  List.iter (fun f1 ->
    match List.find (fun f2 -> f1.typ_name = f2.typ_name) t2.C.ser with
    | exception Not_found ->
        failwith ("Field "^ f1.typ_name ^" is missing")
    | f2 ->
        if f1.typ <> f2.typ then
          failwith ("Fields "^ f1.typ_name ^" have not the same type") ;
        if f1.nullable <> f2.nullable then
          failwith ("Fields "^ f1.typ_name ^" differs with regard to NULLs")
  ) t1.C.ser

let ext_start conf program_name bin timeout =
  (* Sanitize the user provided names: *)
  let%lwt program_name = wrap (fun () -> L.sanitize_name program_name) in
  let bin = simplified_path bin in
  let module RCP = C.RunConf.Program in
  let module RCF = C.RunConf.Func in
  (* Read the RunConf from the binary: *)
  let%lwt rc = wrap (fun () -> runconf_of_bin bin) in
  C.with_wlock conf (fun programs ->
    (* Check parents and children for linking errors: *)
    C.iter_funcs programs (fun prog func ->
      List.iter (fun rc_func ->
        if List.mem (prog.L.name, func.N.name) rc_func.RCF.parents then (
          (* func is a parent of rc_func *)
          match C.typed_tuple_type func.N.out_type with
          | exception C.BadTupleTypedness _ -> ()
          | parent_out ->
              try check_is_subtype rc_func.RCF.in_type parent_out
              with Failure msg ->
                raise (LinkingError {
                  parent_program = prog.L.name ;
                  parent_function = func.N.name ;
                  parent_out ; msg ;
                  child_program = program_name ;
                  child_function = rc_func.RCF.name ;
                  child_in = rc_func.RCF.in_type })) ;
        if List.mem (program_name, rc_func.RCF.name) func.N.parents then (
          (* rc_func is a parent of func *)
          match C.typed_tuple_type func.N.in_type with
          | exception C.BadTupleTypedness _ -> ()
          | child_in ->
              try check_is_subtype child_in rc_func.RCF.out_type
              with Failure msg ->
                raise (LinkingError {
                  parent_program = program_name ;
                  parent_function = rc_func.RCF.name ;
                  parent_out = rc_func.RCF.out_type ;
                  child_program = prog.L.name ;
                  child_function = func.N.name ;
                  child_in ; msg }))
      ) rc.RCP.functions) ;
    (* Check all parents are running, or warn: *)
    let already_warned = ref Set.empty in
    List.iter (fun rc_func ->
      List.iter (fun (pprog_name, pfunc) ->
        match Hashtbl.find programs pprog_name with
        | exception Not_found ->
            !logger.warning "Parent %s/%s is not present" pprog_name pfunc
        | pprog ->
            if pprog.L.status <> Running &&
               not (Set.mem pprog.L.name !already_warned)
            then (
              !logger.warning "Parent program %s is not running (%s)"
                pprog.L.name
                (SL.string_of_status pprog.L.status) ;
              already_warned := Set.add pprog.L.name !already_warned)
      ) rc_func.RCF.parents
    ) rc.RCP.functions ;
    (* Add it into the running configuration: *)
    (* We must not already have a program by that name: *)
    if Hashtbl.mem programs program_name then
      fail_with ("Program "^ program_name ^" already exists") else
    (* Create the C.Program.t manually since make_program takes uncompiled
     * source code: *)
    let funcs = Hashtbl.create (List.length rc.RCP.functions) in
    List.iter (fun rc_func ->
      Hashtbl.add funcs rc_func.RCF.name N.{
        program_name ; name = rc_func.RCF.name ;
        params = rc_func.RCF.params ; operation = fake_operation ;
        in_type = TypedTuple rc_func.RCF.in_type ;
        out_type = TypedTuple rc_func.RCF.out_type ;
        signature = rc_func.RCF.signature ; parents = rc_func.RCF.parents ;
        force_export = rc_func.RCF.force_export ;
        merge_inputs = rc_func.RCF.merge_inputs ;
        event_time = rc_func.RCF.event_time ;
        pid = None ; last_exit = "" ; succ_failures = 0 }
    ) rc.RCP.functions ;
    let now = Unix.gettimeofday () in
    let prog = L.{
      name = program_name ; funcs ; timeout ;
      program = bin ; program_is_path_to_bin = true ;
      last_used = now ; status = Compiled ; last_status_change = now ;
      last_started = None ; last_stopped = None ; test_id = "" } in
    Hashtbl.add programs program_name prog ;
    (* Now start it: *)
    RamenProcesses.run conf programs prog)
