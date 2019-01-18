(* Turn a ramen program into an executable binary.
 * This goes through several phases:
 *
 * 1. The parsing, which is done in RamenProgram, RamenOperation, RamenExpr
 *    and RamenTypes modules;
 * 2. The typing, happening in RamenTyping;
 * 3. The code generation, taking place in CodeGen_OCaml;
 * 4. And finally generating an executable (takes place in RamenOCamlCompiler).
 *)

open Batteries
open RamenHelpers
open RamenLog
module C = RamenConf
module F = C.Func
module P = C.Program
open RamenTypingHelpers

open Binocle

let stats_typing_time =
  RamenBinocle.ensure_inited (fun save_dir ->
    Histogram.make ~save_dir
      RamenConsts.Metric.Names.compiler_typing_time
      "Time spent typing ramen programs, per typer" Histogram.powers_of_two)

let stats_typing_count =
  RamenBinocle.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir
      RamenConsts.Metric.Names.compiler_typing_count
      "How many times a typer have succeeded/failed")

let worker_entry_point = "worker"
let replay_entry_point = "replay"

let init use_external_compiler bundle_dir max_simult_compils smt_solver =
  RamenOCamlCompiler.use_external_compiler := use_external_compiler ;
  RamenOCamlCompiler.bundle_dir := bundle_dir ;
  Atomic.Counter.set RamenOCamlCompiler.max_simult_compilations
                     max_simult_compils ;
  RamenSmt.solver := smt_solver

(* Given a program name, retrieve its binary, either form the disk or
 * the running configuration: *)

let parent_from_lib_path lib_path pn =
  P.bin_of_program_name lib_path pn |>
  P.of_bin pn (Hashtbl.create 0)

let parent_from_programs programs pn =
  let mre, get_rc = Hashtbl.find programs pn in
  if mre.C.status <> MustRun then raise Not_found else get_rc ()

(* [program_name] is used to resolve relative parent names, and name a few
 * temp files.
 * [get_parent] is a function that returns the P.t of a given
 * RamenName.program, used to get the output types of pre-existing
 * functions. *)
let compile conf get_parent ~exec_file source_file program_name =
  let program_code = read_whole_file source_file in
  (*
   * If all goes well, many temporary files are going to be created. Here
   * we collect all their name so we delete them at the end:
   *)
  let temp_files = ref Set.empty in
  let add_single_temp_file f = temp_files := Set.add f !temp_files in
  let add_temp_file f =
    add_single_temp_file (change_ext ".ml" f) ;
    add_single_temp_file (change_ext ".cmx" f) ;
    add_single_temp_file (change_ext ".cmi" f) ;
    add_single_temp_file (change_ext ".o" f) ;
    add_single_temp_file (change_ext ".s" f) ;
    add_single_temp_file (change_ext ".annot" f) in
  let del_temp_files () =
    if not conf.C.keep_temp_files then
      Set.iter (fun fname ->
        !logger.debug "Deleting temp file %s" fname ;
        log_and_ignore_exceptions safe_unlink fname
      ) !temp_files
  in
  finally del_temp_files (fun () ->
    (*
     * Now that we've set up the stage, we have to parse that program,
     * turning the program_code into a list of RamenProgram.fun:
     *)
    !logger.info "Parsing program %s"
      (RamenName.string_of_program program_name) ;
    let parsed_params, condition, parsed_funcs =
      RamenProgram.parse get_parent program_name program_code in
    (*
     * Now we have to type all of these.
     * Here we mainly construct the data required by the typer: it needs
     * parents to be a hash from child name to a list of
     * Func.t, either taken from disk (for the external parents) or new
     * mostly non-initialized ones (for internal parents), and it takes
     * the functions as a hash of FQ-names to operation and new Func.t.
     *)
    !logger.info "Typing program %s"
      (RamenName.string_of_program program_name) ;
    let compiler_funcs = Hashtbl.create 7 in
    List.iter (fun parsed_func ->
      let op = parsed_func.RamenProgram.operation in
      let name = Option.get parsed_func.name in
      let me_func =
        let open RamenOperation in
        F.{ program_name ;
            name ;
            persistent = parsed_func.persistent ;
            doc = parsed_func.doc ;
            operation = op ;
            in_type = RamenFieldMaskLib.in_type_of_operation op ;
            signature = "" ;
            parents = parents_of_operation op ;
            merge_inputs = is_merging op ;
            envvars = envvars_of_operation op } in
      let fq_name = RamenName.fq program_name name in
      Hashtbl.add compiler_funcs fq_name me_func
    ) parsed_funcs ;
    (* Now we have two types of parents: those from this program, that
     * have been created in compiler_funcs above, and those of already
     * compiled programs that have to be present on disk in the same
     * lib-path. Note that we do not look at the running configuration,
     * as we want to compile a program against a lib=path not against a
     * currently running instance.
     * This constraint programs to be compiled in a given order. *)
    (* Hash from function name to Func.t list of parents: *)
    let compiler_parents = Hashtbl.create 7 in
    List.iter (fun parsed_func ->
      RamenOperation.parents_of_operation
        parsed_func.RamenProgram.operation |>
      List.map (fun parent ->
        (* parent is the name as it appears in the source, can be FQed,
         * relative or just a local name. We need to build a set of funcs
         * where all involved func appears only once.
         * For compiling, we want the 'virtual' parents with no parameters
         * and absolute names, as we are only interested in their output
         * type: *)
        let parent_prog_name, parent_func_name =
          match parent with
          | None, func_name -> program_name, func_name
          | Some rel_prog, func_name ->
              RamenName.program_of_rel_program program_name rel_prog,
              func_name in
        let parent_name = RamenName.fq parent_prog_name parent_func_name in
        try Hashtbl.find compiler_funcs parent_name
        with Not_found ->
          if parent_prog_name = program_name then
            Printf.sprintf2
              "Cannot find parent function %a in current program"
              RamenName.fq_print parent_name |>
            failwith ;
          !logger.debug "Found external reference to function %a"
            F.print_parent parent ;
          match get_parent parent_prog_name with
          | exception Not_found ->
              Printf.sprintf2 "Cannot find parent program %a"
                RamenName.program_print parent_prog_name |>
              failwith
          | par_rc ->
              try
                List.find (fun f ->
                  f.F.name = parent_func_name
                ) par_rc.P.funcs
              with Not_found ->
                Printf.sprintf2 "No function %a in parent program %a"
                  RamenName.func_print parent_func_name
                  RamenName.program_print parent_prog_name |>
                failwith
      ) |>
      Hashtbl.add compiler_parents
                  (Option.get parsed_func.RamenProgram.name)
    ) parsed_funcs ;

    (*
     * Now that we know the input/output fields of each function
     * we are ready to set the units for all inputs and outputs.
     *)
    (* Find this field in this tuple and patch it: *)
    let patch_in_typ field units typ =
      match List.find (fun f ->
              (* We won't manage to propagate units from a parent deep
               * fields. TBD when we have proper records, done with
               * RamenTuple.typ and units is part of RamenTypes.t. *)
              f.RamenFieldMaskLib.path =
                [ Name (RamenName.string_of_field field) ]
            ) typ with
      | exception Not_found ->
          !logger.warning "Cannot find field %a in %a"
            RamenName.field_print field
            RamenFieldMaskLib.print_in_type typ
      | ft ->
          if ft.units = None then (
            !logger.debug "Set type of field %a to %a"
              RamenName.field_print field
              (Option.print RamenUnits.print) units ;
            ft.units <- units) in
    let patch_out_typ field units typ =
      match List.find (fun ft ->
              ft.RamenTuple.name = field
            ) typ with
      | exception Not_found ->
          if not (RamenName.is_private field) then (
            !logger.error "Cannot find field %a in %a"
              RamenName.field_print field
              RamenTuple.print_typ typ ;
            assert false)
      | ft ->
          if ft.units = None then (
            !logger.debug "Set type of field %a to %a"
              RamenName.field_print field
              (Option.print RamenUnits.print) units ;
            ft.units <- units) in
    let units_of_output func name =
      !logger.debug "Looking for units of output field %a in %S"
        RamenName.field_print name
        (RamenName.string_of_func func.F.name) ;
      let out_type =
        RamenOperation.out_type_of_operation ~with_private:true
                                             func.F.operation in
      match List.find (fun ft ->
              ft.RamenTuple.name = name
            ) out_type with
        | exception Not_found ->
            !logger.error "In function %a: no such input field %a (have %a)"
              RamenName.func_print func.F.name
              RamenName.field_print name
              RamenTuple.print_typ_names out_type ;
            None
        | ft ->
            !logger.debug "found typed units: %a"
              (Option.print RamenUnits.print) ft.units ;
            ft.units in
    (* Same as above, but look for the units in all funcs (and check they
     * use the same): *)
    let units_of_input func parents field =
      let what =
        Printf.sprintf2 "Field %a in parents of %a"
          RamenName.field_print field
          RamenName.func_print func.F.name in
      let units =
        (List.enum parents /@
         (fun f -> units_of_output f field)) |>
        RamenUnits.check_same_units ~what None in
      (* Patch the input type: *)
      if units <> None then
        patch_in_typ field units func.F.in_type ;
      units in
    let set_expr_units uoi uoo what e =
      let t = RamenExpr.typ_of e in
      if t.RamenExpr.units = None then (
        let u =
          let ctx =
            Printf.sprintf "evaluating units of %s" what in
          fail_with_context ctx (fun () ->
            RamenExpr.units_of_expr parsed_params uoi uoo e) in
        if u <> None then (
          !logger.debug "Set units of %a to %a"
            (RamenExpr.print false) e
            RamenUnits.print (Option.get u) ;
          t.units <- u ;
          true
        ) else false
      ) else false in
    let set_operation_units func =
      let parents =
        Hashtbl.find_default compiler_parents func.F.name [] in
      let uoi = units_of_input func parents in
      let uoo = units_of_output func in
      let changed =
        RamenOperation.fold_top_level_expr false (fun changed what e ->
          let what =
            Printf.sprintf "%s in function %s" what
              (RamenName.func_color func.F.name) in
          !logger.debug "Set units of operation expression %s" what ;
          set_expr_units uoi uoo what e || changed
        ) func.F.operation in
      (* TODO: check that various operations supposed to accept times or
       * durations come with either no units or the expected ones. *)
      (* Now that we have found the units of some expressions, patch the
       * units in the out_type. This is made uglier than necessary because
       * out_types fields are reordered. *)
      if changed then (
        let out_type =
          RamenOperation.out_type_of_operation ~with_private:true
                                               func.F.operation in
        match func.F.operation with
        | RamenOperation.Aggregate { fields ; _ } ->
            List.iter (fun sf ->
              let units = RamenExpr.(typ_of sf.RamenOperation.expr).units in
              if units <> None then
                patch_out_typ sf.alias units out_type
            ) fields
        | _ -> ()) ;
      changed
    in
    if not (reach_fixed_point (fun () ->
        let no_io _ = assert false in (* conditions cannot use in/out tuples *)
        let changed =
          Option.map_default
            (set_expr_units no_io no_io "run condition") false condition in
        Hashtbl.fold (fun _ func changed ->
          set_operation_units func || changed
        ) compiler_funcs changed)) then
      failwith "Cannot perform dimensional analysis" ;

    (*
     * Finally, call the typer:
     *)
    let call_typer typer_name typer =
      with_time (fun () ->
        finally (fun () ->
          IntCounter.add ~labels:["typer", typer_name ; "status", "ko"]
                         (stats_typing_count conf.C.persist_dir) 1)
          (fun () ->
            let res = typer () in
            IntCounter.add ~labels:["typer", typer_name ; "status", "ok"]
                           (stats_typing_count conf.C.persist_dir) 1 ;
            res) ())
        (log_and_ignore_exceptions
          (Histogram.add (stats_typing_time conf.C.persist_dir)
             ~labels:["typer", typer_name])) in
    let open RamenTyping in
    let smt2_file = C.smt_file source_file in
    let types =
      call_typer !RamenSmt.solver (fun () ->
        get_types compiler_parents condition compiler_funcs
                  parsed_params smt2_file) in
    add_single_temp_file smt2_file ;
    apply_types compiler_parents condition compiler_funcs types ;
    Hashtbl.iter (fun _ func ->
      finalize_func compiler_parents parsed_params func
    ) compiler_funcs ;
    (* Also check that the running condition have been typed: *)
    Option.may (fun cond ->
      RamenExpr.iter (check_typed ~what:"Running condition") cond
    ) condition ;

    (*
     * Now the (OCaml) code can be generated and compiled.
     *
     * Each functions is compiled into an object file (an OCaml  module) with a
     * single entry point performing the operation. Functions are processed in
     * parallel (but the number of simultaneous compilations is limited, Cf.
     * RamenOCamlCompiler).
     *
     * The dynamic part of the OCaml "casing" is then generated (dynamic just
     * because it needs to know the functions name - we could find the various
     * symbols introspecting the executable itself but doing this portably
     * would be a lot of work). The casing will then pick a function to run
     * according to some envvar.  Should the workers be implemented in another
     * language then the OCaml casing would have to pass them a few helper
     * functions.
     *)
    !logger.info "Compiling program %s"
      (RamenName.string_of_program program_name) ;
    (* Given a file name, make it a valid module name: *)
    let make_valid_for_module fname =
      let dirname, basename =
        try String.rsplit ~by:"/" fname
        with Not_found -> ".", fname in
      let basename = RamenOCamlCompiler.to_module_name basename in
      dirname ^"/"^ basename
    in
    (* Start by producing a module (used by all funcs and the running_condition
     * in the casing) with the parameters: *)
    let params_obj_name =
      make_valid_for_module (Filename.remove_extension source_file) ^
      "_params_"^ RamenVersions.codegen ^".cmx" in
    mkdir_all ~is_file:true params_obj_name ;
    let params_src_file =
      RamenOCamlCompiler.with_code_file_for params_obj_name conf (fun oc ->
        Printf.fprintf oc "(* Parameter values for program %s *)\n\
          open Batteries\n\
          open Stdint\n\
          open RamenHelpers\n\
          open RamenNullable\n"
          (RamenName.string_of_program program_name) ;
        CodeGen_OCaml.emit_parameters oc parsed_params) in
    add_temp_file params_src_file ;
    let what = "program "^ (RamenName.program_color program_name) in
    RamenOCamlCompiler.compile conf what params_src_file params_obj_name ;
    let params_mod_name =
      RamenOCamlCompiler.module_name_of_file_name params_src_file in
    (* Now one module per function: *)
    let src_name_of_func func =
      Filename.remove_extension source_file ^
      "_"^ func.F.signature ^
      "_"^ RamenVersions.codegen |>
      make_valid_for_module in
    let obj_files =
      Hashtbl.fold (fun _ func lst ->
        let obj_name = src_name_of_func func ^".cmx" in
        mkdir_all ~is_file:true obj_name ;
        (try
          CodeGen_OCaml.compile
            conf worker_entry_point replay_entry_point
            func obj_name params_mod_name parsed_params
        with e ->
          !logger.error "Cannot generate code for %s: %s"
            (RamenName.string_of_func func.name)
            (Printexc.to_string e) ;
          raise e) ;
        add_temp_file obj_name ;
        obj_name :: lst
      ) compiler_funcs [] in
    (* It might happen that we have compiled twice the same thing, if two
     * operations where identical. We must not ask the linker to include
     * the same module twice, though: *)
    let obj_files = List.sort_unique String.compare obj_files in
    (* Also include the params module in the compilation, at the end: *)
    let obj_files = obj_files @ [ params_obj_name ] in
    (*
     * Produce the casing.
     *
     * The casing is the OCaml "case" around the worker, that process the
     * command line argument, mostly for the ramen daemon to read, and then
     * run the worker of the designated operation (which has been compiled
     * above).
     *)
    let casing_obj_name =
      make_valid_for_module (Filename.remove_extension source_file) ^
      "_casing_"^ RamenVersions.codegen ^".cmx" in
    let ocaml_file =
      RamenOCamlCompiler.with_code_file_for casing_obj_name conf (fun oc ->
        let params = parsed_params in
        Printf.fprintf oc "(* Ramen Casing for program %s *)\n\
          open Batteries\n\
          open Stdint\n\
          open RamenHelpers\n\
          open RamenNullable\n\
          open %s\n\n"
          (RamenName.string_of_program program_name)
          params_mod_name ;
        (* Emit the running condition: *)
        CodeGen_OCaml.emit_running_condition oc params condition ;
        (* Embed in the binary all info required for running it: the program
         * name, the function names, their signature, input and output types,
         * force export and merge flags, and parameters default values. We
         * embed this under the shape of the typed operation, as it makes it
         * possible to also analyze the program. For simplicity, all those
         * info are also computed from the operation when we load a program. *)
        let funcs = Hashtbl.values compiler_funcs |> List.of_enum in
        let condition =
          Option.map (IO.to_string (RamenExpr.print false)) condition in
        let runconf =
          P.{ funcs ; params ; condition } in
        Printf.fprintf oc "let rc_marsh_ = %S\n"
          (Marshal.(to_string (P.serialized runconf) [])) ;
        (* Then call CodeGenLib_Casing.run with all this: *)
        Printf.fprintf oc
          "let () = CodeGenLib_Casing.run %S rc_marsh_ run_condition_\n"
            RamenVersions.codegen ;
        let func_list entry_point =
          Printf.fprintf oc "\t[\n" ;
          Hashtbl.iter (fun _ func ->
            let mod_name = src_name_of_func func |>
                           Filename.basename |>
                           String.capitalize_ascii in
            Printf.fprintf oc"\t\t%S, %s.%s ;\n"
              (RamenName.string_of_func func.F.name)
              mod_name
              entry_point
          ) compiler_funcs ;
          Printf.fprintf oc "\t]\n" in
        func_list worker_entry_point ;
        func_list replay_entry_point) in
    (*
     * Compile the casing and link it with everything, giving a single
     * executable that can perform all the operations of this ramen program.
     *)
    RamenOCamlCompiler.link conf program_name obj_files ocaml_file exec_file ;
    add_temp_file ocaml_file
  ) () (* and finally, delete temp files! *)
