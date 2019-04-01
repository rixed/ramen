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
module E = RamenExpr
module O = RamenOperation
module N = RamenName
module Orc = RamenOrc
module Files = RamenFiles
open RamenTypingHelpers
open RamenConsts

open Binocle

let stats_typing_time =
  RamenBinocle.ensure_inited (fun save_dir ->
    Histogram.make ~save_dir:(save_dir :> string)
      Metric.Names.compiler_typing_time
      "Time spent typing ramen programs, per typer" Histogram.powers_of_two)

let stats_typing_count =
  RamenBinocle.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.compiler_typing_count
      "How many times a typer have succeeded/failed")

let init use_external_compiler bundle_dir max_simult_compils smt_solver =
  RamenOCamlCompiler.use_external_compiler := use_external_compiler ;
  RamenOCamlCompiler.bundle_dir := bundle_dir ;
  Atomic.Counter.set RamenOCamlCompiler.max_simult_compilations
                     max_simult_compils ;
  RamenSmt.solver := smt_solver

(* ORC codec C++ module generator: *)
let orc_codec ~debug orc_write_func orc_read_func prefix_name rtyp =
  !logger.debug "Generating an ORC codec for Ramen type %s"
    (IO.to_string T.print_typ rtyp |> abbrev 130) ;
  let xtyp = IO.to_string CodeGen_OCaml.otype_of_type rtyp in
  !logger.debug "Corresponding runtime type: %s" xtyp ;
  let otyp = Orc.of_structure rtyp.T.structure in
  let schema = IO.to_string Orc.print otyp in
  !logger.debug "Corresponding ORC type: %s" schema ;
  let cc_src_file = N.cat prefix_name (N.path "_orc_codec.cc") in
  Files.mkdir_all ~is_file:true cc_src_file ;
  File.with_file_out (cc_src_file :> string) (fun oc ->
    Orc.emit_intro oc ;
    Orc.emit_write_value orc_write_func rtyp oc ;
    Orc.emit_read_values orc_read_func rtyp oc ;
    Orc.emit_outro oc) ;
  !logger.debug "Generated C++ support file in %a"
    N.path_print cc_src_file ;
  let cpp_command (src : N.path) (dst : N.path) =
    let inc =
      N.path_cat [ !RamenOCamlCompiler.bundle_dir ; N.path "include" ] in
    Printf.sprintf2 "%s%s -std=c++17 -W -Wall -c -I %s -I %s -o %s %s"
      (* No quote as it might be a command line: *)
      RamenCompilConfig.cpp_compiler
      (if debug then " -g" else "")
      (shell_quote (RamenCompilConfig.ocamllib :> string))
      (shell_quote (inc :> string))
      (shell_quote (dst :> string))
      (shell_quote (src :> string)) in
  let cc_dst = Files.change_ext "o" cc_src_file in
  let cmd = cpp_command cc_src_file cc_dst in
  let status = Unix.system cmd in
  if status <> Unix.WEXITED 0 then
    Printf.sprintf2 "Compilation of %a with %s failed: %s"
      N.path_print_quoted cc_src_file
      cmd (string_of_process_status status) |>
    failwith ;
  cc_dst, schema

(* Given a program name, retrieve its binary, either form the disk or
 * the running configuration: *)

let parent_from_lib_path lib_path pn =
  let try_path ~errors_ok p =
    P.bin_of_program_name p pn |>
    P.of_bin ~errors_ok pn (Hashtbl.create 0) in
  let rec loop = function
    | [] -> try_path ~errors_ok:false (N.path "./")
    | [p] -> try_path ~errors_ok:false p
    | p :: rest ->
        (try try_path ~errors_ok:true p
        with _ -> loop rest) in
  loop lib_path

let parent_from_programs programs pn =
  let rce, get_rc = Hashtbl.find programs pn in
  if rce.C.status <> MustRun then raise Not_found else get_rc ()

(* [program_name] is used to resolve relative parent names, and name a few
 * temp files.
 * [get_parent] is a function that returns the P.t of a given
 * N.program, used to get the output types of pre-existing
 * functions. *)
let compile conf get_parent ~exec_file source_file
            (program_name : N.program) =
  let program_code = Files.read_whole_file source_file in
  (*
   * If all goes well, many temporary files are going to be created. Here
   * we collect all their name so we delete them at the end:
   *)
  let temp_files = ref Set.empty in
  let add_single_temp_file f = temp_files := Set.add f !temp_files in
  let add_temp_file f =
    add_single_temp_file (Files.change_ext "ml" f) ;
    add_single_temp_file (Files.change_ext "cmx" f) ;
    add_single_temp_file (Files.change_ext "cmi" f) ;
    add_single_temp_file (Files.change_ext "o" f) ;
    add_single_temp_file (Files.change_ext "s" f) ;
    add_single_temp_file (Files.change_ext "cc" f) ;
    add_single_temp_file (Files.change_ext "cmt" f) ;
    add_single_temp_file (Files.change_ext "cmti" f) ;
    add_single_temp_file (Files.change_ext "annot" f) in
  let del_temp_files () =
    if not conf.C.keep_temp_files then
      Set.iter (fun fname ->
        !logger.debug "Deleting temp file %a" N.path_print fname ;
        log_and_ignore_exceptions Files.safe_unlink fname
      ) !temp_files
  in
  finally del_temp_files (fun () ->
    (*
     * Now that we've set up the stage, we have to parse that program,
     * turning the program_code into a list of RamenProgram.fun:
     *)
    !logger.info "Parsing program %s"
      (program_name :> string) ;
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
      (program_name :> string) ;
    let compiler_funcs = Hashtbl.create 7 in
    List.iter (fun parsed_func ->
      let op = parsed_func.RamenProgram.operation in
      let name = Option.get parsed_func.name in
      let me_func =
        F.{ program_name ;
            name ;
            persistent = parsed_func.persistent ;
            doc = parsed_func.doc ;
            operation = op ;
            in_type = RamenFieldMaskLib.in_type_of_operation op ;
            signature = "" ;
            parents = O.parents_of_operation op ;
            merge_inputs = O.is_merging op ;
            envvars = O.envvars_of_operation op } in
      let fq_name = N.fq_of_program program_name name in
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
      O.parents_of_operation
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
              N.program_of_rel_program program_name rel_prog,
              func_name in
        let parent_name =
          N.fq_of_program parent_prog_name parent_func_name in
        try Hashtbl.find compiler_funcs parent_name
        with Not_found ->
          if parent_prog_name = program_name then
            Printf.sprintf2
              "Cannot find parent function %a in current program"
              N.fq_print parent_name |>
            failwith ;
          !logger.debug "Found external reference to function %a"
            F.print_parent parent ;
          match get_parent parent_prog_name with
          | exception Not_found ->
              Printf.sprintf2 "Cannot find parent program %a"
                N.program_print parent_prog_name |>
              failwith
          | par_rc ->
              try
                List.find (fun f ->
                  f.F.name = parent_func_name
                ) par_rc.P.funcs
              with Not_found ->
                Printf.sprintf2 "No function %a in parent program %a"
                  N.func_print parent_func_name
                  N.program_print parent_prog_name |>
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
              f.RamenFieldMaskLib.path = [ Name field ]
            ) typ with
      | exception Not_found ->
          !logger.warning "Cannot find field %a in %a"
            N.field_print field
            RamenFieldMaskLib.print_in_type typ
      | ft ->
          if ft.units = None then (
            !logger.debug "Set type of field %a to %a"
              N.field_print field
              (Option.print RamenUnits.print) units ;
            ft.units <- units) in
    let patch_out_typ field units typ =
      match List.find (fun ft ->
              ft.RamenTuple.name = field
            ) typ with
      | exception Not_found ->
          !logger.error "Cannot find field %a in %a"
            N.field_print field
            RamenTuple.print_typ typ ;
          assert false
      | ft ->
          if ft.units = None then (
            !logger.debug "Set type of field %a to %a"
              N.field_print field
              (Option.print RamenUnits.print) units ;
            ft.units <- units) in
    let units_of_output func name =
      !logger.debug "Looking for units of output field %a in %S"
        N.field_print name
        (func.F.name :> string) ;
      let out_type =
        O.out_type_of_operation func.F.operation in
      match List.find (fun ft ->
              ft.RamenTuple.name = name
            ) out_type with
        | exception Not_found ->
            !logger.error "In function %a: no such input field %a (have %a)"
              N.func_print func.F.name
              N.field_print name
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
          N.field_print field
          N.func_print func.F.name in
      let units =
        (List.enum parents /@
         (fun f -> units_of_output f field)) |>
        RamenUnits.check_same_units ~what None in
      (* Patch the input type: *)
      if units <> None then
        patch_in_typ field units func.F.in_type ;
      units in
    let set_expr_units uoi uoo what e =
      if e.E.units = None then (
        let u =
          let ctx =
            Printf.sprintf "evaluating units of %s" what in
          fail_with_context ctx (fun () ->
            E.units_of_expr parsed_params uoi uoo e) in
        if u <> None then (
          !logger.debug "Set units of %a to %a"
            (E.print false) e
            RamenUnits.print (Option.get u) ;
          e.units <- u ;
          true
        ) else false
      ) else false in
    let set_operation_units func =
      let parents =
        Hashtbl.find_default compiler_parents func.F.name [] in
      let uoi = units_of_input func parents in
      let uoo = units_of_output func in
      let changed =
        O.fold_top_level_expr false (fun changed what e ->
          let what =
            Printf.sprintf "%s in function %s" what
              (N.func_color func.F.name) in
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
          O.out_type_of_operation func.F.operation in
        match func.F.operation with
        | O.Aggregate { fields ; _ } ->
            List.iter (fun sf ->
              if sf.O.expr.E.units <> None then
                patch_out_typ sf.alias sf.expr.E.units out_type
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
     * Before typing and generating code, transform the operation so that
     * deep field that are cherry-picked appears "flat" in the input.
     * We won't be able to type or generate code for the Get operator that
     * is going to be eluded.
     *)
    Hashtbl.iter (fun fq func ->
      !logger.debug "Substituting cherry-picked fields in %a"
        N.fq_print fq ;
      !logger.debug "in_type: %a"
        RamenFieldMaskLib.print_in_type func.F.in_type ;
      let op = func.F.operation in
      !logger.debug "Original operation: %a" (O.print true) op ;
      let op = RamenFieldMaskLib.subst_deep_fields func.F.in_type op in
      !logger.debug "After substitutions for deep fields access: %a"
        (O.print true) op ;
      func.operation <- op
    ) compiler_funcs ;

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
      E.iter (check_typed ~what:"Running condition") cond
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
    !logger.info "Compiling program %s" (program_name :> string) ;
    let debug = conf.C.log_level = Debug
    and keep_temp_files = conf.C.keep_temp_files in
    let what = "program "^ (N.program_color program_name) in
    (* Start by producing a module (used by all funcs and the running_condition
     * in the casing) with the parameters: *)
    let params_obj_name =
      N.cat (Files.remove_ext source_file)
            (N.path ("_params_"^ RamenVersions.codegen ^".cmx")) |>
      RamenOCamlCompiler.make_valid_for_module in
    Files.mkdir_all ~is_file:true params_obj_name ;
    let params_src_file =
      RamenOCamlCompiler.with_code_file_for
        params_obj_name conf.C.keep_temp_files (fun oc ->
        Printf.fprintf oc "(* Parameter values for program %s *)\n\
          open Batteries\n\
          open Stdint\n\
          open RamenHelpers\n\
          open RamenNullable\n"
          (program_name :> string) ;
        CodeGen_OCaml.emit_parameters oc parsed_params) in
    add_temp_file params_src_file ;
    RamenOCamlCompiler.compile
      ~debug ~keep_temp_files what params_src_file params_obj_name ;
    let params_mod_name =
      RamenOCamlCompiler.module_name_of_file_name params_src_file in
    (* We need to collect all envvars used in the whole program (same as
     * the env tuple that's just been typed): *)
    let envvars =
      Hashtbl.fold (fun _ func envvars ->
        List.rev_append
          (O.envvars_of_operation func.F.operation)
          envvars
      ) compiler_funcs [] |>
      List.fast_sort N.compare in
    let src_name_of_func func =
      N.cat (Files.remove_ext source_file)
            (N.path ("_"^ func.F.signature ^
                     "_"^ RamenVersions.codegen)) |>
      RamenOCamlCompiler.make_valid_for_module in
    let obj_files =
      Hashtbl.fold (fun _ func obj_files ->
        (* Start with the C++ object file for ORC support: *)
        let orc_write_func = "orc_write_"^ func.F.signature
        and orc_read_func = "orc_read_"^ func.F.signature
        and rtyp = O.out_record_of_operation func.F.operation in
        let obj_files =
          !logger.debug "Generating ORC support modules" ;
          let obj_file, _ = orc_codec ~debug orc_write_func orc_read_func
                                      (src_name_of_func func) rtyp in
          add_temp_file obj_file ;
          obj_file :: obj_files in
          (* Note: the OCaml wrappers will be written in the single ML
           * module generated by [CodeGen_OCaml.compile below] *)
        (* Then the OCaml module that implement the function operation: *)
        let obj_name = Files.add_ext (src_name_of_func func) "cmx" in
        Files.mkdir_all ~is_file:true obj_name ;
        (try
          CodeGen_OCaml.compile
            conf func obj_name params_mod_name orc_write_func orc_read_func
            parsed_params envvars
        with e ->
          let bt = Printexc.get_raw_backtrace () in
          let exn =
            Failure (
              Printf.sprintf2 "Cannot generate code for %s: %s"
                (func.name :> string) (Printexc.to_string e)) in
          Printexc.raise_with_backtrace exn bt) ;
        add_temp_file obj_name ;
        obj_name :: obj_files
      ) compiler_funcs [ params_obj_name ] in
    (* It might happen that we have compiled twice the same thing, if two
     * operations where identical. We must not ask the linker to include
     * the same module twice, though: *)
    let obj_files = list_remove_dups String.compare obj_files in
    (*
     * Produce the casing.
     *
     * The casing is the OCaml "case" around the worker, that process the
     * command line argument, mostly for the ramen daemon to read, and then
     * run the worker of the designated operation (which has been compiled
     * above).
     *)
    let casing_obj_name =
      N.cat (Files.remove_ext source_file)
            (N.path ("_casing_"^ RamenVersions.codegen ^".cmx")) |>
      RamenOCamlCompiler.make_valid_for_module in
    let src_file =
      RamenOCamlCompiler.with_code_file_for
        casing_obj_name conf.C.keep_temp_files (fun oc ->
        let params = parsed_params in
        Printf.fprintf oc "(* Ramen Casing for program %s *)\n\
          open Batteries\n\
          open Stdint\n\
          open RamenHelpers\n\
          open RamenNullable\n\
          open %s\n\n"
          (program_name :> string)
          params_mod_name ;
        (* Emit the running condition: *)
        CodeGen_OCaml.emit_running_condition oc params envvars condition ;
        (* Embed in the binary all info required for running it: the program
         * name, the function names, their signature, input and output types,
         * force export and merge flags, and parameters default values. We
         * embed this under the shape of the typed operation, as it makes it
         * possible to also analyze the program. For simplicity, all those
         * info are also computed from the operation when we load a program. *)
        let funcs = Hashtbl.values compiler_funcs |> List.of_enum in
        let condition =
          Option.map (IO.to_string (E.print false)) condition in
        let runconf =
          P.{ funcs ; params ; condition } in
        Printf.fprintf oc "let rc_marsh_ = %S\n"
          (Marshal.(to_string (P.serialized runconf) [])) ;
        (* Then call CodeGenLib_Casing.run with all this: *)
        Printf.fprintf oc
          "let () = CodeGenLib_Casing.run %S rc_marsh_ run_condition_\n"
            RamenVersions.codegen ;
        Printf.fprintf oc "\t[\n" ;
        Hashtbl.iter (fun _ func ->
          let mod_name =
            ((src_name_of_func func |> Files.basename) :> string) |>
            String.capitalize_ascii in
          Printf.fprintf oc
            "\t\t%S,\n\
             \t\t\t{ worker_entry_point = %s.%s ;\n\
             \t\t\t  top_half_entry_point = %s.%s ;\n\
             \t\t\t  replay_entry_point = %s.%s ;\n\
             \t\t\t  convert_entry_point = %s.%s } ;\n"
            (func.F.name :> string)
            mod_name EntryPoints.worker
            mod_name EntryPoints.top_half
            mod_name EntryPoints.replay
            mod_name EntryPoints.convert
        ) compiler_funcs ;
        Printf.fprintf oc "\t]\n"
      ) in
    (*
     * Compile the casing and link it with everything, giving a single
     * executable that can perform all the operations of this ramen program.
     *)
    let what = "program "^ N.program_color program_name in
    RamenOCamlCompiler.link ~debug ~keep_temp_files ~what ~obj_files
                            ~src_file ~exec_file ;
    add_temp_file src_file
  ) () (* and finally, delete temp files! *)
