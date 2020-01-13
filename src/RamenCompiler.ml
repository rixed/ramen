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
module VSI = RamenSync.Value.SourceInfo
module E = RamenExpr
module O = RamenOperation
module N = RamenName
module Orc = RamenOrc
module Files = RamenFiles
module Paths = RamenPaths
module Processes = RamenProcesses
open RamenTypingHelpers
open RamenConsts

open Binocle

let stats_typing_time =
  RamenWorkerStats.ensure_inited (fun save_dir ->
    Histogram.make ~save_dir:(save_dir :> string)
      Metric.Names.compiler_typing_time
      "Time spent typing ramen programs, per typer" Histogram.powers_of_two)

let stats_typing_count =
  RamenWorkerStats.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.compiler_typing_count
      "How many times a typer have succeeded/failed")

let init use_external_compiler max_simult_compils smt_solver =
  RamenOCamlCompiler.use_external_compiler := use_external_compiler ;
  Atomic.Counter.set RamenOCamlCompiler.max_simult_compilations
                     max_simult_compils ;
  RamenSmt.solver := smt_solver

(* Helper for C++ compilation, takes a code generator and returns the object
 * file: *)
let cpp_compile print_code conf prefix_name suffix_name =
  let debug = conf.C.log_level = Debug in
  let src_file =
    let (+) = N.cat in
    prefix_name + (N.path "_") + suffix_name + (N.path ".cc") in
  Files.mkdir_all ~is_file:true src_file ;
  File.with_file_out (src_file :> string) print_code ;
  !logger.debug "Generated C++ support file in %a"
    N.path_print src_file ;
  let run_cmd cmd =
    let failed reason =
      Printf.sprintf2 "Compilation of %a with %s failed: %s"
        N.path_print_quoted src_file
        cmd reason |>
      failwith in
    let what = "Compilation of C++ helper" in
    let max_count = RamenOCamlCompiler.max_simult_compilations in
    match run_coprocess ~max_count what cmd with
    | None ->
        failed "Cannot run command"
    | Some (Unix.WEXITED 0) ->
        !logger.debug "%s: Done!" what
    | Some status ->
        failed (string_of_process_status status)
  in
  let cpp_command (src : N.path) (dst : N.path) =
    let inc =
      N.path_cat [ conf.bundle_dir ; N.path "include" ] in
    let optim_level = if debug then 0 else 3 in
    Printf.sprintf2 "%s%s -std=c++17 -W -Wall -O%d -c -I %s -I %s -o %s %s"
      (* No quote as it might be a command line: *)
      RamenCompilConfig.cpp_compiler
      (if debug then " -g" else "")
      optim_level
      (shell_quote (N.path_cat [ conf.bundle_dir ; N.path "ocaml" ] :> string))
      (shell_quote (inc :> string))
      (shell_quote (dst :> string))
      (shell_quote (src :> string)) in
  let obj_file = Files.change_ext "o" src_file in
  let cmd = cpp_command src_file obj_file in
  run_cmd cmd ;
  obj_file

let ocaml_compile print_code conf prefix_name suffix_name =
  let src_file =
    let (+) = N.cat in
    prefix_name + (N.path "_") + suffix_name + (N.path ".ml") in
  Files.mkdir_all ~is_file:true src_file ;
  File.with_file_out (src_file :> string) print_code ;
  !logger.debug "Generated OCaml support file in %a"
    N.path_print src_file ;
  let obj_file = Files.change_ext "cmx" src_file in
  let keep_temp_files = conf.C.keep_temp_files
  and what = "Compilation of OCaml helper" in
  RamenOCamlCompiler.compile conf ~keep_temp_files what src_file obj_file ;
  obj_file

(* ORC codec C++ module generator: *)
let orc_codec conf orc_write_func orc_read_func prefix_name rtyp =
  !logger.debug "Generating an ORC codec for Ramen type %s"
    (IO.to_string T.print_typ rtyp |> abbrev 130) ;
  let xtyp = IO.to_string CodeGen_OCaml.otype_of_type rtyp in
  !logger.debug "Corresponding runtime type: %s" xtyp ;
  let otyp = Orc.of_structure rtyp.T.structure in
  let schema = IO.to_string Orc.print otyp in
  !logger.debug "Corresponding ORC type: %s" schema ;
  let print_code oc =
    Orc.emit_intro oc ;
    Orc.emit_write_value orc_write_func rtyp oc ;
    Orc.emit_read_values orc_read_func rtyp oc ;
    Orc.emit_outro oc in
  cpp_compile print_code conf prefix_name ObjectSuffixes.orc_codec,
  schema

(* Given a program name, retrieve its binary, either form the disk or
 * the running configuration: *)

let program_from_lib_path lib_path pn =
  let try_path ~errors_ok p =
    Paths.bin_of_program_name p pn |>
    Processes.of_bin ~errors_ok (Hashtbl.create 0) in
  let rec loop = function
    | [] -> try_path ~errors_ok:false (N.path "./")
    | [p] -> try_path ~errors_ok:false p
    | p :: rest ->
        (try try_path ~errors_ok:true p
        with _ -> loop rest) in
  loop lib_path

let program_from_confserver clt (pn : N.program) =
  let open RamenSync in
  let src_path = N.src_path_of_program pn in
  let info_key =
    (* Contrary to Paths.bin_of_program_name, no need to abbreviate here: *)
    Key.Sources (src_path, "info") in
  !logger.debug "Looking for key %a" Key.print info_key ;
  match (Client.find clt info_key).value with
  | Value.SourceInfo { detail = Compiled info } ->
      info
  | _ -> raise Not_found

(* [program_name] is used to resolve relative parent names, and name a few
 * temp files.
 * [get_parent] is a function that returns the P.t of a given
 * N.program, used to get the output types of pre-existing
 * functions.
 * Will fail with Failure or MissingParent; The later is meant to be temporary
 * and is in no way abnormal. *)

let precompile conf get_parent src_file (program_name : N.program) =
  let program_code = Files.read_whole_file src_file in
  let keep = conf.C.keep_temp_files in
  Files.clean_temporary ~keep (fun add_single_temp_file _add_temp_file ->
    (*
     * First thing is to parse that program,
     * turning the program_code into a list of RamenProgram.fun:
     *)
    !logger.info "Parsing program %s"
      (program_name :> string) ;
    let parsed_params, condition, globals, parsed_funcs =
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
        VSI.{ name ;
              retention = parsed_func.retention ;
              is_lazy = parsed_func.is_lazy ;
              doc = parsed_func.doc ;
              operation = op ;
              (* Those two are set later by finalize_func: *)
              out_record = T.make TEmpty ;
              factors = [] ;
              signature = "" ;
              in_signature = "" } in
      let fq_name = N.fq_of_program program_name name in
      Hashtbl.add compiler_funcs fq_name me_func
    ) parsed_funcs ;
    (* Now we have two types of parents: those from this program, that
     * have been created in compiler_funcs above, and those of already
     * compiled programs that have to be present in the same src-path.
     * Note that we do not look at the running configuration,
     * as we want to compile a program against a src-path not against a
     * currently running instance.
     * This constrains programs to be compiled in a given order. *)
    (* Hash from function name to (N.program * Func.t) list of parents: *)
    let compiler_parents = Hashtbl.create 7 in
    let parent_prog_func = function
      | _, None, func_name -> program_name, func_name
      | _, Some rel_prog, func_name ->
          N.program_of_rel_program program_name rel_prog,
          func_name in
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
        let parent_prog_name, parent_func_name = parent_prog_func parent in
        let parent_name =
          N.fq_of_program parent_prog_name parent_func_name in
        parent_prog_name,
        try Hashtbl.find compiler_funcs parent_name
        with Not_found ->
          if parent_prog_name = program_name then
            Printf.sprintf2
              "Cannot find parent function %a in current program"
              N.fq_print parent_name |>
            failwith ;
          !logger.debug "Found external reference to function %a"
            O.print_parent parent ;
          match get_parent parent_prog_name with
          | exception Not_found ->
              let src_path = N.src_path_of_program parent_prog_name in
              raise (RamenProgram.MissingParent src_path)
          | par_rc ->
              try
                List.find (fun f ->
                  f.VSI.name = parent_func_name
                ) par_rc.VSI.funcs
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
(*
    (* We need to reify in_type and out_type for each function so that we
     * can update their units and let the unit computation reach a fixed
     * point. So let's reify them here: *)
    let input_of_func =
      let h = Hashtbl.create 5 in
      fun func ->
        try Hashtbl.find h func.VSI.name
        with Not_found ->
          let inp =
            RamenFieldMaskLib.in_type_of_operation func.VSI.operation in
          Hashtbl.add h func.VSI.name inp ;
          inp
    and output_of_func =
      let h = Hashtbl.create 5 in
      fun func ->
        try Hashtbl.find h func.VSI.name
        with Not_found ->
          let out =
            O.out_type_of_operation ~with_private:true func.VSI.operation in
          Hashtbl.add h func.VSI.name out ;
          out in *)
    let units_of_output func name =
      !logger.debug "Looking for units of output field %a in %S"
        N.field_print name
        (func.VSI.name :> string) ;
      let out_type =
        O.out_type_of_operation ~with_private:true func.VSI.operation in
(*        (func.VSI.name :> string) ;
     let out_type = output_of_func func in *)
      match List.find (fun ft ->
              ft.RamenTuple.name = name
            ) out_type with
        | exception Not_found ->
            !logger.error "In function %a: no such input field %a (have %a)"
              N.func_print func.VSI.name
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
          N.func_print func.VSI.name in
      let units =
        (List.enum parents /@
         (fun (_n, f) -> units_of_output f field)) |>
        RamenUnits.check_same_units ~what None in
      (* Patch the input type: *)
      if units <> None then (
        let in_type =
          RamenFieldMaskLib.in_type_of_operation func.operation in
        patch_in_typ field units in_type ;
(*      if units <> None then (
        let inp = input_of_func func in
        patch_in_typ field units inp) ;*)
      ) ;
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
        Hashtbl.find_default compiler_parents func.VSI.name [] in
      let uoi = units_of_input func parents in
      let uoo = units_of_output func in
      let changed =
        O.fold_top_level_expr false (fun changed what e ->
          let what =
            Printf.sprintf "%s in function %s" what
              (N.func_color func.VSI.name) in
          !logger.debug "Set units of operation expression %s" what ;
          try set_expr_units uoi uoo what e || changed
          with Failure msg ->
            !logger.warning "%s" msg ; changed
        ) func.VSI.operation in
      (* TODO: check that various operations supposed to accept times or
       * durations come with either no units or the expected ones. *)
      (* Now that we have found the units of some expressions, patch the
       * units in the out_type. This is made uglier than necessary because
       * out_types fields are reordered. *)
      if changed then (
        let out_type =
          O.out_type_of_operation ~with_private:true func.VSI.operation in
        match func.VSI.operation with
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
          set_expr_units no_io no_io "run condition" condition in
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
(*      let inp =
        input_of_func func in *)
      let in_type =
        RamenFieldMaskLib.in_type_of_operation func.VSI.operation in
      !logger.debug "in_type: %a"
        RamenFieldMaskLib.print_in_type in_type ;
      let op = func.VSI.operation in
      !logger.debug "Original operation: %a" (O.print true) op ;
      let op = RamenFieldMaskLib.subst_deep_fields in_type op in
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
    let smt2_file = Files.change_ext "smt2" src_file in
    let types =
      call_typer !RamenSmt.solver (fun () ->
        get_types compiler_parents condition program_name compiler_funcs
                  parsed_params globals smt2_file) in
    add_single_temp_file smt2_file ;
    apply_types compiler_parents condition compiler_funcs types ;
    (* For the inference of event times and factors performed by
     * [finalize_func] to work, we should process functions from topmost
     * to bottom (parents to children). But loops are allowed within a
     * program, so such an order is not guaranteed to exist. Therefore,
     * inference is best effort.
     *
     * While rem_funcs is not null, try to find a function in there with no parents
     * in rem_funcs and process it. If no such function can be found, just process
     * all the remaining functions in any order: *)
    let has_parent_in fs fname =
      let func = Hashtbl.find compiler_funcs fname in
      O.parents_of_operation func.VSI.operation |>
      List.exists (fun parent ->
        let parent_prog_name, parent_func_name = parent_prog_func parent in
        let parent_fq =
          N.fq_of_program parent_prog_name parent_func_name in
        List.mem parent_fq fs) in
    let finalize fname =
      let func = Hashtbl.find compiler_funcs fname in
      let parents = Hashtbl.find_default compiler_parents func.VSI.name [] in
      finalize_func parents parsed_params program_name func in
    let rec loop = function
      | [] -> ()
      | [fname] ->
          finalize fname
      | fname :: next ->
          let rec search prev fname next =
            let prev' =
              if has_parent_in prev fname ||
                 has_parent_in next fname
              then
                fname :: prev
              else (
                finalize fname ;
                prev
              ) in
            match next with
            | [] -> prev'
            | fname' :: next' -> search prev' fname' next' in
          let rem = search [] fname next in
          List.iter finalize rem
    in
    loop (Hashtbl.keys compiler_funcs |> List.of_enum) ;
    (* Also check that the running condition have been typed: *)
    E.iter (check_typed "Running condition") condition ;
    VSI.{
      default_params = parsed_params ;
      condition ;
      globals ;
      funcs =
        Hashtbl.values compiler_funcs |>
        List.of_enum }
  ) () (* and finally, delete temp files! *)

(* [program_name] is used to resolve relative parent names, and name a few
 * temp files.
 * [get_parent] is a function that returns the P.t of a given
 * N.program, used to get the output types of pre-existing
 * functions. *)
let compile conf info ~exec_file base_file (program_name : N.program) =
  let keep = conf.C.keep_temp_files in
  Files.clean_temporary ~keep (fun _add_single_temp_file add_temp_file ->
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
    let what = "program "^ (N.program_color program_name) in
    (* Start by producing a module (used by all funcs and the running_condition
     * in the casing) with the parameters: *)
    let params_obj_name =
      N.cat base_file
            (N.path ("_params_"^ RamenVersions.codegen ^".cmx")) |>
      RamenOCamlCompiler.make_valid_for_module in
    (* We need to collect all envvars used in the whole program (same as
     * the env tuple that's just been typed). Notice that the running
     * condition is excluded, as the environment of the choreographer
     * may not match that of the supervisor: *)
    let keep_temp_files = conf.C.keep_temp_files in
    let envvars =
      List.fold_left (fun envvars func ->
        List.rev_append
          (O.envvars_of_operation func.VSI.operation)
          envvars
      ) [] info.VSI.funcs |>
      List.fast_sort N.compare in
    Files.mkdir_all ~is_file:true params_obj_name ;
    let params_src_file =
      RamenOCamlCompiler.with_code_file_for
        params_obj_name conf.C.reuse_prev_files (fun oc ->
        Printf.fprintf oc "(* Parameter values for program %s *)\n\
          open Batteries\n\
          open Stdint\n\
          open RamenHelpers\n\
          open RamenNullable\n\
          open RamenLog\n\
          open RamenConsts\n"
          (program_name :> string) ;
        (* FIXME: too bad default values are encoded in the binary. We could pass
         * default value at spawning time, and use only the signature of parameter
         * types in program signature in order to reuse the binary when only the
         * default value of a parameter changes. *)
        CodeGen_OCaml.emit_parameters oc info.default_params envvars) in
    add_temp_file params_src_file ;
    RamenOCamlCompiler.compile
      conf ~keep_temp_files what params_src_file params_obj_name ;
    let params_mod_name =
      RamenOCamlCompiler.module_name_of_file_name params_src_file in
    (* Same technique to produce a module used by all funcs with all global
     * parameters with program, site or global scope: *)
    let globals = info.VSI.globals in
    let globals_obj_name =
      N.cat base_file
            (N.path ("_globals_"^ RamenVersions.codegen ^".cmx")) |>
      RamenOCamlCompiler.make_valid_for_module in
    Files.mkdir_all ~is_file:true globals_obj_name ;
    let globals_src_file =
      RamenOCamlCompiler.with_code_file_for
        globals_obj_name conf.C.reuse_prev_files (fun oc ->
        Printf.fprintf oc "(* Global variables for program %s *)\n\
          open Batteries\n\
          open Stdint\n\
          open RamenHelpers\n\
          open RamenNullable\n\
          open RamenLog\n\
          open RamenConsts\n"
          (program_name :> string) ;
        CodeGen_OCaml.GlobalVariables.emit oc globals program_name) in
    add_temp_file globals_src_file ;
    RamenOCamlCompiler.compile
      conf ~keep_temp_files what globals_src_file globals_obj_name ;
    let globals_mod_name =
      RamenOCamlCompiler.module_name_of_file_name globals_src_file in
    (* Now generate and compile the code for functions: *)
    let src_name_of_func func =
      N.cat base_file
            (N.path ("_"^ func.VSI.signature ^
                     "_"^ RamenVersions.codegen)) |>
      RamenOCamlCompiler.make_valid_for_module in
    let obj_files =
      List.fold_left (fun obj_files func ->
        let func_src_name = src_name_of_func func in
        (* Start with the C++ object file for ORC support: *)
        let orc_write_func = "orc_write_"^ func.VSI.signature
        and orc_read_func = "orc_read_"^ func.VSI.signature
        and rtyp =
          O.out_record_of_operation ~with_private:true func.VSI.operation in
        let obj_files =
          !logger.debug "Generating ORC support modules" ;
          let obj_file, _ = orc_codec conf orc_write_func orc_read_func
                                      func_src_name rtyp in
          add_temp_file obj_file ; (* Will also get rid of the "cc" file *)
          obj_file :: obj_files in
          (* Note: the OCaml wrappers will be written in the single ML
           * module generated by [CodeGen_OCaml.compile below] *)
        (* Then the Dessser helper for RowBinary parsing (and many more
         * things at a later stage) *)
        let dessser_mod_name =
          RamenOCamlCompiler.module_name_of_file_name (
            let (+) = N.cat in
            func_src_name + (N.path "_") + ObjectSuffixes.dessser_helper) in
        let is_rowbinary =
          match func.VSI.operation with
          | ReadExternal { format = RowBinary _ ; _ } -> true
          | _ -> false in
        let obj_files =
          if is_rowbinary then (
            let typ =
              (* Private fields have to be deserialized: *)
              O.out_record_of_operation ~with_private:true func.VSI.operation in
            let obj_file =
              let can_use_cpp = false in
              let compiler =
              if can_use_cpp then
                cpp_compile (CodeGen_Dessser.CPP.emit typ)
              else
                (* Fallback to OCaml backend: *)
                ocaml_compile (CodeGen_Dessser.OCaml.emit typ) in
              compiler conf func_src_name ObjectSuffixes.dessser_helper in
            add_temp_file obj_file ; (* Will also get rid of the "cc" file *)
            obj_file :: obj_files
          ) else obj_files in
        (* Then the OCaml module that implement the function operation: *)
        let obj_name = Files.add_ext func_src_name "cmx" in
        Files.mkdir_all ~is_file:true obj_name ;
        (try
          (* FIXME: make it merely an "emit_function" function and compile
           * in here: *)
          CodeGen_OCaml.compile
            conf func obj_name params_mod_name dessser_mod_name
            orc_write_func orc_read_func info.default_params envvars globals
            globals_mod_name
        with e ->
          let bt = Printexc.get_raw_backtrace () in
          let exn =
            Failure (
              Printf.sprintf2 "Cannot generate code for %s: %s"
                (func.VSI.name :> string) (Printexc.to_string e)) in
          Printexc.raise_with_backtrace exn bt) ;
        add_temp_file obj_name ;
        obj_name :: obj_files
      ) [ params_obj_name ; globals_obj_name ] info.funcs in
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
      N.cat base_file
            (N.path ("_casing_"^ RamenVersions.codegen ^".cmx")) |>
      RamenOCamlCompiler.make_valid_for_module in
    let src_file =
      RamenOCamlCompiler.with_code_file_for
        casing_obj_name conf.C.reuse_prev_files (fun oc ->
        let params = info.default_params in
        Printf.fprintf oc "(* Ramen Casing for program %s *)\n"
          (program_name :> string) ;
        CodeGen_OCaml.emit_header params_mod_name globals_mod_name oc ;
        (* Emit the running condition: *)
        CodeGen_OCaml.emit_running_condition
          oc params envvars info.VSI.condition ;
        (* Embed in the binary all info required for running it: the program
         * name, the function names, their signature, input and output types
         * and force export, and parameters default values. We
         * embed this under the shape of the typed operation, as it makes it
         * possible to also analyze the program. For simplicity, all those
         * info are also computed from the operation when we load a program. *)
        let runconf =
          VSI.{
            funcs = info.VSI.funcs ;
            default_params = params ;
            condition = info.VSI.condition ;
            globals = info.VSI.globals
          } in
        Printf.fprintf oc "let rc_marsh_ = %S\n"
          (Marshal.(to_string runconf [])) ;
        (* Then call CodeGenLib_Casing.run with all this: *)
        Printf.fprintf oc
          "let () = CodeGenLib_Casing.run %S rc_marsh_ run_condition_\n"
            RamenVersions.codegen ;
        Printf.fprintf oc "\t[\n" ;
        List.iter (fun func ->
          let mod_name =
            ((src_name_of_func func |> Files.basename) :> string) |>
            String.capitalize_ascii in
          Printf.fprintf oc
            "\t\t%S,\n\
             \t\t\t{ worker_entry_point = %s.%s ;\n\
             \t\t\t  top_half_entry_point = %s.%s ;\n\
             \t\t\t  replay_entry_point = %s.%s ;\n\
             \t\t\t  convert_entry_point = %s.%s } ;\n"
            (func.VSI.name :> string)
            mod_name EntryPoints.worker
            mod_name EntryPoints.top_half
            mod_name EntryPoints.replay
            mod_name EntryPoints.convert
        ) info.VSI.funcs ;
        Printf.fprintf oc "\t]\n"
      ) in
    (*
     * Compile the casing and link it with everything, giving a single
     * executable that can perform all the operations of this ramen program.
     *)
    let what = "program "^ N.program_color program_name in
    RamenOCamlCompiler.link conf ~keep_temp_files ~what ~obj_files
                            ~src_file ~exec_file ;
    add_temp_file src_file
  ) () (* and finally, delete temp files! *)
