(* Turn a ramen program into an executable binary.
 * This goes through several phases:
 *
 * 1. The parsing, which is done in RamenProgram, RamenOperation, RamenExpr
 *    and RamenTypes modules;
 * 2. The typing, happening in RamenSmtTyping;
 * 3. The code generation, taking place in CodeGen_Ocaml;
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

let entry_point_name = "start"

let init use_external_compiler bundle_dir max_simult_compils smt_solver =
  RamenOCamlCompiler.use_external_compiler := use_external_compiler ;
  RamenOCamlCompiler.bundle_dir := bundle_dir ;
  AtomicCounter.set RamenOCamlCompiler.max_simult_compilations
                    max_simult_compils ;
  RamenSmtTyping.smt_solver := smt_solver

(* Given a program name, retrieve its binary, either form the disk or
 * the running configuration: *)

let parent_from_root_path root_path pn =
  P.bin_of_program_name root_path pn |> P.of_bin (Hashtbl.create 0)

let parent_from_programs programs pn =
  (Hashtbl.find programs pn |> snd) ()

let compile conf root_path get_parent ?exec_file
            program_name program_code =
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
    add_single_temp_file (change_ext ".annot" f) in
  let del_temp_files () =
    if not conf.C.keep_temp_files then
      Set.iter (fun fname ->
        !logger.debug "Deleting temp file %s" fname ;
        log_and_ignore_exceptions Unix.unlink fname
      ) !temp_files
  in
  finally del_temp_files (fun () ->
    (*
     * Now that we've set up the stage, we have to parse that program,
     * turning the program_code into a list of RamenProgram.fun:
     *)
    !logger.info "Parsing program %s"
      (RamenName.string_of_program program_name) ;
    let parsed_params, parsed_funcs =
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
            doc = parsed_func.doc ;
            in_type = in_type_of_operation op ;
            out_type = out_type_of_operation op ;
            signature = "" ;
            parents = parents_of_operation op ;
            merge_inputs = is_merging op ;
            event_time = event_time_of_operation op ;
            factors = factors_of_operation op ;
            envvars = envvars_of_operation op } in
      let fq_name = RamenName.fq program_name name in
      Hashtbl.add compiler_funcs fq_name (me_func, op)
    ) parsed_funcs ;
    (* Now we have two types of parents: those from this program, that
     * have been created in compiler_funcs above, and those of already
     * compiled programs that have to be present on disk in the same
     * $RAMEN_ROOT. Note that we do not look at the running configuration,
     * as we want to compile a program against a RAMEN_ROOT not against a
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
        try Hashtbl.find compiler_funcs parent_name |> fst
        with Not_found ->
          !logger.debug "Found external reference to function %a"
            F.print_parent parent ;
          (* Or the parent must have been in compiler_funcs: *)
          assert (parent_prog_name <> program_name) ;
          let par_rc = get_parent parent_prog_name in
          List.find (fun f ->
            f.F.name = parent_func_name
          ) par_rc.P.funcs
      ) |>
      Hashtbl.add compiler_parents
                  (Option.get parsed_func.RamenProgram.name)
    ) parsed_funcs ;

    (*
     * Now that we know the input/output fields of each function
     * we are ready to set the units for all inputs and outputs.
     *)
    (* Find this field in this tuple and patch it: *)
    let patch_typ field units typ =
      match List.find (fun ft ->
              ft.RamenTuple.typ_name = field
            ) typ with
      | exception Not_found ->
          assert (is_private_field field)
      | ft ->
          if ft.units = None then (
            !logger.debug "Set type of output field %S to %a"
              field (Option.print RamenUnits.print) units ;
            ft.units <- units) in
    let units_of_output func name =
      !logger.debug "Looking for units of output field %S in %S"
        name (RamenName.string_of_func func.F.name) ;
      match List.find (fun ft ->
              ft.RamenTuple.typ_name = name
            ) func.F.out_type with
        | exception Not_found ->
            !logger.error "In function %s: no such input field %S (have %a)"
              (RamenName.func_color func.F.name)
              name
              RamenTuple.print_typ_names func.F.out_type ;
            None
        | ft ->
            !logger.debug "found typed units: %a"
              (Option.print RamenUnits.print) ft.units ;
            ft.units in
    (* Same as above, but look for the units in all funcs (and check they
     * use the same): *)
    let units_of_input func parents field =
      let what =
        Printf.sprintf "Field %S in parents of %s"
          field
          (RamenName.func_color func.F.name) in
      let units =
        (List.enum parents /@
         (fun f -> units_of_output f field)) |>
        RamenUnits.check_same_units ~what None in
      (* Patch the input type: *)
      if units <> None then
        patch_typ field units func.F.in_type ;
      units in
    let set_units func op =
      let parents =
        Hashtbl.find_default compiler_parents func.F.name [] in
      let uoi = units_of_input func parents in
      let uoo = units_of_output func in
      let changed =
        RamenOperation.fold_top_level_expr false (fun changed what e ->
          let t = RamenExpr.typ_of e in
          if t.RamenExpr.units = None then (
            let u =
              let ctx =
                Printf.sprintf "evaluating units of %s in function %s"
                  what
                  (RamenName.func_color func.F.name) in
              fail_with_context ctx (fun () ->
                RamenExpr.units_of_expr uoi uoo e) in
            if u <> None then (
              !logger.debug "Set units of %a to %a"
                (RamenExpr.print false) e
                RamenUnits.print (Option.get u) ;
              t.units <- u ;
              true
            ) else changed
          ) else changed
        ) op in
      (* TODO: check that various operations supposed to accept times or
       * durations come with either no units or the expected one. *)
      (* Now that we have found the units of some expressions, patch the
       * units in the out_type. This is made uglier than necessary because
       * out_types fields are reordered. *)
      if changed then (
        match op with
        | RamenOperation.Aggregate { fields ; _ } ->
            List.iter (fun sf ->
              let units = RamenExpr.(typ_of sf.RamenOperation.expr).units in
              if units <> None then
                patch_typ sf.alias units func.F.out_type
            ) fields
        | _ -> ()) ;
      changed
    in
    if not (reach_fixed_point (fun () ->
        Hashtbl.fold (fun _ (func, op) changed ->
          set_units func op || changed
        ) compiler_funcs false)) then
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
    let open RamenSmtTyping in
    let smt2_file = C.smt_file root_path program_name in
    add_single_temp_file smt2_file ;
    let types =
      call_typer !RamenSmtTyping.smt_solver (fun () ->
        get_types conf compiler_parents compiler_funcs
                  parsed_params smt2_file) in
    apply_types compiler_parents compiler_funcs types ;
    Hashtbl.iter (fun _ (func, op) ->
      finalize_func conf compiler_parents parsed_params func op
    ) compiler_funcs ;

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
    let path_of_module p =
      let prog_path = RamenName.path_of_program p in
      let dirname, basename =
        try String.rsplit ~by:"/" prog_path
        with Not_found -> "", prog_path in
      let basename = RamenOCamlCompiler.to_module_name basename in
      dirname ^(if dirname <> "" then "/" else "")^ basename
    in
    let obj_files =
      Hashtbl.fold (fun _ (func, op) lst ->
        let obj_name =
          root_path ^"/"^ path_of_module program_name ^
          "_"^ func.F.signature ^
          "_"^ RamenVersions.codegen ^".cmx" in
        mkdir_all ~is_file:true obj_name ;
        (try
          CodeGen_OCaml.compile
            conf entry_point_name func.F.name obj_name
            func.F.in_type func.F.out_type parsed_params op
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
    (*
     * Produce the casing.
     *
     * The casing is the OCaml "case" around the worker, that process the
     * command line argument, mostly for the ramen daemon to read, and then
     * run the worker of the designated operation (which has been compiled
     * above).
     *)
    let exec_file =
      Option.default_delayed (fun () ->
        P.bin_of_program_name root_path program_name
      ) exec_file
    and pname = RamenName.string_of_program program_name in
    let obj_name = root_path ^"/"^ path_of_module program_name
                   ^"_casing_"^ RamenVersions.codegen ^".cmx" in
    let src_file =
      RamenOCamlCompiler.with_code_file_for obj_name conf (fun oc ->
        Printf.fprintf oc "(* Ramen Casing for program %s *)\n"
          (RamenName.string_of_program program_name) ;
        (* Suppress private fields from function output type, purely for
         * aesthetic reasons. Won't change anything since
         * RingBufLib.ser_tuple_typ_of_tuple_typ will ignore them anyway: *)
        Hashtbl.iter (fun _ (func, _op) ->
          func.F.out_type <- List.filter (fun ft ->
            not (is_private_field ft.RamenTuple.typ_name)) func.F.out_type
        ) compiler_funcs ;
        (* Embed in the binary all info required for running it: the program
         * name, the function names, their signature, input and output types,
         * force export and merge flags, and parameters default values. *)
        let funcs = Hashtbl.values compiler_funcs /@ fst |> List.of_enum
        and params = parsed_params in
        let runconf = P.{ default_name = program_name ; funcs ; params } in
        Printf.fprintf oc "let rc_str_ = %S\n"
          ((PPP.to_string P.t_ppp_ocaml runconf) |>
           PPP_prettify.prettify) ;
        Printf.fprintf oc "let rc_marsh_ = %S\n"
          (Marshal.(to_string runconf [])) ;
        (* Then call CodeGenLib_Casing.run with all this: *)
        Printf.fprintf oc
          "let () = CodeGenLib_Casing.run %S rc_str_ rc_marsh_ [\n"
            RamenVersions.codegen ;
        Hashtbl.iter (fun _ (func, _op) ->
          assert (pname.[String.length pname-1] <> '/') ;
          Printf.fprintf oc"\t%S, %s_%s_%s.%s ;\n"
            (RamenName.string_of_func func.F.name)
            (String.capitalize_ascii
              (Filename.basename pname |>
               RamenOCamlCompiler.to_module_name))
            func.F.signature
            RamenVersions.codegen
            entry_point_name
        ) compiler_funcs ;
        Printf.fprintf oc "]\n") in
    add_temp_file src_file ;
    (*
     * Compile the casing and link it with everything, giving a single
     * executable that can perform all the operations of this ramen program.
     *)
    RamenOCamlCompiler.link conf program_name obj_files src_file exec_file
  ) () (* and finally, delete temp files! *)
