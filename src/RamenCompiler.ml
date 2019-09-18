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
module RC = C.Running
module F = C.Func
module P = C.Program
module FS = F.Serialized
module PS = P.Serialized
module E = RamenExpr
module O = RamenOperation
module N = RamenName
module Orc = RamenOrc
module Files = RamenFiles
module ZMQClient = RamenSyncZMQClient
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

(* ORC codec C++ module generator: *)
let orc_codec conf orc_write_func orc_read_func prefix_name rtyp =
  let debug = conf.C.log_level = Debug in
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
      N.path_cat [ conf.bundle_dir ; N.path "include" ] in
    Printf.sprintf2 "%s%s -std=c++17 -W -Wall -c -I %s -I %s -o %s %s"
      (* No quote as it might be a command line: *)
      RamenCompilConfig.cpp_compiler
      (if debug then " -g" else "")
      (shell_quote (N.path_cat [ conf.bundle_dir ; N.path "ocaml" ] :> string))
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
  if rce.RC.status <> MustRun then raise Not_found else get_rc ()

(* TODO: a way to have the site in the program name: *)
(* FIXME: we should ask directly for a function instead of a program, if
 * only to disambiguate the case when several workers are running from
 * different sources. *)
let parent_from_confserver clt (pn : N.program) =
  let open RamenSync in
  (* Since we do not know which site this is about, look for all of them: *)
  let exception Return of N.path in
  let find_parent k hv =
    match k, hv.Client.value with
    | Key.PerSite (_site, (PerWorker (fq, Worker))),
      Worker worker ->
        (* As we do not know which function we are interested about, assume
         * for now that they all use the same program info (See FIXME above). *)
        let prog_name, _func_name = N.fq_parse fq in
        if prog_name = pn then (
          !logger.debug "Found worker running program %a in %a with src_path %a"
            N.program_print pn
            Key.print k
            N.path_print worker.src_path ;
          raise (Return worker.src_path))
    | _ ->
        () in
  match Client.iter clt find_parent with
  | exception Return src_path ->
      let info_key = Key.Sources (src_path, "info") in
      !logger.debug "Looking for program info in %a" Key.print info_key ;
      (match (Client.find clt info_key).value with
      | Value.SourceInfo { detail = Compiled info } ->
          P.unserialized pn info
      | _ ->
          raise Not_found)
  | () ->
      raise Not_found

(*
 * (Pre)Compilation creates a lot of temporary files. Here
 * we collect all their name so we delete them at the end:
 *)
let clean_temporary_files conf f =
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
    f add_single_temp_file add_temp_file)

(* [program_name] is used to resolve relative parent names, and name a few
 * temp files.
 * [get_parent] is a function that returns the P.t of a given
 * N.program, used to get the output types of pre-existing
 * functions. *)
let precompile conf get_parent source_file (program_name : N.program) =
  let program_code = Files.read_whole_file source_file in
  clean_temporary_files conf (fun add_single_temp_file _add_temp_file ->
    (*
     * First thing is to parse that program,
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
            retention = parsed_func.retention ;
            is_lazy = parsed_func.is_lazy ;
            doc = parsed_func.doc ;
            operation = op ;
            in_type = RamenFieldMaskLib.in_type_of_operation op ;
            signature = "" ; (* set later by finalize_func *)
            parents = O.parents_of_operation op ;
            merge_inputs = O.is_merging op } in
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
(*
    (* We need to reify in_type and out_type for each function so that we
     * can update their units and let the unit computation reach a fixed
     * point. So let's reify them here: *)
    let input_of_func =
      let h = Hashtbl.create 5 in
      fun func ->
        try Hashtbl.find h func.FS.name
        with Not_found ->
          let inp =
            RamenFieldMaskLib.in_type_of_operation func.FS.operation in
          Hashtbl.add h func.FS.name inp ;
          inp
    and output_of_func =
      let h = Hashtbl.create 5 in
      fun func ->
        try Hashtbl.find h func.FS.name
        with Not_found ->
          let out =
            O.out_type_of_operation ~with_private:true func.FS.operation in
          Hashtbl.add h func.FS.name out ;
          out in *)
    let units_of_output func name =
      !logger.debug "Looking for units of output field %a in %S"
        N.field_print name
        (func.F.name :> string) ;
      let out_type =
        O.out_type_of_operation ~with_private:true func.F.operation in
(*        (func.FS.name :> string) ;
     let out_type = output_of_func func in *)
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
(*      if units <> None then (
        let inp = input_of_func func in
        patch_in_typ field units inp) ;*)
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
          O.out_type_of_operation ~with_private:true func.F.operation in
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
      List.exists (fun parent ->
        let parent_prog_name, parent_func_name = parent_prog_func parent in
        let parent_fq =
          N.fq_of_program parent_prog_name parent_func_name in
        List.mem parent_fq fs
      ) func.F.parents in
    let finalize fname =
      let func = Hashtbl.find compiler_funcs fname in
      finalize_func compiler_parents parsed_params func in
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
    PS.{
      default_params = parsed_params ;
      condition ;
      funcs =
        Hashtbl.values compiler_funcs /@
        F.serialized |>
        List.of_enum }
  ) () (* and finally, delete temp files! *)

(* [program_name] is used to resolve relative parent names, and name a few
 * temp files.
 * [get_parent] is a function that returns the P.t of a given
 * N.program, used to get the output types of pre-existing
 * functions. *)
let compile conf info ~exec_file base_file (program_name : N.program) =
  clean_temporary_files conf (fun _add_single_temp_file add_temp_file ->
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
     * the env tuple that's just been typed): *)
    let envvars =
      List.fold_left (fun envvars func ->
        List.rev_append
          (O.envvars_of_operation func.FS.operation)
          envvars
      ) [] info.PS.funcs |>
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
         * types in program signature in order to reuse the bonary when only the
         * default value of a parameter changes. *)
        CodeGen_OCaml.emit_parameters oc info.default_params envvars) in
    add_temp_file params_src_file ;
    let keep_temp_files = conf.C.keep_temp_files in
    RamenOCamlCompiler.compile
      conf ~keep_temp_files what params_src_file params_obj_name ;
    let params_mod_name =
      RamenOCamlCompiler.module_name_of_file_name params_src_file in
    let src_name_of_func func =
      N.cat base_file
            (N.path ("_"^ func.FS.signature ^
                     "_"^ RamenVersions.codegen)) |>
      RamenOCamlCompiler.make_valid_for_module in
    let obj_files =
      List.fold_left (fun obj_files func ->
        (* Start with the C++ object file for ORC support: *)
        let orc_write_func = "orc_write_"^ func.FS.signature
        and orc_read_func = "orc_read_"^ func.FS.signature
        and rtyp =
          O.out_record_of_operation ~with_private:true func.FS.operation in
        let obj_files =
          !logger.debug "Generating ORC support modules" ;
          let obj_file, _ = orc_codec conf orc_write_func orc_read_func
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
            info.default_params envvars
        with e ->
          let bt = Printexc.get_raw_backtrace () in
          let exn =
            Failure (
              Printf.sprintf2 "Cannot generate code for %s: %s"
                (func.FS.name :> string) (Printexc.to_string e)) in
          Printexc.raise_with_backtrace exn bt) ;
        add_temp_file obj_name ;
        obj_name :: obj_files
      ) [ params_obj_name ] info.funcs in
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
        CodeGen_OCaml.emit_header params_mod_name oc ;
        (* Emit the running condition: *)
        CodeGen_OCaml.emit_running_condition
          oc params envvars info.PS.condition ;
        (* Embed in the binary all info required for running it: the program
         * name, the function names, their signature, input and output types,
         * force export and merge flags, and parameters default values. We
         * embed this under the shape of the typed operation, as it makes it
         * possible to also analyze the program. For simplicity, all those
         * info are also computed from the operation when we load a program. *)
        let runconf =
          PS.{
            funcs = info.PS.funcs ;
            default_params = params ;
            condition = info.PS.condition
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
            (func.FS.name :> string)
            mod_name EntryPoints.worker
            mod_name EntryPoints.top_half
            mod_name EntryPoints.replay
            mod_name EntryPoints.convert
        ) info.PS.funcs ;
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
