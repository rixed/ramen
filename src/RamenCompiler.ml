(* Turn a ramen program into an executable binary.
 * This goes through several phases:
 *
 * 1. The parsing, which is done in RamenProgram, RamenOperation, RamenExpr
 *    and RamenTypes modules;
 * 2. The typing, happening in RamenTyping;
 * 3. The code generation, taking place in CodeGen_Ocaml;
 * 4. And finally generating an executable (takes place in RamenOCamlCompiler).
 *)

open Batteries
open Lwt
open RamenHelpers
open RamenLog
module C = RamenConf
module F = RamenConf.Func
module P = RamenConf.Program

exception LinkingError of
            { parent_program : string ; parent_function : string ;
              child_program : string ; child_function : string ;
              parent_out : RamenTuple.typed_tuple ;
              child_in : RamenTuple.typed_tuple ;
              msg : string }
exception InvalidParameter of
            { parameter_name : string ;
              supplied_value : RamenTypes.value ;
              expected_type : RamenTypes.typ }

let () =
  Printexc.register_printer (function
    | LinkingError { parent_program ; parent_function ; parent_out ;
                     child_program ; child_function ; child_in ; msg } ->
        Some (Printf.sprintf "Linking error from %s/%s (%s) to %s/%s (%s): %s"
                parent_program parent_function
                (IO.to_string RamenTuple.print_typ parent_out.ser)
                child_program child_function
                (IO.to_string RamenTuple.print_typ child_in.ser) msg)
    | InvalidParameter { parameter_name ; supplied_value ; expected_type } ->
        Some (Printf.sprintf "Invalid type for parameter %S: \
                              value supplied (%s) of type %s \
                              but expected a %s"
                parameter_name (RamenTypes.to_string supplied_value)
                (RamenTypes.string_of_typ (RamenTypes.type_of supplied_value))
                (RamenTypes.string_of_typ expected_type))
    | _ -> None)

let entry_point_name = "start"

let compile conf root_path program_name program_code =
  (*
   * If all goes well, many temporary files are going to be created. Here
   * we collect all their name so we delete them at the end:
   *)
  let temp_files = ref Set.empty in
  let add_temp_file f = temp_files := Set.add f !temp_files in
  let add_temp_file f =
    add_temp_file (change_ext ".ml" f) ;
    add_temp_file (change_ext ".cmx" f) ;
    add_temp_file (change_ext ".cmi" f) ;
    add_temp_file (change_ext ".o" f) ;
    add_temp_file (change_ext ".annot" f) in
  let del_temp_files () =
    if not conf.C.keep_temp_files then
      Set.iter (fun fname ->
        !logger.debug "Deleting temp file %s" fname ;
        log_exceptions Unix.unlink fname
      ) !temp_files
  in
  finally del_temp_files (fun () ->
    (*
     * Now that we've set up the stage, we have to parse that program,
     * turning the program_code into a list of RamenProgram.fun:
     *)
    !logger.info "Parsing program %s"
      (RamenName.string_of_program program_name) ;
    let parsed_params, parsed_funcs = RamenProgram.parse program_code in
    (*
     * Now we have to type all of these.
     * Here we mainly construct the data required by the typer: it needs
     * parents to be a hash from child name to a list of RamenTyping.Func.t
     * (the parents), and it takes the functions as a hash of names to
     * RamenTyping.Func.c.
     *
     * The resulting types will be stored in compiler_funcs.
     *)
    !logger.info "Typing program %s"
      (RamenName.string_of_program program_name) ;
    let compiler_funcs = Hashtbl.create 7 in
    List.iter (fun parsed_func ->
      let fq_name = RamenName.string_of_program program_name ^"/"^
                    RamenName.string_of_func parsed_func.RamenProgram.name in
      (* During compilation we do not care about actual values of params: *)
      let params = [] in
      let me_func =
        RamenTyping.make_untyped_func program_name
          parsed_func.name params parsed_func.operation in
      !logger.debug "Found function %s" fq_name ;
      Hashtbl.add compiler_funcs fq_name me_func
    ) parsed_funcs ;
    (* Now we have two types of parents: those from this program, that
     * have been created in compiler_funcs above, and those of already
     * compiled programs that have to be present on disk in the same
     * $RAMEN_ROOT. Note that we do not look at the running configuration,
     * as we want to compile a program against a RAMEN_ROOT not against a
     * currently running instance.
     * This forces the user to compile programs in a given order. *)
    (* Has from function name to RamenTyping.Func.t list of parents: *)
    let compiler_parents = Hashtbl.create 7 in
    List.iter (fun parsed_func ->
      RamenOperation.parents_of_operation
        parsed_func.RamenProgram.operation |>
      List.map (fun parent_id ->
        (* parent_id is the name as it appears in the source, can be FQed
         * or just a local name. We need to build a set of funcs where all
         * involved func appears only once: *)
        let parent_id = match parent_id with
          | None, func_name -> Some (program_name, []), func_name
          | pn -> pn in
        let parent_prog_name, _parent_prog_params =
          Option.get (fst parent_id)
        and parent_func_name = snd parent_id in
        (* For compiling, we want the 'virtual' parents with no
         * parameters as we are interested only in their output
         * type: *)
        let parent_name = RamenName.string_of_program parent_prog_name ^"/"^
                          RamenName.string_of_func parent_func_name in
        try Hashtbl.find compiler_funcs parent_name
        with Not_found ->
          !logger.debug "Found external reference to function %a"
            RamenLang.print_expansed_function parent_id ;
          (* Or the parent must have been in compiler_funcs: *)
          if parent_prog_name = program_name then
            raise (RamenLang.SyntaxError (UnknownFunc parent_name)) ;
          let parent_func =
            let par_rc =
              P.bin_of_program_name root_path (fst (Option.get (fst parent_id))) |>
              P.of_bin [] in
            List.find (fun f ->
              f.F.name = parent_func_name
            ) par_rc.P.funcs in
          (* Build a typed F.t from this Fun.t: *)
          RamenTyping.make_typed_func parent_prog_name parent_func
      ) |>
      Hashtbl.add compiler_parents parsed_func.RamenProgram.name
    ) parsed_funcs ;
    (* Finally, call the typer: *)
    RamenTyping.set_all_types
      conf compiler_parents compiler_funcs parsed_params ;
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
    let obj_files = Lwt_main.run (
      Hashtbl.values compiler_funcs |> List.of_enum |>
      Lwt_list.map_p (fun func ->
        let obj_name =
          root_path ^"/"^ RamenName.path_of_program program_name ^
          "_"^ func.RamenTypingHelpers.Func.signature ^
          "_"^ RamenVersions.codegen ^".cmx" in
        mkdir_all ~is_file:true obj_name ;
        Lwt.catch (fun () ->
          let open RamenTyping in
          let in_typ = tuple_user_type func.Func.in_type
          and out_typ = tuple_user_type func.Func.out_type
          and operation = Option.get func.Func.operation in
          CodeGen_OCaml.compile
            conf entry_point_name func.Func.name obj_name
            in_typ out_typ parsed_params operation)
          (fun e ->
            !logger.error "Cannot generate code for %s: %s"
              (RamenName.string_of_func func.name)
              (Printexc.to_string e) ;
            fail e) ;%lwt
        add_temp_file obj_name ;
        return obj_name)) in
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
    let exec_file = P.bin_of_program_name root_path program_name
    and pname = RamenName.string_of_program program_name in
    let obj_name = root_path ^"/"^ RamenName.path_of_program program_name
                   ^"_casing_"^ RamenVersions.codegen ^".cmx" in
    let src_file =
      RamenOCamlCompiler.with_code_file_for obj_name conf (fun oc ->
        Printf.fprintf oc "(* Ramen Casing for program %s *)\n"
          (RamenName.string_of_program program_name) ;
        (* Embed in the binary all info required for running it: the program
         * name, the function names, their signature, input and output types,
         * force export and merge flags, and parameters default values. *)
        let funcs =
          Hashtbl.values compiler_funcs |>
          Enum.map (fun func ->
            let operation = Option.get func.RamenTyping.Func.operation in
            F.{ (* exp_program_name will be overwritten at load time: *)
                exp_program_name =
                  RamenName.program_exp_of_program program_name ;
                name = func.name ;
                in_type = RamenTyping.typed_tuple_type func.in_type ;
                out_type = RamenTyping.typed_tuple_type func.out_type ;
                signature = func.signature ;
                parents = func.parents ;
                merge_inputs = RamenOperation.is_merging operation ;
                event_time = func.event_time ;
                factors = func.factors ;
                envvars = func.envvars }
          ) |>
          List.of_enum
        and params = parsed_params in
        let runconf = P.{ name = program_name ; funcs ; params } in
        Printf.fprintf oc "let rc_str_ = %S\n"
          ((PPP.to_string P.t_ppp_ocaml runconf) |>
           PPP_prettify.prettify) ;
        Printf.fprintf oc "let rc_marsh_ = %S\n"
          (Marshal.(to_string runconf [])) ;
        (* Then call CodeGenLib.casing with all this: *)
        Printf.fprintf oc
          "let () = CodeGenLib.casing %S rc_str_ rc_marsh_ [\n"
            RamenVersions.codegen ;
        Hashtbl.iter (fun _ func ->
          assert (pname.[String.length pname-1] <> '/') ;
          Printf.fprintf oc"\t%S, %s_%s_%s.%s ;\n"
            (RamenName.string_of_func func.RamenTyping.Func.name)
            (String.capitalize_ascii (Filename.basename pname))
            func.RamenTyping.Func.signature
            RamenVersions.codegen
            entry_point_name
        ) compiler_funcs ;
        Printf.fprintf oc "]\n") in
    add_temp_file src_file ;
    (*
     * Compile the casing and link it with everything, giving a single
     * executable that can perform all the operations of this ramen program.
     *)
    Lwt_main.run (
      RamenOCamlCompiler.link conf program_name obj_files src_file exec_file)
  ) () (* and finally, delete temp files! *)
