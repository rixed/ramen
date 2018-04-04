(* Turn a ramen program into an executable binary.
 * This goes through several phases:
 *
 * 1. The parsing, which is done in RamenProgram, RamenOperation, RamenExpr
 *    and RamenScalar modules;
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
              supplied_value : RamenScalar.value ;
              expected_type : RamenScalar.typ }

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
                parameter_name (RamenScalar.to_string supplied_value)
                (RamenScalar.string_of_typ (RamenScalar.type_of supplied_value))
                (RamenScalar.string_of_typ expected_type))
    | _ -> None)

let entry_point_name = "start"

let compile conf root_path program_name program_code =
  let program_name = P.sanitize_name program_name in
  (*
   * If all goes well, many temporary files are going to be created. Here
   * we collect all their name so we delete them at the end:
   *)
  let temp_files = ref [] in
  let add_temp_file f = temp_files := f :: !temp_files in
  let add_temp_file f =
    add_temp_file (change_ext ".ml" f) ;
    add_temp_file (change_ext ".cmx" f) ;
    add_temp_file (change_ext ".cmi" f) ;
    add_temp_file (change_ext ".o" f) ;
    if conf.C.debug then
      add_temp_file (change_ext ".annot" f) in
  let del_temp_files () =
    if not conf.C.keep_temp_files then
      List.iter (fun fname ->
        !logger.debug "Deleting temp file %s" fname ;
        log_exceptions Unix.unlink fname
      ) !temp_files
  in
  finally del_temp_files (fun () ->
    (*
     * Now that we've set up the stage, we have to parse that program,
     * turning the program_code into a list of RamenProgram.fun:
     *)
    !logger.info "Parsing program %s" program_name ;
    let parsed_funcs : RamenProgram.func list =
      RamenProgram.parse program_code in
    (*
     * Now we have to type all of these.
     * Here we mainly construct the data required by the typer: it needs
     * parents to be a hash from child name to a list of RamenTyping.Func.t
     * (the parents), and it takes the functions as a hash of names to
     * RamenTyping.Func.c.
     *
     * The resulting types will be stored in compiler_funcs.
     *)
    !logger.info "Typing program %s" program_name ;
    let compiler_funcs = Hashtbl.create 7 in
    List.iter (fun parsed_func ->
      let fq_name = program_name ^"/"^ parsed_func.RamenProgram.name in
      let me_func =
        RamenTyping.make_untyped_func program_name
          parsed_func.name parsed_func.params parsed_func.operation in
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
      RamenOperation.parents_of_operation parsed_func.RamenProgram.operation |>
      List.map (fun parent_name ->
        (* parent_name is the name as it appears in the source, can be FQ name
         * or just a local name. We need to build a set of funcs where all
         * involved func appears only once: *)
        let parent_fq_name =
          if String.contains parent_name '/' then parent_name
          else program_name ^"/"^ parent_name in
        try Hashtbl.find compiler_funcs parent_fq_name
        with Not_found ->
          !logger.debug "Found external reference to function %s"
            parent_fq_name ;
          let parent_program_name, parent_name =
            C.program_func_of_user_string parent_fq_name in
          (* Or the parent must have been in compiler_funcs: *)
          assert (parent_program_name <> program_name) ;
          let parent_func =
            let par_rc =
              C.Program.of_program_name root_path parent_program_name in
            List.find (fun f -> f.F.name = parent_name) par_rc in
          (* Build a typed F.t from this Fun.t: *)
          RamenTyping.make_typed_func parent_program_name parent_func
      ) |>
      Hashtbl.add compiler_parents parsed_func.RamenProgram.name
    ) parsed_funcs ;
    (* Finally, call the typer: *)
    RamenTyping.set_all_types conf compiler_parents compiler_funcs ;
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
    !logger.info "Compiling for program %s" program_name ;
    let obj_files = Lwt_main.run (
      Hashtbl.values compiler_funcs |> List.of_enum |>
      Lwt_list.map_p (fun func ->
        let obj_name =
          root_path ^"/"^ program_name ^
          "_"^ func.RamenTyping.Func.signature ^".cmx" in
        mkdir_all ~is_file:true obj_name ;
        let%lwt () =
          let open RamenTyping in
          let in_typ = tuple_user_type func.Func.in_type
          and out_typ = tuple_user_type func.Func.out_type
          and operation = Option.get func.Func.operation in
          CodeGen_OCaml.compile
            conf entry_point_name func.Func.name obj_name
            in_typ out_typ func.Func.params operation
        in
        add_temp_file obj_name ;
        return obj_name)) in
    (*
     * Produce the casing.
     *
     * The casing is the OCaml "case" around the worker, that process the
     * command line argument, mostly for the ramen daemon to read, and then
     * run the worker of the designated operation (which has been compiled
     * above).
     *)
    let exec_file = C.Program.bin_of_program_name root_path program_name in
    let obj_name = root_path ^"/"^ program_name ^"_casing.cmx" in
    let src_file =
      RamenOCamlCompiler.with_code_file_for obj_name conf (fun oc ->
        Printf.fprintf oc "(* Ramen Casing for program %s *)\n"
          program_name ;
        (* Embed in the binary all info required for running it: the program
         * name, the function names, their signature, input and output types,
         * force export and merge flags, and parameters. *)
        let runconf =
          Hashtbl.values compiler_funcs |>
          Enum.map (fun func ->
            let operation = Option.get func.RamenTyping.Func.operation in
            F.{ program_name ;
                name = func.name ;
                params = func.params ;
                in_type = RamenTyping.typed_tuple_type func.in_type ;
                out_type = RamenTyping.typed_tuple_type func.out_type ;
                signature = func.signature ;
                parents = func.parents ;
                force_export = RamenOperation.is_exporting operation ;
                merge_inputs = RamenOperation.is_merging operation ;
                event_time =
                  RamenOperation.event_time_of_operation operation }
          ) |>
          List.of_enum in
        Printf.fprintf oc "let rc_str_ = %S\n"
          ((PPP.to_string C.Program.t_ppp_ocaml runconf) |>
           PPP_prettify.prettify) ;
        Printf.fprintf oc "let rc_marsh_ = %S\n"
          (Marshal.(to_string runconf [])) ;
        (* Then call CodeGenLib.casing with all this: *)
        Printf.fprintf oc
          "let () = CodeGenLib.casing rc_str_ rc_marsh_ [\n" ;
        Hashtbl.iter (fun _ func ->
          assert (program_name.[String.length program_name-1] <> '/') ;
          Printf.fprintf oc"\t%S, %s_%s.%s ;\n"
            func.RamenTyping.Func.name
            (String.capitalize_ascii (Filename.basename program_name))
            func.RamenTyping.Func.signature
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
