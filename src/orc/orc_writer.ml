(* Small program to test the ORC writing facility:
 * Requires the string representation of a ramen type as a command line
 * argument, then writes and compiles an ORC writer for that format, then
 * reads from stdin string representation of ramen values and write them,
 * until EOF when it exits (C++ OrcHandler being deleted and therefore the
 * ORC file flushed). *)
open Batteries
open RamenHelpers
open RamenLog
module T = RamenTypes
module Orc = RamenOrc

let main =
  init_logger Debug ;
  let exec_file = Sys.argv.(1) in
  let ramen_type = Sys.argv.(2) in
  let rtyp = PPP.of_string_exc T.t_ppp_ocaml ramen_type in
  !logger.info "Generating an ORC writer for Ramen type %s"
    (IO.to_string T.print_typ rtyp |> abbrev 130) ;
  (* We need to generate two things from this type:
   * - a string parser (using [CodeGen_OCaml.emit_value_of_string])
   * - a ORC writer (using [Orc.emit_write_value]).
   * Then we compile this into a program that write stdin into an ORC file. *)
  !logger.info "Corresponding runtime type: %a"
    CodeGen_OCaml.otype_of_type rtyp ;
  let otyp = Orc.of_structure rtyp.T.structure in
  let schema = IO.to_string Orc.print otyp in
  !logger.info "Corresponding ORC type: %s" schema ;
  RamenOCamlCompiler.use_external_compiler := false ;
  RamenOCamlCompiler.bundle_dir := Sys.getenv_opt "RAMEN_LIBS" |? "./bundle" ;
  (*
   * Generate the C++ side:
   *)
  let orc_write_func = "orc_write" in
  let orc_read_func = "orc_read" in
  let cc_src_file =
    Filename.temp_file "orc_writer_cc_" ".cc" in
  mkdir_all ~is_file:true cc_src_file ;
  File.with_file_out cc_src_file (fun oc ->
    Orc.emit_intro oc ;
    Orc.emit_write_value orc_write_func rtyp oc ;
    Orc.emit_read_values orc_read_func rtyp oc ;
    Orc.emit_outro oc) ;
  !logger.info "Generated C++ support file in %s" cc_src_file ;
  let cpp_command src dst =
    let _, where = Unix.run_and_read "ocamlc -where" in
    let where = String.trim where in
    Printf.sprintf "g++ -g -std=c++17 -W -Wall -c -I %S -o %S %S"
      where dst src in
  let cc_dst = change_ext ".o" cc_src_file in
  let cmd = cpp_command cc_src_file cc_dst in
  let status = Unix.system cmd in
  if status <> Unix.WEXITED 0 then (
    !logger.error "Compilation of %S: %s"
      cc_src_file (string_of_process_status status) ;
    !logger.info "I'm so sorry! This is what I tried: %s" cmd ;
    !logger.info "I'm out of ideas. Good luck!" ;
    exit 1) ;
  (*
   * Now the ML side:
   *)
  let ml_obj_name =
    Filename.temp_file "orc_writer_" ".cmx" |>
    RamenOCamlCompiler.make_valid_for_module in
  let keep_temp_files = true in
  let ml_src_file =
    RamenOCamlCompiler.with_code_file_for ml_obj_name keep_temp_files (fun oc ->
      let p fmt = Printf.fprintf oc (fmt^^"\n") in
      p "open Batteries" ;
      p "open Stdint" ;
      p "open RamenHelpers" ;
      p "open RamenNullable" ;
      p "open RamenLog" ;
      p "" ;
      p "let value_of_string str =" ;
      p "  check_parse_all str (" ;
      let emit_is_null fins str_var offs_var oc =
        Printf.fprintf oc
          "if looks_like_null ~offs:%s %s &&
            string_is_term %a %s (%s + 4) then \
         true, %s + 4 else false, %s"
        offs_var str_var
        (List.print char_print_quoted) fins str_var offs_var
        offs_var offs_var in
      CodeGen_OCaml.emit_value_of_string 2 rtyp "str" "0" emit_is_null [] oc ;
      p "  )" ;
      p "" ;
      p "let string_of_value v =" ;
      CodeGen_OCaml.emit_string_of_value 1 rtyp "v" oc ;
      p "" ;
      p "(* A handler to be passed to the function generated by" ;
      p "   emit_write_value: *)" ;
      p "type handler" ;
      p "" ;
      p "external orc_write : handler -> %a -> float -> float -> unit = %S"
        CodeGen_OCaml.otype_of_type rtyp
        orc_write_func ;
      p "external orc_read : string -> int -> (%a -> unit) -> (int * int) = %S"
        CodeGen_OCaml.otype_of_type rtyp
        orc_read_func ;
      (* Destructor do not seems to be called when the OCaml program exits: *)
      p "external orc_close : handler -> unit = \"orc_handler_close\"" ;
      p "" ;
      p "(* Parameters: schema * path * row per batch * batches per file *)" ;
      p "external orc_make_handler : string -> string -> int -> int -> bool -> handler =" ;
      p "  \"orc_handler_create_bytecode_lol\" \"orc_handler_create\"" ;
      p "" ;
      p "let main =" ;
      p "  let syntax () =" ;
      p "    !logger.error \"%%s [read|write] file.orc\" Sys.argv.(0) ;" ;
      p "    exit 1 in" ;
      p "  let batch_size = 1000 and num_batches = 100 in" ;
      p "  if Array.length Sys.argv <> 3 then syntax () ;" ;
      p "  let orc_fname = Sys.argv.(2) in" ;
      p "  match String.lowercase_ascii Sys.argv.(1) with" ;
      p "  | \"read\" | \"r\" ->" ;
      p "      let cb x =" ;
      p "        Printf.printf \"%%s\\n\" (string_of_value x) in" ;
      p "      let lines, errs = orc_read orc_fname batch_size cb in" ;
      p "      (if errs > 0 then !logger.error else !logger.debug)" ;
      p "        \"Read %%d lines (%%d errors)\" lines errs" ;
      p "  | \"write\" | \"w\" ->" ;
      p "      let handler =" ;
      p "        orc_make_handler %S orc_fname batch_size num_batches in"
        schema ;
      p "      (try forever (fun () ->" ;
      p "            let tuple = read_line () |> value_of_string in" ;
      p "            orc_write handler tuple 0. 0." ;
      p "          ) ()" ;
      p "      with End_of_file ->" ;
      p "        !logger.info \"Exiting...\" ;" ;
      p "        orc_close handler)" ;
      p "  | _ -> syntax ()" ;
  ) in
  !logger.info "Generated OCaml support module in %s" ml_src_file ;
  (*
   * Link!
   *)
  let obj_files = [ cc_dst ] in
  RamenOCamlCompiler.link ~debug:true ~keep_temp_files ~what:"ORC writer"
                          ~obj_files ~src_file:ml_src_file ~exec_file
