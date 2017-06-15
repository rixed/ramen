(* Code generator for OCaml.
 * We do not use a templating system because thanks to libraries the generated
 * code should be minimal and limited to the less constant pieces of code.
 *)

(* Each operation must be implemented for OCaml which is used both as a
 * prototyping language and as a reference implementation. Some operations may
 * also exist for other languages.
 *)

(* Regarding generated code names: all generated OCaml identifier has a name
 * ending with underscore.  In addition, tuple field names are prefixed by
 * "f_". *)

open Batteries
open CodeGenLib_Misc
open Log

(* Tuple deconstruction as a function parameter: *)
let id_of_field_name name = "f_"^ name ^"_"

let print_tuple_deconstruct =
  let print_field fmt field_typ =
      String.print fmt (id_of_field_name field_typ.Lang.Tuple.name)
  in
  List.print ~first:"(" ~last:")" ~sep:", " print_field

let emit_sersize_of_field typ oc var =
  let open Lang.Scalar in
  match typ with
  | TFloat  -> Int.print oc (round_up_to_rb_word 8)
  | TString -> Printf.fprintf oc "(%d + CodeGenLib_Misc.round_up_to_rb_word(String.length %s))" rb_word_bytes var
  | TBool | TU8 | TI8 -> Int.print oc (round_up_to_rb_word 1)
  | TU16 | TI16 -> Int.print oc (round_up_to_rb_word 2)
  | TU32 | TI32 -> Int.print oc (round_up_to_rb_word 4)
  | TU64 | TI64 -> Int.print oc (round_up_to_rb_word 8)
  | TU128 | TI128 -> Int.print oc (round_up_to_rb_word 16)

let id_of_typ typ =
  let open Lang.Scalar in
  match typ with
  | TFloat  -> "float"
  | TString -> "string"
  | TBool   -> "bool"
  | TU8     -> "u8"
  | TU16    -> "u16"
  | TU32    -> "u32"
  | TU64    -> "u64"
  | TU128   -> "u128"
  | TI8     -> "i8"
  | TI16    -> "i16"
  | TI32    -> "i32"
  | TI64    -> "i64"
  | TI128   -> "i128"
 
let emit_value_of_string typ oc var =
  Printf.fprintf oc "CodeGenLib.%s_of_string %s" (id_of_typ typ) var

let nullmask_bytes_of_tuple_typ tuple_typ =
  let open Lang.Tuple in
  List.fold_left (fun s field_typ ->
    if field_typ.nullable then s+1 else s) 0 tuple_typ |>
  bytes_for_bits |>
  round_up_to_rb_word

let emit_sersize_of_tuple oc tuple_typ =
  let open Lang.Tuple in
  (* For nullable fields each ringbuf record has a bitmask of as many bits as
   * there are nullable fields, rounded to the greater or equal multiple of rb_word_size.
   * This is a constant given by the tuple type:
   *)
  let size_for_nullmask = nullmask_bytes_of_tuple_typ tuple_typ in
  (* Let's emit the function definition, deconstructing the tuple with identifiers
   * for varsized fields: *)
  Printf.fprintf oc "let sersize_of_tuple %a =\n\t\
      %d (* null bitmask *) + %a\n"
    print_tuple_deconstruct tuple_typ
    size_for_nullmask
    (List.print ~first:"" ~last:"" ~sep:" + " (fun fmt field_typ ->
      let id = id_of_field_name field_typ.name in
      if field_typ.nullable then (
        Printf.fprintf fmt "(match %s with None -> 0 | Some x_ -> %a)"
          id
          (emit_sersize_of_field field_typ.typ) "x_"
      ) else (
        Printf.fprintf fmt "%a" (emit_sersize_of_field field_typ.typ) id
      ))) tuple_typ

let emit_set_value tx_var offs_var field_var oc field_typ =
  Printf.fprintf oc "RingBuf.write_%s %s %s %s"
    (id_of_typ field_typ) tx_var offs_var field_var

(* The function that will serialize the fields of the tuple at the given
 * addresses.  Everything else (allocating on the RB and writing the record
 * size) is independent of the tuple type and is handled in the library.
 * Also, the lib ensure that null bitmask is 0 at the beginning. Returns
 * the final offset for checking with serialized size of this tuple. *)
let emit_serialize_tuple oc tuple_typ =
  let open Lang.Tuple in
  Printf.fprintf oc "let serialize_tuple tx_ %a =\n"
    print_tuple_deconstruct tuple_typ ;
  Printf.fprintf oc "\tlet offs_ = %d in\n" (nullmask_bytes_of_tuple_typ tuple_typ) ;
  let _ = List.fold_left (fun (i, nulli) field ->
      let id = id_of_field_name field.name in
      if field.nullable then (
        (* Write either the null bit or the value *)
        Printf.fprintf oc "\tlet offs_ = match %s with\n" id ;
        Printf.fprintf oc "\t| None ->\n" ;
        Printf.fprintf oc "\t\tCodeGenLib_RingBuf.set_bit tx_ %d\n" nulli ;
        Printf.fprintf oc "\t\toffs_\n" ;
        Printf.fprintf oc "\t| Some x_ ->\n" ;
        Printf.fprintf oc "\t\t%a ;\n"
          (emit_set_value "tx_" "offs_" "x_") field.typ ;
        Printf.fprintf oc "\t\toffs_ + %a in\n"
          (emit_sersize_of_field field.typ) "x_"
      ) else (
        Printf.fprintf oc "\t%a ;\n"
          (emit_set_value "tx_" "offs_" id) field.typ ;
        Printf.fprintf oc "\tlet offs_ = offs_ + %a in\n"
          (emit_sersize_of_field field.typ) id
      ) ;
      i + 1, nulli + (if field.nullable then 1 else 0)
    ) (0, 0) tuple_typ in
  Printf.fprintf oc "\toffs_\n"

(* Emit a function that, given an array of strings (corresponding to a line of
 * CSV) will return the tuple defined by [tuple_typ] or raises
 * some exception *)
let emit_tuple_of_strings oc tuple_typ =
  let open Lang.Tuple in
  Printf.fprintf oc "let tuple_of_strings strs_ =\n" ;
  Printf.fprintf oc "\t(\n" ;
  let nb_fields = List.length tuple_typ in
  List.iteri (fun i field_typ ->
    let sep = if i < nb_fields - 1 then "," else "" in
    if field_typ.nullable then (
      Printf.fprintf oc "\t\tlet s_ = strs_.(%d) in\n" i ;
      Printf.fprintf oc "\t\tif s_ = \"\" then None else Some (%a)%s\n"
        (emit_value_of_string field_typ.typ) "s_"
        sep
    ) else (
      let s_var = Printf.sprintf "strs_.(%d)" i in
      Printf.fprintf oc "\t\t%a%s\n"
        (emit_value_of_string field_typ.typ) s_var sep
    )) tuple_typ ;
  Printf.fprintf oc "\t)\n"

(* Given a Tuple.typ, generate the ReadCSVFile operation. *)
let emit_read_csv_file oc csv_fname csv_separator tuple_typ =
  (* The dynamic part comes from the unpredictable field list.
   * For each input line, we want to read all fields and build a tuple.
   * Then we want to write this tuple in some ring buffer.
   * We need to generate these functions:
   * - reading a CSV string into a tuple type (when nullable fields are option type)
   * - given such a tuple, return its serialized size
   * - given a pointer toward the ring buffer, serialize the tuple *)
  Printf.fprintf oc "%a\n%a\n%a\n\
    let () =\n\
      \tLwt_main.run (\n\
      \t\tCodeGenLib.read_csv_file %S %S sersize_of_tuple serialize_tuple tuple_of_strings)\n"
    emit_sersize_of_tuple tuple_typ
    emit_serialize_tuple tuple_typ
    emit_tuple_of_strings tuple_typ
    csv_fname csv_separator

let emit_select oc in_tuple_typ selected_fields and_all_others where =
  (* We need a function to quickly extract all selected fields
   * from the input ringbuf. Notice that we must wait until we know the
   * exact type of that ringbuf, so we cannot compile before the links
   * are set. We need a rigatoni command to compile a graph. *)
  ignore oc ; ignore in_tuple_typ ; ignore selected_fields ;
  ignore and_all_others ; ignore where ;
  failwith "TODO: emit_select"

let keep_temp_files = ref true

let with_code_file_for name f =
  let mode = [`create; `excl; `text] in
  let mode = if !keep_temp_files then mode else `delete_on_exit::mode in
  let prefix = "gen_"^ name ^"_" in
  File.with_temporary_out ~mode ~prefix ~suffix:".ml" (fun oc fname ->
    !logger.debug "Source code for %s: %s" name fname ;
    f oc fname)

let compile_source fname =
  (* This is not guaranteed to be unique but should be... *)
  let exec_name = String.sub fname 0 (String.length fname - 3) in
  let comp_cmd =
    Printf.sprintf "ocamlfind ocamlopt -o %s -package batteries,stdint,lwt.ppx,cohttp.lwt \
                                       -linkpkg codegen.cmxa %s"
      exec_name fname in
  let exit_code = Sys.command comp_cmd in
  if exit_code = 0 then exec_name else (
    !logger.error "Compilation of %s failed with status %d.\n\
                   Failed command was: %S\n"
                  fname exit_code comp_cmd ;
    failwith "Cannot generate code"
  )

let gen_read_csv_file name csv_fname csv_separator tuple_typ =
  with_code_file_for name (fun oc fname ->
    emit_read_csv_file oc csv_fname csv_separator tuple_typ ;
    compile_source fname)

let gen_select name in_tuple_typ fields and_all_others where =
  with_code_file_for name (fun oc fname ->
    emit_select oc in_tuple_typ fields and_all_others where ;
    compile_source fname)

let gen_operation name in_tuple_typ op =
  let open Lang.Operation in
  match op with
  | Select { fields ; and_all_others ; where } ->
    gen_select name in_tuple_typ fields and_all_others where
  | ReadCSVFile { fname ; fields } ->
    gen_read_csv_file name fname "," fields
  | _ -> "echo TODO"

