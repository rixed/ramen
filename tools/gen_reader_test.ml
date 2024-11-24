(* This tool is used to generate CSV/CHB reader tests. *)
open Batteries
module DT = DessserTypes
module DQ = DessserQCheck

let debug = false

type input_format = CSV | CHB

let opaque_user_type = true

let gen_type num_fields max_depth format =
  let num_fields = num_fields |? 2 + Random.int 5 in
  assert (num_fields > 0) ;
  assert (max_depth > 0) ;
  let open QCheck.Gen in
  let rec ensure_supported mn =
    let again () =
      (* Replace the unsupported type with a type of same depth: *)
      let depth = DT.depth ~opaque_user_type mn.DT.typ in
      if debug then
        Printf.eprintf "Replacing unsupported type %a of depth %d\n%!"
          DT.print_mn mn
          (DT.depth ~opaque_user_type mn.DT.typ) ;
      generate1
        (map ensure_supported (DQ.maybe_nullable_gen_of_depth depth))
    and known_user_type = function
      | "Eth" | "Ip4" | "Ip6" | "Ip" | "Cidr4" | "Cidr6" | "Cidr" -> true
      | _ -> false in
    match mn.DT.typ with
    | DT.TUnknown -> assert false (* not generated *)
    | TVoid -> again ()
    (* To be removed before long: *)
    | TU24 | TU40 | TU48 | TU56 |
      TI24 | TI40 | TI48 | TI56 -> again ()
    | TBool | TChar | TFloat | TString
    | TU8 | TU16 | TU32 | TU64 | TU128
    | TI8 | TI16 | TI32 | TI64 | TI128 -> mn
    | TUsr { name ; _ } when known_user_type name -> mn
    | TUsr _ -> again ()
    | TExt _ -> again ()
    (* Compound types are not serialized the same in ramen and dessser CSV for now: *)
    | TVec (d, mn') ->
        DT.{ mn with typ = TVec (d, ensure_supported mn') }
    | TLst mn' ->
        DT.{ mn with typ = TLst (ensure_supported mn') }
    | TTup mns ->
        DT.{ mn with typ = TTup (Array.map ensure_supported mns) }
    | TRec mns ->
        DT.{ mn with typ = TRec (Array.map (fun (n, mn) ->
                                     n, ensure_supported mn) mns) }
    | _ -> again () in
  let type_gen =
    let field_type_gen =
      map
        (fun mn ->
          if debug then
            Printf.eprintf "For a record field, generated type %a of depth %d\n%!"
              DT.print_mn mn (DT.depth ~opaque_user_type mn.DT.typ) ;
          assert (DT.depth ~opaque_user_type mn.DT.typ <= max_depth) ;
          let mn = ensure_supported mn in
          if format = CSV then (
            DessserCsv.make_serializable mn
          ) else mn)
        (int_range 1 max_depth >>= DQ.maybe_nullable_gen_of_depth) in
    array_repeat num_fields (pair DQ.field_name_gen field_type_gen) in
  let vt = DT.TRec (generate1 type_gen) in
  if debug then
    Printf.eprintf "Generated type %a of depth %d (for %d)\n%!"
      DT.print vt (DT.depth ~opaque_user_type vt) max_depth ;
  assert (DT.depth ~opaque_user_type vt <= max_depth + 1) ; (* +1 for the outer record *)
  DT.print stdout vt

let gen_csv_reader typ func_name files separator null_str =
  Printf.printf "DEFINE '%s' AS\n" func_name ;
  Printf.printf "  READ FROM FILE %S AS CSV SEPARATOR %a NULL %S \n"
    files
    RamenParsing.print_char separator
    null_str ;
  Printf.printf "  VECTORS OF CHARS AS VECTOR (\n" ;
  (match typ with
  | DT.TRec fields ->
      Array.iteri (fun i (n, mn) ->
        let is_last = i = Array.length fields - 1 in
        Printf.printf "    '%s' %a%s\n"
          n
          DT.print_mn mn
          (if is_last then "" else ",")
      ) fields
  | _ -> assert false) ;
  Printf.printf "  );\n"

let gen_chb_reader _typ _fname _separator _null_str =
  assert false

let gen_reader typ format func_name files separator null_str =
  (match format with
  | CSV -> gen_csv_reader
  | CHB -> gen_chb_reader) typ func_name files separator null_str

let csv_of_vec separator lst =
  String.join separator lst

let csv_of_list separator lst =
  Stdlib.string_of_int (List.length lst) :: lst |>
  csv_of_vec separator

let rec value_gen_of_type null_prob separator true_str false_str null_str =
  let gen = value_gen null_prob separator true_str false_str null_str in
  let open QCheck.Gen in
  function
  | DT.TVoid ->
      return "()"
  | TFloat ->
      map string_of_float float
  | TBool ->
      map (function true -> true_str | false -> false_str) bool
  | TString | TChar |
    TU8 | TU16 | TU24 | TU32 | TU40 | TU48 | TU56 | TU64 | TU128 |
    TI8 | TI16 | TI24 | TI32 | TI40 | TI48 | TI56 | TI64 | TI128
    as typ ->
      DQ.sexpr_of_typ_gen typ
  | TUsr ut ->
      value_gen_of_type null_prob separator true_str false_str null_str ut.def
  | TExt n ->
      invalid_arg ("value_gen_of_type for Ext type "^ n)
  | TVec (dim, mn) ->
      list_repeat dim (gen mn) |> map (csv_of_vec separator)
  | TLst mn ->
      DQ.tiny_list (gen mn) |> map (csv_of_list separator)
  | TSet _ ->
      invalid_arg "value_gen_of_type"
  | TTup mns ->
      tup_gen null_prob separator true_str false_str null_str mns
  | TRec mns ->
      tup_gen null_prob separator true_str false_str null_str (Array.map snd mns)
  | TSum mns ->
      join (
        map (fun i ->
          let i = (abs i) mod (Array.length mns) in
          gen (snd mns.(i)) |>
          map (fun se -> string_of_int i ^ separator ^ se)
        ) int)
  | TMap (k, v) ->
      value_gen_of_type null_prob separator true_str false_str null_str
        (TLst DT.(required (TTup [| k ; v |])))
  | t ->
      invalid_arg ("value_gen_of_type: "^ DT.to_string t)

and tup_gen null_prob separator true_str false_str null_str mns st =
  Array.fold_left (fun s mn ->
    (if s = "" then s else s ^ separator) ^
    (value_gen null_prob separator true_str false_str null_str mn st)
  ) "" mns

and value_gen null_prob separator true_str false_str null_str mn =
  if mn.nullable && Random.float 1. < null_prob then
    QCheck.Gen.return null_str
  else
    value_gen_of_type null_prob separator true_str false_str null_str mn.typ

let gen_data typ format length null_prob separator true_str false_str null_str =
  let mn = DT.required typ
  and separator = String.of_char separator in
  assert (format = CSV) ;
  for i = 1 to length do
    Printf.printf "%s\n"
      (QCheck.Gen.generate1
        (value_gen null_prob separator true_str false_str null_str mn))
  done

(*
 * Command line
 *)

open Cmdliner

let num_fields_opt =
  let env = Cmd.Env.info "NUM_FIELDS"
  and doc = "Number of fields (random if unspecified)." in
  let i = Arg.info ~doc ~env [ "n" ; "num-fields" ] in
  Arg.(value (opt (some int) None i))

let max_type_depth_opt =
  let env = Cmd.Env.info "MAX_TYPE_DEPTH"
  and doc = "Maximum depth of any of the field types." in
  let i = Arg.info ~doc ~env [ "d" ; "max-depth" ] in
  Arg.(value (opt int 2 i))

let reader_format_opt =
  let env = Cmd.Env.info "FORMAT"
  and doc = "Format of the input files." in
  let i = Arg.info ~doc ~env [ "f" ; "format" ] in
  let formats = [ "CSV", CSV ; "CHB", CHB ] in
  Arg.(value (opt (enum formats) CSV i))

let gen_type =
  let doc = "Generate a random type." in
  Cmd.v (Cmd.info ~doc "type")
    Term.(
      const gen_type
        $ num_fields_opt
        $ max_type_depth_opt
        $ reader_format_opt)

let typ_t =
  let parse s =
    match DessserParser.typ_of_string s with
    | exception e ->
        Stdlib.Error (`Msg (Printexc.to_string e))
    | typ ->
        Stdlib.Ok typ
  and print fmt typ =
    Format.fprintf fmt "%s" (DT.to_string typ)
  in
  Arg.conv ~docv:"TYPE" (parse, print)

let type_opt =
  let doc = "Type to read (may be generated with the `type` subcommand)." in
  let i = Arg.info ~doc ~docv:"TYPE" [] in
  Arg.(required (pos 0 (some typ_t) None i))

let func_name_opt =
  let env = Cmd.Env.info "FUNCTION_NAME"
  and doc = "Name of the reader function." in
  let i = Arg.info ~doc ~env [ "function-name" ; "func-name" ] in
  Arg.(value (opt string "reader" i))

let files_opt =
  let env = Cmd.Env.info "FILES"
  and doc = "Name (or glob) matching the file names to be read." in
  let i = Arg.info ~doc ~env [ "files" ] in
  Arg.(value (opt string "*.csv" i))

let separator_opt =
  let env = Cmd.Env.info "CSV_SEPARATOR"
  and doc = "Separator to use." in
  let i = Arg.info ~doc ~env [ "csv-separator" ] in
  Arg.(value (opt char ',' i))

let true_str_opt =
  let env = Cmd.Env.info "CSV_TRUE"
  and doc = "String representation of true." in
  let i = Arg.info ~doc ~env [ "csv-true" ] in
  Arg.(value (opt string "true" i))

let false_str_opt =
  let env = Cmd.Env.info "CSV_FALSE"
  and doc = "String representation of false." in
  let i = Arg.info ~doc ~env [ "csv-false" ] in
  Arg.(value (opt string "false" i))

let null_str_opt =
  let env = Cmd.Env.info "CSV_NULL"
  and doc = "String representation of NULL." in
  let i = Arg.info ~doc ~env [ "csv-null" ] in
  Arg.(value (opt string "\\N" i))

let gen_reader =
  let doc = "Generate a reader for a given type." in
  Cmd.v (Cmd.info ~doc "reader")
    Term.(
      const gen_reader
        $ type_opt
        $ reader_format_opt
        $ func_name_opt
        $ files_opt
        $ separator_opt
        $ null_str_opt)

let length_opt =
  let env = Cmd.Env.info "DATA_LENGTH"
  and doc = "Number of values in the data file." in
  let i = Arg.info ~doc ~env [ "l" ; "length" ] in
  Arg.(value (opt int 10_000 i))

let null_prob_opt =
  let env = Cmd.Env.info "NULL_PROBABILITY"
  and doc = "Probability to draw a NULL value for nullable types." in
  let i = Arg.info ~doc ~env [ "null-probability" ] in
  Arg.(value (opt float 0.3 i))

let gen_data =
  let doc = "Generate a data file for the given type." in
  Cmd.v (Cmd.info ~doc "data")
    Term.(
      const gen_data
        $ type_opt
        $ reader_format_opt
        $ length_opt
        $ null_prob_opt
        $ separator_opt
        $ true_str_opt
        $ false_str_opt
        $ null_str_opt)

let () =
  let cmd =
    let doc = "Generate reader tests"
    and sdocs = Manpage.s_common_options in
    let default = Term.(ret (const (`Help (`Pager, None)))) in
    Cmd.group ~default (Cmd.info ~doc ~sdocs "gen_reader_test")
      [ gen_type ; gen_data ; gen_reader ] in
  Cmd.eval cmd |>
  exit
