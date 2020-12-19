(* This tool is used to generate CSV/CHB reader tests. *)
open Batteries
module DT = DessserTypes
module DQ = DessserQCheck

type input_format = CSV | CHB

let gen_type num_fields max_depth format () =
  let num_fields = num_fields |? 2 + Random.int 5 in
  assert (num_fields > 0) ;
  assert (max_depth > 0) ;
  let open QCheck.Gen in
  let rec ensure_supported mn =
    let again () =
      generate1
        (map ensure_supported (DQ.maybe_nullable_gen_of_depth 1))
    and known_user_type = function
      | "Eth" | "Ip4" | "Ip6" | "Ip" | "Cidr4" | "Cidr6" | "Cidr" -> true
      | _ -> false in
    match mn.DT.vtyp with
    | DT.Unknown -> assert false (* not generated *)
    (* To be removed before long: *)
    | Mac (TU24 | TU40 | TU48 | TU56 |
           TI24 | TI40 | TI48 | TI56) -> again ()
    | Mac _ -> mn
    | Usr { name ; _ } when known_user_type name -> mn
    | Usr _ -> again ()
    (* Compound types are not serialized the same in ramen and dessser CSV for now: *)
    | TVec (d, mn') ->
        DT.{ mn with vtyp = TVec (d, ensure_supported mn') }
    | TList mn' ->
        DT.{ mn with vtyp = TList (ensure_supported mn') }
    | TTup mns ->
        DT.{ mn with vtyp = TTup (Array.map ensure_supported mns) }
    | TRec mns ->
        DT.{ mn with vtyp = TRec (Array.map (fun (n, mn) ->
                                    n, ensure_supported mn) mns) }
    | TSet _ -> again ()
    | TSum _ -> again ()
    | TMap _ -> again () in
  let type_gen =
    let field_type_gen =
      map
        (fun mn ->
          let mn = ensure_supported mn in
          if format = CSV then (
            Csv.make_serializable mn
          ) else mn)
        (int_range 1 max_depth >>= DQ.maybe_nullable_gen_of_depth) in
    array_repeat num_fields (pair DQ.field_name_gen field_type_gen) in
  DT.TRec (generate1 type_gen) |>
  DT.print_value_type stdout

let gen_csv_reader vtyp func_name files separator null_str =
  Printf.printf "DEFINE '%s' AS\n" func_name ;
  Printf.printf "  READ FROM FILE %S AS CSV SEPARATOR %a NULL %S (\n"
    files
    RamenParsing.print_char separator
    null_str ;
  (match vtyp with
  | DT.TRec fields ->
      Array.iteri (fun i (n, mn) ->
        let is_last = i = Array.length fields - 1 in
        Printf.printf "    '%s' %a%s\n"
          n
          DT.print_maybe_nullable mn
          (if is_last then "" else ",")
      ) fields
  | _ -> assert false) ;
  Printf.printf "  );\n"

let gen_chb_reader _vtyp _fname _separator _null_str =
  assert false

let gen_reader vtyp format func_name files separator null_str () =
  (match format with
  | CSV -> gen_csv_reader
  | CHB -> gen_chb_reader) vtyp func_name files separator null_str

let csv_of_vec separator lst =
  String.join separator lst

let csv_of_list separator lst =
  Stdlib.string_of_int (List.length lst) :: lst |>
  csv_of_vec separator

let rec value_gen_of_type null_prob separator true_str false_str null_str =
  let gen = value_gen null_prob separator true_str false_str null_str in
  let open QCheck.Gen in
  function
  | DT.Unknown ->
      invalid_arg "value_gen_of_type"
  | Mac TFloat ->
      map string_of_float float
  | Mac TBool ->
      map (function true -> true_str | false -> false_str) bool
  | Mac (TString | TChar |
         TU8 | TU16 | TU24 | TU32 | TU40 | TU48 | TU56 | TU64 | TU128 |
         TI8 | TI16 | TI24 | TI32 | TI40 | TI48 | TI56 | TI64 | TI128)
    as vtyp ->
      DQ.sexpr_of_vtyp_gen vtyp
  | Usr ut ->
      value_gen_of_type null_prob separator true_str false_str null_str ut.def
  | TVec (dim, mn) ->
      list_repeat dim (gen mn) |> map (csv_of_vec separator)
  | TList mn ->
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
        (TList { vtyp = TTup [| k ; v |] ; nullable = false })

and tup_gen null_prob separator true_str false_str null_str mns st =
  (*Printf.eprintf "tup_gen for type %a\n%!" (Array.print DT.print_maybe_nullable) mns ;*)
  Array.fold_left (fun s mn ->
    (if s = "" then s else s ^ separator) ^
    (value_gen null_prob separator true_str false_str null_str mn st)
  ) "" mns

and value_gen null_prob separator true_str false_str null_str mn =
  if mn.nullable && Random.float 1. < null_prob then
    QCheck.Gen.return null_str
  else
    value_gen_of_type null_prob separator true_str false_str null_str mn.vtyp

let gen_data vtyp format length null_prob separator true_str false_str null_str () =
  let mn = DT.{ nullable = false ; vtyp }
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
  let env = Term.env_info "NUM_FIELDS"
  and doc = "Number of fields (random if unspecified)." in
  let i = Arg.info ~doc ~env [ "n" ; "num-fields" ] in
  Arg.(value (opt (some int) None i))

let max_type_depth_opt =
  let env = Term.env_info "MAX_TYPE_DEPTH"
  and doc = "Maximum depth of any of the field types." in
  let i = Arg.info ~doc ~env [ "d" ; "max-depth" ] in
  Arg.(value (opt int 2 i))

let reader_format_opt =
  let env = Term.env_info "FORMAT"
  and doc = "Format of the input files." in
  let i = Arg.info ~doc ~env [ "f" ; "format" ] in
  let formats = [ "CSV", CSV ; "CHB", CHB ] in
  Arg.(value (opt (enum formats) CSV i))

let gen_type =
  let doc = "Generate a random type." in
  Term.(
    (const gen_type
      $ num_fields_opt
      $ max_type_depth_opt
      $ reader_format_opt),
    info ~doc "type")

let vtyp_t =
  let parse s =
    match DT.Parser.of_string s with
    | exception e ->
        Pervasives.Error (`Msg (Printexc.to_string e))
    | DT.TValue { nullable = false ; vtyp } ->
        Pervasives.Ok vtyp
    | _ ->
        Pervasives.Error (`Msg "Outer type must be a non nullable value type.")
  and print fmt vtyp =
    Format.fprintf fmt "%s" (DT.string_of_value_type vtyp)
  in
  Arg.conv ~docv:"TYPE" (parse, print)

let type_opt =
  let doc = "Type to read (may be generated with the `type` subcommand)." in
  let i = Arg.info ~doc ~docv:"TYPE" [] in
  Arg.(required (pos 0 (some vtyp_t) None i))

let func_name_opt =
  let env = Term.env_info "FUNCTION_NAME"
  and doc = "Name of the reader function." in
  let i = Arg.info ~doc ~env [ "function-name" ; "func-name" ] in
  Arg.(value (opt string "reader" i))

let files_opt =
  let env = Term.env_info "FILES"
  and doc = "Name (or glob) matching the file names to be read." in
  let i = Arg.info ~doc ~env [ "files" ] in
  Arg.(value (opt string "*.csv" i))

let separator_opt =
  let env = Term.env_info "CSV_SEPARATOR"
  and doc = "Separator to use." in
  let i = Arg.info ~doc ~env [ "csv-separator" ] in
  Arg.(value (opt char ',' i))

let true_str_opt =
  let env = Term.env_info "CSV_TRUE"
  and doc = "String representation of true." in
  let i = Arg.info ~doc ~env [ "csv-true" ] in
  Arg.(value (opt string "true" i))

let false_str_opt =
  let env = Term.env_info "CSV_FALSE"
  and doc = "String representation of false." in
  let i = Arg.info ~doc ~env [ "csv-false" ] in
  Arg.(value (opt string "false" i))

let null_str_opt =
  let env = Term.env_info "CSV_NULL"
  and doc = "String representation of NULL." in
  let i = Arg.info ~doc ~env [ "csv-null" ] in
  Arg.(value (opt string "\\N" i))

let gen_reader =
  let doc = "Generate a reader for a given type." in
  Term.(
    (const gen_reader
      $ type_opt
      $ reader_format_opt
      $ func_name_opt
      $ files_opt
      $ separator_opt
      $ null_str_opt),
    info ~doc "reader")

let length_opt =
  let env = Term.env_info "DATA_LENGTH"
  and doc = "Number of values in the data file." in
  let i = Arg.info ~doc ~env [ "l" ; "length" ] in
  Arg.(value (opt int 10_000 i))

let null_prob_opt =
  let env = Term.env_info "NULL_PROBABILITY"
  and doc = "Probability to draw a NULL value for nullable types." in
  let i = Arg.info ~doc ~env [ "null-probability" ] in
  Arg.(value (opt float 0.3 i))

let gen_data =
  let doc = "Generate a data file for the given type." in
  Term.(
    (const gen_data
      $ type_opt
      $ reader_format_opt
      $ length_opt
      $ null_prob_opt
      $ separator_opt
      $ true_str_opt
      $ false_str_opt
      $ null_str_opt),
    info ~doc "data")

let default =
  let doc = "Generate reader tests"
  and sdocs = Manpage.s_common_options in
  Term.((ret (const (`Help (`Pager, None)))),
        info "gen_reader_test" ~doc ~sdocs)

let () =
  match
    Term.eval_choice default [ gen_type ; gen_data ; gen_reader ] with
  | `Error _ -> exit 1
  | `Version | `Help -> exit 0
  | `Ok f -> f ()
