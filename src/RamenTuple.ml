(* This module keeps types and functions related to tuples.
 *
 * Tuples are what operations consume and produce.
 * Notice that we have a type to describe the type of a tuple (the type
 * and names of its fields) but no type to describe an actual tuple value.
 * That's because all tuple values appear only in generated code.
 *)
open Batteries
open RamenHelpers

type field_typ =
  { typ_name : string ; typ : RamenTypes.t ; units : RamenUnits.t option }
  [@@ppp PPP_OCaml]

type typ = field_typ list
  [@@ppp PPP_OCaml]

type typed_tuple =
  { user : typ ; (* All the fields as declared in the code *)
    ser : typ } (* Only public fields *)
  [@@ppp PPP_OCaml]

type param =
  { ptyp : field_typ ; value : RamenTypes.value } [@@ppp PPP_OCaml]

type params = param list [@@ppp PPP_OCaml]

let params_sort params =
  let param_compare p1 p2 =
    String.compare p1.ptyp.typ_name p2.ptyp.typ_name in
  List.fast_sort param_compare params

let params_find n = List.find (fun p -> p.ptyp.typ_name = n)
let params_mem n = List.exists (fun p -> p.ptyp.typ_name = n)

(* Given a typed tuple type, return a function that reorder a ser tuple (as
 * an array) into the same column order as in the user version: *)
let reorder_tuple_to_user typed_tuple =
  (* Start by building the array of indices in the ser tuple of fields of
   * the user (minus private) tuple. *)
  let indices =
    List.filter_map (fun f ->
      if is_private_field f.typ_name then None
      else
        Some (List.findi (fun _ f' ->
          f'.typ_name = f.typ_name
        ) typed_tuple.ser |> fst)
    ) typed_tuple.user |>
    Array.of_list in
  (* Now reorder a list of scalar values in ser order into user order: *)
  (* TODO: an inplace version *)
  fun vs ->
    Array.map (fun idx -> vs.(idx)) indices

let print_field_typ oc field =
  (* TODO: check that name is a valid identifier *)
  Printf.fprintf oc "%s %a"
    field.typ_name
    RamenTypes.print_typ field.typ ;
  Option.may (RamenUnits.print oc) field.units

let print_typ oc t =
  (List.print ~first:"(" ~last:")" ~sep:", " print_field_typ) oc t

let type_signature =
  List.fold_left (fun s field ->
    (if s = "" then "" else s ^ "_") ^
    field.typ_name ^ ":" ^
    RamenTypes.string_of_typ field.typ
  ) ""

(* Different signature for different instances of the same program: *)
let param_values_signature params =
  params_sort params |>
  IO.to_string (List.print (fun oc p ->
    Printf.fprintf oc "%s=%a" p.ptyp.typ_name RamenTypes.print p.value)) |>
  md5

(* Same signature for different instances of the same program but changes
 * whenever the type of parameters change: *)
let param_types_signature =
  type_signature % List.map (fun p -> p.ptyp) % params_sort

(* Override ps1 with values from ps2, ignoring the values of ps2 that are
 * not in ps1. Enlarge the values of ps2 as necessary: *)
let overwrite_params ps1 ps2 =
  List.map (fun p1 ->
    match List.find (fun (p2_nam, _) -> p2_nam = p1.ptyp.typ_name) ps2 with
    | exception Not_found -> p1
    | _, p2_val ->
        let open RamenTypes in
        if p2_val = VNull then
          if not p1.ptyp.typ.nullable then
            Printf.sprintf2 "Parameter %s is not nullable so cannot \
                             be set to NULL" p1.ptyp.typ_name |>
            failwith
          else
            { p1 with value = VNull }
        else match enlarge_value p1.ptyp.typ.structure p2_val with
          | exception Invalid_argument _ ->
              Printf.sprintf2 "Parameter %s of type %a can not be \
                               promoted into a %a" p1.ptyp.typ_name
                print_structure (structure_of p2_val)
                print_typ p1.ptyp.typ |>
              failwith
          | value -> { p1 with value }
  ) ps1

module Parser =
struct
  open RamenParsing

  let field m =
    let m = "field declaration" :: m in
    (
      non_keyword +- blanks ++ RamenTypes.Parser.typ ++
      optional ~def:None (some RamenUnits.Parser.p) >>:
      fun ((typ_name, typ), units) -> { typ_name ; typ ; units }
    ) m
end
