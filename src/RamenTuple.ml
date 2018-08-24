(* This module keeps types and functions related to tuples.
 *
 * Tuples are what operations consume and produce.
 * Notice that we have a type to describe the type of a tuple (the type
 * and names of its fields) but no type to describe an actual tuple value.
 * That's because all tuple values appear only in generated code.
 *)
open Batteries
open RamenHelpers

(* TODO: turn this into a pair name * rest so we can use assoc functions *)
type field_typ =
  { typ_name : string ;
    mutable typ : RamenTypes.t ;
    mutable units : RamenUnits.t option ;
    mutable doc : string }
  [@@ppp PPP_OCaml]

type typ = field_typ list
  [@@ppp PPP_OCaml]

type param =
  { ptyp : field_typ ; value : RamenTypes.value }
  [@@ppp PPP_OCaml]

type params = param list [@@ppp PPP_OCaml]

let params_sort params =
  let param_compare p1 p2 =
    String.compare p1.ptyp.typ_name p2.ptyp.typ_name in
  List.fast_sort param_compare params

let params_find n = List.find (fun p -> p.ptyp.typ_name = n)
let params_mem n = List.exists (fun p -> p.ptyp.typ_name = n)

(* Given a tuple type, return a function that reorder a tuple (as
 * an array) into the same column order as in the tuple type: *)
let reorder_tuple_to_user typ =
  (* Start by building the array of indices in the ser tuple of fields of
   * the user (minus private) tuple. *)
  let indices =
    List.filter_map (fun f ->
      if is_private_field f.typ_name then None
      else
        Some (List.findi (fun _ f' ->
          f'.typ_name = f.typ_name
        ) typ |> fst)
    ) typ |>
    Array.of_list in
  (* Now reorder a list of scalar values in ser order into user order: *)
  (* TODO: an inplace version *)
  fun vs ->
    Array.map (fun idx -> vs.(idx)) indices

let print_field_typ oc field =
  Printf.fprintf oc "%s %a"
    field.typ_name
    RamenTypes.print_typ field.typ ;
  Option.may (RamenUnits.print oc) field.units

let print_typ oc t =
  (List.print ~first:"(" ~last:")" ~sep:", "
    print_field_typ) oc t

let print_typ_names oc =
  pretty_list_print (fun oc ft -> String.print oc ft.typ_name) oc

let type_signature =
  List.fold_left (fun s ft ->
    if is_private_field ft.typ_name then s
    else
      (if s = "" then "" else s ^ "_") ^
      ft.typ_name ^ ":" ^
      RamenTypes.string_of_typ ft.typ
  ) ""

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
      optional ~def:None (opt_blanks -+ some RamenUnits.Parser.p) ++
      optional ~def:"" quoted_string >>:
      fun (((typ_name, typ), units), doc) ->
        { typ_name ; typ ; units ; doc }
    ) m
end
