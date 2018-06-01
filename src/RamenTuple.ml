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
  { typ_name : string ; nullable : bool ; typ : RamenTypes.typ }
  [@@ppp PPP_OCaml]

type typ = field_typ list

type typed_tuple =
  { user : field_typ list ; (* All the fields as declared in the code *)
    ser : field_typ list } (* Only public fields *)
  [@@ppp PPP_OCaml]

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

let print_field_typ fmt field =
  (* TODO: check that name is a valid identifier *)
  Printf.fprintf fmt "%s %a %sNULL"
    field.typ_name
    RamenTypes.print_typ field.typ
    (if field.nullable then "" else "NOT ")

let print_typ fmt t =
  (List.print ~first:"(" ~last:")" ~sep:", " print_field_typ) fmt t

let type_signature typed_tuple =
  List.fold_left (fun s field ->
      (if s = "" then "" else s ^ "_") ^
      field.typ_name ^ ":" ^
      RamenTypes.string_of_typ field.typ ^
      if field.nullable then " null" else " notnull"
    ) "" typed_tuple.ser

(* Special case of tuple: parameters *)

type params = (string * RamenTypes.value) list [@@ppp PPP_OCaml]

let print_param oc (n, v) =
  Printf.fprintf oc "%s=%a" n RamenTypes.print v

(* FIXME: make those params a Map so names are unique *)
let param_compare (a1, _) (b1, _) =
  String.compare a1 b1

let string_of_params ps =
  List.fast_sort param_compare ps |>
  IO.to_string (List.print ~first:"" ~last:"" ~sep:"," print_param)

let param_signature ps =
  string_of_params ps |> md5

(* Override ps1 with values from ps2, ignoring the values of ps2 that are
 * not in ps1: *)
let overwrite_params ps1 ps2 =
  List.map (fun (p1_nam, p1_val as p1) ->
    match List.find (fun (p2_nam, p2_val) -> p2_nam = p1_nam) ps2 with
    | exception Not_found -> p1
    | _, p2_val as p2 ->
        if RamenTypes.type_of p2_val = RamenTypes.type_of p1_val then p2
        else
          let msg = Printf.sprintf2 "Parameter %s has wrong type, \
                                     %a instead of %a" p1_nam
                      RamenTypes.print_typ (RamenTypes.type_of p2_val)
                      RamenTypes.print_typ (RamenTypes.type_of p1_val) in
          failwith msg
  ) ps1
