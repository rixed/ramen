open Batteries
open RamenSharedTypes

let print_field_typ fmt field =
  (* TODO: check that name is a valid identifier *)
  Printf.fprintf fmt "%s %a %sNULL"
    field.typ_name
    RamenScalar.print_typ field.typ
    (if field.nullable then "" else "NOT ")

type typ = field_typ list

let print_typ fmt lst =
  (List.print ~first:"(" ~last:")" ~sep:", " print_field_typ) fmt lst
