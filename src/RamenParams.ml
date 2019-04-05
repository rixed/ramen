(* Program parameters.
 * String representation is used as part of the signature of a running
 * worker. *)
open Batteries
module T = RamenTypes
module N = RamenName

type param = string * T.value [@@ppp PPP_OCaml]
type t = (N.field, T.value) Hashtbl.t [@@ppp PPP_OCaml]

(* String representation is the printed out values of all parameters
 * ("{p1=n1;p2=v2;...}"). *)
let to_string params =
  let sort =
    let param_compare (a, _) (b, _) = N.compare a b in
    List.fast_sort param_compare in
  let print_param oc (n, v) =
    Printf.fprintf oc "%a=%a" N.field_print n T.print v in
  Hashtbl.enum params |>
  List.of_enum |>
  sort |>
  IO.to_string (List.print ~first:"" ~last:"" ~sep:";" print_param)

let signature = N.md5 % to_string
