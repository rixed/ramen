(* Program parameters.
 * String representation is used as part of the signature of a running
 * worker. *)
open Batteries
open RamenHelpersNoLog
module T = RamenTypes
module N = RamenName

type param = N.field * T.value [@@ppp PPP_OCaml]
type t = (N.field, T.value) Hashtbl.t [@@ppp PPP_OCaml]

let compare (a, _) (b, _) =
  N.compare a b

let print_list oc params =
  let print_param oc (n, v) =
    Printf.fprintf oc "%a=%a" N.field_print n T.print v in
  List.fast_sort compare params |>
  List.print ~first:"" ~last:"" ~sep:";" print_param oc

(* In an alternate universe, params are just an array of string->value: *)
let print_array oc params =
  let print_param oc (n, v) =
    Printf.fprintf oc "%a=%a" N.field_print n T.print v in
  let params = Array.copy params in
  Array.fast_sort compare params ;
  Array.print ~first:"" ~last:"" ~sep:";" print_param oc params

let print oc t =
  alist_of_hashtbl t |>
  print_list oc

(* String representation is the printed out values of all parameters
 * ("{p1=n1;p2=v2;...}"). *)
let to_string t =
  IO.to_string print t

let signature = N.md5 % to_string

let signature_of_list params =
  IO.to_string print_list params |> N.md5

let signature_of_array params =
  IO.to_string print_array params |> N.md5
