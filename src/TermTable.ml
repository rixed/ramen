(* Simple tables for the terminal *)

type valtype = ValStr of string | ValInt of int | ValFlt of float

let string_of_val = function
  | ValStr s -> s
  | ValInt i -> string_of_int i
  | ValFlt f -> string_of_float f

let print_table head lines =
  Array.iter (fun h ->
    Printf.printf "%s " h) head ;
  Printf.printf "\n" ;
  List.iter (fun line ->
    Array.iter (fun v ->
      Printf.printf "%s " (string_of_val v)
    ) line ;
    Printf.printf "\n"
  ) lines

let print_form form =
  List.iter (fun (n, v) ->
    Printf.printf "%s: %s " n (string_of_val v)
  ) form ;
  Printf.printf "\n"
