(* Simple tables for the terminal *)
open Batteries
open RamenHelpers

type valtype =
  | ValStr of string
  | ValInt of int
  | ValFlt of float
  | ValDate of float

let string_of_val = function
  | ValStr s -> s
  | ValInt i -> string_of_int i
  | ValFlt f -> string_of_float f
  | ValDate t -> ctime t

let sort ~sort_col lines =
  let safe_get l i =
    try l.(i-1) with Invalid_argument _ -> ValStr "n/a" in
  (* We want higher numerical values first, but lower string values first. *)
  let cmp l1 l2 =
    match safe_get l1 sort_col, safe_get l2 sort_col with
    | ValStr s1, ValStr s2 -> String.compare s1 s2
    | ValInt i1, ValInt i2 -> Int.compare i2 i1
    | (ValFlt f1 | ValDate f1), (ValFlt f2 | ValDate f2) ->
        Float.compare f2 f1
    | (ValFlt f1 | ValDate f1), ValInt i2 ->
        Float.compare (float_of_int i2) f1
    | ValInt i1, (ValFlt f2 | ValDate f2) ->
        Float.compare f2 (float_of_int i1)
    | ValStr s1, v2 -> String.compare s1 (string_of_val v2)
    | v1, ValStr s2 -> String.compare (string_of_val v1) s2
  in
  List.fast_sort cmp lines

(* Note: sort_col starts at 1 *)
let print_table ?sort_col ?(with_header=true) ?top head lines =
  let lines =
    match sort_col with
    | None -> lines
    | Some c -> sort ~sort_col:c lines in
  let lines =
    match top with
    | None -> lines
    | Some n -> List.take n lines
  in
  if with_header then (
    Array.iter (fun h -> Printf.printf "%s\t" h) head ;
    Printf.printf "\n") ;
  List.iter (fun line ->
    Array.iter (fun v ->
      Printf.printf "%s\t" (string_of_val v)
    ) line ;
    Printf.printf "\n"
  ) lines

let print_form form =
  List.iter (fun (n, v) ->
    Printf.printf "%s: %s " n (string_of_val v)
  ) form ;
  Printf.printf "\n"
