(* Simple tables for the terminal *)
open Batteries
open RamenHelpers
open RamenLog

type valtype =
  | ValStr of string
  | ValInt of int
  | ValFlt of float
  | ValDate of float

let string_of_val = function
  | ValStr s -> s
  | ValInt i -> string_of_int i
  | ValFlt f -> nice_string_of_float f
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

let print_table_terse ?sort_col ~with_header ?top head lines =
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

(* Formatters: given the list of all (string) values going in a column, and
 * the max width we want for that colulm, return a formatter for those
 * values (formatted value has to be [width] in length): *)
let make_left_justify vals width =
  ignore vals ;
  fun s ->
    let len = String.length s in
    if len > width then
      String.sub s 0 width
    else
      s ^ String.make (width - len) ' '

let make_right_justify vals width =
  ignore vals ;
  fun s ->
    let len = String.length s in
    if len > width then
      String.sub s (len - width) width
    else
      String.make (width - len) ' ' ^ s

let make_dot_align vals width =
  (* It will be right aligned once padded to the right to align the dots
   * (or the end): *)
  let after_dot s = (* number of chars after the dot, including the dot *)
    try String.length s - String.rindex s '.'
    with Not_found -> 0 in
  let max_after =
    List.fold_left (fun ma s ->
      max ma (after_dot s)
    ) 0 vals in
  let right_justify = make_right_justify [] width in
  fun s ->
    let after = after_dot s in
    let s = s ^ String.make (max_after - after) ' ' in
    right_justify s

let print_table_pretty ?sort_col ~with_header ?top head lines =
  assert (lines <> []) ; (* for we need the types *)
  let lines =
    match sort_col with
    | None -> lines
    | Some c -> sort ~sort_col:c lines in
  let lines =
    match top with
    | None -> lines
    | Some n -> List.take n lines
  in
  (* Choose a formatter according to the type of the first line...: *)
  let fmts =
    Array.map (function
      | ValStr _ -> make_left_justify
      | ValInt _ -> make_right_justify
      | ValFlt _ -> make_dot_align
      | ValDate _ -> make_left_justify
    ) (List.hd lines) in
  (* Get the string representation of headers and values in a single list: *)
  let lines =
    List.map (fun line ->
      Array.map string_of_val line
    ) lines in
  let col_width = Array.create (Array.length head) 0 in
  let lines_to_size =
    if with_header then head :: lines else lines in
  List.iter (fun line ->
    Array.iteri (fun i s ->
      let len = String.length s in
      if i < Array.length col_width && len > col_width.(i) then
        col_width.(i) <- len
    ) line
  ) lines_to_size ;
  (* Build the list of all values for each column, so the formatters can be
   * finalized: *)
  let vals =
    Array.init (Array.length head) (fun i ->
      List.map (fun line -> line.(i)) lines
    ) in
  let fmts = Array.mapi (fun i fmt ->
    fmt vals.(i) col_width.(i)) fmts in
  let col_sep = blue " | " in
  if with_header then (
    Array.iteri (fun i h ->
      let h = make_left_justify [] col_width.(i) h in
      Printf.printf "%s%s" (cyan h) col_sep
    ) head ;
    Printf.printf "\n") ;
  List.iter (fun line ->
    Array.iteri (fun i s ->
      let s = fmts.(i) s in
      Printf.printf "%s%s" s col_sep
    ) line ;
    Printf.printf "\n"
  ) lines

(* Note: sort_col starts at 1 *)
let print_table ?(pretty=false) ?sort_col ?(with_header=true) ?top head lines =
  if lines <> [] then
    (if pretty then print_table_pretty else print_table_terse)
      ?sort_col ~with_header ?top head lines
