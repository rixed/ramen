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
  | ValDate t -> string_of_time t

let int_or_na = Option.map (fun i -> ValInt (Stdint.Uint64.to_int i))
let flt_or_na = Option.map (fun f -> ValFlt f)
let date_or_na = Option.map (fun t -> ValDate t)
let str_or_na = Option.map (fun s -> ValStr s)

let sort ~sort_col lines =
  (* In case a line has less values that others, assume "n/a": *)
  let safe_get l i =
    try l.(i-1) with Invalid_argument _ -> None in
  (* We want higher numerical values first, but lower string values first. *)
  let cmp l1 l2 =
    match safe_get l1 sort_col, safe_get l2 sort_col with
    | Some (ValStr s1), Some (ValStr s2) ->
        String.compare s1 s2
    | Some (ValInt i1), Some (ValInt i2) ->
        Int.compare i2 i1
    | Some (ValFlt f1 | ValDate f1), Some (ValFlt f2 | ValDate f2) ->
        Float.compare f2 f1
    | Some (ValFlt f1 | ValDate f1), Some (ValInt i2) ->
        Float.compare (float_of_int i2) f1
    | Some (ValInt i1), Some (ValFlt f2 | ValDate f2) ->
        Float.compare f2 (float_of_int i1)
    | Some (ValStr s1), Some v2 ->
        String.compare s1 (string_of_val v2)
    | Some v1, Some (ValStr s2) ->
        String.compare (string_of_val v1) s2
    | Some _, None -> 1
    | None, Some _ -> -1
    | None, None -> 0
  in
  List.fast_sort cmp lines

let print_table_terse ~with_header ~na head lines =
  if with_header then (
    Array.iter (fun h -> Printf.printf "%s\t" h) head ;
    Printf.printf "\n") ;
  List.iter (fun line ->
    Array.iter (fun v ->
      Printf.printf "%s\t" (Option.map_default string_of_val na v)
    ) line ;
    Printf.printf "\n"
  ) lines

(* Formatters: given the list of all (string) values going in a column, and
 * the max width we want for that column, return a formatter for those
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

let print_table_pretty ~with_header ~na head lines =
  let fmts = Array.create (Array.length head) None in
  (try
    List.iter (fun line ->
      let has_unset =
        Array.fold_lefti (fun has_unset i -> function
          | None ->
              (match line.(i) with
              | Some (ValStr _) ->
                  fmts.(i) <- Some make_left_justify ; has_unset
              | Some (ValInt _) ->
                  fmts.(i) <- Some make_right_justify ; has_unset
              | Some (ValFlt _) ->
                  fmts.(i) <- Some make_dot_align ; has_unset
              | Some (ValDate _) ->
                  fmts.(i) <- Some make_left_justify ; has_unset
              | None ->
                  fmts.(i) <- None ; true)
          | Some _ -> has_unset
        ) false fmts in
      if not has_unset then raise Exit
    ) lines
  with Exit -> ()) ;
  (* When we have no values in any line then just use justify right: *)
  let fmts = Array.map (Option.default make_right_justify) fmts in
  (* Get the string representation of headers and values in a single list: *)
  let lines =
    List.map (fun line ->
      Array.map (Option.map_default string_of_val na) line
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
let print_table ?(pretty=false) ?sort_col ?(with_header=true) ?top
                ?(na="n/a") head lines =
  let lines =
    match sort_col with
    | None -> lines
    | Some c -> sort ~sort_col:c lines in
  let lines =
    match top with
    | None -> lines
    | Some n -> List.take n lines
  in
  if lines <> [] then
    (if pretty then print_table_pretty else print_table_terse)
      ~with_header ~na head lines

(* Instead of representing the entries as a table, display a tree using the
 * given two columns to establish node relationship: *)

let rec print_subtree ~parent ~child h head ~indent ~is_last root done_set =
  if not (Set.String.mem root done_set) then
    let done_set = Set.String.add root done_set in
    let print_info indent line =
      for i = 0 to Array.length head - 1 do
        if i <> parent && i <> child then
          Printf.printf "%s %s: %s\n"
            indent head.(i) line.(i)
      done
    in
    let indent_end, indent_end_next =
      if indent = "" then "", "" else
      if is_last then "└ ", "  " else "├ ", "│ " in
    Printf.printf "%s%s%s\n"
      indent indent_end root ;
    let indent = indent ^ indent_end_next ^ "   " in
    match Hashtbl.find h root with
    | exception Not_found -> ()
    | children ->
        list_iter_first_last (fun _is_first is_last (c, line) ->
          print_info (indent ^ "│ ") line ;
          print_subtree ~parent ~child h head ~indent ~is_last c done_set
        ) children

(* [parent] and [child] are indices within lines *)
let print_tree ~parent ~child ?(na="n/a") head lines roots =
  (* Turn the list of lines into a hash of node -> children *)
  let h = Hashtbl.create 11 in
  let to_str = Option.map_default string_of_val na in
  List.iter (fun line ->
    let line = Array.map to_str line in
    let p = line.(parent) and c = line.(child) in
    Hashtbl.modify_def [] p (fun lst -> (c, line) :: lst) h
  ) lines ;
  List.iter (fun root ->
    print_subtree ~parent ~child h head ~indent:"" ~is_last:true root
                  Set.String.empty
  ) roots
