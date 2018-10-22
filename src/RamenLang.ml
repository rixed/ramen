(* AST for the stream processor graph *)
open Batteries
open Stdint
open RamenHelpers
open RamenLog

type tuple_prefix =
  | TupleUnknown (* Either In, Out or Param*)
  | TupleIn
  | TupleGroup
  | TupleOutPrevious
  | TupleOut
  (* Tuple usable in sort expressions *)
  | TupleSortFirst
  | TupleSortSmallest
  | TupleSortGreatest
  (* Largest tuple from the merged streams (smallest being TupleIn),
   * usable in WHERE clause: *)
  | TupleMergeGreatest
  (* Parameters *)
  | TupleParam
  (* Environments for nullable string only parameters: *)
  | TupleEnv
  (* TODO: TupleOthers? *)
  [@@ppp PPP_OCaml]

let string_of_prefix = function
  | TupleIn -> "in"
  | TupleGroup -> "group"
  | TupleOutPrevious -> "out.previous"
  | TupleOut -> "out"
  | TupleUnknown -> "unknown"
  | TupleSortFirst -> "sort.first"
  | TupleSortSmallest -> "sort.smallest"
  | TupleSortGreatest -> "sort.greatest"
  | TupleMergeGreatest -> "merge.greatest"
  | TupleParam -> "param"
  | TupleEnv -> "env"

let tuple_prefix_print oc p =
  Printf.fprintf oc "%s" (string_of_prefix p)

let parse_prefix ~def m =
  let open RamenParsing in
  let m = "tuple prefix" :: m in
  let prefix s = strinG (s ^ ".") in
  (optional ~def (
    (prefix "in" >>: fun () -> TupleIn) |||
    (prefix "group" >>: fun () -> TupleGroup) |||
    (prefix "out.previous" >>: fun () -> TupleOutPrevious) |||
    (prefix "previous" >>: fun () -> TupleOutPrevious) |||
    (prefix "out" >>: fun () -> TupleOut) |||
    (prefix "sort.first" >>: fun () -> TupleSortFirst) |||
    (prefix "sort.smallest" >>: fun () -> TupleSortSmallest) |||
    (prefix "sort.greatest" >>: fun () -> TupleSortGreatest) |||
    (prefix "merge.greatest" >>: fun () -> TupleMergeGreatest) |||
    (prefix "smallest" >>: fun () -> TupleSortSmallest) |||
    (* Note that since sort.greatest and merge.greatest cannot appear in
     * the same clauses we could convert one into the other (TODO) *)
    (prefix "greatest" >>: fun () -> TupleSortGreatest) |||
    (prefix "param" >>: fun () -> TupleParam) |||
    (prefix "env" >>: fun () -> TupleEnv))
  ) m

(* Tuple that has the fields of this func input type *)
let tuple_has_type_input = function
  | TupleIn
  | TupleSortFirst | TupleSortSmallest | TupleSortGreatest
  | TupleMergeGreatest -> true
  | _ -> false

(* Tuple that has the fields of this func output type *)
let tuple_has_type_output = function
  | TupleOutPrevious | TupleOut -> true
  | _ -> false

let tuple_need_state = function
  | TupleGroup -> true
  | _ -> false

open RamenParsing

(* Defined here as both RamenProgram and RamenOperation need to parse/print
 * function and program names: *)

let program_name ?(quoted=false) m =
  let not_quote =
    cond "quoted identifier" ((<>) '\'') 'x' in
  let quoted_quote = string "''" >>: fun () -> '\'' in
  let what = "program name" in
  let m = what :: m in
  let first_char =
    if quoted then not_quote ||| quoted_quote
    else letter ||| underscore ||| dot ||| slash in
  let any_char = if quoted then not_quote
                 else first_char ||| decimal_digit in
  (
    first_char ++ repeat ~sep:none ~what any_char >>:
    fun (c, s) -> RamenName.rel_program_of_string (String.of_list (c :: s))
  ) m

let func_name ?(quoted=false) m =
  let what = "function name" in
  let m = what :: m in
  let not_quote =
    cond "quoted identifier" (fun c -> c <> '\'' && c <> '/') 'x' in
  let first_char = if quoted then not_quote
                   else letter ||| underscore in
  let any_char = if quoted then not_quote
                 else first_char ||| decimal_digit in
  (
    first_char ++ repeat_greedy ~sep:none ~what any_char >>:
    fun (c, s) -> RamenName.func_of_string (String.of_list (c :: s))
  ) m

let function_name =
  let unquoted = func_name
  and quoted =
    id_quote -+ func_name ~quoted:true +- id_quote in
  (quoted ||| unquoted)

let func_identifier m =
  let m = "function identifier" :: m in
  let unquoted =
    optional ~def:None
      (some program_name +- char '/') ++
    func_name
  and quoted =
    id_quote -+
    optional ~def:None
       (some (program_name ~quoted:true) +- char '/') ++
    func_name ~quoted:true +-
    id_quote
  in (quoted ||| unquoted) m
