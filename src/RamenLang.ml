(* AST for the stream processor graph *)
open Batteries
open Stdint
open RamenHelpers
open RamenLog
module T = RamenTypes
module N = RamenName

type variable =
  | Unknown (* Either Record, In, Out, or Param*)
  | In
  | GroupState
  | GlobalState
  | OutPrevious
  | Out
  (* Variables usable in sort expressions *)
  | SortFirst
  | SortSmallest
  | SortGreatest
  (* Command line parameters *)
  | Param
  (* Environments for nullable string only parameters: *)
  | Env
  (* For when a field is from a locally opened record. To know where that
   * record is coming from one has to look through the chain of Gets. *)
  | Record
  (* GlobalVars is the variable containing all global variable.
   * So if the two global variables v1 and v2 are declared, then "GlobalVars" is
   * the variable holding a two field record, named v1 and v2. *)
  | GlobalVar

let string_of_variable = function
  | Unknown -> "unknown"
  | In -> "in"
  | GroupState -> "group_state"
  | GlobalState -> "global_state"
  | OutPrevious -> "out_previous"
  | Out -> "out"
  | SortFirst -> "sort_first"
  | SortSmallest -> "sort_smallest"
  | SortGreatest -> "sort_greatest"
  | Param -> "param"
  | Env -> "env"
  | Record -> "record"
  | GlobalVar -> "global"

let variable_print oc p =
  Printf.fprintf oc "%s" (string_of_variable p)

let parse_variable m =
  let open RamenParsing in
  let m = "variable name" :: m in
  let w s = ParseUsual.string ~case_sensitive:false s +-
            nay legit_identifier_chars in
  (
    (w "unknown" >>: fun () -> Unknown) |<|
    (w "in" >>: fun () -> In) |<|
    (w "group_state" >>: fun () -> GroupState) |<|
    (w "global_state" >>: fun () -> GlobalState) |<|
    (w "out_previous" >>: fun () -> OutPrevious) |<|
    (w "previous" >>: fun () -> OutPrevious) |<|
    (w "out" >>: fun () -> Out) |<|
    (w "sort_first" >>: fun () -> SortFirst) |<|
    (w "sort_smallest" >>: fun () -> SortSmallest) |<|
    (w "sort_greatest" >>: fun () -> SortGreatest) |<|
    (w "smallest" >>: fun () -> SortSmallest) |<|
    (w "greatest" >>: fun () -> SortGreatest) |<|
    (w "param" >>: fun () -> Param) |<|
    (w "env" >>: fun () -> Env) |<|
    (* Not for public consumption: *)
    (w "record" >>: fun () -> Record) |<|
    (w "global" >>: fun () -> GlobalVar)
  ) m

(* Variables that has the fields of this func input type *)
let variable_has_type_input = function
  | In | SortFirst | SortSmallest | SortGreatest -> true
  | _ -> false

(* Variables that has the fields of this func output type *)
let variable_has_type_output = function
  | OutPrevious | Out -> true
  | _ -> false

open RamenParsing

(* Defined here as both RamenProgram and RamenOperation need to parse/print
 * function and program names: *)

let program_name ?(quoted=false) m =
  let quoted_quote = id_quote_escaped >>: fun () -> id_quote_char in
  let what = "program name" in
  let m = what :: m in
  let first_char =
    if quoted then not_id_quote |<| quoted_quote
    else letter |<| underscore |<| dot |<| slash in
  let any_char =
    if quoted then not_id_quote
              else first_char |<| decimal_digit |<| pound in
  (
    first_char ++ repeat ~sep:none ~what any_char >>:
    fun (c, s) -> N.rel_program (String.of_list (c :: s))
  ) m

let func_name ?(quoted=false) m =
  let what = "function name" in
  let m = what :: m in
  let not_quote =
    cond "quoted identifier" (fun c -> c <> id_quote_char && c <> '/') '_' in
  let first_char = if quoted then not_quote
                   else letter |<| underscore in
  let any_char = if quoted then not_quote
                 else first_char |<| decimal_digit in
  (
    first_char ++ repeat_greedy ~sep:none ~what any_char >>:
    fun (c, s) -> N.func (String.of_list (c :: s))
  ) m

let function_name =
  let unquoted = func_name
  and quoted =
    id_quote -+ func_name ~quoted:true +- id_quote in
  (quoted |<| unquoted)

let func_identifier m =
  let m = "function identifier" :: m in
  let unquoted =
    optional ~def:None
      (some program_name +- slash) ++
    func_name
  and quoted =
    id_quote -+
    optional ~def:None
       (some (program_name ~quoted:true) +- slash) ++
    func_name ~quoted:true +-
    id_quote in
  (quoted |<| unquoted) m

let site_identifier m =
  let what = "site identifier" in
  let m = what :: m in
  let site_char =
    letter |<| decimal_digit |<| minus |<|
    underscore |<| star in
  let unquoted =
    repeat_greedy ~sep:none ~what site_char
  and quoted =
    id_quote -+ repeat_greedy ~sep:none ~what not_id_quote +- id_quote in
  (
    quoted |<| unquoted >>: String.of_list
  ) m
