(* AST for the stream processor graph *)
open Batteries
open Stdint
open RamenHelpers
open RamenLog

type tuple_prefix =
  | TupleUnknown (* Either In or Out *)
  | TupleIn | TupleLastIn
  | TupleSelected | TupleLastSelected
  | TupleUnselected | TupleLastUnselected
  | TupleGroup
  (* first input tuple aggregated into that group *)
  | TupleGroupFirst
  (* last input tuple aggregated into that group *)
  | TupleGroupLast
  (* last output tuple computed for that group. Cannot be used in a SELECT
   * clause because we would have no tuple to init the group *)
  | TupleGroupPrevious
  (* last output tuple committed by any group *)
  | TupleOutPrevious
  | TupleOut
  (* Tuple usable in sort expressions *)
  | TupleSortFirst
  | TupleSortSmallest
  | TupleSortGreatest
  (* Parameters *)
  | TupleParam
  (* Environments for nullable string only parameters: *)
  | TupleEnv
  (* TODO: TupleOthers? *)

let string_of_prefix = function
  | TupleIn -> "in"
  | TupleLastIn -> "in.last"
  | TupleSelected -> "selected"
  | TupleLastSelected -> "selected.last"
  | TupleUnselected -> "unselected"
  | TupleLastUnselected -> "unselected.last"
  | TupleGroup -> "group"
  | TupleGroupFirst -> "group.first"
  | TupleGroupLast -> "group.last"
  | TupleGroupPrevious -> "group.previous"
  | TupleOutPrevious -> "out.previous"
  | TupleOut -> "out"
  | TupleUnknown -> "unknown"
  | TupleSortFirst -> "sort.first"
  | TupleSortSmallest -> "sort.smallest"
  | TupleSortGreatest -> "sort.greatest"
  | TupleParam -> "param"
  | TupleEnv -> "env"

type syntax_error =
  | ParseError of { error : string ; text : string }
  | NotConstant of string
  | BadConstant of string
  | TupleNotAllowed of { tuple : tuple_prefix ; where : string ;
                         allowed : tuple_prefix list }
  | StatefulNotAllowed of { clause : string }
  | StateNotAllowed of { state : string ; clause : string }
  | FieldNotInTuple of { field : string ; tuple : tuple_prefix ;
                         tuple_type : string }
  | FieldNotSameTypeInAllParents of { field : string }
  | NoParentForField of { field : string }
  | TupleHasOnlyVirtuals of { tuple : tuple_prefix ; alias : string }
  | InvalidPrivateField of { field : string }
  | MissingClause of { clause : string }
  | CannotTypeField of { field : string ; typ : string ; tuple : tuple_prefix }
  | CannotTypeExpression of { what : string ; expected_type : string ;
                              got_type : string }
  | CannotCompareTypes of { what : string }
  | CannotCombineTypes of { what : string }
  | InvalidNullability of { what : string ; must_be_nullable : bool }
  | InvalidCoalesce of { what : string ; must_be_nullable : bool }
  | CannotCompleteTyping of string
  | CannotGenerateCode of { func : string ; cmd : string ; status : string }
  | AliasNotUnique of string
  | NameNotUnique of string
  | UnknownFunc of string
  | NoAccessToGeneratedFields of { alias : string }
  | UnsolvableDependencyLoop of { program : string }
  | NotAnInteger of RamenTypes.value
  | OutOfBounds of int * int
  | IncompatibleVecItems of { what : string ; indice : int ; typ : string ;
                              largest : string }
  | EveryWithFrom

(* TODO: Move all errors related to compilation into Compiler *)
exception SyntaxError of syntax_error

let tuple_prefix_print oc p =
  Printf.fprintf oc "%s" (string_of_prefix p)

let string_of_syntax_error =
  let h = "Syntax Error: " in
  function
  | ParseError { error } -> "Parse error: "^ error
  | NotConstant s -> h ^ s ^" is not constant"
  | BadConstant s -> s
  | TupleNotAllowed { tuple ; where ; allowed } ->
    "Invalid tuple '"^ string_of_prefix tuple ^"'; in a "^ where ^
    ", all fields must come from " ^
      (IO.to_string
        (List.print ~first:"" ~last:"" ~sep:" or " tuple_prefix_print)
        allowed)
  | StatefulNotAllowed { clause } ->
    "Stateful function not allowed in "^ clause ^" clause"
  | StateNotAllowed { state ; clause } ->
    let state = String.capitalize state in
    state ^" context(s) not allowed in "^ clause ^" clause"
  | FieldNotInTuple { field ; tuple ; tuple_type } ->
    "Field "^ field ^" is not in the "^ string_of_prefix tuple ^" tuple"^
    (if tuple_type <> "" then " (which is "^ tuple_type ^")" else "")
  | FieldNotSameTypeInAllParents { field } ->
    "Field "^ field ^" has different types in different parents"
  | NoParentForField { field } ->
    "Input field "^ field ^" is used but func has no parent"
  | InvalidPrivateField { field } ->
    "Cannot import field "^ field ^" which is private"
  | MissingClause { clause } ->
    "Missing "^ clause ^" clause"
  | CannotTypeField { field ; typ ; tuple } ->
    "Cannot find out the type of field "^ field ^" ("^ typ ^") \
     supposed to be a member of "^ string_of_prefix tuple ^" tuple"
  | CannotTypeExpression { what ; expected_type ; got_type } ->
    what ^" must have type (compatible with) "^ expected_type ^
    " but got "^ got_type
  | CannotCompareTypes { what } ->
    "Cannot compare operands of "^ what
  | CannotCombineTypes { what } ->
    "Cannot find a type able to accommodate the operands of "^ what
  | InvalidNullability { what ; must_be_nullable } ->
    what ^" must"^ (if must_be_nullable then "" else " not") ^
    " be nullable"
  | InvalidCoalesce { what ; must_be_nullable } ->
    "All elements of a COALESCE must be nullable but the last one. "^
    what ^" can"^ (if must_be_nullable then " not" else "") ^" be null."
  | CannotCompleteTyping s -> "Cannot complete typing of "^ s
  | CannotGenerateCode { func ; cmd ; status } ->
    Printf.sprintf
      "Cannot generate code: compilation of func %S with %S %s"
      func cmd status
  | AliasNotUnique name ->
    "Alias is not unique: "^ name
  | NameNotUnique name ->
    "Names must be unique within a program but '"^ name ^"' is defined \
     several times"
  | TupleHasOnlyVirtuals { tuple ; alias } ->
    "Tuple "^ string_of_prefix tuple ^" has only virtual fields, so no \
     field named "^ alias
  | UnknownFunc n ->
    "Referenced func "^ n ^" does not exist"
  | NoAccessToGeneratedFields { alias } ->
    "Cannot access output field "^ alias ^" as it is the result of a generator"
  | UnsolvableDependencyLoop { program } ->
    "Unsolvable dependency loop prevent the compilation of "^ program
  | EveryWithFrom ->
    "Cannot use an every clause in conjunction with a from clause"
  | NotAnInteger v ->
    Printf.sprintf2 "Value %a must be an integer"
      RamenTypes.print v
  | OutOfBounds (n, lim) ->
    Printf.sprintf "Index value %d is outside the permitted bounds (0..%d)"
      n lim
  | IncompatibleVecItems { what ; indice ; typ ; largest } ->
    Printf.sprintf "In vector %s, element %d has type %s which is \
                    incompatible with previous element type %s"
      what indice typ largest

let () =
  Printexc.register_printer (function
    | SyntaxError e -> Some (string_of_syntax_error e)
    | _ -> None)

let parse_prefix ~def m =
  let open RamenParsing in
  let m = "tuple prefix" :: m in
  let prefix s = strinG (s ^ ".") in
  (optional ~def (
    (prefix "in" >>: fun () -> TupleIn) |||
    (prefix "in.last" >>: fun () -> TupleLastIn) |||
    (prefix "selected" >>: fun () -> TupleSelected) |||
    (prefix "selected.last" >>: fun () -> TupleLastSelected) |||
    (prefix "unselected" >>: fun () -> TupleUnselected) |||
    (prefix "unselected.last" >>: fun () -> TupleLastUnselected) |||
    (prefix "group" >>: fun () -> TupleGroup) |||
    (prefix "group.first" >>: fun () -> TupleGroupFirst) |||
    (prefix "first" >>: fun () -> TupleGroupFirst) |||
    (prefix "group.last" >>: fun () -> TupleGroupLast) |||
    (prefix "last" >>: fun () -> TupleGroupLast) |||
    (prefix "group.previous" >>: fun () -> TupleGroupPrevious) |||
    (prefix "out.previous" >>: fun () -> TupleOutPrevious) |||
    (prefix "previous" >>: fun () -> TupleOutPrevious) |||
    (prefix "out" >>: fun () -> TupleOut) |||
    (prefix "sort.first" >>: fun () -> TupleSortFirst) |||
    (prefix "sort.smallest" >>: fun () -> TupleSortSmallest) |||
    (prefix "sort.greatest" >>: fun () -> TupleSortGreatest) |||
    (prefix "smallest" >>: fun () -> TupleSortSmallest) |||
    (prefix "greatest" >>: fun () -> TupleSortGreatest) |||
    (prefix "param" >>: fun () -> TupleParam) |||
    (prefix "env" >>: fun () -> TupleEnv))
  ) m

let tuple_has_count = function
  | TupleIn | TupleSelected | TupleUnselected | TupleGroup | TupleOut -> true
  | _ -> false

let tuple_has_successive = function
  | TupleSelected | TupleUnselected | TupleGroup -> true
  | _ -> false

(* Tuple that has the fields of this func input type *)
let tuple_has_type_input = function
  | TupleIn | TupleLastIn | TupleLastSelected | TupleLastUnselected
  | TupleGroupFirst | TupleGroupLast -> true
  | _ -> false

(* Tuple that has the fields of this func output type *)
let tuple_has_type_output = function
  | TupleGroupPrevious | TupleOutPrevious | TupleOut -> true
  | _ -> false

let tuple_need_state = function
  | TupleGroup | TupleGroupFirst | TupleGroupLast | TupleGroupPrevious -> true
  | _ -> false

open RamenParsing

(* Defined here as both RamenProgram and RamenOperation need to parse/print
 * function and program names: *)

type program_id = RamenName.program * RamenName.params
type function_id = program_id option * RamenName.func

let exp_program_of_id (prog_name, params) =
  RamenName.(program_exp_of_string (
    string_of_program prog_name ^
    string_of_params_exp (params_exp_of_params params)))

let print_expansed_function oc (prog_opt, func) =
  Option.may (fun p ->
    Printf.fprintf oc "%s/"
      (RamenName.string_of_program_exp (exp_program_of_id p))
  ) prog_opt ;
  String.print oc (RamenName.string_of_func func)

let string_of_func_id pn =
  IO.to_string print_expansed_function pn

let program_name ?(quoted=false) m =
  let not_quote =
    cond "quoted identifier" ((<>) '\'') 'x' in
  let what = "program name" in
  let m = what :: m in
  let first_char = if quoted then not_quote
                   else letter ||| underscore ||| char '/' in
  let any_char = if quoted then not_quote
                 else first_char ||| decimal_digit in
  (
    first_char ++ repeat ~sep:none ~what any_char >>:
    fun (c, s) -> RamenName.program_of_string (String.of_list (c :: s))
  ) m

let expansed_program_name ?(quoted=false) m =
  let m = "program name" :: m in
  let expansed_param =
    non_keyword +- opt_blanks +- char '=' +- opt_blanks ++
    RamenTypes.Parser.p in
  let expansed_params m =
    let m = "parameter expansion" :: m in
    (char '{' -- opt_blanks -+
     repeat_greedy ~sep:list_sep expansed_param +-
     opt_blanks +- char '}' >>: RamenName.params_sort) m
  in
  (
    program_name ~quoted ++
    optional ~def:[] expansed_params
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
      (some expansed_program_name +- char '/') ++
    func_name
  and quoted =
    id_quote -+
    optional ~def:None
       (some (expansed_program_name ~quoted:true) +- char '/') ++
    func_name ~quoted:true +-
    id_quote
  in (quoted ||| unquoted) m
