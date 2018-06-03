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

type syntax_error =
  | ParseError of { error : string ; text : string }
  | NotConstant of string
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
  | FuncNameNotUnique of string
  | OnlyTumblingWindowForTop
  | UnknownFunc of string
  | NoAccessToGeneratedFields of { alias : string }
  | UnsolvableDependencyLoop of { program : string }
  | NotAnInteger of RamenTypes.value
  | OutOfBounds of int * int
  | EveryWithFrom

(* TODO: Move all errors related to compilation into Compiler *)
exception SyntaxError of syntax_error

let tuple_prefix_print oc p =
  Printf.fprintf oc "%s" (string_of_prefix p)

let string_of_syntax_error =
  let h = "Syntax Error: " in
  function
    ParseError { error ; text } ->
    "Parse error: "^ error ^" while parsing: "^ text
  | NotConstant s -> h ^ s ^" is not constant"
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
  | FuncNameNotUnique name ->
    "Function names must be unique within a program but '"^ name ^"' is defined \
     several times"
  | OnlyTumblingWindowForTop ->
    "When using TOP the only windowing mode supported is \
     \"COMMIT AND FLUSH\""
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
    (prefix "param" >>: fun () -> TupleParam))
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

(*$inject
  open Batteries
  open RamenParsing
  open TestHelpers
 *)

let keyword =
  let open RamenParsing in
  (
    (* Some values that must not be parsed as field names: *)
    strinG "true" ||| strinG "false" ||| strinG "null" |||
    strinG "all" ||| strinG "as" |||
    (* Or "X in top" could also be parsed as an independent expression: *)
    strinG "top" |||
    (* Some functions with possibly no arguments that must not be
     * parsed as field names: *)
    strinG "now" ||| strinG "random"
  ) -- check (nay (letter ||| underscore ||| decimal_digit))
let non_keyword =
  let open RamenParsing in
  (check ~what:"no quoted identifier" (nay id_quote) -+
   check ~what:"no keyword" (nay keyword) -+
   identifier) |||
  (id_quote -+ identifier +- id_quote)
