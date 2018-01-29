(* AST for the stream processor graph *)
open Batteries
open RamenSharedTypesJS
open RamenSharedTypes
open Stdint
open Helpers
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

type syntax_error =
  | ParseError of { error : string ; text : string }
  | NotConstant of string
  | TupleNotAllowed of { tuple : tuple_prefix ; where : string ;
                         allowed : tuple_prefix list }
  | StatefulNotAllowed of { clause : string }
  | GroupStateNotAllowed of { clause : string }
  | FieldNotInTuple of { field : string ; tuple : tuple_prefix ;
                         tuple_type : string }
  | FieldNotSameTypeInAllParents of { field : string }
  | NoParentForField of { field : string }
  | TupleHasOnlyVirtuals of { tuple : tuple_prefix ; alias : string }
  | InvalidPrivateField of { field : string }
  | MissingClause of { clause : string }
  | CannotTypeField of { field : string ; typ : string ; tuple : tuple_prefix }
  | CannotTypeExpression of { what : string ; expected_type : string ;
                              got : string ; got_type : string }
  | InvalidNullability of { what : string ; must_be_nullable : bool }
  | InvalidCoalesce of { what : string ; must_be_nullable : bool }
  | CannotCompleteTyping of string
  | CannotGenerateCode of { node : string ; cmd : string ; status : string }
  | AliasNotUnique of string
  | NodeNameNotUnique of string
  | OnlyTumblingWindowForTop
  | UnknownNode of string

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
  | GroupStateNotAllowed { clause } ->
    "Group context not allowed in "^ clause ^" clause"
  | FieldNotInTuple { field ; tuple ; tuple_type } ->
    "Field "^ field ^" is not in the "^ string_of_prefix tuple ^" tuple"^
    (if tuple_type <> "" then " (which is "^ tuple_type ^")" else "")
  | FieldNotSameTypeInAllParents { field } ->
    "Field "^ field ^" has different types in differentparents"
  | NoParentForField { field } ->
    "Input field "^ field ^" is used but node has no parent"
  | InvalidPrivateField { field } ->
    "Cannot import field "^ field ^" which is private"
  | MissingClause { clause } ->
    "Missing "^ clause ^" clause"
  | CannotTypeField { field ; typ ; tuple } ->
    "Cannot find out the type of field "^ field ^" ("^ typ ^") \
     supposed to be a member of "^ string_of_prefix tuple ^" tuple"
  | CannotTypeExpression { what ; expected_type ; got ; got_type } ->
    what ^" must have type (compatible with) "^ expected_type ^
    " but got "^ got ^" of type "^ got_type
  | InvalidNullability { what ; must_be_nullable } ->
    what ^" must"^ (if must_be_nullable then "" else " not") ^
    " be nullable"
  | InvalidCoalesce { what ; must_be_nullable } ->
    "All elements of a COALESCE must be nullable but the last one. "^
    what ^" can"^ (if must_be_nullable then " not" else "") ^" be null."
  | CannotCompleteTyping s -> "Cannot complete typing of "^ s
  | CannotGenerateCode { node ; cmd ; status } ->
    Printf.sprintf
      "Cannot generate code: compilation of node %S with command %S %s"
      node cmd status
  | AliasNotUnique name ->
    "Alias is not unique: "^ name
  | NodeNameNotUnique name ->
    "Node names must be unique within a layer but '"^ name ^"' is defined \
     several times"
  | OnlyTumblingWindowForTop ->
    "When using TOP the only windowing mode supported is \
     \"COMMIT AND FLUSH\""
  | TupleHasOnlyVirtuals { tuple ; alias } ->
    "Tuple "^ string_of_prefix tuple ^" has only virtual fields, so no \
     field named "^ alias
  | UnknownNode n ->
    "Referenced node "^ n ^" does not exist"

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
    (prefix "out" >>: fun () -> TupleOut))
  ) m

let tuple_has_count = function
  | TupleIn | TupleSelected | TupleUnselected | TupleGroup | TupleOut -> true
  | _ -> false

let tuple_has_successive = function
  | TupleSelected | TupleUnselected | TupleGroup -> true
  | _ -> false

(* Tuple that has the fields of this node input type *)
let tuple_has_type_input = function
  | TupleIn | TupleLastIn | TupleLastSelected | TupleLastUnselected
  | TupleGroupFirst | TupleGroupLast -> true
  | _ -> false

(* Tuple that has the fields of this node output type *)
let tuple_has_type_output = function
  | TupleGroupPrevious | TupleOutPrevious | TupleOut -> true
  | _ -> false

let tuple_need_state = function
  | TupleGroup | TupleGroupFirst | TupleGroupLast | TupleGroupPrevious -> true
  | _ -> false

(*$inject
  open Batteries
  open RamenSharedTypes
  open RamenParsing

  let test_printer res_printer = function
    | Ok (res, (len, rest)) ->
      Printf.sprintf "%S, parsed_len=%d, rest=%s"
        (IO.to_string res_printer res) len
        (IO.to_string (List.print Char.print) rest)
    | Bad (Approximation _) ->
      "Approximation"
    | Bad (NoSolution e) ->
      Printf.sprintf "No solution (%s)" (IO.to_string print_error e)
    | Bad (Ambiguous lst) ->
      Printf.sprintf "%d solutions: %s"
        (List.length lst)
        (IO.to_string
          (List.print (fun fmt (res,_c,_s) ->
            res_printer fmt res)) lst)

  let strip_linecol = function
    | Ok (res, (x, _line, _col)) -> Ok (res, x)
    | Bad x -> Bad x

  let test_p p s =
    (p +- eof) [] None Parsers.no_error_correction (PConfig.stream_of_string s) |>
    to_result |>
    strip_linecol

  let test_op p s =
    match test_p p s with
    | Ok (res, _) as ok_res ->
      Lang.Operation.check res ; ok_res
    | x -> x

  let typ = Lang.Expr.make_typ "replaced for tests"

  let replace_typ e =
    Lang.Expr.map_type (fun _ -> typ) e

  let replace_typ_in_expr = function
    | Ok (expr, rest) -> Ok (replace_typ expr, rest)
    | x -> x

  let replace_typ_in_operation =
    let open Lang.Operation in
    function
    | Aggregate { fields ; and_all_others ; where ; export ; notify_url ;
                  key ; top ; commit_before ; commit_when ; flush_how ;
                  from } ->
      Aggregate {
        fields = List.map (fun sf -> { sf with expr = replace_typ sf.expr }) fields ;
        and_all_others ;
        where = replace_typ where ;
        export ; notify_url ; from ;
        key = List.map replace_typ key ;
        top = Option.map (fun (n, e) -> replace_typ n, replace_typ e) top ;
        commit_when = replace_typ commit_when ;
        commit_before = commit_before ;
        flush_how = (match flush_how with
          | Reset | Never | Slide _ -> flush_how
          | RemoveAll e -> RemoveAll (replace_typ e)
          | KeepOnly e -> KeepOnly (replace_typ e)) }
    | x -> x

  let replace_typ_in_op = function
    | Ok (op, rest) -> Ok (replace_typ_in_operation op, rest)
    | x -> x

  let replace_typ_in_program =
    let open Lang.Program in
    function
    | Ok (prog, rest) ->
      Ok (
        List.map (fun func ->
          { func with operation = replace_typ_in_operation func.operation }
        ) prog,
        rest)
    | x -> x
 *)

module Scalar =
struct
  (*$< Scalar *)

  type typ = scalar_typ

  let string_of_typ = function
    | TNull   -> "NULL"
    | TFloat  -> "FLOAT"
    | TString -> "STRING"
    | TBool   -> "BOOL"
    | TNum    -> "ANY_NUM" (* This one not for consumption *)
    | TAny    -> "ANY" (* same *)
    | TU8     -> "U8"
    | TU16    -> "U16"
    | TU32    -> "U32"
    | TU64    -> "U64"
    | TU128   -> "U128"
    | TI8     -> "I8"
    | TI16    -> "I16"
    | TI32    -> "I32"
    | TI64    -> "I64"
    | TI128   -> "I128"
    | TEth    -> "Eth"
    | TIpv4   -> "IPv4"
    | TIpv6   -> "IPv6"
    | TCidrv4 -> "CIDRv4"
    | TCidrv6 -> "CIDRv6"

  let print_typ fmt typ =
    String.print fmt (string_of_typ typ)

  type type_class = KNum | KBool | KString | KNull | KCidrv4 | KCidrv6
  let compare_typ typ1 typ2 =
    let rank_of_typ = function
      | TFloat  -> KNum, 200
      | TU128   -> KNum, 128
      | TIpv6   -> KNum, 128
      | TI128   -> KNum, 127
      | TNum    -> KNum, 0
      | TU64    -> KNum, 64
      | TI64    -> KNum, 63
      (* We consider Eth and IPs numbers so we can cast directly to/from ints
       * and use comparison operators. *)
      | TEth    -> KNum, 48
      | TU32    -> KNum, 32
      | TIpv4   -> KNum, 32
      | TI32    -> KNum, 31
      | TU16    -> KNum, 16
      | TI16    -> KNum, 15
      | TU8     -> KNum, 8
      | TI8     -> KNum, 7
      | TString -> KString, 1
      | TBool   -> KBool, 1
      | TNull   -> KNull, 0
      | TCidrv4 -> KCidrv4, 0
      | TCidrv6 -> KCidrv6, 0
      | TAny    -> assert false
    in
    let k1, r1 = rank_of_typ typ1
    and k2, r2 = rank_of_typ typ2 in
    if k1 <> k2 then invalid_arg "types not comparable" ;
    compare r1 r2

  let larger_type (t1, t2) =
    if compare_typ t1 t2 >= 0 then t1 else t2

  let largest_type = function
    | fst :: rest ->
      List.fold_left (fun l t ->
        larger_type (l, t)) fst rest
    | _ -> invalid_arg "largest_type"

  (* The original Float.to_string adds a useless dot at the end of
   * round numbers: *)
  let my_float_to_string v =
    let s = Float.to_string v in
    assert (String.length s > 0) ;
    if s.[String.length s - 1] <> '.' then s else String.rchop s

  let to_string = function
    | VFloat f  -> my_float_to_string f
    | VString s -> Printf.sprintf "%S" s
    | VBool b   -> Bool.to_string b
    | VU8 i     -> Uint8.to_string i
    | VU16 i    -> Uint16.to_string i
    | VU32 i    -> Uint32.to_string i
    | VU64 i    -> Uint64.to_string i
    | VU128 i   -> Uint128.to_string i
    | VI8 i     -> Int8.to_string i
    | VI16 i    -> Int16.to_string i
    | VI32 i    -> Int32.to_string i
    | VI64 i    -> Int64.to_string i
    | VI128 i   -> Int128.to_string i
    | VNull     -> "NULL"
    | VEth i    -> EthAddr.to_string i
    | VIpv4 i   -> Ipv4.to_string i
    | VIpv6 i   -> Ipv6.to_string i
    | VCidrv4 i -> Ipv4.Cidr.to_string i
    | VCidrv6 i -> Ipv6.Cidr.to_string i

  let print fmt v = String.print fmt (to_string v)

  let is_round_integer = function
    | VFloat f  -> fst(modf f) = 0.
    | VString _ | VBool _ | VNull | VEth _ | VIpv4 _ | VIpv6 _
    | VCidrv4 _ | VCidrv6 _ -> false
    | _ -> true

  module Parser =
  struct
    (*$< Parser *)
    open RamenParsing

    let narrowest_int_scalar =
      let min_i32 = Num.of_string "-2147483648"
      and max_i32 = Num.of_string "2147483647"
      and max_u32 = Num.of_string "4294967295"
      and min_i64 = Num.of_string "-9223372036854775808"
      and max_i64 = Num.of_string "9223372036854775807"
      and max_u64 = Num.of_string "18446744073709551615"
      and min_i128 = Num.of_string "-170141183460469231731687303715884105728"
      and max_i128 = Num.of_string "170141183460469231731687303715884105727"
      and max_u128 = Num.of_string "340282366920938463463374607431768211455"
      and zero = Num.zero
      in fun i ->
        let s = Num.to_string i in
        if Num.le_num min_i32 i && Num.le_num i max_i32  then VI32 (Int32.of_string s) else
        if Num.le_num zero i && Num.le_num i max_u32  then VU32 (Uint32.of_string s) else
        if Num.le_num min_i64 i && Num.le_num i max_i64  then VI64 (Int64.of_string s) else
        if Num.le_num zero i && Num.le_num i max_u64  then VU64 (Uint64.of_string s) else
        if Num.le_num min_i128 i && Num.le_num i max_i128  then VI128 (Int128.of_string s) else
        if Num.le_num zero i && Num.le_num i max_u128  then VU128 (Uint128.of_string s) else
        assert false

    (* TODO: Here and elsewhere, we want the location (start+length) of the
     * thing in addition to the thing *)
    let p =
      (integer >>: narrowest_int_scalar) |||
      (integer +- strinG "i8" >>: fun i -> VI8 (Int8.of_string (Num.to_string i))) |||
      (integer +- strinG "i16" >>: fun i -> VI16 (Int16.of_string (Num.to_string i))) |||
      (integer +- strinG "i32" >>: fun i -> VI32 (Int32.of_string (Num.to_string i))) |||
      (integer +- strinG "i64" >>: fun i -> VI64 (Int64.of_string (Num.to_string i))) |||
      (integer +- strinG "i128" >>: fun i -> VI128 (Int128.of_string (Num.to_string i))) |||
      (integer +- strinG "u8" >>: fun i -> VU8 (Uint8.of_string (Num.to_string i))) |||
      (integer +- strinG "u16" >>: fun i -> VU16 (Uint16.of_string (Num.to_string i))) |||
      (integer +- strinG "u32" >>: fun i -> VU32 (Uint32.of_string (Num.to_string i))) |||
      (integer +- strinG "u64" >>: fun i -> VU64 (Uint64.of_string (Num.to_string i))) |||
      (integer +- strinG "u128" >>: fun i -> VU128 (Uint128.of_string (Num.to_string i))) |||
      (floating_point >>: fun f -> VFloat f) |||
      (strinG "false" >>: fun _ -> VBool false) |||
      (strinG "true" >>: fun _ -> VBool true) |||
      (quoted_string >>: fun s -> VString s) |||
      (EthAddr.Parser.p >>: fun v -> VEth v) |||
      (Ipv4.Parser.p >>: fun v -> VIpv4 v) |||
      (Ipv6.Parser.p >>: fun v -> VIpv6 v) |||
      (Ipv4.Cidr.Parser.p >>: fun v -> VCidrv4 v) |||
      (Ipv6.Cidr.Parser.p >>: fun v -> VCidrv6 v)

    (*$= p & ~printer:(test_printer print)
      (Ok (VI32 (Int32.of_int 31000), (5,[])))   (test_p p "31000")
      (Ok (VI32 (Int32.of_int 61000), (5,[])))   (test_p p "61000")
      (Ok (VFloat 3.14, (4,[])))                 (test_p p "3.14")
      (Ok (VFloat ~-.3.14, (5,[])))              (test_p p "-3.14")
      (Ok (VBool false, (5,[])))                 (test_p p "false")
      (Ok (VBool true, (4,[])))                  (test_p p "true")
      (Ok (VString "glop", (6,[])))              (test_p p "\"glop\"")
    *)

    let typ =
      (strinG "float" >>: fun () -> TFloat) |||
      (strinG "string" >>: fun () -> TString) |||
      ((strinG "bool" ||| strinG "boolean") >>: fun () -> TBool) |||
      (strinG "u8" >>: fun () -> TU8) |||
      (strinG "u16" >>: fun () -> TU16) |||
      (strinG "u32" >>: fun () -> TU32) |||
      (strinG "u64" >>: fun () -> TU64) |||
      (strinG "u128" >>: fun () -> TU128) |||
      (strinG "i8" >>: fun () -> TI8) |||
      (strinG "i16" >>: fun () -> TI16) |||
      (strinG "i32" >>: fun () -> TI32) |||
      (strinG "i64" >>: fun () -> TI64) |||
      (strinG "i128" >>: fun () -> TI128) |||
      (strinG "null" >>: fun () -> TNull) |||
      (strinG "eth" >>: fun () -> TEth) |||
      (strinG "ip4" >>: fun () -> TIpv4) |||
      (strinG "ip6" >>: fun () -> TIpv6) |||
      (strinG "cidr4" >>: fun () -> TCidrv4) |||
      (strinG "cidr6" >>: fun () -> TCidrv6)

    (*$>*)
  end
  (*$>*)
end

let keyword =
  let open RamenParsing in
  (
    (* Some values that must not be parsed as field names: *)
    strinG "true" ||| strinG "false" ||| strinG "null" |||
    strinG "all" |||
    (* Some functions with possibly no arguments that must not be
     * parsed as field names: *)
    strinG "now" ||| strinG "sequence"
  ) -- check (nay (letter ||| underscore ||| decimal_digit))
let non_keyword =
  let open RamenParsing in
  (check ~what:"no quoted identifier" (nay id_quote) -+
   check ~what:"no keyword" (nay keyword) -+
   identifier) |||
  (id_quote -+ identifier +- id_quote)

module Tuple =
struct
  let print_field_typ fmt field =
    (* TODO: check that name is a valid identifier *)
    Printf.fprintf fmt "%s %a %sNULL"
      field.typ_name
      Scalar.print_typ field.typ
      (if field.nullable then "" else "NOT ")

  type typ = field_typ list

  let print_typ fmt lst =
    (List.print ~first:"(" ~last:")" ~sep:", " print_field_typ) fmt lst
end

module Expr =
struct
  (*$< Expr *)

  (* Each expression come with a type attached. Starting at None types are
   * progressively set at compilation. *)
  type typ =
    { mutable expr_name : string ;
      uniq_num : int ; (* to build var names or record field names *)
      mutable nullable : bool option ;
      mutable scalar_typ : scalar_typ option }

  let print_typ fmt typ =
    Printf.fprintf fmt "%s of %s%s"
      typ.expr_name
      (match typ.scalar_typ with
      | None -> "unknown type"
      | Some typ -> "type "^ IO.to_string Scalar.print_typ typ)
      (match typ.nullable with
      | None -> ", maybe nullable"
      | Some true -> ", nullable"
      | Some false -> ", not nullable")

  let uniq_num_seq = ref 0
  let make_typ ?nullable ?typ expr_name =
    incr uniq_num_seq ;
    { expr_name ; nullable ; scalar_typ = typ ; uniq_num = !uniq_num_seq }
  let make_bool_typ ?nullable name = make_typ ?nullable ~typ:TBool name
  let make_float_typ ?nullable name = make_typ ?nullable ~typ:TFloat name
  let make_string_typ ?nullable name = make_typ ?nullable ~typ:TString name
  let make_num_typ ?nullable name =
    make_typ ?nullable ~typ:TNum name (* will be enlarged as required *)
  let copy_typ ?name typ =
    let expr_name = name |? typ.expr_name in
    incr uniq_num_seq ;
    { typ with expr_name ; uniq_num = !uniq_num_seq }

  (* Expressions on scalars (aka fields) *)
  (* FIXME: when we end prototyping use objects to make it easier to add
   * operations *)
  type t =
    (* TODO: classify by type and number of operands to simplify adding
     * functions *)
    | Const of typ * scalar_value
    | Field of typ * tuple_prefix ref * string (* field name *)
    | StateField of typ * string (* Name of the state field - met only late in the game *)
    | Param of typ * string
    | Case of typ * case_alternative list * t option
    | Coalesce of typ * t list
    (* On functions, internal states, and aggregates:
     *
     * Functions come in three variety:
     * - pure functions: their value depends solely on their parameters, and
     *   is computed whenever it is required.
     * - functions with an internal state, which need to be:
     *   - initialized when the window starts
     *   - updated with the new values of their parameter at each step
     *   - finalize a result when they need to be evaluated - this can be
     *     done several times, ie the same internal state can "fire" several
     *     values
     *   - clean their initial state when the window is moved (we currently
     *     handle this automatically by resetting the state to its initial
     *     value and replay the kept tuples, but this could be improved with
     *     some support from the functions).
     *
     * Aggregate functions have an internal state, but not all functions with
     * an internal state are aggregate functions. There is actually little
     * need to distinguish.
     *
     * When a parameter to a function with state is another function with state
     * then this second function must deliver a value at every step. This is OK
     * as we have said that a stateful function can fire several times. So for
     * example we could write "min(max(data))", which of course would be equal
     * to "first(data)", or "lag(1, lag(1, data))" equivalently to
     * "lag(2, data)", or more interestingly "lag(1, max(data))", which would
     * return the previous max within the group. Due to the fact that we
     * initialize an internal state only when the first value is met, we must
     * also get the inner function's value when initializing the outer one,
     * which requires initializing in depth first order as well.
     *
     * Pure function can be used anywhere while stateful functions can only be
     * used in clauses that have access to the group (ie. not the where_fast
     * clause).
     *
     * In this list we call AggrX an aggregate function X to help distinguish
     * with the non-aggregate variant. For instance AggrMax(data) is the max
     * of data over the group, while Max(data1, data2) is the max of data1 and
     * data2.
     * We do not further denote pure or stateful functions. *)
    | StatelessFun of typ * stateless_fun
    | StatefulFun of typ * state_lifespan * stateful_fun
    | GeneratorFun of typ * generator_fun

  and case_alternative =
    { case_cond : t (* Must be bool *) ;
      case_cons : t (* All alternatives must share a type *) }

  and state_lifespan = LocalState | GlobalState

  and stateless_fun =
    (* TODO: Other functions: random, date_part... *)
    | Age of t
    | Now
    (* FIXME: see note in CodeGenLib.ml *)
    | Sequence of t * t (* start, step *)
    | Cast of t
    (* String functions *)
    | Length of t
    | Lower of t
    | Upper of t
    (* Unary Ops on scalars *)
    | Not of t
    | Abs of t
    | Defined of t
    (* Binary Ops scalars *)
    | Add of t * t
    | Sub of t * t
    | Mul of t * t
    | Div of t * t
    | IDiv of t * t
    | Mod of t * t
    | Pow of t * t
    | Exp of t
    | Log of t
    | Sqrt of t
    | Hash of t
    | And of t * t
    | Or of t * t
    | Ge of t * t
    | Gt of t * t
    | Eq of t * t
    | Concat of t * t
    (* For network address range checks: *)
    | BeginOfRange of t
    | EndOfRange of t
    (* a LIKE operator using globs, infix *)
    | Like of t * string (* expression then pattern (using %, _ and \) *)
    (* Min/Max of the given values *)
    | Max of t list
    | Min of t list

  and stateful_fun =
    (* TODO: Add stddev... *)
    | AggrMin of t
    | AggrMax of t
    | AggrSum of t
    | AggrAvg of t
    | AggrAnd of t
    | AggrOr  of t
    | AggrFirst of t
    | AggrLast of t
    (* TODO: several percentiles. Requires multi values returns. *)
    | AggrPercentile of t * t
    (* value retarded by k steps. If we have had less than k past values
     * then return the first we've had. *)
    | Lag of t * t
    (* If the current time is t, the seasonal, moving average of period p on k
     * seasons is the average of v(t-p), v(t-2p), ... v(t-kp). Note the absence
     * of v(t).  This is because we want to compare v(t) with this season
     * average.  Notice that lag is a special case of season average with p=k
     * and k=1, but with a universal type for the data (while season-avg works
     * only on numbers).  For instance, a moving average of order 5 would be
     * period=1, count=5 *)
    | MovingAvg of t * t * t (* period, how many seasons to keep, expression *)
    (* Simple linear regression *)
    | LinReg of t * t * t (* as above: period, how many seasons to keep, expression *)
    (* Multiple linear regression - and our first variadic function (the
     * last parameter being a list of expressions to use for the predictors) *)
    | MultiLinReg of t * t * t * t list
    (* Rotating bloom filters. First parameter is the false positive rate we
     * aim at, second is an expression providing the "time", third a
     * "duration", and finally an expression whose value to remember. The
     * function will return true if it *thinks* that value has been seen the
     * same value at a time not older than the given duration. This is based
     * on bloom-filters so there can be false positives but not false
     * negatives.
     * Notes:
     * - to remember several expressions just use the hash function.
     * - if possible, it might save a lot of space to aim for a high false
     * positive rate and account for it in the surrounding calculations than
     * to aim for a low false positive rate. *)
    | Remember of t * t * t * t
    (* Simple exponential smoothing *)
    | ExpSmooth of t * t (* coef between 0 and 1 and expression *)

  and generator_fun =
    (* First function returning more than once (Generator). Here the typ is
     * type of a single value but the function is a generator and can return
     * from 0 to N such values. *)
    | Split of t * t

  let expr_true =
    Const (make_bool_typ ~nullable:false "true", VBool true)

  let expr_false =
    Const (make_bool_typ ~nullable:false "false", VBool false)

  let expr_zero =
    Const (make_typ ~typ:TU8 ~nullable:false "zero", VU8 (Uint8.of_int 0))

  let expr_one =
    Const (make_typ ~typ:TU8 ~nullable:false "one", VU8 (Uint8.of_int 1))

  let expr_null =
    Const (make_typ ~nullable:true ~typ:TNull "NULL", VNull)

  let of_float v =
    Const (make_typ ~nullable:false (string_of_float v), VFloat v)

  let is_true = function
    | Const (_ , VBool true) -> true
    | _ -> false

  let get_string_const = function
    | Const (_ , VString s) -> Some s
    | _ -> None

  let check_const what = function
    | Const _ -> ()
    | _ -> raise (SyntaxError (NotConstant what))

  let rec print with_types fmt =
    let add_types t =
      if with_types then Printf.fprintf fmt " [%a]" print_typ t
    and sl =
      (* TODO: do not display the default *)
      function LocalState -> " locally " | GlobalState -> " globally "
    in
    function
    | Const (t, c) ->
      Scalar.print fmt c ; add_types t
    | Field (t, tuple, field) ->
      Printf.fprintf fmt "%s.%s" (string_of_prefix !tuple) field ;
      add_types t
    | StateField (t, s) ->
      String.print fmt s ; add_types t
    | Param (t, p) ->
      Printf.fprintf fmt "$%s" p ; add_types t
    | Case (t, alts, else_) ->
      let print_alt fmt alt =
        Printf.fprintf fmt "WHEN %a THEN %a"
          (print with_types) alt.case_cond
          (print with_types) alt.case_cons
      in
      Printf.fprintf fmt "CASE %a "
       (List.print ~first:"" ~last:"" ~sep:" " print_alt) alts ;
      Option.may (fun else_ ->
        Printf.fprintf fmt "ELSE %a "
          (print with_types) else_) else_ ;
      Printf.fprintf fmt "END" ;
      add_types t
    | Coalesce (t, es) ->
      Printf.fprintf fmt "COALESCE %a"
        (List.print ~first:"(" ~last:")" ~sep:", "
          (print with_types)) es ;
      add_types t
    | StatelessFun (t, Age e) ->
      Printf.fprintf fmt "age (%a)" (print with_types) e ; add_types t
    | StatelessFun (t, Now) ->
      Printf.fprintf fmt "now" ; add_types t
    | StatelessFun (t, Sequence (e1, e2)) ->
      Printf.fprintf fmt "sequence(%a, %a)"
        (print with_types) e1 (print with_types) e2 ;
      add_types t
    | StatelessFun (t, Cast e) ->
      Printf.fprintf fmt "cast(%a, %a)"
        Scalar.print_typ (Option.get t.scalar_typ) (print with_types) e ;
      add_types t
    | StatelessFun (t, Length e) ->
      Printf.fprintf fmt "length (%a)" (print with_types) e ; add_types t
    | StatelessFun (t, Lower e) ->
      Printf.fprintf fmt "lower (%a)" (print with_types) e ; add_types t
    | StatelessFun (t, Upper e) ->
      Printf.fprintf fmt "upper (%a)" (print with_types) e ; add_types t
    | StatelessFun (t, Not e) ->
      Printf.fprintf fmt "NOT (%a)" (print with_types) e ; add_types t
    | StatelessFun (t, Abs e) ->
      Printf.fprintf fmt "ABS (%a)" (print with_types) e ; add_types t
    | StatelessFun (t, Defined e) ->
      Printf.fprintf fmt "(%a) IS NOT NULL" (print with_types) e ; add_types t
    | StatelessFun (t, Add (e1, e2)) ->
      Printf.fprintf fmt "(%a) + (%a)" (print with_types) e1 (print with_types) e2 ;
      add_types t
    | StatelessFun (t, Sub (e1, e2)) ->
      Printf.fprintf fmt "(%a) - (%a)" (print with_types) e1 (print with_types) e2 ;
      add_types t
    | StatelessFun (t, Mul (e1, e2)) ->
      Printf.fprintf fmt "(%a) * (%a)" (print with_types) e1 (print with_types) e2 ;
      add_types t
    | StatelessFun (t, Div (e1, e2)) ->
      Printf.fprintf fmt "(%a) / (%a)" (print with_types) e1 (print with_types) e2 ;
      add_types t
    | StatelessFun (t, IDiv (e1, e2)) ->
      Printf.fprintf fmt "(%a) // (%a)" (print with_types) e1 (print with_types) e2 ;
      add_types t
    | StatelessFun (t, Mod (e1, e2)) ->
      Printf.fprintf fmt "(%a) %% (%a)" (print with_types) e1 (print with_types) e2 ;
      add_types t
    | StatelessFun (t, Pow (e1, e2)) ->
      Printf.fprintf fmt "(%a) ^ (%a)" (print with_types) e1 (print with_types) e2 ;
      add_types t
    | StatelessFun (t, Exp e) ->
      Printf.fprintf fmt "exp (%a)" (print with_types) e ; add_types t
    | StatelessFun (t, Log e) ->
      Printf.fprintf fmt "log (%a)" (print with_types) e ; add_types t
    | StatelessFun (t, Sqrt e) ->
      Printf.fprintf fmt "sqrt (%a)" (print with_types) e ; add_types t
    | StatelessFun (t, Hash e) ->
      Printf.fprintf fmt "hash (%a)" (print with_types) e ; add_types t
    | StatelessFun (t, And (
        StatelessFun (_, Ge (e1, StatelessFun (_, BeginOfRange e2))),
        StatelessFun (_, Not (
          StatelessFun (_, Ge (e1', StatelessFun (_, EndOfRange e2'))))))) ->
      assert (e2 = e2') ;
      assert (e1 = e1') ;
      Printf.fprintf fmt "(%a) IN (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | StatelessFun (_, (BeginOfRange _|EndOfRange _)) -> assert false
    | StatelessFun (t, And (e1, e2)) -> Printf.fprintf fmt "(%a) AND (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | StatelessFun (t, Or (e1, e2)) -> Printf.fprintf fmt "(%a) OR (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | StatelessFun (t, Ge (e1, e2)) -> Printf.fprintf fmt "(%a) >= (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | StatelessFun (t, Gt (e1, e2)) -> Printf.fprintf fmt "(%a) > (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | StatelessFun (t, Eq (e1, e2)) -> Printf.fprintf fmt "(%a) = (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | StatelessFun (t, Concat (e1, e2)) -> Printf.fprintf fmt "(%a) || (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | StatelessFun (t, Like (e, p)) -> Printf.fprintf fmt "(%a) LIKE %S" (print with_types) e p ; add_types t
    | StatelessFun (t, Max es) ->
      Printf.fprintf fmt "GREATEST (%a)" (List.print (print with_types)) es ;
      add_types t
    | StatelessFun (t, Min es) ->
      Printf.fprintf fmt "LEAST (%a)" (List.print (print with_types)) es ;
      add_types t

    | StatefulFun (t, g, AggrMin e) ->
      Printf.fprintf fmt "min%s(%a)" (sl g) (print with_types) e ; add_types t
    | StatefulFun (t, g, AggrMax e) ->
      Printf.fprintf fmt "max%s(%a)" (sl g) (print with_types) e ; add_types t
    | StatefulFun (t, g, AggrSum e) ->
      Printf.fprintf fmt "sum%s(%a)" (sl g) (print with_types) e ; add_types t
    | StatefulFun (t, g, AggrAvg e) ->
      Printf.fprintf fmt "avg%s(%a)" (sl g) (print with_types) e ; add_types t
    | StatefulFun (t, g, AggrAnd e) ->
      Printf.fprintf fmt "and%s(%a)" (sl g) (print with_types) e ; add_types t
    | StatefulFun (t, g, AggrOr e) ->
      Printf.fprintf fmt "or%s(%a)" (sl g) (print with_types) e ; add_types t
    | StatefulFun (t, g, AggrFirst e) ->
      Printf.fprintf fmt "first%s(%a)" (sl g) (print with_types) e ; add_types t
    | StatefulFun (t, g, AggrLast e) ->
      Printf.fprintf fmt "last%s(%a)" (sl g) (print with_types) e ; add_types t
    | StatefulFun (t, g, AggrPercentile (p, e)) ->
      Printf.fprintf fmt "%ath percentile%s(%a)"
        (print with_types) p (sl g) (print with_types) e ;
      add_types t
    | StatefulFun (t, g, Lag (e1, e2)) ->
      Printf.fprintf fmt "lag%s(%a, %a)"
        (sl g) (print with_types) e1 (print with_types) e2 ;
      add_types t
    | StatefulFun (t, g, MovingAvg (e1, e2, e3)) ->
      Printf.fprintf fmt "season_moveavg%s(%a, %a, %a)"
        (sl g) (print with_types) e1 (print with_types) e2 (print with_types) e3 ;
      add_types t
    | StatefulFun (t, g, LinReg (e1, e2, e3)) ->
      Printf.fprintf fmt "season_fit%s(%a, %a, %a)"
        (sl g) (print with_types) e1 (print with_types) e2 (print with_types) e3 ;
      add_types t
    | StatefulFun (t, g, MultiLinReg (e1, e2, e3, e4s)) ->
      Printf.fprintf fmt "season_fit_multi%s(%a, %a, %a, %a)"
        (sl g)
        (print with_types) e1
        (print with_types) e2
        (print with_types) e3
        (List.print ~first:"" ~last:"" ~sep:", " (print with_types)) e4s ;
      add_types t
    | StatefulFun (t, g, Remember (fpr, tim, dur, e)) ->
      Printf.fprintf fmt "remember%s(%a, %a, %a, %a)"
        (sl g)
        (print with_types) fpr
        (print with_types) tim
        (print with_types) dur
        (print with_types) e ;
      add_types t
    | StatefulFun (t, g, ExpSmooth (e1, e2)) ->
      Printf.fprintf fmt "smooth%s(%a, %a)"
        (sl g)  (print with_types) e1 (print with_types) e2 ;
      add_types t

    | GeneratorFun (t, Split (e1, e2)) ->
      Printf.fprintf fmt "split(%a, %a)"
        (print with_types) e1 (print with_types) e2 ;
      add_types t

  let typ_of = function
    | Const (t, _) | Field (t, _, _) | Param (t, _) | StateField (t, _)
    | StatelessFun (t, _) | StatefulFun (t, _, _) | GeneratorFun (t, _)
    | Case (t, _, _) | Coalesce (t, _) ->
      t

  let is_nullable e =
    let t = typ_of e in
    t.nullable = Some true

  (* Propagate values up the tree only, depth first. *)
  let rec fold_by_depth f i expr =
    match expr with
    | Const _ | Param _ | Field _ | StateField _ | StatelessFun (_, Now) ->
      f i expr

    | StatefulFun (_, _, AggrMin e) | StatefulFun (_, _, AggrMax e)
    | StatefulFun (_, _, AggrSum e) | StatefulFun (_, _, AggrAvg e)
    | StatefulFun (_, _, AggrAnd e) | StatefulFun (_, _, AggrOr e)
    | StatefulFun (_, _, AggrFirst e) | StatefulFun (_, _, AggrLast e)
    | StatelessFun (_, Age e)
    | StatelessFun (_, Not e) | StatelessFun (_, Defined e)
    | StatelessFun (_, Cast e) | StatelessFun (_, Abs e)
    | StatelessFun (_, Length e) | StatelessFun (_, Lower e)
    | StatelessFun (_, Upper e) | StatelessFun (_, BeginOfRange e)
    | StatelessFun (_, EndOfRange e) | StatelessFun (_, Exp e)
    | StatelessFun (_, Log e) | StatelessFun (_, Sqrt e)
    | StatelessFun (_, Hash e) | StatelessFun (_, Like (e, _)) ->
      f (fold_by_depth f i e) expr

    | StatefulFun (_, _, AggrPercentile (e1, e2))
    | StatelessFun (_, Sequence (e1, e2)) | StatelessFun (_, Add (e1, e2))
    | StatelessFun (_, Sub (e1, e2)) | StatelessFun (_, Mul (e1, e2))
    | StatelessFun (_, Div (e1, e2)) | StatelessFun (_, IDiv (e1, e2))
    | StatelessFun (_, Pow (e1, e2)) | StatelessFun (_, And (e1, e2))
    | StatelessFun (_, Or (e1, e2)) | StatelessFun (_, Ge (e1, e2))
    | StatelessFun (_, Gt (e1, e2)) | StatelessFun (_, Eq (e1, e2))
    | StatelessFun (_, Mod (e1, e2)) | StatefulFun (_, _, Lag (e1, e2))
    | StatefulFun (_, _, ExpSmooth (e1, e2))
    | StatelessFun (_, Concat (e1, e2)) | GeneratorFun (_, Split (e1, e2)) ->
      let i' = fold_by_depth f i e1 in
      let i''= fold_by_depth f i' e2 in
      f i'' expr

    | StatefulFun (_, _, MovingAvg (e1, e2, e3))
    | StatefulFun (_, _, LinReg (e1, e2, e3)) ->
      let i' = fold_by_depth f i e1 in
      let i''= fold_by_depth f i' e2 in
      let i'''= fold_by_depth f i'' e3 in
      f i''' expr

    | StatefulFun (_, _, Remember (e1, e2, e3, e4)) ->
      let i' = fold_by_depth f i e1 in
      let i''= fold_by_depth f i' e2 in
      let i'''= fold_by_depth f i'' e3 in
      let i''''= fold_by_depth f i''' e4 in
      f i'''' expr

    | StatefulFun (_, _, MultiLinReg (e1, e2, e3, e4s)) ->
      let i' = fold_by_depth f i e1 in
      let i''= fold_by_depth f i' e2 in
      let i'''= fold_by_depth f i'' e3 in
      let i''''= List.fold_left (fold_by_depth f) i''' e4s in
      f i'''' expr

    | Case (_, alts, else_) ->
      let i' =
        List.fold_left (fun i alt ->
          let i' = fold_by_depth f i alt.case_cond in
          let i''= fold_by_depth f i' alt.case_cons in
          f i'' expr) i alts in
      let i''=
        Option.map_default (fold_by_depth f i') i' else_ in
      f i'' expr
    | Coalesce (_, es)
    | StatelessFun (_, (Max es | Min es)) ->
      let i' = List.fold_left (fold_by_depth f) i es in
      f i' expr

  let iter f = fold_by_depth (fun () e -> f e) ()

  let unpure_iter f e =
    fold_by_depth (fun () -> function
      | StatefulFun _ as e -> f e
      | _ -> ()) () e |> ignore

  (* Any expression that uses a generator is a generator: *)
  let is_generator =
    fold_by_depth (fun is e ->
      is || match e with GeneratorFun _ -> true | _ -> false) false

  let rec map_type ?(recurs=true) f = function
    | Const (t, a) -> Const (f t, a)
    | Field (t, a, b) -> Field (f t, a, b)
    | StateField _ as e -> e
    | Param (t, a) -> Param (f t, a)

    | Case (t, alts, else_) ->
      Case (f t,
            (if recurs then List.map (fun alt ->
               { case_cond = map_type ~recurs f alt.case_cond ;
                 case_cons = map_type ~recurs f alt.case_cons }) alts
             else alts),
            if recurs then Option.map (map_type ~recurs f) else_ else else_)
    | Coalesce (t, es) ->
      Coalesce (f t,
                if recurs then List.map (map_type ~recurs f) es else es)

    | StatefulFun (t, g, AggrMin a) ->
      StatefulFun (f t, g, AggrMin (if recurs then map_type ~recurs f a else a))
    | StatefulFun (t, g, AggrMax a) ->
      StatefulFun (f t, g, AggrMax (if recurs then map_type ~recurs f a else a))
    | StatefulFun (t, g, AggrSum a) ->
      StatefulFun (f t, g, AggrSum (if recurs then map_type ~recurs f a else a))
    | StatefulFun (t, g, AggrAvg a) ->
      StatefulFun (f t, g, AggrAvg (if recurs then map_type ~recurs f a else a))
    | StatefulFun (t, g, AggrAnd a) ->
      StatefulFun (f t, g, AggrAnd (if recurs then map_type ~recurs f a else a))
    | StatefulFun (t, g, AggrOr a) ->
      StatefulFun (f t, g, AggrOr (if recurs then map_type ~recurs f a else a))
    | StatefulFun (t, g, AggrFirst a) ->
      StatefulFun (f t, g, AggrFirst (if recurs then map_type ~recurs f a else a))
    | StatefulFun (t, g, AggrLast a) ->
      StatefulFun (f t, g, AggrLast (if recurs then map_type ~recurs f a else a))
    | StatefulFun (t, g, AggrPercentile (a, b)) ->
      StatefulFun (f t, g, AggrPercentile (
          (if recurs then map_type ~recurs f a else a),
          (if recurs then map_type ~recurs f b else b)))
    | StatefulFun (t, g, Lag (a, b)) ->
      StatefulFun (f t, g, Lag (
          (if recurs then map_type ~recurs f a else a),
          (if recurs then map_type ~recurs f b else b)))
    | StatefulFun (t, g, MovingAvg (a, b, c)) ->
      StatefulFun (f t, g, MovingAvg (
          (if recurs then map_type ~recurs f a else a),
          (if recurs then map_type ~recurs f b else b),
          (if recurs then map_type ~recurs f c else c)))
    | StatefulFun (t, g, LinReg (a, b, c)) ->
      StatefulFun (f t, g, LinReg (
          (if recurs then map_type ~recurs f a else a),
          (if recurs then map_type ~recurs f b else b),
          (if recurs then map_type ~recurs f c else c)))
    | StatefulFun (t, g, MultiLinReg (a, b, c, d)) ->
      StatefulFun (f t, g, MultiLinReg (
          (if recurs then map_type ~recurs f a else a),
          (if recurs then map_type ~recurs f b else b),
          (if recurs then map_type ~recurs f c else c),
          (if recurs then List.map (map_type ~recurs f) d else d)))
    | StatefulFun (t, g, Remember (fpr, tim, dur, e)) ->
      StatefulFun (f t, g, Remember (
          (if recurs then map_type ~recurs f fpr else fpr),
          (if recurs then map_type ~recurs f tim else tim),
          (if recurs then map_type ~recurs f dur else dur),
          (if recurs then map_type ~recurs f e else e)))
    | StatefulFun (t, g, ExpSmooth (a, b)) ->
      StatefulFun (f t, g, ExpSmooth (
          (if recurs then map_type ~recurs f a else a),
          (if recurs then map_type ~recurs f b else b)))

    | StatelessFun (t, Age a) ->
      StatelessFun (f t, Age (if recurs then map_type ~recurs f a else a))
    | StatelessFun (t, Now) -> StatelessFun (f t, Now)
    | StatelessFun (t, Sequence (a, b)) ->
      StatelessFun (f t, Sequence (
          (if recurs then map_type ~recurs f a else a),
          (if recurs then map_type ~recurs f b else b)))
    | StatelessFun (t, Abs a) ->
      StatelessFun (f t, Abs (if recurs then map_type ~recurs f a else a))
    | StatelessFun (t, Cast a) ->
      StatelessFun (f t, Cast (if recurs then map_type ~recurs f a else a))
    | StatelessFun (t, Length a) ->
      StatelessFun (f t, Length (if recurs then map_type ~recurs f a else a))
    | StatelessFun (t, Lower a) ->
      StatelessFun (f t, Lower (if recurs then map_type ~recurs f a else a))
    | StatelessFun (t, Upper a) ->
      StatelessFun (f t, Upper (if recurs then map_type ~recurs f a else a))
    | StatelessFun (t, Not a) ->
      StatelessFun (f t, Not (if recurs then map_type ~recurs f a else a))
    | StatelessFun (t, Defined a) ->
      StatelessFun (f t, Defined (if recurs then map_type ~recurs f a else a))
    | StatelessFun (t, Add (a, b)) ->
      StatelessFun (f t, Add (
          (if recurs then map_type ~recurs f a else a),
          (if recurs then map_type ~recurs f b else b)))
    | StatelessFun (t, Sub (a, b)) ->
      StatelessFun (f t, Sub (
          (if recurs then map_type ~recurs f a else a),
          (if recurs then map_type ~recurs f b else b)))
    | StatelessFun (t, Mul (a, b)) ->
      StatelessFun (f t, Mul (
          (if recurs then map_type ~recurs f a else a),
          (if recurs then map_type ~recurs f b else b)))
    | StatelessFun (t, Div (a, b)) ->
      StatelessFun (f t, Div (
          (if recurs then map_type ~recurs f a else a),
          (if recurs then map_type ~recurs f b else b)))
    | StatelessFun (t, IDiv (a, b)) ->
      StatelessFun (f t, IDiv (
           (if recurs then map_type ~recurs f a else a),
           (if recurs then map_type ~recurs f b else b)))
    | StatelessFun (t, Mod (a, b)) ->
      StatelessFun (f t, Mod (
          (if recurs then map_type ~recurs f a else a),
          (if recurs then map_type ~recurs f b else b)))
    | StatelessFun (t, Pow (a, b)) ->
      StatelessFun (f t, Pow (
          (if recurs then map_type ~recurs f a else a),
          (if recurs then map_type ~recurs f b else b)))
    | StatelessFun (t, Exp a) ->
      StatelessFun (f t, Exp (if recurs then map_type ~recurs f a else a))
    | StatelessFun (t, Log a) ->
      StatelessFun (f t, Log (if recurs then map_type ~recurs f a else a))
    | StatelessFun (t, Sqrt a) ->
      StatelessFun (f t, Sqrt (if recurs then map_type ~recurs f a else a))
    | StatelessFun (t, Hash a) ->
      StatelessFun (f t, Hash (if recurs then map_type ~recurs f a else a))
    | StatelessFun (t, And (a, b)) ->
      StatelessFun (f t, And (
          (if recurs then map_type ~recurs f a else a),
          (if recurs then map_type ~recurs f b else b)))
    | StatelessFun (t, Or (a, b)) ->
      StatelessFun (f t, Or (
          (if recurs then map_type ~recurs f a else a),
          (if recurs then map_type ~recurs f b else b)))
    | StatelessFun (t, Ge (a, b)) ->
      StatelessFun (f t, Ge (
          (if recurs then map_type ~recurs f a else a),
          (if recurs then map_type ~recurs f b else b)))
    | StatelessFun (t, Gt (a, b)) ->
      StatelessFun (f t, Gt (
          (if recurs then map_type ~recurs f a else a),
          (if recurs then map_type ~recurs f b else b)))
    | StatelessFun (t, Eq (a, b)) ->
      StatelessFun (f t, Eq (
          (if recurs then map_type ~recurs f a else a),
          (if recurs then map_type ~recurs f b else b)))
    | StatelessFun (t, Concat (a, b)) ->
      StatelessFun (f t, Concat (
          (if recurs then map_type ~recurs f a else a),
          (if recurs then map_type ~recurs f b else b)))
    | StatelessFun (t, Like (e, p)) ->
      StatelessFun (f t, Like (
          (if recurs then map_type ~recurs f e else e), p))
    | StatelessFun (t, BeginOfRange a) ->
      StatelessFun (f t, BeginOfRange (if recurs then map_type ~recurs f a else a))
    | StatelessFun (t, EndOfRange a) ->
      StatelessFun (f t, EndOfRange (if recurs then map_type ~recurs f a else a))
    | StatelessFun (t, Max es) ->
      StatelessFun (f t, Max
        (if recurs then List.map (map_type ~recurs f) es else es))
    | StatelessFun (t, Min es) ->
      StatelessFun (f t, Min
        (if recurs then List.map (map_type ~recurs f) es else es))

    | GeneratorFun (t, Split (a, b)) ->
      GeneratorFun (f t, Split (
          (if recurs then map_type ~recurs f a else a),
          (if recurs then map_type ~recurs f b else b)))

  module Parser =
  struct
    (*$< Parser *)
    open RamenParsing

    (* Single things *)
    let const m =
      let m = "constant" :: m in
      (Scalar.Parser.p >>: fun c ->
       Const (make_typ ~nullable:false ~typ:(scalar_type_of c) "constant", c)) m
    (*$= const & ~printer:(test_printer (print false))
      (Ok (Const (typ, VBool true), (4, [])))\
        (test_p const "true" |> replace_typ_in_expr)
    *)

    let null m =
      let m = "NULL" :: m in
      (strinG "null" >>: fun () -> expr_null) m

    let field m =
      let m = "field" :: m in
      ((parse_prefix ~def:TupleUnknown ++ non_keyword >>:
        fun (tuple, field) ->
          (* This is important here that the type name is the raw field name,
           * because we use the tuple field type name as their identifier (unless
           * it's a virtual field (starting with #) of course since those are
           * computed on the fly and have no variable corresponding to them in
           * the tuple) *)
          Field (make_typ field, ref tuple, field)) |||
       (parse_prefix ~def:TupleIn ++ that_string "#count" >>:
        fun (tuple, field) ->
          if not (tuple_has_count tuple) then raise (Reject "This tuple has no #count") ;
          Field (make_typ ~nullable:false ~typ:TU64 field, ref tuple, field)) |||
       (parse_prefix ~def:TupleIn ++ that_string "#successive" >>:
        fun (tuple, field) ->
          if not (tuple_has_successive tuple) then raise (Reject "This tuple has no #successive") ;
          Field (make_typ ~nullable:false ~typ:TU64 field, ref tuple, field))
      ) m

    (*$= field & ~printer:(test_printer (print false))
      (Ok (\
        Field (typ, ref TupleUnknown, "bytes"),\
        (5, [])))\
        (test_p field "bytes" |> replace_typ_in_expr)

      (Ok (\
        Field (typ, ref TupleIn, "bytes"),\
        (8, [])))\
        (test_p field "in.bytes" |> replace_typ_in_expr)

      (Ok (\
        Field (typ, ref TupleOut, "bytes"),\
        (9, [])))\
        (test_p field "out.bytes" |> replace_typ_in_expr)

      (Bad (\
        NoSolution (\
          Some { where = ParsersMisc.Item ((0,7), '.');\
                 what=["eof"]})))\
        (test_p field "pasglop.bytes" |> replace_typ_in_expr)

      (Ok (\
        Field (typ, ref TupleIn, "#count"),\
        (6, [])))\
        (test_p field "#count" |> replace_typ_in_expr)

      (Bad (\
        NoSolution (\
          Some { where = ParsersMisc.Item ((0,7), 'c') ;\
                 what = ["\"#successive\"";"field"]})))\
        (test_p field "first.#count" |> replace_typ_in_expr)
    *)

    let param m =
      let m = "param" :: m in
      (char '$' -+ identifier >>: fun s ->
       Param (make_typ ("parameter "^s), s)) m
    (*$= param & ~printer:(test_printer (print false))
      (Ok (\
        Param (typ, "glop"),\
        (5, [])))\
        (test_p param "$glop" |> replace_typ_in_expr)

      (Bad (\
        NoSolution (\
          Some { where = ParsersMisc.Item ((0,0), 'g');\
                 what = ["\"$\""; "param"] })))\
      (test_p param "glop" |> replace_typ_in_expr)
    *)

    let state_lifespan m =
      let m = "state lifespan" :: m in
      ((strinG "globally" >>: fun () -> GlobalState) |||
       (strinG "locally" >>: fun () -> LocalState)) m

    (* operators with lowest precedence *)
    let rec lowest_prec_left_assoc m =
      let m = "logical operator" :: m in
      let op = that_string "and" ||| that_string "or"
      and reduce t1 op t2 = match op with
        | "and" -> StatelessFun (make_bool_typ "and", And (t1, t2))
        | "or" -> StatelessFun (make_bool_typ "or", Or (t1, t2))
        | _ -> assert false in
      (* FIXME: we do not need a blanks if we had parentheses ("(x)AND(y)" is OK) *)
      binary_ops_reducer ~op ~term:low_prec_left_assoc ~sep:blanks ~reduce m

    and low_prec_left_assoc m =
      let m = "comparison operator" :: m in
      let op = that_string ">" ||| that_string ">=" ||| that_string "<" ||| that_string "<=" |||
               that_string "=" ||| that_string "<>" ||| that_string "!=" |||
               that_string "in" ||| that_string "like"
      and reduce t1 op t2 = match op with
        | ">" -> StatelessFun (make_bool_typ "comparison", Gt (t1, t2))
        | "<" -> StatelessFun (make_bool_typ "comparison", Gt (t2, t1))
        | ">=" -> StatelessFun (make_bool_typ "comparison", Ge (t1, t2))
        | "<=" -> StatelessFun (make_bool_typ "comparison", Ge (t2, t1))
        | "=" -> StatelessFun (make_bool_typ "equality", Eq (t1, t2))
        | "!=" | "<>" ->
          StatelessFun (make_bool_typ "not", Not (
            StatelessFun (make_bool_typ "equality", Eq (t1, t2))))
        | "in" ->
          StatelessFun (make_bool_typ "and for range", And (
            StatelessFun (make_bool_typ "comparison operator for range", Ge (
              t1,
              StatelessFun (make_typ "begin of range", BeginOfRange t2))),
            StatelessFun (make_bool_typ "not for range", Not (
              StatelessFun (make_bool_typ "comparison operator for range", Ge (
                t1,
                StatelessFun (make_typ "end of range", EndOfRange t2)))))))
        | "like" ->
          (match get_string_const t2 with
          | None -> raise (Reject "LIKE pattern must be a string constant")
          | Some p ->
            StatelessFun (make_bool_typ "like", Like (t1, p)))
        | _ -> assert false in
      binary_ops_reducer ~op ~term:mid_prec_left_assoc ~sep:opt_blanks ~reduce m

    and mid_prec_left_assoc m =
      let m = "arithmetic operator" :: m in
      let op = that_string "+" ||| that_string "-" ||| that_string "||"
      and reduce t1 op t2 = match op with
        | "+" -> StatelessFun (make_num_typ "addition", Add (t1, t2))
        | "-" -> StatelessFun (make_num_typ "subtraction", Sub (t1, t2))
        | "||" -> StatelessFun (make_string_typ "concatenation", Concat (t1, t2))
        | _ -> assert false in
      binary_ops_reducer ~op ~term:high_prec_left_assoc ~sep:opt_blanks ~reduce m

    and high_prec_left_assoc m =
      let m = "arithmetic operator" :: m in
      let op = that_string "*" ||| that_string "//" ||| that_string "/" ||| that_string "%"
      and reduce t1 op t2 = match op with
        | "*" -> StatelessFun (make_num_typ "multiplication", Mul (t1, t2))
        (* Note: We want the default division to output floats by default *)
        | "/" -> StatelessFun (make_typ ~typ:TFloat "division", Div (t1, t2))
        | "//" -> StatelessFun (make_num_typ "integer-division", IDiv (t1, t2))
        | "%" -> StatelessFun (make_num_typ "modulo", Mod (t1, t2))
        | _ -> assert false in
      binary_ops_reducer ~op ~term:higher_prec_right_assoc ~sep:opt_blanks ~reduce m

    and higher_prec_right_assoc m =
      let m = "arithmetic operator" :: m in
      let op = char '^'
      and reduce t1 _ t2 = StatelessFun (make_num_typ "exponentiation", Pow (t1, t2)) in
      binary_ops_reducer ~op ~right_associative:true
                         ~term:highest_prec_left_assoc ~sep:opt_blanks ~reduce m

    (* "sf" stands for "stateful" *)
    and afunv_sf ?(def_state=GlobalState) a n m =
      let sep = opt_blanks -- char ',' -- opt_blanks in
      let m = n :: m in
      (strinG n -+
       optional ~def:def_state (blanks -+ state_lifespan) +-
       opt_blanks +- char '(' +- opt_blanks ++
       (if a > 0 then
         repeat ~what:"mandatory arguments" ~min:a ~max:a ~sep
           lowest_prec_left_assoc ++
         optional ~def:[] (sep -+ repeat ~what:"variadic arguments" ~sep
                                         lowest_prec_left_assoc)
        else
         return [] ++
         repeat ~what:"variadic arguments" ~sep lowest_prec_left_assoc) +-
       opt_blanks +- char ')') m

    and afun_sf ?def_state a n =
      afunv_sf ?def_state a n >>: fun (g, (a, r)) ->
        if r = [] then g, a else
        raise (Reject "too many arguments")

    and afun1_sf ?(def_state=GlobalState) n =
      let sep = check (char '(') ||| blanks in
      (strinG n -+ optional ~def:def_state (blanks -+ state_lifespan) +-
       sep ++ highestest_prec)

    and afun2_sf ?def_state n =
      afun_sf ?def_state 2 n >>: function (g, [a;b]) -> g, a, b | _ -> assert false

    and afun2v_sf ?def_state n =
      afunv_sf ?def_state 2 n >>: function (g, ([a;b], r)) -> g, a, b, r | _ -> assert false

    and afun3_sf ?def_state n =
      afun_sf ?def_state 3 n >>: function (g, [a;b;c]) -> g, a, b, c | _ -> assert false

    and afun3v_sf ?def_state n =
      afunv_sf ?def_state 3 n >>: function (g, ([a;b;c], r)) -> g, a, b, c, r | _ -> assert false

    and afun4_sf ?def_state n =
      afun_sf ?def_state 4 n >>: function (g, [a;b;c;d]) -> g, a, b, c, d | _ -> assert false

    and afunv a n m =
      let sep = opt_blanks -- char ',' -- opt_blanks in
      let m = n :: m in
      (strinG n -- opt_blanks -- char '(' -- opt_blanks -+
       (if a > 0 then
         repeat ~what:"mandatory arguments" ~min:a ~max:a ~sep
           lowest_prec_left_assoc ++
         optional ~def:[] (sep -+ repeat ~what:"variadic arguments" ~sep
                                         lowest_prec_left_assoc)
        else
         return [] ++
         repeat ~what:"variadic arguments" ~sep lowest_prec_left_assoc) +-
       opt_blanks +- char ')') m

    and afun a n =
      afunv a n >>: fun (a, r) ->
        if r = [] then a else
        raise (Reject "too many arguments")

    and afun1 n =
      let sep = check (char '(') ||| blanks in
      strinG n -- sep -+ highestest_prec

    and afun2 n =
      afun 2 n >>: function [a;b] -> a, b | _ -> assert false

    and afun3 n =
      afun 3 n >>: function [a;b;c] -> a, b, c | _ -> assert false

    and afun4 n =
      afun 4 n >>: function [a;b;c;d] -> a, b, c, d | _ -> assert false

    and afun5 n =
      afun 5 n >>: function [a;b;c;d;e] -> a, b, c, d, e | _ -> assert false

    and afun0v n =
      afunv 0 n >>: function ([], r) -> r | _ -> assert false

    and afun1v n =
      afunv 1 n >>: function ([a], r) -> a, r | _ -> assert false

    and afun2v n =
      afunv 2 n >>: function ([a;b], r) -> a, b, r | _ -> assert false

    and afun3v n =
      afunv 3 n >>: function ([a;b;c], r) -> a, b, c, r | _ -> assert false

    and highest_prec_left_assoc m =
      ((afun1 "not" >>: fun e -> StatelessFun (make_bool_typ "not", Not e)) |||
       (highestest_prec ++
        optional ~def:None (
          blanks -- strinG "is" -- blanks -+
          optional ~def:(Some false)
                   (strinG "not" -- blanks >>: fun () -> Some true) +-
          strinG "null") >>: function
            | e, None -> e
            | e, Some false ->
              StatelessFun (make_bool_typ ~nullable:false "not",
                Not (StatelessFun (make_bool_typ ~nullable:false "is_not_null", Defined e)))
            | e, Some true ->
              StatelessFun (make_bool_typ ~nullable:false "is_not_null", Defined e))
      ) m

    and func m =
      let m = "function" :: m in
      (* Note: min and max of nothing are NULL but sum of nothing is 0, etc *)
      ((afun1 "age" >>: fun e -> StatelessFun (make_num_typ "age function", Age e)) |||
       (afun1 "abs" >>: fun e -> StatelessFun (make_num_typ "absolute value", Abs e)) |||
       (afun1 "length" >>: fun e -> StatelessFun (make_typ ~typ:TU16 "length", Length e)) |||
       (afun1 "lower" >>: fun e -> StatelessFun (make_string_typ "lower", Lower e)) |||
       (afun1 "upper" >>: fun e -> StatelessFun (make_string_typ "upper", Upper e)) |||
       (strinG "now" >>: fun () -> StatelessFun (make_float_typ ~nullable:false "now", Now)) |||
       (afun1 "exp" >>: fun e -> StatelessFun (make_num_typ "exponential", Exp e)) |||
       (afun1 "log" >>: fun e -> StatelessFun (make_num_typ "logarithm", Log e)) |||
       (afun1 "sqrt" >>: fun e -> StatelessFun (make_num_typ "square root", Sqrt e)) |||
       (afun1 "hash" >>: fun e -> StatelessFun (make_typ ~typ:TI64 "hash", Hash e)) |||
       (afun1_sf ~def_state:LocalState "min" >>: fun (g, e) ->
          StatefulFun (make_num_typ "min aggregation", g, AggrMin e)) |||
       (afun1_sf ~def_state:LocalState "max" >>: fun (g, e) ->
          StatefulFun (make_num_typ "max aggregation", g, AggrMax e)) |||
       (afun1_sf ~def_state:LocalState "sum" >>: fun (g, e) ->
          StatefulFun (make_num_typ "sum aggregation", g, AggrSum e)) |||
       (afun1_sf ~def_state:LocalState "avg" >>: fun (g, e) ->
          StatefulFun (make_num_typ "sum aggregation", g, AggrAvg e)) |||
       (afun1_sf ~def_state:LocalState "and" >>: fun (g, e) ->
          StatefulFun (make_bool_typ "and aggregation", g, AggrAnd e)) |||
       (afun1_sf ~def_state:LocalState "or" >>: fun (g, e) ->
          StatefulFun (make_bool_typ "or aggregation", g, AggrOr e)) |||
       (afun1_sf ~def_state:LocalState "first" >>: fun (g, e) ->
          StatefulFun (make_typ "first aggregation", g, AggrFirst e)) |||
       (afun1_sf ~def_state:LocalState "last" >>: fun (g, e) ->
          StatefulFun (make_typ "last aggregation", g, AggrLast e)) |||
       ((const ||| param) +- (optional ~def:() (strinG "th")) +- blanks ++
        afun1_sf ~def_state:LocalState "percentile" >>: fun (p, (g, e)) ->
          StatefulFun (make_num_typ "percentile aggregation",
                        g, AggrPercentile (p, e))) |||
       (afun2_sf "lag" >>: fun (g, e1, e2) ->
          StatefulFun (make_typ "lag", g, Lag (e1, e2))) |||
       (afun1_sf "lag" >>: fun (g, e) ->
          StatefulFun (make_typ "lag", g, Lag (expr_one, e))) |||

       (* avg perform a division thus the float type *)
       (afun3_sf "season_moveavg" >>: fun (g, e1, e2, e3) ->
          StatefulFun (make_float_typ "season_moveavg", g, MovingAvg (e1, e2, e3))) |||
       (afun2_sf "moveavg" >>: fun (g, e1, e2) ->
          StatefulFun (make_float_typ "season_moveavg", g, MovingAvg (expr_one, e1, e2))) |||
       (afun3_sf "season_fit" >>: fun (g, e1, e2, e3) ->
          StatefulFun (make_float_typ "season_fit", g, LinReg (e1, e2, e3))) |||
       (afun2_sf "fit" >>: fun (g, e1, e2) ->
          StatefulFun (make_float_typ "season_fit", g, LinReg (expr_one, e1, e2))) |||
       (afun3v_sf "season_fit_multi" >>: fun (g, e1, e2, e3, e4s) ->
          StatefulFun (make_float_typ "season_fit_multi", g, MultiLinReg (e1, e2, e3, e4s))) |||
       (afun2v_sf "fit_multi" >>: fun (g, e1, e2, e3s) ->
          StatefulFun (make_float_typ "season_fit_multi", g, MultiLinReg (expr_one, e1, e2, e3s))) |||
       (afun2_sf "smooth" >>: fun (g, e1, e2) ->
          StatefulFun (make_float_typ "smooth", g, ExpSmooth (e1, e2))) |||
       (afun1_sf "smooth" >>: fun (g, e) ->
          let alpha =
            Const (make_typ ~typ:TFloat ~nullable:false "alpha", VFloat 0.5) in
          StatefulFun (make_float_typ "smooth", g, ExpSmooth (alpha, e))) |||
       (afun3_sf "remember" >>: fun (g, tim, dir, e) ->
          let fpr = of_float 0.015 in
          StatefulFun (make_bool_typ "remember", g,
                       Remember (fpr, tim, dir, e))) |||
       (afun4_sf "remember" >>: fun (g, fpr, tim, dir, e) ->
          StatefulFun (make_bool_typ "remember", g,
                       Remember (fpr, tim, dir, e))) |||
       (afun2 "split" >>: fun (e1, e2) ->
          GeneratorFun (make_typ ~typ:TString "split", Split (e1, e2))) |||
       (* At least 2 args to distinguish from the aggregate functions: *)
       (afun2v "max" >>: fun (e1, e2, e3s) ->
          StatelessFun (make_num_typ "max", Max (e1 :: e2 :: e3s))) |||
       (afun1v "greatest" >>: fun (e, es) ->
          StatelessFun (make_num_typ "max", Max (e :: es))) |||
       (afun2v "min" >>: fun (e1, e2, e3s) ->
          StatelessFun (make_num_typ "min", Min (e1 :: e2 :: e3s))) |||
       (afun1v "least" >>: fun (e, es) ->
          StatelessFun (make_num_typ "min", Min (e :: es))) |||
       k_moveavg ||| sequence ||| cast ||| hysteresis) m

    and sequence =
      let seq = "sequence"
      and seq_typ = make_typ ~nullable:false ~typ:TI128 "sequence function"
      and seq_default_step = Const (make_typ ~nullable:false ~typ:TU8 "sequence step",
                                    VU8 (Uint8.one))
      and seq_default_start = Const (make_typ ~nullable:false ~typ:TU8 "sequence start",
                                     VU8 (Uint8.zero)) in
      fun m ->
        let m = "sequence function" :: m in
        ((afun2 seq >>: fun (e1, e2) ->
            StatelessFun (seq_typ, Sequence (e1, e2))) |||
         (afun1 seq >>: fun e1 ->
            StatelessFun (seq_typ, Sequence (e1, seq_default_step))) |||
         (strinG seq >>: fun () ->
            StatelessFun (seq_typ, Sequence (seq_default_start, seq_default_step)))
        ) m

    and cast m =
      let m = "cast" :: m in
      let sep = check (char '(') ||| blanks in
      (Scalar.Parser.typ +- sep ++
       highestest_prec >>: fun (typ, e) ->
         StatelessFun (make_typ ~typ ("cast to "^ IO.to_string Scalar.print_typ typ), Cast e)
      ) m

    and k_moveavg m =
      let m = "k-moving average" :: m in
      let sep = check (char '(') ||| blanks in
      ((unsigned_decimal_number >>: Scalar.Parser.narrowest_int_scalar) +-
       (strinG "-moveavg" ||| strinG "-ma") ++
       optional ~def:GlobalState (blanks -+ state_lifespan) +-
       sep ++ highestest_prec >>: fun ((k, g), e) ->
         let k = Const (make_typ ~nullable:false ~typ:(scalar_type_of k)
                                 "moving average order", k) in
         StatefulFun (make_float_typ "moveavg", g, MovingAvg (expr_one, k, e))) m

    (* Syntactic sugar for threshold with hysteresis check, basically
     * replacing:
     *   hysteresis(result, measured, threshold, 0.1)
     * by:
     *   measured >= threshold - (IF result THEN threshold * 0.1
     *                                      ELSE 0) AS result *)
    and hysteresis m =
      let m = "hysteresis" :: m in
      let build_hysteresis prev meas thrd gap is_max =
        let case =
          Case (
            make_num_typ "case for hysteresis",
            [ { case_cond = prev ;
                case_cons =
                  StatelessFun (
                    make_num_typ "hysteresis gap",
                    Mul (gap, thrd)) } ],
            (* else *)
            Some expr_zero) in
        let thrd' =
          if is_max then
            StatelessFun (
              make_num_typ "subtraction for hysteresis",
              Sub (thrd, case))
          else
            StatelessFun (
              make_num_typ "addition for hysteresis",
              Add (thrd, case)) in
        StatelessFun (
          make_bool_typ "comparison operator for hysteresis",
          if is_max then Ge (meas, thrd') else Ge (thrd', meas)) in
      let gap = Const (make_float_typ ~nullable:false "hysteresis ratio",
                       VFloat 0.15)
      in
      ((afun4 "hysteresis_max" >>: fun (res, meas, thrd, gap) ->
          build_hysteresis res meas thrd gap true) |||
       (afun3 "hysteresis_max" >>: fun (res, meas, thrd) ->
          build_hysteresis res meas thrd gap true) |||
       (afun4 "hysteresis_min" >>: fun (res, meas, thrd, gap) ->
          build_hysteresis res meas thrd gap false) |||
       (afun3 "hysteresis_min" >>: fun (res, meas, thrd) ->
          build_hysteresis res meas thrd gap false) |||
       (afun4 "hysteresis" >>: fun (res, meas, min, max) ->
          StatelessFun (make_bool_typ "or", Or (
            build_hysteresis res meas max gap true,
            build_hysteresis res meas min gap false))) |||
       (afun5 "hysteresis" >>: fun (res, meas, min, max, gap) ->
          StatelessFun (make_bool_typ "or", Or (
            build_hysteresis res meas max gap true,
            build_hysteresis res meas min gap false)))) m

    and case m =
      let m = "case" :: m in
      let alt m =
        let m = "case alternative" :: m in
        (strinG "when" -- blanks -+ lowest_prec_left_assoc +-
         blanks +- strinG "then" +- blanks ++ lowest_prec_left_assoc >>:
         fun (cd, cs) -> { case_cond = cd ; case_cons = cs }) m
      in
      (strinG "case" -- blanks -+
       several ~sep:blanks alt +- blanks ++
       optional ~def:None (
         strinG "else" -- blanks -+ some lowest_prec_left_assoc +- blanks) +-
       strinG "end" >>: fun (alts, else_) ->
         Case (make_typ "case", alts, else_)) m

    and if_ m =
      let m = "if" :: m in
      ((strinG "if" -- blanks -+ lowest_prec_left_assoc +-
        blanks +- strinG "then" +- blanks ++ lowest_prec_left_assoc ++
        optional ~def:None (
          blanks -- strinG "else" -- blanks -+
          some lowest_prec_left_assoc) >>:
        fun ((case_cond, case_cons), else_) ->
          Case (make_typ "conditional", [ { case_cond ; case_cons } ], else_)) |||
       (afun2 "if" >>: fun (case_cond, case_cons) ->
          Case (make_typ "conditional", [ { case_cond ; case_cons } ], None)) |||
       (afun3 "if" >>: fun (case_cond, case_cons, else_) ->
          Case (make_typ "conditional", [ { case_cond ; case_cons } ], Some else_))) m

    and coalesce m =
      let m = "coalesce" :: m in
      (afun0v "coalesce" >>: function
         | r ->
           let rec loop es = function
             | [] -> expr_null
             | [e] ->
               if es = [] then e else
               Coalesce (make_typ ~nullable:false "coalesce", List.rev (e::es))
             | e::rest ->
               loop (e :: es) rest
           in
           loop [] r) m

    and highestest_prec m =
      (const ||| field ||| param ||| func ||| null |||
       case ||| if_ ||| coalesce |||
       char '(' -- opt_blanks -+
         lowest_prec_left_assoc +-
       opt_blanks +- char ')'
      ) m

    let p = lowest_prec_left_assoc

    (*$= p & ~printer:(test_printer (print false))
      (Ok (\
        Const (typ, VBool true),\
        (4, [])))\
        (test_p p "true" |> replace_typ_in_expr)

      (Ok (\
        StatelessFun (typ, Not (StatelessFun (typ, Defined (Field (typ, ref TupleUnknown, "zone_src"))))),\
        (16, [])))\
        (test_p p "zone_src IS NULL" |> replace_typ_in_expr)

      (Ok (\
        StatelessFun (typ, Eq (Field (typ, ref TupleUnknown, "zone_src"), Param (typ, "z1"))),\
        (14, [])))\
        (test_p p "zone_src = $z1" |> replace_typ_in_expr)

      (Ok (\
        StatelessFun (typ, And (\
          StatelessFun (typ, Or (\
            StatelessFun (typ, Not (\
              StatelessFun (typ, Defined (Field (typ, ref TupleUnknown, "zone_src"))))),\
            StatelessFun (typ, Eq (Field (typ, ref TupleUnknown, "zone_src"), Param (typ, "z1"))))),\
          StatelessFun (typ, Or (\
            StatelessFun (typ, Not (\
              StatelessFun (typ, Defined (Field (typ, ref TupleUnknown, "zone_dst"))))),\
            StatelessFun (typ, Eq (\
              Field (typ, ref TupleUnknown, "zone_dst"), Param (typ, "z2"))))))),\
        (77, [])))\
        (test_p p "(zone_src IS NULL or zone_src = $z1) and \\
                   (zone_dst IS NULL or zone_dst = $z2)" |> replace_typ_in_expr)

      (Ok (\
        StatelessFun (typ, Div (\
          StatefulFun (typ, LocalState, AggrSum (\
            Field (typ, ref TupleUnknown, "bytes"))),\
          Param (typ, "avg_window"))),\
        (23, [])))\
        (test_p p "(sum bytes)/$avg_window" |> replace_typ_in_expr)

      (Ok (\
        StatelessFun (typ, IDiv (\
          Field (typ, ref TupleUnknown, "start"),\
          StatelessFun (typ, Mul (\
            Const (typ, VI32 1_000_000l),\
            Param (typ, "avg_window"))))),\
        (34, [])))\
        (test_p p "start // (1_000_000 * $avg_window)" |> replace_typ_in_expr)

      (Ok (\
        StatefulFun (typ, LocalState, AggrPercentile (\
          Param (typ, "p"),\
          Field (typ, ref TupleUnknown, "bytes_per_sec"))),\
        (27, [])))\
        (test_p p "$p percentile bytes_per_sec" |> replace_typ_in_expr)

      (Ok (\
        StatelessFun (typ, Gt (\
          StatefulFun (typ, LocalState, AggrMax (\
            Field (typ, ref TupleLastSelected, "start"))),\
          StatelessFun (typ, Add (\
            Field (typ, ref TupleOut, "start"),\
            StatelessFun (typ, Mul (\
              StatelessFun (typ, Mul (\
                Param (typ, "obs_window"),\
                Const (typ, VFloat 1.15))),\
              Const (typ, VI32 1_000_000l))))))),\
        (70, [])))\
        (test_p p "max selected.last.start > \\
                   out.start + ($obs_window * 1.15) * 1_000_000" |> replace_typ_in_expr)

      (Ok (\
        StatelessFun (typ, Mod (\
          Param (typ, "x"),\
          Param (typ, "y"))),\
        (7, [])))\
        (test_p p "$x % $y" |> replace_typ_in_expr)

      (Ok ( \
        StatelessFun (typ, Abs (\
          StatelessFun (typ, Sub (\
            Field (typ, ref TupleUnknown, "bps"), \
            StatefulFun (typ, GlobalState, Lag (\
              Const (typ, VI32 (Int32.of_int 1)), \
              Field (typ, ref TupleUnknown, "bps"))))))), \
        (21, []))) \
        (test_p p "abs(bps - lag(1,bps))" |> replace_typ_in_expr)

      (Ok ( \
        StatelessFun (typ, Ge (\
          Field (typ, ref TupleUnknown, "value"),\
          StatelessFun (typ, Sub (\
            Const (typ, VI32 (Int32.of_int 1000)),\
            Case (typ, [\
              { case_cond = Field (typ, ref TupleGroupPrevious, "firing") ;\
                case_cons = StatelessFun (typ, Mul (\
                  Const (typ, VFloat 0.15),\
                  Const (typ, VI32 (Int32.of_int 1000)))) } ],\
              Some (Const (typ, VU8 (Stdint.Uint8.of_int 0)))))))),\
        (50, [])))\
        (test_p p "hysteresis_max(group.previous.firing, value, 1000)" |> replace_typ_in_expr)
    *)

    (*$>*)
  end
  (*$>*)
end

module Operation =
struct
  (*$< Operation *)

  (* Direct field selection (not for group-bys) *)
  type selected_field = { expr : Expr.t ; alias : string }

  let print_selected_field fmt f =
    let need_alias =
      match f.expr with
      | Expr.Field (_, tuple, field)
        when !tuple = TupleIn && f.alias = field -> false
      | _ -> true in
    if need_alias then
      Printf.fprintf fmt "%a AS %s"
        (Expr.print false) f.expr
        f.alias
    else
      Expr.print false fmt f.expr

  type flush_method = Reset | Slide of int
                    | KeepOnly of Expr.t | RemoveAll of Expr.t
                    | Never

  let print_flush_method ?(prefix="") ?(suffix="") () oc = function
    | Reset ->
      Printf.fprintf oc "%sFLUSH%s" prefix suffix
    | Never ->
      Printf.fprintf oc "%sKEEP ALL%s" prefix suffix
    | Slide n ->
      Printf.fprintf oc "%sSIDE %d%s" prefix n suffix
    | KeepOnly e ->
      Printf.fprintf oc "%sKEEP (%a)%s" prefix (Expr.print false) e suffix
    | RemoveAll e ->
      Printf.fprintf oc "%sREMOVE (%a)%s" prefix (Expr.print false) e suffix

  type event_start = string * float
  type event_duration = DurationConst of float (* seconds *)
                      | DurationField of (string * float)
                      | StopField of (string * float)
  type event_time_info = (event_start * event_duration) option

  type t =
    (* Generate values out of thin air. The difference with Select is that
     * Yield does not wait for some input. *)
    | Yield of { fields : selected_field list ; every : float }
    (* Aggregation of several tuples into one based on some key. Superficially looks like
     * a select but much more involved. *)
    | Aggregate of {
        fields : selected_field list ;
        (* Pass all fields not used to build an aggregated field *)
        and_all_others : bool ;
        (* Simple way to filter out incoming tuples: *)
        where : Expr.t ;
        export : event_time_info option ;
        (* If not empty, will notify this URL with a HTTP GET: *)
        notify_url : string ;
        key : Expr.t list ;
        top : (Expr.t (* N *) * Expr.t (* by *)) option ;
        commit_when : Expr.t ;
        commit_before : bool ; (* commit first and aggregate later *)
        (* How to flush: reset or slide values *)
        flush_how : flush_method ;
        (* List of nodes that are our parents *)
        from : string list }
    | ReadCSVFile of { where : where_specs ; what : csv_specs ; preprocessor : string }
    | ListenFor of { net_addr : Unix.inet_addr ; port : int ;
                     proto : RamenProtocols.net_protocol }

  (* ReadFile has the node reading files directly on disc.
   * DownloadFile is (supposed to be) ramen downloading the content into
   * a temporary directory and spawning a worker that also perform a ReadFile.
   * ReceiveFile is similar: the file is actually received by ramen which
   * write it in a temporary directory for its ReadFile worker. Those files
   * are to be POSTed to $RAMEN_URL/upload/$url_suffix. *)
  and where_specs = ReadFile of file_spec
                  | ReceiveFile
                  | DownloadFile of download_spec
  and file_spec = { fname : string ; unlink : bool }
  and download_spec = { url : string ; accept : string }
  and csv_specs =
    { separator : string ; null : string ;
      fields : Tuple.typ }

  let print_export fmt = function
    | None -> Printf.fprintf fmt "EXPORT"
    | Some (start_field, duration) ->
      let string_of_scale f = "*"^ string_of_float f in
      Printf.fprintf fmt "EXPORT EVENT STARTING AT %s%s AND %s"
        (fst start_field)
        (string_of_scale (snd start_field))
        (match duration with
         | DurationConst f -> "DURATION "^ string_of_float f
         | DurationField (n, s) -> "DURATION "^ n ^ string_of_scale s
         | StopField (n, s) -> "STOPPING AT "^ n ^ string_of_scale s)

  let print_csv_specs fmt specs =
    Printf.fprintf fmt "SEPARATOR %S NULL %S %a"
      specs.separator specs.null
      Tuple.print_typ specs.fields
  let print_file_specs fmt specs =
    Printf.fprintf fmt "READ%s FILES %S"
      (if specs.unlink then "AND DELETE " else "") specs.fname
  let print_download_specs fmt specs =
    Printf.fprintf fmt "DOWNLOAD FROM %S%s" specs.url
      (if specs.accept = "" then "" else
        Printf.sprintf " Accept: %S" specs.accept)
  let print_upload_specs fmt =
    Printf.fprintf fmt "RECEIVE"
  let print_where_specs fmt = function
    | ReadFile specs -> print_file_specs fmt specs
    | DownloadFile specs -> print_download_specs fmt specs
    | ReceiveFile -> print_upload_specs fmt

  let print fmt =
    let sep = ", " in
    function
    | Yield { fields ; every } ->
      Printf.fprintf fmt "YIELD %a EVERY %g SECONDS"
        (List.print ~first:"" ~last:"" ~sep print_selected_field) fields
        every
    | Aggregate { fields ; and_all_others ; where ; export ; notify_url ;
                  key ; top ; commit_when ; commit_before ; flush_how ;
                  from } ->
      Printf.fprintf fmt "FROM '%a' SELECT %a%s%s"
        (List.print ~first:"" ~last:"" ~sep String.print) from
        (List.print ~first:"" ~last:"" ~sep print_selected_field) fields
        (if fields <> [] && and_all_others then sep else "")
        (if and_all_others then "*" else "") ;
      if not (Expr.is_true where) then
        Printf.fprintf fmt " WHERE %a"
          (Expr.print false) where ;
      Option.may (fun e -> Printf.fprintf fmt " %a" print_export e) export ;
      if notify_url <> "" then
        Printf.fprintf fmt " NOTIFY %S" notify_url ;
      if key <> [] then
        Printf.fprintf fmt " GROUP BY %a"
          (List.print ~first:"" ~last:"" ~sep:", " (Expr.print false)) key ;
      Option.may (fun (n, by) ->
        Printf.fprintf fmt " TOP %a BY %a"
          (Expr.print false) n
          (Expr.print false) by) top ;
      if not (Expr.is_true commit_when) ||
         flush_how <> Reset then
        Printf.fprintf fmt " COMMIT %a%s %a"
          (print_flush_method ~prefix:"AND " ~suffix:" " ()) flush_how
          (if commit_before then "BEFORE" else "AFTER")
          (Expr.print false) commit_when
    | ReadCSVFile { where = where_specs ;
                    what = csv_specs ; preprocessor } ->
      Printf.fprintf fmt "%a %s %a"
        print_where_specs where_specs
        (if preprocessor = "" then ""
          else Printf.sprintf "PREPROCESS WITH %S" preprocessor)
        print_csv_specs csv_specs
    | ListenFor { net_addr ; port ; proto } ->
      Printf.fprintf fmt "LISTEN FOR %s ON %s:%d"
        (RamenProtocols.string_of_proto proto)
        (Unix.string_of_inet_addr net_addr)
        port

  let is_exporting = function
    | Aggregate { export = Some _ ; _ } -> true
    (* It's low rate enough. TODO: add an "EXPORT" option to ListenFor and set
     * it to true on the demo operation and false otherwise. *)
    | ListenFor _ (*{ proto = RamenProtocols.Collectd ; _ }*) -> true
    | _ -> false
  let export_event_info = function
    | Aggregate { export = Some e ; _ } -> e
    | ListenFor { proto = RamenProtocols.Collectd ; _ } ->
      Some (("time", 1.), DurationConst 0.)
    | ListenFor { proto = RamenProtocols.NetflowV5 ; _ } ->
      Some (("first", 1.), StopField ("last", 1.))
    | _ -> None

  let parents_of_operation = function
    | ListenFor _ | ReadCSVFile _ | Yield _ -> []
    | Aggregate { from ; _ } -> from

  let fold_expr init f = function
    | ListenFor _ | ReadCSVFile _ -> init
    | Yield { fields ; _ } ->
        List.fold_left (fun prev sf ->
            Expr.fold_by_depth f prev sf.expr
          ) init fields
    | Aggregate { fields ; where ; key ; top ; commit_when ;
                  flush_how ; _ } ->
        let x =
          List.fold_left (fun prev sf ->
              Expr.fold_by_depth f prev sf.expr
            ) init fields in
        let x = Expr.fold_by_depth f x where in
        let x = List.fold_left (fun prev ke ->
              Expr.fold_by_depth f prev ke
            ) x key in
        let x = Option.map_default (fun (n, by) ->
            let x = Expr.fold_by_depth f x n in
            Expr.fold_by_depth f x by
          ) x top in
        let x = Expr.fold_by_depth f x commit_when in
        match flush_how with
        | Slide _ | Never | Reset -> x
        | RemoveAll e | KeepOnly e ->
          Expr.fold_by_depth f x e

  let iter_expr f op =
    fold_expr () (fun () e -> f e) op

  (* Check that the expression is valid, or return an error message.
   * Also perform some optimisation, numeric promotions, etc...
   * This is done after the parse rather than Rejecting the parsing
   * result for better error messages. *)
  let check =
    let pure_in clause = StatefulNotAllowed { clause }
    and no_group clause = GroupStateNotAllowed { clause }
    and fields_must_be_from tuple where allowed =
      TupleNotAllowed { tuple ; where ; allowed } in
    let pure_in_key = pure_in "GROUP-BY"
    and check_pure e =
      Expr.unpure_iter (fun _ -> raise (SyntaxError e))
    and check_no_group e =
      Expr.unpure_iter (function
        | StatefulFun (_, LocalState, _) -> raise (SyntaxError e)
        | _ -> ())
    and check_fields_from lst where =
      Expr.iter (function
        | Expr.Field (_, tuple, _) ->
          if not (List.mem !tuple lst) then (
            let m = fields_must_be_from !tuple where lst in
            raise (SyntaxError m)
          )
        | _ -> ())
    and check_export fields = function
      | None -> ()
      | Some None -> ()
      | Some (Some ((start_field, _), duration)) ->
        let check_field_exists f =
          if not (List.exists (fun sf -> sf.alias = f) fields) then
            let m =
              let print_alias oc sf = String.print oc sf.alias in
              let tuple_type = IO.to_string (List.print print_alias) fields in
              FieldNotInTuple { field = f ; tuple = TupleOut ; tuple_type } in
            raise (SyntaxError m)
        in
        check_field_exists start_field ;
        match duration with
        | DurationConst _ -> ()
        | DurationField (f, _)
        | StopField (f, _) -> check_field_exists f
    in function
    | Yield { fields ; _ } ->
      List.iter (fun sf ->
          let e = StatefulNotAllowed { clause = "YIELD" } in
          check_pure e sf.expr ;
          check_fields_from [TupleLastIn; TupleOut (* FIXME: only if defined earlier *)] "YIELD clause" sf.expr
        ) fields
      (* TODO: check unicity of aliases *)
    | Aggregate { fields ; and_all_others ; where ; key ; top ; commit_when ;
                  flush_how ; export ; from ; _ } as op ->
      (* Set of fields known to come from in (to help prefix_smart): *)
      let fields_from_in = ref Set.empty in
      iter_expr (function
        | Field (_, { contents = (TupleIn|TupleLastIn|TupleSelected|
                                  TupleLastSelected|TupleUnselected|
                                  TupleLastUnselected) }, alias) ->
            fields_from_in := Set.add alias !fields_from_in
        | _ -> ()) op ;
      let is_selected_fields ?i alias = (* Tells if a field is in _out_ *)
        list_existsi (fun i' sf ->
          sf.alias = alias &&
          Option.map_default (fun i -> i' < i) true i) fields in
      (* Resolve TupleUnknown into either TupleIn or TupleOut depending
       * on the presence of this alias in selected_fields (optionally,
       * only before position i) *)
      let prefix_smart ?i =
        Expr.iter (function
          | Field (_, ({ contents = TupleUnknown } as pref), alias) ->
              if Set.mem alias !fields_from_in then
                pref := TupleIn
              else if is_selected_fields ?i alias then
                pref := TupleOut
              else (
                pref := TupleIn ;
                fields_from_in := Set.add alias !fields_from_in) ;
              !logger.debug "Field %S thought to belong to %s"
                alias (string_of_prefix !pref)
          | _ -> ())
      and prefix_def def =
        Expr.iter (function
          | Field (_, ({ contents = TupleUnknown } as pref), _) ->
              pref := def
          | _ -> ()) in
      List.iteri (fun i sf -> prefix_smart ~i sf.expr) fields ;
      prefix_smart where ;
      List.iter (prefix_def TupleIn) key ;
      Option.may (fun (n, by) ->
        prefix_smart n ; prefix_def TupleIn  by) top ;
      prefix_smart commit_when ;
      (match flush_how with
      | KeepOnly e | RemoveAll e -> prefix_def TupleGroup e
      | _ -> ()) ;
      (* Check that we use the TupleGroup only for virtual fields: *)
      iter_expr (function
        | Field (_, { contents = TupleGroup }, alias) ->
          if not (is_virtual_field alias) then
            raise (SyntaxError (TupleHasOnlyVirtuals { tuple = TupleGroup ;
                                                       alias }))
        | _ -> ()) op ;
      (* Now check what tuple prefix are used: *)
      List.fold_left (fun prev_aliases sf ->
          check_fields_from [TupleLastIn; TupleIn; TupleGroup; TupleSelected; TupleLastSelected; TupleUnselected; TupleLastUnselected; TupleGroupFirst; TupleGroupLast; TupleOut (* FIXME: only if defined earlier *); TupleGroupPrevious; TupleOutPrevious] "SELECT clause" sf.expr ;
          (* Check unicity of aliases *)
          if List.mem sf.alias prev_aliases then
            raise (SyntaxError (AliasNotUnique sf.alias)) ;
          sf.alias :: prev_aliases
        ) [] fields |> ignore;
      if not and_all_others then check_export fields export ;
      (* Disallow group state in WHERE because it makes no sense: *)
      check_no_group (no_group "WHERE") where ;
      check_fields_from [TupleLastIn; TupleIn; TupleSelected; TupleLastSelected; TupleUnselected; TupleLastUnselected; TupleGroup; TupleGroupFirst; TupleGroupLast; TupleOutPrevious] "WHERE clause" where ;
      List.iter (fun k ->
        check_pure pure_in_key k ;
        check_fields_from [TupleIn] "Group-By KEY" k) key ;
      Option.may (fun (n, by) ->
        (* TODO: Check also that it's an unsigned integer: *)
        Expr.check_const "TOP size" n ;
        check_fields_from [TupleIn] "TOP clause" by ;
        (* The only windowing mode supported is then `commit and flush`: *)
        if flush_how <> Reset then
          raise (SyntaxError OnlyTumblingWindowForTop)) top ;
      check_fields_from [TupleLastIn; TupleIn; TupleSelected; TupleLastSelected; TupleUnselected; TupleLastUnselected; TupleOut; TupleGroupPrevious; TupleOutPrevious; TupleGroupFirst; TupleGroupLast; TupleGroup; TupleSelected; TupleLastSelected] "COMMIT WHEN clause" commit_when ;
      (match flush_how with
      | Reset | Never | Slide _ -> ()
      | RemoveAll e | KeepOnly e ->
        let m = StatefulNotAllowed { clause = "KEEP/REMOVE" } in
        check_pure m e ;
        check_fields_from [TupleGroup] "REMOVE clause" e) ;
      if from = [] then
        raise (SyntaxError (MissingClause { clause = "FROM" }))
      (* TODO: url_notify: check field names from text templates *)

    | ReadCSVFile _ | ListenFor _ -> ()

  module Parser =
  struct
    (*$< Parser *)
    open RamenParsing

    let default_alias =
      let open Expr in
      let force_public field =
        if String.length field = 0 || field.[0] <> '_' then field
        else String.lchop field in
      function
      | Field (_, _, field)
          when not (is_virtual_field field) -> field
      (* Provide some default name for common aggregate functions: *)
      | StatefulFun (_, _, AggrMin (Field (_, _, field))) -> "min_"^ force_public field
      | StatefulFun (_, _, AggrMax (Field (_, _, field))) -> "max_"^ force_public field
      | StatefulFun (_, _, AggrSum (Field (_, _, field))) -> "sum_"^ force_public field
      | StatefulFun (_, _, AggrAvg (Field (_, _, field))) -> "avg_"^ force_public field
      | StatefulFun (_, _, AggrAnd (Field (_, _, field))) -> "and_"^ force_public field
      | StatefulFun (_, _, AggrOr (Field (_, _, field))) -> "or_"^ force_public field
      | StatefulFun (_, _, AggrFirst (Field (_, _, field))) -> "first_"^ force_public field
      | StatefulFun (_, _, AggrLast (Field (_, _, field))) -> "last_"^ force_public field
      | StatefulFun (_, _, AggrPercentile (Const (_, p), Field (_, _, field)))
        when Scalar.is_round_integer p ->
        Printf.sprintf "%s_%sth" (force_public field) (IO.to_string Scalar.print p)
      | _ -> raise (Reject "must set alias")

    let selected_field m =
      let m = "selected field" :: m in
      (Expr.Parser.p ++ optional ~def:None (
         blanks -- strinG "as" -- blanks -+ some non_keyword) >>:
       fun (expr, alias) ->
        let alias =
          Option.default_delayed (fun () -> default_alias expr) alias in
        { expr ; alias }) m

    let list_sep m =
      let m = "list separator" :: m in
      (opt_blanks -- char ',' -- opt_blanks) m

    let yield =
      strinG "yield" -- blanks -+
      several ~sep:list_sep selected_field ++
      optional ~def:0.
        (blanks -- strinG "every" -- blanks -+ number +-
         blanks +- strinG "seconds") >>: fun (fields, every) ->
        if every < 0. then
          raise (Reject "sleep duration must be greater than 0") ;
        Yield { fields ; every }

    let export_clause m =
      let m = "export clause" :: m in
      let scale m =
        let m = "scale event field" :: m in
        (optional ~def:1. (
          (optional ~def:() blanks -- char '*' --
           optional ~def:() blanks -+ number ))
        ) m
      in
      let export_no_time_info =
        strinG "export" >>: fun () -> None
      and export_with_time_info =
        strinG "export" -- blanks -- strinG "event" -- blanks -- strinG "starting" -- blanks --
        strinG "at" -- blanks -+ non_keyword ++ scale ++
        optional ~def:(DurationConst 0.) (
          (blanks -- optional ~def:() ((strinG "and" ||| strinG "with") -- blanks) --
           strinG "duration" -- blanks -+ (
             (non_keyword ++ scale >>: fun n -> DurationField n) |||
             (number >>: fun n -> DurationConst n)) |||
           blanks -- strinG "and" -- blanks --
           (strinG "stopping" ||| strinG "ending") -- blanks --
           strinG "at" -- blanks -+
             (non_keyword ++ scale >>: fun n -> StopField n)))
        >>: fun (start_field, duration) -> Some (start_field, duration)
      in
      (export_no_time_info ||| export_with_time_info) m

    let notify_clause m =
      let m = "notify clause" :: m in
      (strinG "notify" -- blanks -+ quoted_string) m

    let select_clause m =
      let m = "select clause" :: m in
      (strinG "select" -- blanks -+
       several ~sep:list_sep
               ((char '*' >>: fun _ -> None) |||
                some selected_field)) m

    let where_clause m =
      let m = "where clause" :: m in
      ((strinG "where" ||| strinG "when") -- blanks -+ Expr.Parser.p) m

    let group_by m =
      let m = "group-by clause" :: m in
      (strinG "group" -- blanks -- strinG "by" -- blanks -+
       several ~sep:list_sep Expr.Parser.p) m

    let top_clause m =
      let m ="top-by clause" :: m in
      (strinG "top" -- blanks -+ Expr.Parser.p +- blanks +-
       strinG "by" +- blanks ++ Expr.Parser.p +- blanks +-
       strinG "when" +- blanks ++ Expr.Parser.p) m

    let flush m =
      let m = "flush clause" :: m in
      ((strinG "flush" >>: fun () -> Reset) |||
       (strinG "slide" -- blanks -+ integer >>: fun n ->
         if Num.sign_num n < 0 then raise (Reject "Sliding amount must be >0") else
         Slide (Num.int_of_num n)) |||
       (strinG "keep" -- blanks -- strinG "all" >>: fun () ->
         Never) |||
       (strinG "keep" -- blanks -+ Expr.Parser.p >>: fun e ->
         KeepOnly e) |||
       (strinG "remove" -- blanks -+ Expr.Parser.p >>: fun e ->
         RemoveAll e)
      ) m

    let commit_when m =
      let m = "commit clause" :: m in
      (strinG "commit" -- blanks -+
       optional ~def:None
         (strinG "and" -- blanks -+ some flush +- blanks) ++
       ((strinG "after" >>: fun _ -> false) |||
        (strinG "when" >>: fun _ -> false) |||
        (strinG "before" >>: fun _ -> true)) +- blanks ++ Expr.Parser.p) m

    let from_clause m =
      let m = "from clause" :: m in
      (strinG "from" -- blanks -+
       several ~sep:list_sep (node_identifier ~layer_allowed:true)) m

    type select_clauses =
      | SelectClause of selected_field option list
      | WhereClause of Expr.t
      | ExportClause of event_time_info
      | NotifyClause of string
      | GroupByClause of Expr.t list
      | TopByClause of ((Expr.t (* N *) * Expr.t (* by *)) * Expr.t (* when *))
      | CommitClause of ((flush_method option * bool) * Expr.t)
      | FromClause of string list

    let aggregate m =
      let m = "operation" :: m in
      let part =
        (select_clause >>: fun c -> SelectClause c) |||
        (where_clause >>: fun c -> WhereClause c) |||
        (export_clause >>: fun c -> ExportClause c) |||
        (notify_clause >>: fun c -> NotifyClause c) |||
        (group_by >>: fun c -> GroupByClause c) |||
        (top_clause >>: fun c -> TopByClause c) |||
        (commit_when >>: fun c -> CommitClause c) |||
        (from_clause >>: fun c -> FromClause c) in
      (several ~sep:blanks part >>: fun clauses ->
        (* Used for its address: *)
        let default_commit_when = Expr.expr_true in
        let is_default_commit = (==) default_commit_when in
        let default_select =
          [], true, Expr.expr_true, None, "", [],
          None, false, default_commit_when, Reset, [] in
        let fields, and_all_others, where, export, notify_url, key,
            top, commit_before, commit_when, flush_how, from =
          List.fold_left (
            fun (fields, and_all_others, where, export, notify_url, key,
                 top, commit_before, commit_when, flush_how,
                 from) -> function
              | SelectClause fields_or_stars ->
                let fields, and_all_others =
                  List.fold_left (fun (fields, and_all_others) -> function
                      | Some f -> f::fields, and_all_others
                      | None when not and_all_others -> fields, true
                      | None -> raise (Reject "All fields (\"*\") included several times")
                    ) ([], false) fields_or_stars in
                (* The above fold_left inverted the field order. *)
                let fields = List.rev fields in
                fields, and_all_others, where, export, notify_url, key,
                top, commit_before, commit_when, flush_how, from
              | WhereClause where ->
                fields, and_all_others, where, export, notify_url, key,
                top, commit_before, commit_when, flush_how, from
              | ExportClause export ->
                fields, and_all_others, where, Some export, notify_url, key,
                top, commit_before, commit_when, flush_how, from
              | NotifyClause notify_url ->
                fields, and_all_others, where, export, notify_url, key,
                top, commit_before, commit_when, flush_how, from
              | GroupByClause key ->
                fields, and_all_others, where, export, notify_url, key,
                top, commit_before, commit_when, flush_how, from
              | CommitClause ((Some flush_how, commit_before), commit_when) ->
                fields, and_all_others, where, export, notify_url, key,
                top, commit_before, commit_when, flush_how, from
              | CommitClause ((None, commit_before), commit_when) ->
                fields, and_all_others, where, export, notify_url, key,
                top, commit_before, commit_when, flush_how, from
              | TopByClause top ->
                fields, and_all_others, where, export, notify_url, key,
                Some top, commit_before, commit_when, flush_how, from
              | FromClause from ->
                fields, and_all_others, where, export, notify_url, key,
                top, commit_before, commit_when, flush_how, from
            ) default_select clauses in
        let commit_when, top =
          match top with
          | None -> commit_when, None
          | Some (top, top_when) ->
            if not (is_default_commit commit_when) ||
               flush_how <> Reset then
              raise (Reject "COMMIT and FLUSH clauses are incompatible \
                             with TOP") ;
            top_when, Some top in
        Aggregate { fields ; and_all_others ; where ; export ; notify_url ;
                    key ; top ; commit_before ; commit_when ; flush_how ;
                    from }
      ) m

    (* FIXME: It should be possible to enter separator, null, preprocessor in any order *)
    let read_file_specs m =
      let m = "read file operation" :: m in
      (strinG "read" -- blanks -+
       optional ~def:false (
         strinG "and" -- blanks -- strinG "delete" -- blanks >>:
           fun () -> true) +-
       (strinG "file" ||| strinG "files") +- blanks ++
       quoted_string >>: fun (unlink, fname) ->
         ReadFile { unlink ; fname }) m

    let download_file_specs m =
      let m = "download operation" :: m in
      (strinG "download" -- blanks --
       optional ~def:() (strinG "from" -- blanks) -+
       quoted_string ++
       repeat ~what:"download headers" ~sep:none (
         blanks -- strinG "accept" -- opt_blanks -- char ':' --
         opt_blanks -+ quoted_string) >>:
       function url, [accept] ->
         DownloadFile { url ; accept }
       | url, [] ->
         DownloadFile { url ; accept = "" }
       | _ ->
         raise (Reject "Only one header field can be set: Accept.")) m

    let upload_file_specs m =
      let m = "upload operation" :: m in
      (strinG "receive" >>: fun () -> ReceiveFile) m

    let where_specs =
      read_file_specs ||| download_file_specs ||| upload_file_specs

    let csv_specs m =
      let m = "CSV format" :: m in
      let field =
        non_keyword +- blanks ++ Scalar.Parser.typ ++
        optional ~def:true (
          optional ~def:true (
            blanks -+ (strinG "not" >>: fun () -> false)) +-
          blanks +- strinG "null") >>:
        fun ((typ_name, typ), nullable) -> { typ_name ; typ ; nullable }
      in
      (optional ~def:"," (
         strinG "separator" -- opt_blanks -+ quoted_string +- opt_blanks) ++
       optional ~def:"" (
         strinG "null" -- opt_blanks -+ quoted_string +- opt_blanks) +-
       char '(' +- opt_blanks ++
       several ~sep:list_sep field +- opt_blanks +- char ')' >>:
       fun ((separator, null), fields) ->
         if separator = null || separator = "" then
           raise (Reject "Invalid CSV separator") ;
         { separator ; null ; fields }) m

    let preprocessor m =
      let m = "file preprocessor" :: m in
      (strinG "preprocess" -- blanks -- strinG "with" -- opt_blanks -+
       quoted_string) m

    let read_csv_file m =
      let m = "read operation" :: m in
      (where_specs +- blanks ++
       optional ~def:"" (preprocessor +- blanks) ++
       csv_specs >>: fun ((where, preprocessor), what) ->
       ReadCSVFile { where ; what ; preprocessor }) m

    let default_port_of_protocol = function
      | RamenProtocols.Collectd -> 25826
      | RamenProtocols.NetflowV5 -> 2055

    let net_protocol m =
      let m = "network protocol" :: m in
      ((strinG "collectd" >>: fun () -> RamenProtocols.Collectd) |||
       ((strinG "netflow" ||| strinG "netflowv5") >>: fun () ->
          RamenProtocols.NetflowV5)) m

    let network_address =
      several ~sep:none (cond "inet address" (fun c ->
        (c >= '0' && c <= '9') ||
        (c >= 'a' && c <= 'f') ||
        (c >= 'A' && c <= 'A') ||
        c == '.' || c == ':') '0') >>:
      fun s ->
        let s = String.of_list s in
        try Unix.inet_addr_of_string s
        with Failure x -> raise (Reject x)

    let inet_addr m =
      let m = "network address" :: m in
      ((string "*" >>: fun () -> Unix.inet_addr_any) |||
       (string "[*]" >>: fun () -> Unix.inet6_addr_any) |||
       (network_address)) m

    let listen_on m =
      let m = "listen on operation" :: m in
      (strinG "listen" -- blanks --
       optional ~def:() (strinG "for" -- blanks) -+
       net_protocol ++
       optional ~def:None (
         blanks --
         optional ~def:() (strinG "on" -- blanks) -+
         some (inet_addr ++
               optional ~def:None (char ':' -+ some unsigned_decimal_number))) >>:
       fun (proto, addr_opt) ->
          let net_addr, port =
            match addr_opt with
            | None -> Unix.inet_addr_any, default_port_of_protocol proto
            | Some (addr, None) -> addr, default_port_of_protocol proto
            | Some (addr, Some port) -> addr, Num.int_of_num port in
          ListenFor { net_addr ; port ; proto }) m

    let p m =
      let m = "operation" :: m in
      (yield ||| aggregate ||| read_csv_file ||| listen_on) m

    (*$= p & ~printer:(test_printer print)
      (Ok (\
        Aggregate {\
          fields = [\
            { expr = Expr.(Field (typ, ref TupleIn, "start")) ;\
              alias = "start" } ;\
            { expr = Expr.(Field (typ, ref TupleIn, "stop")) ;\
              alias = "stop" } ;\
            { expr = Expr.(Field (typ, ref TupleIn, "itf_clt")) ;\
              alias = "itf_src" } ;\
            { expr = Expr.(Field (typ, ref TupleIn, "itf_srv")) ;\
              alias = "itf_dst" } ] ;\
          and_all_others = false ;\
          where = Expr.Const (typ, VBool true) ;\
          notify_url = "" ;\
          key = [] ; top = None ;\
          commit_when = replace_typ Expr.expr_true ;\
          commit_before = false ;\
          flush_how = Reset ;\
          export = None ;\
          from = ["foo"] },\
        (67, [])))\
        (test_op p "from foo select start, stop, itf_clt as itf_src, itf_srv as itf_dst" |>\
         replace_typ_in_op)

      (Ok (\
        Aggregate {\
          fields = [] ;\
          and_all_others = true ;\
          where = Expr.(\
            StatelessFun (typ, Gt (\
              Field (typ, ref TupleIn, "packets"),\
              Const (typ, VI32 (Int32.of_int 0))))) ;\
          export = None ; notify_url = "" ;\
          key = [] ; top = None ;\
          commit_when = replace_typ Expr.expr_true ;\
          commit_before = false ;\
          flush_how = Reset ; from = ["foo"] },\
        (26, [])))\
        (test_op p "from foo where packets > 0" |> replace_typ_in_op)

      (Ok (\
        Aggregate {\
          fields = [\
            { expr = Expr.(Field (typ, ref TupleIn, "t")) ;\
              alias = "t" } ;\
            { expr = Expr.(Field (typ, ref TupleIn, "value")) ;\
              alias = "value" } ] ;\
          and_all_others = false ;\
          where = Expr.Const (typ, VBool true) ;\
          export = Some (Some (("t", 10.), DurationConst 60.)) ;\
          notify_url = "" ;\
          key = [] ; top = None ;\
          commit_when = replace_typ Expr.expr_true ;\
          commit_before = false ;\
          flush_how = Reset ; from = ["foo"] },\
        (71, [])))\
        (test_op p "from foo select t, value export event starting at t*10 with duration 60" |>\
         replace_typ_in_op)

      (Ok (\
        Aggregate {\
          fields = [\
            { expr = Expr.(Field (typ, ref TupleIn, "t1")) ;\
              alias = "t1" } ;\
            { expr = Expr.(Field (typ, ref TupleIn, "t2")) ;\
              alias = "t2" } ;\
            { expr = Expr.(Field (typ, ref TupleIn, "value")) ;\
              alias = "value" } ] ;\
          and_all_others = false ;\
          where = Expr.Const (typ, VBool true) ;\
          export = Some (Some (("t1", 10.), StopField ("t2", 10.))) ;\
          notify_url = "" ; key = [] ; top = None ;\
          commit_when = replace_typ Expr.expr_true ;\
          commit_before = false ;\
          flush_how = Reset ; from = ["foo"] },\
        (82, [])))\
        (test_op p "from foo select t1, t2, value export event starting at t1*10 and stopping at t2*10" |>\
         replace_typ_in_op)

      (Ok (\
        Aggregate {\
          fields = [] ;\
          and_all_others = true ;\
          where = Expr.Const (typ, VBool true) ;\
          export = None ;\
          notify_url = "http://firebrigade.com/alert.php" ;\
          key = [] ; top = None ;\
          commit_when = replace_typ Expr.expr_true ;\
          commit_before = false ;\
          flush_how = Reset ; from = ["foo"] },\
        (50, [])))\
        (test_op p "from foo NOTIFY \"http://firebrigade.com/alert.php\"" |>\
         replace_typ_in_op)

      (Ok (\
        Aggregate {\
          fields = [\
            { expr = Expr.(\
                StatefulFun (typ, LocalState, AggrMin (\
                  Field (typ, ref TupleIn, "start")))) ;\
              alias = "start" } ;\
            { expr = Expr.(\
                StatefulFun (typ, LocalState, AggrMax (\
                  Field (typ, ref TupleIn, "stop")))) ;\
              alias = "max_stop" } ;\
            { expr = Expr.(\
                StatelessFun (typ, Div (\
                  StatefulFun (typ, LocalState, AggrSum (\
                    Field (typ, ref TupleIn, "packets"))),\
                  Param (typ, "avg_window")))) ;\
              alias = "packets_per_sec" } ] ;\
          and_all_others = false ;\
          where = Expr.Const (typ, VBool true) ;\
          export = None ; \
          notify_url = "" ;\
          key = [ Expr.(\
            StatelessFun (typ, Div (\
              Field (typ, ref TupleIn, "start"),\
              StatelessFun (typ, Mul (\
                Const (typ, VI32 1_000_000l),\
                Param (typ, "avg_window")))))) ] ;\
          top = None ;\
          commit_when = Expr.(\
            StatelessFun (typ, Gt (\
              StatelessFun (typ, Add (\
                StatefulFun (typ, LocalState, AggrMax (\
                  Field (typ, ref TupleGroupFirst, "start"))),\
                Const (typ, VI32 (Int32.of_int 3600)))),\
              Field (typ, ref TupleOut, "start")))) ; \
          commit_before = false ;\
          flush_how = Reset ;\
          from = ["foo"] },\
          (200, [])))\
          (test_op p "select min start as start, \\
                             max stop as max_stop, \\
                             (sum packets)/$avg_window as packets_per_sec \\
                     from foo \\
                     group by start / (1_000_000 * $avg_window) \\
                     commit when out.start < (max group.first.start) + 3600" |>\
           replace_typ_in_op)

      (Ok (\
        Aggregate {\
          fields = [\
            { expr = Expr.Const (typ, VI32 (Int32.one)) ;\
              alias = "one" } ] ;\
          and_all_others = false ;\
          where = Expr.Const (typ, VBool true) ;\
          export = None ; \
          notify_url = "" ;\
          key = [] ; top = None ;\
          commit_when = Expr.(\
            StatelessFun (typ, Ge (\
              StatefulFun (typ, LocalState, AggrSum (\
                Const (typ, VI32 (Int32.one)))),\
              Const (typ, VI32 (Int32.of_int 5))))) ;\
          commit_before = true ;\
          flush_how = Reset ; from = ["foo"] },\
          (49, [])))\
          (test_op p "select 1 as one from foo commit before sum 1 >= 5" |>\
           replace_typ_in_op)

      (Ok (\
        Aggregate {\
          fields = [\
            { expr = Expr.Field (typ, ref TupleIn, "n") ; alias = "n" } ;\
            { expr = Expr.(\
                StatefulFun (typ, GlobalState, Expr.Lag (\
                Expr.Const (typ, VI32 (Int32.of_int 2)), \
                Expr.Field (typ, ref TupleIn, "n")))) ;\
              alias = "l" } ] ;\
          and_all_others = false ;\
          where = Expr.Const (typ, VBool true) ;\
          export = None ; \
          notify_url = "" ;\
          key = [] ; top = None ;\
          commit_when = replace_typ Expr.expr_true ;\
          commit_before = false ;\
          flush_how = Reset ; from = ["foo/bar"] },\
          (37, [])))\
          (test_op p "SELECT n, lag(2, n) AS l FROM foo/bar" |>\
           replace_typ_in_op)

      (Ok (\
        ReadCSVFile { where = ReadFile { fname = "/tmp/toto.csv" ; unlink = false } ; \
                      preprocessor = "" ; what = { \
                        separator = "," ; null = "" ; \
                        fields = [ \
                          { typ_name = "f1" ; nullable = true ; typ = TBool } ;\
                          { typ_name = "f2" ; nullable = false ; typ = TI32 } ] } },\
        (52, [])))\
        (test_op p "read file \"/tmp/toto.csv\" (f1 bool, f2 i32 not null)")

      (Ok (\
        ReadCSVFile { where = ReadFile { fname = "/tmp/toto.csv" ; unlink = true } ; \
                      preprocessor = "" ; what = { \
                        separator = "," ; null = "" ; \
                        fields = [ \
                          { typ_name = "f1" ; nullable = true ; typ = TBool } ;\
                          { typ_name = "f2" ; nullable = false ; typ = TI32 } ] } },\
        (63, [])))\
        (test_op p "read and delete file \"/tmp/toto.csv\" (f1 bool, f2 i32 not null)")

      (Ok (\
        ReadCSVFile { where = ReadFile { fname = "/tmp/toto.csv" ; unlink = false } ; \
                      preprocessor = "" ; what = { \
                        separator = "\t" ; null = "<NULL>" ; \
                        fields = [ \
                          { typ_name = "f1" ; nullable = true ; typ = TBool } ;\
                          { typ_name = "f2" ; nullable = false ; typ = TI32 } ] } },\
        (81, [])))\
        (test_op p "read file \"/tmp/toto.csv\" \\
                        separator \"\\t\" null \"<NULL>\" \\
                        (f1 bool, f2 i32 not null)")
    *)

    (*$>*)
  end
  (*$>*)
end

module Program =
struct
  (*$< Program *)

  type func = { name : string ; operation : Operation.t }
  type t = func list

  let make_name =
    let seq = ref ~-1 in
    fun () ->
      incr seq ;
      "f"^ string_of_int !seq

  let make_func ?name operation =
    { name = (match name with Some n -> n | None -> make_name ()) ;
      operation }

  let print_func oc n =
    (* TODO: keep the info that func was anonymous? *)
    Printf.fprintf oc "DEFINE '%s' AS %a"
      n.name
      Operation.print n.operation

  let print oc p =
    List.print ~sep:"\n" print_func oc p

  let check lst =
    List.fold_left (fun s n ->
      Operation.check n.operation ;
      if Set.mem n.name s then
        raise (SyntaxError (NodeNameNotUnique n.name)) ;
      Set.add n.name s
    ) Set.empty lst |> ignore

  module Parser =
  struct
    (*$< Parser *)
    open RamenParsing

    let anonymous_node m =
      let m = "anonymous node" :: m in
      (Operation.Parser.p >>: make_func) m

    let named_node m =
      let m = "node" :: m in
      (strinG "define" -- blanks -+ node_identifier ~layer_allowed:false +-
       blanks +- strinG "as" +- blanks ++
       Operation.Parser.p >>: fun (name, op) -> make_func ~name op) m

    let node m =
      let m = "node" :: m in
      (anonymous_node ||| named_node) m

    let p m =
      let m = "program" :: m in
      let sep = opt_blanks -- char ';' -- opt_blanks in
      (several ~sep node +- optional ~def:() (opt_blanks -- char ';')) m

    (*$= p & ~printer:(test_printer print)
     (Ok ([\
      { name = "bar" ;\
        operation = \
          Aggregate {\
            fields = [\
              { expr = Expr.Const (typ, VI32 42l) ;\
                alias = "the_answer" } ] ;\
            and_all_others = false ;\
            where = Expr.Const (typ, VBool true) ;\
            notify_url = "" ;\
            key = [] ; top = None ;\
            commit_when = Expr.Const (typ, VBool true) ;\
            commit_before = false ;\
            flush_how = Reset ;\
            export = None ;\
            from = ["foo"] } } ],\
        (46, [])))\
        (test_p p "DEFINE bar AS SELECT 42 AS the_answer FROM foo" |>\
         replace_typ_in_program)
    *)

    (*$>*)
  end
  (*$>*)
end
