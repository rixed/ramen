(* AST for the stream processor graph *)
open Batteries
open RamenSharedTypes
open Stdint

type tuple_prefix =
  | TupleIn | TupleLastIn
  | TupleSelected | TupleLastSelected
  | TupleUnselected | TupleLastUnselected
  | TupleGroup | TupleGroupFirst | TupleGroupLast | TupleGroupPrevious
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
  | TupleOut -> "out"

type syntax_error =
  | ParseError of { error : string ; text : string }
  | NotConstant of string
  | TupleNotAllowed of { tuple : tuple_prefix ; clause : string ;
                         allowed : tuple_prefix list }
  | StatefulNotAllowed of { clause : string }
  | FieldNotInTuple of { field : string ; tuple : tuple_prefix ;
                         tuple_type : string }
  | MissingClause of { clause : string }
  | CannotTypeField of { field : string ; typ : string ; tuple : tuple_prefix }
  | CannotTypeExpression of { what : string ; expected_type : string ;
                              got : string ; got_type : string }
  | InvalidNullability of { what : string ; must_be_nullable : bool }
  | InvalidCoalesce of { what : string ; must_be_nullable : bool }
  | CannotCompleteTyping

exception SyntaxError of syntax_error

let tuple_prefix_print oc p =
  Printf.fprintf oc "%s" (string_of_prefix p)

let string_of_syntax_error =
  let h = "Syntax Error: " in
  function
    ParseError { error ; text } ->
    "Parse error: "^ error ^" while parsing: "^ text
  | NotConstant s -> h ^ s ^" is not constant"
  | TupleNotAllowed { tuple ; clause ; allowed } ->
    "Invalid tuple '"^ string_of_prefix tuple ^"'; in a "^ clause ^
    " clause, all fields must come from " ^
      (IO.to_string
        (List.print ~first:"" ~last:"" ~sep:" or " tuple_prefix_print)
        allowed)
  | StatefulNotAllowed { clause } ->
    "Stateful function not allowed in "^ clause ^" clause"
  | FieldNotInTuple { field ; tuple ; tuple_type } ->
    "Field "^ field ^" is not in the "^ string_of_prefix tuple ^" tuple"^
    (if tuple_type <> "" then " (which is "^ tuple_type ^")" else "")
  | MissingClause { clause } ->
    "Missing "^ clause ^" clause"
  | CannotTypeField { field ; typ ; tuple } ->
    "Cannot find out the type of field "^ field ^" ("^ typ ^") \
     supposed to be a member of "^ string_of_prefix tuple
  | CannotTypeExpression { what ; expected_type ; got ; got_type } ->
    what ^" must have type (compatible with) "^ expected_type ^
    " but got "^ got ^" of type "^ got_type
  | InvalidNullability { what ; must_be_nullable } ->
    what ^" must"^ (if must_be_nullable then "" else " not") ^
    " be nullable but is"^ if must_be_nullable then " not" else ""
  | InvalidCoalesce { what ; must_be_nullable } ->
    "All elements of a COALESCE must be nullable but the last one. "^
    what ^" can"^ (if must_be_nullable then " not" else "") ^" be null."
  | CannotCompleteTyping -> "Cannot complete typing"

let () =
  Printexc.register_printer (function
    | SyntaxError e -> Some (string_of_syntax_error e)
    | _ -> None)

let parse_prefix m =
  let open RamenParsing in
  let m = "tuple prefix" :: m in
  let prefix s = strinG (s ^ ".") in
  (optional ~def:TupleIn (
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
    (prefix "previous" >>: fun () -> TupleGroupPrevious) |||
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
  | TupleGroupPrevious | TupleOut -> true
  | _ -> false

let tuple_need_state = function
  | TupleGroup | TupleGroupFirst | TupleGroupLast | TupleGroupPrevious -> true
  | _ -> false

(*$inject
  open Stdint
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

  let typ = Lang.Expr.make_typ "replaced for tests"

  let replace_typ e =
    Lang.Expr.map_type (fun _ -> typ) e

  let replace_typ_in_expr = function
    | Ok (expr, rest) -> Ok (replace_typ expr, rest)
    | x -> x

  let replace_typ_in_op =
    let open Lang.Operation in
    function
    | Ok (Aggregate { fields ; and_all_others ; where ; export ; notify_url ;
                      key ; top ; commit_when ; flush_when ;
                      flush_how ; from }, rest) ->
      Ok (Aggregate {
        fields = List.map (fun sf -> { sf with expr = replace_typ sf.expr }) fields ;
        and_all_others ;
        where = replace_typ where ;
        export ; notify_url ; from ;
        key = List.map replace_typ key ;
        top = Option.map (fun (n, e) -> replace_typ n, replace_typ e) top ;
        commit_when = replace_typ commit_when ;
        flush_when = Option.map replace_typ flush_when ;
        flush_how = (match flush_how with
          | Reset | Slide _ -> flush_how
          | RemoveAll e -> RemoveAll (replace_typ e)
          | KeepOnly e -> KeepOnly (replace_typ e)) }, rest)
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

  let print fmt = function
    | VFloat f  -> Printf.fprintf fmt "%g" f
    | VString s -> Printf.fprintf fmt "%S" s
    | VBool b   -> Printf.fprintf fmt "%b" b
    | VU8 i     -> Printf.fprintf fmt "%s" (Uint8.to_string i)
    | VU16 i    -> Printf.fprintf fmt "%s" (Uint16.to_string i)
    | VU32 i    -> Printf.fprintf fmt "%s" (Uint32.to_string i)
    | VU64 i    -> Printf.fprintf fmt "%s" (Uint64.to_string i)
    | VU128 i   -> Printf.fprintf fmt "%s" (Uint128.to_string i)
    | VI8 i     -> Printf.fprintf fmt "%s" (Int8.to_string i)
    | VI16 i    -> Printf.fprintf fmt "%s" (Int16.to_string i)
    | VI32 i    -> Printf.fprintf fmt "%s" (Int32.to_string i)
    | VI64 i    -> Printf.fprintf fmt "%s" (Int64.to_string i)
    | VI128 i   -> Printf.fprintf fmt "%s" (Int128.to_string i)
    | VNull     -> Printf.fprintf fmt "NULL"
    | VEth i    -> EthAddr.print fmt i
    | VIpv4 i   -> Ipv4.print fmt i
    | VIpv6 i   -> Ipv6.print fmt i
    | VCidrv4 i -> Ipv4.Cidr.print fmt i
    | VCidrv6 i -> Ipv6.Cidr.print fmt i

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
      let min_i8 = Num.of_string "-128"
      and max_i8 = Num.of_string "127"
      and max_u8 = Num.of_string "255"
      and min_i16 = Num.of_string "-32768"
      and max_i16 = Num.of_string "32767"
      and max_u16 = Num.of_string "65535"
      and min_i32 = Num.of_string "-2147483648"
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
        if Num.le_num min_i8 i && Num.le_num i max_i8  then VI8 (Int8.of_string s) else
        if Num.le_num zero i && Num.le_num i max_u8  then VU8 (Uint8.of_string s) else
        if Num.le_num min_i16 i && Num.le_num i max_i16  then VI16 (Int16.of_string s) else
        if Num.le_num zero i && Num.le_num i max_u16  then VU16 (Uint16.of_string s) else
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
      (Ok (VI16 (Int16.of_int 31000), (5,[])))   (test_p p "31000")
      (Ok (VU16 (Uint16.of_int 61000), (5,[])))  (test_p p "61000")
      (Ok (VFloat 3.14, (4,[])))                 (test_p p "3.14")
      (Ok (VFloat ~-.3.14, (5,[])))              (test_p p "-3.14")
      (Ok (VBool false, (5,[])))                 (test_p p "false")
      (Ok (VBool true, (4,[])))                  (test_p p "true")
      (Ok (VString "glop", (6,[])))              (test_p p "\"glop\"")
    *)

    let typ =
      (strinG "float" >>: fun () -> TFloat) |||
      (strinG "string" >>: fun () -> TString) |||
      (strinG "bool" >>: fun () -> TBool) |||
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

  type t = (string, scalar_typ) Hashtbl.t

  let make_empty () = Hashtbl.create 7
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

  let signature_of_typ typ =
    Scalar.string_of_typ (Option.get typ.scalar_typ) ^
    (if Option.get typ.nullable then " null" else " notnull")

  let to_expr_type_info typ =
    { name_info = typ.expr_name ;
      nullable_info = typ.nullable ;
      typ_info = typ.scalar_typ }

  let print_typ fmt typ =
    Printf.fprintf fmt "%s of %s%s"
      typ.expr_name
      (match typ.scalar_typ with
      | None -> "unknown type"
      | Some typ -> "type "^ IO.to_string Scalar.print_typ typ)
      (match typ.nullable with
      | None -> ", maybe nullable"
      | Some true -> ", nullable"
      | Some false -> "")

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
    | StatefullFun of typ * state_lifespan * statefull_fun
    | GeneratorFun of typ * generator_fun

  and case_alternative =
    { case_cond : t (* Must be bool *) ;
      case_cons : t (* All alternatives must share a type *) }

  and state_lifespan = LocalState | GlobalState

  and stateless_fun =
    (* TODO: Other functions: random, date_part, coalesce, string_split, case expressions... *)
    | Age of t
    | Now
    (* FIXME: see note in CodeGenLib.ml *)
    | Sequence of t * t (* start, step *)
    | Cast of t
    | Length of t (* string length *)
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

  and statefull_fun =
    (* TODO: Add avg, stddev... *)
    | AggrMin of t
    | AggrMax of t
    | AggrSum of t
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
    (* Rotating bloom filters. First expression is the "time", second a
     * "duration", and third an  expression whose value to remember. The
     * function will return true if it *thinks* that value has been seen the
     * same value at a time not older than the given duration. This is based on
     * bloom-filters so there can be false positives but not false negatives.
     * Note: to remember several expressions just use the hash function (TBD),
     * since it's based on a hash anyway. *)
    | Remember of t * t * t
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

  let expr_one =
    Const (make_typ ~typ:TU8 ~nullable:false "one", VU8 (Uint8.of_int 1))

  let expr_null =
    Const (make_typ ~nullable:true ~typ:TNull "NULL", VNull)

  let is_true = function
    | Const (_ , VBool true) -> true
    | _ -> false

  let is_virtual_field f =
    String.length f > 0 && f.[0] = '#'

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

    | StatefullFun (t, g, AggrMin e) ->
      Printf.fprintf fmt "min%s(%a)" (sl g) (print with_types) e ; add_types t
    | StatefullFun (t, g, AggrMax e) ->
      Printf.fprintf fmt "max%s(%a)" (sl g) (print with_types) e ; add_types t
    | StatefullFun (t, g, AggrSum e) ->
      Printf.fprintf fmt "sum%s(%a)" (sl g) (print with_types) e ; add_types t
    | StatefullFun (t, g, AggrAnd e) ->
      Printf.fprintf fmt "and%s(%a)" (sl g) (print with_types) e ; add_types t
    | StatefullFun (t, g, AggrOr e) ->
      Printf.fprintf fmt "or%s(%a)" (sl g) (print with_types) e ; add_types t
    | StatefullFun (t, g, AggrFirst e) ->
      Printf.fprintf fmt "first%s(%a)" (sl g) (print with_types) e ; add_types t
    | StatefullFun (t, g, AggrLast e) ->
      Printf.fprintf fmt "last%s(%a)" (sl g) (print with_types) e ; add_types t
    | StatefullFun (t, g, AggrPercentile (p, e)) ->
      Printf.fprintf fmt "%ath percentile%s(%a)"
        (print with_types) p (sl g) (print with_types) e ;
      add_types t
    | StatefullFun (t, g, Lag (e1, e2)) ->
      Printf.fprintf fmt "lag%s(%a, %a)"
        (sl g) (print with_types) e1 (print with_types) e2 ;
      add_types t
    | StatefullFun (t, g, MovingAvg (e1, e2, e3)) ->
      Printf.fprintf fmt "season_moveavg%s(%a, %a, %a)"
        (sl g) (print with_types) e1 (print with_types) e2 (print with_types) e3 ;
      add_types t
    | StatefullFun (t, g, LinReg (e1, e2, e3)) ->
      Printf.fprintf fmt "season_fit%s(%a, %a, %a)"
        (sl g) (print with_types) e1 (print with_types) e2 (print with_types) e3 ;
      add_types t
    | StatefullFun (t, g, MultiLinReg (e1, e2, e3, e4s)) ->
      Printf.fprintf fmt "season_fit_multi%s(%a, %a, %a, %a)"
        (sl g)
        (print with_types) e1
        (print with_types) e2
        (print with_types) e3
        (List.print ~first:"" ~last:"" ~sep:", " (print with_types)) e4s ;
      add_types t
    | StatefullFun (t, g, Remember (tim, dur, e)) ->
      Printf.fprintf fmt "remember%s(%a, %a, %a)"
        (sl g)
        (print with_types) tim (print with_types) dur (print with_types) e ;
      add_types t
    | StatefullFun (t, g, ExpSmooth (e1, e2)) ->
      Printf.fprintf fmt "smooth%s(%a, %a)"
        (sl g)  (print with_types) e1 (print with_types) e2 ;
      add_types t

    | GeneratorFun (t, Split (e1, e2)) ->
      Printf.fprintf fmt "split(%a, %a)"
        (print with_types) e1 (print with_types) e2 ;
      add_types t

  let typ_of = function
    | Const (t, _) | Field (t, _, _) | Param (t, _) | StateField (t, _)
    | StatelessFun (t, _) | StatefullFun (t, _, _) | GeneratorFun (t, _)
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

    | StatefullFun (_, _, AggrMin e) | StatefullFun (_, _, AggrMax e)
    | StatefullFun (_, _, AggrSum e) | StatefullFun (_, _, AggrAnd e)
    | StatefullFun (_, _, AggrOr e) | StatefullFun (_, _, AggrFirst e)
    | StatefullFun (_, _, AggrLast e) | StatelessFun (_, Age e)
    | StatelessFun (_, Not e) | StatelessFun (_, Defined e)
    | StatelessFun (_, Cast e) | StatelessFun (_, Abs e)
    | StatelessFun (_, Length e) | StatelessFun (_, BeginOfRange e)
    | StatelessFun (_, EndOfRange e) | StatelessFun (_, Exp e)
    | StatelessFun (_, Log e) | StatelessFun (_, Sqrt e)
    | StatelessFun (_, Hash e) ->
      f (fold_by_depth f i e) expr

    | StatefullFun (_, _, AggrPercentile (e1, e2))
    | StatelessFun (_, Sequence (e1, e2)) | StatelessFun (_, Add (e1, e2))
    | StatelessFun (_, Sub (e1, e2)) | StatelessFun (_, Mul (e1, e2))
    | StatelessFun (_, Div (e1, e2)) | StatelessFun (_, IDiv (e1, e2))
    | StatelessFun (_, Pow (e1, e2)) | StatelessFun (_, And (e1, e2))
    | StatelessFun (_, Or (e1, e2)) | StatelessFun (_, Ge (e1, e2))
    | StatelessFun (_, Gt (e1, e2)) | StatelessFun (_, Eq (e1, e2))
    | StatelessFun (_, Mod (e1, e2)) | StatefullFun (_, _, Lag (e1, e2))
    | StatefullFun (_, _, ExpSmooth (e1, e2))
    | StatelessFun (_, Concat (e1, e2)) | GeneratorFun (_, Split (e1, e2)) ->
      let i' = fold_by_depth f i e1 in
      let i''= fold_by_depth f i' e2 in
      f i'' expr

    | StatefullFun (_, _, MovingAvg (e1, e2, e3))
    | StatefullFun (_, _, LinReg (e1, e2, e3))
    | StatefullFun (_, _, Remember (e1, e2, e3)) ->
      let i' = fold_by_depth f i e1 in
      let i''= fold_by_depth f i' e2 in
      let i'''= fold_by_depth f i'' e3 in
      f i''' expr

    | StatefullFun (_, _, MultiLinReg (e1, e2, e3, e4s)) ->
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
    | Coalesce (_, es) ->
      List.fold_left (fold_by_depth f) i es

  let iter f = fold_by_depth (fun () e -> f e) ()

  let unpure_iter f e =
    fold_by_depth (fun () -> function
      | StatefullFun _ as e -> f e
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

    | StatefullFun (t, g, AggrMin a) ->
      StatefullFun (f t, g, AggrMin (if recurs then map_type ~recurs f a else a))
    | StatefullFun (t, g, AggrMax a) ->
      StatefullFun (f t, g, AggrMax (if recurs then map_type ~recurs f a else a))
    | StatefullFun (t, g, AggrSum a) ->
      StatefullFun (f t, g, AggrSum (if recurs then map_type ~recurs f a else a))
    | StatefullFun (t, g, AggrAnd a) ->
      StatefullFun (f t, g, AggrAnd (if recurs then map_type ~recurs f a else a))
    | StatefullFun (t, g, AggrOr a) ->
      StatefullFun (f t, g, AggrOr (if recurs then map_type ~recurs f a else a))
    | StatefullFun (t, g, AggrFirst a) ->
      StatefullFun (f t, g, AggrFirst (if recurs then map_type ~recurs f a else a))
    | StatefullFun (t, g, AggrLast a) ->
      StatefullFun (f t, g, AggrLast (if recurs then map_type ~recurs f a else a))
    | StatefullFun (t, g, AggrPercentile (a, b)) ->
      StatefullFun (f t, g, AggrPercentile (
          (if recurs then map_type ~recurs f a else a),
          (if recurs then map_type ~recurs f b else b)))
    | StatefullFun (t, g, Lag (a, b)) ->
      StatefullFun (f t, g, Lag (
          (if recurs then map_type ~recurs f a else a),
          (if recurs then map_type ~recurs f b else b)))
    | StatefullFun (t, g, MovingAvg (a, b, c)) ->
      StatefullFun (f t, g, MovingAvg (
          (if recurs then map_type ~recurs f a else a),
          (if recurs then map_type ~recurs f b else b),
          (if recurs then map_type ~recurs f c else c)))
    | StatefullFun (t, g, LinReg (a, b, c)) ->
      StatefullFun (f t, g, LinReg (
          (if recurs then map_type ~recurs f a else a),
          (if recurs then map_type ~recurs f b else b),
          (if recurs then map_type ~recurs f c else c)))
    | StatefullFun (t, g, MultiLinReg (a, b, c, d)) ->
      StatefullFun (f t, g, MultiLinReg (
          (if recurs then map_type ~recurs f a else a),
          (if recurs then map_type ~recurs f b else b),
          (if recurs then map_type ~recurs f c else c),
          (if recurs then List.map (map_type ~recurs f) d else d)))
    | StatefullFun (t, g, Remember (tim, dur, e)) ->
      StatefullFun (f t, g, Remember (
          (if recurs then map_type ~recurs f tim else tim),
          (if recurs then map_type ~recurs f dur else dur),
          (if recurs then map_type ~recurs f e else e)))
    | StatefullFun (t, g, ExpSmooth (a, b)) ->
      StatefullFun (f t, g, ExpSmooth (
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
    | StatelessFun (t, BeginOfRange a) ->
      StatelessFun (f t, BeginOfRange (if recurs then map_type ~recurs f a else a))
    | StatelessFun (t, EndOfRange a) ->
      StatelessFun (f t, EndOfRange (if recurs then map_type ~recurs f a else a))

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
      ((parse_prefix ++ non_keyword >>:
        fun (tuple, field) ->
          (* This is important here that the type name is the raw field name,
           * because we use the tuple field type name as their identifier (unless
           * it's a virtual field (starting with #) of course since those are
           * computed on the fly and have no variable corresponding to them in
           * the tuple) *)
          Field (make_typ field, ref tuple, field)) |||
       (parse_prefix ++ that_string "#count" >>:
        fun (tuple, field) ->
          if not (tuple_has_count tuple) then raise (Reject "This tuple has no #count") ;
          Field (make_typ ~nullable:false ~typ:TU64 field, ref tuple, field)) |||
       (parse_prefix ++ that_string "#successive" >>:
        fun (tuple, field) ->
          if not (tuple_has_successive tuple) then raise (Reject "This tuple has no #successive") ;
          Field (make_typ ~nullable:false ~typ:TU64 field, ref tuple, field))
      ) m

    (*$= field & ~printer:(test_printer (print false))
      (Ok (\
        Field (typ, ref TupleIn, "bytes"),\
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
        | "and" -> StatelessFun (make_bool_typ "and operator", And (t1, t2))
        | "or" -> StatelessFun (make_bool_typ "or operator", Or (t1, t2))
        | _ -> assert false in
      (* FIXME: we do not need a blanks if we had parentheses ("(x)AND(y)" is OK) *)
      binary_ops_reducer ~op ~term:low_prec_left_assoc ~sep:blanks ~reduce m

    and low_prec_left_assoc m =
      let m = "comparison operator" :: m in
      let op = that_string ">" ||| that_string ">=" ||| that_string "<" ||| that_string "<=" |||
               that_string "=" ||| that_string "<>" ||| that_string "!="
      and reduce t1 op t2 = match op with
        | ">" -> StatelessFun (make_bool_typ "comparison operator", Gt (t1, t2))
        | "<" -> StatelessFun (make_bool_typ "comparison operator", Gt (t2, t1))
        | ">=" -> StatelessFun (make_bool_typ "comparison operator", Ge (t1, t2))
        | "<=" -> StatelessFun (make_bool_typ "comparison operator", Ge (t2, t1))
        | "=" -> StatelessFun (make_bool_typ "equality operator", Eq (t1, t2))
        | "!=" | "<>" ->
          StatelessFun (make_bool_typ "not operator", Not (
            StatelessFun (make_bool_typ "equality operator", Eq (t1, t2))))
        | "IN" | "in" ->
          StatelessFun (make_bool_typ "and for range", And (
            StatelessFun (make_bool_typ "comparison operator for range", Ge (
              t1,
              StatelessFun (make_typ "begin of range", BeginOfRange t2))),
            StatelessFun (make_bool_typ "not operator for range", Not (
              StatelessFun (make_bool_typ "comparison operator for range", Ge (
                t1,
                StatelessFun (make_typ "end of range", EndOfRange t2)))))))
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

    and afunv_sf a n m = (* "sf" stands for "stateful" *)
      let sep = opt_blanks -- char ',' -- opt_blanks in
      let m = n :: m in
      (strinG n -+
       optional ~def:LocalState (blanks -+ state_lifespan) +-
       opt_blanks +- char '(' +- opt_blanks ++
       repeat ~min:a ~max:a ~sep lowest_prec_left_assoc ++
       repeat ~sep lowest_prec_left_assoc +- opt_blanks +- char ')') m

    and afun_sf a n =
      afunv_sf a n >>: fun (a, r) ->
        if r = [] then a else
        raise (Reject "too many arguments")

    and afun1_sf n =
      let sep = check (char '(') ||| blanks in
      (strinG n -+ optional ~def:LocalState (blanks -+ state_lifespan) +-
       sep ++ highestest_prec)

    and afun2_sf n =
      afun_sf 2 n >>: function (g, [a;b]) -> g, a, b | _ -> assert false

    and afun2v_sf n =
      afunv_sf 2 n >>: function ((g, [a;b]), r) -> g, a, b, r | _ -> assert false

    and afun3_sf n =
      afun_sf 3 n >>: function (g, [a;b;c]) -> g, a, b, c | _ -> assert false

    and afun3v_sf n =
      afunv_sf 3 n >>: function ((g, [a;b;c]), r) -> g, a, b, c, r | _ -> assert false

    and afunv a n m =
      let sep = opt_blanks -- char ',' -- opt_blanks in
      let m = n :: m in
      (strinG n -- opt_blanks -- char '(' -- opt_blanks -+
       (if a > 0 then
         repeat ~min:a ~max:a ~sep lowest_prec_left_assoc ++
         optional ~def:[] (sep -+ repeat ~sep lowest_prec_left_assoc)
        else
         return [] ++
         repeat ~sep lowest_prec_left_assoc) +-
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

    and afun2v n =
      afunv 2 n >>: function ([a;b], r) -> a, b, r | _ -> assert false

    and afun3v n =
      afunv 3 n >>: function ([a;b;c], r) -> a, b, c, r | _ -> assert false

    and highest_prec_left_assoc m =
      ((afun1 "not" >>: fun e -> StatelessFun (make_bool_typ "not operator", Not e)) |||
       (highestest_prec ++
        optional ~def:None (
          blanks -- strinG "is" -- blanks -+
          optional ~def:(Some false)
                   (strinG "not" -- blanks >>: fun () -> Some true) +-
          strinG "null") >>: function
            | e, None -> e
            | e, Some false ->
              StatelessFun (make_bool_typ ~nullable:false "not operator",
                Not (StatelessFun (make_bool_typ ~nullable:false "is_not_null operator", Defined e)))
            | e, Some true ->
              StatelessFun (make_bool_typ ~nullable:false "is_not_null operator", Defined e))
      ) m

    and func m =
      let m = "function" :: m in
      (* Note: min and max of nothing are NULL but sum of nothing is 0, etc *)
      ((afun1 "age" >>: fun e -> StatelessFun (make_num_typ "age function", Age e)) |||
       (afun1 "abs" >>: fun e -> StatelessFun (make_num_typ "absolute value", Abs e)) |||
       (afun1 "length" >>: fun e -> StatelessFun (make_typ ~typ:TU16 "length", Length e)) |||
       (strinG "now" >>: fun () -> StatelessFun (make_float_typ ~nullable:false "now", Now)) |||
       (afun1 "exp" >>: fun e -> StatelessFun (make_num_typ "exponential", Exp e)) |||
       (afun1 "log" >>: fun e -> StatelessFun (make_num_typ "logarithm", Log e)) |||
       (afun1 "sqrt" >>: fun e -> StatelessFun (make_num_typ "square root", Sqrt e)) |||
       (afun1 "hash" >>: fun e -> StatelessFun (make_typ ~typ:TI64 "hash", Hash e)) |||
       (afun1_sf "min" >>: fun (g, e) ->
          StatefullFun (make_num_typ "min aggregation", g, AggrMin e)) |||
       (afun1_sf "max" >>: fun (g, e) ->
          StatefullFun (make_num_typ "max aggregation", g, AggrMax e)) |||
       (afun1_sf "sum" >>: fun (g, e) ->
          StatefullFun (make_num_typ "sum aggregation", g, AggrSum e)) |||
       (afun1_sf "and" >>: fun (g, e) ->
          StatefullFun (make_bool_typ "and aggregation", g, AggrAnd e)) |||
       (afun1_sf "or" >>: fun (g, e) ->
          StatefullFun (make_bool_typ "or aggregation", g, AggrOr e)) |||
       (afun1_sf "first" >>: fun (g, e) ->
          StatefullFun (make_typ "first aggregation", g, AggrFirst e)) |||
       (afun1_sf "last" >>: fun (g, e) ->
          StatefullFun (make_typ "last aggregation", g, AggrLast e)) |||
       ((const ||| param) +- (optional ~def:() (strinG "th")) +- blanks ++
        afun1_sf "percentile" >>: fun (p, (g, e)) ->
          StatefullFun (make_num_typ ~nullable:false "percentile aggregation",
                        g, AggrPercentile (p, e))) |||
       (afun2_sf "lag" >>: fun (g, e1, e2) ->
          StatefullFun (make_typ "lag", g, Lag (e1, e2))) |||

       (* avg perform a division thus the float type *)
       (afun3_sf "season_moveavg" >>: fun (g, e1, e2, e3) ->
          StatefullFun (make_float_typ "season_moveavg", g, MovingAvg (e1, e2, e3))) |||
       (afun2_sf "moveavg" >>: fun (g, e1, e2) ->
          StatefullFun (make_float_typ "season_moveavg", g, MovingAvg (expr_one, e1, e2))) |||
       (afun3_sf "season_fit" >>: fun (g, e1, e2, e3) ->
          StatefullFun (make_float_typ "season_fit", g, LinReg (e1, e2, e3))) |||
       (afun2_sf "fit" >>: fun (g,e1, e2) ->
          StatefullFun (make_float_typ "season_fit", g, LinReg (expr_one, e1, e2))) |||
       (afun3v_sf "season_fit_multi" >>: fun (g, e1, e2, e3, e4s) ->
          StatefullFun (make_float_typ "season_fit_multi", g, MultiLinReg (e1, e2, e3, e4s))) |||
       (afun2v_sf "fit_multi" >>: fun (g, e1, e2, e3s) ->
          StatefullFun (make_float_typ "season_fit_multi", g, MultiLinReg (expr_one, e1, e2, e3s))) |||
       (afun2_sf "smooth" >>: fun (g, e1, e2) ->
          StatefullFun (make_float_typ "smooth", g, ExpSmooth (e1, e2))) |||
       (afun1_sf "smooth" >>: fun (g, e) ->
          let alpha =
            Const (make_typ ~typ:TFloat ~nullable:false "alpha", VFloat 0.5) in
          StatefullFun (make_float_typ "smooth", g, ExpSmooth (alpha, e))) |||
       (afun3_sf "remember" >>: fun (g, tim, dir, e) ->
          StatefullFun (make_bool_typ "remember", g, Remember (tim, dir, e))) |||

       (afun2 "split" >>: fun (e1, e2) ->
          GeneratorFun (make_typ ~typ:TString "split", Split (e1, e2))) |||
       k_moveavg ||| sequence ||| cast) m

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
       optional ~def:LocalState (blanks -+ state_lifespan) +-
       sep ++ highestest_prec >>: fun ((k, g), e) ->
         let k = Const (make_typ ~nullable:false ~typ:(scalar_type_of k)
                                 "moving average order", k) in
         StatefullFun (make_float_typ "moveavg", g, MovingAvg (expr_one, k, e))) m

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
      ((afun2 "if" >>: fun (case_cond, case_cons) ->
          Case (make_typ "case", [ { case_cond ; case_cons } ], None)) |||
       (afun3 "if" >>: fun (case_cond, case_cons, else_) ->
          Case (make_typ "case", [ { case_cond ; case_cons } ], Some else_))) m

    and coalesce m =
      let m = "coalesce" :: m in
      (afunv 0 "coalesce" >>: function
         | [], r ->
           let rec loop es = function
             | [] -> expr_null
             | [e] ->
               if es = [] then e else
               Coalesce (make_typ ~nullable:false "coalesce", List.rev (e::es))
             | e::rest ->
               loop (e :: es) rest
           in
           loop [] r
         | _ -> assert false) m

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
        StatelessFun (typ, Not (StatelessFun (typ, Defined (Field (typ, ref TupleIn, "zone_src"))))),\
        (16, [])))\
        (test_p p "zone_src IS NULL" |> replace_typ_in_expr)

      (Ok (\
        StatelessFun (typ, Eq (Field (typ, ref TupleIn, "zone_src"), Param (typ, "z1"))),\
        (14, [])))\
        (test_p p "zone_src = $z1" |> replace_typ_in_expr)

      (Ok (\
        StatelessFun (typ, And (\
          StatelessFun (typ, Or (\
            StatelessFun (typ, Not (\
              StatelessFun (typ, Defined (Field (typ, ref TupleIn, "zone_src"))))),\
            StatelessFun (typ, Eq (Field (typ, ref TupleIn, "zone_src"), Param (typ, "z1"))))),\
          StatelessFun (typ, Or (\
            StatelessFun (typ, Not (\
              StatelessFun (typ, Defined (Field (typ, ref TupleIn, "zone_dst"))))),\
            StatelessFun (typ, Eq (\
              Field (typ, ref TupleIn, "zone_dst"), Param (typ, "z2"))))))),\
        (77, [])))\
        (test_p p "(zone_src IS NULL or zone_src = $z1) and \\
                   (zone_dst IS NULL or zone_dst = $z2)" |> replace_typ_in_expr)

      (Ok (\
        StatelessFun (typ, Div (\
          StatefullFun (typ, LocalState, AggrSum (\
            Field (typ, ref TupleIn, "bytes"))),\
          Param (typ, "avg_window"))),\
        (23, [])))\
        (test_p p "(sum bytes)/$avg_window" |> replace_typ_in_expr)

      (Ok (\
        StatelessFun (typ, IDiv (\
          Field (typ, ref TupleIn, "start"),\
          StatelessFun (typ, Mul (\
            Const (typ, VI32 1_000_000l),\
            Param (typ, "avg_window"))))),\
        (34, [])))\
        (test_p p "start // (1_000_000 * $avg_window)" |> replace_typ_in_expr)

      (Ok (\
        StatefullFun (typ, LocalState, AggrPercentile (\
          Param (typ, "p"),\
          Field (typ, ref TupleIn, "bytes_per_sec"))),\
        (27, [])))\
        (test_p p "$p percentile bytes_per_sec" |> replace_typ_in_expr)

      (Ok (\
        StatelessFun (typ, Gt (\
          StatefullFun (typ, LocalState, AggrMax (\
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
            Field (typ, ref TupleIn, "bps"), \
            StatefullFun (typ, LocalState, Lag (\
              Const (typ, VI8 (Int8.of_int 1)), \
              Field (typ, ref TupleIn, "bps"))))))), \
        (21, []))) \
        (test_p p "abs(bps - lag(1,bps))" |> replace_typ_in_expr)
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

  let print_flush_method ?(prefix="") ?(suffix="") () oc = function
    | Reset ->
      Printf.fprintf oc "%sFLUSH%s" prefix suffix
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
     * Yield does not wait for some input. FIXME: aggregate could behave like
     * a yield when there is no input. *)
    | Yield of selected_field list
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
        (* When do we stop aggregating (default: when we commit) *)
        flush_when : Expr.t option ;
        (* How to flush: reset or slide values *)
        flush_how : flush_method ;
        (* List of nodes that are our parents *)
        from : string list }
    | ReadCSVFile of { fname : string ; unlink : bool ; separator : string ;
                       null : string ; fields : Tuple.typ ; preprocessor : string }
    | ListenFor of { net_addr : Unix.inet_addr ; port : int ;
                     proto : RamenProtocols.net_protocol }

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

  let print fmt =
    let sep = ", " in
    function
    | Yield fields ->
      Printf.fprintf fmt "YIELD %a"
        (List.print ~first:"" ~last:"" ~sep print_selected_field) fields
    | Aggregate { fields ; and_all_others ; where ; export ; notify_url ;
                  key ; top ; commit_when ; flush_when ; flush_how ; from } ->
      Printf.fprintf fmt "FROM %a SELECT %a%s%s"
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
         flush_when = None && flush_how <> Reset then
        Printf.fprintf fmt " COMMIT %aWHEN %a"
        (if flush_when = None then print_flush_method ~prefix:"AND " ~suffix:" " ()
         else (fun _oc _fh -> ())) flush_how
        (Expr.print false) commit_when ;
      Option.may (fun flush_when ->
        Printf.fprintf fmt " %a WHEN %a"
          (print_flush_method ()) flush_how
          (Expr.print false) flush_when) flush_when
    | ReadCSVFile { fname ; unlink ; separator ; null ; fields ; preprocessor } ->
      Printf.fprintf fmt "READ %sCSV FILES %S SEPARATOR %S NULL %S %s%a"
        (if unlink then "AND DELETE " else "")
        (if preprocessor = "" then ""
         else Printf.sprintf "PREPROCESS WITH %S" preprocessor)
        fname separator null Tuple.print_typ fields
    | ListenFor { net_addr ; port ; proto } ->
      Printf.fprintf fmt "LISTEN FOR %s ON %s:%d"
        (RamenProtocols.string_of_net_protocol proto)
        (Unix.string_of_inet_addr net_addr)
        port

  let is_exporting = function
    | Aggregate { export = Some _ ; _ } -> true
    (* It's low rate enough. TODO: add an "EXPORT" option to ListenFor and set
     * it to true on the demo operation and false otherwise. *)
    | ListenFor { proto = RamenProtocols.Collectd ; _ } -> true
    | _ -> false
  let export_event_info = function
    | Aggregate { export = Some e ; _ } -> e
    | ListenFor { proto = RamenProtocols.Collectd ; _ } ->
      Some (("time", 1.), DurationConst 0.)
    | _ -> None

  let parents_of_operation = function
    | ListenFor _ | ReadCSVFile _ | Yield _ -> []
    | Aggregate { from ; _ } -> from

  module Parser =
  struct
    (*$< Parser *)
    open RamenParsing

    let default_alias =
      let open Expr in
      function
      | Field (_, { contents=TupleIn }, field)
          when not (is_virtual_field field) -> field
      (* Provide some default name for common aggregate functions: *)
      | StatefullFun (_, _, AggrMin (Field (_, { contents=TupleIn }, field))) -> "min_"^ field
      | StatefullFun (_, _, AggrMax (Field (_, { contents=TupleIn }, field))) -> "max_"^ field
      | StatefullFun (_, _, AggrSum (Field (_, { contents=TupleIn }, field))) -> "sum_"^ field
      | StatefullFun (_, _, AggrAnd (Field (_, { contents=TupleIn }, field))) -> "and_"^ field
      | StatefullFun (_, _, AggrOr (Field (_, { contents=TupleIn }, field))) -> "or_"^ field
      | StatefullFun (_, _, AggrFirst (Field (_, { contents=TupleIn }, field))) -> "first_"^ field
      | StatefullFun (_, _, AggrLast (Field (_, { contents=TupleIn }, field))) -> "last_"^ field
      | StatefullFun (_, _, AggrPercentile (Const (_, p), Field (_, { contents=TupleIn }, field)))
        when Scalar.is_round_integer p ->
        Printf.sprintf "%s_%sth" field (IO.to_string Scalar.print p)
      | _ -> raise (Reject "must set alias")

    let selected_field m =
      let m = "selected field" :: m in
      (Expr.Parser.p ++ optional ~def:None (
         blanks -- strinG "as" -- blanks -+ some non_keyword) >>:
       fun (expr, alias) ->
        let alias = Option.default_delayed (fun () -> default_alias expr) alias in
        { expr ; alias }) m

    let list_sep m =
      let m = "list separator" :: m in
      (opt_blanks -- char ',' -- opt_blanks) m

    let yield =
      strinG "yield" -- blanks -+
      several ~sep:list_sep selected_field >>: fun fields -> Yield fields

    let export_clause m =
      let m = "export clause" :: m in
      let number =
        floating_point ||| (decimal_number >>: Num.to_float) in
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
          (blanks -- (strinG "and" ||| strinG "with") -- blanks -- strinG "duration" -- blanks -+ (
             (non_keyword ++ scale >>: fun n -> DurationField n) |||
             (number >>: fun n -> DurationConst n)) |||
           blanks -- strinG "and" -- blanks -- strinG "stopping" -- blanks --
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
       strinG "by" +- blanks ++ Expr.Parser.p) m

    let flush m =
      let m = "flush clause" :: m in
      ((strinG "flush" >>: fun () -> Reset) |||
       (strinG "slide" -- blanks -+ integer >>: fun n ->
         if Num.sign_num n < 0 then raise (Reject "Sliding amount must be >0") else
         Slide (Num.int_of_num n)) |||
       (strinG "keep" -- blanks -+ Expr.Parser.p >>: fun e ->
         KeepOnly e) |||
       (strinG "remove" -- blanks -+ Expr.Parser.p >>: fun e ->
         RemoveAll e)
      ) m

    let commit_when m =
      let m = "commit clause" :: m in
      (strinG "commit" -- blanks -+
       optional ~def:None
         (strinG "and" -- blanks -+ some flush +- blanks) +-
       strinG "when" +- blanks ++ Expr.Parser.p) m

    let flush_when m =
      let m = "flush clause" :: m in
      ((flush +- blanks +- strinG "when" +- blanks ++ Expr.Parser.p)) m

    let from_clause m =
      let m = "from clause" :: m in
      (strinG "from" -- blanks -+
       several ~sep:list_sep node_identifier) m

    type select_clauses =
      | SelectClause of selected_field option list
      | WhereClause of Expr.t
      | ExportClause of event_time_info
      | NotifyClause of string
      | GroupByClause of Expr.t list
      | TopByClause of (Expr.t (* N *) * Expr.t (* by *))
      | CommitClause of flush_method option * Expr.t
      | FlushClause of flush_method * Expr.t
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
        (commit_when >>: fun (m, c) -> CommitClause (m, c)) |||
        (flush_when >>: fun (m, f) -> FlushClause (m, f)) |||
        (from_clause >>: fun lst -> FromClause lst) in
      (several ~sep:blanks part >>: fun clauses ->
        if clauses = [] then raise (Reject "Empty select") ;
        let default_select =
          [], true, Expr.expr_true, None, "", [],
          None, Expr.expr_true, Some Expr.expr_false, Reset, [] in
        let fields, and_all_others, where, export, notify_url, key,
            top, commit_when, flush_when, flush_how, from =
          List.fold_left (
            fun (fields, and_all_others, where, export, notify_url, key,
                 top, commit_when, flush_when, flush_how, from) -> function
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
                top, commit_when, flush_when, flush_how, from
              | WhereClause where ->
                fields, and_all_others, where, export, notify_url, key,
                top, commit_when, flush_when, flush_how, from
              | ExportClause export ->
                fields, and_all_others, where, Some export, notify_url, key,
                top, commit_when, flush_when, flush_how, from
              | NotifyClause notify_url ->
                fields, and_all_others, where, export, notify_url, key,
                top, commit_when, flush_when, flush_how, from
              | GroupByClause key ->
                fields, and_all_others, where, export, notify_url, key,
                top, commit_when, flush_when, flush_how, from
              | CommitClause (Some flush_how, commit_when) ->
                fields, and_all_others, where, export, notify_url, key,
                top, commit_when, None, flush_how, from
              | CommitClause (None, commit_when) ->
                fields, and_all_others, where, export, notify_url, key,
                top, commit_when, flush_when, flush_how, from
              | FlushClause (flush_how, flush_when) ->
                fields, and_all_others, where, export, notify_url, key,
                top, commit_when, Some flush_when, flush_how, from
              | TopByClause top ->
                fields, and_all_others, where, export, notify_url, key,
                Some top, commit_when, flush_when, flush_how, from
              | FromClause from ->
                fields, and_all_others, where, export, notify_url, key,
                top, commit_when, flush_when, flush_how, from
            ) default_select clauses in
        Aggregate { fields ; and_all_others ; where ; export ; notify_url ; key ;
                    top ; commit_when ; flush_when ; flush_how ; from }
      ) m

    (* FIXME: It should be possible to enter separator, null, preprocessor in any order *)
    let read_csv_file m =
      let m = "read csv operation" :: m in
      let field =
        non_keyword +- blanks ++ Scalar.Parser.typ ++
        optional ~def:true (
          optional ~def:true (blanks -+ (strinG "not" >>: fun () -> false)) +-
          blanks +- strinG "null") >>:
        fun ((typ_name, typ), nullable) -> { typ_name ; typ ; nullable }
      in
      (strinG "read" -- blanks -+
       optional ~def:false (
        strinG "and" -- blanks -- strinG "delete" -- blanks >>:
        fun () -> true) +-
       optional ~def:() (strinG "csv" +- blanks) +-
       (strinG "file" ||| strinG "files") +- blanks ++
       quoted_string +- opt_blanks ++
       optional ~def:"," (
         strinG "separator" -- opt_blanks -+ quoted_string +- opt_blanks) ++
       optional ~def:"" (
         strinG "null" -- opt_blanks -+ quoted_string +- opt_blanks) ++
       optional ~def:"" (
         strinG "preprocess" -- opt_blanks -- strinG "with" -- opt_blanks -+
         quoted_string +- opt_blanks) +-
       char '(' +- opt_blanks ++
       several ~sep:list_sep field +- opt_blanks +- char ')' >>:
       fun (((((unlink, fname), separator), null), preprocessor), fields) ->
         if separator = null || separator = "" then
           raise (Reject "Invalid CSV separator/null") ;
         ReadCSVFile { fname ; unlink ; separator ; null ; fields ; preprocessor }) m

    let default_port_of_protocol = function
      | RamenProtocols.Collectd -> 25826

    let net_protocol m =
      let m = "network protocol" :: m in
      (strinG "collectd" >>: fun () -> RamenProtocols.Collectd) m

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
      (yield ||| aggregate ||| read_csv_file ||| listen_on
      ) m

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
          flush_when = Some (replace_typ Expr.expr_false) ;\
          flush_how = Reset ;\
          export = None ;\
          from = ["foo"] },\
        (67, [])))\
        (test_p p "from foo select start, stop, itf_clt as itf_src, itf_srv as itf_dst" |>\
         replace_typ_in_op)

      (Ok (\
        Aggregate {\
          fields = [] ;\
          and_all_others = true ;\
          where = Expr.(\
            StatelessFun (typ, Gt (\
              Field (typ, ref TupleIn, "packets"),\
              Const (typ, VI8 (Int8.of_int 0))))) ;\
          export = None ; notify_url = "" ;\
          key = [] ; top = None ;\
          commit_when = replace_typ Expr.expr_true ;\
          flush_when = Some (replace_typ Expr.expr_false) ;\
          flush_how = Reset ; from = ["foo"] },\
        (26, [])))\
        (test_p p "from foo where packets > 0" |> replace_typ_in_op)

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
          flush_when = Some (replace_typ Expr.expr_false) ;\
          flush_how = Reset ; from = ["foo"] },\
        (71, [])))\
        (test_p p "from foo select t, value export event starting at t*10 with duration 60" |>\
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
          flush_when = Some (replace_typ Expr.expr_false) ;\
          flush_how = Reset ; from = ["foo"] },\
        (82, [])))\
        (test_p p "from foo select t1, t2, value export event starting at t1*10 and stopping at t2*10" |>\
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
          flush_when = Some (replace_typ Expr.expr_false) ;\
          flush_how = Reset ; from = ["foo"] },\
        (50, [])))\
        (test_p p "from foo NOTIFY \"http://firebrigade.com/alert.php\"" |>\
         replace_typ_in_op)

      (Ok (\
        Aggregate {\
          fields = [\
            { expr = Expr.(\
                StatefullFun (typ, LocalState, AggrMin (\
                  Field (typ, ref TupleIn, "start")))) ;\
              alias = "start" } ;\
            { expr = Expr.(\
                StatefullFun (typ, LocalState, AggrMax (\
                  Field (typ, ref TupleIn, "stop")))) ;\
              alias = "max_stop" } ;\
            { expr = Expr.(\
                StatelessFun (typ, Div (\
                  StatefullFun (typ, LocalState, AggrSum (\
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
                StatefullFun (typ, LocalState, AggrMax (\
                  Field (typ, ref TupleGroupFirst, "start"))),\
                Const (typ, VI16 (Int16.of_int 3600)))),\
              Field (typ, ref TupleOut, "start")))) ; \
          flush_when = None ; flush_how = Reset ;\
          from = ["foo"] },\
          (210, [])))\
          (test_p p "select min start as start, \\
                            max stop as max_stop, \\
                            (sum packets)/$avg_window as packets_per_sec \\
                     from foo \\
                     group by start / (1_000_000 * $avg_window) \\
                     commit and flush when out.start < (max group.first.start) + 3600" |>\
           replace_typ_in_op)

      (Ok (\
        Aggregate {\
          fields = [\
            { expr = Expr.Const (typ, VI8 (Int8.one)) ;\
              alias = "one" } ] ;\
          and_all_others = false ;\
          where = Expr.Const (typ, VBool true) ;\
          export = None ; \
          notify_url = "" ;\
          key = [] ; top = None ;\
          commit_when = Expr.(\
            StatelessFun (typ, Ge (\
              StatefullFun (typ, LocalState, AggrSum (\
                Const (typ, VI8 (Int8.one)))),\
              Const (typ, VI8 (Int8.of_int 5))))) ;\
          flush_when = None ; flush_how = Reset ; from = ["foo"] },\
          (57, [])))\
          (test_p p "select 1 as one from foo commit and flush when sum 1 >= 5" |>\
           replace_typ_in_op)

      (Ok (\
        Aggregate {\
          fields = [\
            { expr = Expr.Field (typ, ref TupleIn, "n") ; alias = "n" } ;\
            { expr = Expr.(\
                StatefullFun (typ, LocalState, Expr.Lag (\
                Expr.Const (typ, VI8 (Int8.of_int 2)), \
                Expr.Field (typ, ref TupleIn, "n")))) ;\
              alias = "l" } ] ;\
          and_all_others = false ;\
          where = Expr.Const (typ, VBool true) ;\
          export = None ; \
          notify_url = "" ;\
          key = [] ; top = None ;\
          commit_when = replace_typ Expr.expr_true ;\
          flush_when = Some (replace_typ Expr.expr_false) ;\
          flush_how = Reset ; from = ["foo/bar"] },\
          (37, [])))\
          (test_p p "SELECT n, lag(2, n) AS l FROM foo/bar" |>\
           replace_typ_in_op)

      (Ok (\
        ReadCSVFile { fname = "/tmp/toto.csv" ; unlink = false ; \
                      separator = "," ; null = "" ; preprocessor = "" ; \
                      fields = [ \
                        { typ_name = "f1" ; nullable = true ; typ = TBool } ;\
                        { typ_name = "f2" ; nullable = false ; typ = TI32 } ] },\
        (56, [])))\
        (test_p p "read csv file \"/tmp/toto.csv\" (f1 bool, f2 i32 not null)")

      (Ok (\
        ReadCSVFile { fname = "/tmp/toto.csv" ; unlink = true ; \
                      separator = "," ; null = "" ; preprocessor = "" ; \
                      fields = [ \
                        { typ_name = "f1" ; nullable = true ; typ = TBool } ;\
                        { typ_name = "f2" ; nullable = false ; typ = TI32 } ] },\
        (67, [])))\
        (test_p p "read and delete csv file \"/tmp/toto.csv\" (f1 bool, f2 i32 not null)")

      (Ok (\
        ReadCSVFile { fname = "/tmp/toto.csv" ; unlink = false ; \
                      separator = "\t" ; null = "<NULL>" ; preprocessor = "" ; \
                      fields = [ \
                        { typ_name = "f1" ; nullable = true ; typ = TBool } ;\
                        { typ_name = "f2" ; nullable = false ; typ = TI32 } ] },\
        (85, [])))\
        (test_p p "read csv file \"/tmp/toto.csv\" \\
                            separator \"\\t\" null \"<NULL>\" \\
                            (f1 bool, f2 i32 not null)")
    *)

    (* Check that the expression is valid, or return an error message.
     * Also perform some optimisation, numeric promotions, etc... *)
    let check =
      let pure_in clause = StatefulNotAllowed { clause }
      and fields_must_be_from tuple clause allowed =
        TupleNotAllowed { tuple ; clause ; allowed } in
      let pure_in_where = pure_in "WHERE"
      and pure_in_key = pure_in "GROUP-BY"
      and pure_in_top = pure_in "TOP"
      and check_pure e =
        Expr.unpure_iter (fun _ -> raise (SyntaxError e))
      and check_fields_from lst clause =
        Expr.iter (function
          | Expr.Field (_, tuple, _) ->
            if not (List.mem !tuple lst) then (
              let m = fields_must_be_from !tuple clause lst in
              raise (SyntaxError m)
            )
          | _ -> ())
      and check_export fields = function
        | None -> ()
        | Some None -> ()
        | Some (Some ((start_field, _), duration)) ->
          let check_field_exists f =
            if not (List.exists (fun sf -> sf.alias = f) fields) then
              let m = FieldNotInTuple { field = f ; tuple = TupleOut ;
                                        tuple_type = "" (* TODO *) } in
              raise (SyntaxError m)
          in
          check_field_exists start_field ;
          match duration with
          | DurationConst _ -> ()
          | DurationField (f, _)
          | StopField (f, _) -> check_field_exists f
      in function
      | Yield fields ->
        List.iter (fun sf ->
            let e = StatefulNotAllowed { clause = "YIELD" } in
            check_pure e sf.expr ;
            check_fields_from [TupleLastIn; TupleOut (* FIXME: only if defined earlier *)] "YIELD" sf.expr
          ) fields
        (* TODO: check unicity of aliases *)
      | Aggregate { fields ; where ; key ; top ; commit_when ;
                    flush_when ; flush_how ; export ; from ; _ } ->
        List.iter (fun sf ->
            check_fields_from [TupleLastIn; TupleIn; TupleGroup; TupleSelected; TupleLastSelected; TupleUnselected; TupleLastUnselected; TupleGroupFirst; TupleGroupLast; TupleOut (* FIXME: only if defined earlier *)] "SELECT" sf.expr
          ) fields ;
        check_export fields export ;
        (* TODO: we could allow this if we had not only a state per group but
         * also a global state. But then in some place we would need a way to
         * distinguish between group or global context. *)
        check_pure pure_in_where where ;
        check_fields_from [TupleLastIn; TupleIn; TupleSelected; TupleLastSelected; TupleUnselected; TupleLastUnselected; TupleGroup; TupleGroupFirst; TupleGroupLast; TupleOut] "WHERE" where ;
        List.iter (fun k ->
          check_pure pure_in_key k ;
          check_fields_from [TupleIn] "KEY" k) key ;
        Option.may (fun (n, by) ->
          (* TODO: Check also that it's an unsigned integer: *)
          Expr.check_const "TOP size" n ;
          check_pure pure_in_top by ;
          check_fields_from [TupleIn] "TOP" by) top ;
        check_fields_from [TupleLastIn; TupleIn; TupleSelected; TupleLastSelected; TupleUnselected; TupleLastUnselected; TupleOut; TupleGroupPrevious; TupleGroupFirst; TupleGroupLast; TupleGroup; TupleSelected; TupleLastSelected] "COMMIT WHEN" commit_when ;
        Option.may (fun flush_when ->
            check_fields_from [TupleLastIn; TupleIn; TupleSelected; TupleLastSelected; TupleUnselected; TupleLastUnselected; TupleOut; TupleGroupPrevious; TupleGroupFirst; TupleGroupLast; TupleGroup; TupleSelected; TupleLastSelected] "FLUSH WHEN" flush_when
          ) flush_when ;
        (match flush_how with
        | Reset | Slide _ -> ()
        | RemoveAll e | KeepOnly e ->
          let m = StatefulNotAllowed { clause = "KEEP/REMOVE" } in
          check_pure m e ;
          check_fields_from [TupleGroup] "REMOVE" e) ;
        if from = [] then
          raise (SyntaxError (MissingClause { clause = "FROM" }))
        (* TODO: check from is not empty *)
        (* TODO: url_notify: check field names from text templates *)
        (* TODO: check unicity of aliases *)
      | ReadCSVFile _ | ListenFor _ -> ()

    (*$>*)
  end
  (*$>*)
end
