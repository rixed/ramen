(* AST for the stream processor graph *)
open Batteries
open RamenSharedTypes
open Stdint

exception SyntaxError of string

let () =
  Printexc.register_printer (function
    | SyntaxError e -> Some ("Syntax Error: "^ e)
    | _ -> None)

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

let tuple_prefix_print oc p =
  Printf.fprintf oc "%s" (string_of_prefix p)

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
      Printf.sprintf "%d solutions" (List.length lst)

  let strip_linecol = function
    | Ok (res, (x, _line, _col)) -> Ok (res, x)
    | Bad x -> Bad x

  let test_p p s =
    (p +- eof) [] None Parsers.no_error_correction (PConfig.stream_of_string s) |>
    to_result |>
    strip_linecol

  let typ = Lang.Expr.make_typ "replaced for tests"

  let rec replace_typ =
    let open Lang.Expr in
    function
    | Const (_, a) -> Const (typ, a)
    | Field (_, a, b) -> Field (typ, a, b)
    | Param (_, a) -> Param (typ, a)
    | AggrMin (_, a) -> AggrMin (typ, replace_typ a)
    | AggrMax (_, a) -> AggrMax (typ, replace_typ a)
    | AggrSum (_, a) -> AggrSum (typ, replace_typ a)
    | AggrAnd (_, a) -> AggrAnd (typ, replace_typ a)
    | AggrOr  (_, a) -> AggrOr  (typ, replace_typ a)
    | AggrFirst (_, a) -> AggrFirst (typ, replace_typ a)
    | AggrLast (_, a) -> AggrLast (typ, replace_typ a)
    | AggrPercentile (_, a, b) -> AggrPercentile (typ, replace_typ a, replace_typ b)
    | Age (_, a) -> Age (typ, replace_typ a)
    | Now _ -> Now typ
    | Sequence (_, a, b) -> Sequence (typ, replace_typ a, replace_typ b)
    | Abs (_, a) -> Abs (typ, replace_typ a)
    | Cast (_, a) -> Cast (typ, replace_typ a)
    | Length (_, a) -> Length (typ, replace_typ a)
    | Not (_, a) -> Not (typ, replace_typ a)
    | Defined (_, a) -> Defined (typ, replace_typ a)
    | Add (_, a, b) -> Add (typ, replace_typ a, replace_typ b)
    | Sub (_, a, b) -> Sub (typ, replace_typ a, replace_typ b)
    | Mul (_, a, b) -> Mul (typ, replace_typ a, replace_typ b)
    | Div (_, a, b) -> Div (typ, replace_typ a, replace_typ b)
    | IDiv (_, a, b) -> IDiv (typ, replace_typ a, replace_typ b)
    | Mod (_, a, b) -> Mod (typ, replace_typ a, replace_typ b)
    | Pow (_, a, b) -> Pow (typ, replace_typ a, replace_typ b)
    | Exp (_, a) -> Exp (typ, replace_typ a)
    | Log (_, a) -> Log (typ, replace_typ a)
    | And (_, a, b) -> And (typ, replace_typ a, replace_typ b)
    | Or (_, a, b) -> Or (typ, replace_typ a, replace_typ b)
    | Ge (_, a, b) -> Ge (typ, replace_typ a, replace_typ b)
    | Gt (_, a, b) -> Gt (typ, replace_typ a, replace_typ b)
    | Eq (_, a, b) -> Eq (typ, replace_typ a, replace_typ b)
    | BeginOfRange (_, a) -> BeginOfRange (typ, replace_typ a)
    | EndOfRange (_, a) -> EndOfRange (typ, replace_typ a)
    | Lag (_, a, b) -> Lag (typ, replace_typ a, replace_typ b)
    | SeasonAvg (_, a, b, c) -> SeasonAvg (typ, replace_typ a, replace_typ b, replace_typ c)
    | LinReg (_, a, b, c) -> LinReg (typ, replace_typ a, replace_typ b, replace_typ c)
    | ExpSmooth (_, a, b) -> ExpSmooth (typ, replace_typ a, replace_typ b)

  let replace_typ_in_expr = function
    | Ok (expr, rest) -> Ok (replace_typ expr, rest)
    | x -> x

  let replace_typ_in_op =
    let open Lang.Operation in
    function
    | Ok (Aggregate { fields ; and_all_others ; where ; export ; notify_url ;
                      key ; commit_when ; flush_when ; flush_how }, rest) ->
      Ok (Aggregate {
        fields = List.map (fun sf -> { sf with expr = replace_typ sf.expr }) fields ;
        and_all_others ;
        where = replace_typ where ;
        export ; notify_url ;
        key = List.map replace_typ key ;
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
  let id_quote = char ~what:"quote" '\'' in
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
    { expr_name : string ;
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
    | Param of typ * string
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
     * initilize an internal state only when the first value is met, we must
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
    (* TODO: Add avg, stddev... *)
    | AggrMin of typ * t
    | AggrMax of typ * t
    | AggrSum of typ * t
    | AggrAnd of typ * t
    | AggrOr  of typ * t
    | AggrFirst of typ * t
    | AggrLast of typ * t
    (* TODO: several percentiles. Requires multi values returns. *)
    | AggrPercentile of typ * t * t
    (* TODO: Other functions: random, date_part, coalesce, string_split, case expressions... *)
    | Age of typ * t
    | Now of typ
    (* FIXME: see note in CodeGenLib.ml *)
    | Sequence of typ * t * t (* start, step *)
    | Cast of typ * t
    | Length of typ * t (* string length *)
    (* Unary Ops on scalars *)
    | Not of typ * t
    | Abs of typ * t
    | Defined of typ * t
    (* Binary Ops scalars *)
    | Add of typ * t * t
    | Sub of typ * t * t
    | Mul of typ * t * t
    | Div of typ * t * t
    | IDiv of typ * t * t
    | Mod of typ * t * t
    | Pow of typ * t * t
    | Exp of typ * t
    | Log of typ * t
    | And of typ * t * t
    | Or of typ * t * t
    | Ge of typ * t * t
    | Gt of typ * t * t
    | Eq of typ * t * t
    (* For network address range checks: *)
    | BeginOfRange of typ * t
    | EndOfRange of typ * t
    (* value retarded by k steps. If we have had less than k past values
     * then return the first we've had. *)
    | Lag of typ * t * t
    (* If the current time is t, the season average of period p on k seasons is
     * the average of v(t-p), v(t-2p), ... v(t-kp). Note the absence of v(t).
     * This is because we want to compare v(t) with this season average.
     * Notice that lag is a special case of season average with p=k and k=1,
     * but with a universal type for the data (while season-avg works only on
     * numbers). *)
    | SeasonAvg of typ * t * t * t (* period, how many season to keep, expression *)
    (* Simple linear regression *)
    | LinReg of typ * t * t * t (* as above: period, how many season to keep, expression *)
    (* Simple exponential smoothing *)
    | ExpSmooth of typ * t * t (* coef between 0 and 1 and expression *)

  let expr_true =
    Const (make_bool_typ ~nullable:false "true", VBool true)

  let expr_false =
    Const (make_bool_typ ~nullable:false "false", VBool false)

  let expr_one =
    Const (make_typ ~typ:TU8 ~nullable:false "one", VU8 (Uint8.of_int 1))

  let is_true = function
    | Const (_ , VBool true) -> true
    | _ -> false

  let is_virtual_field f =
    String.length f > 0 && f.[0] = '#'

  let check_const what = function
    | Const _ -> ()
    | _ -> raise (SyntaxError (what ^" must be constant"))

  let rec print with_types fmt =
    let add_types t =
      if with_types then Printf.fprintf fmt " [%a]" print_typ t
    in
    function
    | Const (t, c) -> Scalar.print fmt c ; add_types t
    | Field (t, tuple, field) -> Printf.fprintf fmt "%s.%s" (string_of_prefix !tuple) field ; add_types t
    | Param (t, p) -> Printf.fprintf fmt "$%s" p ; add_types t
    | AggrMin (t, e) -> Printf.fprintf fmt "min (%a)" (print with_types) e ; add_types t
    | AggrMax (t, e) -> Printf.fprintf fmt "max (%a)" (print with_types) e ; add_types t
    | AggrSum (t, e) -> Printf.fprintf fmt "sum (%a)" (print with_types) e ; add_types t
    | AggrAnd (t, e) -> Printf.fprintf fmt "and (%a)" (print with_types) e ; add_types t
    | AggrOr  (t, e) -> Printf.fprintf fmt "or (%a)" (print with_types) e ; add_types t
    | AggrFirst (t, e) -> Printf.fprintf fmt "first (%a)" (print with_types) e ; add_types t
    | AggrLast (t, e) -> Printf.fprintf fmt "last (%a)" (print with_types) e ; add_types t
    | AggrPercentile (t, p, e) -> Printf.fprintf fmt "%ath percentile (%a)" (print with_types) p (print with_types) e ; add_types t
    | Age (t, e) -> Printf.fprintf fmt "age (%a)" (print with_types) e ; add_types t
    | Now t -> Printf.fprintf fmt "now" ; add_types t
    | Sequence (t, e1, e2) -> Printf.fprintf fmt "sequence(%a, %a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | Cast (t, e) -> Printf.fprintf fmt "cast(%a, %a)" Scalar.print_typ (Option.get t.scalar_typ) (print with_types) e ; add_types t
    | Length (t, e) -> Printf.fprintf fmt "length (%a)" (print with_types) e ; add_types t
    | Not (t, e) -> Printf.fprintf fmt "NOT (%a)" (print with_types) e ; add_types t
    | Abs (t, e) -> Printf.fprintf fmt "ABS (%a)" (print with_types) e ; add_types t
    | Defined (t, e) -> Printf.fprintf fmt "(%a) IS NOT NULL" (print with_types) e ; add_types t
    | Add (t, e1, e2) -> Printf.fprintf fmt "(%a) + (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | Sub (t, e1, e2) -> Printf.fprintf fmt "(%a) - (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | Mul (t, e1, e2) -> Printf.fprintf fmt "(%a) * (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | Div (t, e1, e2) -> Printf.fprintf fmt "(%a) / (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | IDiv (t, e1, e2) -> Printf.fprintf fmt "(%a) // (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | Mod (t, e1, e2) -> Printf.fprintf fmt "(%a) %% (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | Pow (t, e1, e2) -> Printf.fprintf fmt "(%a) ^ (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | Exp (t, e) -> Printf.fprintf fmt "exp (%a)" (print with_types) e ; add_types t
    | Log (t, e) -> Printf.fprintf fmt "log (%a)" (print with_types) e ; add_types t
    | And (t, Ge (_, e1, BeginOfRange (_, e2)), Not (_, (Ge (_, e1', EndOfRange (_, e2'))))) ->
      assert (e2 = e2') ;
      assert (e1 = e1') ;
      Printf.fprintf fmt "(%a) IN (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | BeginOfRange _ | EndOfRange _ -> assert false
    | And (t, e1, e2) -> Printf.fprintf fmt "(%a) AND (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | Or (t, e1, e2) -> Printf.fprintf fmt "(%a) OR (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | Ge (t, e1, e2) -> Printf.fprintf fmt "(%a) >= (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | Gt (t, e1, e2) -> Printf.fprintf fmt "(%a) > (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | Eq (t, e1, e2) -> Printf.fprintf fmt "(%a) = (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | Lag (t, e1, e2) -> Printf.fprintf fmt "lag(%a, %a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | SeasonAvg (t, e1, e2, e3) ->
      Printf.fprintf fmt "season_avg(%a, %a, %a)" (print with_types) e1 (print with_types) e2 (print with_types) e3 ; add_types t
    | LinReg (t, e1, e2, e3) ->
      Printf.fprintf fmt "season_fit(%a, %a, %a)" (print with_types) e1 (print with_types) e2 (print with_types) e3 ; add_types t
    | ExpSmooth (t, e1, e2) -> Printf.fprintf fmt "smooth(%a, %a)" (print with_types) e1 (print with_types) e2 ; add_types t

  let typ_of = function
    | Const (t, _) | Field (t, _, _) | Param (t, _) | AggrMin (t, _)
    | AggrMax (t, _) | AggrSum (t, _) | AggrAnd (t, _) | AggrOr  (t, _)
    | AggrFirst (t, _) | AggrLast (t, _) | AggrPercentile (t, _, _)
    | Age (t, _) | Sequence (t, _, _) | Not (t, _) | Defined (t, _)
    | Add (t, _, _) | Sub (t, _, _) | Mul (t, _, _) | Div (t, _, _)
    | IDiv (t, _, _) | Pow (t, _, _) | And (t, _, _) | Or (t, _, _)
    | Ge (t, _, _) | Gt (t, _, _) | Eq (t, _, _) | Mod (t, _, _)
    | Cast (t, _) | Abs (t, _) | Length (t, _) | Now t
    | BeginOfRange (t, _) | EndOfRange (t, _) | Lag (t, _, _)
    | SeasonAvg (t, _, _, _) | LinReg (t, _, _, _) | ExpSmooth (t, _, _)
    | Exp (t, _) | Log (t, _) ->
      t

  let is_nullable e =
    let t = typ_of e in
    t.nullable = Some true

  (* Propagate values up the tree only, depth first. *)
  let rec fold_by_depth f i expr =
    match expr with
    | Const _ | Param _ | Field _ | Now _ ->
      f i expr
    | AggrMin (_, e) | AggrMax (_, e) | AggrSum (_, e) | AggrAnd (_, e)
    | AggrOr (_, e) | AggrFirst (_, e) | AggrLast (_, e) | Age (_, e)
    | Not (_, e) | Defined (_, e) | Cast (_, e) | Abs (_, e) | Length (_, e)
    | BeginOfRange (_, e) | EndOfRange (_, e) | Exp (_, e) | Log (_, e) ->
      f (fold_by_depth f i e) expr
    | AggrPercentile (_, e1, e2) | Sequence (_, e1, e2)
    | Add (_, e1, e2) | Sub (_, e1, e2) | Mul (_, e1, e2) | Div (_, e1, e2)
    | IDiv (_, e1, e2) | Pow (_, e1, e2) | And (_, e1, e2) | Or (_, e1, e2)
    | Ge (_, e1, e2) | Gt (_, e1, e2) | Eq (_, e1, e2) | Mod (_, e1, e2)
    | Lag (_, e1, e2) | ExpSmooth (_, e1, e2) ->
      let i' = fold_by_depth f i e1 in
      let i''= fold_by_depth f i' e2 in
      f i'' expr
    | SeasonAvg (_, e1, e2, e3) | LinReg (_, e1, e2, e3) ->
      let i' = fold_by_depth f i e1 in
      let i''= fold_by_depth f i' e2 in
      let i'''= fold_by_depth f i'' e3 in
      f i''' expr

  let iter f = fold_by_depth (fun () e -> f e) ()

  let unpure_iter f e =
    fold_by_depth (fun () -> function
      | AggrMin _ | AggrMax _ | AggrSum _ | AggrAnd _ | AggrOr _ | AggrFirst _
      | AggrLast _ | AggrPercentile _ | Lag _ | SeasonAvg _ | LinReg _
      | ExpSmooth _ as e ->
        f e
      | Const _ | Param _ | Field _ | Cast _
      | Now _ | Age _ | Sequence _ | Not _ | Defined _ | Add _ | Sub _ | Mul _ | Div _
      | IDiv _ | Pow _ | And _ | Or _ | Ge _ | Gt _ | Eq _ | Mod _ | Abs _
      | Length _ | BeginOfRange _ | EndOfRange _ | Exp _ | Log _ ->
        ()) () e |> ignore

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
      (strinG "null" >>: fun () ->
       Const (make_typ ~nullable:true ~typ:TNull "NULL", VNull)) m

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

    (* operators with lowest precedence *)
    let rec lowest_prec_left_assoc m =
      let m = "logical operator" :: m in
      let op = that_string "and" ||| that_string "or"
      and reduce t1 op t2 = match op with
        | "and" -> And (make_bool_typ "and operator", t1, t2)
        | "or" -> Or (make_bool_typ "or operator", t1, t2)
        | _ -> assert false in
      (* FIXME: we do not need a blanks if we had parentheses ("(x)AND(y)" is OK) *)
      binary_ops_reducer ~op ~term:low_prec_left_assoc ~sep:blanks ~reduce m

    and low_prec_left_assoc m =
      let m = "comparison operator" :: m in
      let op = that_string ">" ||| that_string ">=" ||| that_string "<" ||| that_string "<=" |||
               that_string "=" ||| that_string "<>" ||| that_string "!="
      and reduce t1 op t2 = match op with
        | ">" -> Gt (make_bool_typ "comparison operator", t1, t2)
        | "<" -> Gt (make_bool_typ "comparison operator", t2, t1)
        | ">=" -> Ge (make_bool_typ "comparison operator", t1, t2)
        | "<=" -> Ge (make_bool_typ "comparison operator", t2, t1)
        | "=" -> Eq (make_bool_typ "equality operator", t1, t2)
        | "!=" | "<>" -> Not (make_bool_typ "not operator", Eq (make_bool_typ "equality operator", t1, t2))
        | "IN" | "in" ->
          And (make_bool_typ "and for range",
               Ge (make_bool_typ "comparison operator for range", t1,
                   BeginOfRange (make_typ "begin of range", t2)),
               Not (make_bool_typ "not operator for range",
                    Ge (make_bool_typ "comparison operator for range", t1,
                        EndOfRange (make_typ "end of range", t2))))
        | _ -> assert false in
      binary_ops_reducer ~op ~term:mid_prec_left_assoc ~sep:opt_blanks ~reduce m

    and mid_prec_left_assoc m =
      let m = "arithmetic operator" :: m in
      let op = that_string "+" ||| that_string "-"
      and reduce t1 op t2 = match op with
        | "+" -> Add (make_num_typ "addition", t1, t2)
        | "-" -> Sub (make_num_typ "subtraction", t1, t2)
        | _ -> assert false in
      binary_ops_reducer ~op ~term:high_prec_left_assoc ~sep:opt_blanks ~reduce m

    and high_prec_left_assoc m =
      let m = "arithmetic operator" :: m in
      let op = that_string "*" ||| that_string "//" ||| that_string "/" ||| that_string "%"
      and reduce t1 op t2 = match op with
        | "*" -> Mul (make_num_typ "multiplication", t1, t2)
        (* Note: We want the default division to output floats by default *)
        | "/" -> Div (make_typ ~typ:TFloat "division", t1, t2)
        | "//" -> IDiv (make_num_typ "integer-division", t1, t2)
        | "%" -> Mod (make_num_typ "modulo", t1, t2)
        | _ -> assert false in
      binary_ops_reducer ~op ~term:higher_prec_right_assoc ~sep:opt_blanks~reduce m

    and higher_prec_right_assoc m =
      let m = "arithmetic operator" :: m in
      let op = char '^'
      and reduce t1 _ t2 = Pow (make_num_typ "exponentiation", t1, t2) in
      binary_ops_reducer ~op ~right_associative:true
                         ~term:highest_prec_left_assoc ~sep:opt_blanks ~reduce m

    and afun a n m =
      let sep = opt_blanks -- char ',' -- opt_blanks in
      let m = n :: m in
      (strinG n -- opt_blanks -- char '(' -- opt_blanks -+
       repeat ~min:a ~max:a ~sep lowest_prec_left_assoc +- opt_blanks +- char ')') m

    and afun1 n =
      (strinG n -- blanks -- optional ~def:() (strinG "of" -- blanks) -+
       highestest_prec) |||
      (afun 1 n >>: function [a] -> a | _ -> assert false)

    and afun2 n =
      afun 2 n >>: function [a;b] -> (a, b) | _ -> assert false

    and afun3 n =
      afun 3 n >>: function [a;b;c] -> (a, b, c) | _ -> assert false

    and highest_prec_left_assoc m =
      ((afun1 "not" >>: fun e -> Not (make_bool_typ "not operator", e)) |||
       (highestest_prec ++
        optional ~def:None (
          blanks -- strinG "is" -- blanks -+
          optional ~def:(Some false)
                   (strinG "not" -- blanks >>: fun () -> Some true) +-
          strinG "null") >>: function
            | e, None -> e
            | e, Some false -> Not (make_bool_typ ~nullable:false "not operator", Defined (make_bool_typ ~nullable:false "is_not_null operator", e))
            | e, Some true -> Defined (make_bool_typ ~nullable:false "is_not_null operator", e))
      ) m

    and aggregate m =
      let m = "aggregate function" :: m in
      (* Note: min and max of nothing are NULL but sum of nothing is 0, etc *)
      ((afun1 "min" >>: fun e -> AggrMin (make_num_typ "min aggregation", e)) |||
       (afun1 "max" >>: fun e -> AggrMax (make_num_typ "max aggregation", e)) |||
       (afun1 "sum" >>: fun e -> AggrSum (make_num_typ "sum aggregation", e)) |||
       (afun1 "and" >>: fun e -> AggrAnd (make_bool_typ "and aggregation", e)) |||
       (afun1 "or" >>: fun e -> AggrOr (make_bool_typ "or aggregation", e)) |||
       (afun1 "first" >>: fun e -> AggrFirst (make_typ "first aggregation", e)) |||
       (afun1 "last" >>: fun e -> AggrLast (make_typ "last aggregation", e)) |||
       ((const ||| param) +- (optional ~def:() (strinG "th")) +- blanks ++
        afun1 "percentile" >>: fun (p, e) ->
        AggrPercentile (make_num_typ ~nullable:false "percentile aggregation", p, e))
      ) m

    and func =
      fun m ->
        let m = "function" :: m in
        ((afun1 "age" >>: fun e -> Age (make_num_typ "age function", e)) |||
         (afun1 "abs" >>: fun e -> Abs (make_num_typ "absolute value", e)) |||
         (afun1 "length" >>: fun e -> Length (make_typ ~typ:TU16 "length", e)) |||
         (strinG "now" >>: fun () -> Now (make_float_typ ~nullable:false "now")) |||
         (afun2 "lag" >>: fun (e1, e2) -> Lag (make_typ "lag", e1, e2)) |||
         (* season-avg perform a division thus the float type *)
         (afun3 "season_avg" >>: fun (e1, e2, e3) ->
          SeasonAvg (make_float_typ "season_avg", e1, e2, e3)) |||
         (afun2 "recent_avg" >>: fun (e1, e2) ->
          SeasonAvg (make_float_typ "season_avg", expr_one, e1, e2)) |||
         (afun3 "season_fit" >>: fun (e1, e2, e3) ->
          LinReg (make_float_typ "season_fit", e1, e2, e3)) |||
         (afun2 "recent_fit" >>: fun (e1, e2) ->
          LinReg (make_float_typ "season_fit", expr_one, e1, e2)) |||
         (afun2 "smooth" >>: fun (e1, e2) ->
          ExpSmooth (make_float_typ "smooth", e1, e2)) |||
         (afun1 "smooth" >>: fun e ->
          let alpha =
            Const (make_typ ~typ:TFloat ~nullable:false "alpha", VFloat 0.5) in
          ExpSmooth (make_float_typ "smooth", alpha, e)) |||
         (afun1 "exp" >>: fun e -> Exp (make_num_typ "exponential", e)) |||
         (afun1 "log" >>: fun e -> Log (make_num_typ "logarithm", e)) |||
         sequence ||| cast) m

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
            Sequence (seq_typ, e1, e2)) |||
         (afun1 seq >>: fun e1 ->
            Sequence (seq_typ, e1, seq_default_step)) |||
         (strinG seq >>: fun () ->
            Sequence (seq_typ, seq_default_start, seq_default_step))
        ) m

    and cast m =
      let m = "cast" :: m in
      let sep = check (char '(') ||| blanks in
      (Scalar.Parser.typ +- optional ~def:() (blanks -- strinG "of") +- sep ++
       highestest_prec >>: fun (typ, e) ->
         Cast (make_typ ~typ ("cast to "^ IO.to_string Scalar.print_typ typ), e)
      ) m

    and highestest_prec m =
      (const ||| field ||| param ||| func ||| aggregate ||| null |||
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
        Not (typ, Defined (typ, Field (typ, ref TupleIn, "zone_src"))),\
        (16, [])))\
        (test_p p "zone_src IS NULL" |> replace_typ_in_expr)

      (Ok (\
        Eq (typ, Field (typ, ref TupleIn, "zone_src"), Param (typ, "z1")),\
        (14, [])))\
        (test_p p "zone_src = $z1" |> replace_typ_in_expr)

      (Ok (\
        And (typ, \
          Or (typ, \
            Not (typ, Defined (typ, Field (typ, ref TupleIn, "zone_src"))),\
            Eq (typ, Field (typ, ref TupleIn, "zone_src"), Param (typ, "z1"))),\
          Or (typ, \
            Not (typ, Defined (typ, Field (typ, ref TupleIn, "zone_dst"))),\
            Eq (typ, Field (typ, ref TupleIn, "zone_dst"), Param (typ, "z2")))),\
        (77, [])))\
        (test_p p "(zone_src IS NULL or zone_src = $z1) and \\
                   (zone_dst IS NULL or zone_dst = $z2)" |> replace_typ_in_expr)

      (Ok (\
        Div (typ, \
          AggrSum (typ, Field (typ, ref TupleIn, "bytes")),\
          Param (typ, "avg_window")),\
        (23, [])))\
        (test_p p "(sum bytes)/$avg_window" |> replace_typ_in_expr)

      (Ok (\
        IDiv (typ, \
          Field (typ, ref TupleIn, "start"),\
          Mul (typ, \
            Const (typ, VI32 1_000_000l),\
            Param (typ, "avg_window"))),\
        (34, [])))\
        (test_p p "start // (1_000_000 * $avg_window)" |> replace_typ_in_expr)

      (Ok (\
        AggrPercentile (typ,\
          Param (typ, "p"),\
          Field (typ, ref TupleIn, "bytes_per_sec")),\
        (30, [])))\
        (test_p p "$p percentile of bytes_per_sec" |> replace_typ_in_expr)

      (Ok (\
        Gt (typ, \
          AggrMax (typ, \
            Field (typ, ref TupleLastSelected, "start")),\
          Add (typ, \
            Field (typ, ref TupleOut, "start"),\
            Mul (typ, \
              Mul (typ, \
                Param (typ, "obs_window"),\
                Const (typ, VFloat 1.15)),\
              Const (typ, VI32 1_000_000l)))),\
        (70, [])))\
        (test_p p "max selected.last.start > \\
                   out.start + ($obs_window * 1.15) * 1_000_000" |> replace_typ_in_expr)

      (Ok (\
        Mod (typ,\
          Param (typ, "x"),\
          Param (typ, "y")),\
        (7, [])))\
        (test_p p "$x % $y" |> replace_typ_in_expr)

      (Ok ( \
        Abs (typ, \
          Sub (typ, \
            Field (typ, ref TupleIn, "bps"), \
            Lag (typ, \
              Const (typ, VI8 (Int8.of_int 1)), \
              Field (typ, ref TupleIn, "bps")))), \
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
        commit_when : Expr.t ;
        (* When do we stop aggregating (default: when we commit) *)
        flush_when : Expr.t option ;
        (* How to flush: reset or slide values *)
        flush_how : flush_method }
    | ReadCSVFile of { fname : string ; unlink : bool ; separator : string ;
                       null : string ; fields : Tuple.typ ; preprocessor : string }

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
                  key ; commit_when ; flush_when ; flush_how } ->
      Printf.fprintf fmt "SELECT %a%s%s"
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

  let is_exporting = function
    | Aggregate { export = Some _ ; _ } -> true
    | _ -> false
  let export_event_info = function
    | Aggregate { export = Some e ; _ } -> e
    | _ -> None

  module Parser =
  struct
    (*$< Parser *)
    open RamenParsing

    let default_alias = function
      | Expr.Field (_, { contents=TupleIn }, field)
          when not (Expr.is_virtual_field field) -> field
      (* Provide some default name for current aggregate functions: *)
      | Expr.AggrMin (_, Expr.Field (_, { contents=TupleIn }, field)) -> "min_"^ field
      | Expr.AggrMax (_, Expr.Field (_, { contents=TupleIn }, field)) -> "max_"^ field
      | Expr.AggrSum (_, Expr.Field (_, { contents=TupleIn }, field)) -> "sum_"^ field
      | Expr.AggrAnd (_, Expr.Field (_, { contents=TupleIn }, field)) -> "and_"^ field
      | Expr.AggrOr  (_, Expr.Field (_, { contents=TupleIn }, field)) -> "or_"^ field
      | Expr.AggrFirst (_, Expr.Field (_, { contents=TupleIn }, field)) -> "first_"^ field
      | Expr.AggrLast (_, Expr.Field (_, { contents=TupleIn }, field)) -> "last_"^ field
      | Expr.AggrPercentile (_, Expr.Const (_, p), Expr.Field (_, { contents=TupleIn }, field))
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

    type select_clauses =
      | SelectClause of selected_field option list
      | WhereClause of Expr.t
      | ExportClause of event_time_info
      | NotifyClause of string
      | GroupByClause of Expr.t list
      (* FIXME: have a separate FlushClause *)
      | CommitClause of Expr.t * Expr.t option * flush_method

    let group_by m =
      let m = "group-by clause" :: m in
      (strinG "group" -- blanks -- strinG "by" -- blanks -+
       several ~sep:list_sep Expr.Parser.p) m

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
       strinG "when" +- blanks ++
       Expr.Parser.p ++
       optional ~def:None
         (some (blanks -+ flush +- blanks +- strinG "when" +- blanks ++
          Expr.Parser.p)) >>:
       function
       | (Some flush_how, commit_when), None ->
         commit_when, None, flush_how
       | (Some _, _), Some _ ->
         raise (Reject "AND FLUSH incompatible with FLUSH WHEN")
       | (None, commit_when), (Some (flush_how, flush_when)) ->
         commit_when, Some flush_when, flush_how
       | (None, _), None ->
         raise (Reject "Must specify when to flush")
      ) m

    let aggregate m =
      let m = "operation" :: m in
      let part =
        (select_clause >>: fun c -> SelectClause c) |||
        (where_clause >>: fun c -> WhereClause c) |||
        (export_clause >>: fun c -> ExportClause c) |||
        (notify_clause >>: fun c -> NotifyClause c) |||
        (group_by >>: fun c -> GroupByClause c) |||
        (commit_when >>: fun (c, f, m) -> CommitClause (c, f, m)) in
      (several ~sep:blanks part >>: fun clauses ->
        if clauses = [] then raise (Reject "Empty select") ;
        let default_select = [], true, Expr.expr_true, None, "", [],
                             Expr.expr_true, Some Expr.expr_false, Reset in
        let fields, and_all_others, where, export, notify_url, key,
            commit_when, flush_when, flush_how =
          List.fold_left (
            fun (fields, and_all_others, where, export, notify_url, key,
                 commit_when, flush_when, flush_how) -> function
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
                commit_when, flush_when, flush_how
              | WhereClause where ->
                fields, and_all_others, where, export, notify_url, key,
                commit_when, flush_when, flush_how
              | ExportClause export ->
                fields, and_all_others, where, Some export, notify_url, key,
                commit_when, flush_when, flush_how
              | NotifyClause notify_url ->
                fields, and_all_others, where, export, notify_url, key,
                commit_when, flush_when, flush_how
              | GroupByClause key ->
                fields, and_all_others, where, export, notify_url, key,
                commit_when, flush_when, flush_how
              | CommitClause (commit_when, flush_when, flush_how) ->
                fields, and_all_others, where, export, notify_url, key,
                commit_when, flush_when, flush_how
            ) default_select clauses in
        Aggregate { fields ; and_all_others ; where ; export ; notify_url ; key ;
                    commit_when ; flush_when ; flush_how }
      ) m

    (* FIXME: It should be possible to enter separator, null, preprocessor in any order *)
    let read_csv_file m =
      let m = "read csv file" :: m in
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

    let p m =
      let m = "operation" :: m in
      (yield ||| aggregate ||| read_csv_file
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
          key = [] ;\
          commit_when = replace_typ Expr.expr_true ;\
          flush_when = Some (replace_typ Expr.expr_false) ;\
          flush_how = Reset ;\
          export = None },\
        (58, [])))\
        (test_p p "select start, stop, itf_clt as itf_src, itf_srv as itf_dst" |>\
         replace_typ_in_op)

      (Ok (\
        Aggregate {\
          fields = [] ;\
          and_all_others = true ;\
          where = Expr.(\
            Gt (typ,\
              Field (typ, ref TupleIn, "packets"),\
              Const (typ, VI8 (Int8.of_int 0)))) ;\
          export = None ; notify_url = "" ;\
          key = [] ;\
          commit_when = replace_typ Expr.expr_true ;\
          flush_when = Some (replace_typ Expr.expr_false) ; flush_how = Reset },\
        (17, [])))\
        (test_p p "where packets > 0" |> replace_typ_in_op)

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
          key = [] ;\
          commit_when = replace_typ Expr.expr_true ;\
          flush_when = Some (replace_typ Expr.expr_false) ; flush_how = Reset },\
        (62, [])))\
        (test_p p "select t, value export event starting at t*10 with duration 60" |>\
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
          notify_url = "" ; key = [] ;\
          commit_when = replace_typ Expr.expr_true ;\
          flush_when = Some (replace_typ Expr.expr_false) ; flush_how = Reset },\
        (73, [])))\
        (test_p p "select t1, t2, value export event starting at t1*10 and stopping at t2*10" |>\
         replace_typ_in_op)

      (Ok (\
        Aggregate {\
          fields = [] ;\
          and_all_others = true ;\
          where = Expr.Const (typ, VBool true) ;\
          export = None ;\
          notify_url = "http://firebrigade.com/alert.php" ;\
          key = [] ;\
          commit_when = replace_typ Expr.expr_true ;\
          flush_when = Some (replace_typ Expr.expr_false) ; flush_how = Reset },\
        (41, [])))\
        (test_p p "NOTIFY \"http://firebrigade.com/alert.php\"" |>\
         replace_typ_in_op)

      (Ok (\
        Aggregate {\
          fields = [\
            { expr = Expr.(\
                AggrMin (typ, Field (typ, ref TupleIn, "start"))) ;\
              alias = "start" } ;\
            { expr = Expr.(\
                AggrMax (typ, Field (typ, ref TupleIn, "stop"))) ;\
              alias = "max_stop" } ;\
            { expr = Expr.(\
                Div (typ,\
                  AggrSum (typ, Field (typ, ref TupleIn, "packets")),\
                  Param (typ, "avg_window"))) ;\
              alias = "packets_per_sec" } ] ;\
          and_all_others = false ;\
          where = Expr.Const (typ, VBool true) ;\
          export = None ; \
          notify_url = "" ;\
          key = [ Expr.(\
            Div (typ,\
              Field (typ, ref TupleIn, "start"),\
              Mul (typ,\
                Const (typ, VI32 1_000_000l),\
                Param (typ, "avg_window")))) ] ;\
          commit_when = Expr.(\
            Gt (typ,\
              Add (typ,\
                AggrMax (typ,Field (typ, ref TupleGroupFirst, "start")),\
                Const (typ, VI16 (Int16.of_int 3600))),\
              Field (typ, ref TupleOut, "start"))) ; \
          flush_when = None ; flush_how = Reset },\
          (201, [])))\
          (test_p p "select min start as start, \\
                            max stop as max_stop, \\
                            (sum packets)/$avg_window as packets_per_sec \\
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
          key = [] ;\
          commit_when = Expr.(\
            Ge (typ,\
              AggrSum (typ, Const (typ, VI8 (Int8.one))),\
              Const (typ, VI8 (Int8.of_int 5)))) ;\
          flush_when = None ; flush_how = Reset },\
          (48, [])))\
          (test_p p "select 1 as one commit and flush when sum 1 >= 5" |>\
           replace_typ_in_op)

      (Ok (\
        Aggregate {\
          fields = [\
            { expr = Expr.Field (typ, ref TupleIn, "n") ; alias = "n" } ;\
            { expr = Expr.Lag (typ, \
                       Expr.Const (typ, VI8 (Int8.of_int 2)), \
                       Expr.Field (typ, ref TupleIn, "n")) ; alias = "l" } ] ;\
          and_all_others = false ;\
          where = Expr.Const (typ, VBool true) ;\
          export = None ; \
          notify_url = "" ;\
          key = [] ;\
          commit_when = replace_typ Expr.expr_true ;\
          flush_when = Some (replace_typ Expr.expr_false) ;\
          flush_how = Reset },\
          (24, [])))\
          (test_p p "SELECT n, lag(2, n) AS l" |>\
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
      let pure_in clause =
        "Aggregation function not allowed in "^ clause
      and fields_must_be_from lst clause =
        Printf.sprintf "All fields must come from %s in %s"
          (IO.to_string
            (List.print ~first:"" ~last:"" ~sep:" or " tuple_prefix_print)
            lst)
          clause
        in
      let pure_in_where = pure_in "WHERE clause"
      and pure_in_key = pure_in "GROUP-BY clause"
      and check_pure m =
        Expr.unpure_iter (fun _ -> raise (SyntaxError m))
      and check_fields_from lst clause =
        Expr.iter (function
          | Expr.Field (_, tuple, _) ->
            if not (List.mem !tuple lst) then (
              let m = fields_must_be_from lst clause in
              raise (SyntaxError m)
            )
          | _ -> ())
      and check_export fields = function
        | None -> ()
        | Some None -> ()
        | Some (Some ((start_field, _), duration)) ->
          let check_field_exists f =
            if not (List.exists (fun sf -> sf.alias = f) fields) then
              let m = "Field "^ f ^" is not in the output tuple" in
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
            let m = "Stateful functions not allowed in YIELDs" in
            check_pure m sf.expr ;
            check_fields_from [TupleLastIn; TupleOut (* FIXME: only if defined earlier *)] "YIELD operation" sf.expr
          ) fields
        (* TODO: check unicity of aliases *)
      | Aggregate { fields ; where ; key ; commit_when ; flush_when ; flush_how ; export ; _ } ->
        List.iter (fun sf ->
            check_fields_from [TupleLastIn; TupleIn; TupleGroup; TupleSelected; TupleLastSelected; TupleUnselected; TupleLastUnselected; TupleGroupFirst; TupleGroupLast; TupleOut (* FIXME: only if defined earlier *)] "SELECT clause" sf.expr
          ) fields ;
        check_export fields export ;
        (* TODO: we could allow this if we had not only a state per group but
         * also a global state. But then in some place we would need a way to
         * distinguish between group or global context. *)
        check_pure pure_in_where where ;
        check_fields_from [TupleLastIn; TupleIn; TupleSelected; TupleLastSelected; TupleUnselected; TupleLastUnselected; TupleGroup; TupleGroupFirst; TupleGroupLast; TupleOut] "WHERE clause" where ;
        List.iter (fun k ->
          check_pure pure_in_key k ;
          check_fields_from [TupleIn] "KEY clause" k) key ;
        check_fields_from [TupleLastIn; TupleIn; TupleSelected; TupleLastSelected; TupleUnselected; TupleLastUnselected; TupleOut; TupleGroupPrevious; TupleGroupFirst; TupleGroupLast; TupleGroup; TupleSelected; TupleLastSelected] "COMMIT WHEN clause" commit_when ;
        Option.may (fun flush_when ->
            check_fields_from [TupleLastIn; TupleIn; TupleSelected; TupleLastSelected; TupleUnselected; TupleLastUnselected; TupleOut; TupleGroupPrevious; TupleGroupFirst; TupleGroupLast; TupleGroup; TupleSelected; TupleLastSelected] "FLUSH WHEN clause" flush_when
          ) flush_when ;
        (match flush_how with
        | Reset | Slide _ -> ()
        | RemoveAll e | KeepOnly e ->
          let m = "Aggregation functions not allowed in KEEP/REMOVE clause" in
          check_pure m e ;
          check_fields_from [TupleGroup] "REMOVE clause" e)
        (* TODO: url_notify: check field names from text templates *)
        (* TODO: check unicity of aliases *)
      | ReadCSVFile _ -> ()

    (*$>*)
  end
  (*$>*)
end
