(* AST for the stream processor graph *)

(* Example: alerting configuration in this language:
 *
 * The first operation was to convert one TCP v29 event into zero, one or two
 * UniDir events. This is already challenging since we cannot rely on OCaml
 * code but must use only our primitives. So, we'd have to duplicate the TCPv29
 * event and then build two unidir events and collect them both in a filter
 * removing those without packets.
 *
 * r1: replicate
 *
 * u1: select start,
 *            stop,
 *            itf_clt as itf_src,
 *            itf_srv as itf_dst,
 *            zone_clt as zone_src,
 *            zone_srv as zone_dst,
 *            socket,
 *            packets_clt as packets,
 *            bytes_clt as bytes
 *
 * u2: select start,
 *            stop,
 *            itf_srv as itf_src,
 *            itf_clt as itf_dst,
 *            zone_srv as zone_src,
 *            zone_clt as zone_dst,
 *            socket,
 *            packets_srv as packets,
 *            bytes_srv as bytes
 *
 * r1->u1
 * r1->u2
 *
 * f1: where packets > 0
 *
 * u1->f1
 * u2->f1
 *
 * Note: Probably select and where should be the same operation combining both.
 *
 * Then we filter for zone z1 to z2:
 *
 * f2: where (zone_src IS NULL or zone_src = $z1) AND
 *           (zone_dst IS NULL or zone_dst = $z2)
 * f1 -> f2
 *
 * Note: It looks wrong that we default to true here, but that gives us an
 * example of NULL usage so let's keep it as this for the time being.
 * Note: Surely two successive filters could be optimized into a single one.
 * Note: $z1 and $z2 are parameters. We do not need parameters when we build
 * rules programmatically but they could become useful at some point.
 *
 * Now the hardest part: aggregate by avg_window.
 *
 * a1: select min start as start or out_start, max stop,
 *            itf_clt, itf_srv, zone_clt, zone_srv, socket,
 *            (sum packets)/$avg_window as packets_per_sec,
 *            (sum bytes)/$avg_window as bytes_per_sec
 *     group by start / (1_000_000 * $avg_window)
 *     commit after 3 seconds untouched or 100 other events
 *
 * f2 -> a1
 *
 * Note: the default field name for an aggregate function using only one field
 * is that field name. We can have several alias for a field name ("as start
 * or out_start" which is useful to keep the old name ("start") while still
 * allowing to refer to it using another name (the unique "out_start"). But in
 * any cases we still need tuple prefixes ("in.", "out.", "others."...) for
 * aggregates so not sure how useful...
 *
 * Note: Other possible syntax for commit: "commit after 3 seconds" (after
 * creation, regardless of what happened), "commit after 42 additions" count only
 * events added to an aggregate, while "commit after 42 events" count any events,
 * matching or not. Also, could be any expression involving the resulting
 * aggregate (out) or incoming event (in) or other events (others) or any
 * events (any), including aggregation function of those: "commit after out.start
 * < (max any.start) + 3600".
 *
 * Note: Every tuple that takes part in an aggregation operation ("out", "any"
 * and also the special "others") maintain virtual fields "start" (the time
 * when the first entry was added in this tuple), "last" (the time when
 * the last one was), "size" (the number of entries so far) and "successive"
 * (the number of entries added successively without any tuple aggregated
 * elsewhere). With those it is possible to construct all interesting committing
 * condition I can think about. Note that the "other" tuple has to be simulated
 * since we cannot update every group others for each input. So in practice
 * the above condition is equivalent to "commit when age(last) > 3 or
 * others.successive > 100".
 *
 * Note: The group-by operation could also embed an optional where clause, but
 * we probably want the group by to be a different op than select/where because
 * it's much more involved.
 *
 * Note: we convert into bytes/packets per seconds directly, which imply that
 * the aggregation operation knows about a final operation not involving
 * aggregation operators on each field. Also, here the division will return a
 * float, which is actually better than to stick with integers for a flow.
 *
 * Then we have the sliding window. This one is annoying because it produces a
 * list of tuples. But actually what we really want is to aggregate all the
 * $avg_window averages into buckets of $obs_window, which is another group-by.
 * The value of interest here would be the $p percentile of bytes_per_sec:
 *
 * a2: select min start, max stop,
 *            $p percentile of bytes_per_sec as peak_traffic
 *     group by start / (1_000_000 * $obs_window)
 *     commit after max others.start > out.start + ($obs_window * 1.15) * 1_000_000
 *
 * a1 -> a2
 *
 * Note: we automatically promote integer types as in C, so here the condition
 * on "max others.start" is done as float.
 *
 * Note: the percentile aggregation function has two parameters. The first
 * parameter (giving which percentile we want) can be an expression (thus we
 * can use a parameter) but must resolve into a float and must be constrained
 * to constants and parameters.
 *
 * Now it depends on whether $min_bps and $max_bps are defined.
 *
 * f3: select $min_bps IS NOT NULL and peak_traffic < $min_bps as too_low
 * f4: select $max_bps IS NOT NULL and peak_traffic > $max_bps as too_high
 *
 * a2 -> f3
 * a2 -> f4
 *
 * c1: on change too_low
 * c2: on change too_high
 *
 * f3 -> c1
 * f4 -> c2
 *
 * Note: on-change is a simple operation that takes a single value and output it
 * whenever it changes, under the same name. Could as well be named "deduplicate".
 *
 * A1: alert "network firefighters"
 *           subject "Too little traffic from zone $z1 to $z2"
 *           text "The traffic from zone $z1 to $z2 has sunk below
 *                 the configured minimum of $min_bps for the last $obs_window usec.
 *                 See https://event_proc.home.lan/show_alert?id=%{id}"
 *
 * A2: alert "network firefighters"
 *           subject "Too much traffic from zone $z1 to $z2"
 *           text "The traffic from zones $z1 to $z2 has raised above
 *                 the configured maximum of $max_bps for the last $obs_window usec.
 *                 See https://event_proc.home.lan/show_alert?id=%{id}"
 *
 * c1 -> A1
 * c2 -> A2
 *
 *)

open Batteries
open Stdint
open RamenSharedTypes
type uint8 = Uint8.t
type uint16 = Uint16.t

exception SyntaxError of string

let () =
  Printexc.register_printer (function
    | SyntaxError e -> Some ("Syntax Error: "^ e)
    | _ -> None)

module PConfig = ParsersPositions.LineCol (Parsers.SimpleConfig (Char))
module P = Parsers.Make (PConfig)
module ParseUsual = ParsersUsual.Make (P)
let strinG = ParseUsual.string ~case_sensitive:false
let that_string s =
  let open P in
  strinG s >>: fun () -> s (* because [string] returns () *)
let blanks =
  let open P in
  ParseUsual.whitespace >>: fun _ -> ()
let opt_blanks =
  P.optional_greedy ~def:() blanks

let same_tuple_as_in = function
  | "in" | "first" | "last" | "any" -> true
  | "out" | "previous" | "others" -> false
  | _ -> assert false

(*$inject
  open Stdint
  open Batteries
  open RamenSharedTypes
  open Lang.P

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
    | Sequence (_, a, b) -> Sequence (typ, replace_typ a, replace_typ b)
    | Cast (_, a) -> Cast (typ, replace_typ a)
    | Not (_, a) -> Not (typ, replace_typ a)
    | Defined (_, a) -> Defined (typ, replace_typ a)
    | Add (_, a, b) -> Add (typ, replace_typ a, replace_typ b)
    | Sub (_, a, b) -> Sub (typ, replace_typ a, replace_typ b)
    | Mul (_, a, b) -> Mul (typ, replace_typ a, replace_typ b)
    | Div (_, a, b) -> Div (typ, replace_typ a, replace_typ b)
    | IDiv (_, a, b) -> IDiv (typ, replace_typ a, replace_typ b)
    | Mod (_, a, b) -> Mod (typ, replace_typ a, replace_typ b)
    | Exp (_, a, b) -> Exp (typ, replace_typ a, replace_typ b)
    | And (_, a, b) -> And (typ, replace_typ a, replace_typ b)
    | Or (_, a, b) -> Or (typ, replace_typ a, replace_typ b)
    | Ge (_, a, b) -> Ge (typ, replace_typ a, replace_typ b)
    | Gt (_, a, b) -> Gt (typ, replace_typ a, replace_typ b)
    | Eq (_, a, b) -> Eq (typ, replace_typ a, replace_typ b)

  let replace_typ_in_expr = function
    | Ok (expr, rest) -> Ok (replace_typ expr, rest)
    | x -> x

  let replace_typ_in_op =
    let open Lang.Operation in
    function
    | Ok (Select { fields ; and_all_others ; where }, rest) ->
      Ok (Select {
        fields = List.map (fun sf -> { sf with expr = replace_typ sf.expr }) fields ;
        and_all_others ; where = replace_typ where }, rest)
    | Ok (Aggregate { fields ; and_all_others ; where ; key ; commit_when ; flush_when }, rest) ->
      Ok (Aggregate {
        fields = List.map (fun sf -> { sf with expr = replace_typ sf.expr }) fields ;
        and_all_others ;
        where = replace_typ where ;
        key = List.map replace_typ key ;
        commit_when = replace_typ commit_when ;
        flush_when = Option.map replace_typ flush_when }, rest)
    | Ok (OnChange e, rest) -> Ok (OnChange (replace_typ e), rest)
    | x -> x
 *)

module Scalar =
struct
  (*$< Scalar *)

  type typ = scalar

  let print_typ fmt typ =
    let s = match typ with
      | TFloat  -> "FLOAT"
      | TString -> "STRING"
      | TBool   -> "BOOL"
      | TNum    -> "ANY_NUM"
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
    in
    String.print fmt s

  type type_class = KNum | KBool | KString
  let compare_typ typ1 typ2 =
    let rank_of_typ = function
      | TFloat  -> KNum, 200
      | TU128   -> KNum, 128
      | TI128   -> KNum, 127
      | TNum    -> KNum, 0
      | TU64    -> KNum, 64
      | TI64    -> KNum, 63
      | TU32    -> KNum, 32
      | TI32    -> KNum, 31
      | TU16    -> KNum, 16
      | TI16    -> KNum, 15
      | TU8     -> KNum, 8
      | TI8     -> KNum, 7
      | TString -> KString, 1
      | TBool   -> KBool, 1
    in
    let k1, r1 = rank_of_typ typ1
    and k2, r2 = rank_of_typ typ2 in
    if k1 <> k2 then invalid_arg "types not comparable" ;
    compare r1 r2

  let larger_type (t1, t2) =
    if compare_typ t1 t2 >= 0 then t1 else t2

  (* stdint types are implemented as custom blocks, therefore are slower than ints.
   * But we do not care as we merely represents code here, we do not run the operators. *)
  type t = VFloat of float | VString of string | VBool of bool
         | VU8 of uint8 | VU16 of uint16 | VU32 of uint32
         | VU64 of uint64 | VU128 of uint128
         | VI8 of int8 | VI16 of int16 | VI32 of int32
         | VI64 of int64 | VI128 of int128

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

  let type_of = function
    | VFloat _ -> TFloat | VString _ -> TString | VBool _ -> TBool
    | VU8 _ -> TU8 | VU16 _ -> TU16 | VU32 _ -> TU32 | VU64 _ -> TU64
    | VU128 _ -> TU128 | VI8 _ -> TI8 | VI16 _ -> TI16 | VI32 _ -> TI32
    | VI64 _ -> TI64 | VI128 _ -> TI128

  module Parser =
  struct
    (*$< Parser *)
    open ParseUsual
    open P

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
      (integer >>: narrowest_int_scalar)        |||
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
      (floating_point >>: fun f -> VFloat f)    |||
      (strinG "false" >>: fun _ -> VBool false) |||
      (strinG "true" >>: fun _ -> VBool true)   |||
      (quoted_string >>: fun s -> VString s)

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
      (strinG "i128" >>: fun () -> TI128)

    (*$>*)
  end
  (*$>*)
end

let keyword =
  let open P in
  let open ParseUsual in
  (
    strinG "true" ||| strinG "false" ||| strinG "and" ||| strinG "or" |||
    strinG "min" ||| strinG "max" ||| strinG "sum" ||| strinG "percentile" |||
    strinG "of" ||| strinG "is" ||| strinG "not" ||| strinG "null" |||
    strinG "group" ||| strinG "by" ||| strinG "select" ||| strinG "where" |||
    strinG "on" ||| strinG "change" ||| strinG "flush" ||| strinG "when" |||
    strinG "age" ||| strinG "alert" ||| strinG "subject" ||| strinG "text" |||
    strinG "read" ||| strinG "from" ||| strinG "csv" ||| strinG "file" |||
    strinG "separator" ||| strinG "as" ||| strinG "first" ||| strinG "last" |||
    strinG "sequence" ||| strinG "int8" ||| strinG "int16" |||
    strinG "int32" ||| strinG "int64" ||| strinG "int128" |||
    strinG "uint8" ||| strinG "uint16" ||| strinG "uint32" |||
    strinG "uint64" ||| strinG "uint128" |||
    (Scalar.Parser.typ >>: fun _ -> ())
  ) -- check (nay (letter ||| underscore ||| decimal_digit))
let non_keyword =
  (* TODO: allow keywords if quoted *)
  let open P in
  check ~what:"no keyword" (nay keyword) -+ ParseUsual.identifier

module Tuple =
struct
  type field_typ = { name : string ; nullable : bool ; typ : scalar }

  let print_field_typ fmt field =
    (* TODO: check that name is a valid identifier *)
    Printf.fprintf fmt "%s %a %sNULL"
      field.name
      Scalar.print_typ field.typ
      (if field.nullable then "" else "NOT ")

  type typ = field_typ list

  let print_typ fmt lst =
    (List.print ~first:"(" ~last:")" ~sep:", " print_field_typ) fmt lst

  type t = (string, Scalar.t) Hashtbl.t
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
      mutable scalar_typ : scalar option }

  let to_expr_type_info typ =
    { RamenSharedTypes.name = typ.expr_name ;
      RamenSharedTypes.nullable = Option.get typ.nullable ;
      RamenSharedTypes.typ = Option.get typ.scalar_typ }

  let typ_is_complete typ =
    typ.nullable <> None && typ.scalar_typ <> None

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
  let make_num_typ ?nullable name =
    make_typ ?nullable ~typ:TNum name (* will be enlarged as required *)
  let copy_typ typ =
    incr uniq_num_seq ;
    { typ with expr_name = typ.expr_name ; uniq_num = !uniq_num_seq }

  (* Expressions on scalars (aka fields) *)
  type t =
    | Const of typ * Scalar.t
    | Field of typ * string ref (* tuple: in, out, others... *) * string (* field name *)
    | Param of typ * string
    (* Valid only within an aggregation operation; but must be here to allow
     * operations on top of the result of an aggregation function, such as: "(1
     * + min field1) / (max field2)". Even within an aggregation, not valid
     * within another aggregation function. *)
    | AggrMin of typ * t
    | AggrMax of typ * t
    | AggrSum of typ * t
    | AggrAnd of typ * t
    | AggrOr  of typ * t
    | AggrFirst of typ * t
    | AggrLast of typ * t
    (* TODO: several percentiles.
     * Not easy because then the function must return a list instead of a
     * scalar. It's probably easier to try to optimise the code generated
     * for when the same expression is used in several percentile functions. *)
    | AggrPercentile of typ * t * t
    (* Other functions: abs, random, date_part, string_concat, string_length, now... *)
    | Age of typ * t
    | Sequence of typ * t * t (* start, step *)
    | Cast of typ * t
    (* Unary Ops on scalars *)
    | Not of typ * t
    | Defined of typ * t
    (* Binary Ops scalars *)
    | Add of typ * t * t
    | Sub of typ * t * t
    | Mul of typ * t * t
    | Div of typ * t * t
    | IDiv of typ * t * t
    | Mod of typ * t * t
    | Exp of typ * t * t
    | And of typ * t * t
    | Or  of typ * t * t
    | Ge  of typ * t * t
    | Gt  of typ * t * t
    | Eq  of typ * t * t

  let rec print with_types fmt =
    let add_types t =
      if with_types then Printf.fprintf fmt " [%a]" print_typ t
    in
    function
    | Const (t, c) -> Scalar.print fmt c ; add_types t
    | Field (t, tuple, field) -> Printf.fprintf fmt "%s.%s" !tuple field ; add_types t
    | Param (t, p) -> Printf.fprintf fmt "$%s" p ; add_types t
    | AggrMin (t, e) -> Printf.fprintf fmt "min (%a)" (print with_types) e ; add_types t
    | AggrMax (t, e) -> Printf.fprintf fmt "max (%a)" (print with_types) e ; add_types t
    | AggrSum (t, e) -> Printf.fprintf fmt "sum (%a)" (print with_types) e ; add_types t
    | AggrAnd (t, e) -> Printf.fprintf fmt "and (%a)" (print with_types) e ; add_types t
    | AggrOr  (t, e) -> Printf.fprintf fmt "or (%a)" (print with_types) e ; add_types t
    | AggrFirst (t, e) -> Printf.fprintf fmt "first (%a)" (print with_types) e ; add_types t
    | AggrLast (t, e) -> Printf.fprintf fmt "last (%a)" (print with_types) e ; add_types t
    | AggrPercentile (t, p, e) -> Printf.fprintf fmt "%ath percentile (%a)" (print with_types) p (print with_types) e ; add_types t
    | Age (t, e) -> Printf.fprintf fmt "age(%a)" (print with_types) e ; add_types t
    | Sequence (t, e1, e2) -> Printf.fprintf fmt "sequence(%a, %a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | Cast (t, e) -> Printf.fprintf fmt "cast(%a, %a)" Scalar.print_typ (Option.get t.scalar_typ) (print with_types) e ; add_types t
    | Not (t, e) -> Printf.fprintf fmt "NOT (%a)" (print with_types) e ; add_types t
    | Defined (t, e) -> Printf.fprintf fmt "(%a) IS NOT NULL" (print with_types) e ; add_types t
    | Add (t, e1, e2) -> Printf.fprintf fmt "(%a) + (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | Sub (t, e1, e2) -> Printf.fprintf fmt "(%a) - (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | Mul (t, e1, e2) -> Printf.fprintf fmt "(%a) * (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | Div (t, e1, e2) -> Printf.fprintf fmt "(%a) / (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | IDiv (t, e1, e2) -> Printf.fprintf fmt "(%a) // (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | Mod (t, e1, e2) -> Printf.fprintf fmt "(%a) %% (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | Exp (t, e1, e2) -> Printf.fprintf fmt "(%a) ^ (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | And (t, e1, e2) -> Printf.fprintf fmt "(%a) AND (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | Or (t, e1, e2) -> Printf.fprintf fmt "(%a) OR (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | Ge (t, e1, e2) -> Printf.fprintf fmt "(%a) >= (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | Gt (t, e1, e2) -> Printf.fprintf fmt "(%a) > (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
    | Eq (t, e1, e2) -> Printf.fprintf fmt "(%a) = (%a)" (print with_types) e1 (print with_types) e2 ; add_types t

  let typ_of = function
    | Const (t, _) | Field (t, _, _) | Param (t, _) | AggrMin (t, _)
    | AggrMax (t, _) | AggrSum (t, _) | AggrAnd (t, _) | AggrOr  (t, _)
    | AggrFirst (t, _) | AggrLast (t, _) | AggrPercentile (t, _, _)
    | Age (t, _) | Sequence (t, _, _) | Not (t, _) | Defined (t, _)
    | Add (t, _, _) | Sub (t, _, _) | Mul (t, _, _) | Div (t, _, _)
    | IDiv (t, _, _) | Exp (t, _, _) | And (t, _, _) | Or (t, _, _)
    | Ge (t, _, _) | Gt (t, _, _) | Eq (t, _, _) | Mod (t, _, _)
    | Cast (t, _) -> t

  let is_nullable e =
    let t = typ_of e in
    t.nullable = Some true

  let rec fold f i expr =
    match expr with
    | Const _ | Param _ | Field _ ->
      f i expr
    | AggrMin (_, e) | AggrMax (_, e) | AggrSum (_, e) | AggrAnd (_, e)
    | AggrOr (_, e) | AggrFirst (_, e) | AggrLast (_, e) | Age (_, e)
    | Not (_, e) | Defined (_, e) | Cast (_, e) ->
      fold f (f i expr) e ;
    | AggrPercentile (_, e1, e2) | Sequence (_, e1, e2)
    | Add (_, e1, e2) | Sub (_, e1, e2) | Mul (_, e1, e2) | Div (_, e1, e2)
    | IDiv (_, e1, e2) | Exp (_, e1, e2) | And (_, e1, e2) | Or (_, e1, e2)
    | Ge (_, e1, e2) | Gt (_, e1, e2) | Eq (_, e1, e2) | Mod (_, e1, e2) ->
      fold f (fold f (f i expr) e1) e2

  (* Propagate values down the tree only. Final return value is thus
   * mostly meaningless (it's the value for the last path down to the
   * last leaf). *)
  let rec fold_by_depth f i expr =
    match expr with
    | Const _ | Param _ | Field _ ->
      f i expr
    | AggrMin (_, e) | AggrMax (_, e) | AggrSum (_, e) | AggrAnd (_, e)
    | AggrOr (_, e) | AggrFirst (_, e) | AggrLast (_, e) | Age (_, e)
    | Not (_, e) | Defined (_, e) | Cast (_, e) ->
      fold_by_depth f (f i expr) e ;
    | AggrPercentile (_, e1, e2) | Sequence (_, e1, e2)
    | Add (_, e1, e2) | Sub (_, e1, e2) | Mul (_, e1, e2) | Div (_, e1, e2)
    | IDiv (_, e1, e2) | Exp (_, e1, e2) | And (_, e1, e2) | Or (_, e1, e2)
    | Ge (_, e1, e2) | Gt (_, e1, e2) | Eq (_, e1, e2) | Mod (_, e1, e2) ->
      let i' = f i expr in
      fold_by_depth f i' e1 ;
      fold_by_depth f i' e2

  let iter f = fold_by_depth (fun () e -> f e) ()

  let aggr_iter f expr =
    fold_by_depth (fun in_aggr -> function
      | AggrMin _ | AggrMax _ | AggrSum _ | AggrAnd _ | AggrOr _ | AggrFirst _
      | AggrLast _ | AggrPercentile _ as expr ->
        if in_aggr then (
          let m = "Aggregate functions are not allowed within \
                   aggregate functions" in
          raise (SyntaxError m)) ;
        f expr ;
        true
      | Const _ | Param _ | Field _ | Cast _
      | Age _ | Sequence _ | Not _ | Defined _ | Add _ | Sub _ | Mul _ | Div _
      | IDiv _ | Exp _ | And _ | Or _ | Ge _ | Gt _ | Eq _ | Mod _ ->
        in_aggr) false expr |> ignore

  module Parser =
  struct
    (*$< Parser *)
    open ParseUsual
    open P

    (* Single things *)
    let const m =
      let m = "constant" :: m in
      (Scalar.Parser.p >>: fun c ->
       Const (make_typ ~nullable:false ~typ:(Scalar.type_of c) "constant", c)) m
    (*$= const & ~printer:(test_printer (print false))
      (Ok (Const (typ, Scalar.VBool true), (4, [])))\
        (test_p const "true" |> replace_typ_in_expr)
    *)

    let field m =
      let m = "field" :: m in
      let prefix s = strinG (s ^ ".") >>: fun () -> s in
      (optional ~def:"in" (
         prefix "in" ||| prefix "out" ||| prefix "first" |||
         prefix "previous" ||| prefix "others" ||| prefix "any") ++
       non_keyword >>: fun (tuple, field) ->
       (* This is important here that the type name is the raw field name,
        * because we use the tuple field type name as their identifier *)
       Field (make_typ field, ref tuple, field)) m
    (*$= field & ~printer:(test_printer (print false))
      (Ok (\
        Field (typ, ref "in", "bytes"),\
        (5, [])))\
        (test_p field "bytes" |> replace_typ_in_expr)

      (Ok (\
        Field (typ, ref "in", "bytes"),\
        (8, [])))\
        (test_p field "in.bytes" |> replace_typ_in_expr)

      (Ok (\
        Field (typ, ref "out", "bytes"),\
        (9, [])))\
        (test_p field "out.bytes" |> replace_typ_in_expr)

      (Bad (\
        NoSolution (\
          Some { where = ParsersMisc.Item ((0,7), '.');\
                 what=["eof"]})))\
        (test_p field "pasglop.bytes" |> replace_typ_in_expr)
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
        | _ -> assert false in
      binary_ops_reducer ~op ~term:mid_prec_left_assoc ~sep:opt_blanks ~reduce m

    and mid_prec_left_assoc m =
      let m = "arithmetic operator" :: m in
      let op = that_string "+" ||| that_string "-"
      and reduce t1 op t2 = match op with
        | "+" -> Add (make_num_typ "addition", t1, t2)
        | "-" -> Sub (make_num_typ "subtraction", t1, t2)
        | _ -> assert false in
      binary_ops_reducer ~op ~term:high_prec_left_assoc ~sep:opt_blanks~reduce m

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
      and reduce t1 _ t2 = Exp (make_num_typ "exponentiation", t1, t2) in
      binary_ops_reducer ~op ~right_associative:true
                         ~term:highest_prec_left_assoc ~sep:opt_blanks~reduce m

    and highest_prec_left_assoc m =
      ((strinG "not" -+ highestest_prec >>: fun e -> Not (make_bool_typ "not operator", e)) |||
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

    and afun1 n =
      let sep = check (char '(') ||| blanks in
      strinG n -- optional ~def:() (blanks -- strinG "of") -- sep -+
      highestest_prec

    and afun2 n =
      let sep = check (char '(') ||| blanks in
      strinG n -- optional ~def:() (blanks -- strinG "of") -- sep -+
      highestest_prec +- opt_blanks +- char ',' +- opt_blanks ++
      highestest_prec

    and aggregate m =
      let m = "aggregate function" :: m in
      (* Note: min and max of nothing are NULL but sum of nothing is 0, etc *)
      ((afun1 "min" >>: fun e -> AggrMin (make_num_typ "min aggregation", e)) |||
       (afun1 "max" >>: fun e -> AggrMax (make_num_typ "max aggregation", e)) |||
       (afun1 "sum" >>: fun e -> AggrSum (make_num_typ "sum aggregation", e)) |||
       (afun1 "and" >>: fun e -> AggrAnd (make_bool_typ "and aggregation", e)) |||
       (afun1 "or" >>: fun e -> AggrOr (make_bool_typ "or aggregation", e)) |||
       (afun1 "first" >>: fun e -> AggrFirst (make_bool_typ "first aggregation", e)) |||
       (afun1 "last" >>: fun e -> AggrLast (make_bool_typ "last aggregation", e)) |||
       ((const ||| param) +- (optional ~def:() (strinG "th")) +- blanks ++
        afun1 "percentile" >>: fun (p, e) ->
        (* Percentile aggr function is nullable because we want it null when
         * we do not have enough measures to compute the requested percentiles.
         * Alternatively, we could return the best bet. *)
        AggrPercentile (make_num_typ ~nullable:true "percentile aggregation", p, e))
      ) m

    and func =
      fun m ->
        let m = "function" :: m in
        ((afun1 "age" >>: fun e -> Age (make_num_typ "age function", e)) |||
         sequence ||| cast) m

    and sequence =
      let seq = "sequence"
      and seq_typ = make_typ ~nullable:false ~typ:TI128 "sequence function"
      and seq_default_step = Const (make_typ ~nullable:false ~typ:TU8 "sequence step",
                                    Scalar.VU8 (Uint8.of_int 1))
      and seq_default_start = Const (make_typ ~nullable:false ~typ:TU8 "sequence start",
                                     Scalar.VU8 (Uint8.of_int 0)) in
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
      ((afun1 "int8" >>: fun e -> Cast (make_typ ~typ:TI8 "cast to int8", e)) |||
       (afun1 "int16" >>: fun e -> Cast (make_typ ~typ:TI16 "cast to int16", e)) |||
       (afun1 "int32" >>: fun e -> Cast (make_typ ~typ:TI32 "cast to int32", e)) |||
       (afun1 "int64" >>: fun e -> Cast (make_typ ~typ:TI64 "cast to int64", e)) |||
       (afun1 "int128" >>: fun e -> Cast (make_typ ~typ:TI128 "cast to int128", e)) |||
       (afun1 "uint8" >>: fun e -> Cast (make_typ ~typ:TU8 "cast to uint8", e)) |||
       (afun1 "uint16" >>: fun e -> Cast (make_typ ~typ:TU16 "cast to uint16", e)) |||
       (afun1 "uint32" >>: fun e -> Cast (make_typ ~typ:TU32 "cast to uint32", e)) |||
       (afun1 "uint64" >>: fun e -> Cast (make_typ ~typ:TU64 "cast to uint64", e)) |||
       (afun1 "uint128" >>: fun e -> Cast (make_typ ~typ:TU128 "cast to uint128", e))) m

    and highestest_prec m =
      let sep = optional_greedy ~def:() blanks in
      (const ||| field ||| param ||| func ||| aggregate |||
       char '(' -- sep -+ lowest_prec_left_assoc +- sep +- char ')'
      ) m

    let p = lowest_prec_left_assoc

    (*$= p & ~printer:(test_printer (print false))
      (Ok (\
        Const (typ, Scalar.VBool true),\
        (4, [])))\
        (test_p p "true" |> replace_typ_in_expr)

      (Ok (\
        Not (typ, Defined (typ, Field (typ, ref "in", "zone_src"))),\
        (16, [])))\
        (test_p p "zone_src IS NULL" |> replace_typ_in_expr)

      (Ok (\
        Eq (typ, Field (typ, ref "in", "zone_src"), Param (typ, "z1")),\
        (14, [])))\
        (test_p p "zone_src = $z1" |> replace_typ_in_expr)

      (Ok (\
        And (typ, \
          Or (typ, \
            Not (typ, Defined (typ, Field (typ, ref "in", "zone_src"))),\
            Eq (typ, Field (typ, ref "in", "zone_src"), Param (typ, "z1"))),\
          Or (typ, \
            Not (typ, Defined (typ, Field (typ, ref "in", "zone_dst"))),\
            Eq (typ, Field (typ, ref "in", "zone_dst"), Param (typ, "z2")))),\
        (77, [])))\
        (test_p p "(zone_src IS NULL or zone_src = $z1) and \\
                   (zone_dst IS NULL or zone_dst = $z2)" |> replace_typ_in_expr)

      (Ok (\
        Div (typ, \
          AggrSum (typ, Field (typ, ref "in", "bytes")),\
          Param (typ, "avg_window")),\
        (23, [])))\
        (test_p p "(sum bytes)/$avg_window" |> replace_typ_in_expr)

      (Ok (\
        IDiv (typ, \
          Field (typ, ref "in", "start"),\
          Mul (typ, \
            Const (typ, Scalar.VI32 1_000_000l),\
            Param (typ, "avg_window"))),\
        (34, [])))\
        (test_p p "start // (1_000_000 * $avg_window)" |> replace_typ_in_expr)

      (Ok (\
        AggrPercentile (typ,\
          Param (typ, "p"),\
          Field (typ, ref "in", "bytes_per_sec")),\
        (30, [])))\
        (test_p p "$p percentile of bytes_per_sec" |> replace_typ_in_expr)

      (Ok (\
        Gt (typ, \
          AggrMax (typ, \
            Field (typ, ref "others", "start")),\
          Add (typ, \
            Field (typ, ref "out", "start"),\
            Mul (typ, \
              Mul (typ, \
                Param (typ, "obs_window"),\
                Const (typ, Scalar.VFloat 1.15)),\
              Const (typ, Scalar.VI32 1_000_000l)))),\
        (63, [])))\
        (test_p p "max others.start > \\
                   out.start + ($obs_window * 1.15) * 1_000_000" |> replace_typ_in_expr)

      (Ok (\
        Mod (typ,\
          Param (typ, "x"),\
          Param (typ, "y")),\
        (7, [])))\
        (test_p p "$x % $y" |> replace_typ_in_expr)
    *)

    (*$>*)
  end
  (*$>*)
end

module Operation =
struct
  (*$< Operation *)

  (* Direct field selection (not for group-bys) *)
  type selected_field = { expr : Expr.t ; alias : string list }

  let print_selected_field fmt f =
    let need_alias =
      match f.expr, f.alias with
      | _, [] -> false
      | Expr.Field (_, tuple, field), [ a ]
        when !tuple = "in" && a = field -> false
      | _ -> true in
    if need_alias then
      Printf.fprintf fmt "%a AS %a"
        (Expr.print false) f.expr
        (List.print ~first:"" ~last:"" ~sep:" OR " String.print) f.alias
    else
      Expr.print false fmt f.expr

  type t =
    (* Generate values out of thin air. The difference with Select is that
     * Yield does not wait for some input. *)
    | Yield of selected_field list
    (* Simple operation that merely filters / projects / constructs fields and
     * produce 0 or 1 tuple for each input tuple. *)
    | Select of {
        fields : selected_field list ;
        (* As in the SQL "*", but selects only the fields which name won't collide
         * with the above. Useful for when the select part is implicit. *)
        and_all_others : bool ;
        where : Expr.t }
    (* Aggregation of several tuples into one based on some key. Superficially looks like
     * a select but much more involved. *)
    | Aggregate of {
        fields : selected_field list ;
        (* Pass all fields not used to build an aggregated field *)
        and_all_others : bool ;
        (* Simple way to filter out incoming tuples: *)
        where : Expr.t ;
        key : Expr.t list ;
        commit_when : Expr.t ;
        (* When do we stop aggregating (default: when we commit) *)
        flush_when : Expr.t option }
    (* Not sure we need OnChange if we have access to last tuple in select
     * where clause... *)
    | OnChange of Expr.t
    | Alert of { team : string ; subject : string ; text : string }
    | ReadCSVFile of { fname : string ; separator : string ;
                       null : string ; fields : Tuple.typ }

  let print fmt =
    let sep = ", " in
    function
    | Yield fields ->
      Printf.fprintf fmt "YIELD %a"
        (List.print ~first:"" ~last:"" ~sep print_selected_field) fields
    | Select { fields ; and_all_others ; where } ->
      Printf.fprintf fmt "SELECT %a%s%s WHERE %a"
        (List.print ~first:"" ~last:"" ~sep print_selected_field) fields
        (if fields <> [] && and_all_others then sep else "")
        (if and_all_others then "*" else "")
        (Expr.print false) where
    | Aggregate { fields ; and_all_others ; where ; key ;
                  commit_when ; flush_when } ->
      Printf.fprintf fmt "SELECT %a%s%s WHERE %a%s%a COMMIT %sWHEN %a"
        (List.print ~first:"" ~last:"" ~sep print_selected_field) fields
        (if fields <> [] && and_all_others then sep else "")
        (if and_all_others then "*" else "")
        (Expr.print false) where
        (if key <> [] then " GROUP BY " else "")
        (List.print ~first:"" ~last:"" ~sep:", " (Expr.print false)) key
        (if flush_when = None then "AND FLUSH " else "")
        (Expr.print false) commit_when ;
      Option.may (fun flush_when ->
        Printf.fprintf fmt " FLUSH WHEN %a"
          (Expr.print false) flush_when) flush_when
    | OnChange e ->
      Printf.fprintf fmt "ON CHANGE %a" (Expr.print false) e
    | Alert { team ; subject ; text } ->
      Printf.fprintf fmt "ALERT %S SUBJECT %S TEXT %S" team subject text
    | ReadCSVFile { fname ; separator ; null ; fields } ->
      Printf.fprintf fmt "READ FROM CSV FILE %S SEPARATOR %S NULL %S %a"
        fname separator null Tuple.print_typ fields

  module Parser =
  struct
    (*$< Parser *)
    open ParseUsual
    open P

    let selected_field m =
      let m = "selected field" :: m in
      (Expr.Parser.p ++ optional ~def:[] (
         blanks -- strinG "as" -- blanks -+
         several ~sep:(blanks -- strinG "or" -- blanks)
                non_keyword) >>:
       fun (expr, alias) ->
         let alias =
           if alias <> [] then alias else (
             match expr with
             | Expr.Field (_, { contents="in" }, field) -> [ field ]
             (* Provide some default name for current aggregate functions: *)
             | Expr.AggrMin (_, Expr.Field (_, { contents="in" }, field)) -> [ "min_"^ field ]
             | Expr.AggrMax (_, Expr.Field (_, { contents="in" }, field)) -> [ "max_"^ field ]
             | Expr.AggrSum (_, Expr.Field (_, { contents="in" }, field)) -> [ "sum_"^ field ]
             | Expr.AggrAnd (_, Expr.Field (_, { contents="in" }, field)) -> [ "and_"^ field ]
             | Expr.AggrOr  (_, Expr.Field (_, { contents="in" }, field)) -> [ "or_"^ field ]
             | Expr.AggrFirst (_, Expr.Field (_, { contents="in" }, field)) -> [ "first_"^ field ]
             | Expr.AggrLast (_, Expr.Field (_, { contents="in" }, field)) -> [ "last_"^ field ]
             | _ -> raise (Reject "must set alias")
           ) in
         { expr ; alias }) m

    let list_sep m =
      let m = "list separator" :: m in
      (opt_blanks -- char ',' -- opt_blanks) m

    let yield =
      strinG "yield" -- blanks -+
      several ~sep:list_sep selected_field >>: fun fields -> Yield fields

    let select_clause m =
      let m = "select clause" :: m in
      (strinG "select" -- blanks -+
       several ~sep:list_sep
              ((char '*' >>: fun _ -> None) |||
               some selected_field)) m

    let where_clause m =
      let m = "where clause" :: m in
      (strinG "where" -- blanks -+ Expr.Parser.p) m

    let select =
      (select_clause ++
       (let def = Expr.Const (Expr.make_bool_typ ~nullable:false "true", Scalar.VBool true) in
        optional ~def (blanks -+ where_clause)) |||
       return [None] ++ where_clause) >>:
      fun (fields_or_stars, where) ->
        let fields, and_all_others =
          List.fold_left (fun (fields, and_all_others) -> function
              | Some f -> f::fields, and_all_others
              | None when not and_all_others -> fields, true
              | None ->
                  (* P.map should catch exceptions and we should set a
                     parse error from here. *)
                  Printf.eprintf "Already included star...\n" ;
                  fields, and_all_others
            ) ([], false) fields_or_stars in
        (* The above fold_left inverted the field order. *)
        let fields = List.rev fields in
        Select { fields ; and_all_others ; where }

    let group_by m =
      let m = "group-by clause" :: m in
      (strinG "group" -- blanks -- strinG "by" -- blanks -+
       several ~sep:list_sep Expr.Parser.p) m

    let commit_when m =
      let m = "commit clause" :: m in
      (strinG "commit" -- blanks -+
       optional ~def:false
         (strinG "and" -- blanks -- strinG "flush" -- blanks >>: fun () -> true) +-
       strinG "when" +- blanks ++
       Expr.Parser.p ++
       optional ~def:None
         (blanks -- strinG "flush" -- blanks -- strinG "when" -- blanks -+
          some Expr.Parser.p) >>:
       function
       | (true, commit_when), None ->
         commit_when, None
       | (true, _), Some _ ->
         raise (Reject "AND FLUSH incompatible with FLUSH WHEN")
       | (false, commit_when), (Some _ as flush_when) ->
         commit_when, flush_when
       | (false, _), None ->
         raise (Reject "Must specify when to flush")
      ) m

    let aggregate m =
      let m = "aggregate" :: m in
      (select +- blanks ++ optional ~def:[] (group_by +- blanks) ++ commit_when >>: function
       | (Select { fields ; and_all_others ; where }, key), (commit_when, flush_when) ->
         Aggregate { fields ; and_all_others ; where ; key ; commit_when ; flush_when }
       | _ -> assert false) m

    let on_change m =
      let m = "on-change" :: m in
      (strinG "on" -- blanks -- strinG "change" -- blanks -+ Expr.Parser.p >>:
       fun e -> OnChange e) m

    let alert m =
      let opt_field title m =
        let m = title :: m in
        (optional ~def:"" (blanks -- strinG title -- blanks -+ quoted_string)) m
      in
      let m = "alert" :: m in
      (strinG "alert" -- blanks -+ quoted_string ++
       opt_field "subject" ++ opt_field "text" >>:
       fun ((team, subject), text) -> Alert { team ; subject ; text }) m

    let read_csv_file m =
      let m = "read-csv" :: m in
      let field =
        non_keyword +- blanks ++ Scalar.Parser.typ ++
        optional ~def:true (
          optional ~def:true (blanks -+ (strinG "not" >>: fun () -> false)) +-
          blanks +- strinG "null") >>:
        fun ((name, typ), nullable) -> Tuple.{ name ; typ ; nullable }
      in
      (strinG "read" -- blanks -- strinG "from" -- blanks --
       optional ~def:() (strinG "csv" +- blanks) -- strinG "file" -- blanks -+
       quoted_string +- opt_blanks ++
       optional ~def:"," (
         strinG "separator" -- opt_blanks -+ quoted_string +- opt_blanks) ++
       optional ~def:"" (
         strinG "null" -- opt_blanks -+ quoted_string +- opt_blanks) +-
       char '(' +- opt_blanks ++
       several ~sep:list_sep field +- opt_blanks +- char ')' >>:
       fun (((fname, separator), null), fields) ->
        ReadCSVFile { fname ; separator ; null ; fields }) m

    let p m =
      let m = "operation" :: m in
      (yield ||| select ||| aggregate ||| on_change ||| alert ||| read_csv_file) m

    (*$= p & ~printer:(test_printer print)
      (Ok (\
        Select {\
          fields = [\
            { expr = Expr.(Field (typ, ref "in", "start")) ;\
              alias = [ "start" ] } ;\
            { expr = Expr.(Field (typ, ref "in", "stop")) ;\
              alias = [ "stop" ] } ;\
            { expr = Expr.(Field (typ, ref "in", "itf_clt")) ;\
              alias = [ "itf_src" ] } ;\
            { expr = Expr.(Field (typ, ref "in", "itf_srv")) ;\
              alias = [ "itf_dst" ] } ] ;\
          and_all_others = false ;\
          where = Expr.Const (typ, Scalar.VBool true) },\
        (58, [])))\
        (test_p p "select start, stop, itf_clt as itf_src, itf_srv as itf_dst" |>\
         replace_typ_in_op)

      (Ok (\
        Select {\
          fields = [] ;\
          and_all_others = true ;\
          where = Expr.(\
            Gt (typ,\
              Field (typ, ref "in", "packets"),\
              Const (typ, Scalar.VI8 (Int8.of_int 0)))) },\
        (17, [])))\
        (test_p p "where packets > 0" |> replace_typ_in_op)

      (Ok (\
        Aggregate {\
          fields = [\
            { expr = Expr.(\
                AggrMin (typ, Field (typ, ref "in", "start"))) ;\
              alias = [ "start"; "out_start" ] } ;\
            { expr = Expr.(\
                AggrMax (typ, Field (typ, ref "in", "stop"))) ;\
              alias = [ "max_stop" ] } ;\
            { expr = Expr.(\
                Div (typ,\
                  AggrSum (typ, Field (typ, ref "in", "packets")),\
                  Param (typ, "avg_window"))) ;\
              alias = [ "packets_per_sec" ] } ] ;\
          and_all_others = false ;\
          where = Expr.Const (typ, Scalar.VBool true) ;\
          key = [ Expr.(\
            Div (typ,\
              Field (typ, ref "in", "start"),\
              Mul (typ,\
                Const (typ, Scalar.VI32 1_000_000l),\
                Param (typ, "avg_window")))) ] ;\
          commit_when = Expr.(\
            Gt (typ,\
              Add (typ,\
                AggrMax (typ,Field (typ, ref "any", "start")),\
                Const (typ, Scalar.VI16 (Int16.of_int 3600))),\
              Field (typ, ref "out", "start"))) ; \
          flush_when = None },\
          (206, [])))\
          (test_p p "select min start as start or out_start, \\
                            max stop as max_stop, \\
                            (sum packets)/$avg_window as packets_per_sec \\
                     group by start / (1_000_000 * $avg_window) \\
                     commit and flush when out.start < (max any.start) + 3600" |>\
           replace_typ_in_op)

      (Ok (\
        Aggregate {\
          fields = [\
            { expr = Expr.Const (typ, Scalar.VI8 (Int8.of_int 1)) ;\
              alias = [ "one" ] } ] ;\
          and_all_others = false ;\
          where = Expr.Const (typ, Scalar.VBool true) ;\
          key = [] ;\
          commit_when = Expr.(\
            Ge (typ,\
              AggrSum (typ, Const (typ, Scalar.VI8 (Int8.of_int 1))),\
              Const (typ, Scalar.VI8 (Int8.of_int 5)))) ;\
          flush_when = None },\
          (48, [])))\
          (test_p p "select 1 as one commit and flush when sum 1 >= 5" |>\
           replace_typ_in_op)

      (Ok (\
        OnChange Expr.(Field (typ, ref "in", "too_low")),\
        (17, [])))\
        (test_p p "on change too_low" |> replace_typ_in_op)

      (Ok (\
        Alert { team = "network firefighters" ;\
                subject = "Too little traffic from zone ${z1} to ${z2}" ;\
                text = "fatigue..." },\
        (100, [])))\
        (test_p p "alert \"network firefighters\" \\
                         subject \"Too little traffic from zone ${z1} to ${z2}\" \\
                         text \"fatigue...\"")
      (Ok (\
        Alert { team = "glop" ; subject = "" ; text = "" },\
        (12, [])))\
        (test_p p "alert \"glop\"")

      (Ok (\
        ReadCSVFile { fname = "/tmp/toto.csv" ; separator = "," ; null = "" ; \
                      fields = Lang.Tuple.[ \
                        { name = "f1" ; nullable = true ; typ = TBool } ;\
                        { name = "f2" ; nullable = false ; typ = TI32 } ] },\
        (61, [])))\
        (test_p p "read from csv file \"/tmp/toto.csv\" (f1 bool, f2 i32 not null)")

      (Ok (\
        ReadCSVFile { fname = "/tmp/toto.csv" ; separator = "\t" ; null = "<NULL>" ; \
                      fields = Lang.Tuple.[ \
                        { name = "f1" ; nullable = true ; typ = TBool } ;\
                        { name = "f2" ; nullable = false ; typ = TI32 } ] },\
        (90, [])))\
        (test_p p "read from csv file \"/tmp/toto.csv\" \\
                                 separator \"\\t\" null \"<NULL>\" \\
                                 (f1 bool, f2 i32 not null)")
    *)

    (* Check that the expression is valid, or return an error message.
     * Also perform some optimisation, numeric promotions, etc... *)
    let check =
      let no_aggr_in clause =
        "Aggregation function not allowed in "^ clause
      and fields_must_be_from lst clause =
        Printf.sprintf "All fields must come from %s in %s"
          (IO.to_string
            (List.print ~first:"" ~last:"" ~sep:" or " String.print)
            lst)
          clause
        in
      let no_aggr_in_where = no_aggr_in "WHERE clause"
      and no_aggr_in_key = no_aggr_in "GROUP-BY clause"
      and no_aggr_in_on_change = no_aggr_in "ON-CHANGE clause"
      and check_no_aggr m =
        Expr.aggr_iter (fun _ -> raise (SyntaxError m))
      and check_fields_from lst clause =
        Expr.iter (function
          | Expr.Field (_, tuple, _) ->
            if not (List.mem !tuple lst) then (
              let m = fields_must_be_from lst clause in
              raise (SyntaxError m)
            )
          | _ -> ())
      in function
      | Yield fields ->
        List.iter (fun sf ->
            let m = "Aggregation functions not allowed in YIELDs" in
            check_no_aggr m sf.expr ;
            check_fields_from [] "YIELD operation" sf.expr
          ) fields
      | Select { fields ; where ; _ } ->
        List.iter (fun sf ->
            let m = "Aggregation functions not allowed without a \
                     GROUP-BY clause" in
            check_no_aggr m sf.expr ;
            check_fields_from ["in"] "SELECT clause" sf.expr
          ) fields ;
        check_no_aggr no_aggr_in_where where ;
        check_fields_from ["in"] "WHERE clause" where
      | Aggregate { fields ; where ; key ; commit_when ; flush_when ; _ } ->
        List.iter (fun sf ->
            check_fields_from ["in"; "first"; "last"] "SELECT clause" sf.expr
          ) fields ;
        check_no_aggr no_aggr_in_where where ;
        check_fields_from ["in"; "first"; "last"] "WHERE clause" where ;
        List.iter (fun k ->
          check_no_aggr no_aggr_in_key k ;
          check_fields_from ["in"] "KEY clause" k) key ;
        Expr.aggr_iter ignore commit_when ; (* standards checks *)
        check_fields_from ["in";"out";"previous";"first";"last"] "COMMIT WHEN clause" commit_when ;
        Option.may (fun flush_when ->
            Expr.aggr_iter ignore flush_when ;
            check_fields_from ["in";"out";"previous";"first";"last"] "FLUSH WHEN clause" flush_when
          ) flush_when
      | OnChange e ->
        check_no_aggr no_aggr_in_on_change e
      | Alert _ ->
        () (* TODO: check field names from text templates *)
      | ReadCSVFile { separator ; null ; _ } ->
        if separator = null || separator = "" then (
          let m = "Invalid CSV separator/null" in
          raise (SyntaxError m))

    (*$>*)
  end
  (*$>*)
end
