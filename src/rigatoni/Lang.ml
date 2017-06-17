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
 *     emit after 3 seconds untouched or 100 other events
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
 * Note: Other possible syntax for emit: "emit after 3 seconds" (after
 * creation, regardless of what happened), "emit after 42 additions" count only
 * events added to an aggregate, while "emit after 42 events" count any events,
 * matching or not. Also, could be any expression involving the resulting
 * aggregate (out) or incoming event (in) or other events (others) or any
 * events (any), including aggregation function of those: "emit after out.start
 * < (max any.start) + 3600".
 *
 * Note: Every tuple that takes part in an aggregation operation ("out", "any"
 * and also the special "others") maintain virtual fields "start" (the time
 * when the first entry was added in this tuple), "last" (the time when
 * the last one was), "size" (the number of entries so far) and "successive"
 * (the number of entries added successively without any tuple aggregated
 * elsewhere). With those it is possible to construct all interesting emitting
 * condition I can think about. Note that the "other" tuple has to be simulated
 * since we cannot update every group others for each input. So in practice
 * the above condition is equivalent to "emit when age(last) > 3 or
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
 *     emit after max others.start > out.start + ($obs_window * 1.15) * 1_000_000
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
type uint8 = Uint8.t
type uint16 = Uint16.t

module P = Parsers.Make (Parsers.SimpleConfig (Char))
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
  | "in" | "others" | "any" -> true
  | _ -> false

(*$inject
  open Stdint
  open Batteries
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

  let test_p p s =
    (p +- eof) [] None Parsers.no_error_correction (stream_of_string s) |>
    to_result

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
    | AggrPercentile (_, a, b) -> AggrPercentile (typ, replace_typ a, replace_typ b)
    | Age (_, a) -> Age (typ, replace_typ a)
    | Not (_, a) -> Not (typ, replace_typ a)
    | Defined (_, a) -> Defined (typ, replace_typ a)
    | Add (_, a, b) -> Add (typ, replace_typ a, replace_typ b)
    | Sub (_, a, b) -> Sub (typ, replace_typ a, replace_typ b)
    | Mul (_, a, b) -> Mul (typ, replace_typ a, replace_typ b)
    | Div (_, a, b) -> Div (typ, replace_typ a, replace_typ b)
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
    | Ok (Aggregate { fields ; and_all_others ; where ; key ; emit_when }, rest) ->
      Ok (Aggregate {
        fields = List.map (fun sf -> { sf with expr = replace_typ sf.expr }) fields ;
        and_all_others ;
        where = replace_typ where ;
        key = List.map replace_typ key ;
        emit_when = replace_typ emit_when }, rest)
    | Ok (OnChange e, rest) -> Ok (OnChange (replace_typ e), rest)
    | x -> x
 *)

module Scalar =
struct
  (*$< Scalar *)

  type typ = TFloat | TString | TBool
           | TU8 | TU16 | TU32 | TU64 | TU128
           | TI8 | TI16 | TI32 | TI64 | TI128 [@@ppp PPP_JSON]

  let print_typ fmt typ =
    let s = match typ with
      | TFloat  -> "FLOAT"
      | TString -> "STRING"
      | TBool   -> "BOOL"
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

    let narrowest_int_scalar i =
      if Int8.(to_int min_int) <= i && i <= Int8.(to_int max_int) then
        VI8 (Int8.of_int i) else
      if Uint8.(to_int min_int) <= i && i <= Uint8.(to_int max_int) then
        VU8 (Uint8.of_int i) else
      if Int16.(to_int min_int) <= i && i <= Int16.(to_int max_int) then
        VI16 (Int16.of_int i) else
      if Uint16.(to_int min_int) <= i && i <= Uint16.(to_int max_int) then
        VU16 (Uint16.of_int i) else
      if Int32.(to_int min_int) <= i && i <= Int32.(to_int max_int) then
        VI32 (Int32.of_int i) else
      if Uint32.(to_int min_int) <= i && i <= Uint32.(to_int max_int) then
        VU32 (Uint32.of_int i) else
      (* FIXME: as integer returns merely an int we won't go that far: *)
      if Int64.(to_int min_int) <= i && i <= Int64.(to_int max_int) then
        VI64 (Int64.of_int i) else
      if Uint64.(to_int min_int) <= i && i <= Uint64.(to_int max_int) then
        VU64 (Uint64.of_int i) else
      if Int128.(to_int min_int) <= i && i <= Int128.(to_int max_int) then
        VI128 (Int128.of_int i) else
      if Uint128.(to_int min_int) <= i && i <= Uint128.(to_int max_int) then
        VU128 (Uint128.of_int i) else
      assert false

    (* TODO: Here and elsewhere, we want the location (start+length) of the
     * thing in addition to the thing *)
    let p =
      (integer >>: narrowest_int_scalar)        |||
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
    strinG "on" ||| strinG "change" ||| strinG "after" ||| strinG "when" |||
    strinG "age" ||| strinG "alert" ||| strinG "subject" ||| strinG "text" |||
    strinG "read" ||| strinG "from" ||| strinG "csv" ||| strinG "file" |||
    strinG "separator" |||
    (Scalar.Parser.typ >>: fun _ -> ())
  ) -- check (nay (letter ||| underscore ||| decimal_digit))
let non_keyword =
  (* TODO: allow keywords if quoted *)
  let open P in
  check (nay keyword) -+ ParseUsual.identifier

module Tuple =
struct
  type field_typ = { name : string ; nullable : bool ; typ : Scalar.typ } [@@ppp PPP_JSON]

  let print_field_typ fmt field =
    (* TODO: check that name is a valid identifier *)
    Printf.fprintf fmt "%s %a %sNULL"
      field.name
      Scalar.print_typ field.typ
      (if field.nullable then "" else "NOT ")

  type typ = field_typ list [@@ppp PPP_JSON]

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
    { name : string ;
      mutable nullable : bool option ;
      mutable typ : Scalar.typ option } [@@ppp PPP_JSON]

  let typ_is_complete typ =
    typ.nullable <> None && typ.typ <> None

  let print_typ fmt typ =
    Printf.fprintf fmt "%s%s of %s"
      (match typ.nullable with
      | None -> "maybe nullable "
      | Some true -> "nullable "
      | Some false -> "")
      typ.name
      (match typ.typ with
      | None -> "unknown type"
      | Some typ -> "type "^ IO.to_string Scalar.print_typ typ)

  let make_typ ?nullable ?typ name = { name ; nullable ; typ }
  let make_bool_typ ?nullable name = make_typ ?nullable ~typ:Scalar.TBool name
  let make_num_typ ?nullable name =
    make_typ ?nullable ~typ:Scalar.TU8 name (* will be enlarged as required *)
  let copy_typ typ = { typ with name = typ.name }

  (* Expressions on scalars (aka fields) *)
  type t =
    | Const of typ * Scalar.t
    | Field of typ * string (* tuple: in, out, others... *) * string (* field name *)
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
    | AggrPercentile of typ * t * t
    (* Other functions *)
    | Age of typ * t
    (* Unary Ops on scalars *)
    | Not of typ * t
    | Defined of typ * t
    (* Binary Ops scalars *)
    | Add of typ * t * t
    | Sub of typ * t * t
    | Mul of typ * t * t
    | Div of typ * t * t
    | Exp of typ * t * t
    | And of typ * t * t
    | Or  of typ * t * t
    | Ge  of typ * t * t
    | Gt  of typ * t * t
    | Eq  of typ * t * t

  let rec print fmt = function
    | Const (_, c) -> Scalar.print fmt c
    | Field (_, tuple, field) -> Printf.fprintf fmt "%s.%s" tuple field
    | Param (_, p) -> Printf.fprintf fmt "$%s" p
    | AggrMin (_, e) -> Printf.fprintf fmt "min (%a)" print e
    | AggrMax (_, e) -> Printf.fprintf fmt "max (%a)" print e
    | AggrSum (_, e) -> Printf.fprintf fmt "sum (%a)" print e
    | AggrAnd (_, e) -> Printf.fprintf fmt "and (%a)" print e
    | AggrOr  (_, e) -> Printf.fprintf fmt "or (%a)" print e
    | AggrPercentile (_, p, e) -> Printf.fprintf fmt "%ath percentile (%a)" print p print e
    | Age (_, e) -> Printf.fprintf fmt "age(%a)" print e
    | Not (_, e) -> Printf.fprintf fmt "NOT (%a)" print e
    | Defined (_, e) -> Printf.fprintf fmt "(%a) IS NOT NULL" print e
    | Add (_, e1, e2) -> Printf.fprintf fmt "(%a) + (%a)" print e1 print e2
    | Sub (_, e1, e2) -> Printf.fprintf fmt "(%a) - (%a)" print e1 print e2
    | Mul (_, e1, e2) -> Printf.fprintf fmt "(%a) * (%a)" print e1 print e2
    | Div (_, e1, e2) -> Printf.fprintf fmt "(%a) / (%a)" print e1 print e2
    | Exp (_, e1, e2) -> Printf.fprintf fmt "(%a) ^ (%a)" print e1 print e2
    | And (_, e1, e2) -> Printf.fprintf fmt "(%a) AND (%a)" print e1 print e2
    | Or (_, e1, e2) -> Printf.fprintf fmt "(%a) OR (%a)" print e1 print e2
    | Ge (_, e1, e2) -> Printf.fprintf fmt "(%a) >= (%a)" print e1 print e2
    | Gt (_, e1, e2) -> Printf.fprintf fmt "(%a) > (%a)" print e1 print e2
    | Eq (_, e1, e2) -> Printf.fprintf fmt "(%a) = (%a)" print e1 print e2

  let typ_of = function
    | Const (t, _) | Field (t, _, _) | Param (t, _) | AggrMin (t, _)
    | AggrMax (t, _) | AggrSum (t, _) | AggrAnd (t, _) | AggrOr  (t, _)
    | AggrPercentile (t, _, _) | Age (t, _) | Not (t, _) | Defined (t, _)
    | Add (t, _, _) | Sub (t, _, _) | Mul (t, _, _) | Div (t, _, _)
    | Exp (t, _, _) | And (t, _, _) | Or (t, _, _) | Ge (t, _, _)
    | Gt (t, _, _) | Eq (t, _, _) -> t

  module Parser =
  struct
    (*$< Parser *)
    open ParseUsual
    open P

    (* Single things *)
    let const =
      Scalar.Parser.p >>: fun c ->
      Const (make_typ ~nullable:false ~typ:(Scalar.type_of c) "constant", c)
    (*$= const & ~printer:(test_printer print)
      (Ok (Const (typ, Scalar.VBool true), (4, [])))\
        (test_p const "true" |> replace_typ_in_expr)
    *)

    let field =
      let prefix s = strinG (s ^ ".") >>: fun () -> s in
      optional ~def:"in" (
        prefix "in" ||| prefix "out" ||| prefix "others" ||| prefix "any") ++
      non_keyword >>: fun (tuple, field) ->
      Field (make_typ (tuple ^"."^ field), tuple, field)
    (*$= field & ~printer:(test_printer print)
      (Ok (\
        Field (typ, "in", "bytes"),\
        (5, [])))\
        (test_p field "bytes" |> replace_typ_in_expr)

      (Ok (\
        Field (typ, "in", "bytes"),\
        (8, [])))\
        (test_p field "in.bytes" |> replace_typ_in_expr)

      (Bad (\
        NoSolution (\
          Some { where = ParsersMisc.Item (7, '.');\
                 what=["eof"]})))\
        (test_p field "pasglop.bytes" |> replace_typ_in_expr)
    *)

    let param =
      char '$' -+ identifier >>: fun s ->
      Param (make_typ ("parameter "^s), s)
    (*$= param & ~printer:(test_printer print)
      (Ok (\
        Param (typ, "glop"),\
        (5, [])))\
        (test_p param "$glop" |> replace_typ_in_expr)

      (Bad (\
        NoSolution (\
          Some { where = ParsersMisc.Item (0, 'g');\
                 what = ["\"$\""] })))\
      (test_p param "glop" |> replace_typ_in_expr)
    *)

    (* operators with lowest precedence *)
    let rec lowest_prec_left_assoc m =
      let op = that_string "and" ||| that_string "or"
      and reduce t1 op t2 = match op with
        | "and" -> And (make_bool_typ "and operator", t1, t2)
        | "or" -> Or (make_bool_typ "or operator", t1, t2)
        | _ -> assert false in
      (* FIXME: we do not need a blanks if we had parentheses ("(x)AND(y)" is OK) *)
      binary_ops_reducer ~op ~term:low_prec_left_assoc ~sep:blanks ~reduce m

    and low_prec_left_assoc m =
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
      let op = that_string "+" ||| that_string "-"
      and reduce t1 op t2 = match op with
        | "+" -> Add (make_num_typ "addition", t1, t2)
        | "-" -> Sub (make_num_typ "subtraction", t1, t2)
        | _ -> assert false in
      binary_ops_reducer ~op ~term:high_prec_left_assoc ~sep:opt_blanks~reduce m

    and high_prec_left_assoc m =
      let op = that_string "*" ||| that_string "/"
      and reduce t1 op t2 = match op with
        | "*" -> Mul (make_num_typ "multiplication", t1, t2)
        | "/" -> Div (make_num_typ "division", t1, t2)
        | _ -> assert false in
      binary_ops_reducer ~op ~term:higher_prec_right_assoc ~sep:opt_blanks~reduce m

    and higher_prec_right_assoc m =
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

    and afun n =
      let sep = check (char '(') ||| blanks in
      strinG n -- optional ~def:() (blanks -- strinG "of") --
      sep -+ highestest_prec

    and aggregate m =
      ((afun "min" >>: fun e -> AggrMin (make_num_typ "min aggregation", e)) |||
       (afun "max" >>: fun e -> AggrMax (make_num_typ "max aggregation", e)) |||
       (afun "sum" >>: fun e -> AggrSum (make_num_typ "sum aggregation", e)) |||
       (afun "and" >>: fun e -> AggrAnd (make_bool_typ "and aggregation", e)) |||
       (afun "or"  >>: fun e -> AggrOr  (make_bool_typ "or aggregation", e)) |||
       ((const ||| param) +- (optional ~def:() (string "th")) +- blanks ++
        afun "percentile" >>: fun (p, e) -> AggrPercentile (make_num_typ "percentile aggregation", p, e))
      ) m

    and func m =
      ((afun "age" >>: fun e -> Age (make_num_typ "age function", e))
      ) m

    and highestest_prec m =
      let sep = optional_greedy ~def:() blanks in
      (const ||| field ||| param ||| func ||| aggregate |||
       char '(' -- sep -+ lowest_prec_left_assoc +- sep +- char ')'
      ) m

    let p = lowest_prec_left_assoc

    (*$= p & ~printer:(test_printer print)
      (Ok (\
        Const (typ, Scalar.VBool true),\
        (4, [])))\
        (test_p p "true" |> replace_typ_in_expr)

      (Ok (\
        Not (typ, Defined (typ, Field (typ, "in", "zone_src"))),\
        (16, [])))\
        (test_p p "zone_src IS NULL" |> replace_typ_in_expr)

      (Ok (\
        Eq (typ, Field (typ, "in", "zone_src"), Param (typ, "z1")),\
        (14, [])))\
        (test_p p "zone_src = $z1" |> replace_typ_in_expr)

      (Ok (\
        And (typ, \
          Or (typ, \
            Not (typ, Defined (typ, Field (typ, "in", "zone_src"))),\
            Eq (typ, Field (typ, "in", "zone_src"), Param (typ, "z1"))),\
          Or (typ, \
            Not (typ, Defined (typ, Field (typ, "in", "zone_dst"))),\
            Eq (typ, Field (typ, "in", "zone_dst"), Param (typ, "z2")))),\
        (77, [])))\
        (test_p p "(zone_src IS NULL or zone_src = $z1) and \\
                   (zone_dst IS NULL or zone_dst = $z2)" |> replace_typ_in_expr)

      (Ok (\
        Div (typ, \
          AggrSum (typ, Field (typ, "in", "bytes")),\
          Param (typ, "avg_window")),\
        (23, [])))\
        (test_p p "(sum bytes)/$avg_window" |> replace_typ_in_expr)

      (Ok (\
        Div (typ, \
          Field (typ, "in", "start"),\
          Mul (typ, \
            Const (typ, Scalar.VI32 1_000_000l),\
            Param (typ, "avg_window"))),\
        (33, [])))\
        (test_p p "start / (1_000_000 * $avg_window)" |> replace_typ_in_expr)

      (Ok (\
        AggrPercentile (typ,\
          Param (typ, "p"),\
          Field (typ, "in", "bytes_per_sec")),\
        (30, [])))\
        (test_p p "$p percentile of bytes_per_sec" |> replace_typ_in_expr)

      (Ok (\
        Gt (typ, \
          AggrMax (typ, \
            Field (typ, "others", "start")),\
          Add (typ, \
            Field (typ, "out", "start"),\
            Mul (typ, \
              Mul (typ, \
                Param (typ, "obs_window"),\
                Const (typ, Scalar.VFloat 1.15)),\
              Const (typ, Scalar.VI32 1_000_000l)))),\
        (63, [])))\
        (test_p p "max others.start > \\
                   out.start + ($obs_window * 1.15) * 1_000_000" |> replace_typ_in_expr)
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
        when tuple = "in" && a = field -> false
      | _ -> true in
    if need_alias then
      Printf.fprintf fmt "%a AS %a"
        Expr.print f.expr
        (List.print ~first:"" ~last:"" ~sep:" OR " String.print) f.alias
    else
      Expr.print fmt f.expr

  type t =
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
        emit_when : Expr.t }
    | OnChange of Expr.t
    | Alert of { team : string ; subject : string ; text : string }
    | ReadCSVFile of { fname : string ; separator : string ; fields : Tuple.typ }

  let print fmt =
    let sep = ", " in
    function
    | Select { fields ; and_all_others ; where } ->
      Printf.fprintf fmt "SELECT %a%s%s WHERE %a"
        (List.print ~first:"" ~last:"" ~sep print_selected_field) fields
        (if fields <> [] && and_all_others then sep else "")
        (if and_all_others then "*" else "")
        Expr.print where
    | Aggregate { fields ; and_all_others ; where ; key ; emit_when } ->
      Printf.fprintf fmt "SELECT %a%s%s WHERE %a GROUP BY %a EMIT WHEN %a"
        (List.print ~first:"" ~last:"" ~sep print_selected_field) fields
        (if fields <> [] && and_all_others then sep else "")
        (if and_all_others then "*" else "")
        Expr.print where
        (List.print ~first:"" ~last:"" ~sep:", " Expr.print) key
        Expr.print emit_when
    | OnChange e ->
      Printf.fprintf fmt "ON CHANGE %a" Expr.print e
    | Alert { team ; subject ; text } ->
      Printf.fprintf fmt "ALERT %S SUBJECT %S TEXT %S" team subject text
    | ReadCSVFile { fname ; separator ; fields } ->
      Printf.fprintf fmt "READ FROM CSV FILE %S SEPARATOR %S %a"
        fname separator Tuple.print_typ fields

  module Parser =
  struct
    (*$< Parser *)
    open ParseUsual
    open P

    let selected_field =
      Expr.Parser.p ++ optional ~def:[] (
        blanks -- strinG "as" -- blanks -+
        repeat ~min:1 ~sep:(blanks -- strinG "or" -- blanks)
               non_keyword) >>:
      fun (expr, alias) ->
        let alias =
          if alias <> [] then alias else (
            match expr with
            | Expr.Field (_, tuple, field) when tuple = "in" -> [ field ]
            | _ -> raise (Reject "must set alias")
          ) in
        { expr ; alias }

    let list_sep = opt_blanks -- char ',' -- opt_blanks

    let select_clause =
      strinG "select" -- blanks -+
      several ~sep:list_sep
             ((char '*' >>: fun _ -> None) |||
              some selected_field)

    let where_clause =
      strinG "where" -- blanks -+ Expr.Parser.p

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

    let emit_when =
      strinG "emit" -- blanks -- (strinG "after" ||| strinG "when") --
      blanks -+ Expr.Parser.p

    let aggregate =
      select +- blanks +- strinG "group" +- blanks +- strinG "by" +- blanks ++
      several ~sep:list_sep Expr.Parser.p +- blanks ++ emit_when >>: function
      | (Select { fields ; and_all_others ; where }, key), emit_when ->
        Aggregate { fields ; and_all_others ; where ; key ; emit_when }
      | _ -> assert false

    let on_change =
      strinG "on" -- blanks -- strinG "change" -- blanks -+ Expr.Parser.p >>:
      fun e -> OnChange e

    let alert =
      strinG "alert" -- blanks -+ quoted_string +- blanks +-
      strinG "subject" +- blanks ++ quoted_string +- blanks +-
      strinG "text" +- blanks ++ quoted_string >>:
      fun ((team, subject), text) -> Alert { team ; subject ; text }

    let read_csv_file =
      let field =
        non_keyword +- blanks ++ Scalar.Parser.typ ++
        optional ~def:true (
          optional ~def:true (blanks -+ (strinG "not" >>: fun () -> false)) +-
          blanks +- strinG "null") >>:
        fun ((name, typ), nullable) -> Tuple.{ name ; typ ; nullable }
      in
      strinG "read" -- blanks -- strinG "from" -- blanks --
      optional ~def:() (strinG "csv" +- blanks) -- strinG "file" -- blanks -+
      quoted_string +- opt_blanks ++
      optional ~def:"," (
        strinG "separator" -- opt_blanks -+ quoted_string +- opt_blanks) +-
      char '(' +- opt_blanks ++
      several ~sep:list_sep field +- opt_blanks +- char ')' >>:
      fun ((fname, separator), fields) -> ReadCSVFile { fname ; separator ; fields }

    let p =
      select ||| aggregate ||| on_change ||| alert ||| read_csv_file

    (*$= p & ~printer:(test_printer print)
      (Ok (\
        Select {\
          fields = [\
            { expr = Expr.(Field (typ, "in", "start")) ;\
              alias = [ "start" ] } ;\
            { expr = Expr.(Field (typ, "in", "stop")) ;\
              alias = [ "stop" ] } ;\
            { expr = Expr.(Field (typ, "in", "itf_clt")) ;\
              alias = [ "itf_src" ] } ;\
            { expr = Expr.(Field (typ, "in", "itf_srv")) ;\
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
              Field (typ, "in", "packets"),\
              Const (typ, Scalar.VI8 (Int8.of_int 0)))) },\
        (17, [])))\
        (test_p p "where packets > 0" |> replace_typ_in_op)

      (Ok (\
        Aggregate {\
          fields = [\
            { expr = Expr.(\
                AggrMin (typ, Field (typ, "in", "start"))) ;\
              alias = [ "start"; "out_start" ] } ;\
            { expr = Expr.(\
                AggrMax (typ, Field (typ, "in", "stop"))) ;\
              alias = [ "max_stop" ] } ;\
            { expr = Expr.(\
                Div (typ,\
                  AggrSum (typ,Field (typ, "in", "packets")),\
                  Param (typ, "avg_window"))) ;\
              alias = [ "packets_per_sec" ] } ] ;\
          and_all_others = false ;\
          where = Expr.Const (typ, Scalar.VBool true) ;\
          key = [ Expr.(\
            Div (typ,\
              Field (typ, "in", "start"),\
              Mul (typ,\
                Const (typ, Scalar.VI32 1_000_000l),\
                Param (typ, "avg_window")))) ] ;\
          emit_when = Expr.(\
            Gt (typ,\
              Add (typ,\
                AggrMax (typ,Field (typ, "any", "start")),\
                Const (typ, Scalar.VI16 (Int16.of_int 3600))),\
              Field (typ, "out", "start"))) },\
          (195, [])))\
          (test_p p "select min start as start or out_start, \\
                            max stop as max_stop, \\
                            (sum packets)/$avg_window as packets_per_sec \\
                     group by start / (1_000_000 * $avg_window) \\
                     emit after out.start < (max any.start) + 3600" |>\
           replace_typ_in_op)

      (Ok (\
        OnChange Expr.(Field (typ, "in", "too_low")),\
        (17, [])))\
        (test_p p "on change too_low" |> replace_typ_in_op)

      (Ok (\
        Alert { team = "network firefighters" ;\
                subject = "Too little traffic from zone $z1 to $z2" ;\
                text = "fatigue..." },\
        (96, [])))\
        (test_p p "alert \"network firefighters\" \\
                         subject \"Too little traffic from zone $z1 to $z2\" \\
                         text \"fatigue...\"")

      (Ok (\
        ReadCSVFile { fname = "/tmp/toto.csv" ; separator = "," ; \
                      fields = Lang.Tuple.[ \
                        { name = "f1" ; nullable = true ; typ = Scalar.TBool } ;\
                        { name = "f2" ; nullable = false ; typ = Scalar.TI32 } ] },\
        (61, [])))\
        (test_p p "read from csv file \"/tmp/toto.csv\" (f1 bool, f2 i32 not null)")

      (Ok (\
        ReadCSVFile { fname = "/tmp/toto.csv" ; separator = "\t" ; \
                      fields = Lang.Tuple.[ \
                        { name = "f1" ; nullable = true ; typ = Scalar.TBool } ;\
                        { name = "f2" ; nullable = false ; typ = Scalar.TI32 } ] },\
        (76, [])))\
        (test_p p "read from csv file \"/tmp/toto.csv\" separator \"\\t\" (f1 bool, f2 i32 not null)")
    *)

    (* Check that the expression is valid, or return an error message.
     * Also perform some optimisation, numeric promotions, etc... *)
    let check op = Ok op (* TODO *)

    (*$>*)
  end
  (*$>*)
end
