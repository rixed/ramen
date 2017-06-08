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
let keyword =
  let open P in
  let open ParseUsual in
  (
    strinG "true" ||| strinG "false" ||| strinG "and" ||| strinG "or" |||
    strinG "min" ||| strinG "max" ||| strinG "sum" ||| strinG "percentile" |||
    strinG "of" ||| strinG "is" ||| strinG "not" ||| strinG "null" |||
    strinG "group" ||| strinG "by" ||| strinG "select" ||| strinG "where" |||
    strinG "on" ||| strinG "change" ||| strinG "after" ||| strinG "when" |||
    strinG "age" ||| strinG "alert" ||| strinG "subject" ||| strinG "text"
  ) -- check (nay (letter ||| underscore ||| decimal_digit))
let non_keyword =
  let open P in
  check (nay keyword) -+ ParseUsual.identifier


(*$inject
  open Stdint
  open Batteries
  open Lang.P

  let test_printer res_printer = function
    | Ok (res, (len, _)) ->
      Printf.sprintf "%S, parsed_len=%d" (IO.to_string res_printer res) len
    | Bad (Approximation _) ->
      "Approximation"
    | Bad (NoSolution e) ->
      Printf.sprintf "No solution (%s)" (IO.to_string print_error e)
    | Bad (Ambiguous lst) ->
      Printf.sprintf "%d solutions" (List.length lst)

  let test_p p s =
    (p +- eof) [] None Parsers.no_error_correction (P.stream_of_string s) |>
    to_result
 *)

module Scalar =
struct
  (*$< Scalar *)

  type typ = TFloat | TString | TBool
           | TU8 | TU16 | TU32 | TU64 | TU128
           | TI8 | TI16 | TI32 | TI64 | TI128
  (* stdint types are implemented as custom blocks, therefore are slower than ints.
   * But we do not care as we merely represents code here, we do not run the operators. *)
  type t = VFloat of float | VString of string | VBool of bool
         | VU8 of Uint8.t | VU16 of Uint16.t | VU32 of Uint32.t | VU64 of Uint64.t | VU128 of Uint128.t
         | VI8 of Int8.t  | VI16 of Int16.t  | VI32 of Int32.t  | VI64 of Int64.t  | VI128 of Int128.t

  let print fmt = function
    | VFloat f  -> Printf.fprintf fmt "%g" f
    | VString s -> Printf.fprintf fmt "%S" s
    | VBool b   -> Printf.fprintf fmt "%b" b
    | VU8 i     -> Printf.fprintf fmt "%su8" (Uint8.to_string i)
    | VU16 i    -> Printf.fprintf fmt "%su16" (Uint16.to_string i)
    | VU32 i    -> Printf.fprintf fmt "%su32" (Uint32.to_string i)
    | VU64 i    -> Printf.fprintf fmt "%su64" (Uint64.to_string i)
    | VU128 i   -> Printf.fprintf fmt "%su128" (Uint128.to_string i)
    | VI8 i     -> Printf.fprintf fmt "%sd8" (Int8.to_string i)
    | VI16 i    -> Printf.fprintf fmt "%sd16" (Int16.to_string i)
    | VI32 i    -> Printf.fprintf fmt "%sd32" (Int32.to_string i)
    | VI64 i    -> Printf.fprintf fmt "%sd64" (Int64.to_string i)
    | VI128 i   -> Printf.fprintf fmt "%sd128" (Int128.to_string i)

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

    (*$>*)
  end
  (*$>*)
end

module Tuple =
struct
  type typ = (string, bool (* nullable *) * Scalar.typ) Hashtbl.t
  type t = (string, Scalar.t) Hashtbl.t
end

module Expr =
struct
  (*$< Expr *)

  (* Expressions on scalars (aka fields) *)
  type t =
    | Const of Scalar.t
    | Field of { tuple : string (* in, out, others... *) ; field : string }
    | Param of string
    (* Valid only within an aggregation operation; but must be here to allow
     * operations on top of the result of an aggregation function, such as: "(1
     * + min field1) / (max field2)". Even within an aggregation, not valid
     * within another AggrFun. *)
    | AggrFun of aggregate
    (* Other functions *)
    | Age of t
    (* Unary Ops on scalars *)
    | Not of t
    | Defined of t
    (* Binary Ops scalars *)
    | Add of t * t
    | Sub of t * t
    | Mul of t * t
    | Div of t * t
    | Exp of t * t
    | And of t * t
    | Or  of t * t
    | Ge  of t * t
    | Gt  of t * t
    | Eq  of t * t
  and aggregate =
    | AggrMin of t
    | AggrMax of t
    | AggrSum of t
    | AggrAnd of t
    | AggrOr  of t
    | AggrPercentile of t * t

  let rec print_aggregate fmt = function
    | AggrMin e -> Printf.fprintf fmt "min (%a)" print e
    | AggrMax e -> Printf.fprintf fmt "max (%a)" print e
    | AggrSum e -> Printf.fprintf fmt "sum (%a)" print e
    | AggrAnd e -> Printf.fprintf fmt "and (%a)" print e
    | AggrOr  e -> Printf.fprintf fmt "or (%a)" print e
    | AggrPercentile (p, e) -> Printf.fprintf fmt "%ath percentile (%a)" print p print e
  and print fmt = function
    | Const c -> Scalar.print fmt c
    | Field { tuple ; field } -> Printf.fprintf fmt "%s.%s" tuple field
    | Param p -> Printf.fprintf fmt "$%s" p
    | AggrFun a -> print_aggregate fmt a
    | Age e -> Printf.fprintf fmt "age(%a)" print e
    | Not e -> Printf.fprintf fmt "NOT (%a)" print e
    | Defined e -> Printf.fprintf fmt "(%a) IS NOT NULL" print e
    | Add (e1, e2) -> Printf.fprintf fmt "(%a) + (%a)" print e1 print e2
    | Sub (e1, e2) -> Printf.fprintf fmt "(%a) - (%a)" print e1 print e2
    | Mul (e1, e2) -> Printf.fprintf fmt "(%a) * (%a)" print e1 print e2
    | Div (e1, e2) -> Printf.fprintf fmt "(%a) / (%a)" print e1 print e2
    | Exp (e1, e2) -> Printf.fprintf fmt "(%a) ^ (%a)" print e1 print e2
    | And (e1, e2) -> Printf.fprintf fmt "(%a) AND (%a)" print e1 print e2
    | Or (e1, e2) -> Printf.fprintf fmt "(%a) OR (%a)" print e1 print e2
    | Ge (e1, e2) -> Printf.fprintf fmt "(%a) >= (%a)" print e1 print e2
    | Gt (e1, e2) -> Printf.fprintf fmt "(%a) > (%a)" print e1 print e2
    | Eq (e1, e2) -> Printf.fprintf fmt "(%a) = (%a)" print e1 print e2

  module Parser =
  struct
    (*$< Parser *)
    open ParseUsual
    open P

    (* Single things *)
    let const = Scalar.Parser.p >>: fun c -> Const c
    (*$= const & ~printer:(test_printer print)
      (Ok (Const (Scalar.VBool true), (4, [])))  (test_p const "true")
    *)

    let field =
      let prefix s = strinG (s ^ ".") >>: fun () -> s in
      optional ~def:"in" (
        prefix "in" ||| prefix "out" ||| prefix "others" ||| prefix "any") ++
      non_keyword >>: fun (tuple, field) -> Field { tuple ; field }
    (*$= field & ~printer:(test_printer print)
      (Ok (\
        Field { tuple="in"; field="bytes" },\
        (5, [])))\
        (test_p field "bytes")

      (Ok (\
        Field { tuple="in"; field="bytes" },\
        (8, [])))\
        (test_p field "in.bytes")

      (Bad (\
        NoSolution (\
          Some { where = ParsersMisc.Item (7, '.');\
                 what=["eof"]})))\
        (test_p field "pasglop.bytes")
    *)

    let param =
      char '$' -+ identifier >>: fun s -> Param s
    (*$= param & ~printer:(test_printer print)
      (Ok (\
        Param "glop",\
        (5, [])))\
        (test_p param "$glop")

      (Bad (\
        NoSolution (\
          Some { where = ParsersMisc.Item (0, 'g');\
                 what = ["\"$\""] })))\
      (test_p param "glop")
    *)

    (* operators with lowest precedence *)
    let rec lowest_prec_left_assoc m =
      let op = that_string "and" ||| that_string "or"
      and reduce t1 op t2 = match op with
        | "and" -> And (t1, t2)
        | "or" -> Or (t1, t2)
        | _ -> assert false in
      (* FIXME: we do not need a blanks if we had parentheses ("(x)AND(y)" is OK) *)
      binary_ops_reducer ~op ~term:low_prec_left_assoc ~sep:blanks ~reduce m

    and low_prec_left_assoc m =
      let op = that_string ">" ||| that_string ">=" ||| that_string "<" ||| that_string "<=" |||
               that_string "=" ||| that_string "<>" ||| that_string "!="
      and reduce t1 op t2 = match op with
        | ">" -> Gt (t1, t2)
        | "<" -> Gt (t2, t1)
        | ">=" -> Ge (t1, t2)
        | "<=" -> Ge (t2, t1)
        | "=" -> Eq (t1, t2)
        | "!=" | "<>" -> Not (Eq (t1, t2))
        | _ -> assert false in
      binary_ops_reducer ~op ~term:mid_prec_left_assoc ~sep:opt_blanks ~reduce m

    and mid_prec_left_assoc m =
      let op = that_string "+" ||| that_string "-"
      and reduce t1 op t2 = match op with
        | "+" -> Add (t1, t2)
        | "-" -> Sub (t1, t2)
        | _ -> assert false in
      binary_ops_reducer ~op ~term:high_prec_left_assoc ~sep:opt_blanks~reduce m

    and high_prec_left_assoc m =
      let op = that_string "*" ||| that_string "/"
      and reduce t1 op t2 = match op with
        | "*" -> Mul (t1, t2)
        | "/" -> Div (t1, t2)
        | _ -> assert false in
      binary_ops_reducer ~op ~term:higher_prec_right_assoc ~sep:opt_blanks~reduce m

    and higher_prec_right_assoc m =
      let op = char '^'
      and reduce t1 _ t2 = Exp (t1, t2) in
      binary_ops_reducer ~op ~right_associative:true
                         ~term:highest_prec_left_assoc ~sep:opt_blanks~reduce m

    and highest_prec_left_assoc m =
      ((strinG "not" -+ highestest_prec >>: fun e -> Not e) |||
       (highestest_prec ++
        optional ~def:None (
          blanks -- strinG "is" -- blanks -+
          optional ~def:(Some false)
                   (strinG "not" -- blanks >>: fun () -> Some true) +-
          strinG "null") >>: function
            | e, None -> e
            | e, Some false -> Not (Defined e)
            | e, Some true -> Defined e)
      ) m

    and afun n =
      let sep = check (char '(') ||| blanks in
      strinG n -- optional ~def:() (blanks -- strinG "of") --
      sep -+ highestest_prec

    and aggregate m =
      ((afun "min" >>: fun e -> AggrMin e) |||
       (afun "max" >>: fun e -> AggrMax e) |||
       (afun "sum" >>: fun e -> AggrSum e) |||
       (afun "and" >>: fun e -> AggrAnd e) |||
       (afun "or"  >>: fun e -> AggrOr  e) |||
       ((const ||| param) +- (optional ~def:() (string "th")) +- blanks ++
        afun "percentile" >>: fun (p, e) -> AggrPercentile (p, e))
      ) m

    and func m =
      ((afun "age" >>: fun e -> Age e)
      ) m

    and highestest_prec m =
      let sep = optional_greedy ~def:() blanks in
      (const ||| field ||| param ||| func |||
       (aggregate >>: fun a -> AggrFun a) |||
       char '(' -- sep -+ lowest_prec_left_assoc +- sep +- char ')'
      ) m

    let p = lowest_prec_left_assoc

    (*$= p & ~printer:(test_printer print)
      (Ok (\
        Const (Scalar.VBool true),\
        (4, [])))\
        (test_p p "true")

      (Ok (\
        Not (Defined (Field {tuple="in";field="zone_src"})),\
        (16, [])))\
        (test_p p "zone_src IS NULL")

      (Ok (\
        Eq (Field {tuple="in";field="zone_src"}, Param "z1"),\
        (14, [])))\
        (test_p p "zone_src = $z1")

      (Ok (\
        And (\
          Or (\
            Not (Defined (Field {tuple="in";field="zone_src"})),\
            Eq (Field {tuple="in";field="zone_src"}, Param "z1")),\
          Or (\
            Not (Defined (Field {tuple="in";field="zone_dst"})),\
            Eq (Field {tuple="in";field="zone_dst"}, Param "z2"))),\
        (77, [])))\
        (test_p p "(zone_src IS NULL or zone_src = $z1) AND \\
                   (zone_dst IS NULL or zone_dst = $z2)")

      (Ok (\
        Div (\
          AggrFun (AggrSum (Field {tuple="in"; field="bytes"})),\
          Param "avg_window"),\
        (23, [])))\
        (test_p p "(sum bytes)/$avg_window")

      (Ok (\
        Div (\
          Field {tuple="in";field="start"},\
          Mul (\
            Const (Scalar.VI32 1_000_000l),\
            Param "avg_window")),\
        (33, [])))\
        (test_p p "start / (1_000_000 * $avg_window)")

      (Ok (\
        AggrFun (AggrPercentile (\
          Param "p",\
          Field { tuple="in"; field="bytes_per_sec" })),\
        (30, [])))\
        (test_p p "$p percentile of bytes_per_sec")

      (Ok (\
        Gt (\
          AggrFun (AggrMax (\
            Field { tuple="others"; field="start" })),\
          Add (\
            Field { tuple="out"; field="start" },\
            Mul (\
              Mul (\
                Param "obs_window",\
                Const (Scalar.VFloat 1.15)),\
              Const (Scalar.VI32 1_000_000l)))),\
        (63, [])))\
        (test_p p "max others.start > \\
                   out.start + ($obs_window * 1.15) * 1_000_000")
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

  let selected_field_print fmt f =
    Printf.fprintf fmt "%a%s%a"
      Expr.print f.expr
      (if f.alias <> [] then " AS " else "")
      (List.print ~first:"" ~last:"" ~sep:" OR " String.print) f.alias

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
        key : Expr.t ;
        emit_when : Expr.t }
    | OnChange of Expr.t
    | Alert of { team : string ; subject : string ; text : string }

  let print fmt =
    let sep = ", " in
    function
    | Select { fields ; and_all_others ; where } ->
      Printf.fprintf fmt "SELECT %a%s%s WHERE %a"
        (List.print ~first:"" ~last:"" ~sep selected_field_print) fields
        (if fields <> [] && and_all_others then sep else "")
        (if and_all_others then "*" else "")
        Expr.print where
    | Aggregate { fields ; and_all_others ; where ; key ; emit_when } ->
      Printf.fprintf fmt "SELECT %a%s%s WHERE %a GROUP BY %a EMIT WHEN %a"
        (List.print ~first:"" ~last:"" ~sep selected_field_print) fields
        (if fields <> [] && and_all_others then sep else "")
        (if and_all_others then "*" else "")
        Expr.print where
        Expr.print key
        Expr.print emit_when
    | OnChange e ->
      Printf.fprintf fmt "ON CHANGE %a" Expr.print e
    | Alert { team ; subject ; text } ->
      Printf.fprintf fmt "ALERT %S SUBJECT %S TEXT %S" team subject text

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
      fun (expr, alias) -> { expr ; alias }

    let select_clause =
      strinG "select" -- blanks -+
      repeat ~min:1 ~sep:(opt_blanks -- char ',' -- opt_blanks)
             ((char '*' >>: fun _ -> None) |||
              some selected_field)

    let where_clause =
      strinG "where" -- blanks -+ Expr.Parser.p

    let select =
      (select_clause +- blanks ++ where_clause |||
       select_clause ++ return (Expr.Const (Scalar.VBool true)) |||
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
      Expr.Parser.p +- blanks ++ emit_when >>: function
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

    let p =
      select ||| aggregate ||| on_change ||| alert

    (*$= p & ~printer:(test_printer print)
      (Ok (\
        Select {\
          fields = [\
            { expr = Expr.(Field { tuple = "in" ; field = "start" }) ;\
              alias = [] } ;\
            { expr = Expr.(Field { tuple = "in" ; field = "stop" }) ;\
              alias = [] } ;\
            { expr = Expr.(Field { tuple = "in" ; field = "itf_clt" }) ;\
              alias = [ "itf_src" ] } ;\
            { expr = Expr.(Field { tuple = "in" ; field = "itf_srv" }) ;\
              alias = [ "itf_dst" ] } ] ;\
          and_all_others = false ;\
          where = Expr.Const (Scalar.VBool true) },\
        (58, [])))\
        (test_p p "select start, stop, itf_clt as itf_src, itf_srv as itf_dst")

      (Ok (\
        Select {\
          fields = [] ;\
          and_all_others = true ;\
          where = Expr.(\
            Gt (\
              Field { tuple="in" ; field="packets" },\
              Const (Scalar.VI8 (Int8.of_int 0)))) },\
        (17, [])))\
        (test_p p "where packets > 0")

      (Ok (\
        Aggregate {\
          fields = [\
            { expr = Expr.(AggrFun (\
                AggrMin (Field { tuple = "in" ; field = "start" }))) ;\
              alias = [ "start"; "out_start" ] } ;\
            { expr = Expr.(AggrFun (\
                AggrMax (Field { tuple = "in" ; field = "stop" }))) ;\
              alias = [] } ;\
            { expr = Expr.(\
                Div (\
                  AggrFun (\
                    AggrSum (Field { tuple = "in" ; field = "packets" })),\
                  Param "avg_window")) ;\
              alias = [ "packets_per_sec" ] } ] ;\
          and_all_others = false ;\
          where = Expr.Const (Scalar.VBool true) ;\
          key = Expr.(\
            Div (\
              Field { tuple = "in" ; field = "start" },\
              Mul (\
                Const (Scalar.VI32 1_000_000l),\
                Param "avg_window"))) ;\
          emit_when = Expr.(\
            Gt (\
              Add (\
                AggrFun (\
                  AggrMax (Field { tuple = "any" ; field = "start" })),\
                Const (Scalar.VI16 (Int16.of_int 3600))),\
              Field { tuple = "out" ; field = "start" })) },\
          (183, [])))\
          (test_p p "select min start as start or out_start, max stop, \\
                            (sum packets)/$avg_window as packets_per_sec \\
                     group by start / (1_000_000 * $avg_window) \\
                     emit after out.start < (max any.start) + 3600")

      (Ok (\
        OnChange Expr.(Field { tuple = "in" ; field = "too_low" }),\
        (17, [])))\
        (test_p p "on change too_low")

      (Ok (\
        Alert { team = "network firefighters" ;\
                subject = "Too little traffic from zone $z1 to $z2" ;\
                text = "fatigue..." },\
        (96, [])))\
        (test_p p "alert \"network firefighters\" \\
                         subject \"Too little traffic from zone $z1 to $z2\" \\
                         text \"fatigue...\"")
    *)

    (*$>*)
  end
  (*$>*)
end

module Graph =
struct
  type node = { operation : Operation.t ;
                parents : node list ;
                children : node list }
  type t = { mutable nodes : node list }
end
