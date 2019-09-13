(* This module deals with expressions.
 * Expressions are the flesh of ramen programs.
 * Every expression is typed.
 *
 * Immediate values are parsed in RamenTypes.
 *)
open Batteries
open Stdint
open RamenLang
open RamenHelpers
open RamenLog
module T = RamenTypes
module N = RamenName

(*$inject
  open TestHelpers
  open RamenLang
  open Stdint
*)

(* Stateful function can have either a unique global state a one state per
 * aggregation group (local). Each function has its own default (functions
 * that tends to be used mostly for aggregation have a local default state,
 * while others have a global state), but you can select explicitly using
 * the "locally" and "globally" keywords. For instance: "sum globally 1". *)
type state_lifespan = LocalState | GlobalState
  [@@ppp PPP_OCaml]

type skip_nulls = bool
  [@@ppp PPP_OCaml]

(* Each expression come with a type and a uniq identifier attached (to build
 * var names, record field names or identify SAT variables).
 * Starting at Any, types are set during compilation. *)
type t =
  { text : text ;
    uniq_num : int ;
    mutable typ : T.t ;
    (* TODO: Units might be better in T.t *)
    mutable units : RamenUnits.t option }
  [@@ppp PPP_OCaml]

and text =
  (* TODO: Those should go into Stateless0: *)
  (* Immediate value: *)
  | Const of T.value
  (* A tuple of expression (not to be confounded with an immediate tuple).
   * (1; "two"; 3.0) is a T.VTup (an immediate constant of type
   * T.TTup...) whereas (3-2; "t"||"wo"; sqrt(9)) is an expression
   * (Tuple of...). *)
  | Tuple of t list
  (* Literal records where fields are constant but values can be any other
   * expression. Note that the same field name can appear several time in the
   * definition but only the last occurrence will be present in the final
   * value (handy for refining the value of some field).
   * The bool indicates the presence of a STAR selector, which is always
   * cleared after a program is parsed. *)
  | Record of (N.field * t) list
  (* The same distinction applies to vectors.
   * Notice there are no list expressions though, for the same reason that
   * there is no such thing as a list immediate, but only vectors. Lists, ie
   * vectors which dimensions are variable, appear only at typing. *)
  | Vector of t list
  (* Variables refer to a value from the input or output of the function.
   *
   * This is unrelated to elision of the get in the syntax: when one write
   * for instance "counter" instead of "in.counter" (or "get("counter", in)")
   * then it is first parsed as a field from unknown tuple, before being
   * grounded to the actual tuple or rejected. Later we will instead parse
   * this as a Get from the unknown tuple variable. *)
  | Variable of tuple_prefix
  (* Bindings are met only late in the game in the code generator. They are
   * used at code generation time to pass around an OCaml identifier as an
   * expression. *)
  | Binding of binding_key
  (* A conditional with all conditions and consequents, and finally an optional
   * "else" clause. *)
  | Case of case_alternative list * t option
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
   * skip_nulls is a flag (default: true to ressemble SQL) controlling whether
   * the operator should not update its state on NULL values. This is valid
   * regardless of the nullability of that parameters (or we would need a None
   * default).  This does not change the nullability of the result of the
   * operator (so has no effect on typing), as even when NULLs are skipped the
   * result can still be NULL, when all inputs were NULLs. And if no input are
   * nullable, then skip null does nothing
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
   * which requires initializing in depth first order as well.  *)
  | Stateless of stateless
  | Stateful of (state_lifespan * skip_nulls * stateful)
  | Generator of generator
  [@@ppp PPP_OCaml]

and stateless =
  | SL0 of stateless0
  | SL1 of stateless1 * t
  | SL1s of stateless1s * t list
  | SL2 of stateless2 * t * t
  | SL3 of stateless3 * t * t * t
  [@@ppp PPP_OCaml]

and stateless0 =
  | Now
  | Random
  | EventStart
  | EventStop
  (* Reach a sub-element from [t], directly, with no intermediary Gets.
   * [t] can be a Variable, a literal Record, Vector or Tuple, or another
   * Path.
   * No Path is created at parse time. Only when compiling the function
   * does the RamenFieldMaskLib.subst_deep_fields turn all input paths
   * of that function (chains of Get expressions) into Path expressions ;
   * only to be converted into a Binding in the environment once the
   * typing is done. *)
  | Path of path_comp list
  [@@ppp PPP_OCaml]

and stateless1 =
  (* TODO: Other functions: date_part... *)
  | Age
  (* Cast (if possible) a value into some other of type t. For instance,
   * strings are parsed as numbers, or numbers printed into strings: *)
  | Cast of T.t
  (* Read some bytes into an integer: *)
  | Peek of T.t * endianness
  (* String functions *)
  | Length (* Also for lists *)
  | Lower
  | Upper
  (* Unary Ops on scalars *)
  | Not
  | Abs
  | Minus
  | Defined
  | Exp
  | Log
  | Log10
  | Sqrt
  | Ceil
  | Floor
  | Round
  | Hash
  (* Give the bounds of a CIDR: *)
  | BeginOfRange
  | EndOfRange
  | Sparkline
  | Strptime
  (* Return the name of the variant we are in, or NULL: *)
  | Variant
  (* a LIKE operator using globs, infix *)
  | Like of string (* pattern (using %, _ and \) *)
  [@@ppp PPP_OCaml]

and endianness = LittleEndian | BigEndian
  [@@ppp PPP_OCaml]

and stateless1s =
  (* Min/Max of the given values. Not like AggrMin/AggrMax, which are
   * aggregate functions! The parser distinguish the cases due to the
   * number of arguments: just 1 and that's the aggregate function, more
   * and that's the min/max of the given arguments. *)
  (* FIXME: those two are useless now that any aggregate function can be
   * used on lists: *)
  | Max
  | Min
  (* For debug: prints all its arguments, and output its first. *)
  | Print
  (* A coalesce expression as a list of expression: *)
  | Coalesce
  [@@ppp PPP_OCaml]

and stateless2 =
  (* Binary Ops scalars *)
  | Add
  | Sub
  | Mul
  | Div
  | IDiv
  | Mod
  | Pow
  (* truncate a float to a multiple of the given interval: *)
  | Trunc
  (* Compare a and b by computing:
   *   min(abs(a-b), max(a, b)) / max(abs(a-b), max(a, b))
   * Returns 0 when a = b. *)
  | Reldiff
  | And
  | Or
  | Ge
  | Gt
  | Eq
  | Concat
  | StartsWith
  | EndsWith
  | BitAnd
  | BitOr
  | BitXor
  (* Negative does shift right. Will be signed for signed integers: *)
  | BitShift
  | Get
  (* For network address range test membership, or for an efficient constant
   * set membership test, or for a non-efficient sequence of OR kind of
   * membership test if the set is not constant: *)
  | In
  (* Takes format then time: *)
  | Strftime
  (* Takes a list/vector of expressions and a vector of desired percentiles *)
  | Percentile
  [@@ppp PPP_OCaml]

and stateless3 =
  | SubString
  | DontBeLonely
  [@@ppp PPP_OCaml]

and stateful =
  | SF1 of stateful1 * t
  | SF1s of stateful1s * t list
  | SF2 of stateful2 * t * t
  | SF3 of stateful3 * t * t * t
  | SF3s of stateful3s * t * t * t list
  | SF4s of stateful4s * t * t * t * t list
  (* Top-k operation *)
  | Top of { want_rank : bool ; c : t ; max_size : t option ; what : t list ;
             by : t ; time : t ; duration : t }
  (* like `Latest` but based on time rather than number of entries, and with
   * integrated sampling: *)
  | Past of { what : t ; time : t ; max_age : t ; sample_size : t option }
  [@@ppp PPP_OCaml]

and stateful1 =
  (* TODO: Add stddev... *)
  | AggrMin
  | AggrMax
  | AggrSum
  | AggrAvg
  | AggrAnd
  | AggrOr
  (* Returns the first/last value in the aggregation: *)
  | AggrFirst
  | AggrLast (* FIXME: Should be stateless *)
  (* FIXME: those float should be expressions so we could use params *)
  | AggrHistogram of float * float * int
  (* Build a list with all values from the group *)
  | Group
  (* If its argument is a boolean, count how many are true; Otherwise, merely
   * count how many are present like `sum 1` would do. *)
  | Count
  [@@ppp PPP_OCaml]

and stateful1s =
  (* Accurate version of Remember, that remembers all instances of the given
   * tuple and returns a boolean. Only for when number of expected values
   * is small, obviously: *)
  | Distinct
  (* FIXME: PPP does not support single constructors without parameters: *)
  | AccompanyMe
  [@@ppp PPP_OCaml]

and stateful2 =
  (* value retarded by k steps. If we have had less than k past values
   * then return NULL. *)
  | Lag
  (* Simple exponential smoothing *)
  | ExpSmooth (* coef between 0 and 1 and expression *)
  (* Sample(n, e) -> Keep max n values of e and return them as a list. *)
  | Sample
  [@@ppp PPP_OCaml]

and stateful3 =
  (* If the current time is t, the seasonal, moving average of period p on k
   * seasons is the average of v(t-p), v(t-2p), ... v(t-kp). Note the absence
   * of v(t).  This is because we want to compare v(t) with this season
   * average.  Notice that lag is a special case of season average with p=k
   * and k=1, but with a universal type for the data (while season-avg works
   * only on numbers).  For instance, a moving average of order 5 would be
   * period=1, count=5.
   * When we have not enough history then the result will be NULL. *)
  | MovingAvg (* period, how many seasons to keep, expression *)
  (* Simple linear regression *)
  | LinReg (* as above: period, how many seasons to keep, expression *)
  (* Hysteresis *)
  | Hysteresis (* measured value, acceptable, maximum *)
  [@@ppp PPP_OCaml]

and stateful3s =
  (* GREATEST N e1 [BY e2, e3...] - or by arrival.
   * or `LATEST N e1` without `BY` clause, equivalent to (but faster than?)
   * `GREATEST e1 BY SUM GLOBALLY 1`
   * Also `SMALLEST`, with inverted comparison function.
   * Note: BY followed by more than one expression will require to parentheses
   * the whole expression to avoid ambiguous parsing. *)
  | Largest of
      { inv : bool (* inverted order if true *) ;
        up_to : bool (* shorter result list if less entries are available *) }
  [@@ppp PPP_OCaml]

and stateful4s =
  (* TODO: in (most) functions below it should be doable to replace the
   * variadic lists of expressions by a single expression that's a tuple. *)
  (* Multiple linear regression - and our first variadic function.
   * Parameters:
   * - p: length of the period for seasonal data, in buckets;
   * - n: number of time steps per bucket;
   * - e: the expression to evaluate;
   * - es: the predictors (variadic). *)
  | MultiLinReg
  (* Rotating bloom filters. First parameter is the false positive rate we
   * aim at, second is an expression providing the "time", third a
   * "duration", and finally expressions whose values to remember. The function
   * will return true if it *thinks* this combination of values has been seen
   * the at a time not older than the given duration. This is based on
   * bloom-filters so there can be false positives but not false negatives.
   * Note: If possible, it might save a lot of space to aim for a high false
   * positive rate and account for it in the surrounding calculations than to
   * aim for a low false positive rate. *)
  | Remember
  [@@ppp PPP_OCaml]

and generator =
  (* First function returning more than once (Generator). Here the typ is
   * type of a single value but the function is a generator and can return
   * from 0 to N such values. *)
  | Split of t * t
  [@@ppp PPP_OCaml]

and case_alternative =
  { case_cond : t (* Must be bool *) ;
    case_cons : t (* All alternatives must share a type *) }
  [@@ppp PPP_OCaml]

and binding_key =
  (* Placeholder for the variable holding the state of this expression;
   * Name of the actual variable to be found in the environment: *)
  | State of int
  (* Placeholder for the variable holding the value of that field; Again,
   * name of the actual variable to be found in the environment: *)
  | RecordField of tuple_prefix * N.field
  (* Placeholder for the variable holding the value of the whole IO value;
   * Name of the actual variable to be found in the environment: *)
  | RecordValue of tuple_prefix
  (* Placeholder for any variable that will be in scope when the Binding
   * is evaluated; Can be emitted as-is, no need for looking up the
   * environment: *)
  | Direct of string
  [@@ppp PPP_OCaml]

and path_comp = Int of int | Name of N.field
  [@@ppp PPP_OCaml]

let print_binding_key oc = function
  | State n ->
      Printf.fprintf oc "State of %d" n
  | RecordField (pref, field) ->
      Printf.fprintf oc "%s.%a"
        (string_of_prefix pref)
        N.field_print field
  | RecordValue pref ->
      String.print oc (string_of_prefix pref)
  | Direct s ->
      Printf.fprintf oc "Direct %S" s

let print_path_comp oc = function
  | Int i -> Printf.fprintf oc "[%d]" i
  | Name n -> N.field_print oc n

let print_path oc =
  List.print ~first:"" ~last:"" ~sep:"." print_path_comp oc

let id_of_path p =
  List.fold_left (fun id p ->
    id ^(
      match p with
      | Int i -> "["^ string_of_int i ^"]"
      | Name s -> (if id = "" then "" else ".")^ (s :> string))
  ) "" p |>
  N.field

let uniq_num_seq = ref 0

let make ?(structure=T.TAny) ?nullable ?units text =
  incr uniq_num_seq ;
  { text ; uniq_num = !uniq_num_seq ;
    typ = T.make ?nullable structure ;
    units }

(* Constant expressions must be typed independently and therefore have
 * a distinct uniq_num for each occurrence: *)
let null () =
  make (Const T.VNull)

let of_bool b =
  make ~structure:T.TBool ~nullable:false (Const (T.VBool b))

let of_u8 ?units n =
  make ~structure:T.TU8 ~nullable:false ?units
    (Const (T.VU8 (Uint8.of_int n)))

let of_float ?units n =
  make ~structure:T.TFloat ~nullable:false ?units (Const (T.VFloat n))

let of_string s =
  make ~structure:T.TString ~nullable:false (Const (VString s))

let zero () = of_u8 0
let one () = of_u8 1
let one_hour () = of_float ~units:RamenUnits.seconds 3600.

let string_of_const e =
  match e.text with
  | Const (VString s) -> Some s
  | _ -> None

let float_of_const e =
  match e.text with
  | Const v ->
      (* float_of_scalar and int_of_scalar returns an option because they
       * accept nullable numeric values; they fail on non-numerics, while
       * we want to merely return None here: *)
      (try T.float_of_scalar v
      with Invalid_argument _ -> None)
  | _ -> None

let int_of_const e =
  match e.text with
  | Const v ->
      (try T.int_of_scalar v
      with Invalid_argument _ -> None)
  | _ -> None

(* Return the set of all unique fields in the record expression, preserving
 * the order: *)
let fields_of_record kvs =
  List.enum kvs /@ fst |>
  remove_dups N.compare |>
  Array.of_enum

let rec print ?(max_depth=max_int) with_types oc e =
  if max_depth <= 0 then
    Printf.fprintf oc "..."
  else (
    print_text ~max_depth with_types oc e.text ;
    Option.may (RamenUnits.print oc) e.units ;
    if with_types then Printf.fprintf oc " [#%d, %a]" e.uniq_num T.print_typ e.typ)

and print_text ?(max_depth=max_int) with_types oc text =
  let st g n =
    (* TODO: do not display default *)
    (match g with LocalState -> " LOCALLY" | GlobalState -> " GLOBALLY") ^
    (if n then " skip nulls" else " keep nulls")
  and print_args =
    List.print ~first:"(" ~last:")" ~sep:", " (print with_types) in
  let p oc = print ~max_depth:(max_depth-1) with_types oc in
  (match text with
  | Const c ->
      T.print oc c
  | Tuple es ->
      List.print ~first:"(" ~last:")" ~sep:"; " p oc es
  | Record kvs ->
      Char.print oc '(' ;
      List.print ~first:"" ~last:"" ~sep:", "
        (fun oc (k, e) ->
          Printf.fprintf oc "%a AZ %s"
            p e
            (ramen_quote k))
        oc (kvs :> (string * t) list);
      Char.print oc ')'
  | Vector es ->
      List.print ~first:"[" ~last:"]" ~sep:"; " p oc es
  | Stateless (SL0 (Path path)) ->
      Printf.fprintf oc "in.%a" print_path path
  | Variable pref ->
      Printf.fprintf oc "%s" (string_of_prefix pref)
  | Binding k ->
      Printf.fprintf oc "<BINDING FOR %a>"
        print_binding_key k
  | Case (alts, else_) ->
      let print_alt oc alt =
        Printf.fprintf oc "WHEN %a THEN %a"
          p alt.case_cond
          p alt.case_cons
      in
      Printf.fprintf oc "CASE %a "
       (List.print ~first:"" ~last:"" ~sep:" " print_alt) alts ;
      Option.may (fun else_ ->
        Printf.fprintf oc "ELSE %a "
          p else_) else_ ;
      Printf.fprintf oc "END"
  | Stateless (SL1s (Coalesce, es)) ->
      Printf.fprintf oc "COALESCE %a" print_args es
  | Stateless (SL1 (Age, e)) ->
      Printf.fprintf oc "AGE (%a)" p e
  | Stateless (SL0 Now) ->
      Printf.fprintf oc "NOW"
  | Stateless (SL0 Random) ->
      Printf.fprintf oc "RANDOM"
  | Stateless (SL0 EventStart) ->
      Printf.fprintf oc "#start"
  | Stateless (SL0 EventStop) ->
      Printf.fprintf oc "#stop"
  | Stateless (SL1 (Cast typ, e)) ->
      Printf.fprintf oc "%a(%a)"
        p e T.print_typ typ
  | Stateless (SL1 (Peek (typ, endianness), e)) ->
      Printf.fprintf oc "PEEK %a %a %a"
        T.print_typ typ
        print_endianness endianness
        p e
  | Stateless (SL1 (Length, e)) ->
      Printf.fprintf oc "LENGTH(%a)" p e
  | Stateless (SL1 (Lower, e)) ->
      Printf.fprintf oc "LOWER(%a)" p e
  | Stateless (SL1 (Upper, e)) ->
      Printf.fprintf oc "UPPER(%a)" p e
  | Stateless (SL1 (Not, e)) ->
      Printf.fprintf oc "NOT(%a)" p e
  | Stateless (SL1 (Abs, e)) ->
      Printf.fprintf oc "ABS(%a)" p e
  | Stateless (SL1 (Minus, e)) ->
      Printf.fprintf oc "-(%a)" p e
  | Stateless (SL1 (Defined, e)) ->
      Printf.fprintf oc "(%a) IS NOT NULL" p e
  | Stateless (SL2 (Add, e1, e2)) ->
      Printf.fprintf oc "(%a) + (%a)" p e1 p e2
  | Stateless (SL2 (Sub, e1, e2)) ->
      Printf.fprintf oc "(%a) - (%a)" p e1 p e2
  | Stateless (SL2 (Mul, e1, e2)) ->
      Printf.fprintf oc "(%a) * (%a)" p e1 p e2
  | Stateless (SL2 (Div, e1, e2)) ->
      Printf.fprintf oc "(%a) / (%a)" p e1 p e2
  | Stateless (SL2 (Reldiff, e1, e2)) ->
      Printf.fprintf oc "RELDIFF((%a), (%a))" p e1 p e2
  | Stateless (SL2 (IDiv, e1, e2)) ->
      Printf.fprintf oc "(%a) // (%a)" p e1 p e2
  | Stateless (SL2 (Mod, e1, e2)) ->
      Printf.fprintf oc "(%a) %% (%a)" p e1 p e2
  | Stateless (SL2 (Pow, e1, e2)) ->
      Printf.fprintf oc "(%a) ^ (%a)" p e1 p e2
  | Stateless (SL3 (SubString, e1, e2, e3)) ->
      Printf.fprintf oc "SUBSTRING (%a, %a, %a)" p e1 p e2 p e3
  | Stateless (SL1 (Exp, e)) ->
      Printf.fprintf oc "EXP (%a)" p e
  | Stateless (SL1 (Log, e)) ->
      Printf.fprintf oc "LOG (%a)" p e
  | Stateless (SL1 (Log10, e)) ->
      Printf.fprintf oc "LOG10 (%a)" p e
  | Stateless (SL1 (Sqrt, e)) ->
      Printf.fprintf oc "SQRT (%a)" p e
  | Stateless (SL1 (Ceil, e)) ->
      Printf.fprintf oc "CEIL (%a)" p e
  | Stateless (SL1 (Floor, e)) ->
      Printf.fprintf oc "FLOOR (%a)" p e
  | Stateless (SL1 (Round, e)) ->
      Printf.fprintf oc "ROUND (%a)" p e
  | Stateless (SL1 (Hash, e)) ->
      Printf.fprintf oc "HASH (%a)" p e
  | Stateless (SL1 (Sparkline, e)) ->
      Printf.fprintf oc "SPARKLINE (%a)" p e
  | Stateless (SL2 (Trunc, e1, e2)) ->
      Printf.fprintf oc "TRUNCATE (%a, %a)" p e1 p e2
  | Stateless (SL2 (In, e1, e2)) ->
      Printf.fprintf oc "(%a) IN (%a)" p e1 p e2
  | Stateless (SL1 ((BeginOfRange|EndOfRange as op), e)) ->
      Printf.fprintf oc "%s of (%a)"
        (if op = BeginOfRange then "BEGIN" else "END")
        p e ;
  | Stateless (SL1 (Strptime, e)) ->
      Printf.fprintf oc "PARSE_TIME (%a)" p e
  | Stateless (SL1 (Variant, e)) ->
      Printf.fprintf oc "VARIANT (%a)" p e
  | Stateless (SL2 (And, e1, e2)) ->
      Printf.fprintf oc "(%a) AND (%a)" p e1 p e2
  | Stateless (SL2 (Or, e1, e2)) ->
      Printf.fprintf oc "(%a) OR (%a)" p e1 p e2
  | Stateless (SL2 (Ge, e1, e2)) ->
      Printf.fprintf oc "(%a) >= (%a)" p e1 p e2
  | Stateless (SL2 (Gt, e1, e2)) ->
      Printf.fprintf oc "(%a) > (%a)" p e1 p e2
  | Stateless (SL2 (Eq, e1, e2)) ->
      Printf.fprintf oc "(%a) = (%a)" p e1 p e2
  | Stateless (SL2 (Concat, e1, e2)) ->
      Printf.fprintf oc "(%a) || (%a)" p e1 p e2
  | Stateless (SL2 (StartsWith, e1, e2)) ->
      Printf.fprintf oc "(%a) STARTS WITH (%a)" p e1 p e2
  | Stateless (SL2 (EndsWith, e1, e2)) ->
      Printf.fprintf oc "(%a) ENDS WITH (%a)" p e1 p e2
  | Stateless (SL2 (Strftime, e1, e2)) ->
      Printf.fprintf oc "FORMAT_TIME (%a, %a)" p e1 p e2
  | Stateless (SL2 (BitAnd, e1, e2)) ->
      Printf.fprintf oc "(%a) & (%a)" p e1 p e2
  | Stateless (SL2 (BitOr, e1, e2)) ->
      Printf.fprintf oc "(%a) | (%a)" p e1 p e2
  | Stateless (SL2 (BitXor, e1, e2)) ->
      Printf.fprintf oc "(%a) ^ (%a)" p e1 p e2
  | Stateless (SL2 (BitShift, e1, e2)) ->
      Printf.fprintf oc "(%a) << (%a)" p e1 p e2
  | Stateless (SL2 (Get, { text = Const (VString n) ; _ },
                         { text = Variable pref ; _ }))
    when not with_types ->
      Printf.fprintf oc "%s.%s" (string_of_prefix pref) n
  | Stateless (SL2 (Get, e1, e2)) ->
      Printf.fprintf oc "GET(%a, %a)" p e1 p e2
  | Stateless (SL2 (Percentile, e1, e2)) ->
      Printf.fprintf oc "%a PERCENTILE(%a)" p e2 p e1
  | Stateless (SL1 (Like pat, e)) ->
      Printf.fprintf oc "(%a) LIKE %S" p e pat
  | Stateless (SL1s (Max, es)) ->
      Printf.fprintf oc "GREATEST %a" print_args es
  | Stateless (SL1s (Min, es)) ->
      Printf.fprintf oc "LEAST %a" print_args es
  | Stateless (SL1s (Print, es)) ->
      Printf.fprintf oc "PRINT %a" print_args es
  | Stateful (g, n, SF1 (AggrMin, e)) ->
      Printf.fprintf oc "MIN%s(%a)" (st g n) p e
  | Stateful (g, n, SF1 (AggrMax, e)) ->
      Printf.fprintf oc "MAX%s(%a)" (st g n) p e
  | Stateful (g, n, SF1 (AggrSum, e)) ->
      Printf.fprintf oc "SUM%s(%a)" (st g n) p e
  | Stateful (g, n, SF1 (AggrAvg, e)) ->
      Printf.fprintf oc "AVG%s(%a)" (st g n) p e
  | Stateful (g, n, SF1 (AggrAnd, e)) ->
      Printf.fprintf oc "AND%s(%a)" (st g n) p e
  | Stateful (g, n, SF1 (AggrOr, e)) ->
      Printf.fprintf oc "OR%s(%a)" (st g n) p e
  | Stateful (g, n, SF1 (AggrFirst, e)) ->
      Printf.fprintf oc "FIRST%s(%a)" (st g n) p e
  | Stateful (g, n, SF1 (AggrLast, e)) ->
      Printf.fprintf oc "LAST%s(%a)" (st g n) p e
  | Stateful (g, n, SF1 (AggrHistogram (min, max, num_buckets), e)) ->
      Printf.fprintf oc "HISTOGRAM%s(%a, %g, %g, %d)" (st g n)
        p e min max num_buckets
  | Stateful (g, n, SF2 (Lag, e1, e2)) ->
      Printf.fprintf oc "LAG%s(%a, %a)" (st g n) p e1 p e2
  | Stateful (g, n, SF3 (MovingAvg, e1, e2, e3)) ->
      Printf.fprintf oc "SEASON_MOVEAVG%s(%a, %a, %a)"
        (st g n) p e1 p e2 p e3
  | Stateful (g, n, SF3 (LinReg, e1, e2, e3)) ->
      Printf.fprintf oc "SEASON_FIT%s(%a, %a, %a)"
        (st g n) p e1 p e2 p e3
  | Stateful (g, n, SF4s (MultiLinReg, e1, e2, e3, e4s)) ->
      Printf.fprintf oc "SEASON_FIT_MULTI%s(%a, %a, %a, %a)"
        (st g n) p e1 p e2 p e3 print_args e4s
  | Stateful (g, n, SF4s (Remember, fpr, tim, dur, es)) ->
      Printf.fprintf oc "REMEMBER%s(%a, %a, %a, %a)"
        (st g n) p fpr p tim p dur print_args es
  | Stateful (g, n, SF1s (Distinct, es)) ->
      Printf.fprintf oc "DISTINCT%s(%a)" (st g n) print_args es
  | Stateful (g, n, SF2 (ExpSmooth, e1, e2)) ->
      Printf.fprintf oc "SMOOTH%s(%a, %a)" (st g n) p e1 p e2
  | Stateful (g, n, SF3 (Hysteresis, meas, accept, max)) ->
      Printf.fprintf oc "HYSTERESIS%s(%a, %a, %a)"
        (st g n) p meas p accept p max
  | Stateful (g, n, Top { want_rank ; c ; max_size ; what ; by ; time ;
                          duration }) ->
      Printf.fprintf oc "%s %a in top %a %a%s by %a in the last %a at time %a"
        (if want_rank then "rank of" else "is")
        (List.print ~first:"" ~last:"" ~sep:", " p) what
        (fun oc -> function
         | None -> Unit.print oc ()
         | Some e -> Printf.fprintf oc " over %a" p e) max_size
        p c (st g n) p by p duration p time
  | Stateful (g, n, SF3s (Largest { inv ; up_to }, c, e, es)) ->
      let print_by oc es =
        if es <> [] then
          Printf.fprintf oc " BY %a"
            (List.print ~first:"" ~last:"" ~sep:", " p) es in
      Printf.fprintf oc "%s %a%s %s%a%a"
        (if es <> [] then
          if inv then "SMALLEST" else "LARGEST"
        else
          if inv then "OLDEST" else "LATEST")
        p c (st g n)
        (if up_to then "UP TO " else "")
        p e print_by es
  | Stateful (g, n, SF2 (Sample, c, e)) ->
      Printf.fprintf oc "SAMPLE%s(%a, %a)" (st g n) p c p e
  | Stateful (g, n, Past { what ; time ; max_age ; sample_size }) ->
      (match sample_size with
      | None -> ()
      | Some sz ->
          Printf.fprintf oc "SAMPLE OF SIZE %a OF THE " p sz) ;
      Printf.fprintf oc "LAST %a%s OF %a AT TIME %a"
        p max_age (st g n) p what p time
  | Stateful (g, n, SF1 (Group, e)) ->
      Printf.fprintf oc "GROUP%s %a" (st g n) p e
  | Stateful (g, n, SF1 (Count, e)) ->
      Printf.fprintf oc "COUNT%s %a" (st g n) p e

  | Generator (Split (e1, e2)) ->
      Printf.fprintf oc "SPLIT(%a, %a)" p e1 p e2
  | Stateful (_, _, SF1s (AccompanyMe, _))
  | Stateless (SL3 (DontBeLonely, _, _, _)) ->
      assert false)

and print_endianness oc = function
  | LittleEndian -> String.print oc "LITTLE ENDIAN"
  | BigEndian -> String.print oc "BIG ENDIAN"

let is_nullable e = e.typ.T.nullable

let is_const e =
  match e.text with
  | Const _ -> true | _ -> false

let is_bool_const b e =
  match e.text with
  | Const (VBool b') -> b' = b
  | _ -> false

let is_true = is_bool_const true
let is_false = is_bool_const false

let is_a_string e =
  e.typ.T.structure = TString

(* Tells if [e] (that must be typed) is a list or a vector, ie anything
 * which is represented with an OCaml array. *)
let is_a_list e =
  match e.typ.T.structure with
  | TList _ | TVec _ -> true
  | _ -> false

let rec map f s e =
  (* [s] is the stack of expressions to the AST root *)
  let s' = e :: s in
  (* Shorthands : *)
  let m = map f s' in
  let mm = List.map m
  and om = Option.map m in
  (match e.text with
  | Const _ | Variable _ | Binding _ | Stateless (SL0 _) ->
      e

  | Case (alts, else_) ->
      { e with text = Case (
        List.map (fun a ->
          { case_cond = m a.case_cond ; case_cons = m a.case_cons }
        ) alts, om else_) }

  | Tuple es -> { e with text = Tuple (mm es) }

  | Record kvs ->
      { e with text = Record (List.map (fun (k, e) -> k, m e) kvs) }

  | Vector es -> { e with text = Vector (mm es) }

  | Stateless (SL1 (o, e1)) ->
      { e with text = Stateless (SL1 (o, m e1)) }
  | Stateless (SL1s (o, es)) ->
      { e with text = Stateless (SL1s (o, mm es)) }
  | Stateless (SL2 (o, e1, e2)) ->
      { e with text = Stateless (SL2 (o, m e1, m e2)) }
  | Stateless (SL3 (o, e1, e2, e3)) ->
      { e with text = Stateless (SL3 (o, m e1, m e2, m e3)) }

  | Stateful (g, n, SF1 (o, e1)) ->
      { e with text = Stateful (g, n, SF1 (o, m e1)) }
  | Stateful (g, n, SF2 (o, e1, e2)) ->
      { e with text = Stateful (g, n, SF2 (o, m e1, m e2)) }
  | Stateful (g, n, SF3 (o, e1, e2, e3)) ->
      { e with text = Stateful (g, n, SF3 (o, m e1, m e2, m e3)) }
  | Stateful (g, n, SF3s (o, c, x, es)) ->
      { e with text = Stateful (g, n, SF3s (o, m c, m x, mm es)) }
  | Stateful (g, n, SF4s (o, e1, e2, e3, e4s)) ->
      { e with text = Stateful (g, n, SF4s (o, m e1, m e2, m e3, mm e4s)) }
  | Stateful (g, n, Top ({ c ; by ; time ; duration ; what ; max_size } as a)) ->
      { e with text = Stateful (g, n, Top { a with
        c = m c ; by = m by ; time = m time ; duration = m duration ;
        what = mm what ; max_size = om max_size }) }
  | Stateful (g, n, Past { what ; time ; max_age ; sample_size }) ->
      { e with text = Stateful (g, n, Past {
        what = m what ; time = m time ; max_age = m max_age ;
        sample_size = om sample_size }) }
  | Stateful (g, n, SF1s (o, es)) ->
      { e with text = Stateful (g, n, SF1s (o, mm es)) }

  | Generator (Split (e1, e2)) ->
      { e with text = Generator (Split (m e1, m e2)) }) |>
  f s

(* Run [f] on all sub-expressions in turn. *)
let fold_subexpressions f s i e =
  (* [s] is the stack of expressions to the AST root *)
  let s = e :: s in
  (* Shorthands: *)
  let f = f s in
  let fl = List.fold_left f in
  let om i = Option.map_default (f i) i
  in
  match e.text with
  | Const _ | Variable _ | Binding _ | Stateless (SL0 _) ->
      i

  | Case (alts, else_) ->
      let i =
        List.fold_left (fun i a ->
          f (f i a.case_cond) a.case_cons
        ) i alts in
      om i else_

  | Tuple es | Vector es -> fl i es

  | Record kvs ->
      List.fold_left (fun i (_, e) -> f i e) i kvs

  | Stateless (SL1 (_, e1)) | Stateful (_, _, SF1 (_, e1)) -> f i e1

  | Stateless (SL1s (_, e1s)) -> fl i e1s

  | Stateless (SL2 (_, e1, e2))
  | Stateful (_, _, SF2 (_, e1, e2)) -> f (f i e1) e2

  | Stateless (SL3 (_, e1, e2, e3))
  | Stateful (_, _, SF3 (_, e1, e2, e3)) -> f (f (f i e1) e2) e3

  | Stateful (_, _, SF3s (_, e1, e2, e3s)) -> fl (f (f i e1) e2) e3s
  | Stateful (_, _, SF4s (_, e1, e2, e3, e4s)) ->
      fl (f (f (f i e1) e2) e3) e4s

  | Stateful (_, _, Top { c ; by ; time ; duration ; what ; max_size }) ->
      om (fl i (c :: by :: time :: duration :: what)) max_size

  | Stateful (_, _, Past { what ; time ; max_age ; sample_size }) ->
      om (f (f (f i what) time) max_age) sample_size
  | Stateful (_, _, SF1s (_, es)) -> fl i es

  | Generator (Split (e1, e2)) -> f (f i e1) e2

(* Fold depth first, calling [f] bottom up: *)
let rec fold_up f s i e =
  let i = fold_subexpressions (fold_up f) s i e in
  f s i e

let rec fold_down f s i e =
  let i = f s i e in
  fold_subexpressions (fold_down f) s i e

(* Iterate bottom up by default as that's what most callers expect: *)
let fold = fold_up

let iter f =
  fold (fun s () e -> f s e) [] ()

let unpure_iter f e =
  fold (fun s () e -> match e.text with
    | Stateful _ -> f s e
    | _ -> ()
  ) [] () e |> ignore

let unpure_fold i f e =
  fold (fun s i e -> match e.text with
    | Stateful _ -> f s i e
    | _ -> i
  ) [] i e

let is_pure e =
  try
    unpure_iter (fun _ _ -> raise Exit) e ;
    true
  with Exit ->
    false

(* Any expression that uses a generator is a generator: *)
let is_generator e =
  try
    iter (fun _s e ->
      match e.text with
      | Generator _ -> raise Exit
      | _ -> ()) e ;
    false
  with Exit -> true

(* When optimising it is useful to consider an expression as a sequence of
 * ANDs or ORs. This function takes an expression and a stateless binary
 * operator and returns the list of all sub-expressions that would be the
 * arguments of that operator if it were n-ary. Note that this list always
 * at least includes one elements (if only one then that's the expression
 * that was passed). *)
let rec as_nary op = function
  | { text = Stateless (SL2 (o, e1, e2)) } when o = op ->
      List.rev_append (as_nary op e1) (as_nary op e2)
  | e -> [ e ]

(*$inject
  let test_as_nary str =
    let e = parse str in
    let lst = as_nary And e |>
              List.fast_sort compare in
    BatPrintf.sprintf2 "AND%a"
      (BatList.print ~first:"(" ~last:")" (print false)) lst
*)
(*$= test_as_nary & ~printer:BatPervasives.identity
  "AND(false; false; true)" (test_as_nary "true AND false AND false")
  "AND(false; false; true)" (test_as_nary "(true AND false) AND false")
  "AND(false; false; true)" (test_as_nary "true AND (false AND false)")
  "AND(false)" (test_as_nary "false")
  "AND((1) + (1))" (test_as_nary "1+1")
*)

(* In the other way around, to rebuild the cascading expression from a
 * list of sub-expressions and an operator. Useful after we have extracted
 * a sub-expression from the list returned by [as_nary]: *)
let of_nary ~structure ~nullable ~units op lst =
  let rec loop = function
    | [] ->
        (match op with
        | And -> of_bool true
        | Or -> of_bool false
        | _ -> todo "of_nary for any operation")
    | x::rest ->
        make ~structure ~nullable ?units
             (Stateless (SL2 (op, x, loop rest))) in
  loop lst

(* Given a predicate on expressions [p] and an expression [e], tells whether
 * all expressions composing [e] satisfy [p]: *)
let forall p e =
  try
    iter (fun _s e -> if not (p e) then raise Exit) e ;
    true
  with Exit ->
    false

(* Given a boolean expression [e] and a predicate on expressions [p], return
 * 2 expressions [e1] and [e2] so that all expressions composing [e1]
 * satisfy [p] and [e1 AND e2] is equivalent to [e]. *)
let and_partition p e =
  let es = as_nary And e in
  let e1s, e2s =
    List.fold_left (fun (e1, e2) e ->
      if forall p e then
        e::e1, e2
      else
        e1, e::e2
    ) ([], []) es in
  let of_nary es =
    of_nary ~structure:e.typ.structure
            ~nullable:e.typ.nullable
            ~units:e.units And es in
  of_nary e1s, of_nary e2s

module Parser =
struct
  type expr = t
  let const_of_string = of_string
  (*$< Parser *)
  open RamenParsing

  (* We can share default values: *)
  let default_start = make (Stateless (SL0 EventStart))
  let default_zero = zero ()
  let default_one = one ()
  let default_1hour = one_hour ()

  (* Single things *)
  let const m =
    let m = "constant" :: m in
    (
      (
        T.Parser.scalar ~min_int_width:32 >>:
        fun c ->
          (* We'd like to consider all constants as dimensionless, but that'd
             be a pain (for instance, COALESCE(x, 0) would be invalid if x had
             a unit, while by leaving the const unit unspecified it has the
             unit of x.
          let units =
            if T.(is_a_num (structure_of c)) then
              Some RamenUnits.dimensionless
            else None in*)
          make (Const c)
      ) ||| (
        duration >>: fun x ->
          make ~units:RamenUnits.seconds (Const (VFloat x))
      )
    ) m

  (*$= const & ~printer:BatPervasives.identity
    "true" \
      (test_expr ~printer:(print false) const "true")

    "15" \
      (test_expr ~printer:(print false) const "15i8")
  *)

  let null m =
    (
      T.Parser.null >>:
      fun v ->
        make (Const v) (* Type of "NULL" is yet unknown *)
    ) m

  let variable m =
    let m = "variable" :: m in
    (
      parse_prefix >>:
      fun (n) ->
        make (Variable n)
    ) m

  let param m =
    let m = "parameter" :: m in
    (
      (* You can choose any tuple as long as it's TupleParam: *)
      optional ~def:() (
        parse_prefix +- char '.' >>:
        fun p ->
          if p <> TupleParam then raise (Reject "not a param")
      ) -+ non_keyword >>:
      fun n ->
        make (Stateless (SL2 (Get, const_of_string n, make (Variable TupleParam))))
    ) m

  (*$= param & ~printer:BatPervasives.identity
    "param.glop" \
      (test_expr ~printer:(print false) param "glop")
    "param.glop" \
      (test_expr ~printer:(print false) param "param.glop")
  *)

  let immediate_or_param m =
    let m = "an immediate or a parameter" :: m in
    (const ||| param) m

  let state_lifespan m =
    let m = "state lifespan" :: m in
    (
      (strinG "globally" >>: fun () -> GlobalState) |||
      (strinG "locally" >>: fun () -> LocalState)
    ) m

  let skip_nulls m =
    let m = "skip nulls" :: m in
    (
      ((strinG "skip" >>: fun () -> true) |||
       (strinG "keep" >>: fun () -> false)) +-
      blanks +- strinGs "null"
    ) m

  let state_and_nulls ?(def_state=LocalState)
                      ?(def_skipnulls=true) m =
    (
      optional ~def:def_state (blanks -+ state_lifespan) ++
      optional ~def:def_skipnulls (blanks -+ skip_nulls)
    ) m

  (* Empty vectors are disallowed so we cannot ignore the element type: *)
  let vector p m =
    let m = "vector" :: m in
    (
      char '[' -- opt_blanks -+
      several ~sep:T.Parser.tup_sep p +-
      opt_blanks +- char ']' >>:
      fun es ->
        let num_items = List.length es in
        assert (num_items >= 1) ;
        make (Vector es)
    ) m

  (* operators with lowest precedence *)
  let rec lowestest_prec_left_assoc m =
    let m = "logical OR operator" :: m in
    let op = strinG "or"
    and reduce e1 _op e2 = make (Stateless (SL2 (Or, e1, e2))) in
    (* FIXME: we do not need a blanks if we had parentheses ("(x)OR(y)" is OK) *)
    binary_ops_reducer ~op ~term:lowest_prec_left_assoc ~sep:blanks ~reduce m

  and lowest_prec_left_assoc m =
    let m = "logical AND operator" :: m in
    let op = strinG "and"
    and reduce e1 _op e2 = make (Stateless (SL2 (And, e1, e2))) in
    binary_ops_reducer ~op ~term:conditional ~sep:blanks ~reduce m

  and conditional m =
    let m = "conditional expression" :: m in
    (
      case ||| if_ ||| low_prec_left_assoc
    ) m

  and low_prec_left_assoc m =
    let m = "comparison operator" :: m in
    let op =
      that_string ">" ||| that_string ">=" ||| that_string "<" ||| that_string "<=" |||
      that_string "=" ||| that_string "<>" ||| that_string "!=" |||
      that_string "in" |||
      (strinG "not" -- blanks -- strinG "in" >>: fun () -> "not in") |||
      that_string "like" |||
      ((that_string "starts" ||| that_string "ends") +- blanks +- strinG "with")
    and reduce e1 op e2 = match op with
      | ">" -> make (Stateless (SL2 (Gt, e1, e2)))
      | "<" -> make (Stateless (SL2 (Gt, e2, e1)))
      | ">=" -> make (Stateless (SL2 (Ge, e1, e2)))
      | "<=" -> make (Stateless (SL2 (Ge, e2, e1)))
      | "=" -> make (Stateless (SL2 (Eq, e1, e2)))
      | "!=" | "<>" ->
          make (Stateless (SL1 (Not, make (Stateless (SL2 (Eq, e1, e2))))))
      | "in" -> make (Stateless (SL2 (In, e1, e2)))
      | "not in" ->
          make (Stateless (SL1 (Not, make (Stateless (SL2 (In, e1, e2))))))
      | "like" ->
          (match string_of_const e2 with
          | None -> raise (Reject "LIKE pattern must be a string constant")
          | Some p -> make (Stateless (SL1 (Like p, e1))))
      | "starts" -> make (Stateless (SL2 (StartsWith, e1, e2)))
      | "ends" -> make (Stateless (SL2 (EndsWith, e1, e2)))
      | _ -> assert false in
    binary_ops_reducer ~op ~term:mid_prec_left_assoc ~sep:opt_blanks ~reduce m

  and mid_prec_left_assoc m =
    let m = "arithmetic operator" :: m in
    let op = that_string "+" ||| that_string "-" ||| that_string "||" |||
             that_string "|?"
    and reduce e1 op e2 = match op with
      | "+" -> make (Stateless (SL2 (Add, e1, e2)))
      | "-" -> make (Stateless (SL2 (Sub, e1, e2)))
      | "||" -> make (Stateless (SL2 (Concat, e1, e2)))
      | "|?" -> make (Stateless (SL1s (Coalesce, [ e1 ; e2 ])))
      | _ -> assert false in
    binary_ops_reducer ~op ~term:high_prec_left_assoc ~sep:opt_blanks ~reduce m

  and high_prec_left_assoc m =
    let m = "arithmetic operator" :: m in
    let op = that_string "*" ||| that_string "//" ||| that_string "/" |||
             that_string "%"
    and reduce e1 op e2 = match op with
      | "*" -> make (Stateless (SL2 (Mul, e1, e2)))
      (* Note: We want the default division to output floats by default *)
      (* Note: We reject IP/INT because that's a CIDR *)
      | "//" -> make (Stateless (SL2 (IDiv, e1, e2)))
      | "%" -> make (Stateless (SL2 (Mod, e1, e2)))
      | "/" ->
          (* "1.2.3.4/1" can be parsed both as a CIDR or a dubious division of
           * an IP by a number. Reject that one: *)
          (match e1.text, e2.text with
          | Const c1, Const c2 when
              T.(structure_of c1 |> is_an_ip) &&
              T.(structure_of c2 |> is_an_int) ->
              raise (Reject "That's a CIDR")
          | _ ->
              make (Stateless (SL2 (Div, e1, e2))))
      | _ -> assert false
    in
    binary_ops_reducer ~op ~term:higher_prec_left_assoc ~sep:opt_blanks ~reduce m

  and higher_prec_left_assoc m =
    let m = "bitwise logical operator" :: m in
    let op = that_string "&" ||| that_string "|" ||| that_string "#" |||
             that_string "<<" ||| that_string ">>"
    and reduce e1 op e2 = match op with
      | "&" -> make (Stateless (SL2 (BitAnd, e1, e2)))
      | "|" -> make (Stateless (SL2 (BitOr, e1, e2)))
      | "#" -> make (Stateless (SL2 (BitXor, e1, e2)))
      | "<<" -> make (Stateless (SL2 (BitShift, e1, e2)))
      | ">>" ->
          let e2 = make (Stateless (SL1 (Minus, e2))) in
          make (Stateless (SL2 (BitShift, e1, e2)))
      | _ -> assert false in
    binary_ops_reducer ~op ~term:higher_prec_right_assoc ~sep:opt_blanks ~reduce m

  and higher_prec_right_assoc m =
    let m = "arithmetic operator" :: m in
    let op = char '^'
    and reduce e1 _ e2 = make (Stateless (SL2 (Pow, e1, e2))) in
    binary_ops_reducer ~op ~right_associative:true
                       ~term:highest_prec_left_assoc ~sep:opt_blanks ~reduce m

  and highest_prec_left_assoc m =
    (
      (afun1 "not" >>: fun e ->
        make (Stateless (SL1 (Not, e)))) |||
      (strinG "-" -- opt_blanks --
        check (nay decimal_digit) -+ highestest_prec >>: fun e ->
          make (Stateless (SL1 (Minus, e)))) |||
      (highestest_prec ++
        optional ~def:None (
          blanks -- strinG "is" -- blanks -+
          optional ~def:(Some false)
                   (strinG "not" -- blanks >>: fun () -> Some true) +-
          strinG "null") >>: function
            | e, None -> e
            | e, Some false ->
                make (Stateless (SL1 (Not,
                  make (Stateless (SL1 (Defined, e))))))
            | e, Some true ->
                make (Stateless (SL1 (Defined, e)))) |||
      (strinG "begin" -- blanks -- strinG "of" -- blanks -+ highestest_prec >>:
        fun e -> make (Stateless (SL1 (BeginOfRange, e)))) |||
      (strinG "end" -- blanks -- strinG "of" -- blanks -+ highestest_prec >>:
        fun e -> make (Stateless (SL1 (EndOfRange, e))))
    ) m

  and dotted_get m =
    let m = "dotted dereference" :: m in
    (
      (
        some (variable ||| parenthesized func ||| record) +-
        char '.' ++ non_keyword
      ) ||| (
        (return None) ++ (nay parse_prefix -+ non_keyword)
      ) >>:
      fun (e_opt, n) ->
        let e = match e_opt with
          | Some e -> e
          | None -> make (Variable TupleUnknown)
        in
        let n = make (Const (VString n)) in
        make (Stateless (SL2 (Get, n, e)))
    ) m

  (*$= dotted_get & ~printer:BatPervasives.identity
    "in.glop" \
      (test_expr ~printer:(print false) dotted_get "in.glop")
    "unknown.glop" \
      (test_expr ~printer:(print false) dotted_get "glop")
  *)

  (* "sf" stands for "stateful" *)
  and afunv_sf ?def_state a n m =
    let sep = list_sep in
    let m = n :: m in
    (
      strinG n -+
      state_and_nulls ?def_state +-
      opt_blanks +- char '(' +- opt_blanks ++ (
        if a > 0 then
          repeat ~what:"mandatory arguments" ~min:a ~max:a ~sep p ++
          optional ~def:[] (sep -+ repeat ~what:"variadic arguments" ~sep p)
        else
          return [] ++
          repeat ~what:"variadic arguments" ~sep p
      ) +- opt_blanks +- char ')'
    ) m

  and afun_sf ?def_state a n =
    afunv_sf ?def_state a n >>: fun (g, (a, r)) ->
      if r = [] then g, a else
      raise (Reject "too many arguments")

  and afun1_sf ?def_state n =
    let sep = check (char '(') ||| blanks in
    (strinG n -+ state_and_nulls ?def_state +-
     sep ++ highestest_prec)

  and afun2_sf ?def_state n =
    afun_sf ?def_state 2 n >>: function (g, [a;b]) -> g, a, b | _ -> assert false

  and afun0v_sf ?def_state n =
    (* afunv_sf takes parentheses but it's nicer to also accept non
     * parenthesized highestest_prec, but then there would be 2 ways to
     * parse "distinct (x)" as highestest_prec also accept parenthesized
     * lower precedence expressions. Thus the "highestest_prec_no_parenthesis": *)
    (strinG n -+ state_and_nulls ?def_state +-
     blanks ++ highestest_prec_no_parenthesis >>: fun (f, e) -> f, [e]) |||
    (afunv_sf ?def_state 0 n >>:
     function (g, ([], r)) -> g, r | _ -> assert false)

  and afun2v_sf ?def_state n =
    afunv_sf ?def_state 2 n >>: function (g, ([a;b], r)) -> g, a, b, r | _ -> assert false

  and afun3_sf ?def_state n =
    afun_sf ?def_state 3 n >>: function (g, [a;b;c]) -> g, a, b, c | _ -> assert false

  and afun3v_sf ?def_state n =
    afunv_sf ?def_state 3 n >>: function (g, ([a;b;c], r)) -> g, a, b, c, r | _ -> assert false

  and afun4_sf ?def_state n =
    afun_sf ?def_state 4 n >>: function (g, [a;b;c;d]) -> g, a, b, c, d | _ -> assert false

  and afunv a n m =
    let m = n :: m in
    let sep = list_sep in
    (strinG n -- opt_blanks -- char '(' -- opt_blanks -+
     (if a > 0 then
       repeat ~what:"mandatory arguments" ~min:a ~max:a ~sep p ++
       optional ~def:[] (sep -+ repeat ~what:"variadic arguments" ~sep p)
      else
       return [] ++
       repeat ~what:"variadic arguments" ~sep p) +-
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

  and func m =
    let m = "function" :: m in
    (* Note: min and max of nothing are NULL but sum of nothing is 0, etc *)
    (
      (afun1 "age" >>: fun e -> make (Stateless (SL1 (Age, e)))) |||
      (afun1 "abs" >>: fun e -> make (Stateless (SL1 (Abs, e)))) |||
      (afun1 "length" >>: fun e -> make (Stateless (SL1 (Length, e)))) |||
      (afun1 "lower" >>: fun e -> make (Stateless (SL1 (Lower, e)))) |||
      (afun1 "upper" >>: fun e -> make (Stateless (SL1 (Upper, e)))) |||
      (strinG "now" >>: fun () -> make (Stateless (SL0 Now))) |||
      (strinG "random" >>: fun () -> make (Stateless (SL0 Random))) |||
      (strinG "#start" >>: fun () -> make (Stateless (SL0 EventStart))) |||
      (strinG "#stop" >>: fun () -> make (Stateless (SL0 EventStop))) |||
      (afun1 "exp" >>: fun e -> make (Stateless (SL1 (Exp, e)))) |||
      (afun1 "log" >>: fun e -> make (Stateless (SL1 (Log, e)))) |||
      (afun1 "log10" >>: fun e -> make (Stateless (SL1 (Log10, e)))) |||
      (afun1 "sqrt" >>: fun e -> make (Stateless (SL1 (Sqrt, e)))) |||
      (afun1 "ceil" >>: fun e -> make (Stateless (SL1 (Ceil, e)))) |||
      (afun1 "floor" >>: fun e -> make (Stateless (SL1 (Floor, e)))) |||
      (afun1 "round" >>: fun e -> make (Stateless (SL1 (Round, e)))) |||
      (afun1 "truncate" >>: fun e ->
         make (Stateless (SL2 (Trunc, e, of_float 1.)))) |||
      (afun2 "truncate" >>: fun (e1, e2) ->
         make (Stateless (SL2 (Trunc, e1, e2)))) |||
      (afun1 "hash" >>: fun e -> make (Stateless (SL1 (Hash, e)))) |||
      (afun1 "sparkline" >>: fun e -> make (Stateless (SL1 (Sparkline, e)))) |||
      (afun1_sf "min" >>: fun ((g, n), e) ->
         make (Stateful (g, n, SF1 (AggrMin, e)))) |||
      (afun1_sf "max" >>: fun ((g, n), e) ->
         make (Stateful (g, n, SF1 (AggrMax, e)))) |||
      (afun1_sf "sum" >>: fun ((g, n), e) ->
         make (Stateful (g, n, SF1 (AggrSum, e)))) |||
      (afun1_sf "avg" >>: fun ((g, n), e) ->
         make (Stateful (g, n, SF1 (AggrAvg, e)))) |||
      (afun1_sf "and" >>: fun ((g, n), e) ->
         make (Stateful (g, n, SF1 (AggrAnd, e)))) |||
      (afun1_sf "or" >>: fun ((g, n), e) ->
         make (Stateful (g, n, SF1 (AggrOr, e)))) |||
      (afun1_sf "first" >>: fun ((g, n), e) ->
         make (Stateful (g, n, SF1 (AggrFirst, e)))) |||
      (afun1_sf "last" >>: fun ((g, n), e) ->
         make (Stateful (g, n, SF1 (AggrLast, e)))) |||
      (afun1_sf "group" >>: fun ((g, n), e) ->
         make (Stateful (g, n, SF1 (Group, e)))) |||
      (afun1_sf "all" >>: fun ((g, n), e) ->
         make (Stateful (g, n, SF1 (Group, e)))) |||
      (afun1_sf "count" >>: fun ((g, n), e) ->
         make (Stateful (g, n, SF1 (Count, e)))) |||
      (
        let perc =
          (const ||| param) +-
          (optional ~def:() (strinG "th")) in
        (perc ||| vector perc) +- blanks ++
        afun1 "percentile" >>:
        fun (ps, e) ->
          make (Stateless (SL2 (Percentile, e, ps)))
      ) |||
      (afun2_sf "lag" >>: fun ((g, n), e1, e2) ->
         make (Stateful (g, n, SF2 (Lag, e1, e2)))) |||
      (afun1_sf "lag" >>: fun ((g, n), e) ->
         make (Stateful (g, n, SF2 (Lag, one (), e)))) |||

      (afun3_sf "season_moveavg" >>: fun ((g, n), e1, e2, e3) ->
         make (Stateful (g, n, SF3 (MovingAvg, e1, e2, e3)))) |||
      (afun2_sf "moveavg" >>: fun ((g, n), e1, e2) ->
         make (Stateful (g, n, SF3 (MovingAvg, one (), e1, e2)))) |||
      (afun3_sf "season_fit" >>: fun ((g, n), e1, e2, e3) ->
         make (Stateful (g, n, SF3 (LinReg, e1, e2, e3)))) |||
      (afun2_sf "fit" >>: fun ((g, n), e1, e2) ->
         make (Stateful (g, n, SF3 (LinReg, one (), e1, e2)))) |||
      (afun3v_sf "season_fit_multi" >>: fun ((g, n), e1, e2, e3, e4s) ->
         make (Stateful (g, n, SF4s (MultiLinReg, e1, e2, e3, e4s)))) |||
      (afun2v_sf "fit_multi" >>: fun ((g, n), e1, e2, e3s) ->
         make (Stateful (g, n, SF4s (MultiLinReg, one (), e1, e2, e3s)))) |||
      (afun2_sf "smooth" >>: fun ((g, n), e1, e2) ->
         make (Stateful (g, n, SF2 (ExpSmooth, e1, e2)))) |||
      (afun1_sf "smooth" >>: fun ((g, n), e) ->
         let alpha = of_float 0.5 in
         make (Stateful (g, n, SF2 (ExpSmooth, alpha, e)))) |||
      (afun3_sf "remember" >>: fun ((g, n), tim, dur, e) ->
         (* If we allowed a list of expressions here then it would be ambiguous
          * with the following "3+v" signature: *)
         let fpr = of_float 0.015 in
         make (Stateful (g, n, SF4s (Remember, fpr, tim, dur, [e])))) |||
      (afun3v_sf "remember" >>: fun ((g, n), fpr, tim, dur, es) ->
         make (Stateful (g, n, SF4s (Remember, fpr, tim, dur, es)))) |||
      (afun0v_sf "distinct" >>: fun ((g, n), es) ->
         make (Stateful (g, n, SF1s (Distinct, es)))) |||
      (afun3_sf "hysteresis" >>: fun ((g, n), value, accept, max) ->
         make (Stateful (g, n, SF3 (Hysteresis, value, accept, max)))) |||
      (afun4_sf "histogram" >>:
       fun ((g, n), what, min, max, num_buckets) ->
         match float_of_const min,
               float_of_const max,
               int_of_const num_buckets with
         | Some min, Some max, Some num_buckets ->
             if num_buckets <= 0 then
               raise (Reject "Histogram size must be positive") ;
             make (Stateful (g, n, SF1 (
              AggrHistogram (min, max, num_buckets), what)))
         | _ -> raise (Reject "histogram dimensions must be constants")) |||
      (afun2 "split" >>: fun (e1, e2) ->
         make (Generator (Split (e1, e2)))) |||
      (afun2 "format_time" >>: fun (e1, e2) ->
         make (Stateless (SL2 (Strftime, e1, e2)))) |||
      (afun1 "parse_time" >>: fun e ->
         make (Stateless (SL1 (Strptime, e)))) |||
      (afun1 "variant" >>: fun e ->
         make (Stateless (SL1 (Variant, e)))) |||
      (* At least 2 args to distinguish from the aggregate functions: *)
      (afun2v "max" >>: fun (e1, e2, e3s) ->
         make (Stateless (SL1s (Max, e1 :: e2 :: e3s)))) |||
      (afun1v "greatest" >>: fun (e, es) ->
         make (Stateless (SL1s (Max, e :: es)))) |||
      (afun2v "min" >>: fun (e1, e2, e3s) ->
         make (Stateless (SL1s (Min, e1 :: e2 :: e3s)))) |||
      (afun1v "least" >>: fun (e, es) ->
         make (Stateless (SL1s (Min, e :: es)))) |||
      (afun1v "print" >>: fun (e, es) ->
         make (Stateless (SL1s (Print, e :: es)))) |||
      (afun2 "reldiff" >>: fun (e1, e2) ->
        make (Stateless (SL2 (Reldiff, e1, e2)))) |||
      (afun2_sf "sample" >>: fun ((g, n), c, e) ->
        make (Stateful (g, n, SF2 (Sample, c, e)))) |||
      (afun3 "substring" >>: fun (s, a, b) ->
        make (Stateless (SL3 (SubString, s, a, b)))) |||
      k_moveavg ||| cast ||| top_expr ||| nth ||| largest ||| past ||| get |||
      changed_field ||| peek
    ) m

  and get m =
    let m = "get" :: m in
    (
      afun2 "get" >>: fun (n, v) ->
        (match n.text with
        | Const _ ->
            (match int_of_const n with
            | Some n ->
                if n < 0 then
                  raise (Reject "GET index must be positive")
            | None ->
                if string_of_const n = None then
                  raise (Reject "GET requires a numeric or string index"))
        | _ -> ()) ;
        make (Stateless (SL2 (Get, n, v)))
    ) m

  (* Syntactic sugar for `x <> previous.x` *)
  and changed_field m =
    let m = "changed" :: m in
    (
      afun1 "changed" >>:
      fun f ->
        let subst_expr pref n =
          (* If tuple is still unknown and we figure out later that it's
           * not output then the error message will be about that field
           * not present in the output tuple. Not too bad. *)
          if pref <> TupleOut && pref <> TupleUnknown then
            raise (Reject "Changed operator is only valid for \
                           fields of the output tuple") ;
          make (Stateless (SL2 ( Get, n, make (Variable TupleOutPrevious))))
        in
        let prev_field =
          match f.text with
          | Stateless (SL2 (Get, n, { text = Variable pref ; _ })) ->
              subst_expr pref n
          | _ ->
              raise (Reject "Changed operator is only valid for fields")
        in
        let text =
          Stateless (SL1 (Not, make (Stateless (SL2 (Eq, f, prev_field))))) in
        make text
    ) m

  and cast m =
    let m = "cast" :: m in
    let cast_a_la_c =
      let sep = check (char '(') ||| blanks in
      T.Parser.scalar_typ +- sep ++
      highestest_prec >>:
      fun (t, e) ->
        (* The nullability of [value] should propagate to [type(value)],
         * while [type?(value)] should be nullable no matter what. *)
        make (Stateless (SL1 (Cast t, e))) in
    let cast_a_la_sql =
      strinG "cast" -- opt_blanks -- char '(' -- opt_blanks -+
      highestest_prec +- blanks +- strinG "as" +- blanks ++
      T.Parser.scalar_typ +- opt_blanks +- char ')' >>:
      fun (e, t) ->
        make (Stateless (SL1 (Cast t, e))) in
    (cast_a_la_c ||| cast_a_la_sql) m

  and peek m =
    let m = "peek" :: m in
    (
      strinG "peek" -- blanks -+
      T.Parser.scalar_typ +- blanks ++
      optional ~def:LittleEndian (
        (
          (strinG "little" >>: fun () -> LittleEndian) |||
          (strinG "big" >>: fun () -> BigEndian)
        ) +- blanks +- strinG "endian" +- blanks) ++
      highestest_prec >>:
      fun ((t, endianness), e) ->
        make (Stateless (SL1 (Peek (t, endianness), e)))
    ) m

  and k_moveavg m =
    let m = "k-moving average" :: m in
    let sep = check (char '(') ||| blanks in
    (
      (unsigned_decimal_number >>: T.Parser.narrowest_int_scalar) +-
      (strinG "-moveavg" ||| strinG "-ma") ++
      state_and_nulls +-
      sep ++ highestest_prec >>:
      fun ((k, (g, n)), e) ->
        if k = VNull then raise (Reject "Cannot use NULL here") ;
        let k = make (Const k) in
        make (Stateful (g, n, SF3 (MovingAvg, one (), k, e)))
    ) m

  and top_expr m =
    let m = "top expression" :: m in
    (
      (
        (strinG "rank" -- blanks -- strinG "of" >>: fun () -> true) |||
        (strinG "is" >>: fun () -> false)
      ) +- blanks ++
      (* We can allow lowest precedence expressions here because of the
       * keywords that follow: *)
      several ~sep:list_sep p +- blanks +-
      strinG "in" +- blanks +- strinG "top" +- blanks ++ (const ||| param) ++
      optional ~def:None (
        some (blanks -- strinG "over" -- blanks -+ p)) ++
      state_and_nulls ++
      optional ~def:default_one (
        blanks -- strinG "by" -- blanks -+ highestest_prec) ++
      optional ~def:None (
        blanks -- strinG "at" -- blanks -- strinG "time" -- blanks -+ some p) ++
      optional ~def:None (
        blanks -- strinG "for" --
        optional ~def:() (blanks -- strinG "the" -- blanks -- strinG "last") --
        blanks -+ some (const ||| param)) >>:
      fun (((((((want_rank, what), c), max_size),
              (g, n)), by), time), duration) ->
        let time, duration =
          match time, duration with
          (* If we asked for no time decay, use neutral values: *)
          | None, None -> default_zero, default_1hour
          | Some t, None -> t, default_1hour
          | None, Some d -> default_start, d
          | Some t, Some d -> t, d
        in
        make (Stateful (g, n, Top {
          want_rank ; c ; max_size ; what ; by ; duration ; time }))
    ) m

  and largest m =
    let m = "largest expression" :: m in
    let up_to_c =
      blanks -+
      optional ~def:false (
        strinG "up" -- blanks -- strinG "to" -- blanks >>: fun () -> true
      ) ++ immediate_or_param
    in
    (
      (
        (
          (strinG "largest" >>: fun () -> false) |||
          (strinG "smallest" >>: fun () -> true)
        ) ++ up_to_c ++
        state_and_nulls +- opt_blanks ++ p ++
        optional ~def:[] (
          blanks -- strinG "by" -- blanks -+
          several ~sep:list_sep p) >>:
          fun ((((inv, (up_to, c)), (g, n)), e), es) ->
            (* The result is null when the number of input is less than c: *)
            make (Stateful (g, n, SF3s (Largest {inv ; up_to }, c, e, es)))
      ) ||| (
        (
          (strinG "latest" >>: fun () -> false) |||
          (strinG "oldest" >>: fun () -> true)
        ) ++ up_to_c ++ state_and_nulls +- opt_blanks ++ p >>:
          fun (((inv, (up_to, c)), (g, n)), e) ->
            make (Stateful (g, n, SF3s (Largest { inv ; up_to }, c, e, [])))
      )
    ) m

  and sample m =
    let m = "sample expression" :: m in
    (
      strinG "sample" -- blanks --
      optional ~def:() (strinG "of" -- blanks -- strinG "size" -- blanks) -+
      p +- optional ~def:() (blanks -- strinG "of" -- blanks -- strinG "the")
    ) m

  and past m =
    let m = "recent expression" :: m in
    (
      optional ~def:None (some sample +- blanks) +-
      strinG "past" +- blanks ++ p ++
      state_and_nulls +- opt_blanks +-
      strinG "of" +- blanks ++ p ++
      optional ~def:default_start
        (blanks -- strinG "at" -- blanks -- strinG "time" -- blanks -+ p) >>:
      fun ((((sample_size, max_age), (g, n)), what), time) ->
        make (Stateful (g, n, Past { what ; time ; max_age ; sample_size }))
    ) m

  and nth m =
    let m = "n-th" :: m in
    let q =
      pos_decimal_integer "nth" ++
      (that_string "th" ||| that_string "st" ||| that_string "nd" |||
       that_string "rd") >>:
      fun (n, th) ->
        if n = 0 then raise (Reject "tuple indices start at 1") ;
        if ordinal_suffix n = th then n
        (* Pedantic but also helps disambiguating the syntax: *)
        else raise (Reject ("bad suffix "^ th ^" for "^ string_of_int n))
    and sep = check (char '(') ||| blanks in
    (
      q +- sep ++ highestest_prec >>:
      fun (n, es) ->
        let n = make (Const (T.scalar_of_int (n - 1))) in
        make (Stateless (SL2 (Get, n, es)))
    ) m

  and case m =
    let m = "case" :: m in
    let alt m =
      let m = "case alternative" :: m in
      (strinG "when" -- blanks -+ p +-
       blanks +- strinG "then" +- blanks ++ p >>:
       fun (cd, cs) -> { case_cond = cd ; case_cons = cs }) m
    in
    (
      strinG "case" -- blanks -+
      several ~sep:blanks alt +- blanks ++
      optional ~def:None (
        strinG "else" -- blanks -+ some p +- blanks) +-
      strinG "end" >>:
      fun (alts, else_) -> make (Case (alts, else_))
    ) m

  and if_ m =
    let m = "if" :: m in
    (
      (
        strinG "if" -- blanks -+ p +-
        blanks +- strinG "then" +- blanks ++ p ++
        optional ~def:None (
          blanks -- strinG "else" -- blanks -+
          some p) >>:
        fun ((case_cond, case_cons), else_) ->
          make (Case ([ { case_cond ; case_cons } ], else_))
      ) ||| (
        afun2 "if" >>:
        fun (case_cond, case_cons) ->
          make (Case ([ { case_cond ; case_cons } ], None))
      ) ||| (
        afun3 "if" >>:
        fun (case_cond, case_cons, else_) ->
          make (Case ([ { case_cond ; case_cons } ], Some else_))
      )
    ) m

  and coalesce m =
    let m = "coalesce" :: m in
    (
      afun0v "coalesce" >>: function
        | [] -> raise (Reject "empty COALESCE")
        | [_] -> raise (Reject "COALESCE must have at least 2 arguments")
        | r -> make (Stateless (SL1s (Coalesce, r)))
    ) m

  and accept_units q =
    q ++ optional ~def:None (opt_blanks -+ some RamenUnits.Parser.p) >>:
    function e, None -> e
           | e, units -> { e with units }

  and highestest_prec_no_parenthesis m =
    (
      accept_units (const ||| dotted_get ||| null) |||
      variable ||| func ||| coalesce
    ) m

  and parenthesized p =
    char '(' -- opt_blanks -+ p +- opt_blanks +- char ')'

  and highestest_prec m =
    (
      highestest_prec_no_parenthesis |||
      accept_units (parenthesized p) |||
      tuple ||| vector p ||| record
    ) m

  (* Empty tuples and tuples of arity 1 are disallowed in order not to
   * conflict with parentheses used as grouping symbols. We could do the
   * same trick as in python though (TODO): *)
  and tuple m =
    let m = "tuple" :: m in
    (
      char '(' -- opt_blanks -+
      repeat ~min:2 ~sep:T.Parser.tup_sep p +-
      opt_blanks +- char ')' >>:
      fun es ->
        make (Tuple es)
    ) m

  and record m =
    let m = "record" :: m in
    (
      char '(' -- opt_blanks -+
      repeat ~min:1 ~sep:T.Parser.tup_sep (
        p +- T.Parser.kv_sep ++ non_keyword >>:
        fun (v, k) -> N.field k, v) +-
      opt_blanks +- char ')' >>:
      fun kvs ->
        make (Record kvs)
    ) m

  and p m = lowestest_prec_left_assoc m

  (*$= p & ~printer:BatPervasives.identity
    "13{secs^2}" \
      (test_expr ~printer:(print false) p "13i32{secs^2}")

    "13{secs^2}" \
      (test_expr ~printer:(print false) p "13i32 {secs ^ 2}")

    "true" \
      (test_expr ~printer:(print false) p "true")

    "NOT((unknown.zone_src) IS NOT NULL)" \
      (test_expr ~printer:(print false) p "zone_src IS NULL")

    "((NOT((unknown.zone_src) IS NOT NULL)) OR ((unknown.zone_src) = (unknown.z1))) AND ((NOT((unknown.zone_dst) IS NOT NULL)) OR ((unknown.zone_dst) = (unknown.z2)))" \
      (test_expr ~printer:(print false) p "(zone_src IS NULL or zone_src = z1) and \\
                 (zone_dst IS NULL or zone_dst = z2)")

    "(SUM LOCALLY skip nulls(unknown.bytes)) / (unknown.avg_window)" \
      (test_expr ~printer:(print false) p "(sum bytes)/avg_window")

    "(unknown.start) // ((1000000) * (unknown.avg_window))" \
      (test_expr ~printer:(print false) p "start // (1_000_000 * avg_window)")

    "param.p PERCENTILE(unknown.bytes_per_sec)" \
      (test_expr ~printer:(print false) p "p percentile bytes_per_sec")

    "(MAX LOCALLY skip nulls(in.start)) > ((out.start) + (((unknown.obs_window) * (1.15)) * (1000000)))" \
      (test_expr ~printer:(print false) p \
        "max in.start > out.start + (obs_window * 1.15) * 1_000_000")

    "(unknown.x) % (unknown.y)" \
      (test_expr ~printer:(print false) p "x % y")

    "ABS((unknown.bps) - (LAG LOCALLY skip nulls(1, unknown.bps)))" \
      (test_expr ~printer:(print false) p "abs(bps - lag(1,bps))")

    "HYSTERESIS LOCALLY skip nulls(unknown.value, 900, 1000)" \
      (test_expr ~printer:(print false) p "hysteresis(value, 900, 1000)")

    "((4) & (4)) * (2)" \
      (test_expr ~printer:(print false) p "4 & 4 * 2")
  *)

  (*$>*)
end

(* Used only for tests but could be handy in a REPL: *)
let parse =
  let print = print false in
  RamenParsing.string_parser ~what:"expression" ~print Parser.p

(* Function to check an expression after typing, to check that we do not
 * use any IO tuple for init, non constants, etc, when not allowed. *)
let check =
  let check_no_io what =
    iter (fun _s e ->
      match e.text with
      (* params and env are available from everywhere: *)
      | Variable pref when tuple_has_type_input pref ||
                           tuple_has_type_output pref ->
          Printf.sprintf2 "%s is not allowed to use %s"
            what (string_of_prefix pref) |>
          failwith
      (* TODO: all other similar cases *)
      | _ -> ())
  in
  iter (fun _s e ->
    match e.text with
    | Stateful (_, _, Past { max_age ; sample_size ; _ }) ->
        check_no_io "duration of function past" max_age ;
        Option.may (check_no_io "sample size of function past")
          sample_size
    | _ -> ())

(* Return the expected units for a given expression.
 * Fail if the operation does not accept the arguments units.
 * Returns None if the unit is unknown or if the value cannot have a unit
 * (non-numeric).
 * This is best-effort:
 * - units are not propagated from one conditional consequent to another;
 * - units of a Get is not inferred but in the simplest cases;
 * - units of x**y is not inferred unless y is constant.
 *)
let units_of_expr params units_of_input units_of_output =
  let units_of_params name =
    match List.find (fun param ->
            param.RamenTuple.ptyp.name = name
          ) params with
    | exception Not_found ->
        Printf.sprintf2 "Unknown parameter %a while looking for units"
          N.field_print name |>
        failwith
    | p -> p.RamenTuple.ptyp.units
  in
  let rec uoe ~indent e =
    let char_of_indent = Char.chr (Char.code 'a' + indent) in
    let prefix = Printf.sprintf "%s%c. " (String.make (indent * 2) ' ')
                                         char_of_indent in
    !logger.debug "%sUnits of expression %a...?" prefix (print true) e ;
    let indent = indent + 1 in
    if e.units <> None then e.units else
    (match e.text with
    | Const v ->
        if T.(is_a_num (structure_of v)) then e.units
        else None
    | Stateless (SL0 (Path [ Name n ])) -> (* Should not happen *)
        units_of_input n
    | Case (cas, else_opt) ->
        (* We merely check that the units of the alternatives are either
         * the same of unknown. *)
        List.iter (fun ca -> check_no_units ~indent ca.case_cond) cas ;
        let units_opt = Option.bind else_opt (uoe ~indent) in
        List.map (fun ca -> ca.case_cons) cas |>
        same_units ~indent "Conditional alternatives" units_opt
    | Stateless (SL1s (Coalesce, es)) ->
        same_units ~indent "Coalesce alternatives" None es
    | Stateless (SL0 (Now|EventStart|EventStop)) ->
        Some RamenUnits.seconds_since_epoch
    | Stateless (SL1 (Age, e)) ->
        check ~indent e RamenUnits.seconds_since_epoch ;
        Some RamenUnits.seconds
    | Stateless (SL1 ((Peek _|Cast _|Abs|Minus|Ceil|Floor|Round), e))
    | Stateless (SL2 (Trunc, e, _)) ->
        uoe ~indent e
    | Stateless (SL1 (Length, e)) ->
        check_no_units ~indent e ;
        Some RamenUnits.chars
    | Stateless (SL1 (Sqrt, e)) ->
        Option.map (fun e -> RamenUnits.pow e 0.5) (uoe ~indent e)
    | Stateless (SL2 (Add, e1, e2)) ->
        option_map2 RamenUnits.add (uoe ~indent e1) (uoe ~indent e2)
    | Stateless (SL2 (Sub, e1, e2)) ->
        option_map2 RamenUnits.sub (uoe ~indent e1) (uoe ~indent e2)
    | Stateless (SL2 ((Mul|Mod), e1, e2)) ->
        option_map2 RamenUnits.mul (uoe ~indent e1) (uoe ~indent e2)
    | Stateless (SL2 ((Div|IDiv), e1, e2)) ->
        option_map2 RamenUnits.div (uoe ~indent e1) (uoe ~indent e2)
    | Stateless (SL2 (Pow, e1, e2)) ->
        (* Best effort in case the exponent is a constant, otherwise we
         * just don't know what the unit is. *)
        option_map2 RamenUnits.pow (uoe ~indent e1) (float_of_const e2)
    (* Although shifts could be seen as mul/div, we'd rather consider
     * only dimensionless values receive this treatment, esp. since
     * it's not possible here to tell the difference between a mul-shift
     * and a div-shift. *)
    | Stateless (SL2 ((And|Or|Concat|StartsWith|EndsWith|
                         BitAnd|BitOr|BitXor|BitShift), e1, e2)) ->
        check_no_units ~indent e1 ;
        check_no_units ~indent e2 ;
        None
    | Stateless (SL2 (Get, e1, { text = Vector es ; _ })) ->
        Option.bind (int_of_const e1) (fun n ->
          List.at es n |> uoe ~indent)
    | Stateless (SL2 (Get, n, { text = Tuple es ; _ })) ->
        (* Not super useful. FIXME: use the solver. *)
        let n = int_of_const n |>
                option_get "Get from tuple must have const index" in
        (try List.at es n |> uoe ~indent
        with Invalid_argument _ -> None)
    | Stateless (SL2 (Get, s, { text = Record kvs ; _ })) ->
        (* Not super useful neither and that's more annoying as records
         * are supposed to replace operation fields.
         * FIXME: Compute and set the units after type-checking using the
         *        solver. *)
        let s = string_of_const s |>
                option_get "Get from record must have string index" in
        (try
          list_rfind_map (fun (k, v) ->
            if k = s then Some v else None
          ) (kvs :> (string * t) list) |> uoe ~indent
        with Not_found -> None)
    | Stateless (SL2 (Get, { text = Const (VString n) ; _ },
                           { text = Variable pref ; _ })) ->
        let n = N.field n in
        if tuple_has_type_input pref then
          units_of_input n
        else if tuple_has_type_output pref then
          units_of_output n
        else if pref = TupleParam then
          units_of_params n
        else None
    | Stateless (SL2 (Percentile,
                      { text = Stateful (_, _, SF3s (Largest _,  _, e, _))
                             | Stateful (_, _, SF2 (Sample, _, e))
                             | Stateful (_, _, SF1 (Group, e)) ; _ }, _)) ->
        uoe ~indent e
    | Stateless (SL1 (Like _, e)) ->
        check_no_units ~indent e ;
        None
    | Stateless (SL1s ((Max|Min), es)) ->
        same_units ~indent "Min/Max alternatives" None es
    | Stateless (SL1s (Print, e::_)) ->
        uoe ~indent e
    | Stateful (_, _, SF1 ((AggrMin|AggrMax|AggrAvg|AggrFirst|AggrLast), e))
    | Stateful (_, _, SF2 ((Lag|ExpSmooth), _, e))
    | Stateful (_, _, SF3 ((MovingAvg|LinReg), _, _, e)) ->
        uoe ~indent e
    | Stateful (_, _, SF1 (AggrSum, e)) ->
        let u = uoe ~indent e in
        check_not_rel e u ;
        u
    | Stateful (_, _, SF1 (Count, _)) ->
        (* Or "tuples" if we had such a unit. *)
        Some RamenUnits.dimensionless
    | Generator (Split (e1, e2)) ->
        check_no_units ~indent e1 ;
        check_no_units ~indent e2 ;
        None
    | _ -> None) |>
    function
      | Some u as res ->
          !logger.debug "%s-> %a" prefix RamenUnits.print u ;
          res
      | None -> None

  and check ~indent e u =
    match uoe ~indent e with
    | None -> ()
    | Some u' ->
        if not (RamenUnits.eq u u') then
          Printf.sprintf2 "%a must have units %a not %a"
            (print false) e
            RamenUnits.print u
            RamenUnits.print u' |>
          failwith

  and check_no_units ~indent e =
    match uoe ~indent e with
    | None -> ()
    | Some u ->
        Printf.sprintf2 "%a must have no units but has unit %a"
          (print false) e
          RamenUnits.print u |>
        failwith

  and check_not_rel e u =
    Option.may (fun u ->
      if RamenUnits.is_relative u then
        Printf.sprintf2 "%a must not have relative unit but has unit %a"
          (print false) e
          RamenUnits.print u |>
        failwith
    ) u

  and same_units ~indent what i es =
    List.enum es /@ (uoe ~indent) |>
    RamenUnits.check_same_units ~what i

  in uoe ~indent:0
