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

(*$inject
  open TestHelpers
  open RamenLang
  open Stdint
*)

(* Each expression come with a type attached. Starting at None, types are
 * set during compilation. *)
type typ =
  { mutable expr_name : string ;
    (* To build var names, record field names or identify SAT variables: *)
    uniq_num : int ;
    mutable typ : RamenTypes.t option ;
    mutable units : RamenUnits.t option }
  [@@ppp PPP_OCaml]

let print_typ oc typ =
  Printf.fprintf oc "%s of %s"
    typ.expr_name
    (match typ.typ with
    | None -> "unknown type"
    | Some t -> "type "^ IO.to_string RamenTypes.print_typ t) ;
  (match typ.units with
  | None -> ()
  | Some units ->
      Printf.fprintf oc "%a" RamenUnits.print units)

let uniq_num_seq = ref 0

let make_typ ?typ ?units expr_name =
  incr uniq_num_seq ;
  { expr_name ; typ ; units ; uniq_num = !uniq_num_seq }

(* Stateful function can have either a unique global state a one state per
 * aggregation group (local). Each function has its own default (functions
 * that tends to be used mostly for aggregation have a local default state,
 * while others have a global state), but you can select explicitly using
 * the "locally" and "globally" keywords. For instance: "sum globally 1". *)
type state_lifespan = LocalState | GlobalState
  [@@ppp PPP_OCaml]

type skip_nulls = bool
  [@@ppp PPP_OCaml]

(* The type of an expression. Each is accompanied with a typ
 * (TODO: not for long!) *)
type t =
  (* Immediate value: *)
  | Const of typ * RamenTypes.value
  (* A tuple of expression (not to be confounded with an immediate tuple).
   * (1; "two"; 3.0) is a RamenTypes.VTup (an immediate constant of type
   * RamenTypes.TTup...) whereas (3-2; "t"||"wo"; sqrt(9)) is an expression
   * (Tuple of...). *)
  | Tuple of typ * t list
  (* The same distinction applies to vectors.
   * Notice there are no list expressions though, for the same reason that
   * there is no such thing as a list immediate, but only vectors. Lists, ie
   * vectors which dimensions are variable, appear only at typing. *)
  | Vector of typ * t list
  (* A field from a tuple (or parameter, or environment, as special cases of
   * "tuples": *)
  | Field of typ * tuple_prefix ref * string (* field name *)
  (* StateField are met only late in the game in the code generator. Refer to
   * CodeGen_OCaml. *)
  | StateField of typ * string
  (* A conditional with all conditions and consequents, and finally an optional
   * "else" clause. *)
  | Case of typ * case_alternative list * t option
  (* A coalesce expression as a list of expression: *)
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
  | StatelessFun0 of typ * stateless_fun0
  | StatelessFun1 of typ * stateless_fun1 * t
  | StatelessFun2 of typ * stateless_fun2 * t * t
  | StatelessFunMisc of typ * stateless_fun_misc
  | StatefulFun of typ * state_lifespan * skip_nulls * stateful_fun
  | GeneratorFun of typ * generator_fun
  [@@ppp PPP_OCaml]

and stateless_fun0 =
  | Now
  | Random
  | EventStart
  | EventStop
  [@@ppp PPP_OCaml]

and stateless_fun1 =
  (* TODO: Other functions: date_part... *)
  | Age
  | Cast of RamenTypes.t
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
  | Nth of int (* Where the int starts at 0 for the first item *)
  | Sparkline
  | Strptime
  (* Return the name of the variant we are in, or NULL: *)
  | Variant
  [@@ppp PPP_OCaml]

and stateless_fun2 =
  (* Binary Ops scalars *)
  | Add
  | Sub
  | Mul
  | Div
  | IDiv
  | Mod
  | Pow
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
  (* Same as Nth but only for vectors/lists (accepts non constant index, and
   * indices start at 0) *)
  | VecGet
  (* For network address range test membership, or for an efficient constant
   * set membership test, or for a non-efficient sequence of OR kind of
   * membership test if the set is not constant: *)
  | In
  (* Takes format then time: *)
  | Strftime
  (* TODO: several percentiles. Requires multi values returns. *)
  | Percentile
  [@@ppp PPP_OCaml]

and case_alternative =
  { case_cond : t (* Must be bool *) ;
    case_cons : t (* All alternatives must share a type *) }
  [@@ppp PPP_OCaml]

and stateless_fun_misc =
  (* a LIKE operator using globs, infix *)
  | Like of t * string (* expression then pattern (using %, _ and \) *)
  (* Min/Max of the given values. Not like AggrMin/AggrMax, which are
   * aggregate functions! The parser distinguish the cases due to the
   * number of arguments: just 1 and that's the aggregate function, more
   * and that's the min/max of the given arguments. *)
  | Max of t list
  | Min of t list
  (* For debug: prints all its arguments, and output its first. *)
  | Print of t list
  [@@ppp PPP_OCaml]

and stateful_fun =
  (* TODO: Add stddev... *)
  | AggrMin of t
  | AggrMax of t
  | AggrSum of t
  | AggrAvg of t
  | AggrAnd of t
  | AggrOr  of t
  (* Returns the first/last value in the aggregation: *)
  | AggrFirst of t
  | AggrLast of t (* FIXME: Should be stateless *)
  (* FIXME: those float should be expressions so we could use params *)
  | AggrHistogram of t * float * float * int
  (* value retarded by k steps. If we have had less than k past values
   * then return NULL. *)
  | Lag of t * t
  (* If the current time is t, the seasonal, moving average of period p on k
   * seasons is the average of v(t-p), v(t-2p), ... v(t-kp). Note the absence
   * of v(t).  This is because we want to compare v(t) with this season
   * average.  Notice that lag is a special case of season average with p=k
   * and k=1, but with a universal type for the data (while season-avg works
   * only on numbers).  For instance, a moving average of order 5 would be
   * period=1, count=5.
   * When we have not enough history then the result will be NULL. *)
  | MovingAvg of t * t * t (* period, how many seasons to keep, expression *)
  (* Simple linear regression *)
  | LinReg of t * t * t (* as above: period, how many seasons to keep, expression *)
  (* TODO: in (most) functions below it should be doable to replace the
   * variadic lists of expressions by a single expression that's a tuple. *)
  (* Multiple linear regression - and our first variadic function (the
   * last parameter being a list of expressions to use for the predictors) *)
  | MultiLinReg of t * t * t * t list
  (* Rotating bloom filters. First parameter is the false positive rate we
   * aim at, second is an expression providing the "time", third a
   * "duration", and finally expressions whose values to remember. The function
   * will return true if it *thinks* this combination of values has been seen
   * the at a time not older than the given duration. This is based on
   * bloom-filters so there can be false positives but not false negatives.
   * Note: If possible, it might save a lot of space to aim for a high false
   * positive rate and account for it in the surrounding calculations than to
   * aim for a low false positive rate. *)
  | Remember of t * t * t * t list
  (* Accurate version of the above, remembering all instances of the given
   * tuple and returning a boolean. Only for when number of expected values
   * is small, obviously: *)
  | Distinct of t list
  (* Simple exponential smoothing *)
  | ExpSmooth of t * t (* coef between 0 and 1 and expression *)
  (* Hysteresis *)
  | Hysteresis of t * t * t (* measured value, acceptable, maximum *)
  (* Top-k operation *)
  | Top of { want_rank : bool ; c : t ; max_size : t option ; what : t list ;
             by : t ; time : t ; duration : t }
  (* Last N e1 [BY e2, e3...] - or by arrival.
   * Note: BY followed by more than one expression will require to parentheses
   * the whole expression to avoid ambiguous parsing. *)
  | Last of t (* N *) * t (* what *) * t list (* by *)
  (* Sample(n, e) -> Keep max n values of e and return them as a list. *)
  | Sample of t * t
  (* Build a list with all values from the group *)
  | Group of t
  [@@ppp PPP_OCaml]

and generator_fun =
  (* First function returning more than once (Generator). Here the typ is
   * type of a single value but the function is a generator and can return
   * from 0 to N such values. *)
  | Split of t * t
  [@@ppp PPP_OCaml]

(* Constant expressions must be typed independently and therefore have
 * a distinct uniq_num for each occurrence: *)
let expr_true () =
  let typ = RamenTypes.{ nullable = false ; structure = TBool } in
  Const (make_typ ~typ "true", VBool true)

let expr_false () =
  let typ = RamenTypes.{ nullable = false ; structure = TBool } in
  Const (make_typ ~typ "false", VBool false)

let expr_u8 ?units name n =
  let typ = RamenTypes.{ nullable = false ; structure = TU8 } in
  Const (make_typ ~typ ?units name, VU8 (Uint8.of_int n))

let expr_float ?units name n =
  let typ = RamenTypes.{ nullable = false ; structure = TFloat } in
  Const (make_typ ~typ ?units name, VFloat n)

let expr_zero () = expr_u8 "zero" 0
let expr_one () = expr_u8 "one" 1
let expr_1hour () = expr_float ~units:RamenUnits.seconds "1hour" 3600.

let of_float ?units v =
  let typ = RamenTypes.{ nullable = false ; structure = TFloat } in
  Const (make_typ ~typ ?units (string_of_float v), VFloat v)

let is_true = function
  | Const (_ , VBool true) -> true
  | _ -> false

let string_of_const = function
  | Const (_ , VString s) -> Some s
  | _ -> None

let float_of_const = function
  | Const (_, v) -> RamenTypes.float_of_scalar v
  | _ -> None

let int_of_const = function
  | Const (_, v) -> RamenTypes.int_of_scalar v
  | _ -> None

let rec print ?(max_depth=max_int) with_types oc e =
  let add_types t =
    if with_types then Printf.fprintf oc " [%a]" print_typ t
  and st g n =
    (* TODO: do not display default *)
    (match g with LocalState -> " locally" | GlobalState -> " globally") ^
    (if n then " skip nulls" else " keep nulls")
  and print_args =
    List.print ~first:"(" ~last:")" ~sep:", " (print with_types)
  in
  if max_depth <= 0 then Printf.fprintf oc "..." else
    let p oc = print ~max_depth:(max_depth-1) with_types oc in
    match e with
    | Const (t, c) ->
      RamenTypes.print oc c ; add_types t
    | Tuple (t, es) ->
      List.print ~first:"(" ~last:")" ~sep:"; " p oc es ;
      add_types t
    | Vector (t, es) ->
      List.print ~first:"[" ~last:"]" ~sep:"; " p oc es ;
      add_types t
    | Field (t, tuple, field) ->
      Printf.fprintf oc "%s.%s" (string_of_prefix !tuple) field ;
      add_types t
    | StateField (t, s) ->
      String.print oc s ; add_types t
    | Case (t, alts, else_) ->
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
      Printf.fprintf oc "END" ;
      add_types t
    | Coalesce (t, es) ->
      Printf.fprintf oc "COALESCE %a" print_args es ;
      add_types t
    | StatelessFun1 (t, Age, e) ->
      Printf.fprintf oc "age (%a)" p e ; add_types t
    | StatelessFun0 (t, Now) ->
      Printf.fprintf oc "now" ; add_types t
    | StatelessFun0 (t, Random) ->
      Printf.fprintf oc "random" ; add_types t
    | StatelessFun0 (t, EventStart) ->
      Printf.fprintf oc "#start" ; add_types t
    | StatelessFun0 (t, EventStop) ->
      Printf.fprintf oc "#stop" ; add_types t
    | StatelessFun1 (t, Cast typ, e) ->
      Printf.fprintf oc "cast(%a, %a)"
        RamenTypes.print_typ typ
        p e ;
      add_types t
    | StatelessFun1 (t, Length, e) ->
      Printf.fprintf oc "length (%a)" p e ; add_types t
    | StatelessFun1 (t, Lower, e) ->
      Printf.fprintf oc "lower (%a)" p e ; add_types t
    | StatelessFun1 (t, Upper, e) ->
      Printf.fprintf oc "upper (%a)" p e ; add_types t
    | StatelessFun1 (t, Not, e) ->
      Printf.fprintf oc "NOT (%a)" p e ; add_types t
    | StatelessFun1 (t, Abs, e) ->
      Printf.fprintf oc "ABS (%a)" p e ; add_types t
    | StatelessFun1 (t, Minus, e) ->
      Printf.fprintf oc "-(%a)" p e ; add_types t
    | StatelessFun1 (t, Defined, e) ->
      Printf.fprintf oc "(%a) IS NOT NULL" p e ;
      add_types t
    | StatelessFun2 (t, Add, e1, e2) ->
      Printf.fprintf oc "(%a) + (%a)"
        p e1 p e2 ;
      add_types t
    | StatelessFun2 (t, Sub, e1, e2) ->
      Printf.fprintf oc "(%a) - (%a)"
        p e1 p e2 ;
      add_types t
    | StatelessFun2 (t, Mul, e1, e2) ->
      Printf.fprintf oc "(%a) * (%a)"
        p e1 p e2 ;
      add_types t
    | StatelessFun2 (t, Div, e1, e2) ->
      Printf.fprintf oc "(%a) / (%a)"
        p e1 p e2 ;
      add_types t
    | StatelessFun2 (t, Reldiff, e1, e2) ->
      Printf.fprintf oc "reldiff((%a), (%a))"
        p e1 p e2 ;
      add_types t
    | StatelessFun2 (t, IDiv, e1, e2) ->
      Printf.fprintf oc "(%a) // (%a)"
        p e1 p e2 ;
      add_types t
    | StatelessFun2 (t, Mod, e1, e2) ->
      Printf.fprintf oc "(%a) %% (%a)"
        p e1 p e2 ;
      add_types t
    | StatelessFun2 (t, Pow, e1, e2) ->
      Printf.fprintf oc "(%a) ^ (%a)"
        p e1 p e2 ;
      add_types t
    | StatelessFun1 (t, Exp, e) ->
      Printf.fprintf oc "exp (%a)" p e ; add_types t
    | StatelessFun1 (t, Log, e) ->
      Printf.fprintf oc "log (%a)" p e ; add_types t
    | StatelessFun1 (t, Log10, e) ->
      Printf.fprintf oc "log10 (%a)" p e ; add_types t
    | StatelessFun1 (t, Sqrt, e) ->
      Printf.fprintf oc "sqrt (%a)" p e ; add_types t
    | StatelessFun1 (t, Ceil, e) ->
      Printf.fprintf oc "ceil (%a)" p e ; add_types t
    | StatelessFun1 (t, Floor, e) ->
      Printf.fprintf oc "floor (%a)" p e ; add_types t
    | StatelessFun1 (t, Round, e) ->
      Printf.fprintf oc "round (%a)" p e ; add_types t
    | StatelessFun1 (t, Hash, e) ->
      Printf.fprintf oc "hash (%a)" p e ; add_types t
    | StatelessFun1 (t, Sparkline, e) ->
      Printf.fprintf oc "sparkline (%a)" p e ;
      add_types t
    | StatelessFun2 (t, In, e1, e2) ->
      Printf.fprintf oc "(%a) IN (%a)"
        p e1 p e2 ; add_types t
    | StatelessFun1 (t, (BeginOfRange|EndOfRange as op), e) ->
      Printf.fprintf oc "%s of (%a)"
        (if op = BeginOfRange then "begin" else "end")
        p e ;
      add_types t
    | StatelessFun1 (t, Nth n, e) ->
      let n = n + 1 in
      Printf.fprintf oc "%d%s %a"
        n (ordinal_suffix n) p e ;
      add_types t
    | StatelessFun1 (t, Strptime, e) ->
      Printf.fprintf oc "parse_time (%a)" p e ;
      add_types t
    | StatelessFun1 (t, Variant, e) ->
      Printf.fprintf oc "variant (%a)" p e ;
      add_types t
    | StatelessFun2 (t, And, e1, e2) ->
      Printf.fprintf oc "(%a) AND (%a)"
        p e1 p e2 ;
      add_types t
    | StatelessFun2 (t, Or, e1, e2) ->
      Printf.fprintf oc "(%a) OR (%a)"
        p e1 p e2 ;
      add_types t
    | StatelessFun2 (t, Ge, e1, e2) ->
      Printf.fprintf oc "(%a) >= (%a)"
        p e1 p e2 ;
      add_types t
    | StatelessFun2 (t, Gt, e1, e2) ->
      Printf.fprintf oc "(%a) > (%a)"
        p e1 p e2 ;
      add_types t
    | StatelessFun2 (t, Eq, e1, e2) ->
      Printf.fprintf oc "(%a) = (%a)"
        p e1 p e2 ;
      add_types t
    | StatelessFun2 (t, Concat, e1, e2) ->
      Printf.fprintf oc "(%a) || (%a)"
        p e1 p e2 ;
      add_types t
    | StatelessFun2 (t, StartsWith, e1, e2) ->
      Printf.fprintf oc "(%a) STARTS WITH (%a)"
        p e1 p e2 ;
      add_types t
    | StatelessFun2 (t, EndsWith, e1, e2) ->
      Printf.fprintf oc "(%a) ENDS WITH (%a)"
        p e1 p e2 ;
      add_types t
    | StatelessFun2 (t, Strftime, e1, e2) ->
      Printf.fprintf oc "format_time (%a, %a)"
        p e1 p e2 ;
      add_types t
    | StatelessFun2 (t, BitAnd, e1, e2) ->
      Printf.fprintf oc "(%a) & (%a)"
        p e1 p e2 ;
      add_types t
    | StatelessFun2 (t, BitOr, e1, e2) ->
      Printf.fprintf oc "(%a) | (%a)"
        p e1 p e2 ;
      add_types t
    | StatelessFun2 (t, BitXor, e1, e2) ->
      Printf.fprintf oc "(%a) ^ (%a)"
        p e1 p e2 ;
      add_types t
    | StatelessFun2 (t, BitShift, e1, e2) ->
      Printf.fprintf oc "(%a) << (%a)"
        p e1 p e2 ;
      add_types t
    | StatelessFun2 (t, VecGet, e1, e2) ->
      Printf.fprintf oc "get(%a, %a)"
        p e1 p e2 ;
      add_types t
    | StatelessFun2 (t, Percentile, e1, e2) ->
      Printf.fprintf oc "%ath percentile(%a)"
        p e1 p e2 ;
      add_types t
    | StatelessFunMisc (t, Like (e, pat)) ->
      Printf.fprintf oc "(%a) LIKE %S"
        p e pat ;
      add_types t
    | StatelessFunMisc (t, Max es) ->
      Printf.fprintf oc "GREATEST %a" print_args es ;
      add_types t
    | StatelessFunMisc (t, Min es) ->
      Printf.fprintf oc "LEAST %a" print_args es ;
      add_types t
    | StatelessFunMisc (t, Print es) ->
      Printf.fprintf oc "PRINT %a" print_args es ;
      add_types t
    | StatefulFun (t, g, n, AggrMin e) ->
      Printf.fprintf oc "min%s(%a)"
        (st g n) p e ;
      add_types t
    | StatefulFun (t, g, n, AggrMax e) ->
      Printf.fprintf oc "max%s(%a)"
        (st g n) p e ;
      add_types t
    | StatefulFun (t, g, n, AggrSum e) ->
      Printf.fprintf oc "sum%s(%a)"
        (st g n) p e ;
      add_types t
    | StatefulFun (t, g, n, AggrAvg e) ->
      Printf.fprintf oc "avg%s(%a)"
        (st g n) p e ;
      add_types t
    | StatefulFun (t, g, n, AggrAnd e) ->
      Printf.fprintf oc "and%s(%a)"
        (st g n) p e ;
      add_types t
    | StatefulFun (t, g, n, AggrOr e) ->
      Printf.fprintf oc "or%s(%a)"
        (st g n) p e ;
      add_types t
    | StatefulFun (t, g, n, AggrFirst e) ->
      Printf.fprintf oc "first%s(%a)"
        (st g n) p e ;
      add_types t
    | StatefulFun (t, g, n, AggrLast e) ->
      Printf.fprintf oc "last%s(%a)"
        (st g n) p e ;
      add_types t
    | StatefulFun (t, g, n, AggrHistogram (what, min, max, num_buckets)) ->
      Printf.fprintf oc "histogram%s(%a, %g, %g, %d)"
        (st g n)
        p what min max num_buckets ;
      add_types t
    | StatefulFun (t, g, n, Lag (e1, e2)) ->
      Printf.fprintf oc "lag%s(%a, %a)"
        (st g n)
        p e1 p e2 ;
      add_types t
    | StatefulFun (t, g, n, MovingAvg (e1, e2, e3)) ->
      Printf.fprintf oc "season_moveavg%s(%a, %a, %a)"
        (st g n)
        p e1
        p e2 p e3 ;
      add_types t
    | StatefulFun (t, g, n, LinReg (e1, e2, e3)) ->
      Printf.fprintf oc "season_fit%s(%a, %a, %a)"
        (st g n)
        p e1
        p e2 p e3 ;
      add_types t
    | StatefulFun (t, g, n, MultiLinReg (e1, e2, e3, e4s)) ->
      Printf.fprintf oc "season_fit_multi%s(%a, %a, %a, %a)"
        (st g n)
        p e1
        p e2
        p e3
        print_args e4s ;
      add_types t
    | StatefulFun (t, g, n, Remember (fpr, tim, dur, es)) ->
      Printf.fprintf oc "remember%s(%a, %a, %a, %a)"
        (st g n)
        p fpr
        p tim
        p dur
        print_args es ;
      add_types t
    | StatefulFun (_, g, n, Distinct es) ->
      Printf.fprintf oc "distinct%s(%a)"
        (st g n)
        print_args es
    | StatefulFun (t, g, n, ExpSmooth (e1, e2)) ->
      Printf.fprintf oc "smooth%s(%a, %a)"
        (st g n) p e1 p e2 ;
      add_types t
    | StatefulFun (t, g, n, Hysteresis (meas, accept, max)) ->
      Printf.fprintf oc "hysteresis%s(%a, %a, %a)"
        (st g n)
        p meas
        p accept
        p max ;
      add_types t
    | StatefulFun (t, g, n, Top { want_rank ; c ; max_size ; what ; by ; time ;
                                  duration }) ->
      Printf.fprintf oc "%s %a in top %a %a%s by %a in the last %a at time %a"
        (if want_rank then "rank of" else "is")
        (List.print ~first:"" ~last:"" ~sep:", " p) what
        (fun oc -> function
         | None -> Unit.print oc ()
         | Some e -> Printf.fprintf oc " over %a" p e) max_size
        p c
        (st g n)
        p by
        p duration
        p time ;
      add_types t
    | StatefulFun (t, g, n, Last (c, e, es)) ->
      let print_by oc es =
        if es <> [] then
          Printf.fprintf oc " BY %a"
            (List.print ~first:"" ~last:"" ~sep:", " p) es in
      Printf.fprintf oc "LAST %a%s %a%a"
        p c
        (st g n)
        p e
        print_by es ;
      add_types t
    | StatefulFun (t, g, n, Sample (c, e)) ->
      Printf.fprintf oc "SAMPLE%s(%a, %a)"
        (st g n)
        p c
        p e ;
      add_types t
    | StatefulFun (t, g, n, Group e) ->
      Printf.fprintf oc "GROUP%s %a"
        (st g n)
        p e ;
      add_types t

    | GeneratorFun (t, Split (e1, e2)) ->
      Printf.fprintf oc "split(%a, %a)"
        p e1 p e2 ;
      add_types t

let typ_of = function
  | Const (t, _) | Tuple (t, _) | Vector (t, _) | Field (t, _, _)
  | StateField (t, _) | StatelessFun0 (t, _) | StatelessFun1 (t, _, _)
  | StatelessFun2 (t, _, _, _) | StatelessFunMisc (t, _)
  | StatefulFun (t, _, _, _) | GeneratorFun (t, _) | Case (t, _, _)
  | Coalesce (t, _) -> t

let is_nullable e =
  let t = typ_of e in
  (Option.get t.typ).RamenTypes.nullable = true

let is_const = function
  | Const _ -> true | _ -> false

let is_a_string e =
  (Option.get (typ_of e).typ).structure = TString

(* Tells if [e] (that must be typed) is a list or a vector, ie anything
 * which is represented with an OCaml array. *)
let is_a_list e =
  match (Option.get (typ_of e).typ).structure with
  | TList _ | TVec _ -> true
  | _ -> false

(* Propagate values up the tree only, depth first. *)
let fold_subexpressions f i expr =
  match expr with
  | Const _ | Field _ | StateField _
  | StatelessFun0 _ ->
      i

  | StatefulFun (_, _, _, AggrMin e) | StatefulFun (_, _, _, AggrMax e)
  | StatefulFun (_, _, _, AggrSum e) | StatefulFun (_, _, _, AggrAvg e)
  | StatefulFun (_, _, _, AggrAnd e) | StatefulFun (_, _, _, AggrOr e)
  | StatefulFun (_, _, _, AggrFirst e) | StatefulFun (_, _, _, AggrLast e)
  | StatelessFun1 (_, _, e) | StatelessFunMisc (_, Like (e, _))
  | StatefulFun (_, _, _, AggrHistogram (e, _, _, _))
  | StatefulFun (_, _, _, Group e) ->
      f i e

  | StatelessFun2 (_, _, e1, e2)
  | StatefulFun (_, _, _, Lag (e1, e2))
  | StatefulFun (_, _, _, ExpSmooth (e1, e2))
  | StatefulFun (_, _, _, Sample (e1, e2))
  | GeneratorFun (_, Split (e1, e2)) ->
      f (f i e1) e2

  | StatefulFun (_, _, _, MovingAvg (e1, e2, e3))
  | StatefulFun (_, _, _, LinReg (e1, e2, e3))
  | StatefulFun (_, _, _, Hysteresis (e1, e2, e3)) ->
      f (f (f i e1) e2) e3

  | StatefulFun (_, _, _, Remember (e1, e2, e3, e4s))
  | StatefulFun (_, _, _, MultiLinReg (e1, e2, e3, e4s)) ->
      List.fold_left f i (e1::e2::e3::e4s)

  | StatefulFun (_, _, _,
      Top { c = e1 ; by = e2 ; time = e3 ; duration = e4 ; what = e5s ;
            max_size = e_opt }) ->
      let i = List.fold_left f i (e1::e2::e3::e4::e5s) in
      Option.map_default (f i) i e_opt

  | StatefulFun (_, _, _, Last (c, e, es)) ->
      List.fold_left f i (c::e::es)

  | Case (_, alts, else_) ->
      let i =
        List.fold_left (fun i a ->
          f (f i a.case_cond) a.case_cons
        ) i alts in
      Option.map_default (f i) i else_

  | Tuple (_, es)
  | Vector (_, es)
  | StatefulFun (_, _, _, Distinct es)
  | Coalesce (_, es)
  | StatelessFunMisc (_, (Max es|Min es|Print es)) ->
      List.fold_left f i es

let rec fold_by_depth f i expr =
    f (
      fold_subexpressions (fold_by_depth f) i expr
    ) expr

let iter f = fold_by_depth (fun () e -> f e) ()

let unpure_iter f e =
  fold_by_depth (fun () -> function
    | StatefulFun _ as e -> f e
    | _ -> ()) () e |> ignore

let unpure_fold u f e =
  fold_by_depth (fun u -> function
    | StatefulFun _ as e -> f u e
    | _ -> u) u e

(* Any expression that uses a generator is a generator: *)
let is_generator =
  fold_by_depth (fun is e ->
    is || match e with GeneratorFun _ -> true | _ -> false) false

(* Unlike is_const, which merely compare the given expression with Const,
 * this looks recursively for values that can change from input to input. *)
let is_constant e =
  try
    iter (function
      | Field (_, tuple, _) when RamenLang.tuple_has_type_input !tuple ->
          raise Exit
      | StatelessFun0 (_, (Now | Random | EventStart | EventStop))
      | StatelessFun1 (_, Age, _)
      | StatefulFun _
      | GeneratorFun _ ->
          raise Exit
      | _ -> ()) e ;
    true
  with Exit -> false

(* FIXME: store the type and the expression separately! *)

let rec map_type ?(recurs=true) f = function
  | Const (t, a) -> Const (f t, a)
  | Tuple (t, es) ->
    Tuple (f t,
           if recurs then List.map (map_type ~recurs f) es else es)
  | Vector (t, es) ->
    Vector (f t,
            if recurs then List.map (map_type ~recurs f) es else es)
  | Field (t, a, b) -> Field (f t, a, b)
  | StateField _ as e -> e

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

  | StatefulFun (t, g, n, AggrMin a) ->
    StatefulFun (f t, g, n, AggrMin (if recurs then map_type ~recurs f a else a))
  | StatefulFun (t, g, n, AggrMax a) ->
    StatefulFun (f t, g, n, AggrMax (if recurs then map_type ~recurs f a else a))
  | StatefulFun (t, g, n, AggrSum a) ->
    StatefulFun (f t, g, n, AggrSum (if recurs then map_type ~recurs f a else a))
  | StatefulFun (t, g, n, AggrAvg a) ->
    StatefulFun (f t, g, n, AggrAvg (if recurs then map_type ~recurs f a else a))
  | StatefulFun (t, g, n, AggrAnd a) ->
    StatefulFun (f t, g, n, AggrAnd (if recurs then map_type ~recurs f a else a))
  | StatefulFun (t, g, n, AggrOr a) ->
    StatefulFun (f t, g, n, AggrOr (if recurs then map_type ~recurs f a else a))
  | StatefulFun (t, g, n, AggrFirst a) ->
    StatefulFun (f t, g, n, AggrFirst (if recurs then map_type ~recurs f a else a))
  | StatefulFun (t, g, n, AggrLast a) ->
    StatefulFun (f t, g, n, AggrLast (if recurs then map_type ~recurs f a else a))
  | StatefulFun (t, g, n, AggrHistogram (a, min, max, num_buckets)) ->
    StatefulFun (f t, g, n, AggrHistogram (
        (if recurs then map_type ~recurs f a else a), min, max, num_buckets))
  | StatefulFun (t, g, n, Lag (a, b)) ->
    StatefulFun (f t, g, n, Lag (
        (if recurs then map_type ~recurs f a else a),
        (if recurs then map_type ~recurs f b else b)))
  | StatefulFun (t, g, n, MovingAvg (a, b, c)) ->
    StatefulFun (f t, g, n, MovingAvg (
        (if recurs then map_type ~recurs f a else a),
        (if recurs then map_type ~recurs f b else b),
        (if recurs then map_type ~recurs f c else c)))
  | StatefulFun (t, g, n, LinReg (a, b, c)) ->
    StatefulFun (f t, g, n, LinReg (
        (if recurs then map_type ~recurs f a else a),
        (if recurs then map_type ~recurs f b else b),
        (if recurs then map_type ~recurs f c else c)))
  | StatefulFun (t, g, n, MultiLinReg (a, b, c, d)) ->
    StatefulFun (f t, g, n, MultiLinReg (
        (if recurs then map_type ~recurs f a else a),
        (if recurs then map_type ~recurs f b else b),
        (if recurs then map_type ~recurs f c else c),
        (if recurs then List.map (map_type ~recurs f) d else d)))
  | StatefulFun (t, g, n, Remember (fpr, tim, dur, es)) ->
    StatefulFun (f t, g, n, Remember (
        (if recurs then map_type ~recurs f fpr else fpr),
        (if recurs then map_type ~recurs f tim else tim),
        (if recurs then map_type ~recurs f dur else dur),
        (if recurs then List.map (map_type ~recurs f) es else es)))
  | StatefulFun (t, g, n, Distinct es) ->
    StatefulFun (f t, g, n, Distinct
        (if recurs then List.map (map_type ~recurs f) es else es))
  | StatefulFun (t, g, n, ExpSmooth (a, b)) ->
    StatefulFun (f t, g, n, ExpSmooth (
        (if recurs then map_type ~recurs f a else a),
        (if recurs then map_type ~recurs f b else b)))
  | StatefulFun (t, g, n, Hysteresis (a, b, c)) ->
    StatefulFun (f t, g, n, Hysteresis (
        (if recurs then map_type ~recurs f a else a),
        (if recurs then map_type ~recurs f b else b),
        (if recurs then map_type ~recurs f c else c)))
  | StatefulFun (t, g, n, Top { want_rank ; c ; max_size ; what ; by ;
                                duration ; time }) ->
    StatefulFun (f t, g, n, Top {
      want_rank ;
      c = (if recurs then map_type ~recurs f c else c) ;
      max_size = (if recurs then Option.map (map_type ~recurs f) max_size else max_size) ;
      duration = (if recurs then map_type ~recurs f duration else duration) ;
      what = (if recurs then List.map (map_type ~recurs f) what else what) ;
      by = (if recurs then map_type ~recurs f by else by) ;
      time = (if recurs then map_type ~recurs f time else time) })
  | StatefulFun (t, g, n, Last (c, e, es)) ->
    StatefulFun (f t, g, n, Last (c,
      (if recurs then map_type ~recurs f e else e),
      (if recurs then List.map (map_type ~recurs f) es else es)))
  | StatefulFun (t, g, n, Sample (c, e)) ->
    StatefulFun (f t, g, n, Sample (c,
      (if recurs then map_type ~recurs f e else e)))
  | StatefulFun (t, g, n, Group e) ->
    StatefulFun (f t, g, n, Group (if recurs then map_type ~recurs f e else e))

  | StatelessFun0 (t, cst) ->
    StatelessFun0 (f t, cst)

  | StatelessFun1 (t, cst, a) ->
    StatelessFun1 (f t, cst, (if recurs then map_type ~recurs f a else a))
  | StatelessFun2 (t, cst, a, b) ->
    StatelessFun2 (f t, cst,
        (if recurs then map_type ~recurs f a else a),
        (if recurs then map_type ~recurs f b else b))
  | StatelessFunMisc (t, Like (e, p)) ->
    StatelessFunMisc (f t, Like (
        (if recurs then map_type ~recurs f e else e), p))
  | StatelessFunMisc (t, Max es) ->
    StatelessFunMisc (f t, Max
      (if recurs then List.map (map_type ~recurs f) es else es))
  | StatelessFunMisc (t, Min es) ->
    StatelessFunMisc (f t, Min
      (if recurs then List.map (map_type ~recurs f) es else es))
  | StatelessFunMisc (t, Print es) ->
    StatelessFunMisc (f t, Print
      (if recurs then List.map (map_type ~recurs f) es else es))

  | GeneratorFun (t, Split (a, b)) ->
    GeneratorFun (f t, Split (
        (if recurs then map_type ~recurs f a else a),
        (if recurs then map_type ~recurs f b else b)))

module Parser =
struct
  type expr = t
  (*$< Parser *)
  open RamenParsing

  (* Single things *)
  let const m =
    let m = "constant" :: m in
    (
      (
        RamenTypes.Parser.scalar ~min_int_width:32 >>:
        fun c ->
          (* We'd like to consider all constants as dimensionless, but that'd
             be a pain (for instance, COALESCE(x, 0) would be invalid if x had
             a unit, while by leaving the const unit unspecified it has the
             unit of x.
          let units =
            if RamenTypes.(is_a_num (structure_of c)) then
              Some RamenUnits.dimensionless
            else None in*)
          Const (make_typ "constant", c)
      ) ||| (
        duration >>: fun x ->
          Const (make_typ ~units:RamenUnits.seconds "constant", VFloat x)
      )
    ) m

  (*$= const & ~printer:(test_printer (print false))
    (Ok (Const (typ, VBool true), (4, [])))\
      (test_p const "true" |> replace_typ_in_expr)

    (Ok (Const (typ, VI8 (Stdint.Int8.of_int 15)), (4, []))) \
      (test_p const "15i8" |> replace_typ_in_expr)
  *)

  let null m =
    (RamenTypes.Parser.null >>: fun v ->
      (* Type of "NULL" is unknown yet *)
      Const (make_typ "NULL", v)
    ) m

  let field m =
    let m = "field" :: m in
    (
      parse_prefix ~def:TupleUnknown ++ non_keyword >>:
      fun (tuple, field) ->
        (* This is important here that the type name is the raw field name,
         * because we use the tuple field type name as their identifier (unless
         * it's a virtual field (starting with #) of course since those are
         * computed on the fly and have no corresponding variable in the
         * tuple) *)
        Field (make_typ field, ref tuple, field)
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
        Some { where = ParsersMisc.Item ((1,8), '.');\
               what=["eof"]})))\
      (test_p field "pasglop.bytes" |> replace_typ_in_expr)
  *)

  let param m =
    let m = "parameter" :: m in
    (non_keyword >>: fun p ->
      Field (make_typ p, ref TupleParam, p)) m

  (*$= param & ~printer:(test_printer (print false))
    (Ok (\
      Field (typ, ref TupleParam, "glop"),\
      (4, [])))\
      (test_p param "glop" |> replace_typ_in_expr)
  *)

  let state_lifespan m =
    let m = "state lifespan" :: m in
    ((strinG "globally" >>: fun () -> GlobalState) |||
     (strinG "locally" >>: fun () -> LocalState)) m

  let skip_nulls m =
    let m = "skip nulls" :: m in
    (
      ((strinG "skip" >>: fun () -> true) |||
       (strinG "keep" >>: fun () -> false)) +-
      blanks +- strinGs "null"
    ) m

  let state_and_nulls ?(def_state=GlobalState)
                      ?(def_skipnulls=true) m =
    (
      optional ~def:def_state (blanks -+ state_lifespan) ++
      optional ~def:def_skipnulls (blanks -+ skip_nulls)
    ) m

  (* operators with lowest precedence *)
  let rec lowestest_prec_left_assoc m =
    let m = "logical OR operator" :: m in
    let op = strinG "or"
    and reduce e1 _op e2 = StatelessFun2 (make_typ "or", Or, e1, e2) in
    (* FIXME: we do not need a blanks if we had parentheses ("(x)OR(y)" is OK) *)
    binary_ops_reducer ~op ~term:lowest_prec_left_assoc ~sep:blanks ~reduce m

  and lowest_prec_left_assoc m =
    let m = "logical AND operator" :: m in
    let op = strinG "and"
    and reduce e1 _op e2 = StatelessFun2 (make_typ "and", And, e1, e2) in
    binary_ops_reducer ~op ~term:conditional ~sep:blanks ~reduce m

  and conditional m =
    let m = "conditional expression" :: m in
    (case ||| if_ ||| low_prec_left_assoc) m

  and low_prec_left_assoc m =
    let m = "comparison operator" :: m in
    let op = that_string ">" ||| that_string ">=" ||| that_string "<" ||| that_string "<=" |||
             that_string "=" ||| that_string "<>" ||| that_string "!=" |||
             that_string "in" ||| that_string "like" |||
             ((that_string "starts" ||| that_string "ends") +- blanks +- strinG "with")
    and reduce e1 op e2 = match op with
      | ">" -> StatelessFun2 (make_typ "comparison (>)", Gt, e1, e2)
      | "<" -> StatelessFun2 (make_typ "comparison (<)", Gt, e2, e1)
      | ">=" -> StatelessFun2 (make_typ "comparison (>=)", Ge, e1, e2)
      | "<=" -> StatelessFun2 (make_typ "comparison (<=)", Ge, e2, e1)
      | "=" -> StatelessFun2 (make_typ "equality", Eq, e1, e2)
      | "!=" | "<>" ->
        StatelessFun1 (make_typ "not", Not,
          StatelessFun2 (make_typ "equality", Eq, e1, e2))
      | "in" -> StatelessFun2 (make_typ "in", In, e1, e2)
      | "like" ->
        (match string_of_const e2 with
        | None -> raise (Reject "LIKE pattern must be a string constant")
        | Some p ->
          StatelessFunMisc (make_typ "like", Like (e1, p)))
      | "starts" -> StatelessFun2 (make_typ "starts with", StartsWith, e1, e2)
      | "ends" -> StatelessFun2 (make_typ "ends with", EndsWith, e1, e2)
      | _ -> assert false in
    binary_ops_reducer ~op ~term:mid_prec_left_assoc ~sep:opt_blanks ~reduce m

  and mid_prec_left_assoc m =
    let m = "arithmetic operator" :: m in
    let op = that_string "+" ||| that_string "-" ||| that_string "||" |||
             that_string "|?"
    and reduce e1 op e2 = match op with
      | "+" -> StatelessFun2 (make_typ "addition", Add, e1, e2)
      | "-" -> StatelessFun2 (make_typ "subtraction", Sub, e1, e2)
      | "||" -> StatelessFun2 (make_typ "concatenation", Concat, e1, e2)
      | "|?" -> Coalesce (make_typ "default", [ e1 ; e2 ])
      | _ -> assert false in
    binary_ops_reducer ~op ~term:high_prec_left_assoc ~sep:opt_blanks ~reduce m

  and high_prec_left_assoc m =
    let m = "arithmetic operator" :: m in
    let op = that_string "*" ||| that_string "//" ||| that_string "/" ||| that_string "%"
    and reduce e1 op e2 = match op with
      | "*" -> StatelessFun2 (make_typ "multiplication", Mul, e1, e2)
      (* Note: We want the default division to output floats by default *)
      (* Note: We reject IP/INT because that's a CIDR *)
      | "//" -> StatelessFun2 (make_typ "integer-division", IDiv, e1, e2)
      | "%" -> StatelessFun2 (make_typ "modulo", Mod, e1, e2)
      | "/" ->
          (match e1, e2 with
          | Const (_, c1), Const (_, c2) when
              RamenTypes.(structure_of c1 |> is_an_ip) &&
              RamenTypes.(structure_of c2 |> is_an_int) ->
              raise (Reject "That's a CIDR")
          | _ ->
              StatelessFun2 (make_typ "division", Div, e1, e2))
      | _ -> assert false in
    binary_ops_reducer ~op ~term:higher_prec_left_assoc ~sep:opt_blanks ~reduce m

  and higher_prec_left_assoc m =
    let m = "bitwise logical operator" :: m in
    let op = that_string "&" ||| that_string "|" ||| that_string "#" |||
             that_string "<<" ||| that_string ">>"
    and reduce e1 op e2 = match op with
      | "&" -> StatelessFun2 (make_typ "bitwise and", BitAnd, e1, e2)
      | "|" -> StatelessFun2 (make_typ "bitwise or", BitOr, e1, e2)
      | "#" -> StatelessFun2 (make_typ "bitwise xor", BitXor, e1, e2)
      | "<<" -> StatelessFun2 (make_typ "bitwise shift", BitShift, e1, e2)
      | ">>" ->
          let e2 = StatelessFun1 (make_typ "unary minus", Minus, e2) in
          StatelessFun2 (make_typ "bitwise shift", BitShift, e1, e2)
      | _ -> assert false in
    binary_ops_reducer ~op ~term:higher_prec_right_assoc ~sep:opt_blanks ~reduce m

  and higher_prec_right_assoc m =
    let m = "arithmetic operator" :: m in
    let op = char '^'
    and reduce e1 _ e2 =
      StatelessFun2 (make_typ "exponentiation", Pow, e1, e2) in
    binary_ops_reducer ~op ~right_associative:true
                       ~term:highest_prec_left_assoc ~sep:opt_blanks ~reduce m

  (* "sf" stands for "stateful" *)
  and afunv_sf ?def_state a n m =
    let sep = list_sep in
    let m = n :: m in
    (strinG n -+
     state_and_nulls ?def_state +-
     opt_blanks +- char '(' +- opt_blanks ++
     (if a > 0 then
       repeat ~what:"mandatory arguments" ~min:a ~max:a ~sep p ++
       optional ~def:[] (sep -+ repeat ~what:"variadic arguments" ~sep p)
      else
       return [] ++
       repeat ~what:"variadic arguments" ~sep p) +-
     opt_blanks +- char ')') m

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

  and highest_prec_left_assoc m =
    (
      (afun1 "not" >>: fun e ->
        StatelessFun1 (make_typ "not", Not, e)) |||
      (strinG "-" -- opt_blanks --
        check (nay decimal_digit) -+ highestest_prec >>: fun e ->
          StatelessFun1 (make_typ "unary minus", Minus, e)) |||
      (highestest_prec ++
        optional ~def:None (
          blanks -- strinG "is" -- blanks -+
          optional ~def:(Some false)
                   (strinG "not" -- blanks >>: fun () -> Some true) +-
          strinG "null") >>: function
            | e, None -> e
            | e, Some false ->
              StatelessFun1 (make_typ "not", Not,
                StatelessFun1 (make_typ "is_not_null", Defined, e))
            | e, Some true ->
              StatelessFun1 (make_typ "is_not_null", Defined, e)) |||
      (strinG "begin" -- blanks -- strinG "of" -- blanks -+ highestest_prec >>:
        fun e -> StatelessFun1 (make_typ "begin of", BeginOfRange, e)) |||
      (strinG "end" -- blanks -- strinG "of" -- blanks -+ highestest_prec >>:
        fun e -> StatelessFun1 (make_typ "end of", EndOfRange, e))
    ) m

  and func m =
    let m = "function" :: m in
    (* Note: min and max of nothing are NULL but sum of nothing is 0, etc *)
    ((afun1 "age" >>: fun e -> StatelessFun1 (make_typ "age function", Age, e)) |||
     (afun1 "abs" >>: fun e -> StatelessFun1 (make_typ "absolute value", Abs, e)) |||
     (afun1 "length" >>: fun e -> StatelessFun1 (make_typ "length", Length, e)) |||
     (afun1 "lower" >>: fun e -> StatelessFun1 (make_typ "lower", Lower, e)) |||
     (afun1 "upper" >>: fun e -> StatelessFun1 (make_typ "upper", Upper, e)) |||
     (strinG "now" >>: fun () -> StatelessFun0 (make_typ "now", Now)) |||
     (strinG "random" >>: fun () -> StatelessFun0 (make_typ "random", Random)) |||
     (strinG "#start" >>: fun () -> StatelessFun0 (make_typ "#start", EventStart)) |||
     (strinG "#stop" >>: fun () -> StatelessFun0 (make_typ "#stop", EventStop)) |||
     (afun1 "exp" >>: fun e -> StatelessFun1 (make_typ "exponential", Exp, e)) |||
     (afun1 "log" >>: fun e -> StatelessFun1 (make_typ "natural logarithm", Log, e)) |||
     (afun1 "log10" >>: fun e -> StatelessFun1 (make_typ "common logarithm", Log10, e)) |||
     (afun1 "sqrt" >>: fun e -> StatelessFun1 (make_typ "square root", Sqrt, e)) |||
     (afun1 "ceil" >>: fun e -> StatelessFun1 (make_typ "ceil", Ceil, e)) |||
     (afun1 "floor" >>: fun e -> StatelessFun1 (make_typ "floor", Floor, e)) |||
     (afun1 "round" >>: fun e -> StatelessFun1 (make_typ "round", Round, e)) |||
     (afun1 "hash" >>: fun e -> StatelessFun1 (make_typ "hash", Hash, e)) |||
     (afun1 "sparkline" >>: fun e -> StatelessFun1 (make_typ "sparkline", Sparkline, e)) |||
     (afun1_sf ~def_state:LocalState "min" >>: fun ((g, n), e) ->
        StatefulFun (make_typ "min aggregation", g, n, AggrMin e)) |||
     (afun1_sf ~def_state:LocalState "max" >>: fun ((g, n), e) ->
        StatefulFun (make_typ "max aggregation", g, n, AggrMax e)) |||
     (afun1_sf ~def_state:LocalState "sum" >>: fun ((g, n), e) ->
        StatefulFun (make_typ "sum aggregation", g, n, AggrSum e)) |||
     (afun1_sf ~def_state:LocalState "avg" >>: fun ((g, n), e) ->
        StatefulFun (make_typ "average", g, n, AggrAvg e)) |||
     (afun1_sf ~def_state:LocalState "and" >>: fun ((g, n), e) ->
        StatefulFun (make_typ "and aggregation", g, n, AggrAnd e)) |||
     (afun1_sf ~def_state:LocalState "or" >>: fun ((g, n), e) ->
        StatefulFun (make_typ "or aggregation", g, n, AggrOr e)) |||
     (afun1_sf ~def_state:LocalState "first" >>: fun ((g, n), e) ->
        StatefulFun (make_typ "first aggregation", g, n, AggrFirst e)) |||
     (afun1_sf ~def_state:LocalState "last" >>: fun ((g, n), e) ->
        StatefulFun (make_typ "last aggregation", g, n, AggrLast e)) |||
     (afun1_sf ~def_state:LocalState "group" >>: fun ((g, n), e) ->
        StatefulFun (make_typ "group aggregation", g, n, Group e)) |||
     (afun1_sf ~def_state:GlobalState "all" >>: fun ((g, n), e) ->
        StatefulFun (make_typ "group aggregation", g, n, Group e)) |||
     ((const ||| param) +-
      (optional ~def:() (strinG "th")) +- blanks ++
      afun1 "percentile" >>: fun (p, e) ->
        StatelessFun2 (make_typ "percentile", Percentile, p, e)) |||
     (afun2_sf "lag" >>: fun ((g, n), e1, e2) ->
        StatefulFun (make_typ "lag", g, n, Lag (e1, e2))) |||
     (afun1_sf "lag" >>: fun ((g, n), e) ->
        StatefulFun (make_typ "lag", g, n, Lag (expr_one (), e))) |||

     (* avg perform a division thus the float type *)
     (afun3_sf "season_moveavg" >>: fun ((g, n), e1, e2, e3) ->
        StatefulFun (make_typ "season_moveavg", g, n, MovingAvg (e1, e2, e3))) |||
     (afun2_sf "moveavg" >>: fun ((g, n), e1, e2) ->
        StatefulFun (make_typ "season_moveavg", g, n, MovingAvg (expr_one (), e1, e2))) |||
     (afun3_sf "season_fit" >>: fun ((g, n), e1, e2, e3) ->
        StatefulFun (make_typ "season_fit", g, n, LinReg (e1, e2, e3))) |||
     (afun2_sf "fit" >>: fun ((g, n), e1, e2) ->
        StatefulFun (make_typ "season_fit", g, n, LinReg (expr_one (), e1, e2))) |||
     (afun3v_sf "season_fit_multi" >>: fun ((g, n), e1, e2, e3, e4s) ->
        StatefulFun (make_typ "season_fit_multi", g, n, MultiLinReg (e1, e2, e3, e4s))) |||
     (afun2v_sf "fit_multi" >>: fun ((g, n), e1, e2, e3s) ->
        StatefulFun (make_typ "season_fit_multi", g, n, MultiLinReg (expr_one (), e1, e2, e3s))) |||
     (afun2_sf "smooth" >>: fun ((g, n), e1, e2) ->
        StatefulFun (make_typ "smooth", g, n, ExpSmooth (e1, e2))) |||
     (afun1_sf "smooth" >>: fun ((g, n), e) ->
        let alpha =
          Const (make_typ "alpha", VFloat 0.5) in
        StatefulFun (make_typ "smooth", g, n, ExpSmooth (alpha, e))) |||
     (afun3_sf "remember" >>: fun ((g, n), tim, dur, e) ->
        (* If we allowed a list of expressions here then it would be ambiguous
         * with the following "3+v" signature: *)
        let fpr = of_float 0.015 in
        StatefulFun (make_typ "remember", g, n,
                     Remember (fpr, tim, dur, [e]))) |||
     (afun3v_sf "remember" >>: fun ((g, n), fpr, tim, dur, es) ->
        StatefulFun (make_typ "remember", g, n,
                     Remember (fpr, tim, dur, es))) |||
     (afun0v_sf ~def_state:LocalState "distinct" >>: fun ((g, n), es) ->
         StatefulFun (make_typ "distinct", g, n, Distinct es)) |||
     (afun3_sf "hysteresis" >>: fun ((g, n), value, accept, max) ->
        StatefulFun (make_typ "hysteresis", g, n,
                     Hysteresis (value, accept, max))) |||
     (afun4_sf ~def_state:LocalState "histogram" >>:
      fun ((g, n), what, min, max, num_buckets) ->
        match float_of_const min,
              float_of_const max,
              int_of_const num_buckets with
        | Some min, Some max, Some num_buckets ->
            if num_buckets <= 0 then
              raise (Reject "Histogram size must be positive") ;
            StatefulFun (make_typ "histogram",
                         g, n, AggrHistogram (what, min, max, num_buckets))
        | _ -> raise (Reject "histogram dimensions must be constants")) |||
     (afun2 "split" >>: fun (e1, e2) ->
        GeneratorFun (make_typ "split", Split (e1, e2))) |||
     (afun2 "format_time" >>: fun (e1, e2) ->
        StatelessFun2 (make_typ "format_time", Strftime, e1, e2)) |||
     (afun1 "parse_time" >>: fun e ->
        StatelessFun1 (make_typ "parse_time", Strptime, e)) |||
     (afun1 "variant" >>: fun e ->
        StatelessFun1 (make_typ "variant", Variant, e)) |||
     (* At least 2 args to distinguish from the aggregate functions: *)
     (afun2v "max" >>: fun (e1, e2, e3s) ->
        StatelessFunMisc (make_typ "max", Max (e1 :: e2 :: e3s))) |||
     (afun1v "greatest" >>: fun (e, es) ->
        StatelessFunMisc (make_typ "max", Max (e :: es))) |||
     (afun2v "min" >>: fun (e1, e2, e3s) ->
        StatelessFunMisc (make_typ "min", Min (e1 :: e2 :: e3s))) |||
     (afun1v "least" >>: fun (e, es) ->
        StatelessFunMisc (make_typ "min", Min (e :: es))) |||
     (afun2 "get" >>: fun (n, v) ->
        Option.may (fun n ->
          if n < 0 then
            raise (Reject "GET index must be positive")
        ) (int_of_const n) ;
        StatelessFun2 (make_typ "get", VecGet, n, v)) |||
     (afun1v "print" >>: fun (e, es) ->
        StatelessFunMisc (make_typ "print", Print (e :: es))) |||
     (afun2 "reldiff" >>: fun (e1, e2) ->
       StatelessFun2 (make_typ "reldiff", Reldiff, e1, e2)) |||
     (afun2_sf "sample" >>: fun ((g, n), c, e) ->
        StatefulFun (make_typ "sample", g, n, Sample (c, e))) |||
     k_moveavg ||| cast ||| top_expr ||| nth ||| last) m

  and cast m =
    let m = "cast" :: m in
    let sep = check (char '(') ||| blanks in
    (RamenTypes.Parser.scalar_typ +- sep ++
     highestest_prec >>: fun (t, e) ->
       (* The nullability of [value] should propagate to [type(value)],
        * while [type?(value)] should be nullable no matter what. *)
       let name = "cast to "^ IO.to_string RamenTypes.print_typ t in
       StatelessFun1 (make_typ name, Cast t, e)
    ) m

  and k_moveavg m =
    let m = "k-moving average" :: m in
    let sep = check (char '(') ||| blanks in
    ((unsigned_decimal_number >>: RamenTypes.Parser.narrowest_int_scalar) +-
     (strinG "-moveavg" ||| strinG "-ma") ++
     state_and_nulls +-
     sep ++ highestest_prec >>: fun ((k, (g, n)), e) ->
       if k = VNull then raise (Reject "Cannot use NULL here") ;
       let k = Const (make_typ "moving average order", k) in
       StatefulFun (make_typ "moveavg", g, n, MovingAvg (expr_one (), k, e))) m

  and top_expr m =
    let m = "top expression" :: m in
    (((strinG "rank" -- blanks -- strinG "of" >>: fun () -> true) |||
      (strinG "is" >>: fun () -> false)) +- blanks ++
     (* We can allow lowest precedence expressions here because of the
      * keywords that follow: *)
     several ~sep:list_sep p +- blanks +-
     strinG "in" +- blanks +- strinG "top" +- blanks ++ (const ||| param) ++
     optional ~def:None (
      some (blanks -- strinG "over" -- blanks -+ p)) ++
     state_and_nulls ++
     optional ~def:(expr_one ()) (
       blanks -- strinG "by" -- blanks -+ highestest_prec) ++
     optional ~def:(expr_zero ()) (
       blanks -- strinG "at" -- blanks -- strinG "time" -- blanks -+ p) ++
     optional ~def:(expr_1hour ()) (
       blanks -- strinG "for" --
       optional ~def:() (blanks -- strinG "the" -- blanks -- strinG "last") --
       blanks -+ (const ||| param)) >>:
     fun (((((((want_rank, what), c), max_size),
             (g, n)), by), time), duration) ->
       StatefulFun (
         (if want_rank then make_typ "rank in top"
                       (* same nullability as what+by+time: *)
                       else make_typ "is in top"),
         g, n,
         Top { want_rank ; c ; max_size ; what ; by ; duration ; time })) m

  and last m =
    let m = "last expression" :: m in
    (
      (* The quantity N disambiguates from the "last" aggregate. *)
      strinG "last" -- blanks -+ p ++
      state_and_nulls +- opt_blanks ++ p ++
      optional ~def:[] (
        blanks -- strinG "by" -- blanks -+
        several ~sep:list_sep p) >>: fun (((c, (g, n)), e), es) ->
      (* We cannot check that c is_constant yet since fields have not
       * been assigned yet. *)
      (* The result is null when the number of input is less than c: *)
      StatefulFun (make_typ "last", g, n, Last (c, e, es))
    ) m

  and nth m =
    let m = "n-th" :: m in
    let q =
      pos_decimal_integer "nth" ++
      (that_string "th" ||| that_string "st" ||| that_string "nd") >>:
      fun (n, th) ->
        if n = 0 then raise (Reject "tuple indices start at 1") ;
        if ordinal_suffix n = th then n
        (* Pedantic but also helps disambiguating the syntax: *)
        else raise (Reject ("bad suffix "^ th ^" for "^ string_of_int n))
    and sep = check (char '(') ||| blanks in
    (q +- sep ++ highestest_prec >>: fun (n, es) ->
      StatelessFun1 (make_typ "nth", Nth (n-1), es)) m

  and case m =
    let m = "case" :: m in
    let alt m =
      let m = "case alternative" :: m in
      (strinG "when" -- blanks -+ p +-
       blanks +- strinG "then" +- blanks ++ p >>:
       fun (cd, cs) -> { case_cond = cd ; case_cons = cs }) m
    in
    (strinG "case" -- blanks -+
     several ~sep:blanks alt +- blanks ++
     optional ~def:None (
       strinG "else" -- blanks -+ some p +- blanks) +-
     strinG "end" >>: fun (alts, else_) ->
       Case (make_typ "case", alts, else_)) m

  and if_ m =
    let m = "if" :: m in
    ((strinG "if" -- blanks -+ p +-
      blanks +- strinG "then" +- blanks ++ p ++
      optional ~def:None (
        blanks -- strinG "else" -- blanks -+
        some p) >>:
      fun ((case_cond, case_cons), else_) ->
        Case (make_typ "conditional", [ { case_cond ; case_cons } ], else_)) |||
     (afun2 "if" >>: fun (case_cond, case_cons) ->
        Case (make_typ "conditional", [ { case_cond ; case_cons } ], None)) |||
     (afun3 "if" >>: fun (case_cond, case_cons, else_) ->
        Case (make_typ "conditional", [ { case_cond ; case_cons } ], Some else_))) m

  and coalesce m =
    let m = "coalesce" :: m in
    (
      afun0v "coalesce" >>: function
      | [] -> raise (Reject "empty COALESCE")
      | [_] -> raise (Reject "COALESCE must have at least 2 arguments")
      | r ->
          Coalesce (make_typ "coalesce", r)
    ) m

  and accept_units q =
    q ++ optional ~def:None (opt_blanks -+ some RamenUnits.Parser.p) >>:
      function (e, None) -> e
             | (e, units) -> (typ_of e).units <- units ; e

  and highestest_prec_no_parenthesis m =
    (accept_units (const ||| field ||| null) ||| func ||| coalesce) m

  and highestest_prec m =
    (highestest_prec_no_parenthesis |||
     accept_units (char '(' -- opt_blanks -+ p +- opt_blanks +- char ')') |||
     tuple ||| vector
    ) m

  (* Empty tuples and tuples of arity 1 are disallowed in order not to
   * conflict with parentheses used as grouping symbols: *)
  and tuple m =
    let m = "tuple" :: m in
    (
      char '(' -- opt_blanks -+
      repeat ~min:2 ~sep:RamenTypes.Parser.tup_sep p +-
      opt_blanks +- char ')' >>: fun es ->
        let num_items = List.length es in
        assert (num_items >= 2) ;
        (* Even if all the fields are null the tuple is not null.
         * No immediate tuple can be null. *)
        Tuple (make_typ "tuple", es)
    ) m

  (* Empty vectors are disallowed so we cannot ignore the element type: *)
  and vector m =
    let m = "vector" :: m in
    (
      char '[' -- opt_blanks -+
      several ~sep:RamenTypes.Parser.tup_sep p +-
      opt_blanks +- char ']' >>: fun es ->
        let num_items = List.length es in
        assert (num_items >= 1) ;
        Vector (make_typ "vector", es)
    ) m

  and p m = lowestest_prec_left_assoc m

  (*$= p & ~printer:(test_printer (print false))
    (Ok (Const (typ, VI32 (Stdint.Int32.of_int 13)), (13, []))) \
      (test_p p "13i32{secs^2}" |> replace_typ_in_expr)

    (Ok (Const (typ, VI32 (Stdint.Int32.of_int 13)), (16, []))) \
      (test_p p "13i32 {secs ^ 2}" |> replace_typ_in_expr)

    (Ok (\
      Const (typ, VBool true),\
      (4, [])))\
      (test_p p "true" |> replace_typ_in_expr)

    (Ok (\
      StatelessFun1 (typ, Not, StatelessFun1 (typ, Defined, Field (typ, ref TupleUnknown, "zone_src"))),\
      (16, [])))\
      (test_p p "zone_src IS NULL" |> replace_typ_in_expr)

    (Ok (\
      StatelessFun2 (typ, And, \
        StatelessFun2 (typ, Or, \
          StatelessFun1 (typ, Not, \
            StatelessFun1 (typ, Defined, Field (typ, ref TupleUnknown, "zone_src"))),\
          StatelessFun2 (typ, Eq, Field (typ, ref TupleUnknown, "zone_src"),\
                                  Field (typ, ref TupleUnknown, "z1"))), \
        StatelessFun2 (typ, Or, \
          StatelessFun1 (typ, Not, \
            StatelessFun1 (typ, Defined, Field (typ, ref TupleUnknown, "zone_dst"))),\
          StatelessFun2 (typ, Eq, \
            Field (typ, ref TupleUnknown, "zone_dst"), \
            Field (typ, ref TupleUnknown, "z2")))),\
      (75, [])))\
      (test_p p "(zone_src IS NULL or zone_src = z1) and \\
                 (zone_dst IS NULL or zone_dst = z2)" |> replace_typ_in_expr)

    (Ok (\
      StatelessFun2 (typ, Div, \
        StatefulFun (typ, LocalState, true, AggrSum (\
          Field (typ, ref TupleUnknown, "bytes"))),\
        Field (typ, ref TupleUnknown, "avg_window")),\
      (22, [])))\
      (test_p p "(sum bytes)/avg_window" |> replace_typ_in_expr)

    (Ok (\
      StatelessFun2 (typ, IDiv, \
        Field (typ, ref TupleUnknown, "start"),\
        StatelessFun2 (typ, Mul, \
          Const (typ, VU32 (Uint32.of_int 1_000_000)),\
          Field (typ, ref TupleUnknown, "avg_window"))),\
      (33, [])))\
      (test_p p "start // (1_000_000 * avg_window)" |> replace_typ_in_expr)

    (Ok (\
      StatelessFun2 (typ, Percentile, \
        Field (typ, ref TupleParam, "p"),\
        Field (typ, ref TupleUnknown, "bytes_per_sec")),\
      (26, [])))\
      (test_p p "p percentile bytes_per_sec" |> replace_typ_in_expr)

    (Ok (\
      StatelessFun2 (typ, Gt, \
        StatefulFun (typ, LocalState, true, AggrMax (\
          Field (typ, ref TupleIn, "start"))),\
        StatelessFun2 (typ, Add, \
          Field (typ, ref TupleOut, "start"),\
          StatelessFun2 (typ, Mul, \
            StatelessFun2 (typ, Mul, \
              Field (typ, ref TupleUnknown, "obs_window"),\
              Const (typ, VFloat 1.15)),\
            Const (typ, VU32 (Uint32.of_int 1_000_000))))),\
      (58, [])))\
      (test_p p "max in.start > \\
                 out.start + (obs_window * 1.15) * 1_000_000" |> replace_typ_in_expr)

    (Ok (\
      StatelessFun2 (typ, Mod, \
        Field (typ, ref TupleUnknown, "x"),\
        Field (typ, ref TupleUnknown, "y")),\
      (5, [])))\
      (test_p p "x % y" |> replace_typ_in_expr)

    (Ok ( \
      StatelessFun1 (typ, Abs, \
        StatelessFun2 (typ, Sub, \
          Field (typ, ref TupleUnknown, "bps"), \
          StatefulFun (typ, GlobalState, true, Lag (\
            Const (typ, VU32 Uint32.one), \
            Field (typ, ref TupleUnknown, "bps"))))), \
      (21, []))) \
      (test_p p "abs(bps - lag(1,bps))" |> replace_typ_in_expr)

    (Ok ( \
      StatefulFun (typ, GlobalState, true, Hysteresis (\
        Field (typ, ref TupleUnknown, "value"),\
        Const (typ, VU32 (Uint32.of_int 900)),\
        Const (typ, VU32 (Uint32.of_int 1000)))),\
      (28, [])))\
      (test_p p "hysteresis(value, 900, 1000)" |> replace_typ_in_expr)

    (Ok ( \
      StatelessFun2 (typ, Mul, \
        StatelessFun2 (typ, BitAnd, \
          Const (typ, VU32 (Uint32.of_int 4)), \
          Const (typ, VU32 (Uint32.of_int 4))), \
        Const (typ, VU32 (Uint32.of_int 2))), \
      (9, []))) \
      (test_p p "4 & 4 * 2" |> replace_typ_in_expr)
  *)

  (*$>*)
end

(* Return the expected units for a given expression.
 * Fail if the operation does not accept the arguments units.
 * Returns None if the unit is unknown or if the value cannot have a unit
 * (non-numeric).
 * This is best-effort:
 * - units are not propagated from one conditional consequent to another;
 * - units of a Nth/VecGet is not inferred but in the simplest cases;
 * - units of x**y is not inferred unless y is constant.
 *)
let units_of_expr params units_of_input units_of_output =
  let units_of_params name =
    match List.find (fun param ->
            param.RamenTuple.ptyp.typ_name = name
          ) params with
    | exception Not_found ->
        Printf.sprintf "Unknown parameter %S while looking for units" name |>
        failwith
    | p -> p.RamenTuple.ptyp.units
  in
  let rec uoe ~indent e =
    let char_of_indent = Char.chr (Char.code 'a' + indent) in
    let prefix = Printf.sprintf "%s%c. " (String.make (indent * 2) ' ')
                                         char_of_indent in
    !logger.debug "%sUnits of expression %a...?" prefix (print true) e ;
    let indent = indent + 1 in
    let t = typ_of e in
    if t.units <> None then t.units else
    (match e with
    | Const (_, v) ->
        if RamenTypes.(is_a_num (structure_of v)) then t.units
        else None
    | Field (_, tupref, name) ->
        if tuple_has_type_input !tupref then
          units_of_input name
        else if tuple_has_type_output !tupref then
          units_of_output name
        else if !tupref = TupleParam then
          units_of_params name
        else None
    | Case (_, cas, else_opt) ->
        (* We merely check that the units of the alternatives are either
         * the same of unknown. *)
        List.iter (fun ca -> check_no_units ~indent ca.case_cond) cas ;
        let units_opt = Option.bind else_opt (uoe ~indent) in
        List.map (fun ca -> ca.case_cons) cas |>
        same_units ~indent "Conditional alternatives" units_opt
    | Coalesce (_, es) ->
        same_units ~indent "Coalesce alternatives" None es
    | StatelessFun0 (_, (Now|EventStart|EventStop)) ->
        Some RamenUnits.seconds_since_epoch
    | StatelessFun1 (_, Age, e) ->
        check ~indent e RamenUnits.seconds_since_epoch ;
        Some RamenUnits.seconds
    | StatelessFun1 (_, (Cast _|Abs|Minus|Ceil|Floor|Round), e) -> uoe ~indent e
    | StatelessFun1 (_, Length, e) ->
        check_no_units ~indent e ;
        Some RamenUnits.chars
    | StatelessFun1 (_, Nth n, Tuple (_, es)) ->
        (* Not super useful. FIXME: use the solver. *)
        (try List.at es n |> uoe ~indent
        with Invalid_argument _ -> None)
    | StatelessFun1 (_, Sqrt, e) ->
        Option.map (fun e -> RamenUnits.pow e 0.5) (uoe ~indent e)
    | StatelessFun2 (_, Add, e1, e2) ->
        option_map2 RamenUnits.add (uoe ~indent e1) (uoe ~indent e2)
    | StatelessFun2 (_, Sub, e1, e2) ->
        option_map2 RamenUnits.sub (uoe ~indent e1) (uoe ~indent e2)
    | StatelessFun2 (_, (Mul|Mod), e1, e2) ->
        option_map2 RamenUnits.mul (uoe ~indent e1) (uoe ~indent e2)
    | StatelessFun2 (_, (Div|IDiv), e1, e2) ->
        option_map2 RamenUnits.div (uoe ~indent e1) (uoe ~indent e2)
    | StatelessFun2 (_, Pow, e1, e2) ->
        (* Best effort in case the exponent is a constant, otherwise we
         * just don't know what the unit is. *)
        option_map2 RamenUnits.pow (uoe ~indent e1) (float_of_const e2)
    (* Although shifts could be seen as mul/div, we'd rather consider
     * only dimensionless values receive this treatment, esp. since
     * it's not possible to distinguish between a mul and div. *)
    | StatelessFun2 (_, (And|Or|Concat|StartsWith|EndsWith|
                         BitAnd|BitOr|BitXor|BitShift), e1, e2) ->
        check_no_units ~indent e1 ;
        check_no_units ~indent e2 ;
        None
    | StatelessFun2 (_, VecGet, e1, Vector (_, es)) ->
        Option.bind (int_of_const e1) (fun n ->
          List.at es n |> uoe ~indent)
    | StatelessFun2 (_, Percentile, _,
                     StatefulFun (_, _, _, (Last (_, e, _)|Sample (_, e)|
                                  Group e))) ->
        uoe ~indent e
    | StatelessFunMisc (_, Like (e, _)) ->
        check_no_units ~indent e ;
        None
    | StatelessFunMisc (_, (Max es|Min es)) ->
        same_units ~indent "Min/Max alternatives" None es
    | StatelessFunMisc (_, Print es) when es <> [] -> uoe ~indent (List.hd es)
    | StatefulFun (_, _, _, (AggrMin e|AggrMax e|AggrAvg e|AggrFirst e|
                             AggrLast e|Lag (_, e)|MovingAvg (_, _, e)|
                             LinReg (_, _, e)|MultiLinReg (_, _, e, _)|
                             ExpSmooth (_, e))) -> uoe ~indent e
    | StatefulFun (_, _, _, AggrSum e) ->
        let u = uoe ~indent e in
        check_not_rel e u ;
        u
    | GeneratorFun (_, Split (e1, e2)) ->
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

