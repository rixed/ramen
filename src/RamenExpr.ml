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

(*$inject
  open TestHelpers
  open RamenLang
  open Stdint
*)

(* Each expression come with a type attached. Starting at None, types are
 * progressively set at compilation. *)
(* TODO: remove all type from there and do all the typing from the external
 * solver. *)
type typ =
  { mutable expr_name : string ;
    uniq_num : int ; (* to build var names, record field names or identify SAT variables *)
    mutable nullable : bool option ;
    (* TODO: rename scalar_typ to structure: *)
    mutable scalar_typ : RamenTypes.structure option }

let same_type t1 t2 =
  t1.nullable = t2.nullable && t1.scalar_typ = t2.scalar_typ

let print_typ oc typ =
  Printf.fprintf oc "%s of %s%s"
    typ.expr_name
    (match typ.scalar_typ with
    | None -> "unknown type"
    | Some s -> "type "^ IO.to_string RamenTypes.print_structure s)
    (match typ.nullable with
    | None -> ", maybe nullable"
    | Some true -> ", nullable"
    | Some false -> ", not nullable")

let uniq_num_seq = ref 0

let make_typ ?nullable ?typ expr_name =
  incr uniq_num_seq ;
  { expr_name ; nullable ; scalar_typ = typ ; uniq_num = !uniq_num_seq }

let make_bool_typ ?nullable name =
  make_typ ?nullable ~typ:TBool name

let make_float_typ ?nullable name =
  make_typ ?nullable ~typ:TFloat name

let make_string_typ ?nullable name =
  make_typ ?nullable ~typ:TString name

let make_num_typ ?nullable name =
  make_typ ?nullable ~typ:TNum name (* will be enlarged as required *)

let make_int_typ ?nullable name =
  make_typ ?nullable ~typ:TNum name (* TODO: we should have a TInt *)

let copy_typ ?name typ =
  let expr_name = name |? typ.expr_name in
  incr uniq_num_seq ;
  { typ with expr_name ; uniq_num = !uniq_num_seq }

let is_an_ip t =
  match t.scalar_typ with
  | Some x -> RamenTypes.is_an_ip x
  | _ -> false

let is_an_int t =
  match t.scalar_typ with
  | Some x -> RamenTypes.is_an_int x
  | _ -> false

(* Stateful function can have either a unique global state a one state per
 * aggregation group (local). Each function has its own default (functions
 * that tends to be used mostly for aggregation have a local default state,
 * while others have a global state), but you can select explicitly using
 * the "locally" and "globally" keywords. For instance: "sum globally 1". *)
type state_lifespan = LocalState | GlobalState

type skip_nulls = bool

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
   * skip_nulls is a flag (default: true) controlling whether the operator
   * should not update its state on NULL values. This is valid regardless of
   * the nullability of that parameters (or we would need a None default).
   * This does not change the nullability of the result of the operator,
   * as even when NULLs are skipped the result can still be NULL, when all
   * inputs were NULLs.
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

and stateless_fun0 =
  | Now
  | Random
  | EventStart
  | EventStop

and stateless_fun1 =
  (* TODO: Other functions: date_part... *)
  | Age
  | Cast
  (* String functions *)
  | Length
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
  (* Same as Nth but only for vectors/lists (accepts non constant index, and
   * indices start at 0) *)
  | VecGet
  (* For network address range test membership, or for an efficient constant
   * set membership test, or for a non-efficient sequence of OR kind of
   * membership test if the set is not constant: *)
  | In
  (* Takes format then time: *)
  | Strftime

and case_alternative =
  { case_cond : t (* Must be bool *) ;
    case_cons : t (* All alternatives must share a type *) }

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
  | AggrHistogram of t * float * float * int
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
  | Top of { want_rank : bool ; n : t ; what : t list ; by : t ;
             time : t ; duration : t }
  (* Last N e1 [BY e2, e3...] - or by arrival.
   * Note: BY followed by more than one expression will require to parentheses
   * the whole expression to avoid ambiguous parsing. *)
  | Last of int * t * t list
  (* Build a list with all values from the group *)
  | Group of t

and generator_fun =
  (* First function returning more than once (Generator). Here the typ is
   * type of a single value but the function is a generator and can return
   * from 0 to N such values. *)
  | Split of t * t

let expr_true =
  Const (make_bool_typ ~nullable:false "true", VBool true)

let expr_false =
  Const (make_bool_typ ~nullable:false "false", VBool false)

let expr_u8 name n =
  Const (make_typ ~typ:TU8 ~nullable:false name, VU8 (Uint8.of_int n))

let expr_float name n =
  Const (make_typ ~typ:TFloat ~nullable:false name, VFloat n)

let expr_zero = expr_u8 "zero" 0
let expr_one = expr_u8 "one" 1
let expr_two = expr_u8 "two" 2
let expr_three = expr_u8 "three" 3
let expr_1hour = expr_float "1hour" 3600.

let of_float v =
  Const (make_typ ~nullable:false (string_of_float v), VFloat v)

let is_true = function
  | Const (_ , VBool true) -> true
  | _ -> false

let string_of_const = function
  | Const (_ , VString s) -> Some s
  | _ -> None

let float_of_const = function
  | Const (_, v) -> Some (RamenTypes.float_of_scalar v)
  | _ -> None

let int_of_const = function
  | Const (_, v) -> Some (RamenTypes.int_of_scalar v)
  | _ -> None

let is_const = function
  | Const _ -> true | _ -> false

let check_const what e =
  if not (is_const e) then
    raise (SyntaxError (NotConstant what))

let rec print with_types oc =
  let add_types t =
    if with_types then Printf.fprintf oc " [%a]" print_typ t
  and sl =
    (* TODO: do not display the default *)
    function LocalState -> " locally " | GlobalState -> " globally "
  and sn =
    function true -> " skip nulls " | false -> " no skip nulls "
  and print_args =
    List.print ~first:"(" ~last:")" ~sep:", " (print with_types)
  in
  function
  | Const (t, c) ->
    RamenTypes.print oc c ; add_types t
  | Tuple (t, es) ->
    List.print ~first:"(" ~last:")" ~sep:"; " (print with_types) oc es ;
    add_types t
  | Vector (t, es) ->
    List.print ~first:"[" ~last:"]" ~sep:"; " (print with_types) oc es ;
    add_types t
  | Field (t, tuple, field) ->
    Printf.fprintf oc "%s.%s" (string_of_prefix !tuple) field ;
    add_types t
  | StateField (t, s) ->
    String.print oc s ; add_types t
  | Case (t, alts, else_) ->
    let print_alt oc alt =
      Printf.fprintf oc "WHEN %a THEN %a"
        (print with_types) alt.case_cond
        (print with_types) alt.case_cons
    in
    Printf.fprintf oc "CASE %a "
     (List.print ~first:"" ~last:"" ~sep:" " print_alt) alts ;
    Option.may (fun else_ ->
      Printf.fprintf oc "ELSE %a "
        (print with_types) else_) else_ ;
    Printf.fprintf oc "END" ;
    add_types t
  | Coalesce (t, es) ->
    Printf.fprintf oc "COALESCE %a" print_args es ;
    add_types t
  | StatelessFun1 (t, Age, e) ->
    Printf.fprintf oc "age (%a)" (print with_types) e ; add_types t
  | StatelessFun0 (t, Now) ->
    Printf.fprintf oc "now" ; add_types t
  | StatelessFun0 (t, Random) ->
    Printf.fprintf oc "random" ; add_types t
  | StatelessFun0 (t, EventStart) ->
    Printf.fprintf oc "#start" ; add_types t
  | StatelessFun0 (t, EventStop) ->
    Printf.fprintf oc "#stop" ; add_types t
  | StatelessFun1 (t, Cast, e) ->
    Printf.fprintf oc "cast(%a, %a)"
      RamenTypes.print_structure (Option.get t.scalar_typ)
      (print with_types) e ;
    add_types t
  | StatelessFun1 (t, Length, e) ->
    Printf.fprintf oc "length (%a)" (print with_types) e ; add_types t
  | StatelessFun1 (t, Lower, e) ->
    Printf.fprintf oc "lower (%a)" (print with_types) e ; add_types t
  | StatelessFun1 (t, Upper, e) ->
    Printf.fprintf oc "upper (%a)" (print with_types) e ; add_types t
  | StatelessFun1 (t, Not, e) ->
    Printf.fprintf oc "NOT (%a)" (print with_types) e ; add_types t
  | StatelessFun1 (t, Abs, e) ->
    Printf.fprintf oc "ABS (%a)" (print with_types) e ; add_types t
  | StatelessFun1 (t, Minus, e) ->
    Printf.fprintf oc "-(%a)" (print with_types) e ; add_types t
  | StatelessFun1 (t, Defined, e) ->
    Printf.fprintf oc "(%a) IS NOT NULL" (print with_types) e ;
    add_types t
  | StatelessFun2 (t, Add, e1, e2) ->
    Printf.fprintf oc "(%a) + (%a)"
      (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatelessFun2 (t, Sub, e1, e2) ->
    Printf.fprintf oc "(%a) - (%a)"
      (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatelessFun2 (t, Mul, e1, e2) ->
    Printf.fprintf oc "(%a) * (%a)"
      (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatelessFun2 (t, Div, e1, e2) ->
    Printf.fprintf oc "(%a) / (%a)"
      (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatelessFun2 (t, Reldiff, e1, e2) ->
    Printf.fprintf oc "reldiff((%a), (%a))"
      (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatelessFun2 (t, IDiv, e1, e2) ->
    Printf.fprintf oc "(%a) // (%a)"
      (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatelessFun2 (t, Mod, e1, e2) ->
    Printf.fprintf oc "(%a) %% (%a)"
      (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatelessFun2 (t, Pow, e1, e2) ->
    Printf.fprintf oc "(%a) ^ (%a)"
      (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatelessFun1 (t, Exp, e) ->
    Printf.fprintf oc "exp (%a)" (print with_types) e ; add_types t
  | StatelessFun1 (t, Log, e) ->
    Printf.fprintf oc "log (%a)" (print with_types) e ; add_types t
  | StatelessFun1 (t, Log10, e) ->
    Printf.fprintf oc "log10 (%a)" (print with_types) e ; add_types t
  | StatelessFun1 (t, Sqrt, e) ->
    Printf.fprintf oc "sqrt (%a)" (print with_types) e ; add_types t
  | StatelessFun1 (t, Ceil, e) ->
    Printf.fprintf oc "ceil (%a)" (print with_types) e ; add_types t
  | StatelessFun1 (t, Floor, e) ->
    Printf.fprintf oc "floor (%a)" (print with_types) e ; add_types t
  | StatelessFun1 (t, Round, e) ->
    Printf.fprintf oc "round (%a)" (print with_types) e ; add_types t
  | StatelessFun1 (t, Hash, e) ->
    Printf.fprintf oc "hash (%a)" (print with_types) e ; add_types t
  | StatelessFun1 (t, Sparkline, e) ->
    Printf.fprintf oc "sparkline (%a)" (print with_types) e ;
    add_types t
  | StatelessFun2 (t, In, e1, e2) ->
    Printf.fprintf oc "(%a) IN (%a)"
      (print with_types) e1 (print with_types) e2 ; add_types t
  | StatelessFun1 (t, (BeginOfRange|EndOfRange as op), e) ->
    Printf.fprintf oc "%s of (%a)"
      (if op = BeginOfRange then "begin" else "end")
      (print with_types) e ;
    add_types t
  | StatelessFun1 (t, Nth n, e) ->
    let n = n + 1 in
    Printf.fprintf oc "%d%s %a"
      n (ordinal_suffix n) (print with_types) e ;
    add_types t
  | StatelessFun1 (t, Strptime, e) ->
    Printf.fprintf oc "parse_time (%a)" (print with_types) e ;
    add_types t
  | StatelessFun2 (t, And, e1, e2) ->
    Printf.fprintf oc "(%a) AND (%a)"
      (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatelessFun2 (t, Or, e1, e2) ->
		Printf.fprintf oc "(%a) OR (%a)"
      (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatelessFun2 (t, Ge, e1, e2) ->
		Printf.fprintf oc "(%a) >= (%a)"
      (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatelessFun2 (t, Gt, e1, e2) ->
		Printf.fprintf oc "(%a) > (%a)"
      (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatelessFun2 (t, Eq, e1, e2) ->
		Printf.fprintf oc "(%a) = (%a)"
      (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatelessFun2 (t, Concat, e1, e2) ->
		Printf.fprintf oc "(%a) || (%a)"
      (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatelessFun2 (t, StartsWith, e1, e2) ->
		Printf.fprintf oc "(%a) STARTS WITH (%a)"
      (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatelessFun2 (t, EndsWith, e1, e2) ->
		Printf.fprintf oc "(%a) ENDS WITH (%a)"
      (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatelessFun2 (t, Strftime, e1, e2) ->
		Printf.fprintf oc "format_time (%a, %a)"
      (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatelessFun2 (t, BitAnd, e1, e2) ->
		Printf.fprintf oc "(%a) & (%a)"
      (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatelessFun2 (t, BitOr, e1, e2) ->
		Printf.fprintf oc "(%a) | (%a)"
      (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatelessFun2 (t, BitXor, e1, e2) ->
		Printf.fprintf oc "(%a) ^ (%a)"
      (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatelessFun2 (t, VecGet, e1, e2) ->
		Printf.fprintf oc "get(%a, %a)"
      (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatelessFunMisc (t, Like (e, p)) ->
		Printf.fprintf oc "(%a) LIKE %S"
      (print with_types) e p ;
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
    Printf.fprintf oc "min%s%s(%a)"
      (sl g) (sn n) (print with_types) e ;
    add_types t
  | StatefulFun (t, g, n, AggrMax e) ->
    Printf.fprintf oc "max%s%s(%a)"
      (sl g) (sn n) (print with_types) e ;
    add_types t
  | StatefulFun (t, g, n, AggrSum e) ->
    Printf.fprintf oc "sum%s%s(%a)"
      (sl g) (sn n) (print with_types) e ;
    add_types t
  | StatefulFun (t, g, n, AggrAvg e) ->
    Printf.fprintf oc "avg%s%s(%a)"
      (sl g) (sn n) (print with_types) e ;
    add_types t
  | StatefulFun (t, g, n, AggrAnd e) ->
    Printf.fprintf oc "and%s%s(%a)"
      (sl g) (sn n) (print with_types) e ;
    add_types t
  | StatefulFun (t, g, n, AggrOr e) ->
    Printf.fprintf oc "or%s%s(%a)"
      (sl g) (sn n) (print with_types) e ;
    add_types t
  | StatefulFun (t, g, n, AggrFirst e) ->
    Printf.fprintf oc "first%s%s(%a)"
      (sl g) (sn n) (print with_types) e ;
    add_types t
  | StatefulFun (t, g, n, AggrLast e) ->
    Printf.fprintf oc "last%s%s(%a)"
      (sl g) (sn n) (print with_types) e ;
    add_types t
  | StatefulFun (t, g, n, AggrPercentile (p, e)) ->
    Printf.fprintf oc "%ath percentile%s%s(%a)"
      (print with_types) p (sl g) (sn n) (print with_types) e ;
    add_types t
  | StatefulFun (t, g, n, AggrHistogram (what, min, max, num_buckets)) ->
    Printf.fprintf oc "histogram%s%s(%a, %g, %g, %d)"
      (sl g) (sn n)
      (print with_types) what min max num_buckets ;
    add_types t
  | StatefulFun (t, g, n, Lag (e1, e2)) ->
    Printf.fprintf oc "lag%s%s(%a, %a)"
      (sl g) (sn n)
      (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatefulFun (t, g, n, MovingAvg (e1, e2, e3)) ->
    Printf.fprintf oc "season_moveavg%s%s(%a, %a, %a)"
      (sl g) (sn n)
      (print with_types) e1
      (print with_types) e2 (print with_types) e3 ;
    add_types t
  | StatefulFun (t, g, n, LinReg (e1, e2, e3)) ->
    Printf.fprintf oc "season_fit%s%s(%a, %a, %a)"
      (sl g) (sn n)
      (print with_types) e1
      (print with_types) e2 (print with_types) e3 ;
    add_types t
  | StatefulFun (t, g, n, MultiLinReg (e1, e2, e3, e4s)) ->
    Printf.fprintf oc "season_fit_multi%s%s(%a, %a, %a, %a)"
      (sl g) (sn n)
      (print with_types) e1
      (print with_types) e2
      (print with_types) e3
      print_args e4s ;
    add_types t
  | StatefulFun (t, g, n, Remember (fpr, tim, dur, es)) ->
    Printf.fprintf oc "remember%s%s(%a, %a, %a, %a)"
      (sl g) (sn n)
      (print with_types) fpr
      (print with_types) tim
      (print with_types) dur
      print_args es ;
    add_types t
  | StatefulFun (t, g, n, Distinct es) ->
    Printf.fprintf oc "distinct%s%s(%a)"
      (sl g) (sn n)
      print_args es
  | StatefulFun (t, g, n, ExpSmooth (e1, e2)) ->
    Printf.fprintf oc "smooth%s%s(%a, %a)"
      (sl g) (sn n) (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatefulFun (t, g, n, Hysteresis (meas, accept, max)) ->
    Printf.fprintf oc "hysteresis%s%s(%a, %a, %a)"
      (sl g) (sn n)
      (print with_types) meas
      (print with_types) accept
      (print with_types) max ;
    add_types t
  | StatefulFun (t, g, n, Top { want_rank ; n = c ; what ; by ; time ;
                                duration }) ->
    Printf.fprintf oc "%s %a in top %a %s%sby %a in the last %a at time %a"
      (if want_rank then "rank of" else "is")
      (List.print ~first:"" ~last:"" ~sep:", " (print with_types)) what
      (print with_types) c
      (sl g) (sn n)
      (print with_types) by
      (print with_types) duration
      (print with_types) time ;
    add_types t
  | StatefulFun (t, g, n, Last (c, e, es) ) ->
    let print_by oc es =
      if es <> [] then
        Printf.fprintf oc " BY %a"
          (List.print ~first:"" ~last:"" ~sep:", " (print with_types)) es in
    Printf.fprintf oc "LAST %d %s%s%a%a"
      c (sl g) (sn n)
      (print with_types) e
      print_by es ;
    add_types t
  | StatefulFun (t, g, n, Group e) ->
    Printf.fprintf oc "GROUP%s%s %a"
      (sl g) (sn n)
      (print with_types) e ;
    add_types t

  | GeneratorFun (t, Split (e1, e2)) ->
    Printf.fprintf oc "split(%a, %a)"
      (print with_types) e1 (print with_types) e2 ;
    add_types t

let typ_of = function
  | Const (t, _) | Tuple (t, _) | Vector (t, _) | Field (t, _, _)
  | StateField (t, _) | StatelessFun0 (t, _) | StatelessFun1 (t, _, _)
  | StatelessFun2 (t, _, _, _) | StatelessFunMisc (t, _)
  | StatefulFun (t, _, _, _) | GeneratorFun (t, _) | Case (t, _, _)
  | Coalesce (t, _) -> t

let is_nullable e =
  let t = typ_of e in
  t.nullable = Some true

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

  | StatefulFun (_, _, _, AggrPercentile (e1, e2))
  | StatelessFun2 (_, _, e1, e2)
  | StatefulFun (_, _, _, Lag (e1, e2))
  | StatefulFun (_, _, _, ExpSmooth (e1, e2))
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
      Top { n = e1 ; by = e2 ; time = e3 ; duration = e4 ; what = e5s }) ->
      List.fold_left f i (e1::e2::e3::e4::e5s)

  | StatefulFun (_, _, _, Last (_, e, es)) ->
      List.fold_left f i (e::es)

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

module Parser =
struct
  type expr = t
  (*$< Parser *)
  open RamenParsing

  (* Single things *)
  let const m =
    let m = "constant" :: m in
    (
      RamenTypes.Parser.scalar ~min_int_width:32 ++
       optional ~def:false (
         char ~case_sensitive:false 'n' >>: fun _ -> true) >>:
       fun (c, nullable) ->
         Const (make_typ ~nullable ~typ:(RamenTypes.structure_of c)
          "constant", c)
    ) m

  (*$= const & ~printer:(test_printer (print false))
    (Ok (Const (typ, VBool true), (4, [])))\
      (test_p const "true" |> replace_typ_in_expr)

    (Ok (Const (typ, VI8 (Stdint.Int8.of_int 15)), (4, []))) \
      (test_p const "15i8" |> replace_typ_in_expr)

    (Ok (Const (typn, VI8 (Stdint.Int8.of_int 15)), (5, []))) \
      (test_p const "15i8n" |> replace_typ_in_expr)
  *)

  let null m =
    (RamenTypes.Parser.null >>: fun v ->
      (* Type of "NULL" is unknown yet *)
      Const (make_typ ~nullable:true "NULL", v)
    ) m

  let field m =
    let m = "field" :: m in
    ((parse_prefix ~def:TupleUnknown ++ non_keyword >>:
      fun (tuple, field) ->
        (* This is important here that the type name is the raw field name,
         * because we use the tuple field type name as their identifier (unless
         * it's a virtual field (starting with #) of course since those are
         * computed on the fly and have no corresponding variable in the
         * tuple) *)
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
        Some { where = ParsersMisc.Item ((1,8), '.');\
               what=["eof"]})))\
      (test_p field "pasglop.bytes" |> replace_typ_in_expr)

    (Ok (\
      Field (typ, ref TupleIn, "#count"),\
      (6, [])))\
      (test_p field "#count" |> replace_typ_in_expr)

    (Bad (\
      NoSolution (\
        Some { where = ParsersMisc.Item ((1,8), 'c') ;\
               what = ["\"#successive\"";"field"]})))\
      (test_p field "first.#count" |> replace_typ_in_expr)
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
    let skip = strinG "skip" -- blanks -- strinGs "null" in
    (
      (skip >>: fun () -> true) |||
      (strinG "no" -- blanks -- skip >>: fun () -> false)
    ) m

  let state_and_nulls ?(def_state=GlobalState)
                      ?(def_skipnulls=true) m =
    (
      optional ~def:def_state (blanks -+ state_lifespan) ++
      optional ~def:def_skipnulls (blanks -+ skip_nulls)
    ) m

  (* operators with lowest precedence *)
  let rec lowest_prec_left_assoc m =
    let m = "logical operator" :: m in
    let op = that_string "and" ||| that_string "or"
    and reduce e1 op e2 = match op with
      | "and" -> StatelessFun2 (make_bool_typ "and", And, e1, e2)
      | "or" -> StatelessFun2 (make_bool_typ "or", Or, e1, e2)
      | _ -> assert false in
    (* FIXME: we do not need a blanks if we had parentheses ("(x)AND(y)" is OK) *)
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
      | ">" -> StatelessFun2 (make_bool_typ "comparison (>)", Gt, e1, e2)
      | "<" -> StatelessFun2 (make_bool_typ "comparison (<)", Gt, e2, e1)
      | ">=" -> StatelessFun2 (make_bool_typ "comparison (>=)", Ge, e1, e2)
      | "<=" -> StatelessFun2 (make_bool_typ "comparison (<=)", Ge, e2, e1)
      | "=" -> StatelessFun2 (make_bool_typ "equality", Eq, e1, e2)
      | "!=" | "<>" ->
        StatelessFun1 (make_bool_typ "not", Not,
          StatelessFun2 (make_bool_typ "equality", Eq, e1, e2))
      | "in" -> StatelessFun2 (make_bool_typ "in", In, e1, e2)
      | "like" ->
        (match string_of_const e2 with
        | None -> raise (Reject "LIKE pattern must be a string constant")
        | Some p ->
          StatelessFunMisc (make_bool_typ "like", Like (e1, p)))
      | "starts" -> StatelessFun2 (make_bool_typ "starts with", StartsWith, e1, e2)
      | "ends" -> StatelessFun2 (make_bool_typ "ends with", EndsWith, e1, e2)
      | _ -> assert false in
    binary_ops_reducer ~op ~term:mid_prec_left_assoc ~sep:opt_blanks ~reduce m

  and mid_prec_left_assoc m =
    let m = "arithmetic operator" :: m in
    let op = that_string "+" ||| that_string "-" ||| that_string "||"
    and reduce e1 op e2 = match op with
      | "+" -> StatelessFun2 (make_num_typ "addition", Add, e1, e2)
      | "-" -> StatelessFun2 (make_num_typ "subtraction", Sub, e1, e2)
      | "||" -> StatelessFun2 (make_string_typ "concatenation", Concat, e1, e2)
      | _ -> assert false in
    binary_ops_reducer ~op ~term:high_prec_left_assoc ~sep:opt_blanks ~reduce m

  and high_prec_left_assoc m =
    let m = "arithmetic operator" :: m in
    let op = that_string "*" ||| that_string "//" ||| that_string "/" ||| that_string "%"
    and reduce e1 op e2 = match op with
      | "*" -> StatelessFun2 (make_num_typ "multiplication", Mul, e1, e2)
      (* Note: We want the default division to output floats by default *)
      (* Note: We reject IP/INT because that's a CIDR *)
      | "//" -> StatelessFun2 (make_num_typ "integer-division", IDiv, e1, e2)
      | "%" -> StatelessFun2 (make_num_typ "modulo", Mod, e1, e2)
      | "/" ->
          (match e1, e2 with
          | Const (t1, _), Const (t2, _) when is_an_ip t1 && is_an_int t2 ->
              raise (Reject "That's a CIDR")
          | _ ->
              StatelessFun2 (make_float_typ "division", Div, e1, e2))
      | _ -> assert false in
    binary_ops_reducer ~op ~term:higher_prec_left_assoc ~sep:opt_blanks ~reduce m

  and higher_prec_left_assoc m =
    let m = "bitwise logical operator" :: m in
    let op = that_string "&" ||| that_string "|" ||| that_string "^"
    and reduce e1 op e2 = match op with
      | "&" -> StatelessFun2 (make_int_typ "bitwise and", BitAnd, e1, e2)
      | "|" -> StatelessFun2 (make_int_typ "bitwise or", BitOr, e1, e2)
      | "^" -> StatelessFun2 (make_int_typ "bitwise xor", BitXor, e1, e2)
      | _ -> assert false in
    binary_ops_reducer ~op ~term:higher_prec_right_assoc ~sep:opt_blanks ~reduce m

  and higher_prec_right_assoc m =
    let m = "arithmetic operator" :: m in
    let op = char '^'
    and reduce e1 _ e2 = StatelessFun2 (make_num_typ "exponentiation", Pow, e1, e2) in
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
        StatelessFun1 (make_bool_typ "not", Not, e)) |||
      (strinG "-" -- opt_blanks --
        check (nay decimal_digit) -+ highestest_prec >>: fun e ->
          StatelessFun1 (make_num_typ "unary minus", Minus, e)) |||
      (highestest_prec ++
        optional ~def:None (
          blanks -- strinG "is" -- blanks -+
          optional ~def:(Some false)
                   (strinG "not" -- blanks >>: fun () -> Some true) +-
          strinG "null") >>: function
            | e, None -> e
            | e, Some false ->
              StatelessFun1 (make_bool_typ ~nullable:false "not", Not,
                StatelessFun1 (make_bool_typ ~nullable:false "is_not_null", Defined, e))
            | e, Some true ->
              StatelessFun1 (make_bool_typ ~nullable:false "is_not_null", Defined, e)) |||
      (strinG "begin" -- blanks -- strinG "of" -- blanks -+ highestest_prec >>:
        fun e -> StatelessFun1 (make_typ "begin of", BeginOfRange, e)) |||
      (strinG "end" -- blanks -- strinG "of" -- blanks -+ highestest_prec >>:
        fun e -> StatelessFun1 (make_typ "end of", EndOfRange, e))
    ) m

  and func m =
    let m = "function" :: m in
    (* Note: min and max of nothing are NULL but sum of nothing is 0, etc *)
    ((afun1 "age" >>: fun e -> StatelessFun1 (make_num_typ "age function", Age, e)) |||
     (afun1 "abs" >>: fun e -> StatelessFun1 (make_num_typ "absolute value", Abs, e)) |||
     (afun1 "length" >>: fun e -> StatelessFun1 (make_typ ~typ:TU16 "length", Length, e)) |||
     (afun1 "lower" >>: fun e -> StatelessFun1 (make_string_typ "lower", Lower, e)) |||
     (afun1 "upper" >>: fun e -> StatelessFun1 (make_string_typ "upper", Upper, e)) |||
     (strinG "now" >>: fun () -> StatelessFun0 (make_float_typ ~nullable:false "now", Now)) |||
     (strinG "random" >>: fun () -> StatelessFun0 (make_float_typ ~nullable:false "random", Random)) |||
     (strinG "#start" >>: fun () -> StatelessFun0 (make_float_typ ~nullable:false "#start", EventStart)) |||
     (strinG "#stop" >>: fun () -> StatelessFun0 (make_float_typ ~nullable:false "#stop", EventStop)) |||
     (afun1 "exp" >>: fun e -> StatelessFun1 (make_float_typ "exponential", Exp, e)) |||
     (afun1 "log" >>: fun e -> StatelessFun1 (make_float_typ "natural logarithm", Log, e)) |||
     (afun1 "log10" >>: fun e -> StatelessFun1 (make_float_typ "common logarithm", Log10, e)) |||
     (afun1 "sqrt" >>: fun e -> StatelessFun1 (make_float_typ "square root", Sqrt, e)) |||
     (afun1 "ceil" >>: fun e -> StatelessFun1 (make_num_typ "ceil", Ceil, e)) |||
     (afun1 "floor" >>: fun e -> StatelessFun1 (make_num_typ "floor", Floor, e)) |||
     (afun1 "round" >>: fun e -> StatelessFun1 (make_num_typ "round", Round, e)) |||
     (afun1 "hash" >>: fun e -> StatelessFun1 (make_typ ~typ:TI64 "hash", Hash, e)) |||
     (afun1 "sparkline" >>: fun e -> StatelessFun1 (make_typ ~typ:TString "sparkline", Sparkline, e)) |||
     (afun1_sf ~def_state:LocalState "min" >>: fun ((g, n), e) ->
        StatefulFun (make_typ "min aggregation", g, n, AggrMin e)) |||
     (afun1_sf ~def_state:LocalState "max" >>: fun ((g, n), e) ->
        StatefulFun (make_typ "max aggregation", g, n, AggrMax e)) |||
     (afun1_sf ~def_state:LocalState "sum" >>: fun ((g, n), e) ->
        StatefulFun (make_num_typ "sum aggregation", g, n, AggrSum e)) |||
     (afun1_sf ~def_state:LocalState "avg" >>: fun ((g, n), e) ->
        StatefulFun (make_float_typ "average", g, n, AggrAvg e)) |||
     (afun1_sf ~def_state:LocalState "and" >>: fun ((g, n), e) ->
        StatefulFun (make_bool_typ "and aggregation", g, n, AggrAnd e)) |||
     (afun1_sf ~def_state:LocalState "or" >>: fun ((g, n), e) ->
        StatefulFun (make_bool_typ "or aggregation", g, n, AggrOr e)) |||
     (afun1_sf ~def_state:LocalState "first" >>: fun ((g, n), e) ->
        StatefulFun (make_typ "first aggregation", g, n, AggrFirst e)) |||
     (afun1_sf ~def_state:LocalState "last" >>: fun ((g, n), e) ->
        StatefulFun (make_typ "last aggregation", g, n, AggrLast e)) |||
     (afun1_sf ~def_state:LocalState "group" >>: fun ((g, n), e) ->
        StatefulFun (make_typ "group aggregation", g, n, Group e)) |||
     (afun1_sf ~def_state:GlobalState "all" >>: fun ((g, n), e) ->
        StatefulFun (make_typ "group aggregation", g, n, Group e)) |||
     ((const ||| param) +- (optional ~def:() (strinG "th")) +- blanks ++
      afun1_sf ~def_state:LocalState "percentile" >>: fun (p, ((g, n), e)) ->
        StatefulFun (make_num_typ "percentile aggregation",
                      g, n, AggrPercentile (p, e))) |||
     (afun2_sf "lag" >>: fun ((g, n), e1, e2) ->
        StatefulFun (make_typ "lag", g, n, Lag (e1, e2))) |||
     (afun1_sf "lag" >>: fun ((g, n), e) ->
        StatefulFun (make_typ "lag", g, n, Lag (expr_one, e))) |||

     (* avg perform a division thus the float type *)
     (afun3_sf "season_moveavg" >>: fun ((g, n), e1, e2, e3) ->
        StatefulFun (make_float_typ "season_moveavg", g, n, MovingAvg (e1, e2, e3))) |||
     (afun2_sf "moveavg" >>: fun ((g, n), e1, e2) ->
        StatefulFun (make_float_typ "season_moveavg", g, n, MovingAvg (expr_one, e1, e2))) |||
     (afun3_sf "season_fit" >>: fun ((g, n), e1, e2, e3) ->
        StatefulFun (make_float_typ "season_fit", g, n, LinReg (e1, e2, e3))) |||
     (afun2_sf "fit" >>: fun ((g, n), e1, e2) ->
        StatefulFun (make_float_typ "season_fit", g, n, LinReg (expr_one, e1, e2))) |||
     (afun3v_sf "season_fit_multi" >>: fun ((g, n), e1, e2, e3, e4s) ->
        StatefulFun (make_float_typ "season_fit_multi", g, n, MultiLinReg (e1, e2, e3, e4s))) |||
     (afun2v_sf "fit_multi" >>: fun ((g, n), e1, e2, e3s) ->
        StatefulFun (make_float_typ "season_fit_multi", g, n, MultiLinReg (expr_one, e1, e2, e3s))) |||
     (afun2_sf "smooth" >>: fun ((g, n), e1, e2) ->
        StatefulFun (make_float_typ "smooth", g, n, ExpSmooth (e1, e2))) |||
     (afun1_sf "smooth" >>: fun ((g, n), e) ->
        let alpha =
          Const (make_float_typ ~nullable:false "alpha", VFloat 0.5) in
        StatefulFun (make_float_typ "smooth", g, n, ExpSmooth (alpha, e))) |||
     (afun3_sf "remember" >>: fun ((g, n), tim, dur, e) ->
        (* If we allowed a list of expressions here then it would be ambiguous
         * with the following "3+v" signature: *)
        let fpr = of_float 0.015 in
        StatefulFun (make_bool_typ "remember", g, n,
                     Remember (fpr, tim, dur, [e]))) |||
     (afun3v_sf "remember" >>: fun ((g, n), fpr, tim, dur, es) ->
        StatefulFun (make_bool_typ "remember", g, n,
                     Remember (fpr, tim, dur, es))) |||
     (afun0v_sf ~def_state:LocalState "distinct" >>: fun ((g, n), es) ->
         StatefulFun (make_bool_typ "distinct", g, n, Distinct es)) |||
     (afun3_sf "hysteresis" >>: fun ((g, n), value, accept, max) ->
        StatefulFun (make_bool_typ "hysteresis", g, n,
                     Hysteresis (value, accept, max))) |||
     (afun4_sf ~def_state:LocalState "histogram" >>:
      fun ((g, n), what, min, max, num_buckets) ->
        match float_of_const min,
              float_of_const max,
              int_of_const num_buckets with
        | Some min, Some max, Some num_buckets ->
            if num_buckets <= 0 then
              raise (Reject "Histogram size must be positive") ;
            let typ =
              (* If a value is null, the whole histogram will be null. If we
               * have an histogram at all then each bucket is a non-null
               * u32: *)
              RamenTypes.(TVec (num_buckets+2, { structure = TU32 ;
                                                nullable = Some false })) in
            StatefulFun (make_typ "histogram" ~typ,
                         g, n, AggrHistogram (what, min, max, num_buckets))
        | _ -> raise (Reject "histogram dimensions must be constants")) |||
     (afun2 "split" >>: fun (e1, e2) ->
        GeneratorFun (make_typ ~typ:TString "split", Split (e1, e2))) |||
     (afun2 "format_time" >>: fun (e1, e2) ->
        StatelessFun2 (make_typ ~typ:TString "format_time", Strftime, e1, e2)) |||
     (afun1 "parse_time" >>: fun e ->
        StatelessFun1 (make_float_typ ~nullable:true "parse_time", Strptime, e)) |||
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
       StatelessFun2 (make_typ ~typ:TFloat "reldiff", Reldiff, e1, e2)) |||
     k_moveavg ||| cast ||| top_expr ||| nth ||| last) m

  and cast m =
    let m = "cast" :: m in
    let sep = check (char '(') ||| blanks in
    (RamenTypes.Parser.scalar_typ +- sep ++
     highestest_prec >>: fun (t, e) ->
       (* The nullability of [value] should propagate to [type(value)],
        * while [type?(value)] should be nullable no matter what. *)
       let nullable = if t.nullable = Some true then Some true else None in
       StatelessFun1 (make_typ ~typ:t.structure ?nullable
        ("cast to "^ IO.to_string RamenTypes.print_typ t), Cast, e)
    ) m

  and k_moveavg m =
    let m = "k-moving average" :: m in
    let sep = check (char '(') ||| blanks in
    ((unsigned_decimal_number >>: RamenTypes.Parser.narrowest_int_scalar) +-
     (strinG "-moveavg" ||| strinG "-ma") ++
     state_and_nulls +-
     sep ++ highestest_prec >>: fun ((k, (g, n)), e) ->
       if k = VNull then raise (Reject "Cannot use NULL here") ;
       let k = Const (make_typ ~nullable:false
                               ~typ:(RamenTypes.structure_of k)
                               "moving average order", k) in
       StatefulFun (make_float_typ "moveavg", g, n, MovingAvg (expr_one, k, e))) m

  and top_expr m =
    let m = "top expression" :: m in
    (((strinG "rank" -- blanks -- strinG "of" >>: fun () -> true) |||
      (strinG "is" >>: fun () -> false)) +- blanks ++
     (* We can allow lowest precedence expressions here because of the
      * keywords that follow: *)
     several ~sep:list_sep p +- blanks +-
     strinG "in" +- blanks +- strinG "top" +- blanks ++
     (const ||| param) ++
     state_and_nulls ++
     optional ~def:expr_one (
       blanks -- strinG "by" -- blanks -+ highestest_prec) ++
     optional ~def:expr_1hour (
       blanks -- strinG "in" -- blanks -- strinG "the" -- blanks --
       strinG "last" -- blanks -+ (const ||| param)) ++
     optional ~def:expr_zero (
       blanks -- strinG "at" -- blanks -- strinG "time" -- blanks -+ p) >>:
     fun ((((((want_rank, what), c), (g, n)), by), duration), time) ->
       StatefulFun (
         (if want_rank then make_int_typ ~nullable:true "rank in top"
                       (* same nullability as what+by+time: *)
                       else make_bool_typ "is in top"),
         g, n,
         Top { want_rank ; n = c ; what ; by ; duration ; time })) m

  and in_count_field =
    (* Have a single one of them to increase the field id by only one: *)
    Field (make_typ ~nullable:false ~typ:TU64 "#count", ref TupleIn, "#count")

  and last m =
    let m = "last expression" :: m in
    (
      strinG "last" -- blanks -+
      pos_decimal_integer "how many last elements" ++
      state_and_nulls +- opt_blanks ++ p ++
      optional ~def:[ in_count_field ] (
        blanks -- strinG "by" -- blanks -+
        several ~sep:list_sep p) >>: fun (((c, (g, n)), e), es) ->
      if c < 0 then
        raise (Reject "LAST number of elements must be greater than zero") ;
      (* The result is null when the number of input is less than n: *)
      StatefulFun (make_typ ~nullable:true "last", g, n, Last (c, e, es))
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
          Coalesce (make_typ ~nullable:false "coalesce", r)
    ) m

  and highestest_prec_no_parenthesis m =
    (const ||| field ||| func ||| null ||| coalesce) m

  and highestest_prec m =
    (highestest_prec_no_parenthesis |||
     char '(' -- opt_blanks -+ p +- opt_blanks +- char ')' |||
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
        let typ =
          RamenTypes.(TTuple (Array.init num_items (fun i ->
            { structure = TAny ; nullable = None }))) in
        (* Even if all the fields are null the tuple is not null.
         * No immediate tuple can be null. *)
        Tuple (make_typ ~nullable:false ~typ "tuple", es)
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
        let typ =
          RamenTypes.(TVec (num_items, { structure = TAny ;
                                         nullable = None })) in
        Vector (make_typ ~nullable:false ~typ "vector", es)
    ) m

  and p m = lowest_prec_left_assoc m

  (*$= p & ~printer:(test_printer (print false))
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
      StatefulFun (typ, LocalState, true, AggrPercentile (\
        Field (typ, ref TupleParam, "p"),\
        Field (typ, ref TupleUnknown, "bytes_per_sec"))),\
      (26, [])))\
      (test_p p "p percentile bytes_per_sec" |> replace_typ_in_expr)

    (Ok (\
      StatelessFun2 (typ, Gt, \
        StatefulFun (typ, LocalState, true, AggrMax (\
          Field (typ, ref TupleLastSelected, "start"))),\
        StatelessFun2 (typ, Add, \
          Field (typ, ref TupleOut, "start"),\
          StatelessFun2 (typ, Mul, \
            StatelessFun2 (typ, Mul, \
              Field (typ, ref TupleUnknown, "obs_window"),\
              Const (typ, VFloat 1.15)),\
            Const (typ, VU32 (Uint32.of_int 1_000_000))))),\
      (69, [])))\
      (test_p p "max selected.last.start > \\
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
