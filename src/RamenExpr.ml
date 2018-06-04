(* This module deals with expressions.
 * Expressions are the flesh of ramen programs.
 * Every expression is typed and all types are scalar.
 *
 * Scalars are parsed in RamenTypes.
 *)
open Batteries
open Stdint
open RamenLang
open RamenHelpers

(*$inject
  open TestHelpers
  open RamenLang
*)

(* Each expression come with a type attached. Starting at None types are
 * progressively set at compilation. *)
type typ =
  { mutable expr_name : string ;
    uniq_num : int ; (* to build var names or record field names *)
    mutable nullable : bool option ;
    mutable scalar_typ : RamenTypes.typ option }

let print_typ fmt typ =
  Printf.fprintf fmt "%s of %s%s"
    typ.expr_name
    (match typ.scalar_typ with
    | None -> "unknown type"
    | Some typ -> "type "^ IO.to_string RamenTypes.print_typ typ)
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
let make_int_typ ?nullable name = make_typ ?nullable ~typ:TI8 name
let copy_typ ?name typ =
  let expr_name = name |? typ.expr_name in
  incr uniq_num_seq ;
  { typ with expr_name ; uniq_num = !uniq_num_seq }

let is_an_ip t =
  match t.scalar_typ with
  | Some (TIpv4|TIpv6|TIp) -> true
  | _ -> false

let is_an_int t =
  match t.scalar_typ with
  | Some (TNum|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128) -> true
  | _ -> false

type state_lifespan = LocalState | GlobalState

(* Expressions on scalars (aka fields) *)

type stateless_fun0 =
  | Now
  | Random

type stateless_fun1 =
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
  | Hash
  (* For network address range checks: *)
  | BeginOfRange
  | EndOfRange
  | Nth of int (* Where the int starts at 0 for the first item *)

type stateless_fun2 =
  (* Binary Ops scalars *)
  | Add
  | Sub
  | Mul
  | Div
  | IDiv
  | Mod
  | Pow
  | And
  | Or
  | Ge
  | Gt
  | Eq
  | Concat
  | BitAnd
  | BitOr
  | BitXor
  (* Same as Nth but only for vectors (accepts non constant index, and
   * indices start at 0) *)
  | VecGet

(* FIXME: when we end prototyping use objects to make it easier to add
 * operations *)
type t =
  | Const of typ * RamenTypes.value
  | Tuple of typ * t list
  | Vector of typ * t list
  | Field of typ * tuple_prefix ref * string (* field name *)
  | StateField of typ * string (* Name of the state field - met only late in the game *)
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
  | StatelessFun0 of typ * stateless_fun0
  | StatelessFun1 of typ * stateless_fun1 * t
  | StatelessFun2 of typ * stateless_fun2 * t * t
  | StatelessFunMisc of typ * stateless_fun_misc
  | StatefulFun of typ * state_lifespan * stateful_fun
  | GeneratorFun of typ * generator_fun

and case_alternative =
  { case_cond : t (* Must be bool *) ;
    case_cons : t (* All alternatives must share a type *) }

and stateless_fun_misc =
  (* a LIKE operator using globs, infix *)
  | Like of t * string (* expression then pattern (using %, _ and \) *)
  (* Min/Max of the given values *)
  | Max of t list
  | Min of t list
  (* For debug *)
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
  | Top of { want_rank : bool ; n : int ; what : t list ; by : t ;
             time : t ; duration : float }

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

let expr_zero = expr_u8 "zero" 0
let expr_one = expr_u8 "one" 1
let expr_two = expr_u8 "two" 2
let expr_three = expr_u8 "three" 3

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

let check_const what = function
  | Const _ -> ()
  | _ -> raise (SyntaxError (NotConstant what))

let print_duration oc d =
  Printf.fprintf oc "%f seconds" d

let rec print with_types fmt =
  let add_types t =
    if with_types then Printf.fprintf fmt " [%a]" print_typ t
  and sl =
    (* TODO: do not display the default *)
    function LocalState -> " locally " | GlobalState -> " globally "
  and print_args =
    List.print ~first:"(" ~last:")" ~sep:", " (print with_types)
  in
  function
  | Const (t, c) ->
    RamenTypes.print fmt c ; add_types t
  | Tuple (t, es) ->
    List.print ~first:"(" ~last:")" ~sep:"; " (print with_types) fmt es ;
    add_types t
  | Vector (t, es) ->
    List.print ~first:"[" ~last:"]" ~sep:"; " (print with_types) fmt es ;
    add_types t
  | Field (t, tuple, field) ->
    Printf.fprintf fmt "%s.%s" (string_of_prefix !tuple) field ;
    add_types t
  | StateField (t, s) ->
    String.print fmt s ; add_types t
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
    Printf.fprintf fmt "COALESCE %a" print_args es ;
    add_types t
  | StatelessFun1 (t, Age, e) ->
    Printf.fprintf fmt "age (%a)" (print with_types) e ; add_types t
  | StatelessFun0 (t, Now) ->
    Printf.fprintf fmt "now" ; add_types t
  | StatelessFun0 (t, Random) ->
    Printf.fprintf fmt "random" ; add_types t
  | StatelessFun1 (t, Cast, e) ->
    Printf.fprintf fmt "cast(%a, %a)"
      RamenTypes.print_typ (Option.get t.scalar_typ) (print with_types) e ;
    add_types t
  | StatelessFun1 (t, Length, e) ->
    Printf.fprintf fmt "length (%a)" (print with_types) e ; add_types t
  | StatelessFun1 (t, Lower, e) ->
    Printf.fprintf fmt "lower (%a)" (print with_types) e ; add_types t
  | StatelessFun1 (t, Upper, e) ->
    Printf.fprintf fmt "upper (%a)" (print with_types) e ; add_types t
  | StatelessFun1 (t, Not, e) ->
    Printf.fprintf fmt "NOT (%a)" (print with_types) e ; add_types t
  | StatelessFun1 (t, Abs, e) ->
    Printf.fprintf fmt "ABS (%a)" (print with_types) e ; add_types t
  | StatelessFun1 (t, Minus, e) ->
    Printf.fprintf fmt "-(%a)" (print with_types) e ; add_types t
  | StatelessFun1 (t, Defined, e) ->
    Printf.fprintf fmt "(%a) IS NOT NULL" (print with_types) e ; add_types t
  | StatelessFun2 (t, Add, e1, e2) ->
    Printf.fprintf fmt "(%a) + (%a)" (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatelessFun2 (t, Sub, e1, e2) ->
    Printf.fprintf fmt "(%a) - (%a)" (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatelessFun2 (t, Mul, e1, e2) ->
    Printf.fprintf fmt "(%a) * (%a)" (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatelessFun2 (t, Div, e1, e2) ->
    Printf.fprintf fmt "(%a) / (%a)" (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatelessFun2 (t, IDiv, e1, e2) ->
    Printf.fprintf fmt "(%a) // (%a)" (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatelessFun2 (t, Mod, e1, e2) ->
    Printf.fprintf fmt "(%a) %% (%a)" (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatelessFun2 (t, Pow, e1, e2) ->
    Printf.fprintf fmt "(%a) ^ (%a)" (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatelessFun1 (t, Exp, e) ->
    Printf.fprintf fmt "exp (%a)" (print with_types) e ; add_types t
  | StatelessFun1 (t, Log, e) ->
    Printf.fprintf fmt "log (%a)" (print with_types) e ; add_types t
  | StatelessFun1 (t, Log10, e) ->
    Printf.fprintf fmt "log10 (%a)" (print with_types) e ; add_types t
  | StatelessFun1 (t, Sqrt, e) ->
    Printf.fprintf fmt "sqrt (%a)" (print with_types) e ; add_types t
  | StatelessFun1 (t, Hash, e) ->
    Printf.fprintf fmt "hash (%a)" (print with_types) e ; add_types t
  | StatelessFun2 (t, And,
      StatelessFun2 (_, Ge, e1, StatelessFun1 (_, BeginOfRange, e2)),
      StatelessFun1 (_, Not,
        StatelessFun2 (_, Ge, e1', StatelessFun1 (_, EndOfRange, e2')))) ->
    assert (e2 == e2') ;
    assert (e1 == e1') ;
    Printf.fprintf fmt "(%a) IN (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
  | StatelessFun1 (t, (BeginOfRange|EndOfRange as op), e) ->
    (* The typer, for debug, might want to print this type... *)
    Printf.fprintf fmt "%s of range (%a)"
      (if op = BeginOfRange then "begin" else "end")
      (print with_types) e ;
    add_types t
  | StatelessFun1 (t, Nth n, es) ->
    Printf.fprintf fmt "%d%s(%a)" n (ordinal_suffix n) (print with_types) es ; add_types t
  | StatelessFun2 (t, And, e1, e2) -> Printf.fprintf fmt "(%a) AND (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
  | StatelessFun2 (t, Or, e1, e2) -> Printf.fprintf fmt "(%a) OR (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
  | StatelessFun2 (t, Ge, e1, e2) -> Printf.fprintf fmt "(%a) >= (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
  | StatelessFun2 (t, Gt, e1, e2) -> Printf.fprintf fmt "(%a) > (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
  | StatelessFun2 (t, Eq, e1, e2) -> Printf.fprintf fmt "(%a) = (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
  | StatelessFun2 (t, Concat, e1, e2) -> Printf.fprintf fmt "(%a) || (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
  | StatelessFun2 (t, BitAnd, e1, e2) -> Printf.fprintf fmt "(%a) & (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
  | StatelessFun2 (t, BitOr, e1, e2) -> Printf.fprintf fmt "(%a) | (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
  | StatelessFun2 (t, BitXor, e1, e2) -> Printf.fprintf fmt "(%a) ^ (%a)" (print with_types) e1 (print with_types) e2 ; add_types t
  | StatelessFun2 (t, VecGet, e1, e2) -> Printf.fprintf fmt "get(%a, %a)" (print with_types) e1 (print with_types) e2 ; add_types t
  | StatelessFunMisc (t, Like (e, p)) -> Printf.fprintf fmt "(%a) LIKE %S" (print with_types) e p ; add_types t
  | StatelessFunMisc (t, Max es) ->
    Printf.fprintf fmt "GREATEST %a" print_args es ;
    add_types t
  | StatelessFunMisc (t, Min es) ->
    Printf.fprintf fmt "LEAST %a" print_args es ;
    add_types t
  | StatelessFunMisc (t, Print es) ->
    Printf.fprintf fmt "PRINT %a" print_args es ;
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
  | StatefulFun (t, g, AggrHistogram (what, min, max, nb_buckets)) ->
    Printf.fprintf fmt "histogram%s(%a, %g, %g, %d)"
      (sl g) (print with_types) what min max nb_buckets ;
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
      print_args e4s ;
    add_types t
  | StatefulFun (t, g, Remember (fpr, tim, dur, es)) ->
    Printf.fprintf fmt "remember%s(%a, %a, %a, %a)"
      (sl g)
      (print with_types) fpr
      (print with_types) tim
      (print with_types) dur
      print_args es ;
    add_types t
  | StatefulFun (t, g, Distinct es) ->
    Printf.fprintf fmt "distinct%s(%a)"
      (sl g)
      print_args es
  | StatefulFun (t, g, ExpSmooth (e1, e2)) ->
    Printf.fprintf fmt "smooth%s(%a, %a)"
      (sl g)  (print with_types) e1 (print with_types) e2 ;
    add_types t
  | StatefulFun (t, g, Hysteresis (meas, accept, max)) ->
    Printf.fprintf fmt "hysteresis%s(%a, %a, %a)"
      (sl g)
      (print with_types) meas
      (print with_types) accept
      (print with_types) max ;
    add_types t
  | StatefulFun (t, g, Top { want_rank ; n ; what ; by ; time ; duration }) ->
    let print_duration_opt oc duration =
      if duration > 0. then print_duration oc duration in
    Printf.fprintf fmt "%s %a in top %d%sby %a in the last %a at time %a"
      (if want_rank then "rank of" else "is")
      (List.print ~first:"" ~last:"" ~sep:", " (print with_types)) what
      n (sl g)
      (print with_types) by
      print_duration_opt duration
      (print with_types) time

  | GeneratorFun (t, Split (e1, e2)) ->
    Printf.fprintf fmt "split(%a, %a)"
      (print with_types) e1 (print with_types) e2 ;
    add_types t

let typ_of = function
  | Const (t, _) | Tuple (t, _) | Vector (t, _)
  | Field (t, _, _) | StateField (t, _)
  | StatelessFun0 (t, _) | StatelessFun1 (t, _, _)
  | StatelessFun2 (t, _, _, _) | StatelessFunMisc (t, _)
  | StatefulFun (t, _, _) | GeneratorFun (t, _)
  | Case (t, _, _) | Coalesce (t, _) ->
    t

let is_nullable e =
  let t = typ_of e in
  t.nullable = Some true

(* Propagate values up the tree only, depth first. *)
let rec fold_by_depth f i expr =
  match expr with
  | Const _ | Field _ | StateField _
  | StatelessFun0 _ ->
    f i expr

  | StatefulFun (_, _, AggrMin e) | StatefulFun (_, _, AggrMax e)
  | StatefulFun (_, _, AggrSum e) | StatefulFun (_, _, AggrAvg e)
  | StatefulFun (_, _, AggrAnd e) | StatefulFun (_, _, AggrOr e)
  | StatefulFun (_, _, AggrFirst e) | StatefulFun (_, _, AggrLast e)
  | StatelessFun1 (_, _, e) | StatelessFunMisc (_, Like (e, _))
  | StatefulFun (_, _, AggrHistogram (e, _, _, _)) ->
    f (fold_by_depth f i e) expr

  | StatefulFun (_, _, AggrPercentile (e1, e2))
  | StatelessFun2 (_, _, e1, e2)
  | StatefulFun (_, _, Lag (e1, e2))
  | StatefulFun (_, _, ExpSmooth (e1, e2))
  | GeneratorFun (_, Split (e1, e2)) ->
    let i' = fold_by_depth f i e1 in
    let i''= fold_by_depth f i' e2 in
    f i'' expr

  | StatefulFun (_, _, MovingAvg (e1, e2, e3))
  | StatefulFun (_, _, LinReg (e1, e2, e3)) ->
    let i' = fold_by_depth f i e1 in
    let i''= fold_by_depth f i' e2 in
    let i'''= fold_by_depth f i'' e3 in
    f i''' expr

  | StatefulFun (_, _, Remember (e1, e2, e3, e4s))
  | StatefulFun (_, _, MultiLinReg (e1, e2, e3, e4s)) ->
    let i' = fold_by_depth f i e1 in
    let i''= fold_by_depth f i' e2 in
    let i'''= fold_by_depth f i'' e3 in
    let i''''= List.fold_left (fold_by_depth f) i''' e4s in
    f i'''' expr

  | StatefulFun (_, _, Top { what = e3s ; by = e1 ; time = e2 ; _ }) ->
    let i' = fold_by_depth f i e1 in
    let i''= fold_by_depth f i' e2 in
    let i'''= List.fold_left (fold_by_depth f) i'' e3s in
    f i''' expr

  | StatefulFun (_, _, Hysteresis (e1, e2, e3)) ->
    let i' = fold_by_depth f i e1 in
    let i''= fold_by_depth f i' e2 in
    let i'''= fold_by_depth f i'' e3 in
    f i''' expr

  | Case (_, alts, else_) ->
    let i' =
      List.fold_left (fun i alt ->
        let i' = fold_by_depth f i alt.case_cond in
        let i''= fold_by_depth f i' alt.case_cons in
        f i'' expr) i alts in
    let i''=
      Option.map_default (fold_by_depth f i') i' else_ in
    f i'' expr

  | Tuple (_, es)
  | Vector (_, es)
  | StatefulFun (_, _, Distinct es)
  | Coalesce (_, es)
  | StatelessFunMisc (_, (Max es|Min es|Print es)) ->
    let i' = List.fold_left (fold_by_depth f) i es in
    f i' expr

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
  | StatefulFun (t, g, AggrHistogram (a, min, max, nb_buckets)) ->
    StatefulFun (f t, g, AggrHistogram (
        (if recurs then map_type ~recurs f a else a), min, max, nb_buckets))
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
  | StatefulFun (t, g, Remember (fpr, tim, dur, es)) ->
    StatefulFun (f t, g, Remember (
        (if recurs then map_type ~recurs f fpr else fpr),
        (if recurs then map_type ~recurs f tim else tim),
        (if recurs then map_type ~recurs f dur else dur),
        (if recurs then List.map (map_type ~recurs f) es else es)))
  | StatefulFun (t, g, Distinct es) ->
    StatefulFun (f t, g, Distinct
        (if recurs then List.map (map_type ~recurs f) es else es))
  | StatefulFun (t, g, ExpSmooth (a, b)) ->
    StatefulFun (f t, g, ExpSmooth (
        (if recurs then map_type ~recurs f a else a),
        (if recurs then map_type ~recurs f b else b)))
  | StatefulFun (t, g, Hysteresis (a, b, c)) ->
    StatefulFun (f t, g, Hysteresis (
        (if recurs then map_type ~recurs f a else a),
        (if recurs then map_type ~recurs f b else b),
        (if recurs then map_type ~recurs f c else c)))
  | StatefulFun (t, g, Top { want_rank ; n ; what ; by ; duration ; time }) ->
    StatefulFun (f t, g, Top { want_rank ; n ; duration ;
      what = (if recurs then List.map (map_type ~recurs f) what else what) ;
      by = (if recurs then map_type ~recurs f by else by) ;
      time = (if recurs then map_type ~recurs f time else time) })

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
    (RamenTypes.Parser.scalar ++
     optional ~def:false (
       char ~case_sensitive:false 'n' >>: fun _ -> true) >>:
     fun (c, nullable) ->
       Const (make_typ ~nullable ~typ:(RamenTypes.type_of c) "constant", c)) m
  (*$= const & ~printer:(test_printer (print false))
    (Ok (Const (typ, VBool true), (4, [])))\
      (test_p const "true" |> replace_typ_in_expr)

    (Ok (Const (typ, VI8 (Stdint.Int8.of_int 15)), (4, []))) \
      (test_p const "15i8" |> replace_typ_in_expr)

    (Ok (Const (typn, VI8 (Stdint.Int8.of_int 15)), (5, []))) \
      (test_p const "15i8n" |> replace_typ_in_expr)
  *)

  let const_i32 ?(min=Num.of_string (Int32.(to_string min_int)))
                ?(max=Num.of_string (Int32.(to_string max_int))) m =
    let open RamenTypes in
    let what = "constant integer" in
    let m = what :: m in
    (integer_range ~min ~max >>: fun n ->
      Const (make_typ ~nullable:false ~typ:TI32 what,
             VI32 (Int32.of_string (Num.to_string n)))
    ) m

  let null m =
    let m = "NULL" :: m in
    (strinG "null" >>: fun () ->
      (* Type of "NULL" is unknown yet *)
      Const (make_typ ~nullable:true "NULL", VNull)
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

  (* operators with lowest precedence *)
  let rec lowest_prec_left_assoc m =
    let m = "logical operator" :: m in
    let op = that_string "and" ||| that_string "or"
    and reduce e1 op e2 = match op with
      | "and" -> StatelessFun2 (make_bool_typ "and", And, e1, e2)
      | "or" -> StatelessFun2 (make_bool_typ "or", Or, e1, e2)
      | _ -> assert false in
    (* FIXME: we do not need a blanks if we had parentheses ("(x)AND(y)" is OK) *)
    binary_ops_reducer ~op ~term:low_prec_left_assoc ~sep:blanks ~reduce m

  and low_prec_left_assoc m =
    let m = "comparison operator" :: m in
    let op = that_string ">" ||| that_string ">=" ||| that_string "<" ||| that_string "<=" |||
             that_string "=" ||| that_string "<>" ||| that_string "!=" |||
             that_string "in" ||| that_string "like"
    and reduce e1 op e2 = match op with
      | ">" -> StatelessFun2 (make_bool_typ "comparison (>)", Gt, e1, e2)
      | "<" -> StatelessFun2 (make_bool_typ "comparison (<)", Gt, e2, e1)
      | ">=" -> StatelessFun2 (make_bool_typ "comparison (>=)", Ge, e1, e2)
      | "<=" -> StatelessFun2 (make_bool_typ "comparison (<=)", Ge, e2, e1)
      | "=" -> StatelessFun2 (make_bool_typ "equality", Eq, e1, e2)
      | "!=" | "<>" ->
        StatelessFun1 (make_bool_typ "not", Not,
          StatelessFun2 (make_bool_typ "equality", Eq, e1, e2))
      | "in" ->
        StatelessFun2 (make_bool_typ "and for range", And,
          StatelessFun2 (make_bool_typ "comparison operator for range", Ge,
            e1,
            StatelessFun1 (make_typ "begin of range", BeginOfRange, e2)),
          StatelessFun1 (make_bool_typ "not for range", Not,
            StatelessFun2 (make_bool_typ "comparison operator for range", Ge,
              e1,
              StatelessFun1 (make_typ "end of range", EndOfRange, e2))))
      | "like" ->
        (match string_of_const e2 with
        | None -> raise (Reject "LIKE pattern must be a string constant")
        | Some p ->
          StatelessFunMisc (make_bool_typ "like", Like (e1, p)))
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
              StatelessFun2 (make_typ ~typ:TFloat "division", Div, e1, e2))
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
  and afunv_sf ?(def_state=GlobalState) a n m =
    let sep = opt_blanks -- char ',' -- opt_blanks in
    let m = n :: m in
    (strinG n -+
     optional ~def:def_state (blanks -+ state_lifespan) +-
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

  and afun1_sf ?(def_state=GlobalState) n =
    let sep = check (char '(') ||| blanks in
    (strinG n -+ optional ~def:def_state (blanks -+ state_lifespan) +-
     sep ++ highestest_prec)

  and afun2_sf ?def_state n =
    afun_sf ?def_state 2 n >>: function (g, [a;b]) -> g, a, b | _ -> assert false

  and afun0v_sf ?(def_state=GlobalState) n =
    (* afunv_sf takes parentheses but it's nicer to also accept non
     * parenthesized highestest_prec, but then there would be 2 ways to
     * parse "distinct (x)" as highestest_prec also accept parenthesized
     * lower precedence expressions. Thus the "highestest_prec_no_parenthesis": *)
    (strinG n -+ optional ~def:def_state (blanks -+ state_lifespan) +-
     blanks ++ highestest_prec_no_parenthesis >>: fun (f, e) -> f, [e]) |||
    (afunv_sf ~def_state 0 n >>:
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
    let sep = opt_blanks -- char ',' -- opt_blanks in
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
    ((afun1 "not" >>: fun e ->
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
            StatelessFun1 (make_bool_typ ~nullable:false "is_not_null", Defined, e))
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
     (afun1 "exp" >>: fun e -> StatelessFun1 (make_num_typ "exponential", Exp, e)) |||
     (afun1 "log" >>: fun e -> StatelessFun1 (make_num_typ "natural logarithm", Log, e)) |||
     (afun1 "log10" >>: fun e -> StatelessFun1 (make_num_typ "common logarithm", Log10, e)) |||
     (afun1 "sqrt" >>: fun e -> StatelessFun1 (make_num_typ "square root", Sqrt, e)) |||
     (afun1 "hash" >>: fun e -> StatelessFun1 (make_typ ~typ:TI64 "hash", Hash, e)) |||
     (afun1_sf ~def_state:LocalState "min" >>: fun (g, e) ->
        StatefulFun (make_typ "min aggregation", g, AggrMin e)) |||
     (afun1_sf ~def_state:LocalState "max" >>: fun (g, e) ->
        StatefulFun (make_typ "max aggregation", g, AggrMax e)) |||
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
     (afun3_sf "remember" >>: fun (g, tim, dur, e) ->
        (* If we allowed a list of expressions here then it would be ambiguous
         * with the following "3+v" signature: *)
        let fpr = of_float 0.015 in
        StatefulFun (make_bool_typ "remember", g,
                     Remember (fpr, tim, dur, [e]))) |||
     (afun3v_sf "remember" >>: fun (g, fpr, tim, dur, es) ->
        StatefulFun (make_bool_typ "remember", g,
                     Remember (fpr, tim, dur, es))) |||
     (afun0v_sf ~def_state:LocalState "distinct" >>: fun (g, es) ->
         StatefulFun (make_bool_typ "distinct", g, Distinct es)) |||
     (afun3_sf "hysteresis" >>: fun (g, value, accept, max) ->
        StatefulFun (make_bool_typ ~nullable:false "hysteresis", g,
                     Hysteresis (value, accept, max))) |||
     (afun4_sf ~def_state:LocalState "histogram" >>:
      fun (g, what, min, max, nb_buckets) ->
        match float_of_const min,
              float_of_const max,
              int_of_const nb_buckets with
        | Some min, Some max, Some nb_buckets ->
            if nb_buckets <= 0 then
              raise (Reject "Histogram size must be positive") ;
            let typ = RamenTypes.(TVec (nb_buckets+2, TU32)) in
            StatefulFun (make_typ "histogram" ~typ,
                         g, AggrHistogram (what, min, max, nb_buckets))
        | _ -> raise (Reject "histogram dimensions must be constants")) |||
     (afun2 "split" >>: fun (e1, e2) ->
        GeneratorFun (make_typ ~typ:TString "split", Split (e1, e2))) |||
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
        StatelessFun2 (make_typ "get", VecGet, n, v)) |||
     (* Outputs TBool as that's the smallest we have. Actually outputs false. *)
     (afun1v "print" >>: fun (e, es) ->
        StatelessFunMisc (make_typ "print", Print (e :: es))) |||
     k_moveavg ||| cast ||| top_expr ||| nth) m

  and cast m =
    let m = "cast" :: m in
    let sep = check (char '(') ||| blanks in
    (RamenTypes.Parser.scalar_typ +- sep ++
     highestest_prec >>: fun (typ, e) ->
       StatelessFun1 (make_typ ~typ ("cast to "^ IO.to_string RamenTypes.print_typ typ), Cast, e)
    ) m

  and k_moveavg m =
    let m = "k-moving average" :: m in
    let sep = check (char '(') ||| blanks in
    ((unsigned_decimal_number >>: RamenTypes.Parser.narrowest_int_scalar) +-
     (strinG "-moveavg" ||| strinG "-ma") ++
     optional ~def:GlobalState (blanks -+ state_lifespan) +-
     sep ++ highestest_prec >>: fun ((k, g), e) ->
       let k = Const (make_typ ~nullable:false ~typ:(RamenTypes.type_of k)
                               "moving average order", k) in
       StatefulFun (make_float_typ "moveavg", g, MovingAvg (expr_one, k, e))) m

  and top_expr m =
    let m = "top expression" :: m in
    (((strinG "rank" -- blanks -- strinG "of" >>: fun () -> true) |||
      (strinG "is" >>: fun () -> false)) +- blanks ++
     (* We can allow lowest precedence expressions here because of the
      * keywords that follow: *)
     several ~sep:(char ',' -- opt_blanks) p +- blanks +-
     strinG "in" +- blanks +- strinG "top" +- blanks ++
     pos_decimal_integer "top size" ++
     optional ~def:GlobalState (blanks -+ state_lifespan) ++
     optional ~def:expr_one (
       blanks -- strinG "by" -- blanks -+ highestest_prec) ++
     optional ~def:3600. (
       blanks -- strinG "in" -- blanks -- strinG "the" -- blanks --
       strinG "last" -- blanks -+ duration) ++
     optional ~def:expr_zero (
       blanks -- strinG "at" -- blanks -- strinG "time" -- blanks -+ p) >>:
     fun ((((((want_rank, what), n), g), by), duration), time) ->
       StatefulFun (
         (if want_rank then make_int_typ ~nullable:true "rank in top"
                       (* same nullability as what+by+time: *)
                       else make_bool_typ "is in top"),
         g,
         Top { want_rank ; n ; what ; by ; duration ; time })) m

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
    (const ||| field ||| func ||| null |||
     case ||| if_ ||| coalesce) m

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
        let nb_items = List.length es in
        assert (nb_items >= 2) ;
        let typ = RamenTypes.(TTuple (Array.create nb_items TAny)) in
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
        let nb_items = List.length es in
        assert (nb_items >= 1) ;
        let typ = RamenTypes.(TVec (nb_items, TAny)) in
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
        StatefulFun (typ, LocalState, AggrSum (\
          Field (typ, ref TupleUnknown, "bytes"))),\
        Field (typ, ref TupleUnknown, "avg_window")),\
      (22, [])))\
      (test_p p "(sum bytes)/avg_window" |> replace_typ_in_expr)

    (Ok (\
      StatelessFun2 (typ, IDiv, \
        Field (typ, ref TupleUnknown, "start"),\
        StatelessFun2 (typ, Mul, \
          Const (typ, VI32 1_000_000l),\
          Field (typ, ref TupleUnknown, "avg_window"))),\
      (33, [])))\
      (test_p p "start // (1_000_000 * avg_window)" |> replace_typ_in_expr)

    (Ok (\
      StatefulFun (typ, LocalState, AggrPercentile (\
        Field (typ, ref TupleParam, "p"),\
        Field (typ, ref TupleUnknown, "bytes_per_sec"))),\
      (26, [])))\
      (test_p p "p percentile bytes_per_sec" |> replace_typ_in_expr)

    (Ok (\
      StatelessFun2 (typ, Gt, \
        StatefulFun (typ, LocalState, AggrMax (\
          Field (typ, ref TupleLastSelected, "start"))),\
        StatelessFun2 (typ, Add, \
          Field (typ, ref TupleOut, "start"),\
          StatelessFun2 (typ, Mul, \
            StatelessFun2 (typ, Mul, \
              Field (typ, ref TupleUnknown, "obs_window"),\
              Const (typ, VFloat 1.15)),\
            Const (typ, VI32 1_000_000l)))),\
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
          StatefulFun (typ, GlobalState, Lag (\
            Const (typ, VI32 (Int32.of_int 1)), \
            Field (typ, ref TupleUnknown, "bps"))))), \
      (21, []))) \
      (test_p p "abs(bps - lag(1,bps))" |> replace_typ_in_expr)

    (Ok ( \
      StatefulFun (typ, GlobalState, Hysteresis (\
        Field (typ, ref TupleUnknown, "value"),\
        Const (typ, VI32 (Int32.of_int 900)),\
        Const (typ, VI32 (Int32.of_int 1000)))),\
      (28, [])))\
      (test_p p "hysteresis(value, 900, 1000)" |> replace_typ_in_expr)

    (Ok ( \
      StatelessFun2 (typ, Mul, \
        StatelessFun2 (typ, BitAnd, \
          Const (typ, VI32 (Int32.of_int 4)), \
          Const (typ, VI32 (Int32.of_int 4))), \
        Const (typ, VI32 (Int32.of_int 2))), \
      (9, []))) \
      (test_p p "4 & 4 * 2" |> replace_typ_in_expr)
  *)

  (*$>*)
end
