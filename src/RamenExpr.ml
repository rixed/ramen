(* This module deals with expressions.
 * Expressions are the flesh of ramen programs.
 * Every expression is typed.
 *
 * Immediate values are parsed in RamenTypes.
 *)
open Batteries
open Stdint

open RamenHelpersNoLog
open RamenHelpers
open RamenLog
module DT = DessserTypes
module DE = DessserExpressions
module Lang = RamenLang
module N = RamenName
module T = RamenTypes
module Units = RamenUnits
module Variable = RamenVariable

(*$inject
  open TestHelpers
  open RamenLang
  open Stdint

  let () = RamenExperiments.set_variants (N.path "") []
*)

open Raql_path_comp.DessserGen
open Raql_top_output.DessserGen
open Raql_binding_key.DessserGen
include Raql_expr.DessserGen
type path_comp = Raql_path_comp.DessserGen.t
type top_output = Raql_top_output.DessserGen.t
type binding_key = Raql_binding_key.DessserGen.t

let string_of_state_lifespan = function
  | LocalState -> "local"
  | GlobalState -> "global"

let print_binding_key oc = function
  | State n ->
      Printf.fprintf oc "State of %s" (Uint32.to_string n)
  | RecordField (pref, field) ->
      Printf.fprintf oc "%s.%s"
        (Variable.to_string pref)
        (ramen_quote (field :> string))
  | RecordValue pref ->
      String.print oc (Variable.to_string pref)
  | Direct s ->
      Printf.fprintf oc "Direct %S" s

let print_path_comp oc field_sep = function
  | Idx i -> Printf.fprintf oc "[%s]" (Uint32.to_string i)
  | Name n -> Printf.fprintf oc "%s%a" field_sep N.field_print n

let print_path oc path =
  let rec loop field_sep = function
    | [] -> ()
    | path_comp :: rest ->
        print_path_comp oc field_sep path_comp ;
        loop "." rest in
  loop "" path

let id_of_path p =
  List.fold_left (fun id p ->
    id ^(
      match p with
      | Idx i -> "["^ Uint32.to_string i ^"]"
      | Name s -> (if id = "" then "" else ".")^ (s :> string))
  ) "" p |>
  N.field

let uniq_num_seq = ref Uint32.zero

let make ?(typ=DT.Unknown) ?(nullable=false) ?units text =
  uniq_num_seq := Uint32.succ !uniq_num_seq ;
  { text ; uniq_num = !uniq_num_seq ;
    typ = DT.maybe_nullable ~nullable typ ;
    units }

(* Constant expressions must be typed independently and therefore have
 * a distinct uniq_num for each occurrence: *)
let null () =
  make (Stateless (SL0 (Const T.(to_wire VNull))))

let of_bool b =
  make ~typ:DT.(Base Bool) ~nullable:false (Stateless (SL0 (Const T.(to_wire (VBool b)))))

let of_u8 ?units n =
  make ~typ:DT.(Base U8) ~nullable:false ?units
    (Stateless (SL0 (Const T.(to_wire (VU8 (Uint8.of_int n))))))

let of_float ?units n =
  make ~typ:DT.(Base Float) ~nullable:false ?units
    (Stateless (SL0 (Const T.(to_wire (VFloat n)))))

let of_string s =
  make ~typ:DT.(Base String) ~nullable:false
    (Stateless (SL0 (Const T.(to_wire (VString s)))))

let zero () = of_u8 0
let one () = of_u8 1
let one_hour () = of_float ~units:Units.seconds 3600.

let string_of_const e =
  match e.text with
  | Stateless (SL0 (Const (VString s))) -> Some s
  | _ -> None

let float_of_const e =
  match e.text with
  | Stateless (SL0 (Const v)) ->
      (* float_of_scalar and int_of_scalar returns an option because they
       * accept nullable numeric values; they fail on non-numerics, while
       * we want to merely return None here: *)
      (try T.(float_of_scalar (of_wire v))
      with Invalid_argument _ -> None)
  | _ -> None

let int_of_const e =
  match e.text with
  | Stateless (SL0 (Const v)) ->
      (try T.(int_of_scalar (of_wire v))
      with Invalid_argument _ -> None)
  | _ -> None

let bool_of_const e =
  match e.text with
  | Stateless (SL0 (Const v)) ->
      (try T.(bool_of_scalar (of_wire v))
      with Invalid_argument _ -> None)
  | _ -> None

let is_nullable e = e.typ.DT.nullable

let is_const e =
  match e.text with
  | Stateless (SL0 (Const _)) -> true
  | _ -> false

let is_zero e =
  match float_of_const e with
  | None -> false
  | Some f -> f = 0.

let is_one e =
  match float_of_const e with
  | None -> false
  | Some f -> f = 1.

let is_bool_const b e =
  match e.text with
  | Stateless (SL0 (Const (VBool b'))) -> b' = b
  | _ -> false

let is_true = is_bool_const true
let is_false = is_bool_const false

let is_a_string e =
  e.typ.DT.typ = Base String

(* Tells if [e] (that must be typed) is a list or a vector, ie anything
 * which is represented with an OCaml array. *)
let is_a_list e =
  match e.typ.DT.typ with
  | Arr _ | Vec _ -> true
  | Lst _ -> assert false (* RaQL does not encode anything with lists *)
  | _ -> false

(* Similar to DT.is_integer but returns false on Unknown.
 * Used for pretty-printing. *)
let is_integer v =
  let t = T.type_of_value v in
  try DT.is_integer t
  with Invalid_argument _ -> false

(* Same as above: *)
let is_ip v =
  let t = T.type_of_value v in
  try T.is_ip t
  with Invalid_argument _ -> false

(* All expressions can have a unit, either explicit from the source or implicit
 * from inference. Units are only accepted after some expressions so must not
 * be printed after any expression, or the result will be impossible to parse: *)
let accept_units = function
  | Stateless (SL0 (Const _)) (* including NULL *)
  | Stateless (SL2 (Get, _, _))
  (* Also in theory any parenthesized expression can be added units, but those
   * parenthesis are lost after parsing unfortunately.
   * FIXME: save with units if they are explicit or implicit and print only
   * the explicit ones?*)
      ->
      true
  | _ ->
      false

let print_units_on_expr oc text u =
  if accept_units text then Units.print oc u

let rec print ?(max_depth=max_int) with_types oc e =
  if max_depth <= 0 then
    Printf.fprintf oc "…"
  else (
    print_text ~max_depth with_types oc e.text ;
    Option.may (print_units_on_expr oc e.text) e.units ;
    if with_types then
      Printf.fprintf oc " [#%s, %a]"
        (Uint32.to_string e.uniq_num)
        DT.print_mn e.typ)

and print_text ?(max_depth=max_int) with_types oc text =
  let st g n =
    (* TODO: do not display default *)
    (match g with LocalState -> " LOCALLY" | GlobalState -> " GLOBALLY") ^
    (if n then " skip nulls" else " keep nulls")
  and print_args =
    List.print ~first:"(" ~last:")" ~sep:", "
      (print ~max_depth:(max_depth-1) with_types) in
  let p oc = print ~max_depth:(max_depth-1) with_types oc in
  (match text with
  | Stateless (SL0 (Const c)) ->
      T.(print oc (of_wire c))
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
      (* Distinguish Path from Gets using uppercase "IN": *)
      Printf.fprintf oc "IN.%a" print_path path
  | Stateless (SL0 (Variable pref)) ->
      Printf.fprintf oc "%s" (Variable.to_string pref)
  | Stateless (SL0 (Binding k)) ->
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
  | Stateless (SL0 Pi) ->
      Printf.fprintf oc "pi"
  | Stateless (SL0 EventStart) ->
      Printf.fprintf oc "#start"
  | Stateless (SL0 EventStop) ->
      Printf.fprintf oc "#stop"
  | Stateless (SL1 (Cast typ, e)) ->
      Printf.fprintf oc "%a(%a)"
        DT.print_mn typ p e
  | Stateless (SL1 (Force, e)) ->
      Printf.fprintf oc "FORCE(%a)" p e
  | Stateless (SL1 (Peek (mn, endianness), e)) ->
      Printf.fprintf oc "PEEK %a %a %a"
        DT.print mn.DT.typ
        print_endianness endianness
        p e
  | Stateless (SL1 (Length, e)) ->
      Printf.fprintf oc "LENGTH(%a)" p e
  | Stateless (SL1 (Lower, e)) ->
      Printf.fprintf oc "LOWER(%a)" p e
  | Stateless (SL1 (Upper, e)) ->
      Printf.fprintf oc "UPPER(%a)" p e
  | Stateless (SL1 (UuidOfU128, e)) ->
      Printf.fprintf oc "UUID_OF_U128(%a)" p e
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
  | Stateless (SL2 (Index, e1, e2)) ->
      Printf.fprintf oc "INDEX (%a, %a)" p e1 p e2
  | Stateless (SL3 (SubString, e1, e2, e3)) ->
      Printf.fprintf oc "SUBSTRING (%a, %a, %a)" p e1 p e2 p e3
  | Stateless (SL3 (MapSet, e1, e2, e3)) ->
      Printf.fprintf oc "MapSet (%a, %a, %a)" p e1 p e2 p e3
  | Stateless (SL1 (Exp, e)) ->
      Printf.fprintf oc "EXP (%a)" p e
  | Stateless (SL1 (Log, e)) ->
      Printf.fprintf oc "LOG (%a)" p e
  | Stateless (SL1 (Log10, e)) ->
      Printf.fprintf oc "LOG10 (%a)" p e
  | Stateless (SL1 (Sqrt, e)) ->
      Printf.fprintf oc "SQRT (%a)" p e
  | Stateless (SL1 (Sq, e)) ->
      Printf.fprintf oc "SQ (%a)" p e
  | Stateless (SL1 (Ceil, e)) ->
      Printf.fprintf oc "CEIL (%a)" p e
  | Stateless (SL1 (Floor, e)) ->
      Printf.fprintf oc "FLOOR (%a)" p e
  | Stateless (SL1 (Round, e)) ->
      Printf.fprintf oc "ROUND (%a)" p e
  | Stateless (SL1 (Cos, e)) ->
      Printf.fprintf oc "COS (%a)" p e
  | Stateless (SL1 (Sin, e)) ->
      Printf.fprintf oc "COS (%a)" p e
  | Stateless (SL1 (Tan, e)) ->
      Printf.fprintf oc "COS (%a)" p e
  | Stateless (SL1 (ACos, e)) ->
      Printf.fprintf oc "COS (%a)" p e
  | Stateless (SL1 (ASin, e)) ->
      Printf.fprintf oc "COS (%a)" p e
  | Stateless (SL1 (ATan, e)) ->
      Printf.fprintf oc "COS (%a)" p e
  | Stateless (SL1 (CosH, e)) ->
      Printf.fprintf oc "COS (%a)" p e
  | Stateless (SL1 (SinH, e)) ->
      Printf.fprintf oc "COS (%a)" p e
  | Stateless (SL1 (TanH, e)) ->
      Printf.fprintf oc "COS (%a)" p e
  | Stateless (SL1 (Hash, e)) ->
      Printf.fprintf oc "HASH (%a)" p e
  | Stateless (SL1 (Sparkline, e)) ->
      Printf.fprintf oc "SPARKLINE (%a)" p e
  | Stateless (SL1 (Fit, e)) ->
      Printf.fprintf oc "FIT (%a)" p e
  | Stateless (SL1 (CountryCode, e)) ->
      Printf.fprintf oc "COUNTRYCODE (%a)" p e
  | Stateless (SL1 (IpFamily, e)) ->
      Printf.fprintf oc "IPFAMILY (%a)" p e
  | Stateless (SL1 (Basename, e)) ->
      Printf.fprintf oc "BASENAME (%a)" p e
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
  | Stateless (SL1 (Chr, e)) ->
      Printf.fprintf oc "CHR (%a)" p e
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
  | Stateless (SL2 (Get, { text = Stateless (SL0 (Const (VString n))) ; _ },
                         { text = Stateless (SL0 (Variable pref)) ; _ }))
    when not with_types ->
      Printf.fprintf oc "%s.%s" (Variable.to_string pref) (ramen_quote n)
  | Stateless (SL2 (Get, ({ text = Stateless (SL0 (Const n)) ; _ } as e1), e2))
    when not with_types && is_integer T.(of_wire n) ->
      Printf.fprintf oc "%a[%a]" p e2 p e1
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
  | Stateful (g, n, SF1 (AggrBitAnd, e)) ->
      Printf.fprintf oc "BITAND%s(%a)" (st g n) p e
  | Stateful (g, n, SF1 (AggrBitOr, e)) ->
      Printf.fprintf oc "BITOR%s(%a)" (st g n) p e
  | Stateful (g, n, SF1 (AggrBitXor, e)) ->
      Printf.fprintf oc "BITXOR%s(%a)" (st g n) p e
  | Stateful (g, n, SF1 (AggrFirst, e)) ->
      Printf.fprintf oc "FIRST%s(%a)" (st g n) p e
  | Stateful (g, n, SF1 (AggrLast, e)) ->
      Printf.fprintf oc "LAST%s(%a)" (st g n) p e
  | Stateful (g, n, SF1 (AggrHistogram (min, max, num_buckets), e)) ->
      Printf.fprintf oc "HISTOGRAM%s(%a, %g, %g, %s)" (st g n)
        p e min max
        (Uint32.to_string num_buckets)
  | Stateful (g, n, SF2 (Lag, e1, e2)) ->
      Printf.fprintf oc "LAG%s(%a, %a)" (st g n) p e1 p e2
  | Stateful (g, n, SF3 (MovingAvg, e1, e2, e3)) ->
      Printf.fprintf oc "SEASON_MOVEAVG%s(%a, %a, %a)"
        (st g n) p e1 p e2 p e3
  | Stateful (g, n, SF4 (DampedHolt, e1, e2, e3, e4)) ->
      Printf.fprintf oc "DAMPED_HOLT%s(%a, %a, %a, %a)"
        (st g n) p e1 p e2 p e3 p e4
  | Stateful (g, n, SF4 (Remember refresh, fpr, tim, dur, e)) ->
      Printf.fprintf oc "%s%s %a"
        (if refresh then "REMEMBER" else "RECALL")
        (st g n) print_args [ fpr ; tim ; dur ; e ]
  | Stateful (g, n, SF4s (MultiLinReg, e1, e2, e3, e4s)) ->
      Printf.fprintf oc "SEASON_FIT_MULTI%s %a"
        (st g n) print_args (e1 :: e2 :: e3 :: e4s)
  | Stateful (g, n, SF6 (DampedHoltWinter, e1, e2, e3, e4, e5, e6)) ->
      Printf.fprintf oc "DAMPED_HOLD_WINTER%S(%a, %a, %a, %a, %a, %a)"
        (st g n) p e1 p e2 p e3 p e4 p e5 p e6
  | Stateful (g, n, SF1 (Distinct, e1)) ->
      Printf.fprintf oc "DISTINCT%s %a" (st g n) p e1
  | Stateful (g, n, SF2 (ExpSmooth, e1, e2)) ->
      Printf.fprintf oc "SMOOTH%s(%a, %a)" (st g n) p e1 p e2
  | Stateful (g, n, SF3 (Hysteresis, meas, accept, max)) ->
      Printf.fprintf oc "HYSTERESIS%s(%a, %a, %a)"
        (st g n) p meas p accept p max
  | Stateful (g, n, Top { output ; size ; max_size ; top_what ; by ; top_time ;
                          duration ; sigmas }) ->
      (match output with
      | Rank ->
          Printf.fprintf oc
            "RANK OF %a IN TOP %a%a"
            p top_what
            p size
            (fun oc -> function
             | None -> Unit.print oc ()
             | Some e -> Printf.fprintf oc " OVER %a" p e) max_size
      | Membership ->
          Printf.fprintf oc
            "IS %a IN TOP %a%a"
            p top_what
            p size
            (fun oc -> function
             | None -> Unit.print oc ()
             | Some e -> Printf.fprintf oc " OVER %a" p e) max_size
      | List ->
          Printf.fprintf oc
            "TOP %a%a OF %a"
            p size
            (fun oc -> function
             | None -> Unit.print oc ()
             | Some e -> Printf.fprintf oc " OVER %a" p e) max_size
            p top_what) ;
      Printf.fprintf oc " %s BY %a IN THE LAST %a AT TIME %a"
        (st g n) p by p duration p top_time ;
      if not (is_zero sigmas) then
        Printf.fprintf oc " ABOVE %a SIGMAS" p sigmas
  | Stateful (g, n, SF4s (Largest { inv ; up_to }, c, but, e, es)) ->
      let print_by oc es =
        if es <> [] then
          Printf.fprintf oc " BY %a"
            (List.print ~first:"" ~last:"" ~sep:", " p) es in
      Printf.fprintf oc "%s %s%a BUT %a%s %a%a"
        (if es <> [] then
          if inv then "SMALLEST" else "LARGEST"
        else
          if inv then "OLDEST" else "LATEST")
        (if up_to then "UP TO " else "")
        p c
        p but
        (st g n)
        p e
        print_by es
  | Stateful (g, n, SF2 (Sample, c, e)) ->
      Printf.fprintf oc "SAMPLE%s(%a, %a)" (st g n) p c p e
  | Stateful (g, n, SF2 (OneOutOf, i, e)) ->
      Printf.fprintf oc "ONE OUT OF %a%s %a" p i (st g n) p e
  | Stateful (g, n, SF3 (OnceEvery tumbling, d, t, e)) ->
      Printf.fprintf oc "ONCE EVERY %a %s%s(%a, %a)"
        p d
        (if tumbling then "TUMBLING" else "SLIDING")
        (st g n)
        p e
        p t
  | Stateful (g, n, Past { what ; time ; max_age ; tumbling ; sample_size }) ->
      (match sample_size with
      | None -> ()
      | Some sz ->
          Printf.fprintf oc "SAMPLE OF SIZE %a OF THE " p sz) ;
      Printf.fprintf oc "PAST %a %s%s OF %a AT TIME %a"
        p max_age
        (if tumbling then "TUMBLING" else "SLIDING")
        (st g n) p what p time
  | Stateful (g, n, SF1 (Group, e)) ->
      Printf.fprintf oc "GROUP%s %a" (st g n) p e
  | Stateful (g, n, SF1 (Count, e)) ->
      Printf.fprintf oc "COUNT%s %a" (st g n) p e

  | Generator (Split (e1, e2)) ->
      Printf.fprintf oc "SPLIT(%a, %a)" p e1 p e2)

and print_endianness oc = function
  | LittleEndian -> String.print oc "LITTLE ENDIAN"
  | BigEndian -> String.print oc "BIG ENDIAN"

let to_string ?max_depth ?(with_types=false) e =
  Printf.sprintf2 "%a" (print ?max_depth with_types) e

let endianness_of_wire = function
  | LittleEndian -> DE.LittleEndian
  | BigEndian -> DE.BigEndian

let rec get_scalar_test e =
  !logger.debug "get_scalar_test for expr %a" (print true) e ;
  let is_const_scalar e =
    is_const e && T.is_scalar e.typ.DT.typ in
  let to_type t v =
    try T.to_type t v with
    | e ->
        (* This is ok-ish but the filter hit-ratio will suffer: *)
        !logger.error "get_scalar_test: %s" (Printexc.to_string e) ;
        v in
  let value_of_const typ = function
    | { text = Stateless (SL0 (Const v)) ; _ } -> to_type typ T.(of_wire v)
    | _ -> invalid_arg "value_of_const" in
  match e.text with
  (* Direct equality comparison of anything from parent with a constant: *)
  | Stateless (
        SL2 (Eq, { text = Stateless (SL0 (Path p)) ; typ = ftyp },
                 { text = Stateless (SL0 (Const v)) ; typ = ctyp }) |
        SL2 (Eq, { text = Stateless (SL0 (Const v)) ; typ = ctyp },
                 { text = Stateless (SL0 (Path p)) ; typ = ftyp }))
    when T.is_scalar ctyp.DT.typ ->
      Some (p, Set.singleton (to_type ftyp.DT.typ T.(of_wire v)))
  (* Set inclusion test: *)
  | Stateless (
        SL2 (In, { text = Stateless (SL0 (Path p)) ; typ = ftyp },
                 { text = Vector es ; _ }) |
        SL2 (In, { text = Vector es ; _ },
                 { text = Stateless (SL0 (Path p)) ; typ = ftyp }))
    when List.for_all is_const_scalar es ->
      Some (p, Set.of_list (List.map (value_of_const ftyp.DT.typ) es))
  (* ORing several such comparison to the same constant: *)
  | Stateless (SL2 (Or, e1, e2)) ->
      (match get_scalar_test e1, get_scalar_test e2 with
      | Some (p1, v1s), Some (p2, v2s) when p1 = p2 ->
          Some (p1, Set.union v1s v2s)
      | _ ->
          None)
  | _ ->
      None

let print_stack oc stack =
  List.print (print false) oc stack

let rec map f s e =
  (* [s] is the stack of expressions to the AST root *)
  let s' = e :: s in
  (* Shorthands : *)
  let m = map f s' in
  let mm = List.map m
  and om = Option.map m in
  (match e.text with
  | Stateless (SL0 _) ->
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
  | Stateful (g, n, SF4 (o, e1, e2, e3, e4)) ->
      { e with text = Stateful (g, n, SF4 (o, m e1, m e2, m e3, m e4)) }
  | Stateful (g, n, SF6 (o, e1, e2, e3, e4, e5, e6)) ->
      { e with text = Stateful (g, n, SF6 (o, m e1, m e2, m e3, m e4, m e5, m e6)) }
  | Stateful (g, n, SF4s (o, e1, e2, e3, e4s)) ->
      { e with text = Stateful (g, n, SF4s (o, m e1, m e2, m e3, mm e4s)) }
  | Stateful (g, n, Top ({ size ; by ; top_time ; duration ; top_what ; max_size ;
                           sigmas } as a)) ->
      { e with text = Stateful (g, n, Top { a with
        size = m size ; by = m by ; top_time = m top_time ; duration = m duration ;
        top_what = m top_what ; max_size = om max_size ; sigmas = m sigmas }) }
  | Stateful (g, n, Past { what ; time ; max_age ; tumbling ; sample_size }) ->
      { e with text = Stateful (g, n, Past {
        what = m what ; time = m time ; max_age = m max_age ; tumbling ;
        sample_size = om sample_size }) }

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
  | Stateless (SL0 _) ->
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

  | Stateful (_, _, SF4 (_, e1, e2, e3, e4)) -> f (f (f (f i e1) e2) e3) e4
  | Stateful (_, _, SF4s (_, e1, e2, e3, e4s)) ->
      fl (f (f (f i e1) e2) e3) e4s

  | Stateful (_, _, SF6 (_, e1, e2, e3, e4, e5, e6)) -> f (f (f (f (f (f i e1) e2) e3) e4) e5) e6

  | Stateful (_, _, Top { size ; by ; top_time ; duration ; top_what ; max_size ; sigmas }) ->
      om (fl i [ size ; by ; top_time ; duration ; sigmas ; top_what ]) max_size

  | Stateful (_, _, Past { what ; time ; max_age ; sample_size }) ->
      om (f (f (f i what) time) max_age) sample_size

  | Generator (Split (e1, e2)) -> f (f i e1) e2

(* Fold depth first, calling [f] bottom up: *)
let rec fold_up f s i e =
  let i = fold_subexpressions (fold_up f) s i e in
  f s i e

let rec fold_down f s i e =
  let i = f s i e in
  fold_subexpressions (fold_down f) s i e

(* Iterate bottom up by default as that's what most callers expect: *)
let fold f = fold_up f []

let iter f =
  fold (fun s () e -> f s e) ()

let unpure_iter f e =
  fold (fun s () e -> match e.text with
    | Stateful _ -> f s e
    | _ -> ()
  ) () e |> ignore

let unpure_fold i f e =
  fold (fun s i e -> match e.text with
    | Stateful _ -> f s i e
    | _ -> i
  ) i e

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

let is_typed e =
  try
    iter (fun _s e ->
      if e.typ.DT.typ = DT.Unknown then raise Exit
    ) e ;
    true
  with Exit -> false

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

(*$T
  false < true
  parse "false" < parse "true"
*)

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
let of_nary ~typ ~nullable ~units op lst =
  let rec loop = function
    | [] ->
        (match op with
        | And -> of_bool true
        | Or -> of_bool false
        | _ -> todo "of_nary for any operation")
    | x::rest ->
        make ~typ ~nullable ?units
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
    of_nary ~typ:e.typ.DT.typ
            ~nullable:e.typ.DT.nullable
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
        duration >>: fun x ->
          (* In many cases the duration will be an unsigned. This will help to
           * keep it as such for some operators, for instance the modulo. *)
          let v =
            if x < 4294967296. && Float.floor x = x then
              T.VU32 (Uint32.of_float x)
            else
              T.VFloat x in
          make ~units:Units.seconds (Stateless (SL0 (Const T.(to_wire v))))
      ) |<| (
        (* Cannot use [T.Parser.p] because it would be ambiguous with the
         * compound values from expressions: *)
        T.Parser.(empty_list |<| scalar ~min_int_width:32) >>:
        fun c ->
          (* We'd like to consider all constants as dimensionless, but that'd
             be a pain (for instance, COALESCE(x, 0) would be invalid if x had
             a unit, while by leaving the const unit unspecified it has the
             unit of x.
          let units =
            if T.(is_a_num (type_of_value c)) then
              Some Units.dimensionless
            else None in*)
          make (Stateless (SL0 (Const T.(to_wire c))))
      )
    ) m

  (*$= const & ~printer:BatPervasives.identity
    "true" \
      (test_expr ~printer:(print false) const "true")

    "15" \
      (test_expr ~printer:(print false) const "15i8")

    "#\\A" \
      (test_expr ~printer:(print false) const "#\\A")

    "900{seconds}" \
      (test_expr ~printer:(print false) const "15min")

    "7{seconds}" \
      (test_expr ~printer:(print false) const "7 SECONDS")
  *)

  let null m =
    (
      T.Parser.null >>:
      fun v ->
        make (Stateless (SL0 (Const T.(to_wire v)))) (* Type of "NULL" is yet unknown *)
    ) m

  let variable m =
    let m = "variable" :: m in
    (
      Variable.parse >>: fun n -> make (Stateless (SL0 (Variable n)))
    ) m

  let param_name m =
    let m = "parameter" :: m in
    (
      (* You can choose any tuple as long as it's Param: *)
      optional ~def:() (
        Variable.parse +- char '.' >>:
        fun p ->
          if p <> Param then raise (Reject "not a param")
      ) -+ non_keyword
    ) m

  let param =
    param_name >>:
    fun n ->
      make (Stateless (SL2 (
        Get, const_of_string n, make (Stateless (SL0 (Variable Param))))))

  (*$= param & ~printer:BatPervasives.identity
    "param.'glop'" \
      (test_expr ~printer:(print false) param "glop")
    "param.'glop'" \
      (test_expr ~printer:(print false) param "param.glop")
  *)

  let immediate_or_param m =
    let m = "an immediate or a parameter" :: m in
    (const |<| param) m

  let state_lifespan m =
    let m = "state lifespan" :: m in
    (
      (worD "globally" >>: fun () -> GlobalState) |<|
      (worD "locally" >>: fun () -> LocalState)
    ) m

  let skip_nulls m =
    let m = "skip nulls" :: m in
    (
      ((strinG "skip" >>: fun () -> true) |<|
       (strinG "keep" >>: fun () -> false)) +-
      blanks +- worDs "null"
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
      several_greedy ~sep:T.Parser.tup_sep p +-
      opt_blanks +- char ']' >>:
      fun es ->
        let num_items = List.length es in
        assert (num_items >= 1) ;
        make (Vector es)
    ) m

  (* operators with lowest precedence *)
  let rec lowestest_prec_left_assoc m =
    let m = "logical OR operator" :: m in
    let op = worD "or"
    and reduce e1 _op e2 = make (Stateless (SL2 (Or, e1, e2))) in
    (* FIXME: we do not need a blanks if we had parentheses ("(x)OR(y)" is OK) *)
    binary_ops_reducer ~op ~term:lowest_prec_left_assoc ~sep:blanks ~reduce m

  and lowest_prec_left_assoc m =
    let m = "logical AND operator" :: m in
    let op = worD "and"
    and reduce e1 _op e2 = make (Stateless (SL2 (And, e1, e2))) in
    binary_ops_reducer ~op ~term:conditional ~sep:blanks ~reduce m

  and conditional m =
    let m = "conditional expression" :: m in
    (
      if_ |<| low_prec_left_assoc
    ) m

  and low_prec_left_assoc m =
    let m = "comparison operator" :: m in
    let op =
      that_string ">=" |<| that_string ">" |<|
      that_string "<>" |<| that_string "<=" |<| that_string "<" |<|
      that_string "=" |<| that_string "!=" |<| that_string "in" |<|
      (strinG "not" -- blanks -- worD "in" >>: fun () -> "not in") |<|
      that_string "like" |<|
      ((that_string "starts" |<| that_string "ends") +- blanks +- worD "with")
    and reduce e1 op e2 = match op with
      | ">" -> make (Stateless (SL2 (Gt, e1, e2)))
      | "<" -> make (Stateless (SL2 (Gt, e2, e1)))
      | ">=" -> make (Stateless (SL2 (Ge, e1, e2)))
      | "<=" -> make (Stateless (SL2 (Ge, e2, e1)))
      | "=" -> make (Stateless (SL2 (Eq, e1, e2)))
      | "!=" | "<>" ->
          make (Stateless (SL1 (Not, make (Stateless (SL2 (Eq, e1, e2))))))
      | "in" ->
          (* Turn 'in [x]' into '= x': *)
          (match e2.text with
          | Vector [ x ] ->
              make (Stateless (SL2 (Eq, e1, x)))
          | _ ->
              make (Stateless (SL2 (In, e1, e2))))
      | "not in" ->
          (* Turn 'not in [x]' into '<> x': *)
          (match e2.text with
          | Vector [ x ] ->
              make (Stateless (SL1 (Not, make (Stateless (SL2 (Eq, e1, x))))))
          | _ ->
              make (Stateless (SL1 (Not, make (Stateless (SL2 (In, e1, e2)))))))
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
    let op = that_string "+" |<| that_string "-" |<| that_string "||" |<|
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
    let op = that_string "*" |<| that_string "//" |<| that_string "/" |<|
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
          | Stateless (SL0 (Const c1)),
            Stateless (SL0 (Const c2))
            when is_ip T.(of_wire c1) && is_integer T.(of_wire c2) ->
              raise (Reject "That's a CIDR")
          | _ ->
              make (Stateless (SL2 (Div, e1, e2))))
      | _ -> assert false
    in
    binary_ops_reducer ~op ~term:higher_prec_left_assoc ~sep:opt_blanks ~reduce m

  and higher_prec_left_assoc m =
    let m = "bitwise logical operator" :: m in
    let op = that_string "&" |<| that_string "|" |<| that_string "#" |<|
             that_string "<<" |<| that_string ">>"
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
        make (Stateless (SL1 (Not, e)))) |<|
      (strinG "-" -- opt_blanks --
        check (nay decimal_digit) -+ highestest_prec >>: fun e ->
          make (Stateless (SL1 (Minus, e)))) |<|
      (highestest_prec ++
        optional ~def:None (
          blanks -- strinG "is" -- blanks -+
          optional ~def:(Some false)
                   (strinG "not" -- blanks >>: fun () -> Some true) +-
          worD "null") >>: function
            | e, None -> e
            | e, Some false ->
                make (Stateless (SL1 (Not,
                  make (Stateless (SL1 (Defined, e))))))
            | e, Some true ->
                make (Stateless (SL1 (Defined, e)))) |<|
      (strinG "begin" -- blanks -- strinG "of" -- blanks -+ highestest_prec >>:
        fun e -> make (Stateless (SL1 (BeginOfRange, e)))) |<|
      (strinG "end" -- blanks -- strinG "of" -- blanks -+ highestest_prec >>:
        fun e -> make (Stateless (SL1 (EndOfRange, e))))
    ) m

  and sugared_get m =
    let m = "dereference" :: m in
    (* Circonvolutions to avoid left recursion: *)
    let dotted_comp m =
      let m = "dotted path component" :: m in
      (
        char '.' -- nay Variable.parse -+ non_keyword >>: fun n ->
          make (Stateless (SL0 (Const (VString n))))
      ) m
    and indexed_comp m =
      let m = "indexed path component" :: m in
      (
        char '[' -+ p +- char ']'
      ) m in
    let comp =
      dotted_comp |<| indexed_comp in
    let dotted_first =
      (
        (variable |<| parenthesized func |<| record) ++
        dotted_comp ++ repeat ~sep:none comp
      ) |<| (
        nay Variable.parse -+ non_keyword ++
        repeat ~sep:none comp >>:
          fun (n, cs) ->
            (make (Stateless (SL0 (Variable Unknown))),
             make (Stateless (SL0 (Const (VString n))))), cs
      )
    and indexed_first =
      (variable |<| parenthesized func |<| vector p) ++
      indexed_comp ++ repeat ~sep:none comp
    in
    (
      (dotted_first |<| indexed_first) >>:
      fun ((e, n), ns) ->
        List.fold_left (fun e n ->
          make (Stateless (SL2 (Get, n, e)))
        ) (make (Stateless (SL2 (Get, n, e)))) ns
    ) m

  (*$= sugared_get & ~printer:BatPervasives.identity
    "in.'glop'" \
      (test_expr ~printer:(print false) sugared_get "in.glop")
    "GET(\"glop\", in.'pas')" \
      (test_expr ~printer:(print false) sugared_get "in.pas.glop")
    "in.'pas'[42]" \
      (test_expr ~printer:(print false) sugared_get "in.pas[42]")
    "in.'pas'[24][42]" \
      (test_expr ~printer:(print false) sugared_get "in.pas[24][42]")
    "unknown.'glop'" \
      (test_expr ~printer:(print false) sugared_get "glop")
    "in[42]" \
      (test_expr ~printer:(print false) sugared_get "in[42]")
    "[0; 1][1]" \
      (test_expr ~printer:(print false) sugared_get "[0;1][1]")
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
    let sep = check (char '(') |<| blanks in
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
     blanks ++ highestest_prec_no_parenthesis >>: fun (f, e) -> f, [e]) |<|
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

  and afun6_sf ?def_state n =
    afun_sf ?def_state 6 n >>: function (g, [a;b;c;d;e;f]) -> g, a, b, c, d, e, f | _ -> assert false

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
    let sep = check (char '(') |<| blanks in
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
      (afun1 "age" >>: fun e -> make (Stateless (SL1 (Age, e)))) |<|
      (afun1 "force" >>: fun e -> make (Stateless (SL1 (Force, e)))) |<|
      (afun1 "abs" >>: fun e -> make (Stateless (SL1 (Abs, e)))) |<|
      (afun1 "length" >>: fun e -> make (Stateless (SL1 (Length, e)))) |<|
      (afun1 "lower" >>: fun e -> make (Stateless (SL1 (Lower, e)))) |<|
      (afun1 "upper" >>: fun e -> make (Stateless (SL1 (Upper, e)))) |<|
      (afun1 "uuid_of_u128" >>: fun e -> make (Stateless (SL1 (UuidOfU128, e)))) |<|
      (worD "now" >>: fun () -> make (Stateless (SL0 Now))) |<|
      (worD "random" >>: fun () -> make (Stateless (SL0 Random))) |<|
      (worD "pi" >>: fun () -> make (Stateless (SL0 Pi))) |<|
      (worD "#start" >>: fun () -> make (Stateless (SL0 EventStart))) |<|
      (worD "#stop" >>: fun () -> make (Stateless (SL0 EventStop))) |<|
      (afun1 "exp" >>: fun e -> make (Stateless (SL1 (Exp, e)))) |<|
      (afun1 "log" >>: fun e -> make (Stateless (SL1 (Log, e)))) |<|
      (afun1 "log10" >>: fun e -> make (Stateless (SL1 (Log10, e)))) |<|
      (afun1 "sqrt" >>: fun e -> make (Stateless (SL1 (Sqrt, e)))) |<|
      (afun1 "square" >>: fun e -> make (Stateless (SL1 (Sq, e)))) |<|
      (afun1 "sq" >>: fun e -> make (Stateless (SL1 (Sq, e)))) |<|
      (afun1 "ceil" >>: fun e -> make (Stateless (SL1 (Ceil, e)))) |<|
      (afun1 "floor" >>: fun e -> make (Stateless (SL1 (Floor, e)))) |<|
      (afun1 "round" >>: fun e -> make (Stateless (SL1 (Round, e)))) |<|
      (afun1 "cos" >>: fun e -> make (Stateless (SL1 (Cos, e)))) |<|
      (afun1 "sin" >>: fun e -> make (Stateless (SL1 (Sin, e)))) |<|
      (afun1 "tan" >>: fun e -> make (Stateless (SL1 (Tan, e)))) |<|
      (afun1 "acos" >>: fun e -> make (Stateless (SL1 (ACos, e)))) |<|
      (afun1 "asin" >>: fun e -> make (Stateless (SL1 (ASin, e)))) |<|
      (afun1 "atan" >>: fun e -> make (Stateless (SL1 (ATan, e)))) |<|
      (afun1 "cosh" >>: fun e -> make (Stateless (SL1 (CosH, e)))) |<|
      (afun1 "sinh" >>: fun e -> make (Stateless (SL1 (SinH, e)))) |<|
      (afun1 "tanh" >>: fun e -> make (Stateless (SL1 (TanH, e)))) |<|
      (afun1 "truncate" >>: fun e ->
         make (Stateless (SL2 (Trunc, e, of_float 1.)))) |<|
      (afun2 "truncate" >>: fun (e1, e2) ->
         make (Stateless (SL2 (Trunc, e1, e2)))) |<|
      (afun1 "hash" >>: fun e -> make (Stateless (SL1 (Hash, e)))) |<|
      (afun1 "sparkline" >>: fun e -> make (Stateless (SL1 (Sparkline, e)))) |<|
      (afun1_sf "min" >>: fun ((g, n), e) ->
         make (Stateful (g, n, SF1 (AggrMin, e)))) |<|
      (afun1_sf "max" >>: fun ((g, n), e) ->
         make (Stateful (g, n, SF1 (AggrMax, e)))) |<|
      (afun1_sf "sum" >>: fun ((g, n), e) ->
         make (Stateful (g, n, SF1 (AggrSum, e)))) |<|
      (afun1_sf "avg" >>: fun ((g, n), e) ->
         make (Stateful (g, n, SF1 (AggrAvg, e)))) |<|
      (afun1_sf "and" >>: fun ((g, n), e) ->
         make (Stateful (g, n, SF1 (AggrAnd, e)))) |<|
      (afun1_sf "or" >>: fun ((g, n), e) ->
         make (Stateful (g, n, SF1 (AggrOr, e)))) |<|
      (afun1_sf "bitand" >>: fun ((g, n), e) ->
         make (Stateful (g, n, SF1 (AggrBitAnd, e)))) |<|
      (afun1_sf "bitor" >>: fun ((g, n), e) ->
         make (Stateful (g, n, SF1 (AggrBitOr, e)))) |<|
      (afun1_sf "bitxor" >>: fun ((g, n), e) ->
         make (Stateful (g, n, SF1 (AggrBitXor, e)))) |<|
      (afun1_sf "first" >>: fun ((g, n), e) ->
         make (Stateful (g, n, SF1 (AggrFirst, e)))) |<|
      (afun1_sf "last" >>: fun ((g, n), e) ->
         make (Stateful (g, n, SF1 (AggrLast, e)))) |<|
      (afun1_sf "group" >>: fun ((g, n), e) ->
         make (Stateful (g, n, SF1 (Group, e)))) |<|
      (afun1_sf "all" >>: fun ((g, n), e) ->
         make (Stateful (g, n, SF1 (Group, e)))) |<|
      (afun1_sf "count" >>: fun ((g, n), e) ->
         make (Stateful (g, n, SF1 (Count, e)))) |<|
      (
        let perc =
          immediate_or_param +-
          (optional ~def:() (worD "th")) in
        dismiss_error_if (parsed_fewer_than 5) (
          (perc |<| vector perc) +- blanks ++
          afun1 "percentile" >>:
          fun (ps, e) ->
            make (Stateless (SL2 (Percentile, e, ps))))
      ) |<|
      (afun2_sf "lag" >>: fun ((g, n), e1, e2) ->
         make (Stateful (g, n, SF2 (Lag, e1, e2)))) |<|
      (afun1_sf "lag" >>: fun ((g, n), e) ->
         make (Stateful (g, n, SF2 (Lag, one (), e)))) |<|

      (afun3_sf "season_moveavg" >>: fun ((g, n), e1, e2, e3) ->
         make (Stateful (g, n, SF3 (MovingAvg, e1, e2, e3)))) |<|
      (afun2_sf "moveavg" >>: fun ((g, n), e1, e2) ->
         make (Stateful (g, n, SF3 (MovingAvg, one (), e1, e2)))) |<|
      (afun4_sf "smooth_damped_holt" >>: fun ((g, n), e1, e2, e3, e4) ->
         make (Stateful (g, n, SF4 (DampedHolt, e1, e2, e3, e4)))) |<|
      (afun6_sf "smooth_damped_holt_winter" >>: fun ((g, n), e1, e2, e3, e4, e5, e6) ->
         make (Stateful (g, n, SF6 (DampedHoltWinter, e1, e2, e3, e4, e5, e6)))) |<|
      (afun3v_sf "season_fit_multi" >>: fun ((g, n), e1, e2, e3, e4s) ->
         make (Stateful (g, n, SF4s (MultiLinReg, e1, e2, e3, e4s)))) |<|
      (afun2v_sf "fit_multi" >>: fun ((g, n), e1, e2, e3s) ->
         make (Stateful (g, n, SF4s (MultiLinReg, one (), e1, e2, e3s)))) |<|
      (afun6_sf "smooth" >>: fun ((g, n), e1, e2, e3, e4, e5, e6) ->
         make (Stateful (g, n, SF6 (DampedHoltWinter, e1, e2, e3, e4, e5, e6)))) |<|
      (afun4_sf "smooth" >>: fun ((g, n), e1, e2, e3, e4) ->
         make (Stateful (g, n, SF4 (DampedHolt, e1, e2, e3, e4)))) |<|
      (afun2_sf "smooth" >>: fun ((g, n), e1, e2) ->
         make (Stateful (g, n, SF2 (ExpSmooth, e1, e2)))) |<|
      (afun1_sf "smooth" >>: fun ((g, n), e) ->
         let alpha = of_float 0.5 in
         make (Stateful (g, n, SF2 (ExpSmooth, alpha, e)))) |<|
      (afun4_sf "remember" >>: fun ((g, n), fpr, tim, dur, e) ->
         make (Stateful (g, n, SF4 (Remember true, fpr, tim, dur, e)))) |<|
      (afun4_sf "recall" >>: fun ((g, n), fpr, tim, dur, e) ->
         make (Stateful (g, n, SF4 (Remember false, fpr, tim, dur, e)))) |<|
      (afun1_sf "distinct" >>: fun ((g, n), e) ->
         make (Stateful (g, n, SF1 (Distinct, e)))) |<|
      (afun3_sf "hysteresis" >>: fun ((g, n), value, accept, max) ->
         make (Stateful (g, n, SF3 (Hysteresis, value, accept, max)))) |<|
      (afun4_sf "histogram" >>:
       fun ((g, n), what, min, max, num_buckets) ->
         match float_of_const min,
               float_of_const max,
               int_of_const num_buckets with
         | Some min, Some max, Some num_buckets ->
             if num_buckets <= 0 then
               raise (Reject "Histogram size must be positive") ;
             make (Stateful (g, n, SF1 (
              AggrHistogram (min, max, Uint32.of_int num_buckets), what)))
         | _ -> raise (Reject "histogram dimensions must be constants")) |<|
      (afun2 "split" >>: fun (e1, e2) ->
         make (Generator (Split (e1, e2)))) |<|
      (afun2 "format_time" >>: fun (e1, e2) ->
         make (Stateless (SL2 (Strftime, e1, e2)))) |<|
      (afun1 "parse_time" >>: fun e ->
         make (Stateless (SL1 (Strptime, e)))) |<|
      (afun1 "chr" >>: fun e ->
         match int_of_const e with
         | Some v ->
              if v < 0 || v > 255 then
                raise (Reject "const must be between 0 and 255") ;
              make (Stateless (SL1 (Chr, e)))
         | _ -> make (Stateless (SL1 (Chr, e)))) |<|
      (afun1 "variant" >>: fun e ->
         make (Stateless (SL1 (Variant, e)))) |<|
      (afun1 "fit" >>: fun e ->
        make (Stateless (SL1 (Fit, e)))) |<|
      (afun1 "countrycode" >>: fun e ->
        make (Stateless (SL1 (CountryCode, e)))) |<|
      (afun1 "ipfamily" >>: fun e ->
        make (Stateless (SL1 (IpFamily, e)))) |<|
      (afun1 "basename" >>: fun e ->
        make (Stateless (SL1 (Basename, e)))) |<|
      (* At least 2 args to distinguish from the aggregate functions: *)
      (afun2v "max" >>: fun (e1, e2, e3s) ->
         make (Stateless (SL1s (Max, e1 :: e2 :: e3s)))) |<|
      (afun1v "greatest" >>: fun (e, es) ->
         make (Stateless (SL1s (Max, e :: es)))) |<|
      (afun2v "min" >>: fun (e1, e2, e3s) ->
         make (Stateless (SL1s (Min, e1 :: e2 :: e3s)))) |<|
      (afun1v "least" >>: fun (e, es) ->
         make (Stateless (SL1s (Min, e :: es)))) |<|
      (afun1v "print" >>: fun (e, es) ->
         make (Stateless (SL1s (Print, e :: es)))) |<|
      (afun2 "reldiff" >>: fun (e1, e2) ->
        make (Stateless (SL2 (Reldiff, e1, e2)))) |<|
      (afun2_sf "sample" >>: fun ((g, n), c, e) ->
        make (Stateful (g, n, SF2 (Sample, c, e)))) |<|
      (afun2 "index" >>: fun (s, a) ->
        make (Stateless (SL2 (Index, s, a)))) |<|
      (afun3 "substring" >>: fun (s, a, b) ->
        make (Stateless (SL3 (SubString, s, a, b)))) |<|
      (afun3 "mapadd" >>: fun (m, k, v) ->
        make (Stateless (SL3 (MapSet, m, k, v)))) |<|
      dismiss_error_if (parsed_fewer_than 5) (
        k_moveavg |<| cast |<| top_expr |<| nth |<| largest |<| past |<| get |<|
        changed_field |<| peek |<| once_every |<| one_out_of)
    ) m

  and get m =
    let m = "get" :: m in
    (
      afun2 "get" >>: fun (n, v) ->
        (match n.text with
        | Stateless (SL0 (Const _)) ->
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
          if pref <> Variable.Out && pref <> Variable.Unknown then
            raise (Reject "Changed operator is only valid for \
                           fields of the output tuple") ;
          make (Stateless (SL2 (Get, n,
            make (Stateless (SL0 (Variable Variable.OutPrevious))))))
        in
        let prev_field =
          match f.text with
          | Stateless (SL2 (Get, n, { text = Stateless (SL0 (Variable pref)) ;
                                      _ })) ->
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
    let cast_as_func =
      let sep = check (char '(') in
      T.Parser.typ +- sep ++ highestest_prec >>:
      fun (t, e) ->
        (* The nullability of [value] should propagate to [type(value)],
         * while [type?(value)] should be nullable no matter what. *)
        make (Stateless (SL1 (Cast t, e))) in
    let cast_a_la_sql =
      strinG "cast" -- opt_blanks -- char '(' -- opt_blanks -+
      highestest_prec +- blanks +- strinG "as" +- blanks ++
      T.Parser.typ +- opt_blanks +- char ')' >>:
      fun (e, t) ->
        make (Stateless (SL1 (Cast t, e))) in
    (cast_as_func |<| cast_a_la_sql) m

  and peek m =
    let m = "peek" :: m in
    (
      strinG "peek" -- blanks -+
      DT.Parser.typ +- blanks ++
      optional ~def:LittleEndian (
        (
          (strinG "little" >>: fun () -> LittleEndian) |<|
          (strinG "big" >>: fun () -> BigEndian)
        ) +- blanks +- strinG "endian" +- blanks) ++
      highestest_prec >>:
      fun ((typ, endianness), e) ->
        make (Stateless (SL1 (Peek ((DT.required typ), endianness), e)))
    ) m

  and one_out_of m =
    let m = "one-out-of" :: m in
    let sep = check (char '(') |<| blanks in
    (
      strinG "one" -- blanks -- strinG "out" -- blanks --
      strinG "of" -- blanks -+
      highestest_prec ++
      state_and_nulls +-
      sep ++ highestest_prec >>:
      fun ((i, (g, n)), e) ->
        make (Stateful (g, n, SF2 (OneOutOf, i, e)))
    ) m

  and once_every m =
    let m = "once-every" :: m in
    (
      optional ~def:() (strinG "once" -- blanks) -+ (
      (
        (* Natural syntax *)
        let sep = check (char '(') |<| blanks in
        strinG "every" -- blanks -+
        window_length ++
        state_and_nulls +- sep ++
        highestest_prec >>:
        fun (((d, tumbling), (g, n)), e) ->
          let op = OnceEvery tumbling in
          make (Stateful (g, n, SF3 (op, d, default_start, e)))
      ) |<| (
        (* Functional syntax, default event-time *)
        afun3_sf "every" >>:
        fun ((g, n), d, tumb, e) ->
          match bool_of_const tumb with
          | None ->
              raise (Reject "tumbling must be a boolean")
          | Some tumbling ->
              let op = OnceEvery tumbling in
              make (Stateful (g, n, SF3 (op, d, default_start, e)))
      ) |<| (
        (* Functional syntax, explicit event time *)
        afun4_sf "every" >>:
        fun ((g, n), d, tumb, t, e) ->
          match bool_of_const tumb with
          | None ->
              raise (Reject "tumbling must be a boolean")
          | Some tumbling ->
              let op = OnceEvery tumbling in
              make (Stateful (g, n, SF3 (op, d, t, e)))
      ))
    ) m

  and k_moveavg m =
    let m = "k-moving average" :: m in
    let sep = check (char '(') |<| blanks in
    (
      (unsigned_decimal_number >>: T.Parser.narrowest_int_scalar) +-
      (strinG "-moveavg" |<| strinG "-ma") ++
      state_and_nulls +-
      sep ++ highestest_prec >>:
      fun ((k, (g, n)), e) ->
        if k = VNull then raise (Reject "Cannot use NULL here") ;
        let k = make (Stateless (SL0 (Const T.(to_wire k)))) in
        make (Stateful (g, n, SF3 (MovingAvg, one (), k, e)))
    ) m

  and top_expr m =
    let m = "top expression" :: m in
    (
      (
        (
          (
            (strinG "rank" -- blanks -- strinG "of" >>: fun () -> Rank) |<|
            (strinG "is" >>: fun () -> Membership)
          ) +- blanks ++
          (* We can allow lowest precedence expressions here because of the
           * keywords that follow: *)
          p +- blanks +-
          strinG "in" +- blanks +- strinG "top" +- blanks ++ immediate_or_param ++
          optional ~def:None (
            some (blanks -- strinG "over" -- blanks -+ p))
        ) |<| (
          (* We'd like to have "top 2 x" returns that list, and then
           * "y in top 2 x" be interpreted as "(y) in (top 2 x)" but that
           * would need specific optimisation in the code generation phase
           * to recognize this frequent pattern and skip the actual list
           * construction. For now we must disambiguate those cases, including
           * for the parser, thus this questionable "list top 2 x" syntax: *)
          strinG "list" -- blanks -- strinG "top" -- blanks -+ immediate_or_param ++
          optional ~def:None (
            some (blanks -- strinG "over" -- blanks -+ p)) +- blanks ++
          p >>:
          fun ((size, max_size), what) -> ((List, what), size), max_size
        )
      ) ++
      state_and_nulls ++
      optional ~def:default_one (
        blanks -- strinG "by" -- blanks -+ highestest_prec) ++
      optional ~def:None (
        blanks -- strinG "at" -- blanks -- strinG "time" -- blanks -+ some p) ++
      optional ~def:None (
        blanks -- strinG "for" --
        optional ~def:() (blanks -- strinG "the" -- blanks -- strinG "last") --
        blanks -+ some immediate_or_param) ++
      optional ~def:None (
        some (blanks -- strinG "above" -- blanks -+ p +- blanks +-
              strinGs "sigmas")) >>:
      fun ((((((((output, top_what), size), max_size),
               (g, n)), by), top_time), duration), sigmas) ->
        let sigmas = sigmas |? default_zero in
        let top_time, duration =
          match top_time, duration with
          (* If we asked for no top_time decay, use neutral values: *)
          | None, None -> default_zero, default_1hour
          | Some t, None -> t, default_1hour
          | None, Some d -> default_start, d
          | Some t, Some d -> t, d
        in
        make (Stateful (g, n, Top {
          output ; size ; max_size ; top_what ; by ; duration ; top_time ; sigmas }))
    ) m

  and largest m =
    let m = "largest expression" :: m in
    let up_to_c =
      blanks -+
      optional ~def:false (
        strinG "up" -- blanks -- strinG "to" -- blanks >>: fun () -> true
      ) ++ immediate_or_param in
    let but =
      optional ~def:default_zero (
        blanks -- strinG "but" -- blanks -+ immediate_or_param
      ) in
    (
      (
        (
          (strinG "largest" >>: fun () -> false) |<|
          (strinG "smallest" >>: fun () -> true)
        ) ++ but ++ up_to_c ++
        state_and_nulls +- opt_blanks ++ p ++
        optional ~def:[] (
          blanks -- strinG "by" -- blanks -+
          several_greedy ~sep:list_sep p) >>:
          fun (((((inv, but), (up_to, c)), (g, n)), e), es) ->
            (* The result is null when the number of input is less than c: *)
            make (Stateful (g, n, SF4s (Largest { inv ; up_to }, c, but, e, es)))
      ) |<| (
        (
          (strinG "latest" >>: fun () -> false) |<|
          (strinG "oldest" >>: fun () -> true)
        ) ++ but ++ up_to_c ++ state_and_nulls +- opt_blanks ++ p >>:
          fun ((((inv, but), (up_to, c)), (g, n)), e) ->
            make (Stateful (g, n, SF4s (Largest { inv ; up_to }, c, but, e, [])))
      ) |<| (
        strinG "earlier" -+ up_to_c ++ state_and_nulls +- opt_blanks ++ p >>:
          fun (((up_to, c), (g, n)), e) ->
            let inv = false and but = zero () in
            make (Stateful (g, n, SF4s (Largest { inv ; up_to }, c, but, e, [])))
      )
    ) m

  and sample m =
    let m = "sample expression" :: m in
    (
      strinG "sample" -- blanks --
      optional ~def:() (strinG "of" -- blanks -- strinG "size" -- blanks) -+
      p +- optional ~def:() (blanks -- strinG "of" -- blanks -- strinG "the")
    ) m

  and window_length m =
    let m = "time window length" :: m in
    (
      p ++
      optional ~def:false (blanks -+
        (
          (worD "sliding" >>: fun () -> false) |<|
          (worD "tumbling" >>: fun () -> true)
        )
      )
    ) m

  and past m =
    let m = "past expression" :: m in
    (
      optional ~def:None (some sample +- blanks) +-
      strinG "past" +- blanks ++
      window_length ++
      state_and_nulls +-
      optional ~def:() (opt_blanks +- strinG "of") +-
      blanks ++ p ++
      optional ~def:default_start
        (blanks -- strinG "at" -- blanks -- strinG "time" -- blanks -+ p) >>:
      fun ((((sample_size, (max_age, tumbling)), (g, n)), what), time) ->
        let e = Past { what ; time ; max_age ; tumbling ; sample_size } in
        make (Stateful (g, n, e))
    ) m

  and nth m =
    let m = "n-th" :: m in
    let q =
      pos_decimal_integer "nth" ++
      (that_string "th" |<| that_string "st" |<| that_string "nd" |<|
       that_string "rd") >>:
      fun (n, th) ->
        if n = 0 then raise (Reject "tuple indices start at 1") ;
        if ordinal_suffix n = th then n
        (* Pedantic but also helps disambiguating the syntax: *)
        else raise (Reject ("bad suffix "^ th ^" for "^ string_of_int n))
    and sep = check (char '(') |<| blanks in
    (
      q +- sep ++ highestest_prec >>:
      fun (n, es) ->
        let n = make (Stateless (SL0 (Const T.(to_wire (scalar_of_int (n - 1)))))) in
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
      several_greedy ~sep:blanks alt +- blanks ++
      optional ~def:None (
        strinG "else" -- blanks -+ some p +- blanks) +-
      worD "end" >>:
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
      ) |<| (
        afun2 "if" >>:
        fun (case_cond, case_cons) ->
          make (Case ([ { case_cond ; case_cons } ], None))
      ) |<| (
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
    q ++ optional ~def:None (opt_blanks -+ some Units.Parser.p) >>: function
      | e, None -> e
      | { text = Tuple _ ; _ }, _ ->
         (* We have to accept units on tuples because it make sense for
          * singletons, but only for singletons: *)
         raise (Reject "Cannot assign units to a compound type")
      | e, units -> { e with units }

  and highestest_prec_no_parenthesis m =
    (
      coalesce |<|
      accept_units null |<|
      (
        (* Some fields may reuse the name of some function or const, so
         * accept everything for now: *)
        func |||
        accept_units (const ||| sugared_get)
      ) |<|
      variable
    ) m

  and parenthesized p =
    char '(' -- opt_blanks -+ p +- opt_blanks +- char ')'

  and highestest_prec m =
    (
      highestest_prec_no_parenthesis |||
      accept_units tuple ||| vector p ||| record |||
      accept_units case (* delimited by END *)
    ) m

  and tuple m =
    let m = "tuple" :: m in
    (
      char '(' -- opt_blanks -+
      several_greedy ~sep:T.Parser.tup_sep p +-
      opt_blanks +- char ')' >>:
      function
        | [ e ] ->
            (* There is no singleton; this is just a parenthesized
             * expression: *)
            e
        | es ->
            make (Tuple es)
    ) m

  and record m =
    let m = "record" :: m in
    (
      char '{' -- opt_blanks -+
      several_greedy ~sep:T.Parser.tup_sep (
        non_keyword +- T.Parser.kv_sep ++ p >>:
          fun (k, v) -> N.field k, v) +-
      opt_blanks +- char '}' >>:
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

    "NOT((unknown.'zone_src') IS NOT NULL)" \
      (test_expr ~printer:(print false) p "zone_src IS NULL")

    "((NOT((unknown.'zone_src') IS NOT NULL)) OR ((unknown.'zone_src') = (unknown.'z1'))) AND ((NOT((unknown.'zone_dst') IS NOT NULL)) OR ((unknown.'zone_dst') = (unknown.'z2')))" \
      (test_expr ~printer:(print false) p "(zone_src IS NULL or zone_src = z1) and \\
                 (zone_dst IS NULL or zone_dst = z2)")

    "(SUM LOCALLY skip nulls(unknown.'bytes')) / (unknown.'avg_window')" \
      (test_expr ~printer:(print false) p "(sum bytes)/avg_window")

    "(unknown.'start') // ((1000000) * (unknown.'avg_window'))" \
      (test_expr ~printer:(print false) p "start // (1_000_000 * avg_window)")

    "param.'p' PERCENTILE(unknown.'bytes_per_sec')" \
      (test_expr ~printer:(print false) p "p percentile bytes_per_sec")

    "(MAX LOCALLY skip nulls(in.'start')) > ((out.'start') + (((unknown.'obs_window') * (1.15)) * (1000000)))" \
      (test_expr ~printer:(print false) p \
        "max in.start > out.start + (obs_window * 1.15) * 1_000_000")

    "(unknown.'x') % (unknown.'y')" \
      (test_expr ~printer:(print false) p "x % y")

    "ABS((unknown.'bps') - (LAG LOCALLY skip nulls(1, unknown.'bps')))" \
      (test_expr ~printer:(print false) p "abs(bps - lag(1,bps))")

    "HYSTERESIS LOCALLY skip nulls(unknown.'value', 900, 1000)" \
      (test_expr ~printer:(print false) p "hysteresis(value, 900, 1000)")

    "((4) & (4)) * (2)" \
      (test_expr ~printer:(print false) p "4 & 4 * 2")

    "#\\c" \
      (test_expr ~printer:(print false) p "#\\c")
  *)

  (*$= mid_prec_left_assoc & ~printer:BatPervasives.identity
    "(60{seconds}) + (60{seconds})" \
      (test_expr ~printer:(print false) mid_prec_left_assoc "1min + 1min")
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
      | Stateless (SL0 (Variable pref))
        when Variable.has_type_input pref || Variable.has_type_output pref ->
          Printf.sprintf2 "%s is not allowed to use '%s'"
            what (Variable.to_string pref) |>
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
    let open Program_parameter.DessserGen in
    match List.find (fun p -> p.ptyp.name = name) params with
    | exception Not_found ->
        Printf.sprintf2 "Unknown parameter %a while looking for units"
          N.field_print name |>
        failwith
    | p -> p.ptyp.units
  in
  let rec uoe ~indent e =
    let char_of_indent = Char.chr (Char.code 'a' + indent) in
    let prefix = Printf.sprintf "%s%c. " (String.make (indent * 2) ' ')
                                         char_of_indent in
    !logger.debug "%sUnits of expression %a...?" prefix (print true) e ;
    let indent = indent + 1 in
    if e.units <> None then e.units else
    (match e.text with
    | Stateless (SL0 (Const _)) ->
        None
    | Stateless (SL0 (Path [ Name n ])) -> (* Should not happen *)
        units_of_input n
    | Case (cas, else_opt) ->
        (* We merely check that the units of the alternatives are either
         * the same of unknown. *)
        List.iter (fun ca ->
          check_no_units ~indent "because it is a case condition" ca.case_cond
        ) cas ;
        let units_opt = Option.bind else_opt (uoe ~indent) in
        List.map (fun ca -> ca.case_cons) cas |>
        same_units ~indent "Conditional alternatives" units_opt
    | Stateless (SL1s (Coalesce, es)) ->
        same_units ~indent "Coalesce alternatives" None es
    | Stateless (SL0 (Now|EventStart|EventStop)) ->
        Some Units.seconds_since_epoch
    | Stateless (SL1 (Age, e)) ->
        check ~indent e Units.seconds_since_epoch ;
        Some Units.seconds
    | Stateless (SL1 (Strptime, e0)) ->
        check_no_units ~indent "because it's the argument to parse_time" e0 ;
        Some Units.seconds_since_epoch
    | Stateless (SL1 ((Peek _|Cast _|Force|Abs|Minus|Ceil|Floor|Round
                      |Cos|Sin|Tan|ACos|ASin|ATan|CosH|SinH|TanH), e))
    | Stateless (SL2 (Trunc, e, _)) ->
        uoe ~indent e
    | Stateless (SL1 (Length, _)) ->
        Some Units.chars
    | Stateless (SL1 (Sqrt, e)) ->
        Option.map (fun e -> Units.pow e 0.5) (uoe ~indent e)
    | Stateless (SL1 (Sq, e)) ->
        Option.map (fun e -> Units.pow e 2.) (uoe ~indent e)
    | Stateless (SL2 (Add, e1, e2)) ->
        option_map2 Units.add (uoe ~indent e1) (uoe ~indent e2)
    | Stateless (SL2 (Sub, e1, e2)) ->
        option_map2 Units.sub (uoe ~indent e1) (uoe ~indent e2)
    | Stateless (SL2 ((Mul|Mod), e1, e2)) ->
        option_map2 Units.mul (uoe ~indent e1) (uoe ~indent e2)
    | Stateless (SL2 ((Div|IDiv), e1, e2)) ->
        option_map2 Units.div (uoe ~indent e1) (uoe ~indent e2)
    | Stateless (SL2 (Pow, e1, e2)) ->
        (* Best effort in case the exponent is a constant, otherwise we
         * just don't know what the unit is. *)
        option_map2 Units.pow (uoe ~indent e1) (float_of_const e2)
    (* Although shifts could be seen as mul/div, we'd rather consider
     * only dimensionless values receive this treatment, esp. since
     * it's not possible here to tell the difference between a mul-shift
     * and a div-shift. *)
    | Stateless (SL2 ((And|Or|Concat|StartsWith|EndsWith|
                       BitAnd|BitOr|BitXor|BitShift), e1, e2)) ->
        let reason =
          Printf.sprintf2 "because it is an argument of %a"
            (print_text ~max_depth:0 false) e.text in
        check_no_units ~indent reason e1 ;
        check_no_units ~indent reason e2 ;
        None
    | Stateless (SL2 (Get, e1, { text = Vector es ; _ })) ->
        Option.bind (int_of_const e1) (fun n ->
          List.at es n |> uoe ~indent)
    | Stateless (SL2 (Get, n, { text = Tuple es ; _ })) ->
        (* Not super useful. FIXME: use the solver. *)
        let n = int_of_const n |>
                option_get "Get from tuple must have const index" __LOC__ in
        (try List.at es n |> uoe ~indent
        with Invalid_argument _ -> None)
    | Stateless (SL2 (Get, s, { text = Record kvs ; _ })) ->
        (* Not super useful neither and that's more annoying as records
         * are supposed to replace operation fields.
         * FIXME: Compute and set the units after type-checking using the
         *        solver. *)
        let s = string_of_const s |>
                option_get "Get from record must have string index" __LOC__ in
        (try
          list_rfind_map (fun (k, v) ->
            if k = s then Some v else None
          ) (kvs :> (string * t) list) |> uoe ~indent
        with Not_found -> None)
    | Stateless (SL2 (Get, { text = Stateless (SL0 (Const (VString n))) ; _ },
                           { text = Stateless (SL0 (Variable pref)) ; _ })) ->
        let n = N.field n in
        if Variable.has_type_input pref then
          units_of_input n
        else if Variable.has_type_output pref then
          units_of_output n
        else if pref = Param then
          units_of_params n
        else None
    | Stateless (SL2 (Percentile,
                      { text = Stateful (_, _, SF4s (Largest _,  _, _, e, _))
                             | Stateful (_, _, SF2 (Sample, _, e))
                             | Stateful (_, _, SF1 (Group, e)) ; _ }, _))
    | Stateful (_, _, SF2 (OneOutOf, _, e))
    | Stateless (SL1s (Print, e::_))
    | Stateful (_, _, SF1 ((AggrMin|AggrMax|AggrAvg|AggrFirst|AggrLast), e))
    | Stateful (_, _, SF2 ((Lag|ExpSmooth), _, e))
    | Stateful (_, _, SF3 (MovingAvg, _, _, e)) ->
        uoe ~indent e
    | Stateful (_, _, SF3 (OnceEvery _, _, time, x)) ->
        check_time ~indent "because it's the period of \
                            `once every` operator" time ;
        uoe ~indent x
    | Stateful (_, _, Past { time }) ->
        check_time ~indent "because it's the duration of the past operator"
                   time ;
        None
    | Stateful (_, _, Top { top_time ; sigmas ; size }) ->
        check_time ~indent "because it's the duration of the top operator"
                   top_time ;
        check_no_units ~indent "because it is a number of items" size ;
        check_no_units ~indent "because it is a number of deviations" sigmas ;
        None
    | Stateless (SL1 (Like _, e)) ->
        check_no_units ~indent "because it is used as pattern" e ;
        None
    | Stateless (SL1s ((Max|Min), es)) ->
        same_units ~indent "Min/Max alternatives" None es
    | Stateful (_, _, SF1 (AggrSum, e)) ->
        let u = uoe ~indent e in
        check_not_rel e u ;
        u
    | Stateful (_, _, SF1 (Count, _)) ->
        (* Or "tuples" if we had such a unit. *)
        Some Units.dimensionless
    | Generator (Split (e1, e2)) ->
        let reason = "Because it is an argument of split" in
        check_no_units ~indent reason e1 ;
        check_no_units ~indent reason e2 ;
        None
    | _ -> None) |>
    function
      | Some u as res ->
          !logger.debug "%s-> %a" prefix Units.print u ;
          res
      | None -> None

  and check ~indent e u =
    match uoe ~indent e with
    | None -> ()
    | Some u' ->
        if not (Units.eq u u') then
          !logger.warning "%a should have units %a not %a"
            (print false) e
            Units.print u
            Units.print u'

  and check_no_units ~indent reason e =
    match uoe ~indent e with
    | None -> ()
    | Some u ->
        !logger.warning "%a should have no units (%s) but has unit %a"
          (print false) e
          reason
          Units.print u

  and check_not_rel e u =
    Option.may (fun u ->
      if Units.is_relative u then
        !logger.warning "%a should not have relative unit but has unit %a"
          (print false) e
          Units.print u
    ) u

  and check_time ~indent reason e =
    match uoe ~indent e with
    | None -> ()
    | Some u when u = Units.seconds_since_epoch -> ()
    | Some u ->
        !logger.warning "%a should be a time (%s) but has unit %a"
          (print false) e
          reason
          Units.print u

  and same_units ~indent what i es =
    List.enum es /@ (uoe ~indent) |>
    Units.check_same_units ~what i

  in uoe ~indent:0

(* Return the set of all fields of given [tup_type] used in the expression: *)
let vars_of_expr tup_type e =
  fold (fun _ s e ->
    match e.text with
    | Stateless (SL2 (Get, { text = Stateless (SL0 (Const (VString n))) ; _ },
                           { text = Stateless (SL0 (Variable tt)) ; _ }))
      when tt = tup_type ->
        N.SetOfFields.add (N.field n) s
    | _ -> s
  ) N.SetOfFields.empty e
