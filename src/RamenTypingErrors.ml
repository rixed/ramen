(* Module to print/parse the names of SMT named assertions, and to build
 * a user error message out of the core kernel (that is: the almost
 * minimal set of un-satisfiable named assertions).
 *
 * We rely on actual types for each error case, that we convert to and
 * from strings automatically using the PPP library to avoid laborious and
 * error prone manual conversions.
 *)
open Batteries
open RamenHelpers
open RamenTypingHelpers
open RamenSmt
module C = RamenConf
module F = C.Func

type expr =
  | Nullability of bool
  | Sortable
  | Same
  | VecSame
  | CaseCond of int * int (* num over total number of conditions *)
  | CaseCons of int * int (* num over total number of consequent *)
  | CaseElse
  | CaseNullProp
  | CoalesceAlt of int
  | CoalesceNullLast of int
  | GettableByInt
  | GettableByName
  | AnyCidr
  | NumericVec
  | InType
  | LengthType
  | PrevNull
  | Integer
  | Signed
  | Unsigned
  | Numeric
  | ActualType of string
  | InheritType
  | InheritNull
    [@@ppp PPP_OCaml]

let string_of_index c t =
  assert (c < t) ;
  assert (c >= 0) ;
  if c = 0 && t = 1 then "" else
  if c = t - 1 then "last " else
  string_of_int (c + 1) ^ ordinal_suffix (c + 1) ^ " "

let print_expr oc =
  let p fmt = Printf.fprintf oc fmt in
  function
  | Nullability false -> p " must not be nullable"
  | Nullability true -> p " must be nullable"
  | Sortable -> p " must be sortable"
  | Same -> p ": both arguments must have the same type"
  | VecSame -> p ": all vector elements must have the same type"
  | CaseCond (c, t) ->
      p ": %scondition must be a boolean"
        (string_of_index c t)
  | CaseCons (c, t) ->
      p ": %sconsequent must have a type compatible with others"
        (string_of_index c t)
  | CaseElse -> p ": else clause must have a type compatible with other \
                   consequents"
  | CaseNullProp -> p ": case expression is as nullable as conditions and \
                       consequents"
  | CoalesceAlt a -> p ": alternative #%d of coalesce expression must have \
                        a type compatible with others" a
  | CoalesceNullLast a -> p ": alternative #%d of coalesce expression must \
                             be nullable iff it's the last one" a
  | GettableByInt -> p " must be a vector, a list or a tuple"
  | GettableByName -> p " must be a record"
  | AnyCidr -> p " must be a CIDR"
  | NumericVec -> p " must be a vector of numeric elements"
  | InType -> p ": arguments must be compatible with the IN operator"
  | LengthType -> p ": arguments must be compatible with the LENGTH operator"
  | PrevNull -> p " must be null as it is drawn from the previous tuple"
  | Integer -> p " must be an integer"
  | Signed -> p " must be a signed integer"
  | Unsigned -> p " must be an unsigned integer"
  | Numeric -> p " must be numeric"
  | ActualType t -> p " must be of type %s" t
  | InheritType -> p " must match all parents output"
  | InheritNull -> p " must match all parents nullability"

type func =
  | Clause of string * expr
  | Notif of int * expr
  | NotifParam of int * int * expr
  | Preprocessor of expr
  | Filename of expr
  | Unlink of expr
    [@@ppp PPP_OCaml]

let print_func oc =
  let p fmt = Printf.fprintf oc fmt in
  function
  | Clause (c, e) -> p "clause %s%a" c print_expr e
  | Notif (i, e) -> p "notification #%d%a" i print_expr e
  | NotifParam (i, j, e) -> p "notification #%d, parameter #%d%a"
                              i j print_expr e
  | Preprocessor e -> p "CSV preprocessor%a" print_expr e
  | Filename e -> p "CSV filename%a" print_expr e
  | Unlink e -> p "CSV unlink clause%a" print_expr e

type t = Expr of int * expr
       | Func of int * func
       | RunCondition
         [@@ppp PPP_OCaml]

exception ReturnExpr of RamenName.func * RamenExpr.t
let print funcs oc =
  let expr_of_id i =
    try
      List.iter (fun func ->
        let print_expr _ e =
          if e.E.uniq_num = i then
            raise (ReturnExpr (func.F.name, e)) in
        RamenOperation.iter_expr print_expr func.F.operation
      ) funcs ;
      assert false
    with ReturnExpr (f, e) -> f, e
  and func_of_id i = List.at funcs i
  and p fmt = Printf.fprintf oc fmt in
  function
  | Expr (i, e) ->
      let func_name, expr = expr_of_id i in
      p "In function %s: expression %s%a"
        (RamenName.func_color func_name)
        (RamenName.expr_color
          (IO.to_string (RamenExpr.print ~max_depth:3 false) expr))
        print_expr e
  | Func (i, e) ->
      let func_name = (func_of_id i).F.name in
      p "In function %s: %a"
        (RamenName.func_color func_name)
        print_func e
  | RunCondition -> p "running condition must be a non nullable boolean."

(* When annotating an assertion we must always use a unique name, even if we
 * annotate several times the very same expression. It is important to
 * name all those expression though, as otherwise there is no guaranty the
 * solver would choose to fail at the annotated one. Thus this sequence that
 * we use to uniquify annotations: *)
let uniquify =
  let seq = ref 0 in
  fun s ->
    incr seq ;
    s ^"_"^ string_of_int !seq

let deuniquify s =
  String.rsplit ~by:"_" s |> fst

let to_assert_name e =
  PPP.to_string t_ppp_ocaml e |> scramble |> uniquify

let of_assert_name n =
  deuniquify n |> unscramble |> PPP.of_string_exc t_ppp_ocaml

let print_core funcs oc lst =
  List.map of_assert_name lst |>
  Set.of_list |>
  Set.print ~first:"\n     " ~sep:"\n AND " ~last:"" (print funcs) oc
