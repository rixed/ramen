(* Module to print/parse the names of SMT named assertions, and to build
 * a user error message out of the core kernel (that is: the almost
 * minimal set of un-satisfiable named assertions).
 *
 * We rely on actual types for each error case, that we convert to and
 * from strings automatically using the PPP library to avoid laborious and
 * error prone manual conversions.
 *)
open Batteries
open RamenHelpersNoLog
open RamenTypingHelpers
open RamenSmt
module C = RamenConf
module VSI = RamenSync.Value.SourceInfo
module E = RamenExpr
module O = RamenOperation
module N = RamenName

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
  | CoalesceNullLast of int * int
  | GettableByInt
  | GettableByName
  | AnyIp
  | AnyCidr
  | NumericVec
  | Matrix
  | InType
  | LengthType
  | PrevNull
  | Integer
  | Signed
  | Unsigned
  | Numeric
  | Numeric_Or_Numerics
  | ActualType of string
  | InheritType
  | InheritNull
  | OpenedRecordIs of int (* expression uniq_num *)
  | MulType
  | PeekedType
  | MapType
  | MapNullability
  | TopOutput of E.top_output
    [@@ppp PPP_OCaml]

let string_of_index c t =
  assert (c < t) ;
  assert (c >= 0) ;
  if c = 0 && t = 1 then "" else
  if c = t - 1 then "last " else
  string_of_int (c + 1) ^ ordinal_suffix (c + 1) ^ " "

let expr_of_id funcs condition i =
  let exception ReturnExpr of (string * string * E.t list * E.t) in
  try
    List.iter (fun func ->
      let print_expr clause stack e =
        if e.E.uniq_num = i then
          let loc_name =
            Printf.sprintf2 "function %s" (N.func_color func.VSI.name) in
          raise (ReturnExpr (loc_name, clause, stack, e)) in
      O.iter_expr print_expr func.VSI.operation
    ) funcs ;
    E.iter (fun stack e ->
      if e.E.uniq_num = i then
        raise (ReturnExpr ("running condition", "", stack, e))
    ) condition ;
    assert false
  with ReturnExpr x -> x

let print_expr funcs condition oc =
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
  | CoalesceNullLast (a, z) ->
      if a = z-1 then
        p ": last alternative of coalesce expression must not be nullable"
      else
        p ": alternative #%d/%d of coalesce expression must be nullable" (a+1) z
  | GettableByInt -> p " must be a vector, a list, a tuple or a map"
  | GettableByName -> p " must be a record or a map"
  | AnyIp -> p " must be an IP"
  | AnyCidr -> p " must be a CIDR"
  | NumericVec -> p " must be a vector of numeric elements"
  | Matrix -> p " must be a list/vector of tuples of numeric elements"
  | InType -> p ": arguments must be compatible with the IN operator"
  | LengthType -> p ": arguments must be compatible with the LENGTH operator"
  | PrevNull -> p " must be null as it is drawn from the previous tuple"
  | Integer -> p " must be an integer"
  | Signed -> p " must be a signed integer"
  | Unsigned -> p " must be an unsigned integer"
  | Numeric -> p " must be numeric"
  | Numeric_Or_Numerics -> p " must be numeric or a list/vector of numerics"
  | ActualType t -> p " must be of type %s" t
  | InheritType -> p " must match all parents output"
  | InheritNull -> p " must match all parents nullability"
  | OpenedRecordIs i ->
      let _func_name, _clause, _stack, e = expr_of_id funcs condition i in
      p " refers to record %a" (E.print ~max_depth:2 false) e
  | MulType ->
      p ": arguments must be either numeric or and integer and a string"
  | PeekedType ->
      p ": argument must be a string or a vector of unsigned integers"
  | MapType ->
      p " must be a map"
  | MapNullability ->
      p ": Cannot bind a nullable key or a nullable value in a map that's not \
         defined over nullable keys or values"
  | TopOutput Rank ->
      p " must be a top rank, therefore an unsigned"
  | TopOutput Membership ->
      p " must be a top membership, therefore a boolean"
  | TopOutput List ->
      p " must be a top, therefore a list of items"

type func =
  | Clause of string * expr
  | Notif of int * expr
  | NotifParam of int * int * expr
  | ExternalSource of string * expr
    [@@ppp PPP_OCaml]

let print_func funcs condition oc =
  let p fmt = Printf.fprintf oc fmt in
  let print_expr = print_expr funcs condition in
  function
  | Clause (c, e) -> p "clause %s%a" c print_expr e
  | Notif (i, e) -> p "notification #%d%a" i print_expr e
  | NotifParam (i, j, e) -> p "notification #%d, parameter #%d%a"
                              i j print_expr e
  | ExternalSource (w, e) -> p "External source %s%a" w print_expr e

type t = Expr of int * expr
       | Func of int * func
       | RunCondition
         [@@ppp PPP_OCaml]

let print_stack oc stack =
  let rec rewind max_depth last = function
    | [] -> last
    | e :: stack ->
        if max_depth = 0 then last
        else rewind (max_depth - 1) (Some e) stack
  in
  match rewind 1 None stack with
  | Some e ->
      Printf.fprintf oc "In %a: " (E.print ~max_depth:3 false) e
  | None ->
      ()

let print funcs condition oc =
  let func_of_id i = List.at funcs i
  and p fmt = Printf.fprintf oc fmt in
  function
  | Expr (i, e) ->
      let loc_name, clause, stack, expr = expr_of_id funcs condition i in
      let clause = if clause = "" then clause else ", "^ clause in
      p "In %s%s: %aexpression %s%a"
        loc_name
        clause
        print_stack stack
        (N.expr_color
          (IO.to_string (E.print ~max_depth:3 false) expr))
        (print_expr funcs condition) e
  | Func (i, e) ->
      let func_name = (func_of_id i).VSI.name in
      p "In function %s: %a"
        (N.func_color func_name)
        (print_func funcs condition) e
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

let print_core funcs condition oc lst =
  List.map of_assert_name lst |>
  Set.of_list |>
  Set.print ~first:"\n     " ~sep:"\n AND " ~last:"" (print funcs condition) oc
