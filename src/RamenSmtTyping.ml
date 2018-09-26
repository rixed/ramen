(* Typing a ramen program using a SMT solver.
 *
 * Principles:
 *
 * - Typing and nullability are resolved simultaneously in case nullability
 *   depends on the chosen types in some contexts.
 *
 * - For nullability, each expression is associated with a single boolean
 *   variable "nXY" where XY is the expression uniq_num. Some constraints on
 *   nullability also steam from the operation itself (for instance, where
 *   clauses are not nullable) or the parents (such as: fields coming from
 *   parents located in the same program have same nullability, and fields
 *   coming from external parents have some given nullability).
 *
 * - For types this is more convoluted. Each expression will be associated with
 *   a variable named "eXY" (where XY is its uniq_num), and these variables
 *   will be given values drawn from a recursive datatype (keywords: "theory of
 *   inductive data types"). The trickiest data type is the tuple because of its
 *   unknown number of elements. After several unsuccessful attempts, we now
 *   define a specific tuple type for each used arity (this is probably the
 *   easiest for the solver too).
 *
 * - In case the constraints are satisfiable we get the assignment (the
 *   solution might not be unique if an expression is unused or is literal
 *   NULL). If it's not we obtain the unsatisfiable core and build an error
 *   message from it (in the future: we might interact with the solver to
 *   devise a better error message as in
 *   https://cs.nyu.edu/wies/publ/finding_minimum_type_error_sources.pdf)
 *
 * - We use SMT-LIB v2.6 format as it supports datatypes. Both the latest
 *   versions of Z3 and CVC4 have been tested to work.
 *
 * Notes regarding the SMT:
 *
 * - Given all functions are total, we cannot just declare the signature of
 *   our functions and then convert the operation into an smt2 operation ;
 *   even if we introduced an "invalid" type in the sort of types (because
 *   the solver would not try to avoid that value).
 *   So we merely emit assertions for each of our function constraints.
 *)
open Batteries
open RamenHelpers
open RamenTypingHelpers
open RamenExpr
open RamenTypes
open RamenLog
module C = RamenConf
module F = C.Func
module P = C.Program
module Err = RamenSmtErrors

let smt_solver = ref "z3 -t:20000 -smt2 %s"

let t_of_num num =
  Printf.sprintf "t%d" num

let t_of_expr e =
  t_of_num ((typ_of e).uniq_num)

let n_of_num num =
  Printf.sprintf "n%d" num

let n_of_expr e =
  n_of_num((typ_of e).uniq_num)

let emit_comment oc str =
  Printf.fprintf oc "; %s\n" str

let emit_is_true id oc =
  Printf.fprintf oc "%s" id

let emit_is_false id oc =
  Printf.fprintf oc "(not %s)" id

let emit_is_tuple id oc sz =
  Printf.fprintf oc "((_ is tuple%d) %s)" sz id

let rec emit_id_eq_typ tuple_sizes id oc = function
  | TEmpty -> assert false
  | TString -> Printf.fprintf oc "(= string %s)" id
  | TBool -> Printf.fprintf oc "(= bool %s)" id
  | TAny -> Printf.fprintf oc "true"
  | TU8 -> Printf.fprintf oc "(= u8 %s)" id
  | TU16 -> Printf.fprintf oc "(= u16 %s)" id
  | TU32 -> Printf.fprintf oc "(= u32 %s)" id
  | TU64 -> Printf.fprintf oc "(= u64 %s)" id
  | TU128 -> Printf.fprintf oc "(= u128 %s)" id
  | TI8 -> Printf.fprintf oc "(= i8 %s)" id
  | TI16 -> Printf.fprintf oc "(= i16 %s)" id
  | TI32 -> Printf.fprintf oc "(= i32 %s)" id
  | TI64 -> Printf.fprintf oc "(= i64 %s)" id
  | TI128 -> Printf.fprintf oc "(= i128 %s)" id
  | TFloat -> Printf.fprintf oc "(= float %s)" id
  (* Asking for a TNum is asking for any number: *)
  | TNum ->
      Printf.fprintf oc
        "(or (= u8 %s) (= u16 %s) (= u32 %s) (= u64 %s) (= u128 %s) \
             (= i8 %s) (= i16 %s) (= i32 %s) (= i64 %s) (= i128 %s) \
             (= float %s))"
        id id id id id
        id id id id id
        id
  | TEth -> Printf.fprintf oc "(= eth %s)" id
  | TIpv4 -> Printf.fprintf oc "(= ip4 %s)" id
  | TIpv6 -> Printf.fprintf oc "(= ip6 %s)" id
  (* Asking for a TIp is asking for a generic Ip able to handle both: *)
  | TIp -> Printf.fprintf oc "(= ip %s)" id
  | TCidrv4 -> Printf.fprintf oc "(= cidr4 %s)" id
  | TCidrv6 -> Printf.fprintf oc "(= cidr6 %s)" id
  | TCidr -> Printf.fprintf oc "(= cidr %s)" id
  | TTuple ts ->
      let d = Array.length ts in
      if d = 0 then
        Printf.fprintf oc "(or %a)"
          (Set.Int.print ~first:"" ~last:"" ~sep:" " (emit_is_tuple id))
            tuple_sizes
      else
        emit_is_tuple id oc d
  | TVec (d, t) ->
      let id' = Printf.sprintf "(vector-type %s)" id in
      Printf.fprintf oc "(and ((_ is vector) %s) %a"
        id (emit_id_eq_typ tuple_sizes id') t.structure ;
      (* FIXME: assert (d > 0) *)
      if d <> 0 then Printf.fprintf oc " (= %d (vector-dim %s))" d id ;
      Printf.fprintf oc ")"
  | TList t ->
      let id' = Printf.sprintf "(list-type %s)" id in
      Printf.fprintf oc "(and ((_ is list) %s) %a)"
        id (emit_id_eq_typ tuple_sizes id') t.structure

let emit_assert ?name oc p =
  match name with
  | None ->
      Printf.fprintf oc "(assert %t)\n" p
  | Some err ->
      let s = Err.to_assert_name err in
      Printf.fprintf oc "(assert (! %t :named %s))\n" p s

let emit_assert_is_true ?name oc id =
  emit_assert ?name oc (emit_is_true id)

let emit_assert_is_false ?name oc id =
  emit_assert ?name oc (emit_is_false id)

let emit_assert_id_is_bool ?name id oc b =
  (if b then emit_assert_is_true else emit_assert_is_false) ?name oc id

let emit_assert_id_eq_typ ?name tuple_sizes id oc t =
  emit_assert ?name oc (fun oc -> emit_id_eq_typ tuple_sizes id oc t)

let emit_id_eq_smt2 id oc smt2 =
  Printf.fprintf oc "(= %s %s)" id smt2

let emit_id_eq_id = emit_id_eq_smt2

let emit_assert_id_eq_smt2 ?name id oc smt2 =
  emit_assert ?name oc (fun oc -> emit_id_eq_smt2 id oc smt2)

let emit_assert_id_eq_id = emit_assert_id_eq_smt2

(* Check that types are either the same (for those we cannot compare)
 * or that e1 is <= e2.
 * For IP/CIDR, it means version is either the same or generic.
 * For integers, it means that width is <= and sign is also <=. *)
let emit_id_le_smt2 id oc smt2 =
  Printf.fprintf oc
    "(or (= %s %s) \
         (ite (= i16 %s)  (or (= u8 %s) (= i8 %s)) \
         (ite (= i32 %s)  (or (= u8 %s) (= i8 %s) (= u16 %s) (= i16 %s)) \
         (ite (= i64 %s)  (or (= u8 %s) (= i8 %s) (= u16 %s) (= i16 %s) (= u32 %s) (= i32 %s)) \
         (ite (or (= i128 %s) (= float %s)) \
                          (or (= u8 %s) (= i8 %s) (= u16 %s) (= i16 %s) (= u32 %s) (= i32 %s) (= u64 %s) (= i64 %s)) \
         (ite (= u16 %s)  (= u8 %s) \
         (ite (= u32 %s)  (or (= u8 %s) (= u16 %s)) \
         (ite (= u64 %s)  (or (= u8 %s) (= u16 %s) (= u32 %s)) \
         (ite (= u128 %s) (or (= u8 %s) (= u16 %s) (= u32 %s) (= u64 %s)) \
         (ite (= ip %s)   (or (= ip4 %s) (= ip6 %s)) \
         (ite (= cidr %s) (or (= cidr4 %s) (= cidr6 %s)) \
         false)))))))))))"
      smt2 id
      smt2 id id
      smt2 id id id id
      smt2 id id id id id id
      smt2 smt2 id id id id id id id id
      smt2 id
      smt2 id id
      smt2 id id id
      smt2 id id id id
      smt2 id id
      smt2 id id

let emit_assert_id_le_smt2 ?name id oc smt2 =
  emit_assert ?name oc (fun oc -> emit_id_le_smt2 id oc smt2)

let emit_assert_id_le_id = emit_assert_id_le_smt2

let emit_assert_id_eq_any_of_typ ?name tuple_sizes id oc lst =
  emit_assert ?name oc (fun oc ->
    Printf.fprintf oc "(or %a)"
      (List.print ~first:"" ~last:"" ~sep:" " (emit_id_eq_typ tuple_sizes id)) lst)

let expr_err e err = Err.Expr ((typ_of e).uniq_num, err)
let func_err fi err = Err.Func (fi, err)

(* Named constraint used when an argument type is constrained: *)
let arg_is_nullable oc e =
  let name = expr_err e (Err.Nullability true) in
  emit_assert_is_true ~name oc (n_of_expr e)

let arg_is_not_nullable oc e =
  let name = expr_err e (Err.Nullability false) in
  emit_assert_is_false ~name oc (n_of_expr e)

let arg_is_unsigned oc e =
  let name = expr_err e Err.Unsigned in
  let id = t_of_expr e in
  emit_assert ~name oc (fun oc ->
    Printf.fprintf oc
      "(or (= u8 %s) (= u16 %s) (= u32 %s) (= u64 %s) (= u128 %s))"
      id id id id id)

let arg_is_signed oc e =
  let name = expr_err e Err.Signed in
  let id = t_of_expr e in
  emit_assert ~name oc (fun oc ->
    Printf.fprintf oc
      "(or (= float %s) \
           (= i8 %s) (= i16 %s) (= i32 %s) (= i64 %s) (= i128 %s))"
      id
      id id id id id)

let arg_is_integer oc e =
  let name = expr_err e Err.Integer in
  let id = t_of_expr e in
  emit_assert ~name oc (fun oc ->
    Printf.fprintf oc
      "(or (= u8 %s) (= u16 %s) (= u32 %s) (= u64 %s) (= u128 %s) \
           (= i8 %s) (= i16 %s) (= i32 %s) (= i64 %s) (= i128 %s))"
      id id id id id
      id id id id id)

let emit_numeric oc id =
  Printf.fprintf oc
    "(or (= float %s) \
         (= u8 %s) (= u16 %s) (= u32 %s) (= u64 %s) (= u128 %s) \
         (= i8 %s) (= i16 %s) (= i32 %s) (= i64 %s) (= i128 %s))"
    id
    id id id id id
    id id id id id

let arg_is_numeric oc e =
  let name = expr_err e Err.Numeric in
  emit_assert ~name oc (fun oc ->
    emit_numeric oc (t_of_expr e))

let empty_tuple_sizes = Set.Int.empty

let arg_has_type typ oc e =
  let typ_name = IO.to_string RamenTypes.print_structure typ in
  let name = expr_err e (Err.ActualType typ_name) in
  emit_assert ~name oc (fun oc ->
    emit_id_eq_typ empty_tuple_sizes (t_of_expr e) oc typ)

let arg_is_float = arg_has_type TFloat
let arg_is_bool = arg_has_type TBool
let arg_is_string = arg_has_type TString

(* "same" types are either actually the same or at least of the same sort
 * (both numbers/floats, both ip, or both cidr) *)
let emit_same id1 oc id2 =
  Printf.fprintf oc
    "(or (= %s %s) \
         (ite (or (= u8 %s) (= u16 %s) (= u32 %s) (= u64 %s) (= u128 %s) \
                  (= i8 %s) (= i16 %s) (= i32 %s) (= i64 %s) (= i128 %s) \
                  (= float %s)) \
              (or (= u8 %s) (= u16 %s) (= u32 %s) (= u64 %s) (= u128 %s) \
                  (= i8 %s) (= i16 %s) (= i32 %s) (= i64 %s) (= i128 %s) \
                  (= float %s)) \
         (ite (or (= ip4 %s) (= ip6 %s) (= ip %s)) \
              (or (= ip4 %s) (= ip6 %s) (= ip %s)) \
         (ite (or (= cidr4 %s) (= cidr6 %s) (= cidr %s)) \
              (or (= cidr4 %s) (= cidr6 %s) (= cidr %s)) \
         false))))"
     id1 id2
     id1 id1 id1 id1 id1 id1 id1 id1 id1 id1 id1
     id2 id2 id2 id2 id2 id2 id2 id2 id2 id2 id2
     id1 id1 id1
     id2 id2 id2
     id1 id1 id1
     id2 id2 id2

let emit_assert_same e oc id1 id2 =
  let name = expr_err e Err.Same in
  emit_assert ~name oc (fun oc -> emit_same id1 oc id2)

(* Assuming all input/output/constants have been declared already, emit the
 * constraints connecting the parameter to the result: *)
let emit_constraints tuple_sizes out_fields oc e =
  let eid = t_of_expr e and nid = n_of_expr e in
  emit_comment oc (IO.to_string (RamenExpr.print false) e) ;
  (* Then we also have specific rules according to the operation at hand: *)
  match e with
  | Field (_, tupref, field_name) ->
      (* The type of an output field is taken from the out types.
       * The type of a field originating from input/params/env/virtual
       * fields has been set previously. *)
      if RamenLang.tuple_has_type_output !tupref then (
        let open RamenOperation in
        match List.find (fun sf -> sf.alias = field_name) out_fields with
        | exception Not_found ->
            Printf.sprintf2 "Unknown output field %S (out is %a)"
              field_name
              (List.print (fun oc sf ->
                String.print oc sf.alias)) out_fields |>
            failwith
        | { expr ; _ } ->
            emit_assert_id_eq_id eid oc (t_of_expr expr) ;
            (* Some tuples are passed to callback via an option type, and
             * are None each time they are undefined (beginning of worker
             * or of group). Workers access those fields via a special
             * functions, and all fields are forced nullable during typing.
             *)
            if !tupref = TupleOutPrevious then
              let name = expr_err e Err.PrevNull in
              emit_assert_is_true ~name oc nid
            else
              emit_assert_id_eq_id nid oc (n_of_expr expr))

  | Const (_, VNull) ->
      (* - "NULL" is nullable. *)
      arg_is_nullable oc e

  | Const (_, x) ->
      (* - A const cannot be null, unless it's VNull;
       * - The type is the type of the constant. *)
      arg_has_type (RamenTypes.structure_of x) oc e ;
      arg_is_not_nullable oc e

  | StateField _ -> assert false (* Not supposed to appear here *)

  | Tuple (_, es) ->
      (* - The resulting type is a tuple which length, items type and
       *   nullability are given by es;
       * - The result is not nullable since it has a literal value. *)
      let d = List.length es in
      emit_assert oc (fun oc ->
        Printf.fprintf oc "((_ is tuple%d) %s)" d eid) ;
      List.iteri (fun i e ->
        emit_assert_id_eq_smt2 (t_of_expr e) oc
          (Printf.sprintf "(tuple%d-e%d %s)" d i eid) ;
        emit_assert_id_eq_smt2 (n_of_expr e) oc
          (Printf.sprintf "(tuple%d-n%d %s)" d i eid)
      ) es ;
      emit_assert_is_false oc nid

  | Vector (_, es) ->
      (* Typing rules:
       * - Every element in es must have the same sort;
       * - Every element in es must have the same nullability (FIXME:
       *   couldn't we "promote" non nullable to nullable?);
       * - The resulting type is a vector of that size with a type not
       *   smaller then any of the elements;
       * - The resulting type is not nullable since it has a literal
       *   value.
       * - FIXME: If the vector is of length 0, it can have any type *)
      (match es with
      | [] ->
        (* Empty vector literal are not accepted by the parser yet... *)
        emit_assert oc (fun oc ->
          Printf.fprintf oc "((_ is vector) %s)" eid)
      | fst :: _rest ->
        let d = List.length es in
        List.iter (fun x ->
          let name = expr_err x Err.VecSame in
          emit_assert ~name oc (fun oc ->
            Printf.fprintf oc "(and %a %a)"
              (emit_id_eq_id (n_of_expr x)) (n_of_expr fst)
              (emit_id_le_smt2 (t_of_expr x))
                (Printf.sprintf "(vector-type %s)" eid))
        ) es ;
        emit_assert oc (fun oc ->
          Printf.fprintf oc
            "(and ((_ is vector) %s) \
                  (= %d (vector-dim %s)) \
                  (= %s (vector-nullable %s)))"
                  eid
                  d eid
                  (n_of_expr fst) eid)) ;
      emit_assert_is_false oc nid

  | Case (_, cases, else_) ->
      (* Typing rules:
       * - All conditions must have type bool;
       * - All consequents must be of the same sort (that of the case);
       * - The result must not be smaller than any of the consequents (or
       *   else cause if present);
       * - If present, the else must also have that type;
       * - If a condition or a consequent is nullable then the case is;
       * - Conversely, if no condition nor any consequent are nullable
       *   and there is an else branch that is not nullable, then the case
       *   is not;
       * - If there are no else branch then the case is nullable. *)
      let num_cases = List.length cases in
      List.iteri (fun i { case_cond = cond ; case_cons = cons } ->
        let name = expr_err e (Err.CaseCond (i, num_cases)) in
        emit_assert_id_eq_typ ~name tuple_sizes (t_of_expr cond) oc TBool ;
        let name = expr_err e (Err.CaseCons (i, num_cases)) in
        emit_assert_id_le_id ~name (t_of_expr cons) oc eid
      ) cases ;
      Option.may (fun else_ ->
        let name = expr_err e Err.CaseElse in
        emit_assert_id_le_id ~name (t_of_expr else_) oc eid
      ) else_ ;
      if cases <> [] then (
        let name = expr_err e Err.CaseNullProp in
        emit_assert_id_eq_smt2 ~name nid oc
          (Printf.sprintf2 "(or %a %s)"
            (List.print ~first:"" ~last:"" ~sep:" " (fun oc case ->
              Printf.fprintf oc "%s %s"
                (n_of_expr case.case_cond)
                (n_of_expr case.case_cons))) cases
            (match else_ with
            | None -> "true"
            | Some e -> n_of_expr e)))

  | Coalesce (_, es) ->
      (* Typing rules:
       * - Every alternative must be of the same sort and the result must
       *   not be smaller;
       * - The result is not null;
       * - All elements of the list but the last must be nullable ;
       * - The last element of the list must not be nullable. *)
      let len = List.length es in
      arg_is_not_nullable oc e ;
      List.iteri (fun i x ->
        let name = expr_err x (Err.CoalesceAlt i) in
        emit_assert_id_le_id ~name (t_of_expr x) oc eid ;
        let name = expr_err x (Err.CoalesceNullLast i) in
        let is_last = i = len - 1 in
        emit_assert_id_is_bool ~name (n_of_expr x) oc (not is_last)
      ) es ;

  | StatelessFun0 (_, (Now|Random|EventStart|EventStop)) ->
      (* - The result is a non nullable float *)
      emit_assert_id_eq_typ tuple_sizes eid oc TFloat ;
      arg_is_not_nullable oc e

  | StatelessFun1 (_, Defined, x) ->
      (* - x must be nullable;
       * - The result is a non nullable bool. *)
      arg_is_nullable oc x ;
      emit_assert_id_eq_typ tuple_sizes eid oc TBool ;
      arg_is_not_nullable oc e

  | StatefulFun (_, _, _, (AggrMin x|AggrMax x)) ->
      (* - x can be of any type;
       * - The result has its type;
       * - The result nullability is set by propagation from x. *)
      emit_assert_id_eq_id (t_of_expr x) oc eid ;
      emit_assert_id_eq_id nid oc (n_of_expr x)

  | StatefulFun (_, _, _, (AggrFirst x|AggrLast x)) ->
      (* - The result has the same type than that of x;
       * - The result has same nullability than that of x. *)
      emit_assert_id_eq_id (t_of_expr x) oc eid ;
      emit_assert_id_eq_id (n_of_expr x) oc nid

  | StatefulFun (_, _, _, AggrSum x) ->
      (* - x must be numeric;
       * - The result has the same type than x;
       * - The result has same nullability than x. *)
      arg_is_numeric oc x ;
      emit_assert_id_eq_id (t_of_expr x) oc eid ;
      emit_assert_id_eq_id (n_of_expr x) oc nid

  | StatefulFun (_, _, _, AggrAvg x) ->
      (* - x must be numeric;
       * - The result is a float;
       * - The result has same nullability than that of x. *)
      arg_is_numeric oc x ;
      emit_assert_id_eq_typ tuple_sizes eid oc TFloat ;
      emit_assert_id_eq_id (n_of_expr x) oc nid

  | StatelessFun1 (_, Minus, x) ->
      (* - The only argument must be numeric;
       * - The result must not be smaller than x;
       * - The result has same nullability than x;
       * - The result is signed or float. *)
      arg_is_numeric oc x ;
      emit_assert_id_le_id (t_of_expr x) oc eid ;
      arg_is_signed oc e ;
      emit_assert_id_eq_id (n_of_expr x) oc nid

  | StatelessFun1 (_, Age, x) ->
      (* - The only argument must be numeric;
       * - The result is a float;
       * - The result has same nullability than x. *)
      arg_is_numeric oc x ;
      emit_assert_id_eq_typ tuple_sizes eid oc TFloat ;
      emit_assert_id_eq_id (n_of_expr x) oc nid

  | StatelessFun1 (_, Abs, x) ->
      (* - The only argument must be numeric;
       * - The result has same type than x;
       * - The result has same nullability than x. *)
      arg_is_numeric oc x ;
      emit_assert_id_le_id (t_of_expr x) oc eid ;
      emit_assert_id_eq_id (n_of_expr x) oc nid

  | StatefulFun (_, _, _, (AggrAnd x | AggrOr x))
  | StatelessFun1 (_, Not, x) ->
      (* - The only argument must be boolean;
       * - The result type is a boolean;
       * - The resulting nullability depends solely on that of x. *)
      arg_is_bool oc x ;
      emit_assert_id_eq_id nid oc (n_of_expr x) ;
      emit_assert_id_eq_typ tuple_sizes eid oc TBool

  | StatelessFun1 (_, Cast t, x) ->
      (* - The only argument (x) can be anything;
       * - The result type is as described by the chosen type;
       * - The result is nullable if we specifically choose to cast
       *   toward a nullable type, or merely propagates.
       * Note that some cast are actually not implemented so would fail
       * when generating code. *)
      if t.RamenTypes.nullable then
        emit_assert_is_true oc nid
      else
        emit_assert_id_eq_id nid oc (n_of_expr x) ;
      emit_assert_id_eq_typ tuple_sizes eid oc t.structure

  | StatelessFun2 (_, Percentile, e1, e2) ->
      (* - e1 must be numeric;
       * - e2 must be a vector or list of anything, then the result is not
       *   smaller than the element type;
       * - the result is nullable if either e1 or e2 is. *)
      arg_is_numeric oc e1 ;
      emit_assert oc (fun oc ->
        let eid2 = t_of_expr e2 in
        let lst_type = "(list-type "^ eid2 ^")"
        and vec_type = "(vector-type "^ eid2 ^")" in
        Printf.fprintf oc
          "(or (and ((_ is list) %s) %a (or (not (list-nullable %s)) %s))
               (and ((_ is vector) %s) %a (or (not (vector-nullable %s)) %s)))"
          eid2 (emit_id_le_smt2 lst_type) eid eid2 nid
          eid2 (emit_id_le_smt2 vec_type) eid eid2 nid) ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))

  | StatelessFun2 (_, (Add|Sub|Mul|IDiv|Pow), e1, e2) ->
      (* - e1 and e2 must be numeric;
       * - The result is not smaller than e1 or e2. *)
      arg_is_numeric oc e1 ;
      arg_is_numeric oc e2 ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2)) ;
      emit_assert_id_le_id (t_of_expr e1) oc eid ;
      emit_assert_id_le_id (t_of_expr e2) oc eid
      (* TODO: for IDiv, have a TInt type and make_int_typ when parsing *)

  | StatelessFun2 (_, (Reldiff|Div), e1, e2) ->
      (* - e1 and e2 must be numeric;
       * - The result is a float. *)
      arg_is_numeric oc e1 ;
      arg_is_numeric oc e2 ;
      emit_assert_id_eq_typ tuple_sizes eid oc TFloat ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))

  | StatelessFun2 (_, (StartsWith|EndsWith), e1, e2) ->
      (* - e1 and e2 must be strings;
       * - The result is a bool;
       * - Result nullability propagates from arguments. *)
      arg_is_string oc e1 ;
      arg_is_string oc e2 ;
      emit_assert_id_eq_typ tuple_sizes eid oc TBool ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))

  | StatelessFun2 (_, Concat, e1, e2)
  | GeneratorFun (_, Split (e1, e2)) ->
      (* - e1 and e2 must be strings;
       * - The result is also a string;
       * - Result nullability propagates from arguments. *)
      arg_is_string oc e1 ;
      arg_is_string oc e2 ;
      emit_assert_id_eq_typ tuple_sizes eid oc TString ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))

  | StatelessFun2 (_, Strftime, e1, e2) ->
      (* - e1 must be a string and e2 a float (ideally, a time);
       * - Then result will be a string;
       * - Its nullability propagates from arguments. *)
      arg_is_string oc e1 ;
      arg_is_float oc e2 ;
      emit_assert_id_eq_typ tuple_sizes eid oc TString ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))

  | StatelessFun1 (_, Length, x) ->
      (* - x must be a string or a list;
       * - The result type is an U32;
       * - The result nullability is the same as that of x. *)
      let name = expr_err e Err.LengthType in
      let xid = t_of_expr x in
      emit_assert ~name oc (fun oc ->
        Printf.fprintf oc "(or (= string %s) ((_ is list) %s))" xid xid) ;
      emit_assert_id_eq_typ tuple_sizes eid oc TU32 ;
      emit_assert_id_eq_id nid oc (n_of_expr x)

  | StatelessFun1 (_, (Lower|Upper), x) ->
      (* - x must be a string;
       * - The result type is also a string;
       * - The result nullability is the same as that of x. *)
      arg_is_string oc x ;
      emit_assert_id_eq_typ tuple_sizes eid oc TString ;
      emit_assert_id_eq_id nid oc (n_of_expr x)

  | StatelessFunMisc (_, Like (x, _)) ->
      (* - x must be a string;
       * - The result is a bool;
       * - Nullability propagates from x to e. *)
      arg_is_string oc x ;
      emit_assert_id_eq_typ tuple_sizes eid oc TBool ;
      emit_assert_id_eq_id nid oc (n_of_expr x)

  | StatelessFun1 (_, Strptime, x) ->
      (* - x must be a string;
       * - The Result is a float (a time);
       * - The result is always nullable (known from the parse). *)
      arg_is_string oc x ;
      emit_assert_id_eq_typ tuple_sizes eid oc TFloat ;
      emit_assert_is_true oc nid

  | StatelessFun2 (_, Mod, e1, e2)
  | StatelessFun2 (_, (BitAnd|BitOr|BitXor|BitShift), e1, e2) ->
      (* - e1 and e2 must be any integer;
       * - The result must not be smaller that e1 nor e2;
       * - Nullability propagates. *)
      arg_is_integer oc e1 ;
      arg_is_integer oc e2 ;
      emit_assert_id_le_id (t_of_expr e1) oc eid ;
      emit_assert_id_le_id (t_of_expr e2) oc eid ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))

  | StatelessFun2 (_, (Ge|Gt), e1, e2) ->
      (* - e1 and e2 must have the same sort;
       * - The result is a bool;
       * - Nullability propagates. *)
      emit_assert_same e oc (t_of_expr e1) (t_of_expr e2) ;
      emit_assert_id_eq_typ tuple_sizes eid oc TBool ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))

  | StatelessFun2 (_, Eq, e1, e2) ->
      (* - e1 and e2 must be of the same sort;
       * - The result is a bool;
       * - Nullability propagates from arguments. *)
      emit_assert_same e oc (t_of_expr e1) (t_of_expr e2) ;
      emit_assert_id_eq_typ tuple_sizes eid oc TBool ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))

  | StatelessFun2 (_, (And|Or), e1, e2) ->
      (* - e1 and e2 must be booleans;
       * - The result is also a boolean;
       * - Nullability propagates. *)
      arg_is_bool oc e1 ;
      arg_is_bool oc e2 ;
      emit_assert_id_eq_typ tuple_sizes eid oc TBool ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))

  | StatelessFun1 (_, Nth n, x) ->
      (* Note: here n starts at 0 (what the user wrote, minus 1),
       *       and tuple_sizes gives the length.
       * Typing rules:
       * - x must be a tuple of at least n elements;
       * - the resulting type is that if the n-th element;
       * - the result nullability is also that of the n-th elements. *)
      let eid' = t_of_expr x in
      let name = expr_err x Err.Tuple in
      emit_assert ~name oc (fun oc ->
        Printf.fprintf oc "(or %a)"
          (Set.Int.print ~first:"" ~last:"" ~sep:" " (fun oc sz ->
            if sz > n then
              Printf.fprintf oc "(and %a \
                                      (= (tuple%d-e%d %s) %s) \
                                      (= (tuple%d-n%d %s) %s))"
                (emit_is_tuple eid') sz
                sz n eid' eid
                sz n eid' nid))
            tuple_sizes)

  | StatelessFun2 (_, VecGet, i, x) ->
      (* TODO: replaces NTH entirely? NTH(i) == Get(i-1) *)
      (* Typing rules:
       * - x must be a vector or a list;
       * - i must be an unsigned;
       * - if x is a vector and i is a constant, then i must
       *   be less than its length;
       * - the resulting type is the same as the selected type;
       * - if x is a vector and i a constant, then the result has the
       *   same nullability than x or x's elements; in all other cases, the
       *   result is nullable. *)
      arg_is_numeric oc i ;
      let name = expr_err x Err.Gettable in
      (match int_of_const i with
      | None ->
          emit_assert ~name oc (fun oc ->
            Printf.fprintf oc "(let ((tmp %s)) \
                                 (or (and ((_ is vector) tmp) \
                                          (= (vector-type tmp) %s))\
                                     (and ((_ is list) tmp) \
                                          (= (list-type tmp) %s))))"
              (t_of_expr x) eid eid) ;
          arg_is_nullable oc e
      | Some i ->
          emit_assert ~name oc (fun oc ->
            Printf.fprintf oc "(let ((tmp %s)) \
                                 (or (and ((_ is vector) tmp) \
                                          (> (vector-dim tmp) %d) \
                                          (= (vector-type tmp) %s) \
                                          (= (or %s (vector-nullable tmp)) %s)) \
                                     (and ((_ is list) tmp) \
                                          (= (list-type tmp) %s) \
                                          %s)))"
              (t_of_expr x)
              i
              eid
              (n_of_expr x) nid
              eid
              nid))

  | StatelessFun1 (_, (BeginOfRange|EndOfRange), x) ->
      (* - x is any kind of cidr;
       * - The result is a bool;
       * - Nullability propagates from x. *)
      let name = expr_err x Err.AnyCidr in
      let xid = t_of_expr x in
      emit_assert ~name oc (fun oc ->
        Printf.fprintf oc
          "(or (and (= cidr %s) (= ip %s)) \
               (and (= cidr4 %s) (= ip4 %s)) \
               (and (= cidr6 %s) (= ip6 %s)))"
          xid eid
          xid eid
          xid eid) ;
      emit_assert_id_eq_id nid oc (n_of_expr x)

  | StatelessFunMisc (_, (Min es | Max es)) ->
      (* Typing rules:
       * - es must be a list of expressions of compatible types;
       * - the result type is the largest of them all;
       * - If any of the es is nullable then so is the result. *)
      List.iter (fun e ->
        emit_assert_id_le_id (t_of_expr e) oc eid
      ) es ;
      if es <> [] then
        emit_assert_id_eq_smt2 nid oc
          (Printf.sprintf2 "(or %a)"
            (List.print ~first:"" ~last:"" ~sep:" " (fun oc e ->
              String.print oc (n_of_expr e))) es)

  | StatelessFunMisc (_, Print es) ->
      (* The result must have the same type as the first parameter *)
      emit_assert_id_eq_id (t_of_expr (List.hd es)) oc eid ;
      emit_assert_id_eq_id (n_of_expr (List.hd es)) oc nid

  | StatefulFun (_, _, _, Lag (e1, e2)) ->
      (* Typing rules:
       * - e1 must be an unsigned;
       * - e2 has same type as the result;
       * - The result is only nullable by skip or propagation, for if the
       *   lag goes beyond the start of the window then lag merely returns
       *   the oldest value. (FIXME: should return NULL in that case) *)
      arg_is_integer oc e1 ;
      emit_assert_id_eq_id (t_of_expr e2) oc eid ;
      emit_assert_id_eq_id (n_of_expr e2) oc nid

  | StatefulFun (_, _, _, MovingAvg (e1, e2, e3))
  | StatefulFun (_, _, _, LinReg (e1, e2, e3)) ->
      (* Typing rules:
       * - e1 must be an unsigned (the period);
       * - e2 must also be an unsigned (the number of values to average);
       * - e3 must be numeric;
       * - Neither e1 or e2 can be NULL;
       * - The result type is known from parsing to be float;
       * - The result nullability propagates from e3. *)
      arg_is_unsigned oc e1 ;
      arg_is_unsigned oc e2 ;
      arg_is_numeric oc e3 ;
      arg_is_not_nullable oc e1 ;
      arg_is_not_nullable oc e2 ;
      emit_assert_id_eq_typ tuple_sizes eid oc TFloat ;
      emit_assert_id_eq_id (n_of_expr e3) oc nid

  | StatefulFun (_, _, _, MultiLinReg (e1, e2, e3, e4s)) ->
      (* As above, with the addition of predictors that must also be
       * numeric and non null. Why non null? See comment in check_variadic
       * and probably get rid of this limitation. *)
      arg_is_unsigned oc e1 ;
      arg_is_unsigned oc e2 ;
      arg_is_numeric oc e3 ;
      arg_is_not_nullable oc e1 ;
      arg_is_not_nullable oc e2 ;
      emit_assert_id_eq_id (n_of_expr e3) oc nid ;
      emit_assert_id_eq_typ tuple_sizes eid oc TFloat ;
      List.iter (fun e ->
        arg_is_numeric oc e ;
        arg_is_not_nullable oc e ;
      ) e4s

  | StatefulFun (_, _, _, ExpSmooth (e1, e2)) ->
      (* Typing rules:
       * - e1 must be a non-null float (and ideally, between 0 and 1), but
       *   we just ask for a numeric in order to also accept immediate
       *   values parsed as integers, and integer fields;
       * - e2 must be numeric *)
      arg_is_float oc e1 ;
      arg_is_numeric oc e2 ;
      emit_assert_id_eq_typ tuple_sizes eid oc TFloat ;
      arg_is_not_nullable oc e1 ;
      emit_assert_id_eq_id (n_of_expr e2) oc nid

  | StatelessFun1 (_, (Exp|Log|Log10|Sqrt), x) ->
      (* - x must be numeric;
       * - The result is a float;
       * - The result nullability is inherited from arguments *)
      arg_is_numeric oc x ;
      emit_assert_id_eq_typ tuple_sizes eid oc TFloat ;
      emit_assert_id_eq_id (n_of_expr x) oc nid

  | StatelessFun1 (_, (Floor|Ceil|Round), x) ->
      (* - x must be numeric;
       * - The result is not smaller than x;
       * - Nullability propagates from argument. *)
      arg_is_numeric oc x ;
      emit_assert_id_le_smt2 (t_of_expr x) oc eid ;
      emit_assert_id_eq_id (n_of_expr x) oc nid

  | StatelessFun1 (_, Hash, _x) ->
      (* - x can be anything. Notice that hash(NULL) is NULL;
       * - The result is an I64.
       * - Nullability propagates. *)
      emit_assert_id_eq_typ tuple_sizes eid oc TI64 ;
      emit_assert_id_eq_id (n_of_expr e) oc nid

  | StatelessFun1 (_, Sparkline, x) ->
      (* - x must be a vector of non-null numerics;
       * - The result is a string;
       * - The result nullability itself propagates from x itself. *)
      let name = expr_err x Err.NumericVec in
      emit_assert_id_eq_typ ~name tuple_sizes (t_of_expr x) oc
        (TVec (0, { structure = TNum ; nullable = false })) ;
      emit_assert_id_eq_typ tuple_sizes eid oc TString ;
      emit_assert_id_eq_id (n_of_expr x) oc nid

  | StatefulFun (_, _, _, Remember (fpr, tim, dur, es)) ->
      (* Typing rules:
       * - fpr must be a non null (positive) float, so we take any numeric
       *   for now;
       * - time must be a time, so ideally a float, but again we accept any
       *   integer (so that a int field is OK);
       * - dur must be a duration, so a numeric again;
       * - Expressions in es can be anything at all;
       * - The result is a boolean;
       * - The result is as nullable as any of tim, dur and the es. *)
      arg_is_numeric oc fpr ;
      arg_is_numeric oc tim ;
      arg_is_numeric oc dur ;
      arg_is_not_nullable oc fpr ;
      emit_assert_id_eq_typ tuple_sizes eid oc TBool ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf2 "(or %s %s%a)"
          (n_of_expr tim) (n_of_expr dur)
          (List.print ~first:" " ~last:"" ~sep:" " (fun oc e ->
            String.print oc (n_of_expr e))) es)

  | StatefulFun (_, _, _, Distinct es) ->
      (* - The es can be anything;
       * - The result is a boolean;
       * - The result is nullable if any of the es is nullable. *)
      emit_assert_id_eq_typ tuple_sizes eid oc TBool ;
      if es <> [] then
        emit_assert_id_eq_smt2 nid oc
          (Printf.sprintf2 "(or %a)"
            (List.print ~first:"" ~last:"" ~sep:" " (fun oc e ->
              String.print oc (n_of_expr e))) es)

  | StatefulFun (_, _, _, Hysteresis (meas, accept, max)) ->
      (* - meas, accept and max must be numeric;
       * - The result is a boolean;
       * - The result is nullable if and only if any of meas, accept and
       *   max is. *)
      arg_is_numeric oc meas ;
      arg_is_numeric oc accept ;
      arg_is_numeric oc max ;
      emit_assert_id_eq_typ tuple_sizes eid oc TBool ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s %s)"
          (n_of_expr meas) (n_of_expr accept) (n_of_expr max))

  | StatefulFun (_, _, _, Top { want_rank ; c ; what ; by ; duration ; time }) ->
      (* Typing rules:
       * - c must be numeric and not null;
       * - what can be anything;
       * - by must be numeric;
       * - duration must be a numeric and non null;
       * - time must be a time (numeric);
       * - If we want the rank then the result type is the same as c,
       *   otherwise it's a bool (known at parsing time);
       * - If we want the rank then the result is nullable (known at
       *   parsing time), otherwise nullability is inherited from what
       *   and by. *)
      arg_is_numeric oc c ;
      emit_assert_is_false oc (n_of_expr c) ;
      arg_is_numeric oc by ;
      arg_is_numeric oc duration ;
      emit_assert_is_false oc (n_of_expr duration) ;
      arg_is_numeric oc time ;
      if want_rank then (
        emit_assert_id_eq_id (t_of_expr c) oc eid ;
        arg_is_nullable oc e
      ) else (
        emit_assert_id_eq_typ tuple_sizes eid oc TBool ;
        emit_assert_id_eq_smt2 nid oc
          (Printf.sprintf2 "(or%a %s)"
            (List.print ~first:" " ~last:"" ~sep:" " (fun oc w ->
              String.print oc (n_of_expr w))) what
            (n_of_expr by)))

  | StatefulFun (_, _, _, Last (c, x, es)) ->
      (* - c must be a constant (TODO) integer;
       * - The type of the result is a list of items of the same type and
       *   nullability than those of x;
       * - In theory, 'Last c e1 by es` should itself be nullable if c is
       *   nullable or any of the es is nullable. And then become and stays
       *   null forever as soon as one es is actually NULL. This is kind
       *   of useless, so we just disallow sizing with and ordering by a
       *   nullable value;
       * - The Last itself is null whenever the number of received item is
       *   less than c, and so is always nullable. *)
      arg_is_integer oc c ;
      arg_is_not_nullable oc c ;
      emit_assert_id_eq_smt2 eid oc
        (Printf.sprintf "(list %s %s)" (t_of_expr x) (n_of_expr x)) ;
      List.iter (arg_is_not_nullable oc) es ;
      arg_is_nullable oc e

  | StatefulFun (_, _, _, Sample (c, x)) ->
      (* - c must be a constant (TODO) integer, not nullable;
       * - The type of the result is a list of items of the same type and
       *   nullability than x ;
       * - 'sample c x` is itself nullable whenever x is nullable (if not
       *   skip null and we encounter a null x, or if skip null and we
       *   encounter only nulls). *)
      arg_is_integer oc c ;
      arg_is_not_nullable oc c ;
      emit_assert_id_eq_smt2 eid oc
        (Printf.sprintf "(list %s %s)" (t_of_expr x) (n_of_expr x)) ;
      emit_assert_id_eq_id nid oc (n_of_expr x)

  | StatefulFun (_, _, _, Group g) ->
      (* - The result is a list which elements have the exact same type as g;
       * - The result is nullable if we skip nulls and g is nullable.
       * Note: It is possible to build as an immediate value a vector of
       * zero length (although, actually it's not), but it is not possible
       * to build an empty list. So Group will, under any circumstance,
       * never return an empty list. The only possible way to build an
       * empty list is by skipping nulls, but then is we skip all nulls
       * it will be null. *)
      emit_assert_id_eq_smt2 eid oc
        (Printf.sprintf "(list %s %s)" (t_of_expr g) (n_of_expr g)) ;
      emit_assert_id_eq_id (n_of_expr g) oc nid

  | StatefulFun (_, _, _, AggrHistogram (x, _, _, n)) ->
      (* - x must be numeric;
       * - The result is a vector of size n+2, of non nullable U32;
       * - The result itself is as nullable as x. *)
      arg_is_numeric oc x ;
      emit_assert_id_eq_typ tuple_sizes eid oc
        (TVec (n+2, { structure = TU32 ; nullable = false })) ;
      emit_assert_id_eq_id (n_of_expr x) oc nid

  | StatelessFun2 (_, In, e1, e2) ->
      (* Typing rule:
       * - e2 can be a string, a cidr, a list or a vector;
       * - if e2 is a string, then e1 must be a string;
       * - if e2 is a cidr, then e1 must be an ip (TODO: either of the same
       *   version or both the cidr or the ip must be generic);
       * - if e2 is either a list of a vector, then e1 must have the sort
       *   of the elements of this list or vector;
       * - The result is a boolean;
       * - Result is as nullable as e1, e2 and the content of e2 if e2
       *   is a vector or a list. *)
      let name = expr_err e2 Err.InType in
      emit_assert ~name oc (fun oc ->
        let id1 = t_of_expr e1 and id2 = t_of_expr e2 in
        Printf.fprintf oc
          "(or (and (= string %s) (= string %s)) \
               (and (or (= cidr %s) (= cidr4 %s) (= cidr6 %s)) \
                    (or (= ip %s) (= ip4 %s) (= ip6 %s))) \
               (and ((_ is list) %s) %a) \
               (and ((_ is vector) %s) %a))"
          id2 id1
          id2 id2 id2 id1 id1 id1
          id2 (emit_same id1) (Printf.sprintf "(list-type %s)" id2)
          id2 (emit_same id1) (Printf.sprintf "(vector-type %s)" id2)) ;
      emit_assert_id_eq_typ tuple_sizes eid oc TBool ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf2
          "(or %s %s (and ((_ is list) %s) (list-nullable %s)) \
                     (and ((_ is vector) %s) (vector-nullable %s)))"
          (n_of_expr e1) (n_of_expr e2)
          (t_of_expr e2) (t_of_expr e2)
          (t_of_expr e2) (t_of_expr e2))

let emit_operation declare tuple_sizes fi oc op =
  let open RamenOperation in
  (* Declare all variables: *)
  iter_expr declare op ;
  (* Now add specific constraints depending on the clauses: *)
  (match op with
  | Aggregate { fields ; where ; notifications ; commit_cond ; _ } ->
      iter_expr (emit_constraints tuple_sizes fields oc) op ;
      (* Typing rules:
       * - Where must be a bool;
       * - Commit-when must also be a bool;
       * - Flush_how conditions must also be bools;
       * - Notification names and parameter values must be non-nullable
       *   strings. *)
      let name = func_err fi Err.(Clause ("where", ActualType "bool")) in
      emit_assert_id_eq_typ ~name tuple_sizes (t_of_expr where) oc TBool ;
      let name = func_err fi Err.(Clause ("where", Nullability false)) in
      emit_assert_is_false ~name oc (n_of_expr where) ;
      let name = func_err fi Err.(Clause ("commit", ActualType "bool")) in
      emit_assert_id_eq_typ ~name tuple_sizes (t_of_expr commit_cond) oc TBool ;
      let name = func_err fi Err.(Clause ("commit", Nullability false)) in
      emit_assert_is_false ~name oc (n_of_expr commit_cond) ;
      List.iteri (fun i notif ->
        let name = func_err fi Err.(Notif (i, ActualType "string")) in
        emit_assert_id_eq_typ ~name tuple_sizes
          (t_of_expr notif.notif_name) oc TString ;
        let name = func_err fi Err.(Notif (i, Nullability false)) in
        emit_assert_is_false ~name oc (n_of_expr notif.notif_name) ;
        List.iteri (fun j (_n, v) ->
          let name = func_err fi Err.(NotifParam (i, j, ActualType "string")) in
          emit_assert_id_eq_typ ~name tuple_sizes (t_of_expr v) oc TString ;
          let name = func_err fi Err.(NotifParam (i, j, Nullability false)) in
          emit_assert_is_false ~name oc (n_of_expr v)
        ) notif.parameters
      ) notifications

  | ReadCSVFile { preprocessor ; where = { fname ; unlink } ; _ } ->
      iter_expr (emit_constraints tuple_sizes [] oc) op ;
      Option.may (fun p ->
        (*  must be a non-nullable string: *)
        let name = func_err fi Err.(Preprocessor (ActualType "string")) in
        emit_assert_id_eq_typ ~name tuple_sizes (t_of_expr p) oc TString ;
        let name = func_err fi Err.(Preprocessor (Nullability false)) in
        emit_assert_is_false ~name oc (n_of_expr p)
      ) preprocessor ;
      let name = func_err fi Err.(Filename (ActualType "string")) in
      emit_assert_id_eq_typ ~name tuple_sizes (t_of_expr fname) oc TString ;
      let name = func_err fi Err.(Filename (Nullability false)) in
      emit_assert_is_false ~name oc (n_of_expr fname) ;
      let name = func_err fi Err.(Unlink (ActualType "bool")) in
      emit_assert_id_eq_typ ~name tuple_sizes (t_of_expr unlink) oc TBool ;
      let name = func_err fi Err.(Unlink (Nullability false)) in
      emit_assert_is_false ~name oc (n_of_expr unlink)

  | _ -> ())

let emit_program declare tuple_sizes oc funcs =
  (* Output all the constraints for all the operations: *)
  List.iteri (fun i (func, op) ->
    Printf.fprintf oc "\n; Constraints for function %s\n"
      (RamenName.string_of_func func.F.name) ;
    emit_operation declare tuple_sizes i oc op
  ) funcs

let emit_minimize oc funcs =
  (* Minimize total number of bits required to encode all integers: *)
  Printf.fprintf oc "\n; Minimize total number width\n\
                     (define-fun cost_of_number ((n Type)) Int\n\
                       (ite (or (= i8 n) (= u8 n)) 1\n\
                       (ite (or (= i16 n) (= u16 n)) 2\n\
                       (ite (or (= i32 n) (= u32 n)) 3\n\
                       (ite (or (= i64 n) (= u64 n)) 4\n\
                       (ite (= float n) 5\n\
                       (ite (or (= i128 n) (= u128 n)) 6\n\
                       0)))))))\n" ;
  Printf.fprintf oc "(minimize (+ 0" ;
  List.iter (fun (_func, op) ->
    RamenOperation.iter_expr (fun e ->
      let eid = t_of_expr e in
      match (typ_of e).typ with
      | None | Some { structure = (TAny | TNum) ; _ } ->
          Printf.fprintf oc " (cost_of_number %s)" eid
      | _ -> ()
    ) op
  ) funcs ;
  Printf.fprintf oc "))\n" ;
  (* And, separately, number of signed values: *)
  Printf.fprintf oc "\n; Minimize total signed ints\n\
                     (define-fun cost_of_sign ((n Type)) Int\n\
                       (ite (or (= i8 n) (= i16 n) (= i32 n) (= i64 n) (= i128 n)) 1 0))\n" ;
  Printf.fprintf oc "(minimize (+ 0" ;
  List.iter (fun (_func, op) ->
    RamenOperation.iter_expr (fun e ->
      let eid = t_of_expr e in
      match (typ_of e).typ with
      | None | Some { structure = (TAny | TNum) ; _ } ->
          Printf.fprintf oc " (cost_of_sign %s)" eid
      | _ -> ()
    ) op
  ) funcs ;
  Printf.fprintf oc "))\n"

(* When a function refers to an input field from a parent, we need to either
 * equates its type to the type of the expression computing the output
 * field (in case of Aggregates) or directly to the type of the output
 * field if it is a well known field (which has no expression). *)
type id_or_type = Id of int | FieldType of RamenTuple.field_typ
let id_or_type_of_field op name =
  let open RamenOperation in
  let find_field_type =
    List.find (fun ft -> ft.RamenTuple.typ_name = name) in
  match op with
  | Aggregate { fields ; _ } ->
      let sf = List.find (fun sf -> sf.alias = name) fields in
      Id (typ_of sf.expr).uniq_num
  | ReadCSVFile { what = { fields ; _ } ; _ } ->
      FieldType (find_field_type fields)
  | ListenFor { proto ; _ } ->
      FieldType (find_field_type (RamenProtocols.tuple_typ_of_proto proto))
  | Instrumentation _ ->
      FieldType (find_field_type RamenBinocle.tuple_typ)
  | Notifications _ ->
      FieldType (find_field_type RamenNotification.tuple_typ)

(* Reading already compiled parents, set the type of fields originating from
 * external parents, parameters and environment, once and for all.
 * Equals the input type of fields originating from internal parents to
 * those output fields. *)
let emit_input_fields oc tuple_sizes parents params funcs =
  List.iter (fun (func, op) ->
    RamenOperation.iter_expr (function
      | Field (_field_typ, tupref, field_name) as expr ->
          if is_virtual_field field_name then (
            (* Type set during parsing *)
          ) else if !tupref = TupleParam then (
            (* Copy the scalar type from the default value: *)
            match RamenTuple.params_find field_name params with
            | exception Not_found ->
                Printf.sprintf "Function %s is using unknown parameter %S"
                  (RamenName.func_color func.F.name)
                  field_name |>
                failwith
            | param ->
                emit_assert_id_eq_typ tuple_sizes (t_of_expr expr) oc param.ptyp.typ.structure ;
                emit_assert_id_is_bool (n_of_expr expr) oc param.ptyp.typ.nullable
          ) else if !tupref = TupleEnv then (
            emit_assert_id_eq_typ tuple_sizes (t_of_expr expr) oc TString ;
            arg_is_nullable oc expr
          ) else if RamenLang.tuple_has_type_input !tupref then (
            let no_such_field pfunc =
              Printf.sprintf2 "Parent %s of %s does not output a field \
                               named %s (only: %a)"
                (RamenName.func_color pfunc.F.name)
                (RamenName.func_color func.F.name)
                field_name
                RamenTuple.print_typ_names pfunc.F.out_type |>
              failwith
            and aggr_types pfunc t prev =
              let fn = pfunc.F.name in
              match prev with
              | None -> Some ([fn], t)
              | Some (prev_fns, prev_t) ->
                  if t <> prev_t then
                    Printf.sprintf2
                      "All parents of %s must agree on the type of field \
                       %s (%a has %a but %s has %a)"
                      (RamenName.func_color func.F.name)
                      field_name
                      (pretty_list_print (fun oc f ->
                        String.print oc (RamenName.func_color f))) prev_fns
                      RamenTypes.print_typ prev_t
                      (RamenName.func_color fn)
                      RamenTypes.print_typ t |>
                    failwith ;
                  Some ((fn::prev_fns), prev_t) in
            (* Return either the type or a set of id to set this field
             * type to: *)
            let typ, same_as_ids =
              let parents = Hashtbl.find_default parents func.F.name [] in
              List.fold_left (fun (prev_typ, same_as_ids) pfunc ->
                (* Is this parent part of local functions? *)
                if pfunc.F.program_name = func.F.program_name then (
                  (* Typing not finished? This parent is in this very
                   * program then. Output the constraint to bind the
                   * input type to the output type: *)
                  (* Retrieve the id for the parent output fields: *)
                  let _, pop =
                    try
                      List.find (fun (f, _op) ->
                        f.F.name = pfunc.F.name) funcs
                    with Not_found ->
                      !logger.error "Cannot find parent %S in any of this \
                                     program functions (have %a)"
                        (RamenName.string_of_func pfunc.F.name)
                        (pretty_list_print (fun oc (f, _op) ->
                          String.print oc (RamenName.string_of_func f.F.name))) funcs ;
                      raise Not_found in
                  match id_or_type_of_field pop field_name with
                  | exception Not_found -> no_such_field pfunc
                  | Id p_id -> prev_typ, p_id::same_as_ids
                  | FieldType ft ->
                      aggr_types pfunc ft.typ prev_typ, same_as_ids
                ) else (
                  (* External parent: look for the exact type: *)
                  let pser =
                    RingBufLib.ser_tuple_typ_of_tuple_typ pfunc.F.out_type in
                  match List.find (fun fld ->
                          fld.RamenTuple.typ_name = field_name
                        ) pser with
                    | exception Not_found -> no_such_field pfunc
                    | ft ->
                        assert (RamenTypes.is_typed ft.typ.structure) ;
                        aggr_types pfunc ft.typ prev_typ, same_as_ids)
              ) (None, []) parents in
            Option.may (fun (_funcs, t) ->
              emit_assert_id_eq_typ tuple_sizes (t_of_expr expr) oc t.structure ;
              emit_assert_id_is_bool (n_of_expr expr) oc t.nullable
            ) typ ;
            List.iter (fun id ->
              let name = expr_err expr Err.InheritType in
              emit_assert_id_eq_id ~name (t_of_expr expr) oc (t_of_num id) ;
              let name = expr_err expr Err.InheritNull in
              emit_assert_id_eq_id ~name (n_of_expr expr) oc (n_of_num id) ;
            ) same_as_ids
          )
      | _ -> ()
    ) op
  ) funcs

let structure_of_sort_identifier = function
  | "bool" -> TBool
  | "string" -> TString
  | "eth" -> TEth
  | "u8" -> TU8
  | "u16" -> TU16
  | "u32" -> TU32
  | "u64" -> TU64
  | "u128" -> TU128
  | "i8" -> TI8
  | "i16" -> TI16
  | "i32" -> TI32
  | "i64" -> TI64
  | "i128" -> TI128
  | "float" -> TFloat
  | "ip4" -> TIpv4
  | "ip6" -> TIpv6
  | "ip" -> TIp
  | "cidr4" -> TCidrv4
  | "cidr6" -> TCidrv6
  | "cidr" -> TCidr
  | unk ->
      !logger.error "Unknown sort identifier %S" unk ;
      TEmpty

let bool_of_term =
  let open RamenSmtParser in
  function
  | QualIdentifier ((Identifier "true", None), []) ->
      true
  | QualIdentifier ((Identifier "false", None), []) ->
      false
  | x ->
      Printf.sprintf2 "Bad term when expecting boolean: %a"
        print_term x |>
      failwith

let rec structure_of_term =
  let open RamenSmtParser in
  function
  | QualIdentifier ((Identifier typ, None), []) ->
      structure_of_sort_identifier typ
  | QualIdentifier ((Identifier "vector", None),
                    [ ConstantTerm c ; typ ; null ]) ->
      let structure = structure_of_term typ
      and nullable = bool_of_term null in
      let n = int_of_constant c in
      TVec (n, { structure ; nullable })
  | QualIdentifier ((Identifier "list", None), [ typ ; null ]) ->
      let structure = structure_of_term typ
      and nullable = bool_of_term null in
      TList { structure ; nullable }
  | QualIdentifier ((Identifier id, None), sub_terms)
    when String.starts_with id "tuple" ->
      (try Scanf.sscanf id "tuple%d%!" (fun sz ->
        let num_sub_terms = List.length sub_terms in
        (* We should have one term for structure and one for nullability: *)
        if num_sub_terms <> 2 * sz then
          Printf.sprintf "Bad number of sub_terms (%d) for tuple%d"
            num_sub_terms sz |>
          failwith ;
        let ts =
          let rec loop ts = function
          | [] -> List.rev ts |> Array.of_list
          | e::n::rest ->
              let t = { structure = structure_of_term e ;
                        nullable = bool_of_term n } in
              loop (t :: ts) rest
          | _ -> assert false in
          loop [] sub_terms in
        TTuple ts
      )
    with e ->
      let what = "While scanning "^ id in
      print_exception ~what e ;
      TEmpty)
  | _ ->
      !logger.warning "TODO: exploit define-fun with funny term" ;
      TEmpty

let emit_smt2 oc ~optimize parents tuple_sizes funcs params =
  (* We have to start the SMT2 file with declarations before we can
   * produce any assertion: *)
  let decls = IO.output_string () in
  (* We might encounter several times the same expression (for the well
   * known constants defined once in RamenExpr, such as expr_true etc...)
   * So we keep a set of already declared ids: *)
  let ids = ref Set.Int.empty in
  let expr_types = IO.output_string () in
  let parent_types = IO.output_string () in
  let declare e =
    let id = (typ_of e).uniq_num in
    if not (Set.Int.mem id !ids) then (
      ids := Set.Int.add id !ids ;
      let eid = t_of_expr e in
      Printf.fprintf decls
        "; %a\n\
         (declare-fun %s () Bool)\n\
         (declare-fun %s () Type)\n"
        (RamenExpr.print true) e
        (n_of_expr e)
        eid)
  in
  (* Set the types for all fields from parents: *)
  emit_input_fields parent_types tuple_sizes parents params funcs ;
  emit_program declare tuple_sizes expr_types funcs ;
  if optimize then emit_minimize expr_types funcs ;
  Printf.fprintf oc
    "(set-option :print-success false)\n\
     (set-option :produce-models %s)\n\
     (set-option :produce-unsat-cores %s)\n\
     (set-option :smt.core.minimize %s)\n\
     (set-logic ALL) ; TODO\n\
     ; Define a sort for types:\n\
     (declare-datatypes\n\
       ( (Type 0) )\n\
       ( ((bool) (string) (eth) (float)\n\
          (u8) (u16) (u32) (u64) (u128)\n\
          (i8) (i16) (i32) (i64) (i128)\n\
          (ip) (ip4) (ip6) (cidr) (cidr4) (cidr6)\n\
          (list (list-type Type) (list-nullable Bool))\n\
          (vector (vector-dim Int) (vector-type Type) (vector-nullable Bool))\n\
          %a) ))\n\
     \n\
     ; Declarations:\n\
     %s\n\
     ; Constraints:\n\
     %s\n\
     ; Children-Parent relationships:\n\
     %s\n\
     ; Closing words: solve and prints the answer:\n\
     (check-sat) ; also experiment with: (check-sat-using smt)\n\
     (get-unsat-core)\n\
     (get-model)\n"
    (if optimize then "true" else "false")
    (if optimize then "false" else "true")
    (if optimize then "false" else "true")
    (Set.Int.print ~first:"" ~last:"" ~sep:"\n" (fun oc sz ->
      Printf.fprintf oc "(tuple%d" sz ;
      for i = 0 to sz-1 do
        Printf.fprintf oc " (tuple%d-e%d Type)" sz i ;
        Printf.fprintf oc " (tuple%d-n%d Bool)" sz i
      done ;
      Printf.fprintf oc ")")) tuple_sizes
    (IO.close_out decls)
    (IO.close_out expr_types)
    (IO.close_out parent_types)

let run_solver smt2_file =
  let cmd =
    if String.exists !smt_solver "%s" then
      String.nreplace ~sub:"%s" ~by:smt2_file ~str:!smt_solver
    else
      !smt_solver ^" "^ shell_quote smt2_file in
  !logger.debug "Running the solver as %S" cmd ;
  (* Lazy way to split the arguments: *)
  let shell = "/bin/sh" in
  let args = [| shell ; "-c" ; cmd |] in
  (* Now summon the solver: *)
  with_subprocess shell args (fun (_oc, ic, ec) ->
    let output = read_whole_channel ic
    and errors = read_whole_channel ec in
    !logger.debug "Solver is done." ;
    let p =
      RamenParsing.allow_surrounding_blanks RamenSmtParser.response in
    let stream = RamenParsing.stream_of_string output in
    match p ["SMT output"] None Parsers.no_error_correction stream |>
          RamenParsing.to_result with
    | Ok (sol, _) -> sol, output
    | Bad e ->
        !logger.error "Cannot parse solver output:\n%s" output ;
        if errors <> "" then failwith errors else
        IO.to_string (RamenParsing.print_bad_result
                        RamenSmtParser.print_response) e |>
        failwith)

let unsat funcs syms output =
  !logger.debug "Solver output:\n%s" output ;
  Printf.sprintf2 "Cannot solve typing constraints: %a"
    (Err.print_core funcs) syms |>
  failwith

let used_tuple_sizes funcs parents =
  let tuple_sizes =
    List.fold_left (fun s (_, op) ->
      RamenOperation.fold_expr s (fun s -> function
        (* The only way to get a tuple in an op is with a Tuple
         * expression: *)
        | Tuple (_, ts) -> Set.Int.add (List.length ts) s
        | _ -> s
      ) op
    ) Set.Int.empty funcs in
  (* We might also got tuples from our parents. We collect all their output
   * fields even if it's not used anywhere for simplicity. *)
  Hashtbl.fold (fun _ fs s ->
    List.fold_left (fun s f ->
      List.fold_left (fun s ft ->
        let open RamenTypes in
        match ft.RamenTuple.typ.structure with
        | TTuple ts -> Set.Int.add (Array.length ts) s
        | _ -> s
      ) s f.F.out_type
    ) s fs
  ) parents tuple_sizes

let get_types parents funcs params smt2_file =
  let funcs = Hashtbl.values funcs |> List.of_enum in
  let h = Hashtbl.create 71 in
  if funcs <> [] then (
    let tuple_sizes = used_tuple_sizes funcs parents in
    mkdir_all ~is_file:true smt2_file ;
    !logger.debug "Writing SMT2 program into %S" smt2_file ;
    File.with_file_out ~mode:[`create; `text; `trunc] smt2_file (fun oc ->
      emit_smt2 oc ~optimize:true parents tuple_sizes funcs params) ;
    match run_solver smt2_file with
    | RamenSmtParser.Solved (model, sure), output ->
        if not sure then
          !logger.warning "Solver best idea after timeout:\n%s" output ;
        (* Output a hash of structure*nullability per expression id: *)
        let open RamenSmtParser in
        List.iter (fun ((sym, vars, sort, term), _recurs) ->
          try Scanf.sscanf sym "%[tn]%d%!" (fun tn id ->
            match vars, sort, tn with
            | [], NonParametricSort (Identifier "Type"), "t" ->
                let structure = structure_of_term term in
                Hashtbl.modify_opt id (function
                  | None -> Some { structure ; nullable = false (* by default *) }
                  | Some prev -> Some { prev with structure }
                ) h
            | [], NonParametricSort (Identifier "Bool"), "n" ->
                let nullable = bool_of_term term in
                Hashtbl.modify_opt id (function
                  | None -> Some { structure = TAny (* by default *) ; nullable }
                  | Some prev -> Some { prev with nullable }
                ) h
            | [], NonParametricSort (Identifier sort), _ ->
                !logger.error "Result not about sort Type but %S?!" sort
            | _, NonParametricSort (IndexedIdentifier _), _ ->
              !logger.warning "TODO: exploit define-fun with indexed identifier"
            | _, ParametricSort _, _ ->
              !logger.warning "TODO: exploit define-fun of parametric sort"
            | _::_, _, _ ->
              !logger.warning "TODO: exploit define-fun with parameters")
          with Scanf.Scan_failure _ | End_of_file | Failure _ -> ()
        ) model
    | RamenSmtParser.Unsolved [], _ ->
        !logger.debug "No unsat-core, resubmitting." ;
        (* Resubmit the same problem without optimizations to get the unsat
         * core: *)
        let smt2_file' = smt2_file ^".no_opt" in
        File.with_file_out ~mode:[`create; `text; `trunc] smt2_file' (fun oc ->
          emit_smt2 oc ~optimize:false parents tuple_sizes funcs params) ;
        (match run_solver smt2_file' with
        | RamenSmtParser.Unsolved syms, output -> unsat funcs syms output
        | RamenSmtParser.Solved _, _ ->
            failwith "Unsat with optimization but sat without?!")
    | RamenSmtParser.Unsolved syms, output -> unsat funcs syms output
  ) ;
  h

(* From here: belongs to RamenTypingHelpers. *)

(* Copy the types of all input and output fields from their source
 * expression. *)
let set_io_tuples parents funcs h =
  let set_output (func, op) =
    List.iter (fun ft ->
      if not (RamenTypes.is_typed ft.RamenTuple.typ.structure) then (
        match op with
        | RamenOperation.Aggregate { fields ; _ } ->
            let id =
              List.find_map (fun sf ->
                if sf.RamenOperation.alias = ft.typ_name then
                  Some (typ_of sf.expr).uniq_num
                else None) fields in
            (match Hashtbl.find h id with
            | exception Not_found ->
                Printf.sprintf2 "Cannot find type for id %d, field %s"
                  id ft.typ_name |>
                failwith
            | typ ->
                !logger.debug "Set output field %s.%s to %a"
                  (RamenName.string_of_func func.F.name) ft.typ_name
                  RamenTypes.print_typ typ ;
                ft.typ <- typ)
        | _ -> assert false)
    ) func.out_type
  and set_input (func, _op) =
    let parents = Hashtbl.find_default parents func.F.name [] in
    List.iter (fun ft ->
      (* For the in_type we have to check that all parents do export each
       * of the mentioned input fields: *)
      if parents = [] then
        Printf.sprintf "Cannot use input field %s without any parent"
          ft.RamenTuple.typ_name |>
        failwith ;
      if not (RamenTypes.is_typed ft.typ.structure) then (
        (* We already know (from the solver) that all parents export the
         * same type. Copy from the first parent: *)
        let parent = List.hd parents in
        let pser =
          RingBufLib.ser_tuple_typ_of_tuple_typ parent.F.out_type in
        match List.find_map (fun pft ->
                if pft.RamenTuple.typ_name = ft.typ_name then
                  Some pft.typ
                else None) pser with
        | exception Not_found ->
            Printf.sprintf "Cannot find field %s in %s"
              ft.typ_name
              (RamenName.func_color parent.F.name) |>
            failwith
        | typ ->
            !logger.debug "Set input field %s.%s to %a"
              (RamenName.string_of_func func.F.name) ft.typ_name
              RamenTypes.print_typ typ ;
            ft.typ <- typ)
    ) func.in_type
  in
  (* Start by setting the output types so that it's then easy to copy
   * from there to the input types: *)
  Hashtbl.iter (fun _ -> set_output) funcs ;
  Hashtbl.iter (fun _ -> set_input) funcs

let apply_types parents funcs h =
  Hashtbl.iter (fun _ (_func, op) ->
    RamenOperation.iter_expr (fun e ->
      let t = typ_of e in
      match Hashtbl.find h t.uniq_num with
      | exception Not_found ->
          !logger.warning "No type for expression %a"
            (RamenExpr.print true) e
      | typ -> t.typ <- Some typ
    ) op
  ) funcs ;
  set_io_tuples parents funcs h
