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
 *   easiest for the solver too). Records are similar, with the additional
 *   parameter to the sort giving the field name.
 *
 * - In case the constraints are satisfiable we get the assignment (the
 *   solution might not be unique if an expression is unused or is literal
 *   NULL). If it's not we obtain the unsatisfiable core and build an error
 *   message from it (in the future: we might interact with the solver to
 *   devise a better error message as in
 *   https://cs.nyu.edu/wies/publ/finding_minimum_type_error_sources.pdf)
 *
 * - We use SMT-LIB v2.6 format as it supports datatypes. Both the latest
 *   versions of Z3 and CVC4 have been tested to work. CVC4 does not support
 *   `minimize` though.
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
open RamenLog
open RamenSmt
open RamenLang
module C = RamenConf
module F = C.Func
module P = C.Program
module Err = RamenTypingErrors
module E = RamenExpr
module O = RamenOperation
module T = RamenTypes
open RamenTypes (* RamenTypes.Pub? *)

let t_of_num num =
  Printf.sprintf "t%d" num

let t_of_expr e =
  t_of_num e.E.uniq_num

let t_of_prefix pref i =
  Printf.sprintf "t_%s_%d" (string_of_prefix pref) i

let n_of_num num =
  Printf.sprintf "n%d" num

let n_of_expr e =
  n_of_num e.E.uniq_num

let n_of_prefix pref i =
  Printf.sprintf "n_%s_%d" (string_of_prefix pref) i

let f_of_name field_names k =
  try Hashtbl.find field_names k |> string_of_int
  with Not_found ->
    Printf.sprintf2 "Record field %a was forgotten from field_names?!"
      N.field_print k |>
    failwith

let rec find_type_of_path_in_typ typ path =
  let invalid_path () =
    Printf.sprintf2 "Cannot find subpath %a in type %a"
      E.print_path path
      T.print_typ typ |>
    failwith in
  match path with
  | [] -> typ
  | E.Name n :: rest ->
      (match typ.T.structure with
      | T.TRecord kts ->
          let _, t =
            Array.find (fun (k, _) -> k = (n :> string)) kts in
          find_type_of_path_in_typ t rest
      | _ ->
          invalid_path ())
  | E.Int idx :: rest ->
      (match typ.T.structure with
      | T.TTuple ts ->
          if idx >= Array.length ts then
            Printf.sprintf2 "Cannot cherry-pick index %d in a tuple with \
              only %d elements"
              idx (Array.length ts) |>
            failwith ;
          find_type_of_path_in_typ ts.(idx) rest
      | T.TVec (d, t) ->
          if idx >= d then
            Printf.sprintf2 "Cannot cherry-pick index %d in a vector of \
              length %d"
              idx d |>
            failwith ;
          find_type_of_path_in_typ t rest
      | T.TList t ->
          find_type_of_path_in_typ t rest
      | _ ->
          invalid_path ())

let find_type_of_path_in_tuple_typ tuple_typ = function
  | [] -> assert false
  | E.Name n :: rest ->
      let fld =
        List.find (fun fld ->
          fld.RamenTuple.name = n
        ) tuple_typ in
      find_type_of_path_in_typ fld.RamenTuple.typ rest
  | E.Int _ :: _ ->
      failwith "Cannot address parent output with an index (yet)"

let rec find_expr_of_path e path =
  let invalid_path () =
    Printf.sprintf2 "Cannot find subpath %a in expression %a"
      E.print_path path
      (E.print ~max_depth:2 false) e |>
    failwith
  in
  match path with
  | [] -> e
  | E.Name n :: rest ->
      (match e.E.text with
      | E.Record kvs ->
          let _, e = List.find (fun (k, _) -> k = n) kvs in
          find_expr_of_path e rest
      | _ ->
          invalid_path ())
  | E.Int idx :: rest ->
      (match e.E.text with
      | E.Vector es
      | E.Tuple es ->
          (match List.at es idx with
          | exception Invalid_argument _ -> raise Not_found
          | e -> find_expr_of_path e rest)
      | _ ->
          invalid_path ())

let find_expr_of_path_in_selected_fields fields = function
  | [] -> assert false
  | E.Name n :: rest ->
      let sf = List.find (fun sf ->
        sf.O.alias = n) fields in
      find_expr_of_path sf.O.expr rest
  | E.Int _ :: _ ->
      failwith "Cannot address input with an index (yet)"

let expr_err e err = Err.Expr (e.E.uniq_num, err)
let func_err fi err = Err.Func (fi, err)

let emit_id_eq_smt2 id oc smt2 =
  Printf.fprintf oc "(= %s %s)" id smt2

let emit_id_eq_id = emit_id_eq_smt2

let emit_is_tuple id oc sz =
  Printf.fprintf oc "((_ is tuple%d) %s)" sz id

let emit_is_record id oc sz =
  Printf.fprintf oc "((_ is record%d) %s)" sz id

let rec emit_id_eq_typ tuple_sizes records field_names id oc = function
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
      (* FIXME: also emit what is known about the column types in ts *)
  | TRecord ts ->
      let d = Array.length ts in
      Printf.fprintf oc "(and %a" (emit_is_record id) d ;
      (* Also emit what is known about the field types *)
      Array.iteri (fun i (k, t) ->
        let k = N.field k in
        emit_id_eq_typ tuple_sizes records field_names
          (Printf.sprintf "(record%d-e%d %s)" d i id) oc t.T.structure ;
        emit_is_bool t.T.nullable
          (Printf.sprintf "(record%d-n%d %s)" d i id) oc ;
        emit_id_eq_smt2 (f_of_name field_names k) oc
          (Printf.sprintf "(record%d-f%d %s)" d i id)
      ) ts ;
      Printf.fprintf oc ")"
  | TVec (d, t) ->
      let id' = Printf.sprintf "(vector-type %s)" id in
      Printf.fprintf oc "(and ((_ is vector) %s) %a"
        id (emit_id_eq_typ tuple_sizes records field_names id') t.structure ;
      (* FIXME: assert (d > 0) *)
      if d <> 0 then Printf.fprintf oc " (= %d (vector-dim %s))" d id ;
      Printf.fprintf oc ")"
  | TList t ->
      let id' = Printf.sprintf "(list-type %s)" id in
      Printf.fprintf oc "(and ((_ is list) %s) %a)"
        id (emit_id_eq_typ tuple_sizes records field_names id') t.structure

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

let emit_assert_id_eq_typ ?name tuple_sizes records field_names id oc t =
  emit_assert ?name oc (fun oc ->
    emit_id_eq_typ tuple_sizes records field_names id oc t)

let emit_assert_id_eq_smt2 ?name id oc smt2 =
  emit_assert ?name oc (fun oc -> emit_id_eq_smt2 id oc smt2)

let emit_assert_id_eq_id = emit_assert_id_eq_smt2

let assert_imply ?name a oc b =
  emit_assert ?name oc (fun oc -> emit_imply a oc b)

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
let empty_record_fields : (N.field, int * int * int) Hashtbl.t =
  Hashtbl.create 0
let empty_field_names : (N.field, int) Hashtbl.t = Hashtbl.create 0

let arg_has_type typ oc e =
  let name = IO.to_string T.print_structure typ in
  let name = expr_err e (Err.ActualType name) in
  emit_assert ~name oc (fun oc ->
    emit_id_eq_typ empty_tuple_sizes empty_record_fields empty_field_names
                   (t_of_expr e) oc typ)

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

(* Look through the stack for the record this path is referring to. *)
let locate_opened_record stack e path =
  !logger.info "Expr %a (path %a) supposed to be found in the environment"
    (E.print ~max_depth:2 false) e
    E.print_path path ;
  match path with
  | E.Name n :: path' ->
      (match List.find_map (fun e ->
              match e.E.text with
              | Record kvs ->
                  (try Some (List.find_map (fun (k, v) ->
                              if k = n then Some (e, k, v, path')
                              else None
                            ) kvs)
                  with Not_found -> None)
              | _ -> None
            ) stack with
      | exception Not_found ->
          Printf.sprintf2 "Cannot find field %a in scope (%a)"
            N.field_print_quoted n
            (pretty_list_print (E.print ~max_depth:1 false)) stack |>
          failwith
      | ret ->
          !logger.info "Found some opened record in the environment with path %a"
            E.print_path path ;
          ret)
  | _ ->
      Printf.sprintf2
        "Invalid path %a: must start with a name to address an opened record."
        E.print_path path |>
      failwith

let eq_to_opened_record_field stack e oc path =
  let rec_expr, field_name, field_expr, path =
    locate_opened_record stack e path in
  !logger.debug "Looking for path %a in that field (%a)"
    E.print_path path
    (E.print ~max_depth:2 false) field_expr ;
  match find_expr_of_path field_expr path with
  | exception Not_found ->
      Printf.sprintf2 "Cannot find path %a within field %a (%a)"
        E.print_path path
        N.field_print_quoted field_name
        (E.print ~max_depth:2 false) field_expr |>
      failwith
  | path_target ->
      let name =
        expr_err e Err.(OpenedRecordIs rec_expr.E.uniq_num) in
      emit_assert_id_eq_id ~name (t_of_expr e) oc
        (t_of_expr path_target) ;
      emit_assert_id_eq_id ~name (n_of_expr e) oc
        (n_of_expr path_target)

let eq_to_opened_record stack e oc path =
  let rec_expr, _, _, _ = locate_opened_record stack e path in
  let name =
    expr_err e Err.(OpenedRecordIs rec_expr.E.uniq_num) in
  emit_assert_id_eq_id ~name (t_of_expr rec_expr) oc (t_of_expr e) ;
  emit_assert_id_eq_id ~name (n_of_expr rec_expr) oc (n_of_expr e)

(* Assuming all input/output/constants have been declared already, emit the
 * constraints connecting the parameter to the result: *)
let emit_constraints tuple_sizes records field_names
                     in_type out_type param_type env_type ?func_name oc clause
                     stack e =
  let eid = t_of_expr e and nid = n_of_expr e in
  emit_comment oc "%a%s: %a"
    (fun oc -> function
      | Some f -> Printf.fprintf oc "%a, " N.func_print f
      | None -> ()) func_name
    clause
    (E.print false) e ;
  (* Then we also have specific rules according to the operation at hand: *)
  match e.E.text with
  | Stateless (SL0 (Path _)) ->
      (* Input paths have been specified already in [emit_in_types]. *)
      ()

  | Variable Record ->
      (* This expression must be equated to the actual record expression
       * that we could find in the stack, shall we know the field name...
       * So only Get can do that. *)
      ()

  | Variable pref ->
      let id, pref' =
        if tuple_has_type_input pref then
          option_get "Input record type must be defined" in_type, TupleIn
        else if tuple_has_type_output pref then
          option_get "Output record type must be defined" out_type, TupleOut
        else if pref = TupleParam then
          option_get "Params record type must be defined" param_type, pref
        else (
          if pref <> TupleEnv then
            !logger.error "got a variable for %s?!" (string_of_prefix pref) ;
          assert (pref = TupleEnv) ;
          option_get "Environment record type must be defined" env_type, pref
        ) in
      emit_assert_id_eq_id eid oc (t_of_prefix pref' id) ;
      (* This one is always nullable, others never: *)
      let nullable = pref = TupleOutPrevious in
      let name = expr_err e Err.(Nullability nullable) in
      emit_assert_id_is_bool ~name nid oc nullable

  | Const VNull ->
      (* - "NULL" is nullable. *)
      arg_is_nullable oc e

  | Const x ->
      (* - A const cannot be null, unless it's VNull;
       * - The type is the type of the constant. *)
      arg_has_type (T.structure_of x) oc e ;
      arg_is_not_nullable oc e

  | Binding _ -> assert false (* Not supposed to appear that soon *)

  | Tuple es ->
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

  | Record kvs ->
      (* - The resulting type is a record which length, items type and
       *   nullability are given by the values in kvs;
       * - The result is not nullable since it has a literal value.
       * Note that for typing we type all expressions defined in [kvs]
       * even if they end up being shadowed. *)
      let d = List.length kvs in

      emit_assert oc (fun oc -> emit_is_record eid oc d) ;
      List.iteri (fun i (k, v) ->
        emit_assert_id_eq_smt2 (t_of_expr v) oc
          (Printf.sprintf "(record%d-e%d %s)" d i eid) ;
        emit_assert_id_eq_smt2 (n_of_expr v) oc
          (Printf.sprintf "(record%d-n%d %s)" d i eid) ;
        emit_assert_id_eq_smt2 (f_of_name field_names k) oc
          (Printf.sprintf "(record%d-f%d %s)" d i eid)
      ) kvs ;
      emit_assert_is_false oc nid

  | Vector es ->
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

  | Case (cases, else_) ->
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
      List.iteri (fun i E.{ case_cond = cond ; case_cons = cons } ->
        let name = expr_err e (Err.CaseCond (i, num_cases)) in
        emit_assert_id_eq_typ ~name tuple_sizes records field_names
                              (t_of_expr cond) oc TBool ;
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
                (n_of_expr case.E.case_cond)
                (n_of_expr case.E.case_cons))) cases
            (match else_ with
            | None -> "true"
            | Some e -> n_of_expr e)))

  | Stateless (SL1s (Coalesce, es)) ->
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
        let name = expr_err x (Err.CoalesceNullLast (i, len)) in
        let is_last = i = len - 1 in
        emit_assert_id_is_bool ~name (n_of_expr x) oc (not is_last)
      ) es ;

  | Stateless (SL0 (Now|Random|EventStart|EventStop)) ->
      (* - The result is a non nullable float *)
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TFloat ;
      arg_is_not_nullable oc e

  | Stateless (SL1 (Defined, x)) ->
      (* - x must be nullable;
       * - The result is a non nullable bool. *)
      arg_is_nullable oc x ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TBool ;
      arg_is_not_nullable oc e

  | Stateful (_, _, SF1 ((AggrSum|AggrMin|AggrMax|AggrFirst
                         |AggrLast|AggrAnd|AggrOr as aggr), x)) ->
      (* - if x is a list/vector then the result has the type of its elements;
       * - if x is a list/vector then the result is as nullable as its
       *   elements or the list itself;
       * - otherwise the result has the type and nullability of x. *)
      emit_assert oc (fun oc ->
        let xtid = t_of_expr x
        and xnid = n_of_expr x in
        Printf.fprintf oc
          "(or (and ((_ is list) %s) \
                    (= %s (list-type %s)) \
                    (= %s (or %s (list-nullable %s)))) \
               (and ((_ is vector) %s) \
                    (= %s (vector-type %s)) \
                    (= %s (or %s (vector-nullable %s)))) \
               (and (not ((_ is list) %s)) \
                    (not ((_ is vector) %s)) \
                    (= %s %s) \
                    (= %s %s)))"
          xtid eid xtid nid xnid xtid
          xtid eid xtid nid xnid xtid
          xtid xtid eid xtid nid (n_of_expr x)) ;

      (match aggr with AggrSum ->
        (* - The result is numeric *)
        arg_is_numeric oc e
      | AggrAnd | AggrOr ->
        (* - The result is a boolean *)
        emit_assert_id_eq_typ tuple_sizes records field_names eid oc TBool
      | _ -> ())

  | Stateful (_, _, SF1 (AggrAvg, x)) ->
      (* - x must be numeric or a list/vector of numerics;
       * - The result is a float;
       * - The result is as nullable as x and its elements. *)
      let name = expr_err x Err.Numeric in
      emit_assert ~name oc (fun oc ->
        let xid = t_of_expr x in
        Printf.fprintf oc
          "(or (and ((_ is list) %s) \
                    %a \
                    %a) \
               (and ((_ is vector) %s) \
                    %a \
                    %a) \
               (and (not ((_ is list) %s)) \
                    (not ((_ is vector) %s)) \
                    %a))"
          xid
            emit_numeric ("(list-type "^ xid ^")")
            (emit_imply ("(list-nullable "^ xid ^")")) nid
          xid
            emit_numeric ("(vector-type "^ xid ^")")
            (emit_imply ("(vector-nullable "^ xid ^")")) nid
          xid
            xid
            emit_numeric xid) ;

      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TFloat ;
      assert_imply (n_of_expr x) oc nid

  | Stateless (SL1 (Minus, x)) ->
      (* - The only argument must be numeric;
       * - The result must not be smaller than x;
       * - The result has same nullability than x;
       * - The result is signed or float. *)
      arg_is_numeric oc x ;
      emit_assert_id_le_id (t_of_expr x) oc eid ;
      arg_is_signed oc e ;
      emit_assert_id_eq_id (n_of_expr x) oc nid

  | Stateless (SL1 (Age, x)) ->
      (* - The only argument must be numeric;
       * - The result is a float;
       * - The result has same nullability than x. *)
      arg_is_numeric oc x ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TFloat ;
      emit_assert_id_eq_id (n_of_expr x) oc nid

  | Stateless (SL1 (Abs, x)) ->
      (* - The only argument must be numeric;
       * - The result has same type than x;
       * - The result has same nullability than x. *)
      arg_is_numeric oc x ;
      emit_assert_id_le_id (t_of_expr x) oc eid ;
      emit_assert_id_eq_id (n_of_expr x) oc nid

  | Stateless (SL1 (Not, x)) ->
      (* - The only argument must be boolean;
       * - The result type is a boolean;
       * - The resulting nullability depends solely on that of x. *)
      arg_is_bool oc x ;
      emit_assert_id_eq_id nid oc (n_of_expr x) ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TBool

  | Stateless (SL1 (Cast t, x)) ->
      (* - The only argument (x) can be anything;
       * - The result type is as described by the chosen type;
       * - The result is nullable if we specifically choose to cast
       *   toward a nullable type, or merely propagates.
       * Note that some cast are actually not implemented so would fail
       * when generating code. *)
      if t.T.nullable then
        emit_assert_is_true oc nid
      else
        emit_assert_id_eq_id nid oc (n_of_expr x) ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc t.structure

  | Stateless (SL2 (Percentile, e1, e2)) ->
      (* - e1 must be a vector or list of anything;
       * - the result is as nullable as e1 elements;
       * - if e2 is an immediate vector, then:
       *   - e2 must be a non nullable vector of non-nullable numeric
       *     constants;
       *   - the result is a vector of the same length as e2;
       *   - the result element type is the same as the element type of e1;
       *   - the result elements are not nullable.
       * - otherwise, e2 is a single value:
       *   - e2 must be a non-nullable numeric constant;
       *   - the result type is the same as the element type of e1;
       *   - the result elements are not nullable. *)
      let ret_vec =
        match e2 with { text = E.Vector _ ; _ } -> true | _ -> false in
      assert_imply (n_of_expr e1) oc nid ;
      emit_assert oc (fun oc ->
        let eid1 = t_of_expr e1 in
        let eid' = if ret_vec then "(vector-type "^ eid ^")"
                   else eid in
        Printf.fprintf oc
          "(or (and ((_ is list) %s) \
                    (= %s (list-type %s)) \
                    (or (not (list-nullable %s)) %s)) \
               (and ((_ is vector) %s) \
                    (= %s (vector-type %s)) \
                    (or (not (vector-nullable %s)) %s)))"
          eid1
            eid' eid1
            eid1 nid
          eid1
            eid' eid1
            eid1 nid) ;
      if ret_vec then (
        let name = expr_err e2 Err.NumericVec in
        emit_assert ~name oc (fun oc ->
          Printf.fprintf oc
            "(and ((_ is vector) %s) (not (vector-nullable %s)) %a)"
            (t_of_expr e2)
            (t_of_expr e2)
            emit_numeric ("(vector-type "^ t_of_expr e2 ^")")) ;
        emit_assert oc (fun oc ->
          Printf.fprintf oc
            "(and ((_ is vector) %s) \
                  (= (vector-dim %s) (vector-dim %s)) \
                  (not (vector-nullable %s)))"
                  eid
                  eid (t_of_expr e2)
                  eid)
      ) else arg_is_numeric oc e2 ;
      arg_is_not_nullable oc e2

  | Stateless (SL2 ((Add|Mul|IDiv|Pow|Trunc), e1, e2)) ->
      (* - e1 and e2 must be numeric;
       * - The result is not smaller than e1 or e2;
       * - TODO: For Trunc, e2 must be greater than 0 even if float. *)
      arg_is_numeric oc e1 ;
      arg_is_numeric oc e2 ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2)) ;
      emit_assert_id_le_id (t_of_expr e1) oc eid ;
      emit_assert_id_le_id (t_of_expr e2) oc eid
      (* TODO: for IDiv, have a TInt type and make_int_typ when parsing *)

  | Stateless (SL2 (Sub, e1, e2)) ->
      (* Same as above, with the addition that the result is signed even
       * if both operands are unsigned: *)
      arg_is_numeric oc e1 ;
      arg_is_numeric oc e2 ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2)) ;
      emit_assert_id_le_id (t_of_expr e1) oc eid ;
      emit_assert_id_le_id (t_of_expr e2) oc eid ;
      arg_is_signed oc e

  | Stateless (SL2 ((Reldiff|Div), e1, e2)) ->
      (* - e1 and e2 must be numeric;
       * - The result is a float. *)
      arg_is_numeric oc e1 ;
      arg_is_numeric oc e2 ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TFloat ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))

  | Stateless (SL2 ((StartsWith|EndsWith), e1, e2)) ->
      (* - e1 and e2 must be strings;
       * - The result is a bool;
       * - Result nullability propagates from arguments. *)
      arg_is_string oc e1 ;
      arg_is_string oc e2 ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TBool ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))

  | Stateless (SL2 (Concat, e1, e2))
  | Generator (Split (e1, e2)) ->
      (* - e1 and e2 must be strings;
       * - The result is also a string;
       * - Result nullability propagates from arguments. *)
      arg_is_string oc e1 ;
      arg_is_string oc e2 ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TString ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))

  | Stateless (SL2 (Strftime, e1, e2)) ->
      (* - e1 must be a string and e2 a float (ideally, a time);
       * - Then result will be a string;
       * - Its nullability propagates from arguments. *)
      arg_is_string oc e1 ;
      arg_is_float oc e2 ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TString ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))

  | Stateless (SL1 (Length, x)) ->
      (* - x must be a string or a list;
       * - The result type is an U32;
       * - The result nullability is the same as that of x. *)
      let name = expr_err e Err.LengthType in
      let xid = t_of_expr x in
      emit_assert ~name oc (fun oc ->
        Printf.fprintf oc "(or (= string %s) ((_ is list) %s))" xid xid) ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TU32 ;
      emit_assert_id_eq_id nid oc (n_of_expr x)

  | Stateless (SL1 ((Lower|Upper), x)) ->
      (* - x must be a string;
       * - The result type is also a string;
       * - The result nullability is the same as that of x. *)
      arg_is_string oc x ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TString ;
      emit_assert_id_eq_id nid oc (n_of_expr x)

  | Stateless (SL1 (Like _, x)) ->
      (* - x must be a string;
       * - The result is a bool;
       * - Nullability propagates from x to e. *)
      arg_is_string oc x ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TBool ;
      emit_assert_id_eq_id nid oc (n_of_expr x)

  | Stateless (SL1 (Strptime, x)) ->
      (* - x must be a string;
       * - The Result is a float (a time);
       * - The result is always nullable (known from the parse). *)
      arg_is_string oc x ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TFloat ;
      emit_assert_is_true oc nid

  | Stateless (SL1 (Variant, x)) ->
      (* - x must be a string (the experiment name);
       * - The Result is a string (the name of the variant);
       * - The result is always nullable (if the experiment is not defined). *)
      arg_is_string oc x ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TString ;
      emit_assert_is_true oc nid

  | Stateless (SL2 (Mod, e1, e2))
  | Stateless (SL2 ((BitAnd|BitOr|BitXor|BitShift), e1, e2)) ->
      (* - e1 and e2 must be any integer;
       * - The result must not be smaller that e1 nor e2;
       * - Nullability propagates. *)
      arg_is_integer oc e1 ;
      arg_is_integer oc e2 ;
      emit_assert_id_le_id (t_of_expr e1) oc eid ;
      emit_assert_id_le_id (t_of_expr e2) oc eid ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))

  | Stateless (SL2 ((Ge|Gt), e1, e2)) ->
      (* - e1 and e2 must have the same sort;
       * - The result is a bool;
       * - Nullability propagates. *)
      emit_assert_same e oc (t_of_expr e1) (t_of_expr e2) ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TBool ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))

  | Stateless (SL2 (Eq, e1, e2)) ->
      (* - e1 and e2 must be of the same sort;
       * - The result is a bool;
       * - Nullability propagates from arguments. *)
      emit_assert_same e oc (t_of_expr e1) (t_of_expr e2) ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TBool ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))

  | Stateless (SL2 ((And|Or), e1, e2)) ->
      (* - e1 and e2 must be booleans;
       * - The result is also a boolean;
       * - Nullability propagates. *)
      arg_is_bool oc e1 ;
      arg_is_bool oc e2 ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TBool ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))

  | Stateless (SL2 (Get, n, x)) ->
      (* Typing rules:
       * - either:
       *   - x must be a vector, a list or a tuple;
       *   - and n must be an unsigned;
       *   - if x is a vector and n is a constant, then n must be less than
       *     its length;
       *   - if x is a tuple then n must be a constant less than the tuple
       *     length, and the resulting type is that of that element;
       *   - otherwise, the resulting type is the same as the list/vector
       *     element type;
       *   - if x is a vector and n a constant, or if x is a tuple, then the
       *     result has the same nullability than x or x's elements; in all
       *     other cases, the result is nullable.
       * - or:
       *   - n must be a constant string (which exclude the above
       *     alternative); We have checked already that the field name is
       *     legit, ie there exist a record that we know of with such a
       *     field, and we know where to look for it in a record, which
       *     limits the possibilities to a few.
       *   - x must be a record with a field named like that, and the
       *     resulting type is that of that field;
       *   - the result is nullable if that field is nullable or the record
       *     itself is;
       *   - For the above to work we need to know a mapping from any legit
       *     field name to the list of possible tuple sizes and field
       *     position with the tuple: [records].
       *)
      (match E.int_of_const n with
      | Some i ->
          let name = expr_err x Err.GettableByInt in
          arg_is_numeric oc n ;
          emit_assert ~name oc (fun oc ->
            Printf.fprintf oc
              "(let ((tmp %s)) \
                 (or (and ((_ is vector) tmp) \
                          (> (vector-dim tmp) %d) \
                          (= (vector-type tmp) %s) \
                          (= (or %s (vector-nullable tmp)) %s)) \
                     (and ((_ is list) tmp) \
                          (= (list-type tmp) %s) \
                          %s)"
              (t_of_expr x)
              i
              eid
              (n_of_expr x) nid
              eid
              nid ;
            Printf.fprintf oc "%a"
              (Set.Int.print ~first:"" ~last:"" ~sep:" " (fun oc sz ->
                if sz > i then
                  Printf.fprintf oc " \
                     (and %a \
                          (= (tuple%d-e%d %s) %s) \
                          (= (tuple%d-n%d %s) %s))"
                    (emit_is_tuple "tmp") sz
                    sz i "tmp" eid
                    sz i "tmp" nid))
                tuple_sizes ;
            Printf.fprintf oc "))")
      | None ->
          (match E.string_of_const n with
          | None ->
              (* Assuming numeric non const index: *)
              let name = expr_err x Err.GettableByInt in
              arg_is_numeric oc n ;
              emit_assert ~name oc (fun oc ->
                Printf.fprintf oc "(let ((tmp %s)) \
                                     (or (and ((_ is vector) tmp) \
                                              (= (vector-type tmp) %s))\
                                         (and ((_ is list) tmp) \
                                              (= (list-type tmp) %s))))"
                  (t_of_expr x) eid eid) ;
              arg_is_nullable oc e
          | Some i ->
              arg_is_string oc n ;
              let k = N.field i in
              let rec_lst =
                try Hashtbl.find_all records k
                with Not_found ->
                  Printf.sprintf2 "Subfield %S is still unknown!" i |>
                  failwith in
              assert (rec_lst <> []) ;
              let had_several = list_longer_than 1 rec_lst in
              if had_several then
                emit_comment oc "Field %a is present in %d records!"
                  N.field_print k
                  (List.length rec_lst) ;
              let name = expr_err x Err.GettableByName in
              emit_assert ~name oc (fun oc ->
                Printf.fprintf oc
                  (if not had_several then "%a" else "(or %a)")
                  (List.print ~first:"" ~last:"" ~sep:" "
                    (fun oc (name_idx, rec_size, field_pos) ->
                      Printf.fprintf oc
                        "(and ((_ is record%d) %s) \
                              (= (record%d-e%d %s) %s) \
                              (= (or %s (record%d-n%d %s)) %s) \
                              (= (record%d-f%d %s) %d))"
                        rec_size (t_of_expr x)
                        rec_size field_pos (t_of_expr x) eid
                        (n_of_expr x)
                        rec_size field_pos (t_of_expr x) nid
                        rec_size field_pos (t_of_expr x) name_idx
                    )) rec_lst) ;
              (* Note: the above assertions merely equals the n-th field
               * to [x]. In case [x] is a Record variable then we won't tell
               * what record expression this variable refers to. To do this
               * requires the knowledge of the field name, ie [k], so it's a
               * good place to do this though. Let's therefore do it here: *)
              if x.text = Variable Record then
                eq_to_opened_record stack x oc [ Name k ]))

  | Stateless (SL1 ((BeginOfRange|EndOfRange), x)) ->
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

  | Stateless (SL1s ((Min|Max), es)) ->
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

  | Stateless (SL1s (Print, es)) ->
      (* The result must have the same type as the first parameter *)
      emit_assert_id_eq_id (t_of_expr (List.hd es)) oc eid ;
      emit_assert_id_eq_id (n_of_expr (List.hd es)) oc nid

  | Stateless (SL3 (SubString, s, a, b)) ->
      (* - s must be a string;
       * - a and b must be integers (may be negative);
       * - the result is a string;
       * - nullability comes solely from the arguments - in particular, if
       *   a and b does not delimit a valid substring the result will be a
       *   non null empty string. *)
      arg_is_string oc s ;
      arg_is_integer oc a ;
      arg_is_integer oc b ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TString ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf2 "(or %s %s %s)"
          (n_of_expr s) (n_of_expr a) (n_of_expr b))

  | Stateless (SL3 (DontBeLonely, _, _, _))
  | Stateful (_, _, SF3s (DontLeaveMeAlone, _, _, _)) ->
      assert false

  | Stateful (_, _, SF2 (Lag, e1, e2)) ->
      (* Typing rules:
       * - e1 must be an unsigned;
       * - e2 has same type as the result;
       * - The result is only nullable by skip or propagation, for if the
       *   lag goes beyond the start of the window then lag merely returns
       *   the oldest value. (FIXME: should return NULL in that case) *)
      arg_is_integer oc e1 ;
      emit_assert_id_eq_id (t_of_expr e2) oc eid ;
      emit_assert_id_eq_id (n_of_expr e2) oc nid

  | Stateful (_, _, SF3 ((MovingAvg|LinReg), e1, e2, e3)) ->
      (* Typing rules:
       * - e1 must be an unsigned (the period);
       * - e2 must also be an unsigned (the number of values to average);
       * - Neither e1 or e2 can be NULL;
       * - e3 must be numeric or a list/vector of numerics;
       * - The result type is float;
       * - The result nullability propagates from e3 (and its elements). *)
      arg_is_unsigned oc e1 ;
      arg_is_unsigned oc e2 ;
      arg_is_not_nullable oc e1 ;
      arg_is_not_nullable oc e2 ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TFloat ;

      emit_assert oc (fun oc ->
        let xid = t_of_expr e3 in
        Printf.fprintf oc
          "(or (and ((_ is list) %s) %a %a) \
               (and ((_ is vector) %s) %a %a) \
               (and (not ((_ is list) %s)) \
                    (not ((_ is vector) %s)) \
                    %a))"
          xid emit_numeric ("(list-type "^ xid ^")")
              (emit_imply ("(list-nullable "^ xid ^")")) nid
          xid emit_numeric ("(vector-type "^ xid ^")")
              (emit_imply ("(vector-nullable "^ xid ^")")) nid
          xid
            xid
            emit_numeric xid) ;
      assert_imply (n_of_expr e3) oc nid

  | Stateful (_, _, SF4s (MultiLinReg, e1, e2, e3, e4s)) ->
      (* As above, with the addition of predictors that must also be
       * numeric and non null. Why non null? See comment in check_variadic
       * and probably get rid of this limitation. *)
      arg_is_unsigned oc e1 ;
      arg_is_unsigned oc e2 ;
      arg_is_numeric oc e3 ;
      arg_is_not_nullable oc e1 ;
      arg_is_not_nullable oc e2 ;
      emit_assert_id_eq_id (n_of_expr e3) oc nid ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TFloat ;
      List.iter (fun e ->
        arg_is_numeric oc e ;
        arg_is_not_nullable oc e ;
      ) e4s

  | Stateful (_, _, SF2 (ExpSmooth, e1, e2)) ->
      (* Typing rules:
       * - e1 must be a non-null float (and ideally, between 0 and 1), but
       *   we just ask for a numeric in order to also accept immediate
       *   values parsed as integers, and integer fields;
       * - e2 must be numeric *)
      arg_is_float oc e1 ;
      arg_is_numeric oc e2 ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TFloat ;
      arg_is_not_nullable oc e1 ;
      emit_assert_id_eq_id (n_of_expr e2) oc nid

  | Stateless (SL1 ((Exp|Log|Log10|Sqrt), x)) ->
      (* - x must be numeric;
       * - The result is a float;
       * - The result nullability is inherited from arguments *)
      arg_is_numeric oc x ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TFloat ;
      emit_assert_id_eq_id (n_of_expr x) oc nid

  | Stateless (SL1 ((Floor|Ceil|Round), x)) ->
      (* - x must be numeric;
       * - The result is not smaller than x;
       * - Nullability propagates from argument. *)
      arg_is_numeric oc x ;
      emit_assert_id_le_smt2 (t_of_expr x) oc eid ;
      emit_assert_id_eq_id (n_of_expr x) oc nid

  | Stateless (SL1 (Hash, _x)) ->
      (* - x can be anything. Notice that hash(NULL) is NULL;
       * - The result is an I64.
       * - Nullability propagates. *)
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TI64 ;
      emit_assert_id_eq_id (n_of_expr e) oc nid

  | Stateless (SL1 (Sparkline, x)) ->
      (* - x must be a vector of non-null numerics;
       * - The result is a string;
       * - The result nullability itself propagates from x. *)
      let name = expr_err x Err.NumericVec in
      emit_assert_id_eq_typ ~name tuple_sizes records field_names (t_of_expr x) oc
        (TVec (0, T.make ~nullable:false TNum)) ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TString ;
      emit_assert_id_eq_id (n_of_expr x) oc nid

  | Stateful (_, _, SF4s (Remember, fpr, tim, dur, es)) ->
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
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TBool ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf2 "(or %s %s%a)"
          (n_of_expr tim) (n_of_expr dur)
          (List.print ~first:" " ~last:"" ~sep:" " (fun oc e ->
            String.print oc (n_of_expr e))) es)

  | Stateful (_, _, Distinct es) ->
      (* - The es can be anything;
       * - The result is a boolean;
       * - The result is nullable if any of the es is nullable. *)
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TBool ;
      if es <> [] then
        emit_assert_id_eq_smt2 nid oc
          (Printf.sprintf2 "(or %a)"
            (List.print ~first:"" ~last:"" ~sep:" " (fun oc e ->
              String.print oc (n_of_expr e))) es)

  | Stateful (_, _, SF3 (Hysteresis, meas, accept, max)) ->
      (* - meas, accept and max must be numeric;
       * - The result is a boolean;
       * - The result is nullable if and only if any of meas, accept and
       *   max is. *)
      arg_is_numeric oc meas ;
      arg_is_numeric oc accept ;
      arg_is_numeric oc max ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TBool ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s %s)"
          (n_of_expr meas) (n_of_expr accept) (n_of_expr max))

  | Stateful (_, _, Top { want_rank ; c ; max_size ; what ; by ; duration ;
                          time }) ->
      (* Typing rules:
       * - c must be numeric and not null;
       * - max_size, if set, must be numeric and not null ;
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
      Option.may (fun s ->
        arg_is_numeric oc s ;
        emit_assert_is_false oc (n_of_expr s)
      ) max_size ;
      arg_is_numeric oc by ;
      arg_is_numeric oc duration ;
      emit_assert_is_false oc (n_of_expr duration) ;
      arg_is_numeric oc time ;
      if want_rank then (
        emit_assert_id_eq_id (t_of_expr c) oc eid ;
        arg_is_nullable oc e
      ) else (
        emit_assert_id_eq_typ tuple_sizes records field_names eid oc TBool ;
        emit_assert_id_eq_smt2 nid oc
          (Printf.sprintf2 "(or%a %s)"
            (List.print ~first:" " ~last:"" ~sep:" " (fun oc w ->
              String.print oc (n_of_expr w))) what
            (n_of_expr by)))

  | Stateful (_, n, SF3s (Last, c, x, es)) ->
      (* - c must be a constant (TODO) strictly (TODO) positive integer;
       * - The type of the result is a list of items of the same type than x;
       * - If we skip nulls then those items are not nullable, otherwise
       *   they are as nullable as x;
       * - In theory, 'Last c e1 by es` should itself be nullable if c is
       *   nullable or any of the es is nullable. And then become and stays
       *   null forever as soon as one es is actually NULL. This is kind
       *   of useless, so we just disallow sizing with and ordering by a
       *   nullable value;
       * - The Last itself is null whenever the number of received item is
       *   less than c, and so is always nullable. *)
      arg_is_unsigned oc c ;
      arg_is_not_nullable oc c ;
      emit_assert_id_eq_smt2 eid oc
        (Printf.sprintf "(list %s %s)"
          (t_of_expr x)
          (if n then "false" else n_of_expr x)) ;
      List.iter (arg_is_not_nullable oc) es ;
      arg_is_nullable oc e

  | Stateful (_, n, SF2 (Sample, c, x)) ->
      (* - c must be a constant (TODO) strictly (TODO) positive integer;
       * - c must not be nullable;
       * - The type of the result is a list of items of the same type than x;
       * - If we skip nulls then those items are not nullable, otherwise they
       *   are as nullable as x;
       * - 'sample c x` is itself nullable whenever x is nullable (if not
       *   skip null and we encounter a null x, or if skip null and we
       *   encounter only nulls). *)
      arg_is_unsigned oc c ;
      arg_is_not_nullable oc c ;
      emit_assert_id_eq_smt2 eid oc
        (Printf.sprintf "(list %s %s)"
          (t_of_expr x)
          (if n then "false" else n_of_expr x)) ;
      emit_assert_id_eq_id nid oc (n_of_expr x)

  | Stateful (_, n, Past { what ; time ; max_age ; sample_size }) ->
      (* - max_age must be a constant (TODO) numeric, greater than 0 (TODO);
       * - max_age must not be nullable;
       * - time must be a time (numeric);
       * - sample_size, if set, must be a constant (TODO) strictly (TODO)
       *   positive integer, not nullable;
       * - The type of the result is a list of items of the same type than
       *   [what];
       * - If we skip nulls then those items are not nullable, otherwise
       *   they are as nullable as [what];
       * - The result itself is nullable if we skip nulls and [what] is
       *   nullable, and if there are less than max_age of data (similar
       *   behavior than the last function and similarly arguable). Therefore
       *   it is always nullable. *)
      arg_is_numeric oc max_age ;
      arg_is_not_nullable oc max_age ;
      arg_is_numeric oc time ;
      Option.may (fun sample_size ->
        arg_is_unsigned oc sample_size ;
        arg_is_not_nullable oc sample_size
      ) sample_size ;
      emit_assert_id_eq_smt2 eid oc
        (Printf.sprintf "(list %s %s)"
          (t_of_expr what)
          (if n then "false" else n_of_expr what)) ;
      arg_is_nullable oc e

  | Stateful (_, n, SF1 (Group, g)) ->
      (* - The result is a list which elements have the exact same type as g;
       * - If we skip nulls then the elements are not nullable, otherwise they
       *   are as nullable as g;
       * - The group itself is nullable whenever g is nullable.
       * Note: It is possible to build as an immediate value a vector of
       * zero length (although, actually it's not), but it is not possible
       * to build an empty list. So Group will, under any circumstance,
       * never return an empty list. The only possible way to build an
       * empty list is by skipping nulls, but then is we skip all nulls
       * it will be null. *)
      emit_assert_id_eq_smt2 eid oc
        (Printf.sprintf "(list %s %s)"
          (t_of_expr g)
          (if n then "false" else n_of_expr g)) ;
      emit_assert_id_eq_id (n_of_expr g) oc nid

  | Stateful (_, _, SF1 (AggrHistogram (_, _, n), x)) ->
      (* - x must be numeric;
       * - The result is a vector of size n+2, of non nullable U32;
       * - The result itself is as nullable as x. *)
      arg_is_numeric oc x ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc
        (TVec (n+2, T.make ~nullable:false TU32)) ;
      emit_assert_id_eq_id (n_of_expr x) oc nid

  | Stateless (SL2 (In, e1, e2)) ->
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
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TBool ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf2
          "(or %s %s (and ((_ is list) %s) (list-nullable %s)) \
                     (and ((_ is vector) %s) (vector-nullable %s)))"
          (n_of_expr e1) (n_of_expr e2)
          (t_of_expr e2) (t_of_expr e2)
          (t_of_expr e2) (t_of_expr e2))

(* FIXME: we should have only the records known from the run cond *)
let emit_running_condition declare tuple_sizes records field_names
                           param_type env_type oc e =
  !logger.debug "Emit SMT2 for running condition" ;
  Printf.fprintf oc "\n; Running Condition\n\n" ;
  E.iter (fun _ e -> declare e) e ;
  let name = Err.RunCondition in
  emit_assert_id_eq_typ ~name tuple_sizes records field_names (t_of_expr e) oc TBool ;
  emit_assert_is_false ~name oc (n_of_expr e) ;
  E.iter (
    emit_constraints tuple_sizes records field_names
                     None None param_type env_type oc "running condition"
  ) e

(* FIXME: we should have only the records accessible from this operation *)
let emit_operation declare tuple_sizes records field_names
                   in_type out_type param_type env_type func_name fi oc op =
  (* Declare all variables: *)
  O.iter_expr (fun _ _ e -> declare e) op ;
  (* Now add specific constraints depending on the clauses: *)
  (match op with
  | Aggregate { where ; notifications ; commit_cond ; every ; _ } ->
      O.iter_expr (
        emit_constraints tuple_sizes records field_names
                         in_type out_type param_type env_type ~func_name oc
      ) op ;
      (* Typing rules:
       * - Where must be a bool;
       * - Commit-when must also be a bool;
       * - Every-clause must be numeric;
       * - Notification names must be non-nullable strings. *)
      let name = func_err fi Err.(Clause ("where", ActualType "bool")) in
      emit_assert_id_eq_typ ~name tuple_sizes records field_names (t_of_expr where) oc TBool ;
      let name = func_err fi Err.(Clause ("where", Nullability false)) in
      emit_assert_is_false ~name oc (n_of_expr where) ;
      let name = func_err fi Err.(Clause ("commit", ActualType "bool")) in
      emit_assert_id_eq_typ ~name tuple_sizes records field_names (t_of_expr commit_cond) oc TBool ;
      let name = func_err fi Err.(Clause ("commit", Nullability false)) in
      emit_assert_is_false ~name oc (n_of_expr commit_cond) ;
      Option.may (fun e ->
        let name = func_err fi Err.(Clause ("every", ActualType "float")) in
        emit_assert_id_eq_typ ~name tuple_sizes records field_names
                              (t_of_expr e) oc TFloat ;
        let name = func_err fi Err.(Clause ("every", Nullability false)) in
        emit_assert_is_false ~name oc (n_of_expr e)
      ) every ;
      List.iteri (fun i notif ->
        let name = func_err fi Err.(Notif (i, ActualType "string")) in
        emit_assert_id_eq_typ ~name tuple_sizes records field_names
          (t_of_expr notif) oc TString ;
        let name = func_err fi Err.(Notif (i, Nullability false)) in
        emit_assert_is_false ~name oc (n_of_expr notif)
      ) notifications

  | ReadCSVFile { preprocessor ; where = { fname ; unlink } ; _ } ->
      O.iter_expr (
        emit_constraints tuple_sizes records field_names
                         in_type out_type param_type env_type ~func_name oc
      ) op ;
      Option.may (fun p ->
        (*  must be a non-nullable string: *)
        let name = func_err fi Err.(Preprocessor (ActualType "string")) in
        emit_assert_id_eq_typ ~name tuple_sizes records field_names (t_of_expr p) oc TString ;
        let name = func_err fi Err.(Preprocessor (Nullability false)) in
        emit_assert_is_false ~name oc (n_of_expr p)
      ) preprocessor ;
      let name = func_err fi Err.(Filename (ActualType "string")) in
      emit_assert_id_eq_typ ~name tuple_sizes records field_names (t_of_expr fname) oc TString ;
      let name = func_err fi Err.(Filename (Nullability false)) in
      emit_assert_is_false ~name oc (n_of_expr fname) ;
      let name = func_err fi Err.(Unlink (ActualType "bool")) in
      emit_assert_id_eq_typ ~name tuple_sizes records field_names (t_of_expr unlink) oc TBool ;
      let name = func_err fi Err.(Unlink (Nullability false)) in
      emit_assert_is_false ~name oc (n_of_expr unlink)

  | _ -> ())

let emit_program declare tuple_sizes records field_names
                 in_types out_types param_type env_type oc funcs =
  (* Output all the constraints for all the operations: *)
  List.iteri (fun fi func ->
    let fq = F.fq_name func in
    !logger.debug "Emit SMT2 for function %a" N.fq_print fq ;
    Printf.fprintf oc "\n; Constraints for function %a\n"
      N.fq_print fq ;
    (* Not all functions have an input or output type but then it won't
     * be used: *)
    let in_type = List.assoc_opt fq in_types
    and out_type = List.assoc_opt fq out_types in
    (* FIXME: filter the records to pass only those accessible from this op *)
    emit_operation declare tuple_sizes records field_names
                   in_type out_type param_type env_type
                   func.F.name fi oc func.F.operation
  ) funcs

let emit_minimize oc condition funcs =
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
  let cost_of_expr _ _ e =
    let eid = t_of_expr e in
    match e.E.typ with
    | { structure = (TAny | TNum) ; _ } ->
        Printf.fprintf oc " (cost_of_number %s)" eid
    | _ -> () in
  (* "box" tells z3 to optimize both constraints independently and is
   * slightly faster: *)
  Printf.fprintf oc "(set-option :opt.priority box)\n";
  Printf.fprintf oc "(minimize (+ 0" ;
  Option.may (E.iter (cost_of_expr ())) condition ;
  List.iter (fun func ->
    O.iter_expr cost_of_expr func.F.operation
  ) funcs ;
  Printf.fprintf oc "))\n" ;
  (* And, separately, number of signed values: *)
  Printf.fprintf oc
    "\n; Minimize total signed ints\n\
       (define-fun cost_of_sign ((n Type)) Int\n\
       (ite (or (= i8 n) (= i16 n) (= i32 n) (= i64 n) (= i128 n)) 1 0))\n" ;
  let cost_of_expr _ _ e =
    let eid = t_of_expr e in
    match e.E.typ with
    | { structure = (TAny | TNum) ; _ } ->
        Printf.fprintf oc " (cost_of_sign %s)" eid
    | _ -> () in
  Printf.fprintf oc "(minimize (+ 0" ;
  Option.may (E.iter (cost_of_expr ())) condition ;
  List.iter (fun func ->
    O.iter_expr cost_of_expr func.F.operation
  ) funcs ;
  Printf.fprintf oc "))\n"

(* When a function refers to an input field from a parent, we need to either
 * equates its type to the type of the expression computing the output
 * field (in case of Aggregates) or directly to the type of the output
 * field if it is a well known field (which has no expression). *)
type id_or_type = Id of int | FieldType of RamenTuple.field_typ
let id_or_type_of_field op path =
  let find_field_type what fields =
    (* In the future, when [path] does have several components, look through
     * all of them in turn in the TRecords as we do with the external parents *)
    let field =
      match path with [ E.Name n ] -> n
      | _ ->
          Printf.sprintf2 "Cherry-picking (%a) in well-known types (%s) is \
                           not supported"
            E.print_path path what |>
          failwith
    in
    List.find (fun ft -> ft.RamenTuple.name = field) fields in
  match op with
  | O.Aggregate { fields ; _ } ->
      (* In case [path] has several components, look through the
       * Record expression to locate the actual expression which id we
       * should equate to the callers' expression. *)
      Id (find_expr_of_path_in_selected_fields fields path).E.uniq_num
  | O.ReadCSVFile { what = { fields ; _ } ; _ } ->
      FieldType (find_field_type "CSV" fields)
  | O.ListenFor { proto ; _ } ->
      FieldType (find_field_type (RamenProtocols.string_of_proto proto)
                                 (RamenProtocols.tuple_typ_of_proto proto))
  | O.Instrumentation _ ->
      FieldType (find_field_type "instrumentation"
                                 RamenWorkerStats.tuple_typ)
  | O.Notifications _ ->
      FieldType (find_field_type "notifications"
                                 RamenNotification.tuple_typ)

let emit_out_types decls oc field_names funcs =
  !logger.debug "Emitting SMT2 for output types" ;
  List.fold_left (fun (assoc, i as prev) func ->
    let rec_tid = t_of_prefix TupleOut i
    and rec_nid = n_of_prefix TupleOut i in
    match func.F.operation with
    | Aggregate { fields ; _ } ->
        let sz = List.length fields in
        Printf.fprintf decls
          "\n; Output record type for %a\n\n\
           (declare-fun %s () Type)\n\
           (declare-fun %s () Bool)\n"
          N.fq_print (F.fq_name func)
          rec_tid rec_nid ;
        emit_assert oc (fun oc -> emit_is_record rec_tid oc sz) ;
        (* All outputs are non null (but for previous_out, see Variable): *)
        emit_assert_id_is_bool rec_nid oc false ;
        (* For typing this is not necessary to order the structure fields in
         * any specific way, as we carry along the field names. *)
        List.iteri (fun j sf ->
          (* Equates each field expression to its field: *)
          let id = sf.O.expr.uniq_num in
          Printf.fprintf oc
            "; Output field %d (%a.%a) equals expression %d\n"
            j N.func_print func.F.name N.field_print sf.alias id ;
          emit_assert oc (fun oc ->
            Printf.fprintf oc "(= %s (record%d-e%d %s))"
              (t_of_num id) sz j rec_tid) ;
          emit_assert oc (fun oc ->
            Printf.fprintf oc "(= %s (record%d-n%d %s))"
              (n_of_num id) sz j rec_tid) ;
          emit_assert oc (fun oc ->
            Printf.fprintf oc "(= %s (record%d-f%d %s))"
              (f_of_name field_names sf.alias) sz j rec_tid)
        ) fields ;
        (F.fq_name func, i) :: assoc, i + 1
    | _ ->
        (* FIXME: those should also have an out record expression, although
         * their individual fields are not expressions. *)
        prev
  ) ([], 0) funcs |> fst

(* Reading already compiled parents, set the type of fields originating from
 * external parents, parameters and environment, once and for all.
 * Equals the input type of fields originating from internal parents to
 * those output fields.
 * Build the list of keys used for params and env, and from there declare
 * two records, which fields equates the types output above, and return
 * their id. *)
let emit_in_types decls oc tuple_sizes records field_names parents params
                  condition funcs =
  !logger.debug "Emitting SMT2 for input types, with field names = %a"
    (Hashtbl.print N.field_print Int.print) field_names ;
  (* Build the input type of each func by collecting all the Get(name, x) or
   * Path(path) where x is input-like. Then also build the type of the
   * params and env records.
   * Build a record type for each of those and return it. *)
  let in_fields = Hashtbl.create 10
  and param_fields = Hashtbl.create 10
  and env_fields = Hashtbl.create 10
  in
  let get_sub_hash h func =
    let func_name = Option.map F.fq_name func in
    try Hashtbl.find h func_name
    with Not_found ->
      let h' = Hashtbl.create 10 in
      Hashtbl.add h func_name h' ;
      h' in
  let register_input h func path e =
    let h = get_sub_hash h func in
    (* When we know the type from a pre-compiled parent, a well-known
     * output type, or because it's declared with parameters or a given
     * because it's an envvar, then we set a hard constraint: *)
    let field_name = E.id_of_path path in
    Hashtbl.replace h field_name e
  in
  let program_iter f condition funcs =
    Option.may (fun cond ->
        E.iter (f ?func:None "Running condition" "") cond |>
        ignore
    ) condition ;
    List.iter (fun func ->
      let what =
        Printf.sprintf2 "Function %a" N.func_print func.F.name in
      O.iter_expr (f ?func:(Some func) what) func.operation |>
      ignore
    ) funcs
  in
  let register_io ?func what e prefix path =
    emit_comment oc "register_io of %a%s"
      (fun oc -> function
        | Some f -> Printf.fprintf oc "%a, " N.func_print f.F.name
        | None -> ()) func
      what ;
    match prefix with
    | TupleEnv ->
        emit_assert_id_eq_typ tuple_sizes records field_names
          (t_of_expr e) oc TString ;
        arg_is_nullable oc e ;
        (* Also make this expression stands for this env field: *)
        register_input env_fields None path e
    | TupleParam ->
        let field =
          match path with [ E.Name n ] -> n
          | _ -> failwith "Cherry-picking from parameters not supported" in
        (match RamenTuple.params_find field params with
        | exception Not_found ->
            Printf.sprintf2 "%s is using unknown parameter %a"
              what E.print_path path |>
            failwith
        | param ->
            emit_assert_id_eq_typ tuple_sizes records field_names
              (t_of_expr e) oc param.ptyp.typ.structure ;
            emit_assert_id_is_bool (n_of_expr e) oc param.ptyp.typ.nullable ;
            (* Also make this expression stands for this param field: *)
            register_input param_fields None path e)
    | TupleIn
    | TupleSortFirst | TupleSortSmallest | TupleSortGreatest
    | TupleMergeGreatest ->
        (match func with
        | None ->
            Printf.sprintf2 "%s has no input %a"
              what E.print_path path |>
            failwith ;
        | Some func ->
            let no_such_field pfunc =
              Printf.sprintf2 "Parent %a of %s does not output a field \
                               named %a (only: %a) (in: %a)"
                N.func_print pfunc.F.name
                what
                E.print_path path
                RamenTuple.print_typ
                  (O.out_type_of_operation ~with_private:true
                                           pfunc.F.operation)
                (E.print false) e |>
              failwith
            and aggr_types pfunc t prev =
              let fn = pfunc.F.name in
              match prev with
              | None -> Some ([fn], t)
              | Some (prev_fns, prev_t) ->
                  if t <> prev_t then
                    Printf.sprintf2
                      "All parents of %s must agree on the type of field \
                       %a (%a has %a but %s has %a)"
                      what
                      E.print_path path
                      (pretty_list_print (fun oc f ->
                        String.print oc (N.func_color f))) prev_fns
                      T.print_typ prev_t
                      (N.func_color fn)
                      T.print_typ t |>
                    failwith ;
                  Some ((fn::prev_fns), prev_t) in
            (* Return either the type or a set of id to set this field
             * type to: *)
            let typ, same_as_ids =
              let parents = Hashtbl.find_default parents func.F.name [] in
              List.fold_left (fun (prev_typ, same_as_ids) pfunc ->
                (* Is this parent part of local functions? *)
                if pfunc.F.program_name = func.F.program_name then (
                  (* Retrieve the id for the parent output fields: *)
                  (* FIXME: WhaaaaaaaaAAAAAAAT?
                   *        What's wrong with the above pfunc!? *)
                  let pfunc =
                    try List.find (fun f -> f.F.name = pfunc.F.name) funcs
                    with Not_found ->
                      !logger.error "Cannot find parent %S in any of this \
                                     program functions (have %a)"
                        (pfunc.F.name :> string)
                        (pretty_list_print (fun oc f ->
                          String.print oc (f.F.name :> string))) funcs ;
                      raise Not_found in
                  (* When the parent is an aggr then we will just make its
                   * output equal to this input (added to same_as_ids), but
                   * when it has a well-known type then we will make this
                   * type equal to that well known type. *)
                  match id_or_type_of_field pfunc.F.operation path with
                  | exception Not_found -> no_such_field pfunc
                  | Id p_id ->
                      prev_typ, p_id::same_as_ids
                  | FieldType typ ->
                      aggr_types pfunc typ.RamenTuple.typ prev_typ,
                      same_as_ids
                ) else (
                  (* External parent: look for the exact type *)
                  let pser =
                    O.out_type_of_operation ~with_private:true
                                             pfunc.F.operation in
                  (* If [path] has several component then look for each
                   * components one after the other, localizing the type
                   * through the TRecords.
                   * We have to treat differently the first path that must
                   * match the selected field name, from the rest that must
                   * match TRecord fields; TODO: have a single output value!
                   *)
                  match find_type_of_path_in_tuple_typ pser path with
                  | exception Not_found -> no_such_field pfunc
                  | typ ->
                      assert (T.is_typed typ.T.structure) ;
                      aggr_types pfunc typ prev_typ, same_as_ids)
              ) (None, []) parents in
            (* [typ] is the of that input, known either from a pre-compiled
             * parent or a well-known output type of another function. We
             * must equate [e] to this type: *)
            Option.may (fun (_funcs, t) ->
              emit_assert_id_eq_typ tuple_sizes records field_names
                (t_of_expr e) oc t.structure ;
              emit_assert_id_is_bool (n_of_expr e) oc t.nullable
            ) typ ;
            (* If all we know is that [e] must have the same type as other
             * expressions, we set soft constraints for all of them: *)
            List.iter (fun id ->
              let name = expr_err e Err.InheritType in
              emit_assert_id_eq_id ~name (t_of_expr e) oc (t_of_num id) ;
              let name = expr_err e Err.InheritNull in
              emit_assert_id_eq_id ~name (n_of_expr e) oc (n_of_num id)
            ) same_as_ids ;
            (* Also, make [e] stand for the input field [field] of this
             * function: *)
            register_input in_fields (Some func) path e)
    | _tup_ref -> (* Ignore non-inputs *) ()
  in
  program_iter (fun ?func what _ _ e ->
    match e.E.text with
    | Stateless (SL2 (Get, E.{ text = Const (VString s) ; _ },
                           E.{ text = Variable prefix ; _ })) ->
        register_io ?func what e prefix [ Name (N.field s) ]
    | Stateless (SL0 (Path path)) ->
        register_io ?func what e TupleIn path
    | _ -> ()
  ) condition funcs ;
  (* For param_fields, env_fields and each function in in_fields, declare
   * the structure for the first id of the list and make the others equal
   * to it. *)
  (* Given a hash, emit all the declarations and return the assoc list of
   * func fq_name and the identifier for the record type (that we must also
   * declare): *)
  let rec_num = ref 0 in
  let declare_input pref h =
    Hashtbl.fold (fun fq_name h assoc ->
      let id = !rec_num in
      incr rec_num ;
      let rec_tid = t_of_prefix pref id
      and rec_nid = n_of_prefix pref id in
      Printf.fprintf decls
        "\n; Record type for %s\n\n\
         (declare-fun %s () Type)\n\
         (declare-fun %s () Bool)\n"
        (match fq_name with
        | None -> string_of_prefix pref
        | Some fq ->
            Printf.sprintf2 "input of function %a" N.fq_print fq)
        rec_tid rec_nid ;
      let sz = Hashtbl.length h in
      emit_assert oc (fun oc -> emit_is_record rec_tid oc sz) ;
      (* All inputs are non null: *)
      emit_assert_id_is_bool rec_nid oc false ;
      (* We need to set the fields in the right order as we also want to
       * type the variables "in", "env", etc, themselves: *)
      let in_order = Hashtbl.enum h |> Array.of_enum in
      Array.fast_sort (fun (f1, _) (f2, _) ->
        N.compare f1 f2) in_order ;
      Array.iteri (fun i (field, e) ->
        (* Make [e] stands for this field: *)
        Printf.fprintf oc
          "; Expression %a (%d) is field %d (%a)\n"
          (E.print ~max_depth:2 false) e
          e.E.uniq_num
          i N.field_print field ;
        emit_assert oc (fun oc ->
          Printf.fprintf oc "(= %s (record%d-e%d %s))"
            (t_of_expr e) sz i rec_tid) ;
        emit_assert oc (fun oc ->
          Printf.fprintf oc "(= %s (record%d-n%d %s))"
            (n_of_expr e) sz i rec_tid) ;
        emit_assert oc (fun oc ->
          Printf.fprintf oc "(= %s (record%d-f%d %s))"
            (f_of_name field_names field) sz i rec_tid)
      ) in_order ;
      (fq_name, id) :: assoc
    ) h []
  in
  (* We have one structure describing the input of each parent. *)
  let in_types = declare_input TupleIn in_fields
  and param_type = declare_input TupleParam param_fields
  and env_type = declare_input TupleEnv env_fields
  in
  (* In theory we have only one entry (for fq_name = None) for both params
   * and env, since we've never registered func: *)
  let opt_first = function
    | [] -> None
    | [_fq, x] -> Some x
    | _ -> assert false in
  let in_types =
    List.map (fun (fq_opt, x) ->
      option_get "in_types belong to a function" fq_opt, x) in_types
  and param_type = opt_first param_type
  and env_type = opt_first env_type in
  in_types, param_type, env_type

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

let rec structure_of_identifier name_of_idx bindings id =
  match List.assoc id bindings with
  | exception Not_found ->
      (* Better be a predefined binding then: *)
      structure_of_sort_identifier id
  | term ->
      structure_of_term name_of_idx bindings term

and structure_of_term name_of_idx bindings =
  let open RamenSmtParser in
  function
  | QualIdentifier ((Identifier id, None), []) ->
      structure_of_identifier name_of_idx bindings id
  | QualIdentifier ((Identifier "vector", None),
                    [ ConstantTerm c ; typ ; null ]) ->
      let structure = structure_of_term name_of_idx bindings typ
      and nullable = bool_of_term null in
      let n = int_of_constant c in
      TVec (n, T.make ~nullable structure)
  | QualIdentifier ((Identifier "list", None), [ typ ; null ]) ->
      let structure = structure_of_term name_of_idx bindings typ
      and nullable = bool_of_term null in
      TList (T.make ~nullable structure)
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
              let nullable = bool_of_term n
              and structure = structure_of_term name_of_idx bindings e in
              let t = T.make ~nullable structure in
              loop (t :: ts) rest
          | _ -> assert false in
          loop [] sub_terms in
        TTuple ts
      )
    with e ->
      let what = "While scanning "^ id in
      print_exception ~what e ;
      TEmpty)
  | QualIdentifier ((Identifier id, None), sub_terms)
    when String.starts_with id "record" ->
      (try Scanf.sscanf id "record%d%!" (fun sz ->
        let num_sub_terms = List.length sub_terms in
        (* We should have one term for structure, one for nullability and
         * one for field name: *)
        if num_sub_terms <> 3 * sz then
          Printf.sprintf "Bad number of sub_terms (%d) for record%d"
            num_sub_terms sz |>
          failwith ;
        let ts =
          let rec loop ts = function
          | [] -> List.rev ts |> Array.of_list
          | e::n::f::rest ->
              let nullable = bool_of_term n
              and structure = structure_of_term name_of_idx bindings e in
              let t = T.make ~nullable structure in
              let idx = int_of_term f in
              if idx < 0 || idx >= Array.length name_of_idx then
                Printf.sprintf2 "Invalid field number %d (fields are: %a)"
                  idx
                  (pretty_array_print String.print) name_of_idx |>
                failwith ;
              let name = name_of_idx.(idx) in
              loop ((name, t) :: ts) rest
          | _ -> assert false in
          loop [] sub_terms in
        TRecord ts
      )
    with e ->
      let what = "While scanning "^ id in
      print_exception ~what e ;
      TEmpty)
  | Let (bs, t) ->
      let bindings = bs @ bindings in
      structure_of_term name_of_idx bindings t
  | term ->
      !logger.warning "Unimplemented term: %a" print_term term ;
      TEmpty

let emit_smt2 parents tuple_sizes records field_names condition funcs params oc ~optimize =
  (* We have to start the SMT2 file with declarations before we can
   * produce any assertion: *)
  let decls = IO.output_string () in
  (* We might encounter several times the same expression (for the well
   * known constants defined once in RamenExpr, such as expr_true etc...)
   * So we keep a set of already declared ids: *)
  let ids = ref Set.Int.empty in
  let expr_types = IO.output_string () in
  let io_types = IO.output_string () in
  let declare e =
    let id = e.E.uniq_num in
    if not (Set.Int.mem id !ids) then (
      ids := Set.Int.add id !ids ;
      Printf.fprintf decls
        "\n; %a\n\
         (declare-fun %s () Bool)\n\
         (declare-fun %s () Type)\n"
        (E.print true) e
        (n_of_expr e)
        (t_of_expr e))
  in
  (* Declare a record for all output types of all local (Aggr) functions,
   * that we can use to equate any Variable bound to TupleOut. The
   * fields will be equal to the selected_field.expr, which are also
   * used by the child, so this is all equivalent and the output record
   * is a kind of "alias" for the actual fields. The only advantage
   * is that we can now easily type `Get(x, "out")`. *)
  let out_types =
    emit_out_types decls io_types field_names funcs in
  (* Declare relationships between all input types: *)
  let in_types, param_type, env_type =
    emit_in_types decls io_types tuple_sizes records field_names parents
                  params condition funcs in
  Option.may (
    emit_running_condition declare tuple_sizes records field_names
                           param_type env_type expr_types
  ) condition ;
  emit_program declare tuple_sizes records field_names
               in_types out_types param_type env_type expr_types funcs ;
  if optimize then emit_minimize expr_types condition funcs ;
  let record_sizes =
    Hashtbl.fold (fun _ (_, sz, _) szs ->
      Set.Int.add sz szs
    ) records Set.Int.empty in
  Printf.fprintf oc
    "%a\
     ;\n\
     ; Define a sort for types:\n\
     ;\n\
     (declare-datatypes\n\
       ( (Type 0) )\n\
       ( ((bool) (string) (eth) (float)\n\
          (u8) (u16) (u32) (u64) (u128)\n\
          (i8) (i16) (i32) (i64) (i128)\n\
          (ip) (ip4) (ip6) (cidr) (cidr4) (cidr6)\n\
          (list (list-type Type) (list-nullable Bool))\n\
          (vector (vector-dim Int) (vector-type Type) (vector-nullable Bool))\n\
          %a\n\
          %a) ))\n\
     \n\
     ;\n\
     ; Declarations:\n\
     ;\n\
     %s\n\
     ;\n\
     ; Children-Parent relationships:\n\
     ;\n\
     %s\n\
     ;\n\
     ; Constraints:\n\
     ;\n\
     %s\n\
     %t"
    preamble optimize
    (Set.Int.print ~first:"" ~last:"" ~sep:"\n" (fun oc sz ->
      Printf.fprintf oc "(tuple%d" sz ;
      for i = 0 to sz-1 do
        Printf.fprintf oc " (tuple%d-e%d Type)" sz i ;
        Printf.fprintf oc " (tuple%d-n%d Bool)" sz i
      done ;
      Printf.fprintf oc ")")) tuple_sizes
    (Set.Int.print ~first:"" ~last:"" ~sep:"\n" (fun oc sz ->
      Printf.fprintf oc "(record%d" sz ;
      for i = 0 to sz-1 do
        Printf.fprintf oc " (record%d-e%d Type)" sz i ;
        Printf.fprintf oc " (record%d-n%d Bool)" sz i ;
        (* This integer gives us the number of the field name *)
        Printf.fprintf oc " (record%d-f%d Int)" sz i
      done ;
      Printf.fprintf oc ")")) record_sizes
    (IO.close_out decls)
    (IO.close_out io_types)
    (IO.close_out expr_types)
    post_scriptum

let used_tuples_records funcs parents =
  let records = Hashtbl.create 10 in
  let field_names = Hashtbl.create 10 in
  let register_field k rec_sz field_pos =
    let n =
      try Hashtbl.find field_names k
      with Not_found ->
        let n = Hashtbl.length field_names in
        Hashtbl.add field_names k n ;
        n in
    !logger.debug
      "Register field %a (%d) at position %d in a record of length %d"
      N.field_print k
      n field_pos rec_sz ;
    Hashtbl.add records k (n, rec_sz, field_pos)
  in
  let tuple_sizes, params, envvars =
    List.fold_left (fun i func ->
      O.fold_expr i
        (fun _ _ (tuple_sizes, params, envvars as prev) e ->
        let register_param_or_env tuple name =
          if tuple = TupleParam then
            tuple_sizes, (Set.add name params), envvars
          else if tuple = TupleEnv then
            tuple_sizes, params, (Set.add name envvars)
          else
            prev
        in
        match e.E.text with
        (* The simplest ways to get a tuple in an op are with a Tuple or a
         * Record literal expression: *)
        | Tuple ts ->
            (Set.Int.add (List.length ts) tuple_sizes), params, envvars
        | Record kvs ->
            (* We must type all defined fields, including those that are
             * shadowed: *)
            let d = List.length kvs in
            (* For each possible field name we have to record that it's a
             * legit field name for a record of that length at that
             * position: *)
            List.iteri (fun i (k, _) -> register_field k d i) kvs ;
            prev
        (* But the parameters and the UNIX-env are treated as records as
         * well, so here is our chance to learn about them. Since we don't
         * know the length of those records before the end of the loop,
         * just remember the field names: *)
        | Stateless (SL2 (Get, { text = Const (VString name) ; _ },
                               { text = Variable tuple ; _ })) ->
            register_param_or_env tuple (N.field name)
        | _ ->
            prev
      ) func.F.operation
    ) (Set.Int.empty, Set.empty, Set.empty) funcs in
  let register_set s =
    let l = Set.cardinal s in
    set_iteri (fun i n -> register_field n l i) s in
  register_set params ;
  register_set envvars ;
  (* This function accept the set of tuple_sizes and a type, and will
   * (recursively) explore that type for tuples/records and return the new
   * extended tuple_sizes set and will also call register_field on all
   * discovered record fields: *)
  let rec look_into_type tuple_sizes t =
    match t.T.structure with
    | TTuple ts ->
        let tuple_sizes = Set.Int.add (Array.length ts) tuple_sizes in
        Array.fold_left look_into_type tuple_sizes ts
    | TVec (_, t) | TList t ->
        look_into_type tuple_sizes t
    | TRecord kts ->
        let d = Array.length kts in
        Array.fold_lefti (fun tuple_sizes i (k, t) ->
          register_field (N.field k) d i ;
          look_into_type tuple_sizes t
        ) tuple_sizes kts
    | _ ->
        tuple_sizes
  in
  (* We might also get tuples and records from our parents. With the help of
   * the above function, collect all their output fields even if it's not
   * used anywhere for simplicity. *)
  let tuple_sizes =
    Hashtbl.fold (fun _ fs s ->
      List.fold_left (fun s f ->
        let out_type =
          O.out_type_of_operation ~with_private:true f.F.operation in
        List.fold_left (fun s ft ->
          look_into_type s ft.RamenTuple.typ
        ) s out_type
      ) s fs
    ) parents tuple_sizes in
  (* Finally, every input and output of all functions might also be a
   * record: *)
  let look_into_type t =
    (* A variant that only register the record fields recursively: *)
    ignore (look_into_type Set.Int.empty t) in
  List.iter (fun func ->
    let out_typ =
      O.out_type_of_operation ~with_private:true func.F.operation in
    (* Keep user defined order: *)
    let sz = List.length out_typ in
    List.iteri (fun i ft ->
      register_field ft.RamenTuple.name sz i ;
      look_into_type ft.typ
    ) out_typ ;
    let sz = List.length func.in_type in
    List.iteri (fun i fm ->
      let name = E.id_of_path fm.RamenFieldMaskLib.path in
      register_field name sz i
    ) func.in_type
  ) funcs ;
  tuple_sizes, records, field_names

let get_types parents condition funcs params fname =
  let funcs = Hashtbl.values funcs |> List.of_enum in
  let h = Hashtbl.create 71 in
  if funcs <> [] then (
    let open RamenSmtParser in
    let tuple_sizes, records, field_names =
      used_tuples_records funcs parents in
    (* Build the inverse index of field legit names to index for parsing
     * the solution.
     * Note: To avoid circular deps TRecord field names are strings: *)
    let name_of_idx =
      Array.create (Hashtbl.length field_names) "" in
    Hashtbl.iter (fun (k : N.field) idx ->
      name_of_idx.(idx) <- (k :> string)
    ) field_names ;
    assert (Array.for_all (fun n -> n <> "") name_of_idx) ;
    let emit = emit_smt2 parents tuple_sizes records field_names
                         condition funcs params
    and parse_result sym vars sort term =
      try Scanf.sscanf sym "%[tn]%d%!" (fun tn id ->
        match vars, sort, tn with
        | [], NonParametricSort (Identifier "Type"), "t" ->
            let structure = structure_of_term name_of_idx [] term in
            Hashtbl.modify_opt id (function
              | None ->
                  Some { structure ; nullable = false (* by default *) }
              | Some prev ->
                  Some { prev with structure }
            ) h
        | [], NonParametricSort (Identifier "Bool"), "n" ->
            let nullable = bool_of_term term in
            Hashtbl.modify_opt id (function
              | None ->
                  Some { structure = TAny (* by default *) ; nullable }
              | Some prev ->
                  Some { prev with nullable }
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
    and unsat syms output =
      !logger.debug "Solver output:\n%s" output ;
      Printf.sprintf2 "Cannot solve typing constraints: %a"
        (Err.print_core funcs) syms |>
      failwith
    in
    run_smt2 ~fname ~emit ~parse_result ~unsat) ;
  h

(* From here: belongs to RamenTypingHelpers. *)

(* Copy the types of all input and output fields from their source
 * expression. *)
let set_io_tuples parents funcs h =
  let set_output func =
    !logger.debug "set_output of function %a"
      N.func_print func.F.name ;
    O.out_type_of_operation ~with_private:true func.F.operation |>
    List.iter (fun ft ->
      !logger.debug "set_output of field %a"
        N.field_print ft.RamenTuple.name ;
      if T.is_typed ft.RamenTuple.typ.structure then (
        !logger.debug "...already typed to %a" T.print_typ ft.RamenTuple.typ
      ) else (
        match func.F.operation with
        | O.Aggregate { fields ; _ } ->
            let id =
              List.find_map (fun sf ->
                if sf.O.alias = ft.name then
                  Some sf.expr.E.uniq_num
                else None) fields in
            (match Hashtbl.find h id with
            | exception Not_found ->
                Printf.sprintf2 "Cannot find type for id %d, field %a"
                  id N.field_print ft.name |>
                failwith
            | typ ->
                !logger.debug "Set output field %a.%a to %a"
                  N.func_print func.F.name
                  N.field_print ft.name
                  T.print_typ typ ;
                ft.typ <- typ)
        | _ -> assert false))
  and set_input func =
    !logger.debug "set_input of function %a"
      N.func_print func.F.name ;
    let parents = Hashtbl.find_default parents func.F.name [] in
    List.iter (fun f ->
      !logger.debug "set_input for input %a"
        RamenFieldMaskLib.print_in_field f ;
      (* For the in_type we have to check that all parents do export each
       * of the mentioned input fields: *)
      let f_name = E.id_of_path f.RamenFieldMaskLib.path in
      if parents = [] then
        Printf.sprintf2 "Cannot use input field %a without any parent"
          N.field_print_quoted f_name |>
        failwith ;
      if T.is_typed f.typ.structure then (
        !logger.debug "... already typed to %a" T.print_typ f.typ
      ) else (
        (* We already know (from the solver) that all parents export the
         * same type. Copy from the first parent: *)
        let parent = List.hd parents in
        !logger.debug "Copying from parent %a"
          N.func_print parent.F.name ;
        let pser =
          O.out_type_of_operation ~with_private:true parent.F.operation in
        match RamenFieldMaskLib.find_type_of_path pser f.path with
        | exception Not_found ->
            Printf.sprintf2 "Cannot find field %a in %s"
              N.field_print_quoted f_name
              (N.func_color parent.F.name) |>
            failwith
        | typ ->
            !logger.debug "Set input field %a.%a to %a"
              N.func_print func.F.name
              N.field_print f_name
              T.print_typ typ ;
            f.typ <- typ)
    ) func.in_type
  in
  (* Start by setting the output types so that it's then easy to copy
   * from there to the input types: *)
  Hashtbl.iter (fun _ -> set_output) funcs ;
  Hashtbl.iter (fun _ -> set_input) funcs

(* FIXME: get_types and apply_types should be a single function doing both *)
let apply_types parents condition funcs h =
  let apply _ _ e =
    match Hashtbl.find h e.E.uniq_num with
    | exception Not_found ->
        !logger.warning "No type for expression %a"
          (E.print true) e
    | typ -> e.E.typ <- typ in
  Option.may (E.iter (apply ())) condition ;
  Hashtbl.iter (fun _ func ->
    O.iter_expr apply func.F.operation
  ) funcs ;
  set_io_tuples parents funcs h
