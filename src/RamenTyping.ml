(* Typing a ramen program using a SMT solver.
 *
 * Principles:
 *
 * - We generate an optimisation problem where variables represent the types
 *   structure and nullability of every expressions of the AST, and stating the
 *   various relationships that must hold them together
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
 *   inductive data types"). The tuple datatype is tricky because of its
 *   unknown number of elements. After several unsuccessful attempts, we now
 *   define a specific tuple type for each used arity (this is probably the
 *   easiest for the solver too). Records are similar, with an additional
 *   constructor to the sort giving the field name.
 *
 * - Record field names are collected amongst all possible expressions and
 *   assigned an arbitrary unique number from 0 to N, and then encoded in the
 *   special "Field" sort, with possible values "field0" ... "fieldN". We can
 *   thus translate expression like "GET(some_field_name, some_record)"
 *   into the constraint that there must be a field number X (given by the
 *   actual field name) in the record "some_record", and the type of that
 *   field in that record should match the type of that GET expression.
 *
 * - In case the constraints are satisfiable we get the assignment (the
 *   solution might not be unique if an expression is unused or is literal
 *   NULL). If it's not we obtain the unsatisfiable core (by running the
 *   same problem once more with different parameters) and build an error
 *   message from it (in the future: we might interact with the solver to
 *   devise a better error message as in
 *   https://cs.nyu.edu/wies/publ/finding_minimum_type_error_sources.pdf)
 *   And if the solver times out, we use the best types so far and hope for
 *   the best.
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
open Stdint

open RamenHelpers
open RamenHelpersNoLog
open RamenLang
open RamenLog
open RamenSmt
open RamenTypingHelpers
module C = RamenConf
module VSI = RamenSync.Value.SourceInfo
module Err = RamenTypingErrors
module E = RamenExpr
module O = RamenOperation
module T = RamenTypes
module Globals = RamenGlobalVariables
module Retention = RamenRetention
module Variable = RamenVariable
open RamenTypes (* RamenTypes.Pub? *)

let t_of_num num =
  Printf.sprintf "t"^ Uint32.to_string num

let t_of_expr e =
  t_of_num e.E.uniq_num

let t_of_prefix pref i =
  Printf.sprintf "t_%s_%d" (Variable.to_string pref) i

let t_of_user_type name =
  let name' =
    String.map (fun c ->
      let c = Char.lowercase c in
      if c < 'a' || c > 'z' then '_' else c
    ) name in
  name' ^"-"^ scramble name

let user_type_of_t t =
  let _, scrambled = String.split ~by:"-" t in
  unscramble scrambled

let t_eth = t_of_user_type "Eth"
let t_ip = t_of_user_type "Ip"
let t_ip4 = t_of_user_type "Ip4"
let t_ip6 = t_of_user_type "Ip6"
let t_cidr = t_of_user_type "Cidr"
let t_cidr4 = t_of_user_type "Cidr4"
let t_cidr6 = t_of_user_type "Cidr6"

let n_of_num num =
  Printf.sprintf "n"^ Uint32.to_string num

let n_of_expr e =
  n_of_num e.E.uniq_num

let n_of_prefix pref i =
  Printf.sprintf "n_%s_%d" (Variable.to_string pref) i

let f_of_name field_names k =
  match Hashtbl.find field_names k with
  | exception Not_found ->
      Printf.sprintf2
        "Record field %a was forgotten from field_names (have only %a)?!"
        N.field_print k
        (pretty_enum_print N.field_print) (Hashtbl.keys field_names) |>
      failwith
  | i -> "field"^ string_of_int i

let rec find_type_of_path_in_typ typ path =
  let invalid_path () =
    Printf.sprintf2 "Cannot find subpath %a in type %a"
      E.print_path path
      DT.print_mn typ |>
    failwith in
  let open Raql_path_comp.DessserGen in
  match path with
  | [] -> typ
  | Name n :: rest ->
      (match typ.DT.typ with
      | TRec kts ->
          let _, t =
            Array.find (fun (k, _) -> k = (n :> string)) kts in
          find_type_of_path_in_typ t rest
      | _ ->
          invalid_path ())
  | Idx idx :: rest ->
      let idx = Uint32.to_int idx in
      (match typ.DT.typ with
      | TTup ts ->
          if idx >= Array.length ts then
            Printf.sprintf2 "Cannot cherry-pick index %d in a tuple with \
              only %d elements"
              idx (Array.length ts) |>
            failwith ;
          find_type_of_path_in_typ ts.(idx) rest
      | TVec (d, t) ->
          if idx >= d then
            Printf.sprintf2 "Cannot cherry-pick index %d in a vector of \
              length %d"
              idx d |>
            failwith ;
          find_type_of_path_in_typ t rest
      | TArr t ->
          find_type_of_path_in_typ t rest
      | _ ->
          invalid_path ())

let find_type_of_path_in_tuple_typ tuple_typ =
  let open Raql_path_comp.DessserGen in
  function
  | [] -> assert false
  | Name n :: rest ->
      let fld =
        List.find (fun fld ->
          fld.RamenTuple.name = n
        ) tuple_typ in
      find_type_of_path_in_typ fld.RamenTuple.typ rest
  | Idx _ :: _ ->
      failwith "Cannot address parent output with an index (yet)"

let rec find_expr_of_path e path =
  let invalid_path () =
    Printf.sprintf2 "Cannot find subpath %a in expression %a"
      E.print_path path
      (E.print ~max_depth:3 false) e |>
    failwith
  in
  let open Raql_path_comp.DessserGen in
  match path with
  | [] ->
      e
  | Name n :: rest ->
      (match e.E.text with
      | E.Record kvs ->
          let _, e = List.find (fun (k, _) -> k = n) kvs in
          find_expr_of_path e rest
      | _ ->
          invalid_path ())
  | Idx idx :: rest ->
      let idx = Uint32.to_int idx in
      (match e.E.text with
      | E.Vector es
      | E.Tuple es ->
          (match List.at es idx with
          | exception Invalid_argument _ ->
              invalid_path ()
          | e ->
              find_expr_of_path e rest)
      (* Useful mostly for empty lists but would work with any compound
       * constant literal, as long as the type-checker uses the given type of
       * that makeshift expression literally, since this expression is
       * otherwise undeclared: *)
      | E.Stateless (SL0 (Const T.(VTup vs | VVec vs | VArr vs))) ->
          (match vs.(idx) with
          | exception Invalid_argument _ ->
              invalid_path ()
          | v ->
              let e = E.make ~typ:T.(type_of_value v)
                             (E.Stateless (SL0 (Const v))) in
              find_expr_of_path e rest)
      | _ ->
          (* FIXME: Many functions returning vecs ot lists will
           * legitimately fall down here, which individual items will
           * therefore have no expression, although they have a smt2
           * expression (such as `(vector-type 123)`).
           * When no expression can be found we should return the last
           * found expression and the remaining path, and use a function
           * that outputs the smt2 expression giving the type of that. *)
          invalid_path ())

let find_expr_of_path_in_selected_fields fields =
  let open Raql_path_comp.DessserGen in
  let open Raql_select_field.DessserGen in
  function
  | [] -> assert false
  | Name n :: rest ->
      let sf = List.find (fun sf ->
        sf.alias = n) fields in
      find_expr_of_path sf.expr rest
  | Idx _ :: _ ->
      failwith "Cannot address input with an index (yet)"

let expr_err e err = Err.Expr (e.E.uniq_num, err)
let func_err fi err = Err.Func (fi, err)

let emit_eq id1 oc id2 =
  Printf.fprintf oc "(= %s %s)" id1 id2

let emit_is_tuple id oc sz =
  Printf.fprintf oc "((_ is tuple%d) %s)" sz id

let emit_is_record id oc sz =
  Printf.fprintf oc "((_ is record%d) %s)" sz id

let print_nullable what t id oc =
  Printf.fprintf oc
    (if t.DT.nullable then " (%s-nullable %s)"
                      else " (not (%s-nullable %s))")
    what id

(* Prints an integer as a 4 bits BitVec literal: *)
let print_width oc n =
  let digit_of n =
    if n = 0 then '0' else '1' in
  Printf.fprintf oc "#b%c%c%c%c"
    (digit_of (n land 0b1000))
    (digit_of (n land 0b0100))
    (digit_of (n land 0b0010))
    (digit_of (n land 0b0001))

let rec emit_id_eq_typ tuple_sizes records field_names id oc =
  let is_int signed bytes =
    Printf.fprintf oc "(= (int %b %a) %s)"
      signed print_width bytes
      id in
  function
  | DT.TUnknown -> Printf.fprintf oc "true"
  | TVoid -> Printf.fprintf oc "(= unit %s)" id
  | TString -> Printf.fprintf oc "(= string %s)" id
  | TBool -> Printf.fprintf oc "(= bool %s)" id
  | TChar -> Printf.fprintf oc "(= char %s)" id
  | TU8 -> is_int false 0
  | TU16 -> is_int false 1
  | TU24 -> is_int false 2
  | TU32 -> is_int false 3
  | TU40 -> is_int false 4
  | TU48 -> is_int false 5
  | TU56 -> is_int false 6
  | TU64 -> is_int false 7
  | TU128 -> is_int false 15
  | TI8 -> is_int true 0
  | TI16 -> is_int true 1
  | TI24 -> is_int true 2
  | TI32 -> is_int true 3
  | TI40 -> is_int true 4
  | TI48 -> is_int true 5
  | TI56 -> is_int true 6
  | TI64 -> is_int true 7
  | TI128 -> is_int true 15
  | TFloat -> Printf.fprintf oc "(= float %s)" id
  | TUsr { name ; _ } -> Printf.fprintf oc "(= %s %s)" (t_of_user_type name) id
  | TTup ts ->
      let d = Array.length ts in
      Printf.fprintf oc "(and " ;
      if d = 0 then
        Printf.fprintf oc "(xor %a)"
          (Set.Int.print ~first:"" ~last:"" ~sep:" " (emit_is_tuple id))
            tuple_sizes
      else
        emit_is_tuple id oc d ;
      (* Also emit what is known about the field types *)
      Array.iteri (fun i t ->
        emit_id_eq_typ tuple_sizes records field_names
          (Printf.sprintf "(tuple%d-e%d %s)" d i id) oc t.DT.typ ;
        emit_is_bool t.DT.nullable
          (Printf.sprintf "(tuple%d-n%d %s)" d i id) oc
      ) ts ;
      Printf.fprintf oc ")"
  | TRec ts ->
      let d = Array.length ts in
      Printf.fprintf oc "(and %a" (emit_is_record id) d ;
      (* Also emit what is known about the field types *)
      Array.iteri (fun i (k, t) ->
        let k = N.field k in
        emit_id_eq_typ tuple_sizes records field_names
          (Printf.sprintf "(record%d-e%d %s)" d i id) oc t.DT.typ ;
        emit_is_bool t.DT.nullable
          (Printf.sprintf "(record%d-n%d %s)" d i id) oc ;
        emit_eq (f_of_name field_names k) oc
          (Printf.sprintf "(record%d-f%d %s)" d i id)
      ) ts ;
      Printf.fprintf oc ")"
  | TVec (d, t) ->
      let id' = Printf.sprintf "(vector-type %s)" id in
      Printf.fprintf oc "(and ((_ is vector) %s) %a"
        id
        (emit_id_eq_typ tuple_sizes records field_names id') t.DT.typ ;
      (* FIXME: assert (d > 0) *)
      if d <> 0 then Printf.fprintf oc " (= %d (vector-dim %s))" d id ;
      print_nullable "vector" t id oc ;
      Printf.fprintf oc ")"
  | TArr t ->
      let id' = Printf.sprintf "(list-type %s)" id in
      Printf.fprintf oc "(and ((_ is list) %s) %a"
        id
        (emit_id_eq_typ tuple_sizes records field_names id') t.DT.typ ;
      print_nullable "list" t id oc ;
      Printf.fprintf oc ")"
  | TMap (k, v) ->
      let kid = Printf.sprintf "(map-key-type %s)" id in
      let vid = Printf.sprintf "(map-value-type %s)" id in
      Printf.fprintf oc "(and ((_ is map) %s) %a %a" id
        (emit_id_eq_typ tuple_sizes records field_names kid) k.DT.typ
        (emit_id_eq_typ tuple_sizes records field_names vid) v.DT.typ ;
      print_nullable "map-key" k id oc ;
      print_nullable "map-value" v id oc ;
      Printf.fprintf oc ")"
  | TSum _ ->
      todo "Type-checking with sum types"
  | TSet _ ->
      todo "Type-checking with set types"
  | _ ->
      invalid_arg "emit_id_eq_typ"

let emit_named ?name oc p =
  match name with
  | None ->
      p oc
  | Some err ->
      let s = Err.to_assert_name err in
      Printf.fprintf oc "(! %t :named %s)" p s

let emit_assert ?name oc p =
  Printf.fprintf oc "(assert %a)\n" (emit_named ?name) p

let emit_assert_true ?name oc id =
  emit_assert ?name oc (emit_is_true id)

let emit_assert_false ?name oc id =
  emit_assert ?name oc (emit_is_false id)

let emit_assert_id_is_bool ?name id oc b =
  (if b then emit_assert_true else emit_assert_false) ?name oc id

let emit_assert_id_eq_typ ?name tuple_sizes records field_names id oc t =
  emit_assert ?name oc (fun oc ->
    emit_id_eq_typ tuple_sizes records field_names id oc t)

let emit_assert_eq ?name id1 oc id2 =
  emit_assert ?name oc (fun oc -> emit_eq id1 oc id2)

let assert_imply ?name a oc b =
  emit_assert ?name oc (fun oc -> emit_imply a oc b)

let emit_let ?(var="tmp") oc v f =
  Printf.fprintf oc "(let ((%s %s)) %a)"
    var v
    f var

let emit_assert_let ?name ?var oc v f =
  emit_assert ?name oc (fun oc -> emit_let ?var oc v f)

(* Check that types are either the same (for those we cannot compare)
 * or that e1 is <= e2.
 * For IP/CIDR, it means version is either the same or generic.
 * FIXME: this is hardcoded for IPs and CIDRs, but should be generalized for
 * any sum type, be it user defined or not!
 * For integers, it means that width is <= and sign is also <=. *)
let emit_le id1 oc id2 =
  Printf.fprintf oc
    "(or (= %s %s) \
         (and ((_ is int) %s) \
              ((_ is int) %s) \
              (< (bv2nat (int-bytes %s)) \
                 (bv2nat (int-bytes %s))) \
              (or (int-signed %s) (not (int-signed %s)))) \
         (and (= float %s) \
              ((_ is int) %s) \
              (not (= (int-bytes %s) #b1111))) \
         (and (= %s %s) \
              (or (= %s %s) (= %s %s))) \
         (and (= %s %s) \
              (or (= %s %s) (= %s %s))))"
      id1 id2
      id2
        id1
        id1 id2
        id2 id1
      id2
        id1 id1
      t_ip id2
        t_ip4 id1 t_ip6 id1
      t_cidr id2
        t_cidr4 id1 t_cidr6 id1

let emit_assert_le ?name id1 oc id2 =
  emit_assert ?name oc (fun oc -> emit_le id1 oc id2)

(* id = the largest type from id1 and id2: *)
let emit_max id id1 oc id2 =
  Printf.fprintf oc
    "(ite %a (= %s %s) \
             (= %s %s))"
    (emit_le id1) id2
    id id2
    id id1

let emit_assert_max ?name id oc id1 id2 =
  emit_assert ?name oc (fun oc -> emit_max id id1 oc id2)

(* Named constraint used when an argument type is constrained: *)
let emit_assert_nullable oc e =
  let name = expr_err e (Err.Nullability true) in
  emit_assert_true ~name oc (n_of_expr e)

let emit_assert_not_nullable oc e =
  let name = expr_err e (Err.Nullability false) in
  emit_assert_false ~name oc (n_of_expr e)

let emit_assert_unsigned oc e =
  let name = expr_err e (Err.Unsigned None) in
  let id = t_of_expr e in
  emit_assert ~name oc (fun oc ->
    Printf.fprintf oc "(and ((_ is int) %s) (not (int-signed %s)))"
      id id)

let emit_assert_signed oc e =
  let name = expr_err e (Err.Signed None) in
  let id = t_of_expr e in
  emit_assert ~name oc (fun oc ->
    Printf.fprintf oc
      "(or (= float %s) \
           (and ((_ is int) %s) (int-signed %s)))"
      id
      id id)

let emit_small_unsigned oc id =
  Printf.fprintf oc "(is-small-unsigned %s)" id

let emit_integer oc id =
  Printf.fprintf oc "((_ is int) %s)" id

let emit_assert_integer oc e =
  let name = expr_err e (Err.Integer None) in
  emit_assert ~name oc (fun oc ->
    emit_integer oc (t_of_expr e))

let emit_numeric oc id =
  Printf.fprintf oc "(is-numeric %s)" id

let emit_assert_numeric oc e =
  let name = expr_err e Err.Numeric in
  emit_assert ~name oc (fun oc ->
    emit_numeric oc (t_of_expr e))

let emit_assert_ip oc e =
  let name = expr_err e Err.AnyIp in
  let id = t_of_expr e in
  emit_assert ~name oc (fun oc ->
    Printf.fprintf oc
      "(or (= %s %s) (= %s %s) (= %s %s))"
      t_ip4 id t_ip6 id t_ip id)

let emit_string oc =
  Printf.fprintf oc "(= string %s)"

let empty_tuple_sizes = Set.Int.empty
let empty_record_fields : (N.field, int * int * int) Hashtbl.t =
  Hashtbl.create 0
let empty_field_names : (N.field, int) Hashtbl.t = Hashtbl.create 0

let emit_has_type typ oc e =
  let name = expr_err e (Err.ActualType typ) in
  emit_assert ~name oc (fun oc ->
    emit_id_eq_typ empty_tuple_sizes empty_record_fields empty_field_names
                   (t_of_expr e) oc typ)

let emit_assert_float = emit_has_type TFloat
let emit_assert_bool = emit_has_type TBool
let emit_assert_char = emit_has_type TChar
let emit_assert_string = emit_has_type TString

(* "same" types are either actually the same or at least of the same sort
 * (both numbers/floats, both ip, or both cidr) *)
(* FIXME: generalize the IP/CIDR expression somehow... *)
let emit_same id1 oc id2 =
  Printf.fprintf oc
    "(or (= %s %s) \
         (ite (is-numeric %s) \
              (is-numeric %s) \
              (ite (or (= %s %s) (= %s %s) (= %s %s)) \
                   (or (= %s %s) (= %s %s) (= %s %s)) \
                   (ite (or (= %s %s) (= %s %s) (= %s %s)) \
                        (or (= %s %s) (= %s %s) (= %s %s)) \
                        false))))"
     id1 id2
     id1
     id2
     t_ip4 id1 t_ip6 id1 t_ip id1
     t_ip4 id2 t_ip6 id2 t_ip id2
     t_cidr4 id1 t_cidr6 id1 t_cidr id1
     t_cidr4 id2 t_cidr6 id2 t_cidr id2

let emit_assert_same e oc id1 id2 =
  let name = expr_err e Err.Same in
  emit_assert ~name oc (fun oc -> emit_same id1 oc id2)

(* Look through the stack for the record this path is referring to. *)
let locate_opened_record stack e path =
  !logger.info "Expr %a (path %a) supposed to be found in the environment"
    (E.print ~max_depth:2 false) e
    E.print_path path ;
  let open Raql_path_comp.DessserGen in
  match path with
  | Name n :: path' ->
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
      emit_assert_eq ~name (t_of_expr e) oc
        (t_of_expr path_target) ;
      emit_assert_eq ~name (n_of_expr e) oc
        (n_of_expr path_target)

let eq_to_opened_record stack e oc path =
  let rec_expr, _, _, _ = locate_opened_record stack e path in
  let name =
    expr_err e Err.(OpenedRecordIs rec_expr.E.uniq_num) in
  emit_assert_eq ~name (t_of_expr rec_expr) oc (t_of_expr e) ;
  emit_assert_eq ~name (n_of_expr rec_expr) oc (n_of_expr e)

let rec flexible_ints = function
  | [] -> true
  | E.{ text = (
          Vector _ |
          Stateless (SL2 ((BitAnd | BitOr | BitXor | BitShift), _, _))
        ) ; _ } :: _ -> false
  | _ :: rest -> flexible_ints rest

(* Assuming all input/output/constants have been declared already, emit the
 * constraints connecting the parameter to the result: *)
let emit_constraints tuple_sizes records field_names
                     in_type out_type param_type env_type global_type
                     ?func_name oc clause stack e =
  let eid = t_of_expr e and nid = n_of_expr e in
  (* Any signed or unsigned integer of at least that number of bytes: *)
  let int_const min_bytes =
    let name = expr_err e (Err.Integer (Some min_bytes)) in
    let op = if flexible_ints stack then ">=" else "=" in
    emit_assert ~name oc (fun oc ->
      Printf.fprintf oc
        "(and ((_ is int) %s)\
              (%s (bv2nat (int-bytes %s)) %d))"
        eid op eid min_bytes) ;
    emit_assert_not_nullable oc e in
  (* Any unsigned integer of at least that number of bytes: *)
  let uint_const min_bytes =
    let name = expr_err e (Err.Unsigned (Some min_bytes)) in
    let op = if flexible_ints stack then "=" else ">=" in
    emit_assert ~name oc (fun oc ->
      Printf.fprintf oc
        "(and ((_ is int) %s)\
              (not (int-signed %s))\
              (%s (bv2nat (int-bytes %s)) %d))"
        eid eid op eid min_bytes) ;
    emit_assert_not_nullable oc e in
  (* Any signed integer of at least that number of bytes: *)
  let sint_const min_bytes =
    let name = expr_err e (Err.Signed (Some min_bytes)) in
    let op = if flexible_ints stack then "=" else ">=" in
    emit_assert ~name oc (fun oc ->
      Printf.fprintf oc
        "(and ((_ is int) %s)\
              (int-signed %s)\
              (%s (bv2nat (int-bytes %s)) %d))"
        eid eid op eid min_bytes) ;
    emit_assert_not_nullable oc e in
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

  | Stateless (SL0 (Variable Record)) ->
      (* This expression must be equated to the actual record expression
       * that we could find in the stack, shall we know the field name...
       * So only Get can do that. *)
      ()

  | Stateless (SL0 (Variable pref)) ->
      let id, pref' =
        if Variable.has_type_input pref then
          option_get "Input record type must be defined" __LOC__ in_type,
          Variable.In
        else if Variable.has_type_output pref then
          option_get "Output record type must be defined" __LOC__ out_type,
          Out
        else if pref = Param then
          option_get "Params record type must be defined" __LOC__ param_type,
          pref
        else if pref = Env then
          option_get "Environment record type must be defined" __LOC__ env_type,
          pref
        else if pref = GlobalVar then
          option_get "Global variables record type must be defined" __LOC__ global_type,
          pref
        else
          Printf.sprintf "got a variable for %s?!" (Variable.to_string pref) |>
          failwith
        in
      emit_assert_eq eid oc (t_of_prefix pref' id) ;
      (* This one is always nullable, others never: *)
      let nullable = pref = OutPrevious in
      let name = expr_err e Err.(Nullability nullable) in
      emit_assert_id_is_bool ~name nid oc nullable

  | Stateless (SL0 (Const VNull)) ->
      (* - "NULL" is nullable. *)
      emit_assert_nullable oc e

  (* As a special case, constant are parsed as an integer that might not be
   * optimal ; let's allow the type checker to make them wider or signed: *)
  | Stateless (SL0 (Const (VU8 n))) ->
      (if Uint8.(compare n (of_string "127")) <= 0
       then int_const else uint_const) 0
  | Stateless (SL0 (Const (VU16 n))) ->
      (if Uint16.(compare n (of_string "32767")) <= 0
       then int_const else uint_const) 1
  | Stateless (SL0 (Const (VU24 n))) ->
      (if Uint24.(compare n (of_string "8388607")) <= 0
       then int_const else uint_const) 2
  | Stateless (SL0 (Const (VU32 n))) ->
      (if Uint32.(compare n (of_string "2147483647")) <= 0
       then int_const else uint_const) 3
  | Stateless (SL0 (Const (VU40 n))) ->
      (if Uint40.(compare n (of_string "549755813887")) <= 0
       then int_const else uint_const) 4
  | Stateless (SL0 (Const (VU48 n))) ->
      (if Uint48.(compare n (of_string "140737488355327")) <= 0
       then int_const else uint_const) 5
  | Stateless (SL0 (Const (VU56 n))) ->
      (if Uint56.(compare n (of_string "36028797018963967")) <= 0
       then int_const else uint_const) 6
  | Stateless (SL0 (Const (VU64 n))) ->
      (if Uint64.(compare n (of_string "9223372036854775807")) <= 0
       then int_const else uint_const) 7
  | Stateless (SL0 (Const (VI8 n))) ->
      (if Int8.(compare n zero) >= 0 then int_const else sint_const) 0
  | Stateless (SL0 (Const (VI16 n))) ->
      (if Int16.(compare n zero) >= 0 then int_const else sint_const) 1
  | Stateless (SL0 (Const (VI24 n))) ->
      (if Int24.(compare n zero) >= 0 then int_const else sint_const) 2
  | Stateless (SL0 (Const (VI32 n))) ->
      (if Int32.(compare n zero) >= 0 then int_const else sint_const) 3
  | Stateless (SL0 (Const (VI40 n))) ->
      (if Int40.(compare n zero) >= 0 then int_const else sint_const) 4
  | Stateless (SL0 (Const (VI48 n))) ->
      (if Int48.(compare n zero) >= 0 then int_const else sint_const) 5
  | Stateless (SL0 (Const (VI56 n))) ->
      (if Int56.(compare n zero) >= 0 then int_const else sint_const) 6
  | Stateless (SL0 (Const (VI64 n))) ->
      (if Int64.(compare n zero) >= 0 then int_const else sint_const) 7

  | Stateless (SL0 (Const x)) ->
      (* - A const cannot be null, unless it's VNull;
       * - The type is the type of the constant. *)
      emit_has_type T.(type_of_value x) oc e ;
      emit_assert_not_nullable oc e

  | Stateless (SL0 (Binding _)) ->
      assert false (* Not supposed to appear that soon *)

  | Tuple es ->
      (* - The resulting type is a tuple which length, items type and
       *   nullability are given by es;
       * - The result is not nullable since it has a literal value. *)
      let d = List.length es in
      emit_assert oc (fun oc ->
        Printf.fprintf oc "((_ is tuple%d) %s)" d eid) ;
      List.iteri (fun i e ->
        emit_assert_eq (t_of_expr e) oc
          (Printf.sprintf "(tuple%d-e%d %s)" d i eid) ;
        emit_assert_eq (n_of_expr e) oc
          (Printf.sprintf "(tuple%d-n%d %s)" d i eid)
      ) es ;
      emit_assert_false oc nid

  | Record kvs ->
      (* - The resulting type is a record which length, items type and
       *   nullability are given by the values in kvs;
       * - The result is not nullable since it has a literal value.
       * Note that for typing we type all expressions defined in [kvs]
       * even if they end up being shadowed. *)
      let d = List.length kvs in

      emit_assert oc (fun oc -> emit_is_record eid oc d) ;
      List.iteri (fun i (k, v) ->
        emit_assert_eq (t_of_expr v) oc
          (Printf.sprintf "(record%d-e%d %s)" d i eid) ;
        emit_assert_eq (n_of_expr v) oc
          (Printf.sprintf "(record%d-n%d %s)" d i eid) ;
        emit_assert_eq (f_of_name field_names k) oc
          (Printf.sprintf "(record%d-f%d %s)" d i eid)
      ) kvs ;
      emit_assert_false oc nid

  | Vector es ->
      (* Typing rules for short vectors:
       * - Elements in es must have a largest type;
       * - Every element in es must have the same nullability (FIXME:
       *   couldn't we "promote" non nullable to nullable?);
       * - The resulting type is that largest type of the elements;
       * - The resulting type is not nullable since it has a literal
       *   value.
       * Typing rules for long vectors:
       * - Elements must all have the same type;
       * - other rules as above
       * - FIXME: If the vector is of length 0, it can have any type *)
      (match es with
      | [] ->
        (* Empty vector literal are not accepted by the parser yet... *)
        emit_assert oc (fun oc ->
          Printf.fprintf oc "((_ is vector) %s)" eid)
      | fst :: _rest ->
        let d = List.length es in
        let is_short = d <= 4 in
        List.iter (fun x ->
          let name = expr_err x Err.VecSame in
          emit_assert ~name oc (fun oc ->
            Printf.fprintf oc "(and %a %a)"
              (emit_eq (n_of_expr x)) (n_of_expr fst)
              ((if is_short then emit_le else emit_eq) (t_of_expr x))
                (Printf.sprintf "(vector-type %s)" eid))
        ) es ;
        if is_short then
          (* Insist that the vector element type is not larger than each
           * elements, by equating it to at least one of them *)
          emit_assert oc (fun oc ->
            Printf.fprintf oc "(or%a)"
              (list_print
                (fun oc x ->
                  Printf.fprintf oc "(= (vector-type %s) %s)"
                    eid (t_of_expr x))) es) ;

        emit_assert oc (fun oc ->
          Printf.fprintf oc
            "(and ((_ is vector) %s) \
                  (= %d (vector-dim %s)) \
                  (= %s (vector-nullable %s)))"
            eid
            d eid
            (n_of_expr fst) eid)) ;

      emit_assert_false oc nid

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
        emit_assert_le ~name (t_of_expr cons) oc eid
      ) cases ;
      Option.may (fun else_ ->
        let name = expr_err e Err.CaseElse in
        emit_assert_le ~name (t_of_expr else_) oc eid
      ) else_ ;
      if cases <> [] then
        let name = expr_err e Err.CaseNullProp in
        emit_assert_let ~name oc
          (Printf.sprintf2 "(or%a %s)"
            (list_print (fun oc case ->
              Printf.fprintf oc "%s %s"
                (n_of_expr case.E.case_cond)
                (n_of_expr case.E.case_cons))) cases
            (match else_ with
            | None -> "true"
            | Some e -> n_of_expr e))
          (emit_eq nid)

  | Stateless (SL1s (Coalesce, es)) ->
      (* Typing rules:
       * - Every alternative must be of the same sort and the result must
       *   not be smaller;
       * - The result is as nullable as the last alternative;
       * - All elements of the list but the last must be nullable. *)
      let len = List.length es in
      List.iteri (fun i x ->
        let name = expr_err x (Err.CoalesceAlt i) in
        emit_assert_le ~name (t_of_expr x) oc eid ;
        let is_last = i = len - 1 in
        if is_last then
          emit_assert_eq (n_of_expr x) oc nid
        else
          let name = expr_err x (Err.CoalesceNull (i, len)) in
          emit_assert_id_is_bool ~name (n_of_expr x) oc true
      ) es ;

  | Stateless (SL0 (Now|Random|EventStart|EventStop|Pi)) ->
      (* - The result is a non nullable float *)
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TFloat ;
      emit_assert_not_nullable oc e

  | Stateless (SL1 (Defined, x)) ->
      (* - x must be nullable;
       * - The result is a non nullable bool. *)
      emit_assert_nullable oc x ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TBool ;
      emit_assert_not_nullable oc e

  | Stateful { operation =
      SF1 ((AggrSum|AggrMin|AggrMax|AggrFirst
           |AggrLast|AggrAnd|AggrOr
           |AggrBitAnd|AggrBitOr|AggrBitXor as aggr), x) ; _ } ->
      (* - if x is a list/vector then the result has the type of its elements;
       * - if x is a list/vector then the result is as nullable as its
       *   elements or the list itself;
       * - otherwise the result has the type and nullability of x. *)
      emit_assert oc (fun oc ->
        let xtid = t_of_expr x
        and xnid = n_of_expr x in
        Printf.fprintf oc
          "(ite ((_ is list) %s) \
                (and (= %s (list-type %s)) \
                     (= %s (or %s (list-nullable %s)))) \
                (ite ((_ is vector) %s) \
                     (and (= %s (vector-type %s)) \
                          (= %s (or %s (vector-nullable %s)))) \
                     (and (= %s %s) \
                          (= %s %s))))"
            xtid
              eid xtid
              nid xnid xtid
            xtid
              eid xtid
              nid xnid xtid
            eid xtid
            nid (n_of_expr x)) ;

      (match aggr with
      | AggrSum ->
        (* - The result is numeric *)
        emit_assert_numeric oc e
      | AggrBitAnd | AggrBitOr | AggrBitXor ->
        (* - The result is an integer *)
        emit_assert_integer oc e
      | AggrAnd | AggrOr ->
        (* - The result is a boolean *)
        emit_assert_id_eq_typ tuple_sizes records field_names eid oc TBool
      | AggrMin | AggrMax ->
        (* TODO: Check that the result is a comparable *)
        ()
      | _ -> ())

  | Stateful { skip_nulls = n ; operation = SF1 (AggrAvg, x) } ->
      (* - x must be numeric or a list/vector of numerics;
       * - The result is a float;
       * - The result is nullable if x or its elements are, or if skip_null. *)
      let name = expr_err x Err.Numeric_Or_Numerics in
      emit_assert ~name oc (fun oc ->
        let xid = t_of_expr x in
        Printf.fprintf oc
          "(xor (and ((_ is list) %s) \
                     %a \
                     %a) \
                (and ((_ is vector) %s) \
                     %a \
                     %a) \
                (and %a))"
          xid
            emit_numeric ("(list-type "^ xid ^")")
            (emit_imply ("(list-nullable "^ xid ^")")) nid
          xid
            emit_numeric ("(vector-type "^ xid ^")")
            (emit_imply ("(vector-nullable "^ xid ^")")) nid
          emit_numeric xid) ;

      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TFloat ;
      emit_assert oc (fun oc ->
        Printf.fprintf oc "(= %s (or %s %s))"
          nid (n_of_expr x) (if n then "true" else "false"))

  | Stateless (SL1 (Minus, x)) ->
      (* - The only argument must be numeric;
       * - The result must not be smaller than x;
       * - The result has same nullability than x;
       * - The result is signed or float. *)
      emit_assert_numeric oc x ;
      emit_assert_le (t_of_expr x) oc eid ;
      emit_assert_signed oc e ;
      emit_assert_eq (n_of_expr x) oc nid

  | Stateless (SL1 (Age, x)) ->
      (* - The only argument must be numeric;
       * - The result is a float;
       * - The result has same nullability than x. *)
      emit_assert_numeric oc x ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TFloat ;
      emit_assert_eq (n_of_expr x) oc nid

  | Stateless (SL1 (Abs, x)) ->
      (* - The only argument must be numeric;
       * - The result has same type than x;
       * - The result has same nullability than x. *)
      emit_assert_numeric oc x ;
      emit_assert_le (t_of_expr x) oc eid ;
      emit_assert_eq (n_of_expr x) oc nid

  | Stateless (SL1 (Not, x)) ->
      (* - The only argument must be boolean;
       * - The result type is a boolean;
       * - The resulting nullability depends solely on that of x. *)
      emit_assert_bool oc x ;
      emit_assert_eq nid oc (n_of_expr x) ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TBool

  | Stateless (SL1 (Cast t, x)) ->
      (* - The argument (x) can be anything;
       * - The result type is as described by the chosen type;
       * - The result is nullable if we specifically choose to cast
       *   toward a nullable type, or merely propagates.
       * Note that some cast are actually not implemented so would fail
       * at code generation. *)
      if t.DT.nullable then
        emit_assert_true oc nid
      else
        emit_assert_eq nid oc (n_of_expr x) ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc t.DT.typ

  | Stateless (SL1 (Force, x)) ->
      (* - The argument (x) can be anything, but must be nullable;
       * - The result has the same type as x but is not nullable. *)
      emit_assert_nullable oc x ;
      emit_assert_eq eid oc (t_of_expr x) ;
      emit_assert_false oc nid

  | Stateless (SL1 (Peek (mn, _endianess), x)) ->
      (* - The only argument (x) can be either a string, or a vector of
       *   unsigned integers;
       * - The result type is the given integer type typ (mandatory);
       * - Result is always nullable if the argument is a string, as the
       *   string length must match peeked width;
       * - In the case of the vector, the result is nullable iff the vector
       *   is nullable or its elements are. *)
      let name = expr_err x Err.PeekedType in
      let xid = t_of_expr x in
      emit_assert ~name oc (fun oc ->
        Printf.fprintf oc
          "(xor (= string %s) \
                (and ((_ is vector) %s) \
                     (is-unsigned (vector-type %s))))"
          xid xid xid) ;

      emit_assert_let oc
        (Printf.sprintf "(or (= string %s) %s (vector-nullable %s))"
          xid (n_of_expr x) xid)
        (emit_eq nid) ;

      emit_assert_id_eq_typ tuple_sizes records field_names eid oc mn.DT.typ

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
          "(xor (and ((_ is list) %s) \
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
            "(and ((_ is vector) %s) \
                  (not (vector-nullable %s)) \
                  %a)"
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
      ) else emit_assert_numeric oc e2 ;
      emit_assert_not_nullable oc e2

  | Stateless (SL2 ((Add|IDiv|Trunc), e1, e2)) ->
      (* - e1 and e2 must be numeric;
       * - the result is the max from e1 and e2;
       * - TODO: For Trunc, e2 must be greater than 0 even if float. *)
      emit_assert_numeric oc e1 ;
      emit_assert_numeric oc e2 ;
      emit_assert_let oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))
        (emit_eq nid) ;
      emit_assert_max eid oc (t_of_expr e1) (t_of_expr e2)
      (* TODO: for IDiv, have a TInt type and make_int_typ when parsing *)

  | Stateless (SL2 (Pow, e1, e2)) ->
      (* - e1 and e2 must be numeric;
       * - the result is the largest of e1 and e2;
       * - if e1 can be shown to be >= 0 or e2 can be shown to be an integer,
       *   then nullability propagates, otherwise the result is nullable. *)
      emit_assert_numeric oc e1 ;
      emit_assert_numeric oc e2 ;
      emit_assert_max eid oc (t_of_expr e1) (t_of_expr e2) ;
      let always_nullable =
        match E.float_of_const e1 with
        | Some f when f >= 0. -> false
        | _ ->
          (match E.float_of_const e2 with
          | Some f when Float.round f = f -> false
          | _ -> true) in
      if always_nullable then
        emit_assert_true oc nid
      else
        emit_assert_let oc
          (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))
          (emit_eq nid)

  | Stateless (SL2 (Sub, e1, e2)) ->
      (* - e1 and e2 must be numeric;
       * - the result is not smaller than e1 or e2;
       * - the result is signed even if both operands are unsigned:;
       * - nullability propagates; *)
      emit_assert_numeric oc e1 ;
      emit_assert_numeric oc e2 ;
      emit_assert_let oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))
        (emit_eq nid) ;
      emit_assert_le (t_of_expr e1) oc eid ;
      emit_assert_le (t_of_expr e2) oc eid ;
      emit_assert_signed oc e

  | Stateless (SL2 (Mul, e1, e2)) ->
      (* - either both e1 and e2 are numeric, and then the result is the max
       *   of e1 and e2;
       * - or e1 is an integer and e2 is a string, or the other way around,
       *   and the result is a string.
       * - in either case nullability propagates from e1 or e2 to the result.
       *)
      let name = expr_err e Err.MulType in
      let t1 = t_of_expr e1 and t2 = t_of_expr e2 in
      emit_assert ~name oc (fun oc ->
        Printf.fprintf oc
          "(xor (and %a %a %a) \
                (and %a %a %a) \
                (and %a %a %a))"
            (* e1 and e2 are numeric and the result is the largest: *)
            emit_numeric t1
            emit_numeric t2
            (emit_max eid t1) t2
            (* ... or e1 is an int and e2 and the result are strings: *)
            emit_small_unsigned t1
            emit_string t2
            emit_string eid
            (* ... or the other way around: *)
            emit_string t1
            emit_small_unsigned t2
            emit_string eid) ;

      emit_assert_let oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))
        (emit_eq nid)

  | Stateless (SL2 (Reldiff, e1, e2)) ->
      (* - e1 and e2 must be numeric;
       * - The result is a float;
       * - nullability propagates. *)
      emit_assert_numeric oc e1 ;
      emit_assert_numeric oc e2 ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TFloat ;
      emit_assert_let oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))
        (emit_eq nid)

  | Stateless (SL2 (Div, e1, e2)) ->
      (* - e1 and e2 must be numeric;
       * - The result is a float;
       * - If either e1 or e2 can be shown to be not 0 then nullability
       *   propagates, otherwise the result is nullable.
       * Note that a/0 is allowed to be +/-Inf as long as a<>0, following the
       * IEEE. *)
      emit_assert_numeric oc e1 ;
      emit_assert_numeric oc e2 ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TFloat ;
      let always_nullable =
        match E.float_of_const e1 with
        | Some f when f <> 0. -> false
        | _ ->
          (match E.float_of_const e2 with
          | Some f when f <> 0. -> false
          | _ -> true) in
      if always_nullable then
        emit_assert_true oc nid
      else
        emit_assert_let oc
          (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))
          (emit_eq nid)

  | Stateless (SL2 ((StartsWith|EndsWith), e1, e2)) ->
      (* - e1 and e2 must be strings;
       * - The result is a bool;
       * - Result nullability propagates from arguments. *)
      emit_assert_string oc e1 ;
      emit_assert_string oc e2 ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TBool ;
      emit_assert_let oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))
        (emit_eq nid)

  | Stateless (SL2 (Concat, e1, e2))
  | Generator (Split (e1, e2)) ->
      (* - e1 and e2 must be strings;
       * - The result is also a string;
       * - Result nullability propagates from arguments. *)
      emit_assert_string oc e1 ;
      emit_assert_string oc e2 ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TString ;
      emit_assert_let oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))
        (emit_eq nid)

  | Stateless (SL2 (Strftime, e1, e2)) ->
      (* - e1 must be a string and e2 a float (ideally, a time);
       * - Then result will be a string;
       * - Its nullability propagates from arguments. *)
      emit_assert_string oc e1 ;
      emit_assert_float oc e2 ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TString ;
      emit_assert_let oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))
        (emit_eq nid)

  | Stateless (SL1 (Length, x)) ->
      (* - x must be a string or a list;
       * - The result type is an U32;
       * - The result nullability is the same as that of x. *)
      let name = expr_err e Err.LengthType in
      let xid = t_of_expr x in
      emit_assert ~name oc (fun oc ->
        Printf.fprintf oc "(or (= string %s) ((_ is list) %s))" xid xid) ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TU32 ;
      emit_assert_eq nid oc (n_of_expr x)

  | Stateless (SL1 ((Lower|Upper), x)) ->
      (* - x must be a string;
       * - The result type is also a string;
       * - The result nullability is the same as that of x. *)
      emit_assert_string oc x ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TString ;
      emit_assert_eq nid oc (n_of_expr x)

  | Stateless (SL1 (UuidOfU128, x)) ->
      (* - x must be a uint128
       * - The result type is a string;
       * - The result nullability is the same as that of x. *)
      let id = t_of_expr x in
      emit_assert oc (fun oc -> Printf.fprintf oc "(is-u128 %s)" id) ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TString ;
      emit_assert_eq nid oc (n_of_expr x)

  | Stateless (SL1 (Like _, x)) ->
      (* - x must be a string;
       * - The result is a bool;
       * - Nullability propagates from x to e. *)
      emit_assert_string oc x ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TBool ;
      emit_assert_eq nid oc (n_of_expr x)

  | Stateless (SL1 (Strptime, x)) ->
      (* - x must be a string;
       * - The Result is a float (a time);
       * - The result is always nullable (known from the parse). *)
      emit_assert_string oc x ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TFloat ;
      emit_assert_true oc nid

  | Stateless (SL1 (Chr, x)) ->
      (* - x must be an unsigned integer;
       * - Nullability propagates. *)

      emit_assert_unsigned oc x ;
      emit_assert_char oc e ;
      emit_assert_eq nid oc (n_of_expr x)

  | Stateless (SL1 (Variant, x)) ->
      (* - x must be a string (the experiment name);
       * - The Result is a string (the name of the variant);
       * - The result is always nullable (if the experiment is not defined). *)
      emit_assert_string oc x ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TString ;
      emit_assert_true oc nid

  | Stateless (SL2 (Mod, e1, e2)) ->
      (* - e1 and e2 must be any numeric;
       * - The result must not be smaller that e1 nor e2;
       * - If both e1 and e2 are integers then the result is also an integer,
       *   otherwise it is a float;
       * - Nullability propagates. *)
      emit_assert_numeric oc e1 ;
      emit_assert_numeric oc e2 ;
      let id1 = t_of_expr e1
      and id2 = t_of_expr e2 in
      emit_assert oc (fun oc ->
        Printf.fprintf oc
          "(or (and ((_ is int) %s) \
                    ((_ is int) %s) \
                    ((_ is int) %s)) \
               (= float %s))"
          id1 id2 eid eid) ;
      emit_assert_le id1 oc eid ;
      emit_assert_le id2 oc eid ;
      emit_assert_let oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))
        (emit_eq nid)

  | Stateless (SL2 ((BitAnd|BitOr|BitXor|BitShift), e1, e2)) ->
      (* - e1 and e2 must be integers;
       * - The result is the largest of e1 and e2;
       * - Nullability propagates. *)
      emit_assert_integer oc e1 ;
      emit_assert_integer oc e2 ;
      emit_assert_integer oc e ;
      emit_assert_max eid oc (t_of_expr e1) (t_of_expr e2) ;
      emit_assert_let oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))
        (emit_eq nid)

  | Stateless (SL2 ((Ge|Gt), e1, e2)) ->
      (* - e1 and e2 must have the same sort;
       * - The result is a bool;
       * - Nullability propagates. *)
      emit_assert_same e oc (t_of_expr e1) (t_of_expr e2) ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TBool ;
      emit_assert_let oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))
        (emit_eq nid)

  | Stateless (SL2 (Eq, e1, e2)) ->
      (* - e1 and e2 must be of the same sort;
       * - The result is a bool;
       * - Nullability propagates from arguments. *)
      emit_assert_same e oc (t_of_expr e1) (t_of_expr e2) ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TBool ;
      emit_assert_let oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))
        (emit_eq nid)

  | Stateless (SL2 ((And|Or), e1, e2)) ->
      (* - e1 and e2 must be booleans;
       * - The result is also a boolean;
       * - Nullability propagates. *)
      emit_assert_bool oc e1 ;
      emit_assert_bool oc e2 ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TBool ;
      emit_assert_let oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))
        (emit_eq nid)

  | Stateless (SL2 (Get, n, x)) ->
      (* Typing rules:
       * - either (integer indexed vector/list/tuple access):
       *   - x must be a vector, a list, a tuple;
       *   - and n must be an integer (unsigned-ness is not enforced for
       *     convenience since it cannot be guaranteed that the index is within
       *     bounds anyway) ;
       *   - if x is a vector and n is a constant, then n must be less than
       *     its length;
       *   - if x is a tuple then n must be a constant less than the tuple
       *     length, and the resulting type is that of that element;
       *   - otherwise, the resulting type is the same as the list/vector
       *     element type;
       *   - if x is a vector and n a constant, or if x is a tuple, then the
       *     result has the same nullability than x or x's elements; in all
       *     other cases, the result is nullable.
       * - or (name indexed record access):
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
       * - or (map access):
       *   - x must be a map;
       *   - n must have the same type as the type of the map keys;
       *   - the result has the same type as the map values;
       *   - the result is always nullable (returns Null if key is unbound) *)
      (* Note regarding maps: In theory, it should be possible to have a glance
       * at x text and if it's the `Variable Globals` then assume that it's a
       * map and assume it's not otherwise.
       * But for now let's try to be as generic as possible in hope we will
       * have generalized map values at some point. *)
      (* The conditions in case x is a map: *)
      let emit_get_from_map oc mid =
        Printf.fprintf oc
          "(and ((_ is map) %s) \
                (= (map-key-type %s) %s) \
                (or (map-key-nullable %s) (not %s)) \
                (= (map-value-type %s) %s) \
                %s)"
          mid
          mid (t_of_expr n)
          mid (n_of_expr n)
          mid eid
          nid in
      (match E.int_of_const n with
      | Some i ->
          let name = expr_err x Err.GettableByInt in
          emit_assert_numeric oc n ;
          emit_assert ~name oc (fun oc ->
            (* Vector/list case: *)
            Printf.fprintf oc
              "(let ((tmp %s)) \
                 (xor (and ((_ is vector) tmp) \
                           (> (vector-dim tmp) %d) \
                           (= (vector-type tmp) %s) \
                           (= (or %s (vector-nullable tmp)) %s)) \
                      (and ((_ is list) tmp) \
                           (= (list-type tmp) %s) \
                           %s)"
              (t_of_expr x)
                (* vector: *)
                i
                eid
                (n_of_expr x) nid
                (* list: *)
                eid
                nid ;
            (* Tuple case: *)
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
            (* Integer indexed map case: *)
            Printf.fprintf oc
              "(and ((_ is map) tmp) \
                    (is-numeric (map-key-type tmp)) \
                    (= (map-value-type tmp) %s)
                    %s)"
              eid nid ;
            Printf.fprintf oc "))")
      | None ->
          (match E.string_of_const n with
          | None ->
              (* Map access or, assuming n is numeric, vector or list access: *)
              (* FIXME: map-key-type and values should be not less than n/e,
               * as opposed to strictly equals *)
              let name = expr_err x Err.GettableByInt in
              emit_assert ~name oc (fun oc ->
                Printf.fprintf oc
                  "(let ((tmp %s)) \
                     (xor (and ((_ is vector) tmp) \
                               (= (vector-type tmp) %s) \
                               (is-numeric %s) \
                               %s) \
                          (and ((_ is list) tmp) \
                               (= (list-type tmp) %s) \
                               (is-numeric %s) \
                               %s) \
                          %a))"
                  (t_of_expr x)
                  eid
                  (t_of_expr n)
                  nid
                  eid
                  (t_of_expr n)
                  nid
                  emit_get_from_map "tmp")
          | Some i ->
              emit_assert_string oc n ;
              let name = expr_err x Err.GettableByName in
              let k = N.field i in
              let rec_lst = Hashtbl.find_all records k in
              if rec_lst = [] then (
                emit_comment oc "Get from a string keyed map (as there are \
                                 no records with that key" ;
                emit_assert ~name oc (fun oc ->
                  emit_get_from_map oc (t_of_expr x))
              ) else (
                (* Of course, even if there happen to be a record with a
                 * field named [n], there could also be a map of with a
                 * key of that name, so [x] can still be a map! *)
                let had_several = list_longer_than 1 rec_lst in
                if had_several then
                  emit_comment oc "Field %a is present in %d records!"
                    N.field_print k
                    (List.length rec_lst) ;
                emit_assert ~name oc (fun oc ->
                  Printf.fprintf oc
                    (if not had_several then "%a" else "(or%a)")
                    (list_print
                      (fun oc (name_idx, rec_size, field_pos) ->
                        Printf.fprintf oc
                          "(xor (and ((_ is record%d) %s) \
                                     (= (record%d-e%d %s) %s) \
                                     (= (or %s (record%d-n%d %s)) %s) \
                                     (= (record%d-f%d %s) field%d)) \
                                %a)"
                          rec_size (t_of_expr x)
                          rec_size field_pos (t_of_expr x) eid
                          (n_of_expr x)
                          rec_size field_pos (t_of_expr x) nid
                          rec_size field_pos (t_of_expr x) name_idx
                          emit_get_from_map (t_of_expr x)
                      )) rec_lst) ;
                (* Note: the above assertions merely equals the n-th field
                 * to [x]. In case [x] is a Record variable then we won't tell
                 * what record expression this variable refers to. To do this
                 * requires the knowledge of the field name, ie [k], so it's a
                 * good place to do this though. Let's therefore do it here: *)
                if x.text = Stateless (SL0 (Variable Record)) then
                  eq_to_opened_record stack x oc [ Name k ])))

  | Stateless (SL1 ((BeginOfRange|EndOfRange), x)) ->
      (* - x is any kind of cidr;
       * - The result is a bool;
       * - Nullability propagates from x. *)
      let name = expr_err x Err.AnyCidr in
      let xid = t_of_expr x in
      emit_assert ~name oc (fun oc ->
        Printf.fprintf oc
          "(xor (and (= %s %s) (= %s %s)) \
                (and (= %s %s) (= %s %s)) \
                (and (= %s %s) (= %s %s)))"
          t_cidr xid t_ip eid
          t_cidr4 xid t_ip4 eid
          t_cidr6 xid t_ip6 eid) ;
      emit_assert_eq nid oc (n_of_expr x)

  | Stateless (SL1s ((Min|Max), es)) ->
      (* Typing rules:
       * - es must be a list of expressions of compatible types;
       * - the result type is the largest of them all (FIXME: here we just
       *   specify that it is largest than all of them);
       * - If any of the es is nullable then so is the result. *)
      List.iter (fun e ->
        emit_assert_le (t_of_expr e) oc eid
      ) es ;
      if es <> [] then
        emit_assert_let oc
          (Printf.sprintf2 "(or%a)"
            (list_print (fun oc e ->
              String.print oc (n_of_expr e))) es)
        (emit_eq nid)

  | Stateless (SL1s (Print, es)) ->
      let lst = List.last es in
      (* The result must have the same type as the last parameter *)
      emit_assert_eq (t_of_expr lst) oc eid ;
      emit_assert_eq (n_of_expr lst) oc nid

  | Stateless (SL2 (Index, s, c)) ->
      (* - s must be a string;
         - c must be a char;
         - the result is an integer, always nullable;
         - nullability propagates (returns -1 if the char is not found) *)
      emit_assert_string oc s ;
      emit_assert_char oc c ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TI32 ;
      emit_assert_let oc
        (Printf.sprintf2 "(or %s %s)"
          (n_of_expr s) (n_of_expr c))
        (emit_eq nid)

  | Stateless (SL3 (SubString, s, a, b)) ->
      (* - s must be a string;
       * - a and b must be integers (may be negative);
       * - the result is a string;
       * - nullability comes solely from the arguments - in particular, if
       *   a and b does not delimit a valid substring the result will be a
       *   non null empty string. *)
      emit_assert_string oc s ;
      emit_assert_integer oc a ;
      emit_assert_integer oc b ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TString ;
      emit_assert_let oc
        (Printf.sprintf2 "(or %s %s %s)"
          (n_of_expr s) (n_of_expr a) (n_of_expr b))
        (emit_eq nid)

  | Stateless (SL3 (MapSet, m, k, v)) ->
      (* - m must be a map,
       * - its key type is at least as large as the type of k;
       * - its value type must be at least as large as the type of v;
       * - if the map key is not nullable then k must not be nullable;
       * - if the map value is not nullable then v must not be nullable;
       * - the result type is that of the map value type;
       * - if either the map or the value is nullable then the result is
       *   also nullable. *)
      let name = expr_err e Err.MapType in
      emit_assert ~name oc (fun oc ->
        Printf.fprintf oc "((_ is map) %s)" (t_of_expr m)) ;
      let kid = Printf.sprintf "(map-key-type %s)" (t_of_expr m) in
      emit_assert_le (t_of_expr k) oc kid ;
      let vid = Printf.sprintf "(map-value-type %s)" (t_of_expr m) in
      emit_assert_le (t_of_expr v) oc vid ;
      let name = expr_err e Err.MapNullability in
      emit_assert ~name oc (fun oc ->
        Printf.fprintf oc
          "(and (or (map-key-nullable %s) (not %s))\n\
                (or (map-value-nullable %s) (not %s)))"
          (t_of_expr m) (n_of_expr k)
          (t_of_expr m) (n_of_expr v)) ;
      emit_assert_eq (t_of_expr v) oc eid ;
      emit_assert oc (fun oc ->
        Printf.fprintf oc "(= %s (or %s %s))"
          nid (n_of_expr m) (n_of_expr v))

  | Stateful { operation = SF2 (Lag, e1, e2) ; _ } ->
      (* Typing rules:
       * - e1 must be an unsigned;
       * - e2 has same type as the result;
       * - The result is always nullable, as NULL is returned if the value is
       *   finalized before [e1] values have been added. *)
      emit_assert_integer oc e1 ;
      emit_assert_eq (t_of_expr e2) oc eid ;
      emit_assert_true oc nid

  | Stateful { operation = SF3 (MovingAvg, e1, e2, e3) ; _ } ->
      (* Typing rules:
       * - e1 must be an unsigned (the period);
       * - e2 must also be an unsigned (the number of values to average);
       * - Neither e1 or e2 can be NULL;
       * - e3 must be numeric;
       * - The result type is float;
       * - The result nullability propagates from e3 (and its elements). *)
      emit_assert_unsigned oc e1 ;
      emit_assert_unsigned oc e2 ;
      emit_assert_not_nullable oc e1 ;
      emit_assert_not_nullable oc e2 ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TFloat ;
      emit_assert_numeric oc e3 ;
      assert_imply (n_of_expr e3) oc nid

  | Stateful { operation = SF4 (DampedHolt, e1, e2, e3, e4) ; _ } ->
      (* Typing rules:
       * - Parameters are not nullable
       * - e1 must be a float;
       * - e2 must be a float;
       * - e3 must be a float;
       * - e4 must be a numeric;
       * - The result is float and nullability propagates. *)
      emit_assert_float oc e1 ;
      emit_assert_not_nullable oc e1 ;
      emit_assert_float oc e2 ;
      emit_assert_not_nullable oc e2 ;
      emit_assert_float oc e3 ;
      emit_assert_not_nullable oc e3 ;
      emit_assert_numeric oc e4 ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TFloat ;
      emit_assert_let oc
        (Printf.sprintf2 "(or %s %s %s %s)"
          (n_of_expr e1) (n_of_expr e2) (n_of_expr e3) (n_of_expr e4))
        (emit_eq nid)

  | Stateful { operation = SF6 (DampedHoltWinter, e1, e2, e3, e4, e5, e6) ; _ } ->
      (* Typing rules:
       * - Parameters are not nullable
       * - e1 must be a float;
       * - e2 must be a float;
       * - e3 must be a float;
       * - e4 must be an unsigned ;
       * - e5 must be a float;
       * - e6 must be a numeric;
       * - The result is float and nullability propagates. *)
      emit_assert_float oc e1 ;
      emit_assert_not_nullable oc e1 ;
      emit_assert_float oc e2 ;
      emit_assert_not_nullable oc e2 ;
      emit_assert_float oc e3 ;
      emit_assert_not_nullable oc e3 ;
      emit_assert_float oc e3 ;
      emit_assert_unsigned oc e4 ;
      emit_assert_not_nullable oc e4 ;
      emit_assert_float oc e5 ;
      emit_assert_not_nullable oc e5 ;
      emit_assert_numeric oc e6 ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TFloat ;
      emit_assert_let oc
        (Printf.sprintf2 "(or %s %s %s %s %s %s)"
          (n_of_expr e1) (n_of_expr e2) (n_of_expr e3)
          (n_of_expr e4) (n_of_expr e5) (n_of_expr e6))
        (emit_eq nid)

  | Stateful { operation = SF4s (MultiLinReg, e1, e2, e3, e4s) ; _ } ->
      (* As above, with the addition of predictors that must also be
       * numeric and non null. Why non null? See comment in check_variadic
       * and probably get rid of this limitation. *)
      emit_assert_unsigned oc e1 ;
      emit_assert_unsigned oc e2 ;
      emit_assert_numeric oc e3 ;
      emit_assert_not_nullable oc e1 ;
      emit_assert_not_nullable oc e2 ;
      emit_assert_eq (n_of_expr e3) oc nid ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TFloat ;
      List.iter (fun e ->
        emit_assert_numeric oc e ;
        emit_assert_not_nullable oc e ;
      ) e4s

  | Stateful { operation = SF2 (ExpSmooth, e1, e2) ; _ } ->
      (* Typing rules:
       * - e1 must be a non-null float (and ideally, between 0 and 1), but
       *   we just ask for a numeric in order to also accept immediate
       *   values parsed as integers, and integer fields;
       * - e2 must be numeric;
       * - result is as nullable as e2 *)
      emit_assert_float oc e1 ;
      emit_assert_numeric oc e2 ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TFloat ;
      emit_assert_not_nullable oc e1 ;
      emit_assert_eq (n_of_expr e2) oc nid

  | Stateless (SL1 ((Exp|Log|Log10
                    |Cos|Sin|Tan|ACos|ASin|ATan|CosH|SinH|TanH), x)) ->
      (* - x must be numeric;
       * - The result is a float;
       * - The result nullability is inherited from arguments *)
      emit_assert_numeric oc x ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TFloat ;
      emit_assert_eq (n_of_expr x) oc nid

  | Stateless (SL1 (Sqrt, x)) ->
      (* - x must be numeric;
       * - The result is a float;
       * - If x is known to be positive then nullability of the result is that of
       *   x, otherwise the result is nullable *)
      emit_assert_numeric oc x ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TFloat ;
      let always_nullable =
        match E.float_of_const x with
        | Some f when f >= 0. -> false
        | _ -> true in
      if always_nullable then
        emit_assert_true oc nid
      else
        emit_assert_eq (n_of_expr x) oc nid

  | Stateless (SL1 (Sq, x)) ->
      (* - e1 and e2 must be numeric
       * - the result is the same as x;
       *   (TODO: experiment with doubling its integer width?);
       * - nullability propagates from e1 to the result.
       *)
      emit_assert_numeric oc x ;
      emit_assert_eq (t_of_expr x) oc eid ;
      emit_assert_eq (n_of_expr x) oc nid

  | Stateless (SL1 ((Floor|Ceil|Round), x)) ->
      (* - x must be numeric;
       * - The result is the same as x;
       * - Nullability propagates from argument. *)
      emit_assert_numeric oc x ;
      emit_assert_eq (t_of_expr x) oc eid ;
      emit_assert_eq (n_of_expr x) oc nid

  | Stateless (SL1 (Hash, x)) ->
      (* - x can be anything. Notice that hash(NULL) is NULL;
       * - The result is an I64.
       * - Nullability propagates. *)
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TI64 ;
      emit_assert_eq (n_of_expr x) oc nid

  | Stateless (SL1 (Sparkline, x)) ->
      (* - x must be a vector of non-null numerics;
       * - The result is a string;
       * - The result nullability itself propagates from x. *)
      let name = expr_err x Err.NumericVec in
      let xid = t_of_expr x in
      emit_assert ~name oc (fun oc ->
        Printf.fprintf oc "(and ((_ is vector) %s) \
                                (is-numeric (vector-type %s)) \
                                (not (vector-nullable %s)))"
          xid
          xid
          xid) ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TString ;
      emit_assert_eq (n_of_expr x) oc nid

  | Stateful { operation = SF4 (Remember _, fpr, tim, dur, e) ; _ } ->
      (* Typing rules:
       * - fpr must be a non null (positive) float, so we take any numeric
       *   for now;
       * - time must be a time, so ideally a float, but again we accept any
       *   integer (so that a int field is OK);
       * - dur must be a non nullable duration (that's the duration of the
       *   whole bloom filter), so a numeric again;
       * - Expression e can be anything at all;
       * - The result is a boolean;
       * - The result is as nullable as any of tim, dur and e. *)
      emit_assert_numeric oc fpr ;
      emit_assert_numeric oc tim ;
      emit_assert_numeric oc dur ;
      emit_assert_not_nullable oc fpr ;
      emit_assert_not_nullable oc dur ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TBool ;
      emit_assert_let oc
        (Printf.sprintf2 "(or %s %s)" (n_of_expr tim) (n_of_expr e))
        (emit_eq nid)

  | Stateful { operation = SF1 (Distinct, e) ; _ } ->
      (* - e can be anything;
       * - The result is a boolean;
       * - The result is nullable if e is nullable. *)
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TBool ;
      emit_assert_eq nid oc (n_of_expr e)

  | Stateful { operation = SF3 (Hysteresis, meas, accept, max) ; _ } ->
      (* - meas, accept and max must be numeric;
       * - The result is a boolean;
       * - The result is nullable if and only if any of meas, accept and
       *   max is. *)
      emit_assert_numeric oc meas ;
      emit_assert_numeric oc accept ;
      emit_assert_numeric oc max ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TBool ;
      emit_assert_let oc
        (Printf.sprintf "(or %s %s %s)"
          (n_of_expr meas) (n_of_expr accept) (n_of_expr max))
        (emit_eq nid)

  | Stateful { skip_nulls = n ; operation =
        Top { output ; size ; max_size ; top_what ; by ; duration ;
              top_time ; sigmas } ; _ } ->
      (* Typing rules:
       * - size must be numeric and not null;
       * - max_size, if set, must be numeric and not null ;
       * - what can be anything;
       * - by must be numeric;
       * - duration must be a numeric and non null;
       * - time must be a time (numeric) and non null;
       * - sigmas must be a numeric (positive, but we will take the absolute
       *   value), and non null (0 means no sigma filtering);
       * - If we want the rank then the result type is an unsigned,
       *   if we want the membership then it is a bool (known at parsing time)
       *   and if we want the list then it is a list of at most size items of
       *   the same type as what ;
       * - If we want the rank then the result is nullable (known at
       *   parsing time), otherwise nullability is inherited from what
       *   and by, with the special case when we want a list and skip nulls,
       *   then when all values are skipped instead of returning null the top
       *   merely returns an empty list. *)
      emit_assert_numeric oc size ;
      emit_assert_false oc (n_of_expr size) ;
      Option.may (fun s ->
        emit_assert_numeric oc s ;
        emit_assert_false oc (n_of_expr s)
      ) max_size ;
      emit_assert_numeric oc by ;
      emit_assert_numeric oc duration ;
      emit_assert_false oc (n_of_expr duration) ;
      emit_assert_numeric oc top_time ;
      emit_assert_false oc (n_of_expr top_time) ;
      emit_assert_numeric oc sigmas ;
      emit_assert_false oc (n_of_expr sigmas) ;
      (* Given the output result of TOP is complex and error prone, depart
       * from the rule that only parameter types must be named and no
       * output types: *)
      let name = expr_err e (Err.TopOutput output) in
      (match output with
      | Rank ->
          emit_assert ~name oc (fun oc ->
            Printf.fprintf oc "(is-unsigned %s)" eid)
      | Membership ->
          emit_assert_id_eq_typ ~name tuple_sizes records field_names eid oc
                                TBool
      | List ->
          emit_assert ~name oc (fun oc ->
            Printf.fprintf oc "(and ((_ is list) %s) \
                                    (= (list-type %s) %s) \
                                    %s)"
              eid
              eid (t_of_expr top_what)
              (if n then "(not (list-nullable "^ eid ^"))"
                    else "(= (list-nullable "^ eid ^") "^ n_of_expr top_what))) ;
      (match output with
      | Rank ->
          emit_assert_nullable oc e
      | List when n ->
          (* When all values are skipped, return an empty list rather than
           * null: *)
          emit_assert_not_nullable oc e
      | Membership | List ->
          emit_assert_let oc
            (Printf.sprintf2 "(or %s %s)"
              (n_of_expr top_what)
              (n_of_expr by))
            (emit_eq nid))

  | Stateful { skip_nulls = n ; operation = SF4s (Largest _, c, but, x, es) } ->
      (* - c must be a constant (ensured by parser) strictly (TODO) positive
       *   integer;
       * - b must be a constant (also ensured by parser) and non-negative;
       * - The type of the result is a list of items of the same type than x;
       * - the size of that list depends on up_to but it is irrelevant to
       *   typing;
       * - If we skip nulls then those items are not nullable, otherwise
       *   they are as nullable as x;
       * - In theory, 'Largest c e1 by es` should itself be nullable if c or
       *   b is nullable or any of the es is nullable. And then become and stay
       *   null forever as soon as one es is actually NULL. This is kind
       *   of useless, so we just disallow sizing with and ordering by a
       *   nullable value;
       * - The Largest itself is null whenever the number of received item is
       *   less than c (if not up_to) or less than 1 (if up_to, as we still does
       *   not allow empty lists), and so is always nullable. *)
      emit_assert_unsigned oc c ;
      emit_assert_not_nullable oc c ;
      emit_assert_unsigned oc but ;
      emit_assert_not_nullable oc but ;
      emit_assert_let oc
        (Printf.sprintf "(list %s %s)"
          (t_of_expr x)
          (if n then "false" else n_of_expr x))
        (emit_eq eid) ;
      List.iter (emit_assert_not_nullable oc) es ;
      emit_assert_nullable oc e

  | Stateful { skip_nulls = n ; operation = SF2 (Sample, c, x) } ->
      (* - c must be a constant (TODO) strictly (TODO) positive integer;
       * - c must not be nullable;
       * - The type of the result is a list of items of the same type than x;
       * - If we skip nulls then those items are not nullable, otherwise they
       *   are as nullable as x;
       * - 'sample c x` is itself nullable whenever x is nullable (if not
       *   skip null and we encounter a null x, or if skip null and we
       *   encounter only nulls). *)
      emit_assert_unsigned oc c ;
      emit_assert_not_nullable oc c ;
      emit_assert_let oc
        (Printf.sprintf "(list %s %s)"
          (t_of_expr x)
          (if n then "false" else n_of_expr x))
        (emit_eq eid) ;
      emit_assert_eq nid oc (n_of_expr x)

  | Stateful { operation = SF2 (OneOutOf, i, x) ; _ } ->
      (* - i must be a constant (TODO) strictly (TODO) positive integer;
       * - i must not be nullable;
       * - the type of the result is the same as x;
       * - the result is always nullable. *)
      emit_assert_unsigned oc i ;
      emit_assert_not_nullable oc i ;
      emit_assert_eq eid oc (t_of_expr x) ;
      emit_assert_true oc nid

  | Stateful { operation = SF3 (OnceEvery _, d, t, x) ; _ } ->
      (* - d must be a constant (TODO) strictly (TODO) positive (TODO) numeric;
       * - d must not be nullable;
       * - t must be a numeric (event-time);
       * - t must not be nullable (event time);
       * - the type of the result is the same as x;
       * - the result is always nullable. *)
      emit_assert_numeric oc d ;
      emit_assert_not_nullable oc d ;
      emit_assert_numeric oc t ;
      emit_assert_not_nullable oc t ;
      emit_assert_eq eid oc (t_of_expr x) ;
      emit_assert_true oc nid

  | Stateful { skip_nulls = n ; operation = Past { what ; time ; max_age ; sample_size ; _ } } ->
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
      emit_assert_numeric oc max_age ;
      emit_assert_not_nullable oc max_age ;
      emit_assert_numeric oc time ;
      Option.may (fun sample_size ->
        emit_assert_unsigned oc sample_size ;
        emit_assert_not_nullable oc sample_size
      ) sample_size ;
      emit_assert_let oc
        (Printf.sprintf "(list %s %s)"
          (t_of_expr what)
          (if n then "false" else n_of_expr what))
        (emit_eq eid) ;
      emit_assert_nullable oc e

  | Stateful { skip_nulls = n ; operation = SF1 (Group, g) } ->
      (* - The result is a list which elements have the exact same type as g;
       * - If we skip nulls then the elements are not nullable, otherwise they
       *   are as nullable as g;
       * - The group itself is nullable whenever g is nullable.
       * Note: It is possible to build as an immediate value a vector of
       * zero length (although, actually it's not), but it is not possible
       * to build an empty list. So Group will never, under any circumstance,
       * returns an empty list. The only possible way to build an
       * empty list is by skipping nulls, but then is we skip all nulls
       * it will be null. *)
      emit_assert_let oc
        (Printf.sprintf "(list %s %s)"
          (t_of_expr g)
          (if n then "false" else n_of_expr g))
        (emit_eq eid) ;
      emit_assert_eq (n_of_expr g) oc nid

  | Stateful { skip_nulls = n ; operation = SF1 (Count, x) } ->
      (* - The result is always an u32;
       * - The result is only nullable if nulls are not skipped and x is a
       *   nullable boolean, in which case we couldn't tell how many times
       *   FIXME: see note in CodeGen_RaQL2DIL about skip nulls and Count!
       *   the predicate is true. *)
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TU32 ;
      if n then
        emit_assert_false oc nid
      else
        emit_assert_let oc
          (Printf.sprintf "(and %s (= bool %s))"
            (n_of_expr x)
            (t_of_expr x))
          (emit_eq nid)

  | Stateful { operation = SF1 (AggrHistogram (_, _, n), x) ; _ } ->
      (* - x must be numeric;
       * - The result is a vector of size n+2, of non nullable U32;
       * - The result itself is as nullable as x. *)
      let n = Uint32.to_int n in
      emit_assert_numeric oc x ;
      emit_assert_id_eq_typ tuple_sizes records field_names eid oc
        (TVec (n+2, DT.required TU32)) ;
      emit_assert_eq (n_of_expr x) oc nid

  | Stateless (SL2 (In, e1, e2)) ->
      (* Typing rule:
       * - e2 can be a string, a cidr, a list or a vector;
       * - if e2 is a string, then e1 must be a string (TODO: or a u8);
       * - if e2 is a cidr, then e1 must be an ip (TODO: either of the same
       *   version or either the IP or the cidr must be generic);
       * - if e2 is a list or a vector of cidr and e1 is an IP then it is
       *   also accepted and understood as being included in at least one of
       *   the cidrs;
       * - if e2 is either a list of a vector, then e1 must have the sort
       *   of the elements of this list or vector;
       * - The result is a boolean;
       * - Result is as nullable as e1, e2 and the content of e2 if e2
       *   is a vector or a list. *)
      let name = expr_err e2 Err.InType in
      emit_assert ~name oc (fun oc ->
        let id1 = t_of_expr e1 and id2 = t_of_expr e2 in
        Printf.fprintf oc
          "(xor (and (= string %s) (= string %s)) \
                (and (xor (= %s %s) (= %s %s) (= %s %s)) \
                     (xor (= %s %s) (= %s %s) (= %s %s))) \
                (and (xor (and ((_ is list) %s) \
                               (xor (= %s (list-type %s)) \
                                    (= %s (list-type %s)) \
                                    (= %s (list-type %s)))) \
                          (and ((_ is vector) %s) \
                               (xor (= %s (vector-type %s)) \
                                    (= %s (vector-type %s)) \
                                    (= %s (vector-type %s))))) \
                     (xor (= %s %s) (= %s %s) (= %s %s))) \
                (and ((_ is list) %s) %a) \
                (and ((_ is vector) %s) %a))"
          id2 id1
            t_cidr4 id2 t_cidr6 id2 t_cidr id2
            t_ip4 id1 t_ip6 id1 t_ip id1
          id2
            t_cidr4 id2
            t_cidr6 id2
            t_cidr id2
          id2
            t_cidr4 id2
            t_cidr6 id2
            t_cidr id2
          t_ip6 id1 t_ip4 id1 t_ip id1
          id2 (emit_same id1) (Printf.sprintf "(list-type %s)" id2)
          id2 (emit_same id1) (Printf.sprintf "(vector-type %s)" id2)) ;

      emit_assert_id_eq_typ tuple_sizes records field_names eid oc TBool ;

      emit_assert_let oc
        (Printf.sprintf2
          "(or %s %s (and ((_ is list) %s) (list-nullable %s)) \
                     (and ((_ is vector) %s) (vector-nullable %s)))"
          (n_of_expr e1) (n_of_expr e2)
          (t_of_expr e2) (t_of_expr e2)
          (t_of_expr e2) (t_of_expr e2))
        (emit_eq nid)

  | Stateless (SL1 (Fit, e1)) ->
      (* Typing rules:
       * - e1 must be a vector or a list of either numerics or tuples of
       *   numerics;
       * - the result is a float;
       * - the result is always nullable and will be null whenever e1 is null
       *   or if every items of e1 has a null value (columns with null elements
       *   are ignored but if all rows are ignored then the result will be
       *   null) or if the regression fails. *)

    let id1 = t_of_expr e1 in

    let name = expr_err e1 Err.Matrix in
    emit_assert ~name oc (fun oc ->
      Printf.fprintf oc
        "(xor (and ((_ is list) %s) (is-numeric-tuple (list-type %s))) \
              (and ((_ is vector) %s) (is-numeric-tuple (vector-type %s))))"
        id1 id1
        id1 id1) ;

    emit_assert_float oc e ;

    emit_assert_true oc nid

    (*emit_assert_let oc
      (Printf.sprintf2
        "(xor %s \
              (and ((_ is list) %s) \
                   (or (list-nullable %s) \
                       (any-tuple-item-nullable (list-type %s)))) \
              (and ((_ is vector) %s) \
                   (or (vector-nullable %s)
                       (any-tuple-item-nullable (vector-type %s)))))"
        (n_of_expr e1)
        id1 id1 id1
        id1 id1 id1)
       (emit_eq nid)*)

  | Stateless (SL1 (CountryCode, e1)) ->
      (* - e1 must be any kind of IP;
       * - The result will be a nullable string; *)
      emit_assert_ip oc e1 ;
      emit_assert_string oc e ;
      emit_assert_true oc nid

  | Stateless (SL1 (IpFamily, e1)) ->
      (* - e1 must be any kind of IP;
       * - The result will be an unsigned int;
       * - Nullability propagates. *)
      emit_assert_ip oc e1 ;
      emit_assert_unsigned oc e ;
      emit_assert_eq (n_of_expr e1) oc nid

  | Stateless (SL1 (Basename, e1)) ->
      (* - e1 must be a string;
       * - The result will be a string;
       * - Nullability propagates. *)
      emit_assert_string oc e1 ;
      emit_assert_string oc e ;
      emit_assert_eq (n_of_expr e1) oc nid

(* FIXME: we should have only the records known from the run cond *)
let emit_running_condition declare tuple_sizes records field_names
                           param_type env_type oc e =
  !logger.debug "Emit SMT2 for running condition" ;
  Printf.fprintf oc "\n; Running Condition\n\n" ;
  E.iter (fun _ e -> declare e) e ;
  let name = Err.RunCondition in
  emit_assert_id_eq_typ ~name tuple_sizes records field_names (t_of_expr e) oc TBool ;
  emit_assert_false ~name oc (n_of_expr e) ;
  E.iter (
    emit_constraints tuple_sizes records field_names
                     None None param_type env_type None oc
                     "running condition"
  ) e

(* FIXME: we should have only the records accessible from this operation *)
let emit_operation declare tuple_sizes records field_names
                   in_type out_type param_type env_type global_type
                   func_name fi oc op =
  (* Declare all variables: *)
  O.iter_expr (fun _ _ e -> declare e) op ;
  (* Now add specific constraints depending on the clauses: *)
  (match op with
  | Aggregate { where ; notifications ; commit_cond ; every ; _ } ->
      O.iter_expr (
        emit_constraints tuple_sizes records field_names
                         in_type out_type param_type env_type global_type
                         ~func_name oc
      ) op ;
      (* Typing rules:
       * - Where must be a bool;
       * - Commit-when must also be a bool;
       * - Every-clause must be numeric;
       * - Notification names must be non-nullable strings. *)
      let name = func_err fi Err.(Clause ("where", ActualType DT.TBool)) in
      emit_assert_id_eq_typ ~name tuple_sizes records field_names (t_of_expr where) oc TBool ;
      let name = func_err fi Err.(Clause ("where", Nullability false)) in
      emit_assert_false ~name oc (n_of_expr where) ;
      let name = func_err fi Err.(Clause ("commit", ActualType DT.TBool)) in
      emit_assert_id_eq_typ ~name tuple_sizes records field_names (t_of_expr commit_cond) oc TBool ;
      let name = func_err fi Err.(Clause ("commit", Nullability false)) in
      emit_assert_false ~name oc (n_of_expr commit_cond) ;
      Option.may (fun e ->
        let name = func_err fi Err.(Clause ("every", Numeric)) in
        emit_assert ~name oc (fun oc ->
          emit_numeric oc (t_of_expr e)) ;
        let name = func_err fi Err.(Clause ("every", Nullability false)) in
        emit_assert_false ~name oc (n_of_expr e)
      ) every ;
      List.iteri (fun i notif ->
        let name = func_err fi Err.(Notif (i, ActualType DT.TString)) in
        emit_assert_id_eq_typ ~name tuple_sizes records field_names
          (t_of_expr notif) oc TString ;
        let name = func_err fi Err.(Notif (i, Nullability false)) in
        emit_assert_false ~name oc (n_of_expr notif)
      ) notifications

  | ReadExternal { source ; _ } ->
      O.iter_expr (
        emit_constraints tuple_sizes records field_names
                         in_type out_type param_type env_type global_type
                         ~func_name oc
      ) op ;
      let assert_non_nullable typ what e =
        let name =
          func_err fi Err.(ExternalSource (what, ActualType typ)) in
        emit_assert_id_eq_typ ~name tuple_sizes records field_names
                              (t_of_expr e) oc typ ;
        let name =
          func_err fi Err.(ExternalSource (what, Nullability false)) in
        emit_assert_false ~name oc (n_of_expr e) in
      let assert_non_nullable_small_numeric what e =
        let name =
          func_err fi Err.(ExternalSource (what, ActualType DT.TI32)) in
        let id = t_of_expr e in
        emit_assert ~name oc (fun oc ->
          Printf.fprintf oc "(is-small-integer %s)" id) ;
        let name =
          func_err fi Err.(ExternalSource (what, Nullability false)) in
        emit_assert_false ~name oc (n_of_expr e)
      in
      (match source with
      | File { fname ; preprocessor ; unlink } ->
          Option.may (assert_non_nullable TString "file preprocessor")
                     preprocessor ;
          assert_non_nullable TString "file name" fname ;
          assert_non_nullable TBool "file delete condition" unlink
      | Kafka { options ; topic ; partitions ; restart_from } ->
          List.iter (fun (n, e) ->
            let what = Printf.sprintf "Kafka option %S" n in
            assert_non_nullable TString what e
          ) options ;
          assert_non_nullable TString "Kafka topic" topic ;
          (* Partitions are int32_t and offsets are int64_t in rdkafka, but
           * as we use only offset for a diff from tail we are happy enough
           * using also an i32 there: *)
          (* Partitions are specified in RaQL as a vector (when immediate)
           * or list (from a parameter) of small integers. That list can be
           * NULL, meaning all partitions: *)
          Option.may (fun partitions ->
            let name =
              let t = DT.(TArr i32) in
              func_err fi Err.(ExternalSource ("Kafka partitions", ActualType t)) in
            emit_assert ~name oc (fun oc ->
              let id = t_of_expr partitions in
              Printf.fprintf oc
                "(or (and ((_ is list) %s) \
                           (is-small-integer (list-type %s)) \
                           (not (list-nullable %s))) \
                     (and ((_ is vector) %s) \
                          (is-small-integer (vector-type %s)) \
                          (not (list-nullable %s))))"
                id id id
                id id id)
              (* Do not specify nullability of the list itself, which can be
               * either nullable or not *)
          ) partitions ;
          (match restart_from with
          | Beginning | SaveInState -> ()
          | OffsetFromEnd o ->
              assert_non_nullable_small_numeric "Kafka offset" o
          | UseKafkaGroupCoordinator _ ->
              todo "typing of snapshot_period_specs"))

  | _ -> ())

let emit_program declare tuple_sizes records field_names
                 in_types out_types param_type env_type global_type
                 oc prog_name funcs =
  (* Output all the constraints for all the operations: *)
  List.iteri (fun fi func ->
    let fq = VSI.fq_name prog_name func in
    !logger.debug "Emit SMT2 for function %a" N.fq_print fq ;
    Printf.fprintf oc "\n; Constraints for function %a\n"
      N.fq_print fq ;
    (* Not all functions have an input or output type but then it won't
     * be used: *)
    let in_type = List.assoc_opt fq in_types
    and out_type = List.assoc_opt fq out_types in
    (* Start with the retention and best-after expressions (which are both
     * durations): *)
    let emit_duration what e =
      E.iter (fun _ -> declare) e ;
      emit_constraints tuple_sizes records field_names
                       None None param_type env_type None oc
                       (what ^ " clause") [] e ;
      (* Also make sure that the duration is numeric: *)
      let name =
        func_err fi Err.(Clause (what, ActualType DT.TFloat)) in
      emit_assert ~name oc (fun oc -> emit_numeric oc (t_of_expr e)) ;
      let name =
        func_err fi Err.(Clause (what, Nullability false)) in
      emit_assert_false ~name oc (n_of_expr e)
    in
    Option.may (fun retention ->
      emit_duration "retention" retention.Retention.duration
    ) func.VSI.retention ;
    Option.may (fun best_after ->
      emit_duration "best-after" best_after
    ) func.best_after ;
    (* FIXME: filter the records to pass only those accessible from this op *)
    emit_operation declare tuple_sizes records field_names
                   in_type out_type param_type env_type global_type
                   func.name fi oc func.operation
  ) funcs

let emit_minimize oc condition funcs =
  (* "box" tells z3 to optimize each constraint independently and is
   * slightly faster: *)
  Printf.fprintf oc "(set-option :opt.priority box)\n";
  let minimize cost_of_expr =
    Printf.fprintf oc "(minimize (+ 0" ;
    E.iter (fun _ -> cost_of_expr) condition ;
    List.iter (fun func ->
      O.iter_expr (fun _ _ -> cost_of_expr) func.VSI.operation
    ) funcs ;
    Printf.fprintf oc "))\n" in
  (* Minimize total number of bits required to encode all numbers, considering
   * a float is slightly better than a 128 bits integer: *)
  Printf.fprintf oc "\n\
    ; Minimize total number width\n\
    (define-fun cost-of-number ((t Type)) Int\n\
      (ite ((_ is int) t)\n\
           (bv2nat (int-bytes t))\n\
           (ite (= float t) 14 0)))\n" ;
  minimize (fun e ->
    Printf.fprintf oc " (cost-of-number %s)" (t_of_expr e)) ;
  (* And, separately, number of signed values: *)
  Printf.fprintf oc
    "\n; Minimize total signed ints\n\
       (define-fun cost-of-sign ((t Type)) Int\n\
         (ite (and ((_ is int) t) (int-signed t)) 1 0))\n" ;
  minimize (fun e ->
    Printf.fprintf oc " (cost-of-sign %s)" (t_of_expr e)) ;
  (* Finally, minimize the number of nullable values: *)
  Printf.fprintf oc
    "\n; Minimize nullables\n\
       (define-fun cost-of-null ((n Bool)) Int (ite n 1 0))\n" ;
  minimize (fun e ->
    Printf.fprintf oc " (cost-of-null %s)" (n_of_expr e))

(* When a function refers to an input field from a parent, we need to either
 * equates its type to the type of the expression computing the output
 * field (in case of Aggregates) or directly to the type of the output
 * field if it is a well known field (which has no expression). *)
type id_or_type = Id of Uint32.t | FieldType of RamenTuple.field_typ
let id_or_type_of_field op path =
  let open Raql_path_comp.DessserGen in
  let find_field_type what fields =
    (* In the future, when [path] does have several components, look through
     * all of them in turn in the TRecords as we do with the external parents *)
    let field =
      match path with [ Name n ] -> n
      | _ ->
          Printf.sprintf2 "Cherry-picking (%a) in well-known types (%s) is \
                           not supported"
            E.print_path path what |>
          failwith
    in
    List.find (fun ft -> ft.RamenTuple.name = field) fields in
  match op with
  | O.Aggregate { aggregate_fields ; _ } ->
      (* In case [path] has several components, look through the
       * Record expression to locate the actual expression which id we
       * should equate to the callers' expression. *)
      Id (find_expr_of_path_in_selected_fields aggregate_fields path).E.uniq_num
  | O.ReadExternal { format ; _ } ->
      let fields = O.fields_of_external_format format in
      FieldType (find_field_type "CSV" fields)
  | O.ListenFor { proto ; _ } ->
      FieldType (find_field_type (RamenProtocols.string_of_proto proto)
                                 (RamenProtocols.tuple_typ_of_proto proto))

let emit_out_types decls oc field_names prog_name funcs =
  !logger.debug "Emitting SMT2 for output types" ;
  List.fold_left (fun (assoc, i as prev) func ->
    let rec_tid = t_of_prefix Out i
    and rec_nid = n_of_prefix Out i in
    match func.VSI.operation with
    | Aggregate { aggregate_fields ; _ } ->
        let sz = List.length aggregate_fields in
        Printf.fprintf decls
          "\n; Output record type for %a\n\n\
           (declare-fun %s () Type)\n\
           (declare-fun %s () Bool)\n"
          N.fq_print (VSI.fq_name prog_name func)
          rec_tid rec_nid ;
        emit_assert oc (fun oc -> emit_is_record rec_tid oc sz) ;
        (* All outputs are non null (but for previous_out, see Variable): *)
        emit_assert_id_is_bool rec_nid oc false ;
        (* For typing this is not necessary to order the structure fields in
         * any specific way, as we carry along the field names. *)
        let open Raql_select_field.DessserGen in
        List.iteri (fun j sf ->
          (* Equates each field expression to its field: *)
          let id = sf.expr.uniq_num in
          Printf.fprintf oc
            "; Output field %d (%a.%a) equals expression %s\n"
            j
            N.func_print func.VSI.name
            N.field_print sf.alias
            (Uint32.to_string id) ;
          emit_assert oc (fun oc ->
            Printf.fprintf oc "(= %s (record%d-e%d %s))"
              (t_of_num id) sz j rec_tid) ;
          emit_assert oc (fun oc ->
            Printf.fprintf oc "(= %s (record%d-n%d %s))"
              (n_of_num id) sz j rec_tid) ;
          emit_assert oc (fun oc ->
            Printf.fprintf oc "(= %s (record%d-f%d %s))"
              (f_of_name field_names sf.alias) sz j rec_tid)
        ) aggregate_fields ;
        (VSI.fq_name prog_name func, i) :: assoc, i + 1
    | _ ->
        (* FIXME: those should also have an out record expression, although
         * their individual fields are not expressions. *)
        prev
  ) ([], 0) funcs |> fst

exception MissingFieldInParent of N.src_path * string
let () =
  Printexc.register_printer (function
    | MissingFieldInParent (psrc, msg) ->
        Some (Printf.sprintf2 "In %a: %s" N.src_path_print psrc msg)
    | _ ->
        None)

(* Reading already compiled parents, set the type of fields originating from
 * external parents, parameters and environment, once and for all.
 * Equals the input type of fields originating from internal parents to
 * those output fields.
 * Build the list of keys used for params and env, and from there declare
 * two records, which fields equates the types output above, and return
 * their id. *)
let emit_in_types decls oc tuple_sizes records field_names parents params
                  globals condition prog_name funcs =
  !logger.debug "Emitting SMT2 for input types, with field names = %a"
    (Hashtbl.print N.field_print Int.print) field_names ;
  (* Build the input type of each func by collecting all the Get(name, x) or
   * Path(path) where x is input-like. Then also build the type of the
   * params and env records.
   * Build a record type for each of those and return it. *)
  let in_fields = Hashtbl.create 10
  and param_fields = Hashtbl.create 10
  and env_fields = Hashtbl.create 10
  and global_fields = Hashtbl.create 10
  in
  let get_sub_hash h func =
    let func_name = Option.map (VSI.fq_name prog_name) func in
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
    E.iter (f ?func:None "Running condition" "") condition |> ignore ;
    List.iter (fun func ->
      let what =
        Printf.sprintf2 "Function %a" N.func_print func.VSI.name in
      O.iter_expr (f ?func:(Some func) what) func.operation |> ignore ;
      Retention.fold_expr
        () (fun () -> f ?func:(Some func) what "" []) func.retention ;
      Option.may (f ?func:(Some func) what "" []) func.best_after
    ) funcs
  in
  let register_io ?func what e prefix path =
    emit_comment oc "register_io of %a%s"
      (fun oc -> function
        | Some f -> Printf.fprintf oc "%a, " N.func_print f.VSI.name
        | None -> ()) func
      what ;
    match prefix with
    | Variable.Env ->
        emit_assert_id_eq_typ tuple_sizes records field_names
          (t_of_expr e) oc TString ;
        emit_assert_nullable oc e ;
        (* Also make this expression stands for this env field: *)
        register_input env_fields None path e
    | Param ->
        let field =
          match path with [ Name n ] -> n
          | _ -> failwith "Cherry-picking from parameters is not supported" in
        (match RamenTuple.params_find field params with
        | exception Not_found ->
            Printf.sprintf2 "%s is using unknown parameter %a"
              what E.print_path path |>
            failwith
        | param ->
            emit_assert_id_eq_typ tuple_sizes records field_names
              (t_of_expr e) oc param.typ.DT.typ ;
            emit_assert_id_is_bool (n_of_expr e) oc param.typ.DT.nullable ;
            (* Also make this expression stands for this param field: *)
            register_input param_fields None path e)
    | GlobalVar ->
        (* Same as above for Param, albeit looking up definitions in globals
         * rather than params *)
        let field =
          match path with [ Name n ] -> n
          | _ -> failwith "Cherry-picking from globals is not supported" in
        (match List.find (fun g -> g.Globals.name = field) globals with
        | exception Not_found ->
            Printf.sprintf2 "%s is using unknown global variable %a"
              what E.print_path path |>
            failwith
        | global ->
            emit_assert_id_eq_typ tuple_sizes records field_names
              (t_of_expr e) oc global.Globals.typ.DT.typ ;
            emit_assert_id_is_bool (n_of_expr e) oc global.typ.DT.nullable ;
            (* Also make this expression stands for this global field: *)
            register_input global_fields None path e)
    | In
    | SortFirst | SortSmallest | SortGreatest ->
        (match func with
        | None ->
            Printf.sprintf2 "%s has no input %a"
              what E.print_path path |>
            failwith ;
        | Some func ->
            let no_such_field pfunc pname =
              let msg =
                Printf.sprintf2 "Parent %a of %s does not output a field \
                                 named %a (only: %a) (in: %a)"
                  N.func_print pfunc.VSI.name
                  what
                  E.print_path path
                  (pretty_list_print (fun oc ft ->
                    N.field_print oc ft.RamenTuple.name))
                    (O.out_type_of_operation ~reorder:false ~with_priv:false
                                             pfunc.VSI.operation)
                  (E.print false) e
              and psrc = N.src_path_of_program pname in
              raise (MissingFieldInParent (psrc, msg))
            and no_such_path pname msg =
              let psrc = N.src_path_of_program pname in
              raise (MissingFieldInParent (psrc, msg))
            and aggr_types pname pfunc t prev =
              let fq = VSI.fq_name pname pfunc in
              match prev with
              | None -> Some ([fq], t)
              | Some (prev_fqs, prev_t) ->
                  if t <> prev_t then
                    Printf.sprintf2
                      "All parents of %s must agree on the type of field \
                       %a (%a has %a but %s has %a)"
                      what
                      E.print_path path
                      (pretty_list_print (fun oc fq ->
                        String.print oc (N.fq_color fq))) prev_fqs
                      DT.print_mn prev_t
                      (N.fq_color fq)
                      DT.print_mn t |>
                    failwith ;
                  Some ((fq::prev_fqs), prev_t) in
            (* Return either the type or a set of id to set this field
             * type to: *)
            let typ, same_as_ids =
              let parents = Hashtbl.find_default parents func.VSI.name [] in
              List.fold_left (fun (prev_typ, same_as_ids) (pname, pfunc) ->
                (* Is this parent part of local functions? *)
                if pname = prog_name then (
                  (* Retrieve the id for the parent output fields: *)
                  (* FIXME: WhaaaaaaaaAAAAAAAT?
                   *        What's wrong with the above pfunc!? *)
                  let pfunc =
                    try List.find (fun f -> f.VSI.name = pfunc.VSI.name) funcs
                    with Not_found ->
                      !logger.error "Cannot find parent %S in any of this \
                                     program functions (have %a)"
                        (pfunc.VSI.name :> string)
                        (pretty_list_print (fun oc f ->
                          String.print oc (f.VSI.name :> string))) funcs ;
                      raise Not_found in
                  (* When the parent is an aggr then we will just make its
                   * output equal to this input (added to same_as_ids), but
                   * when it has a well-known type then we will make this
                   * type equal to that well known type. *)
                  match id_or_type_of_field pfunc.VSI.operation path with
                  | exception Not_found ->
                      no_such_field pfunc pname
                  | exception Failure msg ->
                      no_such_path pname msg
                  | Id p_id ->
                      prev_typ, p_id::same_as_ids
                  | FieldType typ ->
                      aggr_types pname pfunc typ.RamenTuple.typ prev_typ,
                      same_as_ids
                ) else (
                  (* External parent: look for the exact type *)
                  let pser =
                    O.out_type_of_operation ~reorder:false ~with_priv:false
                                            pfunc.VSI.operation in
                  (* If [path] has several component then look for each
                   * components one after the other, localizing the type
                   * through the records.
                   * We have to treat differently the first path that must
                   * match the selected field name, from the rest that must
                   * match TRecord fields; TODO: have a single output value!
                   *)
                  match find_type_of_path_in_tuple_typ pser path with
                  | exception Not_found ->
                      no_such_field pfunc pname
                  | exception Failure msg ->
                      no_such_path pname msg
                  | typ ->
                      assert (DT.is_defined typ.DT.typ) ;
                      aggr_types pname pfunc typ prev_typ, same_as_ids)
              ) (None, []) parents in
            (* [typ] is the of that input, known either from a pre-compiled
             * parent or a well-known output type of another function. We
             * must equate [e] to this type: *)
            Option.may (fun (_funcs, t) ->
              emit_assert_id_eq_typ tuple_sizes records field_names
                (t_of_expr e) oc t.DT.typ ;
              emit_assert_id_is_bool (n_of_expr e) oc t.DT.nullable
            ) typ ;
            (* If all we know is that [e] must have the same type as other
             * expressions, we set soft constraints for all of them: *)
            List.iter (fun id ->
              let name = expr_err e Err.InheritType in
              emit_assert_eq ~name (t_of_expr e) oc (t_of_num id) ;
              let name = expr_err e Err.InheritNull in
              emit_assert_eq ~name (n_of_expr e) oc (n_of_num id)
            ) same_as_ids ;
            (* Also, make [e] stand for the input field [field] of this
             * function: *)
            register_input in_fields (Some func) path e)
    | _tup_ref -> (* Ignore non-inputs *) ()
  in
  program_iter (fun ?func what _ _ e ->
    match e.E.text with
    | Stateless (SL2 (
          Get, E.{ text = Stateless (SL0 (Const (VString s))) ; _ },
               E.{ text = Stateless (SL0 (Variable prefix)) ; _ })) ->
        register_io ?func what e prefix [ Name (N.field s) ]
    | Stateless (SL0 (Path path)) ->
        register_io ?func what e In path
    | _ -> ()
  ) condition funcs ;
  (* For param_fields, env_fields, global_fields  and each function in
   * in_fields, declare the structure for the first id of the list and make
   * the others equal to it. *)
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
        | None -> Variable.to_string pref
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
          "; Expression %a (%s) is field %d (%a)\n"
          (E.print ~max_depth:2 false) e
          (Uint32.to_string e.E.uniq_num)
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
  let in_types = declare_input In in_fields
  and param_type = declare_input Param param_fields
  and env_type = declare_input Env env_fields
  and global_type = declare_input GlobalVar global_fields
  in
  (* In theory we have only one entry (for fq_name = None) for both params
   * and env, since we've never registered func: *)
  let opt_first = function
    | [] -> None
    | [_fq, x] -> Some x
    | _ -> assert false in
  let in_types =
    List.map (fun (fq_opt, x) ->
      option_get "in_types belong to a function" __LOC__ fq_opt, x) in_types
  and param_type = opt_first param_type
  and env_type = opt_first env_type
  and global_type = opt_first global_type in
  in_types, param_type, env_type, global_type

let type_of_value_sort_identifier = function
  | "bool" -> DT.TBool
  | "string" -> TString
  | "char" -> TChar
  | "float" -> TFloat
  | "unit" -> TVoid
  | id ->
      (match user_type_of_t id with
      | exception Not_found ->
          Printf.sprintf "Unknown sort identifier %S" id |>
          failwith
      | name ->
          DT.get_user_type name)

let field_index_of_term t =
  let open Smt2Types in
  let invalid_term t =
    Format.(fprintf str_formatter "Bad term when expecting a field identifier: %a"
      Term.print t) ;
    Format.flush_str_formatter () |> failwith in
  match t with
  | QualIdentifier (Identifier id, None) ->
      (try Scanf.sscanf id "field%d%!" identity
      with _ -> invalid_term t)
  | _ ->
      invalid_term t

let rec type_of_value_identifier name_of_idx bindings id =
  match List.assoc id bindings with
  | exception Not_found ->
      (* Better be a predefined binding then: *)
      type_of_value_sort_identifier id
  | term ->
      type_of_value_term name_of_idx bindings term

and type_of_value_term name_of_idx bindings =
  let open Smt2Types in
  function
  | QualIdentifier (Identifier id, None) ->
      type_of_value_identifier name_of_idx bindings id
  | Apply ((Identifier "int", None), [ signed ; bytes ]) ->
      let signed = Term.to_bool signed
      and bytes = Big_int.int_of_big_int (Term.to_big_int bytes) in
      (match bytes with
      | 0 -> if signed then TI8 else TU8
      | 1 -> if signed then TI16 else TU16
      | 2 -> if signed then TI24 else TU24
      | 3 -> if signed then TI32 else TU32
      | 4 -> if signed then TI40 else TU40
      | 5 -> if signed then TI48 else TU48
      | 6 -> if signed then TI56 else TU56
      | 7 -> if signed then TI64 else TU64
      | _ ->
          assert (bytes <= 15) ;
          if signed then TI128 else TU128)
  | Apply ((Identifier "vector", None), [ ConstantTerm c ; typ ; null ]) ->
      let typ = type_of_value_term name_of_idx bindings typ
      and nullable = Term.to_bool null in
      let n = Big_int.int_of_big_int (Constant.to_big_int c) in
      TVec (n, DT.maybe_nullable ~nullable typ)
  | Apply ((Identifier "list", None), [ typ ; null ]) ->
      let typ = type_of_value_term name_of_idx bindings typ
      and nullable = Term.to_bool null in
      TArr (DT.maybe_nullable ~nullable typ)
  | Apply ((Identifier id, None), sub_terms)
    when String.starts_with id "tuple" ->
      Scanf.sscanf id "tuple%d%!" (fun sz ->
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
              let nullable = Term.to_bool n
              and typ = type_of_value_term name_of_idx bindings e in
              let t = DT.maybe_nullable ~nullable typ in
              loop (t :: ts) rest
          | _ -> assert false in
          loop [] sub_terms in
        DT.TTup ts)
  | Apply ((Identifier id, None), sub_terms)
    when String.starts_with id "record" ->
      Scanf.sscanf id "record%d%!" (fun sz ->
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
              let nullable = Term.to_bool n
              and typ = type_of_value_term name_of_idx bindings e in
              let t = DT.maybe_nullable ~nullable typ in
              let idx = field_index_of_term f in
              if idx < 0 || idx >= Array.length name_of_idx then
                Printf.sprintf2 "Invalid field number %d (fields are: %a)"
                  idx
                  (pretty_array_print String.print) name_of_idx |>
                failwith ;
              let name = name_of_idx.(idx) in
              loop ((name, t) :: ts) rest
          | _ -> assert false in
          loop [] sub_terms in
        DT.TRec ts)
  | Apply ((Identifier "map", None), [ ktyp ; knull ; typ ; vnull ]) ->
      let ktyp =
        let typ = type_of_value_term name_of_idx bindings ktyp
        and nullable = Term.to_bool knull in
        DT.maybe_nullable ~nullable typ
      and typ =
        let typ = type_of_value_term name_of_idx bindings typ
        and nullable = Term.to_bool vnull in
        DT.maybe_nullable ~nullable typ in
      TMap (ktyp, typ)
  | Let (bs, t) ->
      let bindings = bs @ bindings in
      type_of_value_term name_of_idx bindings t
  | term ->
      Format.(fprintf str_formatter "Unimplemented term: %a" Term.print term) ;
      Format.flush_str_formatter () |> failwith

let emit_smt2 parents tuple_sizes records field_names condition prog_name funcs
              params globals oc ~optimize =
  (* We have to start the SMT2 file with declarations before we can
   * produce any assertion: *)
  let decls = IO.output_string () in
  (* We might encounter several times the same expression (for the well
   * known constants defined once in RamenExpr, such as expr_true etc...)
   * So we keep a set of already declared ids: *)
  let ids = ref Set.empty in
  let expr_types = IO.output_string () in
  let io_types = IO.output_string () in
  let declare e =
    let id = e.E.uniq_num in
    if not (Set.mem id !ids) then (
      ids := Set.add id !ids ;
      Printf.fprintf decls
        "\n; %a\n\
         (declare-fun %s () Bool)\n\
         (declare-fun %s () Type)\n"
        (E.print true) e
        (n_of_expr e)
        (t_of_expr e))
  in
  (* Declare a record for all output types of all local (Aggr) functions,
   * that we can use to equate any Variable bound to Out. The
   * fields will be equal to the selected_field.expr, which are also
   * used by the child, so this is all equivalent and the output record
   * is a kind of "alias" for the actual fields. The only advantage
   * is that we can now easily type `Get(x, "out")`. *)
  let out_types =
    emit_out_types decls io_types field_names prog_name funcs in
  (* Declare relationships between all input types: *)
  let in_types, param_type, env_type, global_type =
    emit_in_types decls io_types tuple_sizes records field_names parents
                  params globals condition prog_name funcs in
  emit_running_condition declare tuple_sizes records field_names
                         param_type env_type expr_types condition ;
  emit_program declare tuple_sizes records field_names
               in_types out_types param_type env_type global_type
               expr_types prog_name funcs ;
  if optimize then
    emit_minimize expr_types condition funcs ;
  let record_sizes =
    Hashtbl.fold (fun _ (_, sz, _) szs ->
      Set.Int.add sz szs
    ) records Set.Int.empty in
  let num_fields =
    Hashtbl.length field_names in
  assert (num_fields > 0) (* Because parsing and checks on STAR *) ;
  Printf.fprintf oc "%a" preamble optimize ;

  Printf.fprintf oc
     ";\n\
     ; Define a sort for types:\n\
     ;\n\
     (declare-datatypes\n\
       ( (Type 0)\n\
         (Field 0) )\n\

       ( ((bool) (string) (float) (char) (unit)\n\
          %a
          (int (int-signed Bool) (int-bytes (_ BitVec 4)))\n\
          (list (list-type Type) (list-nullable Bool))\n\
          (vector (vector-dim Int) (vector-type Type) (vector-nullable Bool))\n\
          (map (map-key-type Type) (map-key-nullable Bool) (map-value-type Type) (map-value-nullable Bool))\n\
          %a\n\
          %a)\n\
         (%t )))\n\
     \n"
    (List.print ~first:"" ~last:"" ~sep:" " (fun oc name ->
      let id = t_of_user_type name in
      String.print oc id))
      (* Only declare the types that are actually supported: *)
      [ "Eth" ; "Ip" ; "Ip4" ; "Ip6" ; "Cidr" ; "Cidr4" ; "Cidr6" ]
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
        Printf.fprintf oc " (record%d-f%d Field)" sz i
      done ;
      Printf.fprintf oc ")")) record_sizes
    (fun oc ->
      for i = 0 to num_fields-1 do
        Printf.fprintf oc " (field%d)" i
      done) ;

  Printf.fprintf oc
     ";\n\
     ; Define type expressions:\n\
     ;\n\
     (define-fun is-u128 ((t Type)) Bool\n\
       (= (int false #b1111) t))\n\n\
     (define-fun is-numeric ((t Type)) Bool\n\
       (or (= float t) \n\
           ((_ is int) t)))\n\n\
     ; aka (bv2nat (int-bytes id)) <= 3:\n\
     (define-fun is-small-integer ((t Type)) Bool\n\
        (and ((_ is int) t)\n\
             (= ((_ extract 3 2) (int-bytes t)) #b00)))\n\n\
     (define-fun is-small-unsigned ((t Type)) Bool\n\
        (and (is-small-integer t)\n\
             (not (int-signed t))))\n\n\
     (define-fun is-numeric-tuple ((t Type)) Bool\n\
        ; Returns true iif t is a numeric (aka 1-tuple) or a tuple of numerics\n\
        (xor (is-numeric t)\n\n\
             %a))\n\n\
     (define-fun any-tuple-item-nullable ((t Type)) Bool\n\
        ; Returns true iif t is a tuple and anyone of its elements is nullable\n\
        (or false%a))\n\n"
    (Set.Int.print ~first:" " ~last:"" ~sep:"\n" (fun oc sz ->
      Printf.fprintf oc "(and ((_ is tuple%d) t)" sz ;
      for i = 0 to sz-1 do
        Printf.fprintf oc " (is-numeric (tuple%d-e%d t))" sz i
      done ;
      Printf.fprintf oc ")")) tuple_sizes
    (Set.Int.print ~first:"" ~last:"" ~sep:"\n" (fun oc sz ->
      Printf.fprintf oc "(and ((_ is tuple%d) t)" sz ;
      Printf.fprintf oc "(or" ;
      for i = 0 to sz-1 do
        Printf.fprintf oc " (tuple%d-n%d t)" sz i
      done ;
      Printf.fprintf oc "))")) tuple_sizes ;
  String.print oc
    "(define-fun is-unsigned ((t Type)) Bool\n\
       (and ((_ is int) t) (not (int-signed t))))\n" ;
  Printf.fprintf oc
     ";\n\
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
    (IO.close_out decls)
    (IO.close_out io_types)
    (IO.close_out expr_types)
    post_scriptum

(* For tuples (and records, which are tuples with named fields) we declare as
 * many types as there are possible arities.  This had proven faster (and
 * simpler) than to use recursive datatype declarations.
 *
 * For instance, if we use tuples with either 3 or 7 fields, then we will
 * create a type named "tuple3" with 3 times 2 constructors (for each fields,
 * one for its type and one for its nullability), and another one named
 * "tuple7" with 7 times 2 constructors, so that for instance "(tuple2-e0 t21)"
 * will hold the type (of type Type) of the first field of the pair "t21", and
 * "(tuple7-n5 t72)" will hold the nullability (type Bool) of the 7-tuple
 * "t72". So the expression "GET(1, x)" imposes that x is a tuple:
 *
 *   (assert (or ((_ is tuple2) x) (_ is tuple7) x))
 *
 * (ignoring other possible uses of GET for this discussion), and also imposes
 * that the 2nd field has the same type (and nullability) as the whole GET
 * expression t:
 *
 *   (assert (or (= (tuple2-e1 x) t) (= (tuple7-e1 x) t)))
 *
 * Of course all this simplifies for a "GET(5, x)" as we can then exclude
 * tuple2 from the assertions.
 *
 * For records we also have to find out which field a GET is referring to.
 * We therefore collect all possible record field names, and give each of them
 * a unique number. An additional constructor for the record types store this
 * field index, so that we end up with, for each expression of type record,
 * with each of its field type, nullability, and name.
 *
 * For the above to work, all possible records are collected, with their size
 * and which name they use at which locations.  with this data in hand, GET
 * expressions such as "GET(foo, r)" can be translated into assertions ensuring
 * that r must be one of the records that have a field named "foo" (in addition
 * to the type and nullability assertions that are similar to those for tuples
 * described above).
 *
 * With all this in mind, it must be clear now to what end [used_tuples_records]
 * function collects all possible tuples and record sizes, field names, etc.
 *
 * Notice that each function input and output type is treated exactly like an
 * actually record (as it should be). But keep in mind that the input type of a
 * function is not the same as the output type of its parents.  Indeed, its
 * performing "deep selection" according to its fieldmask.  So in general, for
 * N functions in the program there are 2*N I/O records in addition to inline
 * records.
 * Given there is no proper equality between output and input record types,
 * we have to emit additional assertions to bind the used fields of output
 * records to the fields of input records, but this princess is in another
 * castle.  *)
let used_tuples_records condition funcs parents =
  (* Map from field names to the global field index (one int index per
   * possible field name) *)
  let field_names = Hashtbl.create 10 in
  (* Map from a field name to where it can be found in a records, defined as
   * the global field index, the record size and the rank of that field in
   * that record.
   * Of course each name can be present in several records so this Hashtbl
   * accepts multiple binding for the same key. *)
  let records = Hashtbl.create 10 in
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
  let all_exprs =
    E.fold (fun _ lst e -> e :: lst) [] condition in
  let all_exprs =
    List.fold_left (fun lst func ->
      let lst =
        match func.VSI.retention with
        | Some Retention.{ duration = e ; _ } -> e :: lst
        | None -> lst in
      let lst =
        match func.best_after with
        | Some e -> e :: lst
        | None -> lst in
      O.fold_expr lst (fun _ _ lst e -> e :: lst) func.VSI.operation
    ) all_exprs funcs
  in
  let tuple_sizes, params, envvars, globals =
    List.fold_left (fun (tuple_sizes, params, envvars, globals as prev) e ->
      match e.E.text with
      (* The simplest ways to get a tuple in an op are with a Tuple or a
       * Record literal expression: *)
      | Tuple ts ->
          (Set.Int.add (List.length ts) tuple_sizes), params, envvars, globals
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
      | Stateless (SL2 (
            Get, { text = Stateless (SL0 (Const (VString name))) ; _ },
                 { text = Stateless (SL0 (Variable tuple)) ; _ })) ->
          let name = N.field name in
          if tuple = Param then
            tuple_sizes, (Set.add name params), envvars, globals
          else if tuple = Env then
            tuple_sizes, params, (Set.add name envvars), globals
          else if tuple = GlobalVar then
            tuple_sizes, params, envvars, (Set.add name globals)
          else
            prev
      | _ ->
          prev
    ) (Set.Int.empty, Set.empty, Set.empty, Set.empty) all_exprs in
  let register_set s =
    let l = Set.cardinal s in
    set_iteri (fun i n -> register_field n l i) s in
  register_set params ;
  register_set envvars ;
  register_set globals ;
  (* This function accept the set of tuple_sizes and a type, and will
   * (recursively) explore that type for tuples/records and return the new
   * extended tuple_sizes set and will also call register_field on all
   * discovered record fields: *)
  let rec look_into_type tuple_sizes t =
    match t.DT.typ with
    | TTup ts ->
        let tuple_sizes = Set.Int.add (Array.length ts) tuple_sizes in
        Array.fold_left look_into_type tuple_sizes ts
    | TVec (_, t) | TArr t ->
        look_into_type tuple_sizes t
    | TRec kts ->
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
      List.fold_left (fun s (_n, f) ->
        let out_type =
          O.out_type_of_operation ~reorder:false ~with_priv:false
                                  f.VSI.operation in
        List.fold_left (fun s ft ->
          look_into_type s ft.RamenTuple.typ
        ) s out_type
      ) s fs
    ) parents tuple_sizes in
  (* Finally, every input and output of all functions might also be a
   * record: *)
  let look_into_type t =
    (* A variant that only registers the record fields recursively: *)
    ignore (look_into_type Set.Int.empty t) in
  List.iter (fun func ->
    let in_type =
      RamenFieldMaskLib.in_type_of_operation func.VSI.operation in
    (* Keep user defined order: *)
    let out_typ =
      O.out_type_of_operation ~reorder:false ~with_priv:true
                              func.VSI.operation in
    let sz = List.length out_typ in
    List.iteri (fun i ft ->
      register_field ft.RamenTuple.name sz i ;
      look_into_type ft.typ
    ) out_typ ;
    let sz = List.length in_type in
    List.iteri (fun i fm ->
      let name = E.id_of_path fm.RamenFieldMaskLib.path in
      register_field name sz i
    ) in_type
  ) funcs ;
  tuple_sizes, records, field_names

let get_types parents condition prog_name funcs params globals fname =
  let funcs = Hashtbl.values funcs |> List.of_enum in
  let h = Hashtbl.create 997 in
  if funcs <> [] then (
    let open RamenSmtParser in
    let tuple_sizes, records, field_names =
      used_tuples_records condition funcs parents in
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
                         condition prog_name funcs params globals
    and parse_result sym vars sort term =
      try Scanf.sscanf sym "%[tn]%d%!" (fun tn id ->
        let open Smt2Types in
        match vars, sort, tn with
        | [], NonParametricSort (Identifier "Type"), "t" ->
            let typ = type_of_value_term name_of_idx [] term in
            Hashtbl.modify_opt id (function
              | None ->
                  Some DT.{ typ ; nullable = false (* by default *) ;
                            default = None }
              | Some prev ->
                  Some DT.{ typ ; nullable = prev.nullable ; default = None }
            ) h
        | [], NonParametricSort (Identifier "Bool"), "n" ->
            let nullable = Term.to_bool term in
            Hashtbl.modify_opt id (function
              | None ->
                  Some (DT.maybe_nullable ~nullable TUnknown)
              | Some prev ->
                  Some (DT.maybe_nullable ~nullable prev.DT.typ)
            ) h
        | [], NonParametricSort (Identifier sort), _ ->
            !logger.error "Result not about sort Type but %S?!" sort
        | _, NonParametricSort (IndexedIdentifier _), _ ->
          !logger.warning "TODO: exploit define-fun with indexed identifier"
        | _, ParametricSort _, _ ->
          !logger.warning "TODO: exploit define-fun of parametric sort"
        | _::_, _, _ ->
          !logger.warning "TODO: exploit define-fun with parameters")
      with Scanf.Scan_failure _ | End_of_file -> ()
         | Failure msg ->
            !logger.error "Cannot parse SMT %s: %s" sym msg
    and unsat syms =
      Printf.sprintf2 "Cannot solve typing constraints: %a"
        (Err.print_core funcs condition) syms |>
      failwith
    in
    run_smt2 ~fname ~emit ~parse_result ~unsat) ;
  h
