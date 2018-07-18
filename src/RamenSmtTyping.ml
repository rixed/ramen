(* Typing a ramen program using a SMT solver.
 *
 * Principles:
 *
 * - Typing and nullability are resolved simultaneously in case nullability
 *   depends on the chosen types in some contexts..
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

let smt_solver = ref "z3 -smt2 %s"

let e_of_num num =
  Printf.sprintf "e%d" num

let e_of_expr e =
  e_of_num ((typ_of e).uniq_num)

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
  | TU8 -> Printf.fprintf oc "(= (number 0 8) %s)" id
  | TU16 -> Printf.fprintf oc "(= (number 0 16) %s)" id
  | TU32 -> Printf.fprintf oc "(= (number 0 32) %s)" id
  | TU64 -> Printf.fprintf oc "(= (number 0 64) %s)" id
  | TU128 -> Printf.fprintf oc "(= (number 0 128) %s)" id
  | TI8 -> Printf.fprintf oc "(= (number 1 8) %s)" id
  | TI16 -> Printf.fprintf oc "(= (number 1 16) %s)" id
  | TI32 -> Printf.fprintf oc "(= (number 1 32) %s)" id
  | TI64 -> Printf.fprintf oc "(= (number 1 64) %s)" id
  | TI128 -> Printf.fprintf oc "(= (number 1 128) %s)" id
  | TFloat -> Printf.fprintf oc "(= (number 1 129) %s)" id
  (* Asking for a TNum is asking for any number: *)
  | TNum -> Printf.fprintf oc "((_ is number) %s)" id
  | TEth -> Printf.fprintf oc "(= eth %s)" id
  | TIpv4 -> Printf.fprintf oc "(= (ip 4) %s)" id
  | TIpv6 -> Printf.fprintf oc "(= (ip 6) %s)" id
  (* Asking for a TIp is asking for a generic Ip able to handle both: *)
  | TIp -> Printf.fprintf oc "(= (ip 9) %s)" id
  | TCidrv4 -> Printf.fprintf oc "(= (cidr 4) %s)" id
  | TCidrv6 -> Printf.fprintf oc "(= (cidr 6) %s)" id
  | TCidr -> Printf.fprintf oc "(= (cidr 9) %s)" id
  | TTuple ts ->
      let d = Array.length ts in
      if d = 0 then
        Printf.fprintf oc "(or %a)"
          (List.print ~first:"" ~last:"" ~sep:" " (emit_is_tuple id))
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
  | Some name ->
      Printf.fprintf oc "(assert (! %t :named %s))\n"
        p name

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
 * For IP/CIDR, it means version is either the same or 9.
 * For number, it means that width is <= and sign is also <=. *)
let emit_id_le_smt2 id oc smt2 =
  Printf.fprintf oc
    "(or (= %s %s) \
       (and ((_ is ip) %s) ((_ is ip) %s) \
            (<= (ip-version %s) (ip-version %s))) \
       (and ((_ is cidr) %s) ((_ is cidr) %s) \
            (<= (cidr-version %s) (cidr-version %s))) \
       (and ((_ is number) %s) ((_ is number) %s) \
            (<= (sign %s) (sign %s)) \
            (<= (width %s) (width %s))))"
      id smt2 id smt2 id smt2 id smt2
      id smt2 id smt2 id smt2 id smt2

let emit_assert_id_le_smt2 ?name id oc smt2 =
  emit_assert ?name oc (fun oc -> emit_id_le_smt2 id oc smt2)

let emit_assert_id_le_id = emit_assert_id_le_smt2

(* TODO: emit_assert_notnull... *)

let emit_assert_id_eq_any_of_typ ?name tuple_sizes id oc lst =
  emit_assert ?name oc (fun oc ->
    Printf.fprintf oc "(or %a)"
      (List.print ~first:"" ~last:"" ~sep:" " (emit_id_eq_typ tuple_sizes id)) lst)

(* When annotating an assertion we must always use a unique name, even if we
 * annotate several times the very same expression. It is important to
 * name all those expression though, as otherwise there is no guaranty the
 * solver would choose to fail at the annotated one. Thus this sequence that
 * we use to uniquify annotations: *)
let make_name =
  let seq = ref 0 in
  fun e tag ->
    incr seq ;
    Printf.sprintf "E%d_%s_%d" (typ_of e).uniq_num tag !seq

let emit_assert_sortable oc e =
  let name = make_name e "SORTABLE" in
  let eid = e_of_expr e in
  emit_assert ~name oc (fun oc ->
    Printf.fprintf oc
      "(or (= string %s) \
           (= bool %s) \
           ((_ is number) %s) \
           ((_ is ip) %s) \
           ((_ is cidr) %s))"
      eid eid eid eid eid)

let emit_assert_unsigned oc e =
  let name = make_name e "UNSIGNED" in
  let eid = e_of_expr e in
  emit_assert ~name oc (fun oc ->
    Printf.fprintf oc
      "(and ((_ is number) %s) \
            (= (sign %s) 0) \
            (<= (width %s) 128))"
      eid eid eid)

let emit_assert_integer oc e =
  let name = make_name e "INTEGER" in
  let eid = e_of_expr e in
  emit_assert ~name oc (fun oc ->
    Printf.fprintf oc
      "(and ((_ is number) %s) \
            (<= (width %s) 128))"
      eid eid)

let emit_assert_numeric oc e =
  let name = make_name e "NUMERIC" in
  let eid = e_of_expr e in
  emit_assert ~name oc (fun oc ->
    Printf.fprintf oc "((_ is number) %s)" eid)

(* "same" types are either actually the same or at least of the same sort
 * (both numbers, both ip, or both cidr) *)
let emit_same id1 oc id2 =
  Printf.fprintf oc
    "(or (= %s %s) \
         (and ((_ is number) %s) ((_ is number) %s)) \
         (and ((_ is ip) %s) ((_ is ip) %s)) \
         (and ((_ is cidr) %s) ((_ is cidr) %s)))"
      id1 id2 id1 id2 id1 id2 id1 id2

let emit_assert_same e oc id1 id2 =
  let name = make_name e "SAME" in
  emit_assert ~name oc (fun oc -> emit_same id1 oc id2)

(* Assuming all input/output/constants have been declared already, emit the
 * constraints connecting the parameter to the result: *)
let emit_constraints tuple_sizes out_fields oc e =
  let eid = e_of_expr e and nid = n_of_expr e in
  (* We may already know the type of this expression from parsing.
   * Those are hard constraints and are not named: *)
  Option.may (fun t ->
    let name = make_name e "KNOWNTYPE" in
    emit_assert_id_eq_typ ~name tuple_sizes eid oc t
  ) (typ_of e).scalar_typ ;
  Option.may (fun n ->
    let name = make_name e "KNOWNNULL" in
    emit_assert_id_is_bool ~name nid oc n
  ) (typ_of e).nullable ;
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
            emit_assert_id_eq_id eid oc (e_of_expr expr) ;
            (* Some tuples are passed to callback via an option type, and
             * are None each time they are undefined (beginning of worker
             * or of group). Workers access those fields via a special
             * functions, and all fields are forced nullable during typing.
             *)
            if !tupref = TupleGroupPrevious ||
               !tupref = TupleOutPrevious
            then
              let name = make_name e "PREVNULL" in
              emit_assert_is_true ~name oc nid
            else
              emit_assert_id_eq_id nid oc (n_of_expr expr))

  | Const (_, _) | StateField _ -> ()

  | Tuple (_, es) ->
      (* Identify each element to identifier used for it: *)
      let d = List.length es in
      List.iteri (fun i e ->
        emit_assert_id_eq_smt2 (e_of_expr e) oc
          (Printf.sprintf "(tuple%d-e%d %s)" d i eid) ;
        emit_assert_id_eq_smt2 (n_of_expr e) oc
          (Printf.sprintf "(tuple%d-n%d %s)" d i eid)
      ) es

  | Vector (_, es) ->
      (* Typing rules:
       * - Every element in es must have the same sort;
       * - Every element in es must have the same nullability (FIXME:
       *   couldn't we "promote" non nullable to nullable?);
       * - The resulting type is a vector of that size with a type not
       *   smaller then any of the elements;
       * - The resulting type is *not* nullable since it has a literal value
       *   (known since parsing);
       * - FIXME: If the vector is of length 0, it can have any type *)
      (match es with
      | [] ->
        (* Empty vector literal are not accepted by the parser yet... *)
        emit_assert oc (fun oc ->
          Printf.fprintf oc
            "(or ((_ is vector) %s) ((_ is list) %s))"
            eid eid)
      | fst :: rest ->
        let d = List.length es in
        List.iter (fun e ->
          let name = make_name e "VECSAME" in
          emit_assert ~name oc (fun oc ->
            Printf.fprintf oc "(and %a %a)"
              (emit_id_eq_id (n_of_expr e)) (n_of_expr fst)
              (emit_id_le_smt2 (e_of_expr e))
                (Printf.sprintf "(vector-type %s)" eid))
        ) es ;
        emit_assert oc (fun oc ->
          Printf.fprintf oc
            "(or (and ((_ is vector) %s) \
                      (= %d (vector-dim %s)) \
                      (= %s (vector-nullable %s))) \
                 (and ((_ is list) %s) \
                      (= %s (list-nullable %s))))"
                      eid
                      d eid
                      (n_of_expr fst) eid
                      eid
                      (n_of_expr fst) eid))

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
      List.iteri (fun i { case_cond = cond ; case_cons = cons } ->
        let name = make_name e ("CASE_COND_"^ string_of_int i ^"_BOOL") in
        emit_assert_id_eq_typ ~name tuple_sizes (e_of_expr cond) oc TBool ;
        let name = make_name e ("CASE_CONS_"^ string_of_int i) in
        emit_assert_id_le_id ~name (e_of_expr cons) oc eid
      ) cases ;
      Option.may (fun else_ ->
        let name = make_name e "CASE_ELSE" in
        emit_assert_id_le_id ~name (e_of_expr else_) oc eid
      ) else_ ;
      if cases <> [] then (
        let name = make_name e "CASE_NULL_PROPAGATION" in
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
       * - All elements of the list but the last must be nullable ;
       * - The last element of the list must not be nullable. *)
      let len = List.length es in
      List.iteri (fun i e ->
        let name = make_name e ("COALESCE_ALTERNATIVE_"^ string_of_int i) in
        emit_assert_id_le_id ~name (e_of_expr e) oc eid ;
        let name = make_name e ("COALESCE_NULL_IFF_LAST_"^ string_of_int i) in
        let is_last = i = len - 1 in
        emit_assert_id_is_bool ~name (n_of_expr e) oc (not is_last)
      ) es ;

  | StatelessFun0 (_, (Now|Random))
  | StatelessFun1 (_, Defined, _) -> ()

  | StatefulFun (_, _, (AggrMin e|AggrMax e)) ->
      (* e must be sortable, and the result has its type *)
      emit_assert_sortable oc e ;
      emit_assert_id_eq_id (e_of_expr e) oc eid ;
      emit_assert_id_eq_id (n_of_expr e) oc nid

  | StatefulFun (_, _, (AggrFirst e|AggrLast e)) ->
      (* e must have the same type as the result: *)
      emit_assert_id_eq_id (e_of_expr e) oc eid ;
      emit_assert_id_eq_id (n_of_expr e) oc nid

  | StatefulFun (_, _, (AggrSum e|AggrAvg e)) ->
      (* The result must not be smaller than e *)
      emit_assert_numeric oc e ;
      emit_assert_id_le_id (e_of_expr e) oc eid ;
      emit_assert_id_eq_id nid oc (n_of_expr e)

  | StatelessFun1 (_, Minus, e) ->
      (* - The only argument must be numeric;
       * - The sign of the result is the inverse of the sign or e. *)
      emit_assert_numeric oc e ;
      let name = make_name e "INVSIGN" in
      emit_assert ~name oc (fun oc ->
        Printf.fprintf oc
          "(ite (= 0 (sign %s)) (= 1 (sign %s)) (= 0 (sign %s)))"
          (e_of_expr e) eid eid) ;
      emit_assert_id_eq_id nid oc (n_of_expr e)

  | StatelessFun1 (_, (Age|Abs), e) ->
      (* The only argument must be numeric: *)
      emit_assert_numeric oc e ;
      emit_assert_id_eq_id nid oc (n_of_expr e)

  | StatefulFun (_, _, (AggrAnd e | AggrOr e))
  | StatelessFun1 (_, Not, e) ->
      (* The only argument must be boolean: *)
      let name = make_name e "BOOL" in
      emit_assert_id_eq_typ ~name tuple_sizes (e_of_expr e) oc TBool ;
      emit_assert_id_eq_id nid oc (n_of_expr e)

  | StatelessFun1 (_, Cast, e) ->
      (* No type restriction on the operand: we might want to forbid some
       * types at some point, for instance strings... Some cast are
       * actually not implemented so would fail when generating code. *)
      emit_assert_id_eq_id nid oc (n_of_expr e)

  | StatefulFun (_, _, AggrPercentile (e1, e2)) ->
      (* - e1 and e2 must be numeric;
       * - The result has same type as e2. *)
      emit_assert_numeric oc e1 ;
      emit_assert_numeric oc e2 ;
      emit_assert_id_eq_id eid oc (e_of_expr e2) ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))

  | StatelessFun2 (_, (Add|Sub|Mul|Div|IDiv|Pow), e1, e2) ->
      (* - e1 and e2 must be numeric;
       * - The result is not smaller than e1 or e2. *)
      emit_assert_numeric oc e1 ;
      emit_assert_numeric oc e2 ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2)) ;
      emit_assert_id_le_id (e_of_expr e1) oc eid ;
      emit_assert_id_le_id (e_of_expr e2) oc eid
      (* TODO: for IDiv, have a TInt type and make_int_typ when parsing *)

  | StatelessFun2 (_, (Concat|StartsWith|EndsWith), e1, e2)
  | GeneratorFun (_, Split (e1, e2)) ->
      (* e1 and e2 must be strings: *)
      let name = make_name e1 "STRING" in
      emit_assert_id_eq_typ ~name tuple_sizes (e_of_expr e1) oc TString ;
      let name = make_name e2 "STRING" in
      emit_assert_id_eq_typ ~name tuple_sizes (e_of_expr e2) oc TString ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))

  | StatelessFun2 (_, Strftime, e1, e2) ->
      (* e1 must be a string and e2 a float (ideally, a time): *)
      let name = make_name e1 "STRING" in
      emit_assert_id_eq_typ ~name tuple_sizes (e_of_expr e1) oc TString ;
      let name = make_name e2 "FLOAT" in
      emit_assert_id_eq_typ ~name tuple_sizes (e_of_expr e2) oc TFloat ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))

  | StatelessFun1 (_, (Length|Lower|Upper), e)
  | StatelessFunMisc (_, Like (e, _)) ->
      (* e must be a string: *)
      let name = make_name e "STRING" in
      emit_assert_id_eq_typ ~name tuple_sizes (e_of_expr e) oc TString ;
      emit_assert_id_eq_id nid oc (n_of_expr e)

  | StatelessFun1 (_, Strptime, e) ->
      (* - e must be a string;
       * - Result is always nullable (known from the parse). *)
      let name = make_name e "STRING" in
      emit_assert_id_eq_typ ~name tuple_sizes (e_of_expr e) oc TString

  | StatelessFun2 (_, Mod, e1, e2)
  | StatelessFun2 (_, (BitAnd|BitOr|BitXor), e1, e2) ->
      (* - e1 and e2 must be any integer;
       * - The result must not be smaller that e1 nor e2. *)
      emit_assert_integer oc e1 ;
      emit_assert_integer oc e2 ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2)) ;
      emit_assert_id_le_id (e_of_expr e1) oc eid ;
      emit_assert_id_le_id (e_of_expr e2) oc eid

  | StatelessFun2 (_, (Ge|Gt), e1, e2) ->
      (* e1 and e2 must have the same sort, and be either strings, numeric,
       * IP or CIDR (aka a sortable): *)
      emit_assert_same e oc (e_of_expr e1) (e_of_expr e2) ;
      emit_assert_sortable oc e1 ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))

  | StatelessFun2 (_, Eq, e1, e2) ->
      (* e1 and e2 must be of the same sort *)
      emit_assert_same e oc (e_of_expr e1) (e_of_expr e2) ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))

  | StatelessFun2 (_, (And|Or), e1, e2) ->
      (* e1 and e2 must be booleans. *)
      let name = make_name e1 "BOOL" in
      emit_assert_id_eq_typ ~name tuple_sizes (e_of_expr e1) oc TBool ;
      let name = make_name e2 "BOOL" in
      emit_assert_id_eq_typ ~name tuple_sizes (e_of_expr e2) oc TBool ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s)" (n_of_expr e1) (n_of_expr e2))

  | StatelessFun1 (_, Nth n, e) ->
      (* Typing rules:
       * - e must be a tuple of at least n elements;
       * - the resulting type is that if the n-th element. *)
      let eid' = e_of_expr e in
      let name = make_name e "TUPLE" in
      emit_assert ~name oc (fun oc ->
        Printf.fprintf oc "(or %a)"
          (List.print ~first:"" ~last:"" ~sep:" " (fun oc sz ->
            if sz > n then
              Printf.fprintf oc "(and %a \
                                      (= (tuple%d-e%d %s) %s) \
                                      (= (tuple%d-n%d %s) %s))"
                (emit_is_tuple eid') sz
                sz n eid' eid
                sz n eid' nid))
            tuple_sizes)

  | StatelessFun2 (_, VecGet, n, e) ->
      (* TODO: replaces NTH entirely? NTH(n) == Get(n-1) *)
      (* Typing rules:
       * - e must be a vector or a list;
       * - n must be an unsigned;
       * - if e is a vector and n is a constant, then n must
       *   be less than its length;
       * - the resulting type is the same as the selected type. *)
      emit_assert_numeric oc n ;
      let name = make_name e "GETTABLE" in
      (match int_of_const n with
      | None ->
          emit_assert ~name oc (fun oc ->
            Printf.fprintf oc "(let ((tmp %s)) \
                                 (or (and ((_ is vector) tmp) \
                                          (= (vector-type tmp) %s))\n\
                                     (and ((_ is list) tmp) \
                                          (= (list-type tmp) %s))))"
              (e_of_expr e) eid eid)
      | Some n ->
          emit_assert ~name oc (fun oc ->
            Printf.fprintf oc "(let ((tmp %s)) \
                                 (or (and ((_ is vector) tmp) \
                                          (> (vector-dim tmp) %d)
                                          (= (vector-type tmp) %s))
                                     (and ((_ is list) tmp) \
                                          (= (list-type tmp) %s))))"
              (e_of_expr e) n eid eid)) ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s)" (n_of_expr n) (n_of_expr e))

  | StatelessFun1 (_, (BeginOfRange|EndOfRange), e) ->
      (* e is any kind of cidr *)
      let name = make_name e "CIDR" in
      emit_assert ~name oc (fun oc ->
        Printf.fprintf oc "((_ is cidr) %s)" (e_of_expr e)) ;
      emit_assert_id_eq_id nid oc (n_of_expr e)

  | StatelessFunMisc (_, (Min es | Max es)) ->
      (* Typing rules:
       * - es must be a list of expressions of compatible types;
       * - the result type is the largest of them all;
       * - The result type must be a sortable;
       * - If any of the es is nullable then so is the result. *)
      List.iter (fun e ->
        emit_assert_id_le_id (e_of_expr e) oc eid
      ) es ;
      emit_assert_sortable oc e ;
      if es <> [] then
        emit_assert_id_eq_smt2 nid oc
          (Printf.sprintf2 "(or %a)"
            (List.print ~first:"" ~last:"" ~sep:" " (fun oc e ->
              String.print oc (n_of_expr e))) es)

  | StatelessFunMisc (_, Print es) ->
      (* The result must have the same type as the first parameter *)
      emit_assert_id_eq_id (e_of_expr (List.hd es)) oc eid ;
      emit_assert_id_eq_id (n_of_expr (List.hd es)) oc nid

  | StatefulFun (_, _, Lag (e1, e2)) ->
      (* Typing rules:
       * - e1 must be an unsigned;
       * - e2 has same type as the result;
       * - The result is only nullable by propagation - if the lag goes
       *   beyond the start of the window then lag merely returns the oldest
       *   value. *)
      emit_assert_integer oc e1 ;
      emit_assert_id_eq_id (e_of_expr e2) oc eid ;
      emit_assert_id_eq_id (n_of_expr e2) oc nid

  | StatefulFun (_, _, MovingAvg (e1, e2, e3))
  | StatefulFun (_, _, LinReg (e1, e2, e3)) ->
      (* Typing rules:
       * - e1 must be an unsigned (the period);
       * - e2 must also be an unsigned (the number of values to average);
       * - e3 must be numeric;
       * - Neither e1 or e2 can be NULL. *)
      emit_assert_unsigned oc e1 ;
      emit_assert_unsigned oc e2 ;
      emit_assert_numeric oc e3 ;
      let name = make_name e1 "NOTNULL" in
      emit_assert_is_false ~name oc (n_of_expr e1) ;
      let name = make_name e2 "NOTNULL" in
      emit_assert_is_false ~name oc (n_of_expr e2) ;
      emit_assert_id_eq_id (n_of_expr e3) oc nid

  | StatefulFun (_, _, MultiLinReg (e1, e2, e3, e4s)) ->
      (* As above, with the addition of predictors that must also be
       * numeric and non null. Why non null? See comment in check_variadic
       * and probably get rid of this limitation. *)
      emit_assert_unsigned oc e1 ;
      emit_assert_unsigned oc e2 ;
      emit_assert_numeric oc e3 ;
      let name = make_name e1 "NOTNULL" in
      emit_assert_is_false ~name oc (n_of_expr e1) ;
      let name = make_name e2 "NOTNULL" in
      emit_assert_is_false ~name oc (n_of_expr e2) ;
      emit_assert_id_eq_id (n_of_expr e3) oc nid ;
      List.iter (fun e ->
        emit_assert_numeric oc e ;
        let name = make_name e "NOTNULL" in
        emit_assert_is_false ~name oc (n_of_expr e)
      ) e4s

  | StatefulFun (_, _, ExpSmooth (e1, e2)) ->
      (* Typing rules:
       * - e1 must be a non-null float (and ideally, between 0 and 1), but
       *   we just ask for a numeric in order to also accept immediate
       *   values parsed as integers, and integer fields;
       * - e2 must be numeric *)
      let name = make_name e1 "FLOAT" in
      emit_assert_id_eq_typ ~name tuple_sizes (e_of_expr e1) oc TFloat ;
      emit_assert_numeric oc e2 ;
      let name = make_name e1 "NOTNULL" in
      emit_assert_is_false ~name oc (n_of_expr e1) ;
      emit_assert_id_eq_id (n_of_expr e2) oc nid

  | StatelessFun1 (_, (Exp|Log|Log10|Sqrt|Floor|Ceil|Round), e) ->
      (* e must be numeric *)
      emit_assert_numeric oc e ;
      emit_assert_id_eq_id (n_of_expr e) oc nid

  | StatelessFun1 (_, Hash, e) ->
      (* e can be anything. Notice that hash(NULL) is NULL. *)
      emit_assert_id_eq_id (n_of_expr e) oc nid

  | StatelessFun1 (_, Sparkline, e) ->
      (* e must be a vector of numerics *)
      let name = make_name e "NUMERIC_VEC" in
      emit_assert_id_eq_typ ~name tuple_sizes (e_of_expr e) oc
        (TVec (0, { structure = TNum ; nullable = Some false })) ;
      emit_assert_id_eq_id (n_of_expr e) oc nid

  | StatefulFun (_, _, Remember (fpr, tim, dur, es)) ->
      (* Typing rules:
       * - fpr must be a non null (positive) float, so we take any numeric
       *   for now;
       * - time must be a time, so ideally a float, but again we accept any
       *   integer (so that a int field is OK);
       * - dur must be a duration, so a numeric again;
       * - expressions in es can be anything at all;
       * - the result is as nullable as any of tim, dur and the es. *)
      emit_assert_numeric oc fpr ;
      emit_assert_numeric oc tim ;
      emit_assert_numeric oc dur ;
      let name = make_name fpr "NOTNULL" in
      emit_assert_is_false ~name oc (n_of_expr fpr) ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf2 "(or %s %s%a)"
          (n_of_expr tim) (n_of_expr dur)
          (List.print ~first:" " ~last:"" ~sep:" " (fun oc e ->
            String.print oc (n_of_expr e))) es)

  | StatefulFun (_, _, Distinct es) ->
      (* the es can be anything *)
      if es <> [] then
        emit_assert_id_eq_smt2 nid oc
          (Printf.sprintf2 "(or %a)"
            (List.print ~first:"" ~last:"" ~sep:" " (fun oc e ->
              String.print oc (n_of_expr e))) es)

  | StatefulFun (_, _, Hysteresis (meas, accept, max)) ->
      (* meas, accept and max must be numeric. *)
      emit_assert_numeric oc meas ;
      emit_assert_numeric oc accept ;
      emit_assert_numeric oc max ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf "(or %s %s %s)"
          (n_of_expr meas) (n_of_expr accept) (n_of_expr max))

  | StatefulFun (_, _, Top { want_rank ; what ; by ; n ; duration ; time }) ->
      (* Typing rules:
       * - what can be anything;
       * - by must be numeric;
       * - time must be a time (numeric);
       * - If we want the rank then the result is nullable (known at
       *   parsing time) ;
       * - Otherwise, nullability is inherited from what and by. *)
      emit_assert_numeric oc by ;
      emit_assert_numeric oc time ;
      if not want_rank then
        emit_assert_id_eq_smt2 nid oc
          (Printf.sprintf2 "(or%a %s)"
            (List.print ~first:" " ~last:"" ~sep:" " (fun oc w ->
              String.print oc (n_of_expr w))) what
            (n_of_expr by))

  | StatefulFun (_, _, Last (n, e, es)) ->
      (* - The type of the return is a vector of the specified length,
       *   with items of the type of e, and is nullable;
       * - In theory, 'Last n e1 by es` should be nullable iff any of the es
       *   is nullable, and become and stays null forever as soon as one es
       *   is actually NULL. This is kind of useless, so we just disallow
       *   ordering by a nullable field;
       * - The Last itself is null whenever the number of received item is
       *   less than n. *)
      emit_assert_id_eq_smt2 eid oc
        (Printf.sprintf "(vector %d %s true)" n (e_of_expr e)) ;
      List.iter (fun e ->
        let name = make_name e "NOTNULL" in
        emit_assert_is_false ~name oc (n_of_expr e)
      ) es ;
      let name = make_name e "NULL" in
      emit_assert_is_true ~name oc nid

  | StatefulFun (_, _, AggrHistogram (e, _, _, _)) ->
      (* e must be numeric *)
      emit_assert_numeric oc e ;
      emit_assert_id_eq_id (n_of_expr e) oc nid

  | StatelessFun2 (_, In, e1, e2) ->
      (* Typing rule:
       * - e2 can be a string, a cidr, a list or a vector;
       * - if e2 is a string, then e1 must be a string;
       * - if e2 is a cidr, then e1 must be an ip (TODO: either of the same
       *   version or either the cidr or the ip must have version 9
       *   (generic));
       * - if e2 is either a list of a vector, then e1 must have the sort
       *   of the elements of this list or vector;
       * - Result is as nullable as e1, e2 and the content of e2 if e2
       *   is a vector or a list. *)
      let name = make_name e2 "IN_TYPE" in
      emit_assert ~name oc (fun oc ->
        let id1 = e_of_expr e1 and id2 = e_of_expr e2 in
        Printf.fprintf oc
          "(or (and (= string %s) (= string %s)) \
               (and ((_ is cidr) %s) ((_ is ip) %s)) \
               (and ((_ is list) %s) %a) \
               (and ((_ is vector) %s) %a))"
          id2 id1
          id2 id1
          id2 (emit_same id1) (Printf.sprintf "(list-type %s)" id2)
          id2 (emit_same id1) (Printf.sprintf "(vector-type %s)" id2)) ;
      emit_assert_id_eq_smt2 nid oc
        (Printf.sprintf2
          "(or %s %s (and ((_ is list) %s) (list-nullable %s)) \
                     (and ((_ is vector) %s) (vector-nullable %s)))"
          (n_of_expr e1) (n_of_expr e2)
          (e_of_expr e2) (e_of_expr e2)
          (e_of_expr e2) (e_of_expr e2))

let emit_operation declare tuple_sizes fi oc func =
  let open RamenOperation in
  (* Now add specific constraints depending on the clauses: *)
  (match Option.get func.Func.operation with
  | Aggregate { fields ; where ; event_time ; notifications ;
                commit_when ; flush_how ; _ } as op ->
      RamenOperation.iter_expr (fun e ->
        declare e ;
        emit_constraints tuple_sizes fields oc e
      ) op ;
      (* Typing rules:
       * - where must be a bool;
       * - commit-when must also be a bool;
       * - flush_how conditions must also be bools. *)
      let name = "F"^ string_of_int fi ^"_WHERE" in
      emit_assert_id_eq_typ ~name tuple_sizes (e_of_expr where) oc TBool ;
      let name = "F"^ string_of_int fi ^"_COMMIT" in
      emit_assert_id_eq_typ ~name tuple_sizes (e_of_expr commit_when) oc TBool ;
      (match flush_how with
      | Reset | Never | Slide _ -> ()
      | RemoveAll e | KeepOnly e ->
          let name = "F"^ string_of_int fi ^"_FLUSH" in
          emit_assert_id_eq_typ ~name tuple_sizes (e_of_expr e) oc TBool)
  | _ -> ())

let emit_program declare tuple_sizes oc funcs =
  (* Output all the constraints for all the operations: *)
  List.iteri (fun i func ->
    Printf.fprintf oc "\n; Constraints for function %s\n"
      (RamenName.string_of_func func.Func.name) ;
    emit_operation declare tuple_sizes i oc func
  ) funcs

type id_or_type = Id of int | FieldType of RamenTuple.field_typ
let id_or_type_of_field op name =
  let open RamenOperation in
  let find_field_type = List.find (fun ft -> ft.RamenTuple.typ_name = name) in
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

(* Reading already compiled parents, set the type of fields originating from
 * external parents, parameters and environment, once and for all: *)
let type_const_input_fields conf parents params funcs =
  List.iter (fun func ->
    let parents = Hashtbl.find_default parents func.Func.name [] in
    RamenOperation.iter_expr (function
      | Field (field_typ, tupref, field_name) ->
          if is_virtual_field field_name then (
            (* Type set during parsing *)
          ) else if !tupref = TupleParam then (
            (* Copy the scalar type from the default value: *)
            match RamenTuple.params_find field_name params with
            | exception Not_found ->
                Printf.sprintf "Function %s is using unknown parameter %s"
                  (RamenName.string_of_func func.Func.name) field_name |>
                failwith
            | param ->
                field_typ.nullable <- param.ptyp.typ.nullable ;
                field_typ.scalar_typ <- Some param.ptyp.typ.structure
          ) else if !tupref = TupleEnv then (
            field_typ.nullable <- Some true ;
            field_typ.scalar_typ <- Some TString
          ) else if RamenLang.tuple_has_type_input !tupref then (
            let no_such_field pfunc =
              Printf.sprintf "Parent %s of %s does not output a field \
                              named %s"
                (RamenName.string_of_func pfunc.Func.name)
                (RamenName.string_of_func func.Func.name)
                field_name |>
              failwith
            and aggr_types t = function
              | None -> Some t
              | (Some prev_t) as prev_typ->
                  if t <> prev_t then
                    Printf.sprintf "All parents of %s must agree on \
                                    the type of field %s"
                      (RamenName.string_of_func func.Func.name)
                      field_name |>
                    failwith ;
                  prev_typ in
            (* Check that every parents have this field with the same type,
             * and set this field type to that: *)
            let typ =
              List.fold_left (fun prev_typ pfunc ->
                if typing_is_finished pfunc.Func.out_type then (
                  match List.find (fun fld ->
                          fld.RamenTuple.typ_name = field_name
                        ) (typed_tuple_type (pfunc.Func.out_type)).ser with
                  | exception Not_found -> no_such_field pfunc
                  | t ->
                      aggr_types t prev_typ
                ) else (
                  (* Typing not finished? This parent is in this very program
                   * then. Output the constraint to bind the input type to
                   * the output type: *)
                  (* Retrieve the id for the parent output fields: *)
                  match id_or_type_of_field
                          (Option.get pfunc.Func.operation) field_name with
                  | exception Not_found -> no_such_field pfunc
                  | Id p_id -> prev_typ
                  | FieldType t -> aggr_types t prev_typ
                )
              ) None parents in
            Option.may (fun t ->
              field_typ.nullable <- t.RamenTuple.typ.nullable ;
              field_typ.scalar_typ <- Some t.RamenTuple.typ.structure) typ ;
          )
      | _ -> ()
    ) (Option.get func.Func.operation)
  ) funcs

(* Equals the input type of fields originating from internal parents to
 * those output fields. *)
let emit_input_fields oc parents funcs =
  List.iter (fun func ->
    let parents = Hashtbl.find_default parents func.Func.name [] in
    RamenOperation.iter_expr (function
      | Field (field_typ, tupref, field_name) as expr ->
          if is_virtual_field field_name then (
            (* Type set during parsing *)
          ) else if RamenLang.tuple_has_type_input !tupref then (
            let no_such_field pfunc =
              Printf.sprintf "Parent %s of %s does not output a field \
                              named %s"
                (RamenName.string_of_func pfunc.Func.name)
                (RamenName.string_of_func func.Func.name)
                field_name |>
              failwith in
            let same_as_ids =
              List.fold_left (fun same_as_ids pfunc ->
                if typing_is_finished pfunc.Func.out_type then same_as_ids
                else (
                  (* Typing not finished? This parent is in this very program
                   * then. Output the constraint to bind the input type to
                   * the output type: *)
                  (* Retrieve the id for the parent output fields: *)
                  match id_or_type_of_field
                          (Option.get pfunc.Func.operation) field_name with
                  | exception Not_found -> no_such_field pfunc
                  | Id p_id -> p_id::same_as_ids
                  | FieldType t -> same_as_ids
                )
              ) [] parents in
            List.iter (fun id ->
              emit_assert_id_eq_id (e_of_expr expr) oc (e_of_num id) ;
              emit_assert_id_eq_id (n_of_expr expr) oc (n_of_num id) ;
            ) same_as_ids
          )
      | _ -> ()
    ) (Option.get func.Func.operation)
  ) funcs

let structure_of_sort_identifier = function
  | "bool" -> TBool
  | "string" -> TString
  | "eth" -> TEth
  | unk ->
      !logger.error "Unknown sort identifier %S" unk ;
      TEmpty

let bool_of_term =
  let open RamenSmtParser in
  function
  | QualIdentifier ((Identifier "true", None), []) ->
      Some true
  | QualIdentifier ((Identifier "false", None), []) ->
      Some false
  | x ->
      !logger.error "Bad term when expecting boolean: %a"
        print_term x ;
      None

let rec structure_of_term =
  let open RamenSmtParser in
  function
  | QualIdentifier ((Identifier typ, None), []) ->
      structure_of_sort_identifier typ
  | QualIdentifier ((Identifier "number", None),
                    [ ConstantTerm sign ; ConstantTerm width ]) ->
      let sign = int_of_constant sign > 0
      and width = int_of_constant width in
      if width > 128 then TFloat else
      if width > 64 then if sign then TI128 else TU128 else
      if width > 32 then if sign then TI64 else TU64 else
      if width > 16 then if sign then TI32 else TU32 else
      if width > 8 then if sign then TI16 else TU16 else
      if sign then TI8 else TU8
  | QualIdentifier ((Identifier "ip", None),
                    [ ConstantTerm version ]) ->
      let version = int_of_constant version in
      if version > 6 then TIp else
      if version > 4 then TIpv6 else TIpv4
  | QualIdentifier ((Identifier "cidr", None),
                    [ ConstantTerm version ]) ->
      let version = int_of_constant version in
      if version > 6 then TCidr else
      if version > 4 then TCidrv6 else TCidrv4
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
        let nb_sub_terms = List.length sub_terms in
        (* We should have one term for structure and one for nullability: *)
        if nb_sub_terms <> 2 * sz then
          Printf.sprintf "Bad number of sub_terms (%d) for tuple%d"
            nb_sub_terms sz |>
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
      print_exception e ;
      TEmpty)
  | _ ->
      !logger.warning "TODO: exploit define-fun with funny term" ;
      TEmpty

let emit_smt2 oc ~optimize parents tuple_sizes funcs =
  (* We have to start the SMT2 file with declarations before we can
   * produce any assertion: *)
  let decls = IO.output_string () in
  (* We might encounter several times the same expression (for the well
   * known constants defined once in RamenExpr, such as expr_true etc...
   * So we keep a set of already declared ids: *)
  let ids = ref Set.Int.empty in
  let declare e =
    let id = (typ_of e).uniq_num in
    if not (Set.Int.mem id !ids) then (
      ids := Set.Int.add id !ids ;
      let eid = e_of_expr e in
      Printf.fprintf decls
        "; %a\n\
         (declare-fun %s () Type)\n\
         (declare-fun %s () Bool)\n\
         (assert (ite ((_ is ip) %s) \
                      (or (= 4 (ip-version %s)) \
                          (= 6 (ip-version %s)) \
                          (= 9 (ip-version %s))) \
                 (ite ((_ is cidr) %s) \
                      (or (= 4 (cidr-version %s)) \
                          (= 6 (cidr-version %s)) \
                          (= 9 (cidr-version %s))) \
                 (ite ((_ is number) %s) \
                      (and (or (= 0 (sign %s)) (= 1 (sign %s)))
                           (or (= 8 (width %s)) \
                               (= 16 (width %s)) \
                               (= 32 (width %s)) \
                               (= 64 (width %s)) \
                               (= 128 (width %s)) \
                               (= 129 (width %s))))
                 (ite ((_ is vector) %s)
                      (>= (vector-dim %s) 0)
                 true)))))\n"
        (RamenExpr.print true) e
        eid (n_of_expr e)
        eid eid eid eid eid eid eid eid eid eid
        eid eid eid eid eid eid eid eid eid ;
      if optimize then
        Printf.fprintf decls
          "(minimize \
             (ite ((_ is number) %s) (width %s) \
               (ite ((_ is ip) %s) (ip-version %s) \
                 (ite ((_ is cidr) %s) (cidr-version %s) 0))))\n\
           (minimize (ite ((_ is number) %s) (sign %s) 0))\n"
          eid eid eid eid eid eid eid eid
    ) in
  let expr_types = IO.output_string () in
  let parent_types = IO.output_string () in
  (* Set the types for all fields from parents, params or env: *)
  emit_input_fields parent_types parents funcs ;
  emit_program declare tuple_sizes expr_types funcs ;
  Printf.fprintf oc
    "(set-option :print-success false)\n\
     (set-option :produce-unsat-cores true)\n\
     (set-option :produce-models true)\n\
     (set-logic ALL) ; TODO\n\
     ; Define a sort for types:\n\
     (declare-datatypes\n\
       ( (Type 0) )\n\
       ( ((bool) (string) (eth)\n\
          ; Available versions are 4 (IPv4) and 6 (IPv6) or 9 (generic)\n\
          (ip (ip-version Int))\n\
          (cidr (cidr-version Int))\n\
          ; We will optimize for the less sign-bits and the less width.\n\
          ; sign should really be a bool but Z3 don't know how to minimize\n\
          ; bools. Floats are represented as a width of >128.\n\
          (number (sign Int) (width Int))\n\
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
     (check-sat)\n\
     (get-unsat-core)\n\
     (get-model)\n"
    (if optimize then "true" else "false")
    (List.print ~first:"" ~last:"" ~sep:"\n" (fun oc sz ->
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
  with_subprocess shell args (fun (oc, ic, ec) ->
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

let unsat syms output =
  !logger.error "Solver output:\n%s" output ;
  (* TODO: a better error message *)
  Printf.sprintf2 "Cannot solve typing constraints %a"
    (List.print String.print) syms |>
  failwith

(* Return the type of the given output field.
 * We already know (from the solver) that all parents export the same
 * type. *)
let type_of_parents_field parents tuple_prefix name =
  if parents = [] then
    Printf.sprintf "Cannot use input field %s without any parent"
      name |>
    failwith ;
  let parent = List.hd parents in
  let find_field () =
    match parent.Func.out_type with
    | UntypedTuple untyped_tuple ->
      !logger.debug "Look for %s in %a"
        name
        (List.print (fun oc (n,_) -> String.print oc n))
          untyped_tuple.fields ;
      let etyp = List.assoc name untyped_tuple.fields in
      (* Output types have been set already: *)
      { structure = Option.get etyp.scalar_typ ;
        nullable = etyp.nullable }
    | TypedTuple { user ; _ } ->
      !logger.debug "Look for %s in %a"
        name
        (List.print (fun oc f -> String.print oc f.RamenTuple.typ_name))
          user ;
      List.find_map (fun f ->
        if f.RamenTuple.typ_name = name
        then Some f.typ
        else None
      ) user in
  try find_field ()
  with Not_found ->
    Printf.sprintf "Cannot find field %s in parent %s"
      name (RamenName.string_of_func parent.name) |>
    failwith

let set_io_tuples parents funcs h =
  (* Sometime we just know the types (CSV, Instrumentation, Protocols...): *)
  let set_type tuple typ =
    tuple.RamenTypingHelpers.fields <-
      List.map (fun ft ->
        ft.RamenTuple.typ_name,
        make_typ ?nullable:ft.typ.nullable ~typ:ft.typ.structure
                 ft.typ_name
      ) typ in
  (* Set i/o types of func without considering and_all_others (star): *)
  let set_non_star_outputs func =
    let out_type = untyped_tuple_type func.Func.out_type in
    match Option.get func.Func.operation with
    | Aggregate { fields ; _ } ->
        out_type.fields <- List.map (fun sf ->
          let id = (typ_of sf.RamenOperation.expr).uniq_num in
          let t = Hashtbl.find h id in
          !logger.debug "Set output field %s.%s"
            (RamenName.string_of_func func.name) sf.alias ;
          sf.alias, make_typ ?nullable:t.nullable ~typ:t.structure sf.alias
        ) fields
    | ReadCSVFile { what = { fields ; _ } ; _ } ->
        set_type out_type (RingBufLib.ser_tuple_typ_of_tuple_typ fields)
    | ListenFor { proto ; _ } ->
        set_type out_type (RamenProtocols.tuple_typ_of_proto proto)
    | Instrumentation _ ->
        set_type out_type RamenBinocle.tuple_typ
  in
  let set_non_star_inputs func =
    let in_type = untyped_tuple_type func.Func.in_type in
    let parents = Hashtbl.find_default parents func.Func.name [] in
    match Option.get func.Func.operation with
    | Aggregate { fields ; _ } as op ->
        (* For the in_type we have to check that all parents do export each
         * of the mentioned input fields: *)
        RamenOperation.iter_expr (function
          | Field (expr_typ, tuple, name)
            when RamenLang.tuple_has_type_input !tuple &&
                 not (is_virtual_field name) ->
              if is_private_field name then
                Printf.sprintf "In function %s, can not use input field %s, \
                                which is private."
                  (RamenName.string_of_func func.Func.name)
                  name |>
                failwith ;
              if not (List.mem_assoc name in_type.fields) then
                let t =
                  type_of_parents_field parents !tuple name in
                let t =
                  make_typ ?nullable:t.nullable ~typ:t.structure name in
                !logger.debug "Set input field %s.%s"
                  (RamenName.string_of_func func.name) name ;
                in_type.fields <- (name, t) :: in_type.fields
          | _ -> ()) op
    | ReadCSVFile _ -> set_type in_type []
    | ListenFor _ -> set_type in_type []
    | Instrumentation _ -> set_type in_type []
  in
  (* We must set outputs before inputs: *)
  Hashtbl.iter (fun _ -> set_non_star_outputs) funcs ;
  Hashtbl.iter (fun _ -> set_non_star_inputs) funcs ;
  (* handle the stars: *)
  let ok =
    reach_fixed_point ~max_try:100 (fun () ->
      Hashtbl.fold (fun _ func changed ->
        let in_type = untyped_tuple_type func.Func.in_type
        and out_type = untyped_tuple_type func.Func.out_type in
        match Option.get func.Func.operation with
        | Aggregate { and_all_others = true ; _ } ->
            true (* DIE! *)
        | _ ->
            if in_type.finished_typing then changed else (
              in_type.finished_typing <- true ;
              out_type.finished_typing <- true ;
              true)
      ) funcs false) in
  if not ok then failwith "Cannot finish typing STAR fields"

let apply_types parents funcs h =
  set_io_tuples parents funcs h ;
  Hashtbl.iter (fun _ func ->
    RamenOperation.iter_expr (fun e ->
      let t = typ_of e in
      match Hashtbl.find h t.uniq_num with
      | exception Not_found ->
          !logger.warning "No type for expression %a"
            (RamenExpr.print true) e
      | typ ->
          t.scalar_typ <- Some typ.structure ;
          t.nullable <- typ.nullable
    ) (Option.get func.Func.operation)
  ) funcs

let get_types conf parents funcs params =
  let funcs = Hashtbl.values funcs |> List.of_enum in
  let h = Hashtbl.create 71 in
  if funcs <> [] then (
    let tuple_sizes = [ 1; 2; 3; 4; 5; 6; 7; 8; 9 ] (* TODO *) in
    (* So that we can now produce the smt2 script: *)
    let program_name = (List.hd funcs).Func.program_name in
    let smt2_file =
      Printf.sprintf "/tmp/%s.smt2"
        (RamenName.path_of_program program_name) in
    mkdir_all ~is_file:true smt2_file ;
    type_const_input_fields conf parents params funcs ;
    !logger.debug "Writing SMT2 program into %S" smt2_file ;
    File.with_file_out ~mode:[`create; `text; `trunc] smt2_file (fun oc ->
      emit_smt2 oc ~optimize:true parents tuple_sizes funcs) ;
    match run_solver smt2_file with
    | RamenSmtParser.Solved (model, sure), output ->
        if not sure then
          !logger.warning "Solver best idea after timeout:\n%s" output ;
        (* Output a hash of structure*nullability per expression id: *)
        let open RamenSmtParser in
        List.iter (fun ((sym, vars, sort, term), _recurs) ->
          try Scanf.sscanf sym "%[en]%d%!" (fun en id ->
            match vars, sort, en with
            | [], NonParametricSort (Identifier "Type"), "e" ->
                let structure = structure_of_term term in
                Hashtbl.modify_opt id (function
                  | None -> Some { structure ; nullable = None }
                  | Some prev -> Some { prev with structure }
                ) h
            | [], NonParametricSort (Identifier "Bool"), "n" ->
                let nullable = bool_of_term term in
                Hashtbl.modify_opt id (function
                  | None -> Some { structure = TAny ; nullable }
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
          emit_smt2 oc ~optimize:false parents tuple_sizes funcs) ;
        (match run_solver smt2_file' with
        | RamenSmtParser.Unsolved syms, output -> unsat syms output
        | RamenSmtParser.Solved _, _ ->
            failwith "Unsat with optimization but sat without?!")
    | RamenSmtParser.Unsolved syms, output -> unsat syms output
  ) ;
  h
