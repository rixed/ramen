(* Typing a ramen program using a SMT solver.
 *
 * Principles:
 *
 * - Typing and nullability are resolved independently.
 *
 * - For nullability, each expression is associated with a single boolean
 *   variable representing the nullability. Each expression then has a
 *   null_constraint function that adds its nullability constraints.
 *   Operations will also add nullability constraints steaming from the
 *   clause (such as: where expression is not nullable) or the parents
 *   (such as: fields coming from parents located in the same program have
 *   same nulability, and fields coming from external parents have some given
 *   nullability).
 *
 * - For types this is the same, but each expression is associated with a value
 *   drawn from a recursive datatype, and we use type (keywords: "theory of
 *   inductive data types"). In case the constraints are satisfiable we get
 *   the assignment (optionally: check that it's the only one by resubmitting
 *   the problem negating the assignment). If it's not we obtain the
 *   unsatisfiable core and build an error message from it (in the future:
 *   we might interact with the solver to devise a better error message as
 *   in https://cs.nyu.edu/wies/publ/finding_minimum_type_error_sources.pdf)
 *
 * - We use SMT-LIB v2.6 format as it supports datatypes.
 *
 * Notes regarding the SMT:
 *
 * - Given all functions are total, we cannot just declare the signature of
 *   our functions and then convert the operation into an smt2 operation ;
 *   even if we introduced an "invalid" type in the sort of types (because
 *   the solver would not try to avoid that value).
 *   So we merely emit assertions for each of our function constraints.
 *
 * - Tuples are encoded using one variable per element, identified by the
 *   tuple identifier suffixed by "_" then the element number (starting at 0).
 *)
open Batteries
open RamenHelpers
open RamenTypingHelpers
open RamenExpr
open RamenTypes
open RamenLog

let smt_solver = ref "z3 -smt2 %s"

let id_of_typ t =
  Printf.sprintf "e%d" t.uniq_num

let id_of_expr = id_of_typ % typ_of

let emit_is_tuple id oc sz =
  Printf.fprintf oc "((_ is tuple%d) %s)" sz id

let rec emit_id_eq_typ tuple_sizes id oc = function
  | TEmpty -> assert false
  | TString -> Printf.fprintf oc "(= string %s)" id
  | TBool -> Printf.fprintf oc "(= bool %s)" id
  | TAny -> Printf.fprintf oc "true"
  | TU8 | TU16 | TU32 | TU64 | TU128
  | TI8 | TI16 | TI32 | TI64 | TI128
  | TNum | TFloat ->
      Printf.fprintf oc "(= number %s)" id
  | TEth -> Printf.fprintf oc "(= eth %s)" id
  | TIpv4 | TIpv6 | TIp ->
      Printf.fprintf oc "(= ip %s)" id
  | TCidrv4 | TCidrv6 | TCidr ->
      Printf.fprintf oc "(= cidr %s)" id
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

let emit_assert_id_eq_typ ?name tuple_sizes id oc t =
  emit_assert ?name oc (fun oc -> emit_id_eq_typ tuple_sizes id oc t)

let emit_assert_id_eq_smt2 ?name id oc smt2 =
  emit_assert ?name oc (fun oc -> Printf.fprintf oc "(= %s %s)" id smt2)

let emit_assert_id_eq_id = emit_assert_id_eq_smt2

let emit_assert_id_eq_any_of_typ ?name tuple_sizes id oc lst =
  emit_assert ?name oc (fun oc ->
    Printf.fprintf oc "(or %a)"
      (List.print ~first:"" ~last:"" ~sep:" " (emit_id_eq_typ tuple_sizes id)) lst)

let emit_assert_id_eq_integer ?name tuple_sizes oc id =
  emit_assert_id_eq_any_of_typ ?name tuple_sizes id oc [ TU8 ; TI8 ]

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

(* Assuming all input/output/constants have been declared already, emit the
 * constraints connecting the parameter to the result: *)
let emit_typing_constraints tuple_sizes out_fields oc e =
  let id = id_of_expr e in
  (* We may already know the type of this expression from typing: *)
  Option.may (fun t ->
    let name = make_name e "KNOWNTYPE" in
    emit_assert_id_eq_typ ~name tuple_sizes id oc t
  ) (typ_of e).scalar_typ ;
  (* This will just output the constraint we know from parsing. Those
   * are hard constraints and are not named: *)
  (* But then we also have specific rules according to the operation at hand:
   *)
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
            emit_assert_id_eq_id id oc (id_of_expr expr))
  | Const (_, _) | StateField _ -> ()
  | Tuple (_, es) ->
      (* Identify each element to identifier used for it: *)
      let d = List.length es in
      List.iteri (fun i e ->
        let smt2 = Printf.sprintf "(tuple%d-t%d %s)" d i id in
        emit_assert_id_eq_smt2 (id_of_expr e) oc smt2
      ) es
  | Vector (_, es) ->
      (* Typing rules:
       * - every element in es must have the same type;
       * - the resulting type is a vector of that size and type;
       * - if the vector is of length 0, it can have any type *)
      let d = List.length es in
      emit_assert oc (fun oc ->
        Printf.fprintf oc "(= %d (vector-dim %s))" d id) ;
      (match es with
      | [] -> ()
      | fst :: rest ->
          Printf.fprintf oc "(assert (= %s (vector-type %s)))\n"
            (id_of_expr fst) id ;
          if rest <> [] then
            let name = make_name e "VECSAMETYPES" in
            emit_assert ~name oc (fun oc ->
              List.print ~first:"(and " ~sep:" " ~last:")"
                (fun oc e ->
                  Printf.fprintf oc "(= %s %s)"
                    (id_of_expr fst) (id_of_expr e))
                oc rest))
  | Case (_, cases, else_) ->
      (* Typing rules:
       * - all conditions must have type bool;
       * - all consequents must have the same type (that of the case);
       * - if present, the else must also have that type. *)
      List.iteri (fun i { case_cond = cond ; case_cons = cons } ->
        let name = make_name e ("CASE_COND_"^ string_of_int i ^"_BOOL") in
        emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr cond) oc TBool ;
        let name = make_name e ("CASE_CONS_"^ string_of_int i) in
        emit_assert_id_eq_id ~name (id_of_expr cons) oc id
      ) cases ;
      Option.may (fun else_ ->
        let name = make_name e "CASE_ELSE" in
        emit_assert_id_eq_id ~name (id_of_expr else_) oc id
      ) else_
  | Coalesce (_, es) ->
      (* Typing rules:
       * - Every alternative must have the same type (that of the case); *)
      List.iteri (fun i e ->
        let name = make_name e ("COALESCE_ALTERNATIVE_"^ string_of_int i) in
        emit_assert_id_eq_id ~name (id_of_expr e) oc id
      ) es

  | StatelessFun0 (_, (Now|Random))
  | StatelessFun1 (_, Defined, _) -> ()
  | StatefulFun (_, _, (AggrMin e|AggrMax e|AggrFirst e|AggrLast e)) ->
      (* e must have the same type as the result: *)
      emit_assert_id_eq_id (id_of_expr e) oc id
  | StatefulFun (_, _, (AggrSum e| AggrAvg e))
  | StatelessFun1 (_, (Age|Abs|Minus), e) ->
      (* The only argument must be numeric: *)
      let name = make_name e "NUMERIC" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr e) oc TNum
  | StatefulFun (_, _, (AggrAnd e | AggrOr e))
  | StatelessFun1 (_, Not, e) ->
      (* The only argument must be boolean: *)
      let name = make_name e "BOOL" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr e) oc TBool
  | StatelessFun1 (_, Cast, e) ->
      (* No type restriction on the operand: we might want to forbid some
       * types at some point, for instance strings... Some cast are
       * actually not implemented so would fail when generating code. *)
      ()
  | StatefulFun (_, _, AggrPercentile (e1, e2))
  | StatelessFun2 (_, (Add|Sub|Mul|Div|IDiv|Pow), e1, e2) ->
      (* e1 and e2 must be numeric: *)
      let name = make_name e1 "NUMERIC" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr e1) oc TNum ;
      let name = make_name e2 "NUMERIC" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr e2) oc TNum
  | StatelessFun2 (_, (Concat|StartsWith|EndsWith), e1, e2)
  | GeneratorFun (_, Split (e1, e2)) ->
      (* e1 and e2 must be strings: *)
      let name = make_name e1 "STRING" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr e1) oc TString ;
      let name = make_name e2 "STRING" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr e2) oc TString
  | StatelessFun2 (_, Strftime, e1, e2) ->
      (* e1 must be a string and e2 a float (ideally, a time): *)
      let name = make_name e1 "STRING" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr e1) oc TString ;
      let name = make_name e2 "FLOAT" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr e2) oc TFloat
  | StatelessFun1 (_, (Strptime|Length|Lower|Upper), e)
  | StatelessFunMisc (_, Like (e, _)) ->
      (* e must be a string: *)
      let name = make_name e "STRING" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr e) oc TString
  | StatelessFun2 (_, Mod, e1, e2)
  | StatelessFun2 (_, (BitAnd|BitOr|BitXor), e1, e2) ->
      (* e1 and e2 must be any integer: *)
      let name = make_name e1 "INTEGER" in
      emit_assert_id_eq_integer ~name tuple_sizes oc (id_of_expr e1) ;
      let name = make_name e2 "INTEGER" in
      emit_assert_id_eq_integer ~name tuple_sizes oc (id_of_expr e2)
  | StatelessFun2 (_, (Ge|Gt), e1, e2) ->
      (* e1 and e2 must have the same type, and be either strings, numeric,
       * IP or CIDR: *)
      let name = make_name e "SAME" in
      emit_assert_id_eq_id ~name (id_of_expr e1) oc (id_of_expr e2) ;
      let name = make_name e "COMPARABLE" in
      emit_assert_id_eq_any_of_typ ~name tuple_sizes (id_of_expr e1) oc
        [ TString ; TNum ; TIp ; TCidr ]
  | StatelessFun2 (_, Eq, e1, e2) ->
      (* e1 and e2 must have the same type *)
      let name = make_name e "SAME" in
      emit_assert_id_eq_id ~name (id_of_expr e1) oc (id_of_expr e2)
  | StatelessFun2 (_, (And|Or), e1, e2) ->
      (* e1 and e2 must be booleans. *)
      let name = make_name e1 "BOOL" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr e1) oc TBool ;
      let name = make_name e2 "BOOL" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr e2) oc TBool
  | StatelessFun1 (_, Nth n, e) ->
      (* Typing rules:
       * - e must be a tuple of at least n elements;
       * - the resulting type is that if the n-th element. *)
      let id' = id_of_expr e in
      let name = make_name e "TUPLE" in
      emit_assert ~name oc (fun oc ->
        Printf.fprintf oc "(or %a)"
          (List.print ~first:"" ~last:"" ~sep:" " (fun oc sz ->
            if sz > n then
              Printf.fprintf oc "(and %a (= (tuple%d-t%d %s) %s))"
                (emit_is_tuple id') sz
                sz n id' id))
            tuple_sizes) ;
  | StatelessFun2 (_, VecGet, n, e) ->
      (* TODO: replaces NTH entirely? NTH(n) == Get(n-1) *)
      (* Typing rules:
       * - e must be a vector or a list;
       * - n must be an unsigned;
       * - if e is a vector and n is a constant, then n must
       *   be less than its length;
       * - the resulting type is the same as the selected type. *)
      let name = make_name n "NUMERIC" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr n) oc TU32 ;
      let name = make_name e "GETTABLE" in
      (match int_of_const n with
      | None ->
          emit_assert ~name oc (fun oc ->
            Printf.fprintf oc "(let ((tmp %s)) \
                                 (or (and ((_ is vector) tmp) \
                                          (= (vector-type tmp) %s))\n\
                                     (and ((_ is list) tmp) \
                                          (= (list-type tmp) %s))))"
              (id_of_expr e) id id)
      | Some n ->
          emit_assert ~name oc (fun oc ->
            Printf.fprintf oc "(let ((tmp %s)) \
                                 (or (and ((_ is vector) tmp) \
                                          (> (vector-dim tmp) %d)
                                          (= (vector-type tmp) %s))
                                     (and ((_ is list) tmp) \
                                          (= (list-type tmp) %s))))"
              (id_of_expr e) n id id))
  | StatelessFun1 (_, (BeginOfRange|EndOfRange), e) ->
      (* e is any kind of cidr *)
      let name = make_name e "CIDR" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr e) oc TCidr
  | StatelessFunMisc (_, (Min es | Max es)) ->
      (* Typing rules:
       * - es must be list of expressions of the same type (that of the
       *   result);
       * - the result type must be a float/string/ip or cidr.
       *   (TODO: why not bool, assuming true > false?) *)
      List.iter (fun e ->
        emit_assert_id_eq_id (id_of_expr e) oc id
      ) es ;
      let name = make_name e "COMPARABLE" in
      emit_assert_id_eq_any_of_typ ~name tuple_sizes id oc
        [ TFloat ; TString ; TIp ; TCidr ]
  | StatelessFunMisc (_, Print es) ->
      (* The result must have the same type as the first parameter *)
      emit_assert_id_eq_id (id_of_expr (List.hd es)) oc id
  | StatefulFun (_, _, Lag (e1, e2)) ->
      (* Typing rules:
       * - e1 must be an unsigned;
       * - e2 has same type as the result. *)
      let name = make_name e1 "INTEGER" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr e1) oc TU32 ;
      emit_assert_id_eq_id (id_of_expr e2) oc id
  | StatefulFun (_, _, MovingAvg (e1, e2, e3))
  | StatefulFun (_, _, LinReg (e1, e2, e3)) ->
      (* Typing rules:
       * - e1 must be an unsigned (the period);
       * - e2 must also be an unsigned (the number of values to average);
       * - e3 must be numeric. *)
      let name = make_name e1 "INTEGER" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr e1) oc TU32 ;
      let name = make_name e2 "INTEGER" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr e2) oc TU32 ;
      let name = make_name e3 "NUMERIC" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr e3) oc TNum
  | StatefulFun (_, _, MultiLinReg (e1, e2, e3, e4s)) ->
      (* As above, with the addition of predictors that must also be
       * numeric. *)
      let name = make_name e1 "INTEGER" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr e1) oc TU32 ;
      let name = make_name e2 "INTEGER" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr e2) oc TU32 ;
      let name = make_name e3 "INTEGER" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr e3) oc TNum ;
      List.iter (fun e ->
        let name = make_name e "NUMERIC" in
        emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr e) oc TNum
      ) e4s
  | StatefulFun (_, _, ExpSmooth (e1, e2)) ->
      (* Typing rules:
       * - e1 must be a float (and ideally, between 0 and 1), but we just ask
       *   for a numeric in order to also accept immediate values parsed as
       *   integers, and integer fields;
       * - e2 must be numeric *)
      let name = make_name e1 "FLOAT" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr e1) oc TNum ;
      let name = make_name e2 "NUMERIC" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr e2) oc TNum
  | StatelessFun1 (_, (Exp|Log|Log10|Sqrt|Floor|Ceil|Round), e) ->
      (* e must be numeric *)
      let name = make_name e "NUMERIC" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr e) oc TNum
  | StatelessFun1 (_, Hash, e) ->
      (* e can be anything *) ()
  | StatelessFun1 (_, Sparkline, e) ->
      (* e must be a vector of numerics *)
      let name = make_name e "NUMERIC_VEC" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr e) oc
        (TVec (0, { structure = TNum ; nullable = Some false }))
  | StatefulFun (_, _, Remember (fpr, tim, dur, _es)) ->
      (* Typing rules:
       * - fpr must be a (positive) float, so we take any numeric for now;
       * - time must be a time, so ideally a float, but again we accept any
       *   integer (so that a int field is OK);
       * - dur must be a duration, so a numeric again;
       * - expressions in es can be anything at all. *)
      let name = make_name fpr "NUMERIC" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr fpr) oc TNum ;
      let name = make_name tim "NUMERIC" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr tim) oc TNum ;
      let name = make_name dur "NUMERIC" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr dur) oc TNum
  | StatefulFun (_, _, Distinct _es) ->
      (* the es can be anything *) ()
  | StatefulFun (_, _, Hysteresis (meas, accept, max)) ->
      (* meas, accept and max must be numeric. *)
      let name = make_name meas "NUMERIC" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr meas) oc TNum ;
      let name = make_name accept "NUMERIC" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr accept) oc TNum ;
      let name = make_name max "NUMERIC" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr max) oc TNum
  | StatefulFun (_, _, Top { want_rank ; what ; by ; n ; duration ; time }) ->
      (* Typing rules:
       * - what can be anything;
       * - by must be numeric;
       * - time must be a time (numeric). *)
      let name = make_name by "NUMERIC" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr by) oc TNum ;
      let name = make_name time "NUMERIC" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr time) oc TNum
  | StatefulFun (_, _, Last (n, e, es)) ->
      (* The type of the return is a vector of the specified length,
       * with items of the type of e. *)
      let smt2 = Printf.sprintf "(vector %d %s)" n (id_of_expr e) in
      emit_assert_id_eq_smt2 id oc smt2
  | StatefulFun (_, _, AggrHistogram (e, _, _, _)) ->
      (* e must be numeric *)
      let name = make_name e "NUMERIC" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr e) oc TNum
  | StatelessFun2 (_, In, e1, e2) ->
      (* Typing rule:
       * - e2 can be a string, a cidr, a list or a vector;
       * - if e2 is a string, then e1 must be a string;
       * - if e2 is a cidr, then e1 must be an ip;
       * - if e2 is either a list of a vector, then e1 must have the type
       *   of the elements of this list or vector. *)
      let name = make_name e2 "IN" in
      emit_assert ~name oc (fun oc ->
        Printf.fprintf oc
        "(or (and (= string %s) (= string %s)) \
             (and (= cidr %s) (= ip %s)) \
             (and ((_ is list) %s) (= (list-type %s) %s)) \
             (and ((_ is vector) %s) (= (vector-type %s) %s)))"
          (id_of_expr e2) (id_of_expr e1)
          (id_of_expr e2) (id_of_expr e1)
          (id_of_expr e2) (id_of_expr e2) (id_of_expr e1)
          (id_of_expr e2) (id_of_expr e2) (id_of_expr e1))

let emit_operation declare tuple_sizes fi oc func =
  let open RamenOperation in
  (* Sometime we just know the types (CSV, Instrumentation, Protocols...): *)
  let op = Option.get func.Func.operation in
  let emit_all_expr out_fields =
    RamenOperation.iter_expr (fun e ->
      let id = id_of_expr e in
      declare id (Some e) ;
      emit_typing_constraints tuple_sizes out_fields oc e
    ) op in
  let set_well_known_type typ =
    emit_all_expr [] ;
    let in_type = untyped_tuple_type func.Func.in_type
    and out_type = untyped_tuple_type func.Func.out_type in
    let set_to t =
      t.RamenTypingHelpers.fields <-
        List.map (fun ft ->
          ft.RamenTuple.typ_name,
          make_typ ?nullable:ft.typ.nullable ~typ:ft.typ.structure ft.typ_name
        ) typ ;
      t.finished_typing <- true in
    set_to in_type ; set_to out_type
  in
  (* Now add specific constraints depending on the clauses: *)
  (match op with
  | Aggregate { fields ; where ; event_time ; notifications ;
                commit_when ; flush_how ; _ } ->
      emit_all_expr fields ;
      (* Typing rules:
       * - where must be a bool;
       * - commit-when must also be a bool;
       * - flush_how conditions must also be bools. *)
      let name = "F"^ string_of_int fi ^"_WHERE" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr where) oc TBool ;
      let name = "F"^ string_of_int fi ^"_COMMIT" in
      emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr commit_when) oc TBool ;
      (match flush_how with
      | Reset | Never | Slide _ -> ()
      | RemoveAll e | KeepOnly e ->
          let name = "F"^ string_of_int fi ^"_FLUSH" in
          emit_assert_id_eq_typ ~name tuple_sizes (id_of_expr e) oc TBool)

  | ReadCSVFile { what = { fields ; _ } ; _ } ->
    set_well_known_type (RingBufLib.ser_tuple_typ_of_tuple_typ fields)

  | ListenFor { proto ; _ } ->
    set_well_known_type (RamenProtocols.tuple_typ_of_proto proto)

  | Instrumentation _ ->
    set_well_known_type RamenBinocle.tuple_typ)

let emit_program declare tuple_sizes oc funcs =
  (* Output all the constraints for all the operations: *)
  List.iteri (fun i func ->
    Printf.fprintf oc "\n; Constraints for function %s\n"
      (RamenName.string_of_func func.Func.name) ;
    emit_operation declare tuple_sizes i oc func
  ) funcs

type id_or_type = Id of string | FieldType of RamenTuple.field_typ
let id_or_type_of_field op name =
  let open RamenOperation in
  let find_field_type = List.find (fun ft -> ft.RamenTuple.typ_name = name) in
  match op with
  | Aggregate { fields ; _ } ->
      let sf = List.find (fun sf -> sf.alias = name) fields in
      Id (id_of_expr sf.expr)
  | ReadCSVFile { what = { fields ; _ } ; _ } ->
      FieldType (find_field_type fields)
  | ListenFor { proto ; _ } ->
      FieldType (find_field_type (RamenProtocols.tuple_typ_of_proto proto))
  | Instrumentation _ ->
      FieldType (find_field_type RamenBinocle.tuple_typ)

(* Reading already compiled parents, set the type of fields originating from
 * external parents, parameters and environment, once and for all: *)
let type_input_fields conf oc parents params funcs =
  List.iter (fun func ->
    let parents = Hashtbl.find_default parents func.Func.name [] in
    RamenOperation.iter_expr (function
      | Field (field_typ, tupref, field_name) as expr ->
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
            let typ, same_as_ids =
              List.fold_left (fun (prev_typ, same_as_ids) pfunc ->
                if typing_is_finished pfunc.Func.out_type then (
                  match List.find (fun fld ->
                          fld.RamenTuple.typ_name = field_name
                        ) (typed_tuple_type (pfunc.Func.out_type)).ser with
                  | exception Not_found -> no_such_field pfunc
                  | t ->
                      aggr_types t prev_typ, same_as_ids
                ) else (
                  (* Typing not finished? This parent is in this very program
                   * then. Output the constraint to bind the input type to
                   * the output type: *)
                  (* Retrieve the id for the parent output fields: *)
                  match id_or_type_of_field
                          (Option.get pfunc.Func.operation) field_name with
                  | exception Not_found -> no_such_field pfunc
                  | Id p_id -> prev_typ, p_id::same_as_ids
                  | FieldType t -> aggr_types t prev_typ, same_as_ids
                )
              ) (None, []) parents in
            (match typ with
            | None ->
                (* No external parent. We'd better have same-prog parents: *)
                if same_as_ids = [] then
                  Printf.sprintf "Function %s has no parent so can't use \
                                  input field %s"
                    (RamenName.string_of_func func.Func.name)
                    field_name |>
                  failwith
            | Some t ->
                field_typ.nullable <- t.RamenTuple.typ.nullable ;
                field_typ.scalar_typ <- Some t.RamenTuple.typ.structure) ;
            List.iter (emit_assert_id_eq_id (id_of_expr expr) oc) same_as_ids
          )
      | _ -> ()
    ) (Option.get func.Func.operation)
  ) funcs

let structure_of_sort_identifier = function
  | "bool" -> TBool
  | "number" -> TNum
  | "string" -> TString
  | "ip" -> TIp
  | "cidr" -> TCidr
  | "eth" -> TEth
  | unk ->
      !logger.error "Unknown sort identifier %S" unk ;
      TEmpty

let rec structure_of_term =
  let open RamenSmtParser in
  function
  | QualIdentifier ((Identifier typ, None), []) ->
      structure_of_sort_identifier typ
  | QualIdentifier ((Identifier "vector", None),
                    [ ConstantTerm c ; sub_term ]) ->
      let sub_structure = structure_of_term sub_term in
      let n = int_of_constant c in
      TVec (n, { structure = sub_structure ; nullable = None (* TODO *)})
  | QualIdentifier ((Identifier "list", None), [ sub_term ]) ->
      let sub_structure = structure_of_term sub_term in
      TList { structure = sub_structure ; nullable = None (* TODO *)}
  | QualIdentifier ((Identifier id, None), sub_terms)
    when String.starts_with id "tuple" ->
      (try Scanf.sscanf id "tuple%d%!" (fun sz ->
        let nb_sub_terms = List.length sub_terms in
        if nb_sub_terms <> sz then
          Printf.sprintf "Bad number of sub_terms (%d) for tuple%d"
            nb_sub_terms sz |>
          failwith ;
        let ts =
          Array.of_list sub_terms |>
          Array.map (fun sub ->
            { structure = structure_of_term sub ;
              nullable = None (* TODO *) }) in
        TTuple ts
      )
    with e ->
      print_exception e ;
      TEmpty)
  | _ ->
      !logger.warning "TODO: exploit define-fun with funny term" ;
      TEmpty

let get_types conf parents funcs params =
  (* Re. signed/unsigned: currently the parser build constants of the
   * tightest possible type. But those constants could as well, if positive,
   * work for unsigned types as well, or floats. And the other way around too,
   * if the parser has been asked to favor unsigned int types.
   * For instance, we may have "f = 1" where 1 is parsed as an unsigned, but
   * f is a float or a signed field. We could have a single type for numbers,
   * but then we have no way to check that a given number must be integer, or
   * positive, etc. Instead, if we want to do that then we must assert that
   * types are compatible, op by op (here, we would check that "f" is either
   * a unsigned or a signed or a float and 1 either a float or unsigned or
   * signed, or f is a list of some type compatible with... This definition
   * is recursive, which is rather annoying for the solver).
   *
   * As a first pass, we could merely check the structure of the types
   * (numeric, bools, strings, lists, tuples and vectors).
   *
   * Later, we want the parser to tell us not what the type is but what the
   * type could be (for instance, 1046 may be float, i16, u16, etc). Also,
   * fields of known type might be cast explicitly in "in.some_u16 =
   * group.#count". Or the constraints for =, >, < might be loosened to allow
   * for any numeric type, and then cast inserted automatically.
   *)

(*  let funcs =
    List.map (fun func ->
      let operation = Some (
        RamenOperation.map_expr (function
          | Const (t, (VU8 _|VU16 _|VU32 _|VU64 _|VU128 _|
                       VI8 _|VI16 _|VI32 _|VI64 _|VI128 _ as v)) ->
              let t = { t with scalar_typ = Some TFloat } in
              Const (t, VFloat (RamenTypes.float_of_scalar v))
          | x -> x
        ) (Option.get func.Func.operation)) in
      { func with operation }
    ) funcs in*)
  let h = Hashtbl.create 71 in
  if funcs <> [] then (
    let tuple_sizes = [ 1; 2; 3; 4; 5; 6; 7; 8; 9 ] (* TODO *) in
    (* We have to start the SMT2 file with declarations before we can
     * produce any assertion: *)
    let decls = IO.output_string () in
    (* We might encounter several times the same expression (for the well
     * known constants defined once in RamenExpr, such as expr_true etc...
     * So we keep a set of already declared ids: *)
    let ids = ref Set.String.empty in
    let declare id e_opt =
      if not (Set.String.mem id !ids) then (
        ids := Set.String.add id !ids ;
        Printf.fprintf decls "(declare-fun %s () Type)" id ;
        match e_opt with
        | None -> Printf.fprintf decls "\n" ;
        | Some e -> Printf.fprintf decls "; %a\n" (RamenExpr.print true) e
      ) in
    let expr_types = IO.output_string () in
    let parent_types = IO.output_string () in
    (* Set the types for all fields from parents, params or env: *)
    type_input_fields conf parent_types parents params funcs ;
    emit_program declare tuple_sizes expr_types funcs ;
    (* So that we can now produce the smt2 script: *)
    let program_name = (List.hd funcs).Func.program_name in
    let smt2_file =
      Printf.sprintf "/tmp/%s.smt2"
        (RamenName.path_of_program program_name) in
    mkdir_all ~is_file:true smt2_file ;
    File.with_file_out ~mode:[`create; `text; `trunc] smt2_file (fun oc ->
      Printf.fprintf oc
        "(set-option :print-success false)\n\
         (set-option :produce-unsat-cores true)\n\
         (set-option :produce-models true)\n\
         (set-logic ALL) ; TODO\n\
         ; Define a sort for types:\n\
         (declare-datatypes\n\
           ( (Type 0) )\n\
           ( ((bool) (number) (string) (ip) (cidr) (eth)\n\
              (list (list-type Type))\n\
              (vector (vector-dim Int) (vector-type Type))\n\
              %a) ))\n\
         \n\
         ; Declarations:\n\
         %s\n\
         ; Constraints:\n\
         %s\n\
         ; Children-Parent relationships:\n\
         %s\n\
         ; Closing words: solve and prints the answer:\n\
         (check-sat)\n(get-unsat-core)\n(get-model)\n"
        (List.print ~first:"" ~last:"" ~sep:"\n" (fun oc sz ->
          Printf.fprintf oc "(tuple%d" sz ;
          for i = 0 to sz-1 do
            Printf.fprintf oc " (tuple%d-t%d Type)" sz i
          done ;
          Printf.fprintf oc ")")) tuple_sizes
        (IO.close_out decls)
        (IO.close_out expr_types)
        (IO.close_out parent_types)) ;
    (* Run the solver *)
    let cmd = String.nreplace ~sub:"%s" ~by:smt2_file ~str:!smt_solver in
    (* Lazy way to split the arguments: *)
    let shell = "/bin/sh" in
    let args = [| shell ; "-c" ; cmd |] in
    (* Now summon the solver: *)
    let solution =
      with_subprocess shell args (fun (oc, ic, ec) ->
        let output = read_whole_channel ic
        and errors = read_whole_channel ec in
        let p =
          RamenParsing.allow_surrounding_blanks RamenSmtParser.response in
        let stream = RamenParsing.stream_of_string output in
        match p ["SMT output"] None Parsers.no_error_correction stream |>
              RamenParsing.to_result with
        | Ok (RamenSmtParser.Solved sol, _) -> sol
        | Bad e ->
            !logger.error "Cannot parse solver output:\n%s" output ;
            if errors <> "" then failwith errors else
            IO.to_string (RamenParsing.print_bad_result
                            RamenSmtParser.print_response) e |>
            failwith
        | Ok (RamenSmtParser.Unsolved syms, _) ->
            !logger.error "Solver output:\n%s" output ;
            (* TODO: a better error message *)
            Printf.sprintf2 "Cannot solve typing constraints %a"
              (List.print String.print) syms |>
            failwith)
    in
    (* Output a hash of structure*nullability per expression id: *)
    let open RamenSmtParser in
    List.iter (fun ((sym, vars, sort, term), _recurs) ->
      try Scanf.sscanf sym "e%d%!" (fun id ->
        match vars, sort with
        | [], NonParametricSort (Identifier "Type") ->
            Hashtbl.add h id (structure_of_term term)
        | [], NonParametricSort (Identifier sort) ->
            !logger.error "Result not about sort Type but %S?!" sort
        | _, NonParametricSort (IndexedIdentifier _) ->
          !logger.warning "TODO: exploit define-fun with indexed identifier"
        | _, ParametricSort _ ->
          !logger.warning "TODO: exploit define-fun of parametric sort"
        | _::_, _ ->
          !logger.warning "TODO: exploit define-fun with parameters")
      with Scanf.Scan_failure _ | End_of_file | Failure _ -> ()
    ) solution ;
    ignore ids
  ) ;
  h
