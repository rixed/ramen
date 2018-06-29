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
 * - Tuples are encoded using an (Array Type) sort, which is represented
 *   by a function. We have to interpret that function, which body is in
 *   the output model, to retrieve the individual element types.
 *)
open Batteries
open RamenHelpers
open RamenTypingHelpers
open RamenExpr
open RamenTypes

let smt_solver = ref "z3 -smt2 %s"

let preamble = {|
(set-option :print-success false)
(set-option :produce-unsat-cores true)
(set-option :produce-models true)
(set-logic ALL) ; TODO

; Define a sort for types:
(declare-datatypes
  ( (Type 0) )
  ( ((bool) (number) (string) (ip) (cidr) (eth)
     (tuple (tuple-type (Array Int Type)))
     (list (list-type Type))
     (vector (vector-dim Int) (vector-type Type)))))
|}

let id_of_typ t =
  Printf.sprintf "e%d" t.uniq_num

let id_of_expr = id_of_typ % typ_of

let rec emit_id_eq_typ id oc = function
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
      Printf.fprintf oc "(and ((_ is tuple) %s)" id ;
      if Array.length ts > 0 then (
        Printf.fprintf oc " (let ((tmp_tup (tuple-type %s))) (and " id ;
        for i = 0 to Array.length ts - 1 do
          let id' = Printf.sprintf "(select tmp_tup %d)" i in
          Printf.fprintf oc " %a"
            (emit_id_eq_typ id') ts.(i)
        done ;
        Printf.fprintf oc "))") ;
      Printf.fprintf oc ")\n"
  | TVec (d, t) ->
      let id' = Printf.sprintf "(vector-type %s)" id in
      Printf.fprintf oc "(and ((_ is vector) %s) %a"
        id (emit_id_eq_typ id') t ;
      if d <> 0 then Printf.fprintf oc " (= %d (vector-dim %s))" d id ;
      Printf.fprintf oc ")\n"
  | TList t ->
      let id' = Printf.sprintf "(list-type %s)" id in
      Printf.fprintf oc "(and ((_ is list) %s) %a)\n"
        id (emit_id_eq_typ id') t

let emit_assert_id_eq_typ id oc t =
  Printf.fprintf oc "(assert %a)\n" (emit_id_eq_typ id) t

let emit_assert_id_eq_smt2 id oc smt2 =
  Printf.fprintf oc "(assert (= %s %s))\n" id smt2

let emit_assert_id_eq_id = emit_assert_id_eq_smt2

let emit_assert_id_eq_any_of_typ id oc lst =
  Printf.fprintf oc "(assert (or %a))\n"
    (List.print ~first:"" ~last:"" ~sep:" " (emit_id_eq_typ id)) lst

let emit_assert_id_eq_integer oc id =
  emit_assert_id_eq_any_of_typ id oc [ TU8 ; TI8 ]

(* Assuming all input/output/constants have been declared already, emit the
 * constraints connecting the parameter to the result: *)
let emit_typing_constraints oc out_fields e =
  let id = id_of_expr e in
  (* We may already know the type of this expression from typing: *)
  Option.may (emit_assert_id_eq_typ id oc) (typ_of e).scalar_typ ;
  (* This will just output the constraint we know from parsing. Those
   * are hard constraints and are not named: *)
  (* But then we also have specific rules according to the operation at hand:
   *)
  match e with
  | Field (_, tupref, field_name) ->
      (* The type of an output field is taken from the out types.
       * The type of a field originating from input/params/env/virtual
       * fields has been set previously. *)
      if !tupref = RamenLang.TupleOut then (
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
      (* The resulting type is the succession of the types of es: *)
      Printf.fprintf oc "(assert (and ((_ is tuple) %s)" id ;
      if es <> [] then (
        Printf.fprintf oc " (let ((tmp (tuple-type %s))) (and" id ;
        List.iteri (fun i e ->
          Printf.fprintf oc " (= %s (select tmp %d))" (id_of_expr e) i
        ) es ;
        Printf.fprintf oc "))") ;
      Printf.fprintf oc "))\n"
  | Vector (_, es) ->
      (* Typing rules:
       * - every element in es must have the same type;
       * - the resulting type is a vector of that size and type;
       * - if the vector is of length 0, it can have any type *)
      let d = List.length es in
      Printf.fprintf oc "(assert (= %d (vector-dim %s)))\n" d id ;
      (match es with
      | [] -> ()
      | fst :: rest ->
          Printf.fprintf oc "(assert (= %s (vector-type %s)))\n"
            (id_of_expr fst) id ;
          List.iter (fun e ->
            emit_assert_id_eq_id (id_of_expr fst) oc (id_of_expr e)
          ) rest)
  | Case (_, cases, else_) ->
      (* Typing rules:
       * - all conditions must have type bool;
       * - all consequents must have the same type (that of the case);
       * - if present, the else must also have that type. *)
      List.iter (fun { case_cond = cond ; case_cons = cons } ->
        emit_assert_id_eq_typ (id_of_expr cond) oc TBool ;
        emit_assert_id_eq_id (id_of_expr cons) oc id
      ) cases ;
      Option.may (fun else_ ->
        emit_assert_id_eq_id (id_of_expr else_) oc id
      ) else_
  | Coalesce (_, es) ->
      (* Typing rules:
       * - Every alternative must have the same type (that of the case); *)
      List.iter (fun e ->
        emit_assert_id_eq_id (id_of_expr e) oc id
      ) es

  | StatelessFun0 (_, (Now|Random))
  | StatelessFun1 (_, Defined, _) -> ()
  | StatefulFun (_, _, (AggrMin e|AggrMax e|AggrFirst e|AggrLast e)) ->
      (* e must have the same type as the result: *)
      emit_assert_id_eq_id (id_of_expr e) oc id
  | StatefulFun (_, _, (AggrSum e| AggrAvg e))
  | StatelessFun1 (_, (Age|Abs|Minus), e) ->
      (* The only argument must be numeric: *)
      emit_assert_id_eq_typ (id_of_expr e) oc TNum
  | StatefulFun (_, _, (AggrAnd e | AggrOr e))
  | StatelessFun1 (_, Not, e) ->
      (* The only argument must be boolean: *)
      emit_assert_id_eq_typ (id_of_expr e) oc TBool
  | StatelessFun1 (_, Cast, e) ->
      (* No type restriction on the operand: we might want to forbid some
       * types at some point, for instance strings... Some cast are
       * actually not implemented so would fail when generating code. *)
      ()
  | StatefulFun (_, _, AggrPercentile (e1, e2))
  | StatelessFun2 (_, (Add|Sub|Mul|Div|IDiv|Pow), e1, e2) ->
      (* e1 and e2 must be numeric: *)
      emit_assert_id_eq_typ (id_of_expr e1) oc TNum ;
      emit_assert_id_eq_typ (id_of_expr e2) oc TNum
  | StatelessFun2 (_, (Concat|StartsWith|EndsWith), e1, e2)
  | GeneratorFun (_, Split (e1, e2)) ->
      (* e1 and e2 must be strings: *)
      emit_assert_id_eq_typ (id_of_expr e1) oc TString ;
      emit_assert_id_eq_typ (id_of_expr e2) oc TString
  | StatelessFun2 (_, Strftime, e1, e2) ->
      (* e1 must be a string and e2 a float (ideally, a time): *)
      emit_assert_id_eq_typ (id_of_expr e1) oc TString ;
      emit_assert_id_eq_typ (id_of_expr e2) oc TFloat
  | StatelessFun1 (_, (Strptime|Length|Lower|Upper), e)
  | StatelessFunMisc (_, Like (e, _)) ->
      (* e must be a string: *)
      emit_assert_id_eq_typ (id_of_expr e) oc TString
  | StatelessFun2 (_, Mod, e1, e2)
  | StatelessFun2 (_, (BitAnd|BitOr|BitXor), e1, e2) ->
      (* e1 and e2 must be any integer: *)
      emit_assert_id_eq_integer oc (id_of_expr e1) ;
      emit_assert_id_eq_integer oc (id_of_expr e2)
  | StatelessFun2 (_, (Ge|Gt), e1, e2) ->
      (* e1 and e2 must have the same type, and be either strings, numeric,
       * IP or CIDR: *)
      emit_assert_id_eq_id (id_of_expr e1) oc (id_of_expr e2) ;
      emit_assert_id_eq_any_of_typ (id_of_expr e1) oc
        [ TString ; TNum ; TIp ; TCidr ]
  | StatelessFun2 (_, Eq, e1, e2) ->
      (* e1 and e2 must have the same type *)
      emit_assert_id_eq_id (id_of_expr e1) oc (id_of_expr e2)
  | StatelessFun2 (_, (And|Or), e1, e2) ->
      (* e1 and e2 must be booleans. *)
      emit_assert_id_eq_typ (id_of_expr e1) oc TBool ;
      emit_assert_id_eq_typ (id_of_expr e2) oc TBool
  | StatelessFun1 (_, Nth n, e) ->
      (* TODO: to remove, replaced by VecGet *)
      Printf.fprintf oc "(assert (let ((tmp %s)) \
                           (or (and ((_ is tuple) tmp) \
                                    true \
                                    (= (select (tuple-type tmp) %d) %s))\n\
                               (and ((_ is vector) tmp) \
                                    (> (vector-dim tmp) %d) \
                                    (= (vector-type tmp) %s))\n\
                               (and ((_ is list) tmp) \
                                    (= (list-type tmp) %s)))))\n"
        (id_of_expr e) n id n id id
  | StatelessFun2 (_, VecGet, n, e) ->
      (* TODO: replaces NTH entirely; NTH(n) == Get(n-1) *)
      (* Typing rules:
       * - e must be a vector or a list, or also a tuple if n is constant;
       * - n must be an unsigned;
       * - if e is a vector or a tuple, and n is a constant, then n must
       *   be less than its length;
       * - the resulting type is the same as the selected type. *)
      emit_assert_id_eq_typ (id_of_expr n) oc TU32 ;
      (match int_of_const n with
      | None ->
          (* Not a const integer, e cannot be a tuple: *)
          Printf.fprintf oc "(assert (let ((tmp %s)) \
                               (or (and ((_ is vector) tmp) \
                                        (= (vector-type tmp) %s))\n\
                                   (and ((_ is list) tmp) \
                                        (= (list-type tmp) %s)))))\n"
            (id_of_expr e) id id
      | Some n ->
          (* when n is a constant then we can accept a tuple: *)
          (* TODO: instead of "true", check that n is less than the size
           * of the array *)
          Printf.fprintf oc "(assert (let ((tmp %s)) \
                               (or (and ((_ is tuple) tmp) \
                                        true
                                        (= (select (tuple-type tmp) %d) %s)) \
                                   (and ((_ is vector) tmp) \
                                        (> (vector-dim tmp) %d)
                                        (= (vector-type tmp) %s))
                                   (and ((_ is list) tmp) \
                                        (= (list-type tmp) %s)))))\n"
            (id_of_expr e) n id n id id)
  | StatelessFun1 (_, (BeginOfRange|EndOfRange), e) ->
      (* e is any kind of cidr *)
      emit_assert_id_eq_typ (id_of_expr e) oc TCidr
  | StatelessFunMisc (_, (Min es | Max es)) ->
      (* Typing rules:
       * - es must be list of expressions of the same type (that of the
       *   result);
       * - the result type must be a float/string/ip or cidr.
       *   (TODO: why not bool, assuming true > false?) *)
      List.iter (fun e ->
        emit_assert_id_eq_id (id_of_expr e) oc id
      ) es ;
      emit_assert_id_eq_any_of_typ id oc [ TFloat ; TString ; TIp ; TCidr ]
  | StatelessFunMisc (_, Print es) ->
      (* The result must have the same type as the first parameter *)
      emit_assert_id_eq_id (id_of_expr (List.hd es)) oc id
  | StatefulFun (_, _, Lag (e1, e2)) ->
      (* Typing rules:
       * - e1 must be an unsigned;
       * - e2 has same type as the result. *)
      emit_assert_id_eq_typ (id_of_expr e1) oc TU32 ;
      emit_assert_id_eq_id (id_of_expr e2) oc id
  | StatefulFun (_, _, MovingAvg (e1, e2, e3))
  | StatefulFun (_, _, LinReg (e1, e2, e3)) ->
      (* Typing rules:
       * - e1 must be an unsigned (the period);
       * - e2 must also be an unsigned (the number of values to average);
       * - e3 must be numeric. *)
      emit_assert_id_eq_typ (id_of_expr e1) oc TU32 ;
      emit_assert_id_eq_typ (id_of_expr e2) oc TU32 ;
      emit_assert_id_eq_typ (id_of_expr e3) oc TNum
  | StatefulFun (_, _, MultiLinReg (e1, e2, e3, e4s)) ->
      (* As above, with the addition of predictors that must also be numeric.
       *)
      emit_assert_id_eq_typ (id_of_expr e1) oc TU32 ;
      emit_assert_id_eq_typ (id_of_expr e2) oc TU32 ;
      emit_assert_id_eq_typ (id_of_expr e3) oc TNum ;
      List.iter (fun e ->
        emit_assert_id_eq_typ (id_of_expr e) oc TNum
      ) e4s
  | StatefulFun (_, _, ExpSmooth (e1, e2)) ->
      (* Typing rules:
       * - e1 must be a float (and ideally, between 0 and 1), but we just ask
       *   for a numeric in order to also accept immediate values parsed as
       *   integers, and integer fields;
       * - e2 must be numeric *)
      emit_assert_id_eq_typ (id_of_expr e1) oc TNum ;
      emit_assert_id_eq_typ (id_of_expr e2) oc TNum
  | StatelessFun1 (_, (Exp|Log|Log10|Sqrt|Floor|Ceil|Round), e) ->
      (* e must be numeric *)
      emit_assert_id_eq_typ (id_of_expr e) oc TNum
  | StatelessFun1 (_, Hash, e) ->
      (* e can be absolutely anything *) ()
  | StatelessFun1 (_, Sparkline, e) ->
      (* e must be a vector of numerics *)
      emit_assert_id_eq_typ (id_of_expr e) oc (TVec (0, TNum))
  | StatefulFun (_, _, Remember (fpr, tim, dur, _es)) ->
      (* Typing rules:
       * - fpr must be a (positive) float, so we take any numeric for now;
       * - time must be a time, so ideally a float, but again we accept any
       *   integer (so that a int field is OK);
       * - dur must be a duration, so a numeric again;
       * - expressions in es can be anything at all. *)
      emit_assert_id_eq_typ (id_of_expr fpr) oc TNum ;
      emit_assert_id_eq_typ (id_of_expr tim) oc TNum ;
      emit_assert_id_eq_typ (id_of_expr dur) oc TNum
  | StatefulFun (_, _, Distinct _es) ->
      (* the es can be anything *) ()
  | StatefulFun (_, _, Hysteresis (meas, accept, max)) ->
      (* meas, accept and max must be numeric. *)
      emit_assert_id_eq_typ (id_of_expr meas) oc TNum ;
      emit_assert_id_eq_typ (id_of_expr accept) oc TNum ;
      emit_assert_id_eq_typ (id_of_expr max) oc TNum
  | StatefulFun (_, _, Top { want_rank ; what ; by ; n ; duration ; time }) ->
      (* Typing rules:
       * - what can be anything;
       * - by must be numeric;
       * - time must be a time (numeric). *)
      emit_assert_id_eq_typ (id_of_expr by) oc TNum ;
      emit_assert_id_eq_typ (id_of_expr time) oc TNum
  | StatefulFun (_, _, Last (n, e, es)) ->
      (* The type of the return is a list of the type of e. *)
      let smt2 = Printf.sprintf "(list %s)" (id_of_expr e) in
      emit_assert_id_eq_smt2 id oc smt2
  | StatefulFun (_, _, AggrHistogram (e, _, _, _)) ->
      (* e must be numeric *)
      emit_assert_id_eq_typ (id_of_expr e) oc TNum
  | StatelessFun2 (_, In, e1, e2) ->
      (* Typing rule:
       * - e2 can be a string, a cidr, a list or a vector;
       * - if e2 is a string, then e1 must be a string;
       * - if e2 is a cidr, then e1 must be an ip;
       * - if e2 is either a list of a vector, then e1 must have the type
       *   of the elements of this list or vector. *)
      Printf.fprintf oc
        "(assert (or (and (= string %s) (= string %s)) \
                     (and (= cidr %s) (= ip %s)) \
                     (and ((_ is list) %s) (= (list-type %s) %s)) \
                     (and ((_ is vector) %s) (= (vector-type %s) %s))))"
          (id_of_expr e2) (id_of_expr e1)
          (id_of_expr e2) (id_of_expr e1)
          (id_of_expr e2) (id_of_expr e2) (id_of_expr e1)
          (id_of_expr e2) (id_of_expr e2) (id_of_expr e1)

let emit_declaration oc s e =
  (* We might encounter several times the same expression (for the well
   * known constants defined once in RamenExpr, such as expr_true etc...
   * So we keep a set of already declared ids: *)
  let id = id_of_expr e in
  if Set.String.mem id s then s else (
    Printf.fprintf oc "(declare-fun %s () Type) ; %a\n"
      id (RamenExpr.print true) e ;
    Set.String.add id s)

let emit_operation s oc func =
  let open RamenOperation in
  (* Sometime we just know the types (CSV, Instrumentation, Protocols...): *)
  let op = Option.get func.Func.operation in
  let s = RamenOperation.fold_expr s (emit_declaration oc) op in
  let emit_all_expr out_fields =
    RamenOperation.iter_expr (emit_typing_constraints oc out_fields) op in
  let set_well_known_type typ =
    emit_all_expr [] ;
    let in_type = untyped_tuple_type func.Func.in_type
    and out_type = untyped_tuple_type func.Func.out_type in
    let set_to t =
      t.RamenTypingHelpers.fields <-
        List.map (fun ft ->
          ft.RamenTuple.typ_name,
          make_typ ~nullable:ft.nullable ~typ:ft.typ ft.typ_name
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
      emit_assert_id_eq_typ (id_of_expr where) oc TBool ;
      emit_assert_id_eq_typ (id_of_expr commit_when) oc TBool ;
      (match flush_how with
      | Reset | Never | Slide _ -> ()
      | RemoveAll e | KeepOnly e ->
          emit_assert_id_eq_typ (id_of_expr e) oc TBool)

  | ReadCSVFile { what = { fields ; _ } ; _ } ->
    set_well_known_type (RingBufLib.ser_tuple_typ_of_tuple_typ fields)

  | ListenFor { proto ; _ } ->
    set_well_known_type (RamenProtocols.tuple_typ_of_proto proto)

  | Instrumentation _ ->
    set_well_known_type RamenBinocle.tuple_typ) ;
  s

let emit_program oc funcs =
  String.print oc preamble ;
  (* Output all the constraints for all the operations: *)
  let ids =
    List.fold_left (fun s func ->
      Printf.fprintf oc "\n; Constraints for function %s\n"
        (RamenName.string_of_func func.Func.name) ;
      emit_operation s oc func
    ) Set.String.empty funcs in
  ids

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
                field_typ.nullable <- Some param.ptyp.nullable ;
                field_typ.scalar_typ <- Some param.ptyp.typ
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
                field_typ.nullable <- Some t.RamenTuple.nullable ;
                field_typ.scalar_typ <- Some t.RamenTuple.typ) ;
            List.iter (emit_assert_id_eq_id (id_of_expr expr) oc) same_as_ids
          )
      | _ -> ()
    ) (Option.get func.Func.operation)
  ) funcs

let set_all_types conf parents funcs params =
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
   * is recursive, which is rather annoying for the solver.
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
  if funcs <> [] then (
    (* We have to start the SMT2 file with declarations before we can
     * produce any assertion: *)
    let expr_types = IO.output_string () in
    let parent_types = IO.output_string () in
    (* Set the types for all fields from parents, params or env: *)
    type_input_fields conf parent_types parents params funcs ;
    let ids = emit_program expr_types funcs in
    (* So that we can now produce the smt2 script: *)
    let program_name = (List.hd funcs).Func.program_name in
    let smt2_file =
      Printf.sprintf "/tmp/%s.smt2"
        (RamenName.path_of_program program_name) in
    mkdir_all ~is_file:true smt2_file ;
    File.with_file_out ~mode:[`create; `text; `trunc] smt2_file (fun oc ->
      Printf.fprintf oc
        "%s\n\
         ; Children-Parent relationships:\n\
         %s\n\
         ; Closing words: solve and prints the answer:\n\
         (check-sat)\n(get-unsat-core)\n(get-model)\n"
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
            (* TODO: a better error message *)
            Printf.sprintf2 "Cannot solve typing constraints %a"
              (List.print String.print) syms |>
            failwith)
    in
    (* Set the types of those ids according to the solution: *)
    ignore ids ;
    ignore solution
  )
