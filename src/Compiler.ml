(*
 * Compilation of a graph
 *
 * We must first check all input/output tuple types.  For this we have two
 * temp_tuple_typ per node, one for input tuple and one for output tuple.  Each
 * check operation takes those as input and returns true if they completed any
 * of those.  Beware that those lists are completed bit by bit, since one
 * iteration of the loop might reveal only some info of some field.
 *
 * Once the fixed point is reached we check if we have all the fields we should
 * have.
 *
 * Types propagate to parents output to node input, then from operations to
 * node output, via the expected_type of each expression.
 *)
open Batteries
open RamenLog
open Helpers
module C = RamenConf
module N = RamenConf.Node
module L = RamenConf.Layer
open RamenSharedTypes
open RamenSharedTypesJS
open Lang

exception SyntaxErrorInNode of string * syntax_error
exception MissingDependency of N.t (* The one we depend on *)
exception AlreadyCompiled

(* Check that we have typed all that need to be typed, and set finished_typing *)
let check_finished_tuple_type tuple_prefix tuple_type =
  List.iter (fun (field_name, typ) ->
    let open Expr in
    (* If we couldn't determine nullability for an out field, it means
     * we can pick freely: *)
    if tuple_prefix = TupleOut &&
       typ.scalar_typ <> None && typ.nullable = None
    then (
      !logger.debug "Field %s has no constraint on nullability. \
                     Let's make it non-null." field_name ;
      typ.nullable <- Some false) ;
    if typ.nullable = None || typ.scalar_typ = None then (
      let e = CannotTypeField {
        field = field_name ;
        typ = IO.to_string print_typ typ ;
        tuple = tuple_prefix } in
      raise (SyntaxError e))
  ) tuple_type.C.fields ;
  C.finish_typing tuple_type

let can_cast ~from_scalar_type ~to_scalar_type =
  (* On TBool and Integer conversions:
   * We want to convert easily from bool to int to ease the usage of
   * booleans in arithmetic operations (for instance, summing how many times
   * something is true). But we still want to disallow int to bool automatic
   * conversions to keep conditionals clean of integers (use an IF to convert
   * in this direction).
   * If we merely allowed a TNum to be "enlarged" into a TBool then an
   * expression using a boolean as an integer would have a boolean result,
   * which is certainly not what we want. So we want to disallow such an
   * automatic conversion. Instead, user must manually cast to some integer
   * type. *)
  let compatible_types =
    match from_scalar_type with
    | TNum -> [ TU8 ; TU16 ; TU32 ; TU64 ; TU128 ; TI8 ; TI16 ; TI32 ; TI64 ; TI128 ; TFloat ]
    | TU8 -> [ TU8 ; TU16 ; TU32 ; TU64 ; TU128 ; TI16 ; TI32 ; TI64 ; TI128 ; TFloat ; TNum ]
    | TU16 -> [ TU16 ; TU32 ; TU64 ; TU128 ; TI32 ; TI64 ; TI128 ; TFloat ; TNum ]
    | TU32 -> [ TU32 ; TU64 ; TU128 ; TI64 ; TI128 ; TFloat ; TNum ]
    | TU64 -> [ TU64 ; TU128 ; TI128 ; TFloat ; TNum ]
    | TU128 -> [ TU128 ; TFloat ; TNum ]
    | TI8 -> [ TI8 ; TI16 ; TI32 ; TI64 ; TI128 ; TU16 ; TU32 ; TU64 ; TU128 ; TFloat ; TNum ]
    | TI16 -> [ TI16 ; TI32 ; TI64 ; TI128 ; TU32 ; TU64 ; TU128 ; TFloat ; TNum ]
    | TI32 -> [ TI32 ; TI64 ; TI128 ; TU64 ; TU128 ; TFloat ; TNum ]
    | TI64 -> [ TI64 ; TI128 ; TU128 ; TFloat ; TNum ]
    | TI128 -> [ TI128 ; TFloat ; TNum ]
    | TBool -> [ TBool ; TU8 ; TU16 ; TU32 ; TU64 ; TU128 ; TI8 ; TI16 ; TI32 ; TI64 ; TI128 ; TFloat ; TNum ]
    | x -> [ x ] in
  List.mem to_scalar_type compatible_types

(* Improve to rank with from *)
let check_rank ~from ~to_ =
  match !to_, !from with
  | None, Some _ ->
    to_ := !from ;
    true
  (* Contrary to type, once to_ is set it is better than from.
   * Example: "select 42, *" -> the star will add all fields from input into
   * output with a larger rank than on input. Then checking the rank again
   * would complain. *)
  | _ -> false

let set_nullable ?(indent="") typ nullable =
  let open Expr in
  match typ.nullable with
  | None ->
    !logger.debug "%sSet %a to %snullable" indent
      print_typ typ
      (if nullable then "" else "not ") ;
    typ.nullable <- Some nullable ; true
  | Some n ->
    if n <> nullable then (
      let e = InvalidNullability {
        what = typ.expr_name ;
        must_be_nullable = n } in
      raise (SyntaxError e)
    ) else false

(* Improve to_ while checking compatibility with from.
 * Numerical types of to_ can be enlarged to match those of from. *)
let check_expr_type ~indent ~ok_if_larger ~set_null ~from ~to_ =
  let changed =
    match to_.Expr.scalar_typ, from.Expr.scalar_typ with
    | None, Some _ ->
      !logger.debug "%sImproving %a from %a" indent
        Expr.print_typ to_ Expr.print_typ from ;
      to_.Expr.scalar_typ <- from.Expr.scalar_typ ;
      true
    | Some to_typ, Some from_typ when to_typ <> from_typ ->
      if can_cast ~from_scalar_type:to_typ ~to_scalar_type:from_typ then (
        !logger.debug "%sImproving %a from %a" indent
          Expr.print_typ to_ Expr.print_typ from ;
        to_.Expr.scalar_typ <- from.Expr.scalar_typ ;
        true
      ) else if ok_if_larger then false
      else
        let e = CannotTypeExpression {
          what = to_.Expr.expr_name ;
          expected_type = IO.to_string Scalar.print_typ to_typ ;
          got = from.Expr.expr_name ;
          got_type = IO.to_string Scalar.print_typ from_typ } in
        raise (SyntaxError e)
    | _ -> false in
  if set_null then
    match from.Expr.nullable with
    | None -> changed
    | Some from_null -> set_nullable ~indent to_ from_null || changed
  else changed

let scalar_finished_typing = function TNum | TAny -> false | _ -> true

exception ParentIsUntyped
(* Can raise ParentIsUntyped if the parent is still untyped or
 * SyntaxError when the field does not exist. *)
let type_of_parent_field parent tuple_of_field field =
  (* Wait until the parent has finished typing.
   * Note that it won't work in case of loop. For this we
   * would need to propagate type info as we have it and
   * also detects when we have stopped making progress, or
   * easier: use a constraint solver. *)
  let find_field () =
    match parent.N.out_type with
    | C.UntypedTuple temp_tup_typ ->
      if not temp_tup_typ.C.finished_typing then
        raise ParentIsUntyped ;
      List.assoc field temp_tup_typ.C.fields
    | C.TypedTuple { user ; _ } ->
      List.find_map (fun f ->
        if f.typ_name = field then Some (
          (* Mimick a temp_tup_typ which uniq_num will never be used *)
          Expr.{ expr_name = f.typ_name ;
                 uniq_num = 0 ;
                 nullable = Some f.nullable ;
                 scalar_typ = Some f.typ }
        ) else None) user in
  try find_field ()
  with Not_found ->
    !logger.debug "Cannot find field %s in parent %s"
      field parent.name ;
    let e = FieldNotInTuple {
      field ; tuple = tuple_of_field ;
      tuple_type =
        IO.to_string C.print_tuple_type parent.out_type } in
    raise (SyntaxError e)

(* Same as the above, but return the type common to all parents,
 * or raise SyntaxError FieldNotSameTypeInAllParents if the type is not the
 * same in every parents, or SyntaxError FieldNotInTuple if a parent lacks
 * that field, and SyntaxError NoParentForField if the parents list is
 * empty. *)
let type_of_parents_field parents tuple_of_field field =
  match List.fold_left (fun prev_typ par ->
        let typ = type_of_parent_field par tuple_of_field field in
        (* All parents must have exactly the same type *)
        (match prev_typ with None -> Some typ | Some ptyp ->
           if typ.Expr.nullable = ptyp.Expr.nullable &&
              typ.Expr.scalar_typ = ptyp.Expr.scalar_typ
           then Some typ else
           let e = FieldNotSameTypeInAllParents { field } in
           raise (SyntaxError e))
      ) None parents with
  | None ->
    let e = NoParentForField { field } in
    raise (SyntaxError e) ;
  | Some ptyp -> ptyp

(* Get rid of the short-cutting of or expressions: *)
let (|||) a b = a || b

(* Check that this expression fulfill the type expected by the caller (exp_type).
 * Also, improve exp_type (set typ and nullable, enlarge numerical types ...).
 * When we recurse from an operator to its operand we set the exp_type to the one
 * in the operator so we improve typing of the AST along the way.
 * in_type is the currently know input type of the node, while parents are the
 * currently known output types of this node's parents. We will bring new field
 * in in_type from parents as needed.
 * Notice than when we recurse within the expression we stays in the same node
 * with the same parents. parents is not modified in here. *)
let rec check_expr ?(depth=0) ~parents ~in_type ~out_type ~exp_type =
  let indent = String.make depth ' ' and depth = depth + 2 in
  let open Expr in
  (* Check that the operand [sub_expr] is compatible with expectation (re. type
   * and null) set by the caller (the operator). [op_typ] is used for printing only.
   * Extends the type of sub_expr as required. *)
  let check_operand op_typ ?exp_sub_typ ?exp_sub_nullable sub_expr =
    let sub_typ = typ_of sub_expr in
    !logger.debug "%sChecking operand of (%a), of type (%a) (expected: %a)" indent
      print_typ op_typ
      print_typ sub_typ
      (Option.print Scalar.print_typ) exp_sub_typ ;
    (* Start by recursing into the sub-expression to know its real type: *)
    let changed = check_expr ~depth ~parents ~in_type ~out_type ~exp_type:sub_typ sub_expr in
    (* Now we check this comply with the operator expectations about its operand : *)
    (match sub_typ.scalar_typ, exp_sub_typ with
    | Some actual_typ, Some exp_sub_typ ->
      if not (can_cast ~from_scalar_type:actual_typ ~to_scalar_type:exp_sub_typ) then
        let e = CannotTypeExpression {
          what = "Operand of "^ op_typ.expr_name ;
          expected_type = IO.to_string Scalar.print_typ exp_sub_typ ;
          got = "an expression" ;
          got_type = IO.to_string Scalar.print_typ actual_typ } in
        raise (SyntaxError e)
    | _ -> ()) ;
    (match exp_sub_nullable, sub_typ.nullable with
    | Some n1, Some n2 when n1 <> n2 ->
      let e = InvalidNullability {
        what = "Operand of "^ op_typ.expr_name ;
        must_be_nullable = n1 } in
      raise (SyntaxError e)
    | _ -> ()) ;
    !logger.debug "%s...operand subtype found to be: %a" indent
      print_typ sub_typ ;
    changed
  in
  (* Check that actual_typ is a better version of op_typ and improve op_typ,
   * then check that the resulting op_type fulfill exp_type. *)
  let check_operator op_typ actual_typ nullable =
    !logger.debug "%sChecking operator %a, of actual type %a" indent
      print_typ op_typ
      Scalar.print_typ actual_typ ;
    let from = make_typ ~typ:actual_typ ?nullable op_typ.expr_name in
    let changed = check_expr_type ~indent ~ok_if_larger:false ~set_null:true ~from ~to_:op_typ in
    check_expr_type ~indent ~ok_if_larger:false ~set_null:true ~from:op_typ ~to_:exp_type || changed
  in
  let check_op op_typ make_op_typ ?(propagate_null=true) args =
    (* First we check the operands: do they comply with the expected types
     * (enlarging them if necessary)? *)
    let changed, all_typed, types, nullables =
      List.fold_left (fun (changed, all_typed,
                           prev_sub_types, prev_nullables)
                          (exp_sub_typ, exp_sub_nullable, sub_expr) ->
          let changed =
            check_operand op_typ ?exp_sub_typ ?exp_sub_nullable sub_expr ||
            changed in
          let typ = typ_of sub_expr in
          match typ.scalar_typ, prev_sub_types with
          | _, None (* already failed *) | None, _ (* failing now *) ->
            changed, false, None, []
          | Some scal, Some lst ->
            changed, all_typed && scalar_finished_typing scal,
            Some (scal :: lst), (typ.nullable :: prev_nullables)
        ) (false, true, Some [], []) args in
    (* If we have typed all the operands, find out the type of the
     * operator. *)
    match types with
    | Some lst when all_typed ->
      (* List.fold inverted lst and nullables lists, which would confuse
       * make_op_typ: *)
      let lst = List.rev lst and nullables = List.rev nullables in
      assert (lst = [] || nullables <> []) ;
      let actual_op_typ = make_op_typ lst in
      let nullable = if propagate_null then
          if List.exists ((=) (Some true)) nullables then Some true
          else if List.for_all ((=) (Some false)) nullables then Some false
          else None
        else None in
      check_operator op_typ actual_op_typ nullable || changed
    | _ -> changed
  in
  let rec check_variadic op_typ ?(propagate_null=true) ?exp_sub_typ ?exp_sub_nullable = function
    | [] -> false
    | sub_expr :: rest ->
      let changed =
        check_operand op_typ ?exp_sub_typ ?exp_sub_nullable sub_expr in
        (* TODO: a function to type with a list of arguments and
         * exp types etc, iteratively. Ie. make_op_typ called at
         * each step to refine it. For now, no typing of the op from
         * the args. Especially, XXX no update of the nullability of
         * op! XXX *)
      check_variadic op_typ ~propagate_null ?exp_sub_typ ?exp_sub_nullable rest || changed
  in
  (* Useful helpers for make_op_typ above: *)
  let return_bool _ = TBool
  and return_float _ = TFloat
  and return_i128 _ = TI128
  and return_i64 _ = TI64
  and return_u16 _ = TU16
  and return_string _ = TString
  in
  fun expr ->
  !logger.debug "%s-- Typing expression %a --" indent (Expr.print true) expr ;
  match expr with
  | Const (op_typ, _) ->
    (* op_typ is already optimal. But is it compatible with exp_type? *)
    check_expr_type ~indent ~ok_if_larger:false ~set_null:true ~from:op_typ ~to_:exp_type
  | Field (op_typ, tuple, field) ->
    if tuple_has_type_input !tuple then (
      (* Check that this field is, or could be, in in_type *)
      if is_virtual_field field then false else
      match List.assoc field in_type.C.fields with
      | exception Not_found ->
        !logger.debug "%sCannot find field in in-tuple" indent ;
        if in_type.C.finished_typing then (
          let e = FieldNotInTuple {
            field ; tuple = !tuple ;
            tuple_type = IO.to_string C.print_temp_tup_typ in_type } in
          raise (SyntaxError e)
        ) else (
          (* If all our parents have this field let's bring it in! *)
          match type_of_parents_field parents !tuple field with
          | exception ParentIsUntyped -> false
          | ptyp ->
            !logger.debug "%sCopying field %s from parents, with type %a" indent
              field Expr.print_typ ptyp ;
            if is_private_field field then (
              let m = InvalidPrivateField { field } in
              raise (SyntaxError m)) ;
            let copy = Expr.copy_typ ptyp in
            in_type.C.fields <- (field, copy) :: in_type.C.fields ;
            true)
      | from ->
        if in_type.C.finished_typing then ( (* Save the type *)
          op_typ.nullable <- from.nullable ;
          op_typ.scalar_typ <- from.scalar_typ
        ) ;
        check_expr_type ~indent ~ok_if_larger:false ~set_null:true ~from ~to_:exp_type
    ) else if tuple_has_type_output !tuple then (
      (* FIXME: It is forbidden to access the field of TupleGroupPrevious if it's a generator *)
      (* First thing we know is that if the field is from TupleGroupPrevious
       * then it is nullable: *)
      (
        if !tuple = TupleGroupPrevious then (
          !logger.debug "%sField %s is from previous so nullable." indent field ;
          set_nullable ~indent exp_type true
        ) else false
      ) ||| (
        (* If we already have this field in out then check it's compatible (or
         * enlarge out or exp). If we don't have it then add it. *)
        if is_virtual_field field then false else
        match List.assoc field out_type.C.fields with
        | exception Not_found ->
          !logger.debug "%sCannot find field %s in out-tuple" indent field ;
          if out_type.C.finished_typing then (
            let e = FieldNotInTuple {
              field ; tuple = !tuple ;
              tuple_type = IO.to_string C.print_temp_tup_typ in_type } in
            raise (SyntaxError e)) ;
          out_type.C.fields <- (field, exp_type) :: out_type.C.fields ;
          true
        | out ->
          if out_type.C.finished_typing then ( (* Save the type *)
            op_typ.nullable <-
              (* Regardless of the actual type of the output tuple, fields from
               * group.previous must always be considered nullable as the whole
               * tuple is optional: *)
              if !tuple = TupleGroupPrevious then Some true else out.nullable ;
            op_typ.scalar_typ <- out.scalar_typ
          ) ;
          !logger.debug "%sfield %s found in out type: %a" indent field print_typ out ;
          check_expr_type ~indent ~ok_if_larger:false ~set_null:(!tuple <> TupleGroupPrevious) ~from:out ~to_:exp_type
      )
    ) else (
      (* All other tuples are already typed (virtual fields) *)
      if not (is_virtual_field field) then (
        Printf.eprintf "Field %a.%s is not virtual!?\n%!"
          tuple_prefix_print !tuple
          field ;
        assert false
      ) ;
      false
    )
  | StateField _ -> assert false
  | Param (_op_typ, _pname) ->
    (* TODO: one day we will know the type or value of params *)
    false
  | Case (_op_typ, alts, else_) ->
    (* Rules:
     * - If a condition or a consequent is nullable then the case is;
     * - conversely, if no condition nor any consequent are nullable, then the
     *   case is not;
     * - all conditions must have type bool;
     * - all consequents must have the same type (that of the case);
     * - if there are no else branch then the case is nullable. *)
    (
      (* All conditions must have type bool *)
      !logger.debug "%sTyping CASE: checking boolness of conditions" indent ;
      let exp_cond_type = make_bool_typ "case condition" in
      List.exists (fun alt ->
          (* First typecheck the condition, then check it's a bool: *)
          let cond_typ = typ_of alt.case_cond in
          let chg = check_expr ~depth ~parents ~in_type ~out_type ~exp_type:cond_typ alt.case_cond ||
                    check_expr_type ~indent ~ok_if_larger:false ~set_null:false ~from:exp_cond_type ~to_:cond_typ in
          !logger.debug "%sTyping CASE: condition type is now %a (changed: %b)" indent
            print_typ (typ_of alt.case_cond) chg ;
          chg
        ) alts
    ) || (
      !logger.debug "%sTyping CASE: enlarging CASE from ELSE" indent ;
      match else_ with
      | Some else_ ->
        (* First type the else_ then use the actual type to enlarge exp_type: *)
        let typ = typ_of else_ in
        let chg = check_expr ~depth ~parents ~in_type ~out_type ~exp_type:typ else_ ||
                  check_expr_type ~indent ~ok_if_larger:true ~set_null:false ~from:typ ~to_:exp_type in
        !logger.debug "%sTyping CASE: CASE type is now %a (changed: %b)" indent
          print_typ exp_type chg ;
        chg
      | None ->
        !logger.debug "%sTyping CASE: No ELSE clause so CASE can be NULL" indent ;
        set_nullable ~indent exp_type true
    ) || (
      (* Enlarge exp_type with the consequents: *)
      !logger.debug "%sTyping CASE: enlarging CASE from consequents" indent ;
      List.exists (fun alt ->
          (* First typecheck the consequent, then use it to enlarge exp_type: *)
          let typ = typ_of alt.case_cons in
          let chg = check_expr ~depth ~parents ~in_type ~out_type ~exp_type:typ alt.case_cons ||
                    check_expr_type ~indent ~ok_if_larger:true ~set_null:false ~from:typ ~to_:exp_type in
          !logger.debug "%sTyping CASE: consequent type is %a, and CASE type is now %a (changed: %b)" indent
            print_typ typ
            print_typ exp_type chg ;
          chg
        ) alts
    ) || (
      (* So all consequents and conditions and the else clause should
       * have been fully typed now. Let's check: *)
      !logger.debug "%sTyping CASE: all items should have been fully typed by now:" indent ;
      List.iter (fun alt ->
        !logger.debug "%sTyping CASE: cond type: %a, cons type: %a" indent
          print_typ (typ_of alt.case_cond)
          print_typ (typ_of alt.case_cons)) alts ;
      Option.may (fun else_ ->
        !logger.debug "%sTyping CASE: else type: %a" indent
          print_typ (typ_of else_)) else_ ;
      (* Now set the CASE nullability. *)
      !logger.debug "%sTyping CASE: figuring out if CASE is NULLable" indent ;
      let nullable =
        match else_ with
        | None -> Some true (* No else clause: nullable! *)
        | Some else_ -> (typ_of else_).nullable in
      let nullable = match nullable with
        | None -> None (* We have to wait to know the ELSE better *)
        | Some n ->
          List.fold_left (fun n alt ->
              let t1 = typ_of alt.case_cond
              and t2 = typ_of alt.case_cons in
              match n, t1.nullable, t2.nullable with
              | None, _, _ | _, None, _ | _, _, None ->
                (* We could tell earlier but it's simpler to just wait until
                 * all items are typed: *)
                None
              | Some n0, Some n1, Some n2 -> Some (n0 || n1 || n2)
            ) (Some n) alts
      in
      Option.map_default (fun nullable ->
          if exp_type.nullable <> Some nullable then (
            !logger.debug "%sTyping CASE: Setting the CASE to %sNULLable" indent
              (if nullable then "" else "not ") ;
            exp_type.nullable <- Some nullable ;
            true
          ) else false
        ) false nullable
    )
  | Coalesce (_op_typ, es) ->
    (* Rules:
     * - All elements of the list must have the same scalar type ;
     * - all elements of the list but the last must be nullable ;
     * - the last element of the list must not be nullable. *)
    (* Enlarge exp_type with the consequent: *)
    assert (es <> []) ;
    !logger.debug "%sTyping COALESCE: enlarging COALESCE from elements" indent ;
    let changed, last_typ = List.fold_left (fun (changed, last_typ) e ->
        (* So last_typ (so far) is not allowed to be not nullable: *)
        Option.may (fun last_typ ->
          if last_typ.nullable = Some false then (
            let e = InvalidCoalesce {
              what = IO.to_string print_typ last_typ ;
              must_be_nullable = true } in
            raise (SyntaxError e)
          )) last_typ ;

        let typ = typ_of e in
        (* First typecheck e, then use it to enlarge exp_type: *)
        let chg = check_expr ~depth ~parents ~in_type ~out_type ~exp_type:typ e ||
                  check_expr_type ~indent ~ok_if_larger:true ~set_null:false ~from:typ ~to_:exp_type in
        !logger.debug "%sTyping COALESCE: expr type is %a, and COALESCE type is now %a (changed: %b)" indent
          print_typ typ
          print_typ exp_type chg ;
        changed || chg, Some typ
      ) (false, None) es in
    (match last_typ with
    | None -> changed
    | Some typ ->
      match typ.nullable with
      | Some false -> changed
      | Some true ->
        let e  = InvalidCoalesce {
          what = IO.to_string print_typ typ ;
          must_be_nullable = false } in
        raise (SyntaxError e)
      | None -> changed)
  | StatelessFun (op_typ, Now) ->
    check_expr_type ~indent ~ok_if_larger:false ~set_null:true ~from:op_typ ~to_:exp_type
  | StatefulFun (op_typ, _, AggrMin e) | StatefulFun (op_typ, _, AggrMax e)
  | StatefulFun (op_typ, _, AggrFirst e) | StatefulFun (op_typ, _, AggrLast e) ->
    check_op op_typ List.hd [None, None, e]
  | StatefulFun (op_typ, _, AggrSum e) | StatelessFun (op_typ, Age e)
  | StatelessFun (op_typ, Abs e) ->
    check_op op_typ List.hd [Some TFloat, None, e]
  | StatefulFun (op_typ, _, AggrAvg e) ->
    check_op op_typ return_float [Some TFloat, None, e]
  | StatefulFun (op_typ, _, AggrAnd e) | StatefulFun (op_typ, _, AggrOr e)
  | StatelessFun (op_typ, Not e) ->
    check_op op_typ List.hd [Some TBool, None, e]
  | StatelessFun (op_typ, Cast e) ->
    check_op op_typ (fun _ -> Option.get op_typ.scalar_typ) [Some TI128, None, e]
  | StatelessFun (op_typ, Defined e) ->
    check_op op_typ return_bool  ~propagate_null:false [None, Some true, e]
  | StatefulFun (op_typ, _, AggrPercentile (e1, e2)) ->
    check_op op_typ List.last [Some TFloat, None, e1 ; Some TFloat, None, e2]
  | StatelessFun (op_typ, Add (e1, e2)) | StatelessFun (op_typ, Sub (e1, e2))
  | StatelessFun (op_typ, Mul (e1, e2)) ->
    check_op op_typ Scalar.largest_type [Some TFloat, None, e1 ; Some TFloat, None, e2]
  | StatelessFun (op_typ, Concat (e1, e2)) ->
    check_op op_typ return_string [Some TString, None, e1 ; Some TString, None, e2]
  | StatelessFun (op_typ, Like (e, _)) ->
    check_op op_typ return_bool [Some TString, None, e]
  | StatelessFun (op_typ, Pow (e1, e2)) ->
    check_op op_typ return_float [Some TFloat, None, e1 ; Some TFloat, None, e2]
  | StatelessFun (op_typ, Div (e1, e2)) ->
    (* Same as above but always return a float *)
    check_op op_typ return_float [Some TFloat, None, e1 ; Some TFloat, None, e2]
  | StatelessFun (op_typ, IDiv (e1, e2)) ->
    check_op op_typ Scalar.largest_type [Some TFloat, None, e1 ; Some TFloat, None, e2]
  | StatelessFun (op_typ, Mod (e1, e2)) ->
    check_op op_typ Scalar.largest_type [Some TI128, None, e1 ; Some TI128, None, e2]
  | StatelessFun (op_typ, Sequence (e1, e2)) ->
    check_op op_typ return_i128 [Some TI128, None, e1 ; Some TI128, None, e2]
  | StatelessFun (op_typ, Length e) ->
    check_op op_typ return_u16 [Some TString, None, e]
  | StatelessFun (op_typ, Lower e) ->
    check_op op_typ return_string [Some TString, None, e]
  | StatelessFun (op_typ, Upper e) ->
    check_op op_typ return_string [Some TString, None, e]
  | StatelessFun (op_typ, Ge (e1, e2)) | StatelessFun (op_typ, Gt (e1, e2))
  | StatelessFun (op_typ, Eq (e1, e2)) ->
    (try check_op op_typ return_bool [Some TString, None, e1 ; Some TString, None, e2]
    with SyntaxError (CannotTypeExpression _) as e ->
      (* We do not retry for other errors to keep a better error message. *)
      !logger.debug "%sEquality operator between strings failed with %S, \
                     retrying with numbers" indent (Printexc.to_string e) ;
      check_op op_typ return_bool [Some TFloat, None, e1 ; Some TFloat, None, e2])
  | StatelessFun (op_typ, And (e1, e2)) | StatelessFun (op_typ, Or (e1, e2)) ->
    check_op op_typ return_bool [Some TBool, None, e1 ; Some TBool, None, e2]
  | StatelessFun (op_typ, BeginOfRange e) | StatelessFun (op_typ, EndOfRange e) ->
    (* Not really bullet-proof in theory since check_op may update the
     * types of the operand, but in this case there is no modification
     * possible if it's either TCidrv4 or TCidrv6, so we should be good.  *)
    (try check_op op_typ (fun _ -> TIpv4) [Some TCidrv4, None, e]
    with _ -> check_op op_typ (fun _ -> TIpv6) [Some TCidrv6, None, e])
  | StatelessFun (op_typ, (Min es | Max es)) ->
    check_op op_typ Scalar.largest_type
      (List.map (fun e -> Some TFloat, None, e) es)

  | StatefulFun (op_typ, _, Lag (e1, e2)) ->
    (* e1 must be an unsigned small constant integer. For now that mean user
     * must have entered a constant. Later we might pre-evaluate constant
     * expressions into constant values. *)
    (* FIXME: Check that the const is > 0 *)
    check_const "lag" e1 ;
    (* ... and e2 can be anything and the type of lag will be the same,
     * nullable (since we might lag beyond the start of the window. *)
    check_op op_typ List.last [Some TU64, Some false, e1 ; None, None, e2]
  | StatefulFun (op_typ, _, MovingAvg (e1, e2, e3)) | StatefulFun (op_typ, _, LinReg (e1, e2, e3)) ->
    (* As above, but e3 must be numeric (therefore the result cannot be
     * null) *)
    (* FIXME: Check that the consts are > 0 *)
    check_const "moving average period" e1 ;
    check_const "moving average counts" e2 ;
    check_op op_typ return_float
      [Some TU64, Some false, e1 ;
       Some TU64, Some false, e2 ;
       Some TFloat, None, e3]
  | StatefulFun (op_typ, _, MultiLinReg (e1, e2, e3, e4s)) ->
    (* As above, with the addition of a non empty list of predictors *)
    (* FIXME: Check that the consts are > 0 *)
    check_const "multi-linear regression period" e1 ;
    check_const "multi-linear regression counts" e2 ;
    check_op op_typ return_float
      [Some TU64, Some false, e1 ;
       Some TU64, Some false, e2 ;
       Some TFloat, None, e3] ||
    check_variadic op_typ
      ~exp_sub_typ:TFloat ~exp_sub_nullable:false (*because see comment in check_variadic *) e4s
  | StatefulFun (op_typ, _, ExpSmooth (e1, e2)) ->
    (* FIXME: Check that alpha is between 0 and 1 *)
    check_const "smooth coefficient" e1 ;
    check_op op_typ return_float
      [Some TFloat, Some false, e1 ;
       Some TFloat, None, e2]
  | StatelessFun (op_typ, Exp e) | StatelessFun (op_typ, Log e) | StatelessFun (op_typ, Sqrt e) ->
    check_op op_typ return_float [Some TFloat, None, e]
  | StatelessFun (op_typ, Hash e) ->
    check_op op_typ return_i64 [None, None, e]
  | GeneratorFun (op_typ, Split (e1, e2)) ->
    check_op op_typ return_string [Some TString, None, e1 ;
                                   Some TString, None, e2]
  | StatefulFun (op_typ, _, Remember (fpr, tim, dur, e)) ->
    (* e can be anything *)
    check_const "remember false positive rate" fpr ;
    check_const "remember duration" dur ;
    check_op op_typ return_bool
      [Some TFloat, Some false, fpr ;
       Some TFloat, None, tim ;
       Some TFloat, None, dur ;
       None, None, e]

(* Given two tuple types, transfer all fields from the parent to the child,
 * while checking those already in the child are compatible. *)
let check_inherit_tuple ~including_complete ~is_subset ~from_prefix ~from_tuple ~to_prefix ~to_tuple =
  (* Check that to_tuple is included in from_tuple (if is_subset) and if so
   * that they are compatible. Improve child type using parent type. *)
  let changed =
    List.fold_left (fun changed (child_name, child_field) ->
        match List.assoc child_name from_tuple.C.fields with
        | exception Not_found ->
          if is_subset && from_tuple.C.finished_typing then (
            let e = FieldNotInTuple {
              field = child_name ;
              tuple = from_prefix ;
              tuple_type = "" (* TODO *) } in
            raise (SyntaxError e)) ;
          changed (* no-op *)
        | parent_field ->
          check_expr_type ~indent:"" ~ok_if_larger:false ~set_null:true ~from:parent_field ~to_:child_field ||
          changed
      ) false to_tuple.C.fields in
  (* Add new fields into children. *)
  let changed =
    List.fold_left (fun changed (parent_name, parent_field) ->
        match List.assoc parent_name to_tuple.C.fields with
        | exception Not_found ->
          if to_tuple.C.finished_typing then (
            let e = FieldNotInTuple {
              field = parent_name ;
              tuple = to_prefix ;
              tuple_type = "" (* TODO *) } in
            raise (SyntaxError e)) ;
          let copy = Expr.copy_typ parent_field in
          to_tuple.C.fields <- (parent_name, copy) :: to_tuple.C.fields ;
          true
        | _ ->
          changed (* We already checked those types above. All is good. *)
      ) changed from_tuple.C.fields in
  (* If typing of from_tuple is finished then so is to_tuple *)
  let changed =
    if including_complete && from_tuple.C.finished_typing && not to_tuple.C.finished_typing then (
      !logger.debug "Completing to_tuple from check_inherit_tuple" ;
      check_finished_tuple_type to_prefix to_tuple ;
      true
    ) else changed in
  changed

let check_selected_fields ~parents ~in_type ~out_type fields =
  List.fold_left (fun changed sf ->
      let name = sf.Operation.alias in
      !logger.debug "Type-check field %s" name ;
      let exp_type =
        match List.assoc name out_type.C.fields with
        | exception Not_found ->
          (* Start from the type we already know from the expression
           * because it is already set in some cases (virtual fields -
           * and for them that's our only change to get this type) *)
          let typ =
            let open Expr in
            match sf.Operation.expr with
            | Field (t, _, _) ->
              (* Note: we must create a new type for out distinct from the type
               * of the expression in case the expression is another field (from
               * in, say) because we do not want to alias them. *)
              copy_typ ~name t
            | _ ->
              let typ = typ_of sf.Operation.expr in
              typ.expr_name <- name ;
              typ
          in
          !logger.debug "Adding out-field %s (operation: %s)" name typ.Expr.expr_name ;
          out_type.C.fields <- (name, typ) :: out_type.C.fields ;
          typ
        | typ ->
          !logger.debug "... already in out, current type is %a" Expr.print_typ typ ;
          typ in
      check_expr ~parents ~in_type ~out_type ~exp_type sf.Operation.expr || changed
    ) false fields

let tuple_type_is_finished = function
  | C.TypedTuple _ -> true
  | C.UntypedTuple temp_tup_typ -> temp_tup_typ.finished_typing

let check_input_finished ~parents ~in_type =
  if not in_type.C.finished_typing &&
     List.for_all (fun par ->
       tuple_type_is_finished par.N.out_type) parents
  then (
    !logger.debug "Completing in_type because none of our parent output will change any more." ;
    check_finished_tuple_type TupleIn in_type ;
    true
  ) else false

let check_yield node fields =
  let in_type = C.untyped_tuple_type node.N.in_type
  and out_type = C.untyped_tuple_type node.N.out_type in
  (
    check_selected_fields ~parents:[] ~in_type ~out_type fields
  ) || (
    check_input_finished ~parents:[] ~in_type
  ) || (
    (* If nothing changed so far then we are done *)
    if not out_type.C.finished_typing then (
      !logger.debug "Completing out_type because it won't change any more." ;
      check_finished_tuple_type TupleOut out_type ;
      true
    ) else false
  )

let set_of_fields = function
  | C.TypedTuple { ser ; _ } ->
      List.fold_left (fun s t ->
          Set.add t.typ_name s
        ) Set.empty ser
  | C.UntypedTuple ttt ->
      List.fold_left (fun s (name, _) ->
          Set.add name s
        ) Set.empty ttt.fields

let all_finished nodes =
  List.for_all (fun node ->
      tuple_type_is_finished node.N.out_type
    ) nodes

let check_aggregate conf node fields and_all_others where key top
                    commit_when flush_how =
  let in_type = C.untyped_tuple_type node.N.in_type
  and out_type = C.untyped_tuple_type node.N.out_type
  and parents = List.map (fun (layer, name) ->
      try C.find_node conf layer name |> snd
      with Not_found ->
        let e = UnknownNode (layer ^"/"^ name) in
        raise (SyntaxErrorInNode (node.N.name, e))
    ) node.N.parents in
  let open Operation in
  (
    (* Improve in_type using parent out_type and out_type using in_type if we
     * propagates it all: *)
    (* TODO: find a way to do this only once, since once all_finished
     * returns true there is no use for another try. *)
    if and_all_others && parents <> [] && all_finished parents then (
      (* Add all the fields present (same name and same type) in all the parents,
       * and ignore the fields that are not present everywhere. *)
      let inter_parents =
        List.fold_left (fun inter parent ->
            Set.intersect inter (set_of_fields parent.N.out_type)
          ) (set_of_fields (List.hd parents).out_type) (List.tl parents) in
      (* Remove those that have different types: *)
      let inter_ptyps =
        Set.fold (fun field ptyps ->
            match type_of_parents_field parents TupleOut field with
            | exception SyntaxError _ -> ptyps
            | ptyp -> (field, ptyp) :: ptyps) inter_parents [] in
      !logger.debug "Adding * fields: %a"
        C.print_temp_tup_typ_fields inter_ptyps ;
      let from_tuple = C.{ finished_typing = true ;
                           fields = inter_ptyps } in
      (* This is supposed to propagate parent completeness into in-tuple. *)
      check_inherit_tuple ~including_complete:true ~is_subset:true ~from_prefix:TupleOut ~from_tuple ~to_prefix:TupleIn ~to_tuple:in_type
      |||
      (* If all other fields are selected, add them *)
      check_inherit_tuple ~including_complete:false ~is_subset:false ~from_prefix:TupleIn ~from_tuple:in_type ~to_prefix:TupleOut ~to_tuple:out_type
    ) else (
      (* If we do not select star then we will bring in fields from (all)
       * parents as they are needed. *)
      false
    )
  ) ||| (
    (* Improve out_type using all expressions. Check we satisfy in_type. *)
    List.fold_left (fun changed k ->
        (* The key can be anything *)
        let exp_type = Expr.typ_of k in
        check_expr ~parents ~in_type ~out_type ~exp_type k || changed
      ) false key
  ) ||| (
    match top with
    | None -> false
    | Some (n, by) ->
      (* See the Lag operator for remarks about precomputing constants *)
      Expr.check_const "top size" n ;
      (* check_expr will try to improve exp_type. We don't care we just want
       * to check it does not raise an exception. Here exp_type is build at
       * every call so we wouldn't make progress anyway. *)
      check_expr ~parents ~in_type ~out_type ~exp_type:(Expr.make_num_typ "top size") n |> ignore ;
      check_expr ~parents ~in_type ~out_type ~exp_type:(Expr.make_num_typ "top-by clause") by |> ignore ;
      false
  ) ||| (
    let exp_type = Expr.make_bool_typ ~nullable:false "commit-when clause" in
    check_expr ~parents ~in_type ~out_type ~exp_type commit_when |> ignore ;
    false
  ) ||| (
    match flush_how with
    | Reset | Never | Slide _ -> false
    | RemoveAll e | KeepOnly e ->
      let exp_type = Expr.make_bool_typ ~nullable:false "remove/keep clause" in
      check_expr ~parents ~in_type ~out_type ~exp_type e |> ignore ;
      false
  ) ||| (
    (* Check the expression, improving out_type and checking against in_type: *)
    let exp_type =
      (* That where expressions cannot be null seems a nice improvement
       * over SQL. *)
      Expr.make_bool_typ ~nullable:false "where clause" in
    check_expr ~parents ~in_type ~out_type ~exp_type where |> ignore ;
    false
  ) ||| (
    (* Also check other expression and make use of them to improve out_type.
     * Everything that's selected must be (added) in out_type. *)
    check_selected_fields ~parents ~in_type ~out_type fields
  ) || (
    (* If nothing changed so far and our parents output is finished_typing,
     * then so is out input. *)
    check_input_finished ~parents ~in_type
  ) || (
    (* If nothing changed so far and our input is finished_typing, then our output is. *)
    if in_type.C.finished_typing &&
       not out_type.C.finished_typing
    then (
      !logger.debug "Completing out_type because it won't change any more." ;
      check_finished_tuple_type TupleOut out_type ;
      true
    ) else false
  )

(*
 * Improve out_type using in_type and this node operation.
 * in_type is a given, don't modify it!
 *)
let check_operation conf node =
  let open Operation in
  match node.N.operation with
  | Yield { fields ; _ } ->
    check_yield node fields
  | Aggregate { fields ; and_all_others ; where ; key ; top ;
                commit_when ; flush_how ; _ } ->
    check_aggregate conf node fields and_all_others where key top
                    commit_when flush_how
  | ReadCSVFile { what = { fields ; _ } ; _ } ->
    if tuple_type_is_finished node.N.out_type then false else (
      let user = fields in
      let ser = RingBufLib.ser_tuple_typ_of_tuple_typ user in
      node.N.out_type <- C.TypedTuple { user ; ser } ;
      node.N.in_type <- C.TypedTuple { user = [] ; ser = [] } ;
      true)
  | ListenFor { proto ; _ } ->
    if tuple_type_is_finished node.N.out_type then false else (
      let user = RamenProtocols.tuple_typ_of_proto proto in
      let ser = RingBufLib.ser_tuple_typ_of_tuple_typ user in
      node.N.out_type <- C.TypedTuple { user ; ser } ;
      node.N.in_type <- C.TypedTuple { user = [] ; ser = [] } ;
      true)

(*
 * Type inference for the graph
 *)

let () =
  Printexc.register_printer (function
    | SyntaxErrorInNode (n, e) ->
      Some ("In node "^ n ^": "^ string_of_syntax_error e)
    | MissingDependency node ->
      Some ("Missing dependency for node "^ N.fq_name node)
    | AlreadyCompiled -> Some "Already compiled"
    | _ -> None)

let check_node_types conf node =
  try ( (* Prepend the node name to any SyntaxError *)
    (* Try to improve out_type and the AST types using the in_type and the
     * operation: *)
    check_operation conf node
  ) with SyntaxError e ->
    !logger.debug "Compilation error: %s\n%s"
      (string_of_syntax_error e) (Printexc.get_backtrace ()) ;
    raise (SyntaxErrorInNode (node.N.name, e))

(* Since we can have loops within a layer we can not propagate types
 * immediately. Also, the star selection prevent us from propagating
 * input from output types in one go.
 * We do it bit by bit but will still make sure to make parent
 * output >= children input eventually, and that fields are encoded in
 * the specified order. *)
let set_all_types conf layer =
  let rec loop () =
    if Hashtbl.fold (fun _ node changed ->
          check_node_types conf node || changed
        ) layer.L.persist.L.nodes false
    then loop ()
  in
  loop () ;
  (* We reached the fixed point.
   * We still have a few things to check: *)
  Hashtbl.iter (fun _ node ->
    (* Check that input type no parents => no input *)
    let in_type = (C.temp_tup_typ_of_tuple_type node.N.in_type).fields in
    assert (node.N.parents <> [] || in_type = []) ;
    (* Check that all expressions have indeed be typed: *)
    Operation.iter_expr (fun e ->
      let open Expr in
      let typ = typ_of e in
      if typ.nullable = None || typ.scalar_typ = None then
        let what = IO.to_string (print true) e in
        raise (SyntaxErrorInNode (node.N.name, CannotCompleteTyping what))
    ) node.N.operation
  ) layer.L.persist.L.nodes
  (* TODO: Move all typing in a separate module *)

  (*$inject
    let test_type_single_node op_text =
      try
        let conf = RamenConf.make_conf false "http://127.0.0.1/" true "/tmp" 5 0 in
        RamenConf.add_node conf "foo" "test" op_text |> ignore ;
        set_all_types conf (Hashtbl.find conf.RamenConf.graph.RamenConf.layers "test") ;
        "ok"
      with e ->
        Printf.sprintf "Exception when parsing/typing operation %S: %s\n%s"
          op_text
          (Printexc.to_string e)
          (Printexc.get_backtrace ())

    let test_check_expr ?nullable ?typ expr_text =
      let exp_type = Lang.Expr.make_typ ?nullable ?typ "test" in
      let in_type = RamenConf.make_temp_tup_typ ()
      and out_type = RamenConf.make_temp_tup_typ ()
      and parents = [] in
      RamenConf.finish_typing in_type ;
      let open RamenParsing in
      let p = Lang.Expr.Parser.(p +- eof) in
      let exp =
        match p [] None Parsers.no_error_correction (stream_of_string expr_text) |>
              to_result with
        | Batteries.Bad e ->
          let err =
            BatIO.to_string (print_bad_result (Lang.Expr.print false)) e in
          failwith err
        | Batteries.Ok (exp, _) -> exp in
      if not (check_expr ~parents ~in_type ~out_type ~exp_type exp) then
        failwith "Cannot type expression" ;
      BatIO.to_string (Lang.Expr.print true) exp
   *)

  (*$= & ~printer:BatPervasives.identity
     "ok" (test_type_single_node "SELECT 1-1 AS x FROM foo")
     "ok" (test_type_single_node "SELECT 1-200 AS x FROM foo")
     "ok" (test_type_single_node "SELECT 1-4000000000 AS x FROM foo")
     "ok" (test_type_single_node "SELECT SEQUENCE AS x FROM foo")
     "ok" (test_type_single_node "SELECT 0 AS zero, 1 AS one, SEQUENCE AS seq FROM foo")
   *)

  (*$= test_check_expr & ~printer:(fun x -> x)
     "(1 [constant of type I32, not nullable]) + (1 [constant of type I32, not nullable]) [addition of type I32, not nullable]" \
       (test_check_expr "1+1")

     "(sum locally (1 [constant of type I16, not nullable]) [sum aggregation of type I16, not nullable]) > \\
      (500 [constant of type I32, not nullable]) [comparison operator of type BOOL, not nullable]" \
       (test_check_expr "sum 1i16 > 500")

     "(sum locally (cast(I16, 1 [constant of type I32, not nullable]) [cast to I16 of type I16, not nullable]) \\
          [sum aggregation of type I16, not nullable]) > \\
      (500 [constant of type I32, not nullable]) [comparison operator of type BOOL, not nullable]" \
       (test_check_expr "sum i16(1) > 500")
   *)

let compile_node conf node =
  let open Lwt in
  let exec_name = C.exec_of_node conf.C.persist_dir node in
  mkdir_all ~is_file:true exec_name ;
  let in_typ = C.tuple_user_type node.N.in_type
  and out_typ = C.tuple_user_type node.N.out_type in
  (* In a few cases the worker sees a slightly different version of
   * the code and the ramen daemon will adapt: *)
  let operation =
    let open Operation in
    match node.N.operation with
    | ReadCSVFile { where = ReceiveFile ; what ; preprocessor } ->
      let dir =
        C.upload_dir_of_node conf.C.persist_dir node in
      mkdir_all dir ;
      ReadCSVFile {
        (* The underscore is to restrict ourself to complete files that
         * will appear atomically *)
        where = ReadFile { fname = dir ^"/_*" ; unlink = true } ;
        what ; preprocessor }
    | ReadCSVFile { where = DownloadFile _ ; _ } ->
      failwith "Not Implemented"
    | x -> x in
  let%lwt comp_cmd = wrap (fun () ->
    CodeGen_OCaml.gen_operation
      conf exec_name in_typ out_typ operation) in
  (* Let's compile (or maybe not) *)
  mkdir_all ~is_file:true exec_name ;
  if file_exists ~maybe_empty:false ~has_perms:0o100 exec_name then (
    !logger.debug "Reusing binary %S" exec_name ;
    return_unit
  ) else
    (* TODO: return an array of arguments and get rid of the shell *)
    let cmd = Lwt_process.shell comp_cmd in
    let cmd_name = "compilation of "^ node.N.name in
    let%lwt status =
      run_coprocess ~max_count:conf.max_simult_compilations cmd_name cmd in
    if status = Unix.WEXITED 0 then (
      !logger.debug "Compiled %s with: %s" node.N.name comp_cmd ;
      return_unit
    ) else (
      (* As this might well be an installation problem, makes this error
       * report to the GUI: *)
      let e = CannotGenerateCode {
        node = node.N.name ; cmd = comp_cmd ;
        status = string_of_process_status status } in
      fail (SyntaxError e)
    )

let untyped_dependency conf layer =
  (* Check all layers we depend on (parents only) either belong to
   * this layer or are compiled already. Return the first untyped
   * dependency, or None. *)
  let good (layer_name, node_name) =
    layer_name = layer.L.name ||
    match C.find_node conf layer_name node_name with
    | exception Not_found -> false
    | _, node ->
      C.tuple_is_typed node.N.out_type in
  try Some (
    Hashtbl.values layer.L.persist.L.nodes |>
    Enum.find (fun node ->
      List.exists (not % good) node.N.parents))
  with Not_found -> None

let typed_of_untyped_tuple ?cmp = function
  | C.TypedTuple _ as x -> x
  | C.UntypedTuple temp_tup_typ ->
      if not temp_tup_typ.C.finished_typing then (
        let what = IO.to_string C.print_temp_tup_typ temp_tup_typ in
        raise (SyntaxError (CannotCompleteTyping what))) ;
      let user = C.tup_typ_of_temp_tup_type temp_tup_typ in
      let user =
        match cmp with None -> user
        | Some cmp -> List.fast_sort cmp user in
      let ser = RingBufLib.ser_tuple_typ_of_tuple_typ user in
      TypedTuple { user ; ser }

let get_selected_fields node =
  match node.N.operation with
  | Yield { fields ; _ } -> Some fields
  | Aggregate { fields ; _ } -> Some fields
  | _ -> None

let compile conf layer =
  let open Lwt in
  match layer.L.persist.L.status with
  | Compiled -> fail AlreadyCompiled
  | Running -> fail AlreadyCompiled
  | Compiling -> fail AlreadyCompiled
  | Edition _ ->
    !logger.debug "Trying to compile layer %s" layer.L.name ;
    C.Layer.set_status layer Compiling ;
    try%lwt
      let%lwt () = wrap (fun () ->
        untyped_dependency conf layer |>
        Option.may (fun n -> raise (MissingDependency n)) ;
        set_all_types conf layer ;
        Hashtbl.iter (fun _ node ->
            !logger.debug "node %S:\n\tinput type: %a\n\toutput type: %a"
              node.N.name
              C.print_tuple_type node.N.in_type
              C.print_tuple_type node.N.out_type ;
            node.N.in_type <- typed_of_untyped_tuple node.N.in_type ;
            (* So far order was not taken into account. Reorder output types
             * to match selected fields (not strictly required but improves
             * user experience in the GUI). Beware that the Expr.typ has a
             * unique number so we cannot compare/copy them directly (although
             * that unique number is required to be unique only for a node,
             * better keep it unique globally in case we want to generate
             * several nodes in a single binary, and to avoid needless
             * confusion when debugging): *)
            let cmp =
              get_selected_fields node |>
              Option.map (fun selected_fields ->
                let sf_index name =
                  try List.findi (fun _ sf ->
                        sf.Operation.alias = name) selected_fields |>
                      fst
                  with Not_found ->
                    (* star-imported fields - throw them all at the end in no
                     * specific order. TODO: in parent order? *)
                    max_int in
                fun f1 f2 ->
                  compare (sf_index f1.typ_name) (sf_index f2.typ_name)) in
            node.N.out_type <- typed_of_untyped_tuple ?cmp node.N.out_type ;
            node.N.signature <- N.signature node
          ) layer.L.persist.L.nodes) in
      (* Compile *)
      let _, thds =
        Hashtbl.fold (fun _ node (doing,thds) ->
            (* Avoid compiling twice the same thing (TODO: a lockfile) *)
            if Set.mem node.N.signature doing then doing, thds else (
              Set.add node.N.signature doing,
              compile_node conf node :: thds)
          ) layer.L.persist.L.nodes (Set.empty, []) in
      let%lwt () = join thds in
      C.Layer.set_status layer Compiled ;
      return_unit
    with e ->
      C.Layer.set_status layer (Edition (Printexc.to_string e)) ;
      fail e
