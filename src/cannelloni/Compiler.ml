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
open Log
open RamenSharedTypes
module C = RamenConf
module N = RamenConf.Node
module L = RamenConf.Layer
module SL = RamenSharedTypes.Layer

(* Check that we have typed all that need to be typed *)
let check_finished_tuple_type tuple =
  Hashtbl.iter (fun field_name (_rank, typ) ->
      let open Lang in
      if typ.Expr.nullable = None || typ.Expr.scalar_typ = None then (
        let msg = Printf.sprintf "Cannot find out the type of field %s (%s), \
                                  supposed to be a member of %s"
                    field_name
                    (IO.to_string Expr.print_typ typ)
                    (IO.to_string C.print_temp_tup_typ tuple) in
        raise (SyntaxError msg)))
    tuple.C.fields

let can_cast ~from_scalar_type ~to_scalar_type =
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

(* Improve to_ while checking compatibility with from.
 * Numerical types of to_ can be enlarged to match those of from. *)
let check_expr_type ~from ~to_ =
  let open Lang in
  let changed =
    match to_.Expr.scalar_typ, from.Expr.scalar_typ with
    | None, Some _ ->
      to_.Expr.scalar_typ <- from.Expr.scalar_typ ;
      true
    | Some to_typ, Some from_typ when to_typ <> from_typ ->
      if can_cast ~from_scalar_type:to_typ ~to_scalar_type:from_typ then (
        to_.Expr.scalar_typ <- from.Expr.scalar_typ ;
        true
      ) else (
        let m = Printf.sprintf "%s must have type %s but got %s of type %s"
                    to_.Expr.expr_name (IO.to_string Scalar.print_typ to_typ)
                    from.Expr.expr_name (IO.to_string Scalar.print_typ from_typ) in
        raise (SyntaxError m)
      )
    | _ -> false in
  let changed =
    match to_.Expr.nullable, from.Expr.nullable with
    | None, Some _ ->
      to_.Expr.nullable <- from.Expr.nullable ;
      true
    | Some to_null, Some from_null when to_null <> from_null ->
      let m = Printf.sprintf "%s must%s be nullable but %s is%s"
                to_.Expr.expr_name (if to_null then "" else " not")
                from.Expr.expr_name (if from_null then "" else " not") in
      raise (SyntaxError m)
    | _ -> changed in
  changed

(* Check that this expression fulfill the type expected by the caller (exp_type).
 * Also, improve exp_type (set typ and nullable, enlarge numerical types ...).
 * When we recurse from an operator to its operand we set the exp_type to the one
 * in the operator so we improve typing of the AST along the way. *)
let rec check_expr ~in_type ~out_type ~exp_type =
  let open Lang in
  let open Expr in
  (* Check that the operand [sub_expr] is compatible with expectation (re. type
   * and null) set by the operator. [op_typ] is used for printing only.
   * Extends the type of sub_expr as required. *)
  let check_operand op_typ ?exp_sub_typ ?exp_sub_nullable sub_expr =
    let sub_typ = typ_of sub_expr in
    !logger.debug "Checking operand of (%a), of type (%a) (expected: %a)"
      Expr.print_typ op_typ
      Expr.print_typ sub_typ
      (Option.print Scalar.print_typ) exp_sub_typ ;
    (* Start by recursing into the sub-expression to know its real type: *)
    let changed = check_expr ~in_type ~out_type ~exp_type:sub_typ sub_expr in
    (* Now we check this comply with the operator expectations about its operand : *)
    (match sub_typ.scalar_typ, exp_sub_typ with
    | Some actual_typ, Some exp_sub_typ ->
      if not (can_cast ~from_scalar_type:actual_typ ~to_scalar_type:exp_sub_typ) then
        let m = Printf.sprintf "Operand of %s is supposed to have type compatible with %s, not %s"
          op_typ.expr_name
          (IO.to_string Scalar.print_typ exp_sub_typ)
          (IO.to_string Scalar.print_typ actual_typ) in
        raise (SyntaxError m)
    | _ -> ()) ;
    (match exp_sub_nullable, sub_typ.nullable with
    | Some n1, Some n2 when n1 <> n2 ->
      let m = Printf.sprintf "Operand of %s is%s supposed to be NULLable"
        op_typ.expr_name (if n1 then "" else " not") in
      raise (SyntaxError m)
    | _ -> ()) ;
    !logger.debug "...operand subtype found to be: %a"
      Expr.print_typ sub_typ ;
    changed
  in
  (* Check that actual_typ is a better version of op_typ and improve op_typ,
   * then check that the resulting op_type fulfill exp_type. *)
  let check_operator op_typ actual_typ nullable =
    !logger.debug "Checking operator %a, of actual type %a"
      Expr.print_typ op_typ
      Scalar.print_typ actual_typ ;
    let from = make_typ ~typ:actual_typ ?nullable op_typ.expr_name in
    let changed = check_expr_type ~from ~to_:op_typ in
    check_expr_type ~from:op_typ ~to_:exp_type || changed
  in
  let check_unary_op op_typ make_op_typ ?(propagate_null=true) ?exp_sub_typ ?exp_sub_nullable sub_expr =
    (* First we check the operand: does it comply with the expected type
     * (enlarging it if necessary)? *)
    let changed = check_operand op_typ ?exp_sub_typ ?exp_sub_nullable sub_expr in
    let sub_typ = typ_of sub_expr in
    (* So far so good. So, given the type of the operand, what is the type of the operator? *)
    match sub_typ.scalar_typ with
    | Some sub_typ_typ ->
      let actual_op_typ = make_op_typ sub_typ_typ in
      (* We propagate nullability automatically for most operator *)
      let nullable =
        if propagate_null then sub_typ.nullable else None in
      (* Now check that this is OK with this operator type, enlarging it if required: *)
      check_operator op_typ actual_op_typ nullable || changed
    | None -> changed (* try again later *)
  in
  let check_binary_op op_typ make_op_typ ?(propagate_null=true)
                      ?exp_sub_typ1 ?exp_sub_nullable1 sub_expr1
                      ?exp_sub_typ2 ?exp_sub_nullable2 sub_expr2 =
    let sub_typ1 = typ_of sub_expr1 and sub_typ2 = typ_of sub_expr2 in
    let changed =
        check_operand op_typ ?exp_sub_typ:exp_sub_typ1 ?exp_sub_nullable:exp_sub_nullable1 sub_expr1 in
    let changed =
        check_operand op_typ ?exp_sub_typ:exp_sub_typ2 ?exp_sub_nullable:exp_sub_nullable2 sub_expr2 || changed in
    match sub_typ1.scalar_typ, sub_typ2.scalar_typ with
    | Some sub_typ1_typ, Some sub_typ2_typ ->
      let actual_op_typ = make_op_typ (sub_typ1_typ, sub_typ2_typ) in
      let nullable = if propagate_null then
          match sub_typ1.nullable, sub_typ2.nullable with
          | Some true, _ | _, Some true -> Some true
          | Some false, Some false -> Some false
          | _ -> None
        else None in
      check_operator op_typ actual_op_typ nullable || changed
    | _ -> changed
  in
  let check_ternary_op op_typ make_op_typ ?(propagate_null=true)
                       ?exp_sub_typ1 ?exp_sub_nullable1 sub_expr1
                       ?exp_sub_typ2 ?exp_sub_nullable2 sub_expr2
                       ?exp_sub_typ3 ?exp_sub_nullable3 sub_expr3 =
    let sub_typ1 = typ_of sub_expr1 and sub_typ2 = typ_of sub_expr2 and sub_typ3 = typ_of sub_expr3 in
    let changed =
        check_operand op_typ ?exp_sub_typ:exp_sub_typ1 ?exp_sub_nullable:exp_sub_nullable1 sub_expr1 in
    let changed =
        check_operand op_typ ?exp_sub_typ:exp_sub_typ2 ?exp_sub_nullable:exp_sub_nullable2 sub_expr2 || changed in
    let changed =
        check_operand op_typ ?exp_sub_typ:exp_sub_typ3 ?exp_sub_nullable:exp_sub_nullable3 sub_expr3 || changed in
    match sub_typ1.scalar_typ, sub_typ2.scalar_typ, sub_typ3.scalar_typ with
    | Some sub_typ1_typ, Some sub_typ2_typ, Some sub_typ3_typ ->
      let actual_op_typ = make_op_typ (sub_typ1_typ, sub_typ2_typ, sub_typ3_typ) in
      let nullable = if propagate_null then
          match sub_typ1.nullable, sub_typ2.nullable, sub_typ3.nullable with
          | Some true, _, _ | _, Some true, _ | _, _, Some true -> Some true
          | Some false, Some false, Some false -> Some false
          | _ -> None
        else None in
      check_operator op_typ actual_op_typ nullable || changed
    | _ -> changed
  in
  (* Useful helpers for make_op_typ above: *)
  let return_bool _ = TBool
  and return_float _ = TFloat
  and return_i128 _ = TI128
  and return_u16 _ = TU16
  in
  function
  | Const (op_typ, _) ->
    (* op_typ is already optimal. But is it compatible with exp_type? *)
    check_expr_type ~from:op_typ ~to_:exp_type
  | Field (op_typ, tuple, field) ->
    if tuple_has_type_input !tuple then (
      (* Check that this field is, or could be, in in_type *)
      if Expr.is_virtual_field field then false else
      match Hashtbl.find in_type.C.fields field with
      | exception Not_found ->
        !logger.debug "Cannot find field %s in in-tuple" field ;
        if in_type.C.finished_typing then (
          (* FIXME: that's nice and all, but maybe out was actually not allowed
           * here?  Fix idea: in addition to in_type and out_type, have more
           * context telling us what tuple we can reference - ideally not only
           * the tuple but the fields within those, because in a select we can
           * only refer to out tuple fields that have been defined earlier. *)
          (* FIXME: also, do not look elsewhere if "in" was explicit! *)
          if Hashtbl.mem out_type.C.fields field then (
            !logger.debug "Field %s appears to belongs to out!" field ;
            tuple := TupleOut ;
            true
          ) else if out_type.C.finished_typing then (
            let m =
              Printf.sprintf "field %s not in %S tuple (which is %s)"
                field (string_of_prefix !tuple)
                (IO.to_string C.print_temp_tup_typ in_type) in
            raise (SyntaxError m)
          ) else false
        ) else false
      | _, from ->
        if in_type.C.finished_typing then ( (* Save the type *)
          op_typ.nullable <- from.nullable ;
          op_typ.scalar_typ <- from.scalar_typ
        ) ;
        check_expr_type ~from ~to_:exp_type
    ) else if tuple_has_type_output !tuple then (
      (* If we already have this field in out then check it's compatible (or
       * enlarge out or exp). If we don't have it then add it. *)
      if Expr.is_virtual_field field then false else
      match Hashtbl.find out_type.C.fields field with
      | exception Not_found ->
        !logger.debug "Cannot find field %s in out-tuple" field ;
        if out_type.C.finished_typing then (
          let m =
            Printf.sprintf "field %s not in %S tuple ((which is %s)"
              field (string_of_prefix !tuple)
              (IO.to_string C.print_temp_tup_typ in_type) in
          raise (SyntaxError m)) ;
        Hashtbl.add out_type.C.fields field (ref None, exp_type) ;
        true
      | _, out ->
        if out_type.C.finished_typing then ( (* Save the type *)
          op_typ.nullable <- out.nullable ;
          op_typ.scalar_typ <- out.scalar_typ
        ) ;
        check_expr_type ~from:out ~to_:exp_type
    ) else (
      (* All other tuples are already typed (virtual fields) *)
      if not (Expr.is_virtual_field field) then (
        Printf.eprintf "Field %a . %s is not virtual!?\n%!"
          tuple_prefix_print !tuple
          field ;
        assert false
      ) ;
      false
    )
  | Param (_op_typ, _pname) ->
    (* TODO: one day we will know the type or value of params *)
    false
  | Now op_typ ->
    check_expr_type ~from:op_typ ~to_:exp_type
  | AggrMin (op_typ, e) | AggrMax (op_typ, e)
  | AggrFirst (op_typ, e) | AggrLast (op_typ, e) ->
    check_unary_op op_typ identity e
  | AggrSum (op_typ, e) | Age (op_typ, e)
  | Abs (op_typ, e) ->
    check_unary_op op_typ identity ~exp_sub_typ:TFloat e
  | AggrAnd (op_typ, e) | AggrOr (op_typ, e)
  | Not (op_typ, e) ->
    check_unary_op op_typ identity ~exp_sub_typ:TBool e
  | Cast (op_typ, e) ->
    check_unary_op op_typ (fun _ -> Option.get op_typ.scalar_typ) ~exp_sub_typ:TI128 e
  | Defined (op_typ, e) ->
    check_unary_op op_typ return_bool ~exp_sub_nullable:true ~propagate_null:false e
  | AggrPercentile (op_typ, e1, e2) ->
    check_binary_op op_typ snd ~exp_sub_typ1:TFloat e1 ~exp_sub_typ2:TFloat e2
  | Add (op_typ, e1, e2) | Sub (op_typ, e1, e2)
  | Mul (op_typ, e1, e2) | Exp (op_typ, e1, e2) ->
    check_binary_op op_typ Scalar.larger_type ~exp_sub_typ1:TFloat e1 ~exp_sub_typ2:TFloat e2
  | Div (op_typ, e1, e2) ->
    (* Same as above but always return a float *)
    check_binary_op op_typ return_float ~exp_sub_typ1:TFloat e1 ~exp_sub_typ2:TFloat e2
  | IDiv (op_typ, e1, e2) ->
    check_binary_op op_typ Scalar.larger_type ~exp_sub_typ1:TI128 e1 ~exp_sub_typ2:TI128 e2
  | Mod (op_typ, e1, e2) ->
    check_binary_op op_typ Scalar.larger_type ~exp_sub_typ1:TI128 e1 ~exp_sub_typ2:TI128 e2
  | Sequence (op_typ, e1, e2) ->
    check_binary_op op_typ return_i128 ~exp_sub_typ1:TI128 e1 ~exp_sub_typ2:TI128 e2
  | Length (op_typ, e) ->
    check_unary_op op_typ return_u16 ~exp_sub_typ:TString e
  | Ge (op_typ, e1, e2) | Gt (op_typ, e1, e2)
  | Eq (op_typ, e1, e2) (* FIXME: Eq should work on strings as well *) ->
    check_binary_op op_typ return_bool ~exp_sub_typ1:TFloat e1 ~exp_sub_typ2:TFloat e2
  | And (op_typ, e1, e2) | Or (op_typ, e1, e2) ->
    check_binary_op op_typ return_bool ~exp_sub_typ1:TBool e1 ~exp_sub_typ2:TBool e2
  | BeginOfRange (op_typ, e) | EndOfRange (op_typ, e) ->
    (* Not really bullet-proof in theory since check_unary_op may update the
     * types of the operand, but in this case there is no modification
     * possible if it's either TCidrv4 or TCidrv6, so we should be good.  *)
    (try check_unary_op op_typ (fun _ -> TIpv4) ~exp_sub_typ:TCidrv4 e
    with _ -> check_unary_op op_typ (fun _ -> TIpv6) ~exp_sub_typ:TCidrv6 e)
  | Lag (op_typ, e1, e2) ->
    (* e1 must be an unsigned small constant integer. For now that mean user
     * must have entered a constant. Later we might pre-evaluate constant
     * expressions into constant values. *)
    (* FIXME: Check that the const is > 0 *)
    Expr.check_const "lag" e1 ;
    (* ... and e2 can be anything and the type of lag will be the same,
     * nullable (since we might lag beyond the start of the window. *)
    check_binary_op op_typ snd ~exp_sub_typ1:TU16 ~exp_sub_nullable1:false e1 e2
  | SeasonAvg (op_typ, e1, e2, e3) | LinReg (op_typ, e1, e2, e3) ->
    (* As above, but e3 must be numeric (therefore the result cannot be
     * null) *)
    (* FIXME: Check that the consts are > 0 *)
    Expr.check_const "season-avg period" e1 ;
    Expr.check_const "season-avg counts" e2 ;
    check_ternary_op op_typ return_float
      ~exp_sub_typ1:TU16 ~exp_sub_nullable1:false e1
      ~exp_sub_typ2:TU16 ~exp_sub_nullable2:false e2
      ~exp_sub_typ3:TFloat e3

(* Given two tuple types, transfer all fields from the parent to the child,
 * while checking those already in the child are compatible.
 * If autorank is true, do not try to reuse from rank but add them instead.
 * This is meant to be used when transferring input to output due to "select *"
 *)
let check_inherit_tuple ~including_complete ~is_subset ~from_tuple ~to_tuple ~autorank =
  let max_rank fields =
    Hashtbl.fold (fun _ (rank, _) max_rank ->
      match !rank with
      | None -> max_rank
      | Some r -> max max_rank r) fields ~-1 (* start at -1 so that max+1 starts at 0 *)
  in
  (* Check that to_tuple is included in from_tuple (is is_subset) and if so
   * that they are compatible. Improve child type using parent type. *)
  let changed =
    Hashtbl.fold (fun n (child_rank, child_field) changed ->
        match Hashtbl.find from_tuple.C.fields n with
        | exception Not_found ->
          if is_subset && from_tuple.C.finished_typing then (
            let m = Printf.sprintf "Unknown field %s" n in
            raise (Lang.SyntaxError m)) ;
          changed (* no-op *)
        | parent_rank, parent_field ->
          let c1 = check_expr_type ~from:parent_field ~to_:child_field
          and c2 = check_rank ~from:parent_rank ~to_:child_rank in
          c1 || c2 || changed
      ) to_tuple.C.fields false in
  (* Add new fields into children. *)
  let changed =
    Hashtbl.fold (fun n (parent_rank, parent_field) changed ->
        match Hashtbl.find to_tuple.C.fields n with
        | exception Not_found ->
          if to_tuple.C.finished_typing then (
            let m = Printf.sprintf "Field %s is not in to_tuple" n in
            raise (Lang.SyntaxError m)) ;
          let copy = Lang.Expr.copy_typ parent_field in
          let rank =
            if autorank then ref (Some (max_rank to_tuple.C.fields + 1))
            else ref !parent_rank in
          Hashtbl.add to_tuple.C.fields n (rank, copy) ;
          true
        | _ ->
          changed (* We already checked those types above. All is good. *)
      ) from_tuple.C.fields changed in
  (* If typing of from_tuple is finished then so is to_tuple *)
  let changed =
    if including_complete && from_tuple.C.finished_typing && not to_tuple.C.finished_typing then (
      !logger.debug "Completing to_tuple from check_inherit_tuple" ;
      check_finished_tuple_type to_tuple ;
      to_tuple.C.finished_typing <- true ;
      true
    ) else changed in
  changed

let check_selected_fields ~in_type ~out_type fields =
  let open Lang in
  List.fold_lefti (fun changed i sf ->
      changed || (
        let name = sf.Operation.alias in
        let exp_type =
          match Hashtbl.find out_type.C.fields name with
          | exception Not_found ->
            (* Start from the type we already know from the expression
             * because it is already set in some cases (virtual fields -
             * and for them that's our only change to get this type) *)
            let exp_typ = Expr.copy_typ ~name (Expr.typ_of sf.Operation.expr) in
            !logger.debug "Adding out field %s" name ;
            Hashtbl.add out_type.C.fields name (ref (Some i), exp_typ) ;
            exp_typ
          | rank, exp_typ ->
            if !rank = None then rank := Some i ;
            exp_typ in
        check_expr ~in_type ~out_type ~exp_type sf.Operation.expr)
    ) false fields

let check_yield ~in_type ~out_type fields =
  (
    check_selected_fields ~in_type ~out_type fields
  ) || (
    (* If nothing changed so far then we are done *)
    if not out_type.C.finished_typing then (
      !logger.debug "Completing out_type because it won't change any more." ;
      check_finished_tuple_type out_type ;
      out_type.C.finished_typing <- true ;
      true
    ) else false
  )

let check_aggregate ~in_type ~out_type fields and_all_others
                    where key commit_when flush_when flush_how =
  let open Lang in
  (
    (* Improve out_type using all expressions. Check we satisfy in_type. *)
    List.fold_left (fun changed k ->
        (* The key can be anything *)
        let exp_type = Expr.typ_of k in
        check_expr ~in_type ~out_type ~exp_type k || changed
      ) false key
  ) || (
    let exp_type = Expr.make_bool_typ ~nullable:false "commit-when clause" in
    check_expr ~in_type ~out_type ~exp_type commit_when
  ) || (
    match flush_when with
    | None -> false
    | Some flush_when ->
      let exp_type = Expr.make_bool_typ ~nullable:false "flush-when clause" in
      check_expr ~in_type ~out_type ~exp_type flush_when
  ) || (
    let open Lang.Operation in
    match flush_how with
    | Reset -> false
    | Slide _ -> false
    | RemoveAll e | KeepOnly e ->
      let exp_type = Expr.make_bool_typ ~nullable:false "remove/keep clause" in
      check_expr ~in_type ~out_type ~exp_type e
  ) || (
    (* Check the expression, improving out_type and checking against in_type: *)
    let exp_type =
      (* That where expressions cannot be null seems a nice improvement
       * over SQL. *)
      Lang.Expr.make_bool_typ ~nullable:false "where clause" in
    check_expr ~in_type ~out_type ~exp_type where
  ) || (
    (* Also check other expression and make use of them to improve out_type.
     * Everything that's selected must be (added) in out_type. *)
    check_selected_fields ~in_type ~out_type fields
  ) || (
    (* If all other fields are selected, add them *)
    if and_all_others then (
      check_inherit_tuple ~including_complete:false ~is_subset:false ~from_tuple:in_type ~to_tuple:out_type ~autorank:true
    ) else false
  ) || (
    (* If nothing changed so far and our input is finished_typing, then our output is. *)
    if in_type.C.finished_typing && not out_type.C.finished_typing then (
      !logger.debug "Completing out_type because it won't change any more." ;
      check_finished_tuple_type out_type ;
      out_type.C.finished_typing <- true ;
      true
    ) else false
  )

(*
 * Improve out_type using in_type and this node operation.
 * in_type is a given, don't modify it!
 *)
let check_operation ~in_type ~out_type =
  let open Lang in
  let open Operation in
  function
  | Yield fields ->
    check_yield ~in_type ~out_type fields
  | Aggregate { fields ; and_all_others ; where ;
                key ; commit_when ; flush_when ; flush_how ; _ } ->
    check_aggregate ~in_type ~out_type fields and_all_others where
                    key commit_when flush_when flush_how
  | ReadCSVFile { fields ; _ } ->
    let from_tuple = C.temp_tup_typ_of_tup_typ fields in
    check_inherit_tuple ~including_complete:true ~is_subset:true ~from_tuple ~to_tuple:out_type ~autorank:false

(*
 * Type inference for the graph
 *)

let check_node_types node =
  try ( (* Prepend the node name to any SyntaxError *)
    (* Try to improve the in_type using the out_type of parents: *)
    (
      if node.N.in_type.C.finished_typing then false
      else if node.N.parents = [] then (
        !logger.debug "Completing node %s in-type since we have no parents" node.N.name ;
        check_finished_tuple_type node.N.in_type ;
        node.N.in_type.C.finished_typing <- true ;
        true
      ) else List.fold_left (fun changed par ->
            (* This is supposed to propagate parent completeness into in-tuple. *)
            check_inherit_tuple ~including_complete:true ~is_subset:true ~from_tuple:par.N.out_type ~to_tuple:node.N.in_type ~autorank:false || changed
          ) false node.N.parents
    ) || (
    (* Try to improve out_type and the AST types using the in_type and the
     * operation: *)
      check_operation ~in_type:node.N.in_type ~out_type:node.N.out_type node.N.operation
    )
  ) with Lang.SyntaxError e ->
    !logger.debug "Compilation error: %s, %s"
      e (Printexc.get_backtrace ()) ;
    let e' = Printf.sprintf "node %S: %s" node.N.name e in
    raise (Lang.SyntaxError e')

let node_typing_is_finished conf node =
  node.N.signature <> "" ||
  (if node.N.in_type.C.finished_typing &&
      node.N.out_type.C.finished_typing then
     let s = N.signature conf.C.version_tag node in
     node.N.signature <- s ;
     true
   else false)

let set_all_types conf layer =
  let rec loop pass =
    if pass < 0 then (
      let bad_nodes =
        Hashtbl.values layer.L.persist.L.nodes //
        (fun n -> not (node_typing_is_finished conf n)) in
      let print_bad_node fmt node =
        Printf.fprintf fmt "%s: %a"
          node.N.name
          (List.print ~sep:" and " ~first:"" ~last:"" String.print)
            ((if node.N.in_type.C.finished_typing then [] else ["cannot type input"]) @
             (if node.N.out_type.C.finished_typing then [] else ["cannot type output"])) in
      let msg = IO.to_string (Enum.print ~sep:", " print_bad_node) bad_nodes in
      raise (Lang.SyntaxError msg)) ;
    if Hashtbl.fold (fun _ node changed ->
          check_node_types node || changed
        ) layer.L.persist.L.nodes false
    then loop (pass - 1)
  in
  let max_pass = 50 (* TODO: max number of field for a node times number of nodes? *) in
  loop max_pass
  (* TODO:
   * - check that input type empty <=> no parents
   * - check that output type empty <=> no children
   * - Move all typing in a separate module
   *)

  (*$inject
    let test_type_single_node op_text =
      try
        let conf = RamenConf.make_conf false "http://127.0.0.1/" true "test" "/tmp" in
        RamenConf.add_node conf "test" "test" op_text ;
        set_all_types conf (Hashtbl.find conf.RamenConf.graph.RamenConf.layers "test") ;
        "ok"
      with e ->
        Printf.sprintf "Exception in set_all_types: %s at\n%s"
          (Printexc.to_string e)
          (Printexc.get_backtrace ())

    let test_check_expr ?nullable ?typ expr_text =
      let exp_type = Lang.Expr.make_typ ?nullable ?typ "test" in
      let in_type = RamenConf.make_temp_tup_typ ()
      and out_type = RamenConf.make_temp_tup_typ () in
      in_type.RamenConf.finished_typing <- true ;
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
      if not (check_expr ~in_type ~out_type ~exp_type exp) then
        failwith "Cannot type expression" ;
      BatIO.to_string (Lang.Expr.print true) exp
   *)

  (*$= & ~printer:BatPervasives.identity
     "ok" (test_type_single_node "SELECT 1-1 AS x")
     "ok" (test_type_single_node "SELECT 1-200 AS x")
     "ok" (test_type_single_node "SELECT 1-4000000000 AS x")
     "ok" (test_type_single_node "SELECT SEQUENCE AS x")
     "ok" (test_type_single_node "SELECT 0 AS zero, 1 AS one, SEQUENCE AS seq")
   *)

  (*$= test_check_expr & ~printer:(fun x -> x)
     "(1 [constant of type I8]) + (1 [constant of type I8]) [addition of type I8]" \
       (test_check_expr "1+1")

     "(sum (1 [constant of type I16]) [sum aggregation of type I16]) > \\
      (500 [constant of type I16]) [comparison operator of type BOOL]" \
       (test_check_expr "sum 1i16 > 500")

     "(sum (cast(I16, 1 [constant of type I8]) [cast to I16 of type I16]) \\
          [sum aggregation of type I16]) > \\
      (500 [constant of type I16]) [comparison operator of type BOOL]" \
       (test_check_expr "sum i16(1) > 500")
   *)

let compile_node conf node =
  assert node.N.in_type.C.finished_typing ;
  assert node.N.out_type.C.finished_typing ;
  let in_typ = C.tup_typ_of_temp_tup_type node.N.in_type
  and out_typ = C.tup_typ_of_temp_tup_type node.N.out_type in
  node.N.command <- Some (
    (* gen_operation could compute a signature of it's own, but better provide
     * it so that in the future all code generators use the same. *)
    CodeGen_OCaml.gen_operation conf node.N.signature in_typ out_typ node.N.operation)

let untyped_dependency layer =
  (* Check all layers we depend on (parents only) either belong to
   * this layer or are compiled already. Return the first untyped
   * dependency, or None. *)
  let good node =
    node.N.layer = layer.L.name ||
    node.N.out_type.C.finished_typing in
  try Some (
    Hashtbl.values layer.L.persist.L.nodes |>
    Enum.find (fun node ->
      List.exists (not % good) node.N.parents))
  with Not_found -> None

exception MissingDependency of N.t (* The one we depend on *)
exception AlreadyCompiled

let compile conf layer =
  match layer.L.persist.L.status with
  | SL.Compiled -> raise AlreadyCompiled
  | SL.Running -> raise AlreadyCompiled
  | SL.Compiling -> raise AlreadyCompiled
  | SL.Edition ->
    !logger.debug "Trying to compile layer %s" layer.L.name ;
    untyped_dependency layer |>
    Option.may (fun n -> raise (MissingDependency n)) ;

    C.Layer.set_status layer SL.Compiling ;
    set_all_types conf layer ;
    let finished_typing =
      Hashtbl.fold (fun _ node finished_typing ->
          !logger.debug "node %S:\n\tinput type: %a\n\toutput type: %a"
            node.N.name
            C.print_temp_tup_typ node.N.in_type
            C.print_temp_tup_typ node.N.out_type ;
          finished_typing && node_typing_is_finished conf node
        ) layer.L.persist.L.nodes true in
    (* TODO: better reporting *)
    if not finished_typing then
      raise (Lang.SyntaxError "Cannot complete typing") ;
    Hashtbl.iter (fun _ node ->
        try compile_node conf node
        with Failure m ->
          raise (Failure ("While compiling "^ node.N.name ^": "^ m))
      ) layer.L.persist.L.nodes ;
    C.Layer.set_status layer SL.Compiled ;
    C.save_graph conf
