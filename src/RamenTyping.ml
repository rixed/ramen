(*
 * We must first check all input/output tuple types.  For this we have two
 * temp_tuple_typ per func, one for input tuple and one for output tuple.  Each
 * check operation takes those as input and returns true if they completed any
 * of those.  Beware that those lists are completed bit by bit, since one
 * iteration of the loop might reveal only some info of some field.
 *
 * Once the fixed point is reached we check if we have all the fields we should
 * have.
 *
 * Types propagate from parents output to func input, then from operations to
 * func output, via the expected_type of each expression.
 *)
open Batteries
open RamenLog
open RamenHelpers
module C = RamenConf
module F = C.Func
module Expr = RamenExpr
open RamenLang
open RamenScalar

(* Used to type the input/output of funcs. Of course a compiled/
 * running func must have finished_typing to true and all optional
 * values set, but we keep that type even for typed funcs so that
 * the typing code, which has to use both typed and untyped funcs,
 * has to deal with only one case. We will sometime Option.get those
 * values when we know the func is typed.
 * The other tuple type, RamenTuple.typ, is used to describe tuples
 * outside of this context (for instance, when describing a CSV or other
 * serialization format). *)
type untyped_tuple =
  { mutable finished_typing : bool ;
    mutable fields : (string * Expr.typ) list }

let print_untyped_tuple_fields fmt fs =
  List.print ~first:"{" ~last:"}" ~sep:", "
    (fun fmt (name, expr_typ) ->
      Printf.fprintf fmt "%s: %a"
        name
        Expr.print_typ expr_typ) fmt fs

let print_untyped_tuple fmt t =
  Printf.fprintf fmt "%a (%s)"
    print_untyped_tuple_fields t.fields
    (if t.finished_typing then "finished typing" else "to be typed")

let untyped_tuple_copy t =
  { t with fields =
      List.map (fun (name, typ) -> name, Expr.copy_typ typ) t.fields }

type tuple_type = UntypedTuple of untyped_tuple
                | TypedTuple of RamenTuple.typed_tuple

let tuple_is_typed = function
  | TypedTuple _ -> true
  | UntypedTuple _ -> false

let typing_is_finished = function
  | TypedTuple _ -> true
  | UntypedTuple t -> t.finished_typing

let print_tuple_type fmt = function
  | UntypedTuple untyped_tuple ->
      print_untyped_tuple fmt untyped_tuple
  | TypedTuple t ->
      RamenTuple.(print_typ fmt t.user)

exception BadTupleTypedness of string
let typed_tuple_type = function
  | TypedTuple t -> t
  | UntypedTuple _ ->
      raise (BadTupleTypedness "Function should be typed by now!")

let untyped_tuple_type = function
  | TypedTuple _ ->
      raise (BadTupleTypedness "This func should not be typed!")
  | UntypedTuple untyped_tuple -> untyped_tuple

let tuple_ser_type t = (typed_tuple_type t).ser
let tuple_user_type t = (typed_tuple_type t).user

let make_untyped_tuple () =
  { finished_typing = false ; fields = [] }

let finish_typing t =
  t.finished_typing <- true

let untyped_tuple_of_tup_typ tup_typ =
  let t = make_untyped_tuple () in
  List.iter (fun f ->
      let expr_typ =
        Expr.make_typ ~nullable:f.RamenTuple.nullable
                           ~typ:f.typ f.typ_name in
      t.fields <- t.fields @ [f.typ_name, expr_typ]
    ) tup_typ ;
  finish_typing t ;
  t

let untyped_tuple_of_tuple_type = function
  | UntypedTuple untyped_tuple -> untyped_tuple
  | TypedTuple { ser ; _ } -> untyped_tuple_of_tup_typ ser

let tup_typ_of_untyped_tuplee ttt =
  assert ttt.finished_typing ;
  List.map (fun (name, typ) ->
    RamenTuple.{
      typ_name = name ;
      nullable = Option.get typ.Expr.nullable ;
      typ = Option.get typ.Expr.scalar_typ }) ttt.fields

module Func =
struct
  type t =
    { program_name : string ;
      (* Within a program, funcs are identified by a name that can be
       * optionally provided automatically if its not meant to be referenced.
       *)
      name : string ;
      (* Parameters used by that function, with default values: *)
      params : RamenTuple.params ;
      (* Parsed operation (for untyped funs) and its in/out types: *)
      operation : RamenOperation.t option ;
      mutable in_type : tuple_type ;
      mutable out_type : tuple_type ;
      parents : (string * string) list ;
      (* The signature with default params, used to name compiled modules *)
      mutable signature : string ;
      (* Extracted from the operation or inferred from parents: *)
      mutable event_time : RamenEventTime.t option ;
      mutable factors : string list }

  let signature conf func =
    (* We'd like to be formatting independent so that operation text can be
     * reformatted without ramen recompiling it. For this it is not OK to
     * strip redundant white spaces as some of those might be part of literal
     * string values. So we print it, trusting the printer to be exhaustive.
     * This is not enough to print the expression with types, as those do not
     * contain relevant info such as field rank. We therefore print without
     * types and encode input/output types explicitly below: *)
    "OP="^ IO.to_string RamenOperation.print (Option.get func.operation) ^
    "IN="^ RamenTuple.type_signature (typed_tuple_type func.in_type) ^
    "OUT="^ RamenTuple.type_signature (typed_tuple_type func.out_type) ^
    (* As for parameters, only their default values are embedded in the
     * signature: *)
    "PM="^ IO.to_string (List.print ~first:"" ~last:"" ~sep:","
                           RamenTuple.print_param) func.params ^
    (* Also, as the compiled code would differ: *)
    "FLG="^ (if conf.C.debug then "DBG" else "") |>
    md5
end
module Program =
struct
  type t =
    { name : string ;
      mutable funcs : (string, Func.t) Hashtbl.t }
end

exception InvalidCommand of string

(* FIXME: got bitten by the fact that func_name and program_name are 2 strings
 * so you can mix them up. Make specialized types for all those strings. *)
let make_untyped_func program_name func_name params operation =
  !logger.debug "Creating func %s/%s" program_name func_name ;
  F.sanitize_name func_name ;
  assert (func_name <> "") ;
  let parents =
    RamenOperation.parents_of_operation operation |>
    List.map (fun p ->
      try C.program_func_of_user_string ~default_program:program_name p
      with Not_found ->
        raise (InvalidCommand ("Parent func "^ p ^" does not exist"))) in
  Func.{
    program_name ; name = func_name ; signature = "" ;
    params ; operation = Some operation ; parents ;
    in_type = UntypedTuple (make_untyped_tuple ()) ;
    out_type = UntypedTuple (make_untyped_tuple ()) ;
    event_time = RamenOperation.event_time_of_operation operation ;
    factors = RamenOperation.factors_of_operation operation  }

(* Same as the above, for when a function has already been compiled: *)
let make_typed_func program_name rcf =
  Func.{
    program_name ; name = rcf.F.name ;
    signature = rcf.F.signature ;
    params = rcf.F.params ;
    operation = None ; parents = [] ;
    in_type = TypedTuple rcf.F.in_type ;
    out_type = TypedTuple rcf.F.out_type ;
    event_time = rcf.F.event_time ;
    factors = rcf.F.factors }

(* Check that we have typed all that need to be typed, and set finished_typing *)
let check_finished_tuple_type tuple_prefix tuple_type =
  List.iter (fun (field_name, typ) ->
    let open RamenExpr in
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
  ) tuple_type.fields ;
  finish_typing tuple_type

let iter_fun_expr func f =
  RamenOperation.iter_expr (fun e ->
    let open RamenExpr in
    let typ = typ_of e in
    let what = IO.to_string (print true) e in
    f what typ
  ) (Option.get func.Func.operation)

let cur_func_name = ref ""

let last_chance_to_type_func func =
  let open RamenExpr in
  if not (typing_is_finished func.Func.out_type) then
    let out_type = untyped_tuple_type func.Func.out_type in
    List.fold_left (fun changed (field_name, typ) ->
      (* We could have an expression which type is left undecided to
       * either TNum or TAny. For TAny, better err out (FIXME: what about
       * `SELECT 0.1 + max previous.x AS x`?), but for TNum we might
       * pick a numerical type that suits us. For instance, if we write:
       * `SELECT 0.1 + previous.x AS x` then x will have type TNum, as + can
       * accommodate any two TNums. Ideally we'd like x to be TFloat here, in
       * order to limit the number of conversions and also probably to better
       * match user expectations, but this information is lost by now. *)
      if typ.scalar_typ = Some TNum then (
        !logger.debug "%s: Numeric field %s has no constraint on type. \
                       Let's make it a TI32." !cur_func_name field_name ;
        typ.scalar_typ <- Some TI32 ;
        true
      ) else changed
    ) false out_type.fields
  else
    let changed = ref false in
    iter_fun_expr func (fun what typ ->
      (* Similarly, we could be left with a literal "NULL" somewhere in
       * the AST that's still untyped. Any type will do: *)
      if typ.scalar_typ = None && typ.nullable = Some true then (
        !logger.debug "%s: %S still untyped, make it a nullable boolean"
          !cur_func_name what ;
        typ.scalar_typ <- Some TBool ;
        changed := true)) ;
    !changed

let try_finish_out_type func =
  (* If nothing changed so far and our input is finished_typing,
   * then our output is also finished: *)
  if typing_is_finished func.Func.in_type &&
     not (typing_is_finished func.Func.out_type)
  then (
    !logger.debug "%s: Completing out_type because it won't change any \
                   more." !cur_func_name ;
    let out_type = untyped_tuple_type func.Func.out_type in
    check_finished_tuple_type TupleOut out_type ;
    true
  ) else false

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
  let open RamenExpr in
  match typ.nullable with
  | None ->
    !logger.debug "%s%s: Set %a to %snullable" indent
      !cur_func_name
      print_typ typ
      (if nullable then "" else "not ") ;
    typ.nullable <- Some nullable ;
    true
  | Some n ->
    if n <> nullable then (
      let e = InvalidNullability {
        what = typ.expr_name ;
        must_be_nullable = n } in
      raise (SyntaxError e)
    ) else false

let set_scalar_type ?(indent="") ~ok_if_larger ~expr_name typ scalar_typ =
  match typ.RamenExpr.scalar_typ with
  | None ->
    !logger.debug "%s%s: Improving %s from %a" indent
      !cur_func_name expr_name RamenScalar.print_typ scalar_typ ;
    typ.RamenExpr.scalar_typ <- Some scalar_typ ;
    true
  | Some to_typ when to_typ <> scalar_typ ->
    if RamenScalar.can_enlarge ~from:to_typ ~to_:scalar_typ
    then (
      !logger.debug "%s%s: Improving %a from %a" indent
        !cur_func_name RamenExpr.print_typ typ
        RamenScalar.print_typ scalar_typ ;
      typ.scalar_typ <- Some scalar_typ ;
      true
    ) else if ok_if_larger then false
    else
      let e = CannotTypeExpression {
        what = expr_name ;
        expected_type = IO.to_string RamenScalar.print_typ to_typ ;
        got_type = IO.to_string RamenScalar.print_typ scalar_typ } in
      raise (SyntaxError e)
  | _ -> false

(* Improve to_ while checking compatibility with from.
 * Numerical types of to_ can be enlarged to match those of from. *)
let check_expr_type ~indent ~ok_if_larger ~set_null ~from ~to_ =
  let changed =
    match from.RamenExpr.scalar_typ with
    | Some scalar_typ ->
      set_scalar_type ~indent ~ok_if_larger
                      ~expr_name:to_.RamenExpr.expr_name to_ scalar_typ
    | _ -> false in
  if set_null then
    match from.RamenExpr.nullable with
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
    match parent.Func.out_type with
    | UntypedTuple untyped_tuple ->
      if not untyped_tuple.finished_typing then
        raise ParentIsUntyped ;
      List.assoc field untyped_tuple.fields
    | TypedTuple { user ; _ } ->
      List.find_map (fun f ->
        if f.RamenTuple.typ_name = field then Some (
          (* Mimick a untyped_tuple which uniq_num will never be used *)
          RamenExpr.{ expr_name = f.typ_name ;
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
        IO.to_string print_tuple_type parent.out_type } in
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
           if typ.RamenExpr.nullable = ptyp.RamenExpr.nullable &&
              typ.RamenExpr.scalar_typ = ptyp.RamenExpr.scalar_typ
           then Some typ else
           let e = FieldNotSameTypeInAllParents { field } in
           raise (SyntaxError e))
      ) None parents with
  | None ->
    let e = NoParentForField { field } in
    raise (SyntaxError e) ;
  | Some ptyp -> ptyp

(* From the list of operand types, return the largest type able to accommodate
 * all operands. Most of the time it will be the largest in term of "all
 * others can be enlarged to that one", but for special cases where we want
 * an even larger type; For instance, if we combine an i8 and an u8 then we
 * want the result to be an i16, or if we combine an IPv4 and an IPv6 then
 * we want the result to be an IP. *)
let largest_type = function
  | fst :: rest ->
    List.fold_left large_enough_for fst rest
  | _ -> invalid_arg "largest_type"


(* Get rid of the short-cutting of or expressions: *)
let (|||) a b = a || b

(* Check that this expression fulfill the type expected by the caller (exp_type).
 * Also, improve exp_type (set typ and nullable, enlarge numerical types ...).
 * When we recurse from an operator to its operand we set the exp_type to the one
 * in the operator so we improve typing of the AST along the way.
 * in_type is the currently know input type of the func, while parents are the
 * currently known output types of this func's parents. We will bring new field
 * in in_type from parents as needed.
 * Notice than when we recurse within the expression we stays in the same func
 * with the same parents. parents is not modified in here. *)
let rec check_expr ?(depth=0) ~parents ~in_type ~out_type ~exp_type ~params =
  let indent = String.make depth ' ' and depth = depth + 2 in
  let open RamenExpr in
  (* Check that the operand [sub_expr] is compatible with expectation (re. type
   * and null) set by the caller (the operator). [op_typ] is used for printing
   * only. Extends the type of sub_expr as required. *)
  let check_operand op_typ ?exp_sub_typ ?exp_sub_nullable sub_expr =
    let sub_typ = typ_of sub_expr in
    !logger.debug "%sChecking operand of (%a), of type (%a) (expected: %a)"
      indent print_typ op_typ print_typ sub_typ
      (Option.print RamenScalar.print_typ) exp_sub_typ ;
    (* Start by recursing into the sub-expression to know its real type: *)
    let changed = check_expr ~depth ~parents ~in_type ~out_type
                             ~exp_type:sub_typ ~params sub_expr in
    (* Now we check this comply with the operator expectations about its
     * operand : *)
    (match sub_typ.scalar_typ, exp_sub_typ with
    | Some actual_typ, Some exp_sub_typ ->
      if not (RamenScalar.can_enlarge ~from:actual_typ ~to_:exp_sub_typ) then
        let e = CannotTypeExpression {
          what = "Operand of "^ op_typ.expr_name ;
          expected_type = IO.to_string RamenScalar.print_typ exp_sub_typ ;
          got_type = IO.to_string RamenScalar.print_typ actual_typ } in
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
  let check_operator op_typ actual_typ =
    !logger.debug "%sChecking operator %a, of actual type %a" indent
      print_typ op_typ
      RamenScalar.print_typ actual_typ ;
    let from = make_typ ~typ:actual_typ op_typ.expr_name in
    let changed = check_expr_type ~indent ~ok_if_larger:false
                                  ~set_null:false ~from ~to_:op_typ in
    check_expr_type ~indent ~ok_if_larger:false ~set_null:true
                    ~from:op_typ ~to_:exp_type || changed
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
            changed, false, None, (typ.nullable :: prev_nullables)
          | Some scal, Some lst ->
            changed, all_typed && scalar_finished_typing scal,
            Some (scal :: lst), (typ.nullable :: prev_nullables)
        ) (false, true, Some [], []) args in
    (* If we have any nullable operands we may want to make the operator
     * nullable as well: *)
    let changed =
      if propagate_null then
        if List.exists ((=) (Some true)) nullables then
          set_nullable ~indent op_typ true || changed
        else if List.for_all ((=) (Some false)) nullables then
          set_nullable ~indent op_typ false || changed
        else changed
      else changed in
    (* If we have typed all the operands, find out the type of the
     * operator. *)
    !logger.debug "%soperands types: %a, all typed=%b" indent
      (Option.print (List.print (RamenScalar.print_typ))) types all_typed ;
    match types with
    | Some lst when all_typed ->
      !logger.debug "%sall operands typed, propagating to operator" indent ;
      (* List.fold inverted lst, which would confuse make_op_typ: *)
      let lst = List.rev lst in
      let actual_op_typ =
        try make_op_typ lst
        with _ ->
          let e = CannotCombineTypes { what = op_typ.expr_name } in
          raise (SyntaxError e) in
      check_operator op_typ actual_op_typ || changed
    | _ -> changed
  in
  let rec check_variadic op_typ ?(propagate_null=true) ?exp_sub_typ
                                ?exp_sub_nullable =
    function
    | [] -> false
    | sub_expr :: rest ->
      let changed =
        check_operand op_typ ?exp_sub_typ ?exp_sub_nullable sub_expr in
        (* TODO: a function to type with a list of arguments and
         * exp types etc, iteratively. Ie. make_op_typ called at
         * each step to refine it. For now, no typing of the op from
         * the args. Especially, XXX no update of the nullability of
         * op! XXX *)
      check_variadic op_typ ~propagate_null ?exp_sub_typ ?exp_sub_nullable
                     rest || changed
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
  !logger.debug "%s-- Typing expression %a"
    indent (RamenExpr.print true) expr ;
  match expr with
  | Const (op_typ, _) ->
    (* op_typ is already optimal. But is it compatible with exp_type? *)
    check_expr_type ~indent ~ok_if_larger:false ~set_null:true
                    ~from:op_typ ~to_:exp_type
  | Field (op_typ, tuple, field) ->
    if tuple_has_type_input !tuple then (
      (* Check that this field is, or could be, in in_type *)
      if is_virtual_field field then false else
      match List.assoc field in_type.fields with
      | exception Not_found ->
        !logger.debug "%sCannot find field in in-tuple" indent ;
        if in_type.finished_typing then (
          let e = FieldNotInTuple {
            field ; tuple = !tuple ;
            tuple_type = IO.to_string print_untyped_tuple in_type } in
          raise (SyntaxError e)
        ) else (
          (* If all our parents have this field let's bring it in! *)
          match type_of_parents_field parents !tuple field with
          | exception ParentIsUntyped -> false
          | ptyp ->
            !logger.debug "%sCopying field %s from parents, with type %a"
              indent field RamenExpr.print_typ ptyp ;
            if is_private_field field then (
              let m = InvalidPrivateField { field } in
              raise (SyntaxError m)) ;
            let copy = RamenExpr.copy_typ ptyp in
            in_type.fields <- (field, copy) :: in_type.fields ;
            true)
      | from ->
        (if in_type.finished_typing then ( (* Save the type *)
          Option.map_default
            (set_nullable ~indent op_typ) false from.nullable |||
          Option.map_default
            (set_scalar_type ~indent ~ok_if_larger:true
                             ~expr_name:from.expr_name op_typ)
            false from.scalar_typ
        ) else false) |||
        check_expr_type ~indent ~ok_if_larger:false ~set_null:true
                        ~from ~to_:exp_type
    ) else if tuple_has_type_output !tuple then (
      (* Some tuples are passed to callback via an option type, and are None
       * each time they are undefined (beginning of worker or of group).
       * Workers access those fields via a special functions, and all fields
       * are forced nullable during typing. *)
      let tuple_is_optional =
        !tuple = TupleGroupPrevious || !tuple = TupleOutPrevious in
      (* In those cases, as only the generators are passed rather than
       * the actual tuples, access to generated fields is forbidden (see
       * RamenOperation.check) *)
      (* First thing we know is that if the field is from
       * Tuple{Out,Group}Previous then it is nullable: *)
      (
        if tuple_is_optional then (
          !logger.debug "%sField %s is from previous therefore nullable."
            indent field ;
          set_nullable ~indent exp_type true
        ) else false
      ) ||| (
        (* If we already have this field in out then check it's compatible (or
         * enlarge out or exp). If we don't have it then add it. *)
        if is_virtual_field field then false else
        match List.assoc field out_type.fields with
        | exception Not_found ->
          !logger.debug "%sCannot find field %s in out-tuple" indent field ;
          if out_type.finished_typing then (
            let e = FieldNotInTuple {
              field ; tuple = !tuple ;
              tuple_type = IO.to_string print_untyped_tuple in_type } in
            raise (SyntaxError e)) ;
          out_type.fields <- (field, exp_type) :: out_type.fields ;
          true
        | out ->
          !logger.debug "%sfield %s found in out type: %a"
            indent field print_typ out ;
          (if out_type.finished_typing then ( (* Save the type *)
            Option.map_default (set_nullable ~indent op_typ) false (
              (* Regardless of the actual type of the output tuple, fields from
               * group.previous must always be considered nullable as the whole
               * tuple is optional: *)
              if tuple_is_optional then Some true else out.nullable) |||
            Option.map_default
              (set_scalar_type ~indent ~ok_if_larger:true
                               ~expr_name:out.expr_name op_typ)
              false out.scalar_typ
          ) else false) |||
          check_expr_type ~indent ~ok_if_larger:false ~from:out ~to_:exp_type
                          ~set_null:(not tuple_is_optional)
      )
    ) else if !tuple = TupleParam then (
      (* Copy the scalar type from the default value: *)
      match List.assoc field params with
      | exception Not_found ->
        let e =
          FieldNotInTuple { field ; tuple = !tuple ; tuple_type = "" } in
        raise (SyntaxError e)
      | default_value ->
        let typ = RamenScalar.type_of default_value in
        !logger.debug "%sParameter %s of type %a"
          indent field RamenScalar.print_typ typ ;
        set_nullable ~indent op_typ false |||
        set_scalar_type ~indent ~ok_if_larger:false ~expr_name:field
                        op_typ typ |||
        (* Then propagate upward: *)
        check_expr_type ~indent ~ok_if_larger:false ~set_null:true
                        ~from:op_typ ~to_:exp_type
    ) else (
      (* All other tuples are already typed (virtual fields) *)
      if not (is_virtual_field field) then (
        !logger.error "Field %a.%s is not virtual!?"
          tuple_prefix_print !tuple field ;
        assert false
      ) ;
      false
    )
  | StateField _ -> assert false
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
          let chg =
            check_expr ~depth ~parents ~in_type ~out_type ~exp_type:cond_typ
                       ~params alt.case_cond ||
            check_expr_type ~indent ~ok_if_larger:false ~set_null:false
                            ~from:exp_cond_type ~to_:cond_typ in
          !logger.debug "%sTyping CASE: condition type is now %a (changed: %b)"
            indent print_typ (typ_of alt.case_cond) chg ;
          chg
        ) alts
    ) || (
      !logger.debug "%sTyping CASE: enlarging CASE from ELSE" indent ;
      match else_ with
      | Some else_ ->
        (* First type the else_ then use the actual type to enlarge exp_type: *)
        let typ = typ_of else_ in
        let chg =
          check_expr ~depth ~parents ~in_type ~out_type ~exp_type:typ
                     ~params else_ ||
          check_expr_type ~indent ~ok_if_larger:true ~set_null:false
                          ~from:typ ~to_:exp_type in
        !logger.debug "%sTyping CASE: CASE type is now %a (changed: %b)"
          indent print_typ exp_type chg ;
        chg
      | None ->
        !logger.debug "%sTyping CASE: No ELSE clause so CASE can be NULL"
          indent ;
        set_nullable ~indent exp_type true
    ) || (
      (* Enlarge exp_type with the consequents: *)
      !logger.debug "%sTyping CASE: enlarging CASE from consequents" indent ;
      List.exists (fun alt ->
          (* First typecheck the consequent, then use it to enlarge exp_type: *)
          let typ = typ_of alt.case_cons in
          let chg =
            check_expr ~depth ~parents ~in_type ~out_type ~exp_type:typ
                       ~params alt.case_cons ||
            check_expr_type ~indent ~ok_if_larger:true ~set_null:false
                            ~from:typ ~to_:exp_type in
          !logger.debug "%sTyping CASE: consequent type is %a, and CASE \
                         type is now %a (changed: %b)" indent
            print_typ typ print_typ exp_type chg ;
          chg
        ) alts
    ) || (
      (* So all consequents and conditions and the else clause should
       * have been fully typed now. Let's check: *)
      !logger.debug "%sTyping CASE: all items should have been fully typed \
                     by now:" indent ;
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
            !logger.debug "%sTyping CASE: Setting the CASE to %sNULLable"
              indent (if nullable then "" else "not ") ;
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
    !logger.debug "%sTyping COALESCE: enlarging COALESCE from elements"
      indent ;
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
        let chg =
          check_expr ~depth ~parents ~in_type ~out_type ~exp_type:typ
                     ~params e ||
          check_expr_type ~indent ~ok_if_larger:true ~set_null:false
                          ~from:typ ~to_:exp_type in
        !logger.debug "%sTyping COALESCE: expr type is %a, and COALESCE \
                       type is now %a (changed: %b)" indent
          print_typ typ print_typ exp_type chg ;
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
  | StatelessFun0 (op_typ, Now) ->
    check_expr_type ~indent ~ok_if_larger:false ~set_null:true
                    ~from:op_typ ~to_:exp_type
  | StatelessFun0 (op_typ, Random) ->
    check_expr_type ~indent ~ok_if_larger:false ~set_null:true
                    ~from:op_typ ~to_:exp_type
  | StatefulFun (op_typ, _, AggrMin e) | StatefulFun (op_typ, _, AggrMax e)
  | StatefulFun (op_typ, _, AggrFirst e) | StatefulFun (op_typ, _, AggrLast e) ->
    check_op op_typ List.hd [None, None, e]
  | StatefulFun (op_typ, _, AggrSum e) | StatelessFun1 (op_typ, Age, e)
  | StatelessFun1 (op_typ, Abs, e)
  | StatelessFun1 (op_typ, Minus, e) ->
    check_op op_typ List.hd [Some TFloat, None, e]
  | StatefulFun (op_typ, _, AggrAvg e) ->
    check_op op_typ return_float [Some TFloat, None, e]
  | StatefulFun (op_typ, _, AggrAnd e) | StatefulFun (op_typ, _, AggrOr e)
  | StatelessFun1 (op_typ, Not, e) ->
    check_op op_typ List.hd [Some TBool, None, e]
  | StatelessFun1 (op_typ, Cast, e) ->
    (* No type restriction on the operand: we might want to forbid some
     * types at some point, for instance strings... *)
    check_op op_typ (fun _ -> Option.get op_typ.scalar_typ) [None, None, e]
  | StatelessFun1 (op_typ, Defined, e) ->
    check_op op_typ return_bool  ~propagate_null:false [None, Some true, e]
  | StatefulFun (op_typ, _, AggrPercentile (e1, e2)) ->
    check_op op_typ List.last [Some TFloat, None, e1 ; Some TFloat, None, e2]
  | StatelessFun2 (op_typ, (Add|Sub|Mul), e1, e2) ->
    check_op op_typ largest_type [Some TFloat, None, e1 ; Some TFloat, None, e2]
  | StatelessFun2 (op_typ, Concat, e1, e2) ->
    check_op op_typ return_string [Some TString, None, e1 ; Some TString, None, e2]
  | StatelessFunMisc (op_typ, Like (e, _)) ->
    check_op op_typ return_bool [Some TString, None, e]
  | StatelessFun2 (op_typ, Pow, e1, e2) ->
    check_op op_typ return_float [Some TFloat, None, e1 ; Some TFloat, None, e2]
  | StatelessFun2 (op_typ, Div, e1, e2) ->
    (* Same as above but always return a float *)
    check_op op_typ return_float [Some TFloat, None, e1 ; Some TFloat, None, e2]
  | StatelessFun2 (op_typ, IDiv, e1, e2) ->
    check_op op_typ largest_type [Some TFloat, None, e1 ; Some TFloat, None, e2]
  | StatelessFun2 (op_typ, Mod, e1, e2) ->
    check_op op_typ largest_type [Some TI128, None, e1 ; Some TI128, None, e2]
  | StatelessFun2 (op_typ, Sequence, e1, e2) ->
    check_op op_typ return_i128 [Some TI128, None, e1 ; Some TI128, None, e2]
  | StatelessFun1 (op_typ, Length, e) ->
    check_op op_typ return_u16 [Some TString, None, e]
  | StatelessFun1 (op_typ, Lower, e) ->
    check_op op_typ return_string [Some TString, None, e]
  | StatelessFun1 (op_typ, Upper, e) ->
    check_op op_typ return_string [Some TString, None, e]
  | StatelessFun2 (op_typ, (Ge|Gt|Eq as op), e1, e2) ->
    (* Ge and Gt works for many types and Eq for even more, as long as
     * both operands have the same type: *)
    let ret = ref false in
    (try
      [| (* type, also ge/gt *)
         TString, true ;
         TFloat, true ;
         TIp, true ;
         TCidr, false |] |>
      Array.filter (fun (_, also_g) -> also_g || op = Eq) |>
      Array.iter (fun (typ, _) ->
        try
          ret := check_op op_typ return_bool
                   [Some typ, None, e1 ; Some typ, None, e2] ;
          raise Exit
        with SyntaxError _ as e ->
          !logger.debug "%sComparison between %s failed with %S"
            indent (RamenScalar.string_of_typ typ) (Printexc.to_string e)) ;
      (* The last error we got was not super useful: one operand failed to be
       * a CIDR. Instead the real error is that both operands do not agree
       * on types: *)
      let e = CannotCompareTypes {
        what = IO.to_string (RamenExpr.print true) expr } in
      raise (SyntaxError e)
    with Exit -> !ret)
  | StatelessFun2 (op_typ, (And|Or), e1, e2) ->
    check_op op_typ return_bool [Some TBool, None, e1 ; Some TBool, None, e2]
  | StatelessFun2 (op_typ, (BitAnd|BitOr|BitXor), e1, e2) ->
    check_op op_typ largest_type [Some TI128, None, e1 ; Some TI128, None, e2]
  | StatelessFun1 (op_typ, (BeginOfRange|EndOfRange), e) ->
    (* Not really bullet-proof in theory since check_op may update the
     * types of the operand, but in this case there is no modification
     * possible if it's either TCidrv4 or TCidrv6, so we should be good.  *)
    (try check_op op_typ (fun _ -> TIpv4) [Some TCidrv4, None, e]
    with _ ->
      try check_op op_typ (fun _ -> TIpv6) [Some TCidrv6, None, e]
      with _ -> check_op op_typ (fun _ -> TIp) [Some TCidr, None, e])
  | StatelessFunMisc (op_typ, (Min es | Max es)) ->
    let ret = ref false in
    (try
      [| TFloat ; TString ; TIp ; TCidr |] |>
      Array.iter (fun typ ->
        try
          ret := check_op op_typ largest_type
                   (List.map (fun e -> Some typ, None, e) es) ;
          raise Exit
        with SyntaxError _ as e ->
          !logger.debug "%smin/max failed with %S"
            indent (Printexc.to_string e)) ;
      let e = CannotCompareTypes {
        what = IO.to_string (RamenExpr.print true) expr } in
      raise (SyntaxError e)
    with Exit -> !ret)

  | StatefulFun (op_typ, _, Lag (e1, e2)) ->
    (* e1 must be an unsigned small constant integer. For now that mean user
     * must have entered a constant. Later we might pre-evaluate constant
     * expressions into constant values. *)
    (* FIXME: Check that the const is > 0 *)
    check_const "lag" e1 ;
    (* ... and e2 can be anything and the type of lag will be the same,
     * nullable (since we might lag beyond the start of the window. *)
    check_op op_typ List.last [Some TU64, Some false, e1 ; None, None, e2]
  | StatefulFun (op_typ, _, MovingAvg (e1, e2, e3))
  | StatefulFun (op_typ, _, LinReg (e1, e2, e3)) ->
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
  | StatelessFun1 (op_typ, (Exp|Log|Sqrt), e) ->
    check_op op_typ return_float [Some TFloat, None, e]
  | StatelessFun1 (op_typ, Hash, e) ->
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
  | StatefulFun (op_typ, _, Distinct es) ->
    (* the es can be anything *)
    check_op op_typ return_bool (List.map (fun e -> None, None, e) es)
  | StatefulFun (op_typ, _, Hysteresis (meas, accept, max)) ->
    check_op op_typ return_bool
      [Some TFloat, None, meas ;
       Some TFloat, Some false, accept ;
       Some TFloat, Some false, max]
  | StatefulFun (op_typ, _, Top { want_rank ; what ; by ; n ; _ }) ->
    (* We already know the type returned by the top operation, but maybe
     * for the nullability. But we want to ensure the top-by expression
     * can be cast to a float: *)
    let ret_type =
      if want_rank then
        (fun _ -> RamenScalar.Parser.narrowest_typ_for_int n)
      else return_bool in
    check_op op_typ ret_type [ None, None, what ; Some TFloat, None, by ]

(* Given two tuple types, transfer all fields from the parent to the child,
 * while checking those already in the child are compatible. *)
let check_inherit_tuple ~including_complete ~is_subset ~from_prefix
                        ~from_tuple ~to_prefix ~to_tuple =
  (* Check that to_tuple is included in from_tuple (if is_subset) and if so
   * that they are compatible. Improve child type using parent type. *)
  let changed =
    List.fold_left (fun changed (child_name, child_field) ->
        match List.assoc child_name from_tuple.fields with
        | exception Not_found ->
          if is_subset && from_tuple.finished_typing then (
            let e = FieldNotInTuple {
              field = child_name ;
              tuple = from_prefix ;
              tuple_type = "" (* TODO *) } in
            raise (SyntaxError e)) ;
          changed (* no-op *)
        | parent_field ->
          check_expr_type ~indent:"  " ~ok_if_larger:false ~set_null:true
                          ~from:parent_field ~to_:child_field ||
          changed
      ) false to_tuple.fields in
  (* Add new fields into children. *)
  let changed =
    List.fold_left (fun changed (parent_name, parent_field) ->
        match List.assoc parent_name to_tuple.fields with
        | exception Not_found ->
          if to_tuple.finished_typing then (
            let e = FieldNotInTuple {
              field = parent_name ;
              tuple = to_prefix ;
              tuple_type = "" (* TODO *) } in
            raise (SyntaxError e)) ;
          let copy = RamenExpr.copy_typ parent_field in
          to_tuple.fields <- (parent_name, copy) :: to_tuple.fields ;
          true
        | _ ->
          changed (* We already checked those types above. All is good. *)
      ) changed from_tuple.fields in
  (* If typing of from_tuple is finished then so is to_tuple *)
  let changed =
    if including_complete &&
       from_tuple.finished_typing &&
       not to_tuple.finished_typing
    then (
      !logger.debug "Completing to_tuple from check_inherit_tuple" ;
      check_finished_tuple_type to_prefix to_tuple ;
      true
    ) else changed in
  changed

let check_selected_fields ~parents ~in_type ~out_type params fields =
  List.fold_left (fun changed sf ->
    let name = sf.RamenOperation.alias in
    !logger.debug "Type-check field %s" name ;
    let exp_type =
      match List.assoc name out_type.fields with
      | exception Not_found ->
        (* Start from the type we already know from the expression
         * because it is already set in some cases (virtual fields -
         * and for them that's our only change to get this type) *)
        let typ =
          let open RamenExpr in
          match sf.RamenOperation.expr with
          | Field (t, _, _) ->
            (* Note: we must create a new type for out distinct from the type
             * of the expression in case the expression is another field (from
             * in, say) because we do not want to alias them. *)
            copy_typ ~name t
          | _ ->
            let typ = typ_of sf.RamenOperation.expr in
            typ.expr_name <- name ;
            typ
        in
        !logger.debug "Adding out-field %s (operation: %s)"
          name typ.RamenExpr.expr_name ;
        out_type.fields <- (name, typ) :: out_type.fields ;
        typ
      | typ ->
        !logger.debug "... already in out, current type is %a"
          RamenExpr.print_typ typ ;
        typ in
    check_expr ~depth:1 ~parents ~in_type ~out_type ~exp_type ~params
               sf.RamenOperation.expr || changed
  ) false fields

let tuple_type_is_finished = function
  | TypedTuple _ -> true
  | UntypedTuple untyped_tuple -> untyped_tuple.finished_typing

let check_input_finished ~parents ~in_type =
  if not in_type.finished_typing &&
     List.for_all (fun par ->
       tuple_type_is_finished par.Func.out_type) parents
  then (
    !logger.debug "Completing in_type because none of our parent output \
                   will change any more." ;
    check_finished_tuple_type TupleIn in_type ;
    true
  ) else false

(* Return, as a set, all public fields of an operation output type: *)
let public_fields = function
  | TypedTuple { ser ; _ } ->
      List.fold_left (fun s t ->
          Set.add t.RamenTuple.typ_name s
        ) Set.empty ser
  | UntypedTuple ttt ->
      List.fold_left (fun s (name, _) ->
          if is_private_field name then s
          else Set.add name s
        ) Set.empty ttt.fields

let all_finished funcs =
  List.for_all (fun func ->
      tuple_type_is_finished func.Func.out_type
    ) funcs

let check_aggregate parents func fields and_all_others merge sort where key
                    top commit_when flush_how =
  let in_type = untyped_tuple_type func.Func.in_type
  and out_type = untyped_tuple_type func.Func.out_type
  and params = func.params in
  let open RamenOperation in
  (
    (* Improve in_type using parent out_type and out_type using in_type if we
     * propagates it all: *)
    (* TODO: find a way to do this only once, since once all_finished
     * returns true there is no use for another try. *)
    if and_all_others && parents <> [] && all_finished parents then (
      (* Add all the public fields present (same name and same type) in all the
       * parents, and ignore the fields that are not present everywhere. *)
      let inter_parents =
        List.fold_left (fun inter parent ->
            Set.intersect inter (public_fields parent.Func.out_type)
          ) (public_fields (List.hd parents).out_type) (List.tl parents) in
      (* Remove those that have different types: *)
      let inter_ptyps =
        Set.fold (fun field ptyps ->
            match type_of_parents_field parents TupleOut field with
            | exception SyntaxError _ -> ptyps
            | ptyp -> (field, ptyp) :: ptyps) inter_parents [] in
      !logger.debug "Adding * fields: %a"
        print_untyped_tuple_fields inter_ptyps ;
      let from_tuple = C.{ finished_typing = true ;
                           fields = inter_ptyps } in
      (* This is supposed to propagate parent completeness into in-tuple. *)
      check_inherit_tuple ~including_complete:true ~is_subset:true
                          ~from_prefix:TupleOut ~from_tuple
                          ~to_prefix:TupleIn ~to_tuple:in_type
      |||
      (* If all other fields are selected, add them *)
      check_inherit_tuple ~including_complete:false ~is_subset:false
                          ~from_prefix:TupleIn ~from_tuple:in_type
                          ~to_prefix:TupleOut ~to_tuple:out_type
    ) else (
      (* If we do not select star then we will bring in fields from (all)
       * parents as they are needed. *)
      false
    )
  ) ||| (
    (* Improve in and out_type using all expressions. Check we satisfy in_type. *)
    List.fold_left (fun changed e ->
      let exp_type = RamenExpr.typ_of e in
      check_expr ~depth:1 ~parents ~in_type ~out_type ~exp_type ~params e ||
      changed
    ) false merge
  ) ||| (
    match sort with
    | None -> false
    | Some (_, u_opt, b) ->
        let changed =
          List.fold_left (fun changed e ->
            let exp_type = RamenExpr.typ_of e in
            check_expr ~depth:1 ~parents ~in_type ~out_type ~exp_type
                       ~params e ||
            changed
          ) false b in
        (match u_opt with
        | None -> changed
        | Some u ->
            let exp_type = RamenExpr.typ_of u in
            check_expr ~depth:1 ~parents ~in_type ~out_type ~exp_type
                       ~params u ||
            changed)
  ) ||| (
    List.fold_left (fun changed k ->
        (* The key can be anything *)
        let exp_type = RamenExpr.typ_of k in
        check_expr ~depth:1 ~parents ~in_type ~out_type ~exp_type ~params k ||
        changed
      ) false key
  ) ||| (
    match top with
    | None -> false
    | Some (n, by) ->
      (* See the Lag operator for remarks about precomputing constants *)
      RamenExpr.check_const "top size" n ;
      (* check_expr will try to improve exp_type. We don't care we just want
       * to check it does not raise an exception. Here exp_type is build at
       * every call so we wouldn't make progress anyway. *)
      ignore
        (check_expr ~depth:1 ~parents ~in_type ~out_type
                    ~exp_type:(RamenExpr.make_num_typ "top size") ~params n) ;
      ignore
        (check_expr ~depth:1 ~parents ~in_type ~out_type
                    ~exp_type:(RamenExpr.make_num_typ "top-by clause")
                    ~params by) ;
      false
  ) ||| (
    let exp_type = RamenExpr.typ_of commit_when in
    set_nullable exp_type false |||
    set_scalar_type ~ok_if_larger:false ~expr_name:"commit-clause"
                    exp_type TBool |||
    check_expr ~depth:1 ~parents ~in_type ~out_type ~exp_type ~params
               commit_when
  ) ||| (
    match flush_how with
    | Reset | Never | Slide _ -> false
    | RemoveAll e | KeepOnly e ->
      let exp_type = RamenExpr.typ_of e in
      set_nullable exp_type false |||
      set_scalar_type ~ok_if_larger:false ~expr_name:"flush-clause"
                      exp_type TBool |||
      check_expr ~depth:1 ~parents ~in_type ~out_type ~exp_type ~params e
  ) ||| (
    (* Check the expression, improving out_type and checking against in_type: *)
    let exp_type = RamenExpr.typ_of where in
    set_nullable exp_type false |||
    set_scalar_type ~ok_if_larger:false ~expr_name:"where clause"
                    exp_type TBool |||
    check_expr ~depth:1 ~parents ~in_type ~out_type ~exp_type ~params where
  ) ||| (
    (* Also check other expression and make use of them to improve out_type.
     * Everything that's selected must be (added) in out_type. *)
    check_selected_fields ~parents ~in_type ~out_type params fields
  ) || (
    (* If nothing changed so far and our parents output is finished_typing,
     * then so is our input. *)
    check_input_finished ~parents ~in_type
  )

(*
 * Improve out_type using in_type and this func operation.
 * in_type is a given, don't modify it!
 *)
let check_operation operation parents func =
  !logger.debug "-- Typing operation %a" RamenOperation.print operation ;
  let set_well_known_out_type typ =
    if tuple_type_is_finished func.Func.out_type then false else (
      let user = typ in
      let ser = RingBufLib.ser_tuple_typ_of_tuple_typ user in
      func.Func.out_type <- TypedTuple { user ; ser } ;
      func.Func.in_type <- TypedTuple { user = [] ; ser = [] } ;
      true)
  in
  let open RamenOperation in
  match operation with
  | Aggregate { fields ; and_all_others ; merge ; sort ; where ; key ; top ;
                commit_when ; flush_how ; _ } ->
    check_aggregate parents func fields and_all_others (fst merge) sort where
                    key top commit_when flush_how
  | ReadCSVFile { what = { fields ; _ } ; _ } ->
    if tuple_type_is_finished func.Func.out_type then false else (
      let user = fields in
      let ser = RingBufLib.ser_tuple_typ_of_tuple_typ user in
      func.Func.out_type <- TypedTuple { user ; ser } ;
      func.Func.in_type <- TypedTuple { user = [] ; ser = [] } ;
      true)
  | ListenFor { proto ; _ } ->
    set_well_known_out_type (RamenProtocols.tuple_typ_of_proto proto)
  | Instrumentation _ ->
    set_well_known_out_type RamenBinocle.tuple_typ

(*
 * Type inference for the graph
 *)

exception AlreadyCompiled
exception SyntaxErrorInFunc of string * syntax_error
exception MissingDependency of Func.t (* The one we depend on *)

let () =
  Printexc.register_printer (function
    | SyntaxErrorInFunc (n, e) ->
      Some ("In function "^ n ^": "^ string_of_syntax_error e)
    | MissingDependency func ->
      Some ("Missing dependency: "^
            func.Func.program_name ^"/"^ func.Func.name)
    | AlreadyCompiled -> Some "Already compiled"
    | _ -> None)

let check_func_types parents func =
  try ( (* Prepend the func name to any SyntaxError *)
    (* Try to improve out_type and the AST types using the in_type and the
     * operation: *)
    match func.Func.operation with
    | None -> false
    | Some operation ->
        check_operation operation parents func
  ) with SyntaxError e ->
    !logger.debug "Compilation error: %s\n%s"
      (string_of_syntax_error e) (Printexc.get_backtrace ()) ;
    raise (SyntaxErrorInFunc (func.Func.name, e))

let typed_of_untyped_tuple ?cmp = function
  | TypedTuple _ as x -> x
  | UntypedTuple untyped_tuple ->
      if not untyped_tuple.finished_typing then (
        let what = IO.to_string print_untyped_tuple untyped_tuple in
        raise (SyntaxError (CannotCompleteTyping what))) ;
      let user = tup_typ_of_untyped_tuplee untyped_tuple in
      let user =
        match cmp with None -> user
        | Some cmp -> List.fast_sort cmp user in
      let ser = RingBufLib.ser_tuple_typ_of_tuple_typ user in
      TypedTuple { user ; ser }

let get_selected_fields func =
  match func.Func.operation with
  | Some (Aggregate { fields ; _ }) -> Some fields
  | _ -> None

let forwarded_field func field =
  match get_selected_fields func with
  | None ->
      raise Not_found
  | Some fields ->
      List.find_map (fun sf ->
        match sf.RamenOperation.expr with
        | RamenExpr.Field (_, { contents = TupleIn }, fn) when fn = field ->
            Some sf.alias
        | _ ->
            None
      ) fields

let infer_event_time func event_time_opt =
  let open RamenEventTime in
  try
    Option.map (function ((f1, f1_scale), duration) ->
      (forwarded_field func f1, f1_scale),
      try
        match duration with
        | DurationConst _ -> duration
        | DurationField (f2, f2_scale) ->
            DurationField (forwarded_field func f2, f2_scale)
        | StopField (f2, f2_scale) ->
            StopField (forwarded_field func f2, f2_scale)
      with Not_found ->
        DurationConst 0.
    ) event_time_opt
  with Not_found -> None

let infer_factors func =
  (* All fields that we take without modifications are also our factors *)
  List.filter_map (fun factor ->
    try Some (forwarded_field func factor)
    with Not_found -> None)

(* Since we can have loops within a program we can not propagate types
 * immediately. Also, the star selection prevent us from propagating
 * input from output types in one go.
 * We do it bit by bit but will still make sure to make parent
 * output >= children input eventually, and that fields are encoded in
 * the specified order. *)
let set_all_types conf parents funcs =
  let fold_funcs f =
    Hashtbl.fold (fun _ func changed ->
      cur_func_name := func.Func.name ;
      f func || changed
    ) funcs false in
  let rec loop () =
    let changed =
      fold_funcs (fun func ->
        let parents = Hashtbl.find_default parents func.Func.name [] in
        check_func_types parents func
      ) ||
      fold_funcs try_finish_out_type ||
      fold_funcs last_chance_to_type_func
    in
    if changed then loop ()
  in
  loop () ;
  (* We reached the fixed point.
   * We still have a few things to check: *)
  Hashtbl.iter (fun _ func ->
    (* Check that input type no parents => no input *)
    let in_type = (untyped_tuple_of_tuple_type func.Func.in_type).fields in
    assert (func.Func.parents <> [] || in_type = []) ;
    (* Check that all expressions have indeed be typed: *)
    iter_fun_expr func (fun what typ ->
      if typ.nullable = None || typ.scalar_typ = None ||
         typ.scalar_typ = Some TNum || typ.scalar_typ = Some TAny then
        let e = CannotCompleteTyping what in
        raise (SyntaxErrorInFunc (func.Func.name, e)))
  ) funcs ;
  (* Not quite home and dry yet: *)
  Hashtbl.iter (fun _ func ->
    !logger.debug "func %S:\n\tinput type: %a\n\toutput type: %a"
      func.Func.name
      print_tuple_type func.Func.in_type
      print_tuple_type func.Func.out_type ;
    func.Func.in_type <- typed_of_untyped_tuple func.Func.in_type ;
    (* So far order was not taken into account. Reorder output types
     * to match selected fields (not strictly required but improves
     * user experience in the GUI). Beware that the Expr.typ has a
     * unique number so we cannot compare/copy them directly (although
     * that unique number is required to be unique only for a func,
     * better keep it unique globally in case we want to generate
     * several funcs in a single binary, and to avoid needless
     * confusion when debugging): *)
    let cmp =
      get_selected_fields func |>
      Option.map (fun selected_fields ->
        let sf_index name =
          try List.findi (fun _ sf ->
                sf.RamenOperation.alias = name) selected_fields |>
              fst
          with Not_found ->
            (* star-imported fields - throw them all at the end in no
             * specific order. TODO: in parent order? *)
            max_int in
        fun f1 f2 ->
          compare (sf_index f1.RamenTuple.typ_name)
                  (sf_index f2.RamenTuple.typ_name)) in
    func.Func.out_type <- typed_of_untyped_tuple ?cmp func.Func.out_type ;
    (* Finally, if no event time info or factors have been given then maybe
     * we can infer them from the parents (we consider only the first parent
     * here): *)
    Option.may (fun operation ->
      let parents = Hashtbl.find_default parents func.Func.name [] in
      if parents <> [] && func.event_time = None then (
        func.event_time <-
          infer_event_time func (List.hd parents).event_time ;
        if func.event_time <> None then
          !logger.debug "Function %s can reuse event time from parents"
            func.name
      ) ;
      if parents <> [] && func.factors = [] then (
        func.factors <-
          infer_factors func (List.hd parents).factors ;
        if func.factors <> [] then
          !logger.debug "Function %s can reuse factors %a from parents"
            func.name (List.print String.print) func.factors
      )
    ) func.operation ;
    (* Seal everything: *)
    func.Func.signature <- Func.signature conf func
  ) funcs

  (*$inject
    let test_type_single_func op_text =
      try
        let funcs = Hashtbl.create 3 in
        let txt = "DEFINE foo AS "^ op_text in
        let defs = RamenProgram.parse txt in
        let operation = (List.nth defs 0).RamenProgram.operation in
        Hashtbl.add funcs "test"
          (make_untyped_func "test" "foo" [] operation) ;
        let parents = BatHashtbl.of_list [ "test", [] ] in
        let conf =
          RamenConf.make_conf ~do_persist:false "/tmp/glop" in
        set_all_types conf parents funcs ;
        "ok"
      with e ->
        Printf.sprintf "Exception when parsing/typing operation %S: %s\n%s"
          op_text
          (Printexc.to_string e)
          (Printexc.get_backtrace ())

    let test_check_expr ?nullable ?typ expr_text =
      let exp_type = RamenExpr.make_typ ?nullable ?typ "test" in
      let in_type = make_untyped_tuple ()
      and out_type = make_untyped_tuple ()
      and parents = [] in
      finish_typing in_type ;
      let open RamenParsing in
      let p = RamenExpr.Parser.(p +- eof) in
      let exp =
        match p [] None Parsers.no_error_correction (stream_of_string expr_text) |>
              to_result with
        | Batteries.Bad e ->
          let err =
            BatIO.to_string (print_bad_result (RamenExpr.print false)) e in
          failwith err
        | Batteries.Ok (exp, _) -> exp in
      if not (check_expr ~parents ~in_type ~out_type ~exp_type ~params:[] exp) then
        failwith "Cannot type expression" ;
      BatIO.to_string (RamenExpr.print true) exp
   *)

  (*$= & ~printer:BatPervasives.identity
     "ok" (test_type_single_func "SELECT 1-1 AS x FROM foo")
     "ok" (test_type_single_func "SELECT 1-200 AS x FROM foo")
     "ok" (test_type_single_func "SELECT 1-4000000000 AS x FROM foo")
     "ok" (test_type_single_func "SELECT SEQUENCE AS x FROM foo")
     "ok" (test_type_single_func "SELECT 0 AS zero, 1 AS one, SEQUENCE AS seq FROM foo")
   *)

  (*$= test_check_expr & ~printer:(fun x -> x)
     "(1 [constant of type I32, not nullable]) + (1 [constant of type I32, not nullable]) [addition of type I32, not nullable]" \
       (test_check_expr "1+1")

     "(sum locally (1 [constant of type I16, not nullable]) [sum aggregation of type I16, not nullable]) > \\
      (500 [constant of type I32, not nullable]) [comparison (>) of type BOOL, not nullable]" \
       (test_check_expr "sum 1i16 > 500")

     "(sum locally (cast(I16, 1 [constant of type I32, not nullable]) [cast to I16 of type I16, not nullable]) \\
          [sum aggregation of type I16, not nullable]) > \\
      (500 [constant of type I32, not nullable]) [comparison (>) of type BOOL, not nullable]" \
       (test_check_expr "sum i16(1) > 500")

     "cast(I8, 1.5 [constant of type FLOAT, not nullable]) [cast to I8 of type I8, not nullable]" \
       (test_check_expr "i8(1.5)")

     "cast(I8, 9999 [constant of type I32, not nullable]) [cast to I8 of type I8, not nullable]" \
       (test_check_expr "i8(9999)")
   *)
