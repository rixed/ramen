(* Various tools to help compilation, independent from the backend *)
open Batteries

open RamenHelpers
open RamenHelpersNoLog
open RamenLang
open RamenLog
module DT = DessserTypes
module E = RamenExpr
module N = RamenName
module O = RamenOperation
module T = RamenTypes
module Variable = RamenVariable

let expr_needs_tuple_from lst e =
  match e.E.text with
  | Stateless (SL0 (Variable tuple))
  | Stateless (SL0 (Binding (RecordField (tuple, _)))) ->
      List.mem tuple lst
  | _ ->
      false

(* Tells whether this expression requires the out tuple (or anything else
 * from the group). *)
let expr_needs_group e =
  expr_needs_tuple_from [ GroupState ; LocalLastOut ] e ||
  (match e.E.text with
  | Stateful { lifespan = Some LocalState ; _ } ->
      true
  | Stateless (SL0 (EventStart|EventStop)) ->
      (* This depends on the definition of the event time really.
       * TODO: pass the event time down here and actually check. *)
      true
  | _ ->
      false)

let operation_uses_local_last_out op =
  try
    O.iter_expr (fun _ _ e ->
      if expr_needs_tuple_from [ LocalLastOut ] e then
        raise Exit
    ) op ;
    false
  with Exit ->
    true

(* Depending on what uses a commit/flush condition, we might need to check
 * all groups after every single input tuple (very slow), or after every
 * selected input tuple (still quite slow), or only when this group is
 * modified (fast). Users should limit all/selected tuple to aggregations
 * with few groups only. *)
let check_commit_for_all expr =
  (* Tells whether the commit condition applies to all or only to the
   * selected group: *)
  try
    E.iter (fun _ e ->
      match e.E.text with
      | Stateless (SL0 (Path _ |
                        Variable In |
                        Binding (RecordField (In, _)))) ->
          raise Exit
      | _ -> ()
    ) expr ;
    false
  with Exit ->
    true

(* Takes an expression and if that expression is equivalent to
 * f(in) op g(out) then returns [f], [neg], [op], [g] where [neg] is true
 * if [op] is meant to be negated, or raise Not_found: *)
let rec defined_order = function
  | E.{ text = Stateless (SL2 ((Gt|Ge as op), l, r)) } ->
      let dep_only_on lst e =
        (* env and params are always ok on both sides of course: *)
        let lst = Variable.Env :: Param :: lst in
        let open RamenOperation in
        try check_depends_only_on lst e ; true
        with DependsOnInvalidVariable _ -> false
      and no_local_state e =
        try
          E.unpure_iter (fun _ -> function
            | E.{ text = Stateful { lifespan = Some LocalState ; _ } } ->
                raise Exit
            | _ -> ()) e ;
          true
        with Exit ->false
      in
      if dep_only_on [ In ] l &&
         no_local_state l &&
         dep_only_on [ Out ; GlobalLastOut ] r
      then l, false, op, r
      else if dep_only_on [ Out ; GlobalLastOut ] l &&
              dep_only_on [ In ] r &&
              no_local_state r
      then r, true, op, r
      else raise Not_found
  | E.{ text = Stateless (SL1 (Not, e)) } ->
      let l, neg, op, r = defined_order e in
      l, not neg, op, r
  | _ -> raise Not_found

let not_minimal_field_name name =
  N.field ("_not_minimal_"^ (name : N.field :> string))

(* Minimal tuple: the subset of the out tuple that must be finalized at
 * every input even in the absence of commit. We need those fields that
 * are used in the commit condition itself, or used as parameter of a
 * stateful function used by another field (as the state update function
 * will need its finalized value) and also if it's used to compute the
 * event time in any way, as we want to know the event time at every
 * input. Also for convenience any field that involves the print function.
 * Of course, any field required to compute a minimal field must also be
 * minimal. *)
let minimal_type func_op =
  let open Raql_select_field.DessserGen in
  let event_time = O.event_time_of_operation func_op
  and fields, commit_cond =
    match func_op with
    | O.Aggregate { aggregate_fields ; commit_cond ; _ } ->
        aggregate_fields, commit_cond
    | _ ->
        assert false in
  let out_typ = O.out_type_of_operation ~with_priv:true func_op in
  let fetch_recursively s =
    let s = ref s in
    if not (reach_fixed_point (fun () ->
      let num_fields = N.SetOfFields.cardinal !s in
      List.iter (fun sf ->
        (* is this out field selected for minimal_out yet? *)
        if N.SetOfFields.mem sf.alias !s then (
          (* Add all other fields from out that are needed in this field
           * expression *)
          E.iter (fun _ e ->
            match e.E.text with
            | Stateless (SL0 (Binding (RecordField (Out, fn)))) ->
                s := N.SetOfFields.add fn !s
            | Stateless (SL2 (
                  Get, { text = Stateless (SL0 (Const (VString fn))) ; _ },
                       { text = Stateless (SL0 (Variable Out)) ; _ })) ->
                s := N.SetOfFields.add (N.field fn) !s
            | _ -> ()
          ) sf.expr)
      ) fields ;
      N.SetOfFields.cardinal !s > num_fields))
    then failwith "Cannot build minimal_out set?!" ;
    !s in
  let add_if_needs_out s e =
    match e.E.text with
    | Stateless (SL0 (Binding (RecordField (Out, fn)))) ->
        (* not supposed to happen *)
        N.SetOfFields.add fn s
    | Stateless (SL2 (
          Get, E.{ text = Stateless (SL0 (Const (VString fn))) ; _ },
               E.{ text = Stateless (SL0 (Variable Out)) ; _ })) ->
        N.SetOfFields.add (N.field fn) s
    | _ -> s in
  let minimal_fields =
    let from_commit_cond =
      E.fold (fun _ s e ->
        add_if_needs_out s e
      ) N.SetOfFields.empty commit_cond
    and for_updates =
      List.fold_left (fun s sf ->
        E.unpure_fold s (fun _ s e ->
          E.fold (fun _ s e ->
            add_if_needs_out s e
          ) s e
        ) sf.expr
      ) N.SetOfFields.empty fields
    and for_event_time =
      let req_fields = Option.map_default RamenEventTime.required_fields
                                          N.SetOfFields.empty event_time in
      List.fold_left (fun s sf ->
        if N.SetOfFields.mem sf.alias req_fields then
          N.SetOfFields.add sf.alias s
        else s
      ) N.SetOfFields.empty fields
    and for_printing =
      List.fold_left (fun s sf ->
        try
          E.iter (fun _ e ->
            match e.E.text with
            | Stateless (SL1s (Print, _)) -> raise Exit | _ -> ()
          ) sf.expr ;
          s
        with Exit -> N.SetOfFields.add sf.alias s
      ) N.SetOfFields.empty fields
    in
    (* Now combine these sets: *)
    N.SetOfFields.union from_commit_cond for_updates |>
    N.SetOfFields.union for_event_time |>
    N.SetOfFields.union for_printing |>
    fetch_recursively
  in
  !logger.debug "minimal fields: %a"
    (N.SetOfFields.print N.field_print) minimal_fields ;
  (* Replace removed values with a dull type. Should not be accessed
   * ever. This is because we want out and minimal to have the same
   * number of fields, so that field access works on both.
   * Notice that this requirement stands for the legacy code generator,
   * which encode tuples as actual tuples, as well as the Dessser based
   * code generator, which encore tuples as records with fields named after
   * their order of appearance in the tuple! *)
  List.map (fun ft ->
    if N.SetOfFields.mem ft.RamenTuple.name minimal_fields then
      ft
    else (* Replace it *)
      RamenTuple.{ ft with
        name = not_minimal_field_name ft.name ;
        typ = DT.void }
  ) out_typ

(* Collect all stateful expressions used in [op]: *)
let stateful_expressions op =
  O.fold_expr ([], []) (fun _c _s (glo, loc as prev) e ->
    match e.E.text with
    | Stateful E.{ lifespan = Some GlobalState ; _ } ->
        e :: glo, loc
    | Stateful E.{ lifespan = Some LocalState; _ } ->
        glo, e :: loc
    | _ ->
        prev
  ) op

let var_name_of_record_field (k : N.field) =
  (k :> string) ^ "_" |>
  RamenOCamlCompiler.make_valid_ocaml_identifier

let dummy_var_name fn =
  "dummy_for_private" ^ var_name_of_record_field fn

let id_of_typ = function
  | DT.TVoid -> "unit"
  | TFloat -> "float"
  | TString -> "string"
  | TChar -> "char"
  | TBool -> "bool"
  | TU8 -> "u8"
  | TU16 -> "u16"
  | TU24 -> "u24"
  | TU32 -> "u32"
  | TU40 -> "u40"
  | TU48 -> "u48"
  | TU56 -> "u56"
  | TU64 -> "u64"
  | TU128 -> "u128"
  | TI8 -> "i8"
  | TI16 -> "i16"
  | TI24 -> "i24"
  | TI32 -> "i32"
  | TI40 -> "i40"
  | TI48 -> "i48"
  | TI56 -> "i56"
  | TI64 -> "i64"
  | TI128 -> "i128"
  | TUsr { name = "Eth" ; _ } -> "eth"
  | TUsr { name = "Ip4" ; _ } -> "ip4"
  | TUsr { name = "Ip6" ; _ } -> "ip6"
  | TUsr { name = "Ip" ; _ } -> "ip"
  | TUsr { name = "Cidr4" ; _ } -> "cidr4"
  | TUsr { name = "Cidr6" ; _ } -> "cidr6"
  | TUsr { name = "Cidr" ; _ } -> "cidr"
  | TExt _ -> assert false
  | TTup _ -> "tuple"
  | TRec _ -> "record"
  | TVec _  -> "vector"
  | TArr _ -> "list"
  | TMap _ -> assert false (* No values of that type *)
  | TUsr ut -> todo ("Generalize user types to "^ ut.DT.name)
  | TSum _ -> todo "id_of_typ for sum types"
  | _ -> invalid_arg "id_of_typ"
