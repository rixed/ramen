(* minimal tuple: the subset of the out tuple that must be finalized at
 * every input even in the absence of commit. We need those fields that
 * are used in the commit condition itself, or used as parameter of a
 * stateful function used by another field (as the state update function
 * will need its finalized value) and also if it's used to compute the
 * event time in any way, as we want to know the front time at every
 * input. Also for convenience any field that involves the print function.
 * Of course, any field required to compute a minimal field must also be
 * minimal. *)
open Batteries
open RamenHelpers
open RamenLang
open RamenLog
module E = RamenExpr
module N = RamenName
module O = RamenOperation
module T = RamenTypes

(* Returns the minimal tuple for an aggregate operation *)
let minimal_type func_op =
  let event_time = O.event_time_of_operation func_op
  and fields, commit_cond =
    match func_op with
    | O.Aggregate { fields ; commit_cond ; _ } -> fields, commit_cond
    | _ -> assert false in
  let out_typ = O.out_type_of_operation ~with_private:true func_op in
  let fetch_recursively s =
    let s = ref s in
    if not (reach_fixed_point (fun () ->
      let num_fields = Set.cardinal !s in
      List.iter (fun sf ->
        (* is this out field selected for minimal_out yet? *)
        if Set.mem sf.O.alias !s then (
          (* Add all other fields from out that are needed in this field
           * expression *)
          E.iter (fun _ e ->
            match e.E.text with
            | Binding (RecordField (Out, fn)) ->
                s := Set.add fn !s
            | _ -> ()
          ) sf.O.expr)
      ) fields ;
      Set.cardinal !s > num_fields))
    then failwith "Cannot build minimal_out set?!" ;
    !s in
  let add_if_needs_out s e =
    match e.E.text with
    | Binding (RecordField (Out, fn)) (* not supposed to happen *) ->
        Set.add fn s
    | Stateless (SL2 (Get, E.{ text = Const (VString fn) ; _ },
                           E.{ text = Variable Out ; _ })) ->
        Set.add (N.field fn) s
    | _ -> s in
  let minimal_fields =
    let from_commit_cond =
      E.fold (fun _ s e ->
        add_if_needs_out s e
      ) Set.empty commit_cond
    and for_updates =
      List.fold_left (fun s sf ->
        E.unpure_fold s (fun _ s e ->
          E.fold (fun _ s e ->
            add_if_needs_out s e
          ) s e
        ) sf.O.expr
      ) Set.empty fields
    and for_event_time =
      let req_fields = Option.map_default RamenEventTime.required_fields
                                          Set.empty event_time in
      List.fold_left (fun s sf ->
        if Set.mem sf.O.alias req_fields then
          Set.add sf.alias s
        else s
      ) Set.empty fields
    and for_printing =
      List.fold_left (fun s sf ->
        try
          E.iter (fun _ e ->
            match e.E.text with
            | Stateless (SL1s (Print, _)) -> raise Exit | _ -> ()
          ) sf.O.expr ;
          s
        with Exit -> Set.add sf.O.alias s
      ) Set.empty fields
    in
    (* Now combine these sets: *)
    Set.union from_commit_cond for_updates |>
    Set.union for_event_time |>
    Set.union for_printing |>
    fetch_recursively
  in
  !logger.debug "minimal fields: %a"
    (Set.print N.field_print) minimal_fields ;
  (* Replace removed values with a dull type. Should not be accessed
   * ever. This is because we want out and minimal to have the same
   * ramen type, so that field access works on both. *)
  List.map (fun ft ->
    if Set.mem ft.RamenTuple.name minimal_fields then
      ft
    else (* Replace it *)
      RamenTuple.{ ft with
        name = N.field ("_not_minimal_"^ (ft.name :> string)) ;
        typ = T.{ ft.typ with vtyp = Mac TBool (* wtv *) } }
  ) out_typ
