open Batteries
open RamenLog
open RamenHelpers
module C = RamenConf
module F = C.Func
module Expr = RamenExpr
open RamenLang
open RamenTypes

(* Return the field alias in operation corresponding to the given input field: *)
let forwarded_field operation field =
  match operation with
  | RamenOperation.Aggregate { fields ; _ } ->
      List.find_map (fun sf ->
        match sf.RamenOperation.expr with
        | Expr.Field (_, { contents = TupleIn }, fn) when fn = field ->
            Some sf.alias
        | _ ->
            None
      ) fields
  | _ -> raise Not_found

let forwarded_field_or_param parent func field = function
  | RamenEventTime.Parameter ->
      (* The scope of any given parameter is the program.
       * It must be assumed that parameters with same name have different
       * values in different programs. *)
      if func.F.program_name = parent.F.program_name then field
      else raise Not_found
  | RamenEventTime.OutputField ->
      forwarded_field func.F.operation field

let infer_event_time func parent =
  let open RamenEventTime in
  try
    RamenOperation.event_time_of_operation parent.F.operation |>
    Option.map (function ((f1, f1_src, f1_scale), duration) ->
      (forwarded_field_or_param parent func f1 !f1_src, f1_src, f1_scale),
      try
        match duration with
        | DurationConst _ -> duration
        | DurationField (f2, f2_src, f2_scale) ->
            DurationField (forwarded_field_or_param parent func f2 !f2_src, f2_src, f2_scale)
        | StopField (f2, f2_src, f2_scale) ->
            StopField (forwarded_field_or_param parent func f2 !f2_src, f2_src, f2_scale)
      with Not_found ->
        DurationConst 0.)
  with Not_found -> None

let infer_factors func =
  (* All fields that we take without modifications are also our factors *)
  List.filter_map (fun factor ->
    try Some (forwarded_field func.F.operation factor)
    with Not_found -> None)

let infer_field_doc_aggr func parents params =
  let set_doc alias doc =
    if doc <> "" then (
      !logger.debug "Function %a can reuse parent doc for %a"
        RamenName.func_print func.F.name
        RamenName.field_print alias ;
      let ft =
        RamenOperation.out_type_of_operation func.F.operation |>
        List.find (fun ft ->
          ft.RamenTuple.name = alias) in
      ft.doc <- doc)
  and set_aggr alias aggr =
    if aggr <> None then (
      !logger.debug "Function %a can reuse parent default aggr for %a"
        RamenName.func_print func.F.name
        RamenName.field_print alias ;
      let ft =
        RamenOperation.out_type_of_operation func.F.operation |>
        List.find (fun ft ->
          ft.RamenTuple.name = alias) in
      ft.aggr <- aggr)
  in
  match func.F.operation with
    | RamenOperation.Aggregate { fields ; _ } ->
        List.iter (function
        | RamenOperation.{
            alias ; doc ; aggr ;
            expr = Expr.Field (_, { contents = TupleIn }, fn) }
            when doc = "" || aggr = None ->
            (* Look for this field fn in parent: *)
            let out_type = (List.hd parents).F.operation |>
                           RamenOperation.out_type_of_operation in
            (match List.find (fun ft -> ft.RamenTuple.name = fn) out_type with
            | exception Not_found -> ()
            | psf ->
                if doc = "" then set_doc alias psf.doc ;
                if aggr = None then set_aggr alias psf.aggr) ;
        | RamenOperation.{
            alias ; doc ; aggr ;
            expr = Expr.Field (_, { contents = TupleParam }, fn) }
            when doc = "" || aggr = None ->
            (match List.find (fun param ->
                     param.RamenTuple.ptyp.name = fn
                   ) params with
            | exception Not_found -> ()
            | param ->
                let p = param.RamenTuple.ptyp in
                if doc = "" then set_doc alias p.doc ;
                if aggr = None then set_aggr alias p.aggr)
        | _ -> ()
      ) fields
  | _ -> ()

let check_typed ~what e =
  let open RamenExpr in
  let typ = typ_of e in
  match typ.typ with
  | None | Some { structure = (TNum | TAny) ; _ } ->
      Printf.sprintf2 "%s: Cannot complete typing of %s, \
                       still of type %a"
        what
        (IO.to_string (print true) e)
        RamenExpr.print_typ typ |>
    failwith
  | _ -> ()

let finalize_func parents params func =
  F.dump_io func ;
  (* Check that no parents => no input *)
  assert (func.F.parents <> [] || func.F.in_type = []) ;
  (* Check that all expressions have indeed be typed: *)
  let what =
    Printf.sprintf "In function %s "
      RamenName.(func_color func.F.name) in
  RamenOperation.iter_expr (check_typed ~what) func.F.operation ;
  (* Not quite home and dry yet.
   * If no event time info or factors have been given then maybe
   * we can infer them from the parents (we consider only the first parent
   * here) (TODO: consider all of them until event time can be inferred
   * from one): *)
  let parents = Hashtbl.find_default parents func.F.name [] in
  if parents <> [] &&
     RamenOperation.event_time_of_operation func.operation = None
  then (
    let inferred = infer_event_time func (List.hd parents) in
    func.operation <-
      RamenOperation.operation_with_event_time func.operation inferred ;
    if inferred <> None then
      !logger.debug "Function %s can reuse event time from parents"
        (RamenName.string_of_func func.name)
  ) ;
  if parents <> [] &&
     RamenOperation.factors_of_operation func.operation = []
  then (
    let inferred =
      RamenOperation.factors_of_operation (List.hd parents).operation |>
      infer_factors func in
    func.operation <-
      RamenOperation.operation_with_factors func.operation inferred ;
    if inferred <> [] then
      !logger.debug "Function %a can reuse factors %a from parents"
        RamenName.func_print func.name
        (List.print RamenName.field_print) inferred
  ) ;
  if parents <> [] then (
    infer_field_doc_aggr func parents params ;
  ) ;
  (* Check that we use the expressions that require the event time to be
   * defined only if it is indeed defined. Do not perform this check in
   * [RamenOperation.check] to give us chance to infer the event time
   * definition: *)
  if RamenOperation.use_event_time func.operation &&
     RamenOperation.event_time_of_operation func.operation = None
  then
     failwith "Cannot use #start/#stop without event time" ;
  (* Check expressions are "valid" (ie. obey some additional constraints) *)
  RamenOperation.iter_expr Expr.check func.operation ;
  (* Seal everything: *)
  func.F.signature <- F.signature func params
