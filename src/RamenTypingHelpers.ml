open Batteries
open RamenLog
open RamenHelpers
module C = RamenConf
module F = C.Func
module E = RamenExpr
module T = RamenTypes
module O = RamenOperation
open RamenLang

(* Tells if an expression is a direct Get from TupleIn: *)
let is_forwarding_input ?specific_field ?(from=E.TupleIn) env e =
  match e.E.text with
  | E.(Stateless (SL2 (Get, { text = Const (VString fn) ; _ },
                            { text = Variable var_name ; _ })))
      when specific_field = None ||
           specific_field = Some (RamenName.field_of_string fn) ->
      (match E.Env.lookup "is_forwarding_input" env var_name with
      | exception Not_found -> None
      | tup_pref when tup_pref = from -> Some (RamenName.field_of_string fn)
      | _ -> None)
  | _ -> None

(* Return the field alias in operation corresponding to the given input field: *)
let forwarded_field operation field =
  match operation with
  | O.Aggregate { output = { text = Record (_, sfs) ; _ } ; _ } ->
      List.find_map (fun sf ->
        if is_forwarding_input ~specific_field:field [] sf.E.expr <> None
        then
          Some sf.alias
        else
          None
      ) sfs
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
    O.event_time_of_operation parent.F.operation |>
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
        O.field_types_of_operation func.F.operation |>
        Enum.find (fun (k, _) -> k = RamenName.string_of_field alias) |>
        snd in
      ft.doc <- doc)
  and set_aggr alias aggr =
    if aggr <> None then (
      !logger.debug "Function %a can reuse parent default aggr for %a"
        RamenName.func_print func.F.name
        RamenName.field_print alias ;
      let ft =
        O.field_types_of_operation func.F.operation |>
        Enum.find (fun (k, _) -> k = RamenName.string_of_field alias) |>
        snd in
      ft.aggr <- aggr)
  in
  let env =
    E.Env.[ env_in ; env_group ; env_previous ; env_env ; env_param ] in
  O.fields_of_operation func.F.operation //
  (fun sf -> sf.doc = "" || sf.aggr = None) |>
  Enum.fold (fun env sf ->
    (match is_forwarding_input env sf.E.expr with
    | None ->
        (* Second chance: get doc from the params? *)
        (match is_forwarding_input ~from:E.TupleParam env sf.expr with
        | None -> ()
        | Some fn ->
            (match List.find (fun param ->
                     param.RamenTuple.ptyp.name = fn
                   ) params with
            | exception Not_found -> ()
            | param ->
                let p = param.RamenTuple.ptyp in
                if sf.doc = "" then set_doc sf.alias p.doc ;
                if sf.aggr = None then set_aggr sf.alias p.aggr))
    | Some fn ->
        (* Look for this field fn in parent: *)
        (match (List.hd parents).F.operation |>
               O.field_types_of_operation |>
               Enum.find (fun (k, _) -> k = RamenName.string_of_field fn) |>
               snd with
        | exception Not_found -> ()
        | ptyp ->
            if sf.doc = "" then set_doc sf.alias ptyp.doc ;
            if sf.aggr = None then set_aggr sf.alias ptyp.aggr)) ;
    if E.is_generator sf.expr then env else
      (sf.alias, E.TupleOut) :: env
  ) env |> ignore

let check_typed ~what e =
  match e.E.typ with
  | { structure = (TNum | TAny) ; _ } ->
      Printf.sprintf2 "%s: Cannot complete typing of %a"
        what (E.print true) e |>
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
  O.iter_expr (check_typed ~what) func.F.operation ;
  (* Not quite home and dry yet.
   * If no event time info or factors have been given then maybe
   * we can infer them from the parents (we consider only the first parent
   * here) (TODO: consider all of them until event time can be inferred
   * from one): *)
  let parents = Hashtbl.find_default parents func.F.name [] in
  if parents <> [] &&
     O.event_time_of_operation func.operation = None
  then (
    let inferred = infer_event_time func (List.hd parents) in
    func.operation <-
      O.operation_with_event_time func.operation inferred ;
    if inferred <> None then
      !logger.debug "Function %s can reuse event time from parents"
        (RamenName.string_of_func func.name)
  ) ;
  if parents <> [] &&
     O.factors_of_operation func.operation = []
  then (
    let inferred =
      O.factors_of_operation (List.hd parents).operation |>
      infer_factors func in
    func.operation <-
      O.operation_with_factors func.operation inferred ;
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
  if O.use_event_time func.operation &&
     O.event_time_of_operation func.operation = None
  then
     failwith "Cannot use #start/#stop without event time" ;
  (* Seal everything: *)
  func.F.signature <- F.signature func params
