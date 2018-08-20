open Batteries
open RamenLog
open RamenHelpers
module C = RamenConf
module F = C.Func
module Expr = RamenExpr
open RamenLang
open RamenTypes

let forwarded_field func operation field =
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

let forwarded_field_or_param parent func operation field = function
  | RamenEventTime.Parameter ->
      (* The scope of any given parameter is the program.
       * It must be assumed that parameters with same name have different
       * values in different programs. *)
      if func.F.program_name = parent.F.program_name then field
      else raise Not_found
  | RamenEventTime.OutputField ->
      forwarded_field func operation field

let infer_event_time func operation parent =
  let open RamenEventTime in
  try
    Option.map (function ((f1, f1_src, f1_scale), duration) ->
      (forwarded_field_or_param parent func operation f1 !f1_src, f1_src, f1_scale),
      try
        match duration with
        | DurationConst _ -> duration
        | DurationField (f2, f2_src, f2_scale) ->
            DurationField (forwarded_field_or_param parent func operation f2 !f2_src, f2_src, f2_scale)
        | StopField (f2, f2_src, f2_scale) ->
            StopField (forwarded_field_or_param parent func operation f2 !f2_src, f2_src, f2_scale)
      with Not_found ->
        DurationConst 0.
    ) parent.event_time
  with Not_found -> None

let infer_factors func operation =
  (* All fields that we take without modifications are also our factors *)
  List.filter_map (fun factor ->
    try Some (forwarded_field func operation factor)
    with Not_found -> None)

let finalize_func conf parents params func operation =
  F.dump_io func ;
  (* Check that no parents => no input *)
  assert (func.F.parents <> [] || func.F.in_type = []) ;
  (* Check that all expressions have indeed be typed: *)
  RamenOperation.iter_expr (fun e ->
    let open RamenExpr in
    let typ = typ_of e in
    let what = IO.to_string (print true) e in
    match typ.typ with
    | None | Some { structure = (TNum | TAny) ; _ } ->
       Printf.sprintf2 "Cannot complete typing of %s, still of type %a, \
                        in function %s"
         what RamenExpr.print_typ typ
         (RamenName.string_of_func func.F.name) |>
      failwith
    | _ -> ()
  ) operation ;
  (* Not quite home and dry yet.
   * If no event time info or factors have been given then maybe
   * we can infer them from the parents (we consider only the first parent
   * here) (TODO: consider all of them until event time can be inferred
   * from one): *)
  let parents = Hashtbl.find_default parents func.F.name [] in
  if parents <> [] && func.event_time = None then (
    func.event_time <-
      infer_event_time func operation (List.hd parents) ;
    if func.event_time <> None then
      !logger.debug "Function %s can reuse event time from parents"
        (RamenName.string_of_func func.name)
  ) ;
  if parents <> [] && func.factors = [] then (
    func.factors <-
      infer_factors func operation (List.hd parents).factors ;
    if func.factors <> [] then
      !logger.debug "Function %s can reuse factors %a from parents"
        (RamenName.string_of_func func.name)
        (List.print String.print) func.factors
  ) ;
  (* Seal everything: *)
  let op_str = IO.to_string RamenOperation.print operation in
  func.F.signature <- F.signature conf func op_str params
