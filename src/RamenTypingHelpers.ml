open Batteries
open RamenLog
open RamenHelpers
module C = RamenConf
module VSI = RamenSync.Value.SourceInfo
module E = RamenExpr
module T = RamenTypes
module O = RamenOperation
open RamenLang

(* Return the field alias in operation corresponding to the given input field: *)
let forwarded_field operation (field : N.field) =
  match operation with
  | O.Aggregate { fields ; _ } ->
      List.find_map (fun sf ->
        match sf.O.expr.E.text with
        | E.Stateless (SL2 (Get, { text = Const (VString n) ; _ },
                                 { text = Variable In ; _ }))
          when n = (field :> string) ->
            Some sf.alias
        | E.Stateless (SL0 (Path [ Name n ]))
          when n = field ->
            Some sf.alias
        | _ ->
            None
      ) fields
  | _ -> raise Not_found

let forwarded_field_or_param parent_prog_name prog_name func field = function
  | RamenEventTime.Parameter ->
      (* The scope of any given parameter is the program.
       * It must be assumed that parameters with same name have different
       * values in different programs. *)
      if prog_name = parent_prog_name then field
      else raise Not_found
  | RamenEventTime.OutputField ->
      forwarded_field func.VSI.operation field

let infer_event_time prog_name func parent_prog_name parent =
  let open RamenEventTime in
  try
    O.event_time_of_operation parent.VSI.operation |>
    Option.map (function ((f1, f1_src, f1_scale), duration) ->
      (forwarded_field_or_param parent_prog_name prog_name func f1 !f1_src, f1_src, f1_scale),
      try
        match duration with
        | DurationConst _ -> duration
        | DurationField (f2, f2_src, f2_scale) ->
            DurationField (forwarded_field_or_param parent_prog_name prog_name func f2 !f2_src, f2_src, f2_scale)
        | StopField (f2, f2_src, f2_scale) ->
            StopField (forwarded_field_or_param parent_prog_name prog_name func f2 !f2_src, f2_src, f2_scale)
      with Not_found ->
        DurationConst 0.)
  with Not_found -> None

let infer_factors func =
  (* All fields that we take without modifications are also our factors *)
  List.filter_map (fun factor ->
    try Some (forwarded_field func.VSI.operation factor)
    with Not_found -> None)

let infer_field_doc_aggr func parents params =
  let set_doc alias doc =
    if doc <> "" then (
      !logger.debug "Function %a can reuse parent doc for %a"
        N.func_print func.VSI.name
        N.field_print alias ;
      let ft =
        O.out_type_of_operation ~with_private:true func.VSI.operation |>
        List.find (fun ft ->
          ft.RamenTuple.name = alias) in
      ft.doc <- doc)
  and set_aggr alias aggr =
    if aggr <> None then (
      !logger.debug "Function %a can reuse parent default aggr for %a"
        N.func_print func.VSI.name
        N.field_print alias ;
      let ft =
        O.out_type_of_operation ~with_private:true func.VSI.operation |>
        List.find (fun ft ->
          ft.RamenTuple.name = alias) in
      ft.aggr <- aggr)
  in
  match func.VSI.operation with
    | O.Aggregate { fields ; _ } ->
        List.iter (function
        | O.{
            alias ; doc ; aggr ;
            expr = E.{ text = Stateless (SL0 (Path [ Name n ])) ; _ }}
            when doc = "" || aggr = None ->
            (* Look for this field n in parent: *)
            let _parent_prog_name, parent = List.hd parents in
            let out_type = parent.VSI.operation |>
                           O.out_type_of_operation ~with_private:true in
            (match List.find (fun ft -> ft.RamenTuple.name = n) out_type with
            | exception Not_found -> ()
            | psf ->
                if doc = "" then set_doc alias psf.doc ;
                if aggr = None then set_aggr alias psf.aggr) ;
        | O.{
            alias ; doc ; aggr ; expr = E.{
              text = Stateless (SL2 (Get, { text = Const (VString n) ; _ },
                                          { text = Variable Param ; _ })) ;
              _ } }
            when doc = "" || aggr = None ->
            let n = N.field n in
            (match List.find (fun param ->
                     param.RamenTuple.ptyp.name = n
                   ) params with
            | exception Not_found -> ()
            | param ->
                let p = param.RamenTuple.ptyp in
                if doc = "" then set_doc alias p.doc ;
                if aggr = None then set_aggr alias p.aggr)
        | _ -> ()
      ) fields
  | _ -> ()

let check_typed ?what clause _stack e =
  let open RamenExpr in
  match e.E.typ.T.structure with
  | TNum | TAny ->
      Printf.sprintf2 "%s%s: Cannot complete typing of %s, \
                       still of type %a"
        (Option.map_default (fun w -> w ^", ") "" what) clause
        (IO.to_string (print true) e)
        T.print_typ e.typ |>
    failwith
  | _ -> ()

let dump_io func =
  let in_type =
    RamenFieldMaskLib.in_type_of_operation func.VSI.operation in
  !logger.debug "func %S:\n\tinput type: %a\n\toutput type: %a"
    (func.VSI.name :> string)
    RamenFieldMaskLib.print_in_type in_type
    RamenTuple.print_typ
      (O.out_type_of_operation ~with_private:false func.operation)

let function_signature func params =
  (* We'd like to be formatting independent so that operation text can be
   * reformatted without ramen recompiling it. For this it is not OK to
   * strip redundant white spaces as some of those might be part of literal
   * string values. So we print it, trusting the printer to be exhaustive.
   * This is not enough to print the expression with types, as those do not
   * contain relevant info such as field rank. We therefore print without
   * types and encode input/output types explicitly below.
   * Also, notice that the program-wide running condition does not alter
   * the function signature, and rightfully so, as a change in the running
   * condition does not imply we should disregard past data or consider the
   * function changed in any way. It's `ramen run` job to evaluate the
   * running condition independently. *)
  let op_str =
    IO.to_string (O.print false) func.VSI.operation
  and in_type =
    RamenFieldMaskLib.in_type_of_operation func.operation
  and out_type =
    O.out_type_of_operation ~with_private:false func.operation in
  "OP="^ op_str ^
  ";IN="^ RamenFieldMaskLib.in_type_signature in_type ^
  ";OUT="^ RamenTuple.type_signature out_type ^
  (* Similarly to input type, also depends on the parameters type: *)
  ";PRM="^ RamenTuple.params_type_signature params |>
  N.md5

let finalize_func parents params prog_name func =
  dump_io func ;
  (* Check that no parents => no input *)
  let func_in_type =
    RamenFieldMaskLib.in_type_of_operation func.operation in
  assert (parents <> [] || func_in_type = []) ;
  (* Check that all expressions have indeed be typed: *)
  let what =
    Printf.sprintf "In function %s "
      (N.func_color func.VSI.name) in
  O.iter_expr (check_typed ~what) func.VSI.operation ;
  (* Not quite home and dry yet.
   * If no event time info or factors have been given then maybe
   * we can infer them from the parents (we consider only the first parent
   * here) (TODO: consider all of them until event time can be inferred
   * from one): *)
  if parents <> [] &&
     O.event_time_of_operation func.operation = None
  then (
    let parent_prog_name, parent = List.hd parents in
    let inferred = infer_event_time prog_name func parent_prog_name parent in
    func.operation <-
      O.operation_with_event_time func.operation inferred ;
    if inferred <> None then
      !logger.debug "Function %s can reuse event time from parents"
        (func.name :> string)
  ) ;
  if parents <> [] &&
     O.factors_of_operation func.operation = []
  then (
    let _parent_prog_name, parent = List.hd parents in
    let inferred =
      O.factors_of_operation parent.VSI.operation |>
      infer_factors func in
    func.operation <-
      O.operation_with_factors func.operation inferred ;
    if inferred <> [] then
      !logger.debug "Function %a can reuse factors %a from parents"
        N.func_print func.name
        (List.print N.field_print) inferred
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
  func.VSI.out_record <-
    O.out_record_of_operation ~with_private:false func.operation ;
  func.VSI.factors <- O.factors_of_operation func.operation ;
  func.VSI.signature <- function_signature func params ;
  let in_type = RamenFieldMaskLib.in_type_of_operation func.operation in
  func.VSI.in_signature <- N.md5 (RamenFieldMaskLib.in_type_signature in_type)
