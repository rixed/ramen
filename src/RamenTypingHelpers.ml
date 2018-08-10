open Batteries
open RamenLog
open RamenHelpers
module C = RamenConf
module F = C.Func
module Expr = RamenExpr
open RamenLang
open RamenTypes

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

let print_untyped_tuple_fields oc fs =
  List.print ~first:"{" ~last:"}" ~sep:", "
    (fun oc (name, expr_typ) ->
      Printf.fprintf oc "%s: %a"
        name
        Expr.print_typ expr_typ) oc fs

let print_untyped_tuple oc t =
  Printf.fprintf oc "%a (%s)"
    print_untyped_tuple_fields t.fields
    (if t.finished_typing then "finished typing" else "to be typed")

let untyped_tuple_copy t =
  { t with fields =
      List.map (fun (name, typ) -> name, Expr.copy_typ typ) t.fields }

type tuple_type = UntypedTuple of untyped_tuple
                | TypedTuple of RamenTuple.typed_tuple

let tuple_copy = function
  | UntypedTuple t -> UntypedTuple (untyped_tuple_copy t)
  | t -> t

let tuple_is_typed = function
  | TypedTuple _ -> true
  | UntypedTuple _ -> false

let typing_is_finished = function
  | TypedTuple _ -> true
  | UntypedTuple t -> t.finished_typing

let print_tuple_type oc = function
  | UntypedTuple untyped_tuple ->
      print_untyped_tuple oc untyped_tuple
  | TypedTuple t ->
      RamenTuple.(print_typ oc t.user)

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
        Expr.make_typ ?nullable:f.RamenTuple.typ.nullable
                           ~typ:f.typ.structure f.typ_name in
      t.fields <- t.fields @ [f.typ_name, expr_typ]
    ) tup_typ ;
  finish_typing t ;
  t

let untyped_tuple_of_tuple_type = function
  | UntypedTuple untyped_tuple -> untyped_tuple
  | TypedTuple { ser ; _ } -> untyped_tuple_of_tup_typ ser

let tup_typ_of_untyped_tuple ttt =
  assert ttt.finished_typing ;
  List.map (fun (name, typ) ->
    assert (typ.Expr.nullable <> None) ;
    RamenTuple.{
      typ_name = name ;
      typ = { structure = Option.get typ.Expr.scalar_typ ;
              nullable = typ.Expr.nullable } ;
      units = None }
  ) ttt.fields

module Func =
struct
  type t =
    { program_name : RamenName.program ;
      (* Within a program, funcs are identified by a name that can be
       * optionally provided automatically if its not meant to be referenced.
       *)
      name : RamenName.func ;
      (* Parsed operation (for untyped funs) and its in/out types: *)
      operation : RamenOperation.t option ;
      mutable in_type : tuple_type ;
      mutable out_type : tuple_type ;
      parents : F.parent list ;
      (* The signature used to name compiled modules *)
      mutable signature : string ;
      (* Extracted from the operation or inferred from parents: *)
      mutable event_time : RamenEventTime.t option ;
      mutable factors : string list ;
      mutable envvars : string list }

  let copy t =
    { t with in_type = tuple_copy t.in_type ;
             out_type = tuple_copy t.out_type }

  let signature conf func params =
    (* We'd like to be formatting independent so that operation text can be
     * reformatted without ramen recompiling it. For this it is not OK to
     * strip redundant white spaces as some of those might be part of literal
     * string values. So we print it, trusting the printer to be exhaustive.
     * This is not enough to print the expression with types, as those do not
     * contain relevant info such as field rank. We therefore print without
     * types and encode input/output types explicitly below: *)
    "OP="^ IO.to_string RamenOperation.print (Option.get func.operation) ^
    "IN="^ RamenTuple.type_signature (typed_tuple_type func.in_type).ser ^
    "OUT="^ RamenTuple.type_signature (typed_tuple_type func.out_type).ser ^
    (* Similarly to input type, also depends on the parameters type: *)
    "PRM="^ RamenTuple.param_types_signature params |>
    md5

  let dump_io func =
    !logger.debug "func %S:\n\tinput type: %a\n\toutput type: %a"
      (RamenName.string_of_func func.name)
      print_tuple_type func.in_type
      print_tuple_type func.out_type ;
end
module Program =
struct
  type t =
    { name : string ;
      mutable funcs : (string, Func.t) Hashtbl.t }
end

let make_untyped_func program_name func_name params operation =
  !logger.debug "Creating func %s/%s"
    (RamenName.string_of_program program_name)
    (RamenName.string_of_func func_name) ;
  let parents =
    RamenOperation.parents_of_operation operation (*|>
    List.map (fun (prog_opt, func_name) ->
      Option.map exp_program_of_id prog_opt, func_name)*) in
  Func.{
    program_name ; name = func_name ; signature = "" ;
    operation = Some operation ; parents ;
    in_type = UntypedTuple (make_untyped_tuple ()) ;
    out_type = UntypedTuple (make_untyped_tuple ()) ;
    event_time = RamenOperation.event_time_of_operation operation ;
    factors = RamenOperation.factors_of_operation operation ;
    envvars = RamenOperation.envvars_of_operation operation }

(* Same as the above, for when a function has already been compiled: *)
let make_typed_func program_name rcf =
  Func.{
    program_name ; name = rcf.F.name ;
    signature = rcf.F.signature ;
    operation = None ; parents = [] ;
    in_type = TypedTuple rcf.F.in_type ;
    out_type = TypedTuple rcf.F.out_type ;
    event_time = rcf.F.event_time ;
    factors = rcf.F.factors ;
    envvars = rcf.F.envvars }

let rec structure_finished_typing = function
  | TNum | TAny | TTuple [||] | TVec (0, _) ->
      false
  | TTuple ts ->
      Array.for_all typ_finished_typing ts
  | TVec (_, t) | TList t ->
      typ_finished_typing t
  | _ ->
      true

and typ_finished_typing t =
  t.nullable <> None && structure_finished_typing t.structure

(* Check that we have typed all that need to be typed, and set finished_typing *)
let check_finished_tuple_type tuple_prefix tuple_type =
  List.iter (fun (field_name, typ) ->
    let open RamenExpr in
    (* If we couldn't determine nullability for an out field, it means
     * we can pick freely: *)
    let type_is_known =
      match typ.scalar_typ with
      | None -> false
      | Some t -> structure_finished_typing t in
    if tuple_prefix = TupleOut &&
       type_is_known && typ.nullable = None
    then (
      !logger.debug "Field %s has no constraint on nullability. \
                     Let's make it non-null." field_name ;
      typ.nullable <- Some false) ;
    if typ.nullable = None || not type_is_known then (
      let e = CannotTypeField {
        field = field_name ;
        typ = IO.to_string print_typ typ ;
        tuple = tuple_prefix } in
      raise (SyntaxError e))
  ) tuple_type.fields ;
  finish_typing tuple_type

let compare_typers funcs res res_smt =
  let expr_of_id id =
    let res = ref None in (* I wish we had an exception Return of 'a *)
    try
      Hashtbl.iter (fun _ func ->
        RamenOperation.iter_expr (fun e ->
          if RamenExpr.(typ_of e).uniq_num = id then (
            res := Some e ; raise Exit)
        ) (Option.get func.Func.operation)
      ) funcs ;
      raise Not_found
    with Exit -> Option.get !res in
  let expr_is_null id =
    let e = expr_of_id id in
    RamenExpr.(typ_of e).expr_name = "NULL"
  in
  let num_nosol = ref 0 in
  Hashtbl.iter (fun id (structure, nullable) ->
    match Hashtbl.find res_smt id with
    | exception Not_found ->
        (* If it's a literal "null", SMT is actually right: *)
        if not (expr_is_null id) then (
          (if !num_nosol < 20 then
            !logger.warning
              "SMT have no solution for expression %d (%a)"
              id (RamenExpr.print true) (expr_of_id id)
          else if !num_nosol = 20 then
            !logger.warning "SMT is lacking more solutions...") ;
          incr num_nosol)
    | smt ->
        let open RamenTypes in
        if structure <> smt.structure &&
            (* Once again, for NULL the handcrafted parser pick a type at
             * random, while the SMT is more clever: *)
           not (expr_is_null id)
        then
          !logger.warning "SMT resolves expression %d into a %a \
                           instead of a %a!"
            id
            print_structure smt.structure
            print_structure structure ;
        match smt.nullable with
        | None ->
            !logger.warning "SMT cannot find out the nullability of \
                             expression %d!" id
        | Some n when n <> nullable ->
            !logger.warning "SMT erroneously thinks expression %d is \
                             %snullable!"
              id (if n then "" else "not ")
        | _ -> ()
  ) res

let iter_fun_expr func f =
  RamenOperation.iter_expr (fun e ->
    let open RamenExpr in
    let typ = typ_of e in
    let what = IO.to_string (print true) e in
    f what typ
  ) (Option.get func.Func.operation)

let get_selected_fields func =
  match func.Func.operation with
  | Some (Aggregate { fields ; _ }) -> Some fields
  | _ -> None

let typed_of_untyped_tuple ?cmp = function
  | TypedTuple _ as x -> x
  | UntypedTuple untyped_tuple ->
      if not untyped_tuple.finished_typing then (
        let what = IO.to_string print_untyped_tuple untyped_tuple in
        raise (SyntaxError (CannotCompleteTyping what))) ;
      let user = tup_typ_of_untyped_tuple untyped_tuple in
      let user =
        match cmp with None -> user
        | Some cmp -> List.fast_sort cmp user in
      let ser = RingBufLib.ser_tuple_typ_of_tuple_typ user in
      TypedTuple { user ; ser }

let forwarded_field func field =
  match get_selected_fields func with
  | None ->
      raise Not_found
  | Some fields ->
      List.find_map (fun sf ->
        match sf.RamenOperation.expr with
        | Expr.Field (_, { contents = TupleIn }, fn) when fn = field ->
            Some sf.alias
        | _ ->
            None
      ) fields

let forwarded_field_or_param parent func field = function
  | RamenEventTime.Parameter ->
      (* The scope of any given parameter is the program.
       * It must be assumed that parameters with same name have different
       * values in different programs. *)
      if func.Func.program_name = parent.Func.program_name then field
      else raise Not_found
  | RamenEventTime.OutputField ->
      forwarded_field func field

let infer_event_time func parent =
  let open RamenEventTime in
  try
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
        DurationConst 0.
    ) parent.event_time
  with Not_found -> None

let infer_factors func =
  (* All fields that we take without modifications are also our factors *)
  List.filter_map (fun factor ->
    try Some (forwarded_field func factor)
    with Not_found -> None)

let finalize_func conf parents params func =
  (* Check that, for Aggregate functions, no parents => no input *)
  (match func.Func.operation with
  | Some RamenOperation.Aggregate _ ->
    let in_type = (untyped_tuple_of_tuple_type func.in_type).fields in
    assert (func.Func.parents <> [] || in_type = [])
  | _ -> ()) ;
  (* Check that all expressions have indeed be typed: *)
  iter_fun_expr func (fun what typ ->
    if typ.nullable = None || typ.scalar_typ = None ||
       typ.scalar_typ = Some TNum || typ.scalar_typ = Some TAny then
      Printf.sprintf2 "Cannot complete typing of %s, still of type %a, \
                       in function %s"
        what RamenExpr.print_typ typ
        (RamenName.string_of_func func.Func.name) |>
      failwith) ;
  (* Not quite home and dry yet: *)
  Func.dump_io func ;
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
   * here) (TODO: consider all of them until event time can be inferred
   * from one): *)
  Option.may (fun operation ->
    let parents = Hashtbl.find_default parents func.Func.name [] in
    if parents <> [] && func.event_time = None then (
      func.event_time <-
        infer_event_time func (List.hd parents) ;
      if func.event_time <> None then
        !logger.debug "Function %s can reuse event time from parents"
          (RamenName.string_of_func func.name)
    ) ;
    if parents <> [] && func.factors = [] then (
      func.factors <-
        infer_factors func (List.hd parents).factors ;
      if func.factors <> [] then
        !logger.debug "Function %s can reuse factors %a from parents"
          (RamenName.string_of_func func.name)
          (List.print String.print) func.factors
    )
  ) func.operation ;
  (* Seal everything: *)
  func.Func.signature <- Func.signature conf func params
