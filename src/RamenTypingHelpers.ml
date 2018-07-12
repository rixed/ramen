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

let tuple_copy = function
  | UntypedTuple t -> UntypedTuple (untyped_tuple_copy t)
  | t -> t

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
              nullable = typ.Expr.nullable } }
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
      parents : (RamenName.program_exp option * RamenName.func) list ;
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
    "PRM="^ RamenTuple.param_types_signature params ^
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

let make_untyped_func program_name func_name params operation =
  !logger.debug "Creating func %s/%s"
    (RamenName.string_of_program program_name)
    (RamenName.string_of_func func_name) ;
  let parents =
    RamenOperation.parents_of_operation operation |>
    List.map (fun (prog_opt, func_name) ->
      Option.map exp_program_of_id prog_opt, func_name) in
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
