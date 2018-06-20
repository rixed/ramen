(* This module deals with parsing programs.
 * A program is a set of named operations.
 * It makes use of RamenOperation, which parses operation.
 *)
open Batteries
open RamenLang
open RamenLog

(*$inject
  open TestHelpers
  open RamenLang
*)

type func =
  { name : [`Function] RamenName.t ;
    operation : RamenOperation.t }

type t = RamenTuple.param list * func list

let make_name =
  let seq = ref ~-1 in
  fun () ->
    incr seq ;
    RamenName.func_of_string ("f"^ string_of_int !seq)

let make_func ?name operation =
  { name = (match name with Some n -> n | None -> make_name ()) ;
    operation }

let print_param oc p =
  Printf.fprintf oc "PARAMETER %a DEFAULTS TO %a;"
    RamenTuple.print_field_typ p.RamenTuple.ptyp RamenTypes.print p.value

let print_func oc n =
  (* TODO: keep the info that func was anonymous? *)
  Printf.fprintf oc "DEFINE '%s' AS %a;"
    (RamenName.string_of_func n.name)
    RamenOperation.print n.operation

let print oc (params, funcs) =
  List.print ~sep:"\n" print_param oc params ;
  List.print ~sep:"\n" print_func oc funcs

let check (params, funcs) =
  List.fold_left (fun s p ->
    if Set.mem p.RamenTuple.ptyp.typ_name s then
      raise (SyntaxError (NameNotUnique p.ptyp.typ_name)) ;
    Set.add p.ptyp.typ_name s
  ) Set.empty params |> ignore ;
  List.fold_left (fun s n ->
    RamenOperation.check params n.operation ;
    if Set.mem n.name s then
      raise (SyntaxError (NameNotUnique (RamenName.string_of_func n.name))) ;
    Set.add n.name s
  ) Set.empty funcs |> ignore

module Parser =
struct
  (*$< Parser *)
  open RamenParsing

  let anonymous_func m =
    let m = "anonymous func" :: m in
    (RamenOperation.Parser.p >>: make_func) m

  let params m =
    let m = "parameter" :: m in
    (
      strinGs "parameter" -- blanks -+
        several ~sep:list_sep_and (
          non_keyword ++
          optional ~def:None (
            blanks -+ some RamenTuple.Parser.type_decl) ++
          optional ~def:RamenTypes.VNull (
            blanks -- strinGs "default" -- blanks -- strinG "to" -- blanks -+
            RamenTypes.Parser.p_ ~min_int_width:0) >>:
          fun ((typ_name, typ_decl), value) ->
            let typ, nullable, value =
              let open RamenTypes in
              match typ_decl with
              | None ->
                  if value = VNull then
                    let e =
                      Printf.sprintf
                        "Declaration of parameter %S must either specify \
                         the type or a non-null default value" typ_name in
                    raise (Reject e)
                  else
                    (* As usual, promote integers to I32 by default: *)
                    let vtyp = type_of value in
                    if can_enlarge ~from:vtyp ~to_:TI32 then
                      TI32, false, enlarge_value TI32 value
                    else
                      vtyp, false, value
              | Some (dtyp, nullable) ->
                  if value = VNull then dtyp, nullable, value else
                  (* Expand the parsed value type up to the declaration: *)
                  let vtyp = type_of value in
                  if can_enlarge ~from:vtyp ~to_:dtyp then
                    dtyp, nullable, enlarge_value dtyp value
                  else
                    let e =
                      Printf.sprintf2
                        "In declaration of parameter %S, type is \
                         incompatible with value %a"
                        typ_name print value in
                    raise (Reject e)
            in
            RamenTuple.{ ptyp = { typ_name ; nullable ; typ } ; value }
        )
    ) m

  let named_func m =
    let m = "function" :: m in
    (
      strinG "define" -- blanks -+ function_name +-
      blanks +- strinG "as" +- blanks ++
      RamenOperation.Parser.p >>: fun (name, op) -> make_func ~name op
    ) m

  let func m =
    let m = "func" :: m in
    (anonymous_func ||| named_func) m

  type definition =
    | DefFunc of func
    | DefParams of RamenTuple.params

  let p m =
    let m = "program" :: m in
    let sep = opt_blanks -- char ';' -- opt_blanks in
    (
      several ~sep ((func >>: fun f -> DefFunc f) |||
                    (params >>: fun lst -> DefParams lst)) +-
      optional ~def:() (opt_blanks -- char ';') >>: fun defs ->
        let params, funcs =
          List.fold_left (fun (params, funcs) -> function
            | DefFunc func -> params, func::funcs
            | DefParams lst -> List.rev_append lst params, funcs
          ) ([], []) defs in
        RamenTuple.params_sort params, funcs
    ) m

  (*$= p & ~printer:(test_printer print)
   (Ok (([], [\
    { name = RamenName.func_of_string "bar" ;\
      operation = \
        Aggregate {\
          fields = [\
            { expr = RamenExpr.Const (typ, VI32 42l) ;\
              alias = "the_answer" } ] ;\
          and_all_others = false ;\
          merge = [], 0. ;\
          sort = None ;\
          where = RamenExpr.Const (typ, VBool true) ;\
          notifications = [] ;\
          key = [] ;\
          commit_when = RamenExpr.Const (typ, VBool true) ;\
          commit_before = false ;\
          flush_how = Reset ;\
          event_time = None ;\
          from = [NamedOperation (None, RamenName.func_of_string "foo")] ; every = 0. ; factors = [] } } ]),\
      (46, [])))\
      (test_p p "DEFINE bar AS SELECT 42 AS the_answer FROM foo" |>\
       replace_typ_in_program)

   (Ok (([ RamenTuple.{ ptyp = { typ_name = "p1" ; nullable = false ; typ = TI32 } ;\
                        value = VI32 0l } ;\
           RamenTuple.{ ptyp = { typ_name = "p2" ; nullable = false ; typ = TI32 } ;\
                        value = VI32 0l } ], [\
    { name = RamenName.func_of_string "add" ;\
      operation = \
        Aggregate {\
          fields = [\
            { expr = RamenExpr.(\
                StatelessFun2 (typ, Add,\
                  Field (typ, ref TupleParam, "p1"),\
                  Field (typ, ref TupleParam, "p2"))) ;\
              alias = "res" } ] ;\
          every = 0. ; event_time = None ;\
          and_all_others = false ; merge = [], 0. ; sort = None ;\
          where = RamenExpr.Const (typ, VBool true) ;\
          notifications = [] ; key = [] ;\
          commit_when = RamenExpr.Const (typ, VBool true) ;\
          commit_before = false ; flush_how = Reset ; from = [] ;\
          factors = [] } } ]),\
      (84, [])))\
      (test_p p "PARAMETERS p1 DEFAULTS TO 0 AND p2 DEFAULTS TO 0; DEFINE add AS YIELD p1 + p2 AS res" |>\
       (function Ok ((ps, fs), _) as x -> check (ps, fs) ; x | x -> x) |>\
       replace_typ_in_program)
  *)

  (*$>*)
end

let reify_subquery =
  let seqnum = ref 0 in
  fun op ->
    let name = RamenName.func_of_string ("_"^ string_of_int !seqnum) in
    incr seqnum ;
    make_func ~name op

(* Returns a list of additional funcs and the list of parents that
 * contains only NamedOperations and GlobPattern: *)
let expurgate from =
  let open RamenOperation in
  List.fold_left (fun (new_funcs, from) -> function
    | SubQuery q ->
        let new_func = reify_subquery q in
        (new_func :: new_funcs), NamedOperation (None, new_func.name) :: from
    | (GlobPattern _ | NamedOperation _) as f -> new_funcs, f :: from
  ) ([], []) from

let reify_subqueries funcs =
  let open RamenOperation in
  List.fold_left (fun fs func ->
    match func.operation with
    | Aggregate ({ from ; _ } as f) ->
        let funcs, from = expurgate from in
        { func with operation = Aggregate { f with from } } ::
          funcs @ fs
    | Instrumentation ({ from ; _ }) ->
        let funcs, from = expurgate from in
        { func with operation = Instrumentation { from } } ::
          funcs @ fs
    | _ -> func :: fs
  ) [] funcs

(*
 * Friendlier version of the parser.
 * Allows for extra spaces and reports errors.
 * Also substitute real functions for subqueries.
 *)

let parse program =
  let p = RamenParsing.allow_surrounding_blanks Parser.p in
  let stream = RamenParsing.stream_of_string program in
  (* TODO: enable error correction *)
  match p ["program"] None Parsers.no_error_correction stream |>
        RamenParsing.to_result with
  | Bad e ->
    let error =
      IO.to_string (RamenParsing.print_bad_result print) e in
    raise (SyntaxError (ParseError { error ; text = program }))
  | Ok ((params, funcs), _) ->
    let funcs = reify_subqueries funcs in
    let t = params, funcs in
    check t ; t
