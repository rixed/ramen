(* This module deals with parsing programs.
 * A program is a set of named operations.
 * It makes use of RamenOperation, which parses operation.
 *)
open Batteries
open RamenLang

(*$inject
  open TestHelpers
  open RamenLang
*)

type func =
  { name : string ;
    operation : RamenOperation.t }

type t = func list

let make_name =
  let seq = ref ~-1 in
  fun () ->
    incr seq ;
    "f"^ string_of_int !seq

let make_func ?name operation =
  { name = (match name with Some n -> n | None -> make_name ()) ;
    operation }

let print_param oc (n, v) =
  Printf.fprintf oc "PARAMETER %s DEFAULTS TO %a;"
    n RamenTypes.print v

let print_func oc n =
  (* TODO: keep the info that func was anonymous? *)
  Printf.fprintf oc "DEFINE %S AS %a;"
    n.name
    RamenOperation.print n.operation

let print oc (params, funcs) =
  List.print ~sep:"\n" print_param oc params ;
  List.print ~sep:"\n" print_func oc funcs

let check params funcs =
  List.fold_left (fun s (n, v) ->
    if Set.mem n s then
      raise (SyntaxError (NameNotUnique n)) ;
    Set.add n s
  ) Set.empty params |> ignore ;
  List.fold_left (fun s n ->
    RamenOperation.check params n.operation ;
    if Set.mem n.name s then
      raise (SyntaxError (NameNotUnique n.name)) ;
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
          non_keyword +- blanks +- strinGs "default" +- blanks +-
          strinG "to" +- blanks ++ RamenTypes.Parser.p)
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
    DefFunc of func | DefParams of (string * RamenTypes.value) list

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
    { name = "bar" ;\
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
          from = [NamedOperation (None, "foo")] ; every = 0. ; factors = [] } } ]),\
      (46, [])))\
      (test_p p "DEFINE bar AS SELECT 42 AS the_answer FROM foo" |>\
       replace_typ_in_program)

   (Ok ((["p1", VI32 0l; "p2", VI32 0l], [\
    { name = "add" ;\
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
       (function Ok ((ps, fs), _) as x -> check ps fs ; x | x -> x) |>\
       replace_typ_in_program)
  *)

  (*$>*)
end

let reify_subquery =
  let seqnum = ref 0 in
  fun op ->
    let name = "_"^ string_of_int !seqnum in
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
    check params funcs ;
    params, funcs
