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
    params : RamenTuple.params ;
    operation : RamenOperation.t }
type t = func list

let make_name =
  let seq = ref ~-1 in
  fun () ->
    incr seq ;
    "f"^ string_of_int !seq

let make_func ?name ?(params=[]) operation =
  { name = (match name with Some n -> n | None -> make_name ()) ;
    params ; operation }

let print_func oc n =
  (* TODO: keep the info that func was anonymous? *)
  Printf.fprintf oc "DEFINE '%s' %aAS %a"
    n.name
    (List.print ~first:"" ~last:" " ~sep:" " RamenTuple.print_param) n.params
    RamenOperation.print n.operation

let print oc p =
  List.print ~sep:"\n" print_func oc p

let check lst =
  List.fold_left (fun s n ->
    RamenOperation.check n.params n.operation ;
    if Set.mem n.name s then
      raise (SyntaxError (FuncNameNotUnique n.name)) ;
    Set.add n.name s
  ) Set.empty lst |> ignore

module Parser =
struct
  (*$< Parser *)
  open RamenParsing

  let anonymous_func m =
    let m = "anonymous func" :: m in
    (RamenOperation.Parser.p >>: make_func) m

  let param m =
    let m = "function parameter" :: m in
    (non_keyword +- opt_blanks +- char '=' +- opt_blanks ++
     RamenTypes.Parser.p) m

  let named_func m =
    let m = "function" :: m in
    (strinG "define" -- blanks -+ func_identifier ~program_allowed:false ++
     repeat ~sep:none ~what:"function argument" (blanks -+ param) +-
     blanks +- strinG "as" +- blanks ++
     RamenOperation.Parser.p >>: fun ((name, params), op) ->
       make_func ~name ~params op) m

  let func m =
    let m = "func" :: m in
    (anonymous_func ||| named_func) m

  let p m =
    let m = "program" :: m in
    let sep = opt_blanks -- char ';' -- opt_blanks in
    (several ~sep func +- optional ~def:() (opt_blanks -- char ';')) m

  (*$= p & ~printer:(test_printer print)
   (Ok ([\
    { name = "bar" ; params = [] ;\
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
          force_export = false ; event_time = None ;\
          from = [NamedOperation "foo"] ; every = 0. ; factors = [] } } ],\
      (46, [])))\
      (test_p p "DEFINE bar AS SELECT 42 AS the_answer FROM foo" |>\
       replace_typ_in_program)

   (Ok ([\
    { name = "add" ; params = ["p1", VI32 0l; "p2", VI32 0l] ;\
      operation = \
        Aggregate {\
          fields = [\
            { expr = RamenExpr.(\
                StatelessFun2 (typ, Add,\
                  Field (typ, ref TupleParam, "p1"),\
                  Field (typ, ref TupleParam, "p2"))) ;\
              alias = "res" } ] ;\
          every = 0. ; force_export = false ; event_time = None ;\
          and_all_others = false ; merge = [], 0. ; sort = None ;\
          where = RamenExpr.Const (typ, VBool true) ;\
          notifications = [] ; key = [] ;\
          commit_when = RamenExpr.Const (typ, VBool true) ;\
          commit_before = false ; flush_how = Reset ; from = [] ;\
          factors = [] } } ],\
      (44, [])))\
      (test_p p "DEFINE add p1=0 p2=0 AS YIELD p1 + p2 AS res" |>\
       (function Ok (ps, _) as x -> check ps ; x | x -> x) |>\
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
 * contains only NamedOperations: *)
let expurgate from =
  let open RamenOperation in
  List.fold_left (fun (new_funcs, from) -> function
    | SubQuery q ->
        let new_func = reify_subquery q in
        (new_func :: new_funcs), NamedOperation new_func.name :: from
    | NamedOperation _ as f -> new_funcs, f :: from
  ) ([], []) from

let reify_subqueries funcs =
  let open RamenOperation in
  List.fold_left (fun fs func ->
    match func.operation with
    | Aggregate ({ from ; _ } as f) ->
        let funcs, from = expurgate from in
        { func with operation = Aggregate { f with from } } ::
          funcs @ fs
    | Instrumentation ({ from ; _ } as f) ->
        let funcs, from = expurgate from in
        { func with operation = Instrumentation { f with from } } ::
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
  | Ok (funcs, _) ->
    let funcs = reify_subqueries funcs in
    check funcs ;
    funcs
