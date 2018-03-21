open Batteries
open Lang
open RamenSharedTypes

(*$inject
  open TestHelpers
  open Lang
*)

type func =
  { name : string ;
    params : (string * scalar_value) list ;
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

let print_param oc (n, v) =
  Printf.fprintf oc "%s=%a" n RamenScalar.print v

let print_func oc n =
  (* TODO: keep the info that func was anonymous? *)
  Printf.fprintf oc "DEFINE '%s' %aAS %a"
    n.name
    (List.print ~first:"" ~last:" " ~sep:" " print_param) n.params
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
     RamenScalar.Parser.p) m

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
          notify_url = "" ;\
          key = [] ; top = None ;\
          commit_when = RamenExpr.Const (typ, VBool true) ;\
          commit_before = false ;\
          flush_how = Reset ;\
          force_export = false ; event_time = None ;\
          from = ["foo"] } } ],\
      (46, [])))\
      (test_p p "DEFINE bar AS SELECT 42 AS the_answer FROM foo" |>\
       replace_typ_in_program)

   (Ok ([\
    { name = "add" ; params = ["p1", VI32 0l; "p2", VI32 0l] ;\
      operation = \
        Yield {\
          fields = [\
            { expr = RamenExpr.(\
                StatelessFun2 (typ, Add,\
                  Field (typ, ref TupleParam, "p1"),\
                  Field (typ, ref TupleParam, "p2"))) ;\
              alias = "res" } ] ;\
          every = 0. ; force_export = false ; event_time = None } } ],\
      (44, [])))\
      (test_p p "DEFINE add p1=0 p2=0 AS YIELD p1 + p2 AS res" |>\
       (function Ok (ps, _) as x -> check ps ; x | x -> x) |>\
       replace_typ_in_program)
  *)

  (*$>*)
end
