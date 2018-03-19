open Batteries
open Lang

(*$inject
  open TestHelpers
  open Lang
*)

type func = { name : string ; operation : RamenOperation.t }
type t = func list

let make_name =
  let seq = ref ~-1 in
  fun () ->
    incr seq ;
    "f"^ string_of_int !seq

let make_func ?name operation =
  { name = (match name with Some n -> n | None -> make_name ()) ;
    operation }

let print_func oc n =
  (* TODO: keep the info that func was anonymous? *)
  Printf.fprintf oc "DEFINE '%s' AS %a"
    n.name
    RamenOperation.print n.operation

let print oc p =
  List.print ~sep:"\n" print_func oc p

let check lst =
  List.fold_left (fun s n ->
    RamenOperation.check n.operation ;
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

  let named_func m =
    let m = "func" :: m in
    (strinG "define" -- blanks -+ func_identifier ~program_allowed:false +-
     blanks +- strinG "as" +- blanks ++
     RamenOperation.Parser.p >>: fun (name, op) -> make_func ~name op) m

  let func m =
    let m = "func" :: m in
    (anonymous_func ||| named_func) m

  let p m =
    let m = "program" :: m in
    let sep = opt_blanks -- char ';' -- opt_blanks in
    (several ~sep func +- optional ~def:() (opt_blanks -- char ';')) m

  (*$= p & ~printer:(test_printer print)
   (Ok ([\
    { name = "bar" ;\
      operation = \
        Aggregate {\
          fields = [\
            { expr = RamenExpr.Const (typ, VI32 42l) ;\
              alias = "the_answer" } ] ;\
          and_all_others = false ;\
          merge = [] ;\
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
  *)

  (*$>*)
end
