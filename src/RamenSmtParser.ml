(*
 * Parser to the SMT2 commands output by RamenSmt, which are:
 *  1. check-sat
 *  2. get-unsat-core
 *  3. get-model
 *)
open Batteries

(*$inject
  open Batteries
  open TestHelpers
*)

type response =
    Solved of Smt2Types.Response.model list * bool (* sure? *)
  | Unsolved of Smt2Types.symbol list

let smt2_of_string parser ?(debug=false) s =
  ignore (Parsing.set_trace debug) ;
  let lexbuf = Lexing.from_string s in
  parser Smt2Lexer.token lexbuf

(*$= smt2_of_string & ~printer:dump
  (Smt2Types.Response.CheckSat Sat) \
    (smt2_of_string Smt2Parser.check_sat_response "sat")

  (Smt2Types.Response.Error "unsat core is not available") \
    (smt2_of_string Smt2Parser.check_sat_response \
      "(error \"unsat core is not available\"\n\
      )")

  (Smt2Types.(Response.GetModel [ \
    DefineFun { recurs = false ; def = Command.{ \
      dec = { name = "e14" ; inputs = [] ; \
              output = Sort.of_string "Type" } ; \
      body = QualIdentifier (Identifier "string", None) } } ; \
    DefineFun { recurs = false ; def = Command.{ \
      dec = { name = "e18" ; inputs = [] ; \
              output = Sort.of_string "Type" } ; \
      body = QualIdentifier (Identifier "float", None) } } ; \
  ])) \
    (smt2_of_string Smt2Parser.get_model_response \
      "(model \n\
         (define-fun e14 () Type\n\
           string)\n\
         (define-fun e18 () Type\n\
           float)\n\
       )")

  (Smt2Types.(Response.GetModel [ \
    DefineFun { recurs = false ; def = Command.{ \
      dec = { name = "n6" ; inputs = [] ; \
              output = Sort.of_string "Bool" } ; \
      body = QualIdentifier (Identifier "false", None) } } ; \
    DefineFun { recurs = false ; def = Command.{ \
      dec = { name = "t6" ; inputs = [] ; \
              output = Sort.of_string "Type" } ; \
      body = QualIdentifier (Identifier "bool", None) } } ; \
  ])) \
    (smt2_of_string Smt2Parser.get_model_response \
      "(model \n\
         (define-fun n6 () Bool\n\
           false)\n\
         (define-fun t6 () Type\n\
           bool)\n\
       )")
*)

let response_of_string ?(debug=false) s =
  ignore (Parsing.set_trace debug) ;
  let lexbuf = Lexing.from_string s in
  let check_sat_resp = Smt2Parser.check_sat_response Smt2Lexer.token lexbuf in
  let unsat_core = Smt2Parser.get_unsat_core_response Smt2Lexer.token lexbuf in
  let model = Smt2Parser.get_model_response Smt2Lexer.token lexbuf in
  let open Smt2Types.Response in
  match [ check_sat_resp ; unsat_core ; model ] with
  | [ CheckSat Sat ; Error _ ; GetModel model ] ->
      Solved (model, true)
  | [ CheckSat Unknown ; _ ; GetModel model ] ->
      Solved (model, false)
  | [ CheckSat Unsat ; GetUnsatCore syms ; Error _ ] ->
      Unsolved syms
  | [ CheckSat Unsat ; Error _ ; Error _ ] ->
      (* Wanted to optimize but it failed so we hauve neither the model nor
       * the unsat core: *)
      Unsolved []
  | responses ->
      Format.(fprintf str_formatter "Cannot parse output:@\n%a"
        Smt2Types.Script.print_responses responses) ;
      Format.flush_str_formatter () |> failwith
