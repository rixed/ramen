(* Interface with the SMT external solver *)
open Batteries
open RamenHelpersNoLog
open RamenLog
module Files = RamenFiles
module N = RamenName

let solver = ref (RamenCompilConfig.z3_path ^" -t:90000 -smt2 %s")

let scramble = Base64.str_encode
let unscramble = Base64.str_decode

let emit_comment oc fmt =
  Printf.fprintf oc ("; "^^fmt^^"\n")

let emit_is_true id oc =
  Printf.fprintf oc "%s" id

let emit_is_false id oc =
  Printf.fprintf oc "(not %s)" id

let emit_is_bool b =
  if b then emit_is_true else emit_is_false

(* a => b *)
let emit_imply a oc b =
  Printf.fprintf oc "(=> %s %s)" a b

(* (not a) => b *)
let emit_imply_not a oc b =
  Printf.fprintf oc "(or %s %s)" a b

let preamble oc optimize =
  Printf.fprintf oc
    "(set-option :print-success false)\n\
     (set-option :produce-models %s)\n\
     (set-option :produce-unsat-cores %s)\n\
     (set-option :smt.core.minimize %s)\n\
     (set-logic ALL) ; TODO\n"
    (string_of_bool optimize)
    (string_of_bool (not optimize))
    (string_of_bool optimize)

let post_scriptum oc =
  Printf.fprintf oc
    "; So what's the answer to life, the universe and everything?\n\
     (check-sat) ; also experiment with: (check-sat-using smt)\n\
     (get-unsat-core)\n\
     (get-model)\n"

let run_solver ?debug (smt2_file : N.path) =
  let cmd =
    if String.exists !solver "%s" then
      String.nreplace ~sub:"%s"
                      ~by:(shell_quote (smt2_file :> string))
                      ~str:!solver
    else
      !solver ^" "^ shell_quote (smt2_file :> string) in
  !logger.debug "Running the solver as %S" cmd ;
  (* Lazy way to split the arguments: *)
  let shell = N.path "/bin/sh" in
  let args = [| (shell :> string) ; "-c" ; cmd |] in
  (* Now summon the solver: *)
  Files.with_subprocess shell args (fun (_ic, oc, ec) ->
    (* FIXME: should read both files at the same time to prevent the children
     * process to block *)
    let output = Files.read_whole_channel oc
    and errors = Files.read_whole_channel ec in
    let lexbuf = Lexing.from_string output in
    try
      let sol = RamenSmtParser.response_of_lexbuf ?debug lexbuf in
      !logger.debug "Solver is done." ;
      sol
    with Parsing.Parse_error as e ->
      let pos = lexbuf.Lexing.lex_curr_p in
      let within_output p =
        if p <= 0 then 0 else
        if p >= String.length output then String.length output else
        p in
      let sta = within_output (pos.pos_cnum - 10)
      and sto = within_output (pos.pos_cnum + 10) in
      !logger.error "Parse Error in SMT2 at line %d col %d (%S)"
        pos.Lexing.pos_lnum
        (pos.pos_cnum - pos.pos_bol + 1)
        (String.sub output sta (sto - sta)) ;
      raise e
    | e ->
      !logger.error "Cannot parse solver output: %s\n%s"
        (Printexc.to_string e)
        (abbrev 2000 output) ;
      if errors <> "" then failwith errors else
      raise e)

let run_smt2 ~fname ~emit ~parse_result ~unsat =
  Files.mkdir_all ~is_file:true fname ;
  !logger.debug "Writing SMT2 program into %a"
    N.path_print_quoted fname ;
  File.with_file_out ~mode:[`create; `text; `trunc] (fname :> string)
    (fun oc -> emit oc ~optimize:true) ;
  match run_solver fname with
  | RamenSmtParser.Solved (model, sure) ->
      if not sure then !logger.warning "Solver timed out" ;
      (* Turn the models into a list of
       * (function name * parameters * return sort * body): *)
      let open Smt2Types in
      List.iter (function
        | Response.DefineFun f ->
            parse_result f.def.Command.dec.name
                         f.def.dec.inputs
                         f.def.dec.output
                         f.def.body
        | Response.DefineFuns _ ->
            todo "Exploit DefineFuns in SMT responses"
      ) model
  | RamenSmtParser.Unsolved [] ->
      !logger.debug "No unsat-core, resubmitting." ;
      (* Resubmit the same problem without optimizations to get the unsat
       * core: *)
      let fname' = N.cat fname (N.path ".no_opt") in
      File.with_file_out ~mode:[`create; `text; `trunc] (fname' :> string)
        (fun oc -> emit oc ~optimize:false) ;
      (match run_solver fname' with
      | RamenSmtParser.Unsolved syms -> unsat syms
      | RamenSmtParser.Solved _ ->
          failwith "Unsat with optimization but sat without?!")
  | RamenSmtParser.Unsolved syms -> unsat syms

let list_print p =
  List.print ~first:" " ~last:"" ~sep:" " p
