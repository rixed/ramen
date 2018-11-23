(* Interface with the SMT external solver *)
open Batteries
open RamenHelpers
open RamenLog

let solver = ref "z3 -t:20000 -smt2 %s"

let scramble = Base64.str_encode
let unscramble = Base64.str_decode

let emit_comment oc str =
  Printf.fprintf oc "; %s\n" str

let emit_is_true id oc =
  Printf.fprintf oc "%s" id

let emit_is_false id oc =
  Printf.fprintf oc "(not %s)" id

(* a => b *)
let emit_imply a oc b =
  Printf.fprintf oc "(or (not %s) %s)" a b

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
    (string_of_bool (not optimize))

let post_scriptum oc =
  Printf.fprintf oc
    "; Now you know it all.\n\
     ; So what's the answer to life, the universe and everything?\n\
     (check-sat) ; also experiment with: (check-sat-using smt)\n\
     (get-unsat-core)\n\
     (get-model)\n"

let run_solver smt2_file =
  let cmd =
    if String.exists !solver "%s" then
      String.nreplace ~sub:"%s" ~by:(shell_quote smt2_file) ~str:!solver
    else
      !solver ^" "^ shell_quote smt2_file in
  !logger.debug "Running the solver as %S" cmd ;
  (* Lazy way to split the arguments: *)
  let shell = "/bin/sh" in
  let args = [| shell ; "-c" ; cmd |] in
  (* Now summon the solver: *)
  with_subprocess shell args (fun (_oc, ic, ec) ->
    let output = read_whole_channel ic
    and errors = read_whole_channel ec in
    !logger.debug "Solver is done." ;
    let p =
      RamenParsing.allow_surrounding_blanks RamenSmtParser.response in
    let stream = RamenParsing.stream_of_string output in
    match p ["SMT output"] None Parsers.no_error_correction stream |>
          RamenParsing.to_result with
    | Ok (sol, _) -> sol, output
    | Bad e ->
        !logger.error "Cannot parse solver output:\n%s" output ;
        if errors <> "" then failwith errors else
        IO.to_string (RamenParsing.print_bad_result
                        RamenSmtParser.print_response) e |>
        failwith)

let run_smt2 ~fname ~emit ~parse_result ~unsat =
  mkdir_all ~is_file:true fname ;
  !logger.debug "Writing SMT2 program into %S" fname ;
  File.with_file_out ~mode:[`create; `text; `trunc] fname (fun oc ->
    emit oc ~optimize:true) ;
  match run_solver fname with
  | RamenSmtParser.Solved (model, sure), output ->
      if not sure then
        !logger.warning "Solver best idea after timeout:\n%s" output ;
      (* Output a hash of structure*nullability per expression id: *)
      List.iter (fun ((sym, vars, sort, term), _recurs) ->
        parse_result sym vars sort term
      ) model
  | RamenSmtParser.Unsolved [], _ ->
      !logger.debug "No unsat-core, resubmitting." ;
      (* Resubmit the same problem without optimizations to get the unsat
       * core: *)
      let fname' = fname ^".no_opt" in
      File.with_file_out ~mode:[`create; `text; `trunc] fname' (fun oc ->
        emit oc ~optimize:false) ;
      (match run_solver fname' with
      | RamenSmtParser.Unsolved syms, output -> unsat syms output
      | RamenSmtParser.Solved _, _ ->
          failwith "Unsat with optimization but sat without?!")
  | RamenSmtParser.Unsolved syms, output -> unsat syms output
