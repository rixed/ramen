(* The entry point of every worker programs.
 * Executes either of the compiled in functions, or display some information. *)
open Batteries
open RamenHelpers

let run codegen_version rc_str rc_marsh run_condition lst =
  (* Init the random number generator *)
  (match Sys.getenv "rand_seed" with
  | exception Not_found -> Random.self_init ()
  | "" -> Random.self_init ()
  | s -> Random.init (int_of_string s)) ;
  let help () =
    Printf.printf
      "This program is a Ramen worker (codegen %s).\n\n\
       Runtime configuration:\n\n%s\n\n\
       Have a nice day!\n"
      codegen_version rc_str in
  (* If we are called "ramen worker:" then we must run: *)
  if Sys.argv.(0) = RamenConsts.worker_argv0 then
    (* Call a function from lst according to envvar "name" *)
    match Sys.getenv "name" with
    | exception Not_found ->
        Printf.eprintf "Missing name envvar\n"
    | name ->
        (match List.assoc name lst with
        | exception Not_found ->
          Printf.eprintf
            "Unknown operation %S.\n\
             Trying to run a Ramen program? Try `ramen run %s`\n"
            name Sys.executable_name ;
            exit 1
        | f -> f ())
  else match Sys.argv.(1) with
  | exception Invalid_argument _ -> help ()
  | s when s = RamenConsts.WorkerCommands.get_info ->
      print_string rc_marsh
  | s when s = RamenConsts.WorkerCommands.wants_to_run ->
      Legacy.(output_value stdout (run_condition ()))
  | s when s = RamenConsts.WorkerCommands.print_version ->
      (* Allow to override the reported version; useful for tests and also
       * maybe as a last-resort production hack: *)
      let v = getenv ~def:codegen_version "pretend_codegen_version" in
      Printf.printf "%s\n" v
  | _ -> help ()
