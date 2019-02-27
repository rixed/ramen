(* The entry point of every worker programs.
 * Executes either of the compiled in functions, or display some information. *)
open Batteries
open RamenHelpers
open RamenConsts

type convert_format = CSV | RB | ORC
let string_of_format = function
  | CSV -> "csv"
  | RB -> "ringbuf"
  | ORC -> "orc"

let format_of_filename s =
  match Filename.extension s with
  | ".orc" -> ORC
  | ".csv" -> CSV
  | ".r" -> RB
  | ".rb" -> RB
  | e -> failwith ("unknown format for extension "^ e)

(* The [per_funcname] association list received by the [run] function use
 * this type of entries: *)
type per_func_info =
  { worker_entry_point : unit -> unit ;
    replay_entry_point : unit -> unit ;
    convert_archive :
      convert_format -> string -> convert_format -> string -> unit }

let run codegen_version rc_marsh run_condition per_funcname =
  (* Init the random number generator *)
  (match Sys.getenv "rand_seed" with
  | exception Not_found -> Random.self_init ()
  | "" -> Random.self_init ()
  | s -> Random.init (int_of_string s)) ;
  let help () =
    Printf.printf
      "This program is a Ramen worker (codegen %s).\n\
       To learn more about this program, run `ramen info %s`.\n\
       To dump the content of an archive file, use \
         `%s %s <function_name> <file>`.\n\
       To convert an archive file into another format, use \
         `%s %s <function_name> <file_in> <file_out>`.\n"
      codegen_version Sys.argv.(0)
      Sys.argv.(0) WorkerCommands.dump_archive
      Sys.argv.(0) WorkerCommands.convert_archive in
  let assoc_or_fail name lst =
    try List.assoc name lst
    with Not_found ->
      Printf.eprintf
        "Unknown operation %S.\n\
         Trying to run a Ramen program? Try `ramen run %s`\n"
        name Sys.executable_name ;
        exit 1 in
  let run_from_list k =
    (* Call a function from lst according to envvar "name" *)
    match Sys.getenv "name" with
    | exception Not_found ->
        Printf.eprintf "Missing name envvar\n"
    | name ->
        k (assoc_or_fail name per_funcname) in
  let convert ?out_format ~in_ ~out func_name =
    let in_format = format_of_filename in_
    and out_format = out_format |? format_of_filename out
    in
    Printf.printf "convert from %s to %s\n"
      (string_of_format in_format)
      (string_of_format out_format)
  in
  (* If we are called "ramen worker:" then we must run: *)
  if Sys.argv.(0) = worker_argv0 then
    run_from_list (fun e -> e.worker_entry_point ())
  else if Sys.argv.(0) = replay_argv0 then
    run_from_list (fun e -> e.replay_entry_point ())
  else match Sys.argv.(1) with
  | exception Invalid_argument _ -> help ()
  | s when s = WorkerCommands.get_info ->
      print_string rc_marsh
  | s when s = WorkerCommands.wants_to_run ->
      Printf.printf "%b\n" (run_condition ())
  | s when s = WorkerCommands.print_version ->
      (* Allow to override the reported version; useful for tests and also
       * maybe as a last-resort production hack: *)
      let v = getenv ~def:codegen_version "pretend_codegen_version" in
      Printf.printf "%s\n" v
  | s when s = WorkerCommands.dump_archive ->
      (match Sys.argv.(2), Sys.argv.(3) with
      | exception Invalid_argument _ -> help ()
      | fname, in_ -> convert ~in_ ~out_format:CSV ~out:"/dev/stdout" fname)
  | s when s = WorkerCommands.convert_archive ->
      (match Sys.argv.(2), Sys.argv.(3), Sys.argv.(4)  with
      | exception Invalid_argument _ -> help ()
      | fname, in_, out -> convert ~in_ ~out fname)
  | _ -> help ()
