open Batteries
open RamenHelpers
open RamenLog
open RamenConsts
module C = RamenConf
module RC = C.Running
module F = C.Func
module P = C.Program
module O = RamenOperation
module N = RamenName
module Files = RamenFiles
module Processes = RamenProcesses
module ZMQClient = RamenSyncZMQClient

(*
 * Stopping a worker from running.
 *)

let check_orphans killed_prog_names programs =
  !logger.debug "Checking orphans of %a..."
    (pretty_list_print N.program_print) killed_prog_names ;
  (* We want to warn if a child is stalled. *)
  Hashtbl.iter (fun prog_name rce ->
    if rce.RC.status = MustRun &&
       not (List.mem prog_name killed_prog_names)
    then (
      match P.of_bin prog_name rce.RC.params rce.RC.bin with
      | exception _ ->
        (* Missing or erroneous programs can't make orphan check fail. *)
        ()
      | prog ->
        List.iter (fun func ->
          (* If this func has parents, all of which are now missing, then
           * complain: *)
          if func.F.parents <> [] &&
             List.for_all (function
               (* FIXME: `kill` should probably accept --on-site? *)
               | _, None, _ -> false (* does not depend upon a killed program *)
               | _, Some par_rel_prog, _ ->
                  let par_prog =
                    N.program_of_rel_program func.F.program_name par_rel_prog in
                  List.mem par_prog killed_prog_names
             ) func.F.parents
          then
            !logger.warning "Operation %s, will be left without parents"
              (F.fq_name func :> string)
        ) prog.P.funcs)
  ) programs

(* Takes a list of globs and returns the number of kills. *)
let kill_locked ?(purge=false) program_names programs =
  let killed_prog_names =
    Hashtbl.enum programs //
    (fun ((n : N.program), rce) ->
      (rce.RC.status <> RC.Killed || purge) &&
      List.exists (fun p ->
        Globs.matches p (n :> string)
      ) program_names) /@
    fst |>
    List.of_enum in
  let running_killed_prog_names =
    List.filter (fun (n : N.program) ->
      let rce = Hashtbl.find programs n in
      rce.RC.status <> RC.Killed
    ) killed_prog_names in
  check_orphans running_killed_prog_names programs ;
  if purge then
    Hashtbl.filteri_inplace (fun name _mre ->
      not (List.mem name killed_prog_names)
    ) programs
  else
    List.iter (fun n ->
      let rce = Hashtbl.find programs n in
      rce.RC.status <- RC.Killed
    ) running_killed_prog_names ;
  List.length killed_prog_names

let kill conf ?purge program_names =
  RC.with_wlock conf (kill_locked ?purge program_names)

(*
 * Starting a new worker from a binary.
 *
 * First, check that the binary file can be added to the configuration
 * ("linking") and then add it.
 *)

let check_links program_name prog running_programs =
  !logger.debug "checking links" ;
  List.iter (fun func ->
    (* Check linkage:
     * We want to warn if a parent is missing. The synchronizer will
     * start the worker but it will be blocked. *)
    let already_warned1 = ref Set.empty
    and already_warned2 = ref Set.empty in
    List.iter (function
      (* FIXME: if a specific host is selected, check that the program is
       * running on that host *)
      | _, None, _ -> ()
      | _, Some par_rel_prog, par_func ->
        let par_prog = N.program_of_rel_program func.F.program_name
                                                        par_rel_prog in
        (match Hashtbl.find running_programs par_prog with
        | exception Not_found ->
          if not (Set.mem par_prog !already_warned1) then (
            !logger.warning "Operation %s depends on program %s, \
                             which is not running."
              (func.F.name :> string)
              (par_prog :> string) ;
            already_warned1 := Set.add par_prog !already_warned1)
        | rce ->
            (match P.of_bin par_prog rce.RC.params rce.RC.bin with
            | exception _ -> (* of_bin already logged the error *) ()
            | pprog ->
                (match List.find (fun p ->
                         p.F.name = par_func) pprog.P.funcs with
                | exception Not_found ->
                  if not (Set.mem (par_prog, par_func) !already_warned2)
                  then (
                    !logger.error
                      "Operation %s depends on operation %s/%s, which is \
                       not part of the running program %s."
                      (func.F.name :> string)
                      (par_prog :> string)
                      (par_func :> string)
                      (par_prog :> string) ;
                    already_warned2 :=
                      Set.add (par_prog, par_func) !already_warned2)
                | par ->
                  (* We want to err if a parent is incompatible. *)
                  let par_out_type =
                    O.out_type_of_operation ~with_private:false
                                            par.F.operation in
                  RamenProcesses.check_is_subtype func.F.in_type
                                                  par_out_type)))
    ) func.parents
  ) prog.P.funcs ;
  (* We want to err if a child is incompatible. *)
  Hashtbl.iter (fun prog_name rce ->
    match P.of_bin prog_name rce.RC.params rce.RC.bin with
    | exception _ -> (* of_bin already logged the error *) ()
    | prog' ->
        List.iter (fun func ->
          (* Check that a children that depends on us gets the proper
           * type (regardless of where it runs): *)
          List.iter (fun (_, rel_par_prog_opt, par_func as parent) ->
            let par_prog = F.program_of_parent_prog func.F.program_name
                                                    rel_par_prog_opt in
            if par_prog = program_name then
              match List.find (fun f ->
                      f.F.name = par_func) prog.P.funcs with
              | exception Not_found ->
                !logger.warning
                  "Operation %s, currently stalled, will still be missing \
                   its parent %a"
                  (F.fq_name func :> string)
                  F.print_parent parent
              | f -> (* so func is depending on f, let's see: *)
                let out_type =
                  O.out_type_of_operation ~with_private:false
                                          f.F.operation in
                RamenProcesses.check_is_subtype func.F.in_type out_type
          ) func.F.parents
        ) prog'.P.funcs
  ) running_programs

(* Check that all params are actually used by some functions: *)
let check_params prog params =
  let used =
    List.fold_left (fun s func ->
      (* Get the result as a set: *)
      let used = O.vars_of_operation TupleParam func.F.operation in
      Set.union used s
    ) Set.empty prog.P.funcs in
  let unused = Set.diff params used in
  if not (Set.is_empty unused) then
    let single = set_is_singleton unused in
    Printf.sprintf2 "Parameter%s %a %s unused"
      (if single then "" else "s")
      (pretty_set_print N.field_print) unused
      (if single then "is" else "are") |>
    failwith

let no_params = Hashtbl.create 0

let default_program_name bin_file =
  let f = Files.(remove_ext (basename bin_file)) in
  N.program (f :> string)

let make_entry replace (program_name : N.program) report_period
               (bin : N.path) (src_file : N.path) on_site debug params programs =
  if not replace then
    (match Hashtbl.find programs program_name with
    | exception Not_found -> ()
    | rce ->
      if rce.RC.status = RC.MustRun then
        Printf.sprintf "A program named %s is already running"
          (program_name :> string) |>
        failwith) ;
  RC.{
    bin ; params ; status = MustRun ; debug ; report_period ;
    src_file ; on_site ; automatic = false }

(*
let run_sync source
      program_name replace kill_if_disabled purge report_period on_site
      debug params =
  let while_ () = !Processes.quit = None in
  let login = conf.C.login in
  let topics = [] in
  ZMQClient.start ~while_ ~on_new:on_set ~on_set conf.C.sync_url login topics
    (fun zock ->
      ZMQClient.sent_msg zock
        RamenSync.Key.(PerProgram program_name, DoCompile)
        RamenSync.Value.(CompileOptions { 
        ~on_ok:(fun () -> Process.quit := ExitCodes.ok)
        ~on_ko:(fun () -> Process.quit := ExitCodes.error))
*)
(* The binary must have been produced already as it's going to be read for
 * linkage checks: *)
let run_local conf bin_file src_file
      program_name replace kill_if_disabled purge report_period on_site
      debug params =
  let bin = Files.absolute_path_of bin_file in
  let can_run = P.wants_to_run conf bin params in
  if not can_run then (
    !logger.info "Program %a is disabled"
      N.program_print program_name ;
    if kill_if_disabled then
      log_and_ignore_exceptions ~what:"Killing disabled program"
        (fun () ->
          let program_name = Globs.(escape (program_name :> string)) in
          let nb_killed = kill conf ?purge [ program_name ] in
          if nb_killed > 0 then !logger.info "...and has been killed") ()
  ) else (
    if N.is_empty bin_file then
      failwith "Without a --confserver a executable file must be provided." ;
    RC.with_wlock conf (fun programs ->
      (* Check linkage. *)
      let prog = P.of_bin program_name params bin_file in
      check_params prog (Hashtbl.keys params |> Set.of_enum) ;
      check_links program_name prog programs ;
      make_entry replace program_name report_period bin_file src_file on_site
                 debug params programs |>
      Hashtbl.replace programs program_name)
  )

let run conf ?(replace=false) ?(kill_if_disabled=false) ?purge
        ?(report_period=Default.report_period) ?(src_file=N.path "")
        ?(on_site=Globs.all) ?(debug=false) ?(params=no_params)
        ?(bin_file=N.path "") program_name_opt =
  if program_name_opt = None && N.is_empty src_file && N.is_empty bin_file then
    failwith "You must provide either the program name, a source file or \
              an executable file." ;
  let program_name =
    Option.default_delayed (fun () ->
      if not (N.is_empty bin_file) then default_program_name bin_file
      else default_program_name src_file
    ) program_name_opt in
  let k =
    if conf.C.sync_url = "" then (
      if N.is_empty bin_file then
        failwith "executable file is mandatory unless --confserver." ;
      run_local conf bin_file src_file
    ) else (
      if N.is_empty src_file then
        failwith "with --confserver a source file must be provided." ;
      if not (N.is_empty bin_file) then
        failwith "with --confserver the executable file is useless and must not be \
                  given." ;
      (*let source = Files.read_whole_file src_file in
      run_sync source*)
      todo "run_sync"
    ) in
  k program_name replace kill_if_disabled purge report_period on_site debug params
