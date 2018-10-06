open Batteries
open RamenHelpers
open RamenLog
module C = RamenConf
module F = C.Func
module P = C.Program

(*
 * Starting a new worker from a binary.
 *
 * First, check that the binary file can be added to the configuration
 * ("linking") and then add it.
 *)

(* TODO: remove that useless force option *)
let check_links ?(force=false) program_name prog running_programs =
  !logger.debug "checking links" ;
  List.iter (fun func ->
    (* Check linkage:
     * We want to warn if a parent is missing. The synchronizer will
     * start the worker but it will be blocked. *)
    let already_warned1 = ref Set.empty
    and already_warned2 = ref Set.empty in
    List.iter (function
      | None, _ -> ()
      | Some par_rel_prog, par_func ->
        let par_prog = RamenName.program_of_rel_program func.F.program_name
                                                        par_rel_prog in
        (match Hashtbl.find running_programs par_prog with
        | exception Not_found ->
          if not (Set.mem par_prog !already_warned1) then (
            !logger.warning "Operation %s depends on program %s, \
                             which is not running."
              (RamenName.string_of_func func.F.name)
              (RamenName.string_of_program par_prog) ;
            already_warned1 := Set.add par_prog !already_warned1)
        | mre ->
            (match P.of_bin par_prog mre.C.params mre.C.bin with
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
                      (RamenName.string_of_func func.F.name)
                      (RamenName.string_of_program par_prog)
                      (RamenName.string_of_func par_func)
                      (RamenName.string_of_program par_prog) ;
                    already_warned2 :=
                      Set.add (par_prog, par_func) !already_warned2)
                | par ->
                  (* We want to err if a parent is incompatible (unless
                   * --force). *)
                  try RamenProcesses.check_is_subtype func.F.in_type
                                                      par.F.out_type
                  with Failure m when force -> (* or let it fail *)
                    !logger.error "%s" m)))
    ) func.parents
  ) prog.P.funcs ;
  (* We want to err if a child is incompatible (unless --force).
   * In case we force the insertion, the bad workers will *not* be
   * run by the process supervisor anyway, unless the incompatible
   * relatives are stopped/restarted, in which case these new workers
   * could be run at the expense of the old ones. *)
  Hashtbl.iter (fun prog_name mre ->
    match P.of_bin prog_name mre.C.params mre.C.bin with
    | exception _ -> (* of_bin already logged the error *) ()
    | prog' ->
        List.iter (fun func ->
          (* Check that a children that depends on us gets the proper
           * type: *)
          List.iter (fun (rel_par_prog_opt, par_func as parent) ->
            let par_prog = F.program_of_parent_prog func.F.program_name
                                                    rel_par_prog_opt in
            if par_prog = program_name then
              match List.find (fun f ->
                      f.F.name = par_func) prog.P.funcs with
              | exception Not_found ->
                !logger.warning
                  "Operation %s, currently stalled, will still be missing \
                   its parent %a"
                  (RamenName.string_of_fq (F.fq_name func))
                  F.print_parent parent
              | f -> (* so func is depending on f, let's see: *)
                try RamenProcesses.check_is_subtype func.F.in_type
                                                    f.F.out_type
                with Failure m when force -> (* or let it fail *)
                  !logger.error "%s" m
          ) func.F.parents
        ) prog'.P.funcs
  ) running_programs

let run conf params replace report_period program_name ?src_file bin_file debug =
  C.with_wlock conf (fun programs ->
    let bin = absolute_path_of bin_file in
    let prog = P.of_bin program_name params bin in
    check_links program_name prog programs ;
    if not replace then
      (match Hashtbl.find programs program_name with
      | exception Not_found -> ()
      | mre ->
        if not mre.C.killed then
          Printf.sprintf "A program named %s is already running"
            (RamenName.string_of_program program_name) |>
          failwith) ;
    (* TODO: Make sure this key is authoritative on a program name: *)
    Hashtbl.replace programs program_name
      C.{ bin ; params ; killed = false ; debug ; report_period ; src_file })

(*
 * Stopping a worker from running.
 *)

let check_orphans killed_prog_names programs =
  (* We want to warn if a child is stalled. *)
  Hashtbl.iter (fun prog_name mre ->
    if not mre.C.killed &&
       not (List.mem prog_name killed_prog_names)
    then (
      let prog = P.of_bin prog_name mre.C.params mre.C.bin in
      List.iter (fun func ->
        (* If this func has parents, all of which are now missing, then
         * complain: *)
        if func.F.parents <> [] &&
           List.for_all (function
             | None, _ -> false (* does not depend in a killed program *)
             | Some par_rel_prog, _ ->
                let par_prog =
                  RamenName.(program_of_rel_program func.F.program_name par_rel_prog) in
                List.mem par_prog killed_prog_names
           ) func.F.parents
        then
          !logger.warning "Operation %s, will be left without parents"
            (RamenName.string_of_fq (F.fq_name func))
      ) prog.P.funcs)
  ) programs

(* Takes a list of globs and returns the number of kills. *)
let kill conf ?(purge=false) program_names =
  C.with_wlock conf (fun programs ->
    let killed_prog_names =
      Hashtbl.enum programs //
      (fun (n, mre) ->
        not mre.C.killed &&
        List.exists (fun p ->
          Globs.matches p (RamenName.string_of_program n)
        ) program_names) /@
      fst |>
      List.of_enum in
    check_orphans killed_prog_names programs ;
    if purge then
      Hashtbl.filteri_inplace (fun name _mre ->
        not (List.mem name killed_prog_names)
      ) programs
    else
      List.iter (fun n ->
        let mre = Hashtbl.find programs n in
        mre.C.killed <- true
      ) killed_prog_names ;
    List.length killed_prog_names)
