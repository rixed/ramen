(* Check that a given binary file can be added to the configuration
 * ("linking") and then add it. *)
open Batteries
open RamenHelpers
open RamenLog
module C = RamenConf
module F = C.Func
module P = C.Program

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
        let par_prog =
          RamenName.program_of_rel_program func.F.program_name par_rel_prog in
        (match Hashtbl.find running_programs par_prog with
        | exception Not_found ->
          if not (Set.mem par_prog !already_warned1) then (
            !logger.warning "Operation %s depends on program %s, \
                             which is not running."
              (RamenName.string_of_func func.F.name)
              (RamenName.string_of_program par_prog) ;
            already_warned1 := Set.add par_prog !already_warned1)
        | mre ->
          let pprog = P.of_bin mre.C.params mre.C.bin in
          (match List.find (fun p -> p.F.name = par_func) pprog.P.funcs with
          | exception Not_found ->
            if not (Set.mem (par_prog, par_func) !already_warned2) then (
              !logger.error "Operation %s depends on operation %s/%s, \
                             which is not part of the running program %s."
                (RamenName.string_of_func func.F.name)
                (RamenName.string_of_program par_prog)
                (RamenName.string_of_func par_func)
                (RamenName.string_of_program par_prog) ;
              already_warned2 := Set.add (par_prog, par_func) !already_warned2)
          | par ->
            (* We want to err if a parent is incompatible (unless --force). *)
            try RamenProcesses.check_is_subtype func.F.in_type.RamenTuple.ser
                                                par.F.out_type.ser
            with Failure m when force -> (* or let it fail *)
              !logger.error "%s" m))
    ) func.parents
  ) prog.P.funcs ;
  (* We want to err if a child is incompatible (unless --force).
   * In case we force the insertion, the bad workers will *not* be
   * run by the process supervisor anyway, unless the incompatible
   * relatives are stopped/restarted, in which case these new workers
   * could be run at the expense of the old ones. *)
  Hashtbl.iter (fun prog_name mre ->
    let prog' = P.of_bin mre.C.params mre.C.bin in
    List.iter (fun func ->
      (* Check that a children that depends on us gets the proper type: *)
      List.iter (fun (rel_par_prog_opt, par_func as parent) ->
        let par_prog =
          F.program_of_parent_prog func.F.program_name rel_par_prog_opt in
        if par_prog = program_name then
          match List.find (fun f -> f.F.name = par_func) prog.P.funcs with
          | exception Not_found ->
            !logger.warning "Operation %s, currently stalled, will still \
                             be missing its parent %a"
              (RamenName.string_of_fq (F.fq_name func)) F.print_parent parent
          | f -> (* so func is depending on f, let's see: *)
            try RamenProcesses.check_is_subtype func.F.in_type.RamenTuple.ser
                                                f.F.out_type.ser
            with Failure m when force -> (* or let it fail *)
              !logger.error "%s" m
      ) func.F.parents
    ) prog'.P.funcs
  ) running_programs

let run conf params replace ?as_ bin_file =
  C.with_wlock conf (fun programs ->
    let bin = absolute_path_of bin_file in
    let prog = P.of_bin ?as_ params bin in
    let program_name = (List.hd prog.P.funcs).F.program_name in
    check_links program_name prog programs ;
    if not replace && Hashtbl.mem programs program_name then
      Printf.sprintf "A program named %s is already running"
        (RamenName.string_of_program program_name) |>
      failwith ;
    (* TODO: Make sure this key is authoritative on a program name: *)
    Hashtbl.replace programs program_name C.{ bin ; params } ;
    Lwt.return_unit)
