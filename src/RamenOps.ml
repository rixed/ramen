open Batteries
open Lwt
open Helpers
open RamenLog
open RamenSharedTypes
module C = RamenConf
module L = RamenConf.Program
module N = RamenConf.Func
module SL = RamenSharedTypes.Info.Program
module SN = RamenSharedTypes.Info.Func

(* High level operations (uses LWT) *)

let del_program programs program =
   if program.L.status = Running then
    fail_with "Cannot delete a running program"
  else
    wrap (fun () -> C.del_program programs program)

let start_program_by_name conf to_run =
  C.with_wlock conf (fun programs ->
    match Hashtbl.find programs to_run with
    | exception Not_found ->
        (* Cannot be an error since we are not passed programs. *)
        !logger.warning "Cannot run unknown program %s" to_run ;
        return_unit
    | p ->
        try%lwt RamenProcesses.run conf programs p
        with RamenProcesses.AlreadyRunning -> return_unit)

(* Compile one program and stop those that depended on it. *)
let compile conf program_name =
  (* Compilation taking a long time, we do a two phase approach:
   * First we take the wlock, read the programs and put their status
   * to compiling, and release the wlock. Then we compile everything
   * at our own peace. Then we take the wlock again, check that the
   * status is still compiling, and store the result of the
   * compilation. *)
  !logger.debug "Compiling phase 1: copy the program info" ;
  match%lwt
    C.with_wlock conf (fun programs ->
      match Hashtbl.find programs program_name with
      | exception Not_found ->
          !logger.warning "Cannot compile unknown node %s" program_name ;
          (* Cannot be a hard error since caller had no lock *)
          return_none
      | to_compile ->
          (match to_compile.L.status with
          | Edition _ ->
              (match
                Hashtbl.map (fun _func_name func ->
                  List.map (fun (par_prog, par_name) ->
                    match C.find_func programs par_prog par_name with
                    | exception Not_found ->
                      let e = Lang.UnknownFunc (par_prog ^"/"^ par_name) in
                      raise (Compiler.SyntaxErrorInFunc (func.N.name, e))
                    | _, par_func -> par_func
                  ) func.N.parents
                ) to_compile.L.funcs with
              | exception exn ->
                  L.set_status to_compile (Edition (Printexc.to_string exn)) ;
                  return_none
              | parents ->
                  L.set_status to_compile Compiling ;
                  return (Some (to_compile, parents)))
          | _ -> return_none)) with
  | None -> (* Nothing to compile *) return_unit
  | Some (to_compile, parents) ->
    (* Helper to retrieve to_compile: *)
    let with_compiled_program programs def f =
      match Hashtbl.find programs to_compile.L.name with
      | exception Not_found ->
          !logger.error "Compiled program %s disappeared"
            to_compile.L.name ;
          def
      | program ->
          if program.L.status <> Compiling then (
            !logger.error "Status of %s have been changed to %s \
                           during compilation (at %10.0f)!"
                program.L.name
                (SL.string_of_status program.L.status)
                program.L.last_status_change ;
            (* Doing nothing is probably the safest bet *)
            def
          ) else f program in
    (* From now on this program is our. Let's make sure we return it
     * if we mess up. *)
    !logger.debug "Compiling phase 2: compiling %s" to_compile.L.name ;
    match%lwt Compiler.compile conf parents to_compile with
    | exception exn ->
      !logger.error "Compilation of %s failed with %s"
        to_compile.L.name (Printexc.to_string exn) ;
      !logger.debug "Compiling phase 3: Returning the erroneous program" ;
      let%lwt () = C.with_wlock conf (fun programs ->
        with_compiled_program programs return_unit (fun program ->
          L.set_status program (Edition (Printexc.to_string exn)) ;
          return_unit)) in
      fail exn
    | () ->
      !logger.debug "Compiling phase 3: Returning the program" ;
      let%lwt to_restart =
        C.with_wlock conf (fun programs ->
          with_compiled_program programs return_nil (fun program ->
            L.set_status program Compiled ;
            program.funcs <- to_compile.funcs ;
            Compiler.stop_all_dependents conf programs program)) in
      (* Be lenient if some of those programs are not there anymore: *)
      Lwt_list.iter_s (start_program_by_name conf) to_restart

let set_program ?test_id conf ~ok_if_running ~start ~name ~program =
  (* Disallow anonymous programs for simplicity: *)
  if name = "" then
    fail_with "Programs must have non-empty names" else (
  let%lwt funcs = wrap (fun () -> C.parse_program program) in
  let name, funcs =
    match test_id with
    | None -> name, funcs
    | Some id ->
      let new_program_name = "temp/tests/"^ id in
      new_program_name,
      List.map (RamenTests.reprogram name new_program_name) funcs in
  let%lwt must_restart =
    C.with_wlock conf (fun programs ->
      (* Delete the program if it already exists. No worries the conf won't be
       * changed if there is any error. *)
      let%lwt stopped_it =
        match Hashtbl.find programs name with
        | exception Not_found -> return_false
        | program ->
          if program.L.status = Running then (
            if ok_if_running then (
              let%lwt () = RamenProcesses.stop conf programs program in
              let%lwt () = del_program programs program in
              return_true
            ) else (
              fail_with ("Program "^ name ^" is running")
            )
          ) else (
            let%lwt () = del_program programs program in
            return_false
          ) in
      (* Create the program *)
      let%lwt _program =
        wrap (fun () -> C.make_program ?test_id programs name program funcs) in
      return stopped_it) in
  if must_restart || start then (
    !logger.debug "Trying to (re)start program %s" name ;
    let%lwt () = compile conf name in
    start_program_by_name conf name
  ) else return_unit)
