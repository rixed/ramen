open Batteries
open Lwt
open RamenLog
open Helpers
module C = RamenConf
module N = RamenConf.Func
module L = RamenConf.Program
module SN = RamenSharedTypes.Info.Func
module SL = RamenSharedTypes.Info.Program
open RamenSharedTypesJS

let quit = ref false

let fd_of_int : int -> Unix.file_descr = Obj.magic

let close_fd i =
  Unix.close (fd_of_int i)

let run_background cmd args env =
  let open Unix in
  (* prog name should be first arg *)
  let prog_name = Filename.basename cmd in
  let args = Array.init (Array.length args + 1) (fun i ->
      if i = 0 then prog_name else args.(i-1))
  in
  !logger.info "Running %s with args %a and env %a"
    cmd
    (Array.print String.print) args
    (Array.print String.print) env ;
  flush_all () ;
  match fork () with
  | 0 ->
    close_fd 0 ;
    for i = 3 to 255 do
      try close_fd i with Unix.Unix_error (Unix.EBADF, _, _) -> ()
    done ;
    execve cmd args env
  | pid -> pid

let wrap_in_valgrind command args =
  "/usr/bin/valgrind", Array.concat [ [| "--tool=massif" ; command |] ; args ]

exception NotYetCompiled
exception AlreadyRunning
exception StillCompiling

(* Return the name of func input ringbuf for the given parent (if func is
 * merging, each parent uses a distinct one) *)
let input_spec conf parent func =
  (* In case of merge, ringbufs are numbered as the node parents: *)
  (if RamenOperation.is_merging func.N.operation then
    match List.findi (fun i (pprog, pname) ->
             pprog = parent.N.program && pname = parent.N.name
           ) func.N.parents with
    | exception Not_found ->
        !logger.error "Operation %S is not a child of %S"
          func.N.name parent.N.name ;
        invalid_arg "input_spec"
    | i, _ ->
        C.in_ringbuf_name_merging conf func i
  else C.in_ringbuf_name_single conf func),
  let out_type = C.tuple_ser_type parent.N.out_type
  and in_type = C.tuple_ser_type func.N.in_type in
  RingBufLib.skip_list ~out_type ~in_type

(* Takes a locked conf.
 * FIXME: a phantom type for this *)
let rec run_func conf programs program func =
  if func.N.pid <> None then fail AlreadyRunning else
  let command = C.Program.exec_of_program conf.C.persist_dir func.program
  and output_ringbufs =
    (* Start to output to funcs of this program. They have all been
     * created above (in [run]), and we want to allow loops in a program. Avoids
     * outputting to other programs, unless they are already running (in
     * which case their ring-buffer exists already), otherwise we would
     * hang. *)
    C.fold_funcs programs Map.empty (fun outs l n ->
      (* Select all func's children that are either running or in the same
       * program *)
      if (n.N.program = program.L.name || l.L.status = Running) &&
         List.exists (fun (pl, pn) ->
           pl = program.L.name && pn = func.N.name
         ) n.N.parents
      then (
        !logger.debug "%s will output to %s" (N.fq_name func) (N.fq_name n) ;
        let k, v = input_spec conf func n in
        Map.add k v outs
      ) else outs) in
  let out_ringbuf_ref = C.out_ringbuf_names_ref conf func in
  let%lwt () = RamenOutRef.set out_ringbuf_ref output_ringbufs in
  (* Now that the out_ref exists, but before we actually fork the worker,
   * we can start importing: *)
  let%lwt () =
    if RamenOperation.is_exporting func.N.operation then
      let%lwt _ = RamenExport.get_or_start conf func in
      return_unit
    else return_unit in
  !logger.info "Start %s" func.N.name ;
  let input_ringbufs = C.in_ringbuf_names conf func in
  let notify_ringbuf =
    (* Where that worker must write its notifications. Normally toward a
     * ringbuffer that's read by Ramen, unless it's a test programs. Tests
     * must not send their notifications to Ramen with real ones, but instead
     * to a ringbuffer specific to the test_id. *)
    C.notify_ringbuf ~test_id:program.test_id conf in
  let ocamlrunparam =
    getenv ~def:(if conf.C.debug then "b" else "") "OCAMLRUNPARAM" in
  let env = [|
    "OCAMLRUNPARAM="^ ocamlrunparam ;
    "debug="^ string_of_bool conf.C.debug ;
    "name="^ N.fq_name func ;
    "signature="^ func.signature ;
    "input_ringbufs="^ String.concat "," input_ringbufs ;
    "output_ringbufs_ref="^ out_ringbuf_ref ;
    "report_ringbuf="^ C.report_ringbuf conf ;
    "notify_ringbuf="^ notify_ringbuf ;
    (* We need to change this dir whenever the func signature change
     * to prevent it to reload an incompatible state: *)
    "persist_dir="^ conf.C.persist_dir ^"/workers/states/"
                  ^ RamenVersions.worker_state
                  ^"/"^ Config.version
                  ^"/"^ (N.fq_name func)
                  ^"/"^ func.N.signature ;
    "ramen_url="^ conf.C.ramen_url ;
    (match !logger.logdir with
      | Some _ ->
        "log_dir="^ conf.C.persist_dir ^"/log/workers/" ^ (N.fq_name func)
      | None -> "no_log_dir=") |] in
  let command, args =
    (* For convenience let's add the fun name as argument: *)
    command, [| N.fq_name func |] in
  let%lwt pid =
    wrap (fun () -> run_background command args env) in
  !logger.debug "Function %s now runs under pid %d" (N.fq_name func) pid ;
  func.N.pid <- Some pid ;
  (* Monitor this worker, wait for its termination, restart...: *)
  async (fun () ->
    let rec wait_child () =
      match%lwt Lwt_unix.waitpid [] pid with
      | exception Unix.Unix_error (Unix.EINTR, _, _) -> wait_child ()
      | exception exn ->
        (* This should not be used *)
        (* TODO: save this error on the func record *)
        !logger.error "Cannot wait for pid %d: %s"
          pid (Printexc.to_string exn) ;
        return_unit
      | _, status ->
        let status_str = string_of_process_status status in
        !logger.info "Operation %s (pid %d) %s."
          (N.fq_name func) pid status_str ;
        (* First and foremost we want to set the error status and clean
         * the PID of this process (if only because another thread might
         * wait for the result of its own kill) *)
        let%lwt succ_failures =
          C.with_wlock conf (fun programs ->
            (* Look again for that program by name: *)
            match C.find_func programs func.N.program func.N.name with
            | exception Not_found ->
                !logger.error "Operation %s (pid %d) %s is not \
                               in the configuration any more!"
                  (N.fq_name func) pid status_str ;
                return 0
            | program, func ->
                let%lwt succ_failures =
                  (* Check this is still the same program: *)
                  if func.pid <> Some pid then (
                    !logger.error "Operation %s (pid %d) %s is in \
                                   the configuration under pid %a!"
                      (N.fq_name func) pid status_str
                      (Option.print Int.print) func.pid ;
                    return 0
                  ) else (
                    func.pid <- None ;
                    func.last_exit <- status_str ;
                    (* The rest of the operation depends on the program status: *)
                    match program.status with
                    | Stopping ->
                        (* If there are no more processes running for this program,
                         * demote it to Compiled: *)
                        if Hashtbl.values program.L.funcs |>
                           Enum.for_all (fun func -> func.N.pid = None) then (
                          !logger.info "All workers for %s have terminated" program.name ;
                          L.set_status program Compiled
                        ) ;
                        return 0
                    | Running when not !quit ->
                        (* If this does not look intentional, restart the worker: *)
                        (match status with
                        | Unix.WSIGNALED signal
                          when signal <> Sys.sigterm &&
                               signal <> Sys.sigkill &&
                               signal <> Sys.sigint ->
                            return (func.succ_failures + 1)
                        | Unix.WEXITED code when code <> 0 ->
                            return (func.succ_failures + 1)
                        | _ ->
                            !logger.debug "Assuming death by natural causes" ;
                            return 0)
                    | _ ->
                        (* We leave the program.status as it is since we have no
                         * idea what's happening to other operations. *)
                        !logger.debug "Don't know what to do since program status is %s"
                          (SL.string_of_status program.status) ;
                        return 0) in
                func.succ_failures <- succ_failures ;
                return succ_failures) in
        if succ_failures = 0 then return_unit
        else (* restart that worker *)
          let add_jitter ratio v =
            (Random.float ratio -. (ratio *. 0.5) +. 1.) *. v in
          let delay = float_of_int (succ_failures * 2 |> min 5) |>
                      add_jitter 0.1 in
          !logger.info "Restarting func %s which is supposed to \
                        be running (failed %d times in a row, pause for %gs)."
            (N.fq_name func) succ_failures delay ;
          let%lwt () = Lwt_unix.sleep delay in
          C.with_wlock conf (fun programs ->
            (* Since we released the lock while waiting, check again that the
             * situation is still the same: *)
            match C.find_func programs func.N.program func.N.name with
            | exception Not_found ->
                return_unit
            | program, func ->
                if func.pid = None && not !quit then
                  (* Note: run_func will start another waiter for that
                   * other worker so our job is done. *)
                  run_func conf programs program func
                else return_unit)
    in
    wait_child ()) ;
  (* Update the parents out_ringbuf_ref if it's in another program (otherwise
   * we have set the correct out_ringbuf_ref just above already) *)
  Lwt_list.iter_p (fun (parent_program, parent_name) ->
      if parent_program = program.name then
        return_unit
      else
        match C.find_func programs parent_program parent_name with
        | exception Not_found ->
          !logger.warning "Starting func %s which parent %s/%s does not \
                           exist yet"
            (N.fq_name func)
            parent_program parent_name ;
          return_unit
        | _, parent ->
          let out_ref =
            C.out_ringbuf_names_ref conf parent in
          (* The parent ringbuf might not exist yet if it has never been
           * started. If the parent is not running then it will overwrite
           * it when it starts, with whatever running children it will
           * have at that time (including us, if we are still running).  *)
          RamenOutRef.add out_ref (input_spec conf parent func)
    ) func.N.parents

(* We take _programs as a sign that we have the lock *)
let kill_worker conf timeout _programs func pid =
  let try_kill pid signal =
    try Unix.kill pid signal
    with Unix.Unix_error _ as e ->
      !logger.error "Cannot kill pid %d: %s" pid (Printexc.to_string e)
  in
  (* First ask politely: *)
  !logger.info "Killing worker %s (pid %d)" (N.fq_name func) pid ;
  try_kill pid Sys.sigterm ;
  (* Now the worker is supposed to tidy up everything and terminate.
   * Then we have a thread that is waiting for him, perform a quick
   * autopsy and clear the pid ; as soon as he get a chance because
   * we are currently holding the conf.
   * We want to check in a few seconds that this had happened: *)
  async (fun () ->
    let%lwt () = Lwt_unix.sleep (timeout +. Random.float 1.) in
    C.with_rlock conf (fun programs ->
      !logger.debug "Checking that pid %d is not around any longer." pid ;
      (* Find that program again, and if it's still having the same pid
       * then shoot him down: *)
      (match C.find_func programs func.N.program func.N.name with
      | exception Not_found ->
        !logger.warning "Worker %s (pid %d) is not in configuration anymore, \
                         sending it sigkill just to be sure."
          (N.fq_name func) pid ;
        try_kill pid Sys.sigkill
      | program, func ->
        (* Here it is assumed that the program was not launched again
         * within 2 seconds with the same pid. In a world where this
         * assumption wouldn't hold we would have to increment a counter
         * in the func for instance... *)
        (match func.N.pid with
        | None ->
          !logger.debug "The waiter cleaned the pid, all is good."
        | Some p when p = pid ->
          !logger.warning "Killing worker %s (pid %d) with bigger guns"
            (N.fq_name func) pid ;
          try_kill pid Sys.sigkill ;
        | Some p ->
          !logger.debug "Another child is running already (pid %d)" p ;
          (* Meaning that it has first been cleared by the waiter, and then
           * restarted. All is good. *))) ;
      return_unit))

let stop conf programs program =
  match program.L.status with
  | Edition _ | Compiled | Stopping -> return_unit
  | Compiling ->
    (* FIXME: do as for Running and make sure run() check the status hasn't
     * changed before launching workers. *)
    return_unit
  | Running ->
    let now = Unix.gettimeofday () in
    let program_funcs =
      Hashtbl.values program.L.funcs |> List.of_enum in
    let timeout = 1. +. float_of_int (List.length program_funcs) *. 0.02 in
    !logger.info "Stopping program %s (timeout %gs)" program.L.name timeout ;
    let%lwt () = Lwt_list.iter_p (fun func ->
        if program.test_id <> "" &&
           not (RamenOperation.run_in_tests func.N.operation)
        then (
          if func.N.pid <> None then
            !logger.error "Node %s should not be running during a test!"
              (N.fq_name func) ;
          return_unit
        ) else (
          let%lwt () = RamenExport.stop conf func in
          (* Start by removing this worker ringbuf from all its parent
           * output references: *)
          let this_ins = C.in_ringbuf_names conf func in
          (* Remove this operation from all its parents output: *)
          let%lwt () = Lwt_list.iter_p (fun (parent_program, parent_name) ->
              match C.find_func programs parent_program parent_name with
              | exception Not_found -> return_unit
              | _, parent ->
                let out_ref = C.out_ringbuf_names_ref conf parent in
                Lwt_list.iter_s (fun this_in ->
                  RamenOutRef.remove out_ref this_in) this_ins
            ) func.N.parents in
          (* Now kill that pid: *)
          (match func.N.pid with
          | None ->
            !logger.warning "Function %s stopped already (INTerrupted?)"
              func.N.name
          | Some pid ->
            !logger.debug "Stopping func %s, pid %d" func.N.name pid ;
            (* Get rid of the worker *)
            kill_worker conf timeout programs func pid) ;
          return_unit
        )
      ) program_funcs in
    L.set_status program Stopping ;
    program.L.last_stopped <- Some now ;
    return_unit

let run conf programs program =
  let open L in
  match program.status with
  | Edition _ -> fail NotYetCompiled
  | Running | Stopping -> fail AlreadyRunning
  | Compiling -> fail StillCompiling
  | Compiled ->
    !logger.info "Starting program %s" program.L.name ;
    (* First prepare all the required ringbuffers *)
    !logger.debug "Creating ringbuffers..." ;
    let program_funcs =
      Hashtbl.values program.funcs |> List.of_enum in
    (* Be sure to cancel everything (threads/execs) we started in case of
     * failure: *)
    try%lwt
      (* We must create all the ringbuffers before starting any worker
       * because there is no good order in which to start them: *)
      let%lwt () = Lwt_list.iter_p (fun func ->
        wrap (fun () ->
          C.in_ringbuf_names conf func |>
          List.iter (fun rb_name ->
            RingBuf.create rb_name RingBufLib.rb_default_words)
        )) program_funcs in
      (* Now run everything in any order: *)
      !logger.debug "Launching generated programs..." ;
      let now = Unix.gettimeofday () in
      let%lwt () =
        Lwt_list.iter_p (fun func ->
          if program.test_id <> "" &&
             not (RamenOperation.run_in_tests func.N.operation)
          then (
            !logger.info "Skipping %s in tests" (N.fq_name func) ;
            return_unit
          ) else
            run_func conf programs program func
        ) program_funcs in
      L.set_status program Running ;
      program.L.last_started <- Some now ;
      return_unit
    with exn ->
      let%lwt () = stop conf programs program in
      fail exn

(*
 * Thread that stops programs at termination (!quit)
 *)

(* Return either one or all programs *)
let graph_programs programs = function
  | None ->
    Hashtbl.values programs |>
    List.of_enum |>
    return
  | Some l ->
    try Hashtbl.find programs l |>
        List.singleton |>
        return
    with Not_found -> fail_with ("Unknown program "^l)

let stop_programs conf programs program_opt =
  let%lwt to_stop = graph_programs programs program_opt in
  Lwt_list.iter_p (stop conf programs) to_stop

let rec monitor_quit conf =
  let%lwt () = Lwt_unix.sleep 0.3 in
  if !quit then (
    !logger.debug "Waiting for the wlock for quitting..." ;
    let%lwt () =
      C.with_wlock conf (fun programs ->
        !logger.info "Stopping HTTP server(s)..." ;
        List.iter (fun condvar ->
          Lwt_condition.signal condvar ()
        ) !RamenHttpHelpers.http_server_done ;
        !logger.info "Stopping all workers..." ;
        let%lwt () = stop_programs conf programs None in
        return_unit) in
    !logger.info "Waiting for all workers to terminate..." ;
    let last_nb_running = ref 0 in
    let rec loop () =
      let%lwt still_running =
        C.with_rlock conf (fun programs ->
          let nb_running =
            C.fold_funcs programs 0 (fun c program func ->
              if func.C.Func.pid = None then c else c + 1) in
          if nb_running > 0 then (
            if nb_running != !last_nb_running then (
              !logger.info "%d workers are still working..." nb_running ;
              last_nb_running := nb_running
            ) ;
            return_true
          ) else return_false) in
      if still_running then (
        Lwt_unix.sleep 0.1 >>= loop
      ) else return_unit in
    let%lwt () = loop () in
    !logger.info "Quitting monitor_quit..." ;
    return_unit
  ) else monitor_quit conf


(* Timeout unused programs.
 * By unused, we mean either: no program depends on it, or no one cares for
 * what it exports. *)

let use_program now program =
  program.L.last_used <- now

let use_program_by_name programs now program_name =
  Hashtbl.find programs program_name |>
  use_program now

let timeout_programs conf programs =
  (* Build the set of all defined and all used programs *)
  let defined, used =
    Hashtbl.fold (fun program_name program (defined, used) ->
      Set.add program_name defined,
      Hashtbl.fold (fun _func_name func used ->
          List.fold_left (fun used (parent_program, _parent_func) ->
              if parent_program = program_name then used
              else Set.add parent_program used
            ) used func.N.parents
        ) program.L.funcs used
    ) programs (Set.empty, Set.empty) in
  let now = Unix.gettimeofday () in
  Set.iter (use_program_by_name programs now) used ;
  let unused = Set.diff defined used |>
               Set.to_list in
  Lwt_list.iter_p (fun program_name ->
      let program = Hashtbl.find programs program_name in
      if program.L.timeout > 0. &&
         now > program.L.last_used +. program.L.timeout
      then (
        !logger.info "Deleting unused program %s after a %gs timeout"
          program_name program.L.timeout ;
        (* Kill first, and only then forget about it. *)
        let%lwt () = stop conf programs program in
        Hashtbl.remove programs program_name ;
        return_unit
      ) else return_unit
    ) unused

(* Instrumentation: Reading workers stats *)

open Stdint

let reports_lock = RWLock.make ()
let last_reports = Hashtbl.create 31

let read_reports rb =
  RingBuf.read_ringbuf rb (fun tx ->
    let worker, time, ic, sc, oc, gc, cpu, ram, wi, wo, bi, bo =
      RamenBinocle.unserialize tx in
    RingBuf.dequeue_commit tx ;
    RWLock.with_w_lock reports_lock (fun () ->
      Hashtbl.replace last_reports worker SN.{
        time ;
        in_tuple_count = Option.map Uint64.to_int ic ;
        selected_tuple_count = Option.map Uint64.to_int sc ;
        out_tuple_count = Option.map Uint64.to_int oc ;
        group_count = Option.map Uint64.to_int gc ;
        cpu_time = cpu ; ram_usage = Uint64.to_int ram ;
        in_sleep = wi ; out_sleep = wo ;
        in_bytes = Option.map Uint64.to_float bi ;
        out_bytes = Option.map Uint64.to_float bo } ;
      return_unit))

let last_report fq_name =
  RWLock.with_r_lock reports_lock (fun () ->
    Hashtbl.find_option last_reports fq_name |?
    { time = 0. ;
      in_tuple_count = None ; selected_tuple_count = None ;
      out_tuple_count = None ; group_count = None ;
      cpu_time = 0. ; ram_usage = 0 ;
      in_sleep = None ; out_sleep = None ;
      in_bytes = None ; out_bytes = None } |>
    return)

(* Notifications:
 * To alleviate workers from the hassle to send HTTP notifications, those are
 * sent to Ramen via a ringbuffer. Advantages are many:
 * Workers do not need an HTTP client and are therefore smaller, faster to
 * link, and easier to port to another language.
 * Also, they might as well be easier to link with the libraries bundle. *)

let process_notifications rb =
  RamenSerialization.read_notifs rb (fun (worker, url) ->
    !logger.info "Received notify instruction from %s to %s"
      worker url ;
    RamenHttpHelpers.http_notify url)

(* A thread that hunt for unused programs / imports *)
let rec timeout_programs_loop conf =
  let%lwt () = C.with_wlock conf (fun programs ->
    timeout_programs conf programs) in
  (* No need for a lock on conf for that one: *)
  let%lwt () = RamenExport.timeout_exports conf in
  let%lwt () = Lwt_unix.sleep 7.1 in
  timeout_programs_loop conf

let cleanup_old_files conf =
  (* Have a list of directories and regexps and current version,
   * Iter through this list for file matching the regexp and that are also directories.
   * If this direntry matches the current version, touch it.
   * If not, and if it hasn't been touched for X days, assume that's an old one and delete it.
   * Then sleep for one day and restart. *)
  let get_log_file () =
    Unix.gettimeofday () |> Unix.localtime |> RamenLog.log_file
  and lwt_touch_file fname =
    let now = Unix.gettimeofday () in
    Lwt_unix.utimes fname now now
  and delete_directory fname = (* TODO: should really delete *)
    Lwt_unix.rename fname (fname ^".todel")
  in
  let open Str in
  let cleanup_dir (dir, sub_re, current_version) =
    let dir = conf.C.persist_dir ^"/"^ dir in
    !logger.debug "Cleaning directory %s" dir ;
    (* Error in there will be delivered to the stream reader: *)
    let files = Lwt_unix.files_of_directory dir in
    try%lwt
      Lwt_stream.iter_s (fun fname ->
        let full_path = dir ^"/"^ fname in
        if fname = current_version then (
          !logger.debug "Touching %s." full_path ;
          lwt_touch_file full_path
        ) else if string_match sub_re fname 0 &&
           is_directory full_path &&
           file_is_older_than (1. *. 86400.) fname (* TODO: should be 10 days *)
        then (
          !logger.info "Deleting old version %s." fname ;
          delete_directory full_path
        ) else return_unit
      ) files
    with Unix.Unix_error (Unix.ENOENT, _, _) ->
        return_unit
       | exn ->
        !logger.error "Cannot list %s: %s" dir (Printexc.to_string exn) ;
        return_unit
  in
  let date_regexp = regexp "^[0-9]+-[0-9]+-[0-9]+$"
  and v_regexp = regexp "v[0-9]+"
  and v1v2_regexp = regexp "v[0-9]+_v[0-9]+" in
  let rec loop () =
    let to_clean =
      [ "log", date_regexp, get_log_file () ;
        "log/workers", v_regexp, get_log_file () ;
        "alerting", v_regexp, RamenVersions.alerting_state ;
        "configuration", v_regexp, RamenVersions.graph_config ;
        "instrumentation_ringbuf", v1v2_regexp, (RamenVersions.instrumentation_tuple ^"_"^ RamenVersions.ringbuf) ;
        "workers/bin", v_regexp, RamenVersions.codegen ;
        "workers/history", v_regexp, RamenVersions.history ;
        "workers/ringbufs", v_regexp, RamenVersions.ringbuf ;
        "workers/out_ref", v_regexp, RamenVersions.out_ref ;
        "workers/src", v_regexp, RamenVersions.codegen ;
        "workers/states", v_regexp, RamenVersions.worker_state ]
    in
    !logger.info "Cleaning old unused files..." ;
    let%lwt () = Lwt_list.iter_s cleanup_dir to_clean in
    let bindir = conf.C.persist_dir ^"/workers/bin/"^ RamenVersions.codegen in
    let%lwt used_bins =
      C.with_rlock conf (fun programs ->
        C.lwt_fold_funcs programs Set.empty (fun set prog func ->
          Set.add (C.obj_of_func conf func) set |>
          Set.add (L.exec_of_program conf.C.persist_dir prog.L.name) |>
          return)) in
    let now = Unix.gettimeofday () in
    let unlink_old_bin fname _rel_fname =
      let bin = Filename.basename fname in
      if String.starts_with bin "ramen_" then
        if Set.mem fname used_bins then
          Unix.utimes fname now now
        else if now -. mtime_of_file fname > float_of_int conf.max_execs_age then (
          !logger.info "Deleting old binary %s" bin ;
          log_exceptions Unix.unlink fname) in
    dir_subtree_iter ~on_file:unlink_old_bin bindir ;
    (* TODO: also unlink old files in src *)
    Lwt_unix.sleep 3600. >>= loop
  in
  loop ()
