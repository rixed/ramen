(* Process supervisor which task is to start and stop workers, make sure they
 * keep working, collect their stats and write them somewhere for anybody
 * to see, send their notifications, delete old unused files...
 *)
open Batteries
open RamenLog
open RamenHelpers
module C = RamenConf
module F = C.Func
module P = C.Program

(* Global quit flag, set (to some RamenConsts.ExitCodes) when the term signal
 * is received or some other bad condition happen: *)
let quit = ref None

let until_quit f =
  let rec loop () =
    if !quit = None && f () then loop () in
  loop ()

let dummy_nop () =
  !logger.warning "Running in dummy mode" ;
  until_quit (fun () -> Unix.sleep 3 ; true)


(* Seed to pass to workers to init their random generator: *)
let rand_seed = ref None

(*
 * Helpers:
 *)

(* To be called before synchronize_running *)
let repair_and_warn what rb =
  if RingBuf.repair rb then
    !logger.warning "Ringbuf for %s was damaged." what

(* Prepare ringbuffer for notifications *)
let prepare_notifs conf =
  let rb_name = C.notify_ringbuf conf in
  RingBuf.create ~wrap:false rb_name ;
  let notify_rb = RingBuf.load rb_name in
  repair_and_warn "notifications" notify_rb ;
  notify_rb

(* Prepare ringbuffer for reports. *)
let prepare_reports conf =
  let rb_name = C.report_ringbuf conf in
  RingBuf.create ~wrap:false rb_name ;
  let report_rb = RingBuf.load rb_name in
  repair_and_warn "instrumentation" report_rb ;
  report_rb

(* Many messages that are exceptional in supervisor are quite expected in tests: *)
let info_or_test conf =
  if conf.C.test then !logger.debug else !logger.info

(* Install signal handlers *)
let prepare_signal_handlers conf =
  set_signals Sys.[sigterm; sigint] (Signal_handle (fun s ->
    info_or_test conf "Received signal %s" (name_of_signal s) ;
    quit :=
      Some (if s = Sys.sigterm then RamenConsts.ExitCodes.terminated
                               else RamenConsts.ExitCodes.interrupted))) ;
  (* Dump stats on sigusr1: *)
  set_signals Sys.[sigusr1] (Signal_handle (fun s ->
    (* This log also useful to rotate the logfile. *)
    !logger.info "Received signal %s" (name_of_signal s) ;
    Binocle.display_console ()))

(*
 * Machinery to spawn other programs.
 *)

let fd_of_int : int -> Unix.file_descr = Obj.magic

let close_fd i =
  Unix.close (fd_of_int i)

let run_background ?cwd ?(and_stop=false) cmd args env =
  let open Unix in
  let quoted oc s = Printf.fprintf oc "%S" s in
  !logger.debug "Running %s as: /usr/bin/env %a %S %a"
    cmd
    (Array.print ~first:"" ~last:"" ~sep:" " quoted) env
    cmd
    (Array.print ~first:"" ~last:"" ~sep:" " quoted) args ;
  flush_all () ;
  match fork () with
  | 0 ->
    let open Unix in
    if and_stop then RingBufLib.kill_myself Sys.sigstop ;
    Option.may chdir cwd ;
    close_fd 0 ;
    for i = 3 to 255 do
      try close_fd i with Unix.Unix_error (Unix.EBADF, _, _) -> ()
    done ;
    execve cmd args env
  | pid -> pid

(*$inject
  open Unix

  let check_status pid expected =
    let rep_pid, status = waitpid [ WNOHANG; WUNTRACED ] pid in
    let status =
      if rep_pid = 0 then None else Some status in
    let printer = function
      | None -> "no status"
      | Some st -> RamenHelpers.string_of_process_status st in
    assert_equal ~printer expected status

  let run ?and_stop args =
    let pid = run_background ?and_stop args.(0) args [||] in
    sleepf 0.1 ;
    pid
*)

(*$R run
  let pid = run [| "tests/test_false" |] in
  check_status pid (Some (WEXITED 1))
 *)

(*$R run
  let pid = run ~and_stop:true [| "/bin/sleep" ; "1" |] in
  check_status pid (Some (WSTOPPED Sys.sigstop)) ;
  kill pid Sys.sigcont ;
  check_status pid None ;
  sleepf 1.1 ;
  check_status pid (Some (WEXITED 0))
 *)

(*
 * Process Supervisor: Start and stop according to a mere file listing which
 * programs to run.
 *
 * Also monitor quit.
 *)

(* Description of a running worker.
 * Not persisted on disk. *)
type running_process =
  { params : RamenName.params ;
    bin : string ;
    func : C.Func.t ;
    log_level : log_level ;
    report_period : float ;
    mutable pid : int option ;
    mutable last_killed : float (* 0 for never *) ;
    mutable continued : bool ;
    (* purely for reporting: *)
    mutable last_exit : float ;
    mutable last_exit_status : string ;
    mutable succ_failures : int ;
    mutable quarantine_until : float }

let print_running_process oc proc =
  Printf.fprintf oc "%s/%s (parents=%a)"
    (RamenName.string_of_program proc.func.F.program_name)
    (RamenName.string_of_func proc.func.F.name)
    (List.print F.print_parent) proc.func.parents

let make_running_process conf mre func =
  let log_level =
    if mre.C.debug then Debug else conf.C.log_level in
  { bin = mre.C.bin ; params = mre.C.params ; pid = None ;
    last_killed = 0. ; continued = false ; func ;
    last_exit = 0. ; last_exit_status = "" ; succ_failures = 0 ;
    quarantine_until = 0. ; log_level ;
    report_period = mre.C.report_period }

(* Returns the name of func input ringbuf for the given parent (if func is
 * merging, each parent uses a distinct one) and the file_spec. *)
let input_spec conf parent child =
  (* In case of merge, ringbufs are numbered as the node parents: *)
  (if child.F.merge_inputs then
    match List.findi (fun _ (pprog, pname) ->
            let pprog_name =
              F.program_of_parent_prog child.F.program_name pprog in
            pprog_name = parent.F.program_name && pname = parent.name
          ) child.parents with
    | exception Not_found ->
        !logger.error "Operation %S is not a child of %S"
          (RamenName.string_of_func child.name)
          (RamenName.string_of_func parent.name) ;
        invalid_arg "input_spec"
    | i, _ ->
        C.in_ringbuf_name_merging conf child i
  else C.in_ringbuf_name_single conf child),
  let out_type = RingBufLib.ser_tuple_typ_of_tuple_typ parent.out_type
  and in_type = child.in_type in
  let field_mask = RingBufLib.skip_list ~out_type ~in_type in
  RamenOutRef.{ field_mask ; timeout = 0. }

let check_is_subtype t1 t2 =
  (* For t1 to be a subtype of t2, all fields of t1 must be present and
   * public in t2. And since there is no more extension from scalar types at
   * this stage, those fields must have the exact same types. *)
  List.iter (fun f1 ->
    match List.find (fun f2 -> f1.RamenTuple.typ_name = f2.RamenTuple.typ_name) t2 with
    | exception Not_found ->
        failwith ("Field "^ f1.typ_name ^" is missing")
    | f2 ->
        if is_private_field f2.typ_name then
          failwith ("Field "^ f2.typ_name ^" is private") ;
        if f1.typ <> f2.typ then
          failwith ("Fields "^ f1.typ_name ^" have not the same type")
  ) t1

(* Returns the running parents and children of a func: *)
let relatives f must_run =
  Hashtbl.fold (fun _k (_, func) (ps, cs) ->
    (* Tells if [func'] is a parent of [func]: *)
    let is_parent_of func func' =
      List.exists (fun (rel_par_prog, par_func) ->
        func'.F.name = par_func &&
        func'.F.program_name =
          F.program_of_parent_prog func.F.program_name rel_par_prog
      ) func.F.parents in
    (if is_parent_of f func then func::ps else ps),
    (if is_parent_of func f then func::cs else cs)
  ) must_run ([], [])

open Binocle

let stats_worker_crashes =
  RamenBinocle.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir
      RamenConsts.Metric.Names.worker_crashes
      "Number of workers that have crashed (or exited with non 0 status).")

let stats_worker_deadloopings =
  RamenBinocle.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir
      RamenConsts.Metric.Names.worker_deadloopings
      "Number of time a worker has been found to deadloop.")

let stats_worker_count =
  IntGauge.make RamenConsts.Metric.Names.worker_count
    "Number of workers configured to run."

let stats_worker_running =
  IntGauge.make RamenConsts.Metric.Names.worker_running
    "Number of workers actually running."

let stats_ringbuf_repairs =
  RamenBinocle.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir
      RamenConsts.Metric.Names.ringbuf_repairs
      "Number of times a worker ringbuf had to be repaired.")

let stats_outref_repairs =
  RamenBinocle.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir
      RamenConsts.Metric.Names.outref_repairs
      "Number of times a worker outref had to be repaired.")

let stats_worker_sigkills =
  RamenBinocle.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir
      RamenConsts.Metric.Names.worker_sigkills
      "Number of times a worker had to be sigkilled instead of sigtermed.")

(* When a worker seems to crashloop, assume it's because of a bad file and
 * delete them! *)
let rescue_worker conf func params =
  (* Maybe the state file is poisoned? At this stage it's probably safer
   * to move it away: *)
  !logger.info "Worker %s is deadlooping. Deleting its state file and \
                input ringbuffers."
    (RamenName.string_of_fq (F.fq_name func)) ;
  let state_file = C.worker_state conf func params in
  move_file_away state_file ;
  (* At this stage there should be no writers since this worker is stopped. *)
  let input_ringbufs = C.in_ringbuf_names conf func in
  List.iter move_file_away input_ringbufs

(* Then this function is cleaning the running hash: *)
let process_workers_terminations conf running =
  let open Unix in
  let now = gettimeofday () in
  Hashtbl.iter (fun _ proc ->
    Option.may (fun pid ->
      let what =
        Printf.sprintf "Operation %s/%s (pid %d)"
          (RamenName.string_of_program proc.func.F.program_name)
          (RamenName.string_of_func proc.func.name)
          pid in
      (match restart_on_EINTR (waitpid [ WNOHANG ; WUNTRACED ]) pid with
      | exception exn ->
          !logger.error "%s: waitpid: %s" what (Printexc.to_string exn)
      | 0, _ -> () (* Nothing to report *)
      | _, (WSIGNALED s | WSTOPPED s) when s = Sys.sigstop ->
          !logger.debug "%s got stopped" what
      | _, status ->
          let status_str = string_of_process_status status in
          let is_err = status <> Unix.WEXITED 0 in
          (if is_err then !logger.error else info_or_test conf)
            "%s %s." what status_str ;
          proc.last_exit <- now ;
          proc.last_exit_status <- status_str ;
          if is_err then (
            proc.succ_failures <- proc.succ_failures + 1 ;
            IntCounter.inc (stats_worker_crashes conf.C.persist_dir) ;
            if proc.succ_failures = 5 then (
              IntCounter.inc (stats_worker_deadloopings conf.C.persist_dir) ;
              rescue_worker conf proc.func proc.params)) ;
          (* Wait before attempting to restart a failing worker: *)
          let max_delay = float_of_int proc.succ_failures in
          proc.quarantine_until <-
            now +. Random.float (min 90. max_delay) ;
          proc.pid <- None)
    ) proc.pid
  ) running

let really_start conf proc parents children =
  (* Create the input ringbufs.
   * We now start the workers one by one in no
   * particular order. The input and out-ref ringbufs are created when the
   * worker start, and the out-ref is filled with the running (or should
   * be running) children. Therefore, if a children fails to run the
   * parents might block. Also, if a child has not been started yet its
   * inbound ringbuf will not exist yet, again implying this worker will
   * block.
   * Each time a new worker is started or stopped the parents outrefs
   * are updated. *)
  let fq_str = RamenName.string_of_fq (F.fq_name proc.func) in
  !logger.debug "Creating in buffers..." ;
  let input_ringbufs = C.in_ringbuf_names conf proc.func in
  List.iter (fun rb_name ->
    RingBuf.create rb_name ;
    (* FIXME: if a worker started to write and we repair while it hasn't
     * committed, that message will be lost. Better address this in repair
     * itself by spinning a bit: if we can't see tail=head on a few tries
     * then only assume it's broken. *)
    let rb = RingBuf.load rb_name in
    finally (fun () -> RingBuf.unload rb)
      (fun () ->
        repair_and_warn fq_str rb ;
        IntCounter.inc (stats_ringbuf_repairs conf.C.persist_dir)) ()
  ) input_ringbufs ;
  (* And the pre-filled out_ref: *)
  !logger.debug "Updating out-ref buffers..." ;
  let out_ringbuf_ref = C.out_ringbuf_names_ref conf proc.func in
  List.iter (fun c ->
    let fname, _specs as out = input_spec conf proc.func c in
    (* The destination ringbuffer must exist before it's referenced in an
     * out-ref, or the worker might err and throw away the tuples: *)
    RingBuf.create fname ;
    RamenOutRef.add out_ringbuf_ref out
  ) children ;
  (* Now that the out_ref exists, but before we actually fork the worker,
   * we can start importing: *)
  (* Always export for a little while at the beginning *)
  let _bname =
    RamenExport.make_temp_export ~duration:conf.initial_export_duration
      conf proc.func in
  (* Now actually start the binary *)
  let notify_ringbuf =
    (* Where that worker must write its notifications. Normally toward a
     * ringbuffer that's read by Ramen, unless it's a test programs.
     * Tests must not send their notifications to Ramen with real ones,
     * but instead to a ringbuffer specific to the test_id. *)
    C.notify_ringbuf conf in
  let ocamlrunparam =
    let def = if proc.log_level = Debug then "b" else "" in
    getenv ~def "OCAMLRUNPARAM" in
  let env = [|
    "OCAMLRUNPARAM="^ ocamlrunparam ;
    "log_level="^ string_of_log_level proc.log_level ;
    (* To know what to log: *)
    "is_test="^ string_of_bool conf.C.test ;
    (* Used to choose the function to perform: *)
    "name="^ RamenName.string_of_func (proc.func.F.name) ;
    "fq_name="^ fq_str ; (* Used for monitoring *)
    "output_ringbufs_ref="^ out_ringbuf_ref ;
    "report_ringbuf="^ C.report_ringbuf conf ;
    "report_period="^ string_of_float proc.report_period ;
    "notify_ringbuf="^ notify_ringbuf ;
    "rand_seed="^ (match !rand_seed with None -> ""
                  | Some s -> string_of_int s) ;
    (* We need to change this dir whenever the func signature or params
     * change to prevent it to reload an incompatible state: *)
    "state_file="^ C.worker_state conf proc.func proc.params ;
    (match !logger.output with
      | Directory _ ->
        "log="^ conf.C.persist_dir ^"/log/workers/" ^ F.path proc.func
      | Stdout -> "no_log" (* aka stdout/err *)
      | Syslog -> "log=syslog") |] in
  (* Pass each input ringbuffer in a sequence of envvars: *)
  let more_env =
    List.enum input_ringbufs |>
    Enum.mapi (fun i n -> "input_ringbuf_"^ string_of_int i ^"="^ n) in
  (* Pass each individual parameter as a separate envvar; envvars are just
   * non interpreted strings (but for the first '=' sign that will be
   * interpreted by the OCaml runtime) so it should work regardless of the
   * actual param name or value, and make it easier to see what's going
   * on from the shell. Notice that we pass all the parameters including
   * those omitted by the user. *)
  let more_env =
    Hashtbl.enum proc.params /@
    (fun (n, v) ->
      Printf.sprintf2 "param_%s=%a"
        n RamenTypes.print v) |>
    Enum.append more_env in
  (* Also add all envvars that are defined and used in the operation: *)
  let more_env =
    List.enum proc.func.envvars //@
    (fun n -> try Some (n ^"="^ Sys.getenv n) with Not_found -> None) |>
    Enum.append more_env in
  let env = Array.append env (Array.of_enum more_env) in
  let args =
    (* For convenience let's add "ramen worker" and the fun name as
     * arguments: *)
    [| RamenConsts.worker_argv0 ; fq_str |] in
  (* Better have the workers CWD where the binary is, so that any file name
   * mentioned in the program is relative to the program. *)
  let cwd = Filename.dirname proc.bin in
  let cmd = Filename.basename proc.bin in
  let pid = run_background ~cwd ~and_stop:conf.C.test cmd args env in
  !logger.debug "Function %s now runs under pid %d" fq_str pid ;
  proc.pid <- Some pid ;
  proc.last_killed <- 0. ;
  (* Update the parents out_ringbuf_ref: *)
  List.iter (fun p ->
    let out_ref =
      C.out_ringbuf_names_ref conf p in
    RamenOutRef.add out_ref (input_spec conf p proc.func)
  ) parents

(* Try to start the given proc.
 * Check links (ie.: do parents and children have the proper types?)
 * Need [must_run] so that types can be checked before linking with parents
 * and children. *)
let really_try_start conf must_run proc =
  info_or_test conf "Starting operation %a"
    print_running_process proc ;
  assert (proc.pid = None) ;
  let parents, children = relatives proc.func must_run in
  (* We must not start if a parent is missing: *)
  let parents_ok =
    List.fold_left (fun ok parent ->
      if match parent with
         | None, _ ->
            true (* Parent runs in the very program we want to start *)
         | Some rel_p_prog, p_func ->
            let p_prog =
              RamenName.(program_of_rel_program proc.func.program_name rel_p_prog) in
            List.exists (fun func ->
              func.F.program_name = p_prog && func.F.name = p_func
            ) parents
      then ok
      else (
        !logger.error "Parent of %s is missing: %a"
          (RamenName.string_of_fq (F.fq_name proc.func))
          F.print_parent parent ;
        false)
    ) true proc.func.parents in
  let check_linkage p c =
    try check_is_subtype c.F.in_type p.F.out_type ;
        true
    with Failure msg ->
      !logger.error "Input type of %s (%a) is not compatible with \
                     output type of %s (%a): %s"
        (RamenName.string_of_fq (F.fq_name c))
        RamenTuple.print_typ_names c.in_type
        (RamenName.string_of_fq (F.fq_name p))
        RamenTuple.print_typ_names p.out_type
        msg ;
      false in
  let linkage_ok =
    List.fold_left (fun ok p ->
      check_linkage p proc.func && ok
    ) true parents in
  let linkage_ok =
    List.fold_left (fun ok c ->
      check_linkage proc.func c && ok
    ) linkage_ok children in
  if parents_ok && linkage_ok then
    really_start conf proc parents children

let try_start conf must_run proc =
  let now = Unix.gettimeofday () in
  if proc.quarantine_until > now then (
    !logger.debug "Operation %a still in quarantine"
      print_running_process proc
  ) else (
    really_try_start conf must_run proc
  )

let try_kill conf must_run proc =
  let pid = Option.get proc.pid in
  (* There is no reason to wait before we remove this worker from its
   * parent out-ref: if it's not replaced then the last unprocessed
   * tuples are lost. If it's indeed a replacement then the new version
   * will have a chance to process the left overs. *)
  let parents, _children = relatives proc.func must_run in
  (* Let's remove this proc from all its parent out-ref. *)
  let input_ringbufs = C.in_ringbuf_names conf proc.func in
  List.iter (fun p ->
    let parent_out_ref =
      C.out_ringbuf_names_ref conf p in
    List.iter (fun this_in ->
      RamenOutRef.remove parent_out_ref this_in
    ) input_ringbufs
  ) parents ;
  (* If it's still stopped, unblock first: *)
  log_and_ignore_exceptions ~what:"Continuing worker (before kill)"
    (Unix.kill pid) Sys.sigcont ;
  (* Start with a TERM (if last_killed = 0.) and then after 5s send a
   * KILL if the worker is still running. *)
  let now = Unix.gettimeofday () in
  if proc.last_killed = 0. then (
    (* Ask politely: *)
    info_or_test conf "Terminating worker %s (pid %d)"
      (RamenName.string_of_fq (F.fq_name proc.func)) pid ;
    log_and_ignore_exceptions ~what:"Terminating worker"
      (Unix.kill pid) Sys.sigterm ;
    proc.last_killed <- now ;
  ) else if now -. proc.last_killed > 10. then (
    !logger.warning "Killing worker %s (pid %d) with bigger guns"
      (RamenName.string_of_fq (F.fq_name proc.func)) pid ;
    log_and_ignore_exceptions ~what:"Killing worker"
      (Unix.kill pid) Sys.sigkill ;
    proc.last_killed <- now ;
    IntCounter.inc (stats_worker_sigkills conf.C.persist_dir))

let check_out_ref =
  let do_check_out_ref conf must_run =
    !logger.debug "Checking out_refs..." ;
    (* Build the set of all wrapping ringbuf that are being read: *)
    let rbs =
      Hashtbl.fold (fun _k (_, func) s ->
        C.in_ringbuf_names conf func |>
        List.fold_left (fun s rb_name -> Set.add rb_name s) s
      ) must_run (Set.singleton (C.notify_ringbuf conf)) in
    Hashtbl.iter (fun _ (_, func) ->
      (* Iter over all functions and check they do not output to a ringbuf not
       * in this set: *)
      let out_ref = C.out_ringbuf_names_ref conf func in
      let outs = RamenOutRef.read out_ref in
      Hashtbl.iter (fun fname _ ->
        if String.ends_with fname ".r" && not (Set.mem fname rbs) then (
          !logger.error "Operation %s outputs to %s, which is not read, fixing"
            (RamenName.string_of_fq (F.fq_name func)) fname ;
          IntCounter.inc (stats_outref_repairs conf.C.persist_dir) ;
          RamenOutRef.remove out_ref fname)
      ) outs ;
      (* Conversely, check that all children are in the out_ref of their
       * parent: *)
      let par_funcs, _c = relatives func must_run in
      let in_rbs = C.in_ringbuf_names conf func |> Set.of_list in
      List.iter (fun par_func ->
        let out_ref = C.out_ringbuf_names_ref conf par_func in
        let outs = RamenOutRef.read out_ref in
        let outs = Hashtbl.keys outs |> Set.of_enum in
        if Set.disjoint in_rbs outs then (
          !logger.error "Operation %s must output to %s but does not, fixing"
            (RamenName.string_of_fq (F.fq_name par_func))
            (RamenName.string_of_fq (F.fq_name func)) ;
          RamenOutRef.add out_ref (input_spec conf par_func func))
      ) par_funcs
    ) must_run
  and last_checked_out_ref = ref 0. in
  fun conf must_run ->
    let now = Unix.time () in
    if now -. !last_checked_out_ref > 5. then (
      last_checked_out_ref := now ;
      do_check_out_ref conf must_run)

let signal_all_cont running =
  Hashtbl.iter (fun _ proc ->
    if proc.pid <> None && not proc.continued then (
      proc.continued <- true ;
      !logger.debug "Signaling %a to continue" print_running_process proc ;
      log_and_ignore_exceptions ~what:"Signaling worker to continue"
        (Unix.kill (Option.get proc.pid)) Sys.sigcont) ;
  ) running

(* Have a single watchdog even when supervisor is restarted: *)
let watchdog = ref None

(*
 * Synchronisation of the rc file of programs we want to run and the
 * actually running workers.
 *
 * [autoreload_delay]: even if the rc file hasn't changed, we want to re-read
 * it from time to time and refresh the [must_run] hash with new signatures
 * from the binaries that might have changed.  If some operations from a
 * program indeed has a new signature, then the former one will disappear from
 * [must_run] and the new one will enter it, and the new one will replace it.
 * We do not have to wait until the previous worker dies before starting the
 * new version: If they have the same input type they will share the same
 * input ringbuf and steal some work from each others, which is still better
 * than having only the former worker doing all the work.  And if they have
 * different input types then the tuples will switch toward the new instance
 * as the parent out-ref gets updated. Similarly, they use different state
 * files.  They might both be present in a parent out_ref though, so some
 * duplication of tuple is possible (or conversely: some input tuples might
 * be missing if we kill the previous before starting the new one).
 * If they do share the same input ringbuf though, it is important that we
 * remove the former one before we add the new one (or the parent out-ref
 * will end-up empty). This is why we first do all the kills, then all the
 * starts (Note that even if a worker does not terminate immediately on
 * kill, its parent out-ref is cleaned immediately).
 *
 * Regarding [conf.test]:
 * In that mode, add a parameter to the workers so that they stop before doing
 * anything. Then, when nothing is left to be started, the synchronizer will
 * unblock all workers. This mechanism enforces that CSV readers do not start
 * to read tuples before all their children are attached to their out_ref file.
 *)
let synchronize_running conf autoreload_delay =
  let rc_file = C.running_config_file conf in
  if !watchdog = None then
    watchdog :=
      (* In the first run we might have *plenty* of workers to start, thus
       * the extended grace_period (it's not unheard of >1min to start all
       * workers on a small VM) *)
      Some (RamenWatchdog.make ~grace_period:180. ~timeout:30.
                               "supervisor" quit) ;
  let watchdog = Option.get !watchdog in
  (* Stop/Start processes so that [running] corresponds to [must_run].
   * [must_run] is a hash from the function mount point, signature and
   * parameters to the [C.must_run_entry] and func.
   * [running] is a hash from the function mount point, signature and
   * parameters to its running_process (mutable pid, cleared asynchronously
   * when the worker terminates). *)
  let synchronize must_run running =
    (* First, remove from running all terminated processes that must not run
     * any longer. Send a kill to those that are still running. *)
    let prev_num_running = Hashtbl.length running in
    IntGauge.set stats_worker_count (Hashtbl.length must_run) ;
    IntGauge.set stats_worker_running prev_num_running ;
    let to_kill = ref [] and to_start = ref []
    and (+=) r x = r := x :: !r in
    Hashtbl.filteri_inplace (fun k proc ->
      if Hashtbl.mem must_run k then true else
      if proc.pid <> None then (to_kill += proc ; true) else
      false
    ) running ;
    (* Then, add/restart all those that must run. *)
    Hashtbl.iter (fun k (mre, func) ->
      match Hashtbl.find running k with
      | exception Not_found ->
          let proc = make_running_process conf mre func in
          Hashtbl.add running k proc ;
          to_start += proc
      | proc ->
          (* If it's dead, restart it: *)
          if proc.pid = None then to_start += proc else
          (* If we were killing it, it's safer to keep killing it until it's
           * dead and then restart it. *)
          if proc.last_killed <> 0. then to_kill += proc
    ) must_run ;
    let num_running = Hashtbl.length running in
    if !quit <> None && num_running > 0 && num_running <> prev_num_running then
      info_or_test conf "Still %d processes running"
        (Hashtbl.length running) ;
    (* See preamble discussion about autoreload for why workers must be
     * started only after all the kills: *)
    if !to_kill <> [] then !logger.debug "Starting the kills" ;
    List.iter (try_kill conf must_run) !to_kill ;
    if !to_start <> [] then !logger.debug "Starting the starts" ;
    List.iter (fun proc ->
      try_start conf must_run proc ;
      (* If we had to compile a program this is worth resetting the watchdog: *)
      RamenWatchdog.reset watchdog
    ) !to_start ;
    (* Try to fix any issue with out_refs: *)
    if !to_start = [] && !to_kill = [] && !quit = None then
      check_out_ref conf must_run ;
    if !to_start = [] && conf.C.test then signal_all_cont running ;
    (* Return if anything changed: *)
    !to_kill <> [] || !to_start <> []
  in
  (* Once we have forked some workers we must not allow an exception to
   * terminate this function or we'd leave unsupervised workers behind: *)
  let rec none_shall_pass f =
    try f ()
    with exn ->
      print_exception exn ;
      !logger.error "Crashed while supervising children, keep trying!" ;
      Unix.sleepf (1. +. Random.float 1.) ;
      (none_shall_pass [@tailcall]) f
  in
  (* The hash of programs that must be running, updated by [loop]: *)
  let must_run = Hashtbl.create 307
  and running = Hashtbl.create 307 in
  let rec loop last_read =
    none_shall_pass (fun () ->
      process_workers_terminations conf running ;
      if !quit <> None && Hashtbl.length running = 0 then (
        !logger.info "All processes stopped, quitting."
      ) else (
        let last_read =
          if !quit <> None then (
            Hashtbl.clear must_run ;
            last_read
          ) else (
            let last_mod =
              try Some (mtime_of_file rc_file)
              with Unix.(Unix_error (ENOENT, _, _)) -> None in
            let now = Unix.gettimeofday () in
            let must_reread =
              match last_mod with
              | None -> false
              | Some lm ->
                  (lm >= last_read -. 1. ||
                   autoreload_delay > 0. &&
                   now -. last_read >= autoreload_delay) &&
                  (* To prevent missing the last writes when the file is
                   * updated several times a second (the possible resolution
                   * of mtime), refuse to refresh the file unless last mod
                   * time is old enough: *)
                  now -. lm > 1. in
            if last_mod <> None && must_reread then (
              (* Reread the content of that file *)
              let must_run_programs = C.with_rlock conf identity in
              (* The run file gives us the programs (and how to run them), but we
               * want [must_run] to be a hash of functions. Also, we want the
               * workers identified by their signature so that if the type of a
               * worker change but not its name we see a different worker. *)
              Hashtbl.clear must_run ;
              Hashtbl.iter (fun program_name (mre, get_rc) ->
                if not mre.C.killed then (
                  if mre.C.src_file <> "" then (
                    !logger.debug "Trying to build %S" mre.C.bin ;
                    RamenMake.build conf program_name mre.C.src_file mre.C.bin) ;
                  match get_rc () with
                  | exception _ ->
                      (* Errors have been logged already, nothing more can
                       * be done about this. *)
                      ()
                  | prog ->
                      List.iter (fun f ->
                        (* Use the mount point + signature + params as the key.
                         * Notice that we take all the parameter values (from
                         * prog.params), not only the explitly set values (from
                         * mre.params), so that if a default value that is
                         * unset is changed in the program then that's considered a
                         * different program. *)
                        let k =
                          program_name, f.F.name, f.F.signature, prog.P.params
                        in
                        Hashtbl.add must_run k (mre, f)
                      ) prog.P.funcs)
              ) must_run_programs ;
              now
            ) else last_read) in
        let changed = synchronize must_run running in
        (* Touch the rc file if anything changed (esp. autoreload) since that
         * mtime is used to signal cache expirations etc. *)
        if changed then touch_file rc_file last_read ;
        Gc.minor () ;
        let delay = if !quit = None then 1. else 0.3 in
        Unix.sleepf delay ;
        RamenWatchdog.reset watchdog ;
        loop last_read))
  in
  RamenWatchdog.enable watchdog ;
  loop 0. ;
  RamenWatchdog.disable watchdog
