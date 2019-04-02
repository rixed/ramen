(* Process supervisor which task is to start and stop workers, make sure they
 * keep working, collect their stats and write them somewhere for anybody
 * to see, send their notifications, delete old unused files...
 *)
open Batteries
open RamenLog
open RamenHelpers
open RamenConsts
module C = RamenConf
module F = C.Func
module P = C.Program
module E = RamenExpr
module O = RamenOperation
module N = RamenName
module OutRef = RamenOutRef
module Files = RamenFiles
module Services = RamenServices

(* Global quit flag, set (to some ExitCodes) when the term signal
 * is received or some other bad condition happen: *)
let quit = ref None

let until_quit f =
  let rec loop () =
    if !quit = None && f () then loop () in
  loop ()

let dummy_nop () =
  !logger.warning "Running in dummy mode" ;
  until_quit (fun () -> Unix.sleep 3 ; true)

let rec sleep_or_exit ?(while_=always) t =
  if t > 0. && while_ () then (
    let delay = min 1. t in
    Unix.sleepf delay ;
    sleep_or_exit ~while_ (t -. delay)
  )

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
      Some (if s = Sys.sigterm then ExitCodes.terminated
                               else ExitCodes.interrupted))) ;
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
  !logger.debug "Running %a as: /usr/bin/env %a %a %a"
    N.path_print cmd
    (Array.print ~first:"" ~last:"" ~sep:" " quoted) env
    N.path_print_quoted cmd
    (Array.print ~first:"" ~last:"" ~sep:" " quoted) args ;
  flush_all () ;
  match fork () with
  | 0 ->
    (try
      if and_stop then RingBufLib.kill_myself Sys.sigstop ;
      Option.may (fun (d : N.path) -> chdir (d :> string)) cwd ;
      let null = openfile "/dev/null" [O_RDONLY] 0 in
      dup2 null stdin ;
      close null ;
      for i = 3 to 255 do
        try close_fd i with Unix.Unix_error (Unix.EBADF, _, _) -> ()
      done ;
      execve (cmd :> string) args env
    with e ->
      Printf.eprintf "Cannot execve: %s\n%!" (Printexc.to_string e) ;
      sys_exit 127)
  | pid -> pid

(*$inject
  open Unix
  module N = RamenName

  let check_status pid expected =
    let rep_pid, status = waitpid [ WNOHANG; WUNTRACED ] pid in
    let status =
      if rep_pid = 0 then None else Some status in
    let printer = function
      | None -> "no status"
      | Some st -> RamenHelpers.string_of_process_status st in
    assert_equal ~printer expected status

  let run ?and_stop args =
    let pid = run_background ?and_stop (N.path args.(0)) args [||] in
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

type top_half_spec =
  { tunneld_host : N.host ; tunneld_port : int ; parent_num : int }

(* What is run from a worker: *)
type worker_part =
  | Whole
  (* Top half: only the filtering part of that function is run, once for
   * every local parent; output is forwarded to another host. *)
  | TopHalf of top_half_spec list
  (* Bottom-half: The remote parent of a locally running function.
   * Nothing will be run in this case actually, but it's useful to know
   * a parent is running somewhere. *)
  | BottomHalf

let print_worker_part oc = function
  | Whole -> String.print oc "whole worker"
  | TopHalf _ -> String.print oc "top half"
  | BottomHalf -> String.print oc "bottom half"

(* What we store in the [must_run] hash: *)
type must_run_entry =
  { rce : C.rc_entry ;
    prog : P.t ;
    func : F.t ;
    part : worker_part }

let print_must_run_entry oc mre =
  Printf.fprintf oc "%a/%a"
    N.program_print mre.func.F.program_name
    N.func_print mre.func.F.name

(* Description of a running worker that is stored in the [running] hash.
 * Not persisted on disk. *)
type running_process =
  { params : N.params ;
    bin : N.path ;
    func : C.Func.t ;
    log_level : log_level ;
    report_period : float ;
    top_half : top_half_spec list ; (* empty list -> not a top half *)
    mutable pid : int option ;
    mutable last_killed : float (* 0 for never *) ;
    mutable continued : bool ;
    (* purely for reporting: *)
    mutable last_exit : float ;
    mutable last_exit_status : string ;
    mutable succ_failures : int ;
    mutable quarantine_until : float }

let print_running_process oc proc =
  Printf.fprintf oc "%s/%s%s (parents=%a)"
    (proc.func.F.program_name :> string)
    (proc.func.F.name :> string)
    (if proc.top_half <> [] then " (TOP-HALF)" else "")
    (List.print F.print_parent) proc.func.parents

let make_running_process conf rce func part =
  let log_level =
    if rce.C.debug then Debug else conf.C.log_level in
  let top_half =
    match part with
    | TopHalf ths -> ths
    | _ -> [] in
  { bin = rce.C.bin ; params = rce.C.params ; top_half ; pid = None ;
    last_killed = 0. ; continued = false ; func ;
    last_exit = 0. ; last_exit_status = "" ; succ_failures = 0 ;
    quarantine_until = 0. ; log_level ;
    report_period = rce.C.report_period }

(* Returns the name of func input ringbuf for the given parent (if func is
 * merging, each parent uses a distinct one) and the file_spec. *)
let input_ringbuf_fname conf parent child =
  (* In case of merge, ringbufs are numbered as the node parents: *)
  if child.F.merge_inputs then
    match List.findi (fun _ (pprog, pname) ->
            let pprog_name =
              F.program_of_parent_prog child.F.program_name pprog in
            pprog_name = parent.F.program_name && pname = parent.name
          ) child.parents with
    | exception Not_found ->
        !logger.error "Operation %S is not a child of %S"
          (child.name :> string)
          (parent.name :> string) ;
        invalid_arg "input_ringbuf_fname"
    | i, _ ->
        C.in_ringbuf_name_merging conf child i
  else C.in_ringbuf_name_single conf child

let make_fieldmask parent child =
  let out_typ =
    O.out_type_of_operation ~with_private:false parent.F.operation in
  RamenFieldMaskLib.fieldmask_of_operation ~out_typ child.F.operation

let check_is_subtype t1 t2 =
  (* For t1 to be a subtype of t2, all fields of t1 must be present and
   * public in t2. And since there is no more extension from scalar types at
   * this stage, those fields must have the exact same types. *)
  List.iter (fun f1 ->
    let f2_typ =
      RamenFieldMaskLib.find_type_of_path t2 f1.RamenFieldMaskLib.path in
    if f1.typ <> f2_typ then
      Printf.sprintf2 "Fields %a have different types"
        N.field_print (E.id_of_path f1.path) |>
      failwith
  ) t1

(* Returns the running parents and children of a func present in must_run. *)
let relatives f must_run =
  Hashtbl.fold (fun _k (mre : must_run_entry) (ps, cs) ->
    (* if [func'] is a parent of [func] then return its parent_num, or
     * raise Not_found: *)
    let is_parent_of func func' =
      List.findi (fun _ (rel_par_prog, par_func) ->
        func'.F.name = par_func &&
        func'.F.program_name =
          F.program_of_parent_prog func.F.program_name rel_par_prog
      ) func.F.parents |> fst in
    (try (mre, is_parent_of f mre.func)::ps with Not_found -> ps),
    (try (mre, is_parent_of mre.func f)::cs with Not_found -> cs)
  ) must_run ([], [])

open Binocle

let stats_worker_crashes =
  RamenBinocle.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.worker_crashes
      "Number of workers that have crashed (or exited with non 0 status).")

let stats_worker_deadloopings =
  RamenBinocle.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.worker_deadloopings
      "Number of time a worker has been found to deadloop.")

let stats_worker_count =
  IntGauge.make Metric.Names.worker_count
    "Number of workers configured to run."

let stats_worker_running =
  IntGauge.make Metric.Names.worker_running
    "Number of workers actually running."

let stats_ringbuf_repairs =
  RamenBinocle.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.ringbuf_repairs
      "Number of times a worker ringbuf had to be repaired.")

let stats_outref_repairs =
  RamenBinocle.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.outref_repairs
      "Number of times a worker outref had to be repaired.")

let stats_worker_sigkills =
  RamenBinocle.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.worker_sigkills
      "Number of times a worker had to be sigkilled instead of sigtermed.")

(* When a worker seems to crashloop, assume it's because of a bad file and
 * delete them! *)
let rescue_worker conf func params =
  (* Maybe the state file is poisoned? At this stage it's probably safer
   * to move it away: *)
  !logger.info "Worker %s is deadlooping. Deleting its state file and \
                input ringbuffers."
    ((F.fq_name func) :> string) ;
  let state_file = C.worker_state conf func params in
  Files.move_away state_file ;
  (* At this stage there should be no writers since this worker is stopped. *)
  let input_ringbufs = C.in_ringbuf_names conf func in
  List.iter Files.move_away input_ringbufs

(* Then this function is cleaning the running hash: *)
let process_workers_terminations conf running =
  let open Unix in
  let now = gettimeofday () in
  Hashtbl.iter (fun _ proc ->
    Option.may (fun pid ->
      let what =
        Printf.sprintf2 "Operation %a (pid %d)"
          print_running_process proc pid in
      (match restart_on_EINTR (waitpid [ WNOHANG ; WUNTRACED ]) pid with
      | exception exn ->
          !logger.error "%s: waitpid: %s" what (Printexc.to_string exn)
      | 0, _ -> () (* Nothing to report *)
      | _, (WSIGNALED s | WSTOPPED s) when s = Sys.sigstop ->
          !logger.debug "%s got stopped" what
      | _, status ->
          let status_str = string_of_process_status status in
          let is_err =
            status <> WEXITED ExitCodes.terminated in
          (if is_err then !logger.error else info_or_test conf)
            "%s %s." what status_str ;
          proc.last_exit <- now ;
          proc.last_exit_status <- status_str ;
          if is_err then (
            proc.succ_failures <- proc.succ_failures + 1 ;
            IntCounter.inc (stats_worker_crashes conf.C.persist_dir) ;
            if proc.succ_failures = 5 then (
              IntCounter.inc (stats_worker_deadloopings conf.C.persist_dir) ;
              rescue_worker conf proc.func proc.params)
          ) else (
            proc.succ_failures <- 0
          ) ;
          (* Wait before attempting to restart a failing worker: *)
          let max_delay = float_of_int proc.succ_failures in
          proc.quarantine_until <-
            now +. Random.float (min 90. max_delay) ;
          proc.pid <- None)
    ) proc.pid
  ) running

let run_worker ?and_stop (bin : N.path) args env =
  (* Better have the workers CWD where the binary is, so that any file name
   * mentioned in the program is relative to the program. *)
  let cwd = Files.dirname bin in
  let cmd = Files.basename bin in
  run_background ~cwd ?and_stop cmd args env

(* Returns the buffer name: *)
let start_export ?(file_type=OutRef.RingBuf)
                 ?(duration=Default.export_duration) conf func =
  let bname = C.archive_buf_name ~file_type conf func in
  if file_type = OutRef.RingBuf then
    RingBuf.create ~wrap:false bname ;
  (* Add that name to the function out-ref *)
  let out_ref = C.out_ringbuf_names_ref conf func in
  let out_typ =
    O.out_type_of_operation ~with_private:false func.F.operation in
  (* Negative durations, yielding a timestamp of 0, means no timeout ;
   * while duration = 0 means to actually not export anything (and we have
   * a cli-test that relies on the spec not being present in the out_ref
   * in that case): *)
  if duration <> 0. then (
    let timeout =
      if duration < 0. then 0. else Unix.gettimeofday () +. duration in
    let fieldmask = RamenFieldMaskLib.fieldmask_all ~out_typ in
    OutRef.add out_ref ~timeout ~file_type bname fieldmask
  ) ;
  bname

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
  let fq_str = (F.fq_name proc.func :> string) in
  let is_top_half = proc.top_half <> [] in
  !logger.debug "Creating in buffers..." ;
  let input_ringbufs =
    if is_top_half then
      [ C.in_ringbuf_name_single conf proc.func ]
    else
      C.in_ringbuf_names conf proc.func in
  List.iter (fun rb_name ->
    RingBuf.create rb_name ;
    let rb = RingBuf.load rb_name in
    finally (fun () -> RingBuf.unload rb)
      (fun () ->
        repair_and_warn fq_str rb ;
        IntCounter.inc (stats_ringbuf_repairs conf.C.persist_dir)) ()
  ) input_ringbufs ;
  (* And the pre-filled out_ref: *)
  let out_ringbuf_ref =
    if is_top_half then None else (
      !logger.debug "Updating out-ref buffers..." ;
      let out_ringbuf_ref = C.out_ringbuf_names_ref conf proc.func in
      List.iter (fun ((cmre : must_run_entry), _) ->
        let fname = input_ringbuf_fname conf proc.func cmre.func
        and fieldmask = make_fieldmask proc.func cmre.func in
        (* The destination ringbuffer must exist before it's referenced in an
         * out-ref, or the worker might err and throw away the tuples: *)
        RingBuf.create fname ;
        OutRef.add out_ringbuf_ref fname fieldmask
      ) children ;
      Some out_ringbuf_ref
    ) in
  (* Export for a little while at the beginning (help with both automatic
   * and manual tests): *)
  if not is_top_half then (
    let duration = conf.initial_export_duration in
    start_export ~duration conf proc.func |> ignore
  ) ;
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
    (* Select the function to be executed: *)
    "name="^ (proc.func.F.name :> string) ;
    "fq_name="^ fq_str ; (* Used for monitoring *)
    "report_ringbuf="^ (C.report_ringbuf conf :> string) ;
    "report_period="^ string_of_float proc.report_period ;
    "notify_ringbuf="^ (notify_ringbuf :> string) ;
    "rand_seed="^ (match !rand_seed with None -> ""
                  | Some s -> string_of_int s) ;
    "hostname="^ (conf.C.hostname :> string) ;
    (match !logger.output with
      | Directory _ ->
        let dir = N.path_cat [ conf.C.persist_dir ; N.path "log/workers" ;
                               F.path proc.func ] in
        "log="^ (dir :> string)
      | Stdout -> "no_log" (* aka stdout/err *)
      | Syslog -> "log=syslog") |] in
  let more_env =
    match proc.top_half with
    | [] ->
        List.enum
          [ "output_ringbufs_ref="^ (Option.get out_ringbuf_ref :> string) ;
            (* We need to change this dir whenever the func signature or
             * params change to prevent it to reload an incompatible state *)
            "state_file="^ (C.worker_state conf proc.func proc.params :>
                              string) ;
            "factors_dir="^ (C.factors_of_function conf proc.func :>
                              string) ]
    | ths ->
        Enum.append
          (List.enum ths |>
          Enum.mapi (fun i th ->
            "tunneld_host_"^ string_of_int i ^"="^
              (th.tunneld_host :> string)))
          (List.enum ths |>
          Enum.mapi (fun i th ->
            "tunneld_port_"^ string_of_int i ^"="^
              string_of_int th.tunneld_port)) |>
        Enum.append
          (List.enum ths |>
          Enum.mapi (fun i th ->
            "parent_num_"^ string_of_int i ^"="^
              string_of_int th.parent_num)) in
  (* Pass each input ringbuffer in a sequence of envvars: *)
  let more_env =
    List.enum input_ringbufs |>
    Enum.mapi (fun i (n : N.path) ->
      "input_ringbuf_"^ string_of_int i ^"="^ (n :> string)) |>
    Enum.append more_env in
  (* Pass each individual parameter as a separate envvar; envvars are just
   * non interpreted strings (but for the first '=' sign that will be
   * interpreted by the OCaml runtime) so it should work regardless of the
   * actual param name or value, and make it easier to see what's going
   * on from the shell. Notice that we pass all the parameters including
   * those omitted by the user. *)
  let more_env =
    P.env_of_params_and_exps conf proc.params |>
    Enum.append more_env in
  (* Also add all envvars that are defined and used in the operation: *)
  let more_env =
    List.enum proc.func.envvars //@
    (fun (n : N.field) ->
      try Some ((n :> string) ^"="^ Sys.getenv (n :> string))
      with Not_found -> None) |>
    Enum.append more_env in
  let env = Array.append env (Array.of_enum more_env) in
  let args =
    [| if proc.top_half = [] then Worker_argv0.full_worker
                             else Worker_argv0.top_half ;
       fq_str |] in
  let pid = run_worker ~and_stop:conf.C.test proc.bin args env in
  !logger.debug "Function %a now runs under pid %d"
    print_running_process proc pid ;
  proc.pid <- Some pid ;
  proc.last_killed <- 0. ;
  (* Update the parents out_ringbuf_ref: *)
  List.iter (fun ((pmre : must_run_entry), _) ->
    let out_ref =
      C.out_ringbuf_names_ref conf pmre.func in
    let fname = input_ringbuf_fname conf pmre.func proc.func
    and fieldmask = make_fieldmask pmre.func proc.func in
    OutRef.add out_ref fname fieldmask
  ) parents

(* Try to start the given proc.
 * Check links (ie.: do parents and children have the proper types?)
 * Need [must_run] so that types can be checked before linking with parents
 * and children. *)
let really_try_start conf now must_run proc =
  info_or_test conf "Starting operation %a"
    print_running_process proc ;
  assert (proc.pid = None) ;
  (* This returns only the parents/children that are actually running on
   * this host: *)
  let parents, children = relatives proc.func must_run in
  (* We must not start if a parent is missing: *)
  let check_parents () =
    List.iter (function
       | None, _ ->
          () (* Parent runs in the very program we want to start *)
       | Some rel_p_prog, p_func as parent ->
          let p_prog =
            N.(program_of_rel_program proc.func.program_name rel_p_prog) in
          let has_parent =
            List.exists (fun ((mre : must_run_entry), _) ->
              mre.func.F.program_name = p_prog &&
              mre.func.F.name = p_func
            ) parents in
          if not has_parent then
            Printf.sprintf2 "Parent of %s is missing: %a"
              (F.fq_name proc.func :> string)
              F.print_parent parent |>
            failwith
    ) proc.func.parents in
  let check_linkage p c =
    let out_type =
      O.out_type_of_operation ~with_private:false p.F.operation in
    try check_is_subtype c.F.in_type out_type
    with Failure msg ->
      Printf.sprintf2
        "Input type of %s (%a) is not compatible with \
         output type of %s (%a): %s"
        (F.fq_name c :> string)
        RamenFieldMaskLib.print_in_type c.in_type
        (F.fq_name p :> string)
        RamenTuple.print_typ_names out_type
        msg |>
      failwith in
  let parents_ok = ref false in (* This is benign *)
  try
    check_parents () ;
    parents_ok := true ;
    List.iter (fun ((pmre : must_run_entry), _) ->
      check_linkage pmre.func proc.func
    ) parents ;
    List.iter (fun ((cmre : must_run_entry), _) ->
      check_linkage proc.func cmre.func
    ) children ;
    really_start conf proc parents children
  with e ->
    print_exception ~what:"Cannot start worker" e ;
    (* Anything goes wrong when starting a node? Quarantine it! *)
    let delay =
      if not !parents_ok then 20. else 600. in
    proc.quarantine_until <-
      now +. Random.float (max 90. delay)

let try_start conf must_run proc =
  let now = Unix.gettimeofday () in
  if proc.quarantine_until > now then (
    !logger.debug "Operation %a still in quarantine"
      print_running_process proc
  ) else (
    really_try_start conf now must_run proc
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
  List.iter (fun ((pmre : must_run_entry), _) ->
    let parent_out_ref =
      C.out_ringbuf_names_ref conf pmre.func in
    List.iter (fun this_in ->
      OutRef.remove parent_out_ref this_in RamenChannel.live
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
      (F.fq_name proc.func :> string) pid ;
    log_and_ignore_exceptions ~what:"Terminating worker"
      (Unix.kill pid) Sys.sigterm ;
    proc.last_killed <- now ;
  ) else if now -. proc.last_killed > 10. then (
    !logger.warning "Killing worker %s (pid %d) with bigger guns"
      (F.fq_name proc.func :> string) pid ;
    log_and_ignore_exceptions ~what:"Killing worker"
      (Unix.kill pid) Sys.sigkill ;
    proc.last_killed <- now ;
    IntCounter.inc (stats_worker_sigkills conf.C.persist_dir))

(* This is used to check that we do not check too often nor too rarely: *)
let last_checked_outref = ref 0.

(* Need also running to check which workers are actually running *)
let check_out_ref conf must_run running =
  !logger.debug "Checking out_refs..." ;
  (* Build the set of all wrapping ringbuf that are being read.
   * Note that input rb of a top-half must be treated the same as input
   * rb of a full worker: *)
  let rbs =
    Hashtbl.fold (fun _k (mre : must_run_entry) s ->
      C.in_ringbuf_names conf mre.func |>
      List.fold_left (fun s rb_name -> Set.add rb_name s) s
    ) must_run (Set.singleton (C.notify_ringbuf conf)) in
  Hashtbl.iter (fun _ proc ->
    (* Iter over all running functions and check they do not output to a
     * ringbuf not in this set: *)
    if proc.pid <> None then (
      let out_ref = C.out_ringbuf_names_ref conf proc.func in
      let outs = OutRef.read_live out_ref in
      Hashtbl.iter (fun fname _ ->
        if Files.has_ext "r" fname && not (Set.mem fname rbs) then (
          !logger.error "Operation %a outputs to %a, which is not read, fixing"
            print_running_process proc
            N.path_print fname ;
          log_and_ignore_exceptions ~what:("fixing "^ (fname :> string))
            (fun () ->
              IntCounter.inc (stats_outref_repairs conf.C.persist_dir) ;
              OutRef.remove out_ref fname RamenChannel.live) ())
      ) outs ;
      (* Conversely, check that all children are in the out_ref of their
       * parent: *)
      let parents, _c = relatives proc.func must_run in
      let in_rbs = C.in_ringbuf_names conf proc.func |> Set.of_list in
      List.iter (fun ((pmre : must_run_entry), _) ->
        let out_ref = C.out_ringbuf_names_ref conf pmre.func in
        let outs = OutRef.read_live out_ref in
        let outs = Hashtbl.keys outs |> Set.of_enum in
        if Set.disjoint in_rbs outs then (
          !logger.error "Operation %a must output to %a but does not, fixing"
            print_must_run_entry pmre
            print_running_process proc ;
          log_and_ignore_exceptions ~what:("fixing "^(out_ref :> string))
            (fun () ->
              let fname = input_ringbuf_fname conf pmre.func proc.func
              and fieldmask = make_fieldmask pmre.func proc.func in
              OutRef.add out_ref fname fieldmask) ())
      ) parents
    )
  ) running

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
  let match_localhost on_hostname =
    Globs.matches on_hostname (conf.C.hostname :> string) in
  let key_of (mre : must_run_entry) part =
      mre.func.F.program_name, mre.func.F.name,
      mre.func.F.signature, mre.prog.P.params, part in
  if !watchdog = None then
    watchdog :=
      (* In the first run we might have *plenty* of workers to start, thus
       * the extended grace_period (it's not unheard of >1min to start all
       * workers on a small VM) *)
      Some (RamenWatchdog.make ~grace_period:180. ~timeout:30.
                               "supervisor" quit) ;
  let watchdog = Option.get !watchdog in
  (* Stop/Start processes so that [running] corresponds to [must_run].
   * [must_run] is a hash from the function mount point (program and function
   * name), signature, parameters and top-half info, to the
   * [C.must_run_entry], prog and func.
   *
   * [running] is a hash from the same key to its running_process (mutable
   * pid, cleared asynchronously when the worker terminates).
   *
   * FIXME: it would be nice if all parents were resolved once and for all
   * in [must_run] as well, as a list of optional function mount point (FQ).
   * When a function is reparented we would then detect it and could fix the
   * outref immediately.
   * Similarly, [running] should keep the previous set of parents (or rather,
   * the name of their out_ref). *)
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
    Hashtbl.iter (fun (_, _, _, _, part as k) mre ->
      match Hashtbl.find running k with
      | exception Not_found ->
          if part <> BottomHalf then (
            let proc = make_running_process conf mre.rce mre.func part in
            Hashtbl.add running k proc ;
            to_start += proc)
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
    (* FIXME: sort it so that parents are started before children,
     * so that in case of linkage error we do not end up with orphans
     * preventing parents to be run. *)
    List.iter (fun proc ->
      try_start conf must_run proc ;
      (* If we had to compile a program this is worth resetting the watchdog: *)
      RamenWatchdog.reset watchdog
    ) !to_start ;
    (* Try to fix any issue with out_refs: *)
    let now = Unix.time () in
    if (now > !last_checked_outref +. 30. ||
        now > !last_checked_outref +. 5. && !to_start = [] && !to_kill = []) &&
       !quit = None
    then (
      last_checked_outref := now ;
      log_and_ignore_exceptions ~what:"checking out_refs"
        (check_out_ref conf must_run) running) ;
    if !to_start = [] && conf.C.test then signal_all_cont running ;
    (* Return if anything changed: *)
    !to_kill <> [] || !to_start <> []
  in
  let build_must_run must_run conf =
    (* Reread the content of that file *)
    let programs = C.with_rlock conf identity in
    (* The run file gives us the programs (and how to run them), but
     * we want [must_run] to be a hash of functions. Also, we want
     * the workers identified by their signature so that if the type
     * of a worker changes but not its name we see a different
     * worker. *)
    Hashtbl.clear must_run ;
    (* First put into [must_run] the programs that must run in this
     * host: *)
    Hashtbl.iter (fun program_name (rce, get_rc) ->
      if match_localhost rce.C.on_hostname then (
        if not (N.is_empty rce.C.src_file) then (
          !logger.debug "Trying to build %a"
            N.path_print rce.C.bin ;
          log_and_ignore_exceptions
            ~what:("rebuilding "^ (rce.C.bin :> string))
            (fun () ->
              let get_parent =
                RamenCompiler.parent_from_programs programs in
              RamenMake.build
                conf get_parent program_name rce.C.src_file
                rce.C.bin) ()) ;
        (match get_rc () with
        | exception _ ->
            (* Errors have been logged already, nothing more can
             * be done about this. *)
            ()
        | prog ->
            (* NOTE: We might want to do this only when
             *   P.wants_to_run conf rce.bin rce.params
             * but given we cannot change the environment nor
             * re-read the external experiment file there would be
             * little use for this. *)
            List.iter (fun f ->
              (* Use the mount point + signature + params as the key.
               * Notice that we take all the parameter values (from
               * prog.params), not only the explicitly set values (from
               * rce.params), so that if a default value that is
               * unset is changed in the program then that's considered a
               * different program. *)
              let k =
                program_name, f.F.name, f.F.signature,
                prog.P.params, Whole
              in
              Hashtbl.modify_opt k (function
                | None ->
                    Some { rce ; prog ; func = f ; part = Whole }
                | prev ->
                    !logger.error
                      "The same function is asked to run several \
                       times: %a/%a, sig %s with params %a"
                      N.program_print program_name
                      N.func_print f.F.name
                      f.F.signature
                      RamenTuple.print_params prog.P.params ;
                    prev
              ) must_run
            ) prog.P.funcs)
      )
    ) programs ;
    (* Now add the bottom (resp top) halves of functions that must
     * run on other hosts but which are parents (resp. children)
     * of functions running locally, thus already in [must_run]: *)
    let halves =
      Hashtbl.fold (fun _ (rce, get_rc) halves ->
        if match_localhost rce.C.on_hostname then
          halves
        else
          match get_rc () with
          | exception _ ->
              halves
          | prog ->
              (match Services.lookup conf rce.C.on_hostname with
              | exception Not_found ->
                  !logger.error "No host matching %s"
                    (Globs.decompile rce.C.on_hostname) ;
                  halves
              | tunnelds ->
                  List.fold_left (fun halves func ->
                    let parents, children = relatives func must_run in
                    (* Add a bottom half for each function with at least
                     * one child running locally: *)
                    let halves =
                      List.fold_left (fun halves ((cmre : must_run_entry), _) ->
                        let k = key_of cmre Whole in
                        match Hashtbl.find must_run k with
                        | exception Not_found -> halves
                        | mre when mre.part <> Whole ->
                            (* Should not happen yet as we've stored only
                             * local functions in must_run so far *)
                            halves
                        | _ ->
                          !logger.info "Child %a/%a of remote %a/%a \
                                        is running locally!"
                            N.program_print cmre.func.F.program_name
                            N.func_print cmre.func.F.name
                            N.program_print func.F.program_name
                            N.func_print func.F.name ;
                          { rce ; prog ; func ; part = BottomHalf }
                            :: halves
                      ) halves children in
                    (* We need a top half for each individual parent
                     * running here (if we had only one for all then
                     * we would have to deal with merging/sorting): *)
                    List.fold_left (fun halves ((pmre : must_run_entry), parent_num) ->
                      let k = key_of pmre Whole in
                      match Hashtbl.find must_run k with
                      | exception Not_found -> halves
                      | mre when mre.part <> Whole ->
                          (* Should not happen yet as we've stored only
                           * local functions in must_run so far *)
                          halves
                      | _ ->
                        !logger.info "Parent %a/%a of remote %a/%a \
                                      (host %a) is running locally!"
                          N.program_print pmre.func.F.program_name
                          N.func_print pmre.func.F.name
                          N.program_print func.F.program_name
                          N.func_print func.F.name
                          (List.print Services.print_entry) tunnelds ;
                        let part =
                          TopHalf (
                            List.map (fun t ->
                              { tunneld_host = t.Services.host ;
                                tunneld_port = t.Services.port ;
                                parent_num }
                            ) tunnelds) in
                        { rce ; prog ; func ; part } :: halves
                    ) halves parents
                  ) halves prog.P.funcs)
      ) programs [] in
    (* Actually add the bottom and top halves into must_run: *)
    List.iter (fun (mre : must_run_entry) ->
      let k = key_of mre mre.part in
      Hashtbl.modify_opt k (function
        | None -> Some mre
        | Some prev ->
            (match prev.part, mre.part with
            | TopHalf lst1, TopHalf lst2 ->
                Some { prev with part = TopHalf (lst1 @ lst2) }
            | BottomHalf, BottomHalf ->
                (* Several local children of the same remote parent are OK *)
                Some prev
            | part1, part2 ->
                !logger.error "Cannot run both a %a and a %a in \
                               the same host for %a"
                  print_worker_part part1
                  print_worker_part part2
                  print_must_run_entry mre ;
                assert false)
      ) must_run
    ) halves
  in
  (* The hash of programs that must be running, updated by [loop]: *)
  let must_run = Hashtbl.create 307
  and running = Hashtbl.create 307 in
  let rec loop last_read =
    (* Once we have forked some workers we must not allow an exception to
     * terminate this function or we'd leave unsupervised workers behind: *)
    restart_on_failure "process supervisor" (fun () ->
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
              try Some (Files.mtime rc_file)
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
              build_must_run must_run conf ;
              now
            ) else last_read) in
        let changed = synchronize must_run running in
        (* Touch the rc file if anything changed (esp. autoreload) since that
         * mtime is used to signal cache expirations etc. *)
        if changed then Files.touch rc_file last_read ;
        Gc.minor () ;
        let delay = if !quit = None then 1. else 0.3 in
        Unix.sleepf delay ;
        RamenWatchdog.reset watchdog ;
        loop last_read)) ()
  in
  RamenWatchdog.enable watchdog ;
  loop 0. ;
  RamenWatchdog.disable watchdog
