(*
 * Process supervisor task is to start and stop workers, connect them
 * properly and make sure they keep working.
 *)
open Batteries
open RamenLog
open RamenHelpers
open RamenConsts
open RamenSyncHelpers
open RamenSync
module C = RamenConf
module RC = C.Running
module F = C.Func
module FS = F.Serialized
module P = C.Program
module PS = P.Serialized
module E = RamenExpr
module O = RamenOperation
module N = RamenName
module T = RamenTypes
module OutRef = RamenOutRef
module Files = RamenFiles
module Services = RamenServices
module Replay = RamenReplay
module Processes = RamenProcesses
module Channel = RamenChannel
module FuncGraph = RamenFuncGraph
module TimeRange = RamenTimeRange
module ZMQClient = RamenSyncZMQClient

(* Seed to pass to workers to init their random generator: *)
let rand_seed = ref None

(* We use the same key for both [must_run] and [running] to make the
 * comparison easier, although strictly speaking the signature is useless
 * in [must_run]: *)
type key =
  { program_name : N.program ;
    func_name : N.func ;
    func_signature : string ;
    params : RamenTuple.params ;
    role : Value.Worker.role }

(* What we store in the [must_run] hash: *)
type must_run_entry =
  { key : key ;
    rce : RC.entry ;
    func : F.t ;
    (* Actual workers not only logical parents as in func.parent: *)
    parents : (N.site * P.t * F.t) list }

let print_must_run_entry oc mre =
  Printf.fprintf oc "%a/%a"
    N.program_print mre.key.program_name
    N.func_print mre.key.func_name

(* Description of a running worker that is stored in the [running] hash.
 * Not persisted on disk.
 * Some of this is updated as the configuration change. *)
type running_process =
  { key : key ;
    params : RamenParams.t ; (* The ones in RCE only! *)
    bin : N.path ;
    func : F.t ;
    parents : (N.site * P.t * F.t) list ;
    children : F.t list ;
    log_level : log_level ;
    report_period : float ;
    mutable pid : int option ;
    last_killed : float ref (* 0 for never *) ;
    mutable continued : bool ;
    (* purely for reporting: *)
    mutable last_exit : float ;
    mutable last_exit_status : string ;
    mutable succ_failures : int ;
    mutable quarantine_until : float }

let print_running_process oc proc =
  Printf.fprintf oc "%s/%s (%a) (parents=%a)"
    (proc.func.F.program_name :> string)
    (proc.func.F.name :> string)
    Value.Worker.print_role proc.key.role
    (List.print F.print_parent) proc.func.parents

let make_running_process conf must_run mre =
  let log_level =
    if mre.rce.RC.debug then Debug else conf.C.log_level in
  (* All children running locally, including top-halves: *)
  let children =
    Hashtbl.fold (fun _ (mre' : must_run_entry) children ->
      List.fold_left (fun children (_h, _pprog, pfunc) ->
        if pfunc == mre.func then
          mre'.func :: children
        else
          children
      ) children mre.parents
    ) must_run [] in
  { key = mre.key ;
    params = mre.rce.RC.params ;
    bin = mre.rce.RC.bin ;
    func = mre.func ;
    parents = mre.parents ;
    children ;
    log_level ;
    report_period = mre.rce.RC.report_period ;
    pid = None ; last_killed = ref 0. ; continued = false ;
    last_exit = 0. ; last_exit_status = "" ; succ_failures = 0 ;
    quarantine_until = 0. }

(* A single worker can replay for several channels. This is very useful
 * when a dashboard reloads with many graphs requesting the same time interval.
 * So replayers are aggregated for a little while before spawning them.
 * No need to persist this hash though. *)

type replayers = (C.Replays.site_fq, replayer) Hashtbl.t

and replayer =
  { (*site_fq : C.Replays.site_fq ;*)
    (* Aggregated from all replays. Won't change once the replayer is
     * spawned. *)
    mutable time_range : TimeRange.t ;
    (* Used to count the end of retransmissions: *)
    id : int ;
    (* Actual process is spawned only a bit later: *)
    creation : float ;
    (* Set when the replayer has started and then always set.
     * Until then new channels can be added. *)
    mutable pid : int option ;
    (* When the replayer actually stopped: *)
    mutable stopped : bool ;
    last_killed : float ref ;
    (* What running replays are using this process.
     * Can be killed/deleted when this drops to zero. *)
    mutable replays : (Channel.t, C.Replays.entry) Map.t }

let make_replayer now =
  { time_range = TimeRange.empty ;
    id = Random.int RingBufLib.max_replayer_id ;
    creation = now ;
    pid = None ;
    stopped = false ;
    last_killed = ref 0. ;
    replays = Map.empty }

open Binocle

let stats_worker_crashes =
  RamenWorkerStats.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.worker_crashes
      "Number of workers that have crashed (or exited with non 0 status).")

let stats_worker_deadloopings =
  RamenWorkerStats.ensure_inited (fun save_dir ->
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
  RamenWorkerStats.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.ringbuf_repairs
      "Number of times a worker ringbuf had to be repaired.")

let stats_outref_repairs =
  RamenWorkerStats.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.outref_repairs
      "Number of times a worker outref had to be repaired.")

let stats_worker_sigkills =
  RamenWorkerStats.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.worker_sigkills
      "Number of times a worker had to be sigkilled instead of sigtermed.")

let stats_replayer_crashes =
  RamenWorkerStats.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.replayer_crashes
      "Number of replayers that have crashed (or exited with non 0 status).")

let stats_replayer_sigkills =
  RamenWorkerStats.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.replayer_sigkills
      "Number of times a replayer had to be sigkilled instead of sigtermed.")

let stats_chans_per_replayer =
  RamenWorkerStats.ensure_inited (fun save_dir ->
    Histogram.make ~save_dir:(save_dir :> string)
      Metric.Names.chans_per_replayer
      "Number of channels per replayer."
      (Histogram.linear_buckets 1.))

(* Many messages that are exceptional in supervisor are quite expected in tests: *)
let info_or_test conf =
  if conf.C.test then !logger.debug else !logger.info

(* When a worker seems to crashloop, assume it's because of a bad file and
 * delete them! *)
let rescue_worker fq state_file input_ringbufs out_ref =
  (* Maybe the state file is poisoned? At this stage it's probably safer
   * to move it away: *)
  !logger.info "Worker %a is deadlooping. Deleting its state file, \
                input ringbuffers and out_ref."
    N.fq_print fq ;
  Files.move_aside state_file ;
  (* At this stage there should be no writers since this worker is stopped. *)
  List.iter Files.move_aside input_ringbufs ;
  Files.move_aside out_ref

(* TODO: workers should monitor the conftree for change in children
 * and need no out_ref any longer *)
let cut_from_parents_outrefs input_ringbufs out_refs =
  List.iter (fun parent_out_ref ->
    List.iter (fun this_in ->
      OutRef.remove parent_out_ref this_in Channel.live
    ) input_ringbufs
  ) out_refs

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
          let input_ringbufs = C.in_ringbuf_names conf proc.func in
          if is_err then (
            proc.succ_failures <- proc.succ_failures + 1 ;
            IntCounter.inc (stats_worker_crashes conf.C.persist_dir) ;
            if proc.succ_failures = 5 then (
              IntCounter.inc (stats_worker_deadloopings conf.C.persist_dir) ;
              let fq = F.fq_name proc.func in
              let params_sign = RamenParams.signature proc.params in
              let state_file = C.worker_state conf proc.func params_sign in
              let out_ref = C.out_ringbuf_names_ref conf proc.func in
              rescue_worker fq state_file input_ringbufs out_ref)
          ) else (
            proc.succ_failures <- 0
          ) ;
          (* In case the worker stopped on its own, remove it from its
           * parents out_ref: *)
          let out_refs =
            List.map (fun (_, _, pfunc) ->
              C.out_ringbuf_names_ref conf pfunc
            ) proc.parents in
          cut_from_parents_outrefs input_ringbufs out_refs ;
          (* Wait before attempting to restart a failing worker: *)
          let max_delay = float_of_int proc.succ_failures in
          proc.quarantine_until <-
            now +. Random.float (min 90. max_delay) ;
          proc.pid <- None)
    ) proc.pid
  ) running

let process_replayers_start_stop get_bin_func conf now replayers =
  let open Unix in
  Hashtbl.filteri_inplace (fun (_, fq as site_fq) replayer ->
    match replayer.pid with
    | None ->
        (* Maybe start it? *)
        if now -. replayer.creation > delay_before_replay then (
          if
            Map.is_empty replayer.replays ||
            TimeRange.is_empty replayer.time_range
          then (
            (* Do away with it *)
            false
          ) else (
            let channels = Map.keys replayer.replays |> Set.of_enum
            and since, until = TimeRange.bounds replayer.time_range
            and bin, func = get_bin_func fq in
            !logger.info
              "Starting a %a replayer created %gs ago for channels %a"
              N.fq_print fq
              (now -. replayer.creation)
              (Set.print Channel.print) channels ;
            match Replay.spawn_source_replay
                    conf func bin since until channels replayer.id with
            | exception e ->
                let what =
                  Printf.sprintf2 "spawning replayer %d for channels %a"
                    replayer.id
                    (Set.print Channel.print) channels in
                print_exception ~what e ;
                false
            | pid ->
                replayer.pid <- Some pid ;
                Histogram.add (stats_chans_per_replayer conf.C.persist_dir)
                              (float_of_int (Set.cardinal channels)) ;
                true
          )
        ) else true
    | Some pid ->
        if replayer.stopped then
          not (Map.is_empty replayer.replays)
        else
          let what =
            Printf.sprintf2 "Replayer for %a (pid %d)"
              C.Replays.site_fq_print site_fq pid in
          (match restart_on_EINTR (waitpid [ WNOHANG ; WUNTRACED ]) pid with
          | exception exn ->
              !logger.error "%s: waitpid: %s" what (Printexc.to_string exn)
              (* assume the replayers is safe *)
          | 0, _ ->
              () (* Nothing to report *)
          | _, (WSIGNALED s | WSTOPPED s) when s = Sys.sigstop ->
              !logger.debug "%s got stopped" what
          | _, status ->
              let status_str = string_of_process_status status in
              let is_err =
                status <> WEXITED ExitCodes.terminated in
              (if is_err then !logger.error else info_or_test conf)
                "%s %s." what status_str ;
              if is_err then
                IntCounter.inc (stats_replayer_crashes conf.C.persist_dir) ;
              replayer.stopped <- true) ;
          true
  ) replayers

let start_worker
      conf func params envvars role log_level report_period worker_instance
      bin parent_links children input_ringbufs (state_file : N.path)
      out_ringbuf_ref =
  (* Create the input ringbufs.
   * Workers are started one by one in no particular order.
   * The input and out-ref ringbufs are created when the worker start, and the
   * out-ref is filled with the running (or should be running) children.
   * Therefore, if a children fails to run the parents might block.
   * Also, if a child has not been started yet its inbound ringbuf will not exist,
   * again implying this worker will block.
   * Each time a new worker is started or stopped the parents outrefs are updated. *)
  let fq = F.fq_name func in
  let fq_str = (fq :> string) in
  !logger.debug "Creating in buffers..." ;
  List.iter (fun rb_name ->
    RingBuf.create rb_name ;
    let rb = RingBuf.load rb_name in
    finally (fun () -> RingBuf.unload rb)
      (fun () ->
        Processes.repair_and_warn fq_str rb ;
        IntCounter.inc (stats_ringbuf_repairs conf.C.persist_dir)) ()
  ) input_ringbufs ;
  (* And the pre-filled out_ref: *)
  Option.may (fun out_ringbuf_ref ->
    !logger.debug "Updating out-ref buffers..." ;
    List.iter (fun cfunc ->
      let fname = C.input_ringbuf_fname conf func cfunc
      and fieldmask = F.make_fieldmask func cfunc in
      (* The destination ringbuffer must exist before it's referenced in an
       * out-ref, or the worker might err and throw away the tuples: *)
      RingBuf.create fname ;
      OutRef.add out_ringbuf_ref fname fieldmask
    ) children ;
  ) out_ringbuf_ref ;
  (* Export for a little while at the beginning (help with both automatic
   * and manual tests): *)
  if not (Value.Worker.is_top_half role) then (
    Processes.start_export
      ~duration:conf.initial_export_duration conf func |>
    ignore
  ) ;
  (* Now actually start the binary *)
  let notify_ringbuf =
    (* Where that worker must write its notifications. Normally toward a
     * ringbuffer that's read by Ramen, unless it's a test programs.
     * Tests must not send their notifications to Ramen with real ones,
     * but instead to a ringbuffer specific to the test_id. *)
    C.notify_ringbuf conf in
  let ocamlrunparam =
    let def = if log_level = Debug then "b" else "" in
    getenv ~def "OCAMLRUNPARAM" in
  let env = [|
    "OCAMLRUNPARAM="^ ocamlrunparam ;
    "log_level="^ string_of_log_level log_level ;
    (* To know what to log: *)
    "is_test="^ string_of_bool conf.C.test ;
    (* Select the function to be executed: *)
    "name="^ (func.F.name :> string) ;
    "fq_name="^ fq_str ; (* Used for monitoring *)
    "instance="^ worker_instance ;
    "report_ringbuf="^ (C.report_ringbuf conf :> string) ;
    "report_period="^ string_of_float report_period ;
    "notify_ringbuf="^ (notify_ringbuf :> string) ;
    "rand_seed="^ (match !rand_seed with None -> ""
                  | Some s -> string_of_int s) ;
    "site="^ (conf.C.site :> string) ;
    (match !logger.output with
      | Directory _ ->
        let dir = N.path_cat [ conf.C.persist_dir ; N.path "log/workers" ;
                               F.path func ] in
        "log="^ (dir :> string)
      | Stdout -> "no_log" (* aka stdout/err *)
      | Syslog -> "log=syslog") |] in
  let more_env =
    match role with
    | Whole ->
        List.enum
          [ "output_ringbufs_ref="^ (Option.get out_ringbuf_ref :> string) ;
            (* We need to change this dir whenever the func signature or
             * params change to prevent it to reload an incompatible state *)
            "state_file="^ (state_file :> string) ;
            "factors_dir="^ (C.factors_of_function conf func :> string) ]
    | TopHalf ths ->
        Enum.append
          (List.enum ths |>
          Enum.mapi (fun i th ->
            "tunneld_host_"^ string_of_int i ^"="^
              (th.Value.Worker.tunneld_host :> string)))
          (List.enum ths |>
          Enum.mapi (fun i th ->
            "tunneld_port_"^ string_of_int i ^"="^
              string_of_int th.Value.Worker.tunneld_port)) |>
        Enum.append
          (List.enum ths |>
          Enum.mapi (fun i th ->
            "parent_num_"^ string_of_int i ^"="^
              string_of_int th.Value.Worker.parent_num)) in
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
    P.env_of_params_and_exps conf params |>
    Enum.append more_env in
  (* Also add all envvars that are defined and used in the operation: *)
  let more_env =
    List.enum envvars //@
    (function
      | (n : N.field), Some v -> Some ((n :> string) ^"="^ v)
      | _, None -> None) |>
    Enum.append more_env in
  (* Workers must be given the address of a config-server: *)
  let more_env =
    List.enum
      [ "sync_url="^ conf.C.sync_url ;
        "sync_srv_pub_key="^ conf.C.srv_pub_key ;
        "sync_username=_worker "^ (conf.C.site :> string) ^"/"^ fq_str ;
        "sync_clt_pub_key="^ conf.C.clt_pub_key ;
        "sync_clt_priv_key="^ conf.C.clt_priv_key ] |>
    Enum.append more_env in
  let env = Array.append env (Array.of_enum more_env) in
  let args =
    [| if role = Whole then Worker_argv0.full_worker
                       else Worker_argv0.top_half ;
       fq_str |] in
  let pid = Processes.run_worker ~and_stop:conf.C.test bin args env in
  !logger.debug "%a for %a now runs under pid %d"
    Value.Worker.print_role role N.fq_print fq pid ;
  (* Update the parents out_ringbuf_ref: *)
  List.iter (fun (out_ref, in_ringbuf, fieldmask) ->
    OutRef.add out_ref in_ringbuf fieldmask
  ) parent_links ;
  pid

let input_ringbufs conf func role =
  if Value.Worker.is_top_half role then
    [ C.in_ringbuf_name_single conf func ]
  else
    C.in_ringbuf_names conf func

let really_start conf proc =
  let envvars =
    O.envvars_of_operation proc.func.operation |>
    List.map (fun (n : N.field) ->
      n, Sys.getenv_opt (n :> string))
  in
  let input_ringbufs =
    input_ringbufs conf proc.func proc.key.role
  and state_file =
    C.worker_state conf proc.func (RamenParams.signature proc.params)
  and out_ringbuf_ref =
    if Value.Worker.is_top_half proc.key.role then None
    else Some (C.out_ringbuf_names_ref conf proc.func)
  and parent_links =
    List.map (fun (_, _, pfunc) ->
      C.out_ringbuf_names_ref conf pfunc,
      C.input_ringbuf_fname conf pfunc proc.func,
      F.make_fieldmask pfunc proc.func
    ) proc.parents in
  let pid =
    start_worker
      conf proc.func proc.params envvars proc.key.role proc.log_level
      proc.report_period "test_instance" proc.bin parent_links
      proc.children input_ringbufs state_file out_ringbuf_ref in
  proc.pid <- Some pid ;
  proc.last_killed := 0.

(* Try to start the given proc.
 * Check links (ie.: do parents and children have the proper types?) *)
let really_try_start conf now proc =
  info_or_test conf "Starting operation %a"
    print_running_process proc ;
  assert (proc.pid = None) ;
  let check_linkage p c =
    let out_type =
      O.out_type_of_operation ~with_private:false p.F.operation in
    try Processes.check_is_subtype c.F.in_type out_type
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
    parents_ok := true ;
    List.iter (fun (_, _, pfunc) ->
      check_linkage pfunc proc.func
    ) proc.parents ;
    List.iter (fun cfunc ->
      check_linkage proc.func cfunc
    ) proc.children ;
    really_start conf proc
  with e ->
    print_exception ~what:"Cannot start worker" e ;
    (* Anything goes wrong when starting a node? Quarantine it! *)
    let delay =
      if not !parents_ok then 20. else 600. in
    proc.quarantine_until <-
      now +. Random.float (max 90. delay)

let try_start conf proc =
  let now = Unix.gettimeofday () in
  if proc.quarantine_until > now then (
    !logger.debug "Operation %a still in quarantine"
      print_running_process proc
  ) else (
    really_try_start conf now proc
  )

let kill_politely conf last_killed what pid stats_sigkills =
  (* Start with a TERM (if last_killed = 0.) and then after 5s send a
   * KILL if the worker is still running. *)
  let now = Unix.gettimeofday () in
  if !last_killed = 0. then (
    (* Ask politely: *)
    info_or_test conf "Terminating %s" what ;
    log_exceptions ~what:("Terminating "^ what) (fun () ->
      Unix.kill pid Sys.sigterm) ;
    last_killed := now
  ) else if now -. !last_killed > 10. then (
    !logger.warning "Killing %s with bigger guns" what ;
    log_exceptions ~what:("Killing "^ what) (fun () ->
      Unix.kill pid Sys.sigkill) ;
    last_killed := now ;
    IntCounter.inc (stats_sigkills conf.C.persist_dir)
  )

let try_kill conf pid func parents last_killed =
  (* There is no reason to wait before we remove this worker from its
   * parent out-ref: if it's not replaced then the last unprocessed
   * tuples are lost. If it's indeed a replacement then the new version
   * will have a chance to process the left overs. *)
  let input_ringbufs = C.in_ringbuf_names conf func in
  let out_refs =
    List.map (fun (_, _, pfunc) ->
      C.out_ringbuf_names_ref conf pfunc
    ) parents in
  cut_from_parents_outrefs input_ringbufs out_refs ;
  (* If it's still stopped, unblock first: *)
  log_and_ignore_exceptions ~what:"Continuing worker (before kill)"
    (Unix.kill pid) Sys.sigcont ;
  let what = Printf.sprintf2 "worker %a (pid %d)"
               N.fq_print (F.fq_name func) pid in
  kill_politely conf last_killed what pid stats_worker_sigkills

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
  Hashtbl.iter (fun _ (proc : running_process) ->
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
              OutRef.remove out_ref fname Channel.live) ())
      ) outs ;
      (* Conversely, check that all children are in the out_ref of their
       * parent: *)
      let in_rbs = C.in_ringbuf_names conf proc.func |> Set.of_list in
      List.iter (fun (_, _, pfunc) ->
        let out_ref = C.out_ringbuf_names_ref conf pfunc in
        let outs = OutRef.read_live out_ref in
        let outs = Hashtbl.keys outs |> Set.of_enum in
        if Set.disjoint in_rbs outs then (
          !logger.error "Operation %a must output to %a but does not, fixing"
            N.fq_print (F.fq_name pfunc)
            print_running_process proc ;
          log_and_ignore_exceptions ~what:("fixing "^(out_ref :> string))
            (fun () ->
              let fname = C.input_ringbuf_fname conf pfunc proc.func
              and fieldmask = F.make_fieldmask pfunc proc.func in
              OutRef.add out_ref fname fieldmask) ())
      ) proc.parents
    )
  ) running

let signal_all_cont running =
  Hashtbl.iter (fun _ (proc : running_process) ->
    if proc.pid <> None && not proc.continued then (
      proc.continued <- true ;
      !logger.debug "Signaling %a to continue" print_running_process proc ;
      log_and_ignore_exceptions ~what:"Signaling worker to continue"
        (Unix.kill (Option.get proc.pid)) Sys.sigcont) ;
  ) running

(* Build the list of workers that must run on this site.
 * Start by getting a list of sites and then populate it with all workers
 * that must run there, with for each the set of their parents.
 *
 * Note: only used by RamenTests. *)

let build_must_run conf programs =
  let graph = FuncGraph.make conf programs in
  !logger.debug "Graph of functions: %a" FuncGraph.print graph ;
  (* Now building the hash of functions that must run from graph is easy: *)
  let must_run =
    match Hashtbl.find graph.h conf.C.site with
    | exception Not_found ->
        Hashtbl.create 0
    | h ->
        Hashtbl.enum h //@
        (fun (_, ge) ->
          if ge.used then
            (* Use the mount point + signature + params as the key.
             * Notice that we take all the parameter values (from
             * prog.params), not only the explicitly set values (from
             * rce.params), so that if a default value that is
             * unset is changed in the program then that's considered a
             * different program. *)
            let parents =
              Set.fold (fun (ps, pp, pf) lst ->
                (* Guaranteed to be in the graph: *)
                let pge = FuncGraph.find graph ps pp pf in
                (ps, pge.prog, pge.func) :: lst
              ) ge.parents [] in
            let key =
              { program_name = ge.func.F.program_name ;
                func_name = ge.func.F.name ;
                func_signature = ge.func.F.signature ;
                params = ge.prog.P.default_params ;
                role = Whole } in
            let v =
              { key ; rce = ge.rce ; func = ge.func ; parents } in
            Some (key, v)
          else
            None) |>
        Hashtbl.of_enum in
  !logger.debug "%d workers must run in full" (Hashtbl.length must_run) ;
  (* We need a top half for every function running locally with a child
   * running remotely.
   * If we shared the same top-half for several local parents, then we would
   * have to deal with merging/sorting in the top-half.
   * If we have N such children with the same program name/func names and
   * signature (therefore the same WHERE filter) then we need only one
   * top-half. *)
  let top_halves = Hashtbl.create 10 in
  Hashtbl.iter (fun site h ->
    if site <> conf.C.site then
      Hashtbl.iter (fun _ ge ->
        if ge.FuncGraph.used then
          (* Then parent will be used as well, no need to check *)
          Set.iter (fun (ps, pp, pf) ->
            if ps = conf.C.site then
              let service = ServiceNames.tunneld in
              match Services.resolve conf site service with
              | exception Not_found ->
                  !logger.error "No service matching %a:%a"
                    N.site_print site
                    N.service_print service
              | srv ->
                  let k =
                    (* local part *)
                    pp, pf,
                    (* remote part *)
                    ge.FuncGraph.func.F.program_name, ge.func.F.name,
                    ge.func.F.signature, ge.prog.P.default_params in
                  (* We could meet several times the same func on different
                   * sites, but that would be the same rce and func! *)
                  Hashtbl.modify_opt k (function
                    | Some (rce, func, srvs) ->
                        Some (rce, func, (srv :: srvs))
                    | None ->
                        Some (ge.rce, ge.func, [ srv ])
                  ) top_halves
          ) ge.parents
      ) h
  ) graph.h ;
  let l = Hashtbl.length top_halves in
  if l > 0 then !logger.info "%d workers must run in half" l ;
  (* Now that we have aggregated all top-halves children, actually adds them
   * to [must_run]: *)
  Hashtbl.iter (fun (pp, pf, cprog, cfunc, sign, params)
                    (rce, func, tunnelds) ->
    let role =
      Value.Worker.TopHalf (
        List.mapi (fun i tunneld ->
          Value.Worker.{
            tunneld_host = tunneld.Services.host ;
            tunneld_port = tunneld.Services.port ;
            parent_num = i }
        ) tunnelds) in
    let key =
      { program_name = cprog ;
        func_name = cfunc ;
        func_signature = sign ;
        params ; role } in
    let parents =
      let pge = FuncGraph.find graph conf.C.site pp pf in
      [ conf.C.site, pge.prog, pge.func ] in
    let v =
      { key ; rce ; func ; parents } in
    assert (not (Hashtbl.mem must_run key)) ;
    Hashtbl.add must_run key v
  ) top_halves ;
  must_run

(* Have a single watchdog even when supervisor is restarted: *)
let watchdog = ref None

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
(* FIXME: move this all all its subfunctions into RamenTests, last user. *)
let synchronize_workers conf must_run running =
  (* First, remove from running all terminated processes that must not run
   * any longer. Send a kill to those that are still running. *)
  IntGauge.set stats_worker_count (Hashtbl.length must_run) ;
  IntGauge.set stats_worker_running (Hashtbl.length running) ;
  let to_kill = ref [] and to_start = ref []
  and (+=) r x = r := x :: !r in
  Hashtbl.filteri_inplace (fun k (proc : running_process) ->
    if Hashtbl.mem must_run k then true else
    if proc.pid <> None then (to_kill += proc ; true) else
    false
  ) running ;
  (* Then, add/restart all those that must run. *)
  Hashtbl.iter (fun k (mre : must_run_entry) ->
    match Hashtbl.find running k with
    | exception Not_found ->
        let proc = make_running_process conf must_run mre in
        Hashtbl.add running k proc ;
        to_start += proc
    | proc ->
        (* If it's dead, restart it: *)
        if proc.pid = None then to_start += proc else
        (* If we were killing it, it's safer to keep killing it until it's
         * dead and then restart it. *)
        if !(proc.last_killed) <> 0. then to_kill += proc
  ) must_run ;
  if !to_kill <> [] then !logger.debug "Starting the kills" ;
  List.iter (fun (proc : running_process) ->
    try_kill conf (Option.get proc.pid) proc.func proc.parents proc.last_killed
  ) !to_kill ;
  if !to_start <> [] then !logger.debug "Starting the starts" ;
  (* FIXME: sort it so that parents are started before children,
   * so that in case of linkage error we do not end up with orphans
   * preventing parents to be run. *)
  List.iter (fun proc ->
    try_start conf proc ;
    (* If we had to compile a program this is worth resetting the watchdog: *)
    Option.may RamenWatchdog.reset !watchdog
  ) !to_start ;
  (* Try to fix any issue with out_refs: *)
  let now = Unix.time () in
  if (now > !last_checked_outref +. 30. ||
      now > !last_checked_outref +. 5. && !to_start = [] && !to_kill = []) &&
     !Processes.quit = None
  then (
    last_checked_outref := now ;
    log_and_ignore_exceptions ~what:"checking out_refs"
      (check_out_ref conf must_run) running) ;
  if !to_start = [] && conf.C.test then signal_all_cont running ;
  (* Return if anything changed: *)
  !to_kill <> [] || !to_start <> []

(* Similarly, try to make [replayers] the same as [must_replay] *)
let synchronize_replays conf func_of_fq now must_replay replayers =
  (* Kill the replayers of channels that are not configured any longer,
   * and create new replayers for new channels: *)
  let run_chans =
    Hashtbl.fold (fun _ r m ->
      Map.union r.replays m
    ) replayers Map.empty
  and def_chans =
    Hashtbl.fold Map.add must_replay Map.empty in
  let kill_replayer r =
    match r.pid with
    | None ->
        !logger.debug "Replay stopped before replayer even started"
    | Some pid ->
        if not r.stopped then
          let what = Printf.sprintf2 "replayer %d (pid %d)" r.id pid in
          (try
            kill_politely conf r.last_killed what pid stats_replayer_sigkills
          with Unix.(Unix_error (ESRCH, _, _)) ->
            () (* Have been logged already *))
  in
  let to_rem chan replay =
    (* Remove this chan from all replayers. If a replayer has no more
     * channels left and has a pid, then kill it (and warn). But first,
     * tear down the channel: *)
    Replay.teardown_links conf func_of_fq replay ;
    Hashtbl.iter (fun _ r ->
      if not (Map.is_empty r.replays) then (
        r.replays <- Map.remove chan r.replays ;
        if Map.is_empty r.replays then kill_replayer r)
    ) replayers in
  let to_add chan replay =
    let replay_range =
      TimeRange.make replay.C.Replays.since replay.until false in
    Replay.settup_links conf func_of_fq replay ;
    (* Find or create all replayers: *)
    List.iter (fun (site, _ as site_fq) ->
      if site = conf.C.site then (
        let rs = Hashtbl.find_all replayers site_fq in
        let r =
          try
            List.find (fun r ->
              r.pid = None &&
              TimeRange.approx_eq replay_range r.time_range
            ) rs
          with Not_found ->
            let r = make_replayer now in
            Hashtbl.add replayers site_fq r ;
            r in
        !logger.debug
          "Adding replay for channel %a into replayer created at %a"
          Channel.print chan print_as_date r.creation ;
        r.time_range <-
          TimeRange.merge r.time_range replay_range ;
        r.replays <- Map.add chan replay r.replays)
    ) replay.C.Replays.sources
  in
  Map.diff run_chans def_chans |> Map.iter to_rem ;
  Map.diff def_chans run_chans |> Map.iter to_add

(* Using the conf server to supervise the workers goes in several stages:
 *
 * First, client tools such as `ramen run`, `ramen kill`, `ramen replay`
 * or rmadmin, write the whole TargetConfig value into the TargetConfig key
 * (alternatively, into the RC file and then filesyncer copy it into
 * TargetConfig).
 *
 * Then, the choregrapher react to that change, recompute the FuncGraph
 * and expose it back on the configuration, into the per site / per
 * worker configuration tree. In particular, the presence of the IsUsed
 * key (be it true or false) tells that a worker must run (Archivist and
 * others might also store per worker info for non-running workers).
 *
 * Finally, the supervisor will react to the exposed FuncGraph for the
 * site it's in charge of, and try to make the running configuration for the
 * local site comply with it. In turn, it will expose the actual state
 * into the per site / per worker configuration tree.
 *
 * The only issue is that when a process state is written, it cannot be
 * read back before a round-trip to the conf server ; an issue that will not
 * cause great harm and that should be alleviated entirely later when the
 * confclient writes in its local hash (at least optionally) the values that
 * it sets.
 *
 * A process is running if its pid is stored (as an int) in the worker
 * info. If not, and there is a binary (a string), then spawn it. If there
 * is no binary, compile the source (ref to the info, as well as its md5,
 * found in the per site config). If there is no such info, wait for the
 * choreographer to finish its job.
 *
 * When a process has a pid but must not be running, send a kill (which signal
 * depends on the last killed info also in the config tree).
 *
 * When a children terminates, also update the config tree (remove the pid,
 * write the last exit status).
 *
 * Info regarding former workers that are no longer running are kept until
 * it becomes clear when to delete them. Maybe a specific command, equivalent
 * to the `ramen kill --purge`?
 *)
let per_instance_key site fq worker_sign k =
  Key.PerSite (site, PerWorker (fq, PerInstance (worker_sign, k)))

let find_or_fail what clt k f =
  let v =
    match Client.find clt k with
    | exception Not_found ->
        None
    | hv ->
        Some hv.value in
  match f v with
  | None ->
      (match v with
      | None ->
          Printf.sprintf2 "Cannot find %s: no such key %a"
            what
            Key.print k |>
          failwith
      | Some v ->
          invalid_sync_type k v what)
  | Some v' ->
      v'

let get_string = function
  | Some (Value.RamenValue (T.VString s)) -> Some s
  | _ -> None

let get_string_list =
  let is_string = function
    | T.VString _ -> true
    | _ -> false in
  function
  | Some (Value.RamenValue (T.VList vs))
    when Array.for_all is_string vs ->
      Array.enum vs /@
      (function VString s -> N.path s | _ -> assert false) |>
      List.of_enum |>
      Option.some
  | _ -> None

let report_worker_death ~while_ clt site fq worker_sign status_str =
  let per_instance_key = per_instance_key site fq worker_sign in
  let now = Unix.gettimeofday () in
  ZMQClient.send_cmd ~while_ ~eager:true
    (SetKey (per_instance_key LastExit,
             Value.of_float now)) ;
  ZMQClient.send_cmd ~while_ ~eager:true
    (SetKey (per_instance_key LastExitStatus,
             Value.of_string status_str)) ;
  let input_ringbufs =
    let k = per_instance_key InputRingFiles in
    find_or_fail "a list of strings" clt k get_string_list in
  (* In case the worker stopped on its own, remove it from its
   * parents out_ref. Temporary. The out_refs were stored in the conftree,
   * but ideally the workers listen to children directly, no more need for
   * out_ref files: *)
  let out_refs =
    let k = per_instance_key ParentOutRefs in
    find_or_fail "a list of strings" clt k get_string_list in
  cut_from_parents_outrefs input_ringbufs out_refs ;
  ZMQClient.send_cmd ~while_ ~eager:true
    (DelKey (per_instance_key Pid))

let update_child_status conf ~while_ clt site fq worker_sign pid =
  let per_instance_key = per_instance_key site fq worker_sign in
  let what = Printf.sprintf2 "Worker %a (pid %d)" N.fq_print fq pid in
  (match Unix.(restart_on_EINTR (waitpid [ WNOHANG ; WUNTRACED ])) pid with
  | exception exn ->
      !logger.error "%s: waitpid: %s" what (Printexc.to_string exn)
  | 0, _ -> () (* Nothing to report *)
  | _, (WSIGNALED s | WSTOPPED s) when s = Sys.sigstop ->
      !logger.debug "%s got stopped" what
  | _, status ->
      let status_str = string_of_process_status status in
      let is_err = status <> WEXITED ExitCodes.terminated in
      (if is_err then !logger.error else info_or_test conf)
        "%s %s." what status_str ;
      let succ_fail_k =
        per_instance_key SuccessiveFailures in
      let succ_failures =
        find_or_fail "an integer" clt succ_fail_k (function
          | None ->
              Some 0
          | Some (Value.RamenValue T.(VI64 i)) ->
              Some (Int64.to_int i)
          | _ ->
              None) in
      if is_err then (
        ZMQClient.send_cmd ~while_ ~eager:true
          (SetKey (succ_fail_k,
                   Value.of_int (succ_failures + 1))) ;
        IntCounter.inc (stats_worker_crashes conf.C.persist_dir) ;
        if succ_failures = 5 then (
          IntCounter.inc (stats_worker_deadloopings conf.C.persist_dir) ;
          let state_file =
            let k = per_instance_key StateFile in
            find_or_fail "a string" clt k get_string
          and out_ref =
            let k = per_instance_key OutRefFile in
            find_or_fail "a string" clt k get_string
          and input_ringbufs =
            let k = per_instance_key InputRingFiles in
            find_or_fail "a list of strings" clt k get_string_list in
          rescue_worker fq (N.path state_file) input_ringbufs
                        (N.path out_ref))
      ) ;
      (* Wait before attempting to restart a failing worker: *)
      let max_delay = float_of_int succ_failures in
      let now = Unix.gettimeofday () in
      let quarantine_until = now +. Random.float (min 90. max_delay) in
      ZMQClient.send_cmd ~while_ ~eager:true
        (SetKey (per_instance_key QuarantineUntil,
                 Value.of_float quarantine_until)) ;
      report_worker_death ~while_ clt site fq worker_sign status_str)

(* This worker is running. Should it? *)
let should_run clt site fq worker_sign =
  let k = Key.PerSite (site, PerWorker (fq, Worker)) in
  match (Client.find clt k).value with
  | exception Not_found -> false
  | Value.Worker worker ->
      worker.enabled && worker.worker_signature = worker_sign
  | v ->
      invalid_sync_type k v "a worker"

let may_kill conf ~while_ clt site fq worker_sign pid =
  let per_instance_key = per_instance_key site fq worker_sign in
  let last_killed_k = per_instance_key LastKilled in
  let prev_last_killed =
    find_or_fail "a float" clt last_killed_k (function
      | None -> Some 0.
      | Some (Value.RamenValue T.(VFloat t)) -> Some t
      | _ -> None) in
  let last_killed = ref prev_last_killed in
  let input_ringbufs =
    let k = per_instance_key InputRingFiles in
    find_or_fail "a list of strings" clt k get_string_list in
  let out_refs =
    let k = per_instance_key ParentOutRefs in
    find_or_fail "a list of strings" clt k get_string_list in
  cut_from_parents_outrefs input_ringbufs out_refs ;
  let no_more_child () =
    (* The worker vanished. Can happen in case of bugs (such as
     * https://github.com/rixed/ramen/issues/789). Must keep
     * calm when that happen: *)
    !logger.error "Worker pid %d has vanished" pid ;
    report_worker_death ~while_ clt site fq worker_sign "vanished" ;
    last_killed := Unix.gettimeofday () in
  let what = Printf.sprintf2 "Killing worker %a (pid %d)" N.fq_print fq pid in
  log_and_ignore_exceptions ~what (fun () ->
    try
      (* Before killing him, let's wake him up: *)
      Unix.kill pid Sys.sigcont ;
      kill_politely conf last_killed what pid stats_worker_sigkills
    with Unix.(Unix_error (ESRCH, _, _)) -> no_more_child ()
  ) () ;
  if !last_killed <> prev_last_killed then
    ZMQClient.send_cmd ~while_ ~eager:true
      (SetKey (last_killed_k, Value.of_float !last_killed))

(* This worker is considered running as soon as it has a pid: *)
let is_running clt site fq worker_sign =
  Client.(H.mem clt.h (per_instance_key site fq worker_sign Pid))

let get_precompiled clt src_path =
  let source_k = Key.Sources (src_path, "info") in
  match Client.find clt source_k with
  | exception Not_found ->
      Printf.sprintf2 "No such source %a"
        Key.print source_k |>
      failwith
  | { value = Value.SourceInfo
                ({ detail = Compiled compiled ; _ } as info) ;
      mtime ; _ } ->
      info, mtime, compiled
  | { value = Value.SourceInfo
                { detail = Failed { err_msg } } ; _ } ->
      Printf.sprintf2 "Compilation failed: %s"
        err_msg |>
      failwith
  | hv ->
      invalid_sync_type source_k hv.value "a source info"

(* [bin_sign] is the signature of the subset of info that affects the
 * executable (ie. not the src_ext/md5 fields) *)
let get_bin_file conf clt fq bin_sign info info_mtime =
  let program_name, _func_name = N.fq_parse fq in
  let get_parent = RamenCompiler.parent_from_confserver clt in
  let info_file = C.supervisor_cache_file conf (N.path bin_sign) "info" in
  let bin_file = C.supervisor_cache_file conf (N.path bin_sign) "x" in
  let info_value = Value.SourceInfo info in
  RamenMake.write_value_into_file info_file info_value info_mtime ;
  RamenMake.(apply_rule conf get_parent program_name info_file bin_file bin_rule) ;
  bin_file

(* First we need to compile (or use a cached of) the source info, that
 * we know from the SourcePath set by the Choreographer.
 * Then for each parent we need the required fieldmask, that the
 * Choreographer also should have set, and update those parents out_ref.
 * Then we can spawn that binary, with the parameters also set by
 * the choreographer. *)
let try_start_instance conf ~while_ clt site fq worker =
  !logger.info "Must start %a for %a"
    Value.Worker.print worker
    N.fq_print fq ;
  let info, info_mtime, precompiled =
    get_precompiled clt worker.Value.Worker.src_path in
  (* Check that info has the proper signature: *)
  let info_sign = Value.SourceInfo.signature info in
  if worker.bin_signature <> info_sign then
    Printf.sprintf "Invalid signature for info: expected %S but got %S"
      worker.bin_signature info_sign |>
    failwith ;
  let bin_file = get_bin_file conf clt fq worker.bin_signature info info_mtime in
  let func_of_precompiled precompiled pname fname =
    List.find (fun f -> f.FS.name = fname)
      precompiled.PS.funcs |>
    (* Temporarily: *)
    F.unserialized pname in
  let worker_of_ref what ref =
    let fq = N.fq_of_program ref.Value.Worker.program ref.func in
    let k = Key.PerSite (ref.site, PerWorker (fq, Worker)) in
    find_or_fail ("a worker for "^ what) clt k (function
      | Some (Value.Worker w) -> Some w
      | _ -> None) in
  let func_of_ref what ref =
    let worker = worker_of_ref what ref in
    let _info, _info_mtime, precompiled =
      get_precompiled clt worker.Value.Worker.src_path in
    func_of_precompiled precompiled ref.program ref.func in
  let program_name, func_name = N.fq_parse fq in
  let func = func_of_precompiled precompiled program_name func_name in
  let params = hashtbl_of_alist worker.params in
  let children =
    List.map (func_of_ref "child") worker.children in
  let envvars =
    List.map (fun (name : N.field) ->
      name, Sys.getenv_opt (name :> string)
    ) worker.envvars in
  let log_level =
    if worker.debug then Debug else Normal in
  (* Workers use local files/ringbufs which name depends on input and/or
   * output types, operation, etc, and that must be stored alongside the
   * pid for later manipulation since they would not be easy to recompute
   * should the Worker config entry change. Esp, other services might
   * want to know them and, again, would have a hard time recomputing
   * them in the face of a Worker change.
   * Therefore it's much simpler to store those paths in the config tree. *)
  let input_ringbufs = input_ringbufs conf func worker.role
  and state_file =
    N.path_cat
      [ conf.persist_dir ; N.path "workers/states" ;
        N.path RamenVersions.(worker_state ^"_"^ codegen) ;
        N.path Config.version ; worker.src_path ;
        N.path worker.worker_signature ; N.path "snapshot" ]
  and out_ringbuf_ref =
    if Value.Worker.is_top_half worker.role then None
    else Some (C.out_ringbuf_names_ref conf func)
  and parent_links =
    List.map (fun p_ref ->
      let pfunc = func_of_ref "parent" p_ref in
      C.out_ringbuf_names_ref conf pfunc,
      C.input_ringbuf_fname conf pfunc func,
      F.make_fieldmask pfunc func
    ) worker.parents in
  let pid =
    start_worker
      conf func params envvars worker.role log_level worker.report_period
      worker.worker_signature bin_file parent_links children input_ringbufs
      state_file out_ringbuf_ref in
  let per_instance_key = per_instance_key site fq worker.worker_signature in
  let k = per_instance_key LastKilled in
  ZMQClient.send_cmd ~eager:true ~while_ (DelKey k) ;
  let k = per_instance_key Pid in
  ZMQClient.send_cmd ~eager:true ~while_
                     (SetKey (k, Value.(of_int pid))) ;
  let k = per_instance_key StateFile
  and v = Value.(of_string (state_file :> string)) in
  ZMQClient.send_cmd ~eager:true ~while_ (SetKey (k, v)) ;
  Option.may (fun out_ringbuf_ref ->
    let k = per_instance_key OutRefFile
    and v = Value.(of_string out_ringbuf_ref) in
    ZMQClient.send_cmd ~eager:true ~while_ (SetKey (k, v))
  ) (out_ringbuf_ref :> string option) ;
  let k = per_instance_key InputRingFiles
  and v =
    let l = List.enum (input_ringbufs :> string list) /@
            (fun f -> T.VString f) |>
            Array.of_enum in
    Value.(RamenValue (VList l)) in
  ZMQClient.send_cmd ~eager:true ~while_ (SetKey (k, v)) ;
  let k = per_instance_key ParentOutRefs
  and v =
    let l = List.enum parent_links /@
            (fun ((f : N.path), _, _) -> T.VString (f :> string)) |>
            Array.of_enum in
    Value.(RamenValue (VList l)) in
  ZMQClient.send_cmd ~eager:true ~while_ (SetKey (k, v))

let remove_dead_chans conf clt ~while_ replayer_k replayer =
  let channels, changed =
    Set.fold (fun chan (channels, changed) ->
      let replay_k = Key.Replays chan in
      if Client.mem clt replay_k then
        Set.add chan channels, changed
      else
        channels, true
    ) replayer.Value.Replayer.channels (Set.empty, false) in
  if changed then
    let last_killed = ref replayer.last_killed in
    if Set.is_empty channels then (
      match replayer.pid with
      | None ->
          !logger.debug "Replays stopped before replayer even started"
      | Some pid ->
          if replayer.exit_status = None then
            let what =
              Printf.sprintf2 "%a (pid %d)"
                Key.print replayer_k pid in
            try
              kill_politely conf last_killed what pid stats_replayer_sigkills
            with Unix.(Unix_error (ESRCH, _, _)) ->
              () (* Have been logged already *)
    ) ;
    let replayer =
      Value.Replayer { replayer with channels ; last_killed = !last_killed } in
    ZMQClient.send_cmd ~while_ ~eager:true
      (UpdKey (replayer_k, replayer))

let update_replayer_status
      conf clt ~while_ now site fq replayer_id replayer_k replayer =
  let rem_replayer () =
    ZMQClient.send_cmd ~while_ (DelKey replayer_k) in
  match replayer.Value.Replayer.pid with
  | None ->
      (* Maybe start it? *)
      if now -. replayer.creation > delay_before_replay then
        if
          Set.is_empty replayer.channels ||
          TimeRange.is_empty replayer.time_range
        then
          (* Do away with it *)
          rem_replayer ()
        else
          let what =
            Printf.sprintf2 "spawning replayer %d for channels %a"
              replayer_id
              (Set.print Channel.print) replayer.channels in
          log_and_ignore_exceptions ~what (fun () ->
            let since, until = TimeRange.bounds replayer.time_range in
            let worker_k =
              Key.PerSite (site, PerWorker (fq, Worker)) in
            let worker =
              match (Client.find clt worker_k).value with
              | Value.Worker worker -> worker
              | v -> invalid_sync_type worker_k v "a Worker" in
            let info_k = Key.Sources (worker.Value.Worker.src_path, "info") in
            let info, mtime =
              match Client.find clt info_k with
              | { value = Value.SourceInfo info ; mtime ; _ } ->
                  info, mtime
              | hv -> invalid_sync_type info_k hv.value "a SourceInfo" in
            let bin =
              get_bin_file conf clt fq worker.bin_signature info mtime in
            let _prog, func = function_of_worker clt fq worker in
            let prog_name, _ = N.fq_parse fq in
            let func = F.unserialized prog_name func in
            !logger.info
              "Starting a %a replayer created %gs ago for channels %a"
              N.fq_print fq
              (now -. replayer.creation)
              (Set.print Channel.print) replayer.channels ;
            let pid =
              Replay.spawn_source_replay
                conf func bin since until replayer.channels replayer_id in
            let v = Value.Replayer { replayer with pid = Some pid } in
            ZMQClient.send_cmd ~while_ ~eager:true
              (UpdKey (replayer_k, v)) ;
            Histogram.add (stats_chans_per_replayer conf.C.persist_dir)
                          (float_of_int (Set.cardinal replayer.channels))
          ) ()
  | Some pid ->
      if replayer.exit_status <> None then (
        if Set.is_empty replayer.channels then rem_replayer ()
      ) else
        let what =
          Printf.sprintf2 "Replayer for %a (pid %d)"
            C.Replays.site_fq_print (site, fq) pid in
        (match Unix.(restart_on_EINTR
                       (waitpid [ WNOHANG ; WUNTRACED ])) pid with
        | exception exn ->
            !logger.error "%s: waitpid: %s" what (Printexc.to_string exn)
            (* assume the replayer is safe *)
        | 0, _ ->
            () (* Nothing to report *)
        | _, (WSIGNALED s | WSTOPPED s) when s = Sys.sigstop ->
            !logger.debug "%s got stopped" what
        | _, status ->
            let status_str = string_of_process_status status in
            let is_err =
              status <> WEXITED ExitCodes.terminated in
            (if is_err then !logger.error else info_or_test conf)
              "%s %s." what status_str ;
            if is_err then
              IntCounter.inc (stats_replayer_crashes conf.C.persist_dir) ;
            let replayer =
              Value.Replayer { replayer with exit_status = Some status_str } in
            ZMQClient.send_cmd ~while_ ~eager:true
              (UpdKey (replayer_k, replayer)))


(* Loop over all keys, which is mandatory to monitor pid terminations,
 * and synchronize running pids with the choreographer output.
 * This is simpler and more robust than reacting to individual key changes. *)
let synchronize_once =
  (* Dates of last errors per key, used to avoid deadlooping: *)
  let poisonous_keys = Hashtbl.create 10 in
  let key_is_safe k now =
    match Hashtbl.find poisonous_keys k with
    | exception Not_found -> true
    | t -> now > t +. worker_quarantine_delay
  in
  fun conf ~while_ clt now ->
    !logger.debug "Synchronizing workers..." ;
    Client.iter_safe clt (fun k hv ->
      (* try_start_instance can take some time so better skip it at exit: *)
      if while_ () && key_is_safe k now then
        try
          match k, hv.Client.value with
          | Key.PerSite (site, PerWorker (fq, PerInstance (worker_sign, Pid))),
            Value.RamenValue T.(VI64 pid)
            when site = conf.C.site ->
              let pid = Int64.to_int pid in
              update_child_status conf ~while_ clt site fq worker_sign pid ;
              if not (should_run clt site fq worker_sign) then
                may_kill conf ~while_ clt site fq worker_sign pid
          | Key.PerSite (site, PerWorker (fq, Worker)),
            Value.Worker worker
            when site = conf.C.site ->
              if worker.enabled &&
                 not (is_running clt site fq worker.worker_signature)
              then
                try_start_instance conf ~while_ clt site fq worker
          | Key.PerSite (site, PerWorker (fq, PerReplayer id)) as replayer_k,
            Value.Replayer replayer
            when site = conf.C.site ->
              remove_dead_chans conf clt ~while_ replayer_k replayer ;
              update_replayer_status
                conf clt ~while_ now site fq id replayer_k replayer
          | _ -> ()
        with exn ->
          !logger.error "While synchronising key %a: %s:\n%s"
            Key.print k
            (Printexc.to_string exn)
            (Printexc.get_backtrace ()) ;
          Hashtbl.replace poisonous_keys k now)

(* In theory, supervisor or choreographer joining or leaving the fray should
 * leave the workers running without supervision. For tests though it is
 * required to stop all workers at exit.
 * Thus, this function kills all workers without even letting them save their
 * state; There is no point saving as there will be no restart. *)
let mass_kill_all conf clt =
  Client.iter clt (fun k hv ->
    match k, hv.Client.value with
    | Key.PerSite (site, PerWorker (fq, PerInstance (_, Pid))),
      Value.RamenValue T.(VI64 pid)
      when site = conf.C.site ->
        let pid = Int64.to_int pid in
        let what = Printf.sprintf2 "Killing %a" N.fq_print fq in
        !logger.info "%s" what ;
        log_and_ignore_exceptions ~what (Unix.kill pid) Sys.sigkill
    | _ -> ())

let synchronize_running conf kill_at_exit =
  let while_ () = !Processes.quit = None in
  let loop clt =
    let last_sync = ref 0. in
    while while_ () do
      ZMQClient.process_in ~while_ clt ;
      let now = Unix.gettimeofday () in
      if now > !last_sync +. delay_between_worker_syncs then (
        last_sync := now ;
        synchronize_once conf ~while_ clt now)
    done ;
    if kill_at_exit then mass_kill_all conf clt in
  let topics =
    [ (* All sites are needed because we need parent worker :(
         TODO: add whatever is needed from the parents (ie. output type)
         in this site workers so we need to listen at only the local
         site. *)
      "sites/*/workers/*" ;
      "sources/*/info" ;
      (* Replays are read directly. Would not add much to go through the
       * Choreographer but latency. *)
      "replays/*" ;
      "sites/"^ (conf.C.site :> string) ^"/workers/*/replayers/*" ] in
  (* Setting up/Tearing down replays is easier when they are added/removed: *)
  let on_del clt k v =
    match k, v with
    | Key.Replays chan,
      Value.Replay replay ->
        (* Replayers chan list will be updated in the loop but we need the replay
         * here to teardown all the links: *)
        !logger.info "Tearing down replay %a" Channel.print chan ;
        let func_of_fq fq =
          let _prog, func = function_of_site_fq clt conf.C.site fq in
          let prog_name, _ = N.fq_parse fq in
          F.unserialized prog_name func in
        Replay.teardown_links conf func_of_fq replay
    | _ -> ()
  and on_new clt k v _uid _mtime _can_write _can_del _owner _expiry =
    match k, v with
    | Key.Replays chan,
      Value.Replay replay ->
        let replay_range =
          TimeRange.make replay.C.Replays.since replay.until false in
        let func_of_fq fq =
          let _prog, func = function_of_site_fq clt conf.C.site fq in
          let prog_name, _ = N.fq_parse fq in
          F.unserialized prog_name func in
        Replay.settup_links conf func_of_fq replay ;
        (* Find or create all replayers: *)
        List.iter (fun (site, fq) ->
          if site = conf.C.site then (
            let rs =
              Client.fold clt (fun k hv rs ->
                match k, hv.value with
                | Key.PerSite (site', PerWorker (fq', PerReplayer _id)) as k,
                  Value.Replayer replayer
                  when N.eq site site' && N.eq fq fq' ->
                    (k, replayer) :: rs
                | _ -> rs
              ) [] in
            match List.find (fun (_, r) ->
                    r.Value.Replayer.pid = None &&
                    TimeRange.approx_eq replay_range r.time_range
                  ) rs with
            | exception Not_found ->
                let id = Random.int RingBufLib.max_replayer_id in
                let now = Unix.gettimeofday () in
                let channels = Set.singleton chan in
                let r = Value.Replayer.make now replay_range channels in
                let replayer_k =
                  Key.PerSite (site, PerWorker (fq, PerReplayer id)) in
                ZMQClient.send_cmd ~while_ ~eager:true
                  (NewKey (replayer_k, Value.Replayer r, 0.))
            | k, r ->
                !logger.debug
                  "Adding replay for channel %a into replayer created at %a"
                  Channel.print chan print_as_date r.creation ;
                let time_range = TimeRange.merge r.time_range replay_range
                and channels = Set.add chan r.channels in
                let replayer = Value.Replayer { r with time_range ; channels } in
                ZMQClient.send_cmd ~while_ ~eager:true
                  (UpdKey (k, replayer)))
        ) replay.sources
    | _ -> ()
  (* When supervisor restarts it must clean the configuration from all
   * remains of previous workers, that must have been killed since last
   * run and that could not only confuse supervisor, but also cause it to
   * not start workers and/or kill random processes: *)
  and on_synced clt =
    Client.iter_safe clt (fun k _hv ->
      match k with
      | Key.PerSite (site, PerWorker (fq, PerInstance (worker_sign, Pid)))
        when site = conf.C.site ->
          report_worker_death ~while_ clt site fq worker_sign "vanished"
      | _ -> ())
  in
  (* Timeout has to be much shorter than delay_before_replay *)
  let timeo = delay_before_replay *. 0.5 in
  start_sync conf ~while_ ~topics ~recvtimeo:timeo ~sndtimeo:timeo
             ~on_new ~on_del ~on_synced loop
