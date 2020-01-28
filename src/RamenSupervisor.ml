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
module VSI = Value.SourceInfo
module VOS = Value.OutputSpecs
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
module TimeRange = RamenTimeRange
module ZMQClient = RamenSyncZMQClient
module Watchdog = RamenWatchdog
module Versions = RamenVersions
module Paths = RamenPaths

(* Seed to pass to workers to init their random generator: *)
let rand_seed = ref None

(* A single worker can replay for several channels. This is very useful
 * when a dashboard reloads with many graphs requesting the same time interval.
 * So replayers are aggregated for a little while before spawning them.
 * No need to persist this hash though. *)

type replayers = (N.site_fq, replayer) Hashtbl.t

and replayer =
  { (*site_fq : N.site_fq ;*)
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
    mutable replays : (Channel.t, Value.Replay.t) Map.t }

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
let rescue_worker session ~while_ site fq state_file input_ringbuf =
  (* Maybe the state file is poisoned? At this stage it's probably safer
   * to move it away: *)
  !logger.info "Worker %a is deadlooping. Deleting its state file, \
                input ringbuffers and out_ref config entry."
    N.fq_print fq ;
  Files.move_aside state_file ;
  (* At this stage there should be no writers since this worker is stopped. *)
  Files.move_aside input_ringbuf ;
  (* Also empty its outref: *)
  let k = OutRef.output_specs_key site fq in
  ZMQClient.send_cmd ~while_ ~eager:true session (DelKey k)

(* Scan all workers on this site and remove that ringbuf from their out_refs.
 * FIXME: replace outrefs by individual references in the PerWorker subtree
 * with a special kind of reference pointing at a child InputRingBuf key,
 * that the actual worker could monitor to stop emission on deletion. *)
let cut_from_parents_outrefs ~while_ session input_ringbuf pid site =
  let now = Unix.gettimeofday () in
  let prefix = "sites/"^ (site : N.site :> string) ^"/workers/" in
  Client.iter ~prefix session.ZMQClient.clt (fun k _hv ->
    match k with
    | Key.PerSite (_, PerWorker (pfq, Worker)) ->
        (* The outref can be broken or an old version. Let's do our best but
         * avoid deadlooping: *)
        (try
          OutRef.(remove ~now ~while_ session site pfq (DirectFile input_ringbuf) ~pid
                  Channel.live)
        with e ->
          !logger.error "Cannot remove from parent outref (%s) ignoring."
            (Printexc.to_string e))
    | _ -> ())

let add_sync_env conf name fq env =
  ("sync_url="^ conf.C.sync_url) ::
  ("sync_srv_pub_key="^ conf.C.srv_pub_key) ::
  ("sync_username=_"^ name ^"_"^ (conf.C.site :> string) ^"/"
                    ^ (fq : N.fq :> string)) ::
  ("sync_clt_pub_key="^ conf.C.clt_pub_key) ::
  ("sync_clt_priv_key="^ conf.C.clt_priv_key) :: env

let start_worker
      conf session ~while_ prog_name func params envvars role
      log_level report_period cwd
      worker_instance bin parent_links children input_ringbuf state_file =
  (* Create the input ringbufs.
   * Workers are started one by one in no particular order.
   * The input and out-ref ringbufs are created when the worker start, and the
   * out-ref is filled with the running (or should be running) children.
   * Therefore, if a children fails to run the parents might block.
   * Also, if a child has not been started yet its inbound ringbuf will not exist,
   * again implying this worker will block.
   * Each time a new worker is started or stopped the parents outrefs are updated. *)
  let fq = VSI.fq_name prog_name func in
  let fq_str = (fq :> string) in
  let globals_dir = Paths.globals_dir conf.C.persist_dir in
  Option.may (fun input_ringbuf ->
    !logger.debug "Creating in buffers..." ;
    RingBuf.create input_ringbuf ;
    let rb = RingBuf.load input_ringbuf in
    finally (fun () -> RingBuf.unload rb)
      (fun () ->
        Processes.repair_and_warn ~what:fq_str rb ;
        IntCounter.inc (stats_ringbuf_repairs conf.C.persist_dir)) ()
  ) input_ringbuf ;
  (* And the pre-filled out_ref: *)
  if not (Value.Worker.is_top_half role) then (
    !logger.debug "Updating out-ref buffers..." ;
    List.iter (fun (pname, cfunc) ->
      let fname = Paths.in_ringbuf_name conf.C.persist_dir pname cfunc
      and fieldmask = RamenFieldMaskLib.make_fieldmask func.VSI.operation
                                                       cfunc.VSI.operation
      and now = Unix.gettimeofday () in
      (* The destination ringbuffer must exist before it's referenced in an
       * out-ref, or the worker might err and throw away the tuples: *)
      RingBuf.create fname ;
      OutRef.(add ~now ~while_ session conf.C.site fq (DirectFile fname)
                  fieldmask)
    ) children ;
    (* Start exporting until told otherwise (helps with both automatic and
     * manual tests): *)
    Processes.start_export ~while_ ~duration:conf.initial_export_duration
                           conf session conf.C.site prog_name func
  ) ;
  (* Now actually start the binary *)
  let notify_ringbuf =
    (* Where that worker must write its notifications. Normally toward a
     * ringbuffer that's read by Ramen, unless it's a test programs.
     * Tests must not send their notifications to Ramen with real ones,
     * but instead to a ringbuffer specific to the test_id. *)
    Paths.notify_ringbuf conf.C.persist_dir in
  let ocamlrunparam =
    let def = if log_level = Debug then "b" else "" in
    getenv ~def "OCAMLRUNPARAM" in
  let env = [
    "OCAMLRUNPARAM="^ ocamlrunparam ;
    "log_level="^ string_of_log_level log_level ;
    (* To know what to log: *)
    "is_test="^ string_of_bool conf.C.test ;
    (* Select the function to be executed: *)
    "name="^ (func.VSI.name :> string) ;
    "fq_name="^ fq_str ;
    "instance="^ worker_instance ;
    "report_ringbuf="^ (Paths.report_ringbuf conf.C.persist_dir :> string) ;
    "report_period="^ string_of_float report_period ;
    "notify_ringbuf="^ (notify_ringbuf :> string) ;
    "rand_seed="^ (match !rand_seed with None -> ""
                  | Some s -> string_of_int s) ;
    "site="^ (conf.C.site :> string) ;
    (match !logger.output with
      | Directory _ ->
        let dir = N.path_cat [ conf.C.persist_dir ; N.path "log/workers" ;
                               VSI.fq_path prog_name func ] in
        "log="^ (dir :> string)
      | Stdout -> "no_log" (* aka stdout/err *)
      | Syslog -> "log=syslog") ] in
  let env =
    match input_ringbuf with
    | None ->
        env
    | Some input_ringbuf ->
        ("input_ringbuf="^ (input_ringbuf :> string)) :: env in
  let env =
    match role with
    | Whole ->
          (* We need to change this dir whenever the func signature or
           * params change to prevent it to reload an incompatible state *)
        let factors_dir =
          Paths.factors_of_function conf.C.persist_dir prog_name func in
        ("state_file="^ (state_file : N.path :> string)) ::
        ("globals_dir="^ (globals_dir : N.path :> string)) ::
        ("factors_dir="^ (factors_dir :> string)) :: env
    | TopHalf ths ->
        List.fold_lefti (fun env i th ->
          ("tunneld_host_"^ string_of_int i ^"="^
            (th.Value.Worker.tunneld_host :> string)) ::
          ("tunneld_port_"^ string_of_int i ^"="^
            string_of_int th.Value.Worker.tunneld_port) ::
          ("parent_num_"^ string_of_int i ^"="^
            string_of_int th.Value.Worker.parent_num) :: env
        ) env ths in
  (* Pass each individual parameter as a separate envvar; envvars are just
   * non interpreted strings (but for the first '=' sign that will be
   * interpreted by the OCaml runtime) so it should work regardless of the
   * actual param name or value, and make it easier to see what's going
   * on from the shell. Notice that we pass all the parameters including
   * those omitted by the user. *)
  let env =
    let extra = Processes.env_of_params_and_exps conf conf.C.site params in
    List.rev_append extra env in
  (* Also add all envvars that are defined and used in the operation: *)
  let env =
    List.fold_left (fun env -> function
      | (n : N.field), Some v -> ((n :> string) ^"="^ v) :: env
      | _, None -> env
    ) env envvars in
  (* Workers must be given the address of a config-server: *)
  let env = add_sync_env conf "worker" fq env in
  let env = Array.of_list env in
  let args =
    [| if role = Whole then Worker_argv0.full_worker
                       else Worker_argv0.top_half ;
       fq_str |] in
  let cwd = if N.is_empty cwd then None else Some cwd in
  let pid = Processes.run_worker ?cwd ~and_stop:conf.C.test bin args env in
  !logger.debug "%a for %a now runs under pid %d"
    Value.Worker.print_role role N.fq_print fq pid ;
  (* Update the parents output specs: *)
  let now = Unix.gettimeofday ()
  and in_ringbuf =
    Paths.in_ringbuf_name conf.C.persist_dir prog_name func in
  List.iter (fun (pfq, fieldmask) ->
    OutRef.(add ~while_ ~now session conf.C.site pfq (DirectFile in_ringbuf)
                ~pid fieldmask)
  ) parent_links ;
  pid

(* Spawn a replayer.
 * Note: there is no top-half for sources. We assume the required
 * top-halves are already running as their full remote children are.
 * Pass to each replayer the name of the function, the out_ref files to
 * obey, the channel id to tag tuples with, and since/until dates.
 * Returns the pid. *)
let start_replayer conf fq func bin since until channels replayer_id =
  let prog_name, _func_name = N.fq_parse fq in
  let args = [| Worker_argv0.replay ; (fq :> string) |]
  and rb_archive =
    (* We pass the name of the current ringbuf archive if
     * there is one, that will be read after the archive
     * directory. Notice that if we cannot read the current
     * ORC file before it's archived, nothing prevent a
     * worker to write both a non-wrapping, non-archive
     * worthy ringbuf in addition to an ORC file. *)
    Paths.archive_buf_name ~file_type:VOS.RingBuf conf.C.persist_dir prog_name func
  in
  let env =
    [ "name="^ (func.VSI.name :> string) ;
      "fq_name="^ (fq :> string) ;
      "log_level="^ string_of_log_level conf.C.log_level ;
      "site="^ (conf.C.site :> string) ;
      "rb_archive="^ (rb_archive :> string) ;
      "since="^ string_of_float since ;
      "until="^ string_of_float until ;
      "channel_ids="^ Printf.sprintf2 "%a"
                        (Set.print ~first:"" ~last:"" ~sep:","
                                   RamenChannel.print) channels ;
      "replayer_id="^ string_of_int replayer_id ;
      "rand_seed="^ (match !rand_seed with None -> ""
                    | Some s -> string_of_int s) ] in
  let env =
    let name = "replayer"^ string_of_int replayer_id in
    add_sync_env conf name fq env |>
    Array.of_list in
  let pid = RamenProcesses.run_worker bin args env in
  !logger.debug "Replay for %a is running under pid %d"
    N.fq_print fq pid ;
  pid

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

(* Have a single watchdog even when supervisor is restarted: *)
let watchdog = ref None

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

let get_path = Option.map N.path % get_string

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

let send_epitaph session ~while_ site fq worker_sign status_str =
  let per_instance_key = per_instance_key site fq worker_sign in
  let now = Unix.gettimeofday () in
  ZMQClient.send_cmd ~while_ ~eager:true session
    (SetKey (per_instance_key LastExit,
             Value.of_float now)) ;
  ZMQClient.send_cmd ~while_ ~eager:true session
    (SetKey (per_instance_key LastExitStatus,
             Value.of_string status_str))

let send_quarantine ~while_ session site fq worker_sign delay =
  let per_instance_key = per_instance_key site fq worker_sign in
  let now = Unix.gettimeofday () in
  let quarantine_until = now +. delay in
  !logger.debug "Will quarantine until %a" print_as_date quarantine_until ;
  ZMQClient.send_cmd ~while_ ~eager:true session
    (SetKey (per_instance_key QuarantineUntil,
             Value.of_float quarantine_until))

let report_worker_death ~while_ session site fq worker_sign status_str pid =
  let per_instance_key = per_instance_key site fq worker_sign in
  send_epitaph session ~while_ site fq worker_sign status_str ;
  let k = per_instance_key InputRingFile in
  (match (Client.find session.clt k).value with
  | exception Not_found -> ()
  | Value.(RamenValue (VString s)) ->
      let input_ringbuf = N.path s in
      cut_from_parents_outrefs ~while_ session input_ringbuf pid site
  | v ->
      if not Value.(equal dummy v) then
        err_sync_type k v "a string") ;
  ZMQClient.send_cmd ~while_ ~eager:true session
    (DelKey (per_instance_key Pid))

(* Update the config for this process, and return true if that process
 * is still running. *)
let update_child_status conf session ~while_ site fq worker_sign pid =
  let per_instance_key = per_instance_key site fq worker_sign in
  let what = Printf.sprintf2 "Worker %a (pid %d)" N.fq_print fq pid in
  (match Unix.(restart_on_EINTR (waitpid [ WNOHANG ; WUNTRACED ])) pid with
  | exception Unix.(Unix_error (ECHILD, _, _)) ->
      !logger.error "%s: no such child, deleting this key" what ;
      (* TODO: assume this is an error ! *)
      report_worker_death ~while_ session site fq worker_sign "vanished" pid ;
      false
  | exception exn ->
      !logger.error "%s: waitpid: %s" what (Printexc.to_string exn) ;
      true
  | 0, _ ->
      true (* Nothing to report *)
  | _, (WSIGNALED s | WSTOPPED s) when s = Sys.sigstop ->
      !logger.debug "%s got stopped" what ;
      true
  | _, status ->
      let status_str = string_of_process_status status in
      let is_err = status <> WEXITED ExitCodes.terminated in
      (if is_err then !logger.error else info_or_test conf)
        "%s %s." what status_str ;
      let succ_fail_k =
        per_instance_key SuccessiveFailures in
      let succ_failures =
        find_or_fail "an integer" session.ZMQClient.clt succ_fail_k (function
          | None ->
              Some 0
          | Some (Value.RamenValue T.(VI64 i)) ->
              Some (Int64.to_int i)
          | _ ->
              None) in
      if is_err then (
        ZMQClient.send_cmd ~while_ ~eager:true session
          (SetKey (succ_fail_k,
                   Value.of_int (succ_failures + 1))) ;
        IntCounter.inc (stats_worker_crashes conf.C.persist_dir) ;
        if succ_failures mod 6 = 5 then (
          IntCounter.inc (stats_worker_deadloopings conf.C.persist_dir) ;
          let state_file =
            let k = per_instance_key StateFile in
            find_or_fail "a string" session.clt k get_string
          and input_ringbuf =
            let k = per_instance_key InputRingFile in
            find_or_fail "a strings" session.clt k get_path in
          rescue_worker
            ~while_ session site fq (N.path state_file) input_ringbuf
        )
      ) ;
      (* Wait before attempting to restart a failing worker: *)
      let max_delay = 1. +. float_of_int succ_failures in
      let delay = Random.float (min 90. max_delay) in
      send_quarantine ~while_ session site fq worker_sign delay ;
      report_worker_death ~while_ session site fq worker_sign status_str pid ;
      false)

let is_quarantined clt site fq worker_sign =
  let per_instance_key = per_instance_key site fq worker_sign in
  let k = per_instance_key QuarantineUntil in
  match (Client.find clt k).value with
  | exception Not_found ->
      false
  | Value.RamenValue (VFloat d) ->
      d > Unix.time ()
  | v ->
      invalid_sync_type k v "a float"

(* This worker is running. Should it?
 * Note: running conditions are not supposed to change once a program has
 * started, as testing them all at every iterations would be expensive. *)
let should_run clt site fq worker_sign =
  let k = Key.PerSite (site, PerWorker (fq, Worker)) in
  match (Client.find clt k).value with
  | exception Not_found -> false
  | Value.Worker worker ->
      (* Is that function lazy and unused? *)
      worker.is_used &&
      (* Is that function momentarily disabled? *)
      worker.enabled &&
      (* Is that instance for an obsolete worker? *)
      worker.worker_signature = worker_sign
  | v ->
      invalid_sync_type k v "a worker"

let may_kill conf ~while_ session site fq worker_sign pid =
  let per_instance_key = per_instance_key site fq worker_sign in
  let last_killed_k = per_instance_key LastKilled in
  let prev_last_killed =
    find_or_fail "a float" session.ZMQClient.clt last_killed_k (function
      | None -> Some 0.
      | Some (Value.RamenValue T.(VFloat t)) -> Some t
      | _ -> None) in
  let last_killed = ref prev_last_killed in
  let input_ringbuf_k = per_instance_key InputRingFile in
  (match (Client.find session.clt input_ringbuf_k).value with
  | exception Not_found ->
      !logger.warning "Cannot find %a" Key.print input_ringbuf_k
  | Value.RamenValue (VString path) ->
      cut_from_parents_outrefs ~while_ session (N.path path) pid site
  | v ->
      err_sync_type input_ringbuf_k v "a string") ;
  let no_more_child () =
    (* The worker vanished. Can happen in case of bugs (such as
     * https://github.com/rixed/ramen/issues/789). Must keep
     * calm when that happen: *)
    !logger.error "Worker pid %d has vanished" pid ;
    report_worker_death ~while_ session site fq worker_sign "vanished" pid ;
    last_killed := Unix.gettimeofday () in
  let what = Printf.sprintf2 "worker %a (pid %d)" N.fq_print fq pid in
  log_and_ignore_exceptions ~what:("Killing "^ what) (fun () ->
    try
      (* Before killing him, let's wake him up: *)
      Unix.kill pid Sys.sigcont ;
      kill_politely conf last_killed what pid stats_worker_sigkills
    with Unix.(Unix_error (ESRCH, _, _)) -> no_more_child ()
  ) () ;
  if !last_killed <> prev_last_killed then (
    ZMQClient.send_cmd ~while_ ~eager:true session
      (SetKey (last_killed_k, Value.of_float !last_killed)))

(* This worker is considered running as soon as it has a pid: *)
let is_running clt site fq worker_sign =
  Client.(Tree.mem clt.h (per_instance_key site fq worker_sign Pid))

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

(* [info_sign] is the signature of the info identifying the result of
 * precompilation. *)
let get_bin_file conf session prog_name info_sign info info_mtime =
  let get_parent =
    RamenCompiler.program_from_confserver session.ZMQClient.clt in
  let info_file =
    Paths.supervisor_cache_file conf.C.persist_dir (N.path info_sign) "info" in
  let bin_file =
    Paths.supervisor_cache_bin conf.C.persist_dir info_sign in
  let info_value = Value.SourceInfo info in
  RamenMake.write_value_into_file info_file info_value info_mtime ;
  RamenMake.(apply_rule conf get_parent prog_name info_file bin_file bin_rule) ;
  bin_file

(* First we need to compile (or use a cached of) the source info, that
 * we know from the SourcePath set by the Choreographer.
 * Then for each parent we need the required fieldmask, that the
 * Choreographer also should have set, and update those parents out_ref.
 * Then we can spawn that binary, with the parameters also set by
 * the choreographer. *)
let try_start_instance conf session ~while_ site fq worker =
  let prog_name, func_name = N.fq_parse fq in
  let src_path = N.src_path_of_program prog_name in
  let info, info_mtime, precompiled =
    get_precompiled session.ZMQClient.clt src_path in
  (* Check that info has the proper signature: *)
  let info_sign = Value.SourceInfo.signature info in
  if worker.Value.Worker.info_signature <> info_sign then
    Printf.sprintf "Invalid signature for info: expected %S but got %S"
      worker.info_signature info_sign |>
    failwith ;
  let bin_file =
    get_bin_file conf session prog_name worker.info_signature info info_mtime in
  let params = hashtbl_of_alist worker.params in
  !logger.info "Must execute %a for %a, function %a"
    N.path_print bin_file
    Value.Worker.print worker
    N.fq_print fq ;
  let func_of_precompiled precompiled fname =
    List.find (fun f -> f.VSI.name = fname)
      precompiled.VSI.funcs in
  let func_of_ref ref =
    let src_path = N.src_path_of_program ref.Value.Worker.program in
    let _info, _info_mtime, precompiled =
      get_precompiled session.clt src_path in
    ref.program, func_of_precompiled precompiled ref.func in
  let func = func_of_precompiled precompiled func_name in
  let children =
    List.map func_of_ref worker.children in
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
  let input_ringbuf =
    if worker.parents = [] then None
    else Some (Paths.in_ringbuf_name conf.C.persist_dir prog_name func)
  and state_file =
    Paths.state_file_path conf.C.persist_dir src_path worker.worker_signature
  and parent_links =
    List.map (fun pref ->
      let _pname, pfunc = func_of_ref pref in
      Value.Worker.fq_of_ref pref,
      RamenFieldMaskLib.make_fieldmask pfunc.VSI.operation func.VSI.operation
    ) worker.parents in
  let pid =
    start_worker
      conf ~while_ session prog_name func params envvars worker.role log_level
      worker.report_period worker.cwd worker.worker_signature bin_file
      parent_links children input_ringbuf state_file in
  let per_instance_key = per_instance_key site fq worker.worker_signature in
  let k = per_instance_key LastKilled in
  ZMQClient.send_cmd ~eager:true ~while_ session (DelKey k) ;
  let k = per_instance_key Pid in
  ZMQClient.send_cmd ~eager:true ~while_ session
                     (SetKey (k, Value.(of_int pid))) ;
  let k = per_instance_key StateFile
  and v = Value.(of_string (state_file :> string)) in
  ZMQClient.send_cmd ~eager:true ~while_ session (SetKey (k, v)) ;
  Option.may (fun input_ringbuf ->
    let k = per_instance_key InputRingFile
    and v = Value.(RamenValue (T.VString (input_ringbuf : N.path :> string))) in
    ZMQClient.send_cmd ~eager:true ~while_ session (SetKey (k, v))
  ) input_ringbuf

let remove_dead_chans conf session ~while_ replayer_k replayer =
  let channels, changed =
    Set.fold (fun chan (channels, changed) ->
      let replay_k = Key.Replays chan in
      if Client.mem session.ZMQClient.clt replay_k then
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
    ZMQClient.send_cmd ~while_ ~eager:true session
      (UpdKey (replayer_k, replayer))

let update_replayer_status
      conf session ~while_ now site fq replayer_id replayer_k replayer =
  let prog_name, _func_name = N.fq_parse fq in
  let src_path = N.src_path_of_program prog_name in
  let rem_replayer () =
    ZMQClient.send_cmd ~while_ session (DelKey replayer_k) in
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
              match (Client.find session.clt worker_k).value with
              | Value.Worker worker -> worker
              | v -> invalid_sync_type worker_k v "a Worker" in
            let info_k = Key.Sources (src_path, "info") in
            let info, mtime =
              match Client.find session.clt info_k with
              | { value = Value.SourceInfo info ; mtime ; _ } ->
                  info, mtime
              | hv -> invalid_sync_type info_k hv.value "a SourceInfo" in
            let bin =
              get_bin_file conf session prog_name worker.info_signature info mtime in
            let _prog, _prog_name, func = function_of_fq session.clt fq in
            !logger.info
              "Starting a %a replayer created %gs ago for channels %a"
              N.fq_print fq
              (now -. replayer.creation)
              (Set.print Channel.print) replayer.channels ;
            let pid =
              start_replayer
                conf fq func bin since until replayer.channels replayer_id in
            let v = Value.Replayer { replayer with pid = Some pid } in
            ZMQClient.send_cmd ~while_ ~eager:true session
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
            N.site_fq_print (site, fq) pid in
        (match Unix.(restart_on_EINTR
                       (waitpid [ WNOHANG ; WUNTRACED ])) pid with
        | exception Unix.(Unix_error (ECHILD, _, _)) ->
            !logger.error "%s: no such process, removing that replayer from \
                           the configuration." what ;
            rem_replayer ()
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
            ZMQClient.send_cmd ~while_ ~eager:true session
              (UpdKey (replayer_k, replayer)))


(* Loop over all keys, which is mandatory to monitor pid terminations,
 * and synchronize running pids with the choreographer output.
 * This is simpler and more robust than reacting to individual key changes. *)
let synchronize_once =
  (* Dates of last errors per key, used to avoid dead-looping: *)
  let poisonous_keys = Hashtbl.create 10 in
  let key_is_safe k now =
    match Hashtbl.find poisonous_keys k with
    | exception Not_found -> true
    | t -> now > t +. worker_quarantine_delay
  in
  fun conf session ~while_ now ->
    !logger.debug "Synchronizing workers..." ;
    Client.iter_safe session.ZMQClient.clt (fun k hv ->
      (* try_start_instance can take some time so better skip it at exit: *)
      if while_ () && key_is_safe k now then
        try
          match k, hv.Client.value with
          | Key.PerSite (site, PerWorker (fq, PerInstance (worker_sign, Pid))),
            Value.RamenValue T.(VI64 pid)
            when site = conf.C.site ->
              let pid = Int64.to_int pid in
              let still_running =
                update_child_status conf session ~while_ site fq worker_sign pid in
              if still_running &&
                 (not (should_run session.clt site fq worker_sign) ||
                  is_quarantined session.clt site fq worker_sign)
              then
                may_kill conf ~while_ session site fq worker_sign pid
          | Key.PerSite (site, PerWorker (fq, Worker)),
            Value.Worker worker
            when site = conf.C.site ->
              if worker.is_used &&
                 worker.enabled &&
                 not (is_running session.clt site fq worker.worker_signature) &&
                 not (is_quarantined session.clt site fq worker.worker_signature)
              then (
                try_start_instance conf session ~while_ site fq worker ;
                (* The above is slow enought that this could be needed: *)
                ZMQClient.may_send_ping ~while_ session ;
                (* If we have many programs to compile in this loop better
                 * reset the watchdog: *)
                Option.may Watchdog.reset !watchdog
              )
          | Key.PerSite (site, PerWorker (fq, PerReplayer id)) as replayer_k,
            Value.Replayer replayer
            when site = conf.C.site ->
              remove_dead_chans conf session ~while_ replayer_k replayer ;
              update_replayer_status
                conf ~while_ session now site fq id replayer_k replayer
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
let mass_kill_all conf session =
  Client.iter session.ZMQClient.clt (fun k hv ->
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
  let loop session =
    let last_sync = ref 0. in
    while while_ () do
      Option.may Watchdog.reset !watchdog ;
      ZMQClient.process_in ~while_ session ;
      let now = Unix.gettimeofday () in
      if now > !last_sync +. delay_between_worker_syncs then (
        last_sync := now ;
        synchronize_once conf session ~while_ now)
    done ;
    if kill_at_exit then mass_kill_all conf session in
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
      (* Currently part of the above sites/*/workers/* but see TODO:
       * "sites/"^ (conf.C.site :> string) ^"/workers/*/replayers/*" *)] in
  (* Setting up/Tearing down replays is easier when they are added/removed: *)
  let on_del session k v =
    match k, v with
    | Key.Replays chan,
      Value.Replay replay ->
        (* Replayers chan list will be updated in the loop but we need the replay
         * here to teardown all the links: *)
        !logger.info "Tearing down replay %a" Channel.print chan ;
        Replay.teardown_links conf session replay
    | _ -> ()
  and on_new session k v _uid _mtime _can_write _can_del _owner _expiry =
    match k, v with
    | Key.Replays chan,
      Value.Replay replay
      when replay.Value.Replay.timeout_date >= Unix.time () ->
        let replay_range =
          TimeRange.make replay.Value.Replay.since replay.until false in
        let func_of_fq fq =
          let prog_name, _func_name = N.fq_parse fq in
          let _prog, _prog_name, func =
            function_of_fq session.ZMQClient.clt fq in
          prog_name, func in
        Replay.settup_links conf ~while_ session func_of_fq replay ;
        (* Find or create all replayers: *)
        List.iter (fun (site, fq) ->
          if site = conf.C.site then (
            let prefix = "sites/"^ (site :> string) ^"/workers/"^
                         (fq : N.fq :> string) ^"/replayers/" in
            let rs =
              Client.fold session.clt ~prefix (fun k hv rs ->
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
                ZMQClient.send_cmd ~while_ ~eager:true session
                  (NewKey (replayer_k, Value.Replayer r, 0.))
            | k, r ->
                !logger.debug
                  "Adding replay for channel %a into replayer created at %a"
                  Channel.print chan print_as_date r.creation ;
                let time_range = TimeRange.merge r.time_range replay_range
                and channels = Set.add chan r.channels in
                let replayer = Value.Replayer { r with time_range ; channels } in
                ZMQClient.send_cmd ~while_ ~eager:true session
                  (UpdKey (k, replayer)))
        ) replay.sources
    | _ -> ()
  (* When supervisor restarts it must clean the configuration from all
   * remains of previous workers, that must have been killed since last
   * run and that could not only confuse supervisor, but also cause it to
   * not start workers and/or kill random processes.
   * This is OK because no workers are started in [on_new] but only later
   * in the [loop] function. *)
  and on_synced session =
    Client.iter_safe session.ZMQClient.clt (fun k hv ->
      match k, hv.value  with
      | Key.PerSite (site, PerWorker (fq, PerInstance (worker_sign, Pid))),
        Value.RamenValue T.(VI64 pid)
        when site = conf.C.site ->
          let pid = Int64.to_int pid in
          !logger.warning "Deleting remains of a previous worker pid %d" pid ;
          report_worker_death ~while_ session site fq worker_sign "vanished" pid
      | Key.PerSite (site, PerWorker (_, PerReplayer _)) as replayer_k,
        Value.Replayer _
        when site = conf.C.site ->
          ZMQClient.send_cmd ~while_ session (DelKey replayer_k)
      | _ -> ())
  in
  (* Timeout has to be much shorter than delay_before_replay *)
  let timeo = delay_before_replay *. 0.5 in
  if !watchdog = None then watchdog :=
    Some (Watchdog.make ~timeout:300. "synchronize workers" Processes.quit) ;
  let watchdog = Option.get !watchdog in
  Watchdog.enable watchdog ;
  start_sync conf ~while_ ~topics ~recvtimeo:timeo ~sndtimeo:timeo
             ~sesstimeo:Default.sync_long_sessions_timeout
             ~on_new ~on_del ~on_synced loop ;
  Watchdog.disable watchdog
