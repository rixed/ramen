(*
 * Process supervisor task is to start and stop workers, connect them
 * properly and make sure they keep working.
 *)
open Batteries
open Stdint

open RamenConsts
open RamenLog
open RamenHelpersNoLog
open RamenHelpers
open RamenSyncHelpers
open RamenSync
module C = RamenConf
module Channel = RamenChannel
module Default = RamenConstsDefault
module E = RamenExpr
module Files = RamenFiles
module Paths = RamenPaths
module Metric = RamenConstsMetric
module N = RamenName
module O = RamenOperation
module OWD = Output_specs_wire.DessserGen
module OutRef = RamenOutRef
module Processes = RamenProcesses
module Replay = RamenReplay
module Services = RamenServices
module T = RamenTypes
module TimeRange = RamenTimeRange
module Versions = RamenVersions
module VR = Value.Replayer
module VSI = Value.SourceInfo
module Watchdog = RamenWatchdog
module Worker_argv0 = RamenConstsWorkerArgv0
module ZMQClient = RamenSyncZMQClient

(* Seed to pass to workers to init their random generator: *)
let rand_seed = ref None

(* Ask workers to send test notifications every so often: *)
let test_notifs_every = ref 0.

(* How to set max_readers on workers: *)
let lmdb_max_readers = ref None

open Binocle

let stats_worker_crashes =
  Files.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.worker_crashes
      "Number of workers that have crashed (or exited with non 0 status).")

let stats_worker_deadloopings =
  Files.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.worker_deadloopings
      "Number of time a worker has been found to deadloop.")

let stats_ringbuf_repairs =
  Files.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.ringbuf_repairs
      "Number of times a worker ringbuf had to be repaired.")

let stats_outref_repairs =
  Files.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.outref_repairs
      "Number of times a worker outref had to be repaired.")

let stats_worker_sigkills =
  Files.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.worker_sigkills
      "Number of times a worker had to be sigkilled instead of sigtermed.")

let stats_replayer_crashes =
  Files.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.replayer_crashes
      "Number of replayers that have crashed (or exited with non 0 status).")

let stats_replayer_sigkills =
  Files.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.replayer_sigkills
      "Number of times a replayer had to be sigkilled instead of sigtermed.")

let stats_chans_per_replayer =
  Files.ensure_inited (fun save_dir ->
    Histogram.make ~save_dir:(save_dir :> string)
      Metric.Names.chans_per_replayer
      "Number of channels per replayer."
      (Histogram.linear_buckets 1.))

(* It is common for crashed/interrupted workers to leave ringbuffers with some
 * memory range allocated. Supervisor must thus ensure all ringbuffers are
 * valid before assigning any pre-existing one to any worker (in or out). *)

let checked_ringbuffers = ref N.SetOfPaths.empty

let check_ringbuffer ?rb conf fname =
  if N.SetOfPaths.mem fname !checked_ringbuffers then
    !logger.debug "Ringbuffer %a has been checked already" N.path_print fname
  else (
    let do_check rb =
      if RingBuf.repair rb then (
        !logger.info "Repaired ringbuffer %a that was damaged" N.path_print fname ;
        IntCounter.inc (stats_ringbuf_repairs conf.C.persist_dir)
      ) else
        !logger.debug "Ringbuffer %a checks OK" N.path_print fname ;
      checked_ringbuffers := N.SetOfPaths.add fname !checked_ringbuffers
    in
    match rb with
    | Some rb ->
        do_check rb
    | None ->
        let rb = RingBuf.load fname in
        finally
          (fun () -> RingBuf.unload rb)
          do_check rb
  )

let get_precompiled clt src_path =
  let source_k = Key.Sources (src_path, "info") in
  match Client.find clt source_k with
  | exception Not_found ->
      Printf.sprintf2 "No such source %a"
        Key.print source_k |>
      failwith
  | { value = Value.SourceInfo
                ({ detail = Compiled compiled ; _ } as info) ; _ } ->
      info, compiled
  | { value = Value.SourceInfo
                { detail = Failed { errors } } ; _ } ->
      Printf.sprintf2 "Compilation failed: %a"
        (pretty_list_print RamenRaqlError.print) errors |>
      failwith
  | hv ->
      invalid_sync_type source_k hv.value "a source info"

(* [info_sign] is the signature of the info identifying the result of
 * precompilation.
 * Raises Not_found if the binary is not available yet,
 * Raises Failure if the value has the wrong type. *)
let get_executable conf session info_sign =
  let clt = option_get "get_executable" __LOC__ session.ZMQClient.clt in
  let exe_key = Key.(PerSite (conf.C.site, PerProgram (info_sign, Executable))) in
  match (Client.find clt exe_key).value with
  | Value.RamenValue (Raql_value.VString path) -> N.path path
  | v -> invalid_sync_type exe_key v "a string"

let has_executable conf session info_sign =
  try
    let fname = get_executable conf session info_sign in
    Files.exists ~has_perms:0o100 fname
  with _ ->
    false

let has_info session fq info_sign =
  let clt = option_get "has_info" __LOC__ session.ZMQClient.clt in
  let prog_name, func_name = N.fq_parse fq in
  let src_path = N.src_path_of_program prog_name in
  match get_precompiled clt src_path with
  | exception _ ->
      false
  | { detail = Compiled ({ funcs ; _ } as compiled) ; _ }, _ ->
      if List.exists (fun f ->
           f.Source_info.DessserGen.name = func_name
         ) funcs then (
        let this_info_sign = VSI.signature_of_compiled compiled in
        if this_info_sign = info_sign then true else (
          !logger.warning
            "Compiled info has a different signature (%s) than worker's (%s)"
            this_info_sign
            info_sign ;
          false
        )
      ) else (
        !logger.warning "Program %a has no function named %a"
          N.program_print prog_name
          N.func_print func_name ;
        false
      )
  | _ ->
      false

let find_worker session site fq =
  let clt = option_get "find_worker" __LOC__ session.ZMQClient.clt in
  let k = Key.PerSite (site, PerWorker (fq, Worker)) in
  match (Client.find clt k).value with
  | Value.Worker worker ->
      worker
  | v ->
      err_sync_type k v "a worker" ;
      raise Not_found

let has_worker session site fq =
  match find_worker session site fq with
  | exception Not_found -> false
  | _ -> true

let worker_should_run conf worker =
  (* Is that function lazy and unused? *)
  (worker.Value.Worker.is_used || conf.C.test) &&
  (* Is that function momentarily disabled? *)
  worker.enabled

let fq_should_run conf session fq =
  match find_worker session conf.C.site fq with
  | exception Not_found -> false
  | worker -> worker_should_run conf worker

(* When a worker seems to crashloop, assume it's because of a bad file and
 * delete them! *)
let rescue_worker conf session site fq state_file input_ringbuf_opt =
  (* Maybe the state file is poisoned? At this stage it's probably safer
   * to move it away: *)
  !logger.info "Worker %a is deadlooping. Deleting its state file, \
                input ringbuffers, binary and out_ref config entry."
    N.fq_print fq ;
  Files.move_aside state_file ;
  (* At this stage there should be no writers since this worker is stopped. *)
  Option.may Files.move_aside input_ringbuf_opt ;
  (* Delete the binary (which may also impact sibling workers): *)
  (match find_worker session conf.C.site fq with
  | exception Not_found ->
      !logger.warning
        "Cannot find crashlooping worker for %a, not deleting its binary"
        N.fq_print fq
  | worker ->
      let bin_file =
        get_executable conf session worker.Value.Worker.info_signature in
      Files.move_aside bin_file) ;
  (* Also empty its outref: *)
  let k = OutRef.output_specs_key site fq in
  ZMQClient.send_cmd ~eager:true session (DelKey k)

(* Scan all workers on this site and remove that ringbuf from their out_refs.
 * FIXME: replace outrefs by individual references in the PerWorker subtree
 * with a special kind of reference pointing at a child InputRingBuf key,
 * that the actual worker could monitor to stop emission on deletion. *)
let cut_from_parents_outrefs ~while_ session input_ringbuf pid site =
  let clt = option_get "cut_from_parents_outrefs" __LOC__ session.ZMQClient.clt in
  let now = Unix.gettimeofday () in
  let prefix = "sites/"^ (site : N.site :> string) ^"/workers/" in
  Client.iter ~prefix clt (fun k _hv ->
    match k with
    | Key.PerSite (_, PerWorker (pfq, Worker)) ->
        (* The outref can be broken or an old version. Let's do our best but
         * avoid deadlooping: *)
        (try
          let pid = Uint32.of_int pid in
          OutRef.(remove ~now ~while_ session site pfq (DirectFile input_ringbuf)
                  ~pid Channel.live)
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
  assert (input_ringbuf <> None || array_is_empty parent_links) ;
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
    finally
      (fun () -> RingBuf.unload rb)
      (check_ringbuffer ~rb conf) input_ringbuf ;
  ) input_ringbuf ;
  (* And the pre-filled out_ref: *)
  if not (Value.Worker.is_top_half role) then (
    !logger.debug "Updating out-ref buffers..." ;
    Array.iter (fun (pname, cfunc) ->
      (* Do not add children if they have no worker though (ie. lazy functions
       * not running yet). When a lazy function starts it will add itself to
       * its parent outref (see the end of this very function). *)
      let cfq = N.fq_of_program pname cfunc.VSI.name in
      if fq_should_run conf session cfq then (
        let fname = Paths.in_ringbuf_name conf.C.persist_dir pname cfunc
        and fieldmask = RamenFieldMaskLib.make_fieldmask func.VSI.operation
                                                         cfunc.VSI.operation
        and filters = O.scalar_filters_of_operation func.VSI.operation
                                                    cfunc.VSI.operation
        and now = Unix.gettimeofday () in
        (* The destination ringbuffer must exist before it's referenced in an
         * out-ref, or the worker might err and throw away the tuples: *)
        RingBuf.create fname ;
        check_ringbuffer conf fname ;
        OutRef.(add ~now ~while_ session conf.C.site fq (DirectFile fname)
                    ~filters fieldmask))
    ) children ;
    (* Start exporting until told otherwise (helps with both automatic and
     * manual tests): *)
    Processes.start_archive ~while_ ~duration:conf.initial_export_duration
                            conf session conf.C.site prog_name func
  ) ;
  (* Now actually start the binary *)
  let ocamlrunparam =
    (* Note that this won't change anything unless the binary has also be
     * compiled with "-g", ie. by an execompserver in debug mode *)
    let def = if log_level = Debug then "b" else "" in
    getenv ~def "OCAMLRUNPARAM" in
  let env = [
    "OCAMLRUNPARAM="^ ocamlrunparam ;
    "log_level="^ string_of_log_level log_level ;
    "log_with_time="^ string_of_bool conf.C.log_with_time ;
    (* To know what to log: *)
    "is_test="^ string_of_bool conf.C.test ;
    (* Select the function to be executed: *)
    "name="^ (func.VSI.name :> string) ;
    "fq_name="^ fq_str ;
    "instance="^ worker_instance ;
    "report_period="^ string_of_float report_period ;
    "test_notifs_every="^ string_of_float !test_notifs_every ;
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
        Array.fold_lefti (fun env i th ->
          ("tunneld_host_"^ string_of_int i ^"="^
            (th.Value.Worker.tunneld_host :> string)) ::
          ("tunneld_port_"^ string_of_int i ^"="^
            Uint16.to_string th.Value.Worker.tunneld_port) ::
          ("parent_num_"^ string_of_int i ^"="^
            Uint32.to_string th.Value.Worker.parent_num) :: env
        ) env ths in
  (* Pass each individual parameter as a separate envvar; envvars are just
   * non interpreted strings (but for the first '=' sign that will be
   * interpreted by the OCaml runtime) so it should work regardless of the
   * actual param name or value, and make it easier to see what's going
   * on from the shell. Notice that we pass all the parameters including
   * those omitted by the user. *)
  let env =
    let extra = Processes.env_of_params_and_exps conf.C.site params envvars in
    List.rev_append extra env in
  (* Workers must be given the address of a config-server: *)
  let env = add_sync_env conf "worker" fq env in
  (* Propagates lmdb_max_readers *)
  let env =
    match !lmdb_max_readers with
    | None -> env
    | Some v -> ("LMDB_MAX_READERS="^ string_of_int v) :: env in
  let env = Array.of_list env in
  let args =
    [| if role = Whole then Worker_argv0.full_worker
                       else Worker_argv0.top_half ;
       fq_str |] in
  let cwd = if N.is_empty cwd then None else Some cwd in
  let pid =
    Processes.run_worker ?cwd ~and_stop:conf.C.test bin args env |>
    Uint32.of_int in
  !logger.debug "%a for %a now runs under pid %s"
    Value.Worker.print_role role N.fq_print fq
    (Uint32.to_string pid) ;
  (* Update the parents output specs: *)
  Option.may (fun input_ringbuf ->
    (* input_ringbuf has been checked already right abovve *)
    let now = Unix.gettimeofday () in
    Array.iter (fun (pfq, fieldmask, filters) ->
      OutRef.(add ~while_ ~now session conf.C.site pfq (DirectFile input_ringbuf)
                  ~pid ~filters fieldmask)
    ) parent_links
  ) input_ringbuf ;
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
    Paths.archive_buf_name ~file_type:OWD.RingBuf conf.C.persist_dir prog_name func
  in
  let ocamlrunparam =
    let def = if !logger.log_level = Debug then "b" else "" in
    getenv ~def "OCAMLRUNPARAM" in
  let mono_chans, multi_chans =
    Array.fold_left (fun (mono_chans, multi_chans) (chan, del_when_done) ->
      if del_when_done then
        chan :: mono_chans, multi_chans
      else
        mono_chans, chan :: multi_chans
    ) ([], []) channels in
  let env =
    [ "OCAMLRUNPARAM="^ ocamlrunparam ;
      "name="^ (func.VSI.name :> string) ;
      "fq_name="^ (fq :> string) ;
      "log_level="^ string_of_log_level !logger.log_level ;
      "site="^ (conf.C.site :> string) ;
      "rb_archive="^ (rb_archive :> string) ;
      "since="^ string_of_float since ;
      "until="^ string_of_float until ;
      "mono_channels="^
        Printf.sprintf2 "%a"
         (List.print ~first:"" ~last:"" ~sep:"," RamenChannel.print)
           mono_chans ;
      "multi_channels="^
        Printf.sprintf2 "%a"
         (List.print ~first:"" ~last:"" ~sep:"," RamenChannel.print)
           multi_chans ;
      "replayer_id="^ Uint32.to_string replayer_id ;
      "rand_seed="^ (match !rand_seed with None -> ""
                    | Some s -> string_of_int s) ] in
  let env =
    let name = "replayer"^ Uint32.to_string replayer_id in
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
    C.info_or_test conf "Terminating %s" what ;
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
  | Some (Value.RamenValue (Raql_value.VString s)) -> Some s
  | _ -> None

let get_path = Option.map N.path % get_string

let get_string_list =
  let is_string = function
    | Raql_value.VString _ -> true
    | _ -> false in
  function
  | Some (Value.RamenValue (Raql_value.VArr vs))
    when Array.for_all is_string vs ->
      Array.enum vs /@
      (function VString s -> N.path s | _ -> assert false) |>
      List.of_enum |>
      Option.some
  | _ -> None

let send_epitaph session site fq worker_sign status_str =
  let per_instance_key = per_instance_key site fq worker_sign in
  let now = Unix.gettimeofday () in
  ZMQClient.send_cmd ~eager:true session
    (SetKey (per_instance_key LastExit,
             Value.of_float now)) ;
  ZMQClient.send_cmd ~eager:true session
    (SetKey (per_instance_key LastExitStatus,
             Value.of_string status_str))

let send_quarantine session site fq worker_sign delay =
  let per_instance_key = per_instance_key site fq worker_sign in
  let now = Unix.gettimeofday () in
  let quarantine_until = now +. delay in
  !logger.info "Will quarantine until %a" print_as_date quarantine_until ;
  ZMQClient.send_cmd ~eager:true session
    (SetKey (per_instance_key QuarantineUntil,
             Value.of_float quarantine_until))

let clear_quarantine session site fq worker_sign =
  let clt = option_get "clear_quarantine" __LOC__ session.ZMQClient.clt in
  let k = per_instance_key site fq worker_sign QuarantineUntil in
  if Client.mem clt k then (
    !logger.info "Clearing quarantine for %a" N.fq_print fq ;
    ZMQClient.send_cmd ~eager:true session (DelKey k))

(* Deletes the pid, cut from parents outrefs... *)
let report_worker_death session ~while_ site fq worker_sign status_str pid =
  let clt = option_get "report_worker_death" __LOC__ session.ZMQClient.clt in
  let per_instance_key = per_instance_key site fq worker_sign in
  send_epitaph session site fq worker_sign status_str ;
  let k = per_instance_key InputRingFile in
  (match (Client.find clt k).value with
  | exception Not_found -> ()
  | Value.(RamenValue (VString s)) ->
      let input_ringbuf = N.path s in
      cut_from_parents_outrefs ~while_ session input_ringbuf pid site
  | v ->
      if not Value.(equal dummy v) then
        err_sync_type k v "a string") ;
  ZMQClient.send_cmd ~eager:true session
    (DelKey (per_instance_key Pid))

(* Update the config for this process, and return true if that process
 * is still running. *)
let update_child_status conf session ~while_ site fq worker_sign pid =
  let clt = option_get "update_child_status" __LOC__ session.ZMQClient.clt in
  let per_instance_key = per_instance_key site fq worker_sign in
  let what = Printf.sprintf2 "Worker %a (pid %d)" N.fq_print fq pid in
  (match Unix.(restart_on_EINTR (waitpid [ WNOHANG ; WUNTRACED ])) pid with
  | exception Unix.(Unix_error (ECHILD, _, _)) ->
      !logger.error "%s: no such child, deleting this key" what ;
      (* TODO: assume this is an error ! *)
      report_worker_death session ~while_ site fq worker_sign "vanished" pid ;
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
      (if is_err then !logger.error else C.info_or_test conf)
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
        ZMQClient.send_cmd ~eager:true session
          (SetKey (succ_fail_k,
                   Value.of_int (succ_failures + 1))) ;
        IntCounter.inc (stats_worker_crashes conf.C.persist_dir) ;
        if succ_failures = 2 || succ_failures mod 6 = 5 then (
          IntCounter.inc (stats_worker_deadloopings conf.C.persist_dir) ;
          let state_file =
            let k = per_instance_key StateFile in
            find_or_fail "a string" clt k get_string
          (* Note: report_worker_death is going to remove it from OutRefs,
           * which will trigger a few warnings because the file is now
           * missing, but it will be removed nonetheless. *)
          and input_ringbuf_opt =
            let k = per_instance_key InputRingFile in
            match (Client.find clt k).value with
            | exception Not_found ->
                None (* This worker has no input ringbuf *)
            | Value.RamenValue (VString s) ->
                Some (N.path s)
            | v ->
                err_sync_type k v "a string" ;
                None in
          rescue_worker conf session site fq (N.path state_file)
                        input_ringbuf_opt
        ) ;
        (* Wait before attempting to restart a failing worker: *)
        let max_delay = sqrt (1. +. 100. *. float_of_int succ_failures) in
        let delay = 10. +. Random.float max_delay in
        send_quarantine session site fq worker_sign delay ;
      ) else (
        clear_quarantine session site fq worker_sign
      ) ;
      report_worker_death session ~while_ site fq worker_sign status_str pid ;
      false)

let is_quarantined clt site fq worker_sign =
  let per_instance_key = per_instance_key site fq worker_sign in
  let k = per_instance_key QuarantineUntil in
  match (Client.find clt k).value with
  | exception Not_found ->
      false
  | Value.RamenValue (VFloat d) ->
      let quarantined = d > Unix.time () in
      if quarantined then
        !logger.debug "Worker %a is quarantined until %a and should not run"
          N.fq_print fq
          print_as_date d ;
      quarantined
  | v ->
      invalid_sync_type k v "a float"

(* This worker is running. Should it?
 * Note: running conditions are not supposed to change once a program has
 * started, as testing them all at every iterations would be expensive. *)
let should_run conf session site fq worker_sign =
  match find_worker session site fq with
  | exception Not_found ->
      !logger.debug "No worker for %a: should not run"
        N.fq_print fq ;
      false
  | worker ->
      if not (worker_should_run conf worker) then (
        !logger.debug "Worker %a should not run" N.fq_print fq ;
        false
      (* Is that instance for an obsolete worker? *)
      ) else if worker.worker_signature <> worker_sign then (
        !logger.debug "Instance %s is obsolete and should not run"
          worker.worker_signature ;
        false
      ) else
        true

(* As the worker may be gone, look at the sources.
 * Fail if that info is not available. *)
let has_parents session fq =
  let clt = option_get "has_parents" __LOC__ session.ZMQClient.clt in
  let prog_name, func_name = N.fq_parse fq in
  let src_path = N.src_path_of_program prog_name in
  let info, _ = get_precompiled clt src_path in
  match info.detail with
  | Compiled info ->
      let func =
        List.find (fun func ->
          func.Value.SourceInfo.name = func_name
        ) info.Value.SourceInfo.funcs in
      O.parents_of_operation func.operation <> []
  | _ ->
      Printf.sprintf2 "Cannot find compiled source info for %a" N.fq_print fq |>
      failwith

let may_kill conf ~while_ session site fq worker_sign pid =
  let clt = option_get "may_kill" __LOC__ session.ZMQClient.clt in
  let per_instance_key = per_instance_key site fq worker_sign in
  let last_killed_k = per_instance_key LastKilled in
  let prev_last_killed =
    find_or_fail "a float" clt last_killed_k (function
      | None -> Some 0.
      | Some (Value.RamenValue T.(VFloat t)) -> Some t
      | _ -> None) in
  let last_killed = ref prev_last_killed in
  let input_ringbuf_k = per_instance_key InputRingFile in
  (match (Client.find clt input_ringbuf_k).value with
  | exception Not_found ->
      (* This is expected from workers with no parents, much less so
       * from workers with parents: *)
      if try has_parents session fq with _ -> true then
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
    report_worker_death session ~while_ site fq worker_sign "vanished" pid ;
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
    ZMQClient.send_cmd ~eager:true session
      (SetKey (last_killed_k, Value.of_float !last_killed)))

(* This worker is considered running as soon as it has a pid: *)
let is_running clt site fq worker_sign =
  Client.(Tree.mem clt.h (per_instance_key site fq worker_sign Pid))

(* First we need to compile (or use a cached of) the source info, that
 * we know from the SourcePath set by the Choreographer.
 * Then for each parent we need the required fieldmask, that the
 * Choreographer also should have set, and update those parents out_ref.
 * Then we can spawn that binary, with the parameters also set by
 * the choreographer. *)
let try_start_instance conf session ~while_ site fq worker =
  let clt = option_get "try_start_instance" __LOC__ session.ZMQClient.clt in
  let prog_name, func_name = N.fq_parse fq in
  let src_path = N.src_path_of_program prog_name in
  let info, precompiled =
    get_precompiled clt src_path in
  (* Check that info has the proper signature: *)
  let info_sign = Value.SourceInfo.signature info in
  if worker.Value.Worker.info_signature <> info_sign then
    Printf.sprintf "Invalid signature for info: expected %S but got %S"
      worker.info_signature info_sign |>
    failwith ;
  let bin_file = get_executable conf session worker.info_signature in
  !logger.info "Must execute %a for %a, function %a"
    N.path_print bin_file
    Value.Worker.print worker
    N.fq_print fq ;
  let func_of_precompiled precompiled fname =
    List.find (fun f -> f.VSI.name = fname)
      precompiled.VSI.funcs in
  let func_of_ref ref =
    let src_path =
      N.src_path_of_program ref.Func_ref.DessserGen.program in
    let _info, precompiled =
      get_precompiled clt src_path in
    ref.program,
    func_of_precompiled precompiled ref.func in
  let func = func_of_precompiled precompiled func_name in
  let children =
    Array.map func_of_ref worker.children in
  let envvars =
    Array.fold_left (fun lst name ->
      (name, Sys.getenv_opt (name : N.field :> string)) :: lst
    ) [] worker.envvars in
  let log_level =
    if worker.debug then Debug else Normal in
  (* Workers use local files/ringbufs which name depends on input and/or
   * output types, operation, etc, and that must be stored alongside the
   * pid for later manipulation since they would not be easy to recompute
   * should the Worker config entry change. Esp, other services might
   * want to know them and, again, would have a hard time recomputing
   * them in the face of a Worker change.
   * Therefore it's much simpler to store those paths in the config tree. *)
  (* FIXME: that it has no parent yet does not mean it selects from nobody!
   * yet Skeletons assumes that! *)
  let input_ringbuf =
    if worker.parents = None then None
    else Some (Paths.in_ringbuf_name conf.C.persist_dir prog_name func)
  and state_file =
    Paths.state_file_path conf.C.persist_dir src_path worker.worker_signature
  and parent_links =
    Array.map (fun pref ->
      let _pname, pfunc = func_of_ref pref in
      Value.Worker.fq_of_ref pref,
      RamenFieldMaskLib.make_fieldmask pfunc.VSI.operation func.VSI.operation,
      O.scalar_filters_of_operation pfunc.VSI.operation func.VSI.operation
    ) (worker.parents |? [||]) in
  let pid =
    start_worker
      conf ~while_ session prog_name func worker.params envvars worker.role
      log_level worker.report_period worker.cwd worker.worker_signature bin_file
      parent_links children input_ringbuf state_file in
  let per_instance_key = per_instance_key site fq worker.worker_signature in
  let k = per_instance_key LastKilled in
  if Client.mem clt k then
    ZMQClient.send_cmd ~eager:true session (DelKey k) ;
  let k = per_instance_key Pid in
  ZMQClient.send_cmd ~eager:true session
                     (SetKey (k, Value.(of_u32 pid))) ;
  let k = per_instance_key StateFile
  and v = Value.(of_string (state_file :> string)) in
  ZMQClient.send_cmd ~eager:true session (SetKey (k, v)) ;
  Option.may (fun input_ringbuf ->
    let k = per_instance_key InputRingFile
    and v = Value.(RamenValue (Raql_value.VString (input_ringbuf : N.path :> string))) in
    ZMQClient.send_cmd ~eager:true session (SetKey (k, v))
  ) input_ringbuf

let remove_dead_chans conf session replayer_k replayer =
  let clt = option_get "remove_dead_chans" __LOC__ session.ZMQClient.clt in
  (* Amongst the replayer channels, select those that still have a Replay: *)
  let channels, some_missing =
    Array.fold (fun (channels, some_missing) (chan, del_when_done) ->
      let replay_k = Key.Replays chan in
      if Client.mem clt replay_k then
        Set.add (chan, del_when_done) channels, some_missing
      else
        channels, true
    ) (Set.empty, false) replayer.VR.channels in
  let channels = Set.to_array channels in
  (* Write the new channels list, and kill the replayer if all channels are gone: *)
  if some_missing then
    let last_killed = ref replayer.last_killed in
    if array_is_empty channels then (
      match replayer.pid with
      | None ->
          !logger.debug "Replays stopped before replayer even started"
      | Some pid ->
          let pid = Uint32.to_int pid in
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
    ZMQClient.send_cmd ~eager:true session
      (UpdKey (replayer_k, replayer))

let update_replayer_status
      conf session now site fq replayer_id replayer_k replayer =
  let clt = option_get "update_replayer_status" __LOC__ session.ZMQClient.clt in
  let rem_replayer () =
    ZMQClient.send_cmd session (DelKey replayer_k) in
  match replayer.VR.pid with
  | None ->
      (* Maybe start it? *)
      if now -. replayer.creation > delay_before_replay then
        if
          array_is_empty replayer.channels ||
          TimeRange.is_empty replayer.time_range
        then
          (* Do away with it *)
          rem_replayer ()
        else (
          try
            let since, until = TimeRange.bounds replayer.time_range in
            let worker = find_worker session site fq in
            let bin =
              get_executable conf session worker.info_signature in
            let _prog, _prog_name, func = function_of_fq clt fq in
            !logger.info
              "Starting a %a replayer created %gs ago for channels %a"
              N.fq_print fq
              (now -. replayer.creation)
              (Array.print (Tuple2.print Channel.print Bool.print)) replayer.channels ;
            let pid =
              start_replayer
                conf fq func bin since until replayer.channels replayer_id in
            let v = Value.Replayer {
                      replayer with pid = Some (Uint32.of_int pid) } in
            ZMQClient.send_cmd ~eager:true session
              (UpdKey (replayer_k, v)) ;
            Histogram.add (stats_chans_per_replayer conf.C.persist_dir)
                          (float_of_int (Array.length replayer.channels))
          with exn ->
            !logger.error "Giving up replayer %s for channels %a: %s"
              (Uint32.to_string replayer_id)
              (Array.print (Tuple2.print Channel.print Bool.print))
                replayer.channels
              (Printexc.to_string exn) ;
            rem_replayer ()
        )
  | Some pid ->
      let pid = Uint32.to_int pid in
      if replayer.exit_status <> None then (
        (* XXX *)
        if array_is_empty replayer.channels then rem_replayer ()
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
            (if is_err then !logger.error else C.info_or_test conf)
              "%s %s." what status_str ;
            if is_err then
              IntCounter.inc (stats_replayer_crashes conf.C.persist_dir) ;
            let replayer =
              Value.Replayer { replayer with exit_status = Some status_str } in
            ZMQClient.send_cmd ~eager:true session
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
    let clt = option_get "synchronize_once" __LOC__ session.ZMQClient.clt in
    let prefix = "sites/"^ (conf.C.site :> string) ^"/workers/" in
    Client.iter_safe clt ~prefix (fun k hv ->
      (* try_start_instance can take some time so better skip it at exit: *)
      if while_ () && key_is_safe k now then
        try
          match k, hv.Client.value with
          | Key.PerSite (site, PerWorker (fq, PerInstance (worker_sign, Pid))),
            Value.RamenValue T.(VU32 pid)
            when site = conf.C.site ->
              let pid = Uint32.to_int pid in
              let still_running =
                update_child_status conf session ~while_ site fq worker_sign pid in
              if still_running &&
                 (not (should_run conf session site fq worker_sign) ||
                  is_quarantined clt site fq worker_sign)
              then
                may_kill conf ~while_ session site fq worker_sign pid
          | Key.PerSite (site, PerWorker (fq, Worker)),
            Value.Worker worker
            when site = conf.C.site ->
              !logger.debug "Considering worker for %a" N.fq_print fq ;
              let reason cause b =
                if not b then !logger.debug "Lacking condition: %s" cause ;
                b in
              if reason "should run" (worker_should_run conf worker) &&
                 reason "is precompiled"
                   (has_info session fq worker.info_signature) &&
                 reason "has executable"
                   (has_executable conf session worker.info_signature) &&
                 reason "already running"
                   (not (is_running clt site fq
                                    worker.worker_signature)) &&
                 reason "quarantine"
                   (not (is_quarantined clt site fq
                                        worker.worker_signature))
              then (
                try_start_instance conf session ~while_ site fq worker ;
                (* The above is slow enough that this could be needed: *)
                ZMQClient.may_send_ping session ;
                (* If we have many programs to compile in this loop better
                 * reset the watchdog: *)
                Option.may Watchdog.reset !watchdog
              )
          | Key.PerSite (site, PerWorker (fq, PerReplayer id)) as replayer_k,
            Value.Replayer replayer
            when site = conf.C.site ->
              remove_dead_chans conf session replayer_k replayer ;
              update_replayer_status
                conf session now site fq id replayer_k replayer
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
  let clt = option_get "mass_kill_all" __LOC__ session.ZMQClient.clt in
  let prefix = "sites/"^ (conf.C.site :> string) ^"/workers/" in
  let pids =
    Client.fold clt ~prefix (fun k hv pids ->
      match k, hv.Client.value with
      | Key.PerSite (site, PerWorker (fq, PerInstance (_, Pid))),
        Value.RamenValue T.(VU32 pid)
        when site = conf.C.site ->
          let pid = Uint32.to_int pid in
          let what = Printf.sprintf2 "Killing %a" N.fq_print fq in
          C.info_or_test conf "%s" what ;
          log_and_ignore_exceptions ~what (Unix.kill pid) Sys.sigterm ;
          Set.Int.add pid pids
      | _ -> pids
    ) Set.Int.empty in
  !logger.info "Waiting for all workers to terminate..." ;
  waitall ~what:"workers termination" ~expected_status:0 pids ;
  !logger.debug "All process have stopped."

(* At start, assume pre-existing pids are left-overs from a previous run, but
 * if synchronize_running is restarted because of some exception then do not
 * assume any longer that previous workers are not running: *)
let previous_pids_are_running = ref false

let synchronize_running ?(while_=always) conf kill_at_exit =
  let loop session =
    let last_sync = ref 0. in
    (* Whatever happen (such as disconnection from confserver), don't exit without
       honoring the kill_at_exit flag: *)
    finally
      (fun () -> if kill_at_exit then mass_kill_all conf session)
      (fun () ->
        while while_ () do
          Option.may Watchdog.reset !watchdog ;
          let max_count = 30 in (* Need to reset the watchdog from time to time *)
          ZMQClient.process_in ~while_ ~max_count session ;
          let now = Unix.gettimeofday () in
          if now > !last_sync +. delay_between_worker_syncs then (
            last_sync := now ;
            synchronize_once conf session ~while_ now)
        done) () in
  let topics =
    [ (* All sites are needed because we need parent worker :(
         TODO: add whatever is needed from the parents (ie. output type)
         in this site workers so we need to listen at only the local
         site. *)
      "sites/*/workers/*" ;
      "sites/*/programs/*/executable" ;
      "sources/*/info" ;
      (* Replays are read directly. Would not add much to go through the
       * Choreographer but latency. *)
      "replays/*" ;
      (* Currently part of the above sites/*/workers/* but see TODO:
       * "sites/"^ (conf.C.site :> string) ^"/workers/*/replayers/*" *)] in
  (* Setting up/Tearing down replays is easier when they are added/removed: *)
  let on_del session k v =
    match k, v with
    | Key.Replays _chan,
      Value.Replay replay ->
        (* Replayers chan list will be updated in the loop but we need the replay
         * here to teardown all the links: *)
        Replay.teardown_links conf session replay
    | _ -> ()
  and on_new session k v _uid _mtime _can_write _can_del _owner _expiry =
    let clt = option_get "on_new" __LOC__ session.ZMQClient.clt in
    match k, v with
    | Key.Replays chan,
      Value.Replay replay
      when replay.Value.Replay.timeout_date >= Unix.time () ->
        let replay_range =
          TimeRange.make replay.Value.Replay.since replay.until false in
        let func_of_fq fq =
          let prog_name, _func_name = N.fq_parse fq in
          let _prog, _prog_name, func = function_of_fq clt fq in
          prog_name, func in
        (* Normally, the target worker will count the EndOfReplay and clean the
         * replay and response keys when done. When a single replayer pours
         * directly the target archive into the response key though it has to
         * do the cleaning itself: *)
        let del_when_done = Replay.is_mono_replayer replay in
        Replay.setup_links conf ~while_ session func_of_fq replay ;
        (* Find or create all replayers: *)
        Array.iter (fun source ->
          let fq =
            N.fq_of_program source.Fq_function_name.DessserGen.program
                            source.function_ in
          if source.site = conf.C.site then (
            (* Find all pre-existing replayers for that function: *)
            let prefix = "sites/"^ (source.site :> string) ^"/workers/"^
                         (source.program :> string) ^"/"^
                         (source.function_ :> string) ^"/replayers/" in
            let rs =
              Client.fold clt ~prefix (fun k hv rs ->
                match k, hv.value with
                | Key.PerSite (site', PerWorker (fq', PerReplayer _id)) as k,
                  Value.Replayer replayer
                  when source.site = site' && N.eq fq fq' ->
                    (k, replayer) :: rs
                | _ -> rs
              ) [] in
            (* Now look in this list for a replayer that have not started yet and
             * that includes this time range: *)
            match List.find (fun (_, r) ->
                    r.VR.pid = None &&
                    TimeRange.approx_eq replay_range r.time_range
                  ) rs with
            | exception Not_found ->
                let id =
                  Uint32.of_int (Random.int RingBufLib.max_replayer_id) in
                let now = Unix.gettimeofday () in
                let channels = [| chan, del_when_done |] in
                let r = VR.make now replay_range channels in
                let replayer_k =
                  Key.PerSite (source.site, PerWorker (fq, PerReplayer id)) in
                ZMQClient.send_cmd ~eager:true session
                  (NewKey (replayer_k, Value.Replayer r, 0., false))
            | k, r ->
                !logger.debug
                  "Adding replay for channel %a into replayer created at %a"
                  Channel.print chan print_as_date r.creation ;
                let time_range = TimeRange.merge r.time_range replay_range
                and channels = array_add (chan, del_when_done) r.channels in
                let replayer = Value.Replayer { r with time_range ; channels } in
                ZMQClient.send_cmd ~eager:true session
                  (UpdKey (k, replayer)))
        ) replay.sources
    | _ -> ()
  (* When supervisor restarts it must clean the configuration from all
   * remains of previous workers, that must have been killed since last
   * run and that could not only confuse supervisor, but also cause it to
   * not start workers and/or kill random processes.
   * This is OK because no workers are started in [on_new] but only later
   * in the [loop] function.
   * While at it, also make sure that all referenced ringbuffers are valid
   * (no producer and no consumer) *)
  and on_synced session =
    let clt = option_get "on_synced" __LOC__ session.ZMQClient.clt in
    if !previous_pids_are_running then
      !logger.debug "Assume previous workers are still running and keep \
                     already defined pids"
    else (
      previous_pids_are_running := true ;
      let prefix = "sites/"^ (conf.C.site :> string) ^"/workers/" in
      Client.iter_safe clt ~prefix (fun k hv ->
        match k, hv.value  with
        | Key.PerSite (site, PerWorker (fq, PerInstance (worker_sign, Pid))),
          Value.RamenValue T.(VU32 pid)
          when site = conf.C.site ->
            let pid = Uint32.to_int pid in
            !logger.warning "Deleting remains of a previous worker pid %d" pid ;
            report_worker_death session ~while_ site fq worker_sign "restarted" pid
        | Key.PerSite (site, PerWorker (_, PerReplayer _)) as replayer_k,
          Value.Replayer _
          when site = conf.C.site ->
            ZMQClient.send_cmd session (DelKey replayer_k)
        | Key.PerSite (site, PerWorker (_, OutputSpecs)),
          Value.OutputSpecs specs
          when site = conf.C.site ->
            Hashtbl.iter (fun recipient file_spec ->
              match file_spec.Value.OutputSpecs.file_type, recipient with
              | RingBuf, Output_specs_wire.DessserGen.DirectFile fname ->
                  if Files.exists fname then
                    check_ringbuffer conf fname ;
              | _ -> ()
            ) specs
        | _ -> ())
    )
  in
  (* Timeout has to be much shorter than delay_before_replay *)
  let timeo = min 1. (delay_before_replay *. 0.5) in
  if !watchdog = None then watchdog :=
    Some (Watchdog.make ~timeout:300. "synchronize workers" Processes.quit) ;
  let watchdog = Option.get !watchdog in
  Watchdog.enable watchdog ;
  start_sync conf ~while_ ~topics ~recvtimeo:timeo
             ~sesstimeo:Default.sync_long_sessions_timeout
             ~on_new ~on_del ~on_synced loop ;
  Watchdog.disable watchdog
