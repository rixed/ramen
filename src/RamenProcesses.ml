(* Process supervisor which task is to start and stop workers, make sure they
 * keep working, collect their stats and write them somewhere for anybody
 * to see, send their notifications, delete old unused files...
 *)
open Batteries
open Lwt
open RamenLog
open RamenHelpers
module C = RamenConf
module F = RamenConf.Func

(* Global quit flag, set when the term signal is received: *)

let quit = ref false
let report_period = ref RamenConsts.default_report_period

(*
 * Machinery to spawn other programs.
 *)

let fd_of_int : int -> Unix.file_descr = Obj.magic

let close_fd i =
  Unix.close (fd_of_int i)

let run_background ?cwd cmd args env =
  let open Unix in
  (* prog name should be first arg *)
  let prog_name = Filename.basename cmd in
  let args = Array.init (Array.length args + 1) (fun i ->
      if i = 0 then prog_name else args.(i-1))
  in
  let quoted oc s = Printf.fprintf oc "%S" s in
  !logger.info "Running %s as: /usr/bin/env %a %S %a"
    cmd
    (Array.print ~first:"" ~last:"" ~sep:" " quoted) env
    cmd
    (Array.print ~first:"" ~last:"" ~sep:" " quoted) args ;
  flush_all () ;
  match fork () with
  | 0 ->
    Option.may chdir cwd ;
    close_fd 0 ;
    for i = 3 to 255 do
      try close_fd i with Unix.Unix_error (Unix.EBADF, _, _) -> ()
    done ;
    execve cmd args env
  | pid -> pid

(*
 * Notifications:
 * To alleviate workers from the hassle to send HTTP notifications, those are
 * sent to Ramen via a ringbuffer. Advantages are many:
 * Workers do not need an HTTP client and are therefore smaller, faster to
 * link, and easier to port to another language. Also, other notification
 * mechanisms are easier to implement in a single location.
 *)

let process_notifications rb =
  RamenSerialization.read_notifs rb (fun (worker, url) ->
    !logger.info "Received notify instruction from %s to %s"
      worker url ;
    RamenHttpHelpers.http_notify url)

let cleanup_old_files max_archives conf =
  (* Have a list of directories and regexps and current version,
   * Iter through this list for file matching the regexp and that are also directories.
   * If this direntry matches the current version, touch it.
   * If not, and if it hasn't been touched for X days, assume that's an old one and delete it.
   * Then sleep for one day and restart. *)
  let get_log_file () =
    Unix.gettimeofday () |> Unix.localtime |> log_file
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
        "configuration", v_regexp, RamenVersions.graph_config ;
        "instrumentation_ringbuf", v1v2_regexp, (RamenVersions.instrumentation_tuple ^"_"^ RamenVersions.ringbuf) ;
        "workers/ringbufs", v_regexp, RamenVersions.ringbuf ;
        "workers/out_ref", v_regexp, RamenVersions.out_ref ;
        "workers/states", v_regexp, RamenVersions.worker_state ]
    in
    !logger.info "Cleaning old unused files..." ;
    let%lwt () = Lwt_list.iter_s cleanup_dir to_clean in
    (* Clean old archives *)
    let arcdir =
      conf.C.persist_dir ^"/workers/ringbufs/"^ RamenVersions.ringbuf in
    let clean_seq_archives dir =
      (* Delete all files matching %d-%d.r but the last ones: *)
      let files = RingBuf.seq_files_of dir |> Array.of_enum in
      Array.fast_sort RingBuf.seq_file_compare files ;
      for i = 0 to Array.length files - max_archives do
        let _, _, f = files.(i) in
        let fname = dir ^"/"^ f in
        !logger.info "Deleting old archive %s" fname ;
        log_exceptions Unix.unlink fname
      done
    and clean_time_archives dir =
      (* Delete all time link with a link ref count at 1 *)
      RingBuf.time_files_of dir //@
      (fun (_, _, f) ->
        let fname = dir ^"/"^ f in
        let s = Unix.stat fname in
        if s.Unix.st_nlink = 1 then Some fname else None) |>
      Enum.iter (fun fname ->
        !logger.info "Deleting old time index %s" fname ;
        log_exceptions Unix.unlink fname)
    in
    let on_dir fname rel_fname =
      let basename = Filename.basename rel_fname in
      if basename = "per_time" then
        clean_time_archives fname
      else if basename = "per_seq" then
        clean_seq_archives fname
    in
    dir_subtree_iter ~on_dir arcdir ;
    Lwt_unix.sleep 3600. >>= loop
  in
  loop ()

(*
 * Process Supervisor: Start and stop according to a mere file listing which
 * programs to run.
 *
 * Also monitor quit.
 *)

(* Description of a running worker.
 * Non persisted on disk. *)
type running_process =
  { program_name : string ;
    bin : string ;
    func : C.Func.t ;
    mutable pid : int option ;
    mutable last_killed : float (* 0 for never *) ;
    (* purely for reporting: *)
    mutable last_exit : float ;
    mutable last_exit_status : string ;
    mutable succ_failures : int }

let print_running_process oc proc =
  Printf.fprintf oc "%s/%s (params=%a, parents=%a)"
    proc.program_name proc.func.F.name
    (List.print RamenTuple.print_param) proc.func.params
    (List.print F.print_parent) proc.func.parents

let make_running_process program_name bin func =
  { program_name ; bin ; pid = None ; last_killed = 0. ; func ;
    last_exit = 0. ; last_exit_status = "" ; succ_failures = 0 }

(* Returns the name of func input ringbuf for the given parent (if func is
 * merging, each parent uses a distinct one) and the file_spec. *)
let input_spec conf parent child =
  (* In case of merge, ringbufs are numbered as the node parents: *)
  (if child.F.merge_inputs then
    match List.findi (fun i (pprog, pname) ->
             pprog = parent.F.program_name && pname = parent.name
           ) child.parents with
    | exception Not_found ->
        !logger.error "Operation %S is not a child of %S"
          child.name parent.name ;
        invalid_arg "input_spec"
    | i, _ ->
        C.in_ringbuf_name_merging conf child i
  else C.in_ringbuf_name_single conf child),
  let out_type = parent.out_type.ser
  and in_type = child.in_type.ser in
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
        if f1.typ <> f2.typ then
          failwith ("Fields "^ f1.typ_name ^" have not the same type") ;
        if f1.nullable <> f2.nullable then
          failwith ("Fields "^ f1.typ_name ^" differs with regard to NULLs")
  ) t1

(* Returns the running parents and children of a func: *)
let relatives f must_run =
  Hashtbl.fold (fun _sign (_bin, _program_name, func) (ps, cs) ->
    let is_parent_of func func' =
      List.mem (func'.F.program_name, func'.F.name) func.F.parents in
    (if is_parent_of f func then func::ps else ps),
    (if is_parent_of func f then func::cs else cs)
  ) must_run ([], [])

(* Try to start the given proc.
 * Check links (ie.: do parents and children have the proper types?)
 *
 * Need [must_run] so that types can be checked before linking with parents
 * and children.
 *)
let try_start conf must_run proc =
  !logger.info "Starting operation %a"
    print_running_process proc ;
  assert (proc.pid = None) ;
  let ps, cs = relatives proc.func must_run in
  (* We must not start if a parent is missing: *)
  let parents_ok =
    List.fold_left (fun ok (p_prog, p_func) ->
      if List.exists (fun func ->
           func.F.program_name = p_prog && func.F.name = p_func
         ) ps then ok
      else (
        !logger.error "Parent of %s/%s is missing: %s/%s"
          proc.program_name proc.func.name
          p_prog p_func ;
        false)
    ) true proc.func.parents in
  let check_linkage p c =
    try check_is_subtype c.F.in_type.RamenTuple.ser p.F.out_type.ser ;
        true
    with Failure msg ->
      !logger.error "Input type of %s/%s (%a) is not compatible with \
                     output type of %s/%s (%a): %s"
        c.program_name c.name RamenTuple.print_typ c.in_type.ser
        p.program_name p.name RamenTuple.print_typ p.out_type.ser
        msg ;
      false in
  let linkage_ok =
    List.fold_left (fun ok p ->
      check_linkage p proc.func && ok) true ps in
  let linkage_ok =
    List.fold_left (fun ok c ->
      check_linkage proc.func c && ok) linkage_ok cs in
  if parents_ok && linkage_ok then (
    (* Create the input ringbufs.
     * We now start the workers one by one in no
     * particular order. The input and out-ref ingbufs are created when the
     * worker start, and the out-ref is filled with the running (or should
     * be running) children. Therefore, if a children fails to run the
     * parents might block. Also, if a child has not been started yet its
     * inbound ringbuf will not exist yet, again implying this worker will
     * block.
     * Each time a new worker is started or stopped the parents outrefs
     * are updated. *)
    !logger.debug "Creating in buffers..." ;
    let input_ringbufs = C.in_ringbuf_names conf proc.func in
    List.iter (fun rb_name ->
      RingBuf.create rb_name RingBufLib.rb_words
    ) input_ringbufs ;
    (* And the pre-filled out_ref: *)
    !logger.debug "Updating out-ref buffers..." ;
    let out_ringbuf_ref = C.out_ringbuf_names_ref conf proc.func in
    let%lwt () =
      Lwt_list.iter_s (fun c ->
        let fname, specs as out = input_spec conf proc.func c in
        (* The destination ringbuffer must exist before it's referenced in an
         * out-ref, or the worker might err and throw away the tuples: *)
        RingBuf.create fname RingBufLib.rb_words ;
        RamenOutRef.add out_ringbuf_ref out
      ) cs in
    (* Now that the out_ref exists, but before we actually fork the worker,
     * we can start importing: *)
    let%lwt () =
      if proc.func.F.force_export then
        let%lwt _ = RamenExport.make_temp_export conf proc.func in
        return_unit
      else return_unit in
    (* Now actually start the binary *)
    !logger.info "Start %s" proc.func.F.name ;
    let notify_ringbuf =
      (* Where that worker must write its notifications. Normally toward a
       * ringbuffer that's read by Ramen, unless it's a test programs.
       * Tests must not send their notifications to Ramen with real ones,
       * but instead to a ringbuffer specific to the test_id. *)
      C.notify_ringbuf (* TODO ~test_id:program.test_id *) conf in
    let ocamlrunparam =
      getenv ~def:(if conf.C.debug then "b" else "") "OCAMLRUNPARAM" in
    let fq_name = proc.program_name ^"/"^ proc.func.name in
    let env = [|
      "OCAMLRUNPARAM="^ ocamlrunparam ;
      "debug="^ string_of_bool conf.C.debug ;
      "name="^ proc.func.F.name ; (* Used to choose the function to perform *)
      "fq_name="^ fq_name ; (* Used for monitoring *)
      "signature="^ proc.func.signature ;
      "input_ringbufs="^ String.concat "," input_ringbufs ;
      "output_ringbufs_ref="^ out_ringbuf_ref ;
      "report_ringbuf="^ C.report_ringbuf conf ;
      "report_period="^ string_of_float !report_period ;
      "notify_ringbuf="^ notify_ringbuf ;
      (* We need to change this dir whenever the func signature or params
       * change to prevent it to reload an incompatible state: *)
      "persist_dir="^ conf.C.persist_dir ^"/workers/states/"
                    ^ RamenVersions.worker_state
                    ^"/"^ Config.version
                    ^"/"^ fq_name
                    ^"/"^ proc.func.F.signature
                    ^"/"^ RamenTuple.param_signature proc.func.F.params ;
      (match !logger.logdir with
        | Some _ ->
          "log_dir="^ conf.C.persist_dir ^"/log/workers/" ^ fq_name
        | None -> "no_log_dir=") |] in
    (* Pass each individual parameter as a separate envvar; envvars are just
     * non interpreted strings (but for the first '=' sign that will be
     * interpreted by the OCaml runtime) so it should work regardless of the
     * actual param name or value, and make it easier to see what's going
     * on fro the shell: *)
    let params =
      List.enum proc.func.params /@
      (fun (n, v) -> Printf.sprintf2 "param_%s=%a" n RamenScalar.print v) |>
      Array.of_enum in
    let env = Array.append env params in
    let args =
      (* For convenience let's add "ramen worker" and the fun name as
       * arguments: *)
      [| "(ramen worker)" ; fq_name |] in
    (* Better have the workers CWD where the binary is, so that any file name
     * mentioned in the program is relative to the program. *)
    let cwd = Filename.dirname proc.bin in
    let cmd = Filename.basename proc.bin in
    let%lwt pid =
      wrap (fun () -> run_background ~cwd cmd args env) in
    !logger.debug "Function %s now runs under pid %d" fq_name pid ;
    proc.pid <- Some pid ;
    (* Monitor this worker, wait for its termination, restart...: *)
    async (fun () ->
      let rec wait_child () =
        match%lwt Lwt_unix.waitpid [] pid with
        | exception Unix.Unix_error (Unix.EINTR, _, _) -> wait_child ()
        | exception exn ->
          (* This should not be used *)
          !logger.error "Cannot wait for pid %d: %s"
            pid (Printexc.to_string exn) ;
          return_unit
        | _, status ->
          let status_str = string_of_process_status status in
          !logger.info "Operation %s/%s (pid %d) %s."
            proc.program_name proc.func.name pid status_str ;
          proc.last_exit <- Unix.gettimeofday () ;
          proc.last_exit_status <- status_str ;
          proc.succ_failures <- proc.succ_failures + 1 ;
          proc.pid <- None ;
          return_unit
      in
      wait_child ()) ;
    (* Update the parents out_ringbuf_ref: *)
    Lwt_list.iter_p (fun p ->
      let out_ref =
        C.out_ringbuf_names_ref conf p in
      RamenOutRef.add out_ref (input_spec conf p proc.func)
    ) ps
  ) else return_unit

let try_kill conf must_run proc =
  let pid = Option.get proc.pid in
  (* There is no reason to wait before we remove this worker from its
   * parent out-ref: if it's not replaced then the last unprocessed
   * tuples are lost. If it's indeed a replacement then the new version
   * will have a chance to process the left overs. *)
  let ps, _cs = relatives proc.func must_run in
  (* We must also remove this proc from all its parent out-ref. *)
  let input_ringbufs = C.in_ringbuf_names conf proc.func in
  let%lwt () =
    Lwt_list.iter_p (fun p ->
      let parent_out_ref =
        C.out_ringbuf_names_ref conf p in
      Lwt_list.iter_s (fun this_in ->
        RamenOutRef.remove parent_out_ref this_in) input_ringbufs
    ) ps in
  (* Start with a TERM (if last_killed = 0.) and then after 5s send a
   * KILL if the worker is still running. *)
  let now = Unix.gettimeofday () in
  if proc.last_killed = 0. then (
    (* Ask politely: *)
    !logger.info "Terminating worker %s/%s (pid %d)"
      proc.program_name proc.func.name pid ;
    log_exceptions ~what:"Terminating worker"
      (Unix.kill pid) Sys.sigterm ;
    proc.last_killed <- now ;
  ) else if now -. proc.last_killed > 5. then (
    !logger.warning "Killing worker %s/%s (pid %d) with bigger guns"
      proc.program_name proc.func.name pid ;
    log_exceptions ~what:"Killing worker"
      (Unix.kill pid) Sys.sigkill ;
    proc.last_killed <- now ;
  ) ;
  return_unit

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
 * different input type then the tuples will switch toward the new instance
 * as the parent out-ref gets updated.  * Similarly, they use different state
 * files.  They might both be present in a parent out_ref though, so some
 * duplication of tuple is possible (or conversely: some input tuples might
 * be missing if we kill the previous before starting the new one).
 * If they do share the same input ringbuf though, it is important that we
 * remove the former one before we add the new one (or the parent out-ref
 * will end-up empty). This is why we first do all the kills, then all the
 * starts (Note that even if a worker does not terminate immediately on
 * kill, its parent out-ref is cleaned immediately).
 *)
let synchronize_running conf autoreload_delay =
  let rc_file = C.running_config_file conf in
  (* Start/Stop processes so that [running] corresponds to [must_run].
   * [must_run] is a hash from the signature (function * params) to
   * its binary path, the program name and Func.
   * FIXME: do we need the program_name since it's now also in func?
   * [running] is a hash from the signature (function * params) to its
   * running_process (mutable pid, cleared asynchronously when the worker
   * terminates). *)
  let synchronize must_run running =
    (* First, remove from running all terminated processes that must not run
     * any longer. Send a kill to those that are still running. *)
    let prev_nb_running = Hashtbl.length running in
    let to_kill = ref [] and to_start = ref []
    and (+=) r x = r := x :: !r in
    Hashtbl.filteri_inplace (fun sign proc ->
      if Hashtbl.mem must_run sign then true else
      if proc.pid <> None then (to_kill += proc ; true) else
      false
    ) running ;
    (* Then, add/restart all those that must run. *)
    Hashtbl.iter (fun sign (bin, program_name, func) ->
      match Hashtbl.find running sign with
      | exception Not_found ->
          let proc = make_running_process program_name bin func in
          Hashtbl.add running sign proc ;
          to_start += proc
      | proc ->
          (* If it's dead, restart it: *)
          if proc.pid = None then to_start += proc else
          (* If we were killing it, it's safer to keep killing it until it's
           * dead and then restart it. *)
          if proc.last_killed <> 0. then to_kill += proc
    ) must_run ;
    let nb_running = Hashtbl.length running in
    if !quit && nb_running > 0 && nb_running <> prev_nb_running then
      !logger.info "Still %d processes running"
        (Hashtbl.length running) ;
    join [
      Lwt_list.iter_p (try_kill conf must_run) !to_kill ;
      Lwt_list.iter_p (try_start conf must_run) !to_start ]
  in
  (* Once we have forked some workers we must not allow an exception to
   * terminate this function or we'd leave unsupervised workers behind: *)
  let rec none_shall_pass f =
    try%lwt f ()
    with exn ->
      print_exception exn ;
      !logger.error "Crashed while supervising children, keep trying!" ;
      let%lwt () = Lwt_unix.sleep 1. in
      none_shall_pass f
  in
  let rec loop last_read must_run running =
    none_shall_pass (fun () ->
      if !quit && Hashtbl.length running = 0 then (
        !logger.info "All processes stopped, quitting." ;
        return_unit
      ) else (
        let%lwt must_run, last_read =
          if !quit then (
            return (Hashtbl.create 0, last_read)
          ) else (
            let last_mod =
              try mtime_of_file rc_file
              with Unix.(Unix_error (ENOENT, _, _)) -> max_float in
            let now = Unix.gettimeofday () in
            let must_autoreload =
                autoreload_delay > 0. &&
                now -. last_read >= autoreload_delay
            and must_reread =
               last_mod > last_read &&
               (* To prevent missing the last writes when the file is updated
                * several times a second (the possible resolution of mtime),
                * refuse to refresh the file unless last_mod is old enough. *)
               now -. last_mod > 1. in
            if must_autoreload || must_reread then (
              (* Reread the content of that file *)
              let%lwt must_run_programs = C.with_rlock conf return in
              (* The run file gives us the programs (and how to run them), but we
               * want [must_run] to be a hash of functions. Also, we want the
               * workers identified by their signature so that if the type of a
               * worker change but not its name we see a different worker. *)
              let must_run = Hashtbl.create 11 in
              let%lwt () = wrap (fun () ->
                Hashtbl.iter (fun program_name get_rc ->
                  let bin, prog = get_rc () in
                  List.iter (fun f ->
                    let k = f.F.signature, RamenTuple.param_signature f.F.params in
                    Hashtbl.add must_run k (bin, program_name, f)
                  ) prog
                ) must_run_programs) in
              return (must_run, last_mod)
            ) else return (must_run, last_read)) in
        let%lwt () = synchronize must_run running in
        let delay = if !quit then 0.1 else 1. in
        let%lwt () = Lwt_unix.sleep delay in
        loop last_read must_run running))
  in
  loop 0. (Hashtbl.create 0) (Hashtbl.create 0)

(* To be called before synchronize_running *)
let prepare_start conf =
  (* Prepare ringbuffers for reports and notifications: *)
  let rb_name = C.report_ringbuf conf in
  RingBuf.create ~wrap:false rb_name RingBufLib.rb_words ;
  let rb_name = C.notify_ringbuf conf in
  RingBuf.create rb_name RingBufLib.rb_words ;
  let notify_rb = RingBuf.load rb_name in
  (* Install signal handlers *)
  set_signals Sys.[sigterm; sigint] (Signal_handle (fun s ->
    !logger.info "Received signal %s" (name_of_signal s) ;
    quit := true)) ;
  notify_rb
