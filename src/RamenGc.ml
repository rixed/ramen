(* Deletes old data files according to storage configuration, and also
 * old data dir of functions that have not been running for a while, as well
 * as old versions of ramen configuration. *)
open Batteries
open Str
open Unix
open RamenLog
open RamenHelpers
open RamenConsts
module C = RamenConf
module RC = C.Running
module F = C.Func
module P = C.Program
module N = RamenName
module OutRef = RamenOutRef
module Files = RamenFiles
module Processes = RamenProcesses
module Versions = RamenVersions
module Watchdog = RamenWatchdog
module ZMQClient = RamenSyncZMQClient

let get_log_file () =
  gettimeofday () |> localtime |> log_file |> N.path

let date_regexp = regexp "^[0-9]+-[0-9]+-[0-9]+$"
let v_regexp = regexp "v[0-9]+"
let v1v2_regexp = regexp "v[0-9]+_v[0-9]+"

let cleanup_dir_old conf dry_run (dir, sub_re, current_version) =
  let dir = N.path_cat [ conf.C.persist_dir ; dir ] in
  !logger.debug "Cleaning directory %a..." N.path_print dir ;
  (* Error in there will be delivered to the stream reader: *)
  match Files.files_of dir with
  | exception (Unix_error (ENOENT, _, _) | Sys_error _) ->
      (* No such directory is OK: *)
      ()
  | exception exn ->
      !logger.error "Cannot list %a: %s"
        N.path_print dir (Printexc.to_string exn)
  | files ->
    Enum.iter (fun fname ->
      let full_path = N.path_cat [ dir ; fname ] in
      if fname = current_version then (
        if not dry_run then
          Files.touch full_path (gettimeofday ())
      ) else if string_match sub_re (fname :> string) 0 &&
                Files.is_directory full_path &&
                (* TODO: should be a few days *)
                Files.is_older_than ~on_err:false (1. *. 86400.) full_path
      then (
        !logger.info "Deleting %a: unused, old version%s"
          N.path_print fname (if dry_run then " (NOPE)" else "") ;
        if not dry_run then
          Files.rm_rf full_path
      )
    ) files

let cleanup_old_versions conf dry_run =
  (* Have a list of directories and regexps and current version,
   * Iter through this list for file matching the regexp and that are also
   * directories.
   * If this direntry matches the current version, touch it.
   * If not, and if it hasn't been touched for X days, assume that's an old
   * one and delete it. *)
  !logger.debug "Cleaning old versions..." ;
  let to_clean =
    [ N.path "log", date_regexp, get_log_file () ;
      N.path "log/workers", v_regexp, get_log_file () ;
      N.path "configuration", v_regexp, N.path Versions.rc ;
      N.path "instrumentation_ringbuf", v1v2_regexp,
        N.path Versions.(instrumentation_tuple ^"_"^ ringbuf) ;
      N.path "services", v_regexp, N.path Versions.services ;
      N.path "workers/ringbufs", v_regexp, N.path Versions.ringbuf ;
      N.path "workers/out_ref", v_regexp, N.path Versions.out_ref ;
      N.path "workers/states", v_regexp, N.path Versions.worker_state ;
      N.path "workers/factors", v_regexp, N.path Versions.factors ]
  in
  List.iter (cleanup_dir_old conf dry_run) to_clean

let clean_seq_archives del_ratio dry_run dir alloced =
  (* Delete oldest files matching %d_%d_%a_%a.r, until the worker is below
   * its allocated storage space, but not more than a given fraction of
   * what we should delete. *)
  (* Delete all files matching %d_%d_%a_%a.r but the last ones.
   * Also, for each of these, try to delete all attached factor files. *)
  let files = Files.files_of dir |> Array.of_enum in
  let arc_files =
    Array.enum files |> RingBufLib.filter_arc_files dir |>
    Array.of_enum in
  Array.fast_sort RingBufLib.arc_file_compare arc_files ;
  (* Older files come first in [arc_files].
   * Now find the allocated size for this worker: *)
  let rec loop i sum_sz num_to_del to_del =
    if i < 0 then num_to_del, to_del else
    let _, _, _, _, _, fpath as f = arc_files.(i) in
    let sum_sz = sum_sz + Files.size fpath in
    let num_to_del, to_del =
      if sum_sz <= alloced then num_to_del, to_del
      else num_to_del + 1, f::to_del in
    loop (i - 1) sum_sz num_to_del to_del in
  let num_to_del, to_del =
    if Array.length arc_files = 0 then 0, []
    else loop (Array.length arc_files - 1) 0 0 [] in
  (* We have at the head of to_del the oldest files. Delete some of them,
   * but not all of them at once: *)
  let num_to_del = round_to_int (float_of_int num_to_del *. del_ratio) in
  let rec del n = function
    | [] -> ()
    | (_, _, _, _, _, fpath) :: to_del->
        if n > 0 then (
          (* TODO: also check that we do not delete younger data than
           * planned. Ie. allocs must also tell the retention for each
           * function. *)
          !logger.info "Deleting %a: old archive%s"
            N.path_print fpath (if dry_run then " (NOPE)" else "") ;
          if not dry_run then (
            let pref = Files.(basename fpath |> remove_ext) in
            Array.iter (fun fname ->
              if N.starts_with fname pref then
                log_and_ignore_exceptions
                  Files.unlink (N.path_cat [ dir ; fname ])
            ) files
          ) ;
          del (n - 1) to_del
        ) in
  del num_to_del to_del

let get_alloced_special _fname _rel_fname =
  150_000_000 (* TODO *)

(* TODO: instrumentation for number of successful/failed compressions *)

let compress_archive (bin : N.path) (func_name : N.func) rb_name =
  let orc_name = Files.change_ext "orc" rb_name in
  let args =
    [| (bin :> string) ; WorkerCommands.convert_archive ;
       (func_name :> string) ; (rb_name :> string) ;
       (orc_name :> string) |] in
  Files.with_subprocess ~expected_status:0 bin args (fun (_ic, oc, ec) ->
    let output = Files.read_whole_channel oc
    and errors = Files.read_whole_channel ec in
    if errors = "" then (
      !logger.debug "Compressed %a into %a"
        N.path_print rb_name N.path_print orc_name ;
      ignore_exceptions Files.safe_unlink rb_name
    ) else
      !logger.error "Cannot compress archive %a with %a: %s"
        N.path_print rb_name N.path_print bin errors ;
    if output <> "" then !logger.debug "Output: %s" output)

let compress_old_archives conf worker_bins dry_run compress_older =
  (* Compress archives of every functions we can find in the RC file (running
   * or not) from ringbuf to ORC: *)
  !logger.debug "Compressing archives..." ;
  List.iter (fun (bin, func) ->
    C.archive_buf_name ~file_type:OutRef.RingBuf conf func |>
    RingBufLib.arc_dir_of_bname |>
    RingBufLib.arc_files_of |>
    Enum.iter (fun (_from, _to, _t1, _t2, arc_typ, fname) ->
      if arc_typ = RingBufLib.RingBuf &&
         Files.is_older_than ~on_err:false compress_older fname
      then (
        !logger.debug "Compressing %a%s"
          N.path_print fname (if dry_run then " (NOPE)" else "") ;
        if not dry_run then compress_archive bin func.F.name fname))
  ) worker_bins

let cleanup_once
      conf dry_run del_ratio compress_older get_alloced_worker worker_bins =
  !logger.info "Cleaning old unused files..." ;
  cleanup_old_versions conf dry_run ;
  if RamenExperiments.archive_in_orc.variant > 0 then
    compress_old_archives conf worker_bins dry_run compress_older ;
  (* Delete old archive files *)
  !logger.debug "Deleting old archives..." ;
  let on_dir get_alloced fname rel_fname =
    (* FIXME: what if a function or program name ends with ".arc"?
     * We should leave the GC explore freely under persist_dir, looking
     * for .gc files giving it instructions (max size and/or max age,
     * and/or account to given FQ. *)
    if Files.has_ext "arc" rel_fname then (
      match get_alloced fname rel_fname with
      | exception e ->
          (* Better not delete anything *)
          let what =
            Printf.sprintf2 "Cannot find allocated storage for archive %a"
              N.path_print rel_fname in
          print_exception ~what e
      | alloced ->
          !logger.debug "%a is allocated %d bytes"
            N.path_print rel_fname alloced ;
          clean_seq_archives del_ratio dry_run fname alloced) in
  let arcdir =
    N.path_cat [ conf.C.persist_dir ;
                 N.path "workers/ringbufs" ; N.path Versions.ringbuf ] in
  Files.dir_subtree_iter ~on_dir:(on_dir get_alloced_worker) arcdir ;
  let factordir =
    N.path_cat [ conf.C.persist_dir ;
                 N.path "workers/factors" ; N.path Versions.factors ] in
  Files.dir_subtree_iter ~on_dir:(on_dir get_alloced_special) factordir ;
  let reportdir =
    Files.dirname (C.report_ringbuf conf) in
  Files.dir_subtree_iter ~on_dir:(on_dir get_alloced_special) reportdir ;
  let notifdir =
    Files.dirname (C.notify_ringbuf conf) in
  Files.dir_subtree_iter ~on_dir:(on_dir get_alloced_special) notifdir

let cleanup_once_local conf dry_run del_ratio compress_older =
  let programs = RC.with_rlock conf identity
  and allocs = RamenArchivist.load_allocs conf in
  let get_alloced_worker fname rel_fname =
    (* We need to retrieve the FQ of that worker and then check if this
     * directory is still the current one, and then look for allocated
     * space (assuming 0 for unknown worker or version).
     * See src/ringbuf/ringbuf.c, function rotate_file_locked for details
     * on how those files are named. *)
    let fq = Files.dirname rel_fname |> Files.dirname in
    let fq = N.fq (fq :> string) in
    match RC.find_func programs fq with
    | exception Not_found ->
        !logger.info
          "Archive directory %a belongs to unknown function %a"
          N.path_print fname N.fq_print fq ;
        0
    | _rce, _prog, func ->
        (* TODO: RingBufLib.arc_dir_of_func ... to avoid selecting an
         * arbitrary file_type: *)
        let arc_dir = C.archive_buf_name ~file_type:RingBuf conf func |>
                      RingBufLib.arc_dir_of_bname in
        if Files.same arc_dir fname then (
          !logger.info
            "Archive directory %a is still the current archive for %a"
            N.path_print fname N.fq_print fq ;
          Hashtbl.find allocs (conf.C.site, fq)
        ) else (
          !logger.warning
            "Archive directory %a seems to be an old archive for %a \
             (which now uses %a). Will delete its content slowly."
            N.path_print fname N.fq_print fq N.path_print arc_dir ;
          0)
  and worker_bins =
    Hashtbl.fold (fun _ (rce, get_rc) lst ->
      let prog = get_rc () in
      List.fold_left (fun lst func ->
        (* only those running in this site in theory: *)
        (rce.RC.bin, func) :: lst
      ) lst prog.P.funcs
    ) programs [] in
  cleanup_once conf dry_run del_ratio compress_older get_alloced_worker worker_bins

let cleanup_loop ?while_ conf dry_run del_ratio compress_older sleep_duration =
  let watchdog =
    let timeout = sleep_duration *. 2. in
    Watchdog.make ~timeout "GC files" Processes.quit in
  Watchdog.enable watchdog ;
  Processes.until_quit (fun () ->
    log_and_ignore_exceptions ~what:"gc cleanup loop"
      (cleanup_once_local conf dry_run del_ratio) compress_older ;
    Watchdog.reset watchdog ;
    Processes.sleep_or_exit ?while_ (jitter sleep_duration) ;
    true)

let cleanup_once_sync conf clt dry_run del_ratio compress_older =
  let open RamenSync in
  let get_alloced_worker _fname rel_fname =
    let fq = Files.dirname rel_fname |> Files.dirname in
    let fq = N.fq (fq :> string) in
    let k = Key.PerSite (conf.C.site, PerWorker (fq, AllocedArcBytes)) in
    match (Client.find clt k).value with
    | exception Not_found -> 0
    | Value.Int i -> Int64.to_int i
    | v -> invalid_sync_type k v "an integer"
  and worker_bins =
    Client.fold clt (fun k hv lst ->
      match k, hv.value with
      | Key.PerSite (site, PerWorker (fq, Worker)),
        Value.Worker worker
        when site = conf.C.site ->
          let prog_name, _func_name = N.fq_parse fq in
          let _prog, func = function_of_worker clt fq worker in
          let func = F.unserialized prog_name func in
          let bin = C.cache_bin_file conf worker.bin_signature in
          (bin, func) :: lst
      | _ -> lst
    ) []
  in
  cleanup_once conf dry_run del_ratio compress_older get_alloced_worker worker_bins

let update_archives ~while_ conf dry_run clt =
  !logger.info "Updating archive stats." ;
  let open RamenSync in
  Client.iter clt (fun k hv ->
    match k, hv.value with
    | Key.PerSite (site, PerWorker (fq, Worker)),
      Value.Worker worker
      when site = conf.C.site &&
           worker.role = Whole ->
        let prog_name, _func_name = N.fq_parse fq in
        let _prog, func = function_of_worker clt fq worker in
        let func = F.unserialized prog_name func in
        let archives, num_files, num_bytes =
          RamenArchivist.compute_archives conf func in
        if not dry_run then (
          let arctimes_k = Key.PerSite (site, PerWorker (fq, ArchivedTimes))
          and arctimes = Value.TimeRange archives in
          ZMQClient.send_cmd clt ~while_ (SetKey (arctimes_k, arctimes)) ;
          let numfiles_k = Key.PerSite (site, PerWorker (fq, NumArcFiles))
          and numfiles = Value.of_int num_files in
          ZMQClient.send_cmd clt ~while_ (SetKey (numfiles_k, numfiles)) ;
          let numbytes_k = Key.PerSite (site, PerWorker (fq, NumArcBytes))
          and numbytes = Value.Int num_bytes in
          ZMQClient.send_cmd clt ~while_ (SetKey (numbytes_k, numbytes)))
    | _ -> ())

let cleanup_sync ~while_ conf dry_run del_ratio compress_older loop =
  (* The GC needs all allocation size per worker for this site.
   * On the other hand storage stats will be written back (ArchivedTimes,
   * NumArcFiles, NumArcBytes) for any discovered archives (which was done
   * by `archivist --stats` whithout confserver.
   * Also need workers and infos to iter over functions: *)
  let topics =
    [ "sites/"^ (conf.C.site :> string) ^"/workers/*/archives/alloc_size" ;
      "sites/"^ (conf.C.site :> string) ^"/workers/*/worker" ;
      "sources/*/info" ] in
  ZMQClient.start ~while_ conf.C.sync_url conf.C.login
                  ~topics ~recvtimeo:5. (fun clt ->
    if loop <= 0. then
      let msg_count = ZMQClient.process_in ~while_ clt in
      !logger.debug "Received %d messages" msg_count ;
      cleanup_once_sync conf clt dry_run del_ratio compress_older
    else
      let last_run =
        ref (if loop <= 0. then 0. else Unix.time () -. Random.float loop) in
      while while_ () do
        let msg_count = ZMQClient.process_in ~while_ clt in
        !logger.debug "Received %d messages" msg_count ;
        let now = Unix.gettimeofday () in
        if now > !last_run +. loop then (
          last_run := now ;
          cleanup_once_sync conf clt dry_run del_ratio compress_older ;
          update_archives ~while_ conf dry_run clt) ;
      done)
