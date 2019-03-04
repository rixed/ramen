(* Deletes old data files according to storage configuration, and also
 * old data dir of functions that have not been running for a while, as well
 * as old versions of ramen configuration. *)
open Batteries
open Str
open Unix
open RamenLog
open RamenHelpers
module C = RamenConf

let get_log_file () =
  gettimeofday () |> localtime |> log_file

let date_regexp = regexp "^[0-9]+-[0-9]+-[0-9]+$"
let v_regexp = regexp "v[0-9]+"
let v1v2_regexp = regexp "v[0-9]+_v[0-9]+"

let cleanup_dir_old conf dry_run (dir, sub_re, current_version) =
  let dir = conf.C.persist_dir ^"/"^ dir in
  !logger.debug "Cleaning directory %s..." dir ;
  (* Error in there will be delivered to the stream reader: *)
  match Sys.files_of dir with
  | exception (Unix_error (ENOENT, _, _) | Sys_error _) ->
      (* No such directory is OK: *)
      ()
  | exception exn ->
      !logger.error "Cannot list %s: %s" dir (Printexc.to_string exn)
  | files ->
    Enum.iter (fun fname ->
      let full_path = dir ^"/"^ fname in
      if fname = current_version then (
        if not dry_run then
          touch_file full_path (gettimeofday ())
      ) else if string_match sub_re fname 0 &&
                is_directory full_path &&
                (* TODO: should be a few days *)
                file_is_older_than ~on_err:false (1. *. 86400.) full_path
      then (
        !logger.info "Deleting %s: unused, old version%s"
          fname (if dry_run then " (NOPE)" else "") ;
        if not dry_run then
          rm_rf full_path
      )
    ) files

let cleanup_once conf dry_run del_ratio =
  (* Have a list of directories and regexps and current version,
   * Iter through this list for file matching the regexp and that are also
   * directories.
   * If this direntry matches the current version, touch it.
   * If not, and if it hasn't been touched for X days, assume that's an old
   * one and delete it. *)
  let to_clean =
    [ "log", date_regexp, get_log_file () ;
      "log/workers", v_regexp, get_log_file () ;
      "configuration", v_regexp, RamenVersions.graph_config ;
      "instrumentation_ringbuf", v1v2_regexp,
        (RamenVersions.instrumentation_tuple ^"_"^ RamenVersions.ringbuf) ;
      "workers/ringbufs", v_regexp, RamenVersions.ringbuf ;
      "workers/out_ref", v_regexp, RamenVersions.out_ref ;
      "workers/states", v_regexp, RamenVersions.worker_state ;
      "workers/factors", v_regexp, RamenVersions.factors ]
  in
  !logger.info "Cleaning old unused files..." ;
  List.iter (cleanup_dir_old conf dry_run) to_clean ;
  (* Clean old archives *)
  let arcdir =
    conf.C.persist_dir ^"/workers/ringbufs/"^ RamenVersions.ringbuf
  and factordir =
    conf.C.persist_dir ^"/workers/factors/"^ RamenVersions.factors
  and reportdir =
    Filename.dirname (RamenConf.report_ringbuf conf)
  and notifdir =
    Filename.dirname (RamenConf.notify_ringbuf conf) in
  let clean_seq_archives dir alloced =
    (* Delete oldest files matching %d_%d_%a_%a.r, until the worker is below
     * its allocated storage space, but not more than a given fraction of
     * what we should delete. *)
    (* Delete all files matching %d_%d_%a_%a.r but the last ones.
     * Also, for each of these, try to delete all attached factor files. *)
    let files = Sys.files_of dir |> Array.of_enum in
    let arc_files =
      Array.enum files |> RingBufLib.filter_arc_files dir |>
      Array.of_enum in
    Array.fast_sort RingBufLib.arc_file_compare arc_files ;
    (* Older files come first in [arc_files].
     * Now find the allocated size for this worker: *)
    let rec loop i sum_sz num_to_del to_del =
      if i < 0 then num_to_del, to_del else
      let _, _, _, _, fpath as f = arc_files.(i) in
      let sum_sz = sum_sz + file_size fpath in
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
      | (_, _, _, _, fpath) :: to_del->
          if n > 0 then (
            (* TODO: also check that we do not delete younger data than
             * planned. Ie. allocs must also tell the retention for each
             * function. *)
            !logger.info "Deleting %s: old archive%s"
              fpath (if dry_run then " (NOPE)" else "") ;
            if not dry_run then (
              let pref = Filename.(basename fpath |> remove_extension) in
              Array.iter (fun fname ->
                if String.starts_with fname pref then
                  log_and_ignore_exceptions unlink (dir ^"/"^ fname)
              ) files
            ) ;
            del (n - 1) to_del
          ) in
    del num_to_del to_del
  in
  let programs = C.with_rlock conf identity in
  let allocs = RamenArchivist.load_allocs conf in
  let get_alloced_worker fname rel_fname =
    (* We need to retrieve the FQ of that worker and then check if this
     * directory is still the current one, and then look for allocated
     * space (assuming 0 for unknown worker or version).
     * See src/ringbuf/ringbuf.c, function rotate_file_locked for details
     * on how those files are named. *)
    let fq = Filename.dirname rel_fname |> Filename.dirname |>
             RamenName.fq_of_string in
    match C.find_func programs fq with
    | exception Not_found ->
        !logger.info
          "Archive directory %s belongs to unknown function %a"
          fname RamenName.fq_print fq ;
        0
    | _mre, _prog, func ->
        (* TODO: RingBufLib.arc_dir_of_func ... to avoid selecting an
         * arbitrary file_type: *)
        let arc_dir = C.archive_buf_name ~file_type:RingBuf conf func |>
                      RingBufLib.arc_dir_of_bname in
        if same_files arc_dir fname then (
          !logger.info
            "Archive directory %s is still the current archive for %a"
            fname RamenName.fq_print fq ;
          Hashtbl.find allocs fq
        ) else (
          !logger.warning
            "Archive directory %s seems to be an old archive for %a \
             (which now uses %s). Will delete its content slowly."
            fname RamenName.fq_print fq arc_dir ;
          0
        ) in
  let get_alloced_special _fname _rel_fname = 150_000_000 (* TODO *) in
  let on_dir get_alloced fname rel_fname =
    (* FIXME: what if a function or program name ends with ".arc"?
     * We should leave the GC explore freely under persist_dir, looking
     * for .gc files giving it instructions (max size and/or max age,
     * and/or account to given FQ. *)
    if String.ends_with rel_fname ".arc" then (
      match get_alloced fname rel_fname with
      | exception e ->
          (* Better not delete anything *)
          let what =
            Printf.sprintf "Cannot find allocated storage for archive %s"
              rel_fname in
          print_exception ~what e
      | alloced ->
          !logger.debug "%s is allocated %d bytes" rel_fname alloced ;
          clean_seq_archives fname alloced
    )
  in
  dir_subtree_iter ~on_dir:(on_dir get_alloced_worker) arcdir ;
  dir_subtree_iter ~on_dir:(on_dir get_alloced_special) factordir ;
  dir_subtree_iter ~on_dir:(on_dir get_alloced_special) reportdir ;
  dir_subtree_iter ~on_dir:(on_dir get_alloced_special) notifdir

let cleanup_loop conf dry_run del_ratio sleep_time =
  let watchdog =
    let timeout = sleep_time *. 2. in
    RamenWatchdog.make ~timeout "GC files" RamenProcesses.quit in
  RamenWatchdog.enable watchdog ;
  forever (fun () ->
    cleanup_once conf dry_run del_ratio ;
    RamenWatchdog.reset watchdog ;
    sleepf (jitter sleep_time)) ()
