(* Deletes old data files according to storage configuration, and also
 * old data dir of functions that have not been running for a while, as well
 * as old versions of ramen configuration. *)
open Batteries
open Str
open RamenLog
open RamenHelpers
module C = RamenConf

(* Have a single watchdog for cleanup_old_files whatever happen: *)
let watchdog = ref None

let get_log_file () =
  Unix.gettimeofday () |> Unix.localtime |> log_file

let delete_directory fname = (* TODO: should really delete *)
  Unix.rename fname (fname ^".todel")

let date_regexp = regexp "^[0-9]+-[0-9]+-[0-9]+$"
let v_regexp = regexp "v[0-9]+"
let v1v2_regexp = regexp "v[0-9]+_v[0-9]+"

let cleanup_dir_old conf dry_run (dir, sub_re, current_version) =
  let dir = conf.C.persist_dir ^"/"^ dir in
  !logger.debug "Cleaning directory %s..." dir ;
  (* Error in there will be delivered to the stream reader: *)
  match Sys.files_of dir with
  | exception Unix.Unix_error (Unix.ENOENT, _, _) -> ()
  | exception exn ->
      !logger.error "Cannot list %s: %s" dir (Printexc.to_string exn)
  | files ->
    Enum.iter (fun fname ->
      let full_path = dir ^"/"^ fname in
      if fname = current_version then (
        if not dry_run then
          touch_file full_path (Unix.gettimeofday ())
      ) else if string_match sub_re fname 0 &&
                is_directory full_path &&
                (* TODO: should be a few days *)
                file_is_older_than (1. *. 86400.) fname
      then (
        !logger.info "Deleting %s: unused, old version%s"
          fname (if dry_run then " (NOPE)" else "") ;
        if not dry_run then
          delete_directory full_path
      )
    ) files

let cleanup_once conf dry_run max_archives =
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
      "workers/states", v_regexp, RamenVersions.worker_state ]
  in
  !logger.info "Cleaning old unused files..." ;
  List.iter (cleanup_dir_old conf dry_run) to_clean ;
  (* Clean old archives *)
  let arcdir =
    conf.C.persist_dir ^"/workers/ringbufs/"^ RamenVersions.ringbuf
  and reportdir =
    Filename.dirname (RamenConf.report_ringbuf conf)
  and notifdir =
    Filename.dirname (RamenConf.notify_ringbuf conf) in
  let clean_seq_archives dir =
    (* Delete all files matching %d_%d_%a_%a.r but the last ones: *)
    let files = RingBufLib.arc_files_of dir |> Array.of_enum in
    Array.fast_sort RingBufLib.arc_file_compare files ;
    for i = 0 to Array.length files - 1 - max_archives do
      let _, _, _, _, fname = files.(i) in
      !logger.info "Deleting %s: old archive%s"
        fname (if dry_run then " (NOPE)" else "") ;
      if not dry_run then
        log_and_ignore_exceptions Unix.unlink fname
    done
  in
  let on_dir fname rel_fname =
    let basename = Filename.basename rel_fname in
    if String.ends_with basename ".arc" then
      clean_seq_archives fname
  in
  dir_subtree_iter ~on_dir arcdir ;
  dir_subtree_iter ~on_dir reportdir ;
  dir_subtree_iter ~on_dir notifdir

let cleanup_loop conf dry_run sleep_time max_archives =
  if !watchdog = None then
    let timeout = float_of_int sleep_time *. 2. in
    watchdog :=
      Some (RamenWatchdog.make ~timeout "GC files" RamenProcesses.quit) ;
  let watchdog = Option.get !watchdog in
  RamenWatchdog.run watchdog ;
  forever (fun () ->
    cleanup_once conf dry_run max_archives ;
    RamenWatchdog.reset watchdog ;
    Unix.sleep sleep_time) ()
