(* Deletes old data files according to storage configuration, and also
 * old data dir of functions that have not been running for a while, as well
 * as old versions of ramen configuration. *)
open Batteries
open Str
open Unix
open RamenLog
open RamenHelpers
open RamenConsts
open RamenSyncHelpers
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
      N.path "workers/factors", v_regexp, N.path Versions.factors ;
      N.path "confserver/snapshots", v_regexp, N.path Versions.sync_conf ]
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
    if Files.basename rel_fname = N.path "arc" ||
       Files.has_ext "factors" rel_fname
    then (
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

let cleanup_once conf clt dry_run del_ratio compress_older =
  let open RamenSync in
  let get_alloced_worker _fname rel_fname =
    let fq = Files.dirname rel_fname |> Files.dirname in
    let fq = N.fq (fq :> string) in
    let k = Key.PerSite (conf.C.site, PerWorker (fq, AllocedArcBytes)) in
    match (Client.find clt k).value with
    | exception Not_found -> 0
    | Value.RamenValue (VI64 i) -> Int64.to_int i
    | v -> invalid_sync_type k v "an integer"
  and worker_bins =
    let prefix = "sites/" in
    Client.fold clt ~prefix (fun k hv lst ->
      match k, hv.value with
      | Key.PerSite (site, PerWorker (fq, Worker)),
        Value.Worker worker
        when site = conf.C.site ->
          let prog_name, _func_name = N.fq_parse fq in
          let _prog, func = function_of_fq clt fq in
          let func = F.unserialized prog_name func in
          let bin =
            C.supervisor_cache_file conf (N.path worker.bin_signature) "x" in
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
        let _prog, func = function_of_fq clt fq in
        let func = F.unserialized prog_name func in
        let archives, num_files, num_bytes =
          RamenArchivist.compute_archives conf func in
        if not dry_run then (
          let arctimes_k = Key.PerSite (site, PerWorker (fq, ArchivedTimes))
          and arctimes = Value.TimeRange archives in
          ZMQClient.send_cmd ~while_ (SetKey (arctimes_k, arctimes)) ;
          let numfiles_k = Key.PerSite (site, PerWorker (fq, NumArcFiles))
          and numfiles = Value.of_int num_files in
          ZMQClient.send_cmd ~while_ (SetKey (numfiles_k, numfiles)) ;
          let numbytes_k = Key.PerSite (site, PerWorker (fq, NumArcBytes))
          and numbytes = Value.of_int64 num_bytes in
          ZMQClient.send_cmd ~while_ (SetKey (numbytes_k, numbytes)))
    | _ -> ())

let cleanup ~while_ conf dry_run del_ratio compress_older loop =
  let open RamenSync in
  (* The GC needs all allocation size per worker for this site.
   * On the other hand storage stats will be written back (ArchivedTimes,
   * NumArcFiles, NumArcBytes) for any discovered archives (which was done
   * by `archivist --stats` without confserver).
   * Also need workers and infos to iter over functions: *)
  let topics =
    [ "sites/"^ (conf.C.site :> string) ^"/workers/*/archives/alloc_size" ;
      "sites/"^ (conf.C.site :> string) ^"/workers/*/worker" ;
      "sources/*/info" ] in
  (* Start right after alloc_sizes are changed (esp. right
   * after the first run from archivist) *)
  let last_alloc_size = ref 0. in
  let on_new _ k _ _ mtime _ _ _ _ =
    match k with Key.PerSite (site, PerWorker (_, AllocedArcBytes))
                 when site = conf.C.site ->
      last_alloc_size := max !last_alloc_size mtime
    | _ -> () in
  start_sync conf ~while_ ~topics ~recvtimeo:5. ~on_new (fun clt ->
    if loop <= 0. then (
      ZMQClient.process_in ~while_ clt ;
      cleanup_once conf clt dry_run del_ratio compress_older ;
      update_archives ~while_ conf dry_run clt
    ) else
      let last_run =
        ref (Unix.time () -. Random.float loop) in
      while while_ () do
        ZMQClient.process_in ~while_ clt ;
        let now = Unix.gettimeofday () in
        if now > !last_run +. loop ||
           !last_run < !last_alloc_size
        then (
          last_run := now ;
          cleanup_once conf clt dry_run del_ratio compress_older ;
          update_archives ~while_ conf dry_run clt) ;
      done)
