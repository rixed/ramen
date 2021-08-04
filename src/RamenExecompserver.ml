(* Service in charge of compiling a binary executable for each type-checked
 * programs. The path to the binary is then written back in the conftree on
 * a per site basis, keyed with the hash of the info.
 * A Execompserver daemon must run on every site, as opposed to the
 * precompserver that can run only once. *)
open Batteries
open RamenConsts
open RamenHelpers
open RamenHelpersNoLog
open RamenLog
open RamenSyncHelpers
open RamenSync
module C = RamenConf
module Compiler = RamenCompiler
module Default = RamenConstsDefault
module Make = RamenMake
module Metric = RamenConstsMetric
module N = RamenName
module Paths = RamenPaths
module ZMQClient = RamenSyncZMQClient

open Binocle

let stats_compilations_count =
  Files.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.compilations_count
        "Number of compilations that have been attempted.")

let execomp_quarantine = ref Default.execomp_quarantine

let compile_one conf session prog_name info_value info_sign info_mtime =
  let clt = option_get "compile_one" __LOC__ session.ZMQClient.clt in
  let get_parent =
    Compiler.program_from_confserver clt in
  let info_file =
    Paths.execompserver_cache_file conf.C.persist_dir (N.path info_sign) "info" in
  let bin_file =
    Paths.execompserver_cache_bin conf.C.persist_dir info_sign in
  Make.write_value_into_file info_file info_value info_mtime ;
  Make.(apply_rule conf get_parent prog_name info_file bin_file bin_rule) ;
  bin_file

let quarantined = Hashtbl.create 10

let compile_info conf ~while_ session src_path info comp mtime =
  let do_compile () =
    !logger.info "Compiling binary for %a"
      N.src_path_print src_path ;
    let info_sign = Value.SourceInfo.signature_of_compiled comp in
    try
      if not (while_ ()) then raise Exit ;
      let bin_file = compile_one conf session src_path info info_sign mtime in
      let exe_key =
        Key.PerSite (conf.C.site, PerProgram (info_sign, Executable)) in
      let exe_path = Value.(of_string (bin_file :> string)) in
      ZMQClient.send_cmd session (SetKey (exe_key, exe_path)) ;
      IntCounter.inc ~labels:["status", "ok"]
        (stats_compilations_count conf.C.persist_dir) ;
      !logger.debug "New binary %a" Key.print exe_key
    with
      | Exit ->
          ()
      | e ->
          IntCounter.inc ~labels:["status", "failure"]
            (stats_compilations_count conf.C.persist_dir) ;
          let retry_date = Unix.time () +. !execomp_quarantine in
          !logger.warning "While compiling %a: %s, quarantining until %a"
            N.src_path_print src_path
            (Printexc.to_string e)
            print_as_date retry_date ;
          Hashtbl.replace quarantined src_path retry_date
  in
  match Hashtbl.find quarantined src_path with
  | exception Not_found ->
      do_compile ()
  | retry_date ->
      let now = Unix.time () in
      if now < retry_date then (
        !logger.info "Compilation of %a under quarantine until %a"
          N.src_path_print src_path
          print_as_date retry_date
      ) else (
        Hashtbl.remove quarantined src_path ;
        do_compile ()
      )

let check_binaries conf ~while_ session =
  let prefix = "sources/" in
  let clt = option_get "check_binaries" __LOC__ session.ZMQClient.clt in
  Client.iter clt ~prefix (fun k hv ->
    match k, hv.Client.value with
    | Key.Sources (src_path, "info"),
      Value.SourceInfo { detail = Compiled comp ; _ } ->
        let info_sign = Value.SourceInfo.signature_of_compiled comp in
        let bin_file =
          Paths.execompserver_cache_bin conf.C.persist_dir info_sign in
        if not (Files.exists bin_file) then (
          !logger.warning "Executable %a vanished, rebuilding it"
            N.path_print bin_file ;
          compile_info conf ~while_ session src_path hv.value comp hv.mtime)
    | _ ->
        ())

let start ?(quarantine=Default.execomp_quarantine) conf ~while_ =
  execomp_quarantine := quarantine ;
  (* We must wait the end of sync to start compiling for [get_parent] above
   * to work: *)
  let synced = ref false in
  let try_after_sync = ref [] in
  let on_set session k v uid mtime =
    match k, v with
    | Key.(Sources (src_path, "info")),
      Value.SourceInfo { detail = Compiled comp ; _ } ->
        if not !synced then
          try_after_sync := (k, v, uid, mtime) :: !try_after_sync
        else
          compile_info conf ~while_ session src_path v comp mtime
    | k, v ->
        !logger.debug "Ignoring key %a, value %a"
          Key.print k
          Value.print v in
  let on_synced session =
    synced := true ;
    if !try_after_sync <> [] then
      !logger.info "Synced, trying to compile %d sources."
        (List.length !try_after_sync) ;
    List.iter (fun (k, v, uid, mtime) ->
      if while_ () then
        on_set session k v uid mtime
    ) !try_after_sync ;
    try_after_sync := [] in
  let on_new session k v uid mtime _can_write _can_del _owner _expiry =
    on_set session k v uid mtime in
  let sync_loop session =
    let next_files_check = ref 0. in
    while while_ () do
      let now = Unix.time () in
      if now >= !next_files_check then (
        check_binaries conf ~while_ session ;
        (* [check_binaries] can run for a long time so do not use [now]: *)
        next_files_check := Unix.time () +. jitter check_binaries_on_disk_every ;
      ) ;
      ZMQClient.process_in ~while_ session
    done in
  let topics = [ "sources/*" ] in
  start_sync conf ~while_ ~on_new ~on_set ~topics ~recvtimeo:1. ~on_synced
             ~sesstimeo:Default.sync_long_sessions_timeout sync_loop
