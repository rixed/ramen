(* Service in charge of compiling a binary executable for each type-checked
 * programs. The path to the binary is then written back in the conftree on
 * a per site basis, keyed with the hash of the info.
 * A Execompserver daemon must run on every site, as opposed to the
 * precompserver that can run only once. *)
open Batteries
open RamenLog
open RamenHelpers
open RamenConsts
open RamenSyncHelpers
open RamenSync
module C = RamenConf
module N = RamenName
module Compiler = RamenCompiler
module Make = RamenMake
module Paths = RamenPaths
module ZMQClient = RamenSyncZMQClient

let start conf ~while_ =
  let compile_one session prog_name info_value info_sign info_mtime =
    let get_parent =
      Compiler.program_from_confserver session.ZMQClient.clt in
    let info_file =
      Paths.execompserver_cache_file conf.C.persist_dir (N.path info_sign) "info" in
    let bin_file =
      Paths.execompserver_cache_bin conf.C.persist_dir info_sign in
    Make.write_value_into_file info_file info_value info_mtime ;
    Make.(apply_rule conf get_parent prog_name info_file bin_file bin_rule) ;
    bin_file in
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
          let info_sign = Value.SourceInfo.signature_of_compiled comp in
          let what = "compiling "^ (src_path :> string) in
          log_and_ignore_exceptions ~what (fun () ->
            let bin_file = compile_one session src_path v info_sign mtime in
            let exe_key =
              Key.PerSite (conf.C.site, PerProgram (info_sign, Executable)) in
            let exe_path = Value.(of_string (bin_file :> string)) in
            ZMQClient.send_cmd ~while_ session (SetKey (exe_key, exe_path)) ;
            !logger.info "New binary %a" Key.print exe_key
          ) ()
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
      on_set session k v uid mtime
    ) !try_after_sync ;
    try_after_sync := [] in
  let on_new session k v uid mtime _can_write _can_del _owner _expiry =
    on_set session k v uid mtime in
  let topics = [ "sources/*" ] in
  start_sync conf ~while_ ~on_new ~on_set ~topics ~recvtimeo:10. ~on_synced
             ~sesstimeo:Default.sync_long_sessions_timeout
             (ZMQClient.process_until ~while_)
