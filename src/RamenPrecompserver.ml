(* Service in charge of generating the "info" files.
 *
 * Before it is possible to start a new program, and before the choreographer
 * can create one (or several) worker(s) for this program, and before the
 * supervisor can compile the worker, then the used source file must be
 * pre-compiled (aka. parsed and type-checked) and the result of this pre-
 * compilation saved in an "info" file.
 * The precompserver is the daemon in charge of this.
 * Sometimes, the pre-compilation can succeed as soon as a new source files
 * appear. But it also frequently happen that a source program depends on some
 * external program that is not (yet) running, and therefore the pre-compilation
 * must be delayed.
 *
 * The downsides of these delays are several:
 * - that we do not know instantly if the new program fails to type check;
 * - the same source may have to be parsed several times before type-checking
 *   can be attempted.
 * For this later point it would be nice to have either an "ast" file before the
 * "info" or a partial error status with the AST in the info file (TODO).
 *
 * The upside is that we do not need to declare parents output types beforehand.
 *
 * Alternatively, we could also type-check only just before supervisor compiles
 * the worker, but it is much more robust for the types to be known beforehand.
 *)
open Batteries
open RamenLog
open RamenHelpers
open RamenHelpersNoLog
open RamenSyncHelpers
open RamenSync
module C = RamenConf
module Default = RamenConstsDefault
module Metric = RamenConstsMetric
module N = RamenName
module ZMQClient = RamenSyncZMQClient

open Binocle

let stats_precompilations_count =
  Files.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.precompilations_count
        "Number of pre-compilations that have been attempted.")

(* Do not generate any executable file, but parse/typecheck new or updated
 * source programs, using the build infrastructure to accept any source format
 * (for now distinguishing them using extension as usual).
 * The only change required from the builder is:
 * - source/target files are read from and written to the config tree instead
 *   of the file system;
 * - instead of using mtime, it must use the md5s;
 * - it must stop before reaching the .x but instead aim for a "/info".
 *)
let start conf ~while_ =
  let compile session ?force path ext =
    (* Program name used to resolve relative names is the location in the
     * source tree: *)
    let get_parent =
      RamenCompiler.program_from_confserver session.ZMQClient.clt in
    try
      RamenMake.build_next
        conf ~while_ ?force session get_parent path ext ;
      IntCounter.inc ~labels:["status", "ok"]
        (stats_precompilations_count conf.C.persist_dir)
    with Exit -> ()
       | e ->
          !logger.warning "Cannot Compile %a: %s"
            N.src_path_print path
            (Printexc.to_string e) ;
          IntCounter.inc ~labels:["status", "failure"]
            (stats_precompilations_count conf.C.persist_dir) in
  (* When the current info is valid, and have been built from the same path,
   * then do not start a costly compilation cycle though: *)
  let md5_of session path ext =
    let src_key = Key.Sources (path, ext) in
    match (Client.find session.ZMQClient.clt src_key).value with
    | exception Not_found -> ""
    | Value.RamenValue (VString str) ->
        N.md5 str
    | Value.Alert alert ->
        N.md5 (PPP.to_string Value.Alert.t_ppp_ocaml alert)
    | _ -> "" in
  let info_still_valid session path ext =
    let info_key = Key.Sources (path, "info") in
    match (Client.find session.ZMQClient.clt info_key).value with
    | exception Not_found ->
        !logger.info "No info for %a yet" Key.print info_key ;
        false
    | Value.SourceInfo { src_ext ; detail = Compiled _ ; md5s ; _ }
      when src_ext = ext && list_starts_with md5s (md5_of session path ext) ->
        !logger.info "Already has info for same md5 %s" (List.hd md5s) ;
        true
    | v ->
        !logger.info "Has some other info for %a: %s"
          Key.print info_key
          (abbrev 500 (Value.to_string v)) ;
        false in
  let compile session ?(force=false) path ext =
    if not force && info_still_valid session path ext then (
      !logger.info "Current info for %a (%s) is still fresh"
        N.src_path_print path
        ext ;
      IntCounter.inc ~labels:["status", "cached"]
        (stats_precompilations_count conf.C.persist_dir)
    ) else (
      !logger.info "Must (re)build the info for %a"
        N.src_path_print path ;
      compile session ~force path ext
    ) in
  let synced = ref false in
  let try_after_sync = ref [] in
  let on_synced session =
    synced := true ; (* Stop adding paths to [try_after_sync] *)
    !logger.info "Synced, trying to pre-compile %d sources."
      (List.length !try_after_sync) ;
    List.iter (fun (path, src_ext) ->
      compile session path src_ext
    ) !try_after_sync ;
    try_after_sync := [] in
  let topics =
    [ "sites/*/workers/*/worker" ; (* for get_programs *)
      "sources/*" ] in
  let on_set session k v uid _mtime =
    let retry_depending_on new_path =
      !logger.debug
        "Retrying to pre-compile sources that failed because of %a"
        N.src_path_print new_path ;
      Client.iter ~prefix:"sources/" session.ZMQClient.clt (fun k hv ->
        match k, hv.Client.value with
        | Key.(Sources (path, ext)),
          Value.(SourceInfo {
            detail = Failed { depends_on ; _ } ;
            src_ext ; _ })
          when ext = "info" && depends_on = Some new_path ->
            !logger.info "Will try to pre-compile %a from %s again!"
              N.src_path_print path src_ext ;
            compile session ~force:true path src_ext
        | _ ->
            ()) in
    match k with
    | Key.(Sources (src_path, "info")) ->
        (match v with
        | Value.SourceInfo { detail = Compiled _ ; _ } ->
            (* Whenever a new program is successfully compiled, check for
             * other info that failed to compile because this one was
             * missing and retry them: *)
            if !synced then retry_depending_on src_path
        | Value.SourceInfo {
            detail = Failed { depends_on ; _ } ;
            src_ext ; _
          } when depends_on <> None ->
            let depends_on = option_get "depends_on" __LOC__ depends_on in
            (* Asynchronous shared configuration is fun: between the time we
             * missed that failed path and the time we receive the compilation
             * error (now), this path may have been successfully compiled.
             * This is actually a frequent occurrence at startup when examples
             * are compiled in no specific order.
             * So let's have a look: *)
            if !synced then
              let k = Key.Sources (depends_on, "info") in
              (match (Client.find session.clt k).value with
              | exception Not_found ->
                  ()
              | Value.SourceInfo { detail = Compiled _ ; _ } ->
                  !logger.info "By the time %a failed to compile, its parent \
                                %a was compiled, so let's retry"
                    N.src_path_print src_path
                    N.src_path_print depends_on ;
                  compile session ~force:true src_path src_ext
              | _ ->
                  ())
        | _ ->
            ())
    | Key.(Sources (_, _)) when Value.equal v Value.dummy ->
        (* When the build LockOrCreateKey the new dummy value is seen
         * before the command is acked, thus triggering another build.
         * Cf https://github.com/rixed/ramen/issues/921 *)
        ()
    | Key.(Sources (path, ext)) ->
        assert (ext <> "info") ; (* Case handled above *)
        if uid = session.ZMQClient.clt.my_uid then
          !logger.debug "Ignoring %a that's been created by us"
            Key.print k
        else
          if !synced then
            compile session path ext
          else (
            !logger.info "Wait until end of sync before trying to compile %a"
              Key.print k ;
            try_after_sync := (path, ext) :: !try_after_sync
          )
    | Key.Error _  ->
        (* Errors have been logged already *)
        ()
    | Key.(PerSite (_, PerWorker (_, Worker))) ->
        (* Expected but unused here *)
        ()
    | k ->
        !logger.warning "Irrelevant: %a, %a"
          Key.print k Value.print v in
  let on_new session k v uid mtime _can_write _can_del _owner _expiry =
    on_set session k v uid mtime in
  start_sync conf ~while_ ~on_new ~on_set ~topics ~recvtimeo:1. ~on_synced
             ~sesstimeo:Default.sync_long_sessions_timeout
             (ZMQClient.process_until ~while_)
