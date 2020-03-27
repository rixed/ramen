(* A library to help with (re)building worker binaries.
 * As we can use several languages for specifying a worker code (ramen language
 * or the alerting API structure) we let users register builders from one file
 * type (extension) to another.
 *
 * All sources are taken from the shared configuration tree, although the
 * underlying builders use local files so that `ramen compile` can be used
 * locally with no need for the confserver daemon. *)

(*
 * Register of all known builders.
 *
 * We use file extension to determine file type: ".ramen" is for ramen language
 * (current version), ".x" is for executable, ".alert" for (current version of)
 * alerts...  Notice that at this point the file type has no version. But we
 * could also append version string to that extension, and use builders to
 * convert one version to the next. In that case we would always keep the
 * original source but as we do the conversions as lazily as possible (based on
 * file modification times, like make) this choice does not incur much latency.
 * The up side is that we are free to improve the converter from release to
 * release.
 *
 * To find a path from source to binary, as the number of registered builders
 * is expected to stay rather small, no fancy alg is required just plain brute
 * force.
 *)

open Batteries
open RamenHelpers
open RamenLog
open RamenSync
open RamenConsts
module C = RamenConf
module VSI = RamenSync.Value.SourceInfo
module N = RamenName
module Files = RamenFiles
module ZMQClient = RamenSyncZMQClient
module Compiler = RamenCompiler
module Processes = RamenProcesses
module Paths = RamenPaths

(* Raise any exception on failure: *)
type builder =
  C.conf ->
  (N.program -> VSI.compiled_program) ->
  N.src_path -> N.path -> N.path -> unit

(* Tells if the target must be rebuilt from the source: *)
type check = N.path -> N.path -> bool

(*
 * Possible check functions
 *)

(* Using source modification time.
 * Notice that if modification time of target = modification time of source then we
 * have to rebuild (but we make sure the target will be > next time!)
 * Fails if src_file does not exist. *)
let target_is_older src_file target_file =
  let st = Files.mtime src_file in
  let rec wait_source_in_past () =
    let now = Unix.gettimeofday () in
    if st >= now then (
      Unix.sleepf (max 0.01 (st -. now)) ;
      wait_source_in_past ()
    ) in
  match Files.mtime target_file with
  | exception Unix.(Unix_error (ENOENT, _, _)) ->
      !logger.info "Source %a is newer than non-existent target %a"
        N.path_print src_file
        N.path_print target_file ;
      wait_source_in_past () ;
      true
  | tt ->
      if tt <= st then
        !logger.info "Target %a is older than source %a (%f vs %f)"
          N.path_print target_file
          N.path_print src_file
          tt st ;
      tt <= st

let target_is_obsolete target_file =
  match Processes.version_of_bin target_file with
  | exception e ->
      !logger.warning "Cannot get version from %a: %s, assuming obsolescence"
        N.path_print target_file (Printexc.to_string e) ;
      true
  | v ->
      if v <> RamenVersions.codegen then
        !logger.info "Target is obsolete (%s vs %s)"
          v RamenVersions.codegen ;
      v <> RamenVersions.codegen

(*
 * Registry of known builder
 *)

(* Map from extension to extension (without dot): *)
type build_rule = string * check * builder
let builders : (string, build_rule) Hashtbl.t = Hashtbl.create 11

let register from to_ check build =
  let rule = to_, check, build in
  Hashtbl.add builders from rule ;
  rule

(* Register a builder from "ramen" to "info". Info src_ext and md5 are unset
 * as we have no knowledge of the rule. It's going to be patched when uploading
 * the result. *)
let info_rule =
  register "ramen" "info"
    (fun src_file target_file ->
      not (Files.exists target_file) ||
      target_is_older src_file target_file)
    (fun conf get_parent program_name src_file target_file ->
      let info =
        let i = Compiler.precompile conf get_parent src_file program_name in
        Value.SourceInfo.{ src_ext = "" ; md5s = [] ; detail = Compiled i } in
      Files.marshal_into_file target_file info)

let may_patch_info src_ext md5s = function
  | Value.(SourceInfo SourceInfo.{ detail ; _ }) ->
      Value.(SourceInfo SourceInfo.{ md5s ; src_ext ; detail })
  | v -> v

(* Register a rule to turn an alert into a ramen source file: *)
let alert_rule =
  register "alert" "ramen" target_is_older
    (fun _conf get_parent _prog_name src_file target_file ->
      let a = Files.ppp_of_file RamenApi.alert_source_ppp_ocaml src_file in
      RamenApi.generate_alert get_parent target_file a)

(* Register a builder that will carry on from ".info" and generate actual
 * executable in ".x". Used only for local builds (from file to file). *)
let bin_rule =
  register "info" "x"
    (fun src_file target_file ->
      target_is_older src_file target_file ||
      target_is_obsolete target_file)
    (fun conf _get_parent program_name src_file exec_file ->
      let info = Files.marshal_from_file src_file in
      let open RamenSync in
      match info.Value.SourceInfo.detail with
      | Compiled comp ->
          let base_file = Files.remove_ext src_file in
          Compiler.compile conf comp ~exec_file base_file program_name
      | Failed { err_msg } ->
          failwith err_msg)

(* Return the list of builders, brute force (N is small, loops are rare): *)
let find_path src dst =
  let fold from_ f u =
    Hashtbl.find_all builders from_ |>
    List.fold_left (fun u (next_to, _, _ as b) ->
      f u next_to b
    ) u
  in
  path_in_graph ~max_len:10 ~src ~dst { fold } |>
  List.rev

(* Also reset the mtime of from_file according to the mtime of the source: *)
let write_value_into_file fname value mtime =
  !logger.debug "writing value of %a into cache file %a"
    Value.print value
    N.path_print fname ;
  (match value with
  | Value.RamenValue T.(VString text) ->
      Files.write_whole_file fname text
  | Value.Alert alert ->
      let alert = RamenApi.alert_of_sync_value alert in
      Files.ppp_to_file fname RamenApi.alert_source_ppp_ocaml alert
  | Value.SourceInfo info ->
      Files.marshal_into_file fname info
  | v ->
      Printf.sprintf2 "Unexpected type: %a"
        Value.print v |>
      failwith) ;
  Files.touch fname mtime

(* Get the build path, then perform the next step (skipping steps that
 * are not required).
 * [program_name], actually a N.src_path, is required to resolve relative parents.
 * Note: this function performs most of it work within callbacks and will
 * therefore return before completion. *)
let build_next =
  let ppp_of_alert_file =
    Files.ppp_of_file RamenApi.alert_source_ppp_ocaml in
  fun conf session ?while_ ?(force=false) get_parent src_path from_ext ->
    let src_ext = ref "" and md5s = ref [] in
    let save_errors f x =
      try f x
      with exn ->
        (* Any error along the way also result in an info file: *)
        !logger.error "Cannot compile %a: %s"
          N.src_path_print src_path
          (Printexc.to_string exn) ;
        let info_key = Key.Sources (src_path, "info") in
        let depends_on =
          match exn with
          | RamenProgram.MissingParent fq -> Some fq
          | _ -> None in
        let v = Value.SourceInfo {
          src_ext = !src_ext ; md5s = List.rev !md5s ;
          detail = Failed { err_msg = Printexc.to_string exn ;
                            depends_on } } in
        ZMQClient.send_cmd ?while_ session (SetKey (info_key, v)) in
    let cached_file ext =
      Paths.precompserver_cache_file conf.C.persist_dir src_path ext in
    let write_path_into_file fname ext cont =
      let key = Key.Sources (src_path, ext) in
      !logger.debug "Copying value of %a into local file %a"
       Key.print key N.path_print fname ;
      Client.with_value session.clt key (save_errors (fun hv ->
        write_value_into_file fname hv.Client.value hv.Client.mtime ;
        cont hv)) in
    let read_value_from_file fname = function
      | "ramen" ->
          Value.RamenValue T.(VString (Files.read_whole_file fname))
      | "alert" ->
          let alert = ppp_of_alert_file fname |>
                      RamenApi.sync_value_of_alert in
          Value.Alert alert
      | "info" ->
          Value.SourceInfo (Files.marshal_from_file fname)
      | ext ->
          invalid_arg ("read_value_from_file for extension "^ ext) in
    (* Beware this loop does not really loop but rather installs a callback
     * that will eventually call loop again later: *)
    let rec loop unlock_all from_file from_ext = function
      | [] ->
          !logger.debug "Done recompiling %a"
            N.src_path_print src_path ;
          unlock_all ()
      | (to_ext, check, builder) :: rules ->
          C.info_or_test conf "Compiling %a from %s to %s"
            N.src_path_print src_path from_ext to_ext ;
          (* Lock the target in the config tree and copy its value locally
           * if it exists already: *)
          let to_key = Key.Sources (src_path, to_ext) in
          let unlock_all () =
            !logger.debug "Unlocking %a" Key.print to_key ;
            ZMQClient.send_cmd ?while_ session (UnlockKey to_key) ;
            unlock_all () in
          !logger.debug "Locking/Creating %a" Key.print to_key ;
          ZMQClient.send_cmd ?while_ session
            (LockOrCreateKey (to_key, Default.sync_compile_timeo))
            ~on_ko:unlock_all
            (* Notice that save_errors have to be repeated for every callback
             * that may fail independently from this thread of execution: *)
            ~on_done:(save_errors (fun () ->
              let to_file = cached_file to_ext in
              Client.with_value session.clt to_key (save_errors (fun hv ->
                (try write_value_into_file to_file hv.Client.value hv.Client.mtime
                with Failure _ ->
                  C.info_or_test conf "Target %a is not yet a proper source."
                    Key.print to_key) ;
                md5s := N.md5 (Files.read_whole_file from_file) :: !md5s ;
                if force || check from_file to_file then (
                  !logger.debug "Must rebuild%s"
                    (if force then " (FORCED)" else "") ;
                  if !src_ext = "" then (
                    C.info_or_test conf "Saving %S as the actual source extension"
                      from_ext ;
                    src_ext := from_ext ;
                  ) ;
                  match (
                    (* Make the target appear in the FS as atomically as possible
                     * as we might be overwriting a running worker, which would
                     * be confusing for the supervisor. Note that RamenCompiler
                     * actually pays no attention to the target file name. *)
                    let tmp_file = N.cat to_file (N.path "_tmp") in
                    builder conf get_parent src_path from_file tmp_file ;
                    Files.rename tmp_file to_file ;
                    read_value_from_file to_file to_ext
                  ) with
                  | exception exn ->
                      !logger.debug "Unlocking all locked sources..." ;
                      unlock_all () ;
                      raise exn
                  | v ->
                      (* info targets must record src_ext and md5: *)
                      let v = may_patch_info !src_ext (List.rev !md5s) v in
                      ZMQClient.send_cmd ?while_ session (SetKey (to_key, v))
                        ~on_ko:unlock_all
                        ~on_ok:unlock_all
                ) else (
                  !logger.debug "%a is still up to date"
                    N.path_print_quoted to_file
                ) ;
                save_errors (loop unlock_all to_file to_ext) rules))))
    in
    let build_rules = find_path from_ext "info" in
    !logger.debug "Will build this chain: %s->%a"
      from_ext
      (List.print ~first:"" ~last:"" ~sep:"->"
                  (fun oc (ext, _, _) -> String.print oc ext))
        build_rules ;
    let from_file = cached_file from_ext in
    (* Copy the source into this file: *)
    write_path_into_file from_file from_ext (fun _ ->
      save_errors (loop ignore from_file from_ext) build_rules)

let apply_rule conf ?(force_rebuild=false) get_parent program_name
               from_file to_file (_to_ext, check, builder) =
  if force_rebuild || check from_file to_file then (
    (* Make the target appear in the FS as atomically as possible
     * as we might be overwriting a running worker, which would
     * be confusing for the supervisor. Note that RamenCompiler
     * actually pays no attention to the target file name. *)
    let tmp_file = N.cat to_file (N.path "_tmp") in
    builder conf get_parent program_name from_file tmp_file ;
    Files.rename tmp_file to_file
  ) else (
    !logger.debug "%a is still up to date"
      N.path_print_quoted to_file
  )
