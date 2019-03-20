(* A library to help with (re)building worker binaries.
 * As we can use several languages for specifying a worker code (ramen language
 * or the alerting API structure) we let users register builders from one file type
 * (extension) to another.
 * Then, this also provides a daemon that will scan the running config for obsolete
 * binaries which source is known and rebuild them. *)

(*
 * Register of all known builders.
 *
 * We use file extension to determine file type: ".ramen" is for ramen language (current
 * version), ".x" is for executable, ".alerts" for (current version of) alerts...
 * Notice that at this point the file type has no version. But we could also append
 * version string to that extension, and use builders to convert one version to the
 * next. In that case we would always keep the original source but as we do the
 * conversions as lazily as possible (based on file modification times, like make)
 * this choice does not incur much latency. The up side is that we are free to
 * improve the converter from release to release.
 *
 * To find a path from source to binary, as the number of registered builders
 * is expected to stay rather small, no fancy alg is required just plain brute
 * force.
 *)

open Batteries
open RamenHelpers
open RamenLog
module C = RamenConf
module F = C.Func
module P = C.Program
module N = RamenName

(* Raise any exception on failure: *)
type builder =
  C.conf ->
  (N.program -> RamenProgram.P.t) ->
  N.program -> string -> string -> unit

(* Tells if the target must be rebuilt from the source: *)
type check = string -> string -> bool

(*
 * Possible check functions
 *)

(* Using files presence and modification time.
 * Notice that if modification time of target = modification time of source then we
 * have to rebuild (but we make sure the target will be > next time!)
 * Fails if src_file does not exist. *)
let target_is_older src_file target_file =
  let st = mtime_of_file src_file in
  let rec wait_source_in_past () =
    let now = Unix.gettimeofday () in
    if st >= now then (
      Unix.sleepf (max 0.01 (st -. now)) ;
      wait_source_in_past ()
    ) in
  match mtime_of_file target_file with
  | exception Unix.(Unix_error (ENOENT, _, _)) ->
      wait_source_in_past () ;
      true
  | tt ->
      tt <= st

let target_is_obsolete target_file =
  match P.version_of_bin target_file with
  | exception e ->
      !logger.warning "Cannot get version from %s: %s, assuming obsolescence"
        target_file (Printexc.to_string e) ;
      true
  | v ->
      v <> RamenVersions.codegen

(*
 * Registry of known builder
 *)

(* Map from extension to extension (without dot): *)
let builders : (string, string * check * builder) Hashtbl.t = Hashtbl.create 11

let register from to_ check build =
  Hashtbl.add builders from (to_, check, build)

(* Register a builder from ".ramen" to ".x" and converters from various
 * versioned ramen language files.
 * Unlike the command line compiler, this compiler can only find parents in
 * the running-config (not on disc) and has no option to set a different
 * program name. (FIXME: try to get rid of that option) *)
let () =
  register "ramen" "x"
    (fun src_file target_file ->
      target_is_older src_file target_file ||
      target_is_obsolete target_file)
    (fun conf get_parent program_name src_file exec_file ->
      RamenCompiler.compile conf get_parent ~exec_file src_file
                            program_name)

(* Return the list of builders, brute force (N is small, loops are rare): *)
let find_path src dst =
  let fold fro f u =
    Hashtbl.find_all builders fro |>
    List.fold_left (fun u (next_to, _, _ as b) ->
      f u next_to b
    ) u
  in
  path_in_graph ~max_len:10 ~src ~dst { fold } |>
  List.rev

(* Get the build path, then for each step from the source, check if the build is
 * required.
 * [program_name] is required to resolve relative parents. *)
let build conf ?(force_rebuild=false) get_parent program_name src_file
          target_file =
  let base_file = Filename.remove_extension target_file in
  let rec loop src_file = function
    | [] ->
        !logger.debug "Done recompiling %S" target_file
    | (to_type, check, builder) :: rest ->
        let target_file = base_file ^"."^ to_type in
        mkdir_all ~is_file:true target_file ;
        if force_rebuild || check src_file target_file then (
          (* Make the target appear in the FS as atomically as possible as
           * we might be overwriting a running worker, which would be
           * confusing for the supervisor. Note that RamenCompiler actually
           * pays no attention to the target file name. *)
          let tmp_file = target_file ^"_tmp" in
          builder conf get_parent program_name src_file tmp_file ;
          Unix.rename tmp_file target_file
        ) else
          !logger.debug "%S is still up to date" target_file ;
        loop target_file rest
  in
  let from_type = file_ext src_file
  and to_type = file_ext target_file in
  let path = find_path from_type to_type in
  loop src_file path
