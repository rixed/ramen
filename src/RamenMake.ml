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
 * version), ".x" is for executable, ".alert" for (current version of) alerts...
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
open RamenSync
module C = RamenConf
module F = C.Func
module P = C.Program
module N = RamenName
module Files = RamenFiles

(* Raise any exception on failure: *)
type builder =
  C.conf ->
  (N.program -> RamenProgram.P.t) ->
  N.program -> N.path -> N.path -> unit

(* Tells if the target must be rebuilt from the source: *)
type check = N.path -> N.path -> bool

(*
 * Possible check functions
 *)

(* Using files presence and modification time.
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
      wait_source_in_past () ;
      true
  | tt ->
      tt <= st

let target_is_obsolete target_file =
  match P.version_of_bin target_file with
  | exception e ->
      !logger.warning "Cannot get version from %a: %s, assuming obsolescence"
        N.path_print target_file (Printexc.to_string e) ;
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

let write_source_info fname (i : Value.SourceInfo.t) =
  Marshal.(to_string i [ No_sharing ]) |>
  Files.write_whole_file fname

let read_source_info fname : Value.SourceInfo.t =
  let s = Files.read_whole_file fname in
  Marshal.from_string s 0

(* Register separate builders from ".ramen" to ".info" and from there to
 * ".x". *)
let () =
  register "ramen" "info"
    (fun src_file target_file ->
      not (Files.exists target_file) ||
      target_is_older src_file target_file)
    (fun conf get_parent program_name src_file target_file ->
      let info =
        let md5 = Files.read_whole_file src_file |> N.md5 in
        match RamenCompiler.precompile conf get_parent src_file program_name with
        | exception e ->
            let s = Printexc.to_string e in
            Value.SourceInfo.{ md5 ; detail = Failed { err_msg = s } }
        | i ->
            Value.SourceInfo.{ md5 ; detail = Compiled i } in
      write_source_info target_file info)

(* Register a builder that will carry on from ".info" and generate actual
 * executable in ".x". *)
let () =
  register "info" "x"
    (fun src_file target_file ->
      target_is_older src_file target_file ||
      target_is_obsolete target_file)
    (fun conf _get_parent program_name src_file exec_file ->
      let info = read_source_info src_file in
      match info.detail with
      | Compiled info ->
          let base_file = Files.remove_ext src_file in
          RamenCompiler.compile conf info ~exec_file base_file program_name
      | Failed { err_msg } ->
          failwith err_msg)

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
let build conf ?(force_rebuild=false) get_parent program_name
          (src_file : N.path) (target_file : N.path) =
  let base_file = Files.remove_ext target_file in
  let rec loop src_file = function
    | [] ->
        !logger.debug "Done recompiling %a" N.path_print_quoted target_file
    | (to_type, check, builder) :: rest ->
        let target_file = N.cat base_file (N.path ("."^ to_type)) in
        Files.mkdir_all ~is_file:true target_file ;
        if force_rebuild || check src_file target_file then (
          (* Make the target appear in the FS as atomically as possible as
           * we might be overwriting a running worker, which would be
           * confusing for the supervisor. Note that RamenCompiler actually
           * pays no attention to the target file name. *)
          let tmp_file = N.cat target_file (N.path "_tmp") in
          builder conf get_parent program_name src_file tmp_file ;
          Files.rename tmp_file target_file
        ) else
          !logger.debug "%a is still up to date"
            N.path_print_quoted target_file ;
        loop target_file rest
  in
  let from_type = Files.ext src_file
  and to_type = Files.ext target_file in
  let path = find_path from_type to_type in
  loop src_file path
