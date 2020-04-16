(* Various path on the file-system where supervisor, alerter and tester store
 * various things. *)
open Batteries
module N = RamenName
module O = RamenOperation
module VSI = RamenSync.Value.SourceInfo
module VOS = RamenSync.Value.OutputSpecs
module Versions = RamenVersions
module Files = RamenFiles

(* The "in" ring-buffers are used to store tuple received by an operation.
 * We want that file to be unique for a given operation name and to change
 * whenever the input type of this operation changes. On the other hand, we
 * would like to keep it in case of a change in code that does not change
 * the input type because data not yet read would still be valid. So we
 * name that file after the function full name and its input type
 * signature. *)

let in_ringbuf_name_base persist_dir pname func =
  N.path_cat
    [ persist_dir ; N.path "workers/ringbufs" ;
      N.path RamenVersions.ringbuf ; VSI.fq_path pname func ;
      N.path func.in_signature ]

let in_ringbuf_name persist_dir pname func =
  N.path_cat [ in_ringbuf_name_base persist_dir pname func ; N.path "all.r" ]

let type_signature_hash = N.md5 % RamenTuple.type_signature

(* Operations can also be asked to output their full result (all the public
 * fields) in a non-wrapping file for later retrieval by the tail or
 * timeseries commands.
 * We want those files to be identified by the name of the operation and
 * the output type of the operation. *)
let archive_buf_name ~file_type persist_dir pname func =
  let ext =
    match file_type with
    | VOS.RingBuf -> "b"
    | VOS.Orc _ -> "orc" in
  let sign =
    O.out_type_of_operation ~with_private:false func.VSI.operation |>
    type_signature_hash in
  N.path_cat
    [ persist_dir ; N.path "workers/ringbufs" ;
      N.path RamenVersions.ringbuf ; VSI.fq_path pname func ;
      N.path sign ; N.path ("archive."^ ext) ]

(* Every function with factors will have a file sequence storing possible
 * values encountered for that time range. This is so that we can quickly
 * do autocomplete for graphite metric names regardless of archiving. This
 * could also be used to narrow down the time range of a replays in presence
 * of filtering by a factor.
 * Those files are cleaned by the GC according to retention times only -
 * they are not taken into account for size computation, as they are
 * unrelated to archives and are supposed to be small anyway.
 * This function merely returns the directory name where the factors possible
 * values are saved. Then, the factor field name (with '/' url-encoded) gives
 * the name of the directory containing a file per time slice (named
 * begin_end). *)
let factors_of_function persist_dir pname func =
  let sign =
    O.out_type_of_operation ~with_private:false func.VSI.operation |>
    type_signature_hash in
  N.path_cat
    [ persist_dir ; N.path "workers/factors" ;
      N.path RamenVersions.factors ; N.path Config.version ;
      VSI.fq_path pname func ;
      (* extension for the GC. *)
      N.path (sign ^".factors") ]

(* Finally, operations have two additional output streams: one for
 * instrumentation statistics, and one for notifications. Both are
 * common to all running operations, low traffic, and archived. *)
let report_ringbuf persist_dir =
  N.path_cat
    [ persist_dir ; N.path "instrumentation_ringbuf" ;
      N.path (RamenVersions.instrumentation_tuple ^"_"^
              RamenVersions.ringbuf) ;
      N.path "ringbuf.r" ]

let notify_ringbuf persist_dir =
  N.path_cat
    [ persist_dir ; N.path "notify_ringbuf" ;
      N.path (RamenVersions.notify_tuple ^"_"^ RamenVersions.ringbuf) ;
      N.path "ringbuf.r" ]

(* This is not a ringbuffer but a mere snapshot of the alerter state: *)
let pending_notifications_file persist_dir =
  N.path_cat
    [ persist_dir ;
      N.path ("pending_notifications_" ^ RamenVersions.pending_notify ^"_"^
              RamenVersions.notify_tuple) ]

let precompserver_cache_file persist_dir src_path ext =
  N.path_cat [ persist_dir ; N.path "precompserver/cache" ;
               N.path Versions.codegen ;
               N.path ((src_path : N.src_path :> string) ^"."^ ext) ]

let execompserver_cache_file persist_dir fname ext =
  N.path_cat [ persist_dir ; N.path "execompserver/cache" ;
               N.path Versions.codegen ; N.cat fname (N.path ("."^ ext)) ]

(* We want the name of the executable to depend on the codegen version, and all
 * other version numbers the executable depends on, such as out_ref version it
 * can parse, instrumentation and notification tuples it can write, ringbuf it
 * can read and write, worker_state, binocle, experiment, factors, services,
 * and sync_conf. And on top of that, we want to force a recompilation after any
 * release update (for security, in case one of the above mentioned version
 * number have not been increased when it should. *)
let execompserver_cache_bin =
  let versions =
    Versions.[
      codegen ; out_ref ; instrumentation_tuple ; notify_tuple ; ringbuf ;
      worker_state ; binocle ; experiment ; factors ; services ; sync_conf ;
      release_tag ] |>
    String.join "_" |>
    N.md5 in
  fun persist_dir info_sign ->
    let bin_name = info_sign ^"_"^ versions in
    execompserver_cache_file persist_dir (N.path bin_name) "x"

(* Location of server key files: *)
let default_srv_pub_key_file persist_dir =
  N.path_cat [ persist_dir ; N.path "confserver/public_key" ]

let default_srv_priv_key_file persist_dir =
  N.path_cat [ persist_dir ; N.path "confserver/private_key" ]

(* Location of the LMDB database used for global variables: *)

let globals_dir persist_dir =
  N.path_cat [ persist_dir ; N.path "supervisor/globals.lmdb" ]

let bin_of_program_name lib_path program_name =
  (* Use an extension so we can still use the plain program_name for a
   * directory holding subprograms. Not using "exe" as it remind me of
   * that operating system, but rather "x" as in the x bit: *)
  N.path_cat
    [ lib_path ;
      Files.add_ext (N.path_of_program ~suffix:false program_name) "x" ]

(* Each workers regularly snapshot its internal state in this file.
 * This data contains tuples and stateful function internal states, so
 * that it has to depend not only on worker_state version (which versions
 * the structure of the state structure itself), but also on codegen
 * version (which versions the language/state), the parameters signature,
 * and also the OCaml version itself since we use stdlib's Marshaller: *)
let state_file_path persist_dir src_path worker_sign =
  N.path_cat
    [ persist_dir ; N.path "workers/states" ;
      N.path Versions.(worker_state ^"_"^ codegen) ;
      N.path Config.version ; N.path (src_path : N.src_path :> string) ;
      N.path worker_sign ; N.path "snapshot" ]
