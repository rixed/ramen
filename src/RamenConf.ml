open Batteries
open RamenLog
open RamenHelpers
module Expr = RamenExpr

let archive_file dir (block_start, block_stop) =
  dir ^"/"^ string_of_int block_start ^"-"^ string_of_int block_stop

type conf =
  { debug : bool ;
    persist_dir : string ;
    do_persist : bool ; (* false for tests *)
    keep_temp_files : bool ;
    initial_export_duration : float }

let tmp_input_of_func persist_dir program_name func_name in_type =
  persist_dir ^"/workers/inputs/"^ program_name ^"/"^ func_name ^"/"
              ^ RamenTuple.type_signature in_type

let upload_dir_of_func persist_dir program_name func_name in_type =
  tmp_input_of_func persist_dir program_name func_name in_type ^"/uploads"

(* Runtime configuration (also embedded in the workers): *)
module Func =
struct
  type t =
    { mutable exp_program_name : string ; (* expansed name *)
      name : string ; (* TODO: name must be allowed to contain params *)
      in_type : RamenTuple.typed_tuple ;
      out_type : RamenTuple.typed_tuple ;
      (* The signature identifies the code but not the actual parameters.
       * Those signatures are used to distinguish sets of ringbufs
       * or any other files where tuples are stored, so that those files
       * change when the code change, without a need to also change the
       * name of the operation. *)
      signature : string ;
      parents : (string option (* expansed program name or None if this
                                  program *) *
                 string (* function name within that program *)) list ;
      merge_inputs : bool ;
      event_time : RamenEventTime.t option ;
      factors : string list ;
      (* List of envvar used in that function: *)
      envvars : string list }
    [@@ppp PPP_OCaml]

  let print_parent oc = function
    | None, f -> String.print oc f
    | Some p, f -> Printf.fprintf oc "%s/%s" p f

  let fq_name f = f.exp_program_name ^"/"^ f.name

  let sanitize_name name =
    (* New lines have to be forbidden because of the out_ref ringbuf files.
     * slashes have to be forbidden because we rsplit to get program names.
     *)
    if name = "" ||
       String.fold_left (fun bad c ->
         bad || c = '\n' || c = '\r' || c = '/') false name then
      invalid_arg "operation name"
end

module Program =
struct
  type t =
    { name : string ;
      mutable params : RamenTuple.params [@ppp_default []] ;
      funcs : Func.t list }
      [@@ppp PPP_OCaml]

  let sanitize_name name =
    let rec remove_heading_slashes s =
      if String.length s > 0 && s.[0] = '/' then
        remove_heading_slashes (String.lchop s)
      else s in
    let name = remove_heading_slashes name in
    if name = "" then
      failwith "Programs must have non-empty names" else
    if has_dotnames name then
      failwith "Program names cannot include directory dotnames" else
    simplified_path name

  let expansed_name t =
    RamenLang.string_of_program_id (t.name, t.params)

  let of_bin =
    (* Cache of path to date of last read and program *)
    let reread_data fname : t =
      !logger.debug "Reading config from %s..." fname ;
      let empty_program = { name = ""; funcs = []; params = [] } in
      match with_stdout_from_command
              fname [| fname ; "version" |] Legacy.input_line with
      | exception e ->
          !logger.error "Cannot get version from %s: %s"
            fname (Printexc.to_string e) ;
          empty_program
      | v when v = RamenVersions.codegen ->
          (try with_stdout_from_command
                 fname [| fname ; "1nf0" |] Legacy.Marshal.from_channel
           with e ->
             !logger.error "Cannot get 1nf0 from %s: %s"
               fname (Printexc.to_string e) ;
             empty_program)
      | v ->
        !logger.error "Executable %s is for version %s (I'm version %s)"
          fname v RamenVersions.codegen ;
        empty_program
    and age_of_data fname =
      try mtime_of_file fname
      with e ->
        !logger.error "Cannot get mtime of %s: %s"
          fname (Printexc.to_string e) ;
        0.
    in
    let get_prog = cached reread_data age_of_data in
    fun params fname ->
      let p = get_prog fname in
      (* Patch actual parameters: *)
      p.params <-
        RamenTuple.overwrite_params p.params params ;
      let exp_program_name =
        RamenLang.string_of_program_id (p.name, params) in
      List.iter (fun f ->
        f.Func.exp_program_name <- exp_program_name
      ) p.funcs ;
      p

  let bin_of_program_name root_path program_name =
    (* Use an extension so we can still use the plain program_name for a
     * directory holding subprograms. Not using "exe" as it remind me of
     * that operating system, but rather "x" as in the x bit: *)
    root_path ^"/"^ program_name ^".x"

  (* Used only by RamenCompiler to get the output type of the parents of
   * compiled functions. Therefore we do not care about the actual
   * parameters of the actual parents (which can not affect types). *)
  let of_program_id root_path (program_name, params) =
    bin_of_program_name root_path program_name |> of_bin params
end

let program_func_of_user_string ?default_program s =
  let s = String.trim s in
  (* rsplit because we might want to have '/'s in the program name. *)
  try String.rsplit ~by:"/" s
  with Not_found ->
    match default_program with
    | Some l -> l, s
    | None ->
        let e = Printf.sprintf "Cannot find function %S" s in
        !logger.error "%s" e ;
        failwith e

(* Cannot be in RamenHelpers since it depends on PPP and CodeGen depends on
 * RamenHelpers: *)
let ppp_of_file ?(error_ok=false) fname ppp =
  let openflags = [ Open_rdonly; Open_text ] in
  match Pervasives.open_in_gen openflags 0o644 fname with
  | exception e ->
      (if error_ok then !logger.debug else !logger.warning)
        "Cannot open %S for reading: %s" fname (Printexc.to_string e) ;
      raise e
  | ic ->
      finally
        (fun () -> Pervasives.close_in ic)
        (PPP.of_in_channel_exc ppp) ic

let ppp_to_file fname ppp v =
  mkdir_all ~is_file:true fname ;
  let openflags = [ Open_wronly; Open_creat; Open_trunc; Open_text ] in
  match Pervasives.open_out_gen openflags 0o644 fname with
  | exception e ->
      !logger.warning "Cannot open %S for writing: %s" fname (Printexc.to_string e) ;
      raise e
  | oc ->
      finally
        (fun () -> Pervasives.close_out oc)
        (PPP.to_out_channel ppp oc) v

let running_config_file conf =
  conf.persist_dir ^"/configuration/"^ RamenVersions.graph_config ^"/rc"

(* Saved configuration is merely a hash from (unique) program names
 * to their binaries, and parameters: *)
type must_run_entry =
  { bin : string ;
    params : RamenTuple.params [@ppp_default []] }
  [@@ppp PPP_OCaml]
(* The must_run file gives us the unique names of the programs. *)
type must_run_file = (string, must_run_entry) Hashtbl.t [@@ppp PPP_OCaml]

(* For tests we don't store the rc_file on disk but in there: *)
let non_persisted_programs = ref (Hashtbl.create 11)

let read_rc_file do_persist rc_file =
  if do_persist then
    try
      ppp_of_file rc_file must_run_file_ppp_ocaml
    with
    | e ->
      !logger.debug "Cannot read configuration: %s. Starting anew."
        (Printexc.to_string e) ;
      Hashtbl.create 1
  else !non_persisted_programs

let save_rc_file do_persist rc_file rc =
  if do_persist then
    ppp_to_file rc_file must_run_file_ppp_ocaml rc

(* Users wanting to know the running config must use with_{r,w}lock.
 * This will return a hash from program name to a function returning
 * the Program.t (with the actual params). *)
let program_of_running_entry mre =
  Program.of_bin mre.params mre.bin

exception RetryLater of float

(* [f] takes a hash-table of program name to running-config getter.
 * Modifications will not be saved. *)
let with_rlock conf f =
  let open Lwt in
  let rc_file = running_config_file conf in
  ensure_file_exists rc_file ;
  mkdir_all ~is_file:true rc_file ;
  let rec loop () =
    try%lwt
      RamenAdvLock.with_r_lock rc_file (fun () ->
        let programs =
          (* TODO: cache reading the rc file *)
          read_rc_file conf.do_persist rc_file |>
          Hashtbl.map (fun _ mre ->
            memoize (fun () -> mre.bin, program_of_running_entry mre)) in
        let%lwt x = f programs in
        return x)
    with RetryLater s ->
      Lwt_unix.sleep s >>= loop
  in
  loop ()

let with_wlock conf f =
  let open Lwt in
  let rc_file = running_config_file conf in
  ensure_file_exists rc_file ;
  let rec loop () =
    try%lwt
      RamenAdvLock.with_w_lock rc_file (fun () ->
        let programs = read_rc_file conf.do_persist rc_file in
        let%lwt ret = f programs in
        (* Save the config only if f did not fail: *)
        save_rc_file conf.do_persist rc_file programs ;
        return ret)
    with RetryLater s ->
      Lwt_unix.sleep s >>= loop
  in
  loop ()

let find_func programs exp_program_name func_name =
  let _bin, prog = Hashtbl.find programs exp_program_name () in
  prog, List.find (fun f -> f.Func.name = func_name) prog.Program.funcs

let make_conf ?(do_persist=true) ?(debug=false) ?(keep_temp_files=false)
              ?(forced_variants=[]) ?(initial_export_duration=900.)
              persist_dir =
  RamenExperiments.set_variants persist_dir forced_variants ;
  { do_persist ; debug ; persist_dir ; keep_temp_files ;
    initial_export_duration }

(* Various directory names: *)

let type_signature_hash = md5 % RamenTuple.type_signature

(* Each workers regularly snapshot its internal state in this file: *)
let worker_state conf func params =
  conf.persist_dir ^"/workers/states/"
                   ^ RamenVersions.worker_state
                   ^"/"^ Config.version
                   ^"/"^ Func.fq_name func
                   ^"/"^ func.signature
                   ^"/"^ RamenTuple.param_signature params
                   ^"/snapshot"

(* The "in" ring-buffers are used to store tuple received by an operation.
 * We want that file to be unique for a given operation name and to change
 * whenever the input type of this operation changes. On the other hand, we
 * would like to keep it in case of a change in code that does not change
 * the input type because data not yet read would still be valid. So we
 * name that file after the function full name and its input type
 * signature.
 * Then some operations have all parents write in a single ring-buffer
 * (called "all.r") and some (those performing a MERGE operation) have one
 * ring-buffer per parent (called after the number of the parent in the
 * FROM clause): *)

let in_ringbuf_name_base conf func =
  let sign = type_signature_hash func.Func.in_type in
  conf.persist_dir ^"/workers/ringbufs/"
                   ^ RamenVersions.ringbuf
                   ^"/"^ Func.fq_name func
                   ^"/"^ sign ^"/"

let in_ringbuf_name_single conf func =
  in_ringbuf_name_base conf func ^ "all.r"

let in_ringbuf_name_merging conf func parent_index =
  in_ringbuf_name_base conf func ^
    string_of_int parent_index ^".r"

let in_ringbuf_names conf func =
  if func.Func.parents = [] then []
  else if func.Func.merge_inputs then
    List.mapi (fun i _ ->
      in_ringbuf_name_merging conf func i
    ) func.Func.parents
  else
    [ in_ringbuf_name_single conf func ]

(* Operations can also be asked to output their full result (all the public
 * fields) in a non-wrapping file for later retrieval by the tail or
 * timeseries commands.
 * We want those files to be identified by the name of the operation and
 * the output type of the operation. *)
let archive_buf_name conf func =
  let sign = type_signature_hash func.Func.out_type in
  conf.persist_dir ^"/workers/ringbufs/"
                   ^ RamenVersions.ringbuf
                   ^"/"^ Func.fq_name func
                   ^"/"^ sign ^"/archive.b"

(* Each individual ringbuf may have a file storing all possible values for
 * special fields identified as factors: *)
let factors_of_ringbuf fname factor =
  Filename.remove_extension fname ^".factors."^ factor

(* Operations are told where to write their output (and which selection of
 * fields) by another file, the "out-ref" file, which is a kind of symbolic
 * link with several destinations (plus a format, plus an expiry date).
 * like the above archive file, the out_ref files must be identified by the
 * operation name and its output type: *)
let out_ringbuf_names_ref conf func =
  let sign = type_signature_hash func.Func.out_type in
  conf.persist_dir ^"/workers/out_ref/"
                   ^ RamenVersions.out_ref
                   ^"/"^ Func.fq_name func
                   ^"/"^ sign ^"/out_ref"

(* Finally, operations have two additional output streams: one for
 * instrumentation statistics, and one for notifications. Both are
 * common to all running operations. *)
let report_ringbuf conf =
  conf.persist_dir ^"/instrumentation_ringbuf/"
                   ^ RamenVersions.instrumentation_tuple ^"_"
                   ^ RamenVersions.ringbuf
                   ^"/ringbuf.b"

let notify_ringbuf conf =
  conf.persist_dir ^"/notify_ringbuf/"
                   ^ RamenVersions.notify_tuple ^"_"
                   ^ RamenVersions.ringbuf
                   ^"/ringbuf.r"

let pending_notifications_file conf =
  conf.persist_dir ^"/pending_notifications_"
                   ^ RamenVersions.pending_notify ^"_"
                   ^ RamenVersions.notify_tuple
