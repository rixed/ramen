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
    keep_temp_files : bool }

let tmp_input_of_func persist_dir program_name func_name in_type =
  persist_dir ^"/workers/inputs/"^ program_name ^"/"^ func_name ^"/"
              ^ RamenTuple.type_signature in_type

let upload_dir_of_func persist_dir program_name func_name in_type =
  tmp_input_of_func persist_dir program_name func_name in_type ^"/uploads"

exception InvalidCommand of string
exception CannotFindBinary of string
let () =
  Printexc.register_printer (function
    | CannotFindBinary s ->
        Some (Printf.sprintf "Cannot find worker executable %s" s)
    | InvalidCommand s ->
        Some (Printf.sprintf "Invalid command: %s" s)
    | _ -> None)

(* Runtime configuration (also embedded in the workers): *)
module Func =
struct
  type t =
    { program_name : string ;
      name : string ; (* TODO: name must be allowed to contain params *)
      mutable params : RamenTuple.params ;
      in_type : RamenTuple.typed_tuple ;
      out_type : RamenTuple.typed_tuple ;
      (* The binary provides a signature with default parameters.
       * As soon as the actual parameters are known, a new signature
       * is computed so that two running programs using the same binary
       * but different params have a different signature.
       * Those signatures are used to distinguish sets of ringbufs
       * or any other files where tuples are stored, so that those files
       * change when the code change, without a need to also change the
       * name of the operation. *)
      signature : string ;
      parents : (string * string) list ;
      force_export : bool ;
      merge_inputs : bool ;
      event_time : RamenEventTime.t option }
    [@@ppp PPP_OCaml]

  let print_parent oc (p, f) = Printf.fprintf oc "%s/%s" p f

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
  type t = Func.t list [@@ppp PPP_OCaml]

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

  (* TODO: cache if binary hasn't changed or asked very recently *)
  let of_bin fname : t =
    !logger.debug "Reading config from %s..." fname ;
    match Unix.open_process_in fname with
    | exception Unix.Unix_error (ENOENT, _, _) ->
        raise (CannotFindBinary fname)
    | ic ->
        let close () =
          match Unix.close_process_in ic with
          | Unix.WEXITED o -> ()
          | s ->
              !logger.error "Process %s exited with %s"
                fname
                (string_of_process_status s) in
        try
          finally close Marshal.from_channel ic
        with BatInnerIO.No_more_input ->
          raise (CannotFindBinary fname)

  let bin_of_program_name root_path program_name =
    (* Use an extension so we can still use the plain program_name for a
     * directory holding subprograms. Not using "exe" as it remind me of
     * that operating system, but rather "x" as in the x bit: *)
    root_path ^"/"^ program_name ^".x"

  let of_program_name root_path program_name : t =
    bin_of_program_name root_path program_name |> of_bin
end

let program_func_of_user_string ?default_program s =
  let s = String.trim s in
  (* rsplit because we might want to have '/'s in the program name. *)
  try String.rsplit ~by:"/" s
  with Not_found ->
    match default_program with
    | Some l -> l, s
    | None ->
        !logger.error "Cannot find function %S" s ;
        raise Not_found

(* Cannot be in RamenHelpers since it depends on PPP and CodeGen depends on
 * RamenHelpers: *)
let ppp_of_file fname ppp =
  let openflags = [ Open_rdonly; Open_text ] in
  match Pervasives.open_in_gen openflags 0o644 fname with
  | exception e ->
      !logger.warning "Cannot open %S for reading: %s"
        fname (Printexc.to_string e) ;
      raise e
  | ic ->
      finally
        (fun () -> Pervasives.close_in ic)
        (PPP.of_in_channel_exc ppp) ic

let ppp_to_file fname ppp v =
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
    parameters : RamenTuple.params [@ppp_default []] }
  [@@ppp PPP_OCaml]
(* The must_run file gives us the unique names of the programs. *)
type must_run_file = (string, must_run_entry) Hashtbl.t [@@ppp PPP_OCaml]

let print_must_run_entry oc mre =
  Printf.fprintf oc "{ bin: %S; parameters: %S }"
    mre.bin (RamenTuple.string_of_params mre.parameters)
let print_running_programs oc =
  Hashtbl.print String.print print_must_run_entry oc

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
 * the Func.t list, with the actual params in the functions. *)
let program_of_running_entry mre =
  let prog = Program.of_bin mre.bin in
  List.iter (fun func ->
    func.Func.params <- RamenTuple.overwrite_params func.Func.params mre.parameters ;
  ) prog ;
  prog

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
        !logger.debug "Took graph lock (read)" ;
        let programs =
          (* TODO: cache reading the rc file *)
          read_rc_file conf.do_persist rc_file |>
          Hashtbl.map (fun _ mre ->
            memoize (fun () -> mre.bin, program_of_running_entry mre)) in
        let%lwt x = f programs in
        !logger.debug "Release graph lock (read)" ;
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
        !logger.debug "Took graph lock (write)" ;
        let programs = read_rc_file conf.do_persist rc_file in
        let%lwt ret = f programs in
        (* Save the config only if f did not fail: *)
        save_rc_file conf.do_persist rc_file programs ;
        !logger.debug "Release graph lock (write)" ;
        return ret)
    with RetryLater s ->
      Lwt_unix.sleep s >>= loop
  in
  loop ()

let find_func programs program_name func_name =
  let _bin, rc = Hashtbl.find programs program_name () in
  List.find (fun f -> f.Func.name = func_name) rc

let make_conf ?(do_persist=true) ?(debug=false) ?(keep_temp_files=false)
              persist_dir =
  { do_persist ; debug ; persist_dir ; keep_temp_files }

(* Various directory names: *)

let type_signature_hash = md5 % RamenTuple.type_signature

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
                   ^"/"^ func.Func.program_name
                   ^"/"^ func.Func.name
                   ^"/"^ sign ^"/"

let in_ringbuf_name_single conf func =
  in_ringbuf_name_base conf func ^ "all.r"

let in_ringbuf_name_merging conf func parent_index =
  in_ringbuf_name_base conf func ^
    string_of_int parent_index ^".r"

let in_ringbuf_names conf func =
  if func.Func.merge_inputs then
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
                   ^"/"^ func.Func.program_name
                   ^"/"^ func.Func.name
                   ^"/"^ sign ^"/archive.b"

(* Operations are told where to write their output (and which selection of
 * fields) by another file, the "out-ref" file, which is a kind of symbolic
 * link with several destinations (plus a format, plus an expiry date).
 * like the above archive file, the out_ref files must be identified by the
 * operation name and its output type: *)
let out_ringbuf_names_ref conf func =
  let sign = type_signature_hash func.Func.out_type in
  conf.persist_dir ^"/workers/out_ref/"
                   ^ RamenVersions.out_ref
                   ^"/"^ func.Func.program_name
                   ^"/"^ func.Func.name
                   ^"/"^ sign ^"/out_ref"

(* Finally, operations have two additional output streams: one for
 * instrumentation statistics, and one for notifications. Both are
 * common to all running operations. *)
let report_ringbuf conf =
  conf.persist_dir ^"/instrumentation_ringbuf/"
                   ^ RamenVersions.instrumentation_tuple ^"_"
                   ^ RamenVersions.ringbuf
                   ^"/ringbuf.b"

let notify_ringbuf ?(test_id = "") conf =
  conf.persist_dir ^"/notify_ringbuf/"
                   ^ RamenVersions.notify_tuple ^"_"
                   ^ RamenVersions.ringbuf
                   ^ (if test_id = "" then "" else "/"^ test_id)
                   ^"/ringbuf.r"
