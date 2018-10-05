open Batteries
open RamenLog
open RamenHelpers
module Expr = RamenExpr

let archive_file dir (block_start, block_stop) =
  dir ^"/"^ string_of_int block_start ^"-"^ string_of_int block_stop

type conf =
  { log_level : log_level ;
    persist_dir : string ;
    do_persist : bool ; (* false for unit-tests *)
    test : bool ; (* true within `ramen test` *)
    keep_temp_files : bool ;
    initial_export_duration : float }

let tmp_input_of_func persist_dir program_name func_name in_type =
  persist_dir ^"/workers/inputs/"^ program_name ^"/"^ func_name ^"/"
              ^ RamenTuple.type_signature in_type

let upload_dir_of_func persist_dir program_name func_name in_type =
  tmp_input_of_func persist_dir program_name func_name in_type ^"/uploads"

(* Configuration that's embedded in the workers: *)
module Func =
struct
  type parent =
    RamenName.rel_program option * RamenName.func
    [@@ppp PPP_OCaml]

  type t =
    { program_name : RamenName.program ;
      name : RamenName.func ;
      doc : string ;
      in_type : RamenTuple.typ ;
      mutable out_type : RamenTuple.typ ;
      (* The signature identifies the code but not the actual parameters.
       * Those signatures are used to distinguish sets of ringbufs
       * or any other files where tuples are stored, so that those files
       * change when the code change, without a need to also change the
       * name of the operation. *)
      mutable signature : string ;
      parents : parent list ;
      merge_inputs : bool ;
      mutable event_time : RamenEventTime.t option ;
      mutable factors : string list ;
      (* List of envvar used in that function: *)
      envvars : string list }
    [@@ppp PPP_OCaml]

  (* TODO: takes a func instead of child_prog? *)
  let program_of_parent_prog child_prog = function
    | None -> child_prog
    | Some rel_prog ->
        RamenName.(program_of_rel_program child_prog rel_prog)

  let print_parent oc = function
    | None, f ->
        String.print oc (RamenName.string_of_func f)
    | Some p, f ->
        Printf.fprintf oc "%s/%s"
          (RamenName.string_of_rel_program p)
          (RamenName.string_of_func f)

  (* Only for debug or keys, not for paths! *)
  let fq_name f = RamenName.fq f.program_name f.name

  let path f =
    RamenName.path_of_program f.program_name
    ^"/"^ RamenName.string_of_func f.name

  let signature func op_str params =
    (* We'd like to be formatting independent so that operation text can be
     * reformatted without ramen recompiling it. For this it is not OK to
     * strip redundant white spaces as some of those might be part of literal
     * string values. So we print it, trusting the printer to be exhaustive.
     * This is not enough to print the expression with types, as those do not
     * contain relevant info such as field rank. We therefore print without
     * types and encode input/output types explicitly below: *)
    "OP="^ op_str ^
    ";IN="^ RamenTuple.type_signature func.in_type ^
    (* type_signature does not look at private fields: *)
    ";OUT="^ RamenTuple.type_signature func.out_type ^
    (* Similarly to input type, also depends on the parameters type: *)
    ";PRM="^ RamenTuple.param_types_signature params |>
    md5

  let dump_io func =
    !logger.debug "func %S:\n\tinput type: %a\n\toutput type: %a"
      (RamenName.string_of_func func.name)
      RamenTuple.print_typ func.in_type
      RamenTuple.print_typ func.out_type
end

module Program =
struct
  type t =
    { params : RamenTuple.params [@ppp_default []] ;
      funcs : Func.t list }
      [@@ppp PPP_OCaml]

  let of_bin =
    (* Cache of path to date of last read and program *)
    let reread_data fname : t =
      !logger.debug "Reading config from %s..." fname ;
      match with_stdout_from_command ~expected_status:0
              fname [| fname ; "version" |] Legacy.input_line with
      | exception e ->
          let err = Printf.sprintf "Cannot get version from %s: %s"
                      fname (Printexc.to_string e) in
          !logger.error "%s" err ;
          failwith err
      | v when v = RamenVersions.codegen ->
          (try with_stdout_from_command ~expected_status:0
                 fname [| fname ; "1nf0" |] Legacy.Marshal.from_channel
           with e ->
             let err = Printf.sprintf "Cannot get 1nf0 from %s: %s"
                         fname (Printexc.to_string e) in
             !logger.error "%s" err ;
             failwith err)
      | v ->
        let err = Printf.sprintf "Executable %s is for version %s \
                                  (I'm version %s)"
                    fname v RamenVersions.codegen in
        !logger.error "%s" err ;
        failwith err
    and age_of_data fname =
      try mtime_of_file fname
      with e ->
        !logger.error "Cannot get mtime of %s: %s"
          fname (Printexc.to_string e) ;
        0.
    in
    let get_prog = cached "of_bin" reread_data age_of_data in
    fun program_name params fname ->
      let p = get_prog fname in
      (* Patch actual parameters (in a _new_ prog not the cached one!): *)
      { params = RamenTuple.overwrite_params p.params params ;
        funcs = List.map (fun f -> Func.{ f with program_name }) p.funcs }

  let bin_of_program_name root_path program_name =
    (* Use an extension so we can still use the plain program_name for a
     * directory holding subprograms. Not using "exe" as it remind me of
     * that operating system, but rather "x" as in the x bit: *)
    root_path ^"/"^ RamenName.path_of_program program_name ^".x"
end

let running_config_file conf =
  conf.persist_dir ^"/configuration/"^ RamenVersions.graph_config ^"/rc"

(* Saved configuration is merely a hash from (unique) program names
 * to their binaries, and parameters (actual ones, not default values): *)
type must_run_entry =
  { (* Tells whether the entry must actually be started. Set to true
       at exit so that we do not loose information of previously run
       entries. *)
    mutable killed : bool [@ppp_default false] ;
    (* Should this worker be started in debug mode regardless of supervisor
     * mode? *)
    debug : bool [@ppp_default false] ;
    (* Stat report period: *)
    report_period : float [@ppp_default RamenConsts.Default.report_period] ;
    (* Full path to the worker's binary: *)
    bin : string ;
    (* "Command line" for that worker: *)
    params : RamenName.params [@ppp_default Hashtbl.create 0] ;
    (* Optionally, file from which this worker can be (re)build (see RamenMake).
     * When it is rebuild, relative parents are found using the program name that's
     * the key in the running config. *)
    source : string option [@ppp_default None] }
  [@@ppp PPP_OCaml]
(* The must_run file gives us the unique names of the programs. *)
type must_run_file = (RamenName.program, must_run_entry) Hashtbl.t
  [@@ppp PPP_OCaml]

(* For tests we don't store the rc_file on disk but in there: *)
let non_persisted_programs = ref (Hashtbl.create 11)

let read_rc_file =
  let get = ppp_of_file must_run_file_ppp_ocaml in
  fun do_persist fname ->
    if do_persist then get fname
    else !non_persisted_programs

let save_rc_file do_persist rc_file rc =
  if do_persist then
    ppp_to_file rc_file must_run_file_ppp_ocaml rc

(* Users wanting to know the running config must use with_{r,w}lock.
 * This will return a hash from program name to a function returning
 * the Program.t (with the actual params). *)
let program_of_running_entry program_name mre =
  Program.of_bin program_name mre.params mre.bin

(* [f] takes a hash-table of program name to running-config getter.
 * Modifications will not be saved. *)
let with_rlock conf f =
  let rc_file = running_config_file conf in
  ensure_file_exists ~contents:"{}" rc_file ;
  mkdir_all ~is_file:true rc_file ;
  RamenAdvLock.with_r_lock rc_file (fun () ->
    let programs =
      read_rc_file conf.do_persist rc_file |>
      Hashtbl.map (fun pn mre ->
        mre,
        memoize (fun () -> program_of_running_entry pn mre)) in
    f programs)

let with_wlock conf f =
  let rc_file = running_config_file conf in
  ensure_file_exists ~contents:"{}" rc_file ;
  RamenAdvLock.with_w_lock rc_file (fun () ->
    let programs = read_rc_file conf.do_persist rc_file in
    let ret = f programs in
    (* Save the config only if f did not fail: *)
    save_rc_file conf.do_persist rc_file programs ;
    ret)

let last_conf_mtime conf =
  running_config_file conf |> mtime_of_file_def 0.

let find_func programs program_name func_name =
  let _mre, get_rc =
    Hashtbl.find programs program_name in
  let prog = get_rc () in
  prog, List.find (fun f -> f.Func.name = func_name) prog.Program.funcs

let make_conf ?(do_persist=true) ?(debug=false) ?(quiet=false)
              ?(keep_temp_files=false) ?(forced_variants=[])
              ?(initial_export_duration=900.) ?(test=false)
              persist_dir =
  if debug && quiet then
    failwith "Options --debug and --quiet are incompatible." ;
  let log_level =
    if debug then Debug else if quiet then Quiet else Normal in
  let persist_dir = simplified_path persist_dir in
  RamenExperiments.set_variants persist_dir forced_variants ;
  { do_persist ; log_level ; persist_dir ; keep_temp_files ;
    initial_export_duration ; test }

(* Various directory names: *)

let type_signature_hash = md5 % RamenTuple.type_signature

(* Each workers regularly snapshot its internal state in this file.
 * This data contains tuples and statefull function internal states, so
 * that it has to depend on not only worker_state version (which versions
 * the structure of the state structure itself), but also on codegen
 * version (which versions the language/state), and also the ocaml
 * version itself as we use stdlib's Marshaller for this: *)
let worker_state conf func params =
  conf.persist_dir ^"/workers/states/"
                   ^ RamenVersions.(worker_state ^"_"^ codegen)
                   ^"/"^ Config.version
                   ^"/"^ Func.path func
                   ^"/"^ func.signature
                   ^"/"^ RamenName.signature_of_params params
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
                   ^"/"^ Func.path func
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
                   ^"/"^ Func.path func
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
                   ^"/"^ Func.path func
                   ^"/"^ sign ^"/out_ref"

(* Finally, operations have two additional output streams: one for
 * instrumentation statistics, and one for notifications. Both are
 * common to all running operations, low traffic, and archived. *)
let report_ringbuf conf =
  conf.persist_dir ^"/instrumentation_ringbuf/"
                   ^ RamenVersions.instrumentation_tuple ^"_"
                   ^ RamenVersions.ringbuf
                   ^"/ringbuf.r"

let notify_ringbuf conf =
  conf.persist_dir ^"/notify_ringbuf/"
                   ^ RamenVersions.notify_tuple ^"_"
                   ^ RamenVersions.ringbuf
                   ^"/ringbuf.r"

(* This is not a ringbuffer but a mere snapshot of the notifier state: *)
let pending_notifications_file conf =
  conf.persist_dir ^"/pending_notifications_"
                   ^ RamenVersions.pending_notify ^"_"
                   ^ RamenVersions.notify_tuple

(* For custom API, where to store alerting thresholds: *)
let api_alerts_root conf =
  conf.persist_dir ^"/api/set_alerts"

let test_literal_programs_root conf =
  conf.persist_dir ^"/tests"

(* Where SMT files (used for type-checking) are written temporarily *)
let smt_file src_file =
  Filename.remove_extension src_file ^".smt2"
