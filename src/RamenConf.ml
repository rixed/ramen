open Batteries
open RamenLog
open RamenHelpers
open RamenConsts
module Expr = RamenExpr

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
      (* A function which history we might want to query in the future
       * so make sure it is either stored or can be computed again from
       * ancestor stored history: *)
      persistent : bool ;
      doc : string ;
      mutable operation : RamenOperation.t ;
      in_type : RamenFieldMaskLib.in_type ;
      (* The signature identifies the code but not the actual parameters.
       * Those signatures are used to distinguish sets of ringbufs
       * or any other files where tuples are stored, so that those files
       * change when the code change, without a need to also change the
       * name of the operation. *)
      mutable signature : string ;
      parents : parent list ;
      merge_inputs : bool ;
      (* List of envvar used in that function: *)
      envvars : RamenName.field list }

  module Serialized = struct
    type t = (* A version of the above without redundancy: *)
      { name : RamenName.func ;
        persistent : bool ;
        doc : string ;
        operation : RamenOperation.t ;
        signature : string }
      [@@ppp PPP_OCaml]
  end

  let serialized (t : t) =
    Serialized.{
      name = t.name ;
      persistent = t.persistent ;
      doc = t.doc ;
      operation = t.operation ;
      signature = t.signature }

  let unserialized program_name (t : Serialized.t) =
    let open RamenOperation in
    { program_name ;
      name = t.name ;
      persistent = t.persistent ;
      doc = t.doc ;
      operation = t.operation ;
      signature = t.signature ;
      in_type = RamenFieldMaskLib.in_type_of_operation t.operation ;
      parents = parents_of_operation t.operation ;
      merge_inputs = is_merging t.operation ;
      envvars = envvars_of_operation t.operation }

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

  let signature func params =
    (* We'd like to be formatting independent so that operation text can be
     * reformatted without ramen recompiling it. For this it is not OK to
     * strip redundant white spaces as some of those might be part of literal
     * string values. So we print it, trusting the printer to be exhaustive.
     * This is not enough to print the expression with types, as those do not
     * contain relevant info such as field rank. We therefore print without
     * types and encode input/output types explicitly below.
     * Also, notice that the program-wide running condition does not alter
     * the function signature, and rightfully so, as a change in the running
     * condition does not imply we should disregard past data or consider the
     * function changed in any way. It's `ramen run` job to evaluate the
     * running condition independently. *)
    let op_str = IO.to_string RamenOperation.print func.operation
    and out_type = RamenOperation.out_type_of_operation func.operation in
    "OP="^ op_str ^
    ";IN="^ RamenFieldMaskLib.in_type_signature func.in_type ^
    (* type_signature does not look at private fields: *)
    ";OUT="^ RamenTuple.type_signature out_type ^
    (* Similarly to input type, also depends on the parameters type: *)
    ";PRM="^ RamenTuple.params_type_signature params |>
    md5

  let dump_io func =
    !logger.debug "func %S:\n\tinput type: %a\n\toutput type: %a"
      (RamenName.string_of_func func.name)
      RamenFieldMaskLib.print_in_type func.in_type
      RamenTuple.print_typ
        (RamenOperation.out_type_of_operation func.operation)
end

module Program =
struct
  type t =
    { params : RamenTuple.params [@ppp_default []] ;
      condition : string option ; (* for debug only *)
      funcs : Func.t list }

  module Serialized = struct
    type t =
      { params : RamenTuple.params [@ppp_default []] ;
        condition : string option ; (* for debug only *)
        funcs : Func.Serialized.t list }
      [@@ppp PPP_OCaml]
  end

  let serialized (t : t) =
    Serialized.{
      params = t.params ;
      condition = t.condition ;
      funcs = List.map Func.serialized t.funcs }

  let unserialized program_name (t : Serialized.t) =
    { params = t.params ;
      condition = t.condition ;
      funcs = List.map (Func.unserialized program_name) t.funcs }

  let version_of_bin fname =
    let args = [| fname ; WorkerCommands.print_version |] in
    with_stdout_from_command ~expected_status:0 fname args Legacy.input_line

  let info_of_bin program_name fname =
    let args = [| fname ; WorkerCommands.get_info |] in
    with_stdout_from_command ~expected_status:0 fname args Legacy.input_value |>
    unserialized program_name

  let env_of_params_and_exps conf params =
    (* First the params: *)
    let env =
      Hashtbl.enum params /@
      (fun (n, v) ->
        Printf.sprintf2 "%s%s=%a"
          param_envvar_prefix
          (RamenName.string_of_field n)
          RamenTypes.print v) in
    (* Then the experiment variants: *)
    let exps =
      RamenExperiments.all_experiments conf.persist_dir |>
      List.map (fun (name, exp) ->
        exp_envvar_prefix ^ name ^"="
          ^ exp.RamenExperiments.variants.(exp.variant).name) |>
      List.enum in
    Enum.append env exps

  let wants_to_run conf fname params =
    let args = [| fname ; WorkerCommands.wants_to_run |] in
    let env = env_of_params_and_exps conf params |> Array.of_enum in
    with_stdout_from_command ~expected_status:0 ~env fname args Legacy.input_line |>
    bool_of_string

  let of_bin =
    (* Cache of path to date of last read and program *)
    let reread_data (program_name, fname) : t =
      !logger.debug "Reading config from %s..." fname ;
      match version_of_bin fname with
      | exception e ->
          let err = Printf.sprintf "Cannot get version from %s: %s"
                      fname (Printexc.to_string e) in
          !logger.error "%s" err ;
          failwith err
      | v when v <> RamenVersions.codegen ->
        let err = Printf.sprintf "Executable %s is for version %s \
                                  (I'm version %s)"
                    fname v RamenVersions.codegen in
        !logger.error "%s" err ;
        failwith err
      | _ ->
          (try info_of_bin program_name fname with e ->
             let err = Printf.sprintf "Cannot get info from %s: %s"
                         fname (Printexc.to_string e) in
             !logger.error "%s" err ;
             failwith err)
    and age_of_data (_, fname) =
      try mtime_of_file fname
      with e ->
        !logger.error "Cannot get mtime of %s: %s"
          fname (Printexc.to_string e) ;
        0.
    in
    let get_prog = cached "of_bin" reread_data age_of_data in
    fun program_name params fname ->
      let p = get_prog (program_name, fname) in
      (* Patch actual parameters (in a _new_ prog not the cached one!): *)
      { params = RamenTuple.overwrite_params p.params params ;
        funcs = List.map (fun f -> Func.{ f with program_name }) p.funcs ;
        condition = p.condition }

  let bin_of_program_name lib_path program_name =
    (* Use an extension so we can still use the plain program_name for a
     * directory holding subprograms. Not using "exe" as it remind me of
     * that operating system, but rather "x" as in the x bit: *)
    lib_path ^"/"^ RamenName.path_of_program program_name ^".x"
end

let running_config_file conf =
  conf.persist_dir ^"/configuration/"^ RamenVersions.graph_config ^"/rc"

type worker_status = MustRun | Killed [@@ppp PPP_OCaml]

(* Saved configuration is merely a hash from (unique) program names
 * to their binaries, and parameters (actual ones, not default values): *)
type must_run_entry =
  { (* Tells whether the entry must actually be started. Set to true
       at exit so that we do not loose information of previously run
       entries. *)
    mutable status : worker_status [@ppp_default MustRun] ;
    (* Should this worker be started in debug mode regardless of supervisor
     * mode? *)
    debug : bool [@ppp_default false] ;
    (* Stat report period: *)
    report_period : float [@ppp_default Default.report_period] ;
    (* Full path to the worker's binary: *)
    bin : string ;
    (* "Command line" for that worker: *)
    params : RamenName.params [@ppp_default Hashtbl.create 0] ;
    (* Optionally, file from which this worker can be (re)build (see RamenMake).
     * When it is rebuild, relative parents are found using the program name that's
     * the key in the running config. *)
    src_file : string [@ppp_default ""] }
  [@@ppp PPP_OCaml]
(* The must_run file gives us the unique names of the programs. *)
type must_run_file = (RamenName.program, must_run_entry) Hashtbl.t
  [@@ppp PPP_OCaml]

(* For tests we don't store the rc_file on disk but in there: *)
let non_persisted_programs = ref (Hashtbl.create 11)

let read_rc_file =
  let get fname =
    fail_with_context "Reading RC file"
      (fun () -> ppp_of_file ~default:"{}" must_run_file_ppp_ocaml fname) in
  fun do_persist fname ->
    if do_persist then (
      get fname
    ) else !non_persisted_programs

let save_rc_file do_persist fd rc =
  if do_persist then
    fail_with_context "Saving RC file"
      (fun () -> ppp_to_fd ~pretty:true must_run_file_ppp_ocaml fd rc)

(* Users wanting to know the running config must use with_{r,w}lock.
 * This will return a hash from program name to a function returning
 * the Program.t (with the actual params). *)
let program_of_running_entry program_name mre =
  Program.of_bin program_name mre.params mre.bin

(* [f] takes a hash-table of program name to running-config getter.
 * Modifications will not be saved. *)
let with_rlock conf f =
  let rc_file = running_config_file conf in
  RamenAdvLock.with_r_lock rc_file (fun _fd ->
    let programs =
      read_rc_file conf.do_persist rc_file |>
      Hashtbl.map (fun pn mre ->
        mre,
        memoize (fun () -> program_of_running_entry pn mre)) in
    f programs)

let with_wlock conf f =
  let rc_file = running_config_file conf in
  RamenAdvLock.with_w_lock rc_file (fun fd ->
    let programs = read_rc_file conf.do_persist rc_file in
    let ret = f programs in
    (* Save the config only if f did not fail: *)
    save_rc_file conf.do_persist fd programs ;
    ret)

let is_program_running programs program_name =
  match Hashtbl.find programs program_name with
  | exception Not_found -> false
  | mre, _get_rc -> mre.status = MustRun

let last_conf_mtime conf =
  running_config_file conf |> mtime_of_file_def 0.

let find_func programs fq =
  let program_name, func_name = RamenName.fq_parse fq in
  let mre, get_rc =
    Hashtbl.find programs program_name in
  let prog = get_rc () in
  mre, prog, List.find (fun f -> f.Func.name = func_name) prog.Program.funcs

let find_func_or_fail programs fq =
  try find_func programs fq
  with Not_found ->
    Printf.sprintf2 "Unknown function %a"
      RamenName.fq_print fq |>
    failwith

let make_conf
      ?(do_persist=true) ?(debug=false) ?(quiet=false)
      ?(keep_temp_files=false) ?(forced_variants=[])
      ?(initial_export_duration=Default.initial_export_duration)
      ?(test=false) persist_dir =
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
  let sign = md5 (RamenFieldMaskLib.in_type_signature func.Func.in_type) in
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
  let sign = RamenOperation.out_type_of_operation func.Func.operation |>
             type_signature_hash in
  conf.persist_dir ^"/workers/ringbufs/"
                   ^ RamenVersions.ringbuf
                   ^"/"^ Func.path func
                   ^"/"^ sign ^"/archive.b"

(* Every function with factors will have a file sequence storing possible
 * values encountered for that time range. This is so that we can quickly
 * do autocomplete for graphite metric names regardless of archiving. This
 * could also be used to narrow down the time range of a replays in presence
 * of filtering by a factor.
 * Those files are cleaned by the GC according to retention times only -
 * they are not taken into account for size computation, as they are
 * independent of archiving and are supposed to be small anyway.
 * This function merely returns the directory name where the factors possible
 * values are saved. Then, the factor field name (with '/' url-encoded) gives
 * the name of the directory containing a file per time slice (named
 * begin_end). *)
let factors_of_function conf func =
  let sign = RamenOperation.out_type_of_operation func.Func.operation |>
             type_signature_hash in
  conf.persist_dir ^"/workers/factors/"
                   ^ RamenVersions.factors
                   ^"/"^ Config.version
                   ^"/"^ Func.path func
                   (* arc extension for the GC. FIXME *)
                   ^"/"^ sign ^".arc"

(* Operations are told where to write their output (and which selection of
 * fields) by another file, the "out-ref" file, which is a kind of symbolic
 * link with several destinations (plus a format, plus an expiry date).
 * like the above archive file, the out_ref files must be identified by the
 * operation name and its output type: *)
let out_ringbuf_names_ref conf func =
  let sign = RamenOperation.out_type_of_operation func.Func.operation |>
             type_signature_hash in
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

(* This is not a ringbuffer but a mere snapshot of the alerter state: *)
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

(* Create a temporary program name: *)
let make_transient_program () =
  let now = Unix.gettimeofday ()
  and pid = Unix.getpid ()
  and rnd = Random.int max_int_for_random in
  Legacy.Printf.sprintf "tmp/_%h_%d.%d" now rnd pid |>
  RamenName.program_of_string
