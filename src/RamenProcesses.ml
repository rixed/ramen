(* Process supervisor which task is to start and stop workers, connect them
 * properly and make sure they keep working. *)
open Batteries
open RamenLog
open RamenHelpersNoLog
open RamenHelpers
open RamenConsts
module C = RamenConf
module FS = C.FuncStats
module VSI = RamenSync.Value.SourceInfo
module VOS = RamenSync.Value.OutputSpecs
module E = RamenExpr
module O = RamenOperation
module N = RamenName
module OutRef = RamenOutRef
module Files = RamenFiles
module Paths = RamenPaths
module T = RamenTypes

(*$inject open Batteries *)

(* Global quit flag, set (to some ExitCodes) when the term signal
 * is received or some other bad condition happen: *)
let quit = ref None

let until_quit f =
  let rec loop () =
    if !quit = None && f () then loop () in
  loop ()

let dummy_nop () =
  !logger.warning "Running in dummy mode" ;
  until_quit (fun () -> Unix.sleep 3 ; true)

let rec sleep_or_exit ?(while_=always) t =
  if t > 0. && while_ () then (
    let delay = min 1. t in
    Unix.sleepf delay ;
    sleep_or_exit ~while_ (t -. delay)
  )

(*
 * Helpers:
 *)

(* To be called before synchronize_running *)
let repair_and_warn ~what rb =
  if RingBuf.repair rb then
    !logger.warning "Ringbuf for %s was damaged." what

(* Prepare ringbuffer for notifications *)
let prepare_notifs conf =
  let rb_name = Paths.notify_ringbuf conf.C.persist_dir in
  RingBuf.create ~wrap:false rb_name ;
  let notify_rb = RingBuf.load rb_name in
  repair_and_warn ~what:"notifications" notify_rb ;
  notify_rb

(* Prepare ringbuffer for reports. *)
let prepare_reports conf =
  let rb_name = Paths.report_ringbuf conf.C.persist_dir in
  RingBuf.create ~wrap:false rb_name ;
  let report_rb = RingBuf.load rb_name in
  repair_and_warn ~what:"instrumentation" report_rb ;
  report_rb

(* Install signal handlers.
 * Beware: do not use BatPrintf in there, as there is a mutex in there that
 * might be held already when the OCaml runtime decides to run this in the
 * same thread: *)
let prepare_signal_handlers _conf =
  set_signals Sys.[sigterm; sigint] (Signal_handle (fun s ->
    ignore s ;
    quit :=
      Some (if s = Sys.sigterm then ExitCodes.terminated
                               else ExitCodes.interrupted))) ;
  (* Dump stats on sigusr1: *)
  set_signals Sys.[sigusr1] (Signal_handle (fun s ->
    (* This log also useful to rotate the logfile. *)
    ignore s ;
    (*!logger.info "Received signal %s" (name_of_signal s) ;*)
    Binocle.display_console ())) ;
  (* Reset the default, in case some library sets it to ignore, because
   * we rely on this not being explicitly set to ignore when we use waitpid.
   * See man waitpid for an explanation about explicit vs implicit ignore: *)
  set_signals Sys.[sigchld] Signal_default

(*
 * Machinery to spawn other programs.
 *)

let fd_of_int : int -> Unix.file_descr = Obj.magic

let close_fd i =
  Unix.close (fd_of_int i)

let run_background ?cwd ?(and_stop=false) cmd args env =
  let open Unix in
  let quoted oc s = Printf.fprintf oc "%S" s in
  let cmd = Files.absolute_path_of cmd in
  !logger.debug "Running%s %a as: /usr/bin/env %a %a %a"
    (if and_stop then " and STOPPING" else "")
    N.path_print cmd
    (Array.print ~first:"" ~last:"" ~sep:" " quoted) env
    N.path_print_quoted cmd
    (Array.print ~first:"" ~last:"" ~sep:" " quoted) args ;
  flush_all () ;
  match fork () with
  | 0 ->
    (try
      if and_stop then RingBufLib.kill_myself Sys.sigstop ;
      Option.may Files.chdir cwd ;
      let null = openfile "/dev/null" [O_RDONLY] 0 in
      dup2 null stdin ;
      close null ;
      for i = 3 to 255 do
        try close_fd i with Unix.(Unix_error (EBADF, _, _)) -> ()
      done ;
      execve (cmd :> string) args env
    with e ->
      Printf.eprintf "Cannot execve: %s\n%!" (Printexc.to_string e) ;
      sys_exit 127)
  | pid -> pid

(*$inject
  open Unix
  module N = RamenName

  let check_status pid expected =
    let rep_pid, status = waitpid [ WNOHANG; WUNTRACED ] pid in
    let status =
      if rep_pid = 0 then None else Some status in
    let printer = function
      | None -> "no status"
      | Some st -> RamenHelpers.string_of_process_status st in
    assert_equal ~printer expected status

  let run ?and_stop args =
    let pid = run_background ?and_stop (N.path args.(0)) args [||] in
    sleepf 0.1 ;
    pid
*)

(*$R run
  let pid = run [| "tests/test_false" |] in
  check_status pid (Some (WEXITED 1))
 *)

(*$R run
  let pid = run ~and_stop:true [| "/bin/sleep" ; "1" |] in
  check_status pid (Some (WSTOPPED Sys.sigstop)) ;
  kill pid Sys.sigcont ;
  check_status pid None ;
  sleepf 1.1 ;
  check_status pid (Some (WEXITED 0))
 *)

let run_worker ?and_stop ?cwd (bin : N.path) args env =
  !logger.debug "Starting worker %a with args %a and env %a"
    N.path_print bin
    (Array.print String.print) args
    (Array.print String.print) env ;
  run_background ?cwd ?and_stop bin args env

(* Returns the buffer name: *)
let start_export ~while_ ?(file_type=VOS.RingBuf)
                 ?(duration=Default.export_duration)
                 conf session site pname func =
  let bname = Paths.archive_buf_name ~file_type conf.C.persist_dir pname func in
  !logger.debug "start_export into %a for %a..."
    N.path_print bname
    print_as_duration duration ;
  if file_type = VOS.RingBuf then
    RingBuf.create ~wrap:false bname ;
  (* Add that name to the function out-ref *)
  let out_typ =
    O.out_type_of_operation ~with_private:false func.VSI.operation in
  (* Negative durations, yielding a timestamp of 0, means no timeout ;
   * while duration = 0 means to actually not export anything (and we have
   * a cli-test that relies on the spec not being present in the out_ref
   * in that case): *)
  if duration <> 0. then (
    let now = Unix.gettimeofday () in
    let timeout_date =
      if duration < 0. then 0. else now +. duration in
    let fieldmask = RamenFieldMaskLib.fieldmask_all ~out_typ in
    let fq = VSI.fq_name pname func in
    OutRef.(add ~while_ session site fq ~now ~timeout_date ~file_type
                (DirectFile bname) fieldmask))

let check_is_subtype t1 t2 =
  (* For t1 to be a subtype of t2, all fields of t1 must be present and
   * public in t2. And since there is no more extension from scalar types at
   * this stage, those fields must have the exact same types. *)
  List.iter (fun f1 ->
    let f2_typ =
      RamenFieldMaskLib.find_type_of_path t2 f1.RamenFieldMaskLib.path in
    if f1.typ <> f2_typ then
      Printf.sprintf2 "Fields %a have different types"
        N.field_print (E.id_of_path f1.path) |>
      failwith
  ) t1

let version_of_bin (fname : N.path) =
  let args = [| (fname :> string) ; WorkerCommands.print_version |] in
  Files.with_stdout_from_command
    ~expected_status:0 fname args Legacy.input_line

let info_of_bin (fname : N.path) =
  let args = [| (fname :> string) ; WorkerCommands.get_info |] in
  Files.with_stdout_from_command
    ~expected_status:0 fname args Legacy.input_value

(* Retrieve a program precompiled info from its binary.
 * Deprecated but still useful in a few places. *)
let of_bin =
  let log errors_ok fmt =
    (if errors_ok then !logger.debug else !logger.error) fmt in
  (* Cache of path to date of last read and program *)
  let reread_data fname errors_ok : VSI.compiled_program =
    !logger.debug "Reading config from %a..." N.path_print fname ;
    match version_of_bin fname with
    | exception e ->
        let err = Printf.sprintf2 "Cannot get version from %a: %s"
                    N.path_print fname (Printexc.to_string e) in
        log errors_ok "%s" err ;
        failwith err
    | v when v <> RamenVersions.codegen ->
      let err = Printf.sprintf2
                  "Executable %a is for version %s (I'm version %s)"
                  N.path_print fname
                  v RamenVersions.codegen in
      log errors_ok "%s" err ;
      failwith err
    | _ ->
        (try info_of_bin fname with e ->
           let err = Printf.sprintf2 "Cannot get info from %a: %s"
                       N.path_print fname
                       (Printexc.to_string e) in
           !logger.error "%s" err ;
           failwith err)
  and age_of_data fname errors_ok =
    try Files.mtime fname
    with e ->
      log errors_ok "Cannot get mtime of %a: %s"
        N.path_print fname
        (Printexc.to_string e) ;
      0.
  in
  let get_prog = cached2 "of_bin" reread_data age_of_data in
  fun ?(errors_ok=false) params (fname : N.path) ->
    let p = get_prog fname errors_ok in
    (* Patch actual parameters (in a _new_ prog not the cached one!): *)
    VSI.{
      default_params = RamenTuple.overwrite_params p.default_params params ;
      funcs = p.funcs ; condition = p.condition ; globals = p.globals }

(* The [site] is not taken from [conf] because choreographer might want
 * to pretend running a worker in another site: *)
let env_of_params_and_exps conf site params =
  (* First the params: *)
  let env =
    Hashtbl.enum params /@
    (fun ((n : N.field), v) ->
      Printf.sprintf2 "%s%s=%a"
        param_envvar_prefix
        (n :> string)
        RamenTypes.print v) |>
    List.of_enum in
  (* Then the experiment variants: *)
  let env =
    RamenExperiments.all_experiments conf.C.persist_dir |>
    List.fold_left (fun env (name, exp) ->
      (exp_envvar_prefix ^ name ^"="^
        exp.RamenExperiments.variants.(exp.variant).name) :: env
    ) env in
  (* Then the site name: *)
  ("site="^ (site : N.site :> string)) :: env

let wants_to_run conf site fname params =
  try
    let args = [| (fname : N.path :> string) ; WorkerCommands.wants_to_run |] in
    let env = env_of_params_and_exps conf site params |> Array.of_list in
    Files.with_stdout_from_command
      ~expected_status:0 ~env fname args Legacy.input_line |>
    bool_of_string
  with e ->
    !logger.error "Cannot find out if worker %a with params %a wants to run: %s, assuming NO"
      N.path_print fname
      (Hashtbl.print N.field_print T.print) params
      (Printexc.to_string e) ;
    false
