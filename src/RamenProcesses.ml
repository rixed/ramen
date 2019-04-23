(* Process supervisor which task is to start and stop workers, connect them
 * properly and make sure they keep working. *)
open Batteries
open RamenLog
open RamenHelpers
open RamenConsts
module C = RamenConf
module RC = C.Running
module FS = C.FuncStats
module F = C.Func
module P = C.Program
module E = RamenExpr
module O = RamenOperation
module N = RamenName
module OutRef = RamenOutRef
module Files = RamenFiles

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
let repair_and_warn what rb =
  if RingBuf.repair rb then
    !logger.warning "Ringbuf for %s was damaged." what

(* Prepare ringbuffer for notifications *)
let prepare_notifs conf =
  let rb_name = C.notify_ringbuf conf in
  RingBuf.create ~wrap:false rb_name ;
  let notify_rb = RingBuf.load rb_name in
  repair_and_warn "notifications" notify_rb ;
  notify_rb

(* Prepare ringbuffer for reports. *)
let prepare_reports conf =
  let rb_name = C.report_ringbuf conf in
  RingBuf.create ~wrap:false rb_name ;
  let report_rb = RingBuf.load rb_name in
  repair_and_warn "instrumentation" report_rb ;
  report_rb

(* Install signal handlers.
 * Beware: do not use BatPrintf in there, as there is a mutex in there that
 * might be held already when the OCaml runtime decides to run this in the
 * same thread: *)
let prepare_signal_handlers conf =
  ignore conf ;
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
    Binocle.display_console ()))

(*
 * Machinery to spawn other programs.
 *)

let fd_of_int : int -> Unix.file_descr = Obj.magic

let close_fd i =
  Unix.close (fd_of_int i)

let run_background ?cwd ?(and_stop=false) cmd args env =
  let open Unix in
  let quoted oc s = Printf.fprintf oc "%S" s in
  !logger.debug "Running %a as: /usr/bin/env %a %a %a"
    N.path_print cmd
    (Array.print ~first:"" ~last:"" ~sep:" " quoted) env
    N.path_print_quoted cmd
    (Array.print ~first:"" ~last:"" ~sep:" " quoted) args ;
  flush_all () ;
  match fork () with
  | 0 ->
    (try
      if and_stop then RingBufLib.kill_myself Sys.sigstop ;
      Option.may (fun (d : N.path) -> chdir (d :> string)) cwd ;
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

let run_worker ?and_stop (bin : N.path) args env =
  (* Better have the workers CWD where the binary is, so that any file name
   * mentioned in the program is relative to the program. *)
  let cwd = Files.dirname bin in
  let cmd = Files.basename bin in
  run_background ~cwd ?and_stop cmd args env

(* Returns the buffer name: *)
let start_export ?(file_type=OutRef.RingBuf)
                 ?(duration=Default.export_duration) conf func =
  let bname = C.archive_buf_name ~file_type conf func in
  if file_type = OutRef.RingBuf then
    RingBuf.create ~wrap:false bname ;
  (* Add that name to the function out-ref *)
  let out_ref = C.out_ringbuf_names_ref conf func in
  let out_typ =
    O.out_type_of_operation ~with_private:false func.F.operation in
  (* Negative durations, yielding a timestamp of 0, means no timeout ;
   * while duration = 0 means to actually not export anything (and we have
   * a cli-test that relies on the spec not being present in the out_ref
   * in that case): *)
  if duration <> 0. then (
    let timeout =
      if duration < 0. then 0. else Unix.gettimeofday () +. duration in
    let fieldmask = RamenFieldMaskLib.fieldmask_all ~out_typ in
    OutRef.add out_ref ~timeout ~file_type bname fieldmask
  ) ;
  bname

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
