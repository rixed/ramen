open Batteries
open RamenLog
module C = RamenConf
module N = RamenConf.Func
open RamenHelpers
open Stdint
open Lwt

exception FuncHasNoEventTimeInfo of string
let () =
  Printexc.register_printer (function
    | FuncHasNoEventTimeInfo n -> Some (
      Printf.sprintf "Function %S has no event-time information" n)
    | _ -> None)

(* Returns the buffer name: *)
let make_temp_export ?duration conf func =
  let bname = C.archive_buf_name conf func in
  RingBuf.create ~wrap:false bname ;
  (* Add that name to the function out-ref *)
  let out_ref = C.out_ringbuf_names_ref conf func in
  let typ = func.C.Func.out_type.ser in
  let file_spec =
    RamenOutRef.{
      field_mask =
        RingBufLib.skip_list ~out_type:typ ~in_type:typ ;
      timeout = match duration with
                | None -> 0.
                | Some d -> Unix.gettimeofday () +. d } in
  let%lwt () = RamenOutRef.add out_ref (bname, file_spec) in
  return bname

(* Returns the func, and the buffer name: *)
let make_temp_export_by_name conf ?duration func_name =
  let program_name, func_name = C.program_func_of_user_string func_name in
  C.with_rlock conf (fun programs ->
    match C.find_func programs program_name func_name with
    | exception Not_found ->
        fail_with ("Function "^ program_name ^"/"^ func_name ^
                   " does not exist")
    | func ->
        let%lwt bname = make_temp_export conf ?duration func in
        return (func, bname))
