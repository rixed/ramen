open Batteries
open RamenLog
module C = RamenConf
module N = RamenConf.Func
open RamenHelpers
open Stdint
open Lwt

(* Building timeseries with points at regular times *)

type timeserie_bucket =
  (* Hopefully count will be small enough that sum can be tracked accurately *)
  { mutable count : int ; mutable sum : float ;
    mutable min : float ; mutable max : float }

let make_buckets n =
  Array.init n (fun _ ->
    { count = 0 ; sum = 0. ; min = max_float ; max = min_float })

let add_into_bucket b i v =
  if i >= 0 && i < Array.length b then (
    b.(i).count <- succ b.(i).count ;
    b.(i).min <- min b.(i).min v ;
    b.(i).max <- max b.(i).max v ;
    b.(i).sum <- b.(i).sum +. v)

let bucket_of_time since dt t = int_of_float ((t -. since) /. dt)

let bucket_avg b =
  if b.count = 0 then None else Some (b.sum /. float_of_int b.count)
let bucket_min b =
  if b.count = 0 then None else Some b.min
let bucket_max b =
  if b.count = 0 then None else Some b.max

exception FuncHasNoEventTimeInfo of string
let () =
  Printexc.register_printer (function
    | FuncHasNoEventTimeInfo n -> Some (
      Printf.sprintf "Function %S has no event-time information" n)
    | _ -> None)

(* Returns the func, the buffer name and the type: *)
let make_temp_export conf ?duration func =
  let bname = C.archive_buf_name conf func in
  RingBuf.create ~wrap:false bname RingBufLib.rb_words ;
  (* Add that name to the function out-ref *)
  let out_ref = C.out_ringbuf_names_ref conf func in
  let typ = func.C.Func.out_type.ser in
  let%lwt file_spec =
    return RamenOutRef.{
      field_mask =
        RingBufLib.skip_list ~out_type:typ ~in_type:typ ;
      timeout = match duration with
                | None -> 0.
                | Some d -> Unix.gettimeofday () +. d } in
  let%lwt () = RamenOutRef.add out_ref (bname, file_spec) in
  return (func, bname, typ)

(* Returns the func, the buffer name and the type: *)
let make_temp_export_by_name conf ?duration func_name =
  match C.program_func_of_user_string func_name with
  | exception Not_found ->
      !logger.error "Cannot find function %S" func_name ;
      fail Not_found
  | program_name, func_name ->
      C.with_rlock conf (fun programs ->
        match C.find_func programs program_name func_name with
        | exception Not_found ->
            fail_with ("Function "^ program_name ^"/"^ func_name ^
                       " does not exist")
        | func ->
            make_temp_export conf ?duration func)
