open Batteries
open RamenLog
open RamenSharedTypes
module C = RamenConf
module N = RamenConf.Func
open Helpers
open Stdint
open Lwt

let is_func_exporting conf func =
  (* func needs to have out_type typed, at least: *)
  if not (C.tuple_is_typed func.N.out_type) then return_false
  else
    (* Is there any bufer file in its out-ref? *)
    let out_ref = C.out_ringbuf_names_ref conf func in
    let%lwt outs = RamenOutRef.read out_ref in
    Map.values outs |>
    Enum.exists (fun file_spec -> file_spec.RamenOutRef.timeout > 0.) |>
    return

(* Garbage in / garbage out *)
let float_of_scalar_value = function
  | VFloat x -> x
  | VBool x -> if x then 1. else 0.
  | VU8 x -> Uint8.to_float x
  | VU16 x -> Uint16.to_float x
  | VU32 x -> Uint32.to_float x
  | VU64 x -> Uint64.to_float x
  | VU128 x -> Uint128.to_float x
  | VI8 x -> Int8.to_float x
  | VI16 x -> Int16.to_float x
  | VI32 x -> Int32.to_float x
  | VI64 x -> Int64.to_float x
  | VI128 x -> Int128.to_float x
  | VEth x -> Uint48.to_float x
  | VIpv4 x -> Uint32.to_float x
  | VIpv6 x -> Uint128.to_float x
  | VNull | VString _ | VCidrv4 _ | VCidrv6 _ -> 0.

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

(* Return the rank of field named [n] in the serialized tuple. *)
let find_ser_field tuple_type n =
  match tuple_type with
  | C.UntypedTuple _ ->
      !logger.error "Asked for serialized rank of an untyped output" ;
      assert false
  | C.TypedTuple { ser ; _ } ->
    (match List.findi (fun _i field -> field.typ_name = n) ser with
    | exception Not_found ->
        failwith ("field "^ n ^" does not exist")
    | i, _ -> i)

(* Returns the func, the buffer name and the type: *)
let make_temp_export conf ?duration func =
  let bname = C.archive_buf_name conf func in
  RingBuf.create ~wrap:false bname RingBufLib.rb_default_words ;
  (* Add that name to the function out-ref *)
  let out_ref = C.out_ringbuf_names_ref conf func in
  let%lwt typ =
    wrap (fun () -> C.tuple_ser_type func.N.out_type) in
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
        | _program, func ->
            make_temp_export conf ?duration func)
