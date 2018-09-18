(* Read and summarize internal instrumentation. *)
open Batteries
open Stdint
open RamenLog
open RamenHelpers
module C = RamenConf
module F = C.Func
module P = C.Program

let no_stats =
  None, None, None, None, None, None, 0., Uint64.zero, Uint64.zero, None, None,
  None, None, None, 0.

let read_stats conf =
  let h = Hashtbl.create 57 in
  let open RamenTypes in
  let bname = C.report_ringbuf conf in
  let typ = RamenBinocle.tuple_typ in
  let event_time = RamenBinocle.event_time in
  let now = Unix.gettimeofday () in
  let until =
    match RamenSerialization.time_range bname typ [] event_time with
    | None ->
        !logger.warning "No time range information for instrumentation" ;
        now
    | Some (_, ma) ->
        if ma < now -. 120. then
          !logger.warning "Instrumentation info is %ds old"
            (int_of_float (now -. ma)) ;
        ma in
  (* FIXME: Not OK because we don't know if report-period has been
   * overridden on `ramen run` command line. Maybe at least make
   * `ramen ps` accept that option too? *)
  let since = until -. 2. *. RamenConsts.Default.report_period in
  let get_string = function VString s -> s [@@ocaml.warning "-8"]
  and get_u64 = function VU64 n -> n [@@ocaml.warning "-8"]
  and get_nu64 = function VNull -> None | VU64 n -> Some n [@@ocaml.warning "-8"]
  and get_float = function VFloat f -> f [@@ocaml.warning "-8"]
  and get_nfloat = function VNull -> None | VFloat f -> Some f [@@ocaml.warning "-8"]
  in
  let while_ () = (* Do not wait more than 1s: *)
    Unix.gettimeofday () -. now < 1. in
  RamenSerialization.fold_time_range ~while_ bname typ [] event_time
                       since until ()  (fun () tuple t1 t2 ->
    let worker = get_string tuple.(0) in
    let time = get_float tuple.(1)
    and min_etime = get_nfloat tuple.(2)
    and max_etime = get_nfloat tuple.(3)
    and in_count = get_nu64 tuple.(4)
    and selected_count = get_nu64 tuple.(5)
    and out_count = get_nu64 tuple.(6)
    and group_count = get_nu64 tuple.(7)
    and cpu = get_float tuple.(8)
    and ram = get_u64 tuple.(9)
    and max_ram = get_u64 tuple.(10)
    and wait_in = get_nfloat tuple.(11)
    and wait_out = get_nfloat tuple.(12)
    and bytes_in = get_nu64 tuple.(13)
    and bytes_out = get_nu64 tuple.(14)
    and last_out = get_nfloat tuple.(15)
    and stime = get_float tuple.(16)
    in
    let stats =
      min_etime, max_etime, in_count, selected_count, out_count,
      group_count, cpu, ram, max_ram, wait_in, wait_out, bytes_in,
      bytes_out, last_out, stime in
    (* Keep only the latest stat line per worker: *)
    Hashtbl.modify_opt worker (function
      | None -> Some (time, stats)
      | Some (time', stats') as prev ->
          if time' > time then prev else Some (time, stats)
    ) h) ;
  (* Clean out time: *)
  Hashtbl.map (fun _ (_time, stats) -> stats) h

let add_stats
      (min_etime', max_etime', in_count', selected_count', out_count',
       group_count', cpu', ram', max_ram', wait_in', wait_out', bytes_in',
       bytes_out', last_out', stime')
      (min_etime, max_etime, in_count, selected_count, out_count, group_count,
       cpu, ram, max_ram, wait_in, wait_out, bytes_in, bytes_out, last_out,
       stime) =
  let combine_opt f a b =
    match a, b with None, b -> b | a, None -> a
    | Some a, Some b -> Some (f a b) in
  let add_nu64 = combine_opt Uint64.add
  and add_nfloat = combine_opt (+.)
  and min_nfloat = combine_opt Float.min
  and max_nfloat = combine_opt Float.max
  in
  min_nfloat min_etime' min_etime,
  max_nfloat max_etime' max_etime,
  add_nu64 in_count' in_count,
  add_nu64 selected_count' selected_count,
  add_nu64 out_count' out_count,
  add_nu64 group_count' group_count,
  cpu' +. cpu,
  Uint64.add ram' ram,
  (* It's more useful to see the sum of all max than the max of all max, as it
   * gives an estimate of the worse that could happen: *)
  Uint64.add max_ram' max_ram,
  add_nfloat wait_in' wait_in,
  add_nfloat wait_out' wait_out,
  add_nu64 bytes_in' bytes_in,
  add_nu64 bytes_out' bytes_out,
  max_nfloat last_out' last_out,
  (* Not sure what the meaning of this would be, so it won't be displayed.
   * Notice though that the max "properly" skip 0 from no_stats: *)
  max stime' stime

let per_program stats =
  let h = Hashtbl.create 17 in
  Hashtbl.iter (fun worker stats ->
    let program, _ = RamenName.(fq_of_string worker |> fq_parse) in
    Hashtbl.modify_opt program (function
      | None -> Some stats
      | Some stats' -> Some (add_stats stats' stats)
    ) h
  ) stats ;
  h
