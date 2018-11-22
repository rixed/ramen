(* Read and summarize internal instrumentation. *)
open Batteries
open Stdint
open RamenLog
open RamenHelpers
module C = RamenConf
module F = C.Func
module P = C.Program

type t =
  { min_etime : float option ;
    max_etime : float option ;
    in_count : Uint64.t option ;
    selected_count : Uint64.t option ;
    out_count : Uint64.t option ;
    group_count : Uint64.t option ;
    cpu : float ;
    ram : Uint64.t ;
    max_ram : Uint64.t ;
    wait_in : float option ;
    wait_out : float option ;
    bytes_in : Uint64.t option ;
    bytes_out : Uint64.t option ;
    last_out : float option ;
    startup_time : float }

let no_stats =
  { min_etime = None ; max_etime = None ; in_count = None ;
    selected_count = None ; out_count = None ; group_count = None ;
    cpu = 0. ; ram = Uint64.zero ; max_ram = Uint64.zero ;
    wait_in = None ; wait_out = None ; bytes_in = None ; bytes_out = None ;
    last_out = None ; startup_time = 0. }

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
                       since until () (fun () tuple _t1 _t2 ->
    let worker = get_string tuple.(0)
    and time = get_float tuple.(1)
    and stats =
      { min_etime = get_nfloat tuple.(2) ;
        max_etime = get_nfloat tuple.(3) ;
        in_count = get_nu64 tuple.(4) ;
        selected_count = get_nu64 tuple.(5) ;
        out_count = get_nu64 tuple.(6) ;
        group_count = get_nu64 tuple.(7) ;
        cpu = get_float tuple.(8) ;
        ram = get_u64 tuple.(9) ;
        max_ram = get_u64 tuple.(10) ;
        wait_in = get_nfloat tuple.(11) ;
        wait_out = get_nfloat tuple.(12) ;
        bytes_in = get_nu64 tuple.(13) ;
        bytes_out = get_nu64 tuple.(14) ;
        last_out = get_nfloat tuple.(15) ;
        startup_time = get_float tuple.(16) }
    in
    (* Keep only the latest stat line per worker: *)
    Hashtbl.modify_opt worker (function
      | None -> Some (time, stats)
      | Some (time', _) as prev ->
          if time' > time then prev else Some (time, stats)
    ) h) ;
  (* Clean out time: *)
  Hashtbl.map (fun _ (_time, stats) -> stats) h

let add_stats s1 s2 =
  let combine_opt f a b =
    match a, b with None, b -> b | a, None -> a
    | Some a, Some b -> Some (f a b) in
  let add_nu64 = combine_opt Uint64.add
  and add_nfloat = combine_opt (+.)
  and min_nfloat = combine_opt Float.min
  and max_nfloat = combine_opt Float.max
  in
  { min_etime = min_nfloat s1.min_etime s2.min_etime ;
    max_etime = max_nfloat s1.max_etime s2.max_etime ;
    in_count = add_nu64 s1.in_count s2.in_count ;
    selected_count = add_nu64 s1.selected_count s2.selected_count ;
    out_count = add_nu64 s1.out_count s2.out_count ;
    group_count = add_nu64 s1.group_count s2.group_count ;
    cpu = s1.cpu +. s2.cpu ;
    ram = Uint64.add s1.ram s2.ram ;
    (* It's more useful to see the sum of all max than the max of all max,
     * as it gives an estimate of the worse that could happen: *)
    max_ram = Uint64.add s1.max_ram s2.max_ram ;
    wait_in = add_nfloat s1.wait_in s2.wait_in ;
    wait_out = add_nfloat s1.wait_out s2.wait_out ;
    bytes_in = add_nu64 s1.bytes_in s2.bytes_in ;
    bytes_out = add_nu64 s1.bytes_out s2.bytes_out ;
    last_out = max_nfloat s1.last_out s2.last_out ;
    (* Not sure what the meaning of this would be, so it won't be displayed.
     * Notice though that the max "properly" skip 0 from no_stats: *)
    startup_time = max s1.startup_time s2.startup_time }

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
