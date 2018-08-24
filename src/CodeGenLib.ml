(* Tools used by the generated OCaml code *)
open Batteries
open Stdint
open RamenLog
open Lwt
open RamenHelpers
open RamenNullable

let () =
  async_exception_hook := (fun exn ->
    !logger.error "Received exception %s\n%s"
      (Printexc.to_string exn)
      (Printexc.get_backtrace ()))

(* Get parameters from the environment.
 * This function is called at module initialization time to get the (constant)
 * value of a parameter (with default value in [def]): *)
let parameter_value ~def scalar_parser name =
  let envvar = "param_"^ name in
  !logger.debug "Looking for envvar %S" envvar ;
  match Sys.getenv envvar with
  | exception Not_found -> def
  | s ->
      try scalar_parser s
      with e ->
        let what =
          Printf.sprintf "Cannot parse value %s for parameter %s: %s"
                         s name (Printexc.to_string e) in
        print_exception ~what e ;
        exit RamenConsts.ExitCodes.cannot_parse_param

(* Functions *)

let age_float x = !CodeGenLib_IO.now -. x
let age_u8 = Uint8.of_float % age_float
let age_u16 = Uint16.of_float % age_float
let age_u32 = Uint32.of_float % age_float
let age_u64 = Uint64.of_float % age_float
let age_u128 = Uint128.of_float % age_float
let age_i8 = Int8.of_float % age_float
let age_i16 = Int16.of_float % age_float
let age_i32 = Int32.of_float % age_float
let age_i64 = Int64.of_float % age_float
let age_i128 = Int128.of_float % age_float
(* FIXME: typecheck age_eth, age_ipv4 etc out of existence *)

let aggr_min s x =
  match s with
  | None -> Some x
  | Some y -> Some (min x y)

let aggr_max s x =
  match s with
  | None -> Some x
  | Some y -> Some (max x y)

let aggr_first s x =
  match s with
  | None -> Some x
  | y -> y

let aggr_last _ x = Some x

(* State is count * sum *)
let avg_init = 0, 0.
let avg_add (count, sum) x = count + 1, sum +. x
let avg_finalize (count, sum) = sum /. float_of_int count

(* Compute the p percentile of an array of anything: *)
let percentile p arr =
  assert (p >= 0.0 && p <= 100.0) ;
  Array.fast_sort Pervasives.compare arr ;
  let p = p *. 0.01 in
  let idx =
    round_to_int (p *. float_of_int (Array.length arr - 1)) in
  arr.(idx)

let smooth prev alpha x = x *. alpha +. prev *. (1. -. alpha)

let split by what k =
  if what = "" then k what else
  String.nsplit ~by what |> Lwt_list.iter_s k

module Remember = struct
(* Remember values *)
  type state =
    { mutable filter : RamenBloomFilter.sliced_filter option ;
      false_positive_ratio : float ;
      duration : float ;
      mutable last_remembered : bool }

  let init false_positive_ratio duration =
    (* We cannot init the bloom filter before we receive the first tuple
     * since we need starting time: *)
    { filter = None ;
      false_positive_ratio ;
      duration ;
      last_remembered = false }

  let really_init st tim =
    let num_slices = 10 in
    let start_time = tim -. st.duration
    and slice_width = st.duration /. float_of_int num_slices in
    let filter =
      RamenBloomFilter.make_sliced start_time num_slices slice_width
                                   st.false_positive_ratio in
    st.filter <- Some filter ;
    filter

  let add st tim es =
    let filter =
      match st.filter with
      | None -> really_init st tim
      | Some f -> f in
    st.last_remembered <- RamenBloomFilter.remember filter tim es ;
    st

  let finalize st = st.last_remembered
end

module Distinct = struct
  (* Distinct op values *)
  type 'a state =
    { distinct_values : ('a, unit) Hashtbl.t ;
      mutable last_was_distinct : bool }

  let init () =
    { distinct_values = Hashtbl.create 31 ; last_was_distinct = false }

  let add st x =
    (* TODO: a Hashtbl.modify which callback also returns the return value *)
    st.last_was_distinct <- not (Hashtbl.mem st.distinct_values x) ;
    Hashtbl.add st.distinct_values x () ;
    st

  let finalize st = st.last_was_distinct
end

module Top = struct
  (* Heavy Hitters wrappers: *)

  let init n duration =
    let n = Uint32.to_int n in
    assert (duration > 0.) ;
    let max_size = 10 * n in (* TODO? *)
    (* We want an entry weight to be halved after [duration]: *)
    let decay = -. log 0.5 /. duration in
    HeavyHitters.make ~max_size ~decay

  let add s t w x =
    HeavyHitters.add s t w x ;
    s

  let rank s n x =
    HeavyHitters.rank (Uint32.to_int n) x s

  let is_in_top s n x =
    HeavyHitters.is_in_top (Uint32.to_int n) x s
end

let hash x = Hashtbl.hash x |> Int64.of_int

(* An operator used only for debugging: *)
let print strs =
  let open RamenNullable in
  !logger.info "PRINT: %a"
    (List.print ~first:"" ~last:"" ~sep:", "
       (fun oc s -> String.print oc (s |! "<NULL>"))) strs

module Hysteresis = struct
  let add was_ok v accept max =
    let extr =
      if was_ok then max else accept in
    if max >= accept then v <= extr else v >= extr

  let finalize is_ok = is_ok
end

module Histogram = struct
  type state =
    { min : float ; span : float ; sw : float ; num_buckets : int ;
      histo : Uint32.t array }

  let init min max num_buckets =
    let histo = Array.create (num_buckets + 2) Uint32.zero in
    let span = max -. min in
    let sw = float_of_int num_buckets /. span in
    { min ; span ; sw ; num_buckets ; histo }

  let add h x =
    let x = x -. h.min in
    let bucket =
      if x < 0. then 0 else
      if x >= h.span then h.num_buckets + 1 else
      let b = int_of_float (x *. h.sw) in
      assert (b >= 0 && b < h.num_buckets) ;
      b + 1 in
    h.histo.(bucket) <- Uint32.succ h.histo.(bucket) ;
    h

  let finalize h = h.histo
end

module Last = struct
  type ('a, 'b) state =
    { (* Ordered according to some generic value, smaller first, and we
         will keep only the N bigger values: *)
      values : ('a * 'b) RamenHeap.t ;
      max_length : int (* The number of values we want to return *) ;
      length : int (* how many values are there already *) ;
      count : int (* Count insertions, to use as default order *) }

  let init n =
    let n = Uint32.to_int n in
    { values = RamenHeap.empty ; max_length = n ; length = 0 ; count = 0 }

  let cmp (_, by1) (_, by2) = compare by1 by2

  let add state x by =
    let values = RamenHeap.add cmp (x, by) state.values in
    assert (state.length <= state.max_length) ;
    if state.length < state.max_length then
      { state with
          values ;
          length = state.length + 1 ;
          count = state.count + 1 }
    else
      { state with
          values = RamenHeap.del_min cmp values ;
          count = state.count + 1 }

  let add_on_count state x =
    add state x state.count

  (* Must return an optional vector of max_length values: *)
  let finalize state =
    if state.length < state.max_length then None
    else
      (* FIXME: faster conversion from heap to array: *)
      let values =
        RamenHeap.fold_left cmp (fun lst (x, _) ->
          x :: lst
        ) [] state.values |>
        List.rev |>
        Array.of_list in
      Some values
end

module Group = struct
  type 'a state = 'a list
  let add lst x = x :: lst
  let finalize = Array.of_list
end

let strftime ?(gmt=false) str tim =
  let open Unix in
  let tm = (if gmt then gmtime else localtime) tim in
  let replacements =
    [ "%Y", string_of_int (tm.tm_year + 1900) ;
      "%d", Printf.sprintf "%02d" tm.tm_mday ;
      "%H", Printf.sprintf "%02d" tm.tm_hour ;
      "%j", string_of_int tm.tm_yday ;
      "%M", Printf.sprintf "%02d" tm.tm_min ;
      "%m", Printf.sprintf "%02d" (tm.tm_mon + 1) ;
      "%n", "\n" ; "%t", "\t" ;
      "%S", Printf.sprintf "%05.2f"
              (float_of_int tm.tm_sec +. mod_float tim 1.) ;
      "%s", string_of_float tim ;
      "%u", string_of_int tm.tm_wday ] in
  List.fold_left (fun str (sub, by) ->
    String.nreplace ~str ~sub ~by
  ) str replacements

let reldiff a b =
  let d = abs_float (a -. b) and a = max a b in
  if a = d then 0. else if d < a then d /. a else a /. d

(* We often want functions that work on the last k elements, or the last k
 * periods of length p for seasonal data. So we often need a small sliding
 * window as a function internal state. If we could join between two different
 * streams we would have an aggregate running with a sliding window in one hand
 * and then join it with the non-aggregated values, but that would be complex
 * to setup and prone to error. The only advantage would be that we would have
 * just one sliding window for computing all seasonal stats, instead of having
 * individual internal states for each computation. This could be later
 * optimized, though, by sharing the sliding windows of same n and p. *)
module Seasonal =
struct
  (* All that is needed, provided the finalizers supply n and p, is an array
   * of past values and a tuple counter: *)
  type 'a t = 'a array * int

  (* We initialize this internal state with a random value [x]: *)
  let init p n x =
    let p = Uint32.to_int p and n = Uint32.to_int n in
    Array.make (n * p + 1) x, 0

  (* Then adding a value: *)
  let add (prevs, count) x =
    prevs.(count mod Array.length prevs) <- x ;
    prevs, count + 1

  (* Now various operations differ only by their finalizers,
   * most of them will want to iterate over the last season: *)
  let fold p n (prevs, count) v0 f =
    (* ex: n=3, p=4 (quarterly), array of size n*p+1 = 13, after 42 entries
       (counted from 0 to 41, last was 41 and next one will be 42):

       count:            last--v   v--count=42
       array index:    0   1   2   3   4   5   6   7   8   9  10  11  12
       value time:    39  40  41  29  30  31  32  33  34  35  36  37  38
       wanted values:              0               1               2

       Start from the oldest entry (which is at index count) then skip p values,
       etc, and stop when we reach count-1 (excluded). Since the order does not
       matter for an average we could proceed differently and avoid the modulo
       but this approach is simpler. *)
    let rec loop v c idx =
      if c >= n then v else
      loop (f v prevs.(idx mod Array.length prevs)) (c+1) (idx+p)
    in
    loop v0 0 count

  let foldi p n t v0 f =
    fold p n t (0, v0) (fun (i, v) x -> i+1, f v i x) |> snd

  let iter p n t f =
    fold p n t () (fun () x -> f x)

  let iteri p n t f =
    foldi p n t () (fun () i x -> f i x)

  let current (prevs, count) =
    prevs.((count - 1) mod Array.length prevs)

  let lag (prevs, count) =
    let idx =
      if count < Array.length prevs then 0
      else count mod Array.length prevs in
    prevs.(idx)

  let avg t p n =
    let p = Uint32.to_int p and n = Uint32.to_int n in
    (fold p n t 0. (+.)) /. float_of_int n

  let linreg t p n =
    let p = Uint32.to_int p and n = Uint32.to_int n in
    let b1n, b1d, last =
      let x_avg = float_of_int (n - 1) /. 2. in
      let sq x = x *. x in
      foldi p n t (0., 0., 0.) (fun (b1n, b1d, _) i y ->
        let x = float_of_int i in
        let xd = x -. x_avg in
        b1n +. y *. xd,
        b1d +. sq xd,
        y) in
    let b1 =
      if n > 1 then b1n /. b1d else 0. in
    last +. b1

  (* For multi variable linear regression we store in the array a pair with
   * the predicted value and an array of all predictors value. *)
  let init_multi_linreg p n x preds = init p n (x, preds)
  let add_multi_linreg t x preds = add t (x, preds)
  let multi_linreg t p n =
    let p = Uint32.to_int p and n = Uint32.to_int n in
    let open Lacaml.D in
    (* We first want to know how many observations and predictors we have: *)
    let num_preds, num_obs =
      fold p n t (-1, 0) (fun (nbp, nbo) (_y, xs) ->
        let nbp' = Array.length xs in
        assert (nbp = -1 || nbp = nbp') ;
        nbp', nbo+1) in
    (* Build the x and y matrices *)
    let xm = Mat.create num_obs num_preds
    and ym = Mat.create_mvec num_obs in (* 1 column of num_obs rows *)
    iteri p n t (fun i (y, xs) ->
      (* Fortran flavors. Indices start at 0 and first index is row: *)
      ym.{i+1, 1} <- y ;
      for j = 0 to num_preds-1 do
        xm.{i+1, j+1} <- xs.(j)
      done) ;
    (* Now ask for the "best" parameters: *)
    match gels xm ym with
    | exception _ ->
      let print_mat oc mat =
        let arr = Mat.to_array mat in
        Array.print (Array.print Float.print) oc arr in
      !logger.error "Cannot multi-fit! xm=%a, ym=%a"
        print_mat xm print_mat ym ;
      0.
    | () -> (* Results are in ym *)
      (* And use that to predict the new y given the new xs *)
      let _cury, cur_preds = current t in
      Array.fold_lefti (fun y i x ->
        y +. ym.{i+1, 1} *. x) 0. cur_preds
end

let begin_of_range_cidr4 (n, l) = RamenIpv4.Cidr.and_to_len l n
let end_of_range_cidr4 (n, l) = RamenIpv4.Cidr.or_to_len l n
let begin_of_range_cidr6 (n, l) = RamenIpv6.Cidr.and_to_len l n
let end_of_range_cidr6 (n, l) = RamenIpv6.Cidr.or_to_len l n

(* Health and Stats
 *
 * Each func has to periodically report to ramen http server its health and some stats.
 * Could have been the other way around, and that would have made the
 * connection establishment easier possibly (since we already must be able to
 * ssh to other machines in order to start a func) but we already have an http
 * server on Ramen and probably want to avoid opening too many ports everywhere, and forcing
 * generated funcs to implement too many things.
 *
 * Stats must include:
 *
 * - total number of tuples input and output
 * - total CPU time consumed
 * - current RAM used
 * and others depending on the operation.
 *)

open Binocle

let stats_in_tuple_count =
  IntCounter.make RamenConsts.MetricNames.in_tuple_count
    RamenConsts.MetricDocs.in_tuple_count

let make_stats_selected_tuple_count () =
  IntCounter.make RamenConsts.MetricNames.selected_tuple_count
    RamenConsts.MetricDocs.selected_tuple_count

let stats_out_tuple_count =
  IntCounter.make RamenConsts.MetricNames.out_tuple_count
    RamenConsts.MetricDocs.out_tuple_count

let stats_cpu =
  FloatCounter.make RamenConsts.MetricNames.cpu_time
    RamenConsts.MetricDocs.cpu_time

let stats_ram =
  IntGauge.make RamenConsts.MetricNames.ram_usage
    RamenConsts.MetricDocs.ram_usage

let stats_rb_read_bytes =
  IntCounter.make RamenConsts.MetricNames.rb_read_bytes
    RamenConsts.MetricDocs.rb_read_bytes

let stats_rb_write_bytes =
  IntCounter.make RamenConsts.MetricNames.rb_write_bytes
    RamenConsts.MetricDocs.rb_write_bytes

let stats_rb_read_sleep_time =
  FloatCounter.make RamenConsts.MetricNames.rb_wait_read
    RamenConsts.MetricDocs.rb_wait_read

let stats_rb_write_sleep_time =
  FloatCounter.make RamenConsts.MetricNames.rb_wait_write
    RamenConsts.MetricDocs.rb_wait_write

let stats_last_out =
  FloatGauge.make RamenConsts.MetricNames.last_out
    RamenConsts.MetricDocs.last_out

let sleep_in d = FloatCounter.add stats_rb_read_sleep_time d
let sleep_out d = FloatCounter.add stats_rb_write_sleep_time d

let tot_cpu_time () =
  let open Unix in
  let pt = times () in
  pt.tms_utime +. pt.tms_stime +. pt.tms_cutime +. pt.tms_cstime

let tot_ram_usage =
  let word_size = Sys.word_size / 8 in
  fun () ->
    let stat = Gc.quick_stat () in
    stat.Gc.heap_words * word_size

let update_stats () =
  FloatCounter.set stats_cpu (tot_cpu_time ()) ;
  IntGauge.set stats_ram (tot_ram_usage ())

(* Basic tuple without aggregate specific counters: *)
let get_binocle_tuple worker ic sc gc =
  let si v =
    if v < 0 then !logger.error "Negative int counter: %d" v ;
    Some (Uint64.of_int v) in
  let s v = Some v in
  let i v = Option.map (fun r -> Uint64.of_int r) v in
  let time = Unix.gettimeofday () in
  worker, time, ic, sc,
  IntCounter.get stats_out_tuple_count |> si,
  gc,
  FloatCounter.get stats_cpu,
  (* Assuming we call update_stats before this: *)
  IntGauge.get stats_ram |> i |> Option.get,
  FloatCounter.get stats_rb_read_sleep_time |> s,
  FloatCounter.get stats_rb_write_sleep_time |> s,
  IntCounter.get stats_rb_read_bytes |> si,
  IntCounter.get stats_rb_write_bytes |> si,
  FloatGauge.get stats_last_out

let send_stats rb (_, time, _, _, _, _, _, _, _, _, _, _, _ as tuple) =
  let sersize = RamenBinocle.max_sersize_of_tuple tuple in
  match RingBuf.enqueue_alloc rb sersize with
  | exception RingBufLib.NoMoreRoom -> () (* Just skip *)
  | tx ->
    let offs = RamenBinocle.serialize tx tuple in
    assert (offs <= sersize) ;
    RingBuf.enqueue_commit tx time time

let update_stats_rb period rb_name get_tuple =
  let rb = RingBuf.load rb_name in
  while%lwt true do
    update_stats () ;
    let tuple = get_tuple () in
    send_stats rb tuple ;
    Lwt_unix.sleep period
  done

(* Helpers *)

(* For non-wrapping buffers we need to know the value for the time, as
 * the min/max times per slice are saved, along the first/last tuple
 * sequence number. *)
let output rb serialize_tuple sersize_of_tuple time_of_tuple tuple =
  let open RingBuf in
  let tmin, tmax = time_of_tuple tuple |? (0., 0.) in
  let sersize = sersize_of_tuple tuple in
  IntCounter.add stats_rb_write_bytes sersize ;
  if IntCounter.get stats_rb_write_bytes < 0 then
    !logger.error "After adding %d, out vol = %d"
      sersize (IntCounter.get stats_rb_write_bytes) ;
  let tx = enqueue_alloc rb sersize in
  let offs = serialize_tuple tx tuple in
  if tmin > 2000000000. then
    !logger.error "wrong tmin (%f) while enqueueing commit" tmin ;
  if tmax > 2000000000. then
    !logger.error "wrong tmax (%f) while enqueueing commit" tmax ;
  enqueue_commit tx tmin tmax ;
  assert (offs = sersize)

(* Each func can write in several ringbuffers (one per children). This list
 * will change dynamically as children are added/removed. *)
let outputer_of rb_ref_out_fname sersize_of_tuple time_of_tuple
                serialize_tuple =
  let out_h = Hashtbl.create 5 (* Hash from fname to rb*outputer *)
  and out_l = ref []  (* list of outputers *) in
  let get_out_fnames = RingBufLib.out_ringbuf_names rb_ref_out_fname in
  fun tuple ->
    IntCounter.add stats_out_tuple_count 1 ;
    FloatGauge.set stats_last_out !CodeGenLib_IO.now ;
    let%lwt fnames = get_out_fnames () in
    Option.may (fun out_specs ->
      if Hashtbl.is_empty out_specs then
        !logger.info "OutRef is now empty!"
      else (
        if Hashtbl.is_empty out_h then
          !logger.debug "OutRef is no more empty!" ;
        !logger.debug "Must now output to: %a"
          RamenOutRef.print_out_specs out_specs) ;
      (* Change occurred, load/unload as required *)
      let current = Hashtbl.keys out_h |> Set.of_enum in
      let next = Hashtbl.keys out_specs |> Set.of_enum in
      let to_open = Set.diff next current
      and to_close = Set.diff current next in
      (* Close some: *)
      Set.iter (fun fname ->
        !logger.debug "Unmapping %S" fname ;
        match Hashtbl.find out_h fname with
        | exception Not_found ->
          !logger.error "While unmapping %S: this file is not mapped?!"
            fname
        | rb, _ ->
          RingBuf.unload rb ;
          Hashtbl.remove out_h fname) to_close ;
      (* Open some: *)
      Set.iter (fun fname ->
          !logger.debug "Mapping %S" fname ;
          let file_spec = Hashtbl.find out_specs fname in
          assert (String.length fname > 0) ;
          match RingBuf.load fname with
          | exception e ->
            !logger.error "Cannot open ringbuf %s: %s"
              fname (Printexc.to_string e) ;
          | rb ->
            let once =
              output rb (serialize_tuple file_spec.RamenOutRef.field_mask)
                        (sersize_of_tuple file_spec.RamenOutRef.field_mask)
                        time_of_tuple in
            let rb_writer =
              let last_retry = ref 0. in
              (* Note: we retry only on NoMoreRoom so that's OK to keep trying; in
               * case the ringbuf disappear altogether because the child is
               * terminated then we won't deadloop.  Also, if one child is full
               * then we will not write to next children until we can eventually
               * write to this one. This is actually desired to have proper message
               * ordering along the stream and avoid ending up with many threads
               * retrying to write to the same child. *)
              RingBufLib.retry_for_ringbuf
                ~while_:(fun () ->
                  (* Also check from time to time that we are still supposed to
                   * write in there: *)
                  if !CodeGenLib_IO.now > !last_retry +. 3. then (
                    last_retry := !CodeGenLib_IO.now ;
                    RamenOutRef.mem rb_ref_out_fname fname
                  ) else return_true)
                ~delay_rec:sleep_out once
            in
            Hashtbl.add out_h fname (rb, rb_writer)
        ) to_open ;
      (* Update the current list of outputers: *)
      out_l := Hashtbl.values out_h /@ snd |> List.of_enum) fnames ;
    Lwt_list.iter_p (fun out ->
      try%lwt out tuple
      with RingBufLib.NoMoreRoom ->
        (* It is OK, just skip it. Next tuple we will reread fnames
         * if it has changed. *)
        return_unit
         | Exit ->
        (* retry_for_ringbuf failing because the recipient is no more in
         * our out_ref: *)
        return_unit
    ) !out_l

type worker_conf =
  { debug : bool ; state_file : string ; ramen_url : string }

let quit = ref None

let worker_start worker_name get_binocle_tuple k =
  let debug = getenv ~def:"false" "debug" |> bool_of_string in
  let default_persist_dir =
    "/tmp/worker_"^ worker_name ^"_"^ string_of_int (Unix.getpid ()) in
  let state_file = getenv ~def:default_persist_dir "state_file" in
  let ramen_url = getenv ~def:"http://localhost:29380" "ramen_url" in
  let prefix = worker_name ^": " in
  (match getenv "log" with
  | exception _ ->
      logger := make_logger ~prefix debug
  | logdir ->
      if logdir = "syslog" then
        logger := make_syslog ~prefix debug
      else (
        mkdir_all logdir ;
        logger := make_logger ~logdir debug)) ;
  !logger.info "Starting %s process..." worker_name ;
  let report_period =
    getenv ~def:(string_of_float RamenConsts.Default.report_period)
           "report_period" |> float_of_string in
  let report_rb =
    getenv ~def:"/tmp/ringbuf_in_report.r" "report_ringbuf" in
  (* Must call this once before get_binocle_tuple because cpu/ram gauges
   * must not be NULL: *)
  update_stats () ;
  let conf = { debug ; state_file ; ramen_url } in
  set_signals Sys.[sigterm; sigint] (Signal_handle (fun s ->
    !logger.info "Received signal %s" (name_of_signal s) ;
    quit := Some RamenConsts.ExitCodes.terminated)) ;
  (* Dump stats on sigusr1 (also on sigusr2 out of security): *)
  set_signals Sys.[sigusr1; sigusr2] (Signal_handle (fun s ->
    (* This log also useful to rotate the logfile. *)
    !logger.info "Received signal %s" (name_of_signal s) ;
    Binocle.display_console ())) ;
  Lwt_unix.set_pool_size 1 ;
  Lwt_main.run (
    catch
      (fun () ->
        join [
          (async (fun () ->
             restart_on_failure "update_stats_rb"
               (update_stats_rb report_period report_rb) get_binocle_tuple) ;
           return_unit) ;
          k conf ])
      (fun e ->
        print_exception e ;
        !logger.error "Exiting..." ;
        return_unit)) ;
  exit (!quit |? 1)

(* Operations that funcs may run: *)

let read_csv_file filename do_unlink separator sersize_of_tuple
                  time_of_tuple serialize_tuple tuple_of_strings
                  preprocessor field_of_params =
  let worker_name = getenv ~def:"?" "fq_name" in
  let get_binocle_tuple () =
    get_binocle_tuple worker_name None None None in
  worker_start worker_name get_binocle_tuple (fun _conf ->
    let rb_ref_out_fname = getenv ~def:"/tmp/ringbuf_out_ref" "output_ringbufs_ref"
    (* For tests, allow to overwrite what's specified in the operation: *)
    and filename = getenv ~def:filename "csv_filename"
    and separator = getenv ~def:separator "csv_separator" in
    let tuples = [ [ "param" ], field_of_params ] in
    let filename = subst_tuple_fields tuples filename
    and separator = subst_tuple_fields tuples separator
    in
    !logger.info "Will read CSV file %S using separator %S"
                  filename separator ;
    let of_string line =
      let strings = strings_of_csv separator line in
      tuple_of_strings (Array.of_list strings)
    in
    let outputer =
      outputer_of rb_ref_out_fname sersize_of_tuple time_of_tuple
                  serialize_tuple in
    (* Allow `ramen test` some time to run all other workers: *)
    let%lwt () = Lwt_unix.sleep 1. in
    let while_ () = !quit = None in
    CodeGenLib_IO.read_glob_lines
      ~while_ ~do_unlink filename preprocessor quit (fun line ->
      match of_string line with
      | exception e ->
        !logger.error "Cannot parse line %S: %s"
          line (Printexc.to_string e) ;
        return_unit ;
      | tuple -> outputer tuple))

let listen_on (collector :
                inet_addr:Lwt_unix.inet_addr ->
                port:int ->
                (* We have to specify this one: *)
                ?while_:(unit -> bool) ->
                ('a -> unit Lwt.t) ->
                unit Lwt.t)
              addr_str port proto_name
              sersize_of_tuple time_of_tuple serialize_tuple =
  let worker_name = getenv ~def:"?" "fq_name" in
  let get_binocle_tuple () =
    get_binocle_tuple worker_name None None None in
  worker_start worker_name get_binocle_tuple (fun _conf ->
    let rb_ref_out_fname = getenv ~def:"/tmp/ringbuf_out_ref" "output_ringbufs_ref"
    and inet_addr = Unix.inet_addr_of_string addr_str
    in
    !logger.debug "Will listen to port %d for incoming %s messages"
                  port proto_name ;
    let outputer =
      outputer_of rb_ref_out_fname sersize_of_tuple time_of_tuple
                  serialize_tuple in
    let while_ () = !quit = None in
    collector ~inet_addr ~port ~while_ outputer)

let read_well_known from sersize_of_tuple time_of_tuple serialize_tuple
                    unserialize_tuple ringbuf_envvar worker_time_of_tuple =
  let worker_name = getenv ~def:"?" "fq_name" in
  let get_binocle_tuple () =
    get_binocle_tuple worker_name None None None in
  worker_start worker_name get_binocle_tuple (fun conf ->
    let bname =
      getenv ~def:"/tmp/ringbuf_in_report.r" ringbuf_envvar in
    let rb_ref_out_fname =
      getenv ~def:"/tmp/ringbuf_out_ref" "output_ringbufs_ref"
    in
    let outputer =
      outputer_of rb_ref_out_fname sersize_of_tuple time_of_tuple
                  serialize_tuple in
    let globs = List.map Globs.compile from in
    let match_from worker =
      from = [] ||
      List.exists (fun g -> Globs.matches g worker) globs
    in
    let start = Unix.gettimeofday () in
    let while_ () = Lwt.return (!quit = None) in
    let rec loop last_seq =
      let rb = RingBuf.load bname in
      let st = RingBuf.stats rb in
      if st.first_seq <= last_seq then (
        let%lwt () = Lwt_unix.sleep (1. +. Random.float 1.) in
        loop last_seq
      ) else (
        !logger.info "Reading buffer..." ;
        let%lwt () =
          RingBufLib.read_buf ~while_ ~delay_rec:sleep_in rb () (fun () tx ->
            let tuple = unserialize_tuple tx in
            let worker, time = worker_time_of_tuple tuple in
            (* Filter by time and worker *)
            if time >= start && match_from worker then
              let%lwt () = outputer tuple in
              Lwt.return ((), true)
            else
              Lwt.return ((), true)) in
        !logger.info "Done reading buffer, waiting for next one." ;
        RingBuf.unload rb ;
        loop st.first_seq
      ) in
    loop ~-1)

(*
 * Aggregate operation
 *)

let notify conf rb worker event_time
           (name, parameters)
           field_of_tuple_in tuple_in
           field_of_tuple_out tuple_out
           field_of_params =
  let tuples =
    [ [ ""; "out" ], field_of_tuple_out tuple_out ;
      [ "in" ], field_of_tuple_in tuple_in ;
      [ "param" ], field_of_params ] in
  let name = subst_tuple_fields tuples name
	and parameters =
    List.map (fun (n, v) -> n, subst_tuple_fields tuples v) parameters in
  let firing, certainty, parameters =
    RingBufLib.normalize_notif_parameters parameters in
  let parameters = Array.of_list parameters in
  RingBufLib.write_notif ~delay_rec:sleep_out rb
    (worker, !CodeGenLib_IO.now, event_time, name, firing, certainty, parameters)

(*type minimal_out = float * bool
type generator_out = string * Stdint.Uint16.t * float * bool*)

type ('aggr, 'tuple_in, 'minimal_out) aggr_value =
  { (* used to compute the actual selected field when outputing the
     * aggregate: *)
    mutable first_in : 'tuple_in ; (* first in-tuple of this aggregate *)
    mutable last_in : 'tuple_in ; (* last in-tuple of this aggregate. *)
    (* minimal_out is a subset of the 'generator_out tuple, with only
     * those fields required for commit_cond and access to
     * group.previous fields.  Alternatively, we could
     * have merely the (non-required) stateful function blanked out, so
     * that building the generator_out would only require the group states
     * and this minimal_out, but then what to do of expressions such as
     * "SELECT in.foo + 95th percentile bar"? *)
    mutable current_out : 'minimal_out ;
    mutable last_ev_count : int ; (* used for others.successive (TODO) *)
    mutable fields : 'aggr (* the record of aggregation values aka the group or local state *) ;
    mutable prev_last_in : 'tuple_in ; (* second but last input tuple for this group. *)
  }

(* WARNING: increase RamenVersions.worker_state whenever this record is
 * changed. *)
type ('key, 'aggr, 'tuple_in, 'minimal_out, 'generator_out, 'global_state, 'sort_key) aggr_persist_state =
  { event_count : int ; (* TBD. used to fake others.count etc *)
    mutable last_out_tuple : 'generator_out option ; (* last committed tuple generator *)
    global_state : 'global_state ;
    (* The hash of all groups: *)
    mutable groups :
      ('key, ('aggr, 'tuple_in, 'minimal_out) aggr_value) Hashtbl.t ;
    (* Input sort buffer and related tuples: *)
    mutable sort_buf : ('sort_key, 'tuple_in) RamenSortBuf.t }

(* TODO: instead of a single point, have 3 conditions for committing
 * (and 3 more for flushing) the groups; So that we could maybe split a
 * complex expression in smaller (faster) sub-conditions? But beware
 * of committing several times the same tuple. *)
type when_to_check_group = ForAll | ForInGroup
let string_of_when_to_check_group = function
  | ForAll -> "every group at every tuple"
  | ForInGroup -> "the group that's updated by a tuple"

type 'tuple_in sort_until_fun =
  Uint64.t (* sort.#count *) ->
  'tuple_in (* sort.first *) ->
  'tuple_in (* last in *) ->
  'tuple_in (* sort.smallest *) ->
  'tuple_in (* sort.greatest *) -> bool

type ('tuple_in, 'sort_by) sort_by_fun =
  Uint64.t (* sort.#count *) ->
  'tuple_in (* sort.first *) ->
  'tuple_in (* last in *) ->
  'tuple_in (* sort.smallest *) ->
  'tuple_in (* sort.greatest *) -> 'sort_by

type ('tuple_in, 'merge_on) merge_on_fun = 'tuple_in (* last in *) -> 'merge_on

let read_single_rb ?while_ ?delay_rec read_tuple rb_in k =
  RingBufLib.read_ringbuf ?while_ ?delay_rec rb_in (fun tx ->
    let in_tuple = read_tuple tx in
    let tx_size = RingBuf.tx_size tx in
    RingBuf.dequeue_commit tx ;
    k tx_size in_tuple)

let merge_rbs ?while_ ?delay_rec merge_on merge_timeout read_tuple rbs k =
  let max_retry_time =
    if merge_timeout > 0. then Some merge_timeout else None in
  let read_tup ?max_retry_time rb =
    let%lwt tx =
      RingBufLib.dequeue_ringbuf_once ?while_ ?delay_rec ?max_retry_time rb in
    let in_tuple = read_tuple tx in
    let tx_size = RingBuf.tx_size tx in
    RingBuf.dequeue_commit tx ;
    let merge_key = merge_on in_tuple in
    return (merge_key, rb, tx_size, in_tuple)
  and cmp_tuples (k1, _, _, _) (k2, _, _, _) =
    compare k1 k2
  (* List of tuples for rbs that were timed out but now have data: *)
  and no_longer_timed_out = ref [] in
  let foreground_wait rb =
    !logger.warning "Timed out a parent" ;
    let%lwt tup = read_tup rb in (* no more max_retry_time *)
    !logger.info "Timed out Parent is back" ;
    no_longer_timed_out := tup :: !no_longer_timed_out ;
    return_unit
  in
  let rec loop heap =
    (* First, reintegrate timed out ringbuffers, if any: *)
    let heap =
      List.fold_left (fun heap tup ->
        RamenHeap.add cmp_tuples tup heap
      ) heap !no_longer_timed_out in
    no_longer_timed_out := [] ;
    if RamenHeap.is_empty heap then
      let%lwt () = Lwt_unix.sleep (max 0.1 merge_timeout) in
      loop heap
    else
      (* Selects the smallest tuple, pass it to the continuation, and then
       * replace it: *)
      let (_k, rb, tx_size, in_tuple), heap =
        RamenHeap.pop_min cmp_tuples heap in
      let%lwt () = k tx_size in_tuple in
      match%lwt read_tup ?max_retry_time rb with
      | exception Exit -> return_unit
      | exception Timeout ->
          async (fun () -> foreground_wait rb) ;
          loop heap
      | tup ->
          loop (RamenHeap.add cmp_tuples tup heap)
  in
  match%lwt
    Lwt_list.fold_left_s (fun heap rb ->
      match%lwt read_tup ?max_retry_time rb with
      | exception Timeout ->
          async (fun () -> foreground_wait rb) ;
          return heap
      | tup ->
          return (RamenHeap.add cmp_tuples tup heap)
    ) RamenHeap.empty rbs with
  | exception Exit -> return_unit
  | heap -> loop heap

let yield_every ~while_ read_tuple every k =
  !logger.debug "YIELD operation"  ;
  let tx = RingBuf.empty_tx () in
  let rec loop () =
    if !quit <> None then return_unit else (
      let start = Unix.gettimeofday () in
      let in_tuple = read_tuple tx in
      let%lwt () = k 0 in_tuple in
      let sleep_time = every -. Unix.gettimeofday () +. start in
      if sleep_time > 0. then (
        !logger.debug "Sleeping for %f seconds" sleep_time ;
        Lwt_unix.sleep sleep_time >>= loop
      ) else loop ()
    ) in
  loop ()

let aggregate
      (read_tuple : RingBuf.tx -> 'tuple_in)
      (sersize_of_tuple : bool list (* skip list *) -> 'tuple_out -> int)
      (time_of_tuple : 'tuple_out -> (float * float) option)
      (serialize_tuple : bool list (* skip list *) -> RingBuf.tx -> 'tuple_out -> int)
      (generate_tuples : ('tuple_in -> 'tuple_out -> unit Lwt.t) -> 'tuple_in -> 'generator_out -> unit Lwt.t)
      (minimal_tuple_of_aggr :
        'tuple_in -> (* current input *)
        'generator_out option -> (* last_out *)
        'aggr -> 'global_state -> 'minimal_out)
      (* Build the generator_out tuple from the minimal_out and all the same
       * inputs as minimal_tuple_of_aggr, all of which must be saved in the
       * group so we can commit other groups as well as the current one. *)
      (out_tuple_of_minimal_tuple :
        'tuple_in -> (* current input *)
        'generator_out option -> (* last_out *)
        'aggr -> 'global_state -> 'minimal_out -> 'generator_out)
      (merge_on : ('tuple_in, 'merge_on) merge_on_fun)
      (merge_timeout : float)
      (sort_last : int)
      (sort_until : 'tuple_in sort_until_fun)
      (sort_by : ('tuple_in, 'sort_by) sort_by_fun)
      (* Where_fast/slow: premature optimisation: if the where filter
       * uses the aggregate then we need where_slow (checked after
       * the aggregate look up) but if it uses only the incoming
       * tuple then we can use only where_fast. *)
      (where_fast :
        'global_state ->
        'tuple_in -> (* current input *)
        'generator_out option -> (* previous.out *)
        bool)
      (where_slow :
        'global_state ->
        'tuple_in -> (* current input *)
        'generator_out option -> (* previous.out *)
        'aggr ->
        bool)
      (key_of_input : 'tuple_in -> 'key)
      (is_single_key : bool)
      (commit_cond :
        'tuple_in -> (* current input *)
        'generator_out option -> (* out_last *)
        'aggr ->
        'global_state ->
        'minimal_out -> (* current minimal out *)
        bool)
      commit_before
      do_flush
      (when_to_check_for_commit : when_to_check_group)
      (global_state : 'global_state)
      (group_init : 'global_state -> 'aggr)
      (field_of_tuple_in : 'tuple_in -> string -> string)
      (field_of_tuple_out : 'tuple_out -> string -> string)
      (field_of_params : string -> string)
      (get_notifications :
        'tuple_in -> 'tuple_out -> (string * (string * string) list) list)
      (every : float) =
  let stats_selected_tuple_count = make_stats_selected_tuple_count ()
  and stats_group_count =
    IntGauge.make RamenConsts.MetricNames.group_count
                  RamenConsts.MetricDocs.group_count in
  IntGauge.set stats_group_count 0 ;
  let worker_name = getenv ~def:"?" "fq_name" in
  let get_binocle_tuple () =
    let si v = Some (Uint64.of_int v) in
    let i v = Option.map (fun r -> Uint64.of_int r) v in
    get_binocle_tuple
      worker_name
      (IntCounter.get stats_in_tuple_count |> si)
      (IntCounter.get stats_selected_tuple_count |> si)
      (IntGauge.get stats_group_count |> i) in
  worker_start worker_name get_binocle_tuple (fun conf ->
    let when_str = string_of_when_to_check_group when_to_check_for_commit in
    !logger.debug "We will commit/flush for... %s" when_str ;
    let rb_in_fnames =
      let rec loop lst i =
        match Sys.getenv ("input_ringbuf_"^ string_of_int i) with
        | exception Not_found -> lst
        | n -> loop (n :: lst) (i + 1) in
      loop [] 0
    and rb_ref_out_fname = getenv ~def:"/tmp/ringbuf_out_ref" "output_ringbufs_ref"
    and notify_rb_name = getenv ~def:"/tmp/ringbuf_notify.r" "notify_ringbuf" in
    let notify_rb = RingBuf.load notify_rb_name in
    let tuple_outputer =
      outputer_of rb_ref_out_fname sersize_of_tuple time_of_tuple
                  serialize_tuple in
    let outputer =
      (* tuple_in is useful for generators and text expansion: *)
      let do_out tuple_in tuple_out =
        let notifications = get_notifications tuple_in tuple_out in
        if notifications <> [] then (
          let event_time = time_of_tuple tuple_out |> Option.map fst in
          Lwt_list.iter_s (fun notif ->
            notify conf notify_rb worker_name event_time notif
                   field_of_tuple_in tuple_in
                   field_of_tuple_out tuple_out
                   field_of_params
          ) notifications
        ) else return_unit ;%lwt
        tuple_outputer tuple_out
      in
      generate_tuples do_out in
    let with_state =
      let open CodeGenLib_State.Persistent in
      (* Try to make the state as small as possible: *)
      let groups =
        Hashtbl.create (if is_single_key then 1 else 701)
      in
      let init_state =
        { event_count = 0 ;
          last_out_tuple = None ;
          global_state ;
          groups ;
          sort_buf = RamenSortBuf.empty } in
      let state =
        ref (make conf.state_file init_state) in
      fun f ->
        let v = restore !state in
        (* We do _not_ want to save the value when f raises an exception: *)
        let%lwt v = f v in
        state := save ~save_every:1013 ~save_timeout:21. !state v ;
        return_unit
    in
    !logger.debug "Will read ringbuffer %a"
      (List.print String.print) rb_in_fnames ;
    let%lwt rb_ins =
      Lwt_list.map_p (fun fname ->
        retry ~on:(fun _ -> return_true) ~min_delay:1.0
              (fun n -> return (RingBuf.load n))
              fname
      ) rb_in_fnames
    in
    (* The big function that aggregate a single tuple *)
    let aggregate_one s in_tuple =
      (* Define some short-hand values and functions we will keep
       * referring to: *)
      (* When committing other groups, this is used to skip the current
       * groupif it has been sent already: *)
      let already_output_aggr = ref None in
      let already_output a =
        Option.map_default ((==) a) false !already_output_aggr in
      (* Tells if the group must be committed/flushed: *)
      let must_commit aggr =
        commit_cond
          in_tuple
          s.last_out_tuple
          aggr.fields
          s.global_state
          aggr.current_out
      in
      let commit_and_flush to_commit =
        Lwt_list.iter_s (fun (k, a) ->
          (* Output the tuple *)
          let out =
            out_tuple_of_minimal_tuple
              a.last_in
              s.last_out_tuple
              a.fields
              s.global_state
              a.current_out in
          s.last_out_tuple <- Some out ;
          outputer in_tuple out ;%lwt
          (* Flush/Keep/Slide *)
          if do_flush then Hashtbl.remove s.groups k ;
          return_unit
        ) to_commit in
      let commit_and_flush_all_if check_when =
        let to_commit =
          if when_to_check_for_commit <> check_when then [] else
            (* FIXME: What if commit-when update the global state? We are
             * going to update it several times here. We should prevent this
             * clause to access the global state. *)
            Hashtbl.fold (fun k a l ->
              if not (already_output a) && must_commit a
              then (k, a)::l else l
            ) s.groups [] in
        commit_and_flush to_commit
      in
      (* Now that this is all in place, here are the next steps:
       * 1. Filtering (fast path)
       * 2. Retrieve the group
       * 3. Filtering (slow path)
       * 4. Update the group (aggregation) and compute (and save)
       *    minimal_out_tuple (with fields required for commit_cond).
       * 6. Check for commit
       * 7. For each sent groups, build their out_tuple from group and (saved)
       *    minimal_out_tuple and send.
       *
       * Note that steps 3 and 4 have two implementations, depending on
       * whether the group is a new one or not. *)
      (* 1. Filtering (fast path) *)
      let k_aggr_opt = (* maybe the key and group that has been updated: *)
        if where_fast
             s.global_state
             in_tuple
             s.last_out_tuple
        then (
          (* 2. Retrieve the group *)
          IntGauge.set stats_group_count (Hashtbl.length s.groups) ;
          let k = key_of_input in_tuple in
          (* Update/create the group if it passes where_slow. *)
          match Hashtbl.find s.groups k with
          | exception Not_found ->
            (* The group does not exist for that key, create one?
             * Notice there is no "commit-before" for new groups.  *)
            let fields = group_init s.global_state in
            (* 3. Filtering (slow path) - for new group *)
            if where_slow
                 s.global_state
                 in_tuple
                 s.last_out_tuple
                 fields
            then (
              (* 4. Compute new minimal_out (and new group) *)
              let current_out =
                minimal_tuple_of_aggr
                  in_tuple
                  s.last_out_tuple
                  fields
                  s.global_state in
              let aggr = {
                first_in = in_tuple ;
                last_in = in_tuple ;
                current_out;
                last_ev_count = s.event_count ;
                fields ;
                prev_last_in = in_tuple } in
              (* Adding this group: *)
              Hashtbl.add s.groups k aggr ;
              Some (k, aggr)
            ) else None (* in-tuple does not pass where_slow *)
          | aggr ->
            (* The group already exist. *)
            (* 3. Filtering (slow path) - for existing group *)
            if where_slow
                 s.global_state
                 in_tuple
                 s.last_out_tuple
                 aggr.fields
            then (
              (* 4. Compute new current_out (and update the group) *)
              aggr.last_ev_count <- s.event_count ;
              (* current_out and last_in are better updated only after we called the
               * various clauses receiving aggr *)
              aggr.prev_last_in <- aggr.last_in ;
              aggr.last_in <- in_tuple ;
              aggr.current_out <-
                minimal_tuple_of_aggr
                  aggr.last_in
                  s.last_out_tuple
                  aggr.fields
                  s.global_state ;
              Some (k, aggr)
            ) else None (* in-tuple does not pass where_slow *)
          ) else None (* in-tuple does not pass where_fast *) in
      (match k_aggr_opt with
      | Some (k, aggr) ->
        (* 5. Post-condition to commit and flush *)
        IntCounter.add stats_selected_tuple_count 1 ;
        if must_commit aggr then (
          already_output_aggr := Some aggr ;
          commit_and_flush [ k, aggr ]
        ) else return_unit
      | None -> (* in_tuple failed filtering *)
        return_unit) ;%lwt
      (* Now there is also the possibility that we need to commit or flush
       * for every single input tuple :-< *)
      let%lwt () = commit_and_flush_all_if ForAll in
      return s
    in
    (* The event loop: *)
    let while_ () = if !quit <> None then return_false else return_true in
    let tuple_reader =
      match rb_ins with
      | [] -> (* yield expression *)
          yield_every ~while_ read_tuple every
      | [rb_in] ->
          read_single_rb ~while_ ~delay_rec:sleep_in read_tuple rb_in
      | rb_ins ->
          merge_rbs ~while_ ~delay_rec:sleep_in merge_on merge_timeout read_tuple rb_ins
    in
    tuple_reader (fun tx_size in_tuple ->
      with_state (fun s ->
        (* Set now and in.#count: *)
        CodeGenLib_IO.on_each_input_pre () ;
        (* Update per in-tuple stats *)
        IntCounter.add stats_in_tuple_count 1 ;
        IntCounter.add stats_rb_read_bytes tx_size ;
        (* Sort: add in_tupple into the heap of sorted tuples, update
         * smallest/greatest, and consider extracting the smallest. *)
        (* If we assume sort_last >= 2 then the sort buffer will never
         * be empty but for the very last tuple. In that case pretend
         * tuple_in is the first (sort.#count will still be 0). *)
        if sort_last <= 1 then
          aggregate_one s in_tuple
        else
          let sort_n = RamenSortBuf.length s.sort_buf in
          let or_in f =
            try f s.sort_buf with Invalid_argument _ -> in_tuple in
          let sort_key =
            sort_by (Uint64.of_int sort_n)
              (or_in RamenSortBuf.first) in_tuple
              (or_in RamenSortBuf.smallest) (or_in RamenSortBuf.greatest) in
          s.sort_buf <- RamenSortBuf.add sort_key in_tuple s.sort_buf ;
          let sort_n = sort_n + 1 in
          if sort_n >= sort_last ||
             sort_until (Uint64.of_int sort_n)
              (or_in RamenSortBuf.first) in_tuple
              (or_in RamenSortBuf.smallest) (or_in RamenSortBuf.greatest)
          then
            let min_in, sb = RamenSortBuf.pop_min s.sort_buf in
            s.sort_buf <- sb ;
            aggregate_one s min_in
          else
            return s)))

let casing codegen_version rc_str rc_marsh lst =
  (* Init the random number generator *)
  (match Sys.getenv "rand_seed" with
  | exception Not_found -> Random.self_init ()
  | "" -> Random.self_init ()
  | s -> Random.init (int_of_string s)) ;
  let help () =
    Printf.printf
      "This program is a Ramen worker (codegen %s).\n\n\
       Runtime configuration:\n\n%s\n\n\
       Have a nice day!\n"
      codegen_version rc_str in
  (* If we are called "ramen worker:" then we must run: *)
  if Sys.argv.(0) = RamenConsts.worker_argv0 then
    (* Call a function from lst according to envvar "name" *)
    match Sys.getenv "name" with
    | exception Not_found ->
        Printf.eprintf "Missing name envvar\n"
    | name ->
        (match List.assoc name lst with
        | exception Not_found ->
          Printf.eprintf
            "Unknown operation %S.\n\
             Trying to run a Ramen program? Try `ramen run %s`\n"
            name Sys.executable_name ;
            exit 1
        | f -> f ())
  else match Sys.argv.(1) with
  | exception Invalid_argument _ -> help ()
  | "1nf0" -> print_string rc_marsh
  | "version" ->
      (* Allow to override the reported version; useful for tests and also
       * maybe as a last-resort production hack: *)
      let v = getenv ~def:codegen_version "pretend_codegen_version" in
      Printf.printf "%s\n" v
  | _ -> help ()
