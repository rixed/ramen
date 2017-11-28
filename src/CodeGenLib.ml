(* Tools used by the generated OCaml code *)
open Batteries
open Stdint
open RamenLog
open Lwt
open Helpers

(* Converters from string to values *)

let float_of_string = Pervasives.float_of_string
let string_of_string x = x
let bool_of_string = Pervasives.bool_of_string
let u8_of_string = Uint8.of_string
let u16_of_string = Uint16.of_string
let u32_of_string = Uint32.of_string
let u64_of_string = Uint64.of_string
let u128_of_string = Uint128.of_string
let i8_of_string = Int8.of_string
let i16_of_string = Int16.of_string
let i32_of_string = Int32.of_string
let i64_of_string = Int64.of_string
let i128_of_string = Int128.of_string
let null_of_string = ()
let eth_of_string s = Uint48.of_string ("0x"^ String.nreplace s ":" "")
let ip4_of_string = Ipv4.of_string
let ip6_of_string = Ipv6.of_string
let cidr4_of_string = Ipv4.Cidr.of_string
let cidr6_of_string = Ipv6.Cidr.of_string

(* Functions *)

(* We are not allowed to have any state specific to this function.
 * Consequently we must compute the sequence number from the start
 * and increment and the global tuple count. *)
(* FIXME: that's fine but now we do have internal state for functions.
 * And we want sequence(start,step) to be reset at start at every
 * group (and maybe another, stateless, global sequence). *)
let sequence start inc =
  Int128.(start +
    (of_uint64 (Uint64.pred !CodeGenLib_IO.tuple_count)) * inc)

let age_float x = x -. !CodeGenLib_IO.now
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

(* State is count * sum, inited with 0, 0. *)
let avg_add (count, sum) x = count + 1, sum +. x
let avg_finalize (count, sum) = sum /. float_of_int count

(* Initialized with an empty list *)
let percentile_add prev x = x::prev
let percentile_finalize pct lst =
  let arr = Array.of_list lst in
  Array.fast_sort Pervasives.compare arr ;
  assert (pct >= 0.0 && pct <= 100.0) ;
  let pct = pct *. 0.01 in
  let idx = round_to_int (pct *. float_of_int (Array.length arr - 1)) in
  arr.(idx)
(* Specialized version for floats that deal with NaNs: *)
let float_percentile_add prev x =
  (* Inf values we can deal with but nan have only two possible outcome:
   * either we decide to make the whole percentile go nan, or we ignore them.
   * I believe that most of the time we got a nan is because a measurement
   * was actually missing, so let's skip them: *)
  if Float.is_nan x then prev else x::prev
let float_percentile_finalize pct lst =
  if lst = [] then
    (* This is annoying, we've skipped all the values! In this case it
     * would make sense to have the percentile function return NULL.
     * For now, just returns 0 and see. *)
    0.
  else percentile_finalize pct lst

let smooth prev alpha x = x *. alpha +. prev *. (1. -. alpha)

let split by what k =
  if what = "" then k what else
  String.nsplit ~by what |> Lwt_list.iter_s k

(* Remember values *)
type remember_state =
  { mutable filter : RamenBloomFilter.sliced_filter option ;
    false_positive_ratio : float ;
    duration : float ;
    mutable last_remembered : bool }

let remember_init false_positive_ratio duration =
  (* We cannot init the bloom filter before we receive the first tuple
   * since we need starting time: *)
  { filter = None ;
    false_positive_ratio ;
    duration ;
    last_remembered = false }

let remember_really_init st tim =
  let nb_slices = 10 in
  let start_time = tim -. st.duration
  and slice_width = st.duration /. float_of_int nb_slices in
  let filter = RamenBloomFilter.make_sliced start_time nb_slices slice_width
                                st.false_positive_ratio in
  st.filter <- Some filter ;
  filter

let remember_add st tim e =
  let filter =
    match st.filter with
    | None -> remember_really_init st tim
    | Some f -> f in
  st.last_remembered <- RamenBloomFilter.remember filter tim e ;
  st

let remember_finalize st = st.last_remembered

let hash x = Hashtbl.hash x |> Int64.of_int

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

  let avg p n t =
    let p = Uint32.to_int p and n = Uint32.to_int n in
    (fold p n t 0. (+.)) /. float_of_int n

  let linreg p n t =
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
  let multi_linreg p n t =
    let p = Uint32.to_int p and n = Uint32.to_int n in
    let open Owl in
    (* We first want to know how many observations and predictors we have: *)
    let nb_preds, nb_obs =
      fold p n t (-1, 0) (fun (nbp, nbo) (_y, xs) ->
        let nbp' = Array.length xs in
        assert (nbp = -1 || nbp = nbp') ;
        nbp', nbo+1) in
    (* Build the x and y matrices *)
    let xm = Mat.zeros nb_obs nb_preds
    and ym = Mat.zeros nb_obs 1 in
    iteri p n t (fun i (y, xs) ->
      ym.{ i, 0 } <- y ;
      for j = 0 to nb_preds-1 do
        xm.{ i, j } <- xs.(j)
      done) ;
    (* Now ask for the "best" parameters: *)
    match Linalg.D.linsolve xm ym with
    | exception _ ->
      let print_mat oc mat =
        let arr = Mat.to_arrays mat in
        Array.print (Array.print Float.print) oc arr in
      !logger.error "Cannot multi-fit! xm=%a, ym=%a"
        print_mat xm print_mat ym ;
      0.
    | p ->
      (* And use that to predict the new y given the new xs *)
      let _cury, cur_preds = current t in
      Array.fold_lefti (fun y i x ->
        y +. p.{i, 0} *. x) 0. cur_preds
end

let begin_of_range_cidr4 (n, l) = Ipv4.Cidr.and_to_len l n
let end_of_range_cidr4 (n, l) = Ipv4.Cidr.or_to_len l n
let begin_of_range_cidr6 (n, l) = Ipv6.Cidr.and_to_len l n
let end_of_range_cidr6 (n, l) = Ipv6.Cidr.or_to_len l n

(* Health and Stats
 *
 * Each node has to periodically report to ramen http server its health and some stats.
 * Could have been the other way around, and that would have made the
 * connection establishment easier possibly (since we already must be able to
 * ssh to other machines in order to start a node) but we already have an http
 * server on Ramen and probably want to avoid opening too many ports everywhere, and forcing
 * generated nodes to implement too many things.
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
  IntCounter.make Consts.in_tuple_count_metric
    "Number of received tuples that have been processed since the \
     node started."

let make_stats_selected_tuple_count () =
  IntCounter.make Consts.selected_tuple_count_metric
    "Number of tuples that have passed the WHERE filter, since the \
     node started."

let stats_out_tuple_count =
  IntCounter.make Consts.out_tuple_count_metric
    "Number of emitted tuples to each child of this node since it started."

let stats_cpu =
  FloatCounter.make Consts.cpu_time_metric
    "Total CPU time, in seconds, spent in this node (this process and any \
     subprocesses)."

let stats_ram =
  IntGauge.make Consts.ram_usage_metric
    "Total RAM size used by the GC, in bytes (does not take into account \
     other heap allocations nor fragmentation)."

let stats_rb_read_bytes =
  IntCounter.make Consts.rb_read_bytes_metric
    "Number of bytes read from the input ring buffer."

let stats_rb_write_bytes =
  IntCounter.make Consts.rb_write_bytes_metric
    "Number of bytes written in each output ring buffer."

let stats_rb_read_sleep_time =
  FloatCounter.make Consts.rb_wait_read_metric
    "Total number of seconds spent waiting for input."

let stats_rb_write_sleep_time =
  FloatCounter.make Consts.rb_wait_write_metric
    "Total number of seconds spent waiting for output."

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

let send_stats url =
  let open Cohttp in
  let open Cohttp_lwt_unix in
  let metrics = Hashtbl.fold (fun _name exporter lst ->
    List.rev_append (exporter ()) lst) Binocle.all_measures [] in
  let body = `String Marshal.(to_string metrics []) in
  let headers = Header.init_with "Content-Type" Consts.ocaml_marshal_type in
  (* TODO: but also fix the server never timeouting! *)
  let headers = Header.add headers "Connection" "close" in
  !logger.debug "Send stats to %S" url ;
  let%lwt resp, body = Client.put ~headers ~body (Uri.of_string url) in
  let code = resp |> Response.status |> Code.code_of_status in
  if code <> 200 then (
    let%lwt body = Cohttp_lwt.Body.to_string body in
    !logger.error "Received code %d, body %S, when reporting stats to %S"
      code body url ;
    return_unit
  ) else
    Cohttp_lwt.Body.drain_body body (* to actually close the connection *)

let update_stats_th report_url period () =
  while%lwt true do
    FloatCounter.set stats_cpu (tot_cpu_time ()) ;
    IntGauge.set stats_ram (tot_ram_usage ()) ;
    let%lwt () = send_stats report_url in
    Lwt_unix.sleep period
  done

(* Helpers *)

let output rb serialize_tuple (sersize, tuple) =
  let open RingBuf in
  let tx = enqueue_alloc rb sersize in
  let offs = serialize_tuple tx tuple in
  enqueue_commit tx ;
  assert (offs = sersize)

(* Each node can write in several ringbuffers (one per children). This list
 * will change dynamically as children are added/removed. *)
let outputer_of rb_ref_out_fname sersize_of_tuple serialize_tuple =
  let out_h = Hashtbl.create 5 (* Hash from fname to rb*outputer *)
  and out_l = ref []  (* list of outputer *)
  and get_out_fnames = RingBufLib.out_ringbuf_names rb_ref_out_fname in
  fun tuple ->
    IntCounter.add stats_out_tuple_count 1 ;
    let%lwt fnames = get_out_fnames () in
    Option.may (fun next ->
      (if Set.is_empty next then !logger.info else !logger.debug)
        "Must now output to: %a"
        (Set.print String.print) next ;
      (* Change occurred, load/unload as required *)
      let current = Hashtbl.keys out_h |> Set.of_enum in
      let to_open = Set.diff next current
      and to_close = Set.diff current next in
      Set.iter (fun fname ->
        !logger.debug "Unmapping %S" fname ;
        match Hashtbl.find out_h fname with
        | exception Not_found ->
          !logger.error "While unmapping %S: this file is not mapped?!" fname
        | rb, _ ->
          RingBuf.unload rb ;
          Hashtbl.remove out_h fname) to_close ;
      Set.iter (fun fname ->
          !logger.debug "Mapping %S" fname ;
          let rb = RingBuf.load fname in
          let once = output rb serialize_tuple in
          (* Note: we retry only on NoMoreRoom so that's OK to keep trying; in
           * case the ringbuf disappear altogether because the child is
           * terminated then we won't deadloop.  Also, if one child is full
           * then we will not write to next children until we can eventually
           * write to this one. This is actually desired to have proper message
           * ordering along the stream and avoid ending up with many threads
           * retrying to write to the same child. *)
          Hashtbl.add out_h fname (rb,
            RingBufLib.retry_for_ringbuf ~delay_rec:sleep_out once)
        ) to_open ;
      out_l := Hashtbl.values out_h /@ snd |> List.of_enum) fnames ;
    let sersize = sersize_of_tuple tuple in
    IntCounter.add stats_rb_write_bytes sersize ;
    List.map (fun out -> out (sersize, tuple)) !out_l |>
    join

type worker_conf =
  { debug : bool ; persist_dir : string ; report_url : string }

let node_start () =
  let debug = getenv ~def:"false" "debug" |> bool_of_string
  and node_name = getenv ~def:"?" "name" in
  let default_persist_dir =
    "/tmp/worker_"^ node_name ^"_"^ string_of_int (Unix.getpid ()) in
  let persist_dir = getenv ~def:default_persist_dir "persist_dir" in
  let logdir, prefix =
    match getenv "log_dir" with
    | exception _ -> None, node_name ^": "
    | ld -> Some ld, "" in
  Option.may mkdir_all logdir ;
  logger := make_logger ?logdir ~prefix debug ;
  !logger.debug "Starting %s process..." node_name ;
  let report_url =
    (* The real one will have a process identifier instead of "anonymous" *)
    getenv ~def:"http://localhost:29380/report/anonymous" "report_url" in
  let report_period =
    getenv ~def:"23.9" "report_period" |> float_of_string in
  async (update_stats_th report_url report_period) (* TODO: catch exceptions in async_exception_hook *) ;
  { debug ; persist_dir ; report_url }


(* Operations that nodes may run: *)

let read_csv_file filename do_unlink separator sersize_of_tuple
                  serialize_tuple tuple_of_strings preprocessor =
  let _conf = node_start () in
  let rb_ref_out_fname = getenv ~def:"/tmp/ringbuf_out_ref" "output_ringbufs_ref"
  (* For tests, allow to overwrite what's specified in the operation: *)
  and filename = getenv ~def:filename "csv_filename"
  and separator = getenv ~def:separator "csv_separator"
  in
  !logger.debug "Will read CSV file %S using separator %S"
                filename separator ;
  let of_string line =
    let strings = strings_of_csv separator line in
    tuple_of_strings (Array.of_list strings)
  in
  let outputer =
    outputer_of rb_ref_out_fname sersize_of_tuple serialize_tuple in
  CodeGenLib_IO.read_glob_lines ~do_unlink filename preprocessor (fun line ->
    match of_string line with
    | exception e ->
      !logger.error "Cannot parse line %S: %s"
        line (Printexc.to_string e) ;
      return_unit ;
    | tuple -> outputer tuple)

let listen_on collector addr_str port proto
              sersize_of_tuple serialize_tuple =
  let _conf = node_start () in
  let rb_ref_out_fname = getenv ~def:"/tmp/ringbuf_out_ref" "output_ringbufs_ref"
  and inet_addr = Unix.inet_addr_of_string addr_str
  in
  !logger.debug "Will listen to port %d for incoming %s messages"
                port (RamenProtocols.string_of_proto proto) ;
  let outputer =
    outputer_of rb_ref_out_fname sersize_of_tuple serialize_tuple in
  collector ~inet_addr ~port outputer

let yield sersize_of_tuple serialize_tuple select every =
  let _conf = node_start () in
  let rb_ref_out_fname = getenv ~def:"/tmp/ringbuf_out_ref" "output_ringbufs_ref"
  in
  let outputer =
    outputer_of rb_ref_out_fname sersize_of_tuple serialize_tuple in
  let rec loop () =
    let start = Unix.gettimeofday () in
    CodeGenLib_IO.on_each_input_pre () ;
    let%lwt () = outputer (select Uint64.zero () ()) in
    let sleep_time = every -. Unix.gettimeofday () +. start in
    if sleep_time > 0. then
      Lwt_unix.sleep sleep_time >>= loop
    else loop () in
  loop ()

(*
 * Aggregate operation
 *)

let notify url field_of_tuple tuple =
  let expand_fields =
    let open Str in
    let re = regexp "\\${\\(out\\.\\)?\\([_a-zA-Z0-9]+\\)}" in
    fun text tuple ->
      global_substitute re (fun s ->
          let field_name = matched_group 2 s in
          try field_of_tuple tuple field_name
          with Not_found ->
            !logger.error "Field %S used in text substitution is not \
                           present in the input!" field_name ;
            "??"^ field_name ^"??"
        ) text
  in
  let url = expand_fields url tuple in
  !logger.debug "Notifying url %S" url ;
  async (fun () -> CodeGenLib_IO.http_notify url)

type ('aggr, 'tuple_in, 'generator_out, 'top_state) aggr_value =
  { (* used to compute the actual selected field when outputing the
     * aggregate: *)
    mutable first_in : 'tuple_in ; (* first in-tuple of this aggregate *)
    mutable last_in : 'tuple_in ; (* last in-tuple of this aggregate *)
    (* We need both current and previous because we might commit/flush
     * groups that are not the one owning the current output tuple: *)
    mutable current_out : 'generator_out ; (* The current one *)
    mutable previous_out : 'generator_out ; (* previously computed temp out tuple, if any *)
    mutable nb_entries : int ;
    mutable nb_successive : int ;
    mutable last_ev_count : int ; (* used for others.successive (TODO) *)
    mutable to_resubmit : 'tuple_in list ; (* in_tuples to resubmit at flush *)
    mutable fields : 'aggr (* the record of aggregation values *) ;
    mutable sure_weight_state : 'top_state ;
    mutable unsure_weight : float }

let tot_weight float_of_top_state aggr =
  float_of_top_state aggr.sure_weight_state +. aggr.unsure_weight

(* TODO: instead of a single point, have 3 conditions for committing
 * (and 3 more for flushing) the groups; So that we could maybe split a
 * complex expression in smaller (faster) sub-conditions? But beware
 * of committing several times the same tuple. *)
type when_to_check_group = ForAll | ForAllSelected | ForInGroup
let string_of_when_to_check_group = function
  | ForAll -> "every group at every tuple"
  | ForAllSelected -> "every group at every selected tuple"
  | ForInGroup -> "the group that's updated by a tuple"

(* A Map from weight to keys with that weight so we can quickly find the
 * lighter of our heavy hitters: *)
module WeightMap = Map.Float

let print_weightmap fmt map =
  let print_key fmt lst =
    Printf.fprintf fmt "%d HH(s)" (List.length lst) in
  (WeightMap.print Float.print print_key) fmt map

let aggregate
      (read_tuple : RingBuf.tx -> 'tuple_in)
      (sersize_of_tuple : 'tuple_out -> int)
      (serialize_tuple : RingBuf.tx -> 'tuple_out -> int)
      (generate_tuples : ('tuple_out -> unit Lwt.t) -> 'tuple_in -> 'generator_out -> unit Lwt.t)
      (tuple_of_aggr :
        Uint64.t -> 'tuple_in -> 'tuple_in -> (* in.#count, current and last *)
        Uint64.t -> Uint64.t -> 'tuple_in -> (* selected.#count, #successive and last *)
        Uint64.t -> Uint64.t -> 'tuple_in -> (* unselected.#count, #successive and last *)
        Uint64.t -> (* out.#count *)
        Uint64.t -> Uint64.t -> 'aggr -> (* group.#count, #successive, aggr *)
        'global_state ->
        'tuple_in -> 'tuple_in -> (* first, last *)
        'generator_out)
      (* Where_fast/slow: premature optimisation: if the where filter
       * uses the aggregate then we need where_slow (checked after
       * the aggregate look up) but if it uses only the incoming
       * tuple then we can use only where_fast. *)
      (where_fast :
        'global_state ->
        Uint64.t -> 'tuple_in -> 'tuple_in -> (* in.#count, current and last *)
        Uint64.t -> Uint64.t -> 'tuple_in -> (* selected.#count, #successive and last *)
        Uint64.t -> Uint64.t -> 'tuple_in -> (* unselected.#count, #successive and last *)
        bool)
      (where_slow :
        'global_state ->
        Uint64.t -> 'tuple_in -> 'tuple_in -> (* in.#count, current and last *)
        Uint64.t -> Uint64.t -> 'tuple_in -> (* selected.#count, #successive and last *)
        Uint64.t -> Uint64.t -> 'tuple_in -> (* unselected.#count, #successive and last *)
        Uint64.t -> Uint64.t -> 'aggr -> (* group.#count, #successive, aggr *)
        'tuple_in -> 'tuple_in -> (* first, last *)
        bool)
      (key_of_input : 'tuple_in -> 'key)
      (top : (int * ('global_state -> 'top_state -> 'tuple_in -> unit)) option)
      (top_init : 'global_state -> 'top_state)
      (float_of_top_state : 'top_state -> float)
      (commit_when :
        Uint64.t -> 'tuple_in -> 'tuple_in -> (* in.#count, current and last *)
        Uint64.t -> Uint64.t -> 'tuple_in -> (* selected.#count, #successive and last *)
        Uint64.t -> Uint64.t -> 'tuple_in -> (* unselected.#count, #successive and last *)
        Uint64.t -> 'generator_out -> (* out.#count, previous *)
        Uint64.t -> Uint64.t -> 'aggr -> (* group.#count, #successive, aggr *)
        'global_state ->
        'tuple_in -> 'tuple_in -> 'generator_out -> (* first, last, current out *)
        bool)
      commit_before
      do_flush
      (when_to_check_for_commit : when_to_check_group)
      (should_resubmit : ('aggr, 'tuple_in, 'generator_out, 'top_state) aggr_value -> 'tuple_in -> bool)
      (global_state : 'global_state)
      (group_init : 'global_state -> 'aggr)
      (field_of_tuple : 'tuple_out -> string -> string)
      (notify_url : string) =
  let conf = node_start () in
  let when_str = string_of_when_to_check_group when_to_check_for_commit in
  !logger.debug "We will commit/flush for... %s" when_str ;
  let rb_in_fname = getenv ~def:"/tmp/ringbuf_in" "input_ringbuf"
  and rb_ref_out_fname = getenv ~def:"/tmp/ringbuf_out_ref" "output_ringbufs_ref"
  and stats_selected_tuple_count = make_stats_selected_tuple_count ()
  (* FIXME: most of this should be in the saved state! *)
  and event_count = ref 0 (* used to fake others.count etc *)
  and last_key = ref None (* used for successive count *)
  and last_in_tuple = ref None (* last incoming tuple *)
  and selected_tuple = ref None (* last incoming tuple that passed the where filter *)
  and selected_count = ref Uint64.zero
  and selected_successive = ref Uint64.zero
  and unselected_tuple = ref None
  and unselected_count = ref Uint64.zero
  and unselected_successive = ref Uint64.zero
  and out_count = ref Uint64.zero
  (* Top book-keeping:
   * In addition to the hash of aggr (which for each entry has a weight) we
   * need to quickly find the key of the smallest weight, thus the WeightMap
   * defined above: *)
  and weightmap : 'key list WeightMap.t ref = ref WeightMap.empty
  (* Each time we have no idea who the weight belongs to we pour it in there,
   * at the bucket corresponding to the hash of the key.
   * Each time we start tracking an item we must (pessimistically) report all of
   * it as its unknown weight.  Unfortunately there is no way this value could
   * ever decrease. *)
  (* TODO: dynamically size according to how quickly we fill it to XX%, which
   * is very cheap to measure. What is less easy is how to spread the keys when
   * we extend it. So, maybe just the other way around: start from very large
   * and reduce it? *)
  and unknown_weights = Array.make 1024 0.
  (* The aggregate entry for "others": *)
  and others = ref None
  and stats_group_count =
    IntGauge.make Consts.group_count_metric "Number of groups currently maintained."
  and top_n, top_by = Option.default (0, fun _ _ _ -> ()) top in
  assert (not commit_before || top_n = 0) ;
  IntGauge.set stats_group_count 0 ;
  let tuple_outputer =
    outputer_of rb_ref_out_fname sersize_of_tuple serialize_tuple in
  let outputer =
    let do_out tuple =
      if notify_url <> "" then
        notify notify_url field_of_tuple tuple ;
      tuple_outputer tuple
    in
    generate_tuples do_out in
  let commit in_tuple out_tuple =
    (* in_tuple here is useful for generators *)
    out_count := Uint64.succ !out_count ;
    outputer in_tuple out_tuple
  and with_state =
    let open CodeGenLib_State.Persistent in
    let init_state = Hashtbl.create 701 in
    let state = ref (make conf.persist_dir "aggregate" init_state) in
    fun f ->
      let v = restore !state in
      (* We do _not_ want to save the value when f raises an exception: *)
      let%lwt v = f v in
      state := save ~save_every:1013 ~save_timeout:21. !state v ;
      return_unit
  in
  !logger.debug "Will read ringbuffer %S" rb_in_fname ;
  let%lwt rb_in =
    retry ~on:(fun _ -> true) ~min_delay:1.0
          (fun n -> return (RingBuf.load n)) rb_in_fname
  in
  (* The big event loop: *)
  CodeGenLib_IO.read_ringbuf ~delay_rec:sleep_in rb_in (fun tx ->
    let in_tuple = read_tuple tx in
    RingBuf.dequeue_commit tx ;
    with_state (fun h ->
      (* Update per in-tuple stats *)
      IntCounter.add stats_in_tuple_count 1 ;
      IntCounter.add stats_rb_read_bytes (RingBuf.tx_size tx) ;
      (* Define some short-hand values and functions we will keep
       * referring to: *)
      let last_in = Option.default in_tuple !last_in_tuple
      and in_count = !CodeGenLib_IO.tuple_count
      and last_selected = Option.default in_tuple !selected_tuple
      and last_unselected = Option.default in_tuple !unselected_tuple
      and already_output_aggr = ref None in
      let already_output a =
        Option.map_default ((==) a) false !already_output_aggr in
      (* Tells if the group must be committed/flushed: *)
      let must cond aggr = (* TODO: pass selected_successive *)
        cond
          in_count in_tuple last_in
          !selected_count !selected_successive last_selected
          !unselected_count !unselected_successive last_unselected
          !out_count aggr.previous_out
          (Uint64.of_int aggr.nb_entries)
          (Uint64.of_int aggr.nb_successive)
          aggr.fields
          global_state
          aggr.first_in aggr.last_in
          aggr.current_out
      in
      let commit_and_flush_list to_commit =
        (* We must commit first and then flush *)
        (* FIXME: Tops: Send tuple for "others" ? The problem with this is
         * that the current_out of this aggregate was build from a given
         * in_tuple and nothing tells us what this additional "others" tuple
         * represents, and that possibly other keys than the one seemingly
         * present have contributed to its aggregated values. It is not easy
         * to fix.  Idea 1: make all fields used in the key clause NULL to
         * anonymise this entry ; but still nothing guarantee that it's
         * distinguishable from normal top output.  Idea 2: add an additional
         * column to top operations to flag the "other" tuple.  Idea 3: Or
         * rather, have a virtual boolean field that says if this is "others",
         * that the user could request in the selected fields and that we
         * would pass to generate_tuples; with it the user could for instance
         * blank values that have no sense.  Idea 4: short term: do not output
         * this additional tuple. *)
        (*
        let to_commit =
          if to_commit <> [] && !others <> None then
            ("others", Option.get !others) :: to_commit
          else to_commit in *)
        if to_commit = [] then return_unit
        else if top_n <> 0 then (
          (* For top operations we must commit and flush all or nothing. *)
          !logger.debug "Committing/Flushing the whole TOP" ;
          let%lwt () =
            Hashtbl.fold (fun _k a th ->
              let%lwt () = th in
              commit in_tuple a.current_out) h return_unit in
          (* TODO: handle resubmission *)
          Hashtbl.clear h ;
          others := None ;
          Array.fill unknown_weights 0 (Array.length unknown_weights) 0. ;
          weightmap := WeightMap.empty ;
          return_unit
        ) else (
          (* Not in a top, do things properly: *)
          (* Commit *)
          Lwt_list.iter_s (fun (k, a, out) ->
            (* Flush/Keep/Slide *)
            if do_flush then (
              if a.to_resubmit = [] then
                Hashtbl.remove h k
              else (
                let to_resubmit = List.rev a.to_resubmit in
                a.nb_entries <- 0 ;
                a.to_resubmit <- [] ;
                a.fields <- group_init global_state ;
                (* Warning: should_resubmit might need realistic nb_entries,
                 * last_in etc *)
                a.first_in <- List.hd to_resubmit ;
                List.iter (fun in_tuple ->
                    a.nb_entries <- a.nb_entries + 1 ;
                    tuple_of_aggr
                      (* We cannot possibly save the values of in_count,
                       * last_in, selected_count, etc, at the time we
                       * originally added those tuples, so this is
                       * approximate. Should we prevent their use in all
                       * sliding windows cases? *)
                      in_count in_tuple last_in
                      !selected_count !selected_successive last_selected
                      !unselected_count !unselected_successive last_unselected
                      !out_count
                      (Uint64.of_int a.nb_entries)
                      (Uint64.of_int a.nb_successive)
                      a.fields
                      global_state
                      a.first_in in_tuple |> ignore ;
                    a.last_in <- in_tuple ;
                    if should_resubmit a in_tuple then
                      a.to_resubmit <- in_tuple :: a.to_resubmit
                  ) to_resubmit)
            ) ;
            (* Output the tuple *)
            commit in_tuple out) to_commit)
      in
      let commit_and_flush_all_if check_when =
        let to_commit =
          if when_to_check_for_commit <> check_when then [] else
            Hashtbl.fold (fun k a l ->
                if not (already_output a) &&
                   must commit_when a then
                  (k, a, a.current_out)::l
                else l
              ) h [] in
        commit_and_flush_list to_commit
      in
      (* Now that this is all in place, here are the next steps:
       * 1. filtering (fast path)
       * 2. retrieve the group
       * 3. filtering (slow path)
       * 4. compute new out_tuple (aggregation)
       * 5. post-condition to commit and flush
       *
       * Note that steps 3 and 4 have two implementations, depending on
       * whether the group is a new one or not. If the group is new we have
       * additional code due to the TOP operation. *)
      (if where_fast
           global_state
           in_count in_tuple last_in
           !selected_count !selected_successive last_selected
           !unselected_count !unselected_successive last_unselected
      then (
        (* build the key and retrieve the group *)
        IntGauge.set stats_group_count (Hashtbl.length h) ;
        let k = key_of_input in_tuple in
        let prev_last_key = !last_key in
        last_key := Some k ;
        let accumulate_into aggr this_key =
          aggr.last_ev_count <- !event_count ;
          aggr.nb_entries <- aggr.nb_entries + 1 ;
          if should_resubmit aggr in_tuple then
            aggr.to_resubmit <- in_tuple :: aggr.to_resubmit ;
          if prev_last_key = this_key then
            aggr.nb_successive <- aggr.nb_successive + 1 ;
          top_by global_state aggr.sure_weight_state in_tuple
        in
        (* Update/create the group if it passes where_slow (or None) *)
        let aggr_opt =
          match Hashtbl.find h k with
          | exception Not_found ->
            (*
             * The group does not exist for that key, create one?
             * This is also where the TOP selection happens.
             * Notice there is no "commit-before" for new groups.
             *)
            let fields = group_init global_state
            and zero = Uint64.zero and one = Uint64.one in
            if where_slow
                 global_state
                 in_count in_tuple last_in
                 !selected_count !selected_successive last_selected
                 !unselected_count !unselected_successive last_unselected
                 zero zero fields
                 (* Although we correctly have 0 fields and group.#count and
                  * #successive to 0, we have to pass first and past tuples
                  * in that empty group; instead, pass the current tuple: *)
                 in_tuple in_tuple
            then (
              IntCounter.add stats_selected_tuple_count 1 ;
              (* TODO: pass selected_successive *)
              let out_generator =
                tuple_of_aggr
                  in_count in_tuple last_in
                  !selected_count !selected_successive last_selected
                  !unselected_count !unselected_successive last_unselected
                  !out_count
                  one one fields
                  global_state
                  in_tuple in_tuple in
              (* What part of unknown weight might belong to this guy? *)
              let kh = Hashtbl.hash k mod Array.length unknown_weights in
              let aggr = {
                first_in = in_tuple ;
                last_in = in_tuple ;
                current_out =  out_generator ;
                previous_out = out_generator ; (* Not correct for the very first check *)
                nb_entries = 1 ;
                nb_successive = 1 ;
                last_ev_count = !event_count ;
                to_resubmit = [] ;
                fields ;
                sure_weight_state = top_init global_state ;
                unsure_weight = unknown_weights.(kh) } in
              top_by global_state aggr.sure_weight_state in_tuple ;
              (* Adding this group and updating the TOP *)
              let add_entry () =
                Hashtbl.add h k aggr ;
                let wk = tot_weight float_of_top_state aggr in
                weightmap := WeightMap.modify_def [] wk (List.cons k) !weightmap ;
                if should_resubmit aggr in_tuple then
                  aggr.to_resubmit <- [ in_tuple ]
              in
              if top_n = 0 || Hashtbl.length h < top_n then (
                add_entry () ;
                Some aggr
              ) else (
                (* H is crowded already, maybe dispose of the less heavy hitter? *)
                match WeightMap.min_binding !weightmap with
                | exception Not_found ->
                  !logger.error "Empty weightmap but full hashtbl?" ;
                  assert false
                | _wk, [] ->
                  !logger.error "Weightmap entry with no map key" ;
                  assert false
                | wk, (min_k::min_ks) ->
                  if wk < tot_weight float_of_top_state aggr then (
                    (* Remove previous entry *)
                    (match Hashtbl.find h min_k with
                    | exception Not_found ->
                      !logger.error "Weight %f from weightmap does not point to a group?!" wk
                    | removed ->
                      (* FIXME: find and remove in one go! Hashtbl.extract? *)
                      Hashtbl.remove h min_k ;
                      let kh' = Hashtbl.hash min_k mod Array.length unknown_weights in
                      (* Note: the unsure_weight we took it from unknown_weights.(kh')
                       * already and it's still there *)
                      unknown_weights.(kh') <-
                        unknown_weights.(kh') +. float_of_top_state removed.sure_weight_state) ;
                    weightmap := snd (WeightMap.pop_min_binding !weightmap) ;
                    if min_ks <> [] then
                      weightmap := WeightMap.add wk min_ks !weightmap ;
                    (* Add new one *)
                    add_entry () ;
                    Some aggr
                  ) else (
                    (* Do not track; aggregate with "others" *)
                    (match !others with
                    | None -> others := Some aggr
                    | Some others_aggr ->
                      (* We do not care about the out tuple but we still
                       * have to update the aggr.fields (which in turn can
                       * need some fields from out). *)
                      tuple_of_aggr
                        in_count in_tuple last_in
                        !selected_count !selected_successive last_selected
                        !unselected_count !unselected_successive last_unselected
                        !out_count
                        (Uint64.of_int others_aggr.nb_entries)
                        (Uint64.of_int others_aggr.nb_successive)
                        others_aggr.fields
                        global_state
                        others_aggr.first_in in_tuple |> ignore ;
                        accumulate_into others_aggr None ;
                        (* Those two are not updated by accumulate_into to allow clauses
                         * code to see their previous values *)
                        others_aggr.current_out <- out_generator ;
                        aggr.last_in <- in_tuple) ;
                    last_key := None ;
                    unknown_weights.(kh) <-
                      unknown_weights.(kh) +. float_of_top_state aggr.sure_weight_state ;
                    None)
              )
            ) else None (* in-tuple does not pass where_slow *)
          | aggr ->
            (*
             * The group already exist.
             *)
            if where_slow
                 global_state
                 in_count in_tuple last_in
                 !selected_count !selected_successive last_selected
                 !unselected_count !unselected_successive last_unselected
                 (Uint64.of_int aggr.nb_entries) (Uint64.of_int aggr.nb_successive) aggr.fields
                 aggr.first_in aggr.last_in
            then (
              IntCounter.add stats_selected_tuple_count 1 ;
              let prev_wk = tot_weight float_of_top_state aggr in
              (* Compute the new out_tuple and update the group *)
              accumulate_into aggr (Some k) ;
              (* current_out and last_in are better updated only after we called the
               * various clauses receiving aggr *)
              (* TODO: pass selected_successive *)
              aggr.current_out <-
                tuple_of_aggr
                  in_count in_tuple last_in
                  !selected_count !selected_successive last_selected
                  !unselected_count !unselected_successive last_unselected
                  !out_count
                  (Uint64.of_int aggr.nb_entries) (Uint64.of_int aggr.nb_successive) aggr.fields
                  global_state
                  aggr.first_in aggr.last_in ;
              aggr.last_in <- in_tuple ;
              let new_wk = tot_weight float_of_top_state aggr in
              (* No need to update the weightmap if the weight haven't changed.
               * If it seldom happen when using a TOP clause, it does happen
               * all the time when _not_ using one! *)
              if prev_wk <> new_wk then (
                (* Move in the WeightMap. First remove: *)
                weightmap := WeightMap.modify_opt prev_wk (function
                  | None -> assert false
                  | Some [k'] ->
                    (* If this is not the usual case then we are in trouble. In
                     * other words you should not have many of your top_n heavy
                     * hitters with the same weight. *)
                    assert (k = k') ; None
                  | Some lst ->
                    let lst' = List.filter ((<>) k) lst in
                    assert (lst' <> []) ;
                    Some lst') !weightmap ;
                (* reinsert with new weight: *)
                weightmap := WeightMap.modify_opt new_wk (function
                  | None -> Some [k]
                  | Some lst as prev ->
                    if List.mem k lst then prev else Some (k::lst)) !weightmap) ;
              Some aggr
            ) else None (* in-tuple does not pass where_slow *) in
        (match aggr_opt with
        | None ->
          selected_successive := Uint64.zero ;
          unselected_tuple := Some in_tuple ;
          unselected_count := Uint64.succ !unselected_count ;
          unselected_successive := Uint64.succ !unselected_successive ;
          return_unit
        | Some aggr ->
          (* Here we passed the where filter and the selected_tuple (and
           * selected_count) must be updated. *)
          unselected_successive := Uint64.zero ;
          selected_tuple := Some in_tuple ;
          selected_count := Uint64.succ !selected_count ;
          selected_successive := Uint64.succ !selected_successive ;
          (* Committing / Flushing *)
          let to_commit, to_flush =
            (* FIXME: we should do the check that early for all when_to_check_for_commit
             * (and skip this aggr when doing ForAllSelected or ForAll)., so that we can
             * commit_before this group (for others that does not matter since we did not
             * change them). *)
            if must commit_when aggr
            then [
              k, aggr,
              if commit_before then aggr.previous_out
              else aggr.current_out
            ], [ k, aggr ] else [], [] in
          if commit_before then
            List.iter (fun (_k, a) ->
              match a.to_resubmit with
              | hd::_ when hd == in_tuple -> ()
              | _ -> a.to_resubmit <- in_tuple :: a.to_resubmit) to_flush ;
          let%lwt () = commit_and_flush_list to_commit in
          (* Maybe any other groups. Notice that there is no risk to commit
           * or flush this aggr twice since when_to_check_for_commit force
           * either one or the other (or none at all) of these chunks of
           * code to be run. *)
          already_output_aggr := Some aggr ;
          let%lwt () = commit_and_flush_all_if ForAllSelected in
          aggr.previous_out <- aggr.current_out ;
          return_unit)
      ) else
        (* in_tuple failed where_fast *)
        return_unit
      ) >>= fun () ->
      (* Save last_in: *)
      last_in_tuple := Some in_tuple ;
      (* Now there is also the possibility that we need to commit or flush
       * for every single input tuple :-< *)
      let%lwt () = commit_and_flush_all_if ForAll in
      return h)
  )

let () =
  async_exception_hook := (fun exn ->
    !logger.error "Received exception %s\n%s"
      (Printexc.to_string exn)
      (Printexc.get_backtrace()))
