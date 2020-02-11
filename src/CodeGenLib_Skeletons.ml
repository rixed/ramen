(* Skeleton of the basic operations (aggregate, read csv...), parameterized
 * by functions that are generated (by CodeGen_OCaml). *)
open Batteries
open Stdint
open RamenLog
open RamenHelpers
open RamenConsts
open RamenNullable
open Binocle
module T = RamenTypes
module Files = RamenFiles
module Channel = RamenChannel
module IO = CodeGenLib_IO
module Casing = CodeGenLib_Casing
module State = CodeGenLib_State
module Publish = CodeGenLib_Publish
module OutRef = RamenOutRef
module Heap = RamenHeap
module SzHeap = RamenSzHeap
module SortBuf = RamenSortBuf
module FieldMask = RamenFieldMask
module VOS = RamenSync.Value.OutputSpecs
module C = CodeGenLib_Config
module Stats = CodeGenLib_Stats

let quit = ref None
let not_quit () = !quit = None

(* Basic tuple without aggregate specific counters: *)
let get_binocle_tuple conf ic sc gc =
  let si v =
    if v < 0 then !logger.error "Negative int counter: %d" v ;
    NotNull (Uint64.of_int v) in
  let sg = function None -> Null | Some (_, v, _) -> si v
  and s v = NotNull v
  and ram, max_ram =
    match IntGauge.get Stats.ram with
    | None -> Uint64.zero, Uint64.zero
    | Some (_mi, x, ma) -> Uint64.of_int x, Uint64.of_int ma
  and min_event_time, max_event_time =
    match FloatGauge.get Publish.Stats.event_time with
    | None -> Null, Null
    | Some (mi, _, ma) -> NotNull mi, NotNull ma
  and time = Unix.gettimeofday ()
  and perf p =
    p.Binocle.Perf.count, p.Binocle.Perf.system, p.Binocle.Perf.user in
  let sp p =
    let count, system, user =
      Option.map_default perf (0, 0., 0.) p in
    Uint32.of_int count, system, user
  in
  (conf.C.site :> string), (conf.fq :> string), conf.is_top_half, time,
  min_event_time, max_event_time,
  nullable_of_option ic,
  nullable_of_option sc,
  IntCounter.get Stats.out_tuple_count |> si,
  nullable_of_option gc,
  FloatCounter.get Stats.cpu,
  (* Assuming we call Stats.update before this: *)
  ram, max_ram,
  (* Start measurements as a single record (BEWARE FIELD ORDERING!): *)
  (* FIXME: make RamenWorkerStats the only place where this record is defined,
   * instead of there, here, in RamenPs. *)
  (Perf.get Stats.perf_commit_incoming |> sp,
   Perf.get Stats.perf_commit_others |> sp,
   Perf.get Stats.perf_finalize_others |> sp,
   Perf.get Stats.perf_find_group |> sp,
   Perf.get Stats.perf_flush_others |> sp,
   Perf.get Stats.perf_select_others |> sp,
   Perf.get Stats.perf_per_tuple |> sp,
   Perf.get Stats.perf_update_group |> sp,
   Perf.get Stats.perf_where_fast |> sp,
   Perf.get Stats.perf_where_slow |> sp),
  FloatCounter.get Stats.read_sleep_time |> s,
  FloatCounter.get Stats.write_sleep_time |> s,
  IntCounter.get Stats.read_bytes |> si,
  IntCounter.get Stats.write_bytes |> si,
  IntGauge.get Stats.avg_full_out_bytes |> sg,
  FloatGauge.get Stats.last_out |>
    Option.map Stats.gauge_current |> nullable_of_option,
  Stats.startup_time

let send_stats
    rb (_, _, _, time, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _
        as tuple) =
  let open RingBuf in
  let head = RingBufLib.DataTuple Channel.live in
  let sersize =
    RingBufLib.message_header_sersize head +
    RamenWorkerStatsSerialization.max_sersize_of_tuple tuple in
  match enqueue_alloc rb sersize with
  | exception NoMoreRoom -> () (* Just skip *)
  | tx ->
    RingBufLib.(write_message_header tx 0 head) ;
    let offs = RingBufLib.message_header_sersize head in
    let offs = RamenWorkerStatsSerialization.serialize tx offs tuple in
    enqueue_commit tx time time ;
    assert (offs <= sersize)

let update_stats_rb report_period rb get_tuple =
  if report_period > 0. then
    Unix.sleepf (Random.float report_period) ;
    while true do
      Stats.update () ;
      let tuple = get_tuple () in
      send_stats rb tuple ;
      Unix.sleepf (jitter ~amplitude:0.1 report_period)
    done

(* Helpers *)

(* Read a list of values from the environment: *)
let getenv_list name f =
  let rec loop lst i =
    match Sys.getenv (name ^"_"^ string_of_int i) with
    | exception Not_found -> lst
    | n -> loop (f n :: lst) (i + 1) in
  loop [] 0

type 'a out_synckey =
  { key : string }

let may_publish_stats =
  (* When did we publish the last tuple in our conf topic and the runtime
   * stats? *)
  let last_publish_stats = ref 0. in
  fun conf publish_stats ->
    (* Cannot use CodeGenLib.now as we want the clock to advance even when no input
     * is received: *)
    let now = Unix.time () in
    if now -. !last_publish_stats > conf.C.report_period then (
      last_publish_stats := now ;
      let cur_ram, max_ram =
        match IntGauge.get Stats.ram with
        | None -> Uint64.(zero, zero)
        | Some (_mi, cur, ma) -> Uint64.(of_int cur, of_int ma)
      and min_etime, max_etime =
        match FloatGauge.get Publish.Stats.event_time with
        | None -> None, None
        | Some (mi, _, ma) -> Some mi, Some ma in
      let stats = RamenSync.Value.RuntimeStats.{
        stats_time = now ;
        first_startup = Stats.startup_time ;
        last_startup = Stats.startup_time ;
        min_etime ; max_etime ;
        first_input = !CodeGenLib.first_input ;
        last_input = !CodeGenLib.last_input ;
        first_output = !Stats.first_output ;
        last_output = !Stats.last_output ;
        tot_in_tuples =
          Uint64.of_int (IntCounter.get Stats.in_tuple_count) ;
        tot_sel_tuples =
          Uint64.of_int (IntCounter.get Stats.selected_tuple_count) ;
        tot_out_tuples =
          Uint64.of_int (IntCounter.get Stats.out_tuple_count) ;
        tot_full_bytes = !Stats.tot_full_bytes ;
        tot_full_bytes_samples = !Stats.tot_full_bytes_samples ;
        cur_groups =
          Uint64.of_int ((IntGauge.get Stats.group_count |>
                          Option.map Stats.gauge_current) |? 0) ;
        tot_in_bytes =
          Uint64.of_int (IntCounter.get Stats.read_bytes) ;
        tot_out_bytes =
          Uint64.of_int (IntCounter.get Stats.write_bytes) ;
        tot_wait_in = FloatCounter.get Stats.read_sleep_time ;
        tot_wait_out = FloatCounter.get Stats.write_sleep_time ;
        tot_firing_notifs =
          Uint64.of_int (IntCounter.get Stats.firing_notif_count) ;
        tot_extinguished_notifs =
          Uint64.of_int (IntCounter.get Stats.extinguished_notif_count) ;
        tot_cpu = FloatCounter.get Stats.cpu ;
        cur_ram ; max_ram } in
      publish_stats stats
    )

let info_or_test conf =
  if conf.C.is_test then !logger.debug else !logger.info

let worker_start conf get_binocle_tuple
                 time_of_tuple factors_of_tuple
                 serialize_tuple sersize_of_tuple
                 orc_make_handler orc_write orc_close
                 k =
  Files.reset_process_name () ;
  let default_persist_dir =
    "/tmp/worker_"^ (conf.C.fq :> string) ^"_"^
    (if conf.C.is_top_half then "TOP_HALF_" else "")^
    string_of_int (Unix.getpid ()) in
  let globals_dir =
    let def = default_persist_dir ^"/globals.lmdb" in
    N.path (getenv ~def "globals_dir") in
  CodeGenLib_Globals.init globals_dir ;
  let report_rb_fname =
    N.path (getenv ~def:"/tmp/ringbuf_in_report.r" "report_ringbuf") in
  let report_rb = RingBuf.load report_rb_fname in
  (* Must call this once before get_binocle_tuple because cpu/ram gauges
   * must not be NULL: *)
  Stats.update () ;
  (* Then, the sooner a new worker appears in the stats the better: *)
  if conf.report_period > 0. then
    ignore_exceptions (send_stats report_rb) (get_binocle_tuple ()) ;
  info_or_test conf
    "Starting %a%s process (pid=%d). Will log into %s at level %s."
    N.fq_print conf.C.fq (if conf.C.is_top_half then " (TOP-HALF)" else "")
    (Unix.getpid ())
    (string_of_log_output !logger.output)
    (string_of_log_level conf.C.log_level) ;
  set_signals Sys.[sigterm; sigint] (Signal_handle (fun s ->
    info_or_test conf "Received signal %s" (name_of_signal s) ;
    quit :=
      Some (if s = Sys.sigterm then ExitCodes.terminated
                               else ExitCodes.interrupted))) ;
  (* Dump stats on sigusr1: *)
  set_signals Sys.[sigusr1] (Signal_handle (fun s ->
    (* This log also useful to rotate the logfile. *)
    !logger.info "Received signal %s, dumping stats"
      (name_of_signal s) ;
    Binocle.display_console ())) ;
  Thread.create (
    restart_on_failure "update_stats_rb"
      (update_stats_rb conf.report_period report_rb)) get_binocle_tuple |>
    ignore ;
  let last_report () =
    (* Sending stats for one last time: *)
    if conf.report_period > 0. then
      ignore_exceptions (send_stats report_rb) (get_binocle_tuple ()) in
  (* Init config sync client if a url was given: *)
  let publish_stats, outputer =
    Publish.start_zmq_client conf ~while_:not_quit
                             time_of_tuple factors_of_tuple
                             serialize_tuple sersize_of_tuple
                             orc_make_handler orc_write orc_close in
  match k publish_stats outputer with
  | exception e ->
      print_exception e ;
      last_report () ;
      exit ExitCodes.uncaught_exception
  | () ->
      last_report () ;
      exit (!quit |? ExitCodes.terminated)

(*
 * Operations that funcs may run: read a CSV file.
 *)

let read read_source parse_data sersize_of_tuple time_of_tuple
         factors_of_tuple serialize_tuple
         orc_make_handler orc_write orc_close =
  let conf = C.make_conf () in
  let get_binocle_tuple () =
    get_binocle_tuple conf None None None in
  worker_start conf get_binocle_tuple
               time_of_tuple factors_of_tuple
               serialize_tuple sersize_of_tuple
               orc_make_handler orc_write orc_close
               (fun publish_stats outputer ->
    let while_ () =
      may_publish_stats conf publish_stats ;
      not_quit () in
    read_source quit while_ (parse_data (fun tuple ->
      CodeGenLib.on_each_input_pre () ;
      IntCounter.inc Stats.in_tuple_count ;
      outputer (RingBufLib.DataTuple Channel.live) (Some tuple))))

(*
 * Operations that funcs may run: listen to some known protocol.
 *)

let listen_on
      (collector : ?while_:(unit -> bool) -> ('a -> unit) -> unit)
      proto_name
      sersize_of_tuple time_of_tuple factors_of_tuple serialize_tuple
      orc_make_handler orc_write orc_close =
  let conf = C.make_conf () in
  let get_binocle_tuple () =
    get_binocle_tuple conf None None None in
  worker_start conf get_binocle_tuple
               time_of_tuple factors_of_tuple
               serialize_tuple sersize_of_tuple
               orc_make_handler orc_write orc_close
               (fun publish_stats outputer ->
    info_or_test conf "Will listen for incoming %s messages" proto_name ;
    let while_ () =
      may_publish_stats conf publish_stats ;
      not_quit () in
    collector ~while_ (fun tup ->
      CodeGenLib.on_each_input_pre () ;
      IntCounter.inc Stats.in_tuple_count ;
      outputer (RingBufLib.DataTuple Channel.live) (Some tup) ;
      ignore (Gc.major_slice 0)))

(*
 * Operations that funcs may run: read known tuples from a ringbuf.
 *)

let log_rb_error =
  let last_err = ref 0
  and err_count = ref 0 in
  fun ?at_exit tx e ->
    let open RingBuf in
    (* Subtract one word from the start of the TX to get to the length
     * of the message, which is a nicer starting position to dump: *)
    let startw = tx_start tx - 1
    and sz = tx_size tx
    and fname = tx_fname tx in
    assert (sz land 3 = 0) ;
    let stopw = tx_start tx + (sz / 4) in
    !logger.error "While reading message from %S at words %d..%d(excl): %s"
        fname startw stopw (Printexc.to_string e) ;
    let now = int_of_float (Unix.time ()) in
    if now = !last_err then (
      incr err_count ;
      if !err_count > 5 then (
        Option.may (fun f -> f ()) at_exit ;
        exit ExitCodes.damaged_ringbuf
      )
    ) else (
      last_err := now ;
      err_count := 0
    )

let read_well_known
      from sersize_of_tuple time_of_tuple factors_of_tuple serialize_tuple
      unserialize_tuple ringbuf_envvar worker_time_of_tuple
      orc_make_handler orc_write orc_close =
  let conf = C.make_conf () in
  let get_binocle_tuple () =
    get_binocle_tuple conf None None None in
  worker_start conf get_binocle_tuple
               time_of_tuple factors_of_tuple
               serialize_tuple sersize_of_tuple
               orc_make_handler orc_write orc_close
               (fun publish_stats outputer ->
    let bname =
      N.path (getenv ~def:"/tmp/ringbuf_in_report.r" ringbuf_envvar) in
    let globs = List.map Globs.compile from in
    let match_from worker =
      from = [] ||
      List.exists (fun g -> Globs.matches g worker) globs
    in
    let while_ () =
      may_publish_stats conf publish_stats ;
      not_quit () in
    let start = Unix.gettimeofday () in
    let rec loop last_seq =
      if while_ () then (
        let rb = RingBuf.load bname in
        let st = RingBuf.stats rb in
        if st.first_seq <= last_seq then (
          Unix.sleepf (1. +. Random.float 1.) ;
          loop last_seq
        ) else (
          info_or_test conf "Reading buffer..." ;
          RingBufLib.read_buf ~while_ ~delay_rec:Stats.sleep_in rb () (fun () tx ->
            let open RingBufLib in
            (match read_message_header tx 0 with
            | exception e ->
                log_rb_error tx e
            | DataTuple chan as m ->
                let offs = message_header_sersize m in
                let tuple = unserialize_tuple tx offs in
                let worker, time = worker_time_of_tuple tuple in
                (* Filter by time and worker *)
                if time >= start && match_from worker then (
                  CodeGenLib.on_each_input_pre () ;
                  IntCounter.inc Stats.in_tuple_count ;
                  outputer (RingBufLib.DataTuple chan) (Some tuple))
            | _ ->
                ()) ;
            (), true) ;
          info_or_test conf "Done reading buffer, waiting for next one." ;
          RingBuf.unload rb ;
          loop st.first_seq
        )
      ) in
    loop ~-1)

(*
 * Operations that funcs may run: aggregate operation.
 *
 * Arguably ramen's core: this is where most data processing takes place.
 *
 * Roughly, here is what happens in the following function:
 *
 * First, there is one input ringbuf where tuples are read from one by one.
 *
 * Then, a first filter is applied (that does not require the group key).
 *
 * Then, the group key is computed and the group retrieved (or created).
 *
 * Then, there is a second filter (that does require the group).
 *
 * Then, the group (and global) states are updated according to the select
 * expression and a _minimal_version_ of the output tuple computed. The state
 * of functions used to compute this minimal out tuple might be updated in a
 * non revertible way (not every functions can be made revertible or its state
 * persistent).
 *
 * Then, if the tuple must be committed, then the output tuple _generator_ is
 * computed, and executed, and all generated tuples are sent to the children.
 *
 * The _minimal_version_ of a tuple is the minimal subset of fields from the
 * output tuple that are required to update the states. Hopefully the most
 * expensive finalizers are only executed when the output is committed.
 *
 * The output _generator_ is for most purposes _the_ output tuple, but fields
 * that are generators are not yet replaced with the generated Cartesian
 * product of their values.
 *)

let notify rb (site : N.site) (worker : N.fq) event_time (name, parameters) =
  let firing, certainty, parameters =
    RingBufLib.normalize_notif_parameters parameters in
  let parameters = Array.of_list parameters in
  IntCounter.inc (if firing |? true then Stats.firing_notif_count
                                    else Stats.extinguished_notif_count) ;
  RingBufLib.write_notif ~delay_rec:Stats.sleep_out rb
    ((site :> string), (worker :> string), !CodeGenLib.now, event_time,
     name, firing, certainty, parameters)

type ('key, 'local_state, 'tuple_in, 'minimal_out, 'group_order) group =
  { (* The key value of this group: *)
    key : 'key ;
    (* used to compute the actual selected field when outputing the
     * aggregate: *)
    mutable first_in : 'tuple_in ; (* first in-tuple of this aggregate *)
    mutable last_in : 'tuple_in ; (* last in-tuple of this aggregate. *)
    (* minimal_out is a subset of the 'generator_out tuple, with only
     * those fields required for commit_cond and update_states.
     * Alternatively, we could have merely the (non-required) stateful
     * function blanked out, so that building the generator_out would only
     * require the group states and this minimal_out, but then what to do of
     * expressions such as "SELECT in.foo + 95th percentile bar"? *)
    mutable current_out : 'minimal_out ;
    mutable previous_out : 'minimal_out option ;
    (* The record of aggregation values aka the group or local state: *)
    mutable local_state : 'local_state ;
    (* The current value for the second operand of commit_cond0, if in
     * use: *)
    mutable g0 : 'group_order option
  }

(* WARNING: increase RamenVersions.worker_state whenever this record is
 * changed. *)
type ('key, 'local_state, 'tuple_in, 'minimal_out, 'generator_out, 'global_state, 'sort_key, 'group_order) aggr_persist_state =
  { mutable last_out_tuple : 'generator_out nullable ; (* last committed tuple generator *)
    global_state : 'global_state ;
    (* The hash of all groups: *)
    mutable groups :
      ('key, ('key, 'local_state, 'tuple_in, 'minimal_out, 'group_order) group) Hashtbl.t ;
    (* The optional heap of groups used to speed up commit condition checks
     * on all groups: *)
    mutable groups_heap :
      ('key, 'local_state, 'tuple_in, 'minimal_out, 'group_order) group Heap.t ;
    (* Input sort buffer and related tuples: *)
    mutable sort_buf : ('sort_key, 'tuple_in) SortBuf.t ;
    (* We have one such state per channel, that we timeout when a
     * channel is unseen for too long: *)
    mutable last_used : float }

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

(* [on_tup] is the continuation for tuples while [on_else] is the
 * continuation for non tuples: *)
let read_single_rb conf ?while_ ?delay_rec read_tuple rb_in publish_stats
                   on_tup on_else =
  let while_ () =
    may_publish_stats conf publish_stats ;
    match while_ with Some f -> f () | None -> true in
  RingBufLib.read_ringbuf ~while_ ?delay_rec rb_in (fun tx ->
    match read_tuple tx with
    | exception e ->
        log_rb_error tx e ;
        RingBuf.dequeue_commit tx
    | msg ->
        let tx_size = RingBuf.tx_size tx in
        RingBuf.dequeue_commit tx ;
        (match msg with
        | RingBufLib.DataTuple chan, Some tuple ->
            on_tup tx_size chan tuple
        | head, None -> on_else head
        | _ -> assert false))

let yield_every conf ~while_ read_tuple every publish_stats on_tup on_else =
  let tx = RingBuf.bytes_tx 0 in
  let rec loop prev_start =
    if while_ () then (
      may_publish_stats conf publish_stats ;
      let start =
        match read_tuple tx with
        | exception e ->
            print_exception ~what:"yielding a tuple" e ;
            prev_start
        | RingBufLib.DataTuple chan, Some tuple ->
            let s = Unix.gettimeofday () in
            on_tup 0 chan tuple ;
            s
        | head, None ->
            on_else head ;
            prev_start
        | _ -> assert false in
      let rec wait () =
        (* Avoid sleeping longer than a few seconds to check while_ in a
         * timely fashion. The 1.33 is supposed to help distinguish this sleep
         * from the many others when using strace: *)
        let sleep_time =
          min 1.33
          ((start +. (every |? 0.)) -. Unix.gettimeofday ()) in
        let keep_going = while_ () in
        if sleep_time > 0. && keep_going then (
          !logger.debug "Sleeping for %f seconds" sleep_time ;
          Unix.sleepf sleep_time ;
          wait ()
        ) else loop start in
      wait ()
    ) in
  loop 0.

let aggregate
      (read_tuple : RingBuf.tx -> RingBufLib.message_header * 'tuple_in option)
      (sersize_of_tuple : FieldMask.fieldmask -> 'tuple_out -> int)
      (time_of_tuple : 'tuple_out -> (float * float) option)
      (factors_of_tuple : 'tuple_out -> (string * T.value) array)
      (serialize_tuple :
        FieldMask.fieldmask -> RingBuf.tx -> int -> 'tuple_out -> int)
      (generate_tuples :
        (Channel.t -> 'tuple_in -> 'tuple_out -> unit) -> Channel.t -> 'tuple_in -> 'generator_out -> unit)
      (* Build as few fields from out_tuple as possible, just enough to
       * evaluate the commit_cond. The idea here is that we do not want to
       * finalize a potentially expensive function for every input (unless
       * its current value is used in commit_cond).
       * This function also updates the stateful functions required for those
       * fields. *)
      (minimal_tuple_of_aggr :
        'tuple_in -> (* current input *)
        'generator_out nullable -> (* last_out *)
        'local_state -> 'global_state -> 'minimal_out)
      (* Update the states for all other fields. *)
      (update_states :
        'tuple_in -> (* current input *)
        'generator_out nullable -> (* last_out *)
        'local_state -> 'global_state -> 'minimal_out -> unit)
      (* Build the generator_out tuple from the minimal_out and all the same
       * parameters as passed to minimal_tuple_of_aggr, all of which must be
       * saved in the group so it's posible for this group to be committed
       * later when another unrelated incoming tuple is handled into another
       * group. *)
      (out_tuple_of_minimal_tuple :
        'tuple_in -> (* current input *)
        'generator_out nullable -> (* last_out *)
        'local_state -> 'global_state -> 'minimal_out -> 'generator_out)
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
        'generator_out nullable -> (* previous.out *)
        bool)
      (where_slow :
        'global_state ->
        'tuple_in -> (* current input *)
        'generator_out nullable -> (* previous.out *)
        'local_state ->
        bool)
      (key_of_input : 'tuple_in -> 'key)
      (is_single_key : bool)
      (* commit_cond needs the local and global states for it can use stateful
       * functions on its own. It will not access the state of functions used
       * to compute output fields, of course, so it's safe even in case of
       * 'commit before'. *)
      (commit_cond :
        'tuple_in -> (* current input *)
        'generator_out nullable -> (* out_last *)
        'local_state ->
        'global_state ->
        'minimal_out -> (* current minimal out *)
        bool)
      (* Optional optimisation: groups can be ordered by group_order, and
       * then an additional commit condition is that [commit_cond_in] must
       * yield a greater (or equal) value than the value according to which
       * groups are ordered. Makes it possible to check a commit condition
       * on many groups quickly: *)
      (commit_cond0 :
        (
          (* Returns the value of the first operand: *)
          ('tuple_in -> 'global_state -> 'group_order) *
          (* Returns the value of the second operand: *)
          ('minimal_out -> 'generator_out nullable -> 'local_state ->
           'global_state -> 'group_order) *
          (* Compare two such values: *)
          ('group_order -> 'group_order -> int) *
          (* True if the commit condition is true even when equals: *)
          bool
        ) option)
      (commit_before : bool)
      (do_flush : bool)
      (check_commit_for_all : bool)
      (global_state : unit -> 'global_state)
      (group_init : 'global_state -> 'local_state)
      (get_notifications :
        'tuple_in -> 'tuple_out -> (string * (string * string) list) list)
      (every : float option)
      orc_make_handler orc_write orc_close =
  let conf = C.make_conf () in
  let cmp_g0 cmp g1 g2 =
    cmp (option_get "g0" __LOC__ g1.g0) (option_get "g0" __LOC__ g2.g0) in
  IntGauge.set Stats.group_count 0 ;
  let get_binocle_tuple () =
    let si v = Some (Uint64.of_int v) in
    let i v = Option.map (fun r -> Uint64.of_int r) v in
    get_binocle_tuple
      conf
      (IntCounter.get Stats.in_tuple_count |> si)
      (IntCounter.get Stats.selected_tuple_count |> si)
      (IntGauge.get Stats.group_count |> Option.map Stats.gauge_current |> i) in
  worker_start conf get_binocle_tuple
               time_of_tuple factors_of_tuple
               serialize_tuple sersize_of_tuple
               orc_make_handler orc_write orc_close
               (fun publish_stats msg_outputer ->
    let rb_in_fname =
      try Some (Sys.getenv "input_ringbuf" |> N.path)
      with Not_found -> None
    and notify_rb_name =
      N.path (getenv ~def:"/tmp/ringbuf_notify.r" "notify_ringbuf") in
    let notify_rb = RingBuf.load notify_rb_name in
    let outputer =
      (* tuple_in is useful for generators and text expansion: *)
      let do_out chan tuple_in tuple_out =
        let notifications =
          if chan = Channel.live then
            get_notifications tuple_in tuple_out
          else [] in
        if notifications <> [] then (
          let event_time = time_of_tuple tuple_out |> Option.map fst in
          List.iter
            (notify notify_rb conf.C.site conf.fq event_time) notifications
        ) ;
        msg_outputer (RingBufLib.DataTuple chan) (Some tuple_out)
      in
      generate_tuples do_out in
    let with_state =
      let open State.Persistent in
      let init_state () =
        { last_out_tuple = Null ;
          global_state = global_state () ;
          groups =
            (* Try to make the state as small as possible: *)
            Hashtbl.create (if is_single_key then 1 else 701) ;
          groups_heap = Heap.empty ;
          sort_buf = SortBuf.empty ;
          last_used = Unix.time () } in
      let live_state =
        ref (make conf.state_file (init_state ())) in
      (* Non live states: *)
      let states = Hashtbl.create 103 in
      let get_state chn =
        if chn = Channel.live then
          restore !live_state
        else
          match Hashtbl.find states chn with
          | exception Not_found ->
              let st = init_state () in
              Hashtbl.add states chn st ;
              st
          | st -> st
      and save_state chn st =
        if chn = Channel.live then
          live_state := save ~save_every:100000 ~save_timeout:31. !live_state st
        else
          Hashtbl.replace states chn st
      in
      fun chn f ->
        let s = get_state chn in
        let s' = f s in
        save_state chn s' (* TODO: have a mutable state and check s==s'? *)
    in
    !logger.debug "Will read ringbuffer %a"
      (Option.print N.path_print) rb_in_fname ;
    let rb_in =
      Option.map (fun fname ->
        let on _ =
          ignore (Gc.major_slice 0) ;
          true in
        retry ~on ~min_delay:1.0 RingBuf.load fname
      ) rb_in_fname
    in
    (* The big function that aggregate a single tuple *)
    let aggregate_one channel_id s in_tuple =
      (* Define some short-hand values and functions we will keep
       * referring to: *)
      (* When committing other groups, this is used to skip the current
       * groupif it has been sent already: *)
      let already_output_aggr = ref None in
      let already_output g =
        Option.map_default ((==) g) false !already_output_aggr in
      (* Tells if the group must be committed/flushed: *)
      let must_commit g =
        Option.map_default (fun (f0, _, cmp, eq) ->
          let f0 = f0 in_tuple s.global_state in
          let c = cmp f0 (option_get "g0" __LOC__ g.g0) in
          c > 0 || c = 0 && eq
        ) true commit_cond0 &&
        commit_cond in_tuple s.last_out_tuple g.local_state
                    s.global_state g.current_out in
      let may_relocate_group_in_heap g =
        Option.may (fun (_, g0, cmp, _) ->
          let g0 = g0 g.current_out s.last_out_tuple g.local_state
                      s.global_state in
          if g.g0 <> Some g0 then (
            (* Relocate that group in the heap: *)
            IntCounter.inc Stats.relocated_groups ;
            let cmp = cmp_g0 cmp in
            s.groups_heap <- Heap.rem_phys cmp g s.groups_heap ;
            (* Now that it's no longer in the heap, its g0 can be updated: *)
            g.g0 <- Some g0 ;
            s.groups_heap <- Heap.add cmp g s.groups_heap
          )
        ) commit_cond0 in
      let may_rem_group_from_heap g =
        Option.may (fun (_, _, cmp, _) ->
          let cmp = cmp_g0 cmp in
          s.groups_heap <- Heap.rem_phys cmp g s.groups_heap
        ) commit_cond0 in
      let finalize_out g =
        (* Output the tuple *)
        match commit_before, g.previous_out with
        | false, _ ->
            let out =
              out_tuple_of_minimal_tuple
                g.last_in s.last_out_tuple g.local_state s.global_state
                g.current_out in
            s.last_out_tuple <- NotNull out ;
            Some out
        | true, None -> None
        | true, Some previous_out ->
            let out =
              out_tuple_of_minimal_tuple
                g.last_in s.last_out_tuple g.local_state s.global_state
                previous_out in
            s.last_out_tuple <- NotNull out ;
            Some out
      and flush g =
        (* Flush/Keep/Slide *)
        if commit_before then (
          (* Note that when "committing before" groups never disappear. *)
          (* Restore the group as if this tuple were the first and only
           * one: *)
          g.first_in <- g.last_in ;
          g.previous_out <- None ;
          (* We cannot rewind the global state, but the local state we
           * can: for other fields than minimum-out we can reset, and
           * for the states owned by minimum-out, where_slow and the
           * commit condition we can replay (TODO): *)
          g.local_state <- group_init s.global_state ;
          may_relocate_group_in_heap g
        ) else (
          Hashtbl.remove s.groups g.key
          (* g Has been removed from the heap already, in theory *)
        )
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
      let perf = ref (Perf.start ()) in
      let aggr_opt =
        (* maybe the key and group that has been updated: *)
        if where_fast s.global_state in_tuple s.last_out_tuple
        then (
          perf := Perf.add_and_transfer Stats.perf_where_fast !perf ;
          (* 2. Retrieve the group *)
          if channel_id = Channel.live then
            IntGauge.set Stats.group_count (Hashtbl.length s.groups) ;
          let k = key_of_input in_tuple in
          (* Update/create the group if it passes where_slow. *)
          match Hashtbl.find s.groups k with
          | exception Not_found ->
            (* The group does not exist for that key. *)
            let local_state = group_init s.global_state in
            perf := Perf.add_and_transfer Stats.perf_find_group !perf ;
            (* 3. Filtering (slow path) - for new group *)
            if where_slow s.global_state in_tuple s.last_out_tuple local_state
            then (
              perf := Perf.add_and_transfer Stats.perf_where_slow !perf ;
              (* 4. Compute new minimal_out (and new group) *)
              let current_out =
                minimal_tuple_of_aggr
                  in_tuple s.last_out_tuple local_state s.global_state in
              let g = {
                key = k ;
                first_in = in_tuple ;
                last_in = in_tuple ;
                current_out ;
                previous_out = None ;
                local_state ;
                g0 =
                  Option.map (fun (_, g0, _, _) ->
                    g0 current_out Null local_state s.global_state
                  ) commit_cond0 } in
              (* Adding this group: *)
              Hashtbl.add s.groups k g ;
              Option.may (fun (_, _, cmp, _) ->
                let cmp = cmp_g0 cmp in
                s.groups_heap <- Heap.add cmp g s.groups_heap
              ) commit_cond0 ;
              perf := Perf.add_and_transfer Stats.perf_update_group !perf ;
              Some g
            ) else ( (* in-tuple does not pass where_slow *)
              perf := Perf.add_and_transfer Stats.perf_where_slow !perf ;
              None
            )
          | g ->
            (* The group already exists. *)
            perf := Perf.add_and_transfer Stats.perf_find_group !perf ;
            (* 3. Filtering (slow path) - for existing group *)
            if where_slow s.global_state in_tuple s.last_out_tuple g.local_state
            then (
              (* 4. Compute new current_out (and update the group) *)
              perf := Perf.add_and_transfer Stats.perf_where_slow !perf ;
              (* current_out and last_in are better updated only after we called the
               * various clauses receiving g *)
              g.last_in <- in_tuple ;
              g.previous_out <- Some g.current_out ;
              g.current_out <-
                minimal_tuple_of_aggr
                  g.last_in s.last_out_tuple g.local_state s.global_state ;
              may_relocate_group_in_heap g ;
              perf := Perf.add_and_transfer Stats.perf_update_group !perf ;
              Some g
            ) else ( (* in-tuple does not pass where_slow *)
              perf := Perf.add_and_transfer Stats.perf_where_slow !perf ;
              None
            )
          ) else ( (* in-tuple does not pass where_fast *)
            perf := Perf.add_and_transfer Stats.perf_where_fast !perf ;
            None
          ) in
      (match aggr_opt with
      | Some g ->
        (* 5. Post-condition to commit and flush *)
        if channel_id = Channel.live then
          IntCounter.inc Stats.selected_tuple_count ;
        if not commit_before then
          update_states g.last_in s.last_out_tuple
                        g.local_state s.global_state g.current_out ;
        if must_commit g then (
          already_output_aggr := Some g ;
          Option.may (outputer channel_id in_tuple) (finalize_out g) ;
          if do_flush then flush g ;
          if do_flush && not commit_before then
            may_rem_group_from_heap g
        ) ;
        if commit_before then
          update_states g.last_in s.last_out_tuple
                        g.local_state s.global_state g.current_out
      | None -> () (* in_tuple failed filtering *)) ;
      perf := Perf.add_and_transfer Stats.perf_commit_incoming !perf ;
      (* Now there is also the possibility that we need to commit or flush
       * for every single input tuple :-< *)
      if check_commit_for_all then (
        (* FIXME: prevent commit_before in that case *)
        let to_commit =
          (* FIXME: What if commit-when update the global state? We are
           * going to update it several times here. We should prevent this
           * clause to access the global state. *)
          match commit_cond0 with
          | Some (f0, _, cmp, eq) ->
              let f0 = f0 in_tuple s.global_state in
              let to_commit, heap =
                Heap.collect (cmp_g0 cmp) (fun g ->
                  let g0 = option_get "g0" __LOC__ g.g0 in
                  let c = cmp f0 g0 in
                  if c > 0 || c = 0 && eq then (
                    (* Or it's been removed from the heap already: *)
                    assert (not (already_output g)) ;
                    if commit_cond in_tuple s.last_out_tuple g.local_state
                                   s.global_state g.current_out
                    then Heap.Collect
                    else Heap.Keep
                  ) else
                    (* No way we will find another true pre-condition *)
                    Heap.KeepAll
                ) s.groups_heap in
              s.groups_heap <- heap ;
              to_commit
          | None ->
              Hashtbl.fold (fun _ g to_commit ->
                if not (already_output g) &&
                   commit_cond in_tuple s.last_out_tuple g.local_state
                               s.global_state g.current_out
                then g :: to_commit else to_commit
              ) s.groups [] in
        (* FIXME: use the channel_id as a label! *)
        perf := Perf.add_and_transfer Stats.perf_select_others !perf ;
        let outs = List.filter_map finalize_out to_commit in
        perf := Perf.add_and_transfer Stats.perf_finalize_others !perf ;
        List.iter (outputer channel_id in_tuple) outs ;
        perf := Perf.add_and_transfer Stats.perf_commit_others !perf ;
        if do_flush then List.iter flush to_commit ;
        Perf.add Stats.perf_flush_others (Perf.stop !perf)
      ) ;
      s
    in
    (* The event loop: *)
    let rate_limit_log_reads = rate_limiter 1 1. in
    let tuple_reader =
      match rb_in with
      | None -> (* yield expression *)
          yield_every conf ~while_:not_quit read_tuple every publish_stats
      | Some rb_in ->
          read_single_rb conf ~while_:not_quit
                         ~delay_rec:Stats.sleep_in read_tuple rb_in
                         publish_stats
    and on_tup tx_size channel_id in_tuple =
      let perf_per_tuple = Perf.start () in
      if channel_id <> Channel.live && rate_limit_log_reads () then
        !logger.debug "Read a tuple from channel %a"
          Channel.print channel_id ;
      with_state channel_id (fun s ->
        (* Set CodeGenLib.now: *)
        CodeGenLib.on_each_input_pre () ;
        (* Update per in-tuple stats *)
        if channel_id = Channel.live then (
          IntCounter.inc Stats.in_tuple_count ;
          IntCounter.add Stats.read_bytes tx_size) ;
        (* Sort: add in_tuple into the heap of sorted tuples, update
         * smallest/greatest, and consider extracting the smallest. *)
        (* If we assume sort_last >= 2 then the sort buffer will never
         * be empty but for the very last tuple. In that case pretend
         * tuple_in is the first (sort.#count will still be 0). *)
        if sort_last <= 1 then
          aggregate_one channel_id s in_tuple
        else (
          let sort_n = SortBuf.length s.sort_buf in
          let or_in f =
            try f s.sort_buf with Invalid_argument _ -> in_tuple in
          let sort_key =
            sort_by (Uint64.of_int sort_n)
              (or_in SortBuf.first) in_tuple
              (or_in SortBuf.smallest) (or_in SortBuf.greatest) in
          s.sort_buf <- SortBuf.add sort_key in_tuple s.sort_buf ;
          let sort_n = sort_n + 1 in
          if sort_n >= sort_last ||
             sort_until (Uint64.of_int sort_n)
              (or_in SortBuf.first) in_tuple
              (or_in SortBuf.smallest) (or_in SortBuf.greatest)
          then (
            let min_in, sb = SortBuf.pop_min s.sort_buf in
            s.sort_buf <- sb ;
            aggregate_one channel_id s min_in
          ) else s)) ;
        if channel_id = Channel.live then
          Perf.add Stats.perf_per_tuple (Perf.stop perf_per_tuple) ;
    and on_else head =
      msg_outputer head None
    in
    tuple_reader on_tup on_else)

type tunneld_dest = { host : N.host ; port : int ; parent_num : int }

(* Simplified version of [aggregate] that performs only the where filter: *)
let top_half
      (read_tuple : RingBuf.tx -> RingBufLib.message_header * 'tuple_in option)
      (where : 'tuple_in ->  bool) =
  let conf = C.make_conf ~is_top_half:true () in
  let tunnelds =
    let hosts = getenv_list "tunneld_host" N.host
    and ports = getenv_list "tunneld_port" int_of_string
    and pnums = getenv_list "parent_num" int_of_string in
    list_revmap_3 (fun host port parent_num ->
      { host ; port ; parent_num }
    ) hosts ports pnums in
  let get_binocle_tuple () =
    let si v = Some (Uint64.of_int v) in
    get_binocle_tuple
      conf
      (IntCounter.get Stats.in_tuple_count |> si)
      (IntCounter.get Stats.selected_tuple_count |> si)
      None in
  let time_of_tuple _ = assert false in
  let factors_of_tuple _ = assert false in
  let serialize_tuple _ _ _ _ = assert false in
  let sersize_of_tuple _ = assert false in
  worker_start conf get_binocle_tuple
               time_of_tuple factors_of_tuple
               serialize_tuple sersize_of_tuple
               ignore5 ignore4 ignore1
               (fun publish_stats _outputer ->
    let rb_in_fname = N.path (getenv "input_ringbuf") in
    !logger.debug "Will read ringbuffer %a" N.path_print rb_in_fname ;
    let forwarders =
      List.map (fun t ->
        RamenCopyClt.copy_client
          conf.site t.host t.port conf.C.fq t.parent_num
      ) tunnelds in
    let forward_bytes b =
      !logger.debug "Forwarding %d bytes to tunneld" (Bytes.length b) ;
      List.iter (fun forwarder -> forwarder b) forwarders in
    let rb_in =
      let on _ =
        ignore (Gc.major_slice 0) ;
        true in
      retry ~on ~min_delay:1.0 RingBuf.load rb_in_fname
    in
    let rate_limit_log_reads = rate_limiter 1 1. in
    let on_tup tx tx_size channel_id in_tuple =
      if channel_id <> Channel.live && rate_limit_log_reads () then
        !logger.debug "Read a tuple from channel %a"
          Channel.print channel_id ;
      (* Set CodeGenLib.now: *)
      CodeGenLib.on_each_input_pre () ;
      (* Update per in-tuple stats *)
      if channel_id = Channel.live then (
        IntCounter.inc Stats.in_tuple_count ;
        IntCounter.add Stats.read_bytes tx_size ;
        IntCounter.inc Stats.out_tuple_count ;
        FloatGauge.set Stats.last_out !CodeGenLib.now) ;
      let perf = Perf.start () in
      let pass = where in_tuple in
      Perf.add Stats.perf_where_fast (Perf.stop perf) ;
      if pass then Some (RingBuf.read_raw_tx tx) else None
    in
    !logger.debug "Starting forwarding loop..." ;
    let while_ () =
      may_publish_stats conf publish_stats ;
      not_quit () in
    RingBufLib.read_ringbuf ~while_ ~delay_rec:Stats.sleep_in rb_in (fun tx ->
      match read_tuple tx with
      | exception e ->
          log_rb_error tx e ;
          RingBuf.dequeue_commit tx
      | msg ->
          let perf_per_tuple = Perf.start () in
          let tx_size = RingBuf.tx_size tx in
          let chan, to_forward =
            match msg with
            | RingBufLib.DataTuple chan, Some tuple ->
                Some chan, on_tup tx tx_size chan tuple
            | _, None ->
                None, Some (RingBuf.read_raw_tx tx)
            | _ -> assert false in
          RingBuf.dequeue_commit tx ;
          IntCounter.add Stats.write_bytes
            (Option.map Bytes.length to_forward |? 0) ;
          Option.may forward_bytes to_forward ;
          if chan = Some Channel.live then
            Perf.add Stats.perf_per_tuple (Perf.stop perf_per_tuple)))

let read_whole_archive ?at_exit ?(while_=always) read_tuplez rb k =
  if while_ () then
    RingBufLib.(read_buf ~wait_for_more:false ~while_ rb () (fun () tx ->
      match read_tuplez tx with
      | exception e ->
          log_rb_error ?at_exit tx e ;
          (), false (* Skip the rest of that file for safety *)
      | DataTuple chn, Some tuple when chn = Channel.live ->
          k tuple, true
      | DataTuple chn, _ ->
          (* This should not happen as we archive only the live channel: *)
          !logger.warning "Read a tuple from channel %a in archive?"
            Channel.print chn ;
          (), true
      | _ -> (), true))

(* Special node that reads the output history instead of computing it.
 * Takes from the env the ringbuf location and the since/until dates to
 * replay, as well as the channel id. Then it must follow the instructions
 * in the same out_ref than the normal worker to inject those tuples in the
 * normal worker tree.
 * We need to properly deserialize and reserialize each tuples (instead of
 * merely copy them) as we must write only the required subset of fields.
 * Other differences:
 * - it emits no reports (or of course notifications);
 * - it has no state therefore no persistence;
 * - it is quieter than a normal worker that has its own log file. *)
let replay
      (read_tuple : RingBuf.tx -> RingBufLib.message_header * 'tuple_out option)
      (sersize_of_tuple : FieldMask.fieldmask -> 'tuple_out -> int)
      (time_of_tuple : 'tuple_out -> (float * float) option)
      (factors_of_tuple : 'tuple_out -> (string * T.value) array)
      (serialize_tuple : FieldMask.fieldmask -> RingBuf.tx -> int -> 'tuple_out -> int)
      orc_make_handler orc_write orc_read orc_close =
  Files.reset_process_name () ;
  let conf = C.make_conf ~is_replayer:true () in
  let rb_archive =
    N.path (getenv ~def:"/tmp/archive.b" "rb_archive")
  and since = getenv "since" |> float_of_string
  and until = getenv "until" |> float_of_string
  and channel_ids = getenv "channel_ids" |>
                    string_split_on_char ',' |>
                    List.map Channel.of_string
  and replayer_id = getenv "replayer_id" |> int_of_string
  in
  !logger.debug "Starting REPLAY of %a. Will log into %s at level %s."
    N.fq_print conf.C.fq
    (string_of_log_output !logger.output)
    (string_of_log_level conf.log_level) ;
  (* TODO: also factorize *)
  set_signals Sys.[sigterm; sigint] (Signal_handle (fun s ->
    !logger.debug "Received signal %s" (name_of_signal s) ;
    quit :=
      Some (if s = Sys.sigterm then ExitCodes.terminated
                               else ExitCodes.interrupted))) ;
  (* Ignore sigusr1: *)
  set_signals Sys.[sigusr1] Signal_ignore ;
  !logger.debug "Will replay archive from %a"
    N.path_print_quoted rb_archive ;
  let num_replayed_tuples = ref 0 in
  let _publish_stats, outputer =
    Publish.start_zmq_client conf ~while_:not_quit
                             time_of_tuple factors_of_tuple
                             serialize_tuple sersize_of_tuple
                             orc_make_handler orc_write orc_close in
  let dir = RingBufLib.arc_dir_of_bname rb_archive in
  let files = RingBufLib.arc_files_of dir in
  let time_overlap t1 t2 = since < t2 && until >= t1 in
  let at_exit () =
    (* TODO: it would be nice to send an error code with the EndOfReplay
     * so that the client would know if everything was alright. *)
    (* Before quitting (normally or because of too many errors), signal
     * the end of this replay: *)
    List.iter (fun channel_id ->
      outputer (RingBufLib.EndOfReplay (channel_id, replayer_id)) None
    ) channel_ids ;
    (* Also wait for the ZMQ command queue to be empty: *)
    if !quit = None then quit := Some 0 ; (* Will end Publish.async_thread *)
    Publish.stop () in
  let output_tuple tuple =
    CodeGenLib.on_each_input_pre () ;
    incr num_replayed_tuples ;
    (* As tuples are not ordered in the archive file we have
     * to read it all: *)
    List.iter (fun channel_id ->
      outputer (RingBufLib.DataTuple channel_id) (Some tuple)
    ) channel_ids in
  let loop_tuples rb =
    read_whole_archive ~at_exit ~while_:not_quit read_tuple rb output_tuple in
  let loop_tuples_of_ringbuf fname =
    !logger.debug "Reading archive %a" N.path_print_quoted fname ;
    match RingBuf.load fname with
    | exception e ->
        let what = "Reading archive "^ (fname :> string) in
        print_exception ~what e
    | rb ->
        finally (fun () -> RingBuf.unload rb) (fun () ->
          let st = RingBuf.stats rb in
          if time_overlap st.t_min st.t_max then
            loop_tuples rb
          else
            !logger.debug "Archive times of %a (%f..%f) does not overlap \
                           with search (%f..%f)"
              N.path_print_quoted rb_archive
              st.t_min st.t_max since until) () in
  let rec loop_files () =
    if not_quit () then
      match Enum.get_exn files with
      | exception Enum.No_more_elements -> ()
      | _s1, _s2, t1, t2, arc_typ, fname ->
          if time_overlap t1 t2 then (
            match arc_typ with
            | RingBufLib.RingBuf ->
                loop_tuples_of_ringbuf fname
            | RingBufLib.Orc ->
                let num_lines, num_errs =
                  orc_read fname Default.orc_rows_per_batch output_tuple in
                if num_errs <> 0 then
                  !logger.error "%d/%d errors" num_errs num_lines) ;
          loop_files ()
  in
  !logger.debug "Reading the past archives..." ;
  loop_files () ;
  (* Finish with the current archive: *)
  !logger.debug "Reading current archive" ;
  loop_tuples_of_ringbuf rb_archive ;
  at_exit () ;
  !logger.info
    "Finished after having replayed %d tuples for channels %a"
    !num_replayed_tuples
    (pretty_list_print Channel.print) channel_ids ;
  exit (!quit |? ExitCodes.terminated)


let convert
      in_fmt (in_fname : N.path) out_fmt (out_fname : N.path)
      orc_read csv_write orc_make_handler orc_write orc_close
      (read_tuple : RingBuf.tx -> RingBufLib.message_header * 'tuple_out option)
      (sersize_of_tuple : FieldMask.fieldmask -> 'tuple_out -> int)
      (time_of_tuple : 'tuple_out -> (float * float) option)
      (serialize_tuple : FieldMask.fieldmask -> RingBuf.tx -> int -> 'tuple_out -> int)
      tuple_of_strings =
  let log_level = getenv ~def:"normal" "log_level" |> log_level_of_string in
  (match getenv "log" with
  | exception _ ->
      init_logger log_level
  | logdir ->
      if logdir = "syslog" then
        init_syslog log_level
      else (
        Files.mkdir_all (N.path logdir) ;
        init_logger ~logdir log_level
      )) ;
  !logger.debug "Going to convert from %s to %s"
    (Casing.string_of_format in_fmt)
    (Casing.string_of_format out_fmt) ;
  let open Unix in
  if in_fname = out_fname then
    failwith "Input and output files must be distinct" ;
  let orc_handler = ref None
  and out_rb = ref None and in_rb = ref None
  and in_fd = ref None and out_fd = ref None
  in
  let reader =
    match in_fmt with
    | Casing.CSV ->
        let fd = openfile (in_fname :> string) [O_RDONLY] 0o644 in
        in_fd := Some fd ;
        (fun k ->
          read_lines fd |>
          Enum.iter (fun line ->
            let consumed, strs =
              strings_of_csv Default.csv_separator true "\\"
                             (Bytes.of_string line) 0 (String.length line) in
            if consumed < String.length line then
              !logger.warning "Consumed only %d bytes over %d"
                consumed (String.length line) ;
            match tuple_of_strings strs with
            | exception e ->
              !logger.error "Cannot parse line %S: %s"
                line (Printexc.to_string e)
            | tuple ->
              k tuple))
    | Casing.ORC ->
        (fun f ->
          let num_lines, num_errs = orc_read in_fname 1000 f in
          if num_errs <> 0 then
            !logger.error "%d/%d errors" num_errs num_lines)
    | Casing.RB ->
        let rb = RingBuf.load in_fname in
        in_rb := Some rb ;
        read_whole_archive read_tuple rb
  and writer =
    match out_fmt with
    | Casing.CSV ->
        let flags = [O_CREAT;O_EXCL;O_WRONLY] in
        let fd = openfile (out_fname :> string) flags 0o644 in
        out_fd := Some fd ;
        csv_write fd
    | Casing.ORC ->
        let with_index = false (* CLI parameters for those *)
        and batch_size = Default.orc_rows_per_batch
        and num_batches = Default.orc_batches_per_file in
        let hdr = orc_make_handler out_fname with_index batch_size
                                   num_batches false in
        orc_handler := Some hdr ;
        (fun tuple ->
          let start_stop = time_of_tuple tuple in
          let start, stop = start_stop |? (0., 0.) in
          orc_write hdr tuple start stop)
    | Casing.RB ->
        RingBuf.create ~wrap:false out_fname ;
        let rb = RingBuf.load out_fname in
        out_rb := Some rb ;
        let head = RingBufLib.DataTuple Channel.live in
        let head_sz = RingBufLib.message_header_sersize head in
        (fun tuple ->
          let start_stop = time_of_tuple tuple in
          let start, stop = start_stop |? (0., 0.) in
          let sz = head_sz
                 + sersize_of_tuple FieldMask.all_fields tuple in
          let tx = RingBuf.enqueue_alloc rb sz in
          RingBufLib.(write_message_header tx 0 head) ;
          let offs =
            serialize_tuple FieldMask.all_fields tx head_sz tuple in
          RingBuf.enqueue_commit tx start stop ;
          assert (offs <= sz))
  in
  reader writer ;
  Option.may RingBuf.unload !out_rb ;
  Option.may RingBuf.unload !in_rb ;
  Option.may orc_close !orc_handler
