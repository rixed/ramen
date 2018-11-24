(* Skeleton of the basic operations (aggregate, read csv...), parameterized
 * by functions that are generated (by CodeGen_OCaml). *)
open Batteries
open Stdint
open RamenLog
open RamenHelpers

(* Health and Stats
 *
 * Each func has to periodically report to ramen http server its health and
 * some stats.
 * Could have been the other way around, and that would have made the
 * connection establishment easier possibly (since we already must be able to
 * ssh to other machines in order to start a func) but we already have an http
 * server on Ramen and probably want to avoid opening too many ports
 * everywhere, and forcing generated funcs to implement too many things.
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
  IntCounter.make RamenConsts.Metric.Names.in_tuple_count
    RamenConsts.Metric.Docs.in_tuple_count

let make_stats_selected_tuple_count () =
  IntCounter.make RamenConsts.Metric.Names.selected_tuple_count
    RamenConsts.Metric.Docs.selected_tuple_count

let stats_out_tuple_count =
  IntCounter.make RamenConsts.Metric.Names.out_tuple_count
    RamenConsts.Metric.Docs.out_tuple_count

let stats_cpu =
  FloatCounter.make RamenConsts.Metric.Names.cpu_time
    RamenConsts.Metric.Docs.cpu_time

let stats_ram =
  IntGauge.make RamenConsts.Metric.Names.ram_usage
    RamenConsts.Metric.Docs.ram_usage

let stats_rb_read_bytes =
  IntCounter.make RamenConsts.Metric.Names.rb_read_bytes
    RamenConsts.Metric.Docs.rb_read_bytes

let stats_rb_write_bytes =
  IntCounter.make RamenConsts.Metric.Names.rb_write_bytes
    RamenConsts.Metric.Docs.rb_write_bytes

let stats_rb_read_sleep_time =
  FloatCounter.make RamenConsts.Metric.Names.rb_wait_read
    RamenConsts.Metric.Docs.rb_wait_read

let stats_rb_write_sleep_time =
  FloatCounter.make RamenConsts.Metric.Names.rb_wait_write
    RamenConsts.Metric.Docs.rb_wait_write

let stats_last_out =
  FloatGauge.make RamenConsts.Metric.Names.last_out
    RamenConsts.Metric.Docs.last_out

let stats_event_time =
  FloatGauge.make RamenConsts.Metric.Names.event_time
    RamenConsts.Metric.Docs.event_time

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

let gauge_current (_mi, x, _ma) = x

let startup_time = Unix.gettimeofday ()

(* Basic tuple without aggregate specific counters: *)
let get_binocle_tuple worker ic sc gc =
  let si v =
    if v < 0 then !logger.error "Negative int counter: %d" v ;
    Some (Uint64.of_int v) in
  let s v = Some v in
  let ram, max_ram =
    match IntGauge.get stats_ram with
    | None -> Uint64.zero, Uint64.zero
    | Some (_mi, x, ma) -> Uint64.of_int x, Uint64.of_int ma in
  let min_event_time, max_event_time =
    match FloatGauge.get stats_event_time with
    | None -> None, None
    | Some (mi, _, ma) -> Some mi, Some ma in
  let time = Unix.gettimeofday () in
  worker, time,
  min_event_time, max_event_time,
  ic, sc,
  IntCounter.get stats_out_tuple_count |> si,
  gc,
  FloatCounter.get stats_cpu,
  (* Assuming we call update_stats before this: *)
  ram, max_ram,
  FloatCounter.get stats_rb_read_sleep_time |> s,
  FloatCounter.get stats_rb_write_sleep_time |> s,
  IntCounter.get stats_rb_read_bytes |> si,
  IntCounter.get stats_rb_write_bytes |> si,
  FloatGauge.get stats_last_out |> Option.map gauge_current,
  startup_time

let send_stats rb (_, time, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _ as tuple) =
  let sersize = RamenBinocle.max_sersize_of_tuple tuple in
  match RingBuf.enqueue_alloc rb sersize with
  | exception RingBuf.NoMoreRoom -> () (* Just skip *)
  | tx ->
    let offs = RamenBinocle.serialize tx tuple in
    RingBuf.enqueue_commit tx time time ;
    assert (offs <= sersize)

let update_stats_rb period rb get_tuple =
  while true do
    update_stats () ;
    let tuple = get_tuple () in
    send_stats rb tuple ;
    Unix.sleepf period
  done

(* Helpers *)

(* For non-wrapping buffers we need to know the value for the time, as
 * the min/max times per slice are saved, along the first/last tuple
 * sequence number. *)
let output rb serialize_tuple sersize_of_tuple start_stop tuple =
  let open RingBuf in
  let sersize = sersize_of_tuple tuple in
  IntCounter.add stats_rb_write_bytes sersize ;
  (* Nodes with no output (but notifications) have no business writing
   * a ringbuf. Want a signal when a notification is sent? SELECT some
   * value! *)
  if sersize > 0 then
    let tx = enqueue_alloc rb sersize in
    let offs = serialize_tuple tx tuple in
    let start, stop = start_stop |? (0., 0.) in
    enqueue_commit tx start stop ;
    assert (offs = sersize)

let quit = ref None

(* Each func can write in several ringbuffers (one per children). This list
 * will change dynamically as children are added/removed. *)
let outputer_of rb_ref_out_fname sersize_of_tuple time_of_tuple
                serialize_tuple =
  let out_h = Hashtbl.create 5 (* Hash from fname to rb*outputer *)
  and out_l = ref []  (* list of outputers *) in
  let get_out_fnames = RingBufLib.out_ringbuf_names rb_ref_out_fname in
  fun tuple ->
    let start_stop = time_of_tuple tuple in
    IntCounter.add stats_out_tuple_count 1 ;
    FloatGauge.set stats_last_out !CodeGenLib_IO.now ;
    (* Update stats_event_time: *)
    Option.may (fun (start, _) ->
      (* We'd rather announce the start time of the event, event for
       * negative durations. *)
      FloatGauge.set stats_event_time start
    ) start_stop ;
    (* Get fnames if they've changed: *)
    let fnames = get_out_fnames () in
    Option.may (fun out_specs ->
      if Hashtbl.is_empty out_specs then (
        if not (Hashtbl.is_empty out_h) then (
          !logger.info "OutRef is now empty!" ;
          Hashtbl.clear out_h)
      ) else (
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
            let tup_serializer =
              serialize_tuple file_spec.RamenOutRef.field_mask
            and tup_sizer =
              sersize_of_tuple file_spec.RamenOutRef.field_mask in
            let last_check_outref = ref 0.
            and last_successful_output = ref (Unix.gettimeofday ())
            and quarantine_until = ref 0.
            and quarantine_delay = ref 0. in
            (* Check that we are still supposed to write in there, but now
             * more frequently than once every 3 secs (how long we are
             * ready to block on a dead child): *)
            let check_outref now =
              if now < !last_check_outref +. 3. then true else (
                last_check_outref := now ;
                RamenOutRef.mem rb_ref_out_fname fname)
            in
            let rb_writer start_stop tuple =
              (* Note: we retry only on NoMoreRoom so that's OK to keep trying; in
               * case the ringbuf disappear altogether because the child is
               * terminated then we won't deadloop.  Also, if one child is full
               * then we will not write to next children until we can eventually
               * write to this one. This is actually desired to have proper message
               * ordering along the stream and avoid ending up with many threads
               * retrying to write to the same child. *)
              retry
                ~while_:(fun () ->
                  if !quit <> None then false else
                  check_outref (Unix.gettimeofday ()))
                ~on:(function
                  | RingBuf.NoMoreRoom ->
                    (* Can't use CodeGenLib_IO.now if we are stuck in output: *)
                    let now = Unix.gettimeofday () in
                    (* Also check from time to time that we are still supposed to
                     * write in there (we check right after the first error to
                     * quickly detect it when a child disappear): *)
                    if not (check_outref now) then false else (
                      if now < !last_successful_output +. 15. then true else (
                        (* At this point, we have been failing for more than 3s
                         * for a child that's still in our out_ref, and should
                         * consider quarantine for a bit: *)
                        quarantine_delay := min 30. (10. +. !quarantine_delay *. 1.5) ;
                        quarantine_until := now +. jitter !quarantine_delay ;
                        !logger.error "Quarantining %s until %s"
                          fname
                          (string_of_time !quarantine_until) ;
                        true))
                  | _ -> false)
                ~first_delay:0.001 ~max_delay:1. ~delay_rec:sleep_out
                (fun () ->
                  if !quarantine_until < !CodeGenLib_IO.now then (
                    output rb tup_serializer tup_sizer start_stop tuple ;
                    if !quarantine_delay > 0. then (
                      last_successful_output := !CodeGenLib_IO.now ;
                      !logger.info "Resuming output to %s" fname ;
                      quarantine_delay := 0.
                    )
                  ) else (
                    !logger.debug "Skipping output to %s (quarantined)" fname
                  )) ()
            in
            Hashtbl.add out_h fname (rb, rb_writer)
        ) to_open ;
      (* Update the current list of outputers: *)
      out_l := Hashtbl.values out_h /@ snd |> List.of_enum) fnames ;
    List.iter (fun out ->
      try out start_stop tuple
      with
        (* It is OK, just skip it. Next tuple we will reread fnames
         * if it has changed. *)
        | RingBuf.NoMoreRoom -> ()
        (* retry_for_ringbuf failing because the recipient is no more in
         * our out_ref: *)
        | Exit -> ()
    ) !out_l

type worker_conf =
  { log_level : log_level ;
    state_file : string ;
    ramen_url : string ;
    is_test : bool }

let info_or_test conf =
  if conf.is_test then !logger.debug else !logger.info

let worker_start worker_name get_binocle_tuple k =
  let log_level = getenv ~def:"normal" "log_level" |> log_level_of_string in
  let default_persist_dir =
    "/tmp/worker_"^ worker_name ^"_"^ string_of_int (Unix.getpid ()) in
  let is_test = getenv ~def:"false" "is_test" |> bool_of_string in
  let state_file = getenv ~def:default_persist_dir "state_file" in
  let ramen_url = getenv ~def:"http://localhost:29380" "ramen_url" in
  let prefix = worker_name ^": " in
  (match getenv "log" with
  | exception _ ->
      init_logger ~prefix log_level
  | logdir ->
      if logdir = "syslog" then
        init_syslog ~prefix log_level
      else (
        mkdir_all logdir ;
        init_logger ~logdir log_level
      )) ;
  let report_period =
    getenv ~def:(string_of_float RamenConsts.Default.report_period)
           "report_period" |> float_of_string in
  let report_rb_fname =
    getenv ~def:"/tmp/ringbuf_in_report.r" "report_ringbuf" in
  let report_rb = RingBuf.load report_rb_fname in
  (* Must call this once before get_binocle_tuple because cpu/ram gauges
   * must not be NULL: *)
  update_stats () ;
  let conf = { log_level ; state_file ; ramen_url ; is_test } in
  info_or_test conf "Starting %s process. Will log into %s at level %s."
    worker_name
    (string_of_log_output !logger.output)
    (string_of_log_level log_level) ;
  set_signals Sys.[sigterm; sigint] (Signal_handle (fun s ->
    info_or_test conf "Received signal %s" (name_of_signal s) ;
    quit :=
      Some (if s = Sys.sigterm then RamenConsts.ExitCodes.terminated
                               else RamenConsts.ExitCodes.interrupted))) ;
  (* Dump stats on sigusr1: *)
  set_signals Sys.[sigusr1] (Signal_handle (fun s ->
    (* This log also useful to rotate the logfile. *)
    !logger.info "Received signal %s, dumping stats"
      (name_of_signal s) ;
    Binocle.display_console ())) ;
  Thread.create (
    restart_on_failure "update_stats_rb"
      (update_stats_rb report_period report_rb)) get_binocle_tuple |>
    ignore ;
  log_exceptions k conf ;
  (* Sending stats for one last time: *)
  ignore_exceptions (send_stats report_rb) (get_binocle_tuple ()) ;
  exit (!quit |? 1)

(*
 * Operations that funcs may run: read a CSV file.
 *)

let read_csv_file filename do_unlink separator sersize_of_tuple
                  time_of_tuple serialize_tuple tuple_of_strings
                  preprocessor field_of_params =
  let worker_name = getenv ~def:"?" "fq_name" in
  let get_binocle_tuple () =
    get_binocle_tuple worker_name None None None in
  worker_start worker_name get_binocle_tuple (fun conf ->
    let rb_ref_out_fname = getenv ~def:"/tmp/ringbuf_out_ref" "output_ringbufs_ref"
    (* For tests, allow to overwrite what's specified in the operation: *)
    and filename = getenv ~def:filename "csv_filename"
    and separator = getenv ~def:separator "csv_separator" in
    let tuples = [ [ "param" ], field_of_params ;
                   [ "env" ], Sys.getenv ] in
    let filename = subst_tuple_fields tuples filename
    and separator = subst_tuple_fields tuples separator
    in
    info_or_test conf "Will read CSV file %S using separator %S"
      filename separator ;
    let of_string line =
      let strings = strings_of_csv separator line in
      tuple_of_strings (Array.of_list strings)
    in
    let outputer =
      outputer_of rb_ref_out_fname sersize_of_tuple time_of_tuple
                  serialize_tuple in
    let while_ () = !quit = None in
    CodeGenLib_IO.read_glob_lines
      ~while_ ~do_unlink filename preprocessor quit (fun line ->
      match of_string line with
      | exception e ->
        !logger.error "Cannot parse line %S: %s"
          line (Printexc.to_string e)
      | tuple -> outputer tuple))

(*
 * Operations that funcs may run: listen to some known protocol.
 *)

let listen_on (collector :
                inet_addr:Unix.inet_addr ->
                port:int ->
                (* We have to specify this one: *)
                ?while_:(unit -> bool) ->
                ('a -> unit) -> unit)
              addr_str port proto_name
              sersize_of_tuple time_of_tuple serialize_tuple =
  let worker_name = getenv ~def:"?" "fq_name" in
  let get_binocle_tuple () =
    get_binocle_tuple worker_name None None None in
  worker_start worker_name get_binocle_tuple (fun conf ->
    let rb_ref_out_fname = getenv ~def:"/tmp/ringbuf_out_ref" "output_ringbufs_ref"
    and inet_addr = Unix.inet_addr_of_string addr_str
    in
    info_or_test conf "Will listen to port %d for incoming %s messages"
      port proto_name ;
    let outputer =
      outputer_of rb_ref_out_fname sersize_of_tuple time_of_tuple
                  serialize_tuple in
    let while_ () = !quit = None in
    collector ~inet_addr ~port ~while_ outputer)

(*
 * Operations that funcs may run: read known tuples from a ringbuf.
 *)

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
    let while_ () = !quit = None in
    let rec loop last_seq =
      let rb = RingBuf.load bname in
      let st = RingBuf.stats rb in
      if st.first_seq <= last_seq then (
        Unix.sleepf (1. +. Random.float 1.) ;
        loop last_seq
      ) else (
        info_or_test conf "Reading buffer..." ;
        RingBufLib.read_buf ~while_ ~delay_rec:sleep_in rb () (fun () tx ->
          let tuple = unserialize_tuple tx in
          let worker, time = worker_time_of_tuple tuple in
          (* Filter by time and worker *)
          if time >= start && match_from worker then
            outputer tuple ;
          (), true) ;
        info_or_test conf "Done reading buffer, waiting for next one." ;
        RingBuf.unload rb ;
        loop st.first_seq
      ) in
    loop ~-1)

(*
 * Operations that funcs may run: aggregate operation.
 *
 * Arguably ramen's core: this is where most data processing takes place.
 *
 * Roughly, here is what happens in the following function:
 *
 * First, there is one (or, in case of merge: several) input ringbufs.
 * where tuples are read from one by one (and optionally, merge-sorted
 * according to some expression into a single input stream).
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

let notify rb worker event_time
           (name, parameters)
           field_of_tuple_in tuple_in
           field_of_tuple_out tuple_out
           field_of_params =
  let tuples =
    [ [ ""; "out" ], field_of_tuple_out tuple_out ;
      [ "in" ], field_of_tuple_in tuple_in ;
      [ "param" ], field_of_params ;
      [ "env" ], Sys.getenv ] in
  let name = subst_tuple_fields tuples name
  and parameters =
    List.map (fun (n, v) -> n, subst_tuple_fields tuples v) parameters in
  let firing, certainty, parameters =
    RingBufLib.normalize_notif_parameters parameters in
  let parameters = Array.of_list parameters in
  RingBufLib.write_notif ~delay_rec:sleep_out rb
    (worker, !CodeGenLib_IO.now, event_time, name, firing, certainty, parameters)

type ('local_state, 'tuple_in, 'minimal_out) group =
  { (* used to compute the actual selected field when outputing the
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
    mutable local_state : 'local_state (* the record of aggregation values aka the group or local state *)
  }

(* WARNING: increase RamenVersions.worker_state whenever this record is
 * changed. *)
type ('key, 'local_state, 'tuple_in, 'minimal_out, 'generator_out, 'global_state, 'sort_key) aggr_persist_state =
  { mutable last_out_tuple : 'generator_out option ; (* last committed tuple generator *)
    global_state : 'global_state ;
    (* The hash of all groups: *)
    mutable groups :
      ('key, ('local_state, 'tuple_in, 'minimal_out) group) Hashtbl.t ;
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
    k tx_size in_tuple in_tuple)

type ('tuple_in, 'merge_on) to_merge =
  { rb : RingBuf.t ;
    mutable tuples : ('tuple_in * int * 'merge_on) RamenSzHeap.t ;
    mutable timed_out : float option (* When it was timed out *) }

let merge_rbs ~while_ ?delay_rec on last timeout read_tuple rbs k =
  ignore delay_rec ; (* TODO: measure how long we spend waiting! *)
  let to_merge =
    Array.of_list rbs |>
    Array.map (fun rb ->
      { rb ; timed_out = None ; tuples = RamenSzHeap.empty }) in
  let tuples_cmp (_, _, k1) (_, _, k2) = compare k1 k2 in
  let read_more () =
    Array.iteri (fun i to_merge ->
      if RamenSzHeap.cardinal to_merge.tuples < last then
        match RingBuf.dequeue_alloc to_merge.rb with
        | exception RingBuf.Empty -> ()
        | tx ->
            (match to_merge.timed_out with
            | Some timed_out ->
                !logger.debug "Source #%d is back after %fs"
                  i (Unix.gettimeofday () -. timed_out) ;
                to_merge.timed_out <- None
            | None -> ()) ;
            let in_tuple = read_tuple tx in
            let tx_size = RingBuf.tx_size tx in
            RingBuf.dequeue_commit tx ;
            let key = on in_tuple in
            to_merge.tuples <-
              RamenSzHeap.add tuples_cmp (in_tuple, tx_size, key)
                              to_merge.tuples
    ) to_merge in
  (* Loop until timeout the given max time or we have a tuple for each
   * non timed out input sources: *)
  let rec wait_for_tuples started =
    read_more () ;
    (* Do we have something to read on any non-timeouted input?
     * If so, return. *)
    let must_wait, all_timed_out =
      Array.fold_left (fun (must_wait, all_timed_out) m ->
        must_wait || m.timed_out = None && RamenSzHeap.is_empty m.tuples,
        all_timed_out && m.timed_out <> None
      ) (false, true) to_merge in
    if all_timed_out then ( (* We could as well wait here forever *)
      if while_ () then (
        Unix.sleepf 0.1 (* TODO *) ;
        wait_for_tuples started)
    ) else if must_wait then (
      (* Some inputs are ready, consider timing out the offenders: *)
      let now = Unix.gettimeofday () in
      if timeout > 0. && now > started +. timeout
      then (
        Array.iteri (fun i m ->
          if m.timed_out = None &&
             RamenSzHeap.is_empty m.tuples
          then (
            !logger.debug "Timing out source #%d" i ;
            m.timed_out <- Some now)
        ) to_merge ;
      ) else (
        (* Wait longer: *)
        if while_ () then (
          Unix.sleepf 0.1 ;
          wait_for_tuples started)
      )
    ) (* all inputs that have not timed out are ready *)
  in
  let rec loop () =
    if while_ () then (
      wait_for_tuples (Unix.gettimeofday ()) ;
      match
        Array.fold_lefti (fun mi_ma i to_merge ->
          match mi_ma, RamenSzHeap.min_opt to_merge.tuples with
          | None, Some t ->
              Some (i, t, t)
          | Some (j, tmi, tma) as prev, Some t ->
              (* Try to preserve [prev] as much as possible: *)
              let repl_tma = tuples_cmp tma t < 0 in
              if tuples_cmp tmi t > 0 then
                Some (i, t, if repl_tma then t else tma)
              else if repl_tma then
                Some (j, tmi, t)
              else prev
          | _ -> mi_ma
        ) None to_merge with
      | None -> loop ()
      | Some (i, (min_tuple, tx_size, key), (max_tuple, _, _)) ->
          !logger.debug "Min in source #%d with key=%s" i (dump key) ;
          to_merge.(i).tuples <-
            RamenSzHeap.del_min tuples_cmp to_merge.(i).tuples ;
          k tx_size min_tuple max_tuple ;
          loop ()
    ) in
  loop ()

let yield_every ~while_ read_tuple every k =
  !logger.debug "YIELD operation"  ;
  let tx = RingBuf.empty_tx () in
  let rec loop () =
    if while_ () then (
      let start = Unix.gettimeofday () in
      let in_tuple = read_tuple tx in
      k 0 in_tuple in_tuple ;
      let rec wait () =
        (* Avoid sleeping longer than a few seconds to check while_ in a
         * timely fashion. The 1.33 is supposed to help distinguish this sleep
         * from the many others when using strace: *)
        let sleep_time =
          min 1.33
          ((start +. every) -. Unix.gettimeofday ()) in
        let keep_going = while_ () in
        if sleep_time > 0. && keep_going then (
          !logger.debug "Sleeping for %f seconds" sleep_time ;
          Unix.sleepf sleep_time ;
          wait ()
        ) else loop () in
      wait ()
    ) in
  loop ()

let aggregate
      (read_tuple : RingBuf.tx -> 'tuple_in)
      (sersize_of_tuple : bool list (* skip list *) -> 'tuple_out -> int)
      (time_of_tuple : 'tuple_out -> (float * float) option)
      (serialize_tuple : bool list (* skip list *) -> RingBuf.tx -> 'tuple_out -> int)
      (generate_tuples : ('tuple_in -> 'tuple_out -> unit) -> 'tuple_in -> 'generator_out -> unit)
      (* Build as few fields as possible, to answer commit_cond. Also update
       * the stateful functions required for those fields, but not others. *)
      (minimal_tuple_of_aggr :
        'tuple_in -> (* current input *)
        'generator_out option -> (* last_out *)
        'local_state -> 'global_state -> 'minimal_out)
      (* Update the states for all other fields. *)
      (update_states :
        'tuple_in -> (* current input *)
        'generator_out option -> (* last_out *)
        'local_state -> 'global_state -> 'minimal_out -> unit)
      (* Build the generator_out tuple from the minimal_out and all the same
       * inputs as minimal_tuple_of_aggr, all of which must be saved in the
       * group so we can commit other groups as well as the current one. *)
      (out_tuple_of_minimal_tuple :
        'tuple_in -> (* current input *)
        'generator_out option -> (* last_out *)
        'local_state -> 'global_state -> 'minimal_out -> 'generator_out)
      (merge_on : ('tuple_in, 'merge_on) merge_on_fun)
      (merge_last : int)
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
        'tuple_in -> (* merge.greatest (or current input if not merging) *)
        'generator_out option -> (* previous.out *)
        bool)
      (where_slow :
        'global_state ->
        'tuple_in -> (* current input *)
        'tuple_in -> (* merge.greatest (or current input if not merging) *)
        'generator_out option -> (* previous.out *)
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
        'generator_out option -> (* out_last *)
        'local_state ->
        'global_state ->
        'minimal_out -> (* current minimal out *)
        bool)
      commit_before
      do_flush
      (when_to_check_for_commit : when_to_check_group)
      (global_state : 'global_state)
      (group_init : 'global_state -> 'local_state)
      (field_of_tuple_in : 'tuple_in -> string -> string)
      (field_of_tuple_out : 'tuple_out -> string -> string)
      (field_of_params : string -> string)
      (get_notifications :
        'tuple_in -> 'tuple_out -> (string * (string * string) list) list)
      (every : float) =
  let stats_selected_tuple_count = make_stats_selected_tuple_count ()
  and stats_group_count =
    IntGauge.make RamenConsts.Metric.Names.group_count
                  RamenConsts.Metric.Docs.group_count in
  IntGauge.set stats_group_count 0 ;
  let worker_name = getenv ~def:"?" "fq_name" in
  let get_binocle_tuple () =
    let si v = Some (Uint64.of_int v) in
    let i v = Option.map (fun r -> Uint64.of_int r) v in
    get_binocle_tuple
      worker_name
      (IntCounter.get stats_in_tuple_count |> si)
      (IntCounter.get stats_selected_tuple_count |> si)
      (IntGauge.get stats_group_count |> Option.map gauge_current |> i) in
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
          List.iter (fun notif ->
            notify notify_rb worker_name event_time notif
                   field_of_tuple_in tuple_in
                   field_of_tuple_out tuple_out
                   field_of_params
          ) notifications
        ) ;
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
        { last_out_tuple = None ;
          global_state ;
          groups ;
          sort_buf = RamenSortBuf.empty } in
      let state =
        ref (make conf.state_file init_state) in
      fun f ->
        let v = restore !state in
        (* We do _not_ want to save the value when f raises an exception: *)
        let v = f v in
        state := save ~save_every:1013 ~save_timeout:21. !state v
    in
    !logger.debug "Will read ringbuffer %a"
      (List.print String.print) rb_in_fnames ;
    let rb_ins =
      List.map (fun fname ->
        retry ~on:(fun _ -> true) ~min_delay:1.0 (RingBuf.load) fname
      ) rb_in_fnames
    in
    (* The big function that aggregate a single tuple *)
    let aggregate_one s in_tuple merge_greatest =
      (* Define some short-hand values and functions we will keep
       * referring to: *)
      (* When committing other groups, this is used to skip the current
       * groupif it has been sent already: *)
      let already_output_aggr = ref None in
      let already_output g =
        Option.map_default ((==) g) false !already_output_aggr in
      (* Tells if the group must be committed/flushed: *)
      let must_commit g =
        commit_cond in_tuple s.last_out_tuple g.local_state
                    s.global_state g.current_out
      in
      let commit_and_flush (k, g) =
        (* Output the tuple *)
        (match commit_before, g.previous_out with
        | false, _ ->
            let out =
              out_tuple_of_minimal_tuple
                g.last_in s.last_out_tuple g.local_state s.global_state
                g.current_out in
            s.last_out_tuple <- Some out ;
            outputer in_tuple out
        | true, None -> ()
        | true, Some previous_out ->
            let out =
              out_tuple_of_minimal_tuple
                g.last_in s.last_out_tuple g.local_state s.global_state
                previous_out in
            s.last_out_tuple <- Some out ;
            outputer in_tuple out) ;
        (* Flush/Keep/Slide *)
        if do_flush then (
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
            g.local_state <- group_init s.global_state
          ) else Hashtbl.remove s.groups k
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
      let k_aggr_opt = (* maybe the key and group that has been updated: *)
        if where_fast s.global_state in_tuple merge_greatest s.last_out_tuple
        then (
          (* 2. Retrieve the group *)
          IntGauge.set stats_group_count (Hashtbl.length s.groups) ;
          let k = key_of_input in_tuple in
          (* Update/create the group if it passes where_slow. *)
          match Hashtbl.find s.groups k with
          | exception Not_found ->
            (* The group does not exist for that key. *)
            let local_state = group_init s.global_state in
            (* 3. Filtering (slow path) - for new group *)
            if where_slow s.global_state in_tuple merge_greatest
                          s.last_out_tuple local_state
            then (
              (* 4. Compute new minimal_out (and new group) *)
              let current_out =
                minimal_tuple_of_aggr
                  in_tuple s.last_out_tuple local_state s.global_state in
              let g = {
                first_in = in_tuple ;
                last_in = in_tuple ;
                current_out ;
                previous_out = None ;
                local_state } in
              (* Adding this group: *)
              Hashtbl.add s.groups k g ;
              Some (k, g)
            ) else None (* in-tuple does not pass where_slow *)
          | g ->
            (* The group already exists. *)
            (* 3. Filtering (slow path) - for existing group *)
            if where_slow s.global_state in_tuple merge_greatest
                          s.last_out_tuple g.local_state
            then (
              (* 4. Compute new current_out (and update the group) *)
              (* current_out and last_in are better updated only after we called the
               * various clauses receiving g *)
              g.last_in <- in_tuple ;
              g.previous_out <- Some g.current_out ;
              g.current_out <-
                minimal_tuple_of_aggr
                  g.last_in s.last_out_tuple g.local_state s.global_state ;
              Some (k, g)
            ) else None (* in-tuple does not pass where_slow *)
          ) else None (* in-tuple does not pass where_fast *) in
      (match k_aggr_opt with
      | Some (k, g) ->
        (* 5. Post-condition to commit and flush *)
        IntCounter.add stats_selected_tuple_count 1 ;
        if not commit_before then
          update_states g.last_in s.last_out_tuple
                        g.local_state s.global_state g.current_out ;
        if must_commit g then (
          already_output_aggr := Some g ;
          commit_and_flush (k, g)
        ) ;
        if commit_before then
          update_states g.last_in s.last_out_tuple
                        g.local_state s.global_state g.current_out
      | None -> () (* in_tuple failed filtering *)) ;
      (* Now there is also the possibility that we need to commit or flush
       * for every single input tuple :-< *)
      if when_to_check_for_commit = ForAll then (
        (* FIXME: prevent commit_before in that case *)
        let to_commit =
          (* FIXME: What if commit-when update the global state? We are
           * going to update it several times here. We should prevent this
           * clause to access the global state. *)
          Hashtbl.fold (fun k g l ->
            if not (already_output g) && must_commit g
            then (k, g)::l else l
          ) s.groups [] in
        List.iter commit_and_flush to_commit
      ) ;
      s
    in
    (* The event loop: *)
    let while_ () = !quit = None in
    let tuple_reader =
      match rb_ins with
      | [] -> (* yield expression *)
          yield_every ~while_ read_tuple every
      | [rb_in] ->
          read_single_rb ~while_ ~delay_rec:sleep_in read_tuple rb_in
      | rb_ins ->
          merge_rbs ~while_ ~delay_rec:sleep_in merge_on merge_last
                    merge_timeout read_tuple rb_ins
    in
    tuple_reader (fun tx_size in_tuple merge_greatest ->
      with_state (fun s ->
        (* Set now and in.#count: *)
        CodeGenLib_IO.on_each_input_pre () ;
        (* Update per in-tuple stats *)
        IntCounter.add stats_in_tuple_count 1 ;
        IntCounter.add stats_rb_read_bytes tx_size ;
        (* Sort: add in_tuple into the heap of sorted tuples, update
         * smallest/greatest, and consider extracting the smallest. *)
        (* If we assume sort_last >= 2 then the sort buffer will never
         * be empty but for the very last tuple. In that case pretend
         * tuple_in is the first (sort.#count will still be 0). *)
        if sort_last <= 1 then
          aggregate_one s in_tuple merge_greatest
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
            aggregate_one s min_in merge_greatest
          else
            s)))
