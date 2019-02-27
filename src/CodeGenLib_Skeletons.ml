(* Skeleton of the basic operations (aggregate, read csv...), parameterized
 * by functions that are generated (by CodeGen_OCaml). *)
open Batteries
open Stdint
open RamenLog
open RamenHelpers
open RamenConsts
open RamenNullable
module T = RamenTypes

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
  IntCounter.make Metric.Names.in_tuple_count
    Metric.Docs.in_tuple_count

let make_stats_selected_tuple_count () =
  IntCounter.make Metric.Names.selected_tuple_count
    Metric.Docs.selected_tuple_count

let stats_out_tuple_count =
  IntCounter.make Metric.Names.out_tuple_count
    Metric.Docs.out_tuple_count

let stats_cpu =
  FloatCounter.make Metric.Names.cpu_time
    Metric.Docs.cpu_time

let stats_ram =
  IntGauge.make Metric.Names.ram_usage
    Metric.Docs.ram_usage

let stats_rb_read_bytes =
  IntCounter.make Metric.Names.rb_read_bytes
    Metric.Docs.rb_read_bytes

let stats_rb_write_bytes =
  IntCounter.make Metric.Names.rb_write_bytes
    Metric.Docs.rb_write_bytes

let stats_rb_read_sleep_time =
  FloatCounter.make Metric.Names.rb_wait_read
    Metric.Docs.rb_wait_read

let stats_rb_write_sleep_time =
  FloatCounter.make Metric.Names.rb_wait_write
    Metric.Docs.rb_wait_write

let sleep_in d = FloatCounter.add stats_rb_read_sleep_time d
let sleep_out d = FloatCounter.add stats_rb_write_sleep_time d

let stats_last_out =
  FloatGauge.make Metric.Names.last_out
    Metric.Docs.last_out

let stats_event_time =
  FloatGauge.make Metric.Names.event_time
    Metric.Docs.event_time

(* From time to time we measure the full size of an output tuple and
 * update this. Have a single Gauge rather than two counters to avoid
 * race conditions between updates and send_stats: *)
let stats_avg_full_out_bytes =
  IntGauge.make Metric.Names.avg_full_out_bytes
    Metric.Docs.avg_full_out_bytes

let measure_full_out =
  let sum = ref 0 and count = ref 0 in
  fun sz ->
    !logger.debug "measured fully fledged out tuple of size %d" sz ;
    let prev_sum = !sum in
    sum := !sum + sz ;
    if !sum < prev_sum then (
      count := !count / 2 ;
      sum := prev_sum / 2 + sz
    ) ;
    incr count ;
    float_of_int !sum /. float_of_int !count |>
    round_to_int |>
    IntGauge.set stats_avg_full_out_bytes

let tot_cpu_time () =
  let open Unix in
  let pt = times () in
  pt.tms_utime +. pt.tms_stime +. pt.tms_cutime +. pt.tms_cstime

let tot_ram_usage =
  let word_size = Sys.word_size / 8 in
  fun () ->
    let stat = Gc.quick_stat () in
    stat.Gc.heap_words * word_size

(* Unfortunately, we cannot distinguish that easily between CPU/RAM usage for
 * live channel and others: *)
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
  let sg = function None -> None | Some (_, v, _) -> si v
  and s v = Some v
  and ram, max_ram =
    match IntGauge.get stats_ram with
    | None -> Uint64.zero, Uint64.zero
    | Some (_mi, x, ma) -> Uint64.of_int x, Uint64.of_int ma
  and min_event_time, max_event_time =
    match FloatGauge.get stats_event_time with
    | None -> None, None
    | Some (mi, _, ma) -> Some mi, Some ma
  and time = Unix.gettimeofday () in
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
  IntGauge.get stats_avg_full_out_bytes |> sg,
  FloatGauge.get stats_last_out |> Option.map gauge_current,
  startup_time

let send_stats
    rb (_, time, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _ as tuple) =
  let open RingBuf in
  let head = RingBufLib.DataTuple RamenChannel.live in
  let sersize =
    RingBufLib.message_header_sersize head +
    RamenBinocle.max_sersize_of_tuple tuple in
  match enqueue_alloc rb sersize with
  | exception NoMoreRoom -> () (* Just skip *)
  | tx ->
    RingBufLib.(write_message_header tx 0 head) ;
    let offs = RingBufLib.message_header_sersize head in
    let offs = RamenBinocle.serialize tx offs tuple in
    enqueue_commit tx time time ;
    assert (offs <= sersize)

let update_stats_rb report_period rb get_tuple =
  if report_period > 0. then
    while true do
      update_stats () ;
      let tuple = get_tuple () in
      send_stats rb tuple ;
      Unix.sleepf report_period
    done

(*
 * Factors possible values:
 *)

type possible_values_index =
  { min_time : float ; max_time : float ;
    (* Name of the previous file where that set was saved.
     * This file is renamed after every write. *)
    fname : string ;
    mutable values : T.value Set.t }

(* Just a placeholder to init the initial array. *)
let possible_values_empty =
  { min_time = max_float ; max_time = min_float ;
    fname = "" ; values = Set.empty }

let save_possible_values prev_fname pvs =
  (* We are going to write a new file and delete the former one.
   * We only need a lock when that's the same file. *)
  let do_write fd = marshal_into_fd fd pvs.values in
  if prev_fname = pvs.fname then (
    !logger.debug "Updating index %s" prev_fname ;
    RamenAdvLock.with_w_lock prev_fname do_write
  ) else (
    !logger.debug "Creating new index %s" pvs.fname ;
    mkdir_all ~is_file:true pvs.fname ;
    let flags = Unix.[ O_CREAT; O_EXCL; O_WRONLY; O_CLOEXEC ] in
    (match Unix.openfile pvs.fname flags 0o644 with
    | exception Unix.(Unix_error (EEXIST, _, _)) ->
        (* Although we though we would create a new file for a singleton,
         * it turns out this file exists already. This could happen when
         * event time goes back and forth, which is allowed. So we have
         * to merge the indices now: *)
        !logger.warning "Stumbled upon preexisting index %s, merging..."
          pvs.fname ;
        RamenAdvLock.with_w_lock pvs.fname (fun fd ->
          let prev_set : T.value Set.t =
            marshal_from_fd ~default:Set.empty pvs.fname fd in
          let s = Set.union prev_set pvs.values in
          (* Keep past values for the next write: *)
          pvs.values <- s ;
          do_write fd)
    | fd -> do_write fd) ;
    if prev_fname <> "" then
      log_and_ignore_exceptions safe_unlink prev_fname)


(* Helpers *)

(* For non-wrapping buffers we need to know the value for the time, as
 * the min/max times per slice are saved, along the first/last tuple
 * sequence number. *)
let output rb serialize_tuple sersize_of_tuple
           (* Those last parameters change at every tuple: *)
           start_stop head tuple_opt =
  let open RingBuf in
  let tuple_sersize =
    Option.map_default sersize_of_tuple 0 tuple_opt in
  let sersize = RingBufLib.message_header_sersize head + tuple_sersize in
  (* Nodes with no output (but notifications) have no business writing
   * a ringbuf. Want a signal when a notification is sent? SELECT some
   * value! *)
  if tuple_opt = None || tuple_sersize > 0 then
    IntCounter.add stats_rb_write_bytes sersize ;
    let tx = enqueue_alloc rb sersize in
    let offs =
      RingBufLib.write_message_header tx 0 head ;
      RingBufLib.message_header_sersize head in
    let offs =
      match tuple_opt with
      | Some tuple -> serialize_tuple tx offs tuple
      | None -> offs in
    (* start = stop = 0. => times are unset *)
    let start, stop = start_stop |? (0., 0.) in
    enqueue_commit tx start stop ;
    assert (offs = sersize)

let quit = ref None

type 'a out_rb =
  { fname : string ;
    rb : RingBuf.t ;
    tup_serializer : RingBuf.tx -> int -> 'a -> int ;
    tup_sizer : 'a -> int ;
    fieldmask : RamenFieldMask.fieldmask ;
    mutable channels : (RamenChannel.t, float) Hashtbl.t ;
    (* When we last checked this is still in the out_ref: *)
    mutable last_check_outref : float ;
    mutable last_successful_output : float ;
    mutable quarantine_until : float ;
    mutable quarantine_delay : float ;
    rate_limit_log_writes : unit -> bool ;
    rate_limit_log_drops : unit -> bool }

let rb_writer rb_ref_out_fname out_rb dest_channel start_stop head tuple_opt =
  (* Check that we are still supposed to write in there, but now
   * more frequently than once every 3 secs (how long we are
   * ready to block on a dead child): *)
  let still_in_outref now =
    if now < out_rb.last_check_outref +. 3. then true else (
      out_rb.last_check_outref <- now ;
      RamenOutRef.mem rb_ref_out_fname out_rb.fname now)
  in
  if dest_channel <> RamenChannel.live && out_rb.rate_limit_log_writes () then
    !logger.debug "Write a tuple to channel %a"
      RamenChannel.print dest_channel ;
  (* Note: we retry only on NoMoreRoom so that's OK to keep trying; in
   * case the ringbuf disappear altogether because the child is
   * terminated then we won't deadloop.  Also, if one child is full
   * then we will not write to next children until we can eventually
   * write to this one. This is actually desired to have proper message
   * ordering along the stream and avoid ending up with many threads
   * retrying to write to the same child. *)
  retry
    ~while_:(fun () ->
      !quit = None && still_in_outref (Unix.gettimeofday ()))
    ~on:(function
      | RingBuf.NoMoreRoom ->
        !logger.debug "NoMoreRoom in %s" out_rb.fname ;
        (* Can't use CodeGenLib_IO.now if we are stuck in output: *)
        let now = Unix.gettimeofday () in
        (* Also check from time to time that we are still supposed to
         * write in there (we check right after the first error to
         * quickly detect it when a child disappear): *)
        still_in_outref now && (
          now < out_rb.last_successful_output +. 5. || (
            (* At this point, we have been failing for a good while
             * for a child that's still in our out_ref, and should
             * consider quarantine for a bit: *)
            out_rb.quarantine_delay <-
              min 30. (10. +. out_rb.quarantine_delay *. 1.5) ;
            out_rb.quarantine_until <-
              now +. jitter out_rb.quarantine_delay ;
            !logger.error "Quarantining %s until %s"
              out_rb.fname (string_of_time out_rb.quarantine_until) ;
            true))
      | _ -> false)
    ~first_delay:0.001 ~max_delay:1. ~delay_rec:sleep_out
    (fun () ->
      match Hashtbl.find out_rb.channels dest_channel with
      | exception Not_found ->
          (* Can happen at leaf functions after a replay: *)
          if out_rb.rate_limit_log_drops () then
            !logger.debug "Drop a tuple for %s unknown channel %a"
              out_rb.fname RamenChannel.print dest_channel ;
      | t ->
          if not (RamenOutRef.timed_out !CodeGenLib_IO.now t) then (
            if out_rb.quarantine_until < !CodeGenLib_IO.now then (
              output out_rb.rb out_rb.tup_serializer out_rb.tup_sizer
                     start_stop head tuple_opt ;
              out_rb.last_successful_output <- !CodeGenLib_IO.now ;
              if out_rb.quarantine_delay > 0. then (
                !logger.info "Resuming output to %s" out_rb.fname ;
                out_rb.quarantine_delay <- 0.)
            ) else (
              !logger.debug "Skipping output to %s (quarantined)"
                out_rb.fname)
          ) else (
            if out_rb.rate_limit_log_drops () then
              !logger.debug "Drop a tuple for %s outdated channel %a"
                out_rb.fname RamenChannel.print dest_channel
          )) ()

(* FIXME: when the output type is a single value, just [| Copy |]: *)
let all_fields = Array.make 100 RamenFieldMask.Copy

(* Each func can write in several ringbuffers (one per children). This list
 * will change dynamically as children are added/removed. *)
let outputer_of
      rb_ref_out_fname sersize_of_tuple time_of_tuple factors_of_tuple
      serialize_tuple =
  let out_h = ref (Hashtbl.create 15) (* Hash from fname to rb*file_spec*)
  and out_rbs = ref []  (* list of outputers *) in
  let max_num_fields = 100 (* FIXME *) in
  let factors_values = Array.make max_num_fields possible_values_empty in
  let factors_dir = getenv ~def:"/tmp/factors" "factors_dir" in
  let last_full_out_measurement = ref 0. in
  let get_out_fnames =
    let last_mtime = ref 0. and last_stat = ref 0. and last_read = ref 0. in
    fun () ->
      (* TODO: make this min_delay_restats a parameter: *)
      if !CodeGenLib_IO.now > !last_stat +. Default.min_delay_restats then (
        last_stat := !CodeGenLib_IO.now ;
        let must_read =
          let t = mtime_of_file_def 0. rb_ref_out_fname in
          if t > !last_mtime then (
            if !last_mtime <> 0. then
              !logger.debug "Have to re-read %s" rb_ref_out_fname ;
            last_mtime := t ;
            true
          (* We have to reread the outref from time to time even if not
           * changed because of timeout expiry. *)
          ) else !CodeGenLib_IO.now > !last_read +. 10.
        in
        if must_read then (
          !logger.debug "Rereading out-ref" ;
          last_read := !CodeGenLib_IO.now ;
          Some (RamenOutRef.read rb_ref_out_fname)
        ) else None
      ) else None
  in
  let update_out_rbs () =
    (* Get out_specs if they've changed: *)
    get_out_fnames () |>
    Option.may (fun out_specs ->
      if Hashtbl.is_empty out_specs then (
        if not (Hashtbl.is_empty !out_h) then (
          !logger.debug "OutRef is now empty!" ;
          Hashtbl.clear !out_h)
      ) else (
        if Hashtbl.is_empty !out_h then
          !logger.debug "OutRef is no longer empty!" ;
        !logger.debug "Must now output to: %a"
          RamenOutRef.print_out_specs out_specs) ;
      (* Change occurred, load/unload as required *)
      out_h :=
        hashtbl_merge !out_h out_specs (fun fname prev new_ ->
          match prev, new_ with
          | None, Some file_spec ->
              !logger.debug "Mapping %S" fname ;
              (match RingBuf.load fname with
              | exception e ->
                  !logger.error "Cannot open ringbuf %s: %s"
                    fname (Printexc.to_string e) ;
                  None
              | rb ->
                  (* Since we never output empty tuples (sersize_of_tuple would fail): *)
                  assert (Array.length file_spec.RamenOutRef.fieldmask > 0) ;
                  Some {
                    fname ; rb ;
                    tup_serializer =
                      serialize_tuple file_spec.RamenOutRef.fieldmask ;
                    tup_sizer =
                      sersize_of_tuple file_spec.RamenOutRef.fieldmask ;
                    fieldmask = file_spec.RamenOutRef.fieldmask ;
                    channels = file_spec.RamenOutRef.channels ;
                    last_check_outref = Unix.gettimeofday () ;
                    last_successful_output = 0. ;
                    quarantine_until = 0. ;
                    quarantine_delay = 0. ;
                    rate_limit_log_writes = rate_limit 1 1. ;
                    rate_limit_log_drops = rate_limit 1 1. })
          | Some out_rb, None ->
              !logger.debug "Unmapping %S" fname ;
              RingBuf.unload out_rb.rb ;
              None
          | Some out_rb, Some file_spec ->
              (* Or the fname would have changed: *)
              assert (file_spec.RamenOutRef.fieldmask = out_rb.fieldmask) ;
              if out_rb.channels <> file_spec.RamenOutRef.channels then (
                !logger.debug "Updating %S" fname ;
                out_rb.channels <- file_spec.RamenOutRef.channels) ;
              out_rb.last_check_outref <- Unix.gettimeofday () ;
              Some out_rb
          | None, None -> assert false)) ;
      out_rbs := Hashtbl.values !out_h |> List.of_enum
  in
  fun head tuple_opt ->
    let dest_channel = RingBufLib.channel_of_message_header head in
    let start_stop =
      match tuple_opt with
      | Some tuple ->
        let start_stop = time_of_tuple tuple in
        if dest_channel = RamenChannel.live then (
          (* Update stats *)
          IntCounter.add stats_out_tuple_count 1 ;
          FloatGauge.set stats_last_out !CodeGenLib_IO.now ;
          if !CodeGenLib_IO.now -. !last_full_out_measurement >
               min_delay_between_full_out_measurement
          then (
            measure_full_out (sersize_of_tuple all_fields tuple) ;
            last_full_out_measurement := !CodeGenLib_IO.now) ;
          (* Update factors possible values: *)
          let start, stop = start_stop |? (0., end_of_times) in
          factors_of_tuple tuple |>
          Array.iteri (fun i (factor, pv) ->
            assert (i < Array.length factors_values) ;
            let pvs = factors_values.(i) in
            if not (Set.mem pv pvs.values) then ( (* FIXME: Set.update *)
              !logger.debug "new value for factor %s: %a"
                factor T.print pv ;
              let min_time = min start pvs.min_time
              and max_time = max stop pvs.max_time in
              let min_time, max_time, values, prev_fname =
                if start_stop = None ||
                   max_time -. min_time <= possible_values_lifespan
                then
                  min_time, max_time, Set.add pv pvs.values, pvs.fname
                else
                  start, stop, Set.singleton pv, "" in
              let fname =
                possible_values_file factors_dir factor min_time max_time in
              let pvs = { min_time ; max_time ; fname ; values } in
              factors_values.(i) <- pvs ;
              (* Warning that this function might in some rare case update
               * pvs.values! *)
              save_possible_values prev_fname pvs)) ;
          (* Update stats_event_time: *)
          Option.may (fun (start, _) ->
            (* We'd rather announce the start time of the event, even for
             * negative durations. *)
            FloatGauge.set stats_event_time start
          ) start_stop) ;
        start_stop
      | None -> None in
    update_out_rbs () ;
    List.iter (fun out_rb ->
      try rb_writer rb_ref_out_fname out_rb dest_channel start_stop head
                    tuple_opt
      with
        (* It is OK, just skip it. Next tuple we will reread fnames
         * if it has changed. *)
        | RingBuf.NoMoreRoom -> ()
        (* retry_for_ringbuf failing because the recipient is no more in
         * our out_ref: *)
        | Exit -> ()
    ) !out_rbs

type worker_conf =
  { log_level : log_level ;
    state_file : string ;
    is_test : bool }

let info_or_test conf =
  if conf.is_test then !logger.debug else !logger.info

let worker_start worker_name get_binocle_tuple k =
  let log_level = getenv ~def:"normal" "log_level" |> log_level_of_string in
  let default_persist_dir =
    "/tmp/worker_"^ worker_name ^"_"^ string_of_int (Unix.getpid ()) in
  let is_test = getenv ~def:"false" "is_test" |> bool_of_string in
  let state_file = getenv ~def:default_persist_dir "state_file" in
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
    getenv ~def:(string_of_float Default.report_period)
           "report_period" |> float_of_string in
  let report_rb_fname =
    getenv ~def:"/tmp/ringbuf_in_report.r" "report_ringbuf" in
  let report_rb = RingBuf.load report_rb_fname in
  (* Must call this once before get_binocle_tuple because cpu/ram gauges
   * must not be NULL: *)
  update_stats () ;
  (* Then, the sooner a new worker appears in the stats the better: *)
  if report_period > 0. then
    ignore_exceptions (send_stats report_rb) (get_binocle_tuple ()) ;
  let conf = { log_level ; state_file ; is_test } in
  info_or_test conf "Starting %s process. Will log into %s at level %s."
    worker_name
    (string_of_log_output !logger.output)
    (string_of_log_level log_level) ;
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
      (update_stats_rb report_period report_rb)) get_binocle_tuple |>
    ignore ;
  log_and_ignore_exceptions k conf ;
  (* Sending stats for one last time: *)
  if report_period > 0. then
    ignore_exceptions (send_stats report_rb) (get_binocle_tuple ()) ;
  exit (!quit |? ExitCodes.terminated)

(*
 * Operations that funcs may run: read a CSV file.
 *)

let read_csv_file
    filename do_unlink separator sersize_of_tuple
    time_of_tuple factors_of_tuple serialize_tuple tuple_of_strings
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
      tuple_of_strings strings
    in
    let outputer =
      outputer_of
        rb_ref_out_fname sersize_of_tuple time_of_tuple factors_of_tuple
        serialize_tuple (RingBufLib.DataTuple RamenChannel.live) in
    let while_ () = !quit = None in
    CodeGenLib_IO.read_glob_lines
      ~while_ ~do_unlink filename preprocessor quit (fun line ->
      match of_string line with
      | exception e ->
        !logger.error "Cannot parse line %S: %s"
          line (Printexc.to_string e)
      | tuple ->
        IntCounter.add stats_in_tuple_count 1 ;
        outputer (Some tuple)))

(*
 * Operations that funcs may run: listen to some known protocol.
 *)

let listen_on
      (collector : ?while_:(unit -> bool) -> ('a -> unit) -> unit)
      proto_name
      sersize_of_tuple time_of_tuple factors_of_tuple serialize_tuple =
  let worker_name = getenv ~def:"?" "fq_name" in
  let get_binocle_tuple () =
    get_binocle_tuple worker_name None None None in
  worker_start worker_name get_binocle_tuple (fun conf ->
    let rb_ref_out_fname = getenv ~def:"/tmp/ringbuf_out_ref" "output_ringbufs_ref"
    in
    info_or_test conf "Will listen for incoming %s messages" proto_name ;
    let outputer =
      outputer_of
        rb_ref_out_fname sersize_of_tuple time_of_tuple factors_of_tuple
        serialize_tuple (RingBufLib.DataTuple RamenChannel.live) in
    let while_ () = !quit = None in
    collector ~while_ (fun tup ->
      CodeGenLib_IO.on_each_input_pre () ;
      IntCounter.add stats_in_tuple_count 1 ;
      outputer (Some tup)))

(*
 * Operations that funcs may run: read known tuples from a ringbuf.
 *)

let read_well_known
      from sersize_of_tuple time_of_tuple factors_of_tuple serialize_tuple
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
      outputer_of
        rb_ref_out_fname sersize_of_tuple time_of_tuple factors_of_tuple
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
          let open RingBufLib in
          (match read_message_header tx 0 with
          | exception e ->
            print_exception ~what:"read well known tuple" e
          | DataTuple chan as m ->
            let offs = message_header_sersize m in
            let tuple = unserialize_tuple tx offs in
            let worker, time = worker_time_of_tuple tuple in
            (* Filter by time and worker *)
            if time >= start && match_from worker then (
              CodeGenLib_IO.on_each_input_pre () ;
              IntCounter.add stats_in_tuple_count 1 ;
              outputer (RingBufLib.DataTuple chan) (Some tuple))
          | _ -> ()) ;
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

let notify rb worker event_time (name, parameters) =
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
  { mutable last_out_tuple : 'generator_out nullable ; (* last committed tuple generator *)
    global_state : 'global_state ;
    (* The hash of all groups: *)
    mutable groups :
      ('key, ('local_state, 'tuple_in, 'minimal_out) group) Hashtbl.t ;
    (* Input sort buffer and related tuples: *)
    mutable sort_buf : ('sort_key, 'tuple_in) RamenSortBuf.t ;
    (* We have one such state per channel, that we timeout when a
     * channel is unseen for too long: *)
    mutable last_used : float }

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

type ('tuple_in, 'merge_on) merge_on_fun =
  'tuple_in (* last in *) -> 'merge_on

(* [on_tup] is the continuation for tuples while [on_else] is the
 * continuation for non tuples: *)
let read_single_rb ?while_ ?delay_rec read_tuple rb_in on_tup on_else =
  RingBufLib.read_ringbuf ?while_ ?delay_rec rb_in (fun tx ->
    match read_tuple tx with
    | exception e ->
        print_exception ~what:"reading a tuple" e
    | msg ->
        let tx_size = RingBuf.tx_size tx in
        RingBuf.dequeue_commit tx ;
        (match msg with
        | RingBufLib.DataTuple chan, Some tuple ->
            on_tup tx_size chan tuple tuple
        | head, None -> on_else head
        | _ -> assert false))

type ('tuple_in, 'merge_on) to_merge =
  { rb : RingBuf.t ;
    mutable tuples : ('tuple_in * int * 'merge_on) RamenSzHeap.t ;
    mutable timed_out : float option (* When it was timed out *) }

let merge_rbs ~while_ ?delay_rec on last timeout read_tuple rbs
              on_tup on_else =
  ignore delay_rec ; (* TODO: measure how long we spend waiting! *)
  let to_merge =
    Array.of_list rbs |>
    Array.map (fun rb ->
      { rb ; timed_out = None ; tuples = RamenSzHeap.empty }) in
  let tuples_cmp (_, _, k1) (_, _, k2) = compare k1 k2 in
  let read_more () =
    Array.iteri (fun i to_merge ->
      if RamenSzHeap.cardinal to_merge.tuples < last then (
        match RingBuf.dequeue_alloc to_merge.rb with
        | exception RingBuf.Empty -> ()
        | tx ->
            (match to_merge.timed_out with
            | Some timed_out ->
                !logger.debug "Source #%d is back after %fs"
                  i (Unix.gettimeofday () -. timed_out) ;
                to_merge.timed_out <- None
            | None -> ()) ;
            (match read_tuple tx with
            | exception e ->
                print_exception ~what:"reading a tuple" e
            | msg ->
                let tx_size = RingBuf.tx_size tx in
                RingBuf.dequeue_commit tx ;
                (match msg with
                | RingBufLib.DataTuple _chan, Some in_tuple ->
                  (* TODO: pick the heap for this chan *)
                  let key = on in_tuple in
                  to_merge.tuples <-
                    RamenSzHeap.add tuples_cmp (in_tuple, tx_size, key)
                                    to_merge.tuples
                | head, None -> on_else head
                | _ -> assert false)))
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
          let chan = RamenChannel.live (* TODO *) in
          on_tup tx_size chan min_tuple max_tuple ;
          loop ()) in
  loop ()

let yield_every ~while_ read_tuple every on_tup on_else =
  !logger.debug "YIELD operation"  ;
  let tx = RingBuf.empty_tx () in
  let rec loop prev_start =
    if while_ () then (
      let start =
        match read_tuple tx with
        | exception e ->
            print_exception ~what:"yielding a tuple" e ;
            prev_start
        | RingBufLib.DataTuple chan, Some tuple ->
            let s = Unix.gettimeofday () in
            on_tup 0 chan tuple tuple ;
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
          ((start +. every) -. Unix.gettimeofday ()) in
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
      (sersize_of_tuple : RamenFieldMask.fieldmask -> 'tuple_out -> int)
      (time_of_tuple : 'tuple_out -> (float * float) option)
      (factors_of_tuple : 'tuple_out -> (string * T.value) array)
      (serialize_tuple : RamenFieldMask.fieldmask -> RingBuf.tx -> int -> 'tuple_out -> int)
      (generate_tuples : (RamenChannel.t -> 'tuple_in -> 'tuple_out -> unit) -> RamenChannel.t -> 'tuple_in -> 'generator_out -> unit)
      (* Build as few fields as possible, to answer commit_cond. Also update
       * the stateful functions required for those fields, but not others. *)
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
       * inputs as minimal_tuple_of_aggr, all of which must be saved in the
       * group so we can commit other groups as well as the current one. *)
      (out_tuple_of_minimal_tuple :
        'tuple_in -> (* current input *)
        'generator_out nullable -> (* last_out *)
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
        'generator_out nullable -> (* previous.out *)
        bool)
      (where_slow :
        'global_state ->
        'tuple_in -> (* current input *)
        'tuple_in -> (* merge.greatest (or current input if not merging) *)
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
      commit_before
      do_flush
      (when_to_check_for_commit : when_to_check_group)
      (global_state : unit -> 'global_state)
      (group_init : 'global_state -> 'local_state)
      (get_notifications :
        'tuple_in -> 'tuple_out -> (string * (string * string) list) list)
      (every : float) =
  let stats_selected_tuple_count = make_stats_selected_tuple_count ()
  and stats_group_count =
    IntGauge.make Metric.Names.group_count
                  Metric.Docs.group_count in
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
    let msg_outputer =
      outputer_of
        rb_ref_out_fname sersize_of_tuple time_of_tuple factors_of_tuple
        serialize_tuple in
    let outputer =
      (* tuple_in is useful for generators and text expansion: *)
      let do_out chan tuple_in tuple_out =
        let notifications =
          if chan = RamenChannel.live then
            get_notifications tuple_in tuple_out
          else [] in
        if notifications <> [] then (
          let event_time = time_of_tuple tuple_out |> Option.map fst in
          List.iter (notify notify_rb worker_name event_time) notifications
        ) ;
        msg_outputer (RingBufLib.DataTuple chan) (Some tuple_out)
      in
      generate_tuples do_out in
    let with_state =
      let open CodeGenLib_State.Persistent in
      let init_state () =
        { last_out_tuple = Null ;
          global_state = global_state () ;
          groups =
            (* Try to make the state as small as possible: *)
            Hashtbl.create (if is_single_key then 1 else 701) ;
          sort_buf = RamenSortBuf.empty ;
          last_used = Unix.time () } in
      let live_state =
        ref (make conf.state_file (init_state ())) in
      (* Non live states: *)
      let states = Hashtbl.create 103 in
      let get_state chn =
        if chn = RamenChannel.live then
          restore !live_state
        else
          match Hashtbl.find states chn with
          | exception Not_found ->
              let st = init_state () in
              Hashtbl.add states chn st ;
              st
          | st -> st
      and save_state chn st =
        if chn = RamenChannel.live then
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
      (List.print String.print) rb_in_fnames ;
    let rb_ins =
      List.map (fun fname ->
        let on _ =
          ignore (Gc.major_slice 0) ;
          true in
        retry ~on ~min_delay:1.0 RingBuf.load fname
      ) rb_in_fnames
    in
    (* The big function that aggregate a single tuple *)
    let aggregate_one channel_id s in_tuple merge_greatest =
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
            s.last_out_tuple <- NotNull out ;
            outputer channel_id in_tuple out
        | true, None -> ()
        | true, Some previous_out ->
            let out =
              out_tuple_of_minimal_tuple
                g.last_in s.last_out_tuple g.local_state s.global_state
                previous_out in
            s.last_out_tuple <- NotNull out ;
            outputer channel_id in_tuple out) ;
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
          if channel_id = RamenChannel.live then
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
        if channel_id = RamenChannel.live then
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
    let rate_limit_log_reads = rate_limit 1 1. in
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
    and on_tup tx_size channel_id in_tuple merge_greatest =
      if channel_id <> RamenChannel.live && rate_limit_log_reads () then
        !logger.debug "Read a tuple from channel %a"
          RamenChannel.print channel_id ;
      with_state channel_id (fun s ->
        (* Set CodeGenLib_IO.now: *)
        CodeGenLib_IO.on_each_input_pre () ;
        (* Update per in-tuple stats *)
        if channel_id = RamenChannel.live then (
          IntCounter.add stats_in_tuple_count 1 ;
          IntCounter.add stats_rb_read_bytes tx_size) ;
        (* Sort: add in_tuple into the heap of sorted tuples, update
         * smallest/greatest, and consider extracting the smallest. *)
        (* If we assume sort_last >= 2 then the sort buffer will never
         * be empty but for the very last tuple. In that case pretend
         * tuple_in is the first (sort.#count will still be 0). *)
        if sort_last <= 1 then
          aggregate_one channel_id s in_tuple merge_greatest
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
            aggregate_one channel_id s min_in merge_greatest
          else
            s)
    and on_else head =
      msg_outputer head None
    in
    tuple_reader on_tup on_else)


let read_whole_archive ?(while_=always) read_tuplez rb k =
  if while_ () then
    RingBufLib.(read_buf ~wait_for_more:false ~while_ rb () (fun () tx ->
      match read_tuplez tx with
      | exception e ->
          print_exception ~what:"reading a tuple from archive" e,
          false (* Skip the rest of that file for safety *)
      | DataTuple chn, Some tuple when chn = RamenChannel.live ->
          k tuple, true
      | DataTuple chn, _ ->
          (* This should not happen as we archive only the live channel: *)
          !logger.warning "Read a tuple from channel %a in archive?"
            RamenChannel.print chn ;
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
      (sersize_of_tuple : RamenFieldMask.fieldmask -> 'tuple_out -> int)
      (time_of_tuple : 'tuple_out -> (float * float) option)
      (factors_of_tuple : 'tuple_out -> (string * T.value) array)
      (serialize_tuple : RamenFieldMask.fieldmask -> RingBuf.tx -> int -> 'tuple_out -> int) =
  let worker_name = getenv ~def:"?" "fq_name" in
  let log_level = getenv ~def:"normal" "log_level" |> log_level_of_string in
  let prefix = worker_name ^" (REPLAY): " in
  (* TODO: factorize *)
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
  let rb_ref_out_fname =
    getenv ~def:"/tmp/ringbuf_out_ref" "output_ringbufs_ref"
  and rb_archive = getenv ~def:"/tmp/archive.b" "rb_archive"
  and since = getenv "since" |> float_of_string
  and until = getenv "until" |> float_of_string
  and channel_id = getenv "channel_id" |> RamenChannel.of_string
  and replayer_id = getenv "replayer_id" |> int_of_string
  in
  !logger.debug "Starting REPLAY of %s. Will log into %s at level %s."
    worker_name
    (string_of_log_output !logger.output)
    (string_of_log_level log_level) ;
  (* TODO: also factorize *)
  set_signals Sys.[sigterm; sigint] (Signal_handle (fun s ->
    !logger.debug "Received signal %s" (name_of_signal s) ;
    quit :=
      Some (if s = Sys.sigterm then ExitCodes.terminated
                               else ExitCodes.interrupted))) ;
  (* Ignore sigusr1: *)
  set_signals Sys.[sigusr1] Signal_ignore ;
  !logger.debug "Will replay archive from %S" rb_archive ;
  let outputer =
    outputer_of
      rb_ref_out_fname sersize_of_tuple time_of_tuple factors_of_tuple
      serialize_tuple in
  let num_replayed_tuples = ref 0 in
  let while_ () = !quit = None in
  let dir = RingBufLib.arc_dir_of_bname rb_archive in
  let files = RingBufLib.arc_files_of dir in
  let time_overlap t1 t2 = since < t2 && until >= t1 in
  let loop_tuples rb =
    read_whole_archive ~while_ read_tuple rb (fun tuple ->
      CodeGenLib_IO.on_each_input_pre () ;
      incr num_replayed_tuples ;
      (* As tuples are not ordered in the archive file we have
       * to read it all: *)
      outputer (RingBufLib.DataTuple channel_id) (Some tuple)) in
  let loop_tuples_of_file fname =
    !logger.debug "Reading archive %S" fname ;
    match RingBuf.load fname with
    | exception e ->
        let what = "Reading archive "^ fname in
        print_exception ~what e
    | rb ->
        finally (fun () -> RingBuf.unload rb) (fun () ->
          let st = RingBuf.stats rb in
          if time_overlap st.t_min st.t_max then
            loop_tuples rb
          else
            !logger.debug "Archive times of %S (%f..%f) does not overlap \
                           with search (%f..%f)"
              rb_archive st.t_min st.t_max since until) () in
  let rec loop_files () =
    if while_ () then
      match Enum.get_exn files with
      | exception Enum.No_more_elements -> ()
      | _s1, _s2, t1, t2, fname ->
          if time_overlap t1 t2 then (
            loop_tuples_of_file fname ;
            loop_files ())
  in
  !logger.debug "Reading the past archives..." ;
  loop_files () ;
  (* Finish with the current archive: *)
  !logger.debug "Reading current archive" ;
  loop_tuples_of_file rb_archive ;
  (* Before quitting, signal the end of this replay: *)
  outputer (RingBufLib.EndOfReplay (channel_id, replayer_id)) None ;
  !logger.debug "Finished after having replayed %d tuples"
    !num_replayed_tuples ;
  exit (!quit |? ExitCodes.terminated)

let convert
      in_fmt in_fname out_fmt out_fname
      orc_read csv_write orc_write orc_make_handler orc_close
      (read_tuple : RingBuf.tx -> RingBufLib.message_header * 'tuple_out option)
      (sersize_of_tuple : RamenFieldMask.fieldmask -> 'tuple_out -> int)
      (time_of_tuple : 'tuple_out -> (float * float) option)
      (serialize_tuple : RamenFieldMask.fieldmask -> RingBuf.tx -> int -> 'tuple_out -> int)
      tuple_of_strings =
  let log_level = getenv ~def:"normal" "log_level" |> log_level_of_string in
  (match getenv "log" with
  | exception _ ->
      init_logger log_level
  | logdir ->
      if logdir = "syslog" then
        init_syslog log_level
      else (
        mkdir_all logdir ;
        init_logger ~logdir log_level
      )) ;
  !logger.debug "Going to convert from %s to %s"
    (CodeGenLib_Casing.string_of_format in_fmt)
    (CodeGenLib_Casing.string_of_format out_fmt) ;
  let open Unix in
  if in_fname = out_fname then
    failwith "Input and output files must be distinct" ;
  let orc_handler = ref None
  and out_rb = ref None and in_rb = ref None
  and in_fd = ref None and out_fd = ref None
  in
  let reader =
    match in_fmt with
    | CodeGenLib_Casing.CSV ->
        let fd = openfile in_fname [O_RDONLY] 0o644 in
        in_fd := Some fd ;
        (fun k ->
          read_lines fd |>
          Enum.iter (fun line ->
            let strings = strings_of_csv Default.csv_separator line in
            match tuple_of_strings strings with
            | exception e ->
              !logger.error "Cannot parse line %S: %s"
                line (Printexc.to_string e)
            | tuple ->
              k tuple))
    | CodeGenLib_Casing.ORC ->
        (fun f ->
          let num_lines, num_errs = orc_read in_fname 1000 f in
          if num_errs <> 0 then
            !logger.error "%d/%d errors" num_errs num_lines)
    | CodeGenLib_Casing.RB ->
        let rb = RingBuf.load in_fname in
        in_rb := Some rb ;
        read_whole_archive read_tuple rb
  and writer =
    match out_fmt with
    | CodeGenLib_Casing.CSV ->
        let fd = openfile out_fname [O_CREAT;O_EXCL;O_WRONLY] 0o644 in
        out_fd := Some fd ;
        csv_write fd
    | CodeGenLib_Casing.ORC ->
        let hdr = orc_make_handler out_fname in
        orc_handler := Some hdr ;
        orc_write hdr
    | CodeGenLib_Casing.RB ->
        RingBuf.create ~wrap:false out_fname ;
        let rb = RingBuf.load out_fname in
        out_rb := Some rb ;
        let head = RingBufLib.DataTuple RamenChannel.live in
        let head_sz = RingBufLib.message_header_sersize head in
        (fun tuple ->
          let start_stop = time_of_tuple tuple in
          let start, stop = start_stop |? (0., 0.) in
          let sz = head_sz + sersize_of_tuple all_fields tuple in
          let tx = RingBuf.enqueue_alloc rb sz in
          RingBufLib.(write_message_header tx 0 head) ;
          let offs = serialize_tuple all_fields tx head_sz tuple in
          RingBuf.enqueue_commit tx start stop ;
          assert (offs <= sz))
  in
  reader writer ;
  Option.may RingBuf.unload !out_rb ;
  Option.may RingBuf.unload !in_rb ;
  Option.may orc_close !orc_handler
