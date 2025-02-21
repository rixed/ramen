(* This module implements the part of a worker that connects to the confserver
 * and output tuples in every requested destinations.
 * When a worker wants to commit a tuple it just calls the function returned
 * by [make_outputer].
 * It is then forwarded to each taillers and configured outputs.
 *
 * The communication with the confserver itself happens in a separate thread,
 * and there is a queue of commands ([cmd_queue]) for outgoing messages to the
 * confserver.
 *
 * For tuple outputs, this module maintains a hash of outputing functions
 * indexed by recipients ([outputers]).
 *)
open Batteries
open Binocle
open DessserOCamlBackEndHelpers
open Stdint

open RamenConsts
open RamenHelpersNoLog
open RamenHelpers
open RamenLog
open RamenSync
module C = CodeGenLib_Config
module CltCmd = Sync_client_cmd.DessserGen
module DT = DessserTypes
module DO = Output_specs.DessserGen
module DWO = Output_specs_wire.DessserGen
module Factors = CodeGenLib_Factors
module FieldMask = RamenFieldMask
module Files = RamenFiles
module OutRef = RamenOutRef
module Stats = CodeGenLib_Stats
module VOS = Value.OutputSpecs
module ZMQClient = RamenSyncZMQClient

(*
 * [async_thread] handle all communication with the confserver asynchronously.
 * It's then easier to deal with timeouts and pings.
 *)

(* FIXME: now that TcpSocket.send is asynchronous, this is useless: *)
let cmd_queue = ref []  (* TODO: a Pfds ring-like data structure *)
let cmd_queue_lock = Mutex.create ()
let cmd_queue_not_empty = Condition.create ()

let add_cmd cmd =
  !logger.debug "Enqueuing a new command: %a"
    Client.CltMsg.print_cmd cmd ;
  with_lock cmd_queue_lock (fun () ->
    cmd_queue := cmd :: !cmd_queue ;
    Condition.signal cmd_queue_not_empty) ;
  !logger.debug "Done enqueuing command"

(* Store all tuple batch per response key.
 * value is a count and a list (in reverse order) *)
let tuple_batches = Hashtbl.create 10

let send_tuple_batch key (n, tuples) =
  let tuples = array_of_list_rev n tuples in
  add_cmd (CltCmd.SetKey (key, Value.Tuples tuples))

let batch_tuple key tuple =
  Hashtbl.modify_opt key (function
    | None -> Some (1, [ tuple ])
    | Some (n, tuples) ->
        let n = n + 1
        and tuples = tuple :: tuples in
        if n >= max_tuples_per_batch then (
          send_tuple_batch key (n, tuples) ;
          None
        ) else (
          Some (n, tuples)
        )
  ) tuple_batches

let flush_all_batches () =
  !logger.debug "Flushing pending tuple batches" ;
  Hashtbl.iter send_tuple_batch tuple_batches ;
  Hashtbl.clear tuple_batches

let flush_batch key =
  !logger.debug "Flush_batch for %a" Key.print key ;
  hashtbl_take tuple_batches key |>
  Option.may (send_tuple_batch key)

(* The list of channels for which this replayer is in charge of deleting the replay
 * and response keys: *)
let del_when_done = ref []

(* Condvar dance to hand on the initial value of stats (right after the
 * initial sync) to the worker thread: *)
let init_stats = ref None
let init_stats_set = ref false
let wait_init_stats_cond = Condition.create ()
let wait_init_stats_lock = Mutex.create ()
let wait_init_stats while_ =
  with_lock wait_init_stats_lock (fun () ->
    while while_ () && not !init_stats_set do
      Condition.wait wait_init_stats_cond wait_init_stats_lock
    done) ;
  !init_stats

let stats_key conf =
  Key.(PerSite (conf.C.site, PerWorker (conf.C.fq, RuntimeStats)))

let set_init_stats conf session =
  let clt = option_get "set_init_stats" __LOC__ session.ZMQClient.clt in
  let stats_key = stats_key conf in
  init_stats :=
    (match (Client.find clt stats_key).value with
    | exception Not_found ->
        None
    | Value.RuntimeStats s ->
        Some s
    | v ->
        err_sync_type stats_key v  "runtime stats" ;
        None) ;
  with_lock wait_init_stats_lock (fun () ->
    init_stats_set := true ;
    Condition.broadcast wait_init_stats_cond)

(* Flag to command the async_thread to exit *)
let quit_async_thd = ref false

(* [async_thread] will set the quit flag whenever the connection with the
 * confserver is broken, but keep running independently of that quit flag so
 * that it does not prematurely disconnect from the confserver before the main
 * thread have a chance to send a last stats report during the exit process: *)
let quit_on_error ~what ~exit_code quit f =
  try f ()
  with Exit ->
    !logger.debug "%s: Exit" what ;
  | e ->
    print_exception ~what e ;
    match !quit with
    | None ->
        !logger.error "%s: Setting quit flag to confserver_unreachable" what ;
        quit := Some exit_code
    | Some c ->
        !logger.debug "%s: Quitting, exit code already set to %d" what c

let async_thread conf quit ?on_new ?on_del ?on_set url topics =
  !logger.debug "async_thread: Starting" ;
  let while_ () = !quit_async_thd = false in
  (* Simulate a Condition.timedwait by pinging the condition every so often: *)
  let rec wakeup_every t =
    Thread.delay t ;
    Condition.signal cmd_queue_not_empty ;
    if while_ () then wakeup_every t in
  let alarm_thread = Thread.create wakeup_every 1. in
  let flush_commands session cmds =
    !logger.debug "async_thread: Got %d commands" (List.length cmds) ;
    (* Do not stop sending commands when the quit flag is set: *)
    let what = "sending confserver cmd"
    and exit_code = ExitCodes.confserver_unreachable in
    quit_on_error ~what ~exit_code quit (fun () ->
      List.iter (ZMQClient.send_cmd session) (List.rev cmds)) in
  let sync_loop session =
    (* Now that the sync is over, get the initial stats: *)
    set_init_stats conf session ;
    (* Now the normal loop: *)
    while while_ () || !cmd_queue <> [] do
      !logger.debug "async_thread: Waiting for commands" ;
      let cmds =
        with_lock cmd_queue_lock (fun () ->
          while while_ () && !cmd_queue = [] do
            Condition.wait cmd_queue_not_empty cmd_queue_lock ;
            (* We cannot recurse in the process_in callbacks with the lock,
             * since cmd_add could be called and try to reacquire that lock *)
            let what = "processing ZMQ input"
            and exit_code = ExitCodes.confserver_unreachable in
            without_lock cmd_queue_lock (fun () ->
              quit_on_error ~what ~exit_code quit (fun () ->
                ZMQClient.process_in ~while_ session))
          done ;
          let cmds = !cmd_queue in
          cmd_queue := [] ;
          cmds) in
      flush_commands session cmds
    done ;
    flush_commands session !cmd_queue
  in
  (* Now that we are in the right thread where to speak ZMQ, start the sync. *)
  let srv_pub_key = getenv ~def:"" "sync_srv_pub_key"
  and username = getenv ~def:"worker" "sync_username"
  and clt_pub_key = getenv ~def:"" "sync_clt_pub_key"
  and clt_priv_key = getenv ~def:"" "sync_clt_priv_key"
  and exit_code = ExitCodes.confserver_unreachable in
  quit_on_error ~what:"Publish.async_thread" ~exit_code quit (fun () ->
    ZMQClient.start ~while_ ~url ~srv_pub_key
                    ~username ~clt_pub_key ~clt_priv_key
                    ~topics ?on_new ?on_del ?on_set
                    (* 0 as timeout means not blocking: *)
                    ~recvtimeo:0. sync_loop) ;
  (* In case we ended up here because of some error then also terminate the
   * alarm_thread: *)
  !logger.debug "async_thread: Waiting for alarm_thread..." ;
  quit_async_thd := true ;
  Thread.join alarm_thread ;
  !logger.debug "async_thread: done!"

(* This is called for every output tuple. It is enough to read sync messages
 * that infrequently, as long as we subscribe only to this low frequency
 * topic and received messages can only impact what this function does.
 * [ocamlify_tuple] turns the internal representation of a tuple into a
 * T.value. *)
let may_publish_tail conf ocamlify_tuple =
  let next_seq = ref (Random.bits ()) in
  let topic_pub seq =
    Key.(Tails (conf.C.site, conf.C.fq, conf.C.instance, LastTuple seq)) in
  fun skipped tuple ->
    (* Broadcast the tuple if there are subscribers: *)
    match IntGauge.get Stats.num_subscribers with
    | Some (_mi, num, _ma) when num > 0 ->
        IntCounter.add Stats.num_rate_limited_unpublished skipped ;
        let skipped = Uint32.of_int skipped
        and values = ocamlify_tuple tuple in
        let v = Value.Tuples [| { skipped ; values } |] in
        let seq = !next_seq in
        incr next_seq ;
        let k = topic_pub (Uint32.of_int seq) in
        add_cmd (CltCmd.SetKey (k, v))
    | _ -> ()

(*
 * Output functions
 *)

(* A reverse mapping from the keys used as target of IndirectFiles to the
 * original recipient in the outref, so we know which outputer to stop
 * when that key is deleted: *)
let indirect_files : (Key.t, VOS.recipient) Hashtbl.t =
  Hashtbl.create 10

(* Check that a tuple pass the early-filters.
 * TODO: compute each test failure likelihood and start with the more likely
 * to fail *)
let filter_tuple scalar_extractors filters tuple =
  Array.for_all (fun (idx, vs) ->
    let v = scalar_extractors.(Uint16.to_int idx) tuple in
    Array.mem v vs
  ) filters

(* For non-wrapping buffers we need to know the value for the time, as
 * the min/max times per slice are saved, along the first/last tuple
 * sequence number. *)
let output_to_rb rb serialize_tuple sersize_of_tuple fieldmask
                 (* Those last parameters change at every tuple: *)
                 start_stop head tuple_opt =
  let open RingBuf in
  let tuple_sersize =
    Option.map_default (sersize_of_tuple fieldmask) 0 tuple_opt in
  let sersize = RingBufLib.message_header_sersize head + tuple_sersize in
  (* Nodes with no output (but notifications) have no business writing
   * a ringbuf. Want a signal when a notification is sent? SELECT some
   * value! *)
  if tuple_opt = None (* ie sending a message *) || tuple_sersize > 0 then (
    IntCounter.add Stats.write_bytes sersize ;
    let tx = enqueue_alloc rb sersize in
    let offs =
      RingBufLib.write_message_header tx 0 head ;
      RingBufLib.message_header_sersize head in
    let offs' =
      match tuple_opt with
      | Some tuple -> serialize_tuple fieldmask tx offs tuple
      | None -> offs in
    (* start = stop = 0. => times are unset *)
    let start, stop = Option.default (0., 0.) start_stop in
    enqueue_commit tx start stop ;
    if offs' <> sersize then
      !logger.error "Outputing to %d@%s, offs=%d whereas sersize=%d"
        offs (RingBuf.tx_fname tx) offs' sersize ;
    assert (offs' = sersize)
  )

type 'a out_rb =
  { fname : N.path ;
    (* To detect when the file that's been mmapped has been replaced on disk
     * by a newer one with the same name: *)
    inode : int ;
    rb : RingBuf.t ;
    (* As configured for that ringbuffer: *)
    timeout : float ;
    tup_filter : 'a -> bool ;
    mutable last_successful_output : float ;
    mutable quarantine_until : float ;
    mutable quarantine_delay : float ;
    rate_limit_log_writes : unit -> bool ;
    rate_limit_log_drops : unit -> bool }

let write_to_rb ~while_ out_rb file_spec
                serialize_tuple sersize_of_tuple
                dest_channel start_stop head tuple_opt =
  let print_event_time =
    Option.print (Tuple2.print print_as_date print_as_date) in
  if dest_channel <> Channel.live && out_rb.rate_limit_log_writes () then
    !logger.debug "Write a %s to channel %a"
      (if tuple_opt = None then "message"
       else ("tuple of etime="^ IO.to_string print_event_time start_stop))
      Channel.print dest_channel ;
  (* Note: we retry only on NoMoreRoom so that's OK to keep trying; in
   * case the ringbuf disappear altogether because the child is
   * terminated then we won't deadloop.  Also, if one child is full
   * then we will not write to next children until we can eventually
   * write to this one. This is actually desired to have proper message
   * ordering along the stream and avoid ending up with many threads
   * retrying to write to the same child. *)
  retry
    ~on:(function
      | RingBuf.NoMoreRoom ->
        !logger.debug "NoMoreRoom in %a" N.path_print out_rb.fname ;
        (* Can't use CodeGenLib.now if we are stuck in output: *)
        let now = Unix.gettimeofday () in
        (* Also check from time to time that we are still supposed to
         * write in there (we check right after the first error to
         * quickly detect it when a child disappear): *)
        (
          now < out_rb.last_successful_output +. out_rb.timeout || (
            (* At this point, we have been failing for a good while
             * for a child that's still in our out_ref, and should
             * consider quarantine for a bit: *)
            out_rb.quarantine_delay <-
              min max_ringbuf_quarantine (10. +. out_rb.quarantine_delay *. 1.5) ;
            out_rb.quarantine_until <-
              now +. jitter out_rb.quarantine_delay ;
            (if out_rb.quarantine_delay >= max_ringbuf_quarantine *. 0.7 then
              !logger.debug else !logger.warning)
              "Quarantining output to %a until %s"
              N.path_print out_rb.fname
              (string_of_time out_rb.quarantine_until) ;
            true)
        )
      | _ -> false)
    ~while_ ~first_delay:0.001 ~max_delay:1. ~delay_rec:Stats.sleep_out
    (fun () ->
      match Hashtbl.find file_spec.DO.channels dest_channel with
      | exception Not_found ->
          (* Can happen at leaf functions after a replay, or when replaying
           * a specific channel that the outref does not accept: *)
          if out_rb.rate_limit_log_drops () then
            !logger.debug "Drop a tuple for %a not interested in channel %a"
              N.path_print out_rb.fname Channel.print dest_channel ;
      | timeo, _num_sources, _pids ->
          if not (OutRef.timed_out !CodeGenLib.now timeo) then (
            if out_rb.quarantine_until < !CodeGenLib.now then (
              if Option.map_default out_rb.tup_filter true tuple_opt then (
                output_to_rb
                  out_rb.rb serialize_tuple sersize_of_tuple
                  file_spec.DO.fieldmask start_stop head tuple_opt ;
                out_rb.last_successful_output <- !CodeGenLib.now ;
                if out_rb.quarantine_delay > 0. then (
                  !logger.info "Resuming output to %a"
                    N.path_print out_rb.fname ;
                  out_rb.quarantine_delay <- 0.)
              ) else ( (* tuple did not pass filter *)
                !logger.debug "Skipping output to %a (filtered)"
                  N.path_print out_rb.fname ;
                IntCounter.inc Stats.out_filtered_count
              )
            ) else ( (* Still in quarantine *)
              !logger.debug "Skipping output to %a (quarantined)"
                N.path_print out_rb.fname ;
              IntCounter.inc Stats.out_quarantined_count
            )
          ) else ( (* output spec timed out *)
            if out_rb.rate_limit_log_drops () then
              !logger.debug "Drop a tuple for %a outdated channel %a"
                N.path_print out_rb.fname Channel.print dest_channel
          )) ()

let writer_to_file ~while_ fname spec scalar_extractors
                   serialize_tuple sersize_of_tuple
                   orc_make_handler orc_write orc_close =
  match spec.DO.file_type with
  | RingBuf ->
      let rb = RingBuf.load fname
      and inode = Files.inode fname in
      let stats = RingBuf.stats rb in
      let out_rb =
        { fname ; inode ; rb ; timeout = stats.timeout ;
          tup_filter = filter_tuple scalar_extractors spec.filters ;
          last_successful_output = 0. ;
          quarantine_until = 0. ;
          quarantine_delay = 0. ;
          rate_limit_log_writes = rate_limiter 10 1. ;
          rate_limit_log_drops = rate_limiter 10 1. } in
      (fun file_spec dest_channel start_stop head tuple_opt ->
          try write_to_rb ~while_ out_rb file_spec
                          serialize_tuple sersize_of_tuple
                          dest_channel start_stop head tuple_opt
          with
            (* Retry failed with NoMoreRoom. It is OK, just skip it.
             * Next tuple we will reread fname if it has changed. *)
            | RingBuf.NoMoreRoom -> ()
            (* Retry had quit because either the worker has been terminated or
             * the recipient is no more in our out_ref: *)
            | Exit -> ()),
      (fun () ->
        RingBuf.may_archive_and_unload rb)
  | Orc { with_index ; batch_size ; num_batches } ->
      let batch_size = Uint32.to_int batch_size
      and num_batches = Uint32.to_int num_batches in
      let hdr =
        orc_make_handler fname with_index batch_size num_batches true in
      (fun file_spec dest_channel start_stop head tuple_opt ->
        match head, tuple_opt with
        | RingBufLib.DataTuple chn, Some tuple ->
            assert (chn = dest_channel) ; (* by definition *)
            (match Hashtbl.find file_spec.DO.channels chn with
            | exception Not_found -> ()
            | timeo, _num_sources, _pids ->
                if not (OutRef.timed_out !CodeGenLib.now timeo) then
                  let start, stop = Option.default (0., 0.) start_stop in
                  orc_write hdr tuple start stop)
        | _ -> ()),
      (fun () -> orc_close hdr)

(* Write a tuple into some key
 * Those functions does not actually send any command but enqueue them
 * for [async_thread] to send. *)
let publish_tuple key mask tuple =
  ignore mask ; (* TODO: use the mask to reduce the tuple! *)
  let tuple = Value.{ skipped = Uint32.zero ; values = tuple } in
  !logger.debug "Published a tuple into %a" Key.print key ;
  batch_tuple key tuple

let delete_key key =
  !logger.info "Deleting key %a" Key.print key ;
  add_cmd (CltCmd.DelKey key)

(* Deletes both the recipient response key and the replay for that channel: *)
let delete_replay chn =
  delete_key (Key.Replays chn)

(* Save the number of sources per channels *)
let num_sources_per_channel = Hashtbl.create 10

let writer_to_sync conf key spec ocamlify_tuple =
  let publish = publish_tuple key spec.DO.fieldmask in
  (fun file_spec dest_channel _start_stop head tuple_opt ->
    match head, tuple_opt with
    | RingBufLib.DataTuple chn, Some tuple ->
        assert (chn = dest_channel) ; (* by definition *)
        (match Hashtbl.find file_spec.DO.channels chn with
        | exception Not_found -> ()
        | timeo, _num_sources, _pids ->
            if not (OutRef.timed_out !CodeGenLib.now timeo)
            then
              publish (ocamlify_tuple tuple))
    | RingBufLib.EndOfReplay (chn, _replayer_id), None ->
        assert (chn = dest_channel) ; (* by definition *)
        if conf.C.is_replayer then (
          (* Check this file is interested in [chn]: *)
          if Hashtbl.mem file_spec.DO.channels chn then (
            (* Replayers do not count EndOfReplay messages, as the only one they
             * will ever see is the one they publish themselves. *)
            flush_batch key ;
            !logger.debug "del when done with %a? %b"
              Channel.print chn
              (List.mem chn !del_when_done) ;
            if List.mem chn !del_when_done then (
              delete_replay chn ;
              delete_key key
            )
          )
        ) else (
          Hashtbl.modify_opt chn (fun prev ->
            let terminate () =
              flush_batch key ;
              delete_replay chn ;
              delete_key key ;
              None in
            match prev with
            | None ->
                (* First time we receive an end-of-channel, lets record how
                 * many sources must terminate before we delete the resp key
                 * and the replay: *)
                (match Hashtbl.find file_spec.DO.channels chn with
                | exception Not_found ->
                    !logger.info "Received an end-of-channel message for \
                                  unknown channel %a"
                      Channel.print chn ;
                    None
                | _timeo, num_sources, _pids ->
                    let left = Int16.to_int num_sources - 1 in
                    !logger.info "Received an end-of-channel message for \
                                  channel %a, waiting for %s more"
                      Channel.print chn
                      (Int16.to_string num_sources) ;
                    if left <= 0 then terminate () else Some left)
            | Some num ->
                let left = num - 1 in
                (if left < 0 then !logger.error else
                 if left = 0 then !logger.info else
                 !logger.debug)
                  "Still waiting for %d sources on channel %a"
                  left Channel.print chn ;
                (* If this process is a normal worker and it's writing into the
                 * confserver, then it must be the target of the replay.
                 * It therefore must close the response key when all sources
                 * have been read in full: *)
                if left <= 0 then terminate () else Some left
          ) num_sources_per_channel
        )
    | _ -> ()),
  (fun () ->
    (* It might have been deleted already though: *)
    flush_batch key)

let publish_stats stats_key init_stats stats =
  (* Those stats are the stats since startup. Combine them with whatever was
   * present previously: *)
  let tot_stats =
    match init_stats with
    | None ->
        stats
    | Some init ->
        Value.RuntimeStats.{
          stats_time = stats.stats_time ;
          first_startup = init.first_startup ;
          last_startup = stats.last_startup ;
          min_etime = min stats.min_etime init.min_etime ;
          max_etime = max stats.max_etime init.max_etime ;
          first_input = init.first_input ;
          last_input = stats.last_input ;
          first_output = init.first_output ;
          last_output = stats.last_output ;
          tot_in_tuples =
            Uint64.add init.tot_in_tuples stats.tot_in_tuples ;
          tot_sel_tuples =
            Uint64.add init.tot_sel_tuples stats.tot_sel_tuples ;
          tot_out_filtered =
            Uint64.add init.tot_out_filtered stats.tot_out_filtered ;
          tot_out_tuples =
            Uint64.add init.tot_out_tuples stats.tot_out_tuples ;
          tot_out_errs =
            Uint64.add init.tot_out_errs stats.tot_out_errs ;
          tot_full_bytes =
            Uint64.add init.tot_full_bytes stats.tot_full_bytes ;
          tot_full_bytes_samples =
            Uint64.add init.tot_full_bytes_samples
                       stats.tot_full_bytes_samples ;
          cur_groups =
            Uint64.add init.cur_groups stats.cur_groups ;
          max_groups = max init.max_groups stats.max_groups ;
          tot_in_bytes =
            Uint64.add init.tot_in_bytes stats.tot_in_bytes ;
          tot_out_bytes =
            Uint64.add init.tot_out_bytes stats.tot_out_bytes ;
          tot_wait_in = init.tot_wait_in +. stats.tot_wait_in ;
          tot_wait_out = init.tot_wait_out +. stats.tot_wait_out ;
          tot_firing_notifs =
            Uint64.add init.tot_firing_notifs stats.tot_firing_notifs ;
          tot_extinguished_notifs =
            Uint64.add init.tot_extinguished_notifs
                       stats.tot_extinguished_notifs ;
          tot_cpu = init.tot_cpu +. stats.tot_cpu ;
          cur_ram = stats.cur_ram ;
          max_ram = max init.max_ram stats.max_ram } in
  let v = Value.RuntimeStats tot_stats in
  add_cmd (CltCmd.SetKey (stats_key, v))

let notify ?(test=false) site worker event_time parameters name =
  let firing, certainty, debounce, timeout, parameters =
    RingBufLib.normalize_notif_parameters parameters in
  IntCounter.inc (if firing then Stats.firing_notif_count
                            else Stats.extinguished_notif_count) ;
  let notif = Value.Alerting.Notification.{
    site ; worker ; test ; sent_time = !CodeGenLib.now ; event_time ; name ;
    firing ; certainty ; debounce ; timeout ; parameters } in
  add_cmd (CltCmd.SetKey (Key.Notifications, Value.Notification notif))

let async_thd = ref None

(* Wait until all cmds have been sent: *)
let stop () =
  flush_all_batches () ;
  quit_async_thd := true ;
  !logger.debug "Waiting for the end of async thread" ;
  Option.may Thread.join !async_thd

(* This function cannot easily be split because the type of tuples must not
 * be allowed to escape: *)
let start_zmq_client conf ~while_ quit
                     time_of_tuple factors_of_tuple scalar_extractors
                     serialize_tuple sersize_of_tuple ocamlify_tuple
                     orc_make_handler orc_write orc_close =
  (* Prepare for tailing: *)
  let publish_tail = may_publish_tail conf ocamlify_tuple in
  (* Rate limit of the tail: In average not more than 1 message per second,
   * but allow 60 msgs per minute to allow some burst: *)
  let rate_limited_tail =
    let avg = min_delay_between_publish in
    rate_limiter (int_of_float (60. *. avg)) avg
  and num_skipped_between_publish = ref 0 in
  (* Prepare for saving factors: *)
  let max_num_fields = 100 (* FIXME *) in
  let factors_values =
    Array.make max_num_fields Factors.possible_values_empty in
  let factors_dir =
    N.path (getenv ~def:(Files.tmp_dir ^"/factors") "factors_dir") in
  (* Prepare for measuring average full tuple size: *)
  let last_full_out_measurement = ref 0. in
  (* The outputer hash specialized for the given type of tuples: *)
  let outputers = ref (Hashtbl.create 10)
  and outputers_lock = Mutex.create () in
  (*
   * The callbacks updating the outputers:
   *)
  (* Function called by the ZMQ thread with the output specifications each time
   * they are changed *)
  let update_outputers_for_out_specs session out_specs =
    let clt = option_get "update_outputers" __LOC__ session.ZMQClient.clt in
    (* Compute the new outputers hash without interfering with the worker thread
     * that may iterate it: *)
    let merge_out_spec rcpt prev new_ =
      match prev, new_ with
      | None, Some new_spec ->
          (*
           * New entry in output specifications:
           *)
          !logger.info "Start outputting to %a"
            VOS.recipient_print rcpt ;
          (* Workers are not in the business of editing their out_ref,
           * but should still protect against stale entries. *)
          (try
            let writer_closer_opt =
              match rcpt with
              | DWO.DirectFile fname ->
                  Some (writer_to_file ~while_ fname new_spec scalar_extractors
                                       serialize_tuple sersize_of_tuple
                                       orc_make_handler orc_write orc_close)
              | DWO.IndirectFile k ->
                  (* Same as DirectFile but we detect ourselves when to stop
                   * the output *)
                  let k = RamenSync.Key.of_string k in
                  (match (Client.find clt k).value with
                  | exception Not_found ->
                      !logger.error "Cannot find IndirectFile %a" Key.print k ;
                      None
                  | Value.RamenValue (Raql_value.VString str) ->
                      Hashtbl.add indirect_files k rcpt ;
                      let fname = N.path str in
                      Some (writer_to_file ~while_ fname new_spec scalar_extractors
                                           serialize_tuple sersize_of_tuple
                                           orc_make_handler orc_write orc_close)
                  | v ->
                      err_sync_type k v "a string" ;
                      None)
              | DWO.SyncKey k ->
                  Some (writer_to_sync conf k new_spec ocamlify_tuple) in
            Option.map (fun (writer, closer) ->
              new_spec,
              writer, closer
            ) writer_closer_opt
          with exn ->
            let what =
              Printf.sprintf2 "preparing output to %a"
                VOS.recipient_print rcpt in
            print_exception ~what exn ;
            None)
      | Some (_, _, closer), None ->
          (*
           * Deletion of a output specification:
           *)
          !logger.info "Stop outputting to %a"
            VOS.recipient_print rcpt ;
          closer () ;
          None
      | Some (cur_spec, writer, closer),
        Some new_spec ->
          (*
           * Some entry that was already present.
           *)
          !logger.debug "Update channel output configuration to %a"
            VOS.recipient_print rcpt ;
          OutRef.check_spec_change rcpt cur_spec new_spec ;
          (* The only allowed change is channels: *)
          Some ({ cur_spec with channels = new_spec.channels },
                writer, closer)
      | None, None ->
          assert false
    in
    with_lock outputers_lock (fun () ->
      (* Warn whenever the output specs become empty (ie worker computes for
       * nothing and should actually be stopped). *)
      if Hashtbl.is_empty out_specs then (
        if not (Hashtbl.is_empty !outputers) then
          !logger.debug "No more outputers!"
      ) else (
        if Hashtbl.is_empty !outputers then
          !logger.debug "Outputers no longer empty!" ;
        !logger.debug "Must now output to: %a"
          VOS.print_out_specs out_specs
      ) ;
      let outputers' = hashtbl_merge !outputers out_specs merge_out_spec in
      let prev_num_outputers = Hashtbl.length !outputers in
      let num_outputers = Hashtbl.length outputers' in
      if num_outputers <> prev_num_outputers then
        !logger.info "Has now %d outputers (had %d)"
          num_outputers prev_num_outputers ;
      outputers := outputers') in
  (* Called by the ZMQ thread after the key referenced by an IndirectFile
   * has been deleted. Time to terminate the corresponding outputer. *)
  let update_outputers_after_indirect_del k =
    match Hashtbl.find indirect_files k with
    | exception Not_found -> ()
    | rcpt ->
        with_lock outputers_lock (fun () ->
          match Hashtbl.find !outputers rcpt with
          | exception Not_found -> ()
          | _, _, closer ->
              !logger.debug "Stop outputting to IndirectFile %a"
                VOS.recipient_print rcpt ;
              closer () ;
              Hashtbl.remove !outputers rcpt)
  in
  (*
   * The outputer function that's going to be returned to the caller.
   * Must not send anything directly to the confserver, as the zocket is
   * alive in another thread, but merely enqueue the commands.
   *)
  let outputer head tuple_opt =
    let dest_channel = RingBufLib.channel_of_message_header head in
    let start_stop =
      match tuple_opt with
      | Some tuple ->
        let start_stop = time_of_tuple tuple in
        if dest_channel = Channel.live then (
          (* Update stats *)
          IntCounter.inc Stats.out_tuple_count ;
          FloatGauge.set Stats.last_out !CodeGenLib.now ;
          if !CodeGenLib.now -. !last_full_out_measurement >
             min_delay_between_full_out_measurement
          then (
            Stats.measure_full_out
              (sersize_of_tuple FieldMask.all_fields tuple) ;
            last_full_out_measurement := !CodeGenLib.now) ;
          (* If we have subscribers, send them something (rate limited): *)
          if rate_limited_tail ~now:!CodeGenLib.now () then (
            publish_tail !num_skipped_between_publish tuple ;
            num_skipped_between_publish := 0
          ) else
            incr num_skipped_between_publish ;
          (* Update factors possible values: *)
          let start, stop = Option.default (0., end_of_times) start_stop in
          factors_of_tuple tuple |>
          Array.iteri (fun i (factor, pv) ->
            assert (i < Array.length factors_values) ;
            let pvs = factors_values.(i) in
            if not (Set.mem pv pvs.values) then ( (* FIXME: Set.update *)
              !logger.debug "New value for factor %s: %a"
                factor T.print pv ;
              let min_time = min start pvs.min_time in
              let min_time, values, prev_fname =
                if start_stop = None ||
                   stop -. min_time <= possible_values_lifespan
                then
                  min_time, Set.add pv pvs.values, pvs.fname
                else (
                  !logger.info "New factor index file (min_time=%g, stop=%g)"
                    min_time stop ;
                  start, Set.singleton pv, N.path ""
                ) in
              let fname =
                Factors.possible_values_file factors_dir factor min_time in
              let pvs = Factors.{ min_time ; fname ; values } in
              factors_values.(i) <- pvs ;
              (* Warning that this function might in some rare case update
               * pvs.values! *)
              Factors.save_possible_values prev_fname pvs)) ;
          (* Update Stats.event_time: *)
          Option.may (fun (start, _) ->
            (* We'd rather announce the start time of the event, even for
             * negative durations. *)
            FloatGauge.set Stats.event_time start
          ) start_stop) ;
        start_stop
      | None ->
        None in
    Stats.update_output_times () ;
    with_lock outputers_lock (fun () ->
      Hashtbl.iter (fun _ (file_spec, writer, _closer) ->
        (* Also pass file_spec to keep the writer posted about channel
         * changes: *)
        writer file_spec dest_channel start_stop head tuple_opt
      ) !outputers)
  in
  (*
   * Now sync to ZMQ
   *)
  let url = getenv ~def:"" "sync_url" in
  let instance_key suffix =
    "tails/"^ (conf.C.site :> string) ^"/"^ (conf.C.fq :> string) ^"/"^
    conf.C.instance ^ suffix in
  let topic_sub = instance_key "/users/*"
  and topics_indirect_files =
    "sites/"^ (conf.C.site :> string) ^"/workers/*/input_ringbuf"
  in
  (* Workers also track their outref: *)
  let output_topic =
    "sites/"^ (conf.C.site :> string) ^"/workers/"^ (conf.C.fq :> string) ^
    "/outputs" in
  let topics =
    if conf.C.is_replayer then [ output_topic ]
    else [ topic_sub ; topics_indirect_files ; output_topic ] in
  let rec on_new session k v uid mtime _can_write _can_del _owner _expiry =
    match k with
    | Key.Tails (_, _, _, Subscriber uid) ->
        !logger.info "New subscriber: %s" uid ;
        IntGauge.inc Stats.num_subscribers
    | _ ->
        on_set session k v uid mtime
  and on_del _session k _v =
    match k with
    | Key.Tails (_, _, _, Subscriber uid) ->
        !logger.info "Leaving subscriber: %s" uid ;
        IntGauge.dec Stats.num_subscribers
    | Key.(PerSite (_, (PerWorker (_, PerInstance (_, InputRingFile))))) ->
        update_outputers_after_indirect_del k
    | _ -> ()
  and on_set session k v _uid _mtime =
    match k, v with
    | Key.(PerSite (site, PerWorker (fq, OutputSpecs))),
      Value.OutputSpecs out_specs
      when site = conf.site && fq = conf.fq ->
        update_outputers_for_out_specs session out_specs
    | _ -> ()
  in
  (* FIXME: the while_ function given here must be reentrant! *)
  async_thd := Some (
    Thread.create (
      async_thread conf quit ~on_new ~on_del ~on_set url) topics) ;
  (* Wait for initial synchronization and initial stats from the sync thread: *)
  let init_stats = wait_init_stats while_ in
  publish_stats (stats_key conf) init_stats,
  outputer
