(* Interface between workers and the confserver.  *)
open Batteries
open Stdint
open RamenLog
open RamenHelpers
open RamenConsts
open RamenSync
module ZMQClient = RamenSyncZMQClient

open Binocle

(* Statistics related to tuple publishing *)
let stats_num_subscribers =
  IntGauge.make Metric.Names.num_subscribers
    Metric.Docs.num_subscribers

let stats_num_rate_limited_unpublished =
  IntCounter.make Metric.Names.num_rate_limited_unpublished
    Metric.Docs.num_rate_limited_unpublished

let on_new _clt k _v _uid _mtime _can_write _can_del _owner _expiry =
  match k with
  | Key.Tails (_, _, _, Subscriber uid) ->
      !logger.info "New subscriber: %s" uid ;
      (* TODO: upgrade binocle
      IntGauge.inc stats_num_subscribers *)
      let _mi, c, _ma = IntGauge.get stats_num_subscribers |? (0, 0, 0) in
      IntGauge.set stats_num_subscribers (c + 1)
  | _ -> ()

let on_del _clt k _v =
  match k with
  | Key.Tails (_, _, _, Subscriber uid) ->
      !logger.info "Leaving subscriber: %s" uid ;
      (* TODO: upgrade binocle
      IntGauge.dec stats_num_subscribers *)
      let _mi, c, _ma = IntGauge.get stats_num_subscribers |? (0, 0, 0) in
      IntGauge.set stats_num_subscribers (c - 1)
  | _ -> ()

(*
 * [async_thread] handle all communication with the confserver asynchronously.
 * It's then easier to deal with timeouts and pings.
 *)

let cmd_queue = ref []  (* TODO: a Pfds ring-like data structure *)
let cmd_queue_lock = Mutex.create ()
let cmd_queue_not_empty = Condition.create ()

let add_cmd cmd =
  !logger.debug "Enqueing a new command: %a"
    Client.CltMsg.print_cmd cmd ;
  with_lock cmd_queue_lock (fun () ->
    cmd_queue := cmd :: !cmd_queue ;
    Condition.signal cmd_queue_not_empty) ;
  !logger.debug "Done enqueing command"

let async_thread ~while_ ?on_new ?on_del url topics =
  !logger.info "async_thread: Starting" ;
  (* Simulate a Condition.timedwait by pinging the condition every so often: *)
  let rec wakeup_every t =
    Thread.delay t ;
    Condition.signal cmd_queue_not_empty ;
    if while_ () then wakeup_every t in
  let alarm_thread = Thread.create wakeup_every 1. in
  (* The async loop: *)
  let rec loop () =
    !logger.debug "async_thread: looping" ;
    if while_ () then (
      !logger.debug "async_thread: Waiting for commands" ;
      let cmds =
        with_lock cmd_queue_lock (fun () ->
          while while_ () && !cmd_queue = [] do
            Condition.wait cmd_queue_not_empty cmd_queue_lock ;
            ZMQClient.process_in ~while_ ()
          done ;
          let cmds = !cmd_queue in
          cmd_queue := [] ;
          cmds) in
      !logger.debug "async_thread: Got %d commands" (List.length !cmd_queue) ;
      List.iter (fun cmd ->
        !logger.debug "async_thread: Sending conftree command %a"
          Client.CltMsg.print_cmd cmd ;
        ZMQClient.send_cmd ~while_ cmd
      ) (List.rev cmds) ;
      loop ())
  in
  (* Now that we are in the right thread where to speak ZMQ, start the sync. *)
  let srv_pub_key = getenv ~def:"" "sync_srv_pub_key"
  and username = getenv ~def:"worker" "sync_username"
  and clt_pub_key = getenv ~def:"" "sync_clt_pub_key"
  and clt_priv_key = getenv ~def:"" "sync_clt_priv_key" in
  ZMQClient.start ~while_ ~url ~srv_pub_key
                  ~username ~clt_pub_key ~clt_priv_key
                  ~topics ?on_new ?on_del
                  (* 0 as timeout means not blocking: *)
                  ~recvtimeo:0. ~sndtimeo:0. (fun _clt ->
    loop ()) ;
  Thread.join alarm_thread

(*
 * Those functions does not actually send any command but enqueue them
 * for [async_thread] to send.
 *)

(* Write a tuple into some key *)
let publish_tuple key sersize_of_tuple serialize_tuple mask tuple =
  if !ZMQClient.zmq_session = None then
    !logger.warning "Not connected to confserver, Cannot publish tuple"
  else
    let ser_len = sersize_of_tuple mask tuple in
    let tx = RingBuf.bytes_tx ser_len in
    serialize_tuple mask tx 0 tuple ;
    let values = RingBuf.read_raw_tx tx in
    let v = Value.Tuple { skipped = 0 ; values } in
    add_cmd (Client.CltMsg.SetKey (key, v)) ;
    !logger.info "Serialized a tuple of %d bytes into %a"
      ser_len Key.print key

let delete_key key =
  !logger.info "Deleting publishing key %a" Key.print key ;
  add_cmd (Client.CltMsg.DelKey key)

(* This is called for every output tuple. It is enough to read sync messages
 * that infrequently, as long as we subscribe only to this low frequency
 * topic and received messages can only impact what this function does. *)
let may_publish_tail key_of_seq =
  let next_seq = ref (Random.bits ()) in
  fun sersize_of_tuple serialize_tuple skipped tuple ->
    (* Now publish (if there are subscribers) *)
    match IntGauge.get stats_num_subscribers with
    | Some (_mi, num, _ma) when num > 0 ->
        IntCounter.add stats_num_rate_limited_unpublished skipped ;
        (* TODO: *)
        let mask = RamenFieldMask.all_fields in
        let ser_len = sersize_of_tuple mask tuple in
        let tx = RingBuf.bytes_tx ser_len in
        serialize_tuple mask tx 0 tuple ;
        let values = RingBuf.read_raw_tx tx in
        let v = Value.Tuple { skipped ; values } in
        let seq = !next_seq in
        incr next_seq ;
        let k = key_of_seq seq in
        add_cmd (Client.CltMsg.NewKey (k, v, 0.))
    | _ -> ()

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
          tot_out_tuples =
            Uint64.add init.tot_out_tuples stats.tot_out_tuples ;
          tot_full_bytes =
            Uint64.add init.tot_full_bytes stats.tot_full_bytes ;
          tot_full_bytes_samples =
            Uint64.add init.tot_full_bytes_samples
                       stats.tot_full_bytes_samples ;
          cur_groups =
            Uint64.add init.cur_groups stats.cur_groups ;
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
  add_cmd (Client.CltMsg.SetKey (stats_key, v))

(* FIXME: the while_ function given here must be reentrant! *)
let start_zmq_client_simple ~while_ ?on_new ?on_del url topics =
  Thread.create (async_thread ~while_ ?on_new ?on_del url) topics |> ignore

let start_zmq_client ~while_ (site : N.site) (fq : N.fq) instance k =
  let url = getenv ~def:"" "sync_url" in
  if url = "" then k ignore4 ignore else
  let topic_sub =
    "tails/"^ (site :> string) ^"/"^ (fq :> string) ^"/"^
    instance ^"/users/*"
  and topic_pub seq =
    Key.(Tails (site, fq, instance, LastTuple seq)) in
  let topics = [ topic_sub ] in
  start_zmq_client_simple ~while_ ~on_new ~on_del url topics ;
  (* Wait for initial synchronization: *)
  ZMQClient.wait_session () ;
  fun conf ->
    let publish_tail = may_publish_tail topic_pub in
    let stats_key =
      Key.(PerSite (site, PerWorker (fq, RuntimeStats))) in
    let init_stats =
      let session = ZMQClient.get_session () in
      match (Client.find session.ZMQClient.clt stats_key).value with
      | exception Not_found ->
          None
      | Value.RuntimeStats s ->
          Some s
      | v ->
          !logger.error "Invalid type for %a: %a"
            Key.print stats_key
            Value.print v ;
          None in
    let publish_stats = publish_stats stats_key init_stats in
    k publish_tail publish_stats conf
