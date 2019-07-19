(* Interface between workers and the confserver.  *)
open Batteries
open Stdint
open RamenLog
open RamenHelpers
open RamenConsts
module ZMQClient = RamenSyncZMQClient

open Binocle

(* Statistics related to tuple publishing *)
let stats_num_subscribers =
  IntGauge.make Metric.Names.num_subscribers
    Metric.Docs.num_subscribers

let stats_num_sync_msgs_in =
  IntCounter.make Metric.Names.num_sync_msgs_in
    Metric.Docs.num_sync_msgs_in

let stats_num_sync_msgs_out =
  IntCounter.make Metric.Names.num_sync_msgs_out
    Metric.Docs.num_sync_msgs_out

let stats_num_rate_limited_unpublished =
  IntCounter.make Metric.Names.num_rate_limited_unpublished
    Metric.Docs.num_rate_limited_unpublished

let on_new _clt k _v _uid _mtime _owner _expiry =
  match k with
  | RamenSync.Key.Tails (_, _, Subscriber uid) ->
      !logger.info "New subscriber: %s" uid ;
      (* TODO: upgrade binocle
      IntGauge.inc stats_num_subscribers *)
      let _mi, c, _ma = IntGauge.get stats_num_subscribers |? (0, 0, 0) in
      IntGauge.set stats_num_subscribers (c + 1)
  | _ -> ()

let on_del _clt k _v =
  match k with
  | RamenSync.Key.Tails (_, _, Subscriber uid) ->
      !logger.info "Leaving subscriber: %s" uid ;
      (* TODO: upgrade binocle
      IntGauge.dec stats_num_subscribers *)
      let _mi, c, _ma = IntGauge.get stats_num_subscribers |? (0, 0, 0) in
      IntGauge.set stats_num_subscribers (c - 1)
  | _ -> ()

(* Try to process input messages before any attempt to send anything
 * (non-blocking): *)
let process_in ?while_ clt =
  let msg_count = ZMQClient.process_in ?while_ clt in
  if msg_count > 0 then
    !logger.info "Received %d ZMQ messages" msg_count ;
  IntCounter.add stats_num_sync_msgs_in msg_count

(* This is called for every output tuple. It is enough to read sync messages
 * that infrequently, as long as we subscribe only to this low frequency
 * topic and received messages can only impact what this function does. *)
let may_publish_tail clt ?while_ key_of_seq
                     sersize_of_tuple serialize_tuple skipped tuple =
  (* Process incoming messages without waiting for them: *)
  process_in ?while_ clt ;
  (* Now publish (if there are subscribers) *)
  match IntGauge.get stats_num_subscribers with
  | Some (_mi, num, _ma) when num > 0 ->
      IntCounter.add stats_num_rate_limited_unpublished skipped ;
      IntCounter.inc stats_num_sync_msgs_out ;
      (* TODO: *)
      let mask = RamenFieldMask.all_fields in
      let ser_len = sersize_of_tuple mask tuple in
      let tx = RingBuf.bytes_tx ser_len in
      serialize_tuple mask tx 0 tuple ;
      let values = RingBuf.read_raw_tx tx in
      let v = RamenSync.Value.Tuple { skipped ; values } in
      let seq = IntCounter.get stats_num_sync_msgs_out in
      let k = key_of_seq seq in
      let cmd = RamenSync.Client.CltMsg.NewKey (k, v, 0.) in
      ZMQClient.send_cmd clt ?while_ cmd
  | _ -> ()

let publish_stats clt ?while_ stats_key init_stats stats =
  (* Those stats are the stats since startup. Combine them with whatever was
   * present previously: *)
  let tot_stats =
    match init_stats with
    | None ->
        stats
    | Some init ->
        RamenSync.Value.RuntimeStats.{
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
  let v = RamenSync.Value.RuntimeStats tot_stats in
  let cmd = RamenSync.Client.CltMsg.SetKey (stats_key, v) in
  ZMQClient.send_cmd clt ?while_ cmd

let start_zmq_client
      ?while_ ~url ~srv_pub_key ~username ~clt_pub_key ~clt_priv_key
      (site : N.site) (fq : N.fq) k =
  let open RamenSync in
  if url = "" then k ignore4 ignore else
  (* TODO: also subscribe to errors! *)
  let topic_sub =
    "tails/"^ (site :> string) ^"/"^ (fq :> string) ^"/users/*"
  and topic_pub seq =
    Key.(Tails (site, fq, LastTuple seq)) in
  fun conf ->
    ZMQClient.start ?while_ ~url ~srv_pub_key
                    ~username ~clt_pub_key ~clt_priv_key
                    ~topics:[ topic_sub ] ~on_new ~on_del
                    ~recvtimeo:0. ~sndtimeo:0. (fun clt ->
      let publish_tail = may_publish_tail clt ?while_ topic_pub in
      let stats_key =
        Key.(PerSite (site, PerWorker (fq, RuntimeStats))) in
      let init_stats =
        match (Client.find clt stats_key).value with
        | exception Not_found ->
            None
        | Value.RuntimeStats s ->
            Some s
        | v ->
            !logger.error "Invalid type for %a: %a"
              Key.print stats_key
              Value.print v ;
            None in
      let publish_stats = publish_stats clt ?while_ stats_key init_stats in
      k publish_tail publish_stats conf)
