(* Interface between workers and the confserver.  *)
open Batteries
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

let on_new _zock _clt k _v _uid _mtime =
  match k with
  | RamenSync.Key.Tail (_, _, Subscriber uid) ->
      !logger.info "New subscriber: %s" uid ;
      (* TODO: upgrade binocle
      IntGauge.inc stats_num_subscribers *)
      let _mi, c, _ma = IntGauge.get stats_num_subscribers |? (0, 0, 0) in
      IntGauge.set stats_num_subscribers (c + 1)
  | _ -> ()

let on_del _zock _clt k _v =
  match k with
  | RamenSync.Key.Tail (_, _, Subscriber uid) ->
      !logger.info "Leaving subscriber: %s" uid ;
      (* TODO: upgrade binocle
      IntGauge.dec stats_num_subscribers *)
      let _mi, c, _ma = IntGauge.get stats_num_subscribers |? (0, 0, 0) in
      IntGauge.set stats_num_subscribers (c - 1)
  | _ -> ()

(* Try to process input messages before any attempt to send anything
 * (non-blocking): *)
let process_in ?while_ zock clt =
  let msg_count = ZMQClient.process_in ?while_ zock clt in
  if msg_count > 0 then
    !logger.info "Received %d ZMQ messages" msg_count ;
  IntCounter.add stats_num_sync_msgs_in msg_count

(* This is called for every output tuple. It is enough to read sync messages
 * that infrequently, as long as we subscribe only to this low frequency
 * topic and received messages can only impact what this function does. *)
let may_publish_tail clt zock ?while_ key_of_seq
                     sersize_of_tuple serialize_tuple skipped tuple =
  (* Process incoming messages without waiting for them: *)
  process_in ?while_ zock clt ;
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
      let v = ZMQClient.Value.Tuple { skipped ; values } in
      let seq = IntCounter.get stats_num_sync_msgs_out in
      let k = key_of_seq seq in
      let cmd = ZMQClient.Client.CltMsg.NewKey (k, v) in
      ZMQClient.send_cmd ?while_ clt zock cmd
  | _ -> ()

let publish_stats clt zock ?while_ stats_key init_stats stats =
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
          tot_in_tuples = init.tot_in_tuples + stats.tot_in_tuples ;
          tot_out_tuples = init.tot_out_tuples + stats.tot_out_tuples ;
          tot_in_bytes = init.tot_in_bytes + stats.tot_in_bytes ;
          tot_out_bytes = init.tot_out_bytes + stats.tot_out_bytes ;
          tot_notifs = init.tot_notifs + stats.tot_notifs ;
          tot_cpu = init.tot_cpu +. stats.tot_cpu ;
          max_ram = max init.max_ram stats.max_ram } in
  let v = RamenSync.Value.RuntimeStats tot_stats in
  let cmd = ZMQClient.Client.CltMsg.SetKey (stats_key, v) in
  ZMQClient.send_cmd ?while_ clt zock cmd

let start_zmq_client ?while_ url creds (site : N.site) (fq : N.fq) k =
  if url = "" then k ignore4 ignore else
  (* TODO: also subscribe to errors! *)
  let topic_sub =
    "tail/"^ (site :> string) ^"/"^ (fq :> string) ^"/users/*"
  and topic_pub seq =
    RamenSync.Key.(Tail (site, fq, LastTuple seq)) in
  fun conf ->
    ZMQClient.start ?while_ url creds ~topics:[ topic_sub ]
                    ~on_new ~on_del
                    ~recvtimeo:0. ~sndtimeo:0. (fun zock clt ->
      let publish_tail = may_publish_tail clt zock ?while_ topic_pub in
      let stats_key =
        RamenSync.Key.(PerSite (site, PerWorker (fq, RuntimeStats))) in
      let init_stats =
        match ZMQClient.Client.find clt stats_key with
        | exception Not_found ->
            None
        | ZMQClient.Client.{ value = RamenSync.Value.RuntimeStats s ; _ } ->
            Some s
        | ZMQClient.Client.{ value ; _ } ->
            RamenSync.invalid_sync_type stats_key value "RuntimeStats" in
      let publish_stats = publish_stats clt zock ?while_ stats_key init_stats in
      k publish_tail publish_stats conf)
