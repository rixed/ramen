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

let on_new _clt _k _v _uid = ()
let on_set _clt _k _v = ()
let on_del _clt _k = ()
let on_lock _clt _k _uid = ()
let on_unlock _clt _k = ()

(* This is called for every output tuple. It is enough to read sync messages
 * that infrequently, as long as we subscribe only to this low frequency
 * topic and received messages can only impact what this function does. *)
let may_publish clt zock key_of_seq
                sersize_of_tuple serialize_tuple skipped tuple =
  (* Process incoming messages without waiting for them: *)
  let msg_count = ZMQClient.process_in clt zock in
  IntCounter.add stats_num_sync_msgs_in msg_count ;
  (* Now publish (if there are subscribers) *)
  match IntGauge.get stats_num_subscribers with
  | Some (_mi, num, _ma) when num > 0 ->
      !logger.debug "TODO: publish that tuple" ;
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
      ZMQClient.send_cmd zock cmd
  | _ -> ()

let ignore_publish _sersize _serialize _skipped _tuple = ()

let start_zmq_client url creds (site : N.site) (fq : N.fq) k =
  if url = "" then k ignore_publish else
  let topic_sub =
    "tail/"^ (site :> string) ^"/"^ (fq :> string) ^"/users/*"
  and topic_pub seq =
    RamenSync.Key.(Tail (site, fq, LastTuple seq)) in
  let on_program stage status =
    !logger.info "Conf.Server: %a: %a"
      ZMQClient.Stage.print stage
      ZMQClient.Status.print status in
  fun conf ->
    ZMQClient.start url creds topic_sub on_program
                    ~recvtimeo:0 ~sndtimeo:0 (fun zock ->
      let clt =
        ZMQClient.Client.make ~on_new ~on_set ~on_del ~on_lock ~on_unlock in
      let publish = may_publish clt zock topic_pub in
      k publish conf)
