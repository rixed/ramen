(* Interface between workers and the confserver.  *)
open Batteries
open RamenLog
open RamenHelpers
open RamenConsts
module Conf = CodeGenLib_Conf
module ZMQClient = RamenSyncZMQClient

open Binocle

(* Statistics related to tuple publishing *)
let stats_num_subscribers =
  IntGauge.make Metric.Names.num_subscribers
    Metric.Docs.num_subscribers

let stats_num_rate_limited_unpublished =
  IntCounter.make Metric.Names.num_rate_limited_unpublished
    Metric.Docs.num_rate_limited_unpublished

let on_new _clt _k _v _uid = ()
let on_set _clt _k _v = ()
let on_del _clt _k = ()
let on_lock _clt _k _uid = ()
let on_unlock _clt _k = ()

let clt =
  ZMQClient.Client.make ~on_new ~on_set ~on_del ~on_lock ~on_unlock

let start_zmq_client url creds (site : N.site) (fq : N.fq) (k : Conf.t -> unit) =
  if url = "" then k else
  let topic =
    (site :> string) ^"/"^ (fq :> string) ^"/tail/users" in
  let on_program stage status =
    !logger.info "Conf.Server: %a: %a"
      ZMQClient.Stage.print stage
      ZMQClient.Status.print status in
  fun conf ->
    ZMQClient.start url creds topic on_program
                    ~recvtimeo:0 ~sndtimeo:0 (fun zock ->
      conf.Conf.zock <- Some zock ;
      k conf)

let may_publish num_skipped tuple =
  match IntGauge.get stats_num_subscribers with
  | Some (_mi, num, _ma) when num > 0 ->
      !logger.debug "TODO: publish that tuple" ;
      IntCounter.add stats_num_rate_limited_unpublished num_skipped ;
      ignore tuple
  | _ -> ()
