(* A small service turning replay requests into actual replays: *)
open Batteries
open RamenLog
open RamenHelpers
open RamenConsts
open RamenSyncHelpers
open RamenSync
module C = RamenConf
module F = C.Func
module N = RamenName
module ZMQClient = RamenSyncZMQClient
module Export = RamenExport
module Replay = RamenReplay

let create_replay conf ~while_ clt (_site, fq as site_fq) since until =
  let prog_name, _func_name = N.fq_parse fq in
  let _prog, func = function_of_fq clt fq in
  let stats = Export.replay_stats clt in
  let func = F.unserialized prog_name func in
  match Replay.create conf stats func since until with
  | exception Replay.NoData ->
      !logger.warning "Not enough data to replay %a since %a until %a"
        N.site_fq_print site_fq
        print_as_date since
        print_as_date until
  | replay ->
      !logger.debug "Creating replay target ringbuf %a"
        N.path_print replay.final_rb ;
      let k = Key.Replays replay.channel
      and v = Value.Replay replay in
      ZMQClient.(send_cmd ~while_ (CltMsg.NewKey (k, v, 0.)))

let start conf ~while_ =
  let topics =
    "replay_requests" :: Export.replay_topics in
  let on_set clt k v _uid _mtime =
    match k, v with
    | Key.ReplayRequests,
      Value.ReplayRequest { target ; since ; until } ->
        create_replay conf ~while_ clt target since until
    | _ -> () in
  start_sync conf ~while_ ~on_set ~topics ~recvtimeo:10.
                  (ZMQClient.process_until ~while_)
