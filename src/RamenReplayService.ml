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

let create_replay
      conf ~while_ clt resp_key (site, fq as site_fq) since until =
  let prog_name, _func_name = N.fq_parse fq in
  let _prog, func = function_of_fq clt fq in
  let stats = Export.replay_stats clt in
  let func = F.unserialized prog_name func in
  match Replay.create conf stats ~resp_key (Some site) func since until with
  | exception Replay.NoData ->
      !logger.warning "Not enough data to replay %a since %a until %a"
        N.site_fq_print site_fq
        print_as_date since
        print_as_date until ;
      (* Terminate the replay at once: *)
      !logger.debug "Deleting publishing key %s" resp_key ;
      let key = Key.of_string resp_key in
      ZMQClient.(send_cmd ~while_ (CltMsg.DelKey key))
  | replay ->
      let k = Key.Replays replay.channel
      and v = Value.Replay replay in
      ZMQClient.(send_cmd ~while_ (CltMsg.NewKey (k, v, 0.)))

let start conf ~while_ =
  let topics =
    "replay_requests" :: Export.replay_topics in
  let on_set clt k v _uid _mtime =
    match k, v with
    | Key.ReplayRequests,
      Value.ReplayRequest { target ; since ; until ; resp_key } ->
        create_replay conf ~while_ clt resp_key target since until
    | _ -> () in
  let on_new clt k v uid mtime _can_write _can_del _owner _expiry =
    on_set clt k v uid mtime in
  start_sync conf ~while_ ~on_set ~on_new ~topics ~recvtimeo:10.
                  (fun _clt -> ZMQClient.process_until ~while_)
