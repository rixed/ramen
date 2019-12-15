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
      conf ~while_ clt resp_key (site, fq as site_fq) since until explain =
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
      let k = Key.of_string resp_key in
      ZMQClient.(send_cmd ~while_ (CltMsg.DelKey k))
  | replay ->
      let v = Value.Replay replay in
      if explain then
        let k = Key.of_string resp_key in
        ZMQClient.(send_cmd ~while_ (CltMsg.SetKey (k, v))) ;
        ZMQClient.(send_cmd ~while_ (CltMsg.DelKey k))
      else
        let k = Key.Replays replay.channel in
        ZMQClient.(send_cmd ~while_ (CltMsg.NewKey (k, v, 0.)))

let start conf ~while_ =
  let topics =
    "replay_requests" :: Export.replay_topics in
  let synced = ref false in
  let on_synced _clt = synced := true in
  let on_set clt k v _uid _mtime =
    match k, v with
    | Key.ReplayRequests,
      Value.ReplayRequest { target ; since ; until ; explain ; resp_key } ->
        (* Be wary of replay requests found at startup that could cause
         * crashloop, better delete them *)
        if !synced then (
          let what = "creating replay for resp_key "^ resp_key in
          log_and_ignore_exceptions ~what
            (create_replay conf ~while_ clt resp_key target since until) explain
        ) else (
          !logger.warning "Deleting pending replay request %a"
            Value.print v ;
          ZMQClient.(send_cmd ~while_ (CltMsg.DelKey k))
        )
    | _ -> () in
  let on_new clt k v uid mtime _can_write _can_del _owner _expiry =
    on_set clt k v uid mtime in
  start_sync conf ~while_ ~on_set ~on_new ~topics ~recvtimeo:10. ~on_synced
             ~sesstimeo:Default.sync_long_sessions_timeout
             (fun _clt -> ZMQClient.process_until ~while_)
