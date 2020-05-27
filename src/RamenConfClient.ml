open Batteries
open Stdint

open RamenLog
open RamenSync
open RamenSyncHelpers
module Processes = RamenProcesses

(* Parse a value from a string according to the key it's intended for *)
let parse_value key str =
  let open Key in
  match key with
  | DevNull (* whatever *)
  | Versions _
  | Sources (_, "ramen")
  | PerSite (_, PerService (_, Host))
  | PerSite (_, PerWorker (_, PerInstance (_, (StateFile | InputRingFile))))
  | PerSite (_, PerProgram (_, Executable))
    ->
      Value.of_string str
  | Time
  | PerSite (_, PerWorker (_, PerInstance (_, (LastKilled | LastExit |
                                               QuarantineUntil))))
  | Storage RecallCost
    ->
      Value.of_float (float_of_string str)
  | Sources (_, "alert") ->
      let alert =
        PPP.of_string_exc RamenApi.alert_source_ppp_ocaml str |>
        RamenApi.sync_value_of_alert in
      Value.Alert alert
  | PerSite (_, IsMaster)
    ->
      Value.of_bool (bool_of_string str)
  | PerSite (_, PerService (_, Port))
  | PerSite (_, PerWorker (_, NumArcFiles))
  | PerSite (_, PerWorker (_, PerInstance (_, (Pid | LastExitStatus |
                                               SuccessiveFailures))))
    ->
      Value.of_int (int_of_string str)
  | PerSite (_, PerWorker (_, (NumArcBytes | AllocedArcBytes)))
  | Storage TotalSize
    ->
      Value.of_int64 (Int64.of_string str)

  | Teams (_, Contacts _)
    ->
      Value.AlertingContact (
        PPP.of_string_exc Value.Alerting.Contact.t_ppp_ocaml str)

  | Sources _
  | TargetConfig
  | PerSite (_, PerWorker _)
  | Storage (RetentionsOverride _)
  | Tails _
  | Replays _
  | Error _
  | ReplayRequests
  | PerClient _
  | Dashboards _
  | Teams (_, Inhibition _)
  | Incidents _
  | Notifications
    ->
      failwith "No parser for this key."

let dump conf ~while_ key =
  let topic = if key = "" then "*" else key in
  let topics = [ topic ] in
  let on_new _session k v u mtime can_write can_del owner expiry =
    !logger.info "%a: %a (set by %s, mtime=%f, can_write=%b, can_del=%b, \
                          owner=%S, expiry=%f)"
      Key.print k
      Value.print v
      u mtime can_write can_del
      owner expiry
  and on_set _session k v u mtime =
    !logger.info "%a: %a (set by %s, mtime=%f)"
      Key.print k
      Value.print v
      u mtime
  in
  start_sync conf ~topics ~on_new ~on_set ~while_ ~recvtimeo:10. (fun session ->
    !logger.info "Monitoring (^C to quit):" ;
    ZMQClient.process_until ~while_ session)

let set conf ~while_ key value =
  let value = parse_value key value in
  start_sync conf ~while_ ~recvtimeo:1. (fun session ->
    let on_ok () = Processes.quit := Some 0
    and on_ko () = Processes.quit := Some 1 in
    let msg = Client.CltMsg.SetKey (key, value) in
    ZMQClient.send_cmd ~while_ session ~on_ok ~on_ko msg ;
    ZMQClient.process_until ~while_ session)

let del conf ~while_ key =
  start_sync conf ~while_ ~recvtimeo:1. (fun session ->
    let on_ok () = Processes.quit := Some 0
    and on_ko () = Processes.quit := Some 1 in
    let msg = Client.CltMsg.DelKey key in
    ZMQClient.send_cmd ~while_ session ~on_ok ~on_ko msg ;
    ZMQClient.process_until ~while_ session)
