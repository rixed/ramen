open Batteries
open Stdint

open RamenHelpersNoLog
open RamenLog
open RamenSync
open RamenSyncHelpers
module CltCmd = Sync_client_cmd.DessserGen
module Processes = RamenProcesses

(* Parse a value from a string according to the key it's intended for *)
let value_of_string key str =
  let open Key in
  match key with
  | DevNull (* whatever *)
  | Versions _
  | Sources (_, "ramen")
  | PerSite (_, PerService (_, Host))
  | PerSite (_, PerWorker (_, PerInstance (_, (StateFile | InputRingFile))))
  | PerSite (_, PerProgram (_, Executable))
  | Incidents (_, Dialogs (_, Ack)) (* whatever *)
    ->
      Value.of_string str
  | Time
  | PerSite (_, PerWorker (_, PerInstance (_, (LastKilled | LastExit |
                                               QuarantineUntil))))
  | Storage RecallCost
    ->
      Value.of_float (float_of_string str)
  | Sources (_, "alert") ->
      let alert = Value.Alert.of_string str in
      Value.Alert alert
  | Sources (_, "pivot") ->
      let pivot = Value.Pivot.of_string str in
      Value.Pivot pivot
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
  | Teams { info = Contacts _ ; _ } ->
      Value.AlertingContact (
        dessser_of_string Alerting_contact.DessserGen.wrap_of_json str)
  | Notifications ->
      Value.Notification (
        dessser_of_string Alerting_notification.DessserGen.wrap_of_json str)
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
  | Teams { info = Inhibition _ ; _ }
  | Incidents _
    ->
      failwith ("No parser for key "^ Key.to_string key)

let dump conf ~while_ key follow =
  let topic = if key = "" then "*" else key in
  let topics = [ topic ] in
  let on_new _session k v u mtime can_write can_del owner expiry =
    Printf.printf "%a: %a (set by %s, mtime=%f, can_write=%b, can_del=%b, \
                   owner=%S, expiry=%f)\n"
      Key.print k
      Value.print v
      u mtime can_write can_del
      owner expiry
  and on_set _session k v u mtime =
    Printf.printf "%a: %a (set by %s, mtime=%f)\n"
      Key.print k
      Value.print v
      u mtime
  in
  start_sync conf ~topics ~on_new ~on_set ~while_ ~recvtimeo:1. (fun session ->
    if follow then (
      !logger.info "Monitoring (^C to quit):" ;
      ZMQClient.process_until ~while_ session))

let set conf ~while_ key value =
  let value = value_of_string key value in
  start_sync conf ~while_ ~recvtimeo:1. (fun session ->
    let on_ok () = Processes.quit := Some 0
    and on_ko () = Processes.quit := Some 1 in
    let msg = CltCmd.SetKey (key, value) in
    ZMQClient.send_cmd session ~on_ok ~on_ko msg ;
    ZMQClient.process_until ~while_ session)

let del conf ~while_ key =
  start_sync conf ~while_ ~recvtimeo:1. (fun session ->
    let on_ok () = Processes.quit := Some 0
    and on_ko () = Processes.quit := Some 1 in
    let msg = CltCmd.DelKey key in
    ZMQClient.send_cmd session ~on_ok ~on_ko msg ;
    ZMQClient.process_until ~while_ session)
