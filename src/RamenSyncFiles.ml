(* Temporary service that upload in the configuration the content of the
 * configuration files. Will disappear as soon as every file is replaced
 * by the synchronized config tree. *)
open Batteries
open RamenHelpers
open RamenLog
open RamenSync
open RamenConsts
module Archivist = RamenArchivist
module Files = RamenFiles
module Processes = RamenProcesses
module ZMQClient = RamenSyncZMQClient
module CltMsg = Client.CltMsg
module SrvMsg = Client.SrvMsg
module Capa = Capacity
module C = RamenConf
module RC = C.Running
module FS = C.FuncStats
module F = C.Func
module P = C.Program
module T = RamenTypes
module O = RamenOperation
module Services = RamenServices

let while_ () = !Processes.quit = None

(* Helper function that takes a key filter and a new set of values, and
 * replaces all keys matching the filter with the new set of values. *)
let replace_keys clt zock f h =
  (* Start by deleting the extraneous keys: *)
  Client.H.enum clt.Client.h //
  (fun (k, _) -> f k && not (Client.H.mem h k)) |>
  Enum.iter (fun (k, _) ->
    ZMQClient.send_cmd clt zock ~while_ (CltMsg.DelKey k)) ;
  (* Now add/update the keys from [h]: *)
  Client.H.iter (fun k (v, _r, _w) ->
    (* FIXME: add r and w in NewKey *)
    Option.may (fun v ->
      (* We have no idea which key preexist or not as we subscribe to no
       * topics, so we have to SetKey: *)
      ZMQClient.send_cmd clt zock ~while_ (CltMsg.SetKey (k, v))
    ) v
  ) h

(* One module per data source so that it's easier to track those *)

(* FIXME: Workers should write the stats in there directly *)
module StatsInfo =
struct
  let update conf clt zock =
    let h = Client.H.create 50 in
    let upd k v =
      let r = Capa.anybody and w = Capa.nobody in
      Client.H.add h k (Some v, r, w) in
    Services.all_sites conf |>
    Set.iter (fun site ->
      let is_master = Set.mem site conf.C.masters in
      upd (PerSite (site, IsMaster)) (Value.Bool is_master) ;
      (* TODO: PerService *)
      let stats = Archivist.load_stats ~site conf in
      Hashtbl.iter (fun fq stats ->
        (* Should be set by the worker itself: *)
        upd (PerSite (site, PerWorker (fq, FirstStartupTime)))
            (Value.Float stats.FS.startup_time) ;
        Option.may (fun min_etime ->
          upd (PerSite (site, PerWorker (fq, MinETime)))
              (Value.Float min_etime) ;
        ) stats.FS.min_etime ;
        Option.may (fun max_etime ->
          upd (PerSite (site, PerWorker (fq, MaxETime)))
              (Value.Float max_etime) ;
        ) stats.FS.max_etime ;
        upd (PerSite (site, PerWorker (fq, TotTuples)))
            (Value.Int stats.FS.tuples) ;
        upd (PerSite (site, PerWorker (fq, TotBytes)))
            (Value.Int stats.FS.bytes) ;
        upd (PerSite (site, PerWorker (fq, TotCpu)))
            (Value.Float stats.FS.cpu) ;
        upd (PerSite (site, PerWorker (fq, MaxRam)))
            (Value.Int stats.FS.ram) ;
        (* Should be set by the GC: *)
        upd (PerSite (site, PerWorker (fq, ArchivedTimes)))
            (Value.TimeRange stats.FS.archives) ;
        upd (PerSite (site, PerWorker (fq, NumArcFiles)))
            (Value.Int (Int64.of_int stats.FS.num_arc_files)) ;
        upd (PerSite (site, PerWorker (fq, NumArcBytes)))
            (Value.Int stats.FS.num_arc_bytes)
      ) stats) ;
    let f = function
      | Key.PerSite
          (_, (IsMaster |
              (PerWorker
                (_, (FirstStartupTime | LastStartupTime | MinETime |
                 MaxETime | TotTuples | TotBytes | TotCpu | MaxRam |
                 ArchivedTimes))))) -> true
      | _ -> false in
    ZMQClient.with_locked_matching clt zock ~while_ f (fun () ->
      replace_keys clt zock f h)
end

(* FIXME: User conf file should be stored in the conftree to begin with,
 * then Archivist and GC should also write directly in there: *)
module Storage =
struct
  let last_read_user_conf = ref 0.

  let update conf clt zock =
    let fname = Archivist.user_conf_file conf in
    let t = Files.mtime_def 0. fname in
    if t > !last_read_user_conf then (
      !logger.info "Updating storage configuration from %a"
        N.path_print fname ;
      let f = function
        | Key.Storage (RetentionsOverride _ | TotalSize | RecallCost) -> true
        | _ -> false in
      ZMQClient.with_locked_matching clt zock ~while_ f (fun () ->
        let h = Client.H.create 20 in
        let upd k v =
          Client.H.add h k (Some v, Capa.Anybody, Capa.Admin) in
        last_read_user_conf := t ;
        let user_conf = Archivist.get_user_conf conf in
        upd (Storage TotalSize)
            Value.(Int user_conf.Archivist.size_limit) ;
        upd (Storage RecallCost)
            Value.(Float user_conf.recall_cost) ;
        Hashtbl.iter (fun glob retention ->
          upd (Storage (RetentionsOverride glob))
              Value.(Retention retention)
        ) user_conf.retentions ;
        replace_keys clt zock f h)
    )
end

(* FIXME: RmAdmin graph info is supposed to come from the choreographer output
 * and the TargetConfig. We keep this temporarily to avoid breaking everything
 * at once: *)
module SrcInfo =
struct
  let update conf clt zock =
    let f = function
      | Key.PerProgram (_, (SourceModTime | SourceFile)) -> true
      | _ -> false in
    ZMQClient.with_locked_matching clt zock ~while_ f (fun () ->
      let h = Client.H.create 20 in
      let upd k v =
        let r = Capa.anybody and w = Capa.nobody in
        Client.H.add h k (v, r, w) in
      RC.with_rlock conf identity |>
      Hashtbl.iter (fun pname (rce, get_rc) ->
        (match Files.mtime rce.RC.src_file with
        | exception Unix.(Unix_error (ENOENT, _, _)) ->
            () (* No file -> No source info *)
        | mtime ->
            let km = Key.PerProgram (pname, SourceModTime)
            and ks = Key.PerProgram (pname, SourceFile) in
            let do_upd () =
              upd km (Some Value.(Float mtime)) ;
              upd ks (Some Value.(String (rce.RC.src_file :> string)))
            and keep k =
              upd k None in
            (match Client.find clt km with
            | exception Not_found -> do_upd ()
            | { value = Value.(Float prev_mtime) ; _ } ->
                if mtime > prev_mtime then do_upd ()
                else (keep km ; keep ks)
            | hv ->
                !logger.error
                  "Wrong type for source modification time (%a), deleting"
                  Value.print hv.value)) ;
        (match get_rc () with
        | exception _ -> ()
        | p ->
            upd (Key.PerProgram (pname, RunCondition))
                (Some Value.(String
                  (IO.to_string (E.print false) p.P.condition))) ;
            List.iter (fun f ->
              let upd fk v =
                upd (Key.PerProgram (pname, PerFunction (f.F.name, fk)))
                    (Some v) in
              Option.may (fun retention ->
                upd Key.Retention Value.(Retention retention)
              ) f.F.retention ;
              upd Key.Doc Value.(String f.F.doc) ;
              upd Key.IsLazy Value.(Bool f.F.is_lazy) ;
              let op = f.F.operation in
              upd Key.Operation Value.(String
                (IO.to_string (O.print true) op)) ;
              List.iteri (fun i (field : N.field) ->
                upd Key.(Factors i) Value.(String (field :> string))
              ) (O.factors_of_operation op) ;
              upd Key.InType Value.(RamenType
                (RamenFieldMaskLib.record_of_in_type f.F.in_type)) ;
              upd Key.OutType Value.(RamenType
                (O.out_record_of_operation ~with_private:false op)) ;
              upd Key.Signature Value.(String f.F.signature) ;
              upd Key.MergeInputs Value.(Bool f.F.merge_inputs)
            ) p.funcs)) ;
      replace_keys clt zock f h)
end


(*
 * The service: update conftree from files in a loop.
 *)

let sync_step conf clt zock =
  log_and_ignore_exceptions ~what:"update StatsInfo"
    (StatsInfo.update conf clt) zock ;
  log_and_ignore_exceptions ~what:"update Storage"
    (Storage.update conf clt) zock ;
  log_and_ignore_exceptions ~what:"update SrcInfo"
    (SrcInfo.update conf clt) zock

let service_loop conf upd_period zock clt =
  let last_upd = ref 0. in
  Processes.until_quit (fun () ->
    let num_msg = ZMQClient.process_in zock clt in
    !logger.debug "Received %d messages" num_msg ;
    let now = Unix.gettimeofday () in
    if now >= !last_upd +. upd_period &&
       (upd_period > 0. || !last_upd = 0.)
    then (
      last_upd := now ;
      sync_step conf clt zock ;
    ) ;
    if upd_period <= 0. && ZMQClient.pending_callbacks () = 0 then
      raise Exit ;
    true
  )

let start conf loop =
  (* Given filesyncer carry on with updates only when the lock have succeeded,
   * no other keys than the error logs are actually needed: *)
  let topics = [] in
  ZMQClient.start ~recvtimeo:1. ~while_ conf.C.sync_url conf.C.login ~topics
    (service_loop conf loop)
