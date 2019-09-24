open Batteries
open Stdint
open RamenLog
open RamenHelpers
open RamenConsts
open RamenSync
module C = RamenConf
module RC = C.Running
module F = C.Func
module P = C.Program
module O = RamenOperation
module T = RamenTypes
module N = RamenName
module OutRef = RamenOutRef
module Files = RamenFiles
module Processes = RamenProcesses
module Replay = RamenReplay
module ZMQClient = RamenSyncZMQClient

exception FuncHasNoEventTimeInfo of string
let () =
  Printexc.register_printer (function
    | FuncHasNoEventTimeInfo n -> Some (
      Printf.sprintf "Function %S has no event-time information" n)
    | _ -> None)

(* Some ringbuf are always available and their type known:
 * instrumentation, notifications. *)
let read_well_known (fq : N.fq) where suffix bname typ () =
  let fq_str = (fq :> string) in
  if fq_str = suffix || String.ends_with fq_str suffix then
    (* For well-known tuple types, serialized tuple is as given (no
     * reordering of fields): *)
    let ser = typ in
    let where =
      if fq_str = suffix then where else
      let fq = String.rchop ~n:(String.length suffix) fq_str in
      let w = N.field "worker", "=", T.VString fq in
      w :: where in
    let filter = RamenSerialization.filter_tuple_by ser where in
    Some (bname, filter, ser)
  else None

(* Returns the ringbuf name, a bool indicating if it's a temporary export or not,
 * the filter corresponding to [where], the tuple serialized type, the parameters
 * and event time of [fq]: *)
let read_output conf ?duration (fq : N.fq) where programs =
  (* Read directly from the instrumentation ringbuf when fq ends
   * with "#stats": *)
  match read_well_known fq where ("#"^ SpecialFunctions.stats)
          (C.report_ringbuf conf) RamenWorkerStats.tuple_typ () with
  | Some (bname, filter, ser) ->
      bname, false, filter, ser, [], RamenWorkerStats.event_time
  | None ->
      (* Or from the notifications ringbuf when fq ends with
       * "#notifs": *)
      (match read_well_known fq where ("#"^ SpecialFunctions.notifs)
               (C.notify_ringbuf conf) RamenNotification.tuple_typ () with
      | Some (bname, filter, ser) ->
          bname, false, filter, ser, [], RamenNotification.event_time
      | None ->
          (* Normal case: Create the non-wrapping RingBuf (under a standard
           * name given by RamenConf. If that worker is already archiving
           * in ORC format we won't use the ORC until it's complete and
           * moved into the archive anyway. What we want to avoid though,
           * is to archive the same tuples twice. Hopefully a worker cannot
           * switch between ORC and RingBuf archiving (since it's controlled
           * by an experiment). Therefore the ORC experiment also disable
           * archival of non-ORC files altogether. Which does not mean that
           * archives of other format must not be read, as on the long term
           * format can indeed switch (worker would restart in between). *)
          let prog, func, bname =
            match RC.find_func programs fq with
            | exception Not_found ->
                Printf.sprintf2 "Function %a does not exist"
                  N.fq_print fq |>
                failwith
            | _rce, prog, func ->
                let bname =
                  Processes.start_export
                    conf ~file_type:OutRef.RingBuf ?duration func in
                prog, func, bname in
          let out_type =
            O.out_type_of_operation ~with_private:false func.F.operation
          and event_time =
            O.event_time_of_operation func.F.operation in
          let ser =
            RingBufLib.ser_tuple_typ_of_tuple_typ out_type |>
            List.map fst in
          let filter = RamenSerialization.filter_tuple_by ser where in
          bname, true, filter, ser, prog.P.default_params, event_time)

(* Returns an array of index in [typ] tuple * field type.
 * Index -1 it for t1 and index -2 for t2. *)
let header_of_type ?(with_event_time=false) field_names typ =
  let h =
    List.map (fun n ->
      try List.findi (fun _ t -> t.RamenTuple.name = n) typ
      with Not_found ->
        Printf.sprintf2 "Unknown field name %a (have %a)"
          N.field_print n
          (pretty_list_print RamenTuple.print_field_typ) typ |>
        failwith
    ) field_names in
  let h =
    if with_event_time then
      (-1, RamenTuple.start_typ) ::
      (-2, RamenTuple.stop_typ) :: h
    else h in
  let idxs, typs = List.enum h |> Enum.uncombine in
  Array.of_enum idxs, List.of_enum typs

(* Check the entered field names are correct: *)
let check_field_names typ field_names =
  (* Asking for no field names is asking for all: *)
  if field_names = [] then
    List.filter_map (fun t ->
      if N.is_private t.RamenTuple.name then None else Some t.name
    ) typ
  else (
    List.iter (fun f ->
      if not (List.exists (fun t -> f = t.RamenTuple.name) typ) then
        Printf.sprintf2 "Unknown field %a, should be one of %a"
          N.field_print f
          RamenTuple.print_typ_names typ |>
        failwith
    ) field_names ;
    field_names)

(* We need the worker and precompiled infos of the target,
 * as well as the parents and archives of all workers (parents we
 * find in workers): *)
let replay_topics =
  [ "sites/*/workers/*/worker" ;
    "sites/*/workers/*/archives/times" ;
    "sources/*/info" ]

let replay_stats clt =
  let stats = Hashtbl.create 30 in
  Client.iter clt (fun k hv ->
    match k, hv.value with
    | Key.PerSite (site, PerWorker (fq, Worker)),
      Value.Worker worker ->
        let archives_k = Key.PerSite (site, PerWorker (fq, ArchivedTimes)) in
        let archives =
          match (Client.find clt archives_k).value with
          | exception Not_found -> []
          | Value.TimeRange archives -> archives
          | v -> err_sync_type archives_k v "a TimeRange" ; [] in
        let parents =
          List.map (fun r ->
            r.Value.Worker.site, N.fq_of_program r.program r.func
          ) worker.Value.Worker.parents in
        let s = Replay.{ parents ; archives } in
        Hashtbl.add stats (site, fq) s
    | _ -> ()) ;
  stats

let replay conf ~while_ fq field_names where since until
           ~with_event_time f clt =
  (* Start with the most hazardous and interesting part: find a way to
   * get the data that's being asked: *)
  let prog_name, _ = N.fq_parse fq in
  let prog, func = function_of_fq clt fq in
  let func = F.unserialized prog_name func in
  let out_type =
    O.out_type_of_operation ~with_private:false func.F.operation in
  let field_names = check_field_names out_type field_names in
  let ser = RingBufLib.ser_tuple_typ_of_tuple_typ out_type |>
            List.map fst in
  let head_idx, head_typ =
    header_of_type ~with_event_time field_names ser in
  !logger.debug "replay for field names %a, head_typ=%a, head_idx=%a"
    (List.print N.field_print) field_names
    RamenTuple.print_typ head_typ
    (Array.print Int.print) head_idx ;
  let on_tuple, on_exit = f head_typ in
  (* The target fq is always local. We need this to retrieve the result tuples
   * from a local ringbuffer. To get the output of a remote function it is
   * easy enough to replay a local transient function that select * from the
   * remote one. *)
  let stats = replay_stats clt in
  (* Find out all required sources: *)
  (* FIXME: Replay.create should be given the clt and should look up itself what
   * it needs instead of forcing callee to build [stats] at every calls *)
  match Replay.create conf stats func since until with
  | exception Replay.NoData ->
      (* When we have not enough archives to replay anything *)
      on_exit ()
  | replay ->
      !logger.debug "Creating replay target ringbuf %a"
        N.path_print replay.final_rb ;
      (* As replays are always created on the target site, we can create the RB
       * and read data from there directly: *)
      RingBuf.create replay.final_rb ;
      let replay_k = Key.Replays replay.channel
      and v = Value.Replay replay in
      ZMQClient.(send_cmd ~while_ (CltMsg.NewKey (replay_k, v, 0.))) ;
      let rb = RingBuf.load replay.final_rb in
      let ret =
        finally
          (fun () ->
            ZMQClient.(send_cmd ~while_ (CltMsg.DelKey replay_k)) ;
            RingBuf.unload rb)
          (fun () ->
            (* Read the rb while monitoring children: *)
            let eofs_num = ref 0 in
            let while_ () =
              !eofs_num < List.length replay.sources && while_ () in
            let while_ () =
              ZMQClient.may_send_ping ~while_ () ;
              while_ () in
            let event_time =
              O.event_time_of_operation func.F.operation in
            let event_time_of_tuple = match event_time with
              | None ->
                  if with_event_time then
                    failwith "Function has no event time information"
                  else (fun _ -> 0., 0.)
              | Some et ->
                  RamenSerialization.event_time_of_tuple
                    ser prog.P.Serialized.default_params et
            in
            let unserialize =
              RamenSerialization.read_array_of_values ser in
            let filter = RamenSerialization.filter_tuple_by ser where in
            RingBufLib.read_ringbuf ~while_ rb (fun tx ->
              let msg = RamenSerialization.read_tuple unserialize tx in
              RingBuf.dequeue_commit tx ;
              match msg with
              | RingBufLib.EndOfReplay (chan, _replay_id), None ->
                  if chan = replay.channel then
                    incr eofs_num
                  else
                    !logger.error "Received EndOfReplay for channel %a not %a"
                      RamenChannel.print chan RamenChannel.print replay.channel
              | RingBufLib.DataTuple chan, Some tuple (* in ser order *) ->
                  if chan = replay.channel then (
                    if filter tuple then (
                      let t1, t2 = event_time_of_tuple tuple in
                      if t2 > since && t1 <= until then (
                        let cols =
                          Array.map (fun idx ->
                            match idx with
                            | -2 -> T.VFloat t2
                            | -1 -> T.VFloat t1
                            | idx -> tuple.(idx)
                          ) head_idx in
                        on_tuple t1 t2 cols
                      ) else !logger.debug "tuple not in time range (%f..%f)" t1 t2
                    ) else !logger.debug "tuple filtered out"
                  ) else
                    !logger.error "Received EndOfReplay for channel %a not %a"
                      RamenChannel.print chan RamenChannel.print replay.channel
              | _ ->
                  !logger.error "Received an unknown message in tx") ;
            (* Signal the end of the replay: *)
            on_exit ()
          ) () in
      (* If all went well, delete the ringbuf: *)
      !logger.debug "Deleting replay target ringbuf %a"
        N.path_print replay.final_rb ;
      Files.safe_unlink replay.final_rb ;
      (* ringbuf lib also create a lock with the rb: *)
      let lock_fname = N.cat replay.final_rb (N.path ".lock") in
      ignore_exceptions Files.safe_unlink lock_fname ;
      ret
