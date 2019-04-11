open Batteries
open Stdint
open RamenLog
open RamenHelpers
open RamenConsts
module C = RamenConf
module F = C.Func
module P = C.Program
module O = RamenOperation
module T = RamenTypes
module N = RamenName
module OutRef = RamenOutRef
module Files = RamenFiles
module Processes = RamenProcesses
module Replay = Processes.Replay

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
    Some (bname, filter, typ, ser)
  else None

(* Returns the ringbuf name, a bool indicating if it's a temporary export or not,
 * the filter corresponding to [where], the tuple type, the tuple serialized type,
 * the parameters and event time of [fq]: *)
let read_output conf ?duration (fq : N.fq) where =
  (* Read directly from the instrumentation ringbuf when fq ends
   * with "#stats": *)
  match read_well_known fq where ("#"^ SpecialFunctions.stats)
          (C.report_ringbuf conf) RamenWorkerStats.tuple_typ () with
  | Some (bname, filter, typ, ser) ->
      bname, false, filter, typ, ser, [], RamenWorkerStats.event_time
  | None ->
      (* Or from the notifications ringbuf when fq ends with
       * "#notifs": *)
      (match read_well_known fq where ("#"^ SpecialFunctions.notifs)
               (C.notify_ringbuf conf) RamenNotification.tuple_typ () with
      | Some (bname, filter, typ, ser) ->
          bname, false, filter, typ, ser, [], RamenNotification.event_time
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
            C.with_rlock conf (fun programs ->
              match C.find_func programs fq with
              | exception Not_found ->
                  failwith ("Function "^ (fq :> string) ^
                            " does not exist")
              | _rce, prog, func ->
                  let bname =
                    Processes.start_export
                      conf ~file_type:OutRef.RingBuf ?duration func in
                  prog, func, bname) in
          let out_type =
            O.out_type_of_operation func.F.operation
          and event_time =
            O.event_time_of_operation func.F.operation in
          let ser =
            RingBufLib.ser_tuple_typ_of_tuple_typ out_type |>
            List.map fst in
          let filter = RamenSerialization.filter_tuple_by ser where in
          bname, true, filter, out_type, ser, prog.P.params, event_time)

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


let replay conf ?(while_=always) fq field_names where since until
           ~with_event_time f =
  (* Start with the most hazardous and interesting part: find a way to
   * get the data that's being asked: *)
  (* First, make sure the operation actually exist: *)
  let programs = C.with_rlock conf identity in
  let _rce, prog, func = C.find_func_or_fail programs fq in
  let out_type = O.out_type_of_operation func.F.operation in
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
  (* Using the archivist stats, find out all required sources: *)
  (* FIXME: here we assume fq is local. Instead, what we want is to tail
   * from all instances of running fq.
   * The only thing that we should do here is to create a new temporary
   * local worker that `SELECT * from *:fq`, with the special additional
   * attribute for since/until and a random channel id, and leave it to
   * the supervisors to run the required nodes and start the required
   * replayers, and to funnel the channel down here. It is important that
   * the new channel is not broadcasted to all children to avoid spamming
   * the whole function tree! So we may want a special kind of entry in
   * the RC, or a special file altogether. *)
  let stats = RamenArchivist.get_global_stats ~while_ conf in
  match Replay.create conf stats func since until with
  | exception Replay.NoData -> on_exit ()
  | replay ->
    RingBuf.create replay.final_rb ;
    let rb = RingBuf.load replay.final_rb in
    let ret =
      finally
        (fun () ->
          Replay.teardown_links conf programs replay ;
          RingBuf.unload rb)
        (fun () ->
          Replay.settup_links conf programs replay ;
          (* Do not start the replay at once or the worker won't have reread
           * its out-ref. TODO: signal it. *)
          Unix.sleepf Default.min_delay_restats ;
          let pids, eofs =
            Replay.spawn_all_local_sources conf programs replay in
          (* Read the rb while monitoring children: *)
          let pids = ref pids and eofs = ref eofs in
          let while_ () =
            if not (Set.Int.is_empty !pids) then (
              pids :=
                waitall_once ~expected_status:ExitCodes.terminated
                            ~what:"replayer" !pids ;
              if Set.Int.is_empty !pids then
                !logger.debug "All replayers have exited, \
                               still waiting for %d EndOfReplay"
                  (Set.cardinal !eofs)) ;
            (not (Set.is_empty !eofs) || not (Set.Int.is_empty !pids)) &&
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
                  ser prog.P.params et
          in
          let unserialize =
            RamenSerialization.read_array_of_values ser in
          let filter = RamenSerialization.filter_tuple_by ser where in
          RingBufLib.read_ringbuf ~while_ rb (fun tx ->
            let msg = RamenSerialization.read_tuple unserialize tx in
            RingBuf.dequeue_commit tx ;
            match msg with
            | RingBufLib.EndOfReplay (chan, replay_id), None ->
              if chan = replay.channel then (
                if Set.mem replay_id !eofs then (
                  !logger.debug "EndOfReplay from %d" replay_id ;
                  eofs := Set.remove replay_id !eofs
                ) else
                  !logger.error "Received EndOfReplay from unknown replayer %d"
                    replay_id
              ) else
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
          let ret = on_exit () in
          (* In case we got all the eofs before all replayers have exited: *)
          waitall ~while_ ~expected_status:ExitCodes.terminated
                  ~what:"replayer" !pids ;
          ret
        ) () in
    (* If all went well, delete the ringbuf: *)
    Files.safe_unlink replay.final_rb ;
    ret
