open Batteries
open Stdint
open RamenLog
open RamenHelpers
open RamenConsts
module C = RamenConf
module F = C.Func
module P = C.Program

exception FuncHasNoEventTimeInfo of string
let () =
  Printexc.register_printer (function
    | FuncHasNoEventTimeInfo n -> Some (
      Printf.sprintf "Function %S has no event-time information" n)
    | _ -> None)

(* Some ringbuf are always available and their type known:
 * instrumentation, notifications. *)
let read_well_known fq where suffix bname typ () =
  let fq_str = RamenName.string_of_fq fq in
  if fq_str = suffix || String.ends_with fq_str suffix then
    (* For well-known tuple types, serialized tuple is as given (no
     * private fields, no reordering of fields): *)
    let ser = typ in
    let where =
      if fq_str = suffix then where else
      let fq = String.rchop ~n:(String.length suffix) fq_str in
      let w = RamenName.field_of_string "worker", "=", RamenTypes.VString fq in
      w :: where in
    let filter = RamenSerialization.filter_tuple_by ser where in
    Some (bname, filter, typ, ser)
  else None

(* Returns the ringbuf name, a bool indicating if it's a temporary export or not,
 * the filter corresponding to [where], the tuple type, the tuple serialized type,
 * the parameters and event time of [fq]: *)
let read_output conf ?duration fq where =
  (* Read directly from the instrumentation ringbuf when fq ends
   * with "#stats": *)
  match read_well_known fq where ("#"^ SpecialFunctions.stats)
          (C.report_ringbuf conf) RamenBinocle.tuple_typ () with
  | Some (bname, filter, typ, ser) ->
      bname, false, filter, typ, ser, [], RamenBinocle.event_time
  | None ->
      (* Or from the notifications ringbuf when fq ends with
       * "#notifs": *)
      (match read_well_known fq where ("#"^ SpecialFunctions.notifs)
               (C.notify_ringbuf conf) RamenNotification.tuple_typ () with
      | Some (bname, filter, typ, ser) ->
          bname, false, filter, typ, ser, [], RamenNotification.event_time
      | None ->
          (* Normal case: Create the non-wrapping RingBuf (under a standard
           * name given by RamenConf *)
          let prog, func, bname =
            C.with_rlock conf (fun programs ->
              match C.find_func programs fq with
              | exception Not_found ->
                  failwith ("Function "^ RamenName.string_of_fq fq ^
                            " does not exist")
              | _mre, prog, func ->
                  let bname =
                    RamenProcesses.start_export conf ?duration func in
                  prog, func, bname) in
          let ser =
            RingBufLib.ser_tuple_typ_of_tuple_typ func.F.out_type in
          let filter = RamenSerialization.filter_tuple_by ser where in
          bname, true, filter, func.F.out_type, ser, prog.P.params,
          func.F.event_time)

(* Returns an array of index in [typ] tuple * field type.
 * Index -1 it for t1 and index -2 for t2. *)
let header_of_type ?(with_event_time=false) field_names typ =
  let h =
    List.map (fun n ->
      List.findi (fun _ t -> t.RamenTuple.name = n) typ
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
    List.map (fun t ->
      t.RamenTuple.name
    ) typ
  else (
    List.iter (fun f ->
      if not (List.exists (fun t -> f = t.RamenTuple.name) typ) then
        Printf.sprintf2 "Unknown field %a, should be one of %a"
          RamenName.field_print f
          RamenTuple.print_typ_names typ |>
        failwith
    ) field_names ;
    field_names)

exception NotInStats of RamenName.fq

let find_replay_sources conf ?while_ fq since until =
  (* Find a way to get the data from func in between from and until.
   * Note that there could be several ways to obtain those. For instance,
   * we could ask for a 1year retention of some node that queried very
   * infrequently, and that is also a parent of some other node for which
   * we asked for a much shorter retention but that is queried more often:
   * in that case the archiver could well decide to archive both the
   * parent and, as an optimisation, this child. Now which best (or only)
   * path to query that child depends on since/until.
   * But the shortest path form func to its archiving parents that have the
   * required data is always the best. So start from func and progress
   * through parents until we have found all required sources with the whole
   * archived content. As a first attempt, just look for a parent that has
   * all the data (avoid "roaming"). Then gaps could be queried independently
   * (narrowed since/until might find the data). Or we could actually query all
   * the gaps while the "main" query is going. TBD.  *)
  (* Using stats, append to lst the list of all sources required to recall
   * data from since to until, and also return the best times we have
   * found so far: *)
  (* TODO: handle gaps in archives (for instance, best_since and best_until
   * could also come with a ratio of the coverage of the asked time
   * slice) *)
  let rec find_sources stats sources fqs since until =
    (* When asked for several fqs, we want to retrieve the same time range from
     * all of them. *)
    List.fold_left (fun (sources, best_opt) fq ->
      let sources, best_opt' =
        find_sources_single stats sources fq since until in
      (* Keep only the intersection of the time range: *)
      sources,
      match best_opt, best_opt' with
      | None, x -> x
      | x, None -> x
      | Some (t1, t2), Some (t1', t2') ->
          Some (max t1 t1', min t2 t2')
    ) (sources, None) fqs
  and find_sources_single stats sources fq since until =
    match Hashtbl.find stats fq with
    | exception Not_found ->
        raise (NotInStats fq)
    | s ->
        let best_opt =
          List.fold_left (fun best_opt (t1, t2) ->
            if t1 > until || t2 < since then best_opt else
            Some (max t1 since, min t2 until)
          ) None s.RamenArchivist.archives in
        !logger.debug "From %a, best_op=%a"
          RamenName.fq_print fq
          (Option.print (Tuple2.print Float.print Float.print)) best_opt ;
        (* Take what we can from here and the rest from the parents: *)
        match best_opt with
        | Some (best_since, best_until) ->
            let sources = fq :: sources in
            (* Complete to the left: *)
            let sources, best_opt =
              if best_since <= since then sources, best_opt else
              match find_sources stats sources s.parents since best_since with
              | sources, Some (best_since', _best_until') ->
                  sources, Some (best_since', best_until)
              | _ -> sources, best_opt in
            (* Complete to the right: *)
            let sources, best_opt =
              if best_until >= until then sources, best_opt else
              match find_sources stats sources s.parents best_until until with
              | sources, Some (_best_since', best_until') ->
                  sources, Some (best_since, best_until')
              | _ -> sources, best_opt in
            sources, best_opt
        | _ ->
            find_sources stats sources s.parents since until
  in
  (* We assume all functions are running for now but this is not required
   * for the replayer itself and for intermediary workers we might be
   * able to start them temporarily, ourself (not involving supervisor),
   * managing the our_ref ourself for this specific channel, once we've
   * made sure supervisor will not "fix" the outrefs in that case... *)
  let stats = RamenArchivist.load_stats conf in
  try find_sources_single stats [] fq since until
  with NotInStats _ ->
    (* Maybe those are old, let's take the time to update them: *)
    !logger.debug "Trying to rebuilt the stats file" ;
    RamenArchivist.update_worker_stats ?while_ conf ;
    let stats = RamenArchivist.load_stats conf in
    try find_sources_single stats [] fq since until
    with NotInStats fq ->
      Printf.sprintf2 "Cannot find %a in the stats"
        RamenName.fq_print fq |>
      failwith


let replay conf ?(while_=always) fq field_names where since until
           ~with_event_time f =
  (* Start with the most hazardous and interesting part: find a way to
   * get the data that's being asked: *)
  (* First, make sure the operation actually exist: *)
  let programs = C.with_rlock conf identity in
  let _mre, prog, func = C.find_func_or_fail programs fq in
  let field_names = check_field_names func.F.out_type field_names in
  let ser = RingBufLib.ser_tuple_typ_of_tuple_typ func.F.out_type in
  let head_idx, head_typ =
    header_of_type ~with_event_time field_names ser in
  let on_tuple, on_exit = f head_typ in
  (* Using the archivist stats, find out all required sources: *)
  match find_replay_sources conf ~while_ fq since until with
  | _, None ->
      !logger.warning "No archive found." ;
      on_exit ()
  | sources, Some (best_since, best_until) ->
      !logger.debug "List of required sources: %a"
        (pretty_list_print RamenName.fq_print) sources ;
      let rel = ref "" in
      let p = print_as_date ~right_justified:false ~rel in
      !logger.debug "Time slice covered: %a..%a"
        p best_since p best_until ;
      (* Then create a ringbuffer for reception: *)
      let rb_name =
        Printf.sprintf "/tmp/replay_test_%d.rb" (Unix.getpid ()) in
      RingBuf.create rb_name ;
      let rb = RingBuf.load rb_name in
      let ret = finally (fun () -> RingBuf.unload rb) (fun () ->
        (* Pick a channel. They are cheap, we do not care if we fail
         * in the next step: *)
        let channel_id = RamenChannel.make conf in
        (* Ask to export only the fields we want. From now on we'd better
         * not fail and retry as we would hammer the out_ref with temp
         * ringbufs.
         * Maybe we should use the channel in the rb name, and pick a
         * channel_id large enough to be a hash of FQ, output fields and
         * dates? But that mean 256bits integers (2xU128?). *)
        (* TODO: for now, we ask for all fields. Ask only for field_names,
         * but beware of with_event_type! *)
        let field_mask = RingBufLib.skip_list ~out_type:ser ~in_type:ser in
        let timeout = Unix.gettimeofday () +. 300. in
        let file_spec =
          RamenOutRef.{ field_mask ; timeout ; channel = Some channel_id } in
        let out_ref = C.out_ringbuf_names_ref conf func in
        RamenOutRef.add out_ref (rb_name, file_spec) ;
        finally (fun () -> RamenOutRef.remove out_ref rb_name) (fun () ->
          (* Do not start the replay at once or the worker won't have reread
           * its out-ref. TODO: signal it. *)
          Unix.sleepf Default.min_delay_restats ;
          (* Now spawn the replayers.
           * For each source, spawn a replayer, passing it the name of the
           * function, the out_ref files to obey, the channel id to tag tuples
           * with, and since/until dates. *)
          let _, pids, eofs =
            List.fold_left (fun (i, pids, eofs) sfq ->
              let smre, _prog, sfunc = C.find_func_or_fail programs sfq in
              let args = [| replay_argv0 ; RamenName.string_of_fq sfq |]
              and out_ringbuf_ref = C.out_ringbuf_names_ref conf sfunc in
              let env =
                [| "name="^ RamenName.string_of_func sfunc.F.name ;
                   "fq_name="^ RamenName.string_of_fq sfq ;
                   "log_level="^ string_of_log_level conf.C.log_level ;
                   "output_ringbufs_ref="^ out_ringbuf_ref ;
                   "rb_archive="^ C.archive_buf_name conf sfunc ;
                   "since="^ string_of_float since ;
                   "until="^ string_of_float until ;
                   "channel_id="^ RamenChannel.to_string channel_id ;
                   "replayer_id="^ string_of_int i |] in
              let pid = RamenProcesses.run_worker smre.C.bin args env in
              !logger.debug "Replay for %a is running under pid %d"
                RamenName.fq_print sfq pid ;
              i + 1,
              Set.Int.add pid pids,
              Set.add i eofs
            ) (0, Set.Int.empty, Set.empty) sources in
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
          let event_time_of_tuple = match func.F.event_time with
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
            match RamenSerialization.read_tuple unserialize tx with
            | RingBufLib.EndOfReplay (chan, replay_id), None ->
              if chan = channel_id then (
                if Set.mem replay_id !eofs then (
                  !logger.debug "EndOfReplay from %d" replay_id ;
                  eofs := Set.remove replay_id !eofs
                ) else
                  !logger.error "Received EndOfReplay from unknown replayer %d"
                    replay_id
              ) else
                !logger.error "Received EndOfReplay for channel %d not %d"
                  chan channel_id
            | RingBufLib.DataTuple chan, Some tuple (* in ser order *) ->
              if chan = channel_id then (
                if filter tuple then (
                  let t1, t2 = event_time_of_tuple tuple in
                  if t2 > since && t1 <= until then (
                    let cols =
                      Array.map (fun idx ->
                        match idx with
                        | -2 -> RamenTypes.VFloat t2
                        | -1 -> RamenTypes.VFloat t1
                        | idx -> tuple.(idx)
                      ) head_idx in
                    on_tuple t1 t2 cols
                  ) else !logger.debug "tuple not in time range (%f..%f)" t1 t2
                ) else !logger.debug "tuple filtered out"
              ) else
                !logger.error "Received EndOfReplay for channel %d not %d"
                  chan channel_id
            | _ ->
                !logger.error "Received an unknown message in tx") ;
          (* Signal the end of the replay: *)
          let ret = on_exit () in
          (* In case we got all the eofs before all replayers have exited: *)
          waitall ~while_ ~expected_status:ExitCodes.terminated
                  ~what:"replayer" !pids ;
          ret
        ) ()
      ) () in
      (* If all went well, delete the ringbuf: *)
      safe_unlink rb_name ;
      ret
