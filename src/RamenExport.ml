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


let cartesian_product f lst =
  (* We have a list of list of things. Call [f] with a all possible lists of
   * thing, one by one. (technically, the list are reversed) *)
  let rec loop selection = function
    | [] -> f selection
    | lst::rest ->
        (* For each x of list, try all possible continuations: *)
        List.iter (fun x ->
          loop (x::selection) rest
        ) lst in
  if lst <> [] then loop [] lst

(*$inject
  open Batteries

  let test_cp lst =
    let ls l = List.fast_sort compare l in
    let s = ref Set.empty in
    cartesian_product (fun l -> s := Set.add (ls l) !s) lst ;
    Set.to_list !s |> ls
*)

(*$= test_cp & ~printer:(IO.to_string (List.print (List.print Int.print)))
  [ [1;4;5] ; [1;4;6] ; \
    [2;4;5] ; [2;4;6] ; \
    [3;4;5] ; [3;4;6] ] (test_cp [ [1;2;3]; [4]; [5;6] ])

 [] (test_cp [])

 [] (test_cp [ []; [] ])

 [] (test_cp [ [1;2;3]; []; [5;6] ])
*)

module Range = struct
  (*$< Range *)
  type t = (float * float) list

  let empty = []

  let is_empty = (=) []

  let make t1 t2 =
    let t1 = min t1 t2 and t2 = max t1 t2 in
    if t1 < t2 then [ t1, t2 ] else []

  (*$T make
    is_empty (make 1. 1.)
    not (is_empty (make 1. 2.))
  *)

  let rec merge l1 l2 =
    match l1, l2 with
    | [], l | l, [] -> l
    | (a1, b1)::r1, (a2, b2)::r2 ->
        let am = min a1 a2 and bm = max b1 b2 in
        if bm -. am <= (b1 -. a1) +. (b2 -. a2) then
          (am, bm) :: merge r1 r2
        else if a1 <= a2 then
          (a1, b1) :: merge r1 l2
        else
          (a2, b2) :: merge l1 r2

  (*$= merge & ~printer:(IO.to_string print)
    [ 1.,2. ; 3.,4. ] (merge [ 1.,2. ] [ 3.,4. ])
    [ 1.,2. ; 3.,4. ] (merge [ 3.,4. ] [ 1.,2. ])
    [ 1.,4. ] (merge [ 1.,3. ] [ 2.,4. ])
    [ 1.,4. ] (merge [ 1.,2. ] [ 2.,4. ])
    [ 1.,4. ] (merge [ 1.,4. ] [ 2.,3. ])
    [] (merge [] [])
    [ 1.,2. ] (merge [ 1.,2. ] [])
    [ 1.,2. ] (merge [] [ 1.,2. ])
  *)

  let print oc =
    let rel = ref "" in
    let p = print_as_date ~right_justified:false ~rel in
    List.print (Tuple2.print p p) oc

  (*$>*)
end

let site_fq_print oc (site, fq) =
  Printf.fprintf oc "%a:%a"
    N.site_print site
    N.fq_print fq

let link_print oc (psite_fq, site_fq) =
  Printf.fprintf oc "%a=>%a"
    site_fq_print psite_fq
    site_fq_print site_fq

exception NotInStats of (N.site * N.fq)
exception NoData

let find_replay_sources conf ?while_ local_site fq since until =
  (* Find a way to get the data out of fq for that time range.
   * Note that there could be several ways to obtain those. For instance,
   * we could ask for a 1year retention of some node that queried very
   * infrequently, and that is also a parent of some other node for which
   * we asked for a much shorter retention but that is queried more often:
   * in that case the archiver could well decide to archive both the
   * parent and, as an optimisation, this child. Now which best (or only)
   * path to query that child depends on since/until.
   * But the shortest path form fq to its archiving parents that have the
   * required data is always the best. So start from fq and progress
   * through parents until we have found all required sources with the whole
   * archived content, recording the all the functions on the way up there
   * in a set.
   * Return all the possible ways to get some data in that range, with the
   * sources, their distance, and the list of covered ranges. Caller
   * will pick what it likes best (or compose a mix).
   *
   * Note regarding distributed mode:
   * We keep assuming for now that fq is unique and runs locally.
   * But its sources might not. In particular, for each parent we have to
   * take into account the optional host identifier and follow there. During
   * recursion we will have to keep track of the current local site.
   *)
  let stats = RamenArchivist.get_global_stats ?while_ conf in
  let find_fq_stats site_fq =
    try Hashtbl.find stats site_fq
    with Not_found -> raise (NotInStats site_fq)
  in
  let cost (sources, links) =
    Set.cardinal sources + Set.cardinal links in
  let merge_ways ways =
    (* We have a list of, for each parent, one time range and one replay
     * configuration. Compute the union of all: *)
    List.fold_left (fun (range, (sources, links))
                        (range', (sources', links')) ->
      Range.merge range range',
      (Set.union sources sources', Set.union links links')
    ) (Range.empty, (Set.empty, Set.empty)) ways in
  let rec find_parent_ways since until links fqs =
    (* When asked for several parent fqs, all the possible ways to retrieve
     * any range of data is the cartesian product. But we are not going to
     * return the full product, only the best alternative for any distinct
     * time range. *)
    let per_parent_ways =
      List.map (find_ways since until links) fqs in
    (* For each parent, we got an alist from time ranges to a set of
     * sources/links (from which we assess a cost).
     * Now the result is an alist from time ranges to ways unioning all
     * possible ways. *)
    let h = Hashtbl.create 11 in
    cartesian_product (fun ways ->
      (* ranges is a list (one item per parent) of all possible ways.
       * Compute the resulting range.
       * If it's better that what we already have (or if
       * we have nothing for that range yet) then compute the actual
       * set of sources and links and store it in [ways]: *)
      let range, v = merge_ways ways in
      Hashtbl.modify_opt range (function
        | None -> Some v
        | Some v' as prev ->
            if cost v < cost v' then Some v else prev
      ) h
    ) per_parent_ways ;
    Hashtbl.enum h |> List.of_enum
  (* Find all ways up to some data sources required for [fq] in the time
   * range [since]..[until]. Returns a list of pairs composed of the Range.t
   * and a pair of the set of fqs and the set of links, a link being a pair
   * of parent and child fq. So for instance, this is a possible result:
   * [
   *   [ (123.456, 125.789) ], (* the range *)
   *   (
   *     { source1; source2; ... }, (* The set of data sources *)
   *     { (source1, fq2); (fq2, fq3); ... } (* The set of links *)
   *   )
   * ]
   *
   * Note regarding distributed mode:
   * Everywhere we have a fq in that return type, what we actually have is a
   * site * fq.
   *)
  and find_ways since until links (local_site, fq as site_fq) =
    (* When given a single function, the possible ways are the one from
     * the archives of this node, plus all possible ways from its parents: *)
    let s = find_fq_stats site_fq in
    let local_range =
      List.fold_left (fun range (t1, t2) ->
        if t1 > until || t2 < since then range else
        Range.merge range (Range.make (max t1 since) (min t2 until))
      ) Range.empty s.RamenArchivist.archives in
    !logger.debug "From %a:%a, range from archives = %a"
      N.site_print local_site
      N.fq_print fq
      Range.print local_range ;
    (* Take what we can from here and the rest from the parents: *)
    (* Note: we are going to ask all the parents to export the replay
     * channel to this function. Although maybe some of those we are
     * going to spawn a replayer. That's normal, the replayer is reusing
     * the out_ref of the function it substitutes to. Which will never
     * receive this channel (unless loops but then we actually want
     * the worker to process the looping tuples!) *)
    let links =
      List.fold_left (fun links pfq ->
        Set.add (pfq, (local_site, fq)) links
      ) links s.parents in
    let from_parents =
      find_parent_ways since until links s.parents in
    if Range.is_empty local_range then from_parents else
      let local_way = local_range, (Set.singleton (local_site, fq), links) in
      local_way :: from_parents
  and pick_best_way ways =
    let span_of_range =
      List.fold_left (fun s (t1, t2) -> s +. (t2 -. t1)) 0. in
    (* For now, consider only the covered range.
     * TODO: Maybe favor also shorter paths? *)
    List.fold_left (fun (best_span, _ as prev) (range, _ as way) ->
      let span = span_of_range range in
      assert (span >= 0.) ;
      if span > best_span then span, Some way else prev
    ) (0., None) ways |>
    snd |>
    option_get "no data in best path"
  in
  (* We assume all functions are running for now but this is not required
   * for the replayer itself. Later we could add the required workers in the
   * RC for just that channel. *)
  match find_ways since until Set.empty (local_site, fq) with
  | exception NotInStats _ ->
      Printf.sprintf2 "Cannot find %a in the stats"
        N.fq_print fq |>
      failwith
  | [] ->
      raise NoData
  | ways ->
      !logger.debug "Found those ways: %a"
        (List.print (Tuple2.print
          Range.print
          (Tuple2.print (Set.print site_fq_print)
                        (Set.print link_print)))) ways ;
      pick_best_way ways


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
  match find_replay_sources conf ~while_ conf.C.site fq since until with
  | exception NoData -> on_exit ()
  | range, (sources, links) ->
    !logger.debug "Required sources: %a"
      (Set.print site_fq_print) sources ;
    !logger.debug "Required links: %a"
      (Set.print link_print) links ;
    !logger.debug "Time slice covered: %a" Range.print range ;
    (* Then create a ringbuffer for reception: *)
    let rb_name =
      Printf.sprintf "/tmp/replay_test_%d.rb" (Unix.getpid ()) |>
      N.path in
    RingBuf.create rb_name ;
    let rb = RingBuf.load rb_name in
    let ret = finally (fun () -> RingBuf.unload rb) (fun () ->
      (* Pick a channel. They are cheap, we do not care if we fail
       * in the next step: *)
      let channel = RamenChannel.make () in
      (* Ask to export only the fields we want. From now on we'd better
       * not fail and retry as we would hammer the out_ref with temp
       * ringbufs.
       * Maybe we should use the channel in the rb name, and pick a
       * channel large enough to be a hash of FQ, output fields and
       * dates? But that mean 256bits integers (2xU128?). *)
      (* TODO: for now, we ask for all fields. Ask only for field_names,
       * but beware of with_event_type! *)
      (* Here we manipulate the outrefs directly, first to set our custom
       * ringbuf as the exit of fq (which we suppose local), and to connect
       * all the links that have been found above. Again, this is going to
       * work only if all the involved workers are local. FIXME. *)
      let fieldmask = RamenFieldMaskLib.fieldmask_all ~out_typ:ser in
      let replay = Processes.Replay.{
        channel ;
        target = (conf.C.site, fq) ;
        target_fieldmask = fieldmask ;
        since ;
        until ;
        final_rb = rb_name ;
        sources ;
        links ;
        timeout = Unix.gettimeofday () +. 300. } in
      finally
        (fun () ->
          Processes.Replay.teardown_links conf programs replay)
        (fun () ->
          Processes.Replay.settup_links conf programs replay ;
          (* Do not start the replay at once or the worker won't have reread
           * its out-ref. TODO: signal it. *)
          Unix.sleepf Default.min_delay_restats ;
          let pids, eofs =
            Processes.Replay.spawn_all_local_sources conf programs replay in
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
              if chan = channel then (
                if Set.mem replay_id !eofs then (
                  !logger.debug "EndOfReplay from %d" replay_id ;
                  eofs := Set.remove replay_id !eofs
                ) else
                  !logger.error "Received EndOfReplay from unknown replayer %d"
                    replay_id
              ) else
                !logger.error "Received EndOfReplay for channel %a not %a"
                  RamenChannel.print chan RamenChannel.print channel
            | RingBufLib.DataTuple chan, Some tuple (* in ser order *) ->
              if chan = channel then (
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
                  RamenChannel.print chan RamenChannel.print channel
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
    Files.safe_unlink rb_name ;
    ret
