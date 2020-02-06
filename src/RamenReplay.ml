(* Replays are temporary workers+paths used to recompute a given target
 * function output in a given time range.
 *
 * This module knows how to:
 * - find the required sources to reconstruct any function output in a
 *   given time range;
 * - connect all those sources for a dedicated channel, down to the function
 *   which output we are interested in (the target);
 * - tear down all these connections.
 *
 * Temporarily also:
 * - load and save the replays configuration from disk.
 *)
open Batteries
open RamenLog
open RamenHelpers
open RamenConsts
open RamenSync
module C = RamenConf
module VSI = Value.SourceInfo
module VOS = Value.OutputSpecs
module VR = Value.Replay
module O = RamenOperation
module OutRef = RamenOutRef
module Files = RamenFiles
module TimeRange = RamenTimeRange
module ZMQClient = RamenSyncZMQClient
module Paths = RamenPaths

(*$< Batteries *)

(* Helper function: *)

let cartesian_product f lst =
  (* We have a list of list of things. Call [f] with a all possible lists of
   * thing, one by one. (technically, the lists are reversed) *)
  let rec loop selection = function
    | [] -> f selection
    | lst::rest ->
        (* For each x of list, try all possible continuations: *)
        List.iter (fun x ->
          loop (x::selection) rest
        ) lst in
  if lst <> [] then loop [] lst

(*$inject
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

(* Like BatSet.t, but with a serializer: *)
type 'a set = 'a Set.t

exception NotInStats of (N.site * N.fq)
exception NoData

type replay_stats =
  { parents : (N.site * N.fq) list ;
    archives : TimeRange.t [@ppp_default []] }

let link_print oc (psite_fq, site_fq) =
  Printf.fprintf oc "%a=>%a"
    N.site_fq_print psite_fq
    N.site_fq_print site_fq

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
 * recursion we will have to keep track of the current local site. *)
let find_sources
      (stats : (N.site_fq, replay_stats) Hashtbl.t) local_site fq since until =
  let since, until =
    if since <= until then since, until
    else until, since in
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
      TimeRange.merge range range',
      (Set.union sources sources', Set.union links links')
    ) (TimeRange.empty, (Set.empty, Set.empty)) ways in
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
   * range [since]..[until]. Returns a list of pairs composed of the
   * TimeRange.t and a pair of the set of fqs and the set of links, a link
   * being a pair of parent and child fq. So for instance, this is a
   * possible result:
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
      List.fold_left (fun range (t1, t2, oe) ->
        if t1 > until || (t2 < since && not oe) then range else
        TimeRange.make (max t1 since) ((if oe then max else min) t2 until) oe |>
        TimeRange.merge range
      ) TimeRange.empty s.archives in
    !logger.debug "From %a:%a, range from archives = %a"
      N.site_print local_site
      N.fq_print fq
      TimeRange.print local_range ;
    (* Take what we can from here and the rest from the parents: *)
    (* Note: we are going to ask all the parents to export the replay
     * channel to this function. Although maybe some of those we are
     * going to spawn a replayer. That's normal, the replayer is reusing
     * the out_ref of the function it substitutes to. Which will never
     * receive this channel (unless loops but then we actually want
     * the worker to process the looping tuples!) *)
    let from_parents =
      let plinks =
        List.fold_left (fun links pfq ->
          Set.add (pfq, (local_site, fq)) links
        ) links s.parents in
      find_parent_ways since until plinks s.parents in
    if TimeRange.is_empty local_range then from_parents else
      let local_way = local_range, (Set.singleton (local_site, fq), links) in
      local_way :: from_parents
  and pick_best_way ways =
    let tot_range = until -. since in
    assert (tot_range >= 0.) ;
    let coverage_of_range range = (* ratio over tot_range *)
      if tot_range = 0. then 1. else
      TimeRange.span range /. tot_range in
    (* It is frequent that a much shorter way is missing a few seconds at
     * the end of the range when querying until `now`, which forces us to
     * consider not only the covered time range but also the number of
     * involved sources when assessing a way. *)
    let cost_of_way (range, (_site_fq, links)) =
      let cov = coverage_of_range range in
      if cov = 0. then 1. else
      log (float_of_int (2 + Set.cardinal links)) /. cov in
    List.fold_left (fun (best_cost, _ as prev) way ->
      let cost = cost_of_way way in
      assert (cost >= 0.) ;
      if cost < best_cost then cost, Some way else prev
    ) (max_float, None) ways |>
    snd |>
    option_get "best path" __LOC__
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
      !logger.debug "No archives" ;
      raise NoData
  | ways ->
      !logger.debug "Found those ways: %a"
        (List.print (Tuple2.print
          TimeRange.print
          (Tuple2.print (Set.print N.site_fq_print)
                        (Set.print link_print)))) ways ;
      pick_best_way ways

(* Create the replay structure but does not start it (nor create the
 * final_rb).
 * Notice that the target is always local, which free us from the
 * site identifier that would otherwise be necessary. If one wants
 * to replay remote (or a combination of local and remote) workers
 * then one can easily replay from an immediate expression, expressing
 * this complex selection in ramen language directly.
 * So, here [func] is supposed to mean the local instance of it only. *)
let create
      conf (stats : (N.site_fq, replay_stats) Hashtbl.t)
      ?(timeout=Default.replay_timeout) ?resp_key site_name prog_name func
      since until =
  let timeout_date = Unix.gettimeofday () +. timeout in
  let out_typ =
    O.out_type_of_operation ~with_private:false func.VSI.operation in
  let fq = VSI.fq_name prog_name func in
  (* Ask to export only the fields we want. From now on we'd better
   * not fail and retry as we would hammer the out_ref with temp
   * ringbufs.
   * Maybe we should use the channel in the rb name, and pick a
   * channel large enough to be a hash of FQ, output fields and
   * dates? But that mean 256bits integers (2xU128?). *)
  (* TODO: for now, we ask for all fields. Ask only for field_names,
   * but beware of with_event_type! *)
  let target_fieldmask = RamenFieldMaskLib.fieldmask_all ~out_typ in
  let site = site_name |? conf.C.site in
  let range, (sources, links) =
    find_sources stats site fq since until in
  (* Pick a channel. They are cheap, we do not care if we fail
   * in the next step: *)
  let channel = RamenChannel.make () in
  let recipient =
    match resp_key with
    | Some k ->
        VR.SyncKey k
    | None ->
        let tmpdir = getenv ~def:"/tmp" "TMPDIR" in
        let rb =
          Printf.sprintf2 "%s/replay_%a_%d.rb"
            tmpdir RamenChannel.print channel (Unix.getpid ()) |>
          N.path in
        VR.RingBuf rb in
  !logger.debug
    "Creating replay channel %a, with sources=%a, links=%a, \
     covered time slices=%a, recipient=%a"
    RamenChannel.print channel
    (Set.print N.site_fq_print) sources
    (Set.print link_print) links
    TimeRange.print range
    Value.Replay.print_recipient recipient ;
  (* For easier sharing with C++: *)
  let sources = Set.to_list sources
  and links = Set.to_list links in
  VR.{ channel ; target = site, fq ; target_fieldmask ; since ; until ;
       recipient ; sources ; links ; timeout_date }

let teardown_links conf session t =
  let rem_out_from (site, fq) =
    if site = conf.C.site then
      let now = Unix.gettimeofday () in
      OutRef.remove_channel ~now session site fq t.VR.channel
  in
  (* Start by removing the links from the graph, then the last one
   * from the target: *)
  List.iter (fun (psite_fq, _) -> rem_out_from psite_fq) t.links ;
  rem_out_from t.target

let settup_links conf ~while_ session func_of_fq t =
  (* Also indicate to the target how many end-of-chans to count before it
   * can end the publication of tuples. *)
  let num_sources = List.length t.VR.sources in
  let now = Unix.gettimeofday () in
  (* Connect the target first, then the graph: *)
  let connect_to prog_name func out_ref_k fieldmask =
    let site = conf.C.site in
    let fq = VSI.fq_name prog_name func in
    OutRef.add ~now ~while_ session site fq out_ref_k
               ~timeout_date:t.timeout_date ~num_sources
               ~channel:t.channel fieldmask
  in
  let connect_to_rb prog_name func fname fieldmask =
    let out_ref_k = VOS.DirectFile fname in
    connect_to prog_name func out_ref_k fieldmask
  and connect_to_sync_key prog_name func sync_key fieldmask =
    let out_ref_k = VOS.SyncKey sync_key in
    connect_to prog_name func out_ref_k fieldmask
  in
  let target_site, target_fq = t.target in
  let what = Printf.sprintf2 "Setting up links for channel %a"
               RamenChannel.print t.channel in
  log_and_ignore_exceptions ~what (fun () ->
    if conf.C.site = target_site then
      let prog_name, func = func_of_fq target_fq in
      match t.recipient with
      | VR.RingBuf rb ->
          connect_to_rb prog_name func rb t.target_fieldmask
      | VR.SyncKey k ->
          connect_to_sync_key prog_name func k t.target_fieldmask
  ) () ;
  List.iter (fun ((psite, pfq), (_, cfq)) ->
    if conf.C.site = psite then
      log_and_ignore_exceptions ~what (fun () ->
        let cprog_name, cfunc = func_of_fq cfq in
        let pprog_name, pfunc = func_of_fq pfq in
        let fname = Paths.in_ringbuf_name conf.C.persist_dir cprog_name cfunc
        and fieldmask =
          RamenFieldMaskLib.make_fieldmask pfunc.VSI.operation
                                           cfunc.VSI.operation in
        connect_to_rb pprog_name pfunc fname fieldmask) ()
  ) t.links

(*$>*)
