(*
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
module C = RamenConf
module RC = C.Running
module F = C.Func
module O = RamenOperation
module OutRef = RamenOutRef
module Files = RamenFiles
module TimeRange = RamenTimeRange

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

open C.Replays

exception NotInStats of (N.site * N.fq)
exception NoData

type replay_stats =
  { parents : (N.site * N.fq) list ;
    archives : TimeRange.t [@ppp_default []] }

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
      (stats : (site_fq, replay_stats) Hashtbl.t) local_site fq since until =
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
    let plinks =
      List.fold_left (fun links pfq ->
        Set.add (pfq, (local_site, fq)) links
      ) links s.parents in
    let from_parents =
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
    option_get "best path"
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
          (Tuple2.print (Set.print site_fq_print)
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
      conf (stats : (site_fq, replay_stats) Hashtbl.t)
      ?(timeout=Default.replay_timeout) func since until =
  let timeout_date = Unix.gettimeofday () +. timeout in
  let fq = F.fq_name func in
  let out_type =
    O.out_type_of_operation ~with_private:true func.F.operation in
  let ser = RingBufLib.ser_tuple_typ_of_tuple_typ out_type |>
            List.map fst in
  (* Ask to export only the fields we want. From now on we'd better
   * not fail and retry as we would hammer the out_ref with temp
   * ringbufs.
   * Maybe we should use the channel in the rb name, and pick a
   * channel large enough to be a hash of FQ, output fields and
   * dates? But that mean 256bits integers (2xU128?). *)
  (* TODO: for now, we ask for all fields. Ask only for field_names,
   * but beware of with_event_type! *)
  let target_fieldmask = RamenFieldMaskLib.fieldmask_all ~out_typ:ser in
  let range, (sources, links) =
    find_sources stats conf.C.site fq since until in
  (* Pick a channel. They are cheap, we do not care if we fail
   * in the next step: *)
  let channel = RamenChannel.make () in
  let final_rb =
    let tmpdir = getenv ~def:"/tmp" "TMPDIR" in
    Printf.sprintf2 "%s/replay_%a_%d.rb"
      tmpdir RamenChannel.print channel (Unix.getpid ()) |>
    N.path in
  !logger.debug
    "Creating replay channel %a, with sources=%a, links=%a, \
     covered time slices=%a, final rb=%a"
    RamenChannel.print channel
    (Set.print site_fq_print) sources
    (Set.print link_print) links
    TimeRange.print range
    N.path_print final_rb ;
  (* For easier sharing with C++: *)
  let sources = Set.to_list sources
  and links = Set.to_list links in
  { channel ; target = (conf.C.site, fq) ; target_fieldmask ;
    since ; until ; final_rb ; sources ; links ; timeout_date }

let teardown_links conf func_of_fq t =
  let rem_out_from (site, fq) =
    if site = conf.C.site then
      match func_of_fq fq with
      | exception Not_found -> (* Not a big deal really *)
          !logger.warning
            "While tearing down channel %a, cannot find function %a"
            RamenChannel.print t.channel N.fq_print fq
      | func ->
          let out_ref = C.out_ringbuf_names_ref conf func in
          OutRef.remove_channel out_ref t.channel
  in
  (* Start by removing the links from the graph, then the last one
   * from the target: *)
  List.iter (fun (psite_fq, _) -> rem_out_from psite_fq) t.links ;
  rem_out_from t.target

let settup_links conf func_of_fq t =
  (* Connect the target first, then the graph: *)
  let connect_to_rb func fname fieldmask =
    let out_ref = C.out_ringbuf_names_ref conf func in
    OutRef.add out_ref ~timeout_date:t.timeout_date
               ~channel:t.channel fname fieldmask
  in
  let target_site, target_fq = t.target in
  let what = Printf.sprintf2 "Setting up links for channel %a"
               RamenChannel.print t.channel in
  log_and_ignore_exceptions ~what (fun () ->
    if conf.C.site = target_site then (
      let func = func_of_fq target_fq in
      connect_to_rb func t.final_rb t.target_fieldmask)) () ;
  List.iter (fun ((psite, pfq), (_, cfq)) ->
    if conf.C.site = psite then
      log_and_ignore_exceptions ~what (fun () ->
        let cfunc = func_of_fq cfq in
        let pfunc = func_of_fq pfq in
        let fname = C.input_ringbuf_fname conf pfunc cfunc
        and fieldmask = F.make_fieldmask pfunc cfunc in
        connect_to_rb pfunc fname fieldmask) ()
  ) t.links

(* Spawn a source.
 * Note: there is no top-half for sources. We assume the required
 * top-halves are already running as their full remote children are.
 * Pass to each replayer the name of the function, the out_ref files to
 * obey, the channel id to tag tuples with, and since/until dates.
 * Returns the pid. *)
let spawn_source_replay conf func bin since until channels replayer_id =
  let fq = F.fq_name func in
  let args = [| Worker_argv0.replay ; (fq :> string) |]
  and out_ringbuf_ref = C.out_ringbuf_names_ref conf func
  and rb_archive =
    (* We pass the name of the current ringbuf archive if
     * there is one, that will be read after the archive
     * directory. Notice that if we cannot read the current
     * ORC file before it's archived, nothing prevent a
     * worker to write both a non-wrapping, non-archive
     * worthy ringbuf in addition to an ORC file. *)
    C.archive_buf_name ~file_type:OutRef.RingBuf conf func
  in
  let env =
    [| "name="^ (func.F.name :> string) ;
       "fq_name="^ (fq :> string) ;
       "log_level="^ string_of_log_level conf.C.log_level ;
       "output_ringbufs_ref="^ (out_ringbuf_ref :> string) ;
       "rb_archive="^ (rb_archive :> string) ;
       "since="^ string_of_float since ;
       "until="^ string_of_float until ;
       "channel_ids="^ Printf.sprintf2 "%a"
                         (Set.print ~first:"" ~last:"" ~sep:","
                                    RamenChannel.print) channels ;
       "replayer_id="^ string_of_int replayer_id |] in
  let pid = RamenProcesses.run_worker bin args env in
  !logger.debug "Replay for %a is running under pid %d"
    N.fq_print fq pid ;
  pid

(*$>*)
