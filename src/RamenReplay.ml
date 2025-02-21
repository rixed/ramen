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
open Stdint

open RamenLog
open RamenHelpersNoLog
open RamenHelpers
open RamenSync
module C = RamenConf
module Default = RamenConstsDefault
module VSI = Value.SourceInfo
module VR = Value.Replay
module O = RamenOperation
module OutRef = RamenOutRef
module OWD = Output_specs_wire.DessserGen
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

(* We need the worker and precompiled infos of the target,
 * as well as the parents and archives of all workers (parents we
 * find in workers): *)
let topics =
  [ "sites/*/workers/*/worker" ;
    "sites/*/workers/*/archives/times" ;
    "sources/*/info" ]

(* Find parents and archives for a specific function, or raise Not_found: *)
let get_stats clt (site, fq) =
  let stats_key = Key.PerSite (site, PerWorker (fq, Worker)) in
  !logger.debug "Looking for runtime stats in %a" Key.print stats_key ;
  match (Client.find clt stats_key).value with
  | Value.Worker worker ->
      let archives_k = Key.PerSite (site, PerWorker (fq, ArchivedTimes)) in
      let archives =
        match (Client.find clt archives_k).value with
        | exception Not_found -> [||]
        | Value.TimeRange archives -> archives
        | v -> err_sync_type archives_k v "a TimeRange" ; [||] in
      let parents =
        Array.map (fun r ->
          r.Func_ref.DessserGen.site,
          N.fq_of_program r.program r.func
        ) (worker.Value.Worker.parents |? [||]) in
      parents, archives
  | v ->
      err_sync_type stats_key v "a Worker" ;
      raise Not_found

let get_best_after clt fq =
  let prog_name, func_name = N.fq_parse fq in
  let info_key = Key.Sources (N.src_path_of_program prog_name, "info") in
  let default = Default.best_after in
  !logger.debug "Looking for best-after in %a" Key.print info_key ;
  match (Client.find clt info_key).value with
  | exception Not_found ->
      !logger.warning
        "Cannot find info_key of %a for its best-after, assuming none"
        Key.print info_key ;
      default
  | Value.SourceInfo { detail = Compiled { funcs ; _ } ; _ } ->
      (match List.find (fun func ->
          func.Source_info.DessserGen.name = func_name) funcs with
      | exception Not_found ->
          !logger.warning
            "Cannot find best-after for non existing function %a, assuming none"
            N.fq_print fq ;
          default
      | func ->
          (match func.best_after with
          | None ->
              default
          | Some e ->
              (match E.float_of_const e with
              | Some v -> v
              | None ->
                  !logger.error
                    "Cannot use non-constant best-after for function %a: \
                    %a, assuming none"
                    N.fq_print fq
                    (E.print ~max_depth:3 false) e ;
                  default)))
  | Value.SourceInfo { detail = Failed _ } ->
      !logger.warning
        "Cannot find best-after for non compiled %a, assuming none"
        N.fq_print fq ;
      default
  | v ->
      err_sync_type info_key v "a SourceInfo" ;
      default

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
 * archived content, recording all the functions (and required replay ranges)
 * on the way up there in a set.
 * Take into account that functions may requires extra history to work
 * properly.
 *
 * Note regarding distributed mode:
 * We keep assuming for now that fq is unique and runs locally.
 * But its sources might not. In particular, for each parent we have to
 * take into account the optional host identifier and follow there. During
 * recursion we will have to keep track of the current local site. *)
let find_sources clt local_site fq since until =
  let since, until =
    if since <= until then since, until
    else until, since in
  let find_fq_stats site_fq =
    try get_stats clt site_fq
    with Not_found -> raise (NotInStats site_fq) in
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
  (* Find all ways to obtain this time range of data from a set of functions
   * [fqs] (that are all parents of some function but this fact doesn't
   * matter here) *)
  let rec find_parent_ways since until links fqs =
    (* When asked for several parent fqs, all the possible ways to retrieve
     * any range of data is the cartesian product (if there are 2 ways to get
     * data from first parent and 3 ways to get data from the second parent,
     * then there are 6 ways total). But we are not going to return the full
     * product, only the best alternative for any distinct time range. *)
    let per_parent_ways =
      Array.map (find_ways since until links) fqs |>
      Array.to_list in
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
   * range [since]..[until]. Returns a list of possibilities, each consisting
   * of a pair: first the covered TimeRange.t and then a pair of the set of fqs
   * (each with its time interval) and the set of links, a link being a pair
   * of parent and child fqs.
   * So for instance, this is a possible result, with just one possibility:
   * [
   *   [ (123.456, 125.789) ], (* the range *)
   *   (
   *     { source1 ; source2 ; ... }, (* The set of data sources *)
   *     { (source1, fq2); (fq2, fq3); ... } (* The set of links *)
   *   )
   * ]
   *
   * Note regarding distributed mode:
   * Everywhere we have a fq above, what we actually have is a site * fq.
   *)
  and find_ways since until links (local_site, fq as site_fq) =
    (* When given a single function, the possible ways are the one from
     * the archives of this node, plus all possible ways from its parents: *)
    let parents, archives = find_fq_stats site_fq in
    (* Build the part of the requested time span that's already present in
     * this function archives, as a TimeRange: *)
    let local_range =
      Array.fold (fun range i ->
        if i.TimeRange.since > until ||
           (i.until < since && not i.growing) then
          range
        else
          TimeRange.make (max i.since since)
                         ((if i.growing then max else min) i.until until)
                         i.growing |>
          TimeRange.merge range
      ) TimeRange.empty archives in
    !logger.debug "From %a:%a, range from archives = %a"
      N.site_print local_site
      N.fq_print fq
      TimeRange.print local_range ;
    (* Take what we can from here and the rest (extended with this function
     * extra-history requirements) from the parents: *)
    (* Note: we are going to ask all the parents to export the replay
     * channel to this function. Although maybe for some of those we are
     * going to spawn a replayer. That's normal, the replayer is reusing
     * the out_ref of the function it substitutes to. Which will never
     * receive this channel (unless loops but then we actually want
     * the worker to process the looping tuples!) *)
    let from_parents =
      let plinks =  (* Add the links from parents to this function: *)
        Array.fold_left (fun links pfq ->
          Set.add (pfq, (local_site, fq)) links
        ) links parents in
      let since = since -. get_best_after clt fq in
      find_parent_ways since until plinks parents in
    (* [from_parents] has all ways to get data from parents.
     * Add the local archives on top of that as another possibility.
     * Note: we do not consider a way that combines replaying local archives
     * for some time range and spawning replayers for parents for some other
     * time range because it would be tricky to reproduce the same time
     * ordering. The client could easily request again the missing bits and
     * reproduce the whole time range. *)
    if TimeRange.is_empty local_range then from_parents else
      let local_way = local_range, (Set.singleton (local_site, fq), links) in
      local_way :: from_parents
  (* This function selects among several ways the one with best coverage
   * and smallest number of links: *)
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
      Printf.sprintf2 "Cannot find runtime stats for %a"
        N.fq_print fq |>
      failwith
  | [] ->
      !logger.debug "No archives" ;
      raise NoData
  | ways ->
      !logger.debug "Found those ways: %a"
        (List.print (pair_print
          TimeRange.print
          (pair_print (Set.print N.site_fq_print)
                      (Set.print link_print)))) ways ;
      pick_best_way ways

(* Create the replay structure but does not start it (nor create the
 * final [resp_key] or temporary ringbuffer file).
 * Notice that the target is always local, which free us from the
 * site identifier that would otherwise be necessary. If one wants
 * to replay remote (or a combination of local and remote) workers
 * then one can easily replay from an immediate expression, expressing
 * this complex selection in ramen language directly.
 * So, here [func] is supposed to mean the local instance of it only. *)
let create
      ?(timeout=Default.replay_timeout) ?resp_key
      conf clt site_name prog_name func since until =
  let timeout_date = Unix.gettimeofday () +. timeout in
  let fq = VSI.fq_name prog_name func in
  (* Ask to export only the fields we want. From now on we'd better
   * not fail and retry as we would hammer the out_ref with temp
   * ringbufs.
   * Maybe we should use the channel in the rb name, and pick a
   * channel large enough to be a hash of FQ, output fields and
   * dates? But that mean 256bits integers (2xU128?). *)
  (* TODO: for now, we ask for all fields. Ask only for field_names,
   * but beware of with_event_type! *)
  let target_fieldmask = RamenFieldMaskLib.all_public func.VSI.operation in
  let site = site_name |? conf.C.site in
  let range, (sources, links) = find_sources clt site fq since until in
  let site_fq_to_dessser (site, fq) =
    let prog, func = N.fq_parse fq in
    Fq_function_name.DessserGen.{ site ; program = prog ; function_ = func } in
  let sources = Set.map site_fq_to_dessser sources in
  (* Pick a channel. They are cheap, we do not care if we fail
   * in the next step: *)
  let channel = RamenChannel.make () in
  !logger.debug "Replay %a fieldmask: %a"
    RamenChannel.print channel
    RamenFieldMask.print target_fieldmask ;
  let recipient =
    match resp_key with
    | Some k ->
        VR.SyncKey k
    | None ->
        let rb =
          Printf.sprintf2 "%s/replay_%a_%d.rb"
            Files.tmp_dir RamenChannel.print channel (Unix.getpid ()) |> N.path in
        VR.RingBuf rb in
  !logger.debug
    "Creating replay channel %a, with sources=%a, links=%a, \
     covered time slices=%a, recipient=%a"
    RamenChannel.print channel
    (Set.print Value.site_fq_print) sources
    (Set.print link_print) links
    TimeRange.print range
    Value.Replay.print_recipient recipient ;
  (* For easier sharing with C++: *)
  let sources = Set.to_array sources in
  let links =
    Set.enum links /@
    (fun (f, t) -> site_fq_to_dessser f, site_fq_to_dessser t) |>
    Array.of_enum in
  let target = Fq_function_name.DessserGen.{
    site ; program = prog_name ; function_ = func.name }
  and target_fieldmask = RamenFieldMask.to_string target_fieldmask in
  (* Replay the actual range, which might include extra history: *)
  let since, until = TimeRange.bounds range in
  VR.{ channel ; target ; target_fieldmask ;
       since ; until ; recipient ; sources ; links ; timeout_date }

let teardown_links conf session t =
  !logger.debug "Tearing down replay %a" Channel.print t.VR.channel ;
  let now = Unix.gettimeofday () in
  let rem_out_from target =
    if target.Fq_function_name.DessserGen.site = conf.C.site then
      let fq = N.fq_of_program target.program target.function_ in
      OutRef.remove_channel ~now session target.site fq t.VR.channel
  in
  (* Start by removing the links from the graph, then the last one
   * from the target: *)
  Array.iter (fun (psite_fq, _) -> rem_out_from psite_fq) t.links ;
  rem_out_from t.target

(* In the general case, a replay will require several replayers and the target
 * function will aggregate the channel and provide the client with the end
 * result.  In simpler cases though, a single replayer will get directly from
 * the archive what the client asked for, and will write it itself directly
 * into client's response key. In that case, this single replayer will have to
 * delete the replay and the response key itself. *)
let is_mono_replayer replay =
  match replay.VR.sources, replay.links with
  | [| single_fq |], [||] ->
      single_fq = replay.target
  | _ ->
      false

(* When a replay target have parents, and (some of) those parents are replayed,
 * replayers take their outputs from the outputs of the worker they impersonate,
 * with the addition of the replay channel.
 * Then all intermediary workers are configured to forward the replay channel.
 * Eventually, the replay target is configured to output to the replay client
 * (usually a SyncKey).
 *
 * [setup_links] does something a bit simpler: it configures all the "links"
 * (ie all connections leading from the replayers to the replay target) with the
 * channel, including the replayed workers, so that the replayer can use the
 * exact same outputs than the worker they impersonate, at the expense of
 * uselessly reconfigure those impersonated workers despite they will never see
 * anything on the replay channel.
 *
 * Now let's consider what happen when the target is itself replayed (links is
 * then empty):
 * The replayer will use the same outputs than the replayed, but only one output
 * will allow this channel: the one to the replay client. In that case, the
 * replayer will then itself answer the client directly.
 * The replayed will also try to write tuples to this client, but it will never
 * receive anything on that channel. *)

let setup_links conf ~while_ session func_of_fq t =
  (* Also indicate to the target how many end-of-chans to count before it
   * can end the publication of tuples. *)
  let num_sources = Int16.of_int (Array.length t.VR.sources) in
  let now = Unix.gettimeofday () in
  (* Connect the target first, then the graph: *)
  let connect_to prog_name func out_ref_k fieldmask =
    let fq = VSI.fq_name prog_name func in
    OutRef.add ~now ~while_ session conf.C.site fq out_ref_k
               ~timeout_date:t.timeout_date ~num_sources
               ~channel:t.channel fieldmask
  in
  let connect_to_rb prog_name func fname fieldmask =
    let out_ref_k = OWD.DirectFile fname in
    connect_to prog_name func out_ref_k fieldmask
  and connect_to_sync_key prog_name func sync_key fieldmask =
    let out_ref_k = OWD.SyncKey sync_key in
    connect_to prog_name func out_ref_k fieldmask
  in
  let target_fq = N.fq_of_program t.target.program t.target.function_ in
  let target_fieldmask = RamenFieldMask.of_string t.target_fieldmask in
  let what = Printf.sprintf2 "Setting up links for channel %a"
               RamenChannel.print t.channel in
  (* Connects the target to the response key/ringbuf (also needed if the
   * mono replayer sends directly to the requestor since its the same
   * OutRef as the target: *)
  log_and_ignore_exceptions ~what (fun () ->
    if conf.C.site = t.target.site then
      match func_of_fq target_fq with
      | exception e ->
          !logger.error "Cannot get compiled function for %a: %s, skipping"
            N.fq_print target_fq
            (Printexc.to_string e)
      | prog_name, func ->
          (match t.recipient with
          | VR.RingBuf rb ->
              connect_to_rb prog_name func rb target_fieldmask
          | VR.SyncKey k ->
              connect_to_sync_key prog_name func k target_fieldmask)
  ) () ;
  (* And then add all the links from workers to workers for that channel: *)
  Array.iter (fun (from, to_) ->
    if conf.C.site = from.Fq_function_name.DessserGen.site then
      let pfq = N.fq_of_program from.program from.function_ in
      let cfq = N.fq_of_program to_.Fq_function_name.DessserGen.program
                                to_.function_ in
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
