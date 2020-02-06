(* Service that listen to the RC in the confserver and built and expose the
 * corresponding per site configuration. *)
open Batteries
open RamenLog
open RamenHelpers
open RamenConsts
open RamenSyncHelpers
open RamenSync
module C = RamenConf
module VSI = Value.SourceInfo
module VR = Value.Replay
module O = RamenOperation
module N = RamenName
module Services = RamenServices
module Files = RamenFiles
module Processes = RamenProcesses
module ZMQClient = RamenSyncZMQClient
module Supervisor = RamenSupervisor

let sites_matching p =
  Set.filter (fun (s : N.site) -> Globs.matches p (s :> string))

let worker_signature func params rce =
  Printf.sprintf2 "%s_%s_%b_%g_%a"
    func.VSI.signature
    (RamenParams.signature_of_list params)
    rce.RamenSync.Value.TargetConfig.debug
    rce.report_period
    N.path_print rce.cwd |>
  N.md5

let fold_my_keys clt f u =
  Client.fold clt ~prefix:"replays/" f u |>
  Client.fold clt ~prefix:"replay_request" f |>
  Client.fold clt ~prefix:"tails/" f

let has_subscriber session site fq instance =
  let prefix =
    Printf.sprintf2 "tails/%a/%a/%s/users/"
      N.site_print site
      N.fq_print fq
      instance in
  try
    Client.iter session.ZMQClient.clt ~prefix (fun _k _v -> raise Exit) ;
    true
  with Exit ->
    false

(* If an executable is required that is not available yet, write it down there
 * and wait for it: *)
let missing_executable = ref Set.String.empty

(* Do not build a hashtbl but update the confserver directly,
 * while avoiding to reset the same values. *)
let update_conf_server conf session ?(while_=always) sites rc_entries =
  assert (conf.C.sync_url <> "") ;
  let locate_parents site pname func =
    O.parents_of_operation func.VSI.operation |>
    List.fold_left (fun parents (psite, rel_pprog, pfunc) ->
      let pprog = O.program_of_parent_prog pname rel_pprog in
      let parent_not_found pprog =
        !logger.warning "Cannot find parent %a of %a"
          N.program_print pprog
          N.func_print pfunc ;
        parents in
      match List.assoc pprog rc_entries with
      | exception Not_found ->
          parent_not_found pprog
      | rce when rce.Value.TargetConfig.enabled ->
          (* Where are the parents running? *)
          let where_running =
            let glob = Globs.compile rce.on_site in
            sites_matching glob sites in
          (* Restricted to where [func] selects from: *)
          let psites =
            match psite with
            | O.AllSites ->
                where_running
            | O.ThisSite ->
                if Set.mem site where_running then
                  Set.singleton site
                else
                  Set.empty
            | O.TheseSites p ->
                sites_matching p where_running in
          Set.fold (fun psite parents ->
            let worker_ref = Value.Worker.{
              site = psite ; program = pprog ; func = pfunc} in
            worker_ref :: parents
          ) psites parents
      | _ ->
          parent_not_found pprog
    ) [] in
  (* To begin with, collect a list of all used functions (replay target or
   * tail subscriber): *)
  let forced_used =
    fold_my_keys session.ZMQClient.clt (fun k hv set ->
      let add_target site_fq =
        Set.add site_fq set in
      match k, hv.value with
      | Key.Replays _,
        Value.Replay replay ->
          add_target replay.VR.target
      | Key.ReplayRequests,
        Value.ReplayRequest replay_request ->
          add_target replay_request.Value.Replay.target
      | Key.Tails (site, fq, _, Subscriber _), _ ->
          add_target (site, fq)
      | _ ->
          set
    ) Set.empty in
  (* Also, a worker is necessarily used if it is archiving: *)
  let is_archiving (site, fq) =
    let k = Key.PerSite (site, PerWorker (fq, AllocedArcBytes)) in
    match (Client.find session.ZMQClient.clt k).value with
    | exception Not_found -> false
    | Value.RamenValue T.(VI64 sz) -> sz > 0L
    | v -> invalid_sync_type k v "a VI64" in
  let force_used site pname fname =
    let site_fq = site, N.fq_of_program pname fname in
    Set.mem site_fq forced_used || is_archiving site_fq
  in
  let all_used = ref Set.empty
  and all_parents = ref Map.empty
  and all_top_halves = ref Map.empty
  (* indexed by pname (which is supposed to be unique within the RC): *)
  and cached_params = ref Map.empty in
  (* Once we have collected in the config tree all the info we need to add
   * a program to the worker graph, do it: *)
  let add_program_with_info pname rce k_info where_running = function
    | Value.SourceInfo.{ detail = Compiled info ; _ } ->
        !logger.debug "Found precompiled info in %a" Key.print k_info ;
        let add_worker func site =
          let worker_ref =
            Value.Worker.{ site ; program = pname ; func = func.VSI.name } in
          let parents = locate_parents site pname func in
          all_parents := Map.add worker_ref (rce, func, parents) !all_parents ;
          let is_used = not func.is_lazy ||
                        O.has_notifications func.operation ||
                        force_used site pname func.name in
          if is_used then
            all_used := Set.add worker_ref !all_used in
        let info_sign = Value.SourceInfo.signature_of_compiled info in
        let rc_params =
          List.enum rce.Value.TargetConfig.params |>
          Hashtbl.of_enum in
        let params =
          RamenTuple.overwrite_params
            info.VSI.default_params rc_params |>
          List.map (fun p -> p.RamenTuple.ptyp.name, p.value) in
        cached_params :=
          Map.add pname (info_sign, params) !cached_params ;
        if Value.SourceInfo.has_running_condition info then (
          if Supervisor.has_executable conf session info_sign then (
            let params = hashtbl_of_alist params in
            let bin_file = Supervisor.get_executable conf session info_sign in
            (* The above operation is long enough that we might need this in case
             * many programs have to be compiled: *)
            ZMQClient.may_send_ping ~while_ session ;
            List.iter (fun func ->
              Set.iter (fun local_site ->
                (* Is this program willing to run on this site? *)
                if Processes.wants_to_run conf local_site bin_file params then (
                  add_worker func local_site
                ) else (
                  !logger.debug "Program %a is conditionally disabled"
                    N.program_print pname
                )
              ) where_running
            ) info.funcs
          ) else (
            !logger.info "Must wait until executable is ready" ;
            missing_executable := Set.String.add info_sign !missing_executable ;
          )
        ) else (
          (* unconditionally *)
          List.iter (fun func ->
            Set.iter (add_worker func) where_running
          ) info.funcs
        )
    | _ ->
        Printf.sprintf2
          "Pre-compilation of %a for program %a had failed"
          Key.print k_info
          N.program_print pname |>
        failwith in
  (* When a program is configured to run, gather all info required and call
   * [add_program_with_info]: *)
  let add_program pname rce =
    if rce.Value.TargetConfig.on_site = "" then
      !logger.warning "An RC entry is configured to run on no site!" ;
    let where_running =
      let glob = Globs.compile rce.on_site in
      sites_matching glob sites in
    !logger.debug "%a must run on sites matching %S: %a"
      N.program_print pname
      rce.on_site
      (Set.print N.site_print_quoted) where_running ;
    (*
     * Update all_used, all_parents and cached_params:
     *)
    if not (Set.is_empty where_running) then (
      (* Look for src_path in the configuration: *)
      let src_path = N.src_path_of_program pname in
      let k_info = Key.Sources (src_path, "info") in
      match Client.find session.ZMQClient.clt k_info with
      | exception Not_found ->
          !logger.error
            "Cannot find pre-compiled info for source %a for program %a, \
             ignoring this entry"
            N.src_path_print src_path
            N.program_print pname
      | { value = Value.SourceInfo info ; _ } ->
          add_program_with_info pname rce k_info where_running info
      | hv ->
          invalid_sync_type k_info hv.value "a SourceInfo"
    ) in
  (* Add all RC entries in the workers graph: *)
  List.enum rc_entries //
  (fun (_, rce) -> rce.enabled) |>
  Enum.iter (fun (pname, rce) ->
    let what = "Adding an RC entry to the workers graph" in
    log_and_ignore_exceptions ~what (add_program pname) rce) ;
  (* Propagate usage to parents: *)
  let rec make_used used f =
    if Set.mem f used then used else
    let used = Set.add f used in
    let parents =
      match Map.find f !all_parents with
      | exception Not_found ->
          (* Can happen when [add_program_with_info] failed for whatever
           * reason.  In that case better do as much sync as we can and
           * rely on the next sync attempt to finish the work. *)
          []
      | _rce, _func, parents ->
          parents in
    List.fold_left make_used used parents in
  let used = Set.fold (fun f used -> make_used used f) !all_used Set.empty in
  (* Invert parents to get children: *)
  let all_children = ref Map.empty in
  Map.iter (fun child_ref (_rce, _func, parents) ->
    List.iter (fun parent_ref ->
      all_children :=
        Map.modify_opt parent_ref (function
          | None -> Some [ child_ref ]
          | Some children -> Some (child_ref :: children)
        ) !all_children
    ) parents
  ) !all_parents ;
  (* Now set the keys: *)
  let set_keys = ref Set.empty in (* Set of keys that will not be deleted *)
  let upd k v =
    set_keys := Set.add k !set_keys ;
    let must_be_set  =
      match (Client.find session.ZMQClient.clt k).value with
      | exception Not_found -> true
      | v' -> not (Value.equal v v') in
    if must_be_set then
      ZMQClient.send_cmd ~while_ session (SetKey (k, v))
    else
      !logger.debug "Key %a keeps its value"
        Key.print k in
  (* Notes regarding non-local children/parents:
   * We never ref top-halves (a ref is only site/program/function, no
   * role). But every time a children is not local, it can be assumed
   * that a top-half will run on the local site for that program/function.
   * Similarly, every time a parent is not local it can be assumed
   * a top-half runs on the remote site for the present program/function.
   * Top halves created by the choreographer will have no children (the
   * tunnelds they connect to are to be found in the role record) and
   * the actual parent as parents. *)
  Map.iter (fun worker_ref (rce, func, parents) ->
    let role = Value.Worker.Whole in
    let info_signature, params =
      Map.find worker_ref.Value.Worker.program !cached_params in
    (* Even lazy functions we do not want to run are part of the stage set by
     * the choreographer, or we wouldn't know which functions are available.
     * They must be in the function graph, they must be compiled (so their
     * fields are known too) but not run by supervisor. *)
    let is_used = Set.mem worker_ref used in
    let children =
      Map.find_default [] worker_ref !all_children in
    let envvars = O.envvars_of_operation func.VSI.operation in
    let worker_signature = worker_signature func params rce in
    let worker : Value.Worker.t =
      { enabled = rce.enabled ; debug = rce.debug ;
        report_period = rce.report_period ; cwd = rce.cwd ;
        envvars ; worker_signature ; info_signature ;
        is_used ; params ; role ; parents ; children } in
    let fq = N.fq_of_program worker_ref.program worker_ref.func in
    upd (PerSite (worker_ref.site, PerWorker (fq, Worker)))
        (Value.Worker worker) ;
    (* We need a top half for every function with a remote child.
     * If we shared the same top-half for several local parents, then we would
     * have to deal with merging/sorting in the top-half.
     * On the other way around, if we have N remote children with the same FQ
     * names and signatures (therefore the same WHERE filter) then we need only
     * one top-half. *)
    if is_used then (
      (* for each parent... *)
      List.iter (fun parent_ref ->
        (* ...running on a different site... *)
        if parent_ref.Value.Worker.site <> worker_ref.site then (
          let top_half_k =
            (* local part *)
            parent_ref,
            (* remote part *)
            worker_ref.program, worker_ref.func,
            worker_signature, info_signature, params in
          all_top_halves :=
            Map.modify_opt top_half_k (function
              | None ->
                  Some (rce, func, [ worker_ref.site ])
              | Some (rce, func, sites) ->
                  Some (rce, func, worker_ref.site :: sites)
            ) !all_top_halves
        ) (* else child runs on same site *)
      ) parents
    ) (* else this worker is unused, thus we need no top-half for it *)
  ) !all_parents ;
  (* Now that we have aggregated all top-halves children, actually run them: *)
  Map.iter (fun (parent_ref, child_prog, child_func,
                 worker_signature, info_signature, params)
                (rce, func, sites) ->
    let service = ServiceNames.tunneld in
    let tunnelds, _ =
      List.fold_left (fun (tunnelds, i) site ->
        match Services.resolve conf site service with
        | exception Not_found ->
            !logger.error "No service matching %a:%a, skipping this remote child"
              N.site_print site
              N.service_print service ;
            tunnelds, i + 1
        | srv ->
            Value.Worker.{
              tunneld_host = srv.Services.host ;
              tunneld_port = srv.Services.port ;
              parent_num = i
            } :: tunnelds, i + 1
      ) ([], 0) sites in
    let role = Value.Worker.TopHalf tunnelds in
    let envvars = O.envvars_of_operation func.VSI.operation in
    let worker : Value.Worker.t =
      { enabled = rce.Value.TargetConfig.enabled ;
        debug = rce.debug ; report_period = rce.report_period ;
        cwd = rce.cwd ; envvars ; worker_signature ; info_signature ;
        is_used = true ; params ; role ;
        parents = [ parent_ref ] ; children = [] } in
    let fq = N.fq_of_program child_prog child_func in
    upd (PerSite (parent_ref.site, PerWorker (fq, Worker)))
        (Value.Worker worker)
  ) !all_top_halves ;
  (* And delete unused: *)
  Client.iter session.ZMQClient.clt (fun k _ ->
    if not (Set.mem k !set_keys) then
      match k with
      | PerSite (_, PerWorker (_, Worker)) ->
          ZMQClient.send_cmd ~while_ session (DelKey k)
      | _ -> ())

(* Choreographer do not have to react very quickly to changes but in two
 * occasions: new replays and new tail subscribers.
 *
 * - Change in a worker: as choreographer is supposed to be the
 *   only writer for those, it is likely the originator of the change.
 *   Just let the new value be recorded in the config tree so that we can
 *   latter skip an update that does not alter the value.
 *
 * - Change of allocated storage size: we are only interested when it goes
 *   from zero to non-zero or the other way around, to force or not this
 *   worker into running. A change of a running flag can cascade to parents
 *   forced status, which can in turn lead to workers being created or
 *   deleted. Thus function to parents relationships must be known (can
 *   be obtained from the conftree as needed), as well as previous forced
 *   status (stored in the workers is_used flag). It is enough to schedule
 *   a complete update.
 *
 * - Change of source info: have to reassess all children and then parents if
 *   this worker used flag is changed. Children are no readily known though.
 *   Schedule a complete update.
 *
 * - Change in target config: unless we diff each entries individually (but
 *   then it's better to split that key) everything must be reassessed. Thus,
 *   also schedule a complete update.
 *
 * - Change in replays/tailers: can only impact the is_used flag of existing
 *   workers. Of all the changes this is the only ones that need to be
 *   happen as quickly as possible, as it is on the query path. Setting the
 *   is_used flags recursively toward parents is trivial, removing it when a
 *   replay is removed is less so, but this need not be fast. *)

let start conf ~while_ =
  let topics =
    [ (* Write the function graph into these keys. Have to know the server
         values to avoid rewriting the same values. *)
      "sites/*/workers/*/worker" ;
      (* Get archival info from those: *)
      "sites/*/workers/*/archives/alloc_size" ;
      (* Get the executables from there: *)
      "sites/*/programs/*/executable" ;
      (* Get source info from these: *)
      "sources/*/info" ;
      (* Read target config from this key: *)
      "target_config" ;
      (* React to new replay requests/tail subscriptions to mark their target
       * as used: *)
      "replays/*" ;
      "replay_requests" ;
      "tails/*/*/*/users/*" ] in
  (* The keys set by the choreographer (and only her): *)
  let is_my_key = function
    | Key.PerSite (_, (PerWorker (_, Worker))) -> true
    | _ -> false in
  let prefix = "sites/" in
  let need_update = ref false in
  let last_update = ref 0. in
  let do_update session rc =
    !logger.info "Attempting to update the running configuration" ;
    let lock_timeo = 120. in
    (* If this fails to acquire all the locks, likely because we are already
     * busy compiling, then it will be retried later: *)
    ZMQClient.with_locked_matching
      session ~lock_timeo ~prefix ~while_ is_my_key
      (fun () ->
        let sites = Services.all_sites conf in
        (* Clear the dirty flag first so new changes while we are busy
         * compiling will not be forgotten: *)
        need_update := false ;
        last_update := Unix.time () ;
        update_conf_server conf session ~while_ sites rc ;
        !logger.info "Running configuration updated") in
  let with_current_rc session cont =
    match (Client.find session.ZMQClient.clt Key.TargetConfig).value with
    | exception Not_found ->
        !logger.warning "Key %a does not exist yet!?"
          Key.print Key.TargetConfig
    | Value.TargetConfig rc ->
        cont rc
    | v ->
        err_sync_type Key.TargetConfig v "a TargetConfig" in
  let update_if_not_running session (site, fq) =
    (* Just have a cursory look at the current local configuration: *)
    let worker_k = Key.PerSite (site, PerWorker (fq, Worker)) in
    match (Client.find session.ZMQClient.clt worker_k).value with
    | exception Not_found ->
        need_update := true
    | Value.Worker worker ->
        if not worker.Value.Worker.is_used then
          need_update := true
    | _ ->
        () in
  let update_if_running session (site, fq) =
    (* Just have a cursory look at the current local configuration: *)
    let worker_k = Key.PerSite (site, PerWorker (fq, Worker)) in
    match (Client.find session.ZMQClient.clt worker_k).value with
    | exception Not_found ->
        ()
    | Value.Worker worker ->
        if worker.Value.Worker.is_used then
          need_update := true
    | _ ->
        () in
  let rec make_used session (site, fq) =
    let k = Key.PerSite (site, PerWorker (fq, Worker)) in
    match (Client.find session.ZMQClient.clt k).value with
    | exception Not_found ->
        !logger.warning "Replay or Tail about unknown worker %a"
          Key.print k ;
        need_update := true
    | Value.Worker worker
      when not worker.Value.Worker.is_used ->
        let v = Value.Worker { worker with is_used = true } in
        ZMQClient.send_cmd ~while_ session (SetKey (k, v)) ;
        (* Also make all parents used: *)
        List.iter (make_used session % Value.Worker.site_fq_of_ref) worker.parents
    | _ ->
        () in
  let on_set session k v _uid _mtime =
    match k, v with
    | Key.TargetConfig, Value.TargetConfig _ ->
        need_update := true
    | Key.Sources (src_path, "info"),
      Value.SourceInfo { detail = Compiled _ ; _ } ->
        (* If any rc entry uses this source, then an entry may have
         * to be updated (directly the workers of this entry, but also
         * the workers whose parents/children are using this source).
         * So in case this source is used anywhere at all then the whole
         * graph of workers is recomputed. *)
        with_current_rc session (fun rc ->
          if List.exists (fun (pname, _rce) ->
               N.src_path_of_program pname = src_path) rc then
            need_update := true
          else
            !logger.debug "No RC entries are using updated source %a (only %a)"
              N.src_path_print src_path
              (pretty_enum_print N.src_path_print)
                (List.enum rc /@ (fun (pname, _) -> N.src_path_of_program pname)))
    (* In case a non-running lazy function is allocated some storage space
     * then it must now be started: *)
    (* TODO: and the other way around! *)
    | Key.PerSite (site, PerWorker (fq, AllocedArcBytes)),
      Value.RamenValue T.(VI64 sz) ->
        if sz > 0L then
          update_if_not_running session (site, fq)
        else
          update_if_running session (site, fq)
    (* Each time a new executable is available locally give it another try: *)
    | Key.PerSite (site, PerProgram (info_sign, Executable)), _
      when site = conf.C.site ->
        !logger.info "New executable available locally: %a"
          Key.print k ;
        if Set.String.mem info_sign !missing_executable then (
          !logger.info "This executable was missed" ;
          missing_executable := Set.String.remove info_sign !missing_executable ;
          need_update := true
        )
    (* Replay targets must be flagged as used (see force_used) or replayers
     * will output into non existent children.
     * Exception: when the source is also the target, then the replayer is
     * enough (TODO).
     * Conversely, they should be unflagged when the replays and tail
     * subscribers are deleted, maybe with some delay.
     * Notice that reacting to ReplayRequests is not strictly necessary, as a
     * Replay will follow, but it helps reduce latency in case the target was
     * not running. *)
    | Key.Replays _,
      Value.Replay replay ->
        make_used session replay.VR.target
    | Key.ReplayRequests,
      Value.ReplayRequest request
      when not request.Value.Replay.explain ->
        make_used session request.Value.Replay.target
    | Key.Tails (site, fq, _, Subscriber _),
      _ ->
        make_used session (site, fq)
    | _ -> () in
  let on_new session k v uid mtime _can_write _can_del _owner _expiry =
    on_set session k v uid mtime in
  let on_del session k v =
    match k, v with
    (* In case a running lazy function is no longer allocated any storage
     * space then it can now be stopped: *)
    | Key.PerSite (site, PerWorker (fq, AllocedArcBytes)),
      _ ->
        update_if_running session (site, fq)
    (* Replayed functions are not necessarily running after the replay is
     * over: *)
    | Key.Replays _,
      Value.Replay replay ->
        update_if_running session replay.VR.target
    | Key.ReplayRequests,
      Value.ReplayRequest replay_request ->
        update_if_running session replay_request.Value.Replay.target
    | Key.Tails (site, fq, instance, Subscriber _),
      _ ->
        if not (has_subscriber session site fq instance) then
          update_if_running session (site, fq)
    | _ -> () in
  let sync_loop session =
    while while_ () do
      if !need_update && !last_update < Unix.time () then
        (* Will clean the dirty flag if successful: *)
        with_current_rc session (do_update session) ;
      ZMQClient.process_in ~while_ session
    done
  in
  start_sync conf ~while_ ~on_new ~on_set ~on_del ~topics ~recvtimeo:1.
             ~sesstimeo:Default.sync_long_sessions_timeout
             sync_loop
