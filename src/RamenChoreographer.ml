(* Service that listen to the RC in the confserver and built and expose the
 * corresponding per site configuration. *)
open Batteries
open Stdint

open RamenHelpersNoLog
open RamenHelpers
open RamenLang
open RamenLog
open RamenSync
open RamenSyncHelpers
module C = RamenConf
module Default = RamenConstsDefault
module VSI = Value.SourceInfo
module VR = Value.Replay
module O = RamenOperation
module N = RamenName
module Services = RamenServices
module ServiceNames = RamenConstsServiceNames
module Files = RamenFiles
module Params = RamenParams
module Processes = RamenProcesses
module ZMQClient = RamenSyncZMQClient
module SetOfSites = Services.SetOfSites
module Supervisor = RamenSupervisor

let sites_matching p =
  SetOfSites.filter (fun (s : N.site) -> Globs.matches p (s :> string))

let worker_signature func params rce =
  Printf.sprintf2 "%s_%s_%b_%g_%s"
    func.VSI.signature
    (Params.signature_of_array params)
    rce.RamenSync.Value.TargetConfig.debug
    rce.report_period
    rce.cwd |>
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

(* For logging purpose, keep the list of programs that are currently
 * conditionally disabled: *)
let cond_disabled = ref Set.String.empty

(* Maps over Value.Worker.func_ref: *)
module FuncRef = struct
  type t = Func_ref.DessserGen.t

  let compare a b =
    match String.compare a.Func_ref.DessserGen.site
                         b.Func_ref.DessserGen.site with
    | 0 ->
        (match String.compare a.program b.program with
        | 0 ->
            String.compare a.func b.func
        | c -> c)
    | c -> c
end
module MapOfFuncs = Map.Make (FuncRef)
module SetOfFuncs = Set.Make (FuncRef)

(* Maps over top half identifiers, namely: *)
module MapOfTopHalves = Map.Make (struct
  type t = (* local part: the parent *)
           FuncRef.t *
           (* remote part: the worker *)
           N.program * N.func * string (* worker signature *) *
           string (* info signature *) *
           (string * T.value) array (* params *)
  let compare (fr1, p1, f1, ws1, is1, ps1) (fr2, p2, f2, ws2, is2, ps2) =
    match FuncRef.compare fr1 fr2 with
    | 0 ->
        (match N.compare p1 p2 with
        | 0 ->
            (match N.compare f1 f2 with
            | 0 ->
                (match String.compare ws1 ws2 with
                | 0 ->
                    (match String.compare is1 is2 with
                    | 0 ->
                      let param_cmp (a, _) (b, _) = String.compare a b in
                      Array.compare param_cmp ps1 ps2
                    | c -> c)
                | c -> c)
            | c -> c)
        | c -> c)
    | c -> c
end)

module MapOfPrograms = Map.Make (struct
  type t = N.program
  let compare = N.compare
end)

module SetOfSiteFqs = Set.Make (struct
  type t = N.site_fq
  let compare = N.site_fq_compare
end)

let site_fq_of_target target =
  N.site target.Fq_function_name.DessserGen.site,
  N.fq_of_program (N.program target.program)
                  (N.func target.function_)

(* Do not build a hashtbl but update the confserver directly,
 * while avoiding to reset the same values. *)
let update_conf_server conf session ?(while_=always) sites rc_entries =
  assert (conf.C.sync_url <> "") ;
  let locate_parents site prog_name func =
    let def_parents = O.parents_of_operation func.VSI.operation in
    if def_parents = [] then None
    else Some (
      List.fold_left (fun parents (psite, rel_pprog, pfunc) ->
        let pprog = O.program_of_parent_prog prog_name rel_pprog in
        let parent_not_found pprog =
          !logger.warning "Cannot find parent %a of %a"
            N.program_print pprog
            N.func_print pfunc ;
          parents in
        let add_parent parents pprog rce =
          (* Where are the parents running? *)
          let where_running =
            let glob = Globs.compile rce.Value.TargetConfig.on_site in
            sites_matching glob sites in
          (* Restricted to where [func] selects from: *)
          let psites =
            match psite with
            | O.AllSites ->
                where_running
            | O.ThisSite ->
                if SetOfSites.mem site where_running then
                  SetOfSites.singleton site
                else
                  SetOfSites.empty
            | O.TheseSites p ->
                sites_matching p where_running in
          SetOfSites.fold (fun psite parents ->
            let worker_ref = Func_ref.DessserGen.{
              site = (psite : N.site :> string) ;
              program = (pprog : N.program :> string) ;
              func = (pfunc : N.func :> string) } in
            worker_ref :: parents
          ) psites parents in
        (* Special case: we might want to read from all workers with a single
         * program and different suffix. This is the only supported way to
         * select from a glob-like list of parents.  This works because all
         * those parents have the same source and therefore the same output
         * type (whereas selecting from an arbitrary glob cannot work in
         * general because the parent type is then unknown).
         * This special case is indicated by the suffix being "_".
         * (TODO: a glob on the suffix to select a subset of the workers with
         * the same program). *)
        (* FIXME: make sure one can not name a program with suffix "_". *)
        if N.suffix_of_program pprog = Some "_" then (
          let psrc_path = N.src_path_of_program pprog in
          !logger.debug "Adding all workers with program %a as parents"
            N.src_path_print psrc_path ;
          (* Add all currently running parents: *)
          Array.fold_left (fun parents (rce_prog, rce) ->
            let rce_src_path = N.src_path_of_program (N.program rce_prog) in
            if N.eq rce_src_path psrc_path &&
               rce.Value.TargetConfig.enabled then (
              !logger.debug "Adding parent %s" rce_prog ;
              add_parent parents (N.program rce_prog) rce
            ) else
              parents
          ) parents rc_entries
        ) else (
          (* Normal case where the parent program name designates only one
           * worker: *)
          match array_assoc (pprog :> string) rc_entries with
          | exception Not_found ->
              parent_not_found pprog
          | rce ->
              if rce.Value.TargetConfig.enabled then
                add_parent parents pprog rce
              else
                parent_not_found pprog
        )
      ) [] def_parents
    ) in
  (* To begin with, collect a list of all used functions (replay target or
   * tail subscriber): *)
  let forced_used =
    fold_my_keys session.ZMQClient.clt (fun k hv set ->
      let add_target site_fq =
        SetOfSiteFqs.add site_fq set in
      match k, hv.value with
      | Key.Replays _,
        Value.Replay replay ->
          let site_fq = site_fq_of_target replay.VR.target in
          add_target site_fq
      | Key.ReplayRequests,
        Value.ReplayRequest replay_request ->
          let site_fq = site_fq_of_target
                          replay_request.Replay_request.DessserGen.target in
          add_target site_fq
      | Key.Tails (site, fq, _, Subscriber _), _ ->
          add_target (site, fq)
      | _ ->
          set
    ) SetOfSiteFqs.empty in
  (* Also, a worker is necessarily used if it is currently archiving: *)
  let is_archiving (site, fq) =
    let k = Key.PerSite (site, PerWorker (fq, AllocedArcBytes)) in
    match (Client.find session.ZMQClient.clt k).value with
    | exception Not_found -> false
    | Value.RamenValue T.(VI64 sz) -> sz > 0L
    | v -> invalid_sync_type k v "a VI64" in
  (* Finally, a worker supposed to persist for some time must also be
   * running, because replayer can not (for now) start missing workers: *)
  let get_param_value (site, fq as site_fq) p =
    let k = Key.PerSite (site, PerWorker (fq, Worker)) in
    match (Client.find session.ZMQClient.clt k).value with
    | exception Not_found ->
        Printf.sprintf2 "No worker for %a" N.site_fq_print site_fq |>
        failwith
    | Value.Worker w ->
        (try array_assoc p w.Value.Worker.params
        with Not_found ->
          Printf.sprintf2 "No parameter named %s" p |>
          failwith)
    | v ->
        invalid_sync_type k v "a worker" in
  let does_persist site_fq func =
    try
      match func.VSI.retention with
      | Some Retention.{ duration = E.{ text = Stateless (SL0 (Const d)) ;
                                        _ } ; _ } ->
          T.float_of_scalar d |> option_get "retention" __LOC__ > 0.
      | Some { duration = E.{ text = Stateless (SL2 (Get, n, _)) ; _ } ; _ } ->
          E.string_of_const n |> option_get "retention" __LOC__ |>
          get_param_value site_fq |>
          T.of_wire |>
          T.float_of_scalar |>
          option_get "scalar retention" __LOC__ > 0.
      | _ -> false
    with e ->
      !logger.error "Cannot evaluate persistence duration %a \
                     for %a: %s, assuming 0!"
        (Option.print Retention.print) func.retention
        N.site_fq_print site_fq
        (Printexc.to_string e) ;
      false in
  let force_used site prog_name func =
    let site_fq = site, N.fq_of_program prog_name func.VSI.name in
    SetOfSiteFqs.mem site_fq forced_used ||
    is_archiving site_fq ||
    does_persist site_fq func
  in
  let all_used = ref SetOfFuncs.empty
  and all_parents = ref MapOfFuncs.empty
  and all_top_halves = ref MapOfTopHalves.empty
  (* indexed by prog_name (which is supposed to be unique within the RC): *)
  and cached_params = ref MapOfPrograms.empty in
  (* Once we have collected in the config tree all the info we need to add
   * a program to the worker graph, do it: *)
  let add_program_with_info prog_name rce k_info where_running = function
    | Value.SourceInfo.{ detail = Compiled info ; _ } ->
        !logger.debug "Found precompiled info in %a" Key.print k_info ;
        let add_worker func site =
          let worker_ref =
            Func_ref.DessserGen.{ site = (site : N.site :> string) ;
                                  program = (prog_name : N.program :> string) ;
                                  func = (func.VSI.name : N.func :> string) } in
          let parents = locate_parents site prog_name func in
          all_parents :=
            MapOfFuncs.add worker_ref (rce, func, parents) !all_parents ;
          let is_used = not func.is_lazy ||
                        O.has_notifications func.operation ||
                        force_used site prog_name func in
          if is_used then
            all_used := SetOfFuncs.add worker_ref !all_used in
        let info_sign = Value.SourceInfo.signature_of_compiled info in
        let rc_params =
          Array.map (fun p ->
            N.field p.Program_run_parameter.DessserGen.name, T.of_wire p.value
          ) rce.Value.TargetConfig.params in
        let params =
          RamenTuple.overwrite_params info.VSI.default_params rc_params |>
          List.map (fun p -> (p.RamenTuple.ptyp.name :> string), p.value) |>
          Array.of_list in
        cached_params :=
          MapOfPrograms.add prog_name (info_sign, params) !cached_params ;
        !logger.debug "Default parameters %a overridden with %a: %a"
          RamenTuple.print_params info.VSI.default_params
          Value.TargetConfig.print_run_params rce.params
          (Array.print (fun oc (n, v) ->
              Printf.fprintf oc "%s=>%a" n T.print v)) params ;
        if Value.SourceInfo.has_running_condition info then (
          if Supervisor.has_executable conf session info_sign then (
            let bin_file = Supervisor.get_executable conf session info_sign in
            let envvars = E.vars_of_expr Env info.condition in
            let envvars =
              N.SetOfFields.fold (fun field envvars ->
                let v =
                  match Sys.getenv (field : N.field :> string) with
                  | exception Not_found ->
                      !logger.debug
                        "Cannot find envvar %a when evaluating the running \
                         condition of %a"
                        N.field_print field
                        N.program_print prog_name ;
                      None
                  | v ->
                      Some v in
                ((field :> string),  v) :: envvars
              ) envvars [] in
            (* The above operation is long enough that we might need this in case
             * many programs have to be compiled: *)
            ZMQClient.may_send_ping ~while_ session ;
            SetOfSites.iter (fun local_site ->
              (* Is this program willing to run on this site? *)
              if Processes.wants_to_run prog_name local_site bin_file params envvars
              then (
                if Set.String.mem (prog_name :> string) !cond_disabled then (
                  !logger.info "Program %a is no longer conditionally disabled!"
                    N.program_print prog_name ;
                  cond_disabled :=
                    Set.String.remove (prog_name :> string) !cond_disabled
                ) ;
                List.iter (fun func ->
                  add_worker func local_site
                ) info.funcs
              ) else (
                if not (Set.String.mem (prog_name :> string) !cond_disabled) then (
                  !logger.info "Program %a is conditionally disabled"
                    N.program_print prog_name ;
                  cond_disabled :=
                    Set.String.add (prog_name :> string) !cond_disabled
                )
              )
            ) where_running
          ) else (
            !logger.info "Must wait until executable %s is ready" info_sign ;
            missing_executable := Set.String.add info_sign !missing_executable ;
          )
        ) else (
          (* unconditionally *)
          List.iter (fun func ->
            SetOfSites.iter (add_worker func) where_running
          ) info.funcs
        )
    | _ ->
        Printf.sprintf2
          "Pre-compilation of %a for program %a had failed"
          Key.print k_info
          N.program_print prog_name |>
        failwith in
  (* When a program is configured to run, gather all info required and call
   * [add_program_with_info]: *)
  let add_program prog_name rce =
    if rce.Value.TargetConfig.on_site = "" then
      !logger.warning "An RC entry is configured to run on no site!" ;
    let where_running =
      let glob = Globs.compile rce.on_site in
      sites_matching glob sites in
    !logger.debug "%a must run on sites matching %S: %a"
      N.program_print prog_name
      rce.on_site
      (SetOfSites.print N.site_print_quoted) where_running ;
    (*
     * Update all_used, all_parents and cached_params:
     *)
    if not (SetOfSites.is_empty where_running) then (
      (* Look for src_path in the configuration: *)
      let src_path = N.src_path_of_program prog_name in
      let k_info = Key.Sources (src_path, "info") in
      match Client.find session.ZMQClient.clt k_info with
      | exception Not_found ->
          !logger.error
            "Cannot find pre-compiled info for source %a for program %a, \
             ignoring this entry"
            N.src_path_print src_path
            N.program_print prog_name
      | { value = Value.SourceInfo info ; _ } ->
          add_program_with_info prog_name rce k_info where_running info
      | hv ->
          invalid_sync_type k_info hv.value "a SourceInfo"
    ) in
  (* Add all RC entries in the workers graph: *)
  Array.enum rc_entries //
  (fun (_, rce) -> rce.Target_config.DessserGen.enabled) |>
  Enum.iter (fun (prog_name, rce) ->
    let what = "Adding an RC entry to the workers graph" in
    log_and_ignore_exceptions ~what (add_program (N.program prog_name)) rce) ;
  (* Propagate usage to parents: *)
  let rec make_used used f =
    if SetOfFuncs.mem f used then used else
    let used = SetOfFuncs.add f used in
    let parents =
      match MapOfFuncs.find f !all_parents with
      | exception Not_found ->
          (* Can happen when [add_program_with_info] failed for whatever
           * reason.  In that case better do as much sync as we can and
           * rely on the next sync attempt to finish the work. *)
          []
      | _rce, _func, Some parents ->
          parents
      | _rce, _func, None ->
          [] in
    List.fold_left make_used used parents in
  let used =
    SetOfFuncs.fold (fun f used ->
      make_used used f
    ) !all_used SetOfFuncs.empty in
  (* Invert parents to get children: *)
  let all_children = ref MapOfFuncs.empty in
  MapOfFuncs.iter (fun child_ref (_rce, _func, parents) ->
    List.iter (fun parent_ref ->
      all_children :=
        MapOfFuncs.modify_opt parent_ref (function
          | None -> Some [ child_ref ]
          | Some children -> Some (child_ref :: children)
        ) !all_children
    ) (parents |? [])
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
  MapOfFuncs.iter (fun worker_ref (rce, func, parents) ->
    let role = Value.Worker.Whole in
    let info_signature, params =
      MapOfPrograms.find (N.program worker_ref.Func_ref.DessserGen.program)
                         !cached_params in
    (* Even lazy functions we do not want to run are part of the stage set by
     * the choreographer, or we wouldn't know which functions are available.
     * They must be in the function graph, they must be compiled (so their
     * fields are known too) but not run by supervisor. *)
    let is_used = SetOfFuncs.mem worker_ref used in
    let children =
      MapOfFuncs.find_default [] worker_ref !all_children |>
      Array.of_list in
    let envvars =
      O.envvars_of_operation func.VSI.operation |>
      Array.of_list |>
      Array.map (fun n -> (n : N.field :> string)) in
    let worker_signature = worker_signature func params rce in
    let parents = Option.map Array.of_list parents in
    let worker : Value.Worker.t =
      { enabled = rce.enabled ; debug = rce.debug ;
        report_period = rce.report_period ; cwd = (rce.cwd :> string) ;
        envvars ; worker_signature ; info_signature ; is_used ;
        params = Array.map (fun (n, v) -> n, T.to_wire v) params ;
        role ; parents ; children } in
    let fq = N.fq_of_program (N.program worker_ref.program)
                             (N.func worker_ref.func) in
    upd (PerSite (N.site worker_ref.site, PerWorker (fq, Worker)))
        (Value.Worker worker) ;
    (* We need a top half for every function with a remote child.
     * If we shared the same top-half for several local parents, then we would
     * have to deal with merging/sorting in the top-half.
     * On the other way around, if we have N remote children with the same FQ
     * names and signatures (therefore the same WHERE filter) then we need only
     * one top-half. *)
    if is_used then (
      (* for each parent... *)
      Array.iter (fun parent_ref ->
        (* ...running on a different site... *)
        if parent_ref.Func_ref.DessserGen.site <> worker_ref.site then (
          let top_half_k =
            (* local part *)
            parent_ref,
            (* remote part *)
            N.program worker_ref.program, N.func worker_ref.func,
            worker_signature, info_signature, params in
          all_top_halves :=
            MapOfTopHalves.modify_opt top_half_k (function
              | None ->
                  Some (rce, func, [ worker_ref.site ])
              | Some (rce, func, sites) ->
                  Some (rce, func, worker_ref.site :: sites)
            ) !all_top_halves
        ) (* else child runs on same site *)
      ) (parents |? [||])
    ) (* else this worker is unused, thus we need no top-half for it *)
  ) !all_parents ;
  (* Now that we have aggregated all top-halves children, actually run them: *)
  MapOfTopHalves.iter (fun (parent_ref, child_prog, child_func,
                 worker_signature, info_signature, params)
                (rce, func, sites) ->
    let service = ServiceNames.tunneld in
    let tunnelds, _ =
      List.fold_left (fun (tunnelds, i) site ->
        match Services.resolve conf (N.site site) service with
        | exception Not_found ->
            !logger.error "No service matching %s:%a, skipping this remote child"
              site
              N.service_print service ;
            tunnelds, i + 1
        | srv ->
            Value.Worker.{
              tunneld_host = (srv.Services.host :> string);
              tunneld_port = Uint16.of_int srv.Services.port ;
              parent_num = Uint32.of_int i
            } :: tunnelds, i + 1
      ) ([], 0) sites in
    let role = Value.Worker.TopHalf (Array.of_list tunnelds) in
    let envvars =
      O.envvars_of_operation func.VSI.operation |>
      Array.of_list |>
      Array.map (fun x -> (x : N.field :> string)) in
    let params = Array.map (fun (n, v) -> n, T.to_wire v) params in
    let worker : Value.Worker.t =
      { enabled = rce.Value.TargetConfig.enabled ;
        debug = rce.debug ; report_period = rce.report_period ;
        cwd = (rce.cwd :> string) ; envvars ; worker_signature ;
        info_signature ;
        is_used = true ; params ; role ;
        parents = Some [| parent_ref |] ; children = [||] } in
    let fq = N.fq_of_program child_prog child_func in
    upd (PerSite (N.site parent_ref.site, PerWorker (fq, Worker)))
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
  let synced = ref false in
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
        (if !synced then !logger.error else !logger.debug)
          "Key %a does not exist yet!?"
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
  let update_if_source_used session src_path reason =
    with_current_rc session (fun rc ->
      if Array.exists (fun (prog_name, _rce) ->
           N.src_path_of_program (N.program prog_name) = src_path
         ) rc
      then (
        !logger.debug "Found an RC entry using %a"
          N.src_path_print src_path ;
        need_update := true
      ) else (
        !logger.debug "No RC entries are using %s source %a (only %a)"
          reason
          N.src_path_print src_path
          (pretty_enum_print N.src_path_print)
            (Array.enum rc /@ (fun (prog_name, _) ->
              N.src_path_of_program (N.program prog_name)))
      )) in
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
        Array.iter (make_used session % Value.Worker.site_fq_of_ref)
                   (worker.parents |? [||])
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
        update_if_source_used session src_path "updated"
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
        if Set.String.mem info_sign !missing_executable then (
          !logger.info "New executable available locally: %a"
            Key.print k ;
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
     * not running. Should the request not be turned into a replay then
     * supervisor may start then stop lazy worker(s) for no reason. *)
    | Key.Replays _,
      Value.Replay replay ->
        let site_fq = site_fq_of_target replay.VR.target in
        make_used session site_fq
    | Key.ReplayRequests,
      Value.ReplayRequest request
      when not request.Replay_request.DessserGen.explain ->
        let site_fq = site_fq_of_target request.target in
        make_used session site_fq
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
        let site_fq = site_fq_of_target replay.VR.target in
        update_if_running session site_fq
    | Key.ReplayRequests,
      Value.ReplayRequest replay_request ->
        let site_fq = site_fq_of_target
                        replay_request.Replay_request.DessserGen.target in
        update_if_running session site_fq
    | Key.Tails (site, fq, instance, Subscriber _),
      _ ->
        if not (has_subscriber session site fq instance) then
          update_if_running session (site, fq)
    | Key.Sources (src_path, "info"),
      _ ->
        (* Deletion of source -> stop the workers *)
        update_if_source_used session src_path "deleted"
    | _ -> () in
  let sync_loop session =
    synced := true ;  (* Help diagnosing some condition *)
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
