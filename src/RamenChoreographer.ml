(* Service that listen to the RC in the confserver and built and expose the
 * corresponding per site configuration. *)
open Batteries
open RamenLog
open RamenHelpers
open RamenConsts
module C = RamenConf
module F = C.Func
module FS = F.Serialized
module O = RamenOperation
module N = RamenName
module Services = RamenServices
module Files = RamenFiles
module ZMQClient = RamenSyncZMQClient

let sites_matching p =
  Set.filter (fun (s : N.site) -> Globs.matches p (s :> string))

(* Do not build a hashtbl but update the confserver directly. *)
let update_conf_server conf ?(while_=always) zock clt sites rc_entries =
  assert (conf.C.sync_url <> "") ;
  let locate_parents site pname func =
    O.parents_of_operation func.FS.operation |>
    List.fold_left (fun parents (psite, rel_pprog, pfunc) ->
      let pprog = F.program_of_parent_prog pname rel_pprog in
      match List.assoc pprog rc_entries with
      | exception Not_found ->
          !logger.warning "Cannot find parent %a of %a"
            N.program_print pprog
            N.func_print pfunc ;
          parents
      | rce ->
          (* Where are the parents running? *)
          let where_running =
            let glob = Globs.compile rce.RamenSync.Value.TargetConfig.on_site in
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
            let worker_ref = RamenSync.Value.Worker.{
              site = psite ; program = pprog ; func = pfunc} in
            worker_ref :: parents
          ) psites parents
    ) [] in
  let force_used _p _f =
    (* TODO: a set of keys to indicate that a given function is (temporarily)
     * read by some command and must be run regardless of lazyness. *)
    false
  in
  let all_used = ref Set.empty in
  let all_parents = ref Map.empty in
  List.iter (fun (pname, rce) ->
    let where_running =
      let glob = Globs.compile rce.RamenSync.Value.TargetConfig.on_site in
      sites_matching glob sites in
    !logger.debug "%a must run on sites matching %S: %a"
      N.program_print pname
      rce.on_site
      (Set.print N.site_print_quoted) where_running ;
    (* Look for rce.src_path in the configuration: *)
    let src_path = Files.remove_ext rce.src_path in
    let k_info = RamenSync.Key.Sources (src_path, "info") in
    match ZMQClient.Client.H.find clt.ZMQClient.Client.h k_info with
    | exception Not_found ->
        !logger.error
          "Cannot find pre-compiled info for source %a for program %a, \
           ignoring this entry"
          N.path_print rce.src_path
          N.program_print pname
    | { value = RamenSync.Value.SourceInfo {
                  detail = Compiled info ; _ } ; _ } ->
        List.iter (fun func ->
          Set.iter (fun local_site ->
            let worker_ref =
              RamenSync.Value.Worker.{
                site = local_site ;
                program = pname ;
                func = func.FS.name } in
            let parents = locate_parents local_site pname func in
            all_parents :=
              Map.add worker_ref (rce, info, func, parents) !all_parents ;
            let is_used =
              not func.is_lazy ||
              O.has_notifications func.operation ||
              force_used pname func.name in
            if is_used then
              all_used := Set.add worker_ref !all_used ;
          ) where_running
        ) info.funcs
    | { value = RamenSync.Value.SourceInfo _ ; _ } ->
        !logger.error
          "Pre-compilation of source %a for program %a had failed, \
           ignoring this entry"
          N.path_print rce.src_path
          N.program_print pname
    | _ -> ()
  ) rc_entries ;
  (* Propagate usage to parents: *)
  let rec make_used used f =
    if Set.mem f used then used else
    let used = Set.add f used in
    let _rce, _info, _func, parents = Map.find f !all_parents in
    List.fold_left make_used used parents in
  let used = Set.fold (fun f used -> make_used used f) !all_used Set.empty in
  (* Invert parents to get children: *)
  let all_children = ref Map.empty in
  Map.iter (fun child_ref (_rce, _info, _func, parents) ->
    List.iter (fun parent_ref ->
      all_children :=
        Map.modify_opt parent_ref (function
          | None -> Some [ child_ref ]
          | Some children -> Some (child_ref :: children)
        ) !all_children
    ) parents
  ) !all_parents ;
  (* Now set the keys: *)
  let set_keys = ref Set.empty in
  let upd k v =
    set_keys := Set.add k !set_keys ;
    ZMQClient.send_cmd clt zock ~while_ (SetKey (k, v)) in
  Map.iter (fun worker_ref (rce, info, func, parents) ->
    (* TODO: also add top halves *)
    let role = RamenSync.Value.Worker.Whole in
    let rc_params =
      List.enum rce.RamenSync.Value.TargetConfig.params |>
      Hashtbl.of_enum in
    let params =
      RamenTuple.overwrite_params
        info.RamenSync.Value.SourceInfo.default_params rc_params |>
      List.map (fun p -> p.RamenTuple.ptyp.name, p.value) in
    let worker : RamenSync.Value.Worker.t =
      { enabled = rce.enabled ;
        debug = rce.debug ;
        report_period = rce.report_period ;
        src_path = Files.remove_ext rce.src_path ;
        signature =
          func.FS.signature ^"_"^ RamenParams.signature_of_list params ;
        is_used = Set.mem worker_ref used ;
        params ;
        envvars = O.envvars_of_operation func.FS.operation ;
        role ;
        parents ;
        children = Map.find_default [] worker_ref !all_children } in
    let fq = N.fq_of_program worker_ref.program worker_ref.func in
    upd (PerSite (worker_ref.site, PerWorker (fq, Worker)))
        (RamenSync.Value.Worker worker)
  ) !all_parents ;
  (* And delete unused: *)
  ZMQClient.Client.H.iter (fun k _ ->
    if not (Set.mem k !set_keys) then
      match k with
      | PerSite (_, PerWorker (_, Worker)) ->
          ZMQClient.send_cmd clt zock ~while_ (DelKey k)
      | _ -> ()
  ) clt.ZMQClient.Client.h

let start conf ~while_ =
  let topics =
    [ (* Read target config from this key: *)
      "target_config" ;
      (* Write the function graph into these keys: *)
      "sites/*/workers/*/worker" ;
      (* Get source info from these: *)
      "sources/*/info" ] in
  (* The keys set by the choreographer (and only her): *)
  let is_my_key = function
    | RamenSync.Key.PerSite (_, (PerWorker (_, Worker))) -> true
    | _ -> false in
  let open RamenSync in
  let on_set zock clt k v _uid _mtime =
    match k, v with
    | Key.TargetConfig, Value.TargetConfig rc  ->
        let open ZMQClient in
        lock_matching clt zock ~while_ is_my_key (fun () ->
          finally
            (fun () -> unlock_matching clt zock ~while_ is_my_key ignore)
            (fun () ->
              let sites = Services.all_sites conf in
              update_conf_server conf ~while_ zock clt sites rc) ())
    | _ -> ()
  in
  ZMQClient.start ~while_ ~on_new:on_set ~on_set conf.C.sync_url conf.C.login
                  ~topics (fun zock clt ->
    let num_msg = ZMQClient.process_in zock clt in
    !logger.debug "Received %d messages" num_msg)
