(* Service that listen to the RC in the confserver and built and expose the
 * corresponding per site configuration. *)
open Batteries
open RamenLog
open RamenHelpers
open RamenConsts
module C = RamenConf
module F = C.Func
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
    O.parents_of_operation func.RamenSync.Value.operation |>
    List.fold_left (fun parents (psite, rel_pprog, pfunc) ->
      let pprog = F.program_of_parent_prog pname rel_pprog in
      match List.assoc pprog rc_entries with
      | exception Not_found ->
          !logger.warning "Cannot find parent %a of %a"
            N.program_print pprog
            N.func_print pfunc ;
          parents
      | rce ->
          if not rce.RamenSync.Value.enabled then (
            !logger.warning "Parent %a is disabled"
              N.program_print pprog ;
            parents
          ) else (
            (* Where are the parents running? *)
            let where_running =
              sites_matching (Globs.compile rce.on_site) sites in
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
            Set.map (fun h -> h, pprog, pfunc) psites |>
            Set.union parents
          )
    ) Set.empty in
  let force_used _p _f =
    (* TODO: a set of keys to indicate that a given function is (temporarily)
     * read by some command and must be run regardless of lazyness. *)
    false
  in
  let all_used = ref Set.empty in
  let all_parents = ref Map.empty in
  List.iter (fun (pname, rce) ->
    if rce.RamenSync.Value.enabled then (
      let where_running =
        sites_matching (Globs.compile rce.on_site) sites in
      !logger.debug "%a must run on sites matching %S: %a"
        N.program_print pname
        rce.on_site
        (Set.print N.site_print_quoted) where_running ;
      (* Look for rce.src_file in the configuration: *)
      let k_info = RamenSync.Key.Sources (Files.remove_ext rce.src_file, "info") in
      match ZMQClient.Client.H.find clt.ZMQClient.Client.h k_info with
      | exception Not_found ->
          !logger.error
            "Cannot find pre-compiled info for source %a for program %a, \
             ignoring this entry"
            N.path_print rce.src_file
            N.program_print pname
      | { v = RamenSync.Value.SourceInfo {
                detail = CompiledSourceInfo info ; _ } ; _ } ->
          List.iter (fun func ->
            Set.iter (fun local_site ->
              let parents = locate_parents local_site pname func in
              all_parents :=
                Map.add (local_site, pname, func.name) parents !all_parents ;
              let is_used =
                not func.is_lazy ||
                O.has_notifications func.operation ||
                force_used pname func.name in
              if is_used then
                all_used := Set.add (local_site, pname, func.name) !all_used ;
            ) where_running
          ) info.funcs
      | { v = RamenSync.Value.SourceInfo _ ; _ } ->
          !logger.error
            "Pre-compilation of source %a for program %a had failed, \
             ignoring this entry"
            N.path_print rce.src_file
            N.program_print pname
      | _ -> ()
    ) (* else this program is not running *)
  ) rc_entries ;
  (* Propagate usage to parents: *)
  let rec make_used f used =
    if Set.mem f used then used else
      let used = Set.add f used in
      let parents = Map.find f !all_parents in
      Set.fold make_used parents used in
  let used = Set.fold make_used !all_used Set.empty in
  (* Now set the keys: *)
  let set_keys = ref Set.empty in
  let upd k v =
    set_keys := Set.add k !set_keys ;
    ZMQClient.send_cmd zock ~while_ (SetKey (k, v)) in
  Map.iter (fun (site, pname, fname as f) parents ->
    let fq = N.fq_of_program pname fname in
    upd (PerSite (site, PerWorker (fq, IsUsed)))
        (RamenSync.Value.Bool (Set.mem f used)) ;
    set_iteri (fun i (psite, pprog, pfunc) ->
      upd (PerSite (site, PerWorker (fq, Parents i)))
          (RamenSync.Value.Worker (psite, pprog, pfunc))
    ) parents
  ) !all_parents ;
  (* And delete unused: *)
  ZMQClient.Client.H.iter (fun k _ ->
    if not (Set.mem k !set_keys) then
      match k with
      | PerSite (_, PerWorker (_, (IsUsed | Parents _))) ->
          ZMQClient.send_cmd zock ~while_ (DelKey k)
      | _ -> ()
  ) clt.ZMQClient.Client.h

let start conf ~while_ =
  let topics =
    [ (* Read target config from this key: *)
      "must_run" ;
      (* Write the function graph in those keys: *)
      "sites/*/workers/*/is_used" ;
      "sites/*/workers/*/parents/*" ;
      (* Get source info from those: *)
      "sources/*/info" ] in
  (* The keys set by the choreographer (and only her): *)
  let is_my_key = function
    | RamenSync.Key.PerSite (_, (PerWorker (_, (IsUsed | Parents _)))) -> true
    | _ -> false in
  let open RamenSync in
  let on_set zock clt k v _uid _mtime =
    match k, v with
    | Key.TargetConfig, Value.TargetConfig rc  ->
        let open ZMQClient in
        lock_matching clt is_my_key ;
        finally
          (fun () -> unlock_matching clt is_my_key)
          (fun () ->
            let sites = Services.all_sites conf in
            update_conf_server conf ~while_ zock clt sites rc) ()
    | _ -> ()
  in
  ZMQClient.start ~while_ ~on_new:on_set ~on_set conf.C.sync_url conf.C.login
                  ~topics (fun zock clt ->
    let num_msg = ZMQClient.process_in zock clt in
    !logger.debug "Received %d messages" num_msg)
