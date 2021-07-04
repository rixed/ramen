(* For each ramen command, check arguments and mostly transfer the control
 * further to more specialized modules. *)
open Batteries
open Stdint
open RamenLog
open RamenHelpers
open RamenHelpersNoLog
open RamenSyncHelpers
module C = RamenConf
module Default = RamenConstsDefault
module DT = DessserTypes
module EventTime = RamenEventTime
module Files = RamenFiles
module N = RamenName
module O = RamenOperation
module OutRef = RamenOutRef
module Processes = RamenProcesses
module ServiceNames = RamenConstsServiceNames
module Services = RamenServices
module T = RamenTypes
module Versions = RamenVersions
module VSI = RamenSync.Value.SourceInfo
module ZMQClient = RamenSyncZMQClient
module CliInfo = RamenConstsCliInfo

let () =
  Printexc.register_printer (function
    | Failure msg -> Some msg
    | _ -> None)

let make_copts
      debug quiet persist_dir rand_seed keep_temp_files reuse_prev_files
      forced_variants local_experiments_ initial_export_duration site
      bundle_dir masters sync_url srv_pub_key username clt_pub_key
      clt_priv_key identity with_colors_ =
  with_colors := with_colors_ ;
  RamenExperiments.local_experiments :=
    if N.is_empty local_experiments_ then
      RamenPaths.local_experiments persist_dir
    else
      local_experiments_ ;
  if not (N.is_empty identity) then
    Files.check_file_is_secure identity ;
  (match rand_seed with
  | None -> Random.self_init ()
  | Some seed ->
      RamenSupervisor.rand_seed := Some seed ;
      Random.init seed) ;
  (* As the RAMEN_VARIANTS and RAMEN_MASTERS envars can only take a list as
   * a single string, let's consider each value can be a list: *)
  let list_of_string_opt opt =
    List.fold_left (fun lst s ->
      List.rev_append (string_split_on_char ',' s) lst
    ) [] opt
  in
  let site =
    Option.default_delayed (fun () ->
      let go_with_default () =
        !logger.info "Cannot find out the hostname, assuming %a"
          N.site_print Default.site_name ;
        Default.site_name in
      match Unix.run_and_read "hostname" with
      | exception e ->
          !logger.debug "Cannot execute hostname: %S"
            (Printexc.to_string e) ;
          go_with_default ()
      | WEXITED 0, hostname ->
          N.site (String.trim hostname)
      | st, _ ->
          !logger.debug "Cannot execute hostname: %s"
            (string_of_process_status st) ;
          go_with_default ()
    ) site in
  let forced_variants = list_of_string_opt forced_variants
  and masters =
    list_of_string_opt masters |> List.map N.site |> Set.of_list in
  let conf =
    C.make_conf
      ~debug ~quiet ~keep_temp_files ~reuse_prev_files ~forced_variants
      ~initial_export_duration ~site ~bundle_dir ~masters ~sync_url
      ~srv_pub_key ~username ~clt_pub_key ~clt_priv_key ~identity persist_dir in
  if srv_pub_key <> "" && (conf.clt_pub_key = "" || conf.clt_priv_key = "") then
    failwith "To connect securely to that server, the client \
              private and secret keys must also be provided" ;
  (* Find out the ZMQ URL to reach the conf server: *)
  if conf.sync_url <> "" then conf
  else
    let sync_url =
      match Services.resolve_every_site conf ServiceNames.confserver with
      | [] ->
          (* TODO: Only when the sync_server experiment have started:
          let host =
            if Set.is_empty conf.masters then
              "localhost"
            else
              (Set.min_elt conf.masters :> string) in
          "tcp://"^ host ^":"^ string_of_int Default.confserver_port *)
          ""
      | (site, se) :: _ ->
          let url =
            Printf.sprintf2 "tcp://%a:%d"
              N.host_print se.Services.host
              se.port in
          !logger.debug "Worker will use %S to contact %a:confserver"
            url N.site_print site ;
          url in
    { conf with sync_url }

(*
 * `ramen supervisor`
 *
 * Start the process supervisor, which will keep running the programs
 * present in the configuration/rc file (and kill the others).
 * This does not return (under normal circumstances).
 *
 * The actual work is done in module RamenProcesses.
 *)

let check_binocle_errors () =
  Option.may raise !Binocle.last_error

let while_ () = !Processes.quit = None

let init_log conf daemonize to_stdout to_syslog prefix_log_with_name
             service_name =
  let prefix =
    if prefix_log_with_name then Some (service_name : N.service :> string)
    else None in
  if to_syslog then
    init_syslog ?prefix conf.C.log_level
  else (
    let logdir =
      if to_stdout then None
      else (
        let log_path =
          N.path_cat [ conf.C.persist_dir ; N.path "log" ;
                       N.path (service_name :> string) ] in
        (* It can be surprising when the command keeps the console but logs
         * elsewhere, and is rarely the desired behavior, so log about it: *)
        if not daemonize then
          Printf.printf "Will log in %a\n%!" N.path_print log_path ;
        Some log_path
      ) in
    Option.may Files.mkdir_all logdir ;
    init_logger ?prefix ?logdir:(logdir :> string option) conf.C.log_level) ;
  !logger.info "Starting ramen-%a %s (on site %a)"
    N.service_print service_name
    Versions.release_tag
    N.site_print conf.C.site

let start_daemon conf daemonize to_stdout to_syslog prefix_log_with_name
                 service_name =
  if to_stdout && daemonize then
    failwith "Options --daemonize and --stdout are incompatible." ;
  if to_stdout && to_syslog then
    failwith "Options --syslog and --stdout are incompatible." ;
  init_log conf daemonize to_stdout to_syslog prefix_log_with_name service_name ;
  check_binocle_errors () ;
  if daemonize then do_daemonize () ;
  Processes.prepare_signal_handlers conf

(* If a scraping port is configured in the environment for that service
 * then start the Binocle thread that serves Prometheus. *)
let start_prometheus_thread service_name =
  let n =
    "RAMEN_" ^ String.uppercase (service_name : N.service :> string) ^
     "_PROMETHEUS_PORT" in
  match Sys.getenv n with
  | exception Not_found ->
      ()
  | v ->
      (* Let it crash if the value is invalid: *)
      let fail () =
        failwith (v ^" is invalid for "^ n) in
      (match int_of_string v with
      | exception _ -> fail ()
      | port when port >= 0 && port < 65536 ->
          let namespace = "ramen_"^ (service_name :> string) in
          BinocleThread.http_expose_prometheus ~port ~namespace () |> ignore
      | _ -> fail ())

let supervisor conf daemonize to_stdout to_syslog prefix_log_with_name
               fail_for_good_ kill_at_exit test_notifs_every
               lmdb_max_readers () =
  start_daemon conf daemonize to_stdout to_syslog prefix_log_with_name
               ServiceNames.supervisor ;
  start_prometheus_thread ServiceNames.supervisor ;
  (* Controls all calls to restart_on_failure: *)
  fail_for_good := fail_for_good_ ;
  RamenSupervisor.test_notifs_every := test_notifs_every ;
  RamenSupervisor.lmdb_max_readers := lmdb_max_readers ;
  (* Also attempt to repair the report/notifs ringbufs.
   * This is OK because there can be no writer right now, and the report
   * ringbuf being a non-wrapping buffer then reader part cannot be damaged
   * anyway. For notifications we could have the alerter reading though,
   * so FIXME: smarter ringbuf_repair that spins before repairing. *)
  let reports_rb = Processes.prepare_reports conf in
  RingBuf.unload reports_rb ;
  (* The main job of this process is to make what's actually running
   * in accordance to the running program list: *)
  restart_on_failure ~while_ "synchronize_running"
    RamenExperiments.(specialize the_big_one) [|
      Processes.dummy_nop ;
      (fun () -> RamenSupervisor.synchronize_running conf kill_at_exit ~while_) |] ;
  Option.may exit !Processes.quit

(*
 * `ramen alerter`
 *
 * Start the alerter process, which will read the notifications ringbuf
 * and perform whatever action it takes, most likely reaching out to
 * external systems.
 *
 * The actual work is done in module RamenAlerter.
 *)

let alerter conf max_fpr daemonize to_stdout
            to_syslog prefix_log_with_name kafka_producers_timeout
            debounce_delay max_last_incidents_kept max_incident_age for_test
            reschedule_clock () =
  RamenCliCheck.alerter max_fpr ;
  start_daemon conf daemonize to_stdout to_syslog prefix_log_with_name
               ServiceNames.alerter ;
  start_prometheus_thread ServiceNames.alerter ;
  restart_on_failure ~while_ "process_notifications"
    RamenExperiments.(specialize the_big_one) [|
      Processes.dummy_nop ;
      (fun () ->
        RamenAlerter.start conf max_fpr kafka_producers_timeout
                           debounce_delay max_last_incidents_kept
                           max_incident_age for_test reschedule_clock) |] ;
  Option.may exit !Processes.quit

let notify conf parameters test name () =
  init_logger conf.C.log_level ;
  let sent_time = Unix.gettimeofday () in
  let parameters = Array.of_list parameters in
  let firing, certainty, debounce, timeout, parameters =
    RingBufLib.normalize_notif_parameters parameters in
  let open RamenSync in
  let notif = Value.Alerting.Notification.{
    site = conf.C.site ; worker = N.fq "CLI" ;
    test ; sent_time ; event_time = None ; name ;
    firing ; certainty ; debounce ; timeout ; parameters } in
  let cmd = Client.CltMsg.SetKey (Key.Notifications, Value.Notification notif) in
  start_sync conf ~while_ ~recvtimeo:1. (fun session ->
    let on_ok () = Processes.quit := Some 0 in
    ZMQClient.send_cmd ~while_ ~on_ok session cmd)

(*
 * `ramen tunneld`
 *
 * Runs a service allowing other sites to transfer tuples to workers running
 * on this one.
 *)

let resolve_port conf port_opt def service_name =
  Option.default_delayed (fun () ->
    match Services.resolve conf conf.C.site service_name with
    | exception Not_found ->
        !logger.warning
          "No port given and cannot resolve service %a on site %a, will \
           use default %d"
          N.service_print service_name
          N.site_print conf.C.site
          def ;
        def
    | se ->
        se.Services.port
  ) port_opt

let tunneld conf daemonize to_stdout to_syslog prefix_log_with_name port_opt
            () =
  let service_name = ServiceNames.tunneld in
  let port =
    resolve_port conf port_opt Default.tunneld_port service_name in
  start_daemon conf daemonize to_stdout to_syslog prefix_log_with_name
               service_name ;
  start_prometheus_thread service_name ;
  RamenCopySrv.copy_server conf port ;
  Option.may exit !Processes.quit

(*
 * `ramen confserver`
 *
 * Runs a service that reads all configuration and makes it available to
 * other processes via a real-time synchronisation protocol.
 *)

let confserver conf daemonize to_stdout to_syslog prefix_log_with_name ports
               ports_sec srv_pub_key_file srv_priv_key_file no_source_examples
               archive_total_size archive_recall_cost oldest_restored_site
               incidents_history_length () =
  RamenCliCheck.confserver ports ports_sec srv_pub_key_file srv_priv_key_file
                           incidents_history_length ;
  start_daemon conf daemonize to_stdout to_syslog prefix_log_with_name
               ServiceNames.confserver ;
  start_prometheus_thread ServiceNames.confserver ;
  RamenSyncZMQServer.start conf ~while_ ports ports_sec srv_pub_key_file
                           srv_priv_key_file no_source_examples
                           archive_total_size archive_recall_cost
                           oldest_restored_site incidents_history_length ;
  Option.may exit !Processes.quit

let confclient conf key value del if_exists follow () =
  RamenCliCheck.confclient key value del if_exists follow ;
  init_logger conf.C.log_level ;
  if del then
    let key = RamenSync.Key.of_string key in
    RamenConfClient.del conf ~while_ key
    (* FIXME: on_ok, on_ko... *)
  else if value = "" then
    RamenConfClient.dump conf ~while_ key follow
  else
    let key = RamenSync.Key.of_string key in
    RamenConfClient.set conf ~while_ key value
    (* FIXME: on_ok, on_ko... *)

(*
 * `ramen compile`
 *
 * Turn a ramen program into an executable binary.
 * Actual work happens in RamenCompiler.
 *)

(* Note: We need a program name to identify relative parents. *)
let compile_local conf lib_path source_file output_file_opt src_path_opt =
  (* There is a long way to calling the compiler so we configure it from
   * here: *)
  let get_parent =
    List.map Files.absolute_path_of lib_path |>
    RamenCompiler.program_from_lib_path in
  let src_path_opt =
    if src_path_opt <> None then
      src_path_opt
    else
      (* Try to get an idea from the lib-path: *)
      match lib_path with
      | p::_ ->
        (try
          let p =
            Files.remove_ext source_file |>
            Files.rel_path_from (Files.absolute_path_of p) in
          Some (N.src_path (p :> string))
        with Failure s ->
          !logger.debug "%s" s ;
          None)
      | [] -> None in
  let src_path =
    Option.default_delayed (fun () ->
      N.src_path (Files.(remove_ext (basename source_file)) :> string)
    ) src_path_opt in
  let dest_file =
    Option.default_delayed (fun () ->
      Files.change_ext "x" source_file
    ) output_file_opt in
  let dest_ext = Files.ext dest_file in
  !logger.debug "Compiling source %a into %a"
    N.path_print source_file
    N.path_print dest_file ;
  let apply_rule =
    RamenMake.apply_rule conf ~force_rebuild:true get_parent src_path in
  let from_ext = Files.ext source_file in
  let build_rules = RamenMake.find_path from_ext dest_ext in
  !logger.debug "Will build this chain: %s->%a"
    from_ext
    (List.print ~first:"" ~last:"" ~sep:"->"
                (fun oc (ext, _, _) -> String.print oc ext))
      build_rules ;
  List.fold_left (fun prev_ext (to_ext, _, _ as rule) ->
    let from_file = Files.change_ext prev_ext source_file
    and to_file =
      if to_ext = dest_ext then dest_file
      else Files.change_ext to_ext source_file in
    apply_rule from_file to_file rule ;
    to_ext
  ) from_ext build_rules |>
  ignore

(* We store sources separately from programs, as the same source can run under
 * several program names (given different parameters, for instance).
 * For simplicity there is no separate command to upload a file: the
 * confserver will taste and rate any uploaded source file.
 *
 * The ConfServer must not compile the source down to an executable file.
 * Only the supervisor daemons must.
 * One can check that a source is valid by compiling it locally.
 * But the ConfServer must parse it and type it in order to extract the META
 * informations. Only the code generation is not required.
 *
 * For parsing and typing, the confserver resolves parent names against the
 * source tree only. Remember that supervisor will link against the running
 * config. Both hierarchies must largely match each other anyway.
 *
 * Therefore, a source has several values attached to it:
 * - the text, that anyone can read/write
 * - and the info, that only the confserver can write.
 *
 * When compiling the confserver locks the source (therefore the creator
 * must unlock it). Then it writes the info and unlock all.
 * The info contains the same thing as the Program.t today (no program name
 * and no param values and no env values), and the typed operation.
 *
 * When generating code, the supervisors do not restart from the source but from
 * the (typed!) operation, thus speeding up the process and alleviating the need
 * for a solver on the sites.
 *
 * Given there are two different keys for the source text and the source info,
 * how to we know the info relate to the version of the source that has just
 * been written? We know because of the MD5 of the source that's present in
 * the info (the MD5 of the full text so that the client can easily check it).
 *
 * Now of course the confserver does not pre-compile itself. Indeed, a daemon
 * compiler can do that, for simpler design and parallelising for free.
 * See `ramen precompserver`.
 *)

let compile_sync conf replace src_file src_path_opt =
  let open RamenSync in
  let source = Files.read_whole_file src_file in
  let src_path =
    Option.default_delayed (fun () ->
      N.src_path (Files.remove_ext (N.simplified_path src_file) :> string)
    ) src_path_opt in
  let ext = Files.ext src_file in
  if ext = "" then
    failwith "Need an extension to build a source file." ;
  let value, md5 =
    match ext with
    | "alert" ->
        let alert = PPP.of_string_exc Value.Alert.t_ppp_ocaml source in
        (* The same string than RamenMake will use, for md5: *)
        let alert_str = PPP.to_string Value.Alert.t_ppp_ocaml alert in
        Value.Alert alert,
        N.md5 alert_str
    | "ramen" ->
        Value.of_string source,
        N.md5 source
    | _ ->
        Printf.sprintf "Unknown extension %S" ext |>
        failwith
  in
  let source_mtime = ref 0. in
  let k_source = Key.(Sources (src_path, ext)) in
  let on_ko () = Processes.quit := Some 1 in
  let synced = ref false in
  let try_quit_on_val v mtime =
    match v with
    | Value.(SourceInfo { md5s ; detail = Compiled _ ; _ })
      when list_starts_with md5s md5 ->
        if !synced then (
          !logger.info "Program %a is compiled" N.src_path_print src_path ;
          Processes.quit := Some 0
        ) (* else wait that we wrote again the (same) source *)
    | Value.(SourceInfo ({ md5s ; detail = Failed _ ; _ } as s))
      when list_starts_with md5s md5 ->
        if !source_mtime > 0. && mtime >= !source_mtime then (
          (* Recent failure *)
          Processes.quit := Some 1 ;
          !logger.error "Cannot compile %a: %s"
            N.src_path_print src_path
            (Value.SourceInfo.compilation_error s)
        ) (* else wait for a recent success or failure *)
    | _ ->
      () in
  let try_quit session =
    (* Look at the info to see if it's worth waiting for: *)
    let info_key = Key.(Sources (src_path, "info")) in
    match Client.find session.ZMQClient.clt info_key with
    | exception Not_found -> ()
    | v -> try_quit_on_val v.value v.mtime in
  let on_synced _ = synced := true in
  let on_set _conf k v _u mtime =
    match k, v with
    | Key.(Sources (p, "info")), Value.(SourceInfo _)
      when p = src_path ->
        try_quit_on_val v mtime
    | _ -> () in
  let on_new conf k v uid mtime _can_write _can_del _owner _expiry =
    on_set conf k v uid mtime in
  let topics = [ "sources/"^ (src_path :> string) ^"/info" ] in
  start_sync conf ~while_ ~on_new ~on_set ~on_synced ~topics ~recvtimeo:1.
             (fun session ->
    let latest_mtime () =
      match ZMQClient.my_errors session.clt with
      | None ->
          !logger.error "Error file still unknown!?" ;
          0.
      | Some err_k ->
          (match (Client.find session.clt err_k).value with
          | exception Not_found ->
              !logger.error "Error file was timed out?!" ;
              0.
          | Error (mtime, _, _) ->
              mtime
          | v ->
              err_sync_type err_k v "an error file" ;
              0.) in
    if replace then
      (* Wait for lockers to unlock, then write: *)
      ZMQClient.send_cmd ~while_ session
        (LockOrCreateKey (k_source, Default.sync_lock_timeout, false))
        (* Not on_done because we aren't subscribed to ramen sources: *)
        ~on_ko ~on_ok:(fun () ->
          ZMQClient.send_cmd ~while_ session (SetKey (k_source, value))
            ~on_ko ~on_ok:(fun () ->
              source_mtime := latest_mtime () ;
              ZMQClient.send_cmd ~while_ session (UnlockKey k_source)
                ~on_ok:(fun () -> try_quit session)))
    else
      (* Fail if that source is already present: *)
      ZMQClient.send_cmd ~while_ session (NewKey (k_source, value, 0., false))
        ~on_ko ~on_ok:(fun () ->
          source_mtime := latest_mtime () ;
          try_quit session);
    ZMQClient.process_until ~while_ session)

(* Do not generate any executable file, but parse/typecheck new or updated
 * source programs, using the build infrastructure to accept any source format
 * (for now distinguishing them using extension as usual).
 * The only change required from the builder is:
 * - source/target files are read from and written to the config tree instead
 *   of the file system;
 * - instead of using mtime, it must use the md5s;
 * - it must stop before reaching the .x but instead aim for a "/info".
 *)
let precompserver conf daemonize to_stdout to_syslog prefix_log_with_name
                  smt_solver () =
  RamenCliCheck.precompserver conf ;
  RamenSmt.solver := smt_solver ;
  start_daemon conf daemonize to_stdout to_syslog prefix_log_with_name
               ServiceNames.precompserver ;
  start_prometheus_thread ServiceNames.precompserver ;
  RamenPrecompserver.start conf ~while_

let execompserver conf daemonize to_stdout to_syslog prefix_log_with_name
                  external_compiler max_simult_compilations
                  dessser_codegen opt_level quarantine () =
  RamenCliCheck.execompserver conf max_simult_compilations quarantine opt_level ;
  RamenCompiler.init external_compiler max_simult_compilations
                     dessser_codegen opt_level ;
  start_daemon conf daemonize to_stdout to_syslog prefix_log_with_name
               ServiceNames.execompserver ;
  start_prometheus_thread ServiceNames.execompserver ;
  RamenExecompserver.start conf ~quarantine ~while_

let compile conf lib_path external_compiler max_simult_compils smt_solver
            dessser_codegen opt_level source_files output_file_opt src_path_opt
            replace () =
  RamenCliCheck.compile source_files src_path_opt opt_level ;
  init_logger conf.C.log_level ;
  RamenSmt.solver := smt_solver ;
  RamenCompiler.init external_compiler max_simult_compils dessser_codegen
                     opt_level ;
  List.iter (fun source_file ->
    if conf.C.sync_url = "" then
      compile_local conf lib_path source_file output_file_opt src_path_opt
    else
      let src_path_opt =
        Option.map (fun s ->
          N.src_path ((Files.remove_ext (N.path (s : N.src_path :> string))) :> string)
        ) src_path_opt in
      compile_sync conf replace source_file src_path_opt
  ) source_files

(*
 * `ramen run`
 *
 * Ask the ramen daemon to start a compiled program.
 *)

let run conf params report_period program_name on_site cwd replace () =
  init_logger conf.C.log_level ;
  let params = List.enum params |> Hashtbl.of_enum in
  (* If we run in --debug mode, also set that worker in debug mode: *)
  let debug = !logger.log_level = Debug in
  RamenRun.run conf ~params ~debug ~report_period ~on_site ?cwd ~replace
               program_name

(*
 * `ramen kill`
 *
 * Remove that program from the list of running programs.
 * This time the program is identified by its name not its executable file.
 *)

let kill conf program_names purge () =
  init_logger conf.C.log_level ;
  let topics = RamenRun.kill_topics in
  let num_kills =
    start_sync conf ~while_ ~topics ~recvtimeo:1. (fun session ->
      RamenRun.kill ~while_ ~purge session program_names) in
  Printf.printf "Killed %d program%s\n"
    num_kills (if num_kills > 1 then "s" else "")

(*
 * `ramen choreographer`
 *
 * Turn the MustRun config into a FuncGraph and expose it in the configuration
 * for the supervisor.
 *)
let choreographer conf daemonize to_stdout to_syslog prefix_log_with_name () =
  RamenCliCheck.choreographer conf ;
  start_daemon conf daemonize to_stdout to_syslog prefix_log_with_name
               ServiceNames.choreographer ;
  start_prometheus_thread ServiceNames.choreographer ;
  RamenChoreographer.start conf ~while_

(*
 * `ramen replayer`
 *
 * Turns simple replay queries into proper query plans for supervisor to
 * execute.
 * GUI won't be able to perform replays if this is not running.
 *)
let replay_service conf daemonize to_stdout to_syslog prefix_log_with_name () =
  RamenCliCheck.replayer conf ;
  start_daemon conf daemonize to_stdout to_syslog prefix_log_with_name
               ServiceNames.replayer ;
  start_prometheus_thread ServiceNames.replayer ;
  RamenReplayService.start conf ~while_

(*
 * `ramen info`
 *
 * Display a program or function meta information.
 *)

let prog_info prog opt_func_name with_types =
  TermTable.print_head 0 "Parameters" ;
  TermTable.print 1 "%a" RamenTuple.print_params prog.VSI.default_params ;
  TermTable.print_head 0 "Running condition" ;
  TermTable.print 1 "%a" (RamenExpr.print true) prog.condition ;
  let info_func i func =
    TermTable.print i "%s" (N.func_color func.VSI.name) ;
    if func.doc <> "" then TermTable.print_abstract i func.doc ;
    TermTable.print_head (i+1) "Parents" ;
    let parents = O.parents_of_operation func.VSI.operation in
    if parents = [] then
      TermTable.print (i+2) "None"
    else
      List.iter (function
        | site, None, f ->
            TermTable.print (i+2) "%a%s"
              O.print_site_identifier site
              (N.func_color f)
        | site, Some rp, f ->
            TermTable.print (i+2) "%a%s/%s"
              O.print_site_identifier site
              (N.rel_program_color rp)
              (N.func_color f)
      ) parents ;
    TermTable.print_head (i+1) "Input type" ;
    let in_type =
      RamenFieldMaskLib.in_type_of_operation func.VSI.operation in
    TermTable.print (i+2) "%a" RamenFieldMaskLib.print_in_type in_type ;
    let out_type = O.out_record_of_operation ~with_priv:false func.operation in
    TermTable.print_head (i+1) "Output type" ;
    TermTable.print (i+2) "Ramen: %a" DT.print_mn out_type ;
    TermTable.print (i+2) "ORC: %a"
      RamenOrc.print (RamenOrc.of_type out_type) ;
    O.event_time_of_operation func.operation |>
    Option.may (fun et ->
      TermTable.print_head (i+1) "Event time" ;
      TermTable.print (i+2) "%a" EventTime.print et) ;
    TermTable.print_head (i+1) "Factors" ;
    TermTable.print (i+2) "%a"
      (List.print N.field_print) (O.factors_of_operation func.operation) ;
    TermTable.print_head (i+1) "Operation" ;
    TermTable.print (i+2) "%a" (O.print with_types) func.operation ;
    if func.VSI.retention <> None || func.VSI.is_lazy then (
      let lst = [] in
      let lst =
        match func.VSI.retention with
        | Some r ->
            IO.to_string RamenProgram.print_retention r :: lst
        | None ->
          lst in
      let lst =
        if func.VSI.is_lazy then "lazy"::lst else lst in
      TermTable.print_head (i+1) "Misc" ;
      TermTable.print (i+2) "%a"
        (List.print ~first:"" ~last:"" ~sep:", " String.print) lst
    ) ;
    print_endline ""
  in
  match opt_func_name with
  | None ->
      TermTable.print_head 0 "Functions" ;
      List.iter (info_func 1) prog.funcs
  | Some func_name ->
      (match List.find (fun func ->
               func.VSI.name = func_name) prog.funcs with
      | exception Not_found ->
          Printf.sprintf2 "No such function %a"
            N.func_print func_name |>
          failwith
      | func -> info_func 0 func)

let info_local params bin_file opt_func_name with_types =
  !logger.debug "Displaying program in file %a" N.path_print bin_file ;
  let params = List.enum params |> Hashtbl.of_enum in
  let prog = Processes.of_bin params bin_file in
  prog_info prog opt_func_name with_types

let info_sync conf src_path opt_func_name with_types =
  !logger.debug "Displaying configured source path %a"
    N.src_path_print src_path ;
  let topics =
    [ "sources/"^ (src_path :> string) ^"/info" ] in
  let recvtimeo = 0. in (* No need to keep alive after initial sync *)
  start_sync conf ~topics ~while_ ~recvtimeo (fun session ->
    match RamenSync.program_of_src_path session.clt src_path with
    | exception Failure msg ->
        let hint_func_name =
          match opt_func_name, String.index (src_path :> string) '/' with
          | exception Not_found ->
              (* No '/' in the name => no function intended *)
              false
          | Some _, _ ->
              (* Already specified a function name *)
              false
          | None, _ ->
              (* Maybe user meant a single function? *)
              true in
        !logger.error "%s" msg ;
        if hint_func_name then
          let pname, fname = N.fq_parse (N.fq (src_path :> string)) in
          !logger.info "Did you mean function %a of program %a?"
            N.func_print fname
            N.program_print pname
    | prog ->
        prog_info prog opt_func_name with_types)

let info conf params path opt_func_name with_types () =
  init_logger conf.C.log_level ;
  let bin_file = N.path path in
  if conf.C.sync_url = "" || Files.exists ~has_perms:0o500 bin_file then
    info_local params bin_file opt_func_name with_types
  else
    (* path is then the source path! *)
    let src_path = N.src_path path in
    info_sync conf src_path opt_func_name with_types

(*
 * `ramen gc`
 *
 * Delete old or unused files.
 *)

let gc conf dry_run del_ratio compress_older loop daemonize
       to_stdout to_syslog prefix_log_with_name () =
  RamenCliCheck.gc daemonize loop ;
  let loop = loop |? Default.gc_loop in
  start_daemon conf daemonize to_stdout to_syslog prefix_log_with_name
               ServiceNames.gc ;
  start_prometheus_thread ServiceNames.gc ;
  RamenGc.cleanup ~while_ conf dry_run del_ratio compress_older loop ;
  Option.may exit !Processes.quit

(*
 * `ramen ps`
 *
 * Display information about running programs and quit.
 *)

let sort_col_of_string spec str =
  let str = simplify_col_name str in
  try int_of_string str
  with Failure _ ->
    let matching =
      Array.enum spec |> Enum.foldi (fun i c l ->
        let c' = simplify_col_name c in
        if String.starts_with c' str then
          (i, c) :: l
        else
          l
      ) [] in
    (match matching with
    | [ i, _ ] -> i
    | [] ->
        Printf.sprintf2 "Unknown column %S (must be one of %a)"
          str
          (pretty_array_print String.print_quoted) spec |>
        failwith
    | l ->
        Printf.sprintf2 "Ambiguous: %S could be %a"
          str
          (pretty_list_print (fun oc (_, s) ->
            String.print_quoted oc s)) l |>
        failwith)

(* TODO: add an option to select the site *)
(* TODO: port profile info to the confserver *)
let ps_ profile conf pretty with_header sort_col top sites pattern all () =
  if profile && conf.C.sync_url <> "" then
    failwith "The profile command is incompatible with --confserver." ;
  init_logger conf.C.log_level ;
  let head =
    [| "site" ; "operation" ; "top-half" ; "#in" ; "#selected" ; "#filtered";
       "#out" ; "#errs" ; "#groups" ; "max #groups" ; "last out" ;
       "min event time" ; "max event time" ; "CPU" ; "wait in" ; "wait out" ;
       "heap" ; "max heap" ; "volume in" ; "volume out" ; "avg out sz" ;
       "startup time" ; "#parents" ; "#children" ; "archive size" ;
       "oldest archived" ; "archive duration" ; "worker signature" ;
       "precomp signature" |] in
  let open TermTable in
  let sort_col = sort_col_of_string head sort_col in
  let print_tbl = print_table ~pretty ~sort_col ~with_header ?top head in
  let topics =
    [ "sites/*/workers/*/stats/runtime" ;
      "sites/*/workers/*/worker" ;
      "sites/*/workers/*/archives/*" ] in
  let topics =
    if all then
      "target_config" :: "sources/*/info" :: topics
    else
      topics in
  (* Also save a list of expected fqs according to target config so we can add
   * blank lines about workers that are not yet running or that haven't
   * sent any stats yet: *)
  let expected_fqs = ref Set.empty
  and found_fqs = ref Set.empty
  and all_sites = Services.all_sites conf in
  let matches_pattern site fq =
    Globs.matches sites (site : N.site :> string) &&
    Globs.matches pattern (fq : N.fq :> string) in
  let recvtimeo = 0. in (* No need to keep alive after initial sync *)
  start_sync conf ~topics ~while_ ~recvtimeo (fun session ->
    let open RamenSync in
    Client.iter session.clt (fun k v ->
      match k, v.value with
      | Key.TargetConfig,
        Value.TargetConfig rc ->
          (* Build the list of expected sites and fqs: *)
          List.iter (fun (prog_name, rce) ->
            let src_path = N.src_path_of_program prog_name in
            let info_key = Key.Sources (src_path, "info") in
            match (Client.find session.clt info_key).value with
            | exception Not_found ->
                !logger.warning "Cannot find info %a for RC entry %a"
                  Key.print info_key
                  Value.TargetConfig.print_entry rce
            | Value.SourceInfo { detail = Compiled prog ; _ } ->
                List.iter (fun func ->
                  let fq =
                    N.fq_of_program prog_name func.Value.SourceInfo.name in
                  let site_pat = Globs.compile rce.Value.TargetConfig.on_site in
                  Services.SetOfSites.iter (fun site ->
                    if Globs.matches site_pat (site : N.site :> string) &&
                       matches_pattern site fq then
                      expected_fqs := Set.add (site, fq) !expected_fqs
                  ) all_sites
                ) prog.funcs
            | Value.SourceInfo { detail = Failed failure ; _ } ->
                !logger.warning "Program %a could not be compiled: %s"
                  N.program_print prog_name
                  failure.VSI.err_msg
            | v ->
                err_sync_type info_key v "a SourceInfo"
          ) rc
      | Key.PerSite (site, PerWorker (fq, Worker)),
        Value.Worker worker
        when matches_pattern site fq ->
          found_fqs := Set.add (site, fq) !found_fqs ;
          let get_k k what f =
            let k = Key.PerSite (site, PerWorker (fq, k)) in
            match (Client.find session.clt k).value with
            | exception Not_found -> None
            | v ->
                (match f v with
                | None -> invalid_sync_type k v what
                | x -> x) in
          let s = get_k RuntimeStats "RuntimeStats" (function
            | Value.RuntimeStats x -> Some x
            | _ -> None) in
          let arc_size = get_k NumArcBytes "NumArcBytes" (function
            | Value.RamenValue T.(VI64 x) -> Some x
            | _ -> None) |? 0L in
          let arc_times = get_k ArchivedTimes "ArchivedTimes" (function
            | Value.TimeRange x -> Some x
            | _ -> None) |? [||] in
          let arc_oldest =
            match arc_times with [||] -> None
                                  | i -> Some i.(0).TimeRange.since in
          let arc_duration = TimeRange.span arc_times in
          let open Value.RuntimeStats in
          [| Some (ValStr (site :> string)) ;
             Some (ValStr (fq :> string)) ;
             Some (ValBool (worker.Value.Worker.role <> Whole)) ;
             Option.map (fun s -> ValInt (Uint64.to_int s.tot_in_tuples)) s ;
             Option.map (fun s -> ValInt (Uint64.to_int s.tot_sel_tuples)) s ;
             Option.map (fun s -> ValInt (Uint64.to_int s.tot_out_filtered)) s ;
             Option.map (fun s -> ValInt (Uint64.to_int s.tot_out_tuples)) s ;
             Option.map (fun s -> ValInt (Uint64.to_int s.tot_out_errs)) s ;
             Option.map (fun s -> ValInt (Uint64.to_int s.cur_groups)) s ;
             Option.map (fun s -> ValInt (Uint64.to_int s.max_groups)) s ;
             Option.bind s (fun s -> date_or_na s.last_output) ;
             Option.bind s (fun s -> date_or_na s.min_etime) ;
             Option.bind s (fun s -> date_or_na s.max_etime) ;
             Option.map (fun s -> ValFlt s.tot_cpu) s ;
             Option.map (fun s -> ValFlt s.tot_wait_in) s ;
             Option.map (fun s -> ValFlt s.tot_wait_out) s ;
             Option.map (fun s -> ValInt (Uint64.to_int s.cur_ram)) s ;
             Option.map (fun s -> ValInt (Uint64.to_int s.max_ram)) s ;
             Option.map (fun s -> ValFlt (Uint64.to_float s.tot_in_bytes)) s ;
             Option.map (fun s -> ValFlt (Uint64.to_float s.tot_out_bytes)) s ;
             Option.bind s (fun s ->
               if Uint64.(compare s.tot_full_bytes_samples zero) > 0 then
                 Some (ValFlt (Uint64.to_float s.tot_full_bytes /.
                               Uint64.to_float s.tot_full_bytes_samples))
               else
                 None) ;
             Option.map (fun s -> ValDate s.last_startup) s ;
             Some (ValInt (Array.length (worker.parents |? [||]))) ;
             Some (ValInt (Array.length worker.children)) ;
             Some (ValInt (Int64.to_int arc_size)) ;
             Option.map (fun s -> ValDate s) arc_oldest ;
             Some (ValDuration arc_duration) ;
             Some (ValStr worker.worker_signature) ;
             Some (ValStr worker.info_signature) |] |>
          print_tbl
      | _ -> ())) ;
  if all then
    (* Add empty lines for workers with no stats: *)
    Set.diff !expected_fqs !found_fqs |>
    Set.iter (fun (site, fq) ->
      print_tbl
        [| Some (ValStr (site : N.site :> string)) ;
           Some (ValStr (fq : N.fq :> string)) |]) ;
  print_tbl [||]

let ps = ps_ false
let profile = ps_ true

(*
 * `ramen tail`
 *
 * Display the last tuple(s) output by an operation.
 *
 * This first create a non-wrapping buffer file and then asks the operation
 * to write in there for 1 hour (by default).
 * This buffer name is standard so that other clients wishing to read those
 * tuples can reuse the same and benefit from a shared history.
 *)

let table_formatter pretty raw null units =
  (* TODO: reuse RamenTypes in TermTable *)
  let open RamenTypes in
  let open TermTable in
  function
  | VFloat t ->
      if units = Some RamenUnits.seconds_since_epoch && pretty then
        Some (ValDate t)
      else if units = Some RamenUnits.seconds && pretty then
        Some (ValDuration t)
      else
        Some (ValFlt t)
  | VNull -> None
  | t ->
    Some (ValStr (IO.to_string
      (print_custom ~null ~quoting:(not raw) ~hex_floats:raw) t))

(* [func_name_or_code] is a list of strings as present in the command
 * line. It could be a function name followed by a list of field names, or
 * it could be the code of a function which output we want to display.
 * We find out by picking the first that works. In case both approach
 * fails we have to display the two error messages though. *)
let parse_func_name_of_code _conf _what func_name_or_code =
  let parse_as_names () =
    match func_name_or_code with
    | worker_name :: field_names ->
        let worker = N.worker worker_name
        and field_names = List.map N.field field_names in
        let ret = worker, field_names in
        (* TODO: Check this function exists *)
        (* FIXME: Assume the function exists. In the future, have
         * the connection to confserver already setup. *)
        ret
    | _ -> assert false (* As the command line parser prevent this *)
  and parse_as_code () =
    failwith "TODO: confserver support for immediate code"
    (*
    (* Create a temporary program name: *)
    let make_transient_program () =
      let now = Unix.gettimeofday ()
      and pid = Unix.getpid ()
      and rnd = Random.int max_int_for_random in
      Legacy.Printf.sprintf "tmp/_%h_%d.%d" now rnd pid |>
      N.program
    in
    let program_name = make_transient_program () in
    let func_name = N.func "f" in
    let programs = RC.with_rlock conf identity in (* best effort *)
    let get_parent = RamenCompiler.program_from_programs programs in
    let oc, src_file =
      BatFile.open_temporary_out ~prefix:what ~suffix:".ramen" () in
    let src_file = N.path src_file in
    !logger.info "Going to use source file %a"
      N.path_print_quoted src_file ;
    let bin_file = Files.change_ext "x" src_file in
    on_error
      (fun () ->
        if not conf.C.keep_temp_files then (
          Files.safe_unlink src_file ;
          Files.safe_unlink bin_file))
      (fun () ->
        Printf.fprintf oc
          "-- Temporary file created on %a for %s\n\
           DEFINE '%s' AS\n%a\n"
          print_as_date (Unix.time ())
          what
          (func_name :> string)
          (List.print ~first:"" ~last:"" ~sep:" " String.print)
            func_name_or_code ;
        close_out oc ;
        Files.safe_unlink bin_file ; (* hahaha! *)
        RamenMake.build conf get_parent program_name src_file bin_file ;
        (* Run it, making sure it archives its history straight from the
         * start: *)
        let debug = !logger.log_level = Debug in
        RamenRun.run conf ~report_period:0. ~src_file ~debug
                     bin_file (Some program_name)) ;
    let fq = N.fq_of_program program_name func_name in
    fq, [], [ program_name ]*)
  in
  try
    parse_as_names ()
  with e1 ->
    try parse_as_code ()
    with e2 ->
      let cmd_line = String.join " " func_name_or_code in
      Printf.sprintf
        "Cannot parse %S as a function name (%s) nor as a program (%s)"
        cmd_line
        (Printexc.to_string e1)
        (Printexc.to_string e2) |>
      failwith

let head_of_types ~with_units head_typ =
  Array.map (fun t ->
    (t.RamenTuple.name :> string) ^
       (if with_units then
         Option.map_default RamenUnits.to_string "" t.units
       else "")
  ) head_typ

(* TODO: add with_site *)
(* FIXME: last/next semantic is unclear. For now we just print last+next
 * tuples. Use the max seqnum found just after sync as the current seqnum?
 * FIXME: regarding tail rate limit: To make tail more useful make the rate
 * limit progressive? *)
let tail conf func_name_or_code with_header with_units sep null raw
         last next continuous where since until
         with_event_time pretty flush
         () =
  init_logger conf.C.log_level ;
  let worker, field_names =
    parse_func_name_of_code conf "ramen tail" func_name_or_code in
  if since <> None || until <> None then
    !logger.warning
      "With --confserver, options --since/--until filter but do not select." ;
  if continuous && next <> None then
    failwith "Option --next and --continuous are incompatible." ;
  if with_units && with_header = 0 then
    failwith "Option --with-units makes no sense without --with-header." ;
  (* Do something useful by default: display the 10 last lines *)
  let last =
    if last = None && next = None && since = None && until = None
    then 10 else last |? 0 in
  let flush = flush || continuous in
  let next = if continuous then max_int else next |? 0 in
  (* We need to check that this worker is indeed running, get its output type,
   * and then we want to receive its tail.
   * No need to receive its subscribers though. *)
  let site_opt, prog_name, func_name = N.worker_parse worker in
  let fq = N.fq_of_program prog_name func_name in
  let sites = (site_opt :> string option) |? "*" in
  let topics =
    [ "sites/"^ sites ^"/workers/"^ (fq :> string) ^"/worker" ;
      (* TODO: would be faster to sync the specific sources once we know
       * their name, rather than all defined sources. *)
      "sources/*/info" ;
      "tails/"^ sites ^"/"^ (fq :> string) ^"/*/lasts/*" ] in
  start_sync conf ~topics ~while_ ~recvtimeo:1. (fun session ->
    let open RamenSync in
    (* Get the workers and their types: *)
    !logger.debug "Looking for running workers %a on sites %S"
      N.fq_print fq
      sites ;
    let workers =
      let prefix = "sites/" in
      Client.fold session.clt ~prefix (fun k v lst ->
        match k, v.value with
        | PerSite (site, PerWorker (fq', Worker)),
          Worker worker
          when worker.role = Whole ->
            assert (fq = fq') ;
            (site, worker) :: lst
        | _ -> lst
      ) [] in
    !logger.debug "Found sites: %a"
      (Enum.print N.site_print) (List.enum workers /@ fst) ;
    if workers = [] then
      Printf.sprintf2 "Function %a is not running anywhere"
        N.fq_print fq |>
      failwith ;
    let _prog, _prog_name, func = function_of_fq session.clt fq in
    let typ = O.out_type_of_operation ~with_priv:false func.VSI.operation in
    let ser = O.out_record_of_operation ~with_priv:false func.VSI.operation in
    let event_time = O.event_time_of_operation func.operation in
    (* Prepare to print the tails *)
    let field_names = RamenExport.checked_field_names typ field_names in
    let head_idx, head_typ =
      RamenExport.header_of_type ~with_event_time field_names typ in
    let open TermTable in
    let head_typ = Array.of_list head_typ in
    let head = head_of_types ~with_units head_typ in
    let print = print_table ~sep ~pretty ~with_header ~flush head in
    (* Pick a "printer" for each column according to the field type: *)
    let formatter = table_formatter pretty raw null
    in
    let open RamenSerialization in
    let event_time_of_tuple = match event_time with
      | None ->
          if with_event_time then
            failwith "Function has no event time information"
          else (fun _ -> 0., 0.)
      | Some et ->
          (* As we might be tailling from several instance of the same
           * worker, we have already lost track of the params at this point.
           * Problem solved by removing the event-time notation and type
           * altogether and use only the conventional start/stop fields. *)
          let params = [] in
          event_time_of_tuple ser params et
    in
    let unserialize = RamenSerialization.read_array_of_values ser in
    let filter = RamenSerialization.filter_tuple_by ser where in
    (* Callback for each tuple: *)
    let count_last = ref last and count_next = ref next in
    let on_key counter k v =
      match k, v with
      | Key.Tails (_site, _fq, _instance, LastTuple _seq),
        Value.Tuples tuples ->
          Array.iter (fun Value.{ skipped ; values } ->
            if skipped > 0 then
              !logger.warning "Skipped %d tuples" skipped ;
            let tx = RingBuf.tx_of_bytes values in
            (match unserialize tx 0 with
            | exception RingBuf.Damaged ->
                !logger.error "Cannot unserialize tail tuple: %t"
                  (hex_print values)
            | tuple when filter tuple ->
                let t1, t2 = event_time_of_tuple tuple in
                if Option.map_default (fun since -> t2 > since) true since &&
                   Option.map_default (fun until -> t1 <= until) true until
                then (
                  let cols =
                    Array.mapi (fun i idx ->
                      match idx with
                      | -2 -> Some (ValDate t2)
                      | -1 -> Some (ValDate t1)
                      | idx -> formatter head_typ.(i).units tuple.(idx)
                    ) head_idx in
                  print cols ;
                  decr counter ;
                  if !counter <= 0 then raise Exit
                ) else
                  !logger.debug "evtime %f..%f filtered out" t1 t2
            | _ -> ())
          ) tuples
      | _ -> () in
    try
      (* Iter first over tuples received during sync: *)
      (* FIXME: display only the last of those, ordered by seqnum! *)
      (try
        !logger.debug "Tailing past tuples..." ;
        Client.iter session.clt (fun k hv -> on_key count_last k hv.value)
      with Exit -> ()) ;
      if !count_next > 0 then (
        (* Subscribe to all those tails: *)
        !logger.debug "Subscribing to %d workers tail" (List.length workers) ;
        let subscriber =
          conf.C.username ^"-tail-"^ string_of_int (Unix.getpid ()) in
        List.iter (fun (site, w) ->
          let k = Key.Tails (site, fq, w.Value.Worker.worker_signature,
                             Subscriber subscriber) in
          let cmd = Client.CltMsg.NewKey (k, Value.dummy, 0., false) in
          ZMQClient.send_cmd ~while_ session cmd
        ) workers ;
        finally
          (fun () -> (* Unsubscribe *)
            !logger.debug "Unsubscribing from %d tails..." (List.length workers) ;
            List.iter (fun (site, w) ->
              let k = Key.Tails (site, fq, w.Value.Worker.worker_signature,
                                 Subscriber subscriber) in
              let cmd = Client.CltMsg.DelKey k in
              ZMQClient.send_cmd ~while_ session cmd
            ) workers)
          (fun () ->
            (* Loop *)
            !logger.debug "Waiting for tuples..." ;
            session.clt.Client.on_new <-
              (fun _ k v _ _ _ _ _ _ -> on_key count_next k v) ;
            ZMQClient.process_until ~while_ session
          ) ()
      )
    with Exit ->
      print [||])


(*
 * `ramen replay`
 *
 * Like tail, but replays history.
 * Mainly for testing purposes for now.
 * `tail` ask a worker to archive its output and then display this archive.
 * Non-live channels are never archived though. So replay has to create its
 * own receiving ring-buffer like a new worker would do, create a channel,
 * ask for the operation to output this channel to this ringbuf,
 * choose the replayers according to the stats file, setup all the out_ref
 * from the replayers to its own ringbuf (no the outref currently in place
 * should be ok), and finally launch the replayers.
 *
 * Note: refactor the CSV dumper to accommodate for tail, replay and
 * time series.
 *)

let replay_ conf worker field_names with_header with_units sep null raw
            where since until with_event_time pretty flush via_confserver =
  if with_units && with_header = 0 then
    failwith "Option --with-units makes no sense without --with-header." ;
  let until = until |? Unix.gettimeofday () in
  let formatter = table_formatter pretty raw null in
  let callback head =
    let head = Array.of_list head in
    let head' = head_of_types ~with_units head in
    let print = TermTable.print_table ~na:null ~sep ~pretty ~with_header
                                      ~flush head' in
    (fun _t1 _t2 tuple ->
      let vals =
        Array.mapi (fun i -> formatter head.(i).units) tuple in
      print vals),
    (fun () -> print [||]) in
  let topics = RamenExport.replay_topics in
  start_sync conf ~topics ~while_ ~recvtimeo:1. (fun session ->
    (RamenExport.(if via_confserver then replay_via_confserver else replay)
      conf session ~while_ worker field_names where since until
      ~with_event_time callback))

let replay conf func_name_or_code with_header with_units sep null raw
           where since until with_event_time pretty flush via_confserver () =
  init_logger conf.C.log_level ;
  let worker, field_names =
    parse_func_name_of_code conf "ramen tail" func_name_or_code in
  replay_ conf worker field_names with_header with_units sep null raw
          where since until with_event_time pretty flush via_confserver

(*
 * `ramen timeseries`
 *
 * Similar to tail, but output only two columns: time and a value, and
 * make sure to provide as many data samples as asked for, consolidating
 * the actual samples as needed.
 *
 * This works only on operations with time-event information and uses the
 * same output archive files as the `ramen tail` command does.
 *)

let timeseries_ conf worker data_fields
                since until with_header where factors num_points
                time_step sep null consolidation
                bucket_time pretty =
  let num_points =
    if num_points <= 0 && time_step <= 0. then 100 else num_points in
  let until = until |? Unix.gettimeofday () in
  let since = since |? until -. 600. in
  (* Obtain the data: *)
  let num_points, since, until =
    RamenTimeseries.compute_num_points time_step num_points since until in
  let columns, timeseries =
    let topics = RamenExport.replay_topics in
    start_sync conf ~topics ~while_ ~recvtimeo:1. (fun session ->
      (RamenTimeseries.get conf session num_points since until where factors
        ~consolidation ~bucket_time worker data_fields ~while_))
  in
  (* Display results: *)
  let single_data_field = List.length data_fields = 1 in
  let head =
    Array.fold_left (fun res sc ->
      let v =
        Array.enum sc /@ T.to_string |>
        List.of_enum |>
        String.concat "." in
      if single_data_field then
        (if v = "" then (List.hd data_fields :> string) else v) :: res
      else
        List.fold_left (fun res (df : N.field) ->
          ((df :> string) ^ (if v = "" then "" else "("^ v ^")")) :: res
        ) res data_fields
    ) [] columns |> List.rev in
  let head = "Time" :: head |> Array.of_list in
  let open TermTable in
  let print = print_table ~na:null ~sep ~pretty ~with_header head in
  Enum.iter (fun (t, vs) ->
    Array.append
      [| Some (ValDate t) |]
      (Array.concat
        (List.map
          (Array.map (function
            | None -> None
            | Some v -> Some (ValFlt v))) (Array.to_list vs))) |>
    print
  ) timeseries ;
  print [||]

let timeseries conf func_name_or_code
               since until with_header where factors num_points
               time_step sep null consolidation bucket_time pretty () =
  init_logger conf.C.log_level ;
  let worker, field_names =
    parse_func_name_of_code conf "ramen tail" func_name_or_code in
  timeseries_ conf worker field_names
              since until with_header where factors num_points
              time_step sep null consolidation
              bucket_time pretty

(*
 * `ramen httpd`
 *
 * Starts an HTTP daemon that will serve (and maybe one day also accept)
 * time series, either impersonating Graphite API (https://graphiteapp.org/)
 * or any other API of our own.
 *)

let httpd conf daemonize to_stdout to_syslog prefix_log_with_name
          fault_injection_rate server_url api table_prefix graphite () =
  if fault_injection_rate > 1. then
    failwith "Fault injection rate is a rate is a rate." ;
  if conf.C.sync_url = "" then
    !logger.warning "http is running without --confserver option";
  start_daemon conf daemonize to_stdout to_syslog prefix_log_with_name
               ServiceNames.httpd ;
  start_prometheus_thread ServiceNames.httpd ;
  RamenHttpd.run_httpd conf server_url api table_prefix
                       graphite fault_injection_rate ;
  Option.may exit !Processes.quit

(* TODO: allow several queries as in the API *)
let graphite_expand conf for_render since until query () =
  init_logger conf.C.log_level ;
  let topics = [ "sites/*/workers/*/worker" ] in
  start_sync conf ~while_ ~topics ~recvtimeo:0. (fun session ->
    if not for_render then
      RamenGraphite.metrics_find conf session ?since ?until query |>
      List.of_enum |>
      PPP.to_string ~pretty:true RamenGraphite.metrics_ppp_json |>
      Printf.printf "%s\n"
    else
      RamenGraphite.targets_for_render conf session ?since ?until [ query ] |>
      Enum.iter (fun (_func, fq, fvals, data_field) ->
        Printf.printf "%a %a with %a"
          N.fq_print fq
          N.field_print data_field
          (Set.print (fun oc (factor, opt_val) ->
            Printf.fprintf oc "%a:" N.field_print factor ;
            match opt_val with
            | None -> String.print oc "*"
            | Some v -> T.print oc v)) fvals))

(*
 * `ramen archivist`
 *
 * A daemon that react to RC stats and a user configuration file
 * and decides what worker should save its history in order to be
 * able to retrieve or rebuild the output of all persistent functions.
 *)

let archivist conf loop daemonize stats allocs reconf
              to_stdout to_syslog prefix_log_with_name smt_solver () =
  RamenCliCheck.archivist conf loop daemonize stats allocs reconf ;
  RamenSmt.solver := smt_solver ;
  let loop = loop |? Default.archivist_loop in
  start_daemon conf daemonize to_stdout to_syslog prefix_log_with_name
               ServiceNames.archivist ;
  start_prometheus_thread ServiceNames.archivist ;
  RamenArchivist.run conf ~while_ loop allocs reconf ;
  Option.may exit !Processes.quit

(*
 * `ramen start`
 * Starts ramen with a minimum configuration.
 *)
let start conf daemonize to_stdout to_syslog ports ports_sec
          smt_solver fail_for_good kill_at_exit
          test_notifs_every lmdb_max_readers external_compiler max_simult_compils
          dessser_codegen opt_level srv_pub_key_file srv_priv_key_file
          no_source_examples archive_total_size
          archive_recall_cost oldest_restored_site
          gc_loop archivist_loop allocs reconf_workers
          del_ratio compress_older
          max_fpr kafka_producers_timeout debounce_delay max_last_incidents_kept
          max_incident_age incidents_history_length
          execomp_quarantine () =
  let ports =
    if ports <> [] then ports
    else [ Default.confserver_port_str ] in
  RamenCliCheck.start conf ;
  let sync_url = List.hd ports in
  let conf = { conf with C.sync_url = sync_url } in
  RamenCliCheck.confserver ports ports_sec srv_pub_key_file srv_priv_key_file
                           incidents_history_length ;
  RamenCliCheck.choreographer conf ;
  RamenCliCheck.execompserver conf max_simult_compils execomp_quarantine
                              opt_level ;
  RamenCliCheck.precompserver conf ;
  RamenCliCheck.gc false gc_loop ;
  (* Unless told otherwise, do both allocs and reconf of workers: *)
  let allocs, reconf_workers =
    if not allocs && not reconf_workers then true, true
    else allocs, reconf_workers in
  RamenCliCheck.archivist conf archivist_loop false false allocs reconf_workers ;
  RamenCliCheck.replayer conf ;
  RamenCliCheck.alerter max_fpr ;
  let pids = ref Map.Int.empty in
  let add_pid service_name pid =
    pids := Map.Int.add pid service_name !pids ;
    !logger.info "Start %a process (%d)" N.service_print service_name pid in
  init_log conf daemonize to_stdout to_syslog false ServiceNames.start ;
  if daemonize then do_daemonize () ;
  (* Even if `ramen start` daemonize, its children must not: *)
  let daemonize = false
  and prefix_log_with_name = true
  and insecure = ports
  and secure = ports_sec
  and default_archive_total_size = string_of_int archive_total_size
  and default_archive_recall_cost = nice_string_of_float archive_recall_cost
  and oldest_restored_site = nice_string_of_float oldest_restored_site
  and incidents_history_length = string_of_int incidents_history_length
  and debug = !logger.log_level = Debug
  and quiet = !logger.log_level = Quiet
  and keep_temp_files = conf.C.keep_temp_files
  and reuse_prev_files = conf.C.reuse_prev_files
  and variant = conf.C.forced_variants
  and initial_export_duration =
    nice_string_of_float conf.C.initial_export_duration
  and bundle_dir = (conf.C.bundle_dir : N.path :> string)
  and colors = CliInfo.string_of_color !with_colors
  and public_key = (srv_pub_key_file : N.path :> string)
  and private_key = (srv_priv_key_file : N.path :> string)
  and confserver = conf.C.sync_url
  and max_simultaneous_compilations = string_of_int max_simult_compils
  and quarantine = string_of_float execomp_quarantine
  and test_notifs = nice_string_of_float test_notifs_every
  and lmdb_max_readers = Option.map string_of_int lmdb_max_readers
  and del_ratio = nice_string_of_float del_ratio
  and compress_older = string_of_duration compress_older
  and gc_loop = Option.map nice_string_of_float gc_loop
  and archivist_loop = Option.map nice_string_of_float archivist_loop
  and default_max_fpr = nice_string_of_float max_fpr
  and kafka_producers_timeout = nice_string_of_float kafka_producers_timeout
  and debounce_delay = nice_string_of_float debounce_delay
  and max_last_incidents_kept = string_of_int max_last_incidents_kept
  and max_incident_age = nice_string_of_float max_incident_age
  and dessser_code_generator =
    RamenCompiler.string_of_dessser_codegen dessser_codegen
  and optimization_level = string_of_int opt_level
  in
  RamenSubcommands.run_confserver
    ~daemonize ~to_stdout ~to_syslog ~prefix_log_with_name ~insecure ~secure
    ~public_key ~private_key ~no_source_examples ~default_archive_total_size
    ~default_archive_recall_cost ~oldest_restored_site ~incidents_history_length
    ~debug ~quiet ~keep_temp_files ~reuse_prev_files ~variant
    ~initial_export_duration ~bundle_dir ~colors () |>
    add_pid ServiceNames.confserver ;
  RamenSubcommands.run_choreographer
    ~daemonize ~to_stdout ~to_syslog ~prefix_log_with_name ~debug ~quiet
    ~keep_temp_files ~reuse_prev_files ~variant ~initial_export_duration
    ~bundle_dir ~confserver ~colors () |>
    add_pid ServiceNames.choreographer ;
  RamenSubcommands.run_execompserver
    ~daemonize ~to_stdout ~to_syslog ~prefix_log_with_name ~external_compiler
    ~max_simultaneous_compilations ~dessser_code_generator ~optimization_level
    ~quarantine ~debug ~quiet ~keep_temp_files ~reuse_prev_files ~variant
    ~initial_export_duration ~bundle_dir ~confserver ~colors () |>
    add_pid ServiceNames.execompserver ;
  RamenSubcommands.run_precompserver
    ~daemonize ~to_stdout ~to_syslog ~prefix_log_with_name ~smt_solver
    ~debug ~quiet ~keep_temp_files ~reuse_prev_files ~variant
    ~initial_export_duration ~bundle_dir ~confserver ~colors () |>
    add_pid ServiceNames.precompserver ;
  RamenSubcommands.run_supervisor
    ~daemonize ~to_stdout ~to_syslog ~prefix_log_with_name ~fail_for_good
    ~kill_at_exit ~test_notifs ?lmdb_max_readers ~debug ~quiet ~keep_temp_files
    ~reuse_prev_files ~variant ~initial_export_duration ~bundle_dir ~confserver
    ~colors () |>
    add_pid ServiceNames.supervisor ;
  RamenSubcommands.run_gc
    ~del_ratio ~compress_older ?loop:gc_loop ~daemonize ~to_stdout ~to_syslog
    ~prefix_log_with_name ~debug ~quiet ~keep_temp_files ~reuse_prev_files
    ~variant ~initial_export_duration ~bundle_dir ~confserver ~colors () |>
    add_pid ServiceNames.gc ;
  RamenSubcommands.run_archivist
    ?loop:archivist_loop ~daemonize ~allocs ~reconf_workers ~to_stdout
    ~to_syslog ~prefix_log_with_name ~smt_solver ~debug ~quiet ~keep_temp_files
    ~reuse_prev_files ~variant ~initial_export_duration ~bundle_dir ~confserver
    ~colors () |>
    add_pid ServiceNames.archivist ;
  RamenSubcommands.run_replayer
    ~daemonize ~to_stdout ~to_syslog ~prefix_log_with_name ~debug ~quiet
    ~keep_temp_files ~reuse_prev_files ~variant ~initial_export_duration
    ~bundle_dir ~confserver ~colors () |>
    add_pid ServiceNames.replayer ;
  RamenSubcommands.run_alerter
    ~default_max_fpr ~daemonize ~to_stdout ~to_syslog ~prefix_log_with_name
    ~kafka_producers_timeout ~debounce_delay ~max_last_incidents_kept
    ~max_incident_age ~debug ~quiet ~keep_temp_files ~reuse_prev_files ~variant
    ~initial_export_duration ~bundle_dir ~confserver ~colors () |>
    add_pid ServiceNames.alerter ;
  let stopped = ref 0. in
  let last_signalled = ref 0. in
  let rec loop () =
    let finished () =
      !logger.info "All processes have stopped" ;
      if !Processes.quit = None then
        Processes.quit := Some 0 in
    if Map.Int.is_empty !pids then
      finished ()
    else (
      (* Kill children when done: *)
      if !stopped > 0. then (
        let now = Unix.gettimeofday () in
        if now -. !last_signalled > 1. then (
          let s = if now -. !stopped > 3. then Sys.sigkill else Sys.sigterm in
          !logger.debug "Terminating %d children with %s"
            (Map.Int.cardinal !pids) (name_of_signal s) ;
          Map.Int.iter (fun p _ -> Unix.kill p s) !pids ;
          last_signalled := now
        )
      ) ;
      (* Do not busy-loop: *)
      Unix.sleepf 0.3 ;
      (* Collect children status: *)
      (match restart_on_eintr ~while_ (Unix.waitpid [ WNOHANG ]) ~-1 with
      | exception Unix.Unix_error (ECHILD, _, _) ->
          finished ()
      | 0, _ ->
          loop ()
      | pid, status ->
          (match Map.Int.extract pid !pids with
          | exception Not_found -> ()
          | service_name, pids_ ->
              let delay_str =
                if !stopped <= 0. then "" else (
                  let delay = Unix.gettimeofday () -. !stopped in
                  Printf.sprintf2 " after %a" print_as_duration delay
                ) in
              !logger.info "%a process (%d) has stopped%s: %s"
                N.service_print service_name
                pid
                delay_str
                (RamenHelpers.string_of_process_status status);
              pids := pids_) ;
          loop ())
    ) in
  set_signals Sys.[ sigterm ; sigint ] (Signal_handle (fun s ->
    !logger.debug "Received signal %s, will propagate to %d children"
      (name_of_signal s)
      (Map.Int.cardinal !pids) ;
    if !stopped <= 0. then stopped := Unix.gettimeofday ())) ;
  loop ()

(*
 * Display various internal informations
 *)

let variants conf () =
  init_logger conf.C.log_level ;
  let open RamenExperiments in
  let experimenter_id = get_experimenter_id conf.C.persist_dir in
  Printf.printf "Experimenter Id: %d\n" experimenter_id ;
  Printf.printf "Experiments" ;
  if !with_colors then
    Printf.printf "(legend: %s | %s | unselected):\n"
      (green "forced") (yellow "selected") ;
  all_experiments () |>
  List.iter (fun (name, e) ->
    Printf.printf "  %s:\n" name ;
    for i = 0 to Array.length e.variants - 1 do
      let v = e.variants.(i) in
      Printf.printf "    %s%s (%s%%):\n%s\n"
        ((if e.variant = i then
           if e.forced then green else yellow
         else identity) v.Variant.name)
        (if !with_colors || e.variant <> i then "" else " (SELECTED)")
        (nice_string_of_float (100. *. v.Variant.share))
        (reindent "      " v.Variant.descr)
    done ;
    Printf.printf "\n")

let stats conf metric_name () =
  init_logger conf.C.log_level ;
  Files.initialize_all_saved_metrics conf.C.persist_dir ;
  if metric_name = "" then
    Binocle.display_console ~colors:!with_colors ()
  else (
    let best_dist = ref max_int and best_name = ref "" in
    try
      Binocle.iter (fun name _help export ->
        if name = metric_name then (
          (match export () with
          | [] ->
              Printf.printf "no info\n"
          | metrics ->
              List.print ~first:"" ~last:"\n\n" ~sep:"\n"
                (Binocle.display_metric ~colors:!with_colors)
                stdout metrics) ;
          raise Exit (* done *)
        ) else (
          let dist = String.edit_distance name metric_name in
          if dist < !best_dist then (
            best_dist := dist ;
            best_name := name
          ))) ;
      Printf.printf "Unknown metric %S" metric_name ;
      match !best_name with
      | "" -> Printf.printf "\n"
      | n -> Printf.printf ", do you mean %S?\n" n
    with Exit ->
        ())
