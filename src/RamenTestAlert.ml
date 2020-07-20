(* Test suite for alerting *)
open Batteries
open RamenHelpersNoLog
open RamenHelpers
open RamenLog
open RamenSync
module C = RamenConf
module CliInfo = RamenConstsCliInfo
module Default = RamenConstsDefault
module ExitCodes = RamenConstsExitCodes
module Files = RamenFiles
module N = RamenName
module Processes = RamenProcesses
module ServiceNames = RamenConstsServiceNames
module ZMQClient = RamenSyncZMQClient

(* A test is a succession of commands to send and expected config values.
 * Commands are sent in order and expected values are also checked in order.
 * Write instructions can be assigned a delay.
 * Expectations can be assigned timeouts. *)

type time_spec =
  Absolute of float | Relative of float
  [@@ppp PPP_OCaml]

let print_time_spec oc = function
  | Absolute t ->
      print_as_date oc t
  | Relative d ->
      print_as_duration oc d

let absolute_time now = function
  | Absolute t -> t
  | Relative dt -> now +. dt

type step =
  | Write of {
      delay : time_spec [@ppp_default Relative 0.] ;
      key : string ;
      value : string }
  | Expect of {
      not_before : time_spec [@ppp_default Relative 0.] ;
      not_after : time_spec [@ppp_default Relative 0.] ; (* 0 = no timeout *)
      key : string ;
      value : string }
  (* Helper for some common writes: *)
  | Notify of {
      delay : time_spec [@ppp_default Relative 0.] ;
      site : N.site [@ppp_default N.site "test_site"] ;
      worker : N.fq [@ppp_default N.fq "test_worker"] ;
      test : bool [@ppp_default false] ;
      sent_time : time_spec [@ppp_default Relative 0.] ;
      event_time : time_spec option [@ppp_default None] ;
      name : string ;
      firing : bool [@ppp_default true] ;
      certainty : float [@ppp_default 1.] ;
      debounce : float [@ppp_default Default.debounce_delay] ;
      timeout : float [@ppp_default 0.] ;
      parameters : (string * string) list [@ppp_default []] }
    (* TODO: Ack *)
  [@@ppp PPP_OCaml]

type test_spec =
  { max_fpr : float [@ppp_default Default.max_fpr] ;
    debounce_delay : float [@ppp_default Default.debounce_delay] ;
    max_last_sent_kept : int [@ppp_default Default.max_last_sent_kept] ;
    max_incident_age : float [@ppp_default Default.max_incident_age] ;
    steps : step list }
  [@@ppp PPP_OCaml]

let do_send_value session ?while_ k v =
  let cmd = Client.CltMsg.SetKey (k, v) in
  !logger.debug "Writing command %a" Client.CltMsg.print_cmd cmd ;
  ZMQClient.send_cmd session ?while_ cmd

let do_send_spec session ?while_ key value =
  let k = Key.of_string key in
  let ctx = Printf.sprintf "Parsing %S for key %S" value key in
  let v =
    fail_with_context ctx (fun () -> RamenConfClient.value_of_string k value) in
  do_send_value session ?while_ k v

let key_test session key value =
  let k_pat = Globs.compile key
  and v_pat = Globs.compile value in
  !logger.debug "Testing if %s matches %s" key value ;
  (* FIXME: check if key is known, otherwise try to take advantage of the
   * prefix *)
  try
    Client.iter session.ZMQClient.clt (fun k hv ->
      let k = Key.to_string k in
      if Globs.matches k_pat k then
        let v = Value.to_string hv.value in
        if Globs.matches v_pat v then (
          !logger.info "%s:%s matches pattern %s:%s" k v key value ;
          raise Exit
        ) else (
          !logger.debug "%s does not match" v
        )) ;
    false
  with Exit ->
    true

(* Called every time the configuration changes, try to advance/reject the test
 *)
let process_test =
  (* Used to compute relative timeouts for Expect actions: *)
  let last_now = ref (Unix.gettimeofday ()) in
  fun session ~while_ test_spec ->
    let now = Unix.gettimeofday () in
    match test_spec.steps with
    | [] ->
        !logger.info "Success!" ;
        Processes.quit := Some 0 ;
        test_spec
    | Write spec :: rest ->
        do_send_spec session ~while_ spec.key spec.value ;
        last_now := now ;
        { test_spec with steps = rest }
    | Notify n :: rest ->
        let k = Key.Notifications
        and v = Value.(Notification Alerting.{
          site = n.site ;
          worker = n.worker ;
          test = n.test ;
          sent_time = absolute_time now n.sent_time ;
          event_time = Option.map (absolute_time now) n.event_time ;
          name = n.name ;
          firing = n.firing ;
          certainty = n.certainty ;
          debounce = n.debounce ;
          timeout = n.timeout ;
          parameters = n.parameters }) in
        do_send_value session ~while_ k v ;
        last_now := now ;
        { test_spec with steps = rest }
    | Expect spec :: rest ->
        let not_before = absolute_time !last_now spec.not_before in
        let not_after = absolute_time !last_now spec.not_after in
        if spec.not_after <> Relative 0. && now >= not_after then (
          !logger.error
            "Failure: Expected %s to match %s after no longer than %a"
            spec.key spec.value
            print_time_spec spec.not_after ;
          Processes.quit := Some ExitCodes.test_failed ;
          test_spec
        ) else (
          if key_test session spec.key spec.value then (
            if now <= not_before then (
              !logger.error
                "Failure: %s match %s %a before min time %a"
                spec.key spec.value
                print_as_duration (not_before -. now)
                print_time_spec spec.not_before ;
              Processes.quit := Some ExitCodes.test_failed ;
              test_spec
            ) else
              { test_spec with steps = rest }
          ) else
            test_spec
        )

let run conf test_file () =
  RamenCliCheck.non_empty "test file name" (test_file : N.path :> string) ;
  (* Parse tests so that we won't have to clean anything if it's bogus *)
  !logger.debug "Parsing test specification in %a..."
    N.path_print_quoted test_file ;
  let test_spec = ref (Files.ppp_of_file test_spec_ppp_ocaml test_file) in
  let name = (Files.(basename test_file |> remove_ext) :> string) in
  (* Now create the temp dir: *)
  let persist_dir =
    Filename.get_temp_dir_name ()
      ^"/ramen_test_alert."^ string_of_int (Unix.getpid ()) |>
    N.path |> Files.uniquify in
  let confserver_port = random_port () in
  let sync_url = "localhost:"^ string_of_int confserver_port in
  let username = "_test" in
  (* Note: the test flag is useless in this context for now, but might be used
   * by the alerter at some point. *)
  let conf =
    C.{ conf with persist_dir ; username ; sync_url ; test = true } in
  (* Init various modules: *)
  init_logger conf.C.log_level ;
  !logger.debug "Using temp dir %a" N.path_print conf.persist_dir ;
  Files.mkdir_all conf.persist_dir ;
  Processes.prepare_signal_handlers conf ;
  (*
   * Start confserver and alerter as subprocesses:
   *)
  let srv_pub_key_file = N.path ""
  and srv_priv_key_file = N.path ""
  and ports = [ string_of_int confserver_port ]
  and ports_sec = []
  in
  RamenCliCheck.confserver ports ports_sec srv_pub_key_file srv_priv_key_file ;
  RamenCliCheck.alerter !test_spec.max_fpr ;
  (* TODO: factorize subprocesses handling with RamenCliCmd.start, into an
   * object that manages pids, stopped, last_signalled and offer a function
   * to manage the wait and set the signals. *)
  let pids = ref Map.Int.empty in
  let add_pid service_name pid =
    pids := Map.Int.add pid service_name !pids ;
    !logger.debug "Start %a process (%d)" N.service_print service_name pid in
  set_signals Sys.[ sigterm ; sigint ] (Signal_handle (fun s ->
    !logger.debug "Received signal %s, will propagate to %d children"
      (name_of_signal s)
      (Map.Int.cardinal !pids) ;
    if !Processes.quit = None then
      Processes.quit := Some ExitCodes.interrupted)) ;
  let to_stdout = true
  and prefix_log_with_name = true
  and debug = conf.C.log_level = Debug
  and keep_temp_files = conf.C.keep_temp_files
  (* FIXME: should come from the test spec: *)
  and variant = conf.C.forced_variants
  and bundle_dir = (conf.C.bundle_dir : N.path :> string)
  and colors = CliInfo.string_of_color !with_colors
  and default_max_fpr = nice_string_of_float !test_spec.max_fpr
  and debounce_delay = nice_string_of_float !test_spec.debounce_delay
  and max_last_sent_kept = string_of_int !test_spec.max_last_sent_kept
  and max_incident_age = nice_string_of_float !test_spec.max_incident_age
  and confserver = conf.C.sync_url
  and persist_dir = (conf.C.persist_dir :> string)
  in
  RamenSubcommands.run_confserver
    ~to_stdout ~prefix_log_with_name ~insecure:ports
    ~no_source_examples:true
    ~debug ~keep_temp_files ~variant
    ~bundle_dir ~colors ~persist_dir () |>
    add_pid ServiceNames.confserver ;
  RamenSubcommands.run_alerter
    ~default_max_fpr ~to_stdout ~prefix_log_with_name
    ~debounce_delay ~max_last_sent_kept ~max_incident_age
    ~debug ~keep_temp_files ~variant
    ~bundle_dir ~confserver ~colors ~for_test:true () |>
    add_pid ServiceNames.alerter ;
  let stopped = ref 0. in
  let last_signalled = ref 0. in
  let wait_children ~while_ =
    let finished () =
      !logger.info "All processes have stopped" in
    if Map.Int.is_empty !pids then
      finished ()
    else (
      (* Kill children when done: *)
      if !stopped <= 0. && !Processes.quit <> None then
        stopped := Unix.gettimeofday () ;
      if !stopped > 0. then (
        let now = Unix.gettimeofday () in
        if now -. !last_signalled > 1. then (
          let s = if now -. !stopped > 3. then Sys.sigkill else Sys.sigterm in
          !logger.info "Terminating %d children with %s (%a)"
            (Map.Int.cardinal !pids)
            (name_of_signal s)
            (pretty_enum_print N.service_print) (Map.Int.values !pids) ;
          Map.Int.iter (fun p _ -> Unix.kill p s) !pids ;
          last_signalled := now
        )
      ) ;
      (* Collect children status: *)
      (match restart_on_eintr ~while_ (Unix.waitpid [ WNOHANG ]) ~-1 with
      | exception Unix.Unix_error (ECHILD, _, _) ->
          finished ()
      | 0, _ ->
          ()
      | pid, status ->
          (match Map.Int.extract pid !pids with
          | exception Not_found -> ()
          | service_name, pids_ ->
              let delay_str =
                if !stopped <= 0. then "" else (
                  let delay = Unix.gettimeofday () -. !stopped in
                  Printf.sprintf2 " after %a" print_as_duration delay
                ) in
              !logger.debug "%a process (%d) has stopped%s: %s"
                N.service_print service_name
                pid
                delay_str
                (RamenHelpers.string_of_process_status status);
              pids := pids_))
    ) in
  (*
   * Connect to the conf server and start the tests
   *)
  !logger.debug "Connecting to the configuration server (%s)..." conf.C.sync_url ;
  let while_ () = !Processes.quit = None in
  (* On error, set the quit flag and return input: *)
  let or_quit f =
    try f ()
    with Exit ->
        !logger.info "Quitting"
      | e ->
        !logger.error "%s: Quitting" (Printexc.to_string e) ;
        if !Processes.quit = None then
          Processes.quit := Some ExitCodes.other_error
  in
  let topics = [ "*" ] in
  let on_new session _k _v _uid _mtime _can_write _can_del _owner _expiry =
    or_quit (fun () -> test_spec := process_test session ~while_ !test_spec)
  and on_set session _k _v _uid _mtime =
    or_quit (fun () -> test_spec := process_test session ~while_ !test_spec)
  and on_del session _k _v =
    or_quit (fun () -> test_spec := process_test session ~while_ !test_spec)
  and on_synced _ =
    !logger.info "Starting test %s..." name
  and timeo = 1.
  in
  let sync_loop session =
    or_quit (fun () -> test_spec := process_test session ~while_ !test_spec) ;
    wait_children ~while_ ;
    ZMQClient.process_until ~while_ session in
  or_quit (fun () ->
    RamenSyncHelpers.start_sync
      conf ~while_ ~topics ~on_synced ~on_new ~on_set ~on_del
      ~recvtimeo:timeo ~sndtimeo:timeo sync_loop) ;
  if !Processes.quit = None then Processes.quit := Some 0 ;
  wait_children ~while_:always ;
  while not (Map.Int.is_empty !pids) do
    !logger.debug "Waiting for %d children..." (Map.Int.cardinal !pids) ;
    Unix.sleep 1 ;
    wait_children ~while_:always ;
  done
