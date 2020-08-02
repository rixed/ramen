(* This is where the command line arguments are processed.
 * This then mostly transfer control to various specialized functions
 * of the RamenCliCmd module.
 *)
open Cmdliner
open Batteries
open RamenHelpersNoLog
open RamenHelpers
module C = RamenConf
module CliInfo = RamenConstsCliInfo
module Default = RamenConstsDefault
module T = RamenTypes
module N = RamenName
module Processes = RamenProcesses
module Files = RamenFiles

(*
 * Common options
 *)

let path =
  let parse s = Pervasives.Ok (N.path s)
  and print fmt (p : N.path) =
    Format.fprintf fmt "%s" (p :> string)
  in
  Arg.conv ~docv:"FILE" (parse, print)

let site =
  let parse s = Pervasives.Ok (N.site s)
  and print fmt (p : N.site) =
    Format.fprintf fmt "%s" (p :> string)
  in
  Arg.conv ~docv:"SITE" (parse, print)

let glob =
  let parse s = Pervasives.Ok (Globs.compile s)
  and print fmt p =
    Format.fprintf fmt "%s" (Globs.decompile p)
  in
  Arg.conv ~docv:"PATTERN" (parse, print)

let port =
  let parse s =
    match int_of_string s with
    | exception Failure _ ->
        Pervasives.Error (`Msg "Cannot parse port into an integer")
    | p ->
        if p < 0 || p > 65535 then
          Pervasives.Error (`Msg "Invalid port number")
        else
          Pervasives.Ok p
  and print fmt p = Format.fprintf fmt "%d" p
  in
  Arg.conv ~docv:"PORT" (parse, print)

(* Never take keys from the command line directly, but read them from
 * files. If secure, also check the file is not world readable. *)
let key secure =
  let parse s =
    if s = "" then Pervasives.Ok "" else
    try Pervasives.Ok (Files.read_key secure (N.path s))
    with Failure msg ->
      Pervasives.Error (`Msg msg) in
  let print fmt k =
    if secure then Format.fprintf fmt "<KEY>"
    else Format.fprintf fmt "%S" k in
  Arg.conv ~docv:"KEYFILE" (parse, print)

let info_of_opt ?docs opt =
  let env =
    if opt.CliInfo.env = "" then None
    else Some (Term.env_info opt.env) in
  let doc =
    if opt.doc = "" then None else Some opt.doc in
  let docv =
    if opt.docv = "" then None else Some opt.docv in
  Arg.info ?doc ?docv ?env ?docs opt.names

let flag_of_opt ?docs opt =
  assert (opt.CliInfo.typ = CliInfo.Flag) ;
  let i = info_of_opt ?docs opt in
  Arg.(value (flag i))

let info_of_cmd cmd =
  Term.info ~doc:cmd.CliInfo.doc cmd.name

let persist_dir =
  let i = info_of_opt ~docs:Manpage.s_common_options CliInfo.persist_dir in
  Arg.(value (opt path Default.persist_dir i))

(* Note regarding default_username:
 * It's either passed the username to use for the Auth message to the
 * confserver, or an empty string to use the Unix $USER, or None when no
 * connection to the confserver is actually needed, in which case none
 * of the confserver options will be initialized. *)
let copts ?default_username () =
  let docs = Manpage.s_common_options in
  let debug =
    flag_of_opt ~docs CliInfo.debug
  and quiet =
    flag_of_opt ~docs CliInfo.quiet
  and rand_seed =
    let i = info_of_opt ~docs CliInfo.rand_seed in
    Arg.(value (opt (some int) None i))
  and keep_temp_files =
    flag_of_opt ~docs CliInfo.keep_temp_files
  and reuse_prev_files =
    flag_of_opt ~docs CliInfo.reuse_prev_files
  and forced_variants =
    let i = info_of_opt ~docs CliInfo.variant in
    Arg.(value (opt_all string [] i))
  and local_experiments =
    let i = info_of_opt ~docs CliInfo.local_experiments in
    Arg.(value (opt path (N.path "") i))
  and initial_export_duration =
    let i = info_of_opt ~docs CliInfo.initial_export_duration in
    Arg.(value (opt float Default.initial_export_duration i))
  and site =
    let i = info_of_opt ~docs CliInfo.site in
    Arg.(value (opt (some site) None i))
  and bundle_dir =
    let i = info_of_opt ~docs CliInfo.bundle_dir in
    Arg.(value (opt path RamenCompilConfig.default_bundle_dir i))
  and masters =
    let i = info_of_opt ~docs CliInfo.masters in
    Arg.(value (opt_all string [] i))
  and confserver_url =
    if default_username = None then Term.const "" else
    let i = info_of_opt ~docs CliInfo.confserver_url in
    Arg.(value (opt ~vopt:"localhost" string "" i))
  and confserver_key =
    if default_username = None then Term.const "" else
    let i = info_of_opt ~docs CliInfo.confserver_key in
    Arg.(value (opt (key false) "" i))
  and username =
    if default_username = None then Term.const "" else
    let def = Option.get default_username in
    let i = info_of_opt ~docs (
      (* Take $USER only for non-service commands: *)
      if default_username = Some "" then CliInfo.username
                                    else { CliInfo.username with env = "" }
    ) in
    Arg.(value (opt string def i))
  and client_pub_key =
    if default_username = None then Term.const "" else
    let i = info_of_opt ~docs CliInfo.client_pub_key in
    Arg.(value (opt (key false) "" i))
  and client_priv_key =
    if default_username = None then Term.const "" else
    let i = info_of_opt ~docs CliInfo.client_priv_key in
    Arg.(value (opt (key true) "" i))
  and identity_file =
    if default_username = None then Term.const (N.path "") else
    let i = info_of_opt ~docs CliInfo.identity_file in
    Arg.(value (opt path (N.path "") i))
  and colors =
    let i = info_of_opt ~docs CliInfo.colors in
    let colors = [ "never", false ; "always", true ] in
    Arg.(value (opt (enum colors) true i))
  in
  Term.(const RamenCliCmd.make_copts
    $ debug
    $ quiet
    $ persist_dir
    $ rand_seed
    $ keep_temp_files
    $ reuse_prev_files
    $ forced_variants
    $ local_experiments
    $ initial_export_duration
    $ site
    $ bundle_dir
    $ masters
    $ confserver_url
    $ confserver_key
    $ username
    $ client_pub_key
    $ client_priv_key
    $ identity_file
    $ colors)

(*
 * Start the process supervisor
 *)

let daemonize =
  flag_of_opt CliInfo.daemonize

let to_stdout =
  flag_of_opt CliInfo.to_stdout

let to_syslog =
  flag_of_opt CliInfo.to_syslog

let prefix_log_with_name =
  flag_of_opt CliInfo.prefix_log_with_name

let external_compiler =
  flag_of_opt CliInfo.external_compiler

let max_simult_compilations =
  let i = info_of_opt CliInfo.max_simult_compilations in
  let def = Atomic.Counter.get RamenOCamlCompiler.max_simult_compilations in
  Arg.(value (opt int def i))

let smt_solver =
  let i = info_of_opt CliInfo.smt_solver in
  Arg.(value (opt string !RamenSmt.solver i))

let fail_for_good =
  flag_of_opt CliInfo.fail_for_good

let kill_at_exit =
  flag_of_opt CliInfo.kill_at_exit

let test_notifs_every =
  let i = info_of_opt CliInfo.test_notifs_every in
  Arg.(value (opt float ~vopt:Default.test_notifs_every 0. i))

let supervisor =
  Term.(
    (const RamenCliCmd.supervisor
      $ copts ~default_username:"_supervisor" ()
      $ daemonize
      $ to_stdout
      $ to_syslog
      $ prefix_log_with_name
      $ external_compiler
      $ max_simult_compilations
      $ smt_solver
      $ fail_for_good
      $ kill_at_exit
      $ test_notifs_every),
    info_of_cmd CliInfo.supervisor)

(*
 * Delete old or unused files
 *)

let loop =
  let i = info_of_opt CliInfo.loop in
  Arg.(value (opt (some float) ~vopt:None (Some 0.) i))

let dry_run =
  flag_of_opt CliInfo.dry_run

let del_ratio =
  let i = info_of_opt CliInfo.del_ratio in
  Arg.(value (opt float Default.del_ratio i))

let duration docv =
  let p = RamenParsing.(string_parser ~what:("parsing "^ docv)
                                      ~print:Float.print duration) in
  let parse s = Pervasives.Ok (p s)
  and print fmt d =
    IO.to_string RamenParsing.print_duration d |>
    Format.pp_print_string fmt
  in
  Arg.conv ~docv (parse, print)

let compress_older =
  let i = info_of_opt CliInfo.compress_older in
  Arg.(value (opt (duration "--compress-older") Default.compress_older i))

let gc =
  Term.(
    (const RamenCliCmd.gc
      $ copts ~default_username:"_gc" ()
      $ dry_run
      $ del_ratio
      $ compress_older
      $ loop
      $ daemonize
      $ to_stdout
      $ to_syslog
      $ prefix_log_with_name),
    info_of_cmd CliInfo.gc)

(*
 * Notifications: Start the alerter and send test ones
 *)

let max_fpr =
  let i = info_of_opt CliInfo.max_fpr in
  Arg.(value (opt float Default.max_fpr i))

let timeout_idle_kafka_producers =
  let i = info_of_opt CliInfo.timeout_idle_kafka_producers in
  Arg.(value (opt float Default.timeout_idle_kafka_producers i))

let debounce_delay =
  let i = info_of_opt CliInfo.debounce_delay in
  Arg.(value (opt float Default.debounce_delay i))

let max_last_sent_kept =
  let i = info_of_opt CliInfo.max_last_sent_kept in
  Arg.(value (opt int Default.max_last_sent_kept i))

let max_incident_age =
  let i = info_of_opt CliInfo.max_incident_age in
  Arg.(value (opt float Default.max_incident_age i))

let for_test =
  flag_of_opt CliInfo.for_test

let reschedule_clock =
  let i = info_of_opt CliInfo.reschedule_clock in
  Arg.(value (opt float Default.reschedule_clock i))

let alerter =
  Term.(
    (const RamenCliCmd.alerter
      $ copts ~default_username:"_alerter" ()
      $ max_fpr
      $ daemonize
      $ to_stdout
      $ to_syslog
      $ prefix_log_with_name
      $ timeout_idle_kafka_producers
      $ debounce_delay
      $ max_last_sent_kept
      $ max_incident_age
      $ for_test
      $ reschedule_clock),
    info_of_cmd CliInfo.alerter)

let text_param =
  let parse s =
    try Pervasives.Ok (String.split s ~by:"=")
    with Not_found ->
      Pervasives.Error (
        `Msg "You must specify the identifier, followed by an equal \
              sign (=), followed by the value.")
  and print fmt (pname, pval) =
    Format.fprintf fmt "%s=%s" pname pval
  in
  Arg.conv ~docv:"IDENTIFIER=VALUE" (parse, print)

let text_params =
  let i = info_of_opt CliInfo.parameter in
  Arg.(value (opt_all text_param [] i))

let is_test_alert =
  flag_of_opt CliInfo.is_test_alert

let notif_name =
  let i = info_of_opt CliInfo.notif_name in
  Arg.(required (pos 0 (some string) None i))

let notify =
  Term.(
    (const RamenCliCmd.notify
      $ copts ()
      $ text_params
      $ is_test_alert
      $ notif_name),
    info_of_cmd CliInfo.notify)

let test_file =
  let i = info_of_opt CliInfo.test_file in
  Arg.(value (pos 0 path (N.path "") i))

let test_alert =
  Term.(
    (const RamenTestAlert.run
      $ copts ()
      $ test_file),
    info_of_cmd CliInfo.test_alert)

(*
 * Service to forward tuples over the network
 *)

let tunneld_port =
  let i = info_of_opt CliInfo.tunneld_port in
  Arg.(value (opt (some port) None i))

let tunneld =
  Term.(
    (const RamenCliCmd.tunneld
      $ copts ~default_username:"_tunneld" ()
      $ daemonize
      $ to_stdout
      $ to_syslog
      $ prefix_log_with_name
      $ tunneld_port),
    info_of_cmd CliInfo.tunneld)

(*
 * Config Server
 *)

let confserver_ports =
  let i = info_of_opt CliInfo.confserver_port
  and vopt = "127.0.0.1:"^ string_of_int Default.confserver_port in
  Arg.(value (opt_all ~vopt string [] i))

let confserver_ports_sec =
  let i = info_of_opt CliInfo.confserver_port_sec
  and vopt = string_of_int Default.confserver_port_sec in
  Arg.(value (opt_all ~vopt string [] i))

let server_priv_key_file =
  let i = info_of_opt CliInfo.server_priv_key in
  Arg.(value (opt path (N.path "") i))

let server_pub_key_file =
  let i = info_of_opt CliInfo.server_pub_key in
  Arg.(value (opt path (N.path "") i))

let no_source_examples =
  flag_of_opt CliInfo.no_source_examples

let archive_total_size =
  let i = info_of_opt CliInfo.default_archive_total_size in
  Arg.(value (opt int Default.archive_total_size i))

let archive_recall_cost =
  let i = info_of_opt CliInfo.default_archive_recall_cost in
  Arg.(value (opt float Default.archive_recall_cost i))

let oldest_site =
  let i = info_of_opt CliInfo.oldest_restored_site in
  Arg.(value (opt float Default.oldest_restored_site i))

let confserver =
  Term.(
    (const RamenCliCmd.confserver
      $ copts ~default_username:"_confserver" ()
      $ daemonize
      $ to_stdout
      $ to_syslog
      $ prefix_log_with_name
      $ confserver_ports
      $ confserver_ports_sec
      $ server_pub_key_file
      $ server_priv_key_file
      $ no_source_examples
      $ archive_total_size
      $ archive_recall_cost
      $ oldest_site),
    info_of_cmd CliInfo.confserver)

(* confclient will dump, read or write conf values according to the presence
 * of those options:*)
let confclient_key =
  let i = info_of_opt CliInfo.conf_key in
  Arg.(value (opt string "" i))

let confclient_value =
  let i = info_of_opt CliInfo.conf_value in
  Arg.(value (opt string "" i))

let confclient_del =
  flag_of_opt CliInfo.conf_delete

let confclient =
  Term.(
    (const RamenCliCmd.confclient
      $ copts ~default_username:"" ()
      $ confclient_key
      $ confclient_value
      $ confclient_del),
    info_of_cmd CliInfo.confclient)

(*
 * User management
 *)

let username =
  let i = info_of_opt CliInfo.added_username in
  Arg.(required (pos 0 (some string) None i))

let roles =
  let i = info_of_opt CliInfo.role in
  let roles = RamenSyncUser.Role.[ "admin", Admin ; "user", User ] in
  Arg.(value (opt_all (enum roles) [] i))

let output_file =
  let i = info_of_opt CliInfo.output_file in
  Arg.(value (opt (some path) None i))

let useradd =
  Term.(
    (const RamenSyncUsers.add
      $ persist_dir
      $ output_file
      $ username
      $ roles
      $ server_pub_key_file),
    info_of_cmd CliInfo.useradd)

let userdel =
  Term.(
    (const RamenSyncUsers.del
      $ persist_dir
      $ username),
    info_of_cmd CliInfo.userdel)

let usermod =
  Term.(
    (const RamenSyncUsers.mod_
      $ persist_dir
      $ username
      $ roles),
    info_of_cmd CliInfo.usermod)

(*
 * Examine the ringbuffers
 *)

(* TODO: check that this is actually the name of a ringbuffer file *)
let rb_file =
  let i = info_of_opt CliInfo.rb_file in
  Arg.(required (pos 0 (some path) None i))

let num_entries =
  let i = info_of_opt CliInfo.num_entries in
  Arg.(value (opt int 1 i))

let dequeue =
  Term.(
    (const RingBufCmd.dequeue
      $ copts ()
      $ rb_file
      $ num_entries),
    info_of_cmd CliInfo.dequeue)

let max_bytes =
  let i = info_of_opt CliInfo.max_bytes in
  Arg.(value (opt int 64 i))

let rb_files =
  let i = info_of_opt CliInfo.rb_files in
  Arg.(non_empty (pos_all path [] i))

let summary =
  Term.(
    (const RingBufCmd.summary
      $ copts ()
      $ max_bytes
      $ rb_files),
    info_of_cmd CliInfo.summary)

let repair =
  Term.(
    (const RingBufCmd.repair
      $ copts ()
      $ rb_files),
    info_of_cmd CliInfo.repair)

let start_word =
  let i = info_of_opt CliInfo.start_word in
  Arg.(required (opt (some int) None i))

let stop_word =
  let i = info_of_opt CliInfo.stop_word in
  Arg.(required (opt (some int) None i))

let dump =
  Term.(
    (const RingBufCmd.dump
      $ copts ()
      $ start_word
      $ stop_word
      $ rb_file),
    info_of_cmd CliInfo.dump_ringbuf)

let pattern =
  let i = info_of_opt CliInfo.pattern in
  Arg.(value (pos 0 glob Globs.all i))

let no_abbrev =
  flag_of_opt CliInfo.no_abbrev

let show_all_links =
  flag_of_opt CliInfo.show_all_links

let show_all_workers =
  flag_of_opt CliInfo.show_all_workers

let as_tree =
  flag_of_opt CliInfo.as_tree

let pretty =
  flag_of_opt CliInfo.pretty

let with_header =
  let i = info_of_opt CliInfo.with_header in
  Arg.(value (opt ~vopt:Default.header_every int 0 i))

let sort_col =
  let i = info_of_opt CliInfo.sort_col in
  Arg.(value (opt string "1" i))

let top =
  let i = info_of_opt CliInfo.top in
  Arg.(value (opt (some int) None i))

let links =
  Term.(
    (const RingBufCmd.links
      $ copts ()  (* TODO: confserver version *)
      $ no_abbrev
      $ show_all_links
      $ as_tree
      $ pretty
      $ with_header
      $ sort_col
      $ top
      $ pattern),
    info_of_cmd CliInfo.links)

(*
 * Compiling/Running/Stopping
 *)

let assignment =
  let parse s =
    match String.split s ~by:"=" with
    | exception Not_found ->
        Pervasives.Error (
          `Msg "You must specify the identifier, followed by an equal \
                sign (=), followed by the value.")
    | pname, pval ->
        let what = "value of command line parameter "^ pname ^
                   "(\""^ pval ^"\")" in
        (match T.of_string ~what pval with
        | Result.Ok v -> Pervasives.Ok (N.field pname, v)
        | Result.Bad e -> Pervasives.Error (`Msg e))
  and print fmt ((pname : N.field), pval) =
    Format.fprintf fmt "%s=%s"
      (pname :> string)
      (T.to_string pval)
  in
  Arg.conv ~docv:"IDENTIFIER=VALUE" (parse, print)

(* Note: parameter with same name in different functions will all take
 * their value from this. Easy to add a prefix with function name when
 * it causes troubles. *)
let params =
  let i = info_of_opt CliInfo.parameter in
  Arg.(value (opt_all assignment [] i))

let program_globs =
  let i = info_of_opt CliInfo.program_globs in
  Arg.(non_empty (pos_all glob [] i))

let lib_path =
  let i = info_of_opt CliInfo.lib_path in
  Arg.(value (opt_all path [] i))

let src_files =
  let i = info_of_opt CliInfo.src_files in
  Arg.(non_empty (pos_all path [] i))

let program =
  let parse s = Pervasives.Ok (N.program s)
  and print fmt (p : N.program) =
    Format.fprintf fmt "%s" (p :> string)
  in
  Arg.conv ~docv:"PROGRAM" (parse, print)

let src_path =
  let parse s = Pervasives.Ok (N.src_path s)
  and print fmt (p : N.src_path ) =
    Format.fprintf fmt "%s" (p :> string)
  in
  Arg.conv ~docv:"PATH" (parse, print)

let as_ =
  let i = info_of_opt CliInfo.as_ in
  Arg.(value (opt (some src_path) None i))

let replace =
  flag_of_opt CliInfo.replace

let compile =
  Term.(
    (const RamenCliCmd.compile
      $ copts ~default_username:"" ()
      $ lib_path
      $ external_compiler
      $ max_simult_compilations
      $ smt_solver
      $ src_files
      $ output_file
      $ as_
      $ replace),
    info_of_cmd CliInfo.compile)

let precompserver =
  Term.(
    (const RamenCliCmd.precompserver
      $ copts ~default_username:"_precompserver" ()
      $ daemonize
      $ to_stdout
      $ to_syslog
      $ prefix_log_with_name
      $ smt_solver),
    info_of_cmd CliInfo.precompserver)

let execompserver =
  Term.(
    (const RamenCliCmd.execompserver
      $ copts ~default_username:"_execompserver" ()
      $ daemonize
      $ to_stdout
      $ to_syslog
      $ prefix_log_with_name
      $ external_compiler
      $ max_simult_compilations),
    info_of_cmd CliInfo.execompserver)

let report_period =
  let i = info_of_opt CliInfo.report_period in
  Arg.(value (opt float Default.report_period i))

let src_file =
  let i = info_of_opt CliInfo.src_file in
  Arg.(required (pos 0 (some path) None i))

let on_site =
  let i = info_of_opt CliInfo.on_site in
  Arg.(value (opt glob Globs.all i))

let program_name =
  let i = info_of_opt CliInfo.program_name in
  Arg.(required (pos 0 (some program) None i))

let cwd =
  let i = info_of_opt CliInfo.cwd in
  Arg.(value (opt (some path) None i))

let run =
  Term.(
    (const RamenCliCmd.run
      $ copts ~default_username:"" ()
      $ params
      $ report_period
      $ program_name
      $ on_site
      $ cwd
      $ replace),
    info_of_cmd CliInfo.run)

let purge =
  flag_of_opt CliInfo.purge

let kill =
  Term.(
    (const RamenCliCmd.kill
      $ copts ~default_username:"" ()
      $ program_globs
      $ purge),
    info_of_cmd CliInfo.kill)

let func_name =
  let parse s = Pervasives.Ok (N.func s)
  and print fmt (p : N.func) =
    Format.fprintf fmt "%s" (p :> string)
  in
  Arg.conv ~docv:"FUNCTION" (parse, print)

let opt_function_name p =
  let i = info_of_opt CliInfo.function_name in
  Arg.(value (pos p (some func_name) None i))

let bin_file =
  let i = info_of_opt CliInfo.bin_file in
  Arg.(required (pos 0 (some path) None i))


let info =
  Term.(
    (const RamenCliCmd.info
      $ copts ~default_username:"" ()
      $ params
      $ bin_file
      $ opt_function_name 1),
    info_of_cmd CliInfo.info)

(*
 * `ramen choreographer`
 *)

let choreographer =
  Term.(
    (const RamenCliCmd.choreographer
      $ copts ~default_username:"_choreographer" ()
      $ daemonize
      $ to_stdout
      $ to_syslog
      $ prefix_log_with_name),
    info_of_cmd CliInfo.choreographer)

(*
 * `ramen replayer`
 *)

let replayer =
  Term.(
    (const RamenCliCmd.replay_service
      $ copts ~default_username:"_replay_service" ()
      $ daemonize
      $ to_stdout
      $ to_syslog
      $ prefix_log_with_name),
    info_of_cmd CliInfo.replayer)

(*
 * Display the output of any operation
 *)

let csv_separator =
  let i = info_of_opt CliInfo.csv_separator in
  Arg.(value (opt string "," i))

let csv_null =
  let i = info_of_opt CliInfo.csv_null in
  Arg.(value (opt string "<NULL>" i))

let csv_raw =
  flag_of_opt CliInfo.csv_raw

let last =
  let i = info_of_opt CliInfo.last in
  Arg.(value (opt (some int) None i))

let next =
  let i = info_of_opt CliInfo.next in
  Arg.(value (opt (some int) None i))

let follow =
  flag_of_opt CliInfo.follow

let filter =
  (* Longer first: *)
  let operators =
    [ " not in"; " in"; ">="; "<="; "!="; "="; "<>"; "<"; ">" ] in
  let parse s =
    match
      List.find_map (fun op ->
        match String.split s ~by:op with
        | exception Not_found -> None
        | pname, pval -> let t = String.trim in Some (t pname, t op, t pval)
      ) operators
    with
    | exception Not_found ->
        Pervasives.Error (
          `Msg "You must specify the identifier, followed by an operator \
                (=, <=, <, >, >=, etc), followed by the value.")
    | pname, op, pval ->
        let what = "value of command line parameter "^ pname in
        (match T.of_string ~what pval with
        | Result.Ok v -> Pervasives.Ok (N.field pname, op, v)
        | Result.Bad e -> Pervasives.Error (`Msg e))
  and print fmt ((pname : N.field), op, pval) =
    Format.fprintf fmt "%s%s%s"
      (pname :> string)
      op
      (T.to_string pval)
  in
  Arg.conv ~docv:"IDENTIFIER[=|>|<|>=|<=]VALUE" (parse, print)

let where =
  let i = info_of_opt CliInfo.where in
  Arg.(value (opt_all filter [] i))

let time =
  let parse s =
    match time_of_graphite_time s with
    | None -> Pervasives.Error (
        `Msg (Printf.sprintf "Cannot parse string %S as time" s))
    | Some f -> Pervasives.Ok f
  and print fmt t =
    Format.fprintf fmt "%f" t
  in
  Arg.conv ~docv:"TIME" (parse, print)

let since =
  let i = info_of_opt CliInfo.since in
  Arg.(value (opt (some time) None i))

let until =
  let i = info_of_opt CliInfo.until in
  Arg.(value (opt (some time) None i))

let with_event_time =
  flag_of_opt CliInfo.with_event_time

let timeout =
  let i = info_of_opt CliInfo.timeout in
  Arg.(value (opt (duration "--timeout") 300. i))

let with_units =
  flag_of_opt CliInfo.with_units

let flush =
  flag_of_opt CliInfo.flush

let func_name_or_code =
  let i = info_of_opt CliInfo.func_name_or_code in
  Arg.(non_empty (pos_all string [] i))

let tail =
  Term.(
    (const RamenCliCmd.tail
      $ copts ~default_username:"" ()
      $ func_name_or_code
      $ with_header
      $ with_units
      $ csv_separator
      $ csv_null
      $ csv_raw
      $ last
      $ next
      $ follow
      $ where
      $ since
      $ until
      $ with_event_time
      $ timeout
      $ pretty
      $ flush
      $ external_compiler
      $ max_simult_compilations
      $ smt_solver),
    info_of_cmd CliInfo.tail)

(*
 * Replay
 *)

let fq_name =
  let parse s = Pervasives.Ok (N.fq s)
  and print fmt (p : N.fq) =
    Format.fprintf fmt "%s" (p :> string)
  in
  Arg.conv ~docv:"FUNCTION" (parse, print)

let function_name p =
  let i = info_of_opt CliInfo.function_name in
  Arg.(required (pos p (some fq_name) None i))

let field =
  let parse s = Pervasives.Ok (N.field s)
  and print fmt (s : N.field)  =
    Format.fprintf fmt "%s" (s :> string)
  in
  Arg.conv ~docv:"FIELD" (parse, print)

let data_fields ~mandatory p =
  let i = info_of_opt CliInfo.data_fields in
  Arg.((if mandatory then non_empty else value) (pos_right (p-1) field [] i))

let since_mandatory =
  let i = info_of_opt CliInfo.since in
  Arg.(required (opt (some time) None i))

let via =
  let i = info_of_opt CliInfo.via in
  let vias = [ "file", false ; "confserver", true ] in
  Arg.(value (opt (enum vias) false i))

let replay =
  Term.(
    (const RamenCliCmd.replay
      $ copts ~default_username:"" ()
      $ func_name_or_code
      $ with_header
      $ with_units
      $ csv_separator
      $ csv_null
      $ csv_raw
      $ where
      $ since_mandatory
      $ until
      $ with_event_time
      $ pretty
      $ flush
      $ external_compiler
      $ max_simult_compilations
      $ smt_solver
      $ via),
    info_of_cmd CliInfo.replay)


(*
 * Timeseries
 *)

let num_points =
  let i = info_of_opt CliInfo.num_points in
  Arg.(value (opt int 0 i))

let time_step =
  let i = info_of_opt CliInfo.time_step in
  Arg.(value (opt float 0. i))

let consolidation =
  let i = info_of_opt CliInfo.consolidation in
  let cons_funcs =
    let p x = x, x in
    [ p "min" ; p "max" ; p "avg" ; p "sum" ; p "count" ] in
  Arg.(value (opt (enum cons_funcs) "avg" i))

let bucket_time =
  let i = info_of_opt CliInfo.bucket_time in
  let open RamenTimeseries in
  let bucket_times =
    [ "begin", Begin ; "middle", Middle ; "end", End ] in
  Arg.(value (opt (enum bucket_times) Begin i))

let factors =
  let i = info_of_opt CliInfo.factor in
  Arg.(value (opt_all field [] i))

let timeseries =
  Term.(
    (const RamenCliCmd.timeseries
      $ copts ~default_username:"" ()
      $ func_name_or_code
      $ since
      $ until
      $ with_header
      $ where
      $ factors
      $ num_points
      $ time_step
      $ csv_separator
      $ csv_null
      $ consolidation
      $ bucket_time
      $ pretty
      $ external_compiler
      $ max_simult_compilations
      $ smt_solver),
    info_of_cmd CliInfo.timeseries)

(*
 * Info
 *)

let ps =
  Term.(
    (const RamenCliCmd.ps
      $ copts ~default_username:"" ()
      $ pretty
      $ with_header
      $ sort_col
      $ top
      $ pattern
      $ show_all_workers),
    info_of_cmd CliInfo.ps)

let profile =
  Term.(
    (const RamenCliCmd.profile
      $ copts ~default_username:"" ()
      $ pretty
      $ with_header
      $ sort_col
      $ top
      $ pattern
      $ show_all_workers),
    info_of_cmd CliInfo.profile)

(*
 * Start the HTTP daemon (graphite impersonator)
 *)

let server_url def =
  let i = info_of_opt CliInfo.server_url in
  Arg.(value (opt string def i))

let graphite =
  let i = info_of_opt CliInfo.graphite in
  Arg.(value (opt ~vopt:(Some "") (some string) None i))

let api =
  let i = info_of_opt CliInfo.api in
  Arg.(value (opt ~vopt:(Some "") (some string) None i))

let table_prefix =
  let i = info_of_opt CliInfo.table_prefix in
  Arg.(value (opt string "" i))

let fault_injection_rate =
  let i = info_of_opt CliInfo.fault_injection_rate in
  Arg.(value (opt float Default.fault_injection_rate i))

let httpd =
  Term.(
    (const RamenCliCmd.httpd
      $ copts ~default_username:"_httpd" ()
      $ daemonize
      $ to_stdout
      $ to_syslog
      $ prefix_log_with_name
      $ fault_injection_rate
      $ server_url "http://127.0.0.1:8080"
      $ api
      $ table_prefix
      $ graphite
      $ external_compiler
      $ max_simult_compilations
      $ smt_solver),
    info_of_cmd CliInfo.httpd)

let for_render =
  flag_of_opt CliInfo.for_render

let query =
  let i = info_of_opt CliInfo.graphite_query in
  Arg.(value (pos 0 string "*" i))

let expand =
  Term.(
    (const RamenCliCmd.graphite_expand
      $ copts ()
      $ for_render
      $ since
      $ until
      $ query),
    info_of_cmd CliInfo.expand)

(*
 * Tests
 *)

let test =
  Term.(
    (const RamenTests.run
      $ copts ()
      $ server_url ""
      $ api
      $ graphite
      $ external_compiler
      $ max_simult_compilations
      $ smt_solver
      $ test_file),
    info_of_cmd CliInfo.test)

(*
 * Allocate disk space to workers archives
 *)

let update_stats =
  flag_of_opt CliInfo.update_stats

let update_allocs =
  flag_of_opt CliInfo.update_allocs

let reconf_workers =
  flag_of_opt CliInfo.reconf_workers

let archivist =
  Term.(
    (const RamenCliCmd.archivist
      $ copts ~default_username:"_archivist" ()
      $ loop
      $ daemonize
      $ update_stats
      $ update_allocs
      $ reconf_workers
      $ to_stdout
      $ to_syslog
      $ prefix_log_with_name
      $ smt_solver),
    info_of_cmd CliInfo.archivist)

(*
 * Start minimum ramen
 *)

let gc_loop =
  let i = info_of_opt CliInfo.gc_loop in
  Arg.(value (opt (some float) (Some Default.gc_loop) i))

let archivist_loop =
  let i = info_of_opt CliInfo.archivist_loop in
  Arg.(value (opt (some float) (Some Default.archivist_loop) i))

let start =
  Term.(
    (const RamenCliCmd.start
      $ copts ~default_username:"_confserver" ()
      $ daemonize
      $ to_stdout
      $ to_syslog
      $ confserver_ports
      $ confserver_ports_sec
      $ smt_solver
      $ fail_for_good
      $ kill_at_exit
      $ test_notifs_every
      $ external_compiler
      $ max_simult_compilations
      $ server_pub_key_file
      $ server_priv_key_file
      $ no_source_examples
      $ archive_total_size
      $ archive_recall_cost
      $ oldest_site
      $ gc_loop
      $ archivist_loop
      $ update_allocs
      $ reconf_workers
      $ del_ratio
      $ compress_older
      $ max_fpr
      $ timeout_idle_kafka_producers
      $ debounce_delay
      $ max_last_sent_kept
      $ max_incident_age),
    info_of_cmd CliInfo.start)

(*
 * Experiments
 *)

let variants =
  Term.(
    (const RamenCliCmd.variants
      $ copts ()),
    info_of_cmd CliInfo.variants)

(*
 * Display internal instrumentation. Also an option of various subsystems.
 *)

let stats =
  Term.(
    (const RamenCliCmd.stats
      $ copts ()),
    info_of_cmd CliInfo.stats)

(*
 * Autocompletion
 *)

let command =
  let i = info_of_opt CliInfo.command in
  Arg.(value (pos 0 string "" i))

let autocomplete =
  Term.(
    (const RamenCompletion.complete
      $ command),
    info_of_cmd CliInfo.autocomplete)

(*
 * Command line evaluation
 *)

let default =
  let sdocs = Manpage.s_common_options in
  let doc = "Ramen Stream Processor" in
  let version = RamenVersions.release_tag in
  Term.((ret (const (`Help (`Pager, None)))),
        info "ramen" ~version ~doc ~sdocs)

(* Run the program printing exceptions, and exit *)
let print_exn f =
  try f ()
  with Exit ->
         exit (!Processes.quit |? 0)
     | Timeout ->
         Printf.eprintf "%s\n" "Timed out" ;
         exit 1
     | Failure msg | Invalid_argument msg ->
         Printf.eprintf "%s\n" msg ;
         exit 1

let () =
  match
    print_exn (fun () ->
      Term.eval_choice ~catch:false default [
        (* daemons: *)
        supervisor ; gc ; httpd ; alerter ; tunneld ; archivist ;
        confserver ; confclient ; precompserver ; execompserver ;
        choreographer ; replayer ;
        start ;
        (* process management: *)
        compile ; run ; kill ; ps ; profile ; info ;
        (* user management: *)
        useradd ; userdel ; usermod ;
        (* reading tuples: *)
        tail ; replay ; timeseries ;
        (* writing tuples: *)
        notify ;
        (* ringbuffers management: *)
        dequeue ; summary ; repair ; dump ; links ;
        (* testing configuration: *)
        test ; test_alert ;
        (* introspection: *)
        variants ; stats ;
        (* debug: *)
        autocomplete ; expand
      ]) with
  | `Error _ -> exit 1
  | `Version | `Help -> exit 0
  | `Ok f ->
      print_exn f ;
      (* If we have set the exit code, use it: *)
      exit (!Processes.quit |? 0)
