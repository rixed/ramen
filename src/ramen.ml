(* This is where the command line arguments are processed.
 * This then mostly transfer control to various specialized functions
 * of the RamenCliCmd module.
 *)
open Cmdliner
open Batteries
open RamenHelpers
open RamenConsts
module T = RamenTypes
module N = RamenName
module C = RamenConf
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

let persist_dir =
  let env = Term.env_info "RAMEN_DIR" in
  let i = Arg.info ~doc:CliInfo.persist_dir
                   ~docs:Manpage.s_common_options
                   ~env [ "persist-dir" ] in
  Arg.(value (opt path Default.persist_dir i))

(* Note regarding default_username:
 * It's either passed the username to use for the Auth message to the
 * confserver, or an empty string to use the Unix $USER, or None when no
 * connection to the confserver is actually needed, in which case none
 * of the confserver options will be initialized. *)
let copts ?default_username () =
  let docs = Manpage.s_common_options in
  let debug =
    let env = Term.env_info "RAMEN_DEBUG" in
    let i = Arg.info ~doc:CliInfo.debug
                     ~docs ~env [ "d"; "debug" ] in
    Arg.(value (flag i))
  and quiet =
    let env = Term.env_info "RAMEN_QUIET" in
    let i = Arg.info ~doc:CliInfo.quiet
                     ~docs ~env [ "q"; "quiet" ] in
    Arg.(value (flag i))
  and rand_seed =
    let env = Term.env_info "RAMEN_RANDOM_SEED" in
    let i = Arg.info ~doc:CliInfo.rand_seed
                     ~docs ~env [ "seed"; "rand-seed" ] in
    Arg.(value (opt (some int) None i))
  and keep_temp_files =
    let env = Term.env_info "RAMEN_KEEP_TEMP_FILES" in
    let i = Arg.info ~doc:CliInfo.keep_temp_files
                     ~docs ~env [ "S" ; "keep-temp-files" ] in
    Arg.(value (flag i))
  and reuse_prev_files =
    let env = Term.env_info "RAMEN_REUSE_PREV_FILES" in
    let i = Arg.info ~doc:CliInfo.reuse_prev_files
                     ~docs ~env [ "reuse-prev-files" ] in
    Arg.(value (flag i))
  and forced_variants =
    let env = Term.env_info "RAMEN_VARIANTS" in
    let i = Arg.info ~doc:CliInfo.variant
                     ~docs ~env [ "variant" ] in
    Arg.(value (opt_all string [] i))
  and initial_export_duration =
    let env = Term.env_info "RAMEN_INITIAL_EXPORT" in
    let i = Arg.info ~doc:CliInfo.initial_export_duration
                     ~docs ~env [ "initial-export-duration" ] in
    Arg.(value (opt float Default.initial_export_duration i))
  and site =
    let env = Term.env_info "HOSTNAME" in
    let i = Arg.info ~docs ~doc:CliInfo.site ~env [ "site" ] in
    Arg.(value (opt (some site) None i))
  and bundle_dir =
    let env = Term.env_info "RAMEN_LIBS" in
    let i = Arg.info ~doc:CliInfo.bundle_dir
                     ~docs ~env [ "bundle-dir" ] in
    Arg.(value (opt path RamenCompilConfig.default_bundle_dir i))
  and masters =
    let env = Term.env_info "RAMEN_MASTERS" in
    let i = Arg.info ~doc:CliInfo.master
                     ~docs ~env ["master"] in
    Arg.(value (opt_all string [] i))
  and confserver_url =
    if default_username = None then Term.const "" else
    let env = Term.env_info "RAMEN_CONFSERVER" in
    let i = Arg.info ~doc:CliInfo.confserver_url
                     ~docs ~env [ "confserver" ] in
    Arg.(value (opt ~vopt:"localhost" string "" i))
  and confserver_key =
    if default_username = None then Term.const "" else
    let env = Term.env_info "RAMEN_CONFSERVER_KEY" in
    let i = Arg.info ~doc:CliInfo.confserver_key
                     ~docs ~env [ "confserver-key" ] in
    Arg.(value (opt (key false) "" i))
  and username =
    if default_username = None then Term.const "" else
    let def = Option.get default_username in
    let env =
      (* Take $USER only for non-service commands: *)
      if default_username = Some "" then Some (Term.env_info "USER")
                                    else None in
    let i = Arg.info ~doc:CliInfo.username
                     ~docs ?env [ "username" ] in
    Arg.(value (opt string def i))
  and client_pub_key =
    if default_username = None then Term.const "" else
    let env = Term.env_info "RAMEN_CLIENT_PUB_KEY" in
    let i = Arg.info ~doc:CliInfo.client_pub_key
                     ~docs ~env [ "pub-key" ] in
    Arg.(value (opt (key false) "" i))
  and client_priv_key =
    if default_username = None then Term.const "" else
    let env = Term.env_info "RAMEN_CLIENT_PRIV_KEY" in
    let i = Arg.info ~doc:CliInfo.client_priv_key
                     ~docs ~env [ "priv-key" ] in
    Arg.(value (opt (key true) "" i))
  and identity_file =
    if default_username = None then Term.const (N.path "") else
    let env = Term.env_info "RAMEN_CLIENT_IDENTITY" in
    let i = Arg.info ~doc:CliInfo.identity_file
                     ~docs ~env [ "i" ; "identity" ] in
    Arg.(value (opt path (N.path "") i))
  and colors =
    let env = Term.env_info "RAMEN_COLORS" in
    let i = Arg.info ~doc:CliInfo.colors
                     ~env [ "colors" ] in
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
  let env = Term.env_info "RAMEN_DAEMONIZE" in
  let i = Arg.info ~doc:CliInfo.daemonize
                   ~env ["daemonize"] in
  Arg.(value (flag i))

let to_stdout =
  let env = Term.env_info "RAMEN_LOG_TO_STDERR" in
  let i = Arg.info ~doc:CliInfo.to_stdout
                   ~env [ "to-stderr"; "stderr" ;
                          "to-stdout"; "stdout" ] in
  Arg.(value (flag i))

let to_syslog =
  let env = Term.env_info "RAMEN_LOG_TO_SYSLOG" in
  let i = Arg.info ~doc:CliInfo.to_syslog
                   ~env [ "to-syslog" ; "syslog" ] in
  Arg.(value (flag i))

let prefix_log_with_name =
  let env = Term.env_info "RAMEN_PREFIX_LOG_WITH_NAME" in
  let i = Arg.info ~doc:CliInfo.prefix_log_with_name
                   ~env [ "prefix-log-with-name" ] in
  Arg.(value (flag i))

let external_compiler =
  let env = Term.env_info "RAMEN_USE_EMBEDDED_COMPILER" in
  let i = Arg.info ~doc:CliInfo.external_compiler
                   ~env [ "use-external-compiler"; "external-compiler" ] in
  Arg.(value (flag i))

let max_simult_compilations =
  let env = Term.env_info "RAMEN_MAX_SIMULT_COMPILATIONS" in
  let i = Arg.info ~doc:CliInfo.max_simult_compilations
                   ~env [ "max-simult-compilations" ;
                          "max-simultaneous-compilations" ] in
  let def = Atomic.Counter.get RamenOCamlCompiler.max_simult_compilations in
  Arg.(value (opt int def i))

let smt_solver =
  let env = Term.env_info "RAMEN_SMT_SOLVER" in
  let i = Arg.info ~doc:CliInfo.smt_solver
                   ~env [ "smt-solver" ; "solver" ] in
  Arg.(value (opt string !RamenSmt.solver i))

let fail_for_good =
  let i = Arg.info ~doc:CliInfo.fail_for_good
                   [ "fail-for-good" ] in
  Arg.(value (flag i))

let kill_at_exit =
  let env = Term.env_info "RAMEN_KILL_AT_EXIT" in
  let i = Arg.info ~doc:CliInfo.kill_at_exit
                   ~env [ "kill-at-exit" ] in
  Arg.(value (flag i))

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
      $ kill_at_exit),
    info ~doc:CliInfo.supervisor "supervisor")

(*
 * Delete old or unused files
 *)

let loop =
  let i = Arg.info ~doc:CliInfo.loop
                   ["loop"] in
  Arg.(value (opt (some float) ~vopt:None (Some 0.) i))

let dry_run =
  let i = Arg.info ~doc:CliInfo.dry_run
                   [ "dry-run" ] in
  Arg.(value (flag i))

let del_ratio =
  let i = Arg.info ~doc:CliInfo.del_ratio
                   [ "del-ratio" ] in
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
  let i = Arg.info ~doc:CliInfo.compress_older
                   [ "compress-older" ] in
  Arg.(value (opt (duration "TIMEOUT") Default.compress_older i))

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
    info ~doc:CliInfo.gc "gc")

(*
 * Notifications: Start the alerter and send test ones
 *)

let conf_file ?env ?(opt_names=["config"; "c"]) ~doc () =
  let env = Option.map Term.env_info env in
  let i = Arg.info ~doc ?env opt_names in
  Arg.(value (opt (some path) None i))

let max_fpr =
  let env = Term.env_info "ALERTER_MAX_FPR" in
  let i = Arg.info ~doc:CliInfo.max_fpr
                   ~env [ "default-max-fpr"; "max-fpr"; "fpr" ] in
  Arg.(value (opt float Default.max_fpr i))

let alerter =
  Term.(
    (const RamenCliCmd.alerter
      $ copts ~default_username:"_alerter" ()
      $ conf_file ~env:"ALERTER_CONFIG"
                  ~doc:CliInfo.conffile ()
      $ max_fpr
      $ daemonize
      $ to_stdout
      $ to_syslog
      $ prefix_log_with_name),
    info ~doc:CliInfo.alerter "alerter")

let text_pos ~doc ~docv p =
  let i = Arg.info ~doc ~docv [] in
  Arg.(required (pos p (some string) None i))

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
  let i = Arg.info ~doc:CliInfo.param
                   ~docv:"PARAM=VALUE" ["p"; "parameter"] in
  Arg.(value (opt_all text_param [] i))

let notify =
  Term.(
    (const RamenCliCmd.notify
      $ copts ()
      $ text_params
      $ text_pos ~doc:"notification name" ~docv:"NAME" 0),
    info ~doc:CliInfo.notify "notify")

(*
 * Service to forward tuples over the network
 *)

let tunneld_port =
  let i = Arg.info ~doc:CliInfo.tunneld_port [ "p"; "port" ] in
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
    info ~doc:CliInfo.tunneld "tunneld")

(*
 * Config Server
 *)

let confserver_ports =
  let i = Arg.info ~doc:CliInfo.confserver_port [ "p"; "insecure" ]
  and vopt = "127.0.0.1:"^ string_of_int Default.confserver_port in
  Arg.(value (opt_all ~vopt string [] i))

let confserver_ports_sec =
  let i = Arg.info ~doc:CliInfo.confserver_port_sec [ "P"; "secure" ]
  and vopt = string_of_int Default.confserver_port_sec in
  Arg.(value (opt_all ~vopt string [] i))

let server_priv_key_file =
  let i = Arg.info ~doc:CliInfo.server_priv_key [ "K"; "private-key" ] in
  Arg.(value (opt path (N.path "") i))

let server_pub_key_file =
  let i = Arg.info ~doc:CliInfo.server_pub_key [ "k"; "public-key" ] in
  Arg.(value (opt path (N.path "") i))

let no_source_examples =
  let i = Arg.info ~doc:CliInfo.no_source_examples [ "no-examples" ] in
  Arg.(value (flag i))

let archive_total_size =
  let env = Term.env_info "RAMEN_DEFAULT_ARCHIVE_SIZE" in
  let i = Arg.info ~doc:CliInfo.default_archive_total_size
                   ~env [ "default-archive-size" ] in
  Arg.(value (opt int Default.archive_total_size i))

let archive_recall_cost =
  let env = Term.env_info "RAMEN_DEFAULT_ARCHIVE_RECALL_COSE" in
  let i = Arg.info ~doc:CliInfo.default_archive_recall_cost
                   ~env [ "default-archive-recall-cost" ] in
  Arg.(value (opt float Default.archive_recall_cost i))

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
      $ archive_recall_cost),
    info ~doc:CliInfo.confserver "confserver")

let confclient =
  Term.(
    (const RamenCliCmd.confclient
      $ copts ~default_username:"" ()),
    info ~doc:CliInfo.confclient "confclient")

(*
 * User management
 *)

let username =
  let i = Arg.info ~docv:"USER" ~doc:CliInfo.username [] in
  Arg.(required (pos 0 (some string) None i))

let roles =
  let i = Arg.info ~docv:"ROLE" ~doc:CliInfo.role [ "r"; "role" ] in
  let roles =
    RamenSyncUser.Role.[ "admin", Admin ; "user", User ] in
  Arg.(value (opt_all (enum roles) [] i))

let output_file =
  let i = Arg.info ~doc:CliInfo.output_file
                   ~docv:"FILE" [ "o" ] in
  Arg.(value (opt (some path) None i))

let useradd =
  Term.(
    (const RamenSyncUsers.add
      $ persist_dir
      $ output_file
      $ username
      $ roles
      $ server_pub_key_file),
    info ~doc:CliInfo.useradd "useradd")

let userdel =
  Term.(
    (const RamenSyncUsers.del
      $ persist_dir
      $ username),
    info ~doc:CliInfo.userdel "userdel")

let usermod =
  Term.(
    (const RamenSyncUsers.mod_
      $ persist_dir
      $ username
      $ roles),
    info ~doc:CliInfo.usermod "usermod")

(*
 * Examine the ringbuffers
 *)

(* TODO: check that this is actually the name of a ringbuffer file *)
let rb_file =
  let i = Arg.info ~doc:CliInfo.rb_file
                   ~docv:"FILE" [] in
  Arg.(required (pos 0 (some path) None i))

let num_tuples =
  let i = Arg.info ~doc:CliInfo.num_tuples
                   [ "n"; "num-entries" ] in
  Arg.(value (opt int 1 i))

let dequeue =
  Term.(
    (const RingBufCmd.dequeue
      $ copts ()
      $ rb_file
      $ num_tuples),
    info ~doc:CliInfo.dequeue "dequeue")

let max_bytes =
  let i = Arg.info ~doc:CliInfo.max_bytes
                   [ "s" ; "max-bytes" ] in
  Arg.(value (opt int 64 i))

let rb_files =
  let i = Arg.info ~doc:CliInfo.rb_files
                   ~docv:"FILE" [] in
  Arg.(non_empty (pos_all path [] i))

let summary =
  Term.(
    (const RingBufCmd.summary
      $ copts ()
      $ max_bytes
      $ rb_files),
    info ~doc:CliInfo.summary "ringbuf-summary")

let repair =
  Term.(
    (const RingBufCmd.repair
      $ copts ()
      $ rb_files),
    info ~doc:CliInfo.repair "repair-ringbuf")

let start_word =
  let i = Arg.info ~doc:CliInfo.start_word
                   [ "f" ; "start" ] in
  Arg.(required (opt (some int) None i))

let stop_word =
  let i = Arg.info ~doc:CliInfo.stop_word
                   [ "t" ; "stop" ] in
  Arg.(required (opt (some int) None i))

let dump =
  Term.(
    (const RingBufCmd.dump
      $ copts ()
      $ start_word
      $ stop_word
      $ rb_file),
    info ~doc:CliInfo.dump "dump-ringbuf")

let pattern =
  let i = Arg.info ~doc:CliInfo.pattern
                   ~docv:"PATTERN" [] in
  Arg.(value (pos 0 glob Globs.all i))

let no_abbrev =
  let env = Term.env_info "RAMEN_NO_ABBREVIATION" in
  let i = Arg.info ~doc:CliInfo.no_abbrev
                   ~env [ "no-abbreviation" ] in
  Arg.(value (flag i))

let show_all doc =
  let env = Term.env_info "RAMEN_SHOW_ALL" in
  let i = Arg.info ~doc ~env [ "show-all" ; "all" ; "a" ] in
  Arg.(value (flag i))

let as_tree =
  let i = Arg.info ~doc:CliInfo.as_tree
                   [ "as-tree" ; "tree" ] in
  Arg.(value (flag i))

let pretty =
  let i = Arg.info ~doc:CliInfo.pretty
                   [ "pretty" ] in
  Arg.(value (flag i))

let with_header =
  let i = Arg.info ~doc:CliInfo.with_header
                   [ "h"; "with-header"; "header" ] in
  Arg.(value (opt ~vopt:Default.header_every int 0 i))

let sort_col =
  let i = Arg.info ~doc:CliInfo.sort_col
                   ~docv:"COL" [ "sort" ; "s" ] in
  Arg.(value (opt string "1" i))

let top =
  let i = Arg.info ~doc:CliInfo.top
                   ~docv:"N" [ "top" ; "t" ] in
  Arg.(value (opt (some int) None i))

let links =
  Term.(
    (const RingBufCmd.links
      $ copts ()  (* TODO: confserver version *)
      $ no_abbrev
      $ show_all CliInfo.show_all_links
      $ as_tree
      $ pretty
      $ with_header
      $ sort_col
      $ top
      $ pattern),
    info ~doc:CliInfo.links "links")

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
        let what = "value of command line parameter "^ pname in
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
  let i = Arg.info ~doc:CliInfo.param
                   ~docv:"PARAM=VALUE" ["p"; "parameter"] in
  Arg.(value (opt_all assignment [] i))

let program_globs =
  let i = Arg.info ~doc:CliInfo.program_globs
                   ~docv:"PROGRAM" [] in
  Arg.(non_empty (pos_all glob [] i))

let lib_path =
  let env = Term.env_info "RAMEN_PATH" in
  let i = Arg.info ~doc:CliInfo.lib_path
                   ~env [ "lib-path" ; "L" ] in
  Arg.(value (opt_all path [] i))

let src_files =
  let i = Arg.info ~doc:CliInfo.src_files
                   ~docv:"FILE" [] in
  Arg.(non_empty (pos_all path [] i))

let program =
  let parse s = Pervasives.Ok (N.program s)
  and print fmt (p : N.program) =
    Format.fprintf fmt "%s" (p :> string)
  in
  Arg.conv ~docv:"PROGRAM" (parse, print)

let as_ =
  let i = Arg.info ~doc:CliInfo.program_name
                   ~docv:"NAME" [ "as" ] in
  Arg.(value (opt (some program) None i))

let replace =
  let i = Arg.info ~doc:CliInfo.replace
                   [ "replace" ; "r" ] in
  Arg.(value (flag i))

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
    info ~doc:CliInfo.compile "compile")

let compserver =
  Term.(
    (const RamenCliCmd.compserver
      $ copts ~default_username:"_compserver" ()
      $ daemonize
      $ to_stdout
      $ to_syslog
      $ prefix_log_with_name
      $ external_compiler
      $ max_simult_compilations
      $ smt_solver),
    info ~doc:CliInfo.compserver "compserver")

let report_period =
  let env = Term.env_info "RAMEN_REPORT_PERIOD" in
  let i = Arg.info ~doc:CliInfo.report_period
                   ~env ["report-period"] in
  Arg.(value (opt float Default.report_period i))

let src_file =
  let i = Arg.info ~doc:CliInfo.src_file
                   ~docv:"FILE" [] in
  Arg.(required (pos 0 (some path) None i))

let on_site =
  let i = Arg.info ~doc:CliInfo.on_site [ "on-site" ] in
  Arg.(value (opt glob Globs.all i))

let program_name =
  let i = Arg.info ~doc:CliInfo.program_name
                   ~docv:"PROGRAM" [] in
  Arg.(required (pos 0 (some program) None i))

let cwd =
  let i = Arg.info ~doc:CliInfo.cwd [ "cwd"; "working-dir" ] in
  Arg.(value (opt (some path) None i))

let run =
  Term.(
    (const RamenCliCmd.run
      $ copts ~default_username:"" ()
      $ params
      $ report_period
      $ program_name
      $ on_site
      $ cwd),
    info ~doc:CliInfo.run "run")

let purge =
  let i = Arg.info ~doc:CliInfo.purge
                   [ "purge" ] in
  Arg.(value (flag i))

let kill =
  Term.(
    (const RamenCliCmd.kill
      $ copts ~default_username:"" ()
      $ program_globs
      $ purge),
    info ~doc:CliInfo.kill "kill")

let func_name =
  let parse s = Pervasives.Ok (N.func s)
  and print fmt (p : N.func) =
    Format.fprintf fmt "%s" (p :> string)
  in
  Arg.conv ~docv:"FUNCTION" (parse, print)

let opt_function_name p =
  let i = Arg.info ~doc:CliInfo.function_name
                   ~docv:"OPERATION" [] in
  Arg.(value (pos p (some func_name) None i))

let bin_file =
  let i = Arg.info ~doc:CliInfo.bin_file
                   ~docv:"FILE" [] in
  Arg.(required (pos 0 (some path) None i))


let info =
  Term.(
    (const RamenCliCmd.info
      $ copts ~default_username:"" ()
      $ params
      $ bin_file
      $ opt_function_name 1),
    info ~doc:CliInfo.info "info")

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
    info ~doc:CliInfo.choreographer "choreographer")

(*
 * `ramen replayer`
 *)

let replay_service =
  Term.(
    (const RamenCliCmd.replay_service
      $ copts ~default_username:"_replay_service" ()
      $ daemonize
      $ to_stdout
      $ to_syslog
      $ prefix_log_with_name),
    info ~doc:CliInfo.replay_service "replayer")

(*
 * Display the output of any operation
 *)

let csv_separator =
  let env = Term.env_info "RAMEN_CSV_SEPARATOR" in
  let i = Arg.info ~doc:CliInfo.csv_separator
                   ~env [ "separator" ] in
  Arg.(value (opt string "," i))

let csv_null =
  let env = Term.env_info "RAMEN_CSV_NULL" in
  let i = Arg.info ~doc:CliInfo.csv_null
                   ~env [ "null" ] in
  Arg.(value (opt string "<NULL>" i))

let csv_raw =
  let i = Arg.info ~doc:CliInfo.csv_raw
                   [ "raw" ] in
  Arg.(value (flag i))

let last =
  let i = Arg.info ~doc:CliInfo.last
                   [ "l"; "last" ] in
  Arg.(value (opt (some int) None i))

let next =
  let i = Arg.info ~doc:CliInfo.next
                   [ "n"; "next" ] in
  Arg.(value (opt (some int) None i))

let follow =
  let i = Arg.info ~doc:CliInfo.follow
                   [ "f"; "follow" ] in
  Arg.(value (flag i))

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
  let i = Arg.info ~doc:CliInfo.where
                   ~docv:"FIELD op VALUE" ["w"; "where"] in
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
  let i = Arg.info ~doc:CliInfo.since
                   ~docv:"SINCE" ["since"] in
  Arg.(value (opt (some time) None i))

let until =
  let i = Arg.info ~doc:CliInfo.until
                   ~docv:"UNTIL" ["until"] in
  Arg.(value (opt (some time) None i))

let with_event_time =
  let i = Arg.info ~doc:CliInfo.with_event_time
                   ["with-event-times"; "with-times"; "event-times"; "t"] in
  Arg.(value (flag i))

let timeout =
  let i = Arg.info ~doc:CliInfo.timeout
                   [ "timeout" ] in
  Arg.(value (opt (duration "TIMEOUT") 300. i))

let with_units =
  let i = Arg.info ~doc:CliInfo.with_units
                   [ "u"; "with-units"; "units" ] in
  Arg.(value (flag i))

let flush =
  let i = Arg.info ~doc:CliInfo.flush
                   [ "flush" ] in
  Arg.(value (flag i))

let func_name_or_code =
  let i = Arg.info ~doc:CliInfo.func_name_or_code
                   ~docv:"OPERATION" [] in
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
    info ~doc:CliInfo.tail "tail")

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
  let i = Arg.info ~doc:CliInfo.function_name
                   ~docv:"OPERATION" [] in
  Arg.(required (pos p (some fq_name) None i))

let field =
  let parse s = Pervasives.Ok (N.field s)
  and print fmt (s : N.field)  =
    Format.fprintf fmt "%s" (s :> string)
  in
  Arg.conv ~docv:"FIELD" (parse, print)

let data_fields ~mandatory p =
  let i = Arg.info ~doc:CliInfo.data_fields
                   ~docv:"FIELD" [] in
  Arg.((if mandatory then non_empty else value) (pos_right (p-1) field [] i))

let since_mandatory =
  let i = Arg.info ~doc:CliInfo.since
                   ~docv:"SINCE" ["since"] in
  Arg.(required (opt (some time) None i))

let via_confserver =
  let i = Arg.info ~doc:CliInfo.via
                   ~docv:"file|confserver" ["via"] in
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
      $ via_confserver),
    info ~doc:CliInfo.replay "replay")


(*
 * Timeseries
 *)

let num_points =
  let i = Arg.info ~doc:CliInfo.num_points
                   ~docv:"POINTS" ["n"; "num-points"] in
  Arg.(value (opt int 0 i))

let time_step =
  let i = Arg.info ~doc:CliInfo.time_step
                   ~docv:"DURATION" ["time-step"] in
  Arg.(value (opt float 0. i))

let consolidation =
  let i = Arg.info ~doc:CliInfo.consolidation
                   ~docv:"min|max|avg|sum" ["consolidation"] in
  let cons_funcs =
    let p x = x, x in
    [ p "min" ; p "max" ; p "avg" ; p "sum" ; p "count" ] in
  Arg.(value (opt (enum cons_funcs) "avg" i))

let bucket_time =
  let i = Arg.info ~doc:CliInfo.bucket_time
                   ~docv:"begin|middle|end" ["bucket-time"] in
  let open RamenTimeseries in
  let bucket_times =
    [ "begin", Begin ; "middle", Middle ; "end", End ] in
  Arg.(value (opt (enum bucket_times) Begin i))

let factors =
  let i = Arg.info ~doc:CliInfo.factors
                   ~docv:"FIELD" ["f"; "factor"] in
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
    info ~doc:CliInfo.timeseries "timeseries")

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
      $ show_all CliInfo.show_all_workers),
    info ~doc:CliInfo.ps "ps")

let profile =
  Term.(
    (const RamenCliCmd.profile
      $ copts ~default_username:"" ()
      $ pretty
      $ with_header
      $ sort_col
      $ top
      $ pattern
      $ show_all CliInfo.show_all_workers),
    info ~doc:CliInfo.profile "_profile")

(*
 * Start the HTTP daemon (graphite impersonator)
 *)

let server_url def =
  let env = Term.env_info "RAMEN_URL" in
  let i = Arg.info ~doc:CliInfo.server_url
                   ~env [ "url" ] in
  Arg.(value (opt string def i))

let graphite =
  let i = Arg.info ~doc:CliInfo.graphite [ "graphite" ] in
  Arg.(value (opt ~vopt:(Some "") (some string) None i))

let api =
  let i = Arg.info ~doc:CliInfo.api [ "api-v1" ] in
  Arg.(value (opt ~vopt:(Some "") (some string) None i))

let fault_injection_rate =
  let env = Term.env_info "RAMEN_FAULT_INJECTION_RATE" in
  let i = Arg.info ~doc:CliInfo.fault_injection_rate
                   ~env [ "fault-injection-rate" ] in
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
      $ graphite
      $ external_compiler
      $ max_simult_compilations
      $ smt_solver),
    info ~doc:CliInfo.httpd "httpd")

let for_render =
  let i = Arg.info ~doc:"exact match as for the render query"
                   [ "for-render" ] in
  Arg.(value (flag i))

let query =
  let i = Arg.info ~doc:"test graphite query expansion"
                   ~docv:"QUERY" [] in
  Arg.(value (pos 0 string "*" i))

let expand =
  Term.(
    (const RamenCliCmd.graphite_expand
      $ copts ()
      $ for_render
      $ since
      $ until
      $ query),
    info ~doc:"test graphite query expansion" "_expand")

(*
 * Tests
 *)

let test_file =
  let i = Arg.info ~doc:CliInfo.test_file
                   ~docv:"file.test" [] in
  Arg.(value (pos 0 path (N.path "") i))

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
    info ~doc:CliInfo.test "test")

(*
 * Allocate disk space to workers archives
 *)

let update_stats =
  let i = Arg.info ~doc:CliInfo.update_stats
                   [ "stats" ] in
  Arg.(value (flag i))

let update_allocs =
  let i = Arg.info ~doc:CliInfo.update_allocs
                   [ "allocs" ] in
  Arg.(value (flag i))

let reconf_workers =
  let i = Arg.info ~doc:CliInfo.reconf_workers
                   [ "reconf-workers" ] in
  Arg.(value (flag i))

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
    info ~doc:CliInfo.archivist "archivist")

(*
 * Experiments
 *)

let variants =
  Term.(
    (const RamenCliCmd.variants
      $ copts ()),
    info ~doc:CliInfo.variants "variants")

(*
 * Display internal instrumentation. Also an option of various subsystems.
 *)

let stats =
  Term.(
    (const RamenCliCmd.stats
      $ copts ()),
    info ~doc:CliInfo.stats "stats")

(*
 * Autocompletion
 *)

let command =
  let i = Arg.info ~doc:CliInfo.command
                   ~docv:"STRING" [] in
  Arg.(value (pos 0 string "" i))

let autocomplete =
  Term.(
    (const RamenCompletion.complete
      $ command),
    info ~doc:CliInfo.autocomplete "_completion")

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
  with Exit -> exit 0
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
        confserver ; confclient ; compserver ; choreographer ; replay_service ;
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
        test ;
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
