(* This is where the command line arguments are processed.
 * This then mostly transfer control to various specialized functions
 * of the RamenCliCmd module.
 *)
open Cmdliner
open Batteries
open RamenHelpers

(*
 * Common options
 *)

let copts =
  let docs = Manpage.s_common_options in
  let debug =
    let env = Term.env_info "RAMEN_DEBUG" in
    let i = Arg.info ~doc:RamenConsts.CliInfo.debug
                     ~docs ~env [ "d"; "debug" ] in
    Arg.(value (flag i))
  and quiet =
    let env = Term.env_info "RAMEN_QUIET" in
    let i = Arg.info ~doc:RamenConsts.CliInfo.quiet
                     ~docs ~env [ "q"; "quiet" ] in
    Arg.(value (flag i))
  and persist_dir =
    let env = Term.env_info "RAMEN_PERSIST_DIR" in
    let i = Arg.info ~doc:RamenConsts.CliInfo.persist_dir
                     ~docs ~env [ "persist-dir" ] in
    Arg.(value (opt string RamenConsts.Default.persist_dir i))
  and rand_seed =
    let env = Term.env_info "RAMEN_RANDOM_SEED" in
    let i = Arg.info ~doc:RamenConsts.CliInfo.rand_seed
                     ~docs ~env [ "seed"; "rand-seed" ] in
    Arg.(value (opt (some int) None i))
  and keep_temp_files =
    let env = Term.env_info "RAMEN_KEEP_TEMP_FILES" in
    let i = Arg.info ~doc:RamenConsts.CliInfo.keep_temp_files
                     ~docs ~env [ "S" ; "keep-temp-files" ] in
    Arg.(value (flag i))
  and forced_variants =
    let env = Term.env_info "RAMEN_VARIANTS" in
    let i = Arg.info ~doc:RamenConsts.CliInfo.variant
                     ~docs ~env [ "variant" ] in
    Arg.(value (opt_all string [] i))
  and initial_export_duration =
    let env = Term.env_info "RAMEN_INITIAL_EXPORT" in
    let i = Arg.info ~doc:RamenConsts.CliInfo.initial_export_duration
                     ~docs ~env [ "initial-export-duration" ] in
    Arg.(value (opt float 900. i))
  in
  Term.(const RamenCliCmd.make_copts
    $ debug
    $ quiet
    $ persist_dir
    $ rand_seed
    $ keep_temp_files
    $ forced_variants
    $ initial_export_duration)

(*
 * Start the process supervisor
 *)

let daemonize =
  let env = Term.env_info "RAMEN_DAEMONIZE" in
  let i = Arg.info ~doc:RamenConsts.CliInfo.daemonize
                   ~env ["daemonize"] in
  Arg.(value (flag i))

let to_stdout =
  let env = Term.env_info "RAMEN_LOG_TO_STDERR" in
  let i = Arg.info ~doc:RamenConsts.CliInfo.to_stdout
                   ~env [ "to-stderr"; "stderr" ;
                          "to-stdout"; "stdout" ] in
  Arg.(value (flag i))

let to_syslog =
  let env = Term.env_info "RAMEN-LOG-SYSLOG" in
     let i = Arg.info ~doc:RamenConsts.CliInfo.to_syslog
                      ~env [ "to-syslog" ; "syslog" ] in
  Arg.(value (flag i))

let autoreload =
  let env = Term.env_info "RAMEN_AUTORELOAD" in
  let i = Arg.info ~doc:RamenConsts.CliInfo.autoreload
                   ~env ["autoreload"] in
  Arg.(value (opt ~vopt:5. float 0. i))

let external_compiler =
  let env = Term.env_info "RAMEN_USE_EMBEDDED_COMPILER" in
  let i = Arg.info ~doc:RamenConsts.CliInfo.external_compiler
                   ~env [ "use-external-compiler"; "external-compiler" ] in
  Arg.(value (flag i))

let bundle_dir =
  let env = Term.env_info "RAMEN_BUNDLE_DIR" in
  let i = Arg.info ~doc:RamenConsts.CliInfo.bundle_dir
                   ~env [ "bundle-dir" ] in
  Arg.(value (opt string RamenCompilConfig.default_bundle_dir i))

let max_simult_compilations =
  let env = Term.env_info "RAMEN_MAX_SIMULT_COMPILATIONS" in
  let i = Arg.info ~doc:RamenConsts.CliInfo.max_simult_compilations
                   ~env [ "max-simult-compilations" ;
                          "max-simultaneous-compilations" ] in
  let def = Atomic.Counter.get RamenOCamlCompiler.max_simult_compilations in
  Arg.(value (opt int def i))

let smt_solver =
  let env = Term.env_info "RAMEN_SMT_SOLVER" in
  let i = Arg.info ~doc:RamenConsts.CliInfo.smt_solver
                   ~env [ "smt-solver" ; "solver" ] in
  Arg.(value (opt string !RamenSmtTyping.smt_solver i))

let fail_for_good =
  let i = Arg.info ~doc:RamenConsts.CliInfo.fail_for_good
                   [ "fail-for-good" ] in
  Arg.(value (flag i))

let supervisor =
  Term.(
    (const RamenCliCmd.supervisor
      $ copts
      $ daemonize
      $ to_stdout
      $ to_syslog
      $ autoreload
      $ external_compiler
      $ bundle_dir
      $ max_simult_compilations
      $ smt_solver
      $ fail_for_good),
    info ~doc:RamenConsts.CliInfo.supervisor "supervisor")

(*
 * Delete old or unused files
 *)

let max_archives =
  let env = Term.env_info "RAMEN_MAX_HISTORY_ARCHIVES" in
  let i = Arg.info ~doc:RamenConsts.CliInfo.max_archives
                   ~env ["max-archives"] in
  Arg.(value (opt int 20 i))

let loop =
  let i = Arg.info ~doc:RamenConsts.CliInfo.loop
                   ["loop"] in
  Arg.(value (opt int ~vopt:3600 0 i))

let dry_run =
  let i = Arg.info ~doc:RamenConsts.CliInfo.dry_run
                   [ "dry-run" ] in
  Arg.(value (flag i))

let gc =
  Term.(
    (const RamenCliCmd.gc
      $ copts
      $ max_archives
      $ dry_run
      $ loop
      $ daemonize
      $ to_stdout
      $ to_syslog),
    info ~doc:RamenConsts.CliInfo.gc "gc")

(*
 * Notifications: Start the notifier and send test ones
 *)

let conf_file ~default ?env ?(opt_names=["config"; "c"]) ~doc () =
  let env = Option.map Term.env_info env in
  let i = Arg.info ~doc ?env opt_names in
  Arg.(value (opt string default i))

let max_fpr =
  let env = Term.env_info "NOTIFIER_MAX_FPR" in
  let i = Arg.info ~doc:RamenConsts.CliInfo.max_fpr
                   ~env [ "default-max-fpr"; "max-fpr"; "fpr" ] in
  Arg.(value (opt float RamenConsts.Default.max_fpr i))

let notifier =
  Term.(
    (const RamenCliCmd.notifier
      $ copts
      $ conf_file ~env:"NOTIFIER_CONFIG"
                  ~doc:RamenConsts.CliInfo.conffile ()
                  ~default:RamenConsts.Default.notif_conf_file
      $ max_fpr
      $ daemonize
      $ to_stdout
      $ to_syslog),
    info ~doc:RamenConsts.CliInfo.notifier "notifier")

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
  let i = Arg.info ~doc:RamenConsts.CliInfo.param
                   ~docv:"PARAM=VALUE" ["p"; "parameter"] in
  Arg.(value (opt_all text_param [] i))

let notify =
  Term.(
    (const RamenCliCmd.notify
      $ copts
      $ text_params
      $ text_pos ~doc:"notification name" ~docv:"NAME" 0),
    info ~doc:RamenConsts.CliInfo.notify "notify")

(*
 * Examine the ringbuffers
 *)

(* TODO: check that this is actually the name of a ringbuffer file *)
let rb_file =
  let i = Arg.info ~doc:RamenConsts.CliInfo.rb_file
                   ~docv:"FILE" [] in
  Arg.(required (pos 0 (some string) None i))

let num_tuples =
  let i = Arg.info ~doc:RamenConsts.CliInfo.num_tuples
                   [ "n"; "num-entries" ] in
  Arg.(value (opt int 1 i))

let dequeue =
  Term.(
    (const RingBufCmd.dequeue
      $ copts
      $ rb_file
      $ num_tuples),
    info ~doc:RamenConsts.CliInfo.dequeue "dequeue")

let rb_files =
  let i = Arg.info ~doc:RamenConsts.CliInfo.rb_files
                   ~docv:"FILE" [] in
  Arg.(non_empty (pos_all string [] i))

let summary =
  Term.(
    (const RingBufCmd.summary
      $ copts
      $ rb_files),
    info ~doc:RamenConsts.CliInfo.summary "ringbuf-summary")

let repair =
  Term.(
    (const RingBufCmd.repair
      $ copts
      $ rb_files),
    info ~doc:RamenConsts.CliInfo.repair "repair-ringbuf")

let pattern =
  let i = Arg.info ~doc:RamenConsts.CliInfo.pattern
                   ~docv:"PATTERN" [] in
  Arg.(value (pos 0 string "*" i))

let no_abbrev =
  let env = Term.env_info "RAMEN_NO_ABBREVIATION" in
  let i = Arg.info ~doc:RamenConsts.CliInfo.no_abbrev
                   ~env [ "no-abbreviation" ] in
  Arg.(value (flag i))

let show_all =
  let env = Term.env_info "RAMEN_SHOW_ALL" in
  let i = Arg.info ~doc:RamenConsts.CliInfo.show_all
                   ~env [ "show-all" ; "all" ; "a" ] in
  Arg.(value (flag i))

let as_tree =
  let i = Arg.info ~doc:RamenConsts.CliInfo.as_tree
                   [ "as-tree" ; "tree" ] in
  Arg.(value (flag i))

let pretty =
  let i = Arg.info ~doc:RamenConsts.CliInfo.pretty
                   [ "pretty" ] in
  Arg.(value (flag i))

let with_header =
  let i = Arg.info ~doc:RamenConsts.CliInfo.with_header
                   [ "h"; "with-header"; "header" ] in
  Arg.(value (flag i))

let sort_col =
  let i = Arg.info ~doc:RamenConsts.CliInfo.sort_col
                   ~docv:"COL" [ "sort" ; "s" ] in
  Arg.(value (opt int 1 i))

let top =
  let i = Arg.info ~doc:RamenConsts.CliInfo.top
                   ~docv:"N" [ "top" ; "t" ] in
  Arg.(value (opt (some int) None i))

let links =
  Term.(
    (const RingBufCmd.links
      $ copts
      $ no_abbrev
      $ show_all
      $ as_tree
      $ pretty
      $ with_header
      $ sort_col
      $ top
      $ pattern),
    info ~doc:RamenConsts.CliInfo.links "links")

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
        (match RamenTypes.of_string ~what pval with
        | Result.Ok v -> Pervasives.Ok (pname, v)
        | Result.Bad e -> Pervasives.Error (`Msg e))
  and print fmt (pname, pval) =
    Format.fprintf fmt "%s=%s" pname (RamenTypes.to_string pval)
  in
  Arg.conv ~docv:"IDENTIFIER=VALUE" (parse, print)

(* Note: parameter with same name in different functions will all take
 * their value from this. Easy to add a prefix with function name when
 * it causes troubles. *)
let params =
  let i = Arg.info ~doc:RamenConsts.CliInfo.param
                   ~docv:"PARAM=VALUE" ["p"; "parameter"] in
  Arg.(value (opt_all assignment [] i))

let program_globs =
  let i = Arg.info ~doc:RamenConsts.CliInfo.program_globs
                   ~docv:"PROGRAM" [] in
  Arg.(non_empty (pos_all string [] i))

let root_path =
  let env = Term.env_info "RAMEN_ROOT" in
  let i = Arg.info ~doc:RamenConsts.CliInfo.root_path
                   ~env [ "root-path" ] in
  Arg.(value (opt string "." i))

let src_files =
  let i = Arg.info ~doc:RamenConsts.CliInfo.src_files
                   ~docv:"FILE" [] in
  Arg.(non_empty (pos_all string [] i))

let bin_file =
  let i = Arg.info ~doc:RamenConsts.CliInfo.bin_file
                   ~docv:"FILE" [] in
  Arg.(required (pos 0 (some string) None i))

let program =
  let parse s = Pervasives.Ok (RamenName.program_of_string s)
  and print fmt p =
    Format.fprintf fmt "%s" (RamenName.string_of_program p)
  in
  Arg.conv ~docv:"PROGRAM" (parse, print)

let output_file =
  let i = Arg.info ~doc:RamenConsts.CliInfo.output_file
                   ~docv:"FILE" [ "o" ] in
  Arg.(value (opt (some string) None i))

let as_ =
  let i = Arg.info ~doc:RamenConsts.CliInfo.program_name
                   ~docv:"NAME" [ "as" ] in
  Arg.(value (opt (some program) None i))

let parents_from_rc =
  let i = Arg.info ~doc:RamenConsts.CliInfo.parents_from_rc
                   [ "parents-from-rc" ;
                     "parents-from-running-configuration" ] in
  Arg.(value (flag i))

let compile =
  Term.(
    (const RamenCliCmd.compile
      $ copts
      $ root_path
      $ external_compiler
      $ bundle_dir
      $ max_simult_compilations
      $ smt_solver
      $ src_files
      $ output_file
      $ as_
      $ parents_from_rc),
    info ~doc:RamenConsts.CliInfo.compile "compile")

let replace =
  let i = Arg.info ~doc:RamenConsts.CliInfo.replace
                   [ "replace" ; "r" ] in
  Arg.(value (flag i))

let report_period =
  let env = Term.env_info "RAMEN_REPORT_PERIOD" in
  let i = Arg.info ~doc:RamenConsts.CliInfo.report_period
                   ~env ["report-period"] in
  Arg.(value (opt float RamenConsts.Default.report_period i))

let src_file =
  let i = Arg.info ~doc:RamenConsts.CliInfo.src_file
                   [ "src-file" ; "source-file" ] in
  Arg.(value (opt (some string) None i))

let run =
  Term.(
    (const RamenCliCmd.run
      $ copts
      $ params
      $ replace
      $ report_period
      $ as_
      $ src_file
      $ bin_file),
    info ~doc:RamenConsts.CliInfo.run "run")

let purge =
  let i = Arg.info ~doc:RamenConsts.CliInfo.purge
                   [ "purge" ] in
  Arg.(value (flag i))

let kill =
  Term.(
    (const RamenCliCmd.kill
      $ copts
      $ program_globs
      $ purge),
    info ~doc:RamenConsts.CliInfo.kill "kill")

(*
 * Display the output of any operation
 *)

let csv_separator =
  let env = Term.env_info "RAMEN_CSV_SEPARATOR" in
  let i = Arg.info ~doc:RamenConsts.CliInfo.csv_separator
                   ~env [ "separator" ] in
  Arg.(value (opt string "," i))

let csv_null =
  let env = Term.env_info "RAMEN_CSV_NULL" in
  let i = Arg.info ~doc:RamenConsts.CliInfo.csv_null
                   ~env [ "null" ] in
  Arg.(value (opt string "<NULL>" i))

let csv_raw =
  let i = Arg.info ~doc:RamenConsts.CliInfo.csv_raw
                   [ "raw" ] in
  Arg.(value (flag i))

let last =
  let i = Arg.info ~doc:RamenConsts.CliInfo.last
                   [ "n"; "last" ] in
  Arg.(value (opt (some int) None i))

let continuous =
  let i = Arg.info ~doc:RamenConsts.CliInfo.continuous
                   [ "f"; "continuous" ] in
  Arg.(value (flag i))

let min_seq =
  let i = Arg.info ~doc:RamenConsts.CliInfo.min_seq
                   ["min-seqnum"] in
  Arg.(value (opt (some int) None i))

let max_seq =
  let i = Arg.info ~doc:RamenConsts.CliInfo.max_seq
                   ["max-seqnum"] in
  Arg.(value (opt (some int) None i))

let filter =
  (* Longer first: *)
  let operators = [ ">="; "<="; "!="; "="; "<>"; "<"; ">"; " in" ] in
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
        (match RamenTypes.of_string ~what pval with
        | Result.Ok v -> Pervasives.Ok (pname, op, v)
        | Result.Bad e -> Pervasives.Error (`Msg e))
  and print fmt (pname, op, pval) =
    Format.fprintf fmt "%s%s%s" pname op (RamenTypes.to_string pval)
  in
  Arg.conv ~docv:"IDENTIFIER[=|>|<|>=|<=]VALUE" (parse, print)

let where =
  let i = Arg.info ~doc:RamenConsts.CliInfo.where
                   ~docv:"FIELD=VALUE" ["w"; "where"] in
  Arg.(value (opt_all filter [] i))

let with_seqnums =
  let i = Arg.info ~doc:RamenConsts.CliInfo.with_seqnums
                   ["with-seqnums"; "seq"; "s"] in
  Arg.(value (flag i))

let with_event_time =
  let i = Arg.info ~doc:RamenConsts.CliInfo.with_event_time
                   ["with-event-times"; "with-times"; "event-times"; "t"] in
  Arg.(value (flag i))

let fq_name =
  let parse s = Pervasives.Ok (RamenName.fq_of_string s)
  and print fmt p =
    Format.fprintf fmt "%s" (RamenName.string_of_fq p)
  in
  Arg.conv ~docv:"FUNCTION" (parse, print)

(* TODO: returns directly the program and function names to spare some
 * calls to RamenName.fq_parse *)
let func_name p =
  let i = Arg.info ~doc:RamenConsts.CliInfo.func_name
                   ~docv:"OPERATION" [] in
  Arg.(required (pos p (some fq_name) None i))

let duration =
  let i = Arg.info ~doc:RamenConsts.CliInfo.duration
                   ["timeout"] in
  Arg.(value (opt float 300. i))

let with_units =
  let i = Arg.info ~doc:RamenConsts.CliInfo.with_units
                   [ "u"; "with-units"; "units" ] in
  Arg.(value (flag i))

let tail =
  Term.(
    (const RamenCliCmd.tail
      $ copts
      $ func_name 0
      $ with_header
      $ with_units
      $ csv_separator
      $ csv_null
      $ csv_raw
      $ last
      $ min_seq
      $ max_seq
      $ continuous
      $ where
      $ with_seqnums
      $ with_event_time
      $ duration
      $ pretty),
    info ~doc:RamenConsts.CliInfo.tail "tail")

(*
 * Timeseries
 *)

let since =
  let i = Arg.info ~doc:RamenConsts.CliInfo.since
                   ~docv:"SINCE" ["since"] in
  Arg.(value (opt (some float) None i))

let until =
  let i = Arg.info ~doc:RamenConsts.CliInfo.until
                   ~docv:"UNTIL" ["until"] in
  Arg.(value (opt float (Unix.gettimeofday ()) i))

let num_points =
  let i = Arg.info ~doc:RamenConsts.CliInfo.num_points
                   ~docv:"POINTS" ["n"; "num-points"] in
  Arg.(value (opt int 0 i))

let time_step =
  let i = Arg.info ~doc:RamenConsts.CliInfo.time_step
                   ~docv:"DURATION" ["time-step"] in
  Arg.(value (opt float 0. i))

let data_fields p =
  let i = Arg.info ~doc:RamenConsts.CliInfo.data_fields
                   ~docv:"FIELD" [] in
  Arg.(non_empty (pos_right (p-1) string [] i))

let consolidation =
  let i = Arg.info ~doc:RamenConsts.CliInfo.consolidation
                   ~docv:"min|max|avg|sum" ["consolidation"] in
  let cons_func =
    let p x = x, x in
    [ p "min" ; p "max" ; p "avg" ; p "sum" ] in
  Arg.(value (opt (enum cons_func) "avg" i))

let bucket_time =
  let i = Arg.info ~doc:RamenConsts.CliInfo.bucket_time
                   ~docv:"begin|middle|end" ["bucket-time"] in
  let open RamenTimeseries in
  let cons_func =
    [ "begin", Begin ; "middle", Middle ; "end", End ] in
  Arg.(value (opt (enum cons_func) Begin i))

let factors =
  let i = Arg.info ~doc:RamenConsts.CliInfo.factors
                   ~docv:"FIELD" ["f"; "factor"] in
  Arg.(value (opt_all string [] i))

let timeseries =
  Term.(
    (const RamenCliCmd.timeseries
      $ copts
      $ since
      $ until
      $ with_header
      $ where
      $ factors
      $ num_points
      $ time_step
      $ csv_separator
      $ csv_null
      $ func_name 0
      $ data_fields 1
      $ consolidation
      $ bucket_time
      $ duration
      $ pretty),
    info ~doc:RamenConsts.CliInfo.timeseries "timeseries")

(*
 * Time Ranges
 *)

let timerange =
  Term.(
    (const RamenCliCmd.timerange
      $ copts
      $ func_name 0),
    info ~doc:RamenConsts.CliInfo.timerange "timerange")

(*
 * Info
 *)

let short =
  let i = Arg.info ~doc:RamenConsts.CliInfo.short
                   [ "short" ; "p" ] in
  Arg.(value (flag i))

let all =
  let i = Arg.info ~doc:RamenConsts.CliInfo.all
                   [ "show-all" ; "all" ; "a" ] in
  Arg.(value (flag i))

let ps =
  Term.(
    (const RamenCliCmd.ps
      $ copts
      $ short
      $ pretty
      $ with_header
      $ sort_col
      $ top
      $ pattern
      $ all),
    info ~doc:RamenConsts.CliInfo.ps "ps")

(*
 * Start the Graphite impersonator
 *)

let server_url def =
  let env = Term.env_info "RAMEN_URL" in
  let i = Arg.info ~doc:RamenConsts.CliInfo.server_url
                   ~env [ "url" ] in
  Arg.(value (opt string def i))

let graphite =
  let i = Arg.info ~doc:RamenConsts.CliInfo.graphite [ "graphite" ] in
  Arg.(value (opt ~vopt:(Some "") (some string) None i))

let api =
  let i = Arg.info ~doc:RamenConsts.CliInfo.api [ "api-v1" ] in
  Arg.(value (opt ~vopt:(Some "") (some string) None i))

let fault_injection_rate =
  let env = Term.env_info "RAMEN_FAULT_INJECTION_RATE" in
  let i = Arg.info ~doc:RamenConsts.CliInfo.fault_injection_rate
                   ~env [ "fault-injection-rate" ] in
  Arg.(value (opt float RamenConsts.Default.fault_injection_rate i))

let httpd =
  Term.(
    (const RamenCliCmd.httpd
      $ copts
      $ daemonize
      $ to_stdout
      $ to_syslog
      $ fault_injection_rate
      $ server_url "http://127.0.0.1:8080"
      $ api
      $ graphite
      $ external_compiler
      $ bundle_dir
      $ max_simult_compilations
      $ smt_solver),
    info ~doc:RamenConsts.CliInfo.httpd "httpd")

let for_render =
  let i = Arg.info ~doc:"exact match as for the render query"
                   [ "for-render" ] in
  Arg.(value (flag i))

let query =
  let i = Arg.info ~doc:"test graphite query expansion"
                   ~docv:"QUERY" [] in
  Arg.(value (pos 0 string "" i))

let expand =
  Term.(
    (const RamenCliCmd.graphite_expand
      $ copts
      $ for_render
      $ all
      $ query),
    info ~doc:"test graphite query expansion" "_expand")

(*
 * Tests
 *)

let test_file =
  let i = Arg.info ~doc:RamenConsts.CliInfo.test_file
                   ~docv:"file.test" [] in
  Arg.(value (pos 0 string "" i))

let test =
  Term.(
    (const RamenTests.run
      $ copts
      $ server_url ""
      $ api
      $ graphite
      $ external_compiler
      $ bundle_dir
      $ max_simult_compilations
      $ smt_solver
      $ test_file),
    info ~doc:RamenConsts.CliInfo.test "test")

(*
 * Experiments
 *)

let variants =
  Term.(
    (const RamenCliCmd.variants $ copts),
    info ~doc:RamenConsts.CliInfo.variants "variants")

(*
 * Display internal instrumentation. Also an option of various subsystems.
 *)

let stats =
  Term.(
    (const RamenCliCmd.stats $ copts),
    info ~doc:RamenConsts.CliInfo.stats "stats")

(*
 * Autocompletion
 *)

let command =
  let i = Arg.info ~doc:RamenConsts.CliInfo.command
                   ~docv:"STRING" [] in
  Arg.(value (pos 0 string "" i))

let autocomplete =
  Term.(
    (const RamenCompletion.complete
      $ command),
    info ~doc:RamenConsts.CliInfo.autocomplete "_completion")

(*
 * Command line evaluation
 *)

let default =
  let sdocs = Manpage.s_common_options in
  let doc = "Ramen Stream Processor" in
  let version = RamenVersions.release_tag in
  Term.((ret (const (`Help (`Pager, None)))),
        info "Ramen" ~version ~doc ~sdocs)

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
        supervisor ; gc ; httpd ; notifier ;
        notify ; compile ; run ; kill ;
        tail ; timeseries ; timerange ; ps ;
        test ; dequeue ; summary ; repair ; links ;
        variants ; stats ; autocomplete ; expand
      ]) with
  | `Error _ -> exit 1
  | `Version | `Help -> exit 0
  | `Ok f ->
      print_exn f
