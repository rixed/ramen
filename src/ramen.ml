(* This is where the command line arguments are processed.
 * This then mostly transfer control to various specialized functions
 * of the RamenCliCmd module.
 *)
open Cmdliner
open Batteries
open RamenHelpers
open RamenConsts
module T = RamenTypes

(*
 * Common options
 *)

let copts =
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
  and persist_dir =
    let env = Term.env_info "RAMEN_DIR" in
    let i = Arg.info ~doc:CliInfo.persist_dir
                     ~docs ~env [ "persist-dir" ] in
    Arg.(value (opt string Default.persist_dir i))
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
  let env = Term.env_info "RAMEN-LOG-SYSLOG" in
     let i = Arg.info ~doc:CliInfo.to_syslog
                      ~env [ "to-syslog" ; "syslog" ] in
  Arg.(value (flag i))

let autoreload =
  let env = Term.env_info "RAMEN_AUTORELOAD" in
  let i = Arg.info ~doc:CliInfo.autoreload
                   ~env ["autoreload"] in
  Arg.(value (opt ~vopt:Default.autoreload float 0. i))

let external_compiler =
  let env = Term.env_info "RAMEN_USE_EMBEDDED_COMPILER" in
  let i = Arg.info ~doc:CliInfo.external_compiler
                   ~env [ "use-external-compiler"; "external-compiler" ] in
  Arg.(value (flag i))

let bundle_dir =
  let env = Term.env_info "RAMEN_LIBS" in
  let i = Arg.info ~doc:CliInfo.bundle_dir
                   ~env [ "bundle-dir" ] in
  Arg.(value (opt string RamenCompilConfig.default_bundle_dir i))

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

let gc =
  Term.(
    (const RamenCliCmd.gc
      $ copts
      $ dry_run
      $ del_ratio
      $ loop
      $ daemonize
      $ to_stdout
      $ to_syslog),
    info ~doc:CliInfo.gc "gc")

(*
 * Notifications: Start the alerter and send test ones
 *)

let conf_file ?env ?(opt_names=["config"; "c"]) ~doc () =
  let env = Option.map Term.env_info env in
  let i = Arg.info ~doc ?env opt_names in
  Arg.(value (opt (some string) None i))

let max_fpr =
  let env = Term.env_info "ALERTER_MAX_FPR" in
  let i = Arg.info ~doc:CliInfo.max_fpr
                   ~env [ "default-max-fpr"; "max-fpr"; "fpr" ] in
  Arg.(value (opt float Default.max_fpr i))

let alerter =
  Term.(
    (const RamenCliCmd.alerter
      $ copts
      $ conf_file ~env:"ALERTER_CONFIG"
                  ~doc:CliInfo.conffile ()
      $ max_fpr
      $ daemonize
      $ to_stdout
      $ to_syslog),
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
      $ copts
      $ text_params
      $ text_pos ~doc:"notification name" ~docv:"NAME" 0),
    info ~doc:CliInfo.notify "notify")

(*
 * Examine the ringbuffers
 *)

(* TODO: check that this is actually the name of a ringbuffer file *)
let rb_file =
  let i = Arg.info ~doc:CliInfo.rb_file
                   ~docv:"FILE" [] in
  Arg.(required (pos 0 (some string) None i))

let num_tuples =
  let i = Arg.info ~doc:CliInfo.num_tuples
                   [ "n"; "num-entries" ] in
  Arg.(value (opt int 1 i))

let dequeue =
  Term.(
    (const RingBufCmd.dequeue
      $ copts
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
  Arg.(non_empty (pos_all string [] i))

let summary =
  Term.(
    (const RingBufCmd.summary
      $ copts
      $ max_bytes
      $ rb_files),
    info ~doc:CliInfo.summary "ringbuf-summary")

let repair =
  Term.(
    (const RingBufCmd.repair
      $ copts
      $ rb_files),
    info ~doc:CliInfo.repair "repair-ringbuf")

let pattern =
  let i = Arg.info ~doc:CliInfo.pattern
                   ~docv:"PATTERN" [] in
  Arg.(value (pos 0 string "*" i))

let no_abbrev =
  let env = Term.env_info "RAMEN_NO_ABBREVIATION" in
  let i = Arg.info ~doc:CliInfo.no_abbrev
                   ~env [ "no-abbreviation" ] in
  Arg.(value (flag i))

let show_all =
  let env = Term.env_info "RAMEN_SHOW_ALL" in
  let i = Arg.info ~doc:CliInfo.show_all
                   ~env [ "show-all" ; "all" ; "a" ] in
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
      $ copts
      $ no_abbrev
      $ show_all
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
        | Result.Ok v -> Pervasives.Ok (RamenName.field_of_string pname, v)
        | Result.Bad e -> Pervasives.Error (`Msg e))
  and print fmt ((pname : RamenName.field), pval) =
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
  Arg.(non_empty (pos_all string [] i))

let lib_path =
  let env = Term.env_info "RAMEN_PATH" in
  let i = Arg.info ~doc:CliInfo.lib_path
                   ~env [ "lib-path" ; "L" ] in
  Arg.(value (opt (some string) None i))

let src_files =
  let i = Arg.info ~doc:CliInfo.src_files
                   ~docv:"FILE" [] in
  Arg.(non_empty (pos_all string [] i))

let bin_file =
  let i = Arg.info ~doc:CliInfo.bin_file
                   ~docv:"FILE" [] in
  Arg.(required (pos 0 (some string) None i))

let program =
  let parse s = Pervasives.Ok (RamenName.program_of_string s)
  and print fmt (p : RamenName.program) =
    Format.fprintf fmt "%s" (p :> string)
  in
  Arg.conv ~docv:"PROGRAM" (parse, print)

let output_file =
  let i = Arg.info ~doc:CliInfo.output_file
                   ~docv:"FILE" [ "o" ] in
  Arg.(value (opt (some string) None i))

let as_ =
  let i = Arg.info ~doc:CliInfo.program_name
                   ~docv:"NAME" [ "as" ] in
  Arg.(value (opt (some program) None i))

let compile =
  Term.(
    (const RamenCliCmd.compile
      $ copts
      $ lib_path
      $ external_compiler
      $ bundle_dir
      $ max_simult_compilations
      $ smt_solver
      $ src_files
      $ output_file
      $ as_),
    info ~doc:CliInfo.compile "compile")

let replace =
  let i = Arg.info ~doc:CliInfo.replace
                   [ "replace" ; "r" ] in
  Arg.(value (flag i))

let kill_if_disabled =
  let i = Arg.info ~doc:CliInfo.kill_if_disabled
                   [ "kill-if-disabled" ] in
  Arg.(value (flag i))

let report_period =
  let env = Term.env_info "RAMEN_REPORT_PERIOD" in
  let i = Arg.info ~doc:CliInfo.report_period
                   ~env ["report-period"] in
  Arg.(value (opt float Default.report_period i))

let src_file =
  let i = Arg.info ~doc:CliInfo.src_file
                   [ "src-file" ; "source-file" ] in
  Arg.(value (opt (some string) None i))

let run =
  Term.(
    (const RamenCliCmd.run
      $ copts
      $ params
      $ replace
      $ kill_if_disabled
      $ report_period
      $ as_
      $ src_file
      $ bin_file),
    info ~doc:CliInfo.run "run")

let purge =
  let i = Arg.info ~doc:CliInfo.purge
                   [ "purge" ] in
  Arg.(value (flag i))

let kill =
  Term.(
    (const RamenCliCmd.kill
      $ copts
      $ program_globs
      $ purge),
    info ~doc:CliInfo.kill "kill")

let func_name =
  let parse s = Pervasives.Ok (RamenName.func_of_string s)
  and print fmt (p : RamenName.func) =
    Format.fprintf fmt "%s" (p :> string)
  in
  Arg.conv ~docv:"FUNCTION" (parse, print)

let opt_function_name p =
  let i = Arg.info ~doc:CliInfo.function_name
                   ~docv:"OPERATION" [] in
  Arg.(value (pos p (some func_name) None i))


let info =
  Term.(
    (const RamenCliCmd.info
      $ copts
      $ params
      $ as_
      $ bin_file
      $ opt_function_name 1),
    info ~doc:CliInfo.info "info")

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

let min_seq =
  let i = Arg.info ~doc:CliInfo.min_seq
                   ["min-seqnum"] in
  Arg.(value (opt (some int) None i))

let max_seq =
  let i = Arg.info ~doc:CliInfo.max_seq
                   ["max-seqnum"] in
  Arg.(value (opt (some int) None i))

let filter =
  (* Longer first: *)
  let operators =
    [ ">="; "<="; "!="; "="; "<>"; "<"; ">"; " in"; " not in" ] in
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
        | Result.Ok v -> Pervasives.Ok (RamenName.field_of_string pname, op, v)
        | Result.Bad e -> Pervasives.Error (`Msg e))
  and print fmt ((pname : RamenName.field), op, pval) =
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

let duration =
  let i = Arg.info ~doc:CliInfo.duration
                   ["timeout"] in
  Arg.(value (opt float 300. i))

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
      $ copts
      $ func_name_or_code
      $ with_header
      $ with_units
      $ csv_separator
      $ csv_null
      $ csv_raw
      $ last
      $ next
      $ min_seq
      $ max_seq
      $ follow
      $ where
      $ since
      $ until
      $ with_event_time
      $ duration
      $ pretty
      $ flush
      $ external_compiler
      $ bundle_dir
      $ max_simult_compilations
      $ smt_solver),
    info ~doc:CliInfo.tail "tail")

(*
 * Replay
 *)

let fq_name =
  let parse s = Pervasives.Ok (RamenName.fq_of_string s)
  and print fmt (p : RamenName.fq) =
    Format.fprintf fmt "%s" (p :> string)
  in
  Arg.conv ~docv:"FUNCTION" (parse, print)

let function_name p =
  let i = Arg.info ~doc:CliInfo.function_name
                   ~docv:"OPERATION" [] in
  Arg.(required (pos p (some fq_name) None i))

let field =
  let parse s = Pervasives.Ok (RamenName.field_of_string s)
  and print fmt (s : RamenName.field)  =
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

let replay =
  Term.(
    (const RamenCliCmd.replay
      $ copts
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
      $ bundle_dir
      $ max_simult_compilations
      $ smt_solver),
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
  let cons_func =
    let p x = x, x in
    [ p "min" ; p "max" ; p "avg" ; p "sum" ; p "count" ] in
  Arg.(value (opt (enum cons_func) "avg" i))

let bucket_time =
  let i = Arg.info ~doc:CliInfo.bucket_time
                   ~docv:"begin|middle|end" ["bucket-time"] in
  let open RamenTimeseries in
  let cons_func =
    [ "begin", Begin ; "middle", Middle ; "end", End ] in
  Arg.(value (opt (enum cons_func) Begin i))

let factors =
  let i = Arg.info ~doc:CliInfo.factors
                   ~docv:"FIELD" ["f"; "factor"] in
  Arg.(value (opt_all field [] i))

let timeseries =
  Term.(
    (const RamenCliCmd.timeseries
      $ copts
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
      $ bundle_dir
      $ max_simult_compilations
      $ smt_solver),
    info ~doc:CliInfo.timeseries "timeseries")

(*
 * Time Ranges
 *)

let timerange =
  Term.(
    (const RamenCliCmd.timerange
      $ copts
      $ function_name 0),
    info ~doc:CliInfo.timerange "timerange")

(*
 * Info
 *)

let short =
  let i = Arg.info ~doc:CliInfo.short
                   [ "short" ; "p" ] in
  Arg.(value (flag i))

let all =
  let i = Arg.info ~doc:CliInfo.all
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
    info ~doc:CliInfo.ps "ps")

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
      $ copts
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
    info ~doc:CliInfo.test "test")

(*
 * Allocate disk space to workers archives
 *)

let no_update_stats =
  let i = Arg.info ~doc:CliInfo.no_update_stats
                   [ "no-stats" ] in
  Arg.(value (flag i))

let no_update_allocs =
  let i = Arg.info ~doc:CliInfo.no_update_allocs
                   [ "no-allocs" ] in
  Arg.(value (flag i))

let no_reconf_workers =
  let i = Arg.info ~doc:CliInfo.no_reconf_workers
                   [ "no-reconf-workers" ] in
  Arg.(value (flag i))

let archivist =
  Term.(
    (const RamenCliCmd.archivist
      $ copts
      $ loop
      $ daemonize
      $ no_update_stats
      $ no_update_allocs
      $ no_reconf_workers
      $ to_stdout
      $ to_syslog),
    info ~doc:CliInfo.archivist "archivist")

(*
 * Experiments
 *)

let variants =
  Term.(
    (const RamenCliCmd.variants $ copts),
    info ~doc:CliInfo.variants "variants")

(*
 * Display internal instrumentation. Also an option of various subsystems.
 *)

let stats =
  Term.(
    (const RamenCliCmd.stats $ copts),
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
 * DEBUG
 *)

let test_orc =
  Term.((const RamenOrc.test), info ~doc:"" "_test_orc")

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
        supervisor ; gc ; httpd ; alerter ; info ;
        notify ; compile ; run ; kill ; archivist ;
        tail ; replay ; timeseries ; timerange ; ps ;
        test ; dequeue ; summary ; repair ; links ;
        variants ; stats ; autocomplete ; expand ;
        test_orc
      ]) with
  | `Error _ -> exit 1
  | `Version | `Help -> exit 0
  | `Ok f ->
      print_exn f
