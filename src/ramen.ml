open Cmdliner
open Batteries

(*
 * Common options
 *)

let copts =
  let docs = Manpage.s_common_options in
  let debug =
    let env = Term.env_info "RAMEN_DEBUG" in
    let i = Arg.info ~doc:"increase verbosity"
                     ~docs ~env [ "d"; "debug" ] in
    Arg.(value (flag i))
  and server_url =
    let env = Term.env_info "RAMEN_URL" in
    let i = Arg.info ~doc:"URL to reach ramen"
                     ~docs ~env [ "ramen-url" ; "server-url" ; "url" ] in
    Arg.(value (opt string "http://127.0.0.1:29380" i))
  and persist_dir =
    let env = Term.env_info "RAMEN_PERSIST_DIR" in
    let i = Arg.info ~doc:"directory where are stored data persisted on disc"
                     ~env [ "persist-dir" ] in
    Arg.(value (opt string Consts.default_persist_dir i))
  and max_history_archives =
    let env = Term.env_info "RAMEN_MAX_HISTORY_ARCHIVES" in
    let i = Arg.info ~doc:"max number of archive files to keep per history ; \
                           0 would disable archiving altogether"
                     ~env ["max-history-archives" ; "max-history-files" ] in
    Arg.(value (opt int 200 i))
  (* Make it an enum option so that it's easier to change the default *)
  and use_embedded_compiler =
    let env = Term.env_info "RAMEN_USE_EMBEDDED_COMPILER" in
    let i = Arg.info ~doc:"use embedded compiler rather than calling system one"
                     ~env [ "use-embedded-compiler"; "use-internal-compiler";
                            "embedded-compiler"; "internal-compiler" ] in
    Arg.(value (flag i))
  and bundle_dir =
    let env = Term.env_info "RAMEN_BUNDLE_DIR" in
    let i = Arg.info ~doc:"directory where to find libraries for the embedded \
                           compiler"
                     ~env [ "bundle-dir" ] in
    Arg.(value (opt string RamenCompilConfig.default_bundle_dir i))
  and max_simult_compilations =
    let env = Term.env_info "RAMEN_MAX_SIMULT_COMPILATIONS" in
    let i = Arg.info ~doc:"max number of compilations to perform \
                           simultansously"
                     ~env ["max-simult-compil" ;
                           "max-simultaneous-compilations" ] in
    Arg.(value (opt int 4 i))
  and rand_seed =
    let i = Arg.info ~doc:"seed to initialize the random generator with. \
                           (will use a random one if unset)"
                     [ "seed"; "rand-seed" ] in
    Arg.(value (opt (some int) None i))
  and max_incidents_per_team =
    let env = Term.env_info "RAMEN_MAX_INCIDENTS_PER_TEAM" in
    let i = Arg.info ~doc:"max number of ongoing incidents per team (after \
                           which older are timed out)"
                     ~env ["max-incidents" ] in
    Arg.(value (opt int 30 i))
  in
  Term.(const ApiCmd.make_copts
    $ debug $ server_url $ persist_dir $ max_history_archives
    $ use_embedded_compiler $ bundle_dir $ max_simult_compilations
    $ rand_seed $ max_incidents_per_team)

(*
 * Start the event processor
 *)

let daemonize =
  let env = Term.env_info "RAMEN_DAEMONIZE" in
  let i = Arg.info ~doc:"daemonize"
                   ~env [ "daemon"; "daemonize"] in
  Arg.(value (flag i))

let no_demo =
  let env = Term.env_info "RAMEN_NO_DEMO" in
  let i = Arg.info ~doc:"do not load demo operations"
                   ~env [ "no-demo" ] in
  Arg.(value (flag i))

let to_stderr =
  let env = Term.env_info "RAMEN_LOG_TO_STDERR" in
  let i = Arg.info ~doc:"log onto stderr"
                   ~env [ "log-to-stderr"; "to-stderr"; "stderr" ] in
  Arg.(value (flag i))

let ssl_cert =
  let env = Term.env_info "RAMEN_SSL_CERT_FILE" in
  let i = Arg.info ~doc:"File containing the SSL certificate"
                   ~env [ "ssl-certificate" ] in
  Arg.(value (opt (some string) None i))

let ssl_key =
  let env = Term.env_info "RAMEN_SSL_KEY_FILE" in
  let i = Arg.info ~doc:"File containing the SSL private key"
                   ~env [ "ssl-key" ] in
  Arg.(value (opt (some string) None i))

let www_dir =
  let env = Term.env_info "RAMEN_WWW_DIR" in
  let i = Arg.info ~doc:"Directory where to read the files served \
                         via HTTP for the GUI (serve from memory \
                         if unset)"
                   ~env [ "www-dir" ; "www-root" ; "web-dir" ; "web-root" ] in
  Arg.(value (opt string "" i))

let alert_conf_json =
  let env = Term.env_info "RAMEN_ALERTER_INITIAL_JSON" in
  let i = Arg.info ~doc:"JSON configuration file for the alerter"
                   ~env [ "alerter-config" ; "alerter-json" ;
                          "alerting-config" ; "alerting-json" ] in
  Arg.(value (opt (some string) None i))

let server_start =
  Term.(
    (const ApiCmd.start
      $ copts
      $ daemonize
      $ no_demo
      $ to_stderr
      $ www_dir
      $ ssl_cert
      $ ssl_key
      $ alert_conf_json),
    info ~doc:Consts.start_info "start")

(*
 * Shutdown the event processor
 *)

let server_stop =
  Term.(
    (const ApiCmd.shutdown
      $ copts),
    info ~doc:Consts.shutdown_info "shutdown")

(* TODO: check that this is actually the name of a ringbuffer file *)
let rb_file =
  let i = Arg.info ~doc:"File with the ring buffer"
                   ~docv:"file" [] in
  Arg.(required (pos 0 (some string) None i))

let nb_tuples =
  let i = Arg.info ~doc:"How many entries to dequeue"
                   [ "n"; "nb-entries" ] in
  Arg.(value (opt int 1 i))

let dequeue =
  Term.(
    (const RingBufCmd.dequeue
      $ copts
      $ rb_file
      $ nb_tuples),
    info ~doc:"Dequeue a message from a ringbuffer" "dequeue")

let summary =
  Term.(
    (const RingBufCmd.summary
      $ copts
      $ rb_file),
    info ~doc:"Dump info about a ringbuffer" "ringbuf-summary")

(*
 * Program Creation/Edition
 *)

let program_name =
  let i = Arg.info ~doc:"Program unique name."
                   ~docv:"name" [] in
  Arg.(required (pos 0 (some string) None i))

let program =
  let i = Arg.info ~doc:"New program (such as 'DEFINE xyz AS SELECT...')"
                   ~docv:"program" [] in
  Arg.(required (pos 1 (some string) None i))

let and_start =
  let i = Arg.info ~doc:"Also start that new program"
                   [ "start" ] in
  Arg.(value (flag i))

let ok_if_running =
  let i = Arg.info ~doc:"If a program exist by that name and is running, \
                         stop it and replace it."
                   [ "ok-if-running" ] in
  Arg.(value (flag i))

let remote =
  let i = Arg.info ~doc:"Use the remote access API" ["remote"] in
  Arg.(value (flag i))

let add =
  Term.(
    (const ApiCmd.add
      $ copts
      $ program_name
      $ program
      $ ok_if_running
      $ and_start
      $ remote),
    info ~doc:"Define a new program or replace a previous one" "add")

(*
 * Sync local sources
 *)

let root_dir =
  let i = Arg.info ~doc:"Root directory."
                   ~docv:"directory" [] in
  Arg.(required (pos 0 (some string) None i))

let program_prefix =
  let i = Arg.info ~doc:"Program prefix."
                   ~docv:"program_prefix" [] in
  Arg.(value (pos 1 string "" i))

let sync =
  Term.(
    (const ApiCmd.sync
      $ copts
      $ root_dir
      $ program_prefix
      $ and_start),
    info ~doc:"Synchronize a subtree of the configuration with source files \
               from the file system." "sync")

(*
 * Compile/Run/Stop Program
 *)

let compile =
  Term.(
    (const ApiCmd.compile
      $ copts),
    info ~doc:"Compile one (or all) program(s)" "compile")

let param =
  let parse s =
    match String.split s ~by:"=" with
    | exception Not_found ->
        Pervasives.Error (
          `Msg "You must specify the parameter name, followed by an equal \
                sign (=), followed by the parameter value.")
    | pname, pval ->
        let open RamenParsing in
        let p = opt_blanks -+ RamenScalar.Parser.p +- opt_blanks +- eof in
        let stream = stream_of_string pval in
        let m = [ "parameter value from command line" ] in
        (match p m  None Parsers.no_error_correction stream |>
              to_result with
        | Bad e ->
            let err =
              IO.to_string (print_bad_result RamenScalar.print) e in
            Pervasives.Error (`Msg err)
        | Ok (v, _) ->
            Pervasives.Ok (pname, v))
  and print fmt (pname, pval) =
    Format.fprintf fmt "%s=%s" pname (RamenScalar.to_string pval)
  in
  Arg.conv ~docv:"PARAM=VALUE" (parse, print)

(* Note: parameter with same name in different functions will all take
 * their value from this. Easy to add a prefix with function name when
 * it causes troubles. *)
let params =
  let i = Arg.info ~doc:"Override parameter's P default value with V"
                   ~docv:"P=V"
                   ["p"; "parameter"] in
  Arg.(value (opt_all param [] i))

let run =
  Term.(
    (const ApiCmd.run
      $ copts),
    info ~doc:"Run one (or all) program(s)" "run")

let stop =
  Term.(
    (const ApiCmd.stop
      $ copts
      $ program_name),
    info ~doc:"Stop one (or all) program(s)" "stop")

(* New "offline" compilation: *)

let root_path =
  let env = Term.env_info "RAMEN_ROOT" in
  let i = Arg.info ~doc:"Path where to find other programs."
                   ~env [ "root" ] in
  Arg.(value (opt string "" i))

let keep_temp_files =
  let i = Arg.info ~doc:"Keep temporary files."
                   [ "keep-temp-files" ] in
  Arg.(value (flag i))

let source_files =
  let i = Arg.info ~doc:"Source files to compile"
                   ~docv:"program.ramen" [] in
  Arg.(non_empty (pos_all string [] i))

let bin_files =
  let i = Arg.info ~doc:"Binary files to run"
                   ~docv:"program.x" [] in
  Arg.(non_empty (pos_all string [] i))

(* TODO: rename as compile once inline compilation command is gone *)
let ext_compile =
  Term.(
    (const ApiCmd.ext_compile
      $ copts
      $ keep_temp_files
      $ root_path
      $ source_files),
    info ~doc:Consts.compile_info "xcompile")

let ext_run =
  Term.(
    (const ApiCmd.ext_run
      $ copts
      $ params
      $ root_path
      $ bin_files),
    info ~doc:Consts.run_info "xrun")

(*
 * Export Tuples
 *)

let func_name p =
  let i = Arg.info ~doc:"Operation unique name."
                   ~docv:"operation" [] in
  Arg.(required (pos p (some string) None i))

let as_csv =
  let i = Arg.info ~doc:"output CSV rather than JSON"
                   [ "as-csv"; "csv" ] in
  Arg.(value (flag i))

let with_header =
  let i = Arg.info ~doc:"Output the header line in CSV"
                   [ "with-header"; "header" ] in
  Arg.(value (flag i))

let last =
  let i = Arg.info ~doc:"output only the last N tuples"
                   [ "last" ] in
  Arg.(value (opt int 10 i))

let continuous =
  let i = Arg.info ~doc:"When done, wait for more tuples instead of quitting"
                   [ "continuous"; "cont" ] in
  Arg.(value (flag i))

let tail =
  Term.(
    (const ApiCmd.tail
      $ copts
      $ func_name 0
      $ as_csv
      $ with_header
      $ last
      $ continuous),
    info ~doc:"Display the last outputs of an operation" "tail")

let csv_separator =
  let env = Term.env_info "RAMEN_CSV_SEPARATOR" in
  let i = Arg.info ~doc:"Field separator."
                   ~env [ "separator" ] in
  Arg.(value (opt string "," i))

let csv_null =
  let env = Term.env_info "RAMEN_CSV_NULL" in
  let i = Arg.info ~doc:"Representation of NULL values."
                   ~env [ "null" ] in
  Arg.(value (opt string "<NULL>" i))

let xlast =
  let i = Arg.info ~doc:"output only the last N tuples."
                   [ "last" ] in
  Arg.(value (opt (some int) None i))

let min_seq =
  let i = Arg.info ~doc:"output only tuples with greater sequence number."
                   ["min-seqnum"] in
  Arg.(value (opt (some int) None i))

let max_seq =
  let i = Arg.info ~doc:"output only tuples with smaller sequence number."
                   ["max-seqnum"] in
  Arg.(value (opt (some int) None i))

let min_ts =
  let i = Arg.info ~doc:"output only tuples with greater timestamp."
                   ["min-timestamp"] in
  Arg.(value (opt (some float) None i))

let max_ts =
  let i = Arg.info ~doc:"output only tuples with smaller timestamp."
                   ["max-timestamp"] in
  Arg.(value (opt (some float) None i))

let print_seqnums =
  let i = Arg.info ~doc:"Prepend tuples with their sequence number."
                   ["with-seqnums"] in
  Arg.(value (flag i))

(* TODO: from/until timestamps *)
let ext_tail =
  Term.(
    (const ApiCmd.ext_tail
      $ copts
      $ func_name 0
      $ with_header
      $ csv_separator
      $ csv_null
      $ xlast
      $ min_seq
      $ max_seq
      $ print_seqnums),
    info ~doc:Consts.tail_info "xtail")

let max_results =
  let i = Arg.info ~doc:"output only the first N tuples"
                   [ "max" ] in
  Arg.(value (opt (some int) None i))

let export =
  Term.(
    (const ApiCmd.export
      $ copts
      $ func_name 0
      $ as_csv
      $ with_header
      $ max_results),
    info ~doc:"Dump a range of output from an operation" "export")

(*
 * Timeseries (no support for NewTempFunc (yet))
 *)

let since =
  let i = Arg.info ~doc:"timestamp of first point"
                   ~docv:"SINCE" ["since"] in
  Arg.(required (opt (some float) None i))

let until =
  let i = Arg.info ~doc:"timestamp of last point"
                   ~docv:"UNTIL" ["until"] in
  Arg.(required (opt (some float) None i))

let max_data_points =
  let i = Arg.info ~doc:"max number of points returned"
                   ~docv:"POINTS" ["nb-points"] in
  Arg.(value (opt int 100 i))

let data_field p =
  let i = Arg.info ~doc:"Field to retrieve values from"
                   ~docv:"data" [] in
  Arg.(required (pos p (some string) None i))

let consolidation =
  let i = Arg.info ~doc:"Consolidation function"
                   ~docv:"cons.function" ["consolidation"] in
  let cons_func =
    let p x = x, x in
    [ p "min" ; p "max" ; p "avg" ] in
  Arg.(value (opt (enum cons_func) "avg" i))

let timeseries =
  Term.(
    (const ApiCmd.timeseries
      $ copts
      $ since
      $ until
      $ max_data_points
      $ func_name 0
      $ data_field 1
      $ consolidation),
    info ~doc:"Extract a timeseries from an operation" "timeseries")

let xtimeseries =
  Term.(
    (const ApiCmd.ext_timeseries
      $ copts
      $ since
      $ until
      $ max_data_points
      $ csv_separator
      $ csv_null
      $ func_name 0
      $ data_field 1
      $ consolidation),
    info ~doc:Consts.timeseries_info "xtimeseries")

(*
 * Time Ranges
 *)

let timerange =
  Term.(
    (const ApiCmd.timerange
      $ copts
      $ func_name 0),
    info ~doc:"Retrieve the available time range of an operation output"
         "timerange")

(*
 * Info
 *)

let name_opt =
  let i = Arg.info ~doc:"Program or operation unique name."
                   ~docv:"name" [] in
  Arg.(value (pos 0 (some string) None i))

let as_json =
  let i = Arg.info ~doc:"Dump the raw json response."
                   [ "json" ; "as-json" ] in
  Arg.(value (flag i))

let short =
  let i = Arg.info ~doc:"Display only a short summary."
                   [ "short" ] in
  Arg.(value (flag i))

let get_info =
  Term.(
    (const ApiCmd.info
      $ copts
      $ as_json
      $ short
      $ name_opt
      $ remote),
    info ~doc:"Get info about a program or an operation" "info")

(*
 * Tests
 *)

let conf_file =
  let i = Arg.info ~doc:"Configuration file to be tested."
                   ~docv:"conf.json" [] in
  Arg.(required (pos 0 (some string) None i))

let test_files =
  let i = Arg.info ~doc:"Definition of a test to run."
                   ~docv:"test.json" [] in
  Arg.(non_empty (pos_right 0 string [] i))

let test =
  Term.(
    (const ApiCmd.test
      $ copts
      $ conf_file
      $ test_files),
    info ~doc:"Test a configuration against a test suite" "test")

(*
 * Autocompletion
 *)

let command =
  let i = Arg.info ~doc:"Ramen command line to be completed."
                   ~docv:"command-line" [] in
  Arg.(value (pos 0 string "" i))

let autocomplete =
  Term.(
    (const RamenCompletion.complete
      $ command),
    info ~doc:"Autocomplete the given command" "_completion")

(*
 * Command line evaluation
 *)

let default =
  let sdocs = Manpage.s_common_options in
  let doc = "Ramen Stream Processor" in
  let version = RamenVersions.release_tag in
  Term.((ret (const (`Help (`Pager, None)))),
        info "Ramen" ~version ~doc ~sdocs)

let () =
  match Term.eval_choice default [
    server_start ; server_stop ; dequeue ; summary ; sync ;
    add ; compile ; run ; stop ; tail ; timeseries ; timerange ;
    get_info ; test ; ext_compile ; ext_run ; ext_tail ; xtimeseries ;
    autocomplete
  ] with `Error _ -> exit 1
       | `Version | `Help -> exit 0
       | `Ok f -> (
          try f ()
          with Exit -> exit 0
             | Helpers.Timeout ->
                 Printf.eprintf "Timed out\n" ;
                 exit 1
             | Failure msg ->
                 Printf.eprintf "%s\n" msg ;
                 exit 1)
