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
    Arg.(value (flag i)) in
  let server_url =
    let env = Term.env_info "RAMEN_URL" in
    let i = Arg.info ~doc:"URL to reach ramen"
                     ~docs ~env [ "ramen-url" ] in
    Arg.(value (opt string "http://127.0.0.1:29380" i))
  in
  Term.(const (fun debug server_url -> ApiCmd.{ debug ; server_url }) $
              debug $ server_url)

(*
 * Start the event processor
 *)

let daemonize =
  let env = Term.env_info "RAMEN_DAEMONIZE" in
  let i = Arg.info ~doc:"daemonize"
                   ~env [ "daemon"; "daemonize"] in
  Arg.(value (flag i))

let rand_seed =
  let i = Arg.info ~doc:"seed to initialize the random generator with. \
                         (will use a random one if unset)"
                   [ "seed"; "rand-seed" ] in
  Arg.(value (opt (some int) None i))

let no_demo =
  let env = Term.env_info "RAMEN_NO_DEMO" in
  let i = Arg.info ~doc:"do not load demo operations"
                   ~env [ "no-demo" ] in
  Arg.(value (flag i))

let to_stderr =
  let env = Term.env_info "RAMEN_LOG_TO_STDERR" in
  let i = Arg.info ~doc:"log onto stderr"
                   ~env [ "log-onto-stderr"; "log-to-stderr"; "to-stderr";
                          "stderr" ] in
  Arg.(value (flag i))

let http_port =
  let env = Term.env_info "RAMEN_HTTP_PORT" in
  let i = Arg.info ~doc:"Port where to run the HTTP server \
                         (HTTPS will be run on that port + 1)"
                   ~env [ "http-port" ] in
  Arg.(value (opt int 29380 i))

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

let persist_dir =
  let env = Term.env_info "RAMEN_PERSIST_DIR" in
  let i = Arg.info ~doc:"Directory where are stored data persisted on disc"
                   ~env [ "persist-dir" ] in
  Arg.(value (opt string "/tmp/ramen" i))

let max_history_archives =
  let env = Term.env_info "RAMEN_MAX_HISTORY_ARCHIVES" in
  let i = Arg.info ~doc:"max number of archive files to keep per history ; \
                         0 would disable archiving altogether."
                   ~env ["max-history-archives" ; "max-history-files" ] in
  Arg.(value (opt int 200 i))

(* Make it an enum option so that it's easier to change the default *)
let use_embedded_compiler =
  let env = Term.env_info "RAMEN_USE_EMBEDDED_COMPILER" in
  let i = Arg.info ~doc:"use embedded compiler rather than calling system one."
                   ~env [ "use-embedded-compiler"; "use-internal-compiler";
                          "embedded-compiler"; "internal-compiler" ] in
  Arg.(value (flag i))

let bundle_dir =
  let env = Term.env_info "RAMEN_BUNDLE_DIR" in
  let i = Arg.info ~doc:"Directory where to find libraries for the embedded \
                         compiler."
                   ~env [ "bundle-dir" ] in
  Arg.(value (opt string RamenCompilConfig.default_bundle_dir i))

let alert_conf_json =
  let env = Term.env_info "RAMEN_ALERTER_INITIAL_JSON" in
  let i = Arg.info ~doc:"JSON configuration file for the alerter"
                   ~env [ "alerter-conf" ; "alerter-json" ] in
  Arg.(value (opt (some string) None i))

let server_start =
  Term.(
    (const ApiCmd.start
      $ copts
      $ daemonize
      $ rand_seed
      $ no_demo
      $ to_stderr
      $ www_dir
      $ persist_dir
      $ max_history_archives
      $ use_embedded_compiler
      $ bundle_dir
      $ http_port
      $ ssl_cert
      $ ssl_key
      $ alert_conf_json),
    info ~doc:"Start the processes orchestrator" "start")

(*
 * Shutdown the event processor
 *)

let server_stop =
  Term.(
    (const ApiCmd.shutdown
      $ copts),
    info ~doc:"Stop all processes" "shutdown")

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
                   ~docv:"program" [] in
  Arg.(required (pos 0 (some string) None i))

let program =
  let i = Arg.info ~doc:"New program (such as 'DEFINE xyz AS SELECT...')"
                   ~docv:"program" [] in
  Arg.(required (pos 1 (some string) None i))

let and_start =
  let i = Arg.info ~doc:"Also start that new program"
                   [ "start" ] in
  Arg.(value (flag i))

let add =
  Term.(
    (const ApiCmd.add
      $ copts
      $ program_name
      $ program
      $ and_start),
    info ~doc:"Define a new program or replace a previous one" "add")

(*
 * Compile/Run/Stop Program
 *)

let compile =
  Term.(
    (const ApiCmd.compile
      $ copts),
    info ~doc:"Compile one (or all) program(s)" "compile")

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
  Arg.(value (opt (some (enum cons_func)) None i))

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
  let i = Arg.info ~doc:"Program of operation unique name."
                   ~docv:"name" [] in
  Arg.(value (pos 0 (some string) None i))

let get_info =
  Term.(
    (const ApiCmd.info
      $ copts
      $ name_opt),
    info ~doc:"Get info about a program or an operation" "info")

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
    server_start ; server_stop ;
    dequeue ; summary ;
    add ; compile ; run ; stop ;
    tail ; timeseries ; timerange ;
    get_info
  ] with `Error _ -> exit 1
       | `Version | `Help -> exit 0
       | `Ok f -> f ()
