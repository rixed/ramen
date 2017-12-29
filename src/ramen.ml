open Cmdliner
open Batteries

(*
 * Start the event processor
 *)

let debug =
  let env = Term.env_info "RAMEN_DEBUG" in
  let i = Arg.info ~doc:"increase verbosity"
                   ~env [ "d"; "debug" ] in
  Arg.(value (flag i))

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

let server_url =
  let env = Term.env_info "RAMEN_URL" in
  let i = Arg.info ~doc:"URL to reach ramen"
                   ~env [ "ramen-url" ] in
  Arg.(value (opt string "http://127.0.0.1:29380" i))

let www_dir =
  let env = Term.env_info "RAMEN_WWW_DIR" in
  let i = Arg.info ~doc:"Directory where to read the files served \
                         via HTTP for the GUI (serve from memory \
                         if unset)"
                   ~env [ "www-dir" ; "www-root" ; "web-dir" ; "web-root" ] in
  Arg.(value (opt string "" i))

let version_tag =
  let env = Term.env_info "RAMEN_VERSION_TAG" in
  let i = Arg.info ~doc:"unique tag identifying the version of ramen \
                         (such as git tag name or sha1)"
                   ~env [ "version-tag"; "tag" ] in
  Arg.(value (opt string "1.1.1" i))

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

let alert_conf_json =
  let env = Term.env_info "RAMEN_ALERTER_INITIAL_JSON" in
  let i = Arg.info ~doc:"JSON initial configuration for the alerter"
                   ~env [ "alerter-conf" ; "alerter-json" ] in
  Arg.(value (opt (some string) None i))

let server_start =
  Term.(
    (const HttpSrv.start
      $ debug
      $ daemonize
      $ rand_seed
      $ no_demo
      $ to_stderr
      $ server_url
      $ www_dir
      $ version_tag
      $ persist_dir
      $ max_history_archives
      $ http_port
      $ ssl_cert
      $ ssl_key
      $ alert_conf_json),
    info "start")

(*
 * Shutdown the event processor
 *)

let server_stop =
  Term.(
    (const ApiCmd.shutdown
      $ debug
      $ server_url),
    info "shutdown")

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
      $ debug
      $ rb_file
      $ nb_tuples),
    info "dequeue")

let summary =
  Term.(
    (const RingBufCmd.summary
      $ debug
      $ rb_file),
    info "ringbuf-summary")

(*
 * Layer Creation/Edition
 *)

let node_name p =
  let i = Arg.info ~doc:"Node unique name."
                   ~docv:"node" [] in
  Arg.(required (pos p (some string) None i))

let layer_name =
  let i = Arg.info ~doc:"Layer unique name."
                   ~docv:"layer" [] in
  Arg.(required (pos 0 (some string) None i))

let node_operations =
  let i = Arg.info ~doc:"New operation (such as 'SELECT etc...') \
                         optionally prefixed with a name and colon \
                         (for instance: 'FOO_FILTER:SELECT * WHERE FOO'). \
                         Names can then be used with --link."
                        [ "node"; "operation"; "op" ] in
  Arg.(non_empty (opt_all string [] i))

let add =
  Term.(
    (const ApiCmd.add
      $ debug
      $ server_url
      $ layer_name
      $ node_operations),
    info "add")

(*
 * Compile/Run/Stop Layer
 *)

let compile =
  Term.(
    (const ApiCmd.compile
      $ debug
      $ server_url),
    info "compile")

let run =
  Term.(
    (const ApiCmd.run
      $ debug
      $ server_url),
    info "run")

let stop =
  Term.(
    (const ApiCmd.stop
      $ debug
      $ layer_name
      $ server_url),
    info "stop")

(*
 * Export Tuples
 *)

let as_csv =
  let i = Arg.info ~doc:"output CSV rather than JSON"
                   [ "as-csv"; "csv" ] in
  Arg.(value (flag i))

let last =
  let i = Arg.info ~doc:"output only the last N tuples"
                   [ "last" ] in
  Arg.(value (opt (some int) None i))

let continuous =
  let i = Arg.info ~doc:"When done, wait for more tuples instead of quitting"
                   [ "continuous"; "cont" ] in
  Arg.(value (flag i))

let tail =
  Term.(
    (const ApiCmd.tail
      $ debug
      $ server_url
      $ node_name 0
      $ as_csv
      $ last
      $ continuous),
    info "tail")

(*
 * Timeseries (no support for NewTempNode (yet))
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
      $ debug
      $ server_url
      $ since
      $ until
      $ max_data_points
      $ node_name 0
      $ data_field 1
      $ consolidation),
    info "timeseries")

(*
 * Time Ranges
 *)

let timerange =
  Term.(
    (const ApiCmd.timerange
      $ debug
      $ server_url
      $ node_name 0),
    info "timerange")

(*
 * Command line evaluation
 *)

let default =
  let doc = "Ramen Stream Processor" in
  Term.((ret (const (`Help (`Pager, None)))),
        info "Ramen" ~doc)

let () =
  match Term.eval_choice default [
    server_start ; server_stop ;
    dequeue ; summary ;
    add ; compile ; run ; stop ;
    tail ; timeseries ; timerange
  ] with `Error _ -> exit 1
       | `Version | `Help -> exit 42
       | `Ok f -> f ()
