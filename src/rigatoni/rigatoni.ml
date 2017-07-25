(* Rigatoni controlling daemon *)
open Cmdliner
open Batteries

let common_opts =
  let debug =
    let env = Term.env_info "RIGATONI_DEBUG" in
    let i = Arg.info ~doc:"increase verbosity"
                     ~env ["d"; "debug"] in
    Arg.(value (flag i))
  (* TODO: Move this out of conf, make it a parameter to HttpSrc.start *)
  and graph_save_file =
    let env = Term.env_info "RIGATONI_SAVE_FILE" in
    let i = Arg.info ~doc:"graph save file"
                     ~env ["graph-save-file"] in
    Arg.(value (opt string "/tmp/rigatoni_graph.raw" i))
  in
  Term.(const RamenConf.make_conf $ debug $ graph_save_file)

(*
 * Start the event processor
 *)

let http_port =
  let env = Term.env_info "RIGATONI_HTTP_PORT" in
  let i = Arg.info ~doc:"Port where to run the HTTP server \
                         (HTTPS will be run on that port + 1)"
                   ~env [ "http-port" ] in
  Arg.(value (opt int 29380 i))

let ssl_cert =
  let env = Term.env_info "RIGATONI_SSL_CERT_FILE" in
  let i = Arg.info ~doc:"File containing the SSL certificate"
                   ~env [ "ssl-certificate" ] in
  Arg.(value (opt (some string) None i))

let ssl_key =
  let env = Term.env_info "RIGATONI_SSL_KEY_FILE" in
  let i = Arg.info ~doc:"File containing the SSL private key"
                   ~env [ "ssl-key" ] in
  Arg.(value (opt (some string) None i))

let server_start =
  Term.(
    (const HttpSrv.start
      $ common_opts
      $ http_port
      $ ssl_cert
      $ ssl_key),
    info "start")

(* TODO: check that this is actually the name of a ringbuffer file *)
let rb_file =
  let i = Arg.info ~doc:"File with the ring buffer"
                   ~docv:"FILE" [] in
  Arg.(required (pos 0 (some string) None i))

let nb_tuples =
  let i = Arg.info ~doc:"How many entries to dequeue"
                   ["n"; "nb-entries"] in
  Arg.(value (opt int 1 i))

let dequeue =
  Term.(
    (const RingBufCmd.dequeue
      $ common_opts
      $ rb_file
      $ nb_tuples),
    info "dequeue")

let summary =
  Term.(
    (const RingBufCmd.summary
      $ common_opts
      $ rb_file),
    info "ringbuf-summary")

(*
 * API Commands
 *)

let ramen_url =
  let i = Arg.info ~doc:"URL to reach ramen" [ "ramen-url" ] in
  Arg.(value (opt string "http://127.0.0.1:29380" i))

let node_name p =
  let i = Arg.info ~doc:"Node unique name" ~docv:"node" [] in
  Arg.(required (pos p (some string) None i))

let node_operation p =
  let i = Arg.info ~doc:"Node operation (such as 'SELECT etc...'"
                   ~docv:"operation" [] in
  Arg.(required (pos p (some string) None i))

let add_node =
  Term.(
    (const ApiCmd.add_node
      $ common_opts
      $ ramen_url
      $ node_name 0
      $ node_operation 1),
    info "add-node")

let add_link =
  Term.(
    (const ApiCmd.add_link
      $ common_opts
      $ ramen_url
      $ node_name 0
      $ node_name 1),
    info "add-link")

let compile =
  Term.(
    (const ApiCmd.compile
      $ common_opts
      $ ramen_url),
    info "compile")

let run =
  Term.(
    (const ApiCmd.run
      $ common_opts
      $ ramen_url),
    info "run")

let as_csv =
  let i = Arg.info ~doc:"output CSV rather than JSON"
                   ["as-csv";"csv"] in
  Arg.(value (flag i))

let last =
  let i = Arg.info ~doc:"output only the last N tuples"
                   ["last"] in
  Arg.(value (opt (some int) None i))

let continuous =
  let i = Arg.info ~doc:"When done, wait for more tuples instead of quitting"
                   ["continuous"; "cont"] in
  Arg.(value (flag i))

let tail =
  Term.(
    (const ApiCmd.tail
      $ common_opts
      $ ramen_url
      $ node_name 0
      $ as_csv
      $ last
      $ continuous),
    info "tail")

(*
 * Command line evaluation
 *)

let default =
  let doc = "Rigatoni Stream Processor" in
  Term.((ret (app (const (fun _ -> `Help (`Pager, None))) common_opts)),
        info "rigatoni" ~doc)

let () =
  match Term.eval_choice default [
    server_start ;
    dequeue ; summary ;
    add_node ; add_link ; compile ; run ; tail
  ] with `Error _ -> exit 1
       | `Version | `Help -> exit 42
       | `Ok f -> f ()
