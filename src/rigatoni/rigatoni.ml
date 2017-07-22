(* Rigatoni controlling daemon *)
open Cmdliner
open Batteries

let common_opts =
  let debug =
    Arg.(value (flag (info ~doc:"increase verbosity" ["d"; "debug"])))
  and graph_save_file =
    Arg.(value (opt string "/tmp/rigatoni_graph.raw"
                    (info ~doc:"graph save file" ["graph-save-file"])))
  in
  Term.(const RamenConf.make_conf $ debug $ graph_save_file)

(*
 * Start the event processor
 *)

let http_port_opt =
  let i = Arg.info ~doc:"Port where to run the HTTP server \
                         (HTTPS will be run on that port + 1)"
                   [ "http-port" ] in
  Arg.(value (opt int 29380 i))

let ssl_cert_opt =
  let i = Arg.info ~doc:"File containing the SSL certificate"
                   [ "ssl-certificate" ] in
  Arg.(value (opt (some string) None i))

let ssl_key_opt =
  let i = Arg.info ~doc:"File containing the SSL private key"
                   [ "ssl-key" ] in
  Arg.(value (opt (some string) None i))

let server_start_cmd =
  Term.(
    (const HttpSrv.start
      $ common_opts
      $ http_port_opt
      $ ssl_cert_opt
      $ ssl_key_opt),
    info "start")

(* TODO: check that this is actually the name of a ringbuffer file *)
let rb_file_opt =
  let i = Arg.info ~docv:"FILE" ~doc:"File with the ring buffer" [] in
  Arg.(required (pos 0 (some string) None i))

let nb_tuples_opt =
  let i = Arg.info ~doc:"How many entries to dequeue"
                   ["n"; "nb-entries"] in
  Arg.(value (opt int 1 i))

let dequeue_cmd =
  Term.(
    (const RingBufCmd.dequeue
      $ common_opts
      $ rb_file_opt
      $ nb_tuples_opt),
    info "dequeue")

let summary_cmd =
  Term.(
    (const RingBufCmd.summary
      $ common_opts
      $ rb_file_opt),
    info "ringbuf-summary")
(*
 * Binary argument evaluation
 *)

let default_cmd =
  let doc = "Rigatoni Stream Processor" in
  Term.((ret (app (const (fun _ -> `Help (`Pager, None))) common_opts)),
        info "rigatoni" ~doc)

let () =
  match Term.eval_choice default_cmd [
    server_start_cmd ; dequeue_cmd ; summary_cmd
  ] with `Error _ -> exit 1
       | `Version | `Help -> exit 42
       | `Ok f -> f ()
