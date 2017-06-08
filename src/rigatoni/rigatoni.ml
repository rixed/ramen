(* Rigatoni controlling daemon *)
open Cmdliner
open Batteries
open RamenLogger

type conf = { logger : logger }

let conf debug =
  { logger = make_logger debug }

let common_opts =
  let debug =
    Arg.(value (flag (info ~doc:"increase verbosity" ["d"; "debug"])))
  in
  Term.(const conf $ debug)

(*
 * Start the event processor
 *)

let start conf http_port ssl_cert ssl_key =
  let srv_thread = HttpSrv.start conf.logger http_port ssl_cert ssl_key in
  Lwt_main.run srv_thread

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

let start_cmd =
  Term.(
    (const start
      $ common_opts
      $ http_port_opt
      $ ssl_cert_opt
      $ ssl_key_opt),
    info "rigatoni")

let () =
  Term.exit @@ Term.eval start_cmd
