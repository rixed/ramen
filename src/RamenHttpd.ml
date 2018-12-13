(* Server the desired HTTP APIs on the desired URLs. *)
open Batteries
open RamenHelpers
open RamenLog
module C = RamenConf

let run_httpd conf server_url api graphite fault_injection_rate =
  (* We take the port and URL prefix from the given URL but does not take
   * into account the hostname or the scheme. *)
  let url = CodecUrl.of_string server_url in
  (* In a user-supplied URL string the default port should be as usual for
   * HTTP scheme: *)
  let port =
    match String.split ~by:":" url.CodecUrl.net_loc with
    | exception Not_found ->
        (match url.CodecUrl.scheme with
        | "https" -> 443
        | _ -> 80)
    | _, p -> int_of_string p in
  let url_prefix = url.CodecUrl.path in
  let (++) rout1 rout2 =
    fun meth path params headers body ->
      try rout1 meth path params headers body
      with RamenHttpHelpers.BadPrefix ->
        rout2 meth path params headers body in
  let router _ path _ _ _ =
    let path = String.join "/" path in
    RamenHttpHelpers.not_found (Printf.sprintf "Unknown resource %S" path)
  in
  let router = Option.map_default (fun prefix ->
    !logger.info "Starting Graphite impersonator on %S"
      (if prefix = "" then "/" else prefix) ;
    RamenGraphite.router conf prefix) router graphite ++ router in
  let router = Option.map_default (fun prefix ->
    !logger.info "Serving custom API on %S" prefix ;
    RamenApi.router conf prefix) router api ++ router in
  let while_ () = !RamenProcesses.quit = None in
  restart_on_failure ~while_ "http server"
    RamenExperiments.(specialize the_big_one) [|
      RamenProcesses.dummy_nop ;
      (fun () ->
        RamenHttpHelpers.http_service
          conf port url_prefix router fault_injection_rate) |]
