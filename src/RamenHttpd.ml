(* Server the desired HTTP APIs on the desired URLs. *)
open Batteries
open RamenHelpers
open RamenLog
module C = RamenConf

(* The HTTP server forks a new process for every client. This child
 * process handles as many queries as the client will send before closing
 * the connection, at which point the process will terminate.
 * Problem: A zmq socket cannot be passed through a fork.
 * We could sync in the server process and then pass a read only KV store
 * to the child, but some of these children will have to write in the config
 * tree (for instance to store an alert, or create a replay for a timeseries).
 * So that's the client which will create the zocket and start the sync. *)
let http_topics api graphite =
  let api_topics =
    [ "sites/*/workers/*/worker" ; (* For get_programs *)
      "target_config" ; (* for running alerts *)
      "sources/*/info" ;
      "sources/*/alert" ] |>
    Set.of_list
  and graphite_topics =
    RamenExport.replay_topics |>
    Set.of_list in
  let topics =
    Set.union
      (if api then api_topics else Set.empty)
      (if graphite then graphite_topics else Set.empty) in
  Set.to_list topics

let run_httpd conf server_url api table_prefix graphite fault_injection_rate =
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
    | _, p ->
        try int_of_string p
        with Failure _ ->
          Printf.sprintf "httpd port (%S) must be numeric" p |>
          failwith
  in
  if port < 0 || port > 65535 then
    Printf.sprintf "httpd port (%d) not in valid range" port |>
    failwith ;
  let url_prefix = url.CodecUrl.path in
  let (++) rout1 rout2 =
    fun meth path params headers body ->
      try rout1 meth path params headers body
      with RamenHttpHelpers.BadPrefix ->
        rout2 meth path params headers body in
  let router _ _ path _ _ _ =
    let path = String.join "/" path in
    RamenHttpHelpers.not_found (Printf.sprintf "Unknown resource %S" path)
  in
  let router = Option.map_default (fun prefix ->
    !logger.info "Starting Graphite impersonator on %S"
      (if prefix = "" then "/" else prefix) ;
    RamenGraphite.router conf prefix) router graphite ++ router in
  let router = Option.map_default (fun prefix ->
    !logger.info "Serving custom API on %S" prefix ;
    RamenApi.router conf prefix table_prefix) router api ++ router in
  let while_ () = !RamenProcesses.quit = None in
  let topics = http_topics (api <> None) (graphite <> None) in
  restart_on_failure ~while_ "http server"
    RamenExperiments.(specialize the_big_one) [|
      RamenProcesses.dummy_nop ;
      (fun () ->
        RamenHttpHelpers.http_service
          conf port url_prefix router fault_injection_rate topics) |]
