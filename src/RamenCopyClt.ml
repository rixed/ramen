(* Forward tuple to a remote server *)
open Batteries
open RamenLog
open RamenHelpers
open RamenConsts
module N = RamenName
module Files = RamenFiles

(* Instrumentation: *)

open Binocle

let stats_connects =
  IntCounter.make Metric.Names.copy_client_connects
    Metric.Docs.copy_client_connects

let stats_tuples =
  IntCounter.make Metric.Names.copy_client_tuples
    Metric.Docs.copy_client_tuples

(* Client: *)

let copy_client clt_site srv_host port child parent_num =
  if port < 0 || port > 65535 then
    Printf.sprintf "tunneld port number (%d) not within valid range" port |>
    failwith ;
  !logger.info "Connecting to copy server at %a:%d"
    N.host_print srv_host port ;
  let service = string_of_int port in
  let open Unix in
  let opts = [ AI_SOCKTYPE SOCK_STREAM ; AI_PASSIVE ] in
  let addrs = getaddrinfo (srv_host :> string) service opts in
  let fd =
    List.find_map (fun addr ->
      try
        let sock = socket ~cloexec:true addr.ai_family addr.ai_socktype 0 in
        connect sock addr.ai_addr ;
        !logger.info "Connected to %s" (string_of_sockaddr addr.ai_addr) ;
        Some sock
      with e ->
        !logger.info "Cannot connect to %s: %s"
          (string_of_sockaddr addr.ai_addr) (Printexc.to_string e) ;
        None
    ) addrs in
  IntCounter.inc stats_connects ;
  let target =
    RamenCopy.{ client_site = clt_site ;
                child ; parent_num } in
  Files.marshal_into_fd ~at_start:false fd target ;
  !logger.info "Send target identification" ;
  fun bytes ->
    IntCounter.inc stats_tuples ;
    let msg : RamenCopy.append_msg = bytes in
    (* If the connection became unusable for any reason, this will raise,
     * and kill the worker. Supersivor will then have another look at it, esp.
     * will resolve tunneld service again, and restart the worker.
     * We do not want a worker to stubbornly retry to connect to some place
     * when the service IP have changed. *)
    Files.marshal_into_fd ~at_start:false fd msg
