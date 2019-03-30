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

let copy_client host port fq parent_num =
  !logger.info "Connecting to copy server at %a:%d" N.host_print host port ;
  let service = string_of_int port in
  let open Unix in
  let opts = [ AI_SOCKTYPE SOCK_STREAM ; AI_PASSIVE ] in
  let addrs = getaddrinfo (host :> string) service opts in
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
  let target : RamenCopy.set_target_msg = fq, parent_num in
  Files.marshal_into_fd fd target ;
  !logger.info "Send target identification" ;
  fun bytes ->
    IntCounter.inc stats_tuples ;
    let msg : RamenCopy.append_msg = bytes in
    Files.marshal_into_fd fd msg