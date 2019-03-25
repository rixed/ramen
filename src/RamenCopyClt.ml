(* Forward tuple to a remote server *)
open Batteries
open RamenLog
open RamenHelpers
open RamenConsts
module C = RamenConf
module F = C.Func
module P = C.Program
module N = RamenName
module Files = RamenFiles

(* Instrumentation: *)

open Binocle

let stats_connects =
  RamenBinocle.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.copy_client_connects
      "Number of attempted connections.")

let stats_tuples =
  RamenBinocle.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.copy_client_tuples
      "Number of sent tuples.")

(* Client: *)

let copy_client conf host port fq parent_num =
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
  IntCounter.inc (stats_connects conf.C.persist_dir) ;
  let target = fq, parent_num in
  Files.marshal_into_fd fd target ;
  !logger.info "Send target identification" ;
  fun bytes start stop ->
    IntCounter.inc (stats_tuples conf.C.persist_dir) ;
    let msg = bytes, start, stop in
    Files.marshal_into_fd fd msg
