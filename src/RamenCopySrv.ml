(* A small service that accept TCP connections and will then copy ringbuffer
 * messages arriving over the network into the proper input ringbuffer of the
 * specified function. *)
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

let stats_accepts =
  RamenBinocle.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.copy_server_accepts
      "Number of accepted connections.")

let stats_tuples =
  RamenBinocle.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.copy_server_tuples
      "Number of copied tuples.")

(* Server: *)

let copy_all conf fd rb =
  forever (fun () ->
    let bytes, start, stop =
      Files.(marshal_from_fd socket_path) fd in
    IntCounter.inc (stats_tuples conf.C.persist_dir) ;
    RingBuf.enqueue rb bytes (Bytes.length bytes) start stop
  ) ()

let serve conf fd =
  !logger.debug "New connection to copy service on fd %d!"
    (Files.int_of_fd fd) ;
  IntCounter.inc (stats_accepts conf.C.persist_dir) ;
  (* First message is supposed to identify what the target is: *)
  let child, parent_num = Files.(marshal_from_fd socket_path) fd in
  !logger.info "Received target identification: %a, #%d"
    N.fq_print child parent_num ;
  let _mre, _prog, func =
    C.with_rlock conf (fun programs ->
      C.find_func_or_fail programs child) in
  let bname =
    if func.F.merge_inputs then
      C.in_ringbuf_name_merging conf func parent_num
    else
      C.in_ringbuf_name_single conf func in
  let rb = RingBuf.load bname in
  copy_all conf fd rb

(* Start the service: *)

let copy_server conf port =
  !logger.info "Starting copy server on port %d" port ;
  let inet = Unix.inet_addr_any in
  let addr = Unix.(ADDR_INET (inet, port)) in
  let while_ () = !RamenProcesses.quit = None in
  (* TODO: a wrapper that compresses and hashes the content. *)
  forking_server ~while_ ~service_name:"tunneld" addr (serve conf)
