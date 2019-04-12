(* A small service that accept TCP connections and will then copy ringbuffer
 * messages arriving over the network into the proper input ringbuffer of the
 * specified function. *)
open Batteries
open RamenLog
open RamenHelpers
open RamenConsts
module C = RamenConf
module RC = C.Running
module F = C.Func
module P = C.Program
module N = RamenName
module Files = RamenFiles

(* Instrumentation: *)

open Binocle

let stats_accepts =
  RamenWorkerStats.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.copy_server_accepts
      "Number of accepted connections.")

let stats_tuples =
(*(* TODO: make Binocle able to merge saved stats with new stats
   * and then make it merge only from time to time, so that it became
   * possible to save stats with high rate of change: *)
  RamenWorkerStats.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.copy_server_tuples
      "Number of copied tuples.") *)
  IntCounter.make Metric.Names.copy_server_tuples
    "Number of copied tuples."

(* Server: *)

let copy_all ~while_ _conf (client_site : N.site) (bname : N.path) fd rb =
  let labels = [ "client", (client_site :> string) ] in
  while while_ () do
    let bytes : RamenCopy.append_msg =
      Files.(marshal_from_fd socket_path) fd in
    (*IntCounter.inc ~labels (stats_tuples conf.C.persist_dir) ;*)
    IntCounter.inc ~labels stats_tuples ;
    retry
      ~while_:(fun () -> !RamenProcesses.quit = None)
      ~on:(function
        | RingBuf.NoMoreRoom ->
            !logger.debug "NoMoreRoom in target ringbuf of %a: %a, waiting"
              N.site_print client_site
              N.path_print bname ;
            true
        | _ ->
            false)
      ~first_delay:0.001 ~max_delay:1.
      (RingBuf.enqueue rb bytes (Bytes.length bytes) 0.) 0.
  done

let serve conf ~while_ fd =
  !logger.debug "New connection to copy service on fd %d!"
    (Files.int_of_fd fd) ;
  IntCounter.inc (stats_accepts conf.C.persist_dir) ;
  (* First message is supposed to identify the client and what the
   * target is: *)
  let id : RamenCopy.set_target_msg =
    Files.(marshal_from_fd socket_path) fd in
  !logger.info "Received target identification: %a, %a, #%d"
    N.site_print id.client_site
    N.fq_print id.child
    id.parent_num ;
  let _mre, _prog, func =
    RC.with_rlock conf (fun programs ->
      RC.find_func_or_fail programs id.child) in
  let bname =
    if func.F.merge_inputs then
      C.in_ringbuf_name_merging conf func id.parent_num
    else
      C.in_ringbuf_name_single conf func in
  let rb =
    (* If supervisor haven't started this worker for any reason, then
     * [load] is going to fail. Just wait. *)
    retry ~on:always ~while_ RingBuf.load bname in
  copy_all ~while_ conf id.client_site bname fd rb

(* Start the service: *)

let copy_server conf port =
  !logger.info "Starting copy server on port %d" port ;
  let inet = Unix.inet_addr_any in
  let addr = Unix.(ADDR_INET (inet, port)) in
  let while_ () = !RamenProcesses.quit = None in
  (* TODO: a wrapper that compresses and hashes the content. *)
  forking_server ~while_ ~service_name:"tunneld" addr (serve conf ~while_)
