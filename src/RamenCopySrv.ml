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
module Supervisor = RamenSupervisor
module ZMQClient = RamenSyncZMQClient

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

let serve_local conf ~while_ fd =
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

let serve_sync conf ~while_ fd =
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
  let topics =
    let pref =
      "sites/"^ (conf.C.site :> string) ^"/workers/"^ (id.child :> string) in
    [ pref ^"/worker" ;
      pref ^"/instances/*/input_ringbufs" ] in
  let msg_count =
    ZMQClient.start ~while_ conf.C.sync_url conf.C.login ~topics
                    (fun zock clt ->
      (* We need the input ringbuf for this parent index. We need to get the
       * current signature for the worker and then read it, waiting to receive
       * those keys if not there yet. *)
      let worker_key =
        RamenSync.Key.(PerSite (conf.C.site, PerWorker (id.child, Worker))) in
      RamenSync.Client.with_value clt worker_key (function
        | { value = RamenSync.Value.Worker worker ; _ } ->
            let ringbufs_key =
              Supervisor.per_instance_key
                conf.C.site id.child worker.signature InputRingFiles in
            RamenSync.Client.with_value clt ringbufs_key (fun hv ->
              match Supervisor.get_string_list (Some hv.value) with
              | Some lst ->
                  (* No idea what to do whan parent_num is beyond the end of
                   * list, let's just crash the forked server: *)
                  let bname = List.at lst id.parent_num in
                  let rb =
                    (* If supervisor, for any reason, haven't created this
                     * ringbuf yet, then [load] is going to fail. Just wait. *)
                    retry ~on:always ~while_ RingBuf.load bname in
                  copy_all ~while_ conf id.client_site bname fd rb
              | None ->
                  Printf.sprintf2 "Cannot find input ringbufs at %a"
                    RamenSync.Key.print ringbufs_key |>
                  failwith)
        | hv ->
            RamenSync.invalid_sync_type worker_key hv.value "a worker") ;
      (* with_value just register a callback but we still have to turn the
       * crank: *)
      ZMQClient.process_in ~while_ zock clt) in
  !logger.debug "Processed %d messages" msg_count

(* Start the service: *)

let copy_server conf port =
  if port < 0 || port > 65535 then
    Printf.sprintf "tunneld port number (%d) not within valid range" port |>
    failwith ;
  !logger.info "Starting copy server on port %d" port ;
  let inet = Unix.inet_addr_any in
  let addr = Unix.(ADDR_INET (inet, port)) in
  let while_ () = !RamenProcesses.quit = None in
  (* TODO: a wrapper that compresses and hashes the content. *)
  let service_name = ServiceNames.tunneld in
  let serve =
    if conf.C.sync_url = "" then serve_local
    else serve_sync in
  forking_server ~while_ ~service_name addr (serve conf ~while_)
