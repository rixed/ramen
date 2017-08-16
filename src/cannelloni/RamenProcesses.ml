open Batteries
open Log
open RamenSharedTypes
module C = RamenConf

let run_background cmd args env =
  let open Unix in
  (* prog name should be first arg *)
  let prog_name = Filename.basename cmd in
  let args = Array.init (Array.length args + 1) (fun i ->
      if i = 0 then prog_name else args.(i-1))
  in
  !logger.info "Running %s with args %a and env %a"
    cmd
    (Array.print String.print) args
    (Array.print String.print) env ;
  match fork () with
  | 0 -> execve cmd args env
  | pid -> pid
    (* TODO: A monitoring thread that report the error in the node structure *)

let run conf graph =
  match graph.C.persist.C.status with
  | Edition ->
    raise (C.InvalidCommand "Cannot run if not compiled")
  | Running ->
    raise (C.InvalidCommand "Graph is already running")
  | Compiled ->
    (* First prepare all the required ringbuffers *)
    let rb_name_of node = RingBufLib.in_ringbuf_name (C.signature node)
    and rb_name_for_export_of node = RingBufLib.exp_ringbuf_name (C.signature node)
    and rb_sz_words = 1000000 in
    !logger.info "Creating ringbuffers..." ;
    Hashtbl.iter (fun _ node ->
        RingBuf.create (rb_name_of node) rb_sz_words ;
        if Lang.Operation.is_exporting node.C.operation then
          RingBuf.create (rb_name_for_export_of node) rb_sz_words
      ) graph.C.persist.C.nodes ;
    (* Now run everything *)
    !logger.info "Launching generated programs..." ;
    let now = Unix.gettimeofday () in
    Hashtbl.iter (fun _ node ->
        let command = Option.get node.C.command
        and output_ringbufs = List.map rb_name_of node.C.children in
        let output_ringbufs =
          if Lang.Operation.is_exporting node.C.operation then
            rb_name_for_export_of node :: output_ringbufs
          else output_ringbufs in
        let signature = C.signature node in
        let out_ringbuf_ref = RingBufLib.out_ringbuf_names_ref signature in
        File.write_lines out_ringbuf_ref (List.enum output_ringbufs) ;
        let env = [|
          "input_ringbuf="^ rb_name_of node ;
          "output_ringbufs_ref="^ out_ringbuf_ref ;
          "report_url="^ conf.C.ramen_url ^"/report/"^ node.C.name |] in
        node.C.pid <- Some (run_background command [||] env)
      ) graph.C.persist.C.nodes ;
    graph.C.persist.C.status <- Running ;
    graph.C.last_started <- Some now ;
    graph.C.importing_threads <- Hashtbl.fold (fun _ node lst ->
        if Lang.Operation.is_exporting node.C.operation then (
          let rb = rb_name_for_export_of node in
          let tuple_type = C.tup_typ_of_temp_tup_type node.C.out_type in
          RamenExport.import_tuples rb node.C.name tuple_type :: lst
        ) else lst
      ) graph.C.persist.C.nodes [] ;
    C.save_graph conf graph

let string_of_process_status = function
  | Unix.WEXITED code -> Printf.sprintf "terminated with code %d" code
  | Unix.WSIGNALED sign -> Printf.sprintf "killed by signal %d" sign
  | Unix.WSTOPPED sign -> Printf.sprintf "stopped by signal %d" sign

let stop conf graph =
  match graph.C.persist.C.status with
  | Edition | Compiled ->
    raise (C.InvalidCommand "Graph is not running")
  | Running ->
    !logger.info "Stopping the graph..." ;
    let now = Unix.gettimeofday () in
    Hashtbl.iter (fun _ node ->
        !logger.debug "Stopping node %s" node.C.name ;
        match node.C.pid with
        | None ->
          !logger.error "Node %s has no pid?!" node.C.name
        | Some pid ->
          let open Unix in
          (try kill pid Sys.sigterm
          with Unix_error _ -> ()) ;
          (try
            let _, status = restart_on_EINTR (waitpid []) pid in
            !logger.info "Node %s %s"
              node.C.name (string_of_process_status status) ;
           with exn ->
            !logger.error "Cannot wait for pid %d: %s"
              pid (Printexc.to_string exn)) ;
          node.C.pid <- None
      ) graph.C.persist.C.nodes ;
    graph.C.persist.C.status <- Compiled ;
    graph.C.last_stopped <- Some now ;
    List.iter Lwt.cancel graph.C.importing_threads ;
    graph.C.importing_threads <- [] ;
    C.save_graph conf graph
