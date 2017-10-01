open Batteries
open Lwt
open RamenLog
module C = RamenConf
module N = RamenConf.Node
module L = RamenConf.Layer
module SL = RamenSharedTypes.Layer

let fd_of_int : int -> Unix.file_descr = Obj.magic

let close_fd i =
  Unix.close (fd_of_int i)

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
  flush_all () ;
  match fork () with
  | 0 ->
    close_fd 0 ;
    for i = 3 to 255 do
      try close_fd i with Unix.Unix_error(Unix.EBADF, _, _) -> ()
    done ;
    execve cmd args env
  | pid -> pid

exception NotYetCompiled
exception AlreadyRunning
exception StillCompiling

(* Compute input ringbuf and output ringbufs given the node fq name. *)

let in_ringbuf_name conf node =
  conf.C.persist_dir ^"/workers/ringbufs/"^ N.fq_name node ^"/in"

let exp_ringbuf_name conf node =
  conf.C.persist_dir ^"/workers/ringbufs/"^ N.fq_name node ^"/exp"

let out_ringbuf_names_ref conf node =
  conf.C.persist_dir ^"/workers/ringbufs/"^ N.fq_name node ^"/out_ref"

let run conf layer =
  let open L in
  match layer.persist.status with
  | SL.Edition -> raise NotYetCompiled
  | SL.Running -> raise AlreadyRunning
  | SL.Compiling -> raise StillCompiling
  | SL.Compiled ->
    (* First prepare all the required ringbuffers *)
    let rb_name_of node =
      in_ringbuf_name conf node
    and rb_name_for_export_of node =
      exp_ringbuf_name conf node
    and rb_sz_words = 1000000 in
    !logger.debug "Creating ringbuffers..." ;
    Hashtbl.iter (fun _ node ->
        RingBuf.create (rb_name_of node) rb_sz_words ;
        if Lang.Operation.is_exporting node.N.operation then
          RingBuf.create (rb_name_for_export_of node) rb_sz_words
      ) layer.persist.nodes ;
    (* Now run everything *)
    !logger.debug "Launching generated programs..." ;
    let now = Unix.gettimeofday () in
    Hashtbl.iter (fun _ node ->
        let command = C.exec_of_node conf.C.persist_dir node
        and output_ringbufs =
          (* Start to output only to nodes on this layer. They have all been
           * created above, and we want to allow loops in a layer. But above
           * layers have no ringbuffer already and we do not want to start
           * outputing in their direction until they are effectively started
           * (or we would hang) *)
          node.N.children |>
          List.filter (fun node -> node.N.layer = layer.L.name) |>
          List.map rb_name_of in
        let output_ringbufs =
          if Lang.Operation.is_exporting node.N.operation then
            rb_name_for_export_of node :: output_ringbufs
          else output_ringbufs in
        let out_ringbuf_ref =
          out_ringbuf_names_ref conf node in
        Helpers.mkdir_all ~is_file:true out_ringbuf_ref ;
        (* We do not care what was there before. This is OK as long as layers
         * are started in dependency order. *)
        File.write_lines out_ringbuf_ref (List.enum output_ringbufs) ;
        let input_ringbuf = rb_name_of node in
        let env = [|
          "OCAMLRUNPARAM=b" ;
          "debug="^ string_of_bool conf.C.debug ;
          "name="^ node.N.name ;
          "input_ringbuf="^ input_ringbuf ;
          "output_ringbufs_ref="^ out_ringbuf_ref ;
          "report_url="^ conf.C.ramen_url
                       ^ "/report/"^ Uri.pct_encode node.N.layer
                       ^ "/"^ Uri.pct_encode node.N.name ;
          "persist_dir="^ conf.C.persist_dir ^"/workers/tmp/"
                        ^ (N.fq_name node) ;
          (match !logger.logdir with
            | Some _ ->
              "log_dir="^ conf.C.persist_dir ^"/workers/log/"
                        ^ (N.fq_name node)
            | None -> "no_log_dir=") |] in
        let pid = run_background command [||] env in
        node.N.pid <- Some pid ;
        async (fun () ->
          let rec wait_child () =
            match%lwt Lwt_unix.waitpid [] pid with
            | exception Unix.Unix_error (Unix.EINTR, _, _) -> wait_child ()
            | exception exn ->
              (* TODO: save this error on the node record *)
              !logger.error "Cannot wait for pid %d: %s"
                pid (Printexc.to_string exn) ;
              return_unit
            | _, status ->
              (* TODO: save this error on the node record *)
              !logger.info "Node %s (pid %d) %s"
                node.N.name pid (Helpers.string_of_process_status status) ;
              return_unit in
          wait_child ()) ;
        (* Update the parents out_ringbuf_ref if it's in another layer *)
        List.iter (fun parent ->
            if parent.N.layer <> layer.name then
              let out_ref =
                out_ringbuf_names_ref conf parent in
              (* The parent ringbuf must exist at that point *)
              let lines = File.lines_of out_ref |> List.of_enum in
              if not (List.mem input_ringbuf lines) then
                File.write_lines out_ref (List.enum (input_ringbuf :: lines))
          ) node.N.parents
      ) layer.persist.nodes ;
    L.set_status layer SL.Running ;
    layer.L.persist.L.last_started <- Some now ;
    layer.L.importing_threads <- Hashtbl.fold (fun _ node lst ->
        if Lang.Operation.is_exporting node.N.operation then (
          let rb = rb_name_for_export_of node in
          RamenExport.import_tuples rb node :: lst
        ) else lst
      ) layer.L.persist.L.nodes [] ;
    C.save_graph conf

exception NotRunning

let stop conf layer =
  match layer.L.persist.L.status with
  | SL.Edition | SL.Compiled -> raise NotRunning
  | SL.Compiling ->
    (* FIXME: do as for Running and make sure run() check the status hasn't
     * changed before launching workers. *)
    raise NotRunning
  | SL.Running ->
    !logger.debug "Stopping layer %s" layer.L.name ;
    let now = Unix.gettimeofday () in
    Hashtbl.iter (fun _ node ->
        Option.may RamenExport.archive_history node.N.history ;
        match node.N.pid with
        | None ->
          !logger.error "Node %s has no pid?!" node.N.name
        | Some pid ->
          !logger.debug "Stopping node %s, pid %d" node.N.name pid ;
          (* Start by removing this worker ringbuf from all its parent output
           * reference *)
          let this_in =
            in_ringbuf_name conf node in
          List.iter (fun parent ->
              let out_ref =
                out_ringbuf_names_ref conf parent in
              File.(lines_of out_ref // (<>) this_in |> write_lines out_ref)
              (* File.(lines_of out_ref // (<>) this_in |> write_lines out_ref) *)
            ) node.N.parents ;
          (* Get rid of the worker *)
          let open Unix in
          (try kill pid Sys.sigterm
           with Unix_error _ as e ->
            !logger.error "Cannot kill pid %d: %s" pid (Printexc.to_string e)) ;
          node.N.pid <- None
      ) layer.L.persist.L.nodes ;
    L.set_status layer SL.Compiled ;
    layer.L.persist.L.last_stopped <- Some now ;
    List.iter cancel layer.L.importing_threads ;
    layer.L.importing_threads <- [] ;
    C.save_graph conf

(* Timeout unused layers.
 * By unused, we mean either: no layer depends on it, or no one cares for
 * what it exports. *)

let use_layer conf now layer_name =
  let layer = Hashtbl.find conf.C.graph.C.layers layer_name in
  layer.L.persist.L.last_used <- now

let timeout_layers conf =
  (* FIXME: We need a lock on the graph config *)
  (* Build the set of all defined and all used layers *)
  let defined, used = Hashtbl.fold (fun layer_name layer (defined, used) ->
      Set.add layer_name defined,
      Hashtbl.fold (fun _node_name node used ->
          List.fold_left (fun used parent ->
              if parent.N.layer = layer_name then used
              else Set.add parent.N.layer used
            ) used node.N.parents
        ) layer.L.persist.L.nodes used
    ) conf.C.graph.C.layers (Set.empty, Set.empty) in
  let now = Unix.gettimeofday () in
  Set.iter (use_layer conf now) used ;
  let unused = Set.diff defined used in
  Set.iter (fun layer_name ->
      let layer = Hashtbl.find conf.C.graph.C.layers layer_name in
      if layer.L.persist.L.timeout > 0. &&
         now > layer.L.persist.L.last_used +. layer.L.persist.L.timeout
      then (
        !logger.info "Deleting unused layer %s after a %gs timeout"
          layer_name layer.L.persist.L.timeout ;
        (* Kill first, and only then forget about it. *)
        (try stop conf layer with NotRunning -> ()) ;
        Hashtbl.remove conf.C.graph.C.layers layer_name
      )
    ) unused ;
  C.save_graph conf
