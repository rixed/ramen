open Batteries
open Lwt
open Log
module C = RamenConf
module N = RamenConf.Node
module L = RamenConf.Layer
module SL = RamenSharedTypes.Layer

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

exception NotYetCompiled
exception AlreadyRunning
exception StillCompiling

let string_of_process_status = function
  | Unix.WEXITED code -> Printf.sprintf "terminated with code %d" code
  | Unix.WSIGNALED sign -> Printf.sprintf "killed by signal %d" sign
  | Unix.WSTOPPED sign -> Printf.sprintf "stopped by signal %d" sign

let run conf layer =
  let open C.Layer in
  match layer.persist.status with
  | SL.Edition -> raise NotYetCompiled
  | SL.Running -> raise AlreadyRunning
  | SL.Compiling -> raise StillCompiling
  | SL.Compiled ->
    (* First prepare all the required ringbuffers *)
    let rb_name_of node =
      RingBufLib.in_ringbuf_name conf.C.persist_dir (N.fq_name node)
    and rb_name_for_export_of node =
      RingBufLib.exp_ringbuf_name conf.C.persist_dir (N.fq_name node)
    and rb_sz_words = 1000000 in
    !logger.info "Creating ringbuffers..." ;
    Hashtbl.iter (fun _ node ->
        RingBuf.create (rb_name_of node) rb_sz_words ;
        if Lang.Operation.is_exporting node.N.operation then
          RingBuf.create (rb_name_for_export_of node) rb_sz_words
      ) layer.persist.nodes ;
    (* Now run everything *)
    !logger.info "Launching generated programs..." ;
    let now = Unix.gettimeofday () in
    Hashtbl.iter (fun _ node ->
        let command = Option.get node.N.command
        and output_ringbufs = List.map rb_name_of node.N.children in
        let output_ringbufs =
          if Lang.Operation.is_exporting node.N.operation then
            rb_name_for_export_of node :: output_ringbufs
          else output_ringbufs in
        let out_ringbuf_ref =
          RingBufLib.out_ringbuf_names_ref conf.C.persist_dir (N.fq_name node) in
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
          "persist_dir="^ conf.C.persist_dir ^"/worker/"
                        ^ node.N.layer ^"/"^ node.N.name |] in
        let pid = run_background command [||] env in
        node.N.pid <- Some pid ;
        async (fun () ->
          let rec restart () =
            match%lwt Lwt_unix.waitpid [] pid with
            | exception Unix.Unix_error (Unix.EINTR, _, _) -> restart ()
            | exception exn ->
              !logger.error "Cannot wait for pid %d: %s"
                pid (Printexc.to_string exn) ;
              return_unit
            | _, status ->
              !logger.info "Node %s %s"
                node.N.name (string_of_process_status status) ;
              return_unit in
          restart ()) ;
        (* Update the parents out_ringbuf_ref if it's in another layer *)
        List.iter (fun parent ->
            if parent.N.layer <> layer.name then
              let out_ref =
                RingBufLib.out_ringbuf_names_ref conf.C.persist_dir (N.fq_name parent) in
              (* The parent ringbuf must exist at that point *)
              let lines = File.lines_of out_ref |> List.of_enum in
              if not (List.mem input_ringbuf lines) then
                File.write_lines out_ref (List.enum (input_ringbuf :: lines))
          ) node.N.parents
      ) layer.persist.nodes ;
    C.Layer.set_status layer SL.Running ;
    layer.C.Layer.persist.C.Layer.last_started <- Some now ;
    layer.C.Layer.importing_threads <- Hashtbl.fold (fun _ node lst ->
        if Lang.Operation.is_exporting node.N.operation then (
          let rb = rb_name_for_export_of node in
          let tuple_type = C.tup_typ_of_temp_tup_type node.N.out_type in
          RamenExport.import_tuples rb node tuple_type :: lst
        ) else lst
      ) layer.C.Layer.persist.C.Layer.nodes [] ;
    C.save_graph conf

exception NotRunning

let stop conf layer =
  match layer.C.Layer.persist.C.Layer.status with
  | SL.Edition | SL.Compiled -> raise NotRunning
  | SL.Compiling ->
    (* FIXME: do as for Running and make sure run() check the status hasn't
     * changed before launching workers. *)
    raise NotRunning
  | SL.Running ->
    !logger.info "Stopping layer %s" layer.L.name ;
    let now = Unix.gettimeofday () in
    Hashtbl.iter (fun _ node ->
        match node.N.pid with
        | None ->
          !logger.error "Node %s has no pid?!" node.N.name
        | Some pid ->
          let open Unix in
          (try kill pid Sys.sigterm
           with Unix_error _ as e ->
            !logger.error "Cannot kill pid %d: %s" pid (Printexc.to_string e)) ;
          node.N.pid <- None
      ) layer.C.Layer.persist.C.Layer.nodes ;
    C.Layer.set_status layer SL.Compiled ;
    layer.C.Layer.persist.C.Layer.last_stopped <- Some now ;
    List.iter cancel layer.C.Layer.importing_threads ;
    layer.C.Layer.importing_threads <- [] ;
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
         now > layer.L.persist.L.last_used +. layer.L.persist.L.timeout then (
        (try
          stop conf layer ;
          !logger.info "Deleting unused layer %s after a %gs timeout"
            layer_name layer.L.persist.L.timeout ;
        with NotRunning -> ()) ;
        Hashtbl.remove conf.C.graph.C.layers layer_name ;
      )
    ) unused ;
  C.save_graph conf
