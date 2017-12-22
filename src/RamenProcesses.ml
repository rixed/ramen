open Batteries
open Lwt
open RamenLog
module C = RamenConf
module N = RamenConf.Node
module L = RamenConf.Layer
open RamenSharedTypesJS
open Helpers

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
      try close_fd i with Unix.Unix_error (Unix.EBADF, _, _) -> ()
    done ;
    execve cmd args env
  | pid -> pid

(* Compute input ringbuf and output ringbufs given the node identifier
 * and its input type, so that if we change the operation of a node we
 * don't risk reading old ringbuf with incompatible values. *)

let in_ringbuf_name conf node =
  let sign = C.type_signature_hash node.N.in_type in
  conf.C.persist_dir ^"/workers/ringbufs/"^ N.fq_name node ^"/"^ sign ^"/in"

let exp_ringbuf_name conf node =
  let sign = C.type_signature_hash node.N.out_type in
  conf.C.persist_dir ^"/workers/ringbufs/"^ N.fq_name node ^"/"^ sign ^"/exp"

let out_ringbuf_names_ref conf node =
  conf.C.persist_dir ^"/workers/ringbufs/"^ N.fq_name node ^"/out_ref"

let report_ringbuf conf =
  conf.C.persist_dir ^"/instrumentation/"^ conf.C.instrumentation_version

exception NotYetCompiled
exception AlreadyRunning
exception StillCompiling

let input_spec conf node =
  in_ringbuf_name conf node, []

let run_node conf layer node =
  let command = C.exec_of_node conf.C.persist_dir node
  and output_ringbufs =
    (* Start to output to nodes of this layer. They have all been
     * created above, and we want to allow loops in a layer. Avoids
     * outputing to other layers, unless they are already running (in
     * which case their ringbuffer exists already), otherwise we would
     * hang. *)
    node.N.children |>
    List.fold_left (fun outs child ->
      if child.N.layer = layer.L.name ||
         match Hashtbl.find conf.C.graph.C.layers child.N.layer with
         | exception Not_found -> false (* uh? Better not ask.*)
         | layer when layer.L.persist.L.status = Running -> true
         | _ -> false
      then
        let k, v = input_spec conf child in
        Map.add k v outs
      else outs) Map.empty in
  let output_ringbufs =
    if Lang.Operation.is_exporting node.N.operation then
      Map.add (exp_ringbuf_name conf node) [] output_ringbufs
    else output_ringbufs in
  let out_ringbuf_ref =
    out_ringbuf_names_ref conf node in
  let%lwt () = RamenOutRef.set out_ringbuf_ref output_ringbufs in
  !logger.info "Start %s" node.N.name ;
  let input_ringbuf = in_ringbuf_name conf node in
  let env = [|
    "OCAMLRUNPARAM="^ if conf.C.debug then "b" else "" ;
    "debug="^ string_of_bool conf.C.debug ;
    "name="^ N.fq_name node ;
    "input_ringbuf="^ input_ringbuf ;
    "output_ringbufs_ref="^ out_ringbuf_ref ;
    "report_ringbuf="^ report_ringbuf conf ;
    (* We need to change this dir whenever the node signature change
     * to prevent it to reload an incompatible state: *)
    "persist_dir="^ conf.C.persist_dir ^"/workers/tmp/"
                  ^ (N.fq_name node) ^"/"^ node.N.signature ;
    (match !logger.logdir with
      | Some _ ->
        "log_dir="^ conf.C.persist_dir ^"/workers/log/"
                  ^ (N.fq_name node)
      | None -> "no_log_dir=") ;
    (* Owl also need HOME (See https://github.com/ryanrhymes/owl/issues/116) *)
    "HOME="^ getenv ~def:"/tmp" "HOME" |] in
  let%lwt pid =
    wrap (fun () -> run_background command [||] env) in
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
  Lwt_list.iter_p (fun parent ->
      if parent.N.layer = layer.name then
        return_unit
      else
        let out_ref =
          out_ringbuf_names_ref conf parent in
        (* The parent ringbuf might not exist yet if it has never been
         * started. If the parent is not running then it will overwrite
         * it when it starts, with whatever running children it will
         * have at that time (including us, if we are still running).  *)
        RamenOutRef.add out_ref (input_spec conf node)
    ) node.N.parents

let run conf layer =
  let open L in
  match layer.persist.status with
  | Edition _ -> fail NotYetCompiled
  | Running -> fail AlreadyRunning
  | Compiling -> fail StillCompiling
  | Compiled ->
    (* First prepare all the required ringbuffers *)
    !logger.debug "Creating ringbuffers..." ;
    let layer_nodes =
      Hashtbl.values layer.persist.nodes |> List.of_enum in
    let rb_sz_words = 1000000 in
    let%lwt () = Lwt_list.iter_p (fun node ->
        wrap (fun () ->
          RingBuf.create (in_ringbuf_name conf node) rb_sz_words ;
          if Lang.Operation.is_exporting node.N.operation then
            RingBuf.create (exp_ringbuf_name conf node) rb_sz_words)
      ) layer_nodes in
    (* Now run everything *)
    !logger.debug "Launching generated programs..." ;
    let now = Unix.gettimeofday () in
    let%lwt () = Lwt_list.iter_p (run_node conf layer) layer_nodes in
    L.set_status layer Running ;
    layer.L.persist.L.last_started <- Some now ;
    layer.L.importing_threads <- Hashtbl.fold (fun _ node lst ->
        if Lang.Operation.is_exporting node.N.operation then (
          let rb = exp_ringbuf_name conf node in
          RamenExport.import_tuples conf rb node :: lst
        ) else lst
      ) layer.L.persist.L.nodes [] ;
    return_unit

exception NotRunning

let stop conf layer =
  match layer.L.persist.L.status with
  | Edition _ | Compiled -> fail NotRunning
  | Compiling ->
    (* FIXME: do as for Running and make sure run() check the status hasn't
     * changed before launching workers. *)
    fail NotRunning
  | Running ->
    !logger.debug "Stopping layer %s" layer.L.name ;
    let now = Unix.gettimeofday () in
    let layer_nodes =
      Hashtbl.values layer.L.persist.L.nodes |> List.of_enum in
    let%lwt () = Lwt_list.iter_p (fun node ->
        let k = RamenExport.history_key node in
        (match Hashtbl.find RamenExport.imported_tuples k with
        | exception Not_found -> ()
        | history -> RamenExport.archive_history conf history) ;
        match node.N.pid with
        | None ->
          !logger.error "Node %s has no pid?!" node.N.name ;
          return_unit
        | Some pid ->
          !logger.debug "Stopping node %s, pid %d" node.N.name pid ;
          (* Start by removing this worker ringbuf from all its parent output
           * reference *)
          let this_in = in_ringbuf_name conf node in
          let%lwt () = Lwt_list.iter_p (fun parent ->
              let out_ref = out_ringbuf_names_ref conf parent in
              RamenOutRef.remove out_ref this_in
            ) node.N.parents in
          (* Get rid of the worker *)
          let open Unix in
          (try kill pid Sys.sigterm
           with Unix_error _ as e ->
            !logger.error "Cannot kill pid %d: %s" pid (Printexc.to_string e)) ;
          node.N.pid <- None ;
          return_unit
      ) layer_nodes in
    L.set_status layer Compiled ;
    layer.L.persist.L.last_stopped <- Some now ;
    List.iter cancel layer.L.importing_threads ;
    layer.L.importing_threads <- [] ;
    return_unit

(* Timeout unused layers.
 * By unused, we mean either: no layer depends on it, or no one cares for
 * what it exports. *)

let use_layer now layer =
  layer.L.persist.L.last_used <- now

let use_layer_by_name conf now layer_name =
  Hashtbl.find conf.C.graph.C.layers layer_name |>
  use_layer now

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
  Set.iter (use_layer_by_name conf now) used ;
  let unused = Set.diff defined used |>
               Set.to_list in
  Lwt_list.iter_p (fun layer_name ->
      let layer = Hashtbl.find conf.C.graph.C.layers layer_name in
      if layer.L.persist.L.timeout > 0. &&
         now > layer.L.persist.L.last_used +. layer.L.persist.L.timeout
      then (
        !logger.info "Deleting unused layer %s after a %gs timeout"
          layer_name layer.L.persist.L.timeout ;
        (* Kill first, and only then forget about it. *)
        let%lwt () =
          try%lwt stop conf layer with NotRunning -> return_unit in
        Hashtbl.remove conf.C.graph.C.layers layer_name ;
        return_unit
      ) else return_unit
    ) unused

(* Instrumentation: Reading workers stats *)

open Stdint

let reports_lock = RWLock.make ()
let last_reports = Hashtbl.create 31

let read_reports conf =
  let rb_name = report_ringbuf conf
  and rb_sz_words = 1000000 in
  RingBuf.create rb_name rb_sz_words ;
  let rb = RingBuf.load rb_name in
  async (fun () ->
    (* TODO: we probably want to move this function elsewhere than in a
     * lib that's designed for workers: *)
    RingBuf.read_ringbuf rb (fun tx ->
      let worker, time, ic, sc, oc, gc, cpu, ram, wi, wo, bi, bo =
        RamenBinocle.unserialize tx in
      RingBuf.dequeue_commit tx ;
      RWLock.with_w_lock reports_lock (fun () ->
        Hashtbl.replace last_reports worker RamenSharedTypes.Node.{
          time ;
          in_tuple_count = Option.map Uint64.to_int ic ;
          selected_tuple_count = Option.map Uint64.to_int sc ;
          out_tuple_count = Option.map Uint64.to_int oc ;
          group_count = Option.map Uint64.to_int gc ;
          cpu_time = cpu ; ram_usage = Uint64.to_int ram ;
          in_sleep = wi ; out_sleep = wo ;
          in_bytes = Option.map Uint64.to_int bi ;
          out_bytes = Option.map Uint64.to_int bo } ;
        return_unit)))

let last_report fq_name =
  RWLock.with_r_lock reports_lock (fun () ->
    Hashtbl.find_option last_reports fq_name |?
    { time = 0. ;
      in_tuple_count = None ; selected_tuple_count = None ;
      out_tuple_count = None ; group_count = None ;
      cpu_time = 0. ; ram_usage = 0 ;
      in_sleep = None ; out_sleep = None ;
      in_bytes = None ; out_bytes = None } |>
    return)
