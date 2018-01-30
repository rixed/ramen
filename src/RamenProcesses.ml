open Batteries
open Lwt
open RamenLog
module C = RamenConf
module N = RamenConf.Func
module L = RamenConf.Program
module SN = RamenSharedTypes.Info.Func
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

(* Compute input ringbuf and output ringbufs given the func identifier
 * and its input type, so that if we change the operation of a func we
 * don't risk reading old ringbuf with incompatible values. *)

let in_ringbuf_name conf func =
  let sign = C.type_signature_hash func.N.in_type in
  conf.C.persist_dir ^"/workers/ringbufs/"
                     ^ RamenVersions.ringbuf ^"/"
                     ^ N.fq_name func ^"/"^ sign ^"/in"

let exp_ringbuf_name conf func =
  let sign = C.type_signature_hash func.N.out_type in
  conf.C.persist_dir ^"/workers/ringbufs/"
                     ^ RamenVersions.ringbuf ^"/"
                     ^ N.fq_name func ^"/"^ sign ^"/exp"

let out_ringbuf_names_ref conf func =
  conf.C.persist_dir ^"/workers/out_ref/"
                     ^ RamenVersions.out_ref ^"/"
                     ^ N.fq_name func ^"/out_ref"

let report_ringbuf conf =
  conf.C.persist_dir ^"/instrumentation_ringbuf/"
                     ^ RamenVersions.instrumentation_tuple ^"_"
                     ^ RamenVersions.ringbuf
                     ^"/ringbuf"

exception NotYetCompiled
exception AlreadyRunning
exception StillCompiling

let input_spec conf parent func =
  in_ringbuf_name conf func,
  let out_type = C.tuple_ser_type parent.N.out_type
  and in_type = C.tuple_ser_type func.N.in_type in
  RingBufLib.skip_list ~out_type ~in_type

(* Takes a locked conf.
 * FIXME: a phantom type for this *)
let rec run_func conf program func =
  let command = C.exec_of_func conf.C.persist_dir func
  and output_ringbufs =
    (* Start to output to funcs of this program. They have all been
     * created above (in [run]), and we want to allow loops in a program. Avoids
     * outputting to other programs, unless they are already running (in
     * which case their ring-buffer exists already), otherwise we would
     * hang. *)
    C.fold_funcs conf Map.empty (fun outs l n ->
      (* Select all func's children that are either running or in the same
       * program *)
      if (n.N.program = program.L.name || l.L.persist.L.status = Running) &&
         List.exists (fun (pl, pn) ->
           pl = program.L.name && pn = func.N.name
         ) n.N.parents
      then (
        !logger.debug "%s will output to %s" (N.fq_name func) (N.fq_name n) ;
        let k, v = input_spec conf func n in
        Map.add k v outs
      ) else outs) in
  let output_ringbufs =
    if Lang.Operation.is_exporting func.N.operation then
      let typ = C.tuple_ser_type func.N.out_type in
      Map.add (exp_ringbuf_name conf func)
              (RingBufLib.skip_list ~out_type:typ ~in_type:typ)
              output_ringbufs
    else output_ringbufs in
  let out_ringbuf_ref =
    out_ringbuf_names_ref conf func in
  let%lwt () = RamenOutRef.set out_ringbuf_ref output_ringbufs in
  !logger.info "Start %s" func.N.name ;
  let input_ringbuf = in_ringbuf_name conf func in
  let env = [|
    "OCAMLRUNPARAM="^ if conf.C.debug then "b" else "" ;
    "debug="^ string_of_bool conf.C.debug ;
    "name="^ N.fq_name func ;
    "input_ringbuf="^ input_ringbuf ;
    "output_ringbufs_ref="^ out_ringbuf_ref ;
    "report_ringbuf="^ report_ringbuf conf ;
    (* We need to change this dir whenever the func signature change
     * to prevent it to reload an incompatible state: *)
    "persist_dir="^ conf.C.persist_dir ^"/workers/tmp/"
                  ^ RamenVersions.worker_state
                  ^"/"^ (N.fq_name func)
                  ^"/"^ func.N.signature ;
    (match !logger.logdir with
      | Some _ ->
        "log_dir="^ conf.C.persist_dir ^"/workers/log/"
                  ^ (N.fq_name func)
      | None -> "no_log_dir=") ;
    (* Owl also need HOME (See https://github.com/ryanrhymes/owl/issues/116) *)
    "HOME="^ getenv ~def:"/tmp" "HOME" |] in
  let%lwt pid =
    wrap (fun () -> run_background command [||] env) in
  func.N.pid <- Some pid ;
  (* Monitor this worker, wait for its termination, restart...: *)
  async (fun () ->
    let rec wait_child () =
      match%lwt Lwt_unix.waitpid [] pid with
      | exception Unix.Unix_error (Unix.EINTR, _, _) -> wait_child ()
      | exception exn ->
        (* This should not be used *)
        (* TODO: save this error on the func record *)
        !logger.error "Cannot wait for pid %d: %s"
          pid (Printexc.to_string exn) ;
        return_unit
      | _, status ->
        (* TODO: save this error on the func record *)
        !logger.info "Function %s (pid %d) %s."
          (N.fq_name func) pid (Helpers.string_of_process_status status) ;
        (* We might want to restart it: *)
        (match status with Unix.WSIGNALED signal when signal <> Sys.sigterm ->
          let%lwt () = Lwt_unix.sleep 3. in
          C.with_wlock conf (fun () ->
            (* Look again for that program by name: *)
            match C.find_func conf program.L.name func.N.name with
            | exception Not_found -> return_unit
            | program, func ->
                if program.persist.status <> Running then return_unit else (
                  !logger.info "Restarting func %s which is supposed to be running."
                    (N.fq_name func) ;
                  (* Note: run_func will start another waiter for that other worker. *)
                  run_func conf program func))
         | _ -> return_unit)
    in
    wait_child ()) ;
  (* Update the parents out_ringbuf_ref if it's in another program (otherwise
   * we have set the correct out_ringbuf_ref just above already) *)
  Lwt_list.iter_p (fun (parent_program, parent_name) ->
      if parent_program = program.name then
        return_unit
      else
        match C.find_func conf parent_program parent_name with
        | exception Not_found ->
          !logger.warning "Starting func %s which parent %s/%s does not \
                           exist yet"
            (N.fq_name func)
            parent_program parent_name ;
          return_unit
        | _, parent ->
          let out_ref =
            out_ringbuf_names_ref conf parent in
          (* The parent ringbuf might not exist yet if it has never been
           * started. If the parent is not running then it will overwrite
           * it when it starts, with whatever running children it will
           * have at that time (including us, if we are still running).  *)
          RamenOutRef.add out_ref (input_spec conf parent func)
    ) func.N.parents

(* Takes a locked conf *)
let run conf program =
  let open L in
  match program.persist.status with
  | Edition _ -> fail NotYetCompiled
  | Running -> fail AlreadyRunning
  | Compiling -> fail StillCompiling
  | Compiled ->
    !logger.info "Starting program %s" program.L.name ;
    (* First prepare all the required ringbuffers *)
    !logger.debug "Creating ringbuffers..." ;
    let program_funcs =
      Hashtbl.values program.persist.funcs |> List.of_enum in
    let rb_sz_words = 1000000 in
    let%lwt () = Lwt_list.iter_p (fun func ->
        wrap (fun () ->
          RingBuf.create (in_ringbuf_name conf func) rb_sz_words ;
          if Lang.Operation.is_exporting func.N.operation then
            RingBuf.create (exp_ringbuf_name conf func) rb_sz_words)
      ) program_funcs in
    (* Now run everything *)
    !logger.debug "Launching generated programs..." ;
    let now = Unix.gettimeofday () in
    let%lwt () = Lwt_list.iter_p (run_func conf program) program_funcs in
    L.set_status program Running ;
    program.L.persist.L.last_started <- Some now ;
    program.L.importing_threads <- Hashtbl.fold (fun _ func lst ->
        if Lang.Operation.is_exporting func.N.operation then (
          let rb = exp_ringbuf_name conf func in
          RamenExport.import_tuples conf rb func :: lst
        ) else lst
      ) program.L.persist.L.funcs [] ;
    return_unit

exception NotRunning

let stop conf program =
  match program.L.persist.L.status with
  | Edition _ | Compiled -> fail NotRunning
  | Compiling ->
    (* FIXME: do as for Running and make sure run() check the status hasn't
     * changed before launching workers. *)
    fail NotRunning
  | Running ->
    !logger.info "Stopping program %s" program.L.name ;
    let now = Unix.gettimeofday () in
    let program_funcs =
      Hashtbl.values program.L.persist.L.funcs |> List.of_enum in
    let%lwt () = Lwt_list.iter_p (fun func ->
        let k = RamenExport.history_key func in
        (match Hashtbl.find RamenExport.imported_tuples k with
        | exception Not_found -> ()
        | history -> RamenExport.archive_history conf history) ;
        match func.N.pid with
        | None ->
          !logger.error "Function %s has no pid?!" func.N.name ;
          return_unit
        | Some pid ->
          !logger.debug "Stopping func %s, pid %d" func.N.name pid ;
          (* Start by removing this worker ringbuf from all its parent output
           * references *)
          let this_in = in_ringbuf_name conf func in
          let%lwt () = Lwt_list.iter_p (fun (parent_program, parent_name) ->
              match C.find_func conf parent_program parent_name with
              | exception Not_found -> return_unit
              | _, parent ->
                let out_ref = out_ringbuf_names_ref conf parent in
                RamenOutRef.remove out_ref this_in
            ) func.N.parents in
          (* Get rid of the worker *)
          let open Unix in
          (try kill pid Sys.sigterm
           with Unix_error _ as e ->
            !logger.error "Cannot kill pid %d: %s" pid (Printexc.to_string e)) ;
          func.N.pid <- None ;
          return_unit
      ) program_funcs in
    L.set_status program Compiled ;
    program.L.persist.L.last_stopped <- Some now ;
    List.iter cancel program.L.importing_threads ;
    program.L.importing_threads <- [] ;
    return_unit

(* Timeout unused programs.
 * By unused, we mean either: no program depends on it, or no one cares for
 * what it exports. *)

let use_program now program =
  program.L.persist.L.last_used <- now

let use_program_by_name conf now program_name =
  Hashtbl.find conf.C.graph.C.programs program_name |>
  use_program now

let timeout_programs conf =
  (* Build the set of all defined and all used programs *)
  let defined, used = Hashtbl.fold (fun program_name program (defined, used) ->
      Set.add program_name defined,
      Hashtbl.fold (fun _func_name func used ->
          List.fold_left (fun used (parent_program, _parent_func) ->
              if parent_program = program_name then used
              else Set.add parent_program used
            ) used func.N.parents
        ) program.L.persist.L.funcs used
    ) conf.C.graph.C.programs (Set.empty, Set.empty) in
  let now = Unix.gettimeofday () in
  Set.iter (use_program_by_name conf now) used ;
  let unused = Set.diff defined used |>
               Set.to_list in
  Lwt_list.iter_p (fun program_name ->
      let program = Hashtbl.find conf.C.graph.C.programs program_name in
      if program.L.persist.L.timeout > 0. &&
         now > program.L.persist.L.last_used +. program.L.persist.L.timeout
      then (
        !logger.info "Deleting unused program %s after a %gs timeout"
          program_name program.L.persist.L.timeout ;
        (* Kill first, and only then forget about it. *)
        let%lwt () =
          try%lwt stop conf program with NotRunning -> return_unit in
        Hashtbl.remove conf.C.graph.C.programs program_name ;
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
        Hashtbl.replace last_reports worker SN.{
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
