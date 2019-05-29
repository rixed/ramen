(* The actual running sync server daemon *)
open Batteries
open RamenHelpers
open RamenLog
open RamenSync
open RamenConsts
module Archivist = RamenArchivist
module Files = RamenFiles
module Processes = RamenProcesses
module FuncGraph = RamenFuncGraph
module Server = RamenSyncServer.Make (Value) (Selector)
module CltMsg = Server.CltMsg
module SrvMsg = Server.SrvMsg
module User = RamenSync.User
module Capa = RamenSync.Capacity
module C = RamenConf
module RC = C.Running
module FS = C.FuncStats
module F = C.Func
module P = C.Program
module T = RamenTypes
module O = RamenOperation
module Services = RamenServices

let u = User.internal

(* Helper function that takes a key filter and a new set of values, and
 * replaces all keys matching the filter with the new set of values. *)
let replace_keys srv f h =
  (* Start by deleting the extraneous keys: *)
  Server.H.enum srv.Server.h //
  (fun (k, hv) ->
    (* Skip over locked keys for now (TODO: timeout every locks!) *)
    (hv.Server.l = None || hv.Server.l = Some User.internal) &&
    f k &&
    not (Server.H.mem h k)) |>
  Enum.iter (fun (k, _hv) -> Server.del srv u k) ;
  (* Now add/update the keys from [h]: *)
  Server.H.iter (fun k (v, r, w, s) ->
    Option.may (fun v ->
      if Server.H.mem srv.Server.h k then
        Server.set srv u k v
      else
        Server.create_unlocked srv k v ~r ~w ~s
    ) v
  ) h

(* One module per data source so that it's easier to track those *)

module type CONF_SYNCER =
sig
  val init : C.conf -> Server.t -> unit
  val update : C.conf -> Server.t -> unit
end

module DevNull : CONF_SYNCER =
struct
  let init _conf srv =
    let on_set _ v =
      !logger.debug "Some clown wrote into DevNull: %a"
        Value.print v ;
      Some (Value.String "")
    in
    Server.register_callback
      srv srv.on_sets on_set (Globs.escape "devnull") ;
    let devnull = Value.String "Waldo" in
    Server.create_unlocked
      srv DevNull devnull ~r:Capa.nobody ~w:Capa.anybody ~s:true

  let update _conf _srv = ()
end

module GraphInfo : CONF_SYNCER =
struct
  let update_from_graph conf srv graph =
    !logger.debug "Update per-site configuration with graph %a"
      FuncGraph.print graph ;
    let h = Server.H.create 50 in
    let upd ?(r=Capa.anybody) ?(w=Capa.nobody) ?(s=true) k v =
      Server.H.add h k (Some v, r, w, s) in
    Hashtbl.iter (fun site per_site_h ->
      let is_master = Set.mem site conf.C.masters in
      upd (PerSite (site, IsMaster)) (Value.Bool is_master) ;
      (* TODO: PerService *)
      let stats = Archivist.load_stats ~site conf in
      Hashtbl.iter (fun (pname, fname) ge ->
        let fq = N.fq_of_program pname fname in
        (* IsUsed *)
        upd (PerSite (site, PerWorker (fq, IsUsed)))
            (Value.Bool ge.FuncGraph.used) ;
        (* Parents *)
        set_iteri (fun i (psite, pprog, pfunc) ->
          upd (PerSite (site, PerWorker (fq, Parents i)))
              (Value.Worker (psite, pprog, pfunc))
        ) ge.FuncGraph.parents ;
        (* Stats *)
        (match Hashtbl.find stats fq with
        | exception Not_found -> ()
        | stats ->
            upd (PerSite (site, PerWorker (fq, StartupTime)))
                (Value.Float stats.FS.startup_time) ;
            Option.may (fun min_etime ->
              upd (PerSite (site, PerWorker (fq, MinETime)))
                  (Value.Float min_etime) ;
            ) stats.FS.min_etime ;
            Option.may (fun max_etime ->
              upd (PerSite (site, PerWorker (fq, MaxETime)))
                  (Value.Float max_etime) ;
            ) stats.FS.max_etime ;
            upd (PerSite (site, PerWorker (fq, TotTuples)))
                (Value.Int stats.FS.tuples) ;
            upd (PerSite (site, PerWorker (fq, TotBytes)))
                (Value.Int stats.FS.bytes) ;
            upd (PerSite (site, PerWorker (fq, TotCpu)))
                (Value.Float stats.FS.cpu) ;
            upd (PerSite (site, PerWorker (fq, MaxRam)))
                (Value.Int stats.FS.ram) ;
            upd (PerSite (site, PerWorker (fq, ArchivedTimes)))
                (Value.TimeRange stats.FS.archives) ;
            upd (PerSite (site, PerWorker (fq, NumArcFiles)))
                (Value.Int (Int64.of_int stats.FS.num_arc_files)) ;
            upd (PerSite (site, PerWorker (fq, NumArcBytes)))
                (Value.Int stats.FS.num_arc_bytes)) ;
      ) per_site_h
    ) graph.FuncGraph.h ;
    let f = function
      | Key.PerSite
          (_, (IsMaster |
              (PerWorker
                (_, (IsUsed | Parents _ | StartupTime | MinETime |
                 MaxETime | TotTuples | TotBytes | TotCpu | MaxRam |
                 ArchivedTimes))))) -> true
      | _ -> false in
    replace_keys srv f h

  let init _conf _srv = ()

  let update conf srv =
    match FuncGraph.make conf with
    | exception e ->
        print_exception ~what:"update PerSite" e ;
        !logger.info "skipping this step..."
    | graph ->
        update_from_graph conf srv graph
end

module Storage : CONF_SYNCER =
struct
  let storage_user_conf_dirty = ref false
  let last_read_user_conf = ref 0.

  let init _conf srv =
    (* Create the minimal set of (sticky) keys: *)
    let r = Capa.Anybody
    and w = Capa.Admin
    and s = true in
    Server.create_unlocked srv (Storage TotalSize) Value.dummy ~r ~w ~s ;
    Server.create_unlocked srv (Storage RecallCost) Value.dummy ~r ~w ~s ;
    (* On any change down there, set the dirty flag: *)
    let set_dirty _ v =
      storage_user_conf_dirty := true ;
      Some v in
    let sel = Globs.escape "storage/*" in
    Server.register_callback srv srv.on_sets set_dirty sel ;
    Server.register_callback srv srv.on_news set_dirty sel ;
    Server.register_callback srv srv.on_dels set_dirty sel

  let update conf srv =
    let fname = Archivist.user_conf_file conf in
    let t = Files.mtime_def 0. fname in
    if t > !last_read_user_conf then (
      !logger.info "Updating storage configuration from %a"
        N.path_print fname ;
      last_read_user_conf := t ;
      let user_conf = Archivist.get_user_conf conf in
      Server.set srv u (Storage TotalSize)
                 Value.(Int (Int64.of_int user_conf.Archivist.size_limit)) ;
      Server.set srv u (Storage RecallCost)
                 Value.(Float user_conf.recall_cost) ;
      let h = Server.H.create 20 in
      let upd k v =
        Server.H.add h k (Some v, Capa.Anybody, Capa.Admin, false) in
      Hashtbl.iter (fun glob retention ->
        upd (Storage (RetentionsOverride glob))
            Value.(Retention retention)
        (* TODO: transactions! (option of replace_keys?) *)
      ) user_conf.retentions ;
      let f = function
        | Key.Storage (RetentionsOverride _) -> true
        | _ -> false in
      replace_keys srv f h
    ) else (
      (* in the other way around: if conf is dirty save the file.
       * TODO *)
      ignore storage_user_conf_dirty
    )
end

(* Additional infos we get from the binaries *)
module BinInfo : CONF_SYNCER =
struct
  let init _conf _srv = ()

  let update conf srv =
    let h = Server.H.create 20 in
    let upd ?(r=Capa.anybody) ?(w=Capa.nobody) ?(s=true) k v =
      Server.H.add h k (Some v, r, w, s) in
    RC.with_rlock conf identity |>
    Hashtbl.iter (fun pname (rce, _get_rc) ->
      upd Key.(PerProgram (pname, Enabled))
          Value.(Bool RC.(rce.status = MustRun)) ;
      upd Key.(PerProgram (pname, Debug))
          Value.(Bool rce.debug) ;
      upd Key.(PerProgram (pname, ReportPeriod))
          Value.(Float rce.report_period) ;
      upd Key.(PerProgram (pname, BinPath))
          Value.(String (rce.bin :> string)) ;
      upd Key.(PerProgram (pname, SrcPath))
          Value.(String (rce.src_file :> string)) ;
      Hashtbl.iter (fun pn pv ->
        upd Key.(PerProgram (pname, Param pn))
            Value.(String (IO.to_string T.print pv))
      ) rce.params ;
      upd Key.(PerProgram (pname, OnSite))
          Value.(String (Globs.decompile rce.on_site)) ;
      upd Key.(PerProgram (pname, Automatic))
          Value.(Bool rce.automatic)) ;
    let f = function
      | Key.PerProgram (_, (Enabled | Debug | ReportPeriod | BinPath |
                            SrcPath | Param _ | OnSite | Automatic)) -> true
      | _ -> false in
    replace_keys srv f h
end

module SrcInfo : CONF_SYNCER =
struct
  let init _conf _srv = ()

  let update conf srv =
    let h = Server.H.create 20 in
    let upd ?(r=Capa.anybody) ?(w=Capa.nobody) ?(s=true) k v =
      Server.H.add h k (v, r, w, s) in
    RC.with_rlock conf identity |>
    Hashtbl.iter (fun pname (rce, get_rc) ->
      (match Files.mtime rce.RC.src_file with
      | exception Unix.(Unix_error (ENOENT, _, _)) ->
          () (* No file -> No source info *)
      | mtime ->
          let km = Key.PerProgram (pname, SourceModTime)
          and ks = Key.PerProgram (pname, SourceFile) in
          let do_upd () =
            upd km (Some Value.(Float mtime)) ;
            upd ks (Some Value.(String (rce.RC.src_file :> string)))
          and keep k =
            upd k None in
          (match Server.H.find srv.Server.h km with
          | exception Not_found -> do_upd ()
          | { v = Value.(Float prev_mtime) ; _ } ->
              if mtime > prev_mtime then do_upd ()
              else (keep km ; keep ks)
          | hv ->
              !logger.error
                "Wrong type for source modification time (%a), deleting"
                Value.print hv.v)) ;
      (match get_rc () with
      | exception _ -> ()
      | p ->
          Option.may (fun condition ->
            upd (Key.PerProgram (pname, RunCondition))
                (Some Value.(String condition))
          ) p.P.condition ;
          List.iter (fun f ->
            let upd fk v =
              upd (Key.PerProgram (pname, PerFunction (f.F.name, fk)))
                  (Some v) in
            Option.may (fun retention ->
              upd Key.Retention Value.(Retention retention)
            ) f.F.retention ;
            upd Key.Doc Value.(String f.F.doc) ;
            upd Key.IsLazy Value.(Bool f.F.is_lazy) ;
            let op = f.F.operation in
            upd Key.Operation Value.(String
              (IO.to_string (O.print true) op)) ;
            List.iteri (fun i (field : N.field) ->
              upd Key.(Factors i) Value.(String (field :> string))
            ) (O.factors_of_operation op) ;
            upd Key.InType Value.(RamenType
              (RamenFieldMaskLib.record_of_in_type f.F.in_type)) ;
            upd Key.OutType Value.(RamenType
              (O.out_record_of_operation ~with_private:false op)) ;
            upd Key.Signature Value.(String f.F.signature) ;
            upd Key.MergeInputs Value.(Bool f.F.merge_inputs)
          ) p.funcs)) ;
    let f = function
      | Key.PerProgram (_, (SourceModTime | SourceFile)) -> true
      | _ -> false in
    replace_keys srv f h
end

module AllocInfo : CONF_SYNCER =
struct
  let last_read_allocs = ref 0.

  let init _conf _srv = ()

  let update conf srv =
    let fname = Archivist.allocs_file conf in
    let t = Files.mtime_def 0. fname in
    if t > !last_read_allocs then (
      !logger.info "Updating storage allocations from %a"
        N.path_print fname ;
      last_read_allocs := t ;
      let allocs = Archivist.load_allocs conf in
      let h = Server.H.create 20 in
      let upd k v =
        Server.H.add h k (Some v, Capa.Anybody, Capa.Admin, false) in
      Hashtbl.iter (fun (site, fq) size ->
        upd (PerSite (site, PerWorker (fq, AllocedArcBytes)))
            Value.(Int (Int64.of_int size))
      ) allocs ;
      let f = function
        | Key.PerSite (_, PerWorker (_, AllocedArcBytes)) -> true
        | _ -> false in
      replace_keys srv f h
    )
end


(*
 * The service: populate and update in a loop.
 * TODO: Save the conf from time to time in a user friendly format, and get
 * rid of other data sources.
 *)

let populate_init conf srv =
  !logger.info "Populating the configuration..." ;
  DevNull.init conf srv ;
  GraphInfo.init conf srv ;
  Storage.init conf srv ;
  BinInfo.init conf srv ;
  SrcInfo.init conf srv ;
  AllocInfo.init conf srv

let sync_step conf srv =
  log_and_ignore_exceptions ~what:"update DevNull"
    (DevNull.update conf) srv ;
  log_and_ignore_exceptions ~what:"update GraphInfo"
    (GraphInfo.update conf) srv ;
  log_and_ignore_exceptions ~what:"update Storage"
    (Storage.update conf) srv ;
  log_and_ignore_exceptions ~what:"update BinInfo"
    (BinInfo.update conf) srv ;
  log_and_ignore_exceptions ~what:"update SrcInfo"
    (SrcInfo.update conf) srv ;
  log_and_ignore_exceptions ~what:"update AllocInfo"
    (AllocInfo.update conf) srv

let last_tuples = Hashtbl.create 50

let zock_step srv zock =
  let peel_multipart msg =
    let too_short l =
      Printf.sprintf "Invalid zmq message with only %d parts" l |>
      failwith in
    let rec look_for_delim l = function
      | [] -> too_short l
      | "" :: rest -> rest
      | _ :: rest -> look_for_delim (l + 1) rest in
    match msg with
      | [] -> too_short 0
      | peer :: rest ->
          peer, look_for_delim 1 rest
  in
  match Zmq.Socket.recv_all ~block:false zock with
  | exception Unix.Unix_error (Unix.EAGAIN, _, _) ->
      ()
  | parts ->
      !logger.info "0MQ: Received message %a" 
        (List.print String.print_quoted) parts ;
      (match peel_multipart parts with
      | peer, [ msg ] ->
          log_and_ignore_exceptions (fun () ->
            let u = User.of_zmq_id peer in
            let m = CltMsg.of_string msg in
            Server.process_msg srv u m ;
            (* Special case: we automatically, and silently, prune old
             * entries under "lasts/" directories (only after a new entry has
             * successfully been added there).
             * TODO: in theory, also monitor DelKey to update last_tuples
             * secondary hash. *)
            (match m with
            | _, CltMsg.NewKey (Key.(Tail (site, fq, LastTuple seq)), _) ->
                Hashtbl.modify_opt (site, fq) (function
                  | None ->
                      let seqs =
                        Array.init max_last_tuples (fun i ->
                          if i = 0 then seq else seq-1) in
                      Some (1, seqs)
                  | Some (n, seqs) ->
                      let to_del = Key.(Tail (site, fq, LastTuple seqs.(n))) in
                      !logger.info "Removing old tuple seq %d" seqs.(n) ;
                      Server.H.remove srv.Server.h to_del ;
                      seqs.(n) <- seq ;
                      Some ((n + 1) mod max_last_tuples, seqs)
                ) last_tuples
            | _ -> ())
          ) ()
      | _, parts ->
          Printf.sprintf "Invalid message with %d parts"
            (List.length parts) |>
          failwith)

let service_loop conf zock srv =
  Processes.until_quit (fun () ->
    sync_step conf srv ;
    zock_step srv zock ;
    Unix.sleepf 0.3 ; (* FIXME *)
    true
  )

let send_msg zock ?block m us =
  let msg = SrvMsg.to_string m in
  Enum.iter (fun u ->
    let peer = User.zmq_id u in
    !logger.debug "0MQ: Sending message %S to %S" msg peer ;
    Zmq.Socket.send_all ?block zock [ peer ; "" ; msg ]
  ) us

let start conf port =
  let ctx = Zmq.Context.create () in
  finally
    (fun () ->
      !logger.info "Terminating 0MQ" ;
      Zmq.Context.terminate ctx)
    (fun () ->
      let zock = Zmq.Socket.(create ctx router) in
      let send_msg = send_msg zock in
      let srv = Server.make ~send_msg in
      populate_init conf srv ;
      finally
        (fun () ->
          Zmq.Socket.close zock)
        (fun () ->
          (* (* For locally running tools: *)
             Zmq.Socket.bind zock "ipc://ramen_conf_server.ipc" ; *)
          (* For the rest of the world: *)
          Zmq.Socket.set_send_high_water_mark zock 0 ;
          let bind_to = "tcp://*:"^ string_of_int port in
          !logger.info "Listening to %s..." bind_to ;
          Zmq.Socket.bind zock bind_to ;
          service_loop conf zock srv
        ) ()
    ) ()


(*
 * Test client
 *)

let test_client conf creds =
  let ctx = Zmq.Context.create () in
  finally
    (fun () -> Zmq.Context.terminate ctx)
    (fun () ->
      let zock = Zmq.Socket.(create ctx dealer) in
      finally
        (fun () -> Zmq.Socket.close zock)
        (fun () ->
          let connect_to = "tcp://"^ conf.C.sync_url in
          !logger.info "Connecting to %s..." connect_to ;
          Zmq.Socket.connect zock connect_to ;
          (* 1: send a wrong message and see what happen *)
          !logger.info "Sending a wrong auth..." ;
          Zmq.Socket.send_all zock [ "" ; "1 AU Waldo?" ] ;
          !logger.info "Waiting for an answer..." ;
          let answer = Zmq.Socket.recv_all zock in
          let print_msg fmt = List.print String.print_quoted fmt in
          !logger.info "Received %a" print_msg answer ;
          assert (String.sub (List.at answer 1) 0 3 = "AU ") ;
          !logger.info "Sending correct auth this time..." ;
          Zmq.Socket.send_all zock [ "" ; "1 AU "^ creds ] ;
          let answer = Zmq.Socket.recv_all zock in
          !logger.info "Auth answer: %a" print_msg answer ;
          !logger.info "Starting to sync..." ;
          Zmq.Socket.send_all zock [ "" ; "2 SS *" ] ;
          !logger.info "Receiving:" ;
          forever (fun () ->
            match Zmq.Socket.recv_all zock with
            | [ ""; s ] ->
                let msg = SrvMsg.of_string s in
                !logger.info "%a" SrvMsg.print msg
            | lst ->
                !logger.info "%a" print_msg lst
          ) ()
        ) ()
    ) ()
