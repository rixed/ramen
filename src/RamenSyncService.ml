(* The actual running sync server daemon *)
open Batteries
open RamenHelpers
open RamenLog
open RamenSync
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
module FS = C.FuncStats

let u = User.internal

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

module PerSite : CONF_SYNCER =
struct
  let init conf srv =
    let graph = FuncGraph.make conf in
    !logger.debug "Populate per-site configuration with graph %a"
      FuncGraph.print graph ;
    Hashtbl.iter (fun site per_site_h ->
      let is_master = Set.mem site conf.C.masters in
      Server.create_unlocked srv
        (PerSite (site, IsMaster)) (Value.Bool is_master)
        ~r:Capa.anybody ~w:Capa.nobody ~s:true ;
      (* TODO: PerService *)
      let stats = Archivist.load_stats ~site conf in
      Hashtbl.iter (fun (pname, fname) ge ->
        let fq = N.fq_of_program pname fname in
        (* IsUsed *)
        Server.create_unlocked
          srv (PerSite (site, PerFunction (fq, IsUsed)))
          (Value.Bool ge.FuncGraph.used)
          ~r:Capa.anybody ~w:Capa.nobody ~s:true ;
        (* Parents *)
        set_iteri (fun i (psite, pprog, pfunc) ->
          Server.create_unlocked
            srv (PerSite (site, PerFunction (fq, Parents i)))
            (Value.Worker (psite, pprog, pfunc))
            ~r:Capa.anybody ~w:Capa.nobody ~s:true
        ) ge.FuncGraph.parents ;
        (* Stats *)
        (match Hashtbl.find stats fq with
        | exception Not_found -> ()
        | stats ->
            Server.create_unlocked
              srv (PerSite (site, PerFunction (fq, StartupTime)))
              (Value.Float stats.FS.startup_time)
              ~r:Capa.anybody ~w:Capa.nobody ~s:true ;
            Option.may (fun min_etime ->
              Server.create_unlocked
                srv (PerSite (site, PerFunction (fq, MinETime)))
                (Value.Float min_etime)
                ~r:Capa.anybody ~w:Capa.nobody ~s:true
            ) stats.FS.min_etime ;
            Option.may (fun max_etime ->
              Server.create_unlocked
                srv (PerSite (site, PerFunction (fq, MaxETime)))
                (Value.Float max_etime)
                ~r:Capa.anybody ~w:Capa.nobody ~s:true
            ) stats.FS.max_etime ;
            Server.create_unlocked
              srv (PerSite (site, PerFunction (fq, TotTuples)))
              (Value.Int stats.FS.tuples)
              ~r:Capa.anybody ~w:Capa.nobody ~s:true ;
            Server.create_unlocked
              srv (PerSite (site, PerFunction (fq, TotBytes)))
              (Value.Int stats.FS.bytes)
              ~r:Capa.anybody ~w:Capa.nobody ~s:true ;
            Server.create_unlocked
              srv (PerSite (site, PerFunction (fq, TotCpu)))
              (Value.Float stats.FS.cpu)
              ~r:Capa.anybody ~w:Capa.nobody ~s:true ;
            Server.create_unlocked
              srv (PerSite (site, PerFunction (fq, MaxRam)))
              (Value.Int stats.FS.ram)
              ~r:Capa.anybody ~w:Capa.nobody ~s:true ;
            Server.create_unlocked
              srv (PerSite (site, PerFunction (fq, ArchivedTimes)))
              (Value.TimeRange stats.FS.archives)
              ~r:Capa.anybody ~w:Capa.nobody ~s:true) ;
      ) per_site_h
    ) graph.FuncGraph.h

  let update conf srv =
    let graph = FuncGraph.make conf in
    !logger.debug "Update per-site configuration with graph %a"
      FuncGraph.print graph ;
    (* First, grab all existing keys, to know what to delete later: *)
    let key_used =
      Server.H.filter_map (fun k hv ->
        (* Skip over locked keys for now (TODO: timeout every locks!) *)
        if hv.Server.l <> None && hv.Server.l <> Some User.internal then
          None
        else
          match k with
          | PerSite _ -> Some false
          | _ -> None
      ) srv.Server.h in
    (* For every key the process is the same:
     * - If it's not already there, add it unlocked;
     * - If it is bound to the same value, pass;
     * - If it is bound to another value, and unlocked, set the value;
     * - If it is bound to another value, and is locked, that's a bug. *)
    let upd k v ~r ~w ~s =
      match Server.H.find srv.Server.h k with
      | exception Not_found ->
          Server.create_unlocked srv k v ~r ~w ~s
      | Server.{ l = Some u' } when not (User.equal u' u) ->
          !logger.error "Ignoring key %a that is locked by user %a"
            Key.print k User.print u'
      | _ ->
          Server.H.replace key_used k true ;
          (* Will deal with equal values itself: *)
          Server.set srv u k v
    in
    Hashtbl.iter (fun site per_site_h ->
      let is_master = Set.mem site conf.C.masters in
      upd (PerSite (site, IsMaster)) (Value.Bool is_master)
          ~r:Capa.anybody ~w:Capa.nobody ~s:true ;
      (* TODO: PerService *)
      let stats = Archivist.load_stats ~site conf in
      Hashtbl.iter (fun (pname, fname) ge ->
        let fq = N.fq_of_program pname fname in
        (* IsUsed *)
        upd (PerSite (site, PerFunction (fq, IsUsed)))
            (Value.Bool ge.FuncGraph.used)
            ~r:Capa.anybody ~w:Capa.nobody ~s:true ;
        (* Parents *)
        set_iteri (fun i (psite, pprog, pfunc) ->
          upd (PerSite (site, PerFunction (fq, Parents i)))
              (Value.Worker (psite, pprog, pfunc))
              ~r:Capa.anybody ~w:Capa.nobody ~s:true
        ) ge.FuncGraph.parents ;
        (* Stats *)
        (match Hashtbl.find stats fq with
        | exception Not_found -> ()
        | stats ->
            upd (PerSite (site, PerFunction (fq, StartupTime)))
                (Value.Float stats.FS.startup_time)
                ~r:Capa.anybody ~w:Capa.nobody ~s:true ;
            Option.may (fun min_etime ->
              upd (PerSite (site, PerFunction (fq, MinETime)))
                  (Value.Float min_etime)
                  ~r:Capa.anybody ~w:Capa.nobody ~s:true
            ) stats.FS.min_etime ;
            Option.may (fun max_etime ->
              upd (PerSite (site, PerFunction (fq, MaxETime)))
                  (Value.Float max_etime)
                  ~r:Capa.anybody ~w:Capa.nobody ~s:true
            ) stats.FS.max_etime ;
            upd (PerSite (site, PerFunction (fq, TotTuples)))
                (Value.Int stats.FS.tuples)
                ~r:Capa.anybody ~w:Capa.nobody ~s:true ;
            upd (PerSite (site, PerFunction (fq, TotBytes)))
                (Value.Int stats.FS.bytes)
                ~r:Capa.anybody ~w:Capa.nobody ~s:true ;
            upd (PerSite (site, PerFunction (fq, TotCpu)))
                (Value.Float stats.FS.cpu)
                ~r:Capa.anybody ~w:Capa.nobody ~s:true ;
            upd (PerSite (site, PerFunction (fq, MaxRam)))
                (Value.Int stats.FS.ram)
                ~r:Capa.anybody ~w:Capa.nobody ~s:true ;
            upd (PerSite (site, PerFunction (fq, ArchivedTimes)))
                (Value.TimeRange stats.FS.archives)
                ~r:Capa.anybody ~w:Capa.nobody ~s:true) ;
      ) per_site_h
    ) graph.FuncGraph.h ;
    (* Now delete unused keys: *)
    Server.H.iter (fun k used ->
      if not used then Server.del srv u k
    ) key_used


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
      let r = Capa.Anybody and w = Capa.Admin in
      Hashtbl.iter (fun glob retention ->
        Server.create_or_update srv (Storage (RetentionsOverride glob))
                                Value.(Retention retention) ~r ~w ~s:false
        (* TODO: delete the left over from previous version *)
        (* TODO: transactions! *)
      ) user_conf.retentions
    ) else (
      (* in the other way around: if conf is dirty save the file.
       * TODO *)
    )
end

(*
 *
 *)

let populate_init conf srv =
  !logger.info "Populating the configuration..." ;
  DevNull.init conf srv ;
  PerSite.init conf srv ;
  Storage.init conf srv

let sync_step conf srv =
  DevNull.update conf srv ;
  PerSite.update conf srv ;
  Storage.update conf srv

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
            Server.process_msg srv u m
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

let send_msg zock m us =
  let msg = SrvMsg.to_string m in
  Enum.iter (fun u ->
    let peer = User.zmq_id u in
    !logger.debug "0MQ: Sending message %S to %S" msg peer ;
    Zmq.Socket.send_all ~block:false zock [ peer ; "" ; msg ]
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
          let bind_to = "tcp://*:"^ string_of_int port in
          !logger.info "Listening to %s..." bind_to ;
          Zmq.Socket.bind zock bind_to ;
          service_loop conf zock srv
        ) ()
    ) ()


(*
 * Test client
 *)

let test_client _conf url =
  let ctx = Zmq.Context.create () in
  finally
    (fun () -> Zmq.Context.terminate ctx)
    (fun () ->
      let zock = Zmq.Socket.(create ctx dealer) in
      finally
        (fun () -> Zmq.Socket.close zock)
        (fun () ->
          let connect_to = "tcp://"^ url in
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
          Zmq.Socket.send_all zock [ "" ; "1 AU tintin" ] ;
          !logger.info "There should be no answer..." ;
          Unix.sleepf 0.5 ;
          (match Zmq.Socket.recv_all ~block:false zock with
          | exception Unix.(Unix_error (EAGAIN, _, _)) ->
              !logger.info "Indeed, no answer."
          | s ->
              !logger.error "Got answer: %a" print_msg s ;
              assert false) ;
          !logger.info "Starting to sync..." ;
          Zmq.Socket.send_all zock [ "" ; "2 SS *" ] ;
          !logger.info "Receiving:" ;
          forever (fun () ->
            !logger.info "%a" print_msg (Zmq.Socket.recv_all zock)
          ) ()
        ) ()
    ) ()
