(* Can depends on all Sync modules but ZMQClient must not depend on this,
 * so that it can be used in rmadmin without drawing too many dependencies. *)
open Batteries

module C = RamenConf
module RC = C.Running
module ZMQClient = RamenSyncZMQClient

(* Returns a hash of program_name to Program.t
 * TODO: to be cleaned once support for config files is removed. *)
let get_programs_sync () =
  let open RamenSync in
  let _zock, _session, clt = ZMQClient.get_connection () in
  let programs = Hashtbl.create 30 in
  Client.iter clt (fun k hv ->
    match k, hv.value with
    | Key.PerSite (_site, PerWorker (fq, Worker)),
      Value.Worker w ->
        let prog_name, _func_name = N.fq_parse fq in
        if not (Hashtbl.mem programs prog_name) then
          let prog = program_of_src_path clt w.Value.Worker.src_path |>
                     C.Program.unserialized prog_name in
          Hashtbl.add programs prog_name prog
    | _ -> ()) ;
  programs

let get_programs_local conf =
  RC.with_rlock conf identity |>
  Hashtbl.filter_map (fun _func_name (_mre, get_rc) ->
    match get_rc () with
    | exception _ -> None
    | prog -> Some prog)

let get_programs conf =
  if conf.C.sync_url = "" then
    get_programs_local conf
  else
    get_programs_sync ()

(* Helps to call ZMQClient.start with the parameters from RamenConf.conf: *)
let start_sync conf ?while_ ?topics ?on_progress ?on_sock ?on_synced
               ?on_new ?on_set ?on_del ?on_lock ?on_unlock
               ?conntimeo ?recvtimeo ?sndtimeo sync_loop =
  let url = conf.C.sync_url
  and srv_pub_key = conf.C.srv_pub_key
  and username = conf.C.username
  and clt_pub_key = conf.C.clt_pub_key
  and clt_priv_key = conf.C.clt_priv_key in
  ZMQClient.start ?while_ ~url ~srv_pub_key ~username ~clt_pub_key ~clt_priv_key
                  ?topics ?on_progress ?on_sock ?on_synced
                  ?on_new ?on_set ?on_del ?on_lock ?on_unlock
                  ?conntimeo ?recvtimeo ?sndtimeo sync_loop
