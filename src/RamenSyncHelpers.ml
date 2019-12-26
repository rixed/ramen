(* Can depends on all Sync modules but ZMQClient must not depend on this,
 * so that it can be used in rmadmin without drawing too many dependencies. *)
open Batteries

open RamenLog
module C = RamenConf
module ZMQClient = RamenSyncZMQClient

(* Returns a hash of program_name to Program.t
 * TODO: to be cleaned once support for config files is removed. *)
let get_programs session =
  let open RamenSync in
  let programs = Hashtbl.create 30 in
  Client.iter session.ZMQClient.clt (fun k _hv ->
    match k with
    | Key.PerSite (_site, PerWorker (fq, Worker)) ->
        let prog_name, _func_name = N.fq_parse fq in
        if not (Hashtbl.mem programs prog_name) then
          let src_path = N.src_path_of_program prog_name in
          let prog = program_of_src_path session.clt src_path in
          Hashtbl.add programs prog_name prog
    | _ -> ()) ;
  (* TODO: get_session could return the topics and we could actually make
   * sure of this: *)
  if Hashtbl.is_empty programs then
    !logger.warning "No defined programs. Are we syncing workers?" ;
  programs

(* Helps to call ZMQClient.start with the parameters from RamenConf.conf: *)
let start_sync conf ?while_ ?topics ?on_progress ?on_sock ?on_synced
               ?on_new ?on_set ?on_del ?on_lock ?on_unlock
               ?conntimeo ~recvtimeo ?sndtimeo ?sesstimeo sync_loop =
  let url = conf.C.sync_url
  and srv_pub_key = conf.C.srv_pub_key
  and username = conf.C.username
  and clt_pub_key = conf.C.clt_pub_key
  and clt_priv_key = conf.C.clt_priv_key in
  ZMQClient.start ?while_ ~url ~srv_pub_key ~username ~clt_pub_key ~clt_priv_key
                  ?topics ?on_progress ?on_sock ?on_synced
                  ?on_new ?on_set ?on_del ?on_lock ?on_unlock
                  ?conntimeo ~recvtimeo ?sndtimeo ?sesstimeo sync_loop
