(* The configuration that's managed by this module includes:
 *
 * - the Running Configuration (RC), which is the set of all programs that are
 *   supposed to run;
 * - the per worker Stats (generated by the archivist with option --stats)
 *   and used for allocating storage space;
 * - the per worker storage allocations (also generated by the archivist with
 *   option --allocs)
 * - the transient ongoing replays.
 *
 * All these bits of configuration have in common that they must be available
 * at every sites at least read-only (also write where the corresponding
 * commands are issued).
 *)
open Batteries
open RamenLog
open RamenHelpersNoLog
open RamenHelpers
module O = RamenOperation
module N = RamenName
module E = RamenExpr
module T = RamenTypes
module Default = RamenConstsDefault
module Files = RamenFiles
module Retention = RamenRetention
module TimeRange = RamenTimeRange
module Versions = RamenVersions
module Globals = RamenGlobalVariables

(*
 * Ramen internal configuration record
 *
 * Just a handy bag of global parameters.
 *)

type conf =
  { log_level : log_level ;
    log_with_time : bool ;
    persist_dir : N.path ;
    users_dir : N.path ;
    test : bool ; (* true within `ramen test` *)
    keep_temp_files : bool ;
    reuse_prev_files : bool ;
    initial_export_duration : float ;
    site : N.site (* this site name *) ;
    masters : N.site Set.t ;
    bundle_dir : N.path ;
    sync_url : string ;
    username : string ;
    (* The keys not the file names: *)
    srv_pub_key : string ;
    clt_pub_key : string ;
    clt_priv_key : string ;
    (* Only useful to transfer to subcommands: *)
    forced_variants : string list }

type identity_file =
  { username : string ;
    server_public_key : string ;
    client_public_key : string ;
    client_private_key : string }
  [@@ppp PPP_JSON]

(* Read values from the file and set parameters with those.
 * In effect, the CLI parameters overwrite the file content. *)
let connection_parameters ?(username="") ?(srv_pub_key="") ?(clt_pub_key="")
                          ?(clt_priv_key="") ?(identity=N.path "") () =
  if N.is_empty identity || not (Files.exists identity) then
    username, srv_pub_key, clt_pub_key, clt_priv_key
  else
    let what = Printf.sprintf2 "Reading identity file %a"
                 N.path_print identity in
    log_exceptions ~what (fun () ->
      (* Ramen commands overwrite what's in the identity file with command line
       * supplied parameters, but for the parameters which associated envvar is
       * almost always present in the environment: *)
      let id = Files.ppp_of_file identity_file_ppp_json identity in
      (* Favor the username from the identity file over $USER: *)
      (if id.username <> "" then id.username else username),
      (* Favor keys passed as parameters over those from the identify file: *)
      (if srv_pub_key <> "" then srv_pub_key else id.server_public_key),
      (if clt_pub_key <> "" then clt_pub_key else id.client_public_key),
      (if clt_priv_key <> "" then clt_priv_key else id.client_private_key))

let make_conf
      ?(debug=false) ?(log_with_time=true) ?(quiet=false)
      ?(keep_temp_files=false) ?(reuse_prev_files=false)
      ?(forced_variants=[])
      ?(initial_export_duration=Default.initial_export_duration)
      ~site ?(test=false)
      ?(bundle_dir=RamenCompilConfig.default_bundle_dir)
      ?(masters=Set.empty)
      ?(sync_url="")
      ?username
      ?srv_pub_key
      ?clt_pub_key
      ?clt_priv_key
      ?identity
      ?(users_dir=N.path "")
      persist_dir =
  if debug && quiet then
    failwith "Options --debug and --quiet are incompatible." ;
  let log_level =
    if debug then Debug else if quiet then Quiet else Normal in
  let persist_dir = N.simplified_path persist_dir in
  let username, srv_pub_key, clt_pub_key, clt_priv_key =
    connection_parameters ?username ?srv_pub_key ?clt_pub_key ?clt_priv_key
                          ?identity () in
  RamenExperiments.set_variants persist_dir forced_variants ;
  let users_dir =
    if N.is_empty users_dir then
      N.path_cat [ persist_dir ; N.path "confserver/users" ]
    else
      users_dir in
  { log_level ; log_with_time ; persist_dir ; keep_temp_files ;
    reuse_prev_files ;
    initial_export_duration ; site ; test ; bundle_dir ; masters ;
    sync_url ; username ; srv_pub_key ; clt_pub_key ; clt_priv_key ;
    forced_variants ; users_dir }


(* Many messages related to starting up/tearing down, that are exceptional
 * in normal circumstances, are quite expected during tests: *)
let info_or_test conf =
  if conf.test then !logger.debug else !logger.info

(*
 * Global per-func stats that are updated by the thread reading #notifs and
 * the one reading the RC, and also saved on disk while ramen is not running:
 *)

module FuncStats =
struct
  type t =
    { startup_time : float ; (* To distinguish from present run *)
      min_etime : float option [@ppp_default None] ;
      max_etime : float option [@ppp_default None] ;
      tuples : int64 [@ppp_default 0L] ;
      bytes : int64 [@ppp_default 0L] ;
      cpu : float (* Cumulated seconds *) [@ppp_default 0.] ;
      ram : int64 (* Max observed heap size *) [@ppp_default 0L] ;
      mutable parents : (N.site * N.fq) list ;
      (* Also gather available history per running workers, to speed up
       * establishing query plans: *)
      mutable archives : TimeRange.t [@ppp_default []] ;
      mutable num_arc_files : int [@ppp_default 0] ;
      mutable num_arc_bytes : int64 [@ppp_default 0L] }

  let make ~startup_time =
    { startup_time ; min_etime = None ; max_etime = None ;
      tuples = 0L ; bytes = 0L ; cpu = 0. ; ram = 0L ; parents = [] ;
      archives = TimeRange.empty ; num_arc_files = 0 ; num_arc_bytes = 0L }

  let archives_print oc =
    List.print (pair_print Float.print Float.print) oc
end
