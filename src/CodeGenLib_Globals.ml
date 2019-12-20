(* Have a single Lmdb environment for all variables: *)
open Batteries
open RamenHelpers
open RamenConsts
module Globals = RamenGlobalVariables
module Files = RamenFiles

let max_global_variables = ref Default.max_global_variables
let db_path = ref (N.path "")  (* Must be overwritten at init *)

let init ?max_globals globals_dir =
  let set_opt r = function
    | None -> ()
    | Some v -> r := v
  in
  set_opt max_global_variables max_globals ;
  db_path := globals_dir ;
  Files.mkdir_all globals_dir

let get_env =
  memoize (fun () ->
    assert (not (N.is_empty !db_path)) ;
    Lmdb.Env.(create Rw ~max_maps:!max_global_variables
                     ~flags:Flags.write_map (!db_path :> string)))

module type MAKE_CONFIG =
sig
  (* [scope_id] is prepended to the global variable name, to limit its scope.
   * If scope is:
   * - global: haha TODO
   * - site: empty string
   * - program: name of this program *)
  val scope_id : string

  type k
  val k_conv : k Lmdb.Conv.t

  type v
  val v_conv : v Lmdb.Conv.t
end

(* TODO: start/commit transactions *)

module MakeMap (Conf : MAKE_CONFIG) =
struct
  let init var_name =
    (* Avoid initializing the database before it's needed (as [init] has to be
     * called first, and the worker may be run just for info, archive
     * conversion, as a replayer or a top-half *)
    memoize (fun () ->
      let env = get_env () in
      let key = Conf.k_conv and value = Conf.v_conv in
      let name = Conf.scope_id ^"/"^ var_name in
      let map = Lmdb.Map.create Nodup ~key ~value ~name env in
      let get ?(txn : [< `Read] Lmdb.Txn.t option) k =
        Lmdb.Map.get map ?txn k
      and set ?(txn : [< `Write] Lmdb.Txn.t option) k v =
        Lmdb.Map.set map ?txn k v in
      get, set)
end
