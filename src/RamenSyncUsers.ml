(* User management for remote access to the configuration synchronisation
 * service. *)
open Batteries
open RamenHelpers
open RamenLog
open RamenSync
module C = RamenConf
module Files = RamenFiles
module User = RamenSyncUser

(*
 * Command line actions:
 *)

let check_username username =
  if String.ends_with username ".del" then
    failwith "User names must not end with \".del\" as that's how deleted \
              users are renamed." ;
  if username = "" || username.[0] = '_' then
    failwith "User names must not be empty and not start with an underscore, \
              as those names are reserved for internal users." ;
  if String.contains username '/' then
    failwith "User names must not use the slash ('/') character."

let add conf output_file username roles srv_pub_key_file () =
  check_username username ;
  if User.Db.user_exists conf username then
    Printf.sprintf "A user named %s is already registered." username |>
    failwith ;
  (* Check we know the server url and public key. Not that it is necessary
   * to connect to confserver, but it must be written in the user identity
   * file: *)
  let srv_pub_key_file =
    if not (N.is_empty srv_pub_key_file) then srv_pub_key_file else
      C.default_srv_pub_key_file conf in
  let srv_pub_key =
    try Files.read_key false srv_pub_key_file
    with Unix.(Unix_error (ENOENT, _, _)) | Sys_error _ -> "" in
  if srv_pub_key = "" then
    !logger.warning "Without the server public key this user will only be \
                    allowed in insecure connections." ;
  let clt_pub_key, clt_priv_key = Zmq.Curve.keypair () in
  User.Db.make_user conf username roles clt_pub_key ;
  (fun f ->
    match output_file with
    | None -> f stdout
    | Some fname ->
        File.with_file_out ~mode:[`create; `text ; `excl]
                           (fname : N.path :> string) f ;
        !logger.info "User identity created in %a." N.path_print fname ;
        !logger.info "You should now transmit this file to this user and \
                      then delete it.")
    (fun oc ->
      PPP.to_string ~pretty:true C.identity_file_ppp_json
        { username ; srv_pub_key ; clt_pub_key ; clt_priv_key } |>
      String.print oc)

let del conf username () =
  if not (User.Db.user_exists conf username) then
    Printf.sprintf "Cannot find user named %s." username |>
    failwith ;
  let fname = User.Db.file_name conf username in
  Files.move_aside ~ext:"del" fname ;
  !logger.info "User %s has been deleted." username

let mod_ conf username roles () =
  match User.Db.lookup conf username with
  | exception Not_found ->
      Printf.sprintf "Cannot find user named %s." username |>
      failwith ;
  | prev ->
      let user = { prev with roles } in
      User.Db.save_user conf username user ;
      let print_roles = pretty_list_print User.Role.print in
      !logger.info "Previous roles: %a" print_roles prev.roles ;
      !logger.info "New roles: %a" print_roles roles
