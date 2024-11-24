(* User management for remote access to the configuration synchronisation
 * service. *)
open Batteries

open RamenHelpersNoLog
open RamenLog
open RamenSync
module Authn = RamenAuthn
module C = RamenConf
module Files = RamenFiles
module User = RamenSyncUser
module Paths = RamenPaths

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

let add conf output_file username roles srv_pub_key_file
        also_dump_server_conf =
  check_username username ;
  if User.Db.user_exists conf.C.users_dir username then
    Printf.sprintf "A user named %s is already registered." username |>
    failwith ;
  (* Check we know the server url and public key. Not that it is necessary
   * to connect to confserver, but it must be written in the user identity
   * file: *)
  let srv_pub_key_file =
    if not (N.is_empty srv_pub_key_file) then srv_pub_key_file else
      Paths.default_srv_pub_key_file conf.C.persist_dir in
  let server_public_key =
    try Files.read_key false srv_pub_key_file
    with Unix.(Unix_error (ENOENT, _, _)) | Sys_error _ -> "" in
  if server_public_key = "" then
    !logger.warning "Without the server public key this user will only be \
                    allowed in insecure connections." ;
  let client_public_key, client_private_key = Authn.random_keypair () in
  User.Db.make_user conf.C.users_dir username roles client_public_key ;
  let output oc =
    let user_id =
      PPP.to_string C.identity_file_ppp_json
        { username ; server_public_key ; client_public_key ;
          client_private_key } in
    let user_id = user_id ^ "\n" in
    if also_dump_server_conf then
      let srv_conf_fname = User.Db.file_name conf.C.users_dir username in
      let srv_conf = Files.read_whole_file srv_conf_fname in
      Printf.fprintf oc
        "This is the server configuration:\n%s\n\n\
         And this is your identity file:\n%s\n"
        srv_conf user_id
    else
      String.print oc user_id in
  match output_file with
  | None ->
      output stdout
  | Some fname ->
      File.with_file_out ~mode:[`create; `text ; `excl]
                         (fname : N.path :> string) output ;
      !logger.info "User identity created in %a for user %s."
        N.path_print fname username ;
      !logger.info "You should now transmit this file to this user and \
                    then delete it."

let del conf username =
  if not (User.Db.user_exists conf.C.users_dir username) then
    Printf.sprintf "Cannot find user named %s." username |>
    failwith ;
  let fname = User.Db.file_name conf.C.users_dir username in
  Files.move_aside ~ext:"del" fname ;
  !logger.info "User %s has been deleted." username

let mod_ conf username roles =
  match User.Db.lookup conf.C.users_dir username with
  | exception Not_found ->
      Printf.sprintf "Cannot find user named %s." username |>
      failwith ;
  | prev ->
      let user = { prev with roles } in
      User.Db.save_user conf.C.users_dir username user ;
      let print_roles = pretty_list_print User.Role.print in
      !logger.info "Previous roles: %a" print_roles prev.roles ;
      !logger.info "New roles: %a" print_roles roles
