open Batteries
open RamenLog
open RamenHelpers
module N = RamenName
module Files = RamenFiles

type id = string [@@ppp PPP_OCaml]

let print_id = String.print

module Role =
struct
  type t =
    | Admin (* The role of administrating ramen *)
    | User  (* The role of manipulating the data *)
    | Specific of id  (* The "role" of being someone in particular *)
    [@@ppp PPP_OCaml]

  let print oc = function
    | Admin -> String.print oc "admin"
    | User -> String.print oc "user"
    | Specific uid -> Printf.fprintf oc "%a" print_id uid

  let equal = (=)
end

type t =
  (* Internal implies no authn at all, only for when the messages do not go
   * through ZMQ: *)
  | Internal
  | Ramen of string  (* A ramen daemon, assumes all roles *)
  | Auth of { name : string ; roles : Role.t Set.t }
  | Anonymous

let internal = Internal

let equal = (=)

module PubCredentials =
struct
  type t = { username : string ; clt_pub_key : string }
  let print oc t =
    Printf.fprintf oc "{ username=%s; public-key=%s }"
      t.username t.clt_pub_key
end

module Db =
struct
  (* Each user is represented on the server by a single file named after the
   * user name and containing its roles and its long term public key (with
   * PPP.OCaml format).
   * The private key is only printed on the console when the user is created, and
   * supposed to be send securely to the user (along with his public key).  *)
  type user =
    { (* SingleUser + Anybody are granted automatically so there is no need to
       * specify them: *)
      roles : Role.t list ;
      clt_pub_key : string }
    [@@ppp PPP_OCaml]

  (* Files where the catalog of users are stored: *)
  let file_name conf username  =
    N.path_cat [ conf.RamenConf.persist_dir ; N.path "users" ; N.path username ]

  (* Lookup a user by name and return its conf if found: *)
  let lookup conf username  =
    let fname = file_name conf username in
    try Files.ppp_of_file ~errors_ok:true user_ppp_ocaml fname
    with Unix.(Unix_error (ENOENT, _, _)) | Sys_error _ ->
      raise Not_found

  let save_user conf username user =
    let fname = file_name conf username in
    Files.ppp_to_file ~pretty:true fname user_ppp_ocaml user

  let make_user conf username roles clt_pub_key =
    let user = { roles ; clt_pub_key } in
    save_user conf username user

  let user_exists conf username =
    match lookup conf username with
    | exception Not_found -> false
    | _ -> true
end

let is_authenticated = function
  | Auth _ | Internal | Ramen _ -> true
  | Anonymous -> false

type socket = int (* ZMQ socket index *) * string (* ZMQ peer *)

let print_socket oc (i, s) =
  Printf.fprintf oc "%03d|%s" i (Base64.str_encode s)

let socket_of_string s =
  if String.length s < 4 || s.[3] <> '|' then
    invalid_arg "socket_of_string" ;
  int_of_string (String.sub s 0 3),
  Base64.str_decode (String.chop ~l:4 s)

(* FIXME: when to delete from these? *)
let socket_to_user : (socket, t) Hashtbl.t =
  Hashtbl.create 90

type db = RamenConf.conf
type pub_key = string
let print_pub_key = String.print

let name_is_reserved name =
  name = "" || name.[0] <> '_'

let authenticate conf u username clt_pub_key socket =
  match u with
  | Auth _ | Internal | Ramen _ as u -> u (* do reauth? *)
  | Anonymous ->
      let u =
        match username with
        | "_internal" | "_anonymous" ->
            failwith "Reserved usernames"
        | "" ->
            failwith "Bad credentials"
        | username when username.[0] = '_' ->
            Ramen username
        | username ->
            let roles =
              match Db.lookup conf username with
              | exception Not_found ->
                  (* User registration on insecure sockets is not mandatory,
                   * and give access to the normal user role: *)
                  if clt_pub_key = "" then (
                    [ Role.User ]
                  ) else
                    failwith "No such user"
              | registered_user ->
                  (* Check user is who he pretends to be: *)
                  if clt_pub_key = "" then (
                    !logger.warning "No public key set for user %s \
                                     (not an encrypted channel)" username
                    (* We assume the insecure sockets, if any,  are
                     * listening only on the loopback interface. *)
                  ) else if clt_pub_key <> registered_user.Db.clt_pub_key then (
                    !logger.warning "Public keys mismatch for user %s: \
                                     received %S but DB has %S"
                      username
                      clt_pub_key
                      registered_user.Db.clt_pub_key ;
                    failwith "public keys do not match"
                  ) ;
                  registered_user.Db.roles in
            let roles = Role.Specific username :: roles |>
                        Set.of_list in
            Auth { name = username ; roles } in
      Hashtbl.replace socket_to_user socket u ;
      u

let of_socket socket =
  try Hashtbl.find socket_to_user socket
  with Not_found -> Anonymous

let print oc = function
  | Internal -> String.print oc "_internal"
  | Ramen name -> String.print oc name
  | Auth { name ; _ } -> Printf.fprintf oc "%s" name
  | Anonymous -> Printf.fprintf oc "anonymous"

(* Anonymous users could subscribe to some stuff... *)
let id = function
  | Internal -> "_internal"
  | Ramen name ->
      if not (name_is_reserved name) then
        !logger.error
          "User authenticated as a ramen process has a civilian name %S"
            name ;
      name
  | Anonymous -> "_anonymous"
  | Auth { name ; _ } -> name

let has_role r = function
  | Internal | Ramen _ -> true
  | Auth { roles ; _ } -> Set.mem r roles
  | Anonymous -> false

let has_any_role rs = function
  | Internal | Ramen _ -> true
  | Auth { roles ; _ } -> not (Set.disjoint roles rs )
  | Anonymous -> false
