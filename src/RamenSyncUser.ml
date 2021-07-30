open Batteries
open Stdint

open RamenLog
open RamenHelpers
module N = RamenName
module Files = RamenFiles

type id = Sync_user_id.DessserGen.t

let id_ppp_ocaml : id PPP.t = PPP.string

let print_id oc (id : id) =
  String.print oc id

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
  let db_dir persist_dir =
    N.path_cat [ persist_dir ; N.path "confserver/users" ]

  let file_name conf username  =
    assert (username <> "") ;
    N.path_cat [ db_dir conf ; N.path username ]

  (* Lookup a user by name and return its conf if found: *)
  let lookup conf username  =
    if username = "" then raise Not_found ;
    let fname = file_name conf username in
    try Files.ppp_of_file ~errors_ok:true user_ppp_ocaml fname
    with Unix.(Unix_error (ENOENT, _, _)) | Sys_error _ ->
      raise Not_found

  let save_user conf username user =
    assert (username <> "") ;
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

type socket = Sync_socket.DessserGen.t

let print_socket oc (i, s) =
  Printf.fprintf oc "%03d|%s" (Uint32.to_int i) (Base64.str_encode s)

let string_of_socket s =
  IO.to_string print_socket s

let socket_of_string s =
  if String.length s < 4 || s.[3] <> '|' then
    invalid_arg ("socket_of_string '"^ s ^"'") ;
  Uint32.of_string (String.sub s 0 3),
  Base64.str_decode (String.lchop ~n:4 s)

let compare_sockets s1 s2 = compare s1 s2

(*$inject
  open Stdint *)
(*$= socket_of_string & ~printer:BatPervasives.dump
  (Uint32.zero, "\000\228<\152x") (socket_of_string "000|AOQ8mHg")
*)

type db = N.path
type pub_key = string
let print_pub_key = String.print

let print oc = function
  | Internal -> String.print oc "_internal"
  | Ramen name -> String.print oc name
  | Auth { name ; _ } -> Printf.fprintf oc "%s" name
  | Anonymous -> Printf.fprintf oc "anonymous"

let name_is_reserved name =
  name = "" || name.[0] = '_'

let authenticate conf u username clt_pub_key =
  match u with
  | Auth _ | Internal | Ramen _ as u ->
      !logger.warning "User already authenticated as %a" print u ;
      u (* do reauth? *)
  | Anonymous ->
      let u =
        match username with
        | "_internal" | "_anonymous" ->
            failwith "Reserved usernames"
        | username when username <> "" && username.[0] = '_' ->
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
      u

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

(* Note: if [rs] is empty it means no role gives the requested access, and thus
 * the returned value must be [false] *)
let has_any_role rs = function
  | Internal | Ramen _ -> true
  | Auth { roles ; _ } as u ->
      let ok = not (Set.disjoint roles rs ) in
      if not ok then
        !logger.debug "User %a has none of roles %a (only %a)"
          print u
          (Set.print Role.print) rs
          (Set.print Role.print) roles ;
      ok
  | Anonymous ->
      !logger.debug "Anonymous user has no role" ;
      false
