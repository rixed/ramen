open Batteries
open Stdint

open RamenLog
open RamenHelpers
module DT = DessserTypes
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
    { (* Not technically used, but helps after some rounds of copy and paste: *)
      username : string [@ppp_default ""] ;
      (* SingleUser + Anybody are granted automatically so there is no need to
       * specify them: *)
      roles : Role.t list ;
      clt_pub_key : string }
    [@@ppp PPP_OCaml]

  let file_name users_dir username  =
    assert (username <> "") ;
    N.path_cat [ users_dir ; N.path username ]

  (* Lookup a user by name and return its conf if found: *)
  let lookup users_dir username =
    if username = "" then raise Not_found ;
    let fname = file_name users_dir username in
    match Files.ppp_of_file ~errors_ok:true user_ppp_ocaml fname with
    | exception (Unix.(Unix_error (ENOENT, _, _)) | Sys_error _) ->
        raise Not_found
    | conf ->
        if conf.username <> "" then conf
        else { conf with username }

  let ensure_valid_username s =
    if s = "" then failwith "User names cannot be empty" ;
    List.iter (fun c ->
      if String.contains s c then (
        Printf.sprintf "Invalid character in user name: %C" c |>
        failwith
      )
    ) [ '/' ; '.' ; '\000' ]

  let save_user users_dir username user =
    ensure_valid_username username ;
    let fname = file_name users_dir username in
    Files.ppp_to_file ~pretty:true fname user_ppp_ocaml user

  let make_user users_dir username roles clt_pub_key =
    let user = { username ; roles ; clt_pub_key } in
    save_user users_dir username user

  let user_exists users_dir username =
    match lookup users_dir username with
    | exception Not_found -> false
    | _ -> true
end

let is_authenticated = function
  | Auth _ | Internal | Ramen _ -> true
  | Anonymous -> false

type socket = Sync_socket.DessserGen.t

(* Returns the string representation of a Dessser IP: *)
let string_of_ip = function
  | Sync_socket.DessserGen.V4 ip ->
      RamenIpv4.to_string ip
  | Sync_socket.DessserGen.V6 ip ->
      RamenIpv6.to_string ip

let print_socket oc s =
  Printf.fprintf oc "%s:%s"
    (string_of_ip s.Sync_socket.DessserGen.ip)
    (Uint16.to_string s.port)

let string_of_socket s =
  IO.to_string print_socket s

let socket_of_string s =
  try
    let ip, o = RamenIp.of_string s 0 in
    if s.[o] <> ':' then raise Exit ;
    let port, o = Uint16.of_substring s ~pos:(o + 1) in
    if o < String.length s then raise Exit ;
    Sync_socket.DessserGen.{
      ip =
        (match ip with
        | RamenIp.V4 ip -> V4 ip
        | V6 ip -> V6 ip) ;
      port }
  with _ ->
    invalid_arg ("socket_of_string '"^ s ^"'")

let compare_sockets s1 s2 = compare s1 s2

(*$inject
  open Stdint *)
(*$= socket_of_string & ~printer:BatPervasives.dump
  "1.2.3.4:5678" (string_of_socket (socket_of_string "1.2.3.4:5678"))
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

let authenticate users_dir u username clt_pub_key =
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
              match Db.lookup users_dir username with
              | exception Not_found ->
                  (* User registration on insecure sockets is not mandatory,
                   * and give access to the normal user role: *)
                  if clt_pub_key = "" then (
                    [ Role.User ]
                  ) else
                    failwith ("No such user: "^ username)
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
