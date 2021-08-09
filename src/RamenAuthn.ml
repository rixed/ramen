open Batteries
open Sodium
open Stdint

open RamenLog
open RamenHelpersNoLog

(* Idea: keep using ZMQ as a low level socket API replacement, but encrypt (and
 * compress) messages ourselves using NaCl directly.  First version could do
 * without forward secrecy, which could be added later easily enough.
 *
 * So the API here is quite simple: we got messages that can be encrypted or
 * not, and we return them as string with optional client long term public key
 * (for actual identification, later, when a client wants to dress as a given
 * userid).
 *
 * We need a memory per connection though, that we associate with the nonce
 * sequence.
 *
 * Notice that to save extra round trips we proceed to the handshake while
 * passing the first message. *)

type msg =
  | SendSessionKey of Box.nonce * Box.public_key * Bytes.t
  | Crypted of Box.nonce * Bytes.t
  | ClearText of Bytes.t
  (* In theory we should drop incorrect messages but let's rather help
   * clients to fail quicker and with a better error message: *)
  | Error of string

type session =
  | Secure of
      { (* All keys in raw binary 32 bytes long form, or "" if unset: *)
      mutable peer_pub_key : Box.public_key option ;
      mutable peer_nonce : Box.nonce option ;
      my_pub_key : Box.public_key ;
      my_priv_key : Box.secret_key ;
      mutable channel_key : Box.channel_key option ;
      mutable my_nonce : Box.nonce ;
      mutable key_sent : bool }
  | Insecure

let pub_key_of_string =
  Box.Bytes.to_public_key % Bytes.of_string

let priv_key_of_string =
  Box.Bytes.to_secret_key % Bytes.of_string

let string_of_pub_key =
  Bytes.to_string % Box.Bytes.of_public_key

let string_of_priv_key =
  Bytes.to_string % Box.Bytes.of_secret_key

module Z85 =
struct
  (*$< Z85 *)

  (*
   * Keys are stored as z85 encoded strings.
   * Following https://rfc.zeromq.org/spec/32/ literally:
   *)

  let tbl =
    "0123456789abcdefghijklmnopqrstuvwxyzABCD\
     EFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{\
     }@%$#"

  let tbl_inv =
    Array.init 256 (fun i ->
      try String.index tbl (Char.chr i)
      with Not_found -> -1)

  let n85 = Uint32.of_int 85

  let encode s =
    let len = String.length s in
    if len mod 4 <> 0 then invalid_arg "Z85.encode" ;
    let out = Bytes.create (5 * (len / 4)) in
    let rec loop i o =
      if i >= len then Bytes.unsafe_to_string out else
      let to_u32 b c =
        Uint32.(shift_left (of_int (Char.code c)) (b lsl 3)) in
      let rec output n j =
        if j < 5 then (
          let c = tbl.[Uint32.(rem n n85 |> to_int)] in
          Bytes.set out (o + 4 - j) c ;
          output Uint32.(div n n85) (j + 1)
        ) in
      let n =
         Uint32.add (to_u32 3 s.[i+0])
        (Uint32.add (to_u32 2 s.[i+1])
        (Uint32.add (to_u32 1 s.[i+2])
                    (to_u32 0 s.[i+3]))) in
      output n 0 ;
      loop (i + 4) (o + 5) in
    loop 0 0

  (*$= encode & ~printer:(fun x -> x)
    "HelloWorld" (encode "\x86\x4F\xD2\x6F\xB5\x59\xF7\x5B")
  *)

  let decode s =
    let len = String.length s in
    if len mod 5 <> 0 then invalid_arg "Z85.decode" ;
    let out = Bytes.create (4 * (len / 5)) in
    let rec loop i o =
      if i >= len then Bytes.unsafe_to_string out else
      let rec to_u32 n j =
        if j >= 5 then n else
        let d = tbl_inv.(Char.code (s.[i+j])) in
        if d < 0 then invalid_arg "Z85.decode" ;
        let n = Uint32.(add (mul n n85) (of_int d)) in
        to_u32 n (j + 1) in
      let n = to_u32 Uint32.zero 0 in
      let output j b =
        let d = Uint32.(shift_right n (b lsl 3) |> to_int) land 0xff in
        Bytes.set out (o + j) (Char.chr d) in
      (* Some pretend big endian is more "natural"... *)
      output 3 0 ;
      output 2 1 ;
      output 1 2 ;
      output 0 3 ;
      loop (i + 5) (o + 4) in
    loop 0 0

  (*$= decode & ~printer:(fun x -> x)
    "\x86\x4F\xD2\x6F\xB5\x59\xF7\x5B" (decode "HelloWorld")
  *)

  (*$>*)
end

let pub_key_of_z85 =
  pub_key_of_string % Z85.decode

let priv_key_of_z85 =
  priv_key_of_string % Z85.decode

let z85_of_pub_key =
  Z85.encode % string_of_pub_key

let z85_of_priv_key =
  Z85.encode % string_of_priv_key

(* Return public/private keys *)
let random_keypair () =
  let priv, pub = Sodium.Box.random_keypair () in
  z85_of_pub_key pub, z85_of_priv_key priv

let box session str nonce =
  match session with
  | Secure sess ->
      let str = Bytes.of_string str in
      (match sess.channel_key with
      | None ->
          Box.Bytes.box sess.my_priv_key (Option.get sess.peer_pub_key) str nonce
      | Some chankey ->
          Box.Bytes.fast_box chankey str nonce)
  | Insecure ->
      invalid_arg "box"

let unbox session my_priv_key bytes nonce =
  match session with
  | Secure sess ->
      (if sess.channel_key <> None &&
          Box.equal_secret_keys my_priv_key sess.my_priv_key
      then
        Box.Bytes.fast_box_open (Option.get sess.channel_key) bytes nonce
      else
        Box.Bytes.box_open my_priv_key (Option.get sess.peer_pub_key)
                           bytes nonce) |>
      Bytes.to_string
  | Insecure ->
      invalid_arg "unbox"

let to_string msg =
  Marshal.(to_string msg [ No_sharing ])

let of_string str : msg =
  Marshal.from_string str 0

let encrypt session str =
  match session with
  | Secure sess ->
      let nonce = sess.my_nonce in
      sess.my_nonce <- Box.increment_nonce sess.my_nonce ;
      let crypted_str = box session str nonce in
      Crypted (nonce, crypted_str)
  | Insecure ->
      invalid_arg "encrypt"

let send_session_key session str =
  match session with
  | Secure sess ->
      let nonce = sess.my_nonce in
      sess.my_nonce <- Box.increment_nonce sess.my_nonce ;
      let crypted_str = box session str nonce in
      sess.key_sent <- true ;
      SendSessionKey (nonce, sess.my_pub_key, crypted_str)
  | Insecure ->
      invalid_arg "send_session_key"

let clear_text session str =
  match session with
  | Insecure ->
      ClearText (Bytes.of_string str)
  | Secure _ ->
      invalid_arg "clear_text"

let wrap session str =
  (match session with
  | Secure sess ->
      if sess.key_sent then encrypt else send_session_key
  | Insecure ->
      clear_text
  ) session str |>
  to_string

let is_crypted session =
  match session with
  | Secure _ -> true
  | Insecure -> false

let make_session peer_pub_key my_pub_key my_priv_key =
  let my_nonce = Box.random_nonce () in
  let channel_key =
    Option.map (fun peer_pub_key ->
      Box.precompute my_priv_key peer_pub_key
    ) peer_pub_key in
  Secure { peer_pub_key ; peer_nonce = None ;
           my_pub_key ; my_priv_key ;
           my_nonce ; channel_key ; key_sent = false }

let make_clear_session () =
  Insecure

let set_peer_pub_key session peer_pub_key =
  match session with
  | Secure sess ->
      sess.peer_pub_key <- Some peer_pub_key ;
      sess.channel_key <-
        Some (Box.precompute sess.my_priv_key peer_pub_key)
  | Insecure ->
      invalid_arg "set_peer_pub_key"

exception RemoteError of string

(* Build a message than must be sent back immediately *)
let error str =
  !logger.error "%s" str ;
  Result.Error (Error str |> to_string)

let decrypt session str =
  let msg = of_string str in
  match session with
  | Secure sess ->
      let resp my_priv_key bytes nonce =
        match unbox session my_priv_key bytes nonce with
        | exception e ->
            error (Printexc.to_string e)
        | decrypted_msg ->
            let ok () =
              sess.peer_nonce <- Some nonce ;
              Ok decrypted_msg in
            (match sess.peer_nonce with
            | None ->
                ok ()
            | Some peer_nonce ->
                let next_nonce = Box.increment_nonce peer_nonce in
                if nonce = next_nonce then
                  ok ()
                else
                  error "Wrong nonce")
      in
      (match msg with
      | SendSessionKey (nonce, peer_pub_key, bytes) ->
          !logger.debug "Decrypting a SendSessionKey" ;
          let my_priv_key =
            match sess.peer_pub_key with
            | None -> (* We must be a server then: *)
                (* This message has been encoded using the server long
                 * term key: *)
                (* FIXME: we should have both long term and short term keys in
                 * the session *)
                sess.my_priv_key
            | Some _-> (* We must be the client then: *)
                !logger.debug
                  "Replacing peer public key with short term one" ;
                sess.my_priv_key in
          set_peer_pub_key session peer_pub_key ;
          resp my_priv_key bytes nonce
      | Crypted (nonce, bytes) ->
          !logger.debug "Decrypting a Crypted" ;
          (match sess.peer_pub_key with
          | None ->
              error "Missing session"
          | Some _ ->
              resp sess.my_priv_key bytes nonce)
      | ClearText _ ->
          error "Secure endpoint must be sent secure messages"
      | Error str ->
          raise (RemoteError str))
  | Insecure ->
      (match msg with
      | SendSessionKey _
      | Crypted _ ->
          error "Insecure endpoint must not be sent secure messages"
      | ClearText bytes ->
          Ok (Bytes.to_string bytes)
      | Error str ->
          raise (RemoteError str))
