(* A sync client connects to the server with admin capacity and synchronise
 * the piece of configuration it's interested into.
 * The configuration is a hash from keys to values * valid flag (which is
 * false while the value is invalid, but we still keep the last known value.
 *)
open Batteries
open RamenLog
open RamenSyncIntf

module Make (Value : VALUE) (Selector : SELECTOR) =
struct
  module Key = Selector.Key
  module User = Key.User
  module Selector = Selector
  module H = Hashtbl.Make (Key)

  include Messages (Key) (Value) (Selector)

  type t =
    { h : hash_maybe_value H.t ;
      my_uid : User.id ;
      mutable my_errors : Key.t option ; (* As returned by AuthOK *)
      mutable on_new : t -> Key.t -> Value.t -> string -> float -> bool -> bool -> string -> float -> unit ;
      mutable on_set : t -> Key.t -> Value.t -> string -> float -> unit ;
      mutable on_del : t -> Key.t -> Value.t -> unit ; (* previous value *)
      mutable on_lock : t -> Key.t -> string -> float -> unit ;
      mutable on_unlock : t -> Key.t -> unit }

  and hash_maybe_value =
    | Value of hash_value
    | Waiters of (hash_value -> unit) list

  and hash_value =
    { mutable value : Value.t ;
      (* These metadata are set exclusively by the confserver.
       * Unreliable if eager. *)
      (* We could also store can_write/del in here but that's not used *)
      mutable uid : string ;
      mutable mtime : float ;
      mutable owner : string ; (* empty is none *)
      mutable expiry : float ; (* irrelevant if not owned *)
      (* If set eagerly but not received from the server yet: *)
      mutable eagerly : eagerly }

  and eagerly = Nope | Created | Overwritten | Deleted

  let make ~my_uid ~on_new ~on_set ~on_del ~on_lock ~on_unlock =
    { h = H.create 99 ; my_errors = None ;
      my_uid ; on_new ; on_set ; on_del ; on_lock ; on_unlock }

  let with_value t k cont =
    match H.find t.h k with
    | exception Not_found ->
        !logger.debug "Waiting for value of %a" Key.print k ;
        H.add t.h k (Waiters [ cont ])
    | Value hv ->
        cont hv
    | Waiters conts ->
        !logger.debug "Waiting for value of %a" Key.print k ;
        H.replace t.h k (Waiters (cont :: conts))

  let find t k =
    match H.find t.h k with
    | Waiters _ -> raise Not_found
    | Value hv -> hv

  let find_option t k =
    match H.find t.h k with
    | exception Not_found -> None
    | Waiters _ -> None
    | Value hv -> Some hv

  let fold t f u =
    H.fold (fun k v u ->
      match v with
      | Waiters _ -> u
      | Value hv -> f k hv u
    ) t.h u

  let iter t f =
    fold t (fun k hv () -> f k hv) ()

  (* Same as [fold], but allow the called back function to modify the hash.
   * If a value is modified, it is undefined whether the callback will see the
   * old or the new value.
   * If an entry is added the callback won't see it.
   * It an entry is removed the callback will not see it if it hasn't already. *)
  let fold_safe t f u =
    let keys = H.keys t.h |> Array.of_enum in
    Array.fold_left (fun u k ->
      match H.find t.h k with
      | exception Not_found -> u
      | Waiters _ -> u
      | Value hv -> f k hv u
    ) u keys

  let iter_safe t f =
    fold_safe t (fun k hv () -> f k hv) ()

  let mem t k = H.mem t.h k

  let process_msg t = function
    | SrvMsg.AuthOk k ->
        !logger.debug "Will receive errors in %a" Key.print k ;
        t.my_errors <- Some k

    | SrvMsg.AuthErr s ->
        Printf.sprintf "Cannot authenticate to the server: %s" s |>
        failwith

    | SrvMsg.SetKey { k ; v ; uid ; mtime } ->
        let new_hv () =
          { value = v ; uid ; mtime ; owner = "" ; expiry = 0. ;
            eagerly = Nope } in
        let set_hv hv =
          !logger.error
            "Server set key %a that has not been created"
            Key.print k ;
          H.replace t.h k (Value hv) ;
          t.on_new t k v uid mtime false false "" 0. in
        (match H.find t.h k with
        | exception Not_found ->
            set_hv (new_hv ())
        | Waiters conts ->
            let hv = new_hv () in
            set_hv hv ;
            List.iter (fun cont -> cont hv) conts
        | Value prev ->
            t.on_set t k v uid mtime ;
            prev.value <- v ;
            prev.uid <- uid ;
            prev.mtime <- mtime ;
            prev.eagerly <- Nope
        )

    | SrvMsg.NewKey { k ; v ; uid ; mtime ; can_write ; can_del ; owner ;
                      expiry } ->
        let new_hv ()  =
          { value = v ; uid ; mtime ; owner ; expiry ; eagerly = Nope } in
        let set_hv hv =
            H.replace t.h k (Value hv) ;
            t.on_new t k v uid mtime can_write can_del owner expiry in
        (match H.find t.h k with
        | exception Not_found ->
            set_hv (new_hv ())
        | Waiters conts ->
            let hv = new_hv () in
            set_hv hv ;
            List.iter (fun cont -> cont hv) conts
        | Value prev ->
            if prev.eagerly = Nope then
              !logger.error
                "Server create key %a that already exist, updating"
                Key.print k ;
            t.on_set t k v uid mtime ;
            if prev.owner = "" && owner <> "" then
              t.on_lock t k owner expiry
            else if prev.owner <> "" && owner = "" then
              t.on_unlock t k ;
            prev.value <- v ;
            prev.uid <- uid ;
            prev.mtime <- mtime ;
            prev.owner <- owner ;
            prev.expiry <- expiry ;
            prev.eagerly <- Nope
        )

    | SrvMsg.DelKey k ->
        if t.my_errors = Some k then
          !logger.error "Bummer! The server timed us out!" ;
        (match H.find t.h k with
        | exception Not_found ->
            !logger.error "Server wanted to delete an unknown key %a"
              Key.print k
        | Waiters conts ->
            !logger.error "%d waiters were waiting for key %a at deletion"
              (List.length conts)
              Key.print k
        | Value hv ->
            H.remove t.h k ;
            t.on_del t k hv.value)

    | SrvMsg.LockKey { k ; owner ; expiry } ->
        if owner = "" then
          !logger.error "Server locked key %a with empty owner (and expiry=%f)"
            Key.print k
            expiry
        else (
          match H.find t.h k with
          | exception Not_found ->
              !logger.error "Server want to lock unknown key %a"
                Key.print k
          | Waiters _ ->
              !logger.error "Server want to lock unknown key %a"
                Key.print k
          | Value prev ->
              prev.owner <- owner ;
              prev.expiry <- expiry ;
              t.on_lock t k owner expiry
        )

    | SrvMsg.UnlockKey k ->
        (match H.find t.h k with
        | exception Not_found ->
            !logger.error "Server want to unlock unknown key %a"
              Key.print k
        | Waiters _ ->
            !logger.error "Server want to unlock unknown key %a"
              Key.print k
        | Value prev ->
            if prev.owner = "" then (
              !logger.error "Server unlocked key %a that is not locked"
                Key.print k ;
            ) else (
              prev.owner <- "" ;
              t.on_unlock t k
            )
        )
end
