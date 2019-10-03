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
  module Tree = RamenSyncTree.Impl.PrefixTree (Key)

  include Messages (Key) (Value) (Selector)

  type t =
    { mutable h : hash_value Tree.t ;
      (* Callbacks waiting for a value not yet in [h].
       * Beware that each waiter is bound separately on the same key. *)
      waiters : (hash_value -> unit) H.t ;
      my_uid : User.id ;
      mutable my_socket : User.socket option ; (* As returned by AuthOK *)
      mutable on_new : t -> Key.t -> Value.t -> string -> float -> bool -> bool -> string -> float -> unit ;
      mutable on_set : t -> Key.t -> Value.t -> string -> float -> unit ;
      mutable on_del : t -> Key.t -> Value.t -> unit ; (* previous value *)
      mutable on_lock : t -> Key.t -> string -> float -> unit ;
      mutable on_unlock : t -> Key.t -> unit }

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
    { h = Tree.empty ; waiters = H.create 99 ; my_socket = None ;
      my_uid ; on_new ; on_set ; on_del ; on_lock ; on_unlock }

  let with_value t k cont =
    match Tree.get t.h k with
    | exception Not_found ->
        !logger.debug "Waiting for value of %a" Key.print k ;
        H.add t.waiters k cont
    | hv ->
        cont hv

  let wait_is_over t k hv =
    let conts = H.find_all t.waiters k in
    H.remove_all t.waiters k ;
    List.iter (fun cont -> cont hv) conts

  let find t k = Tree.get t.h k

  let find_option t k =
    match Tree.get t.h k with
    | exception Not_found -> None
    | hv -> Some hv

  let fold t ?prefix f u =
    Tree.fold t.h ?prefix f u

  let iter t ?prefix f =
    fold t ?prefix (fun k hv () -> f k hv) ()

  (* Same as [fold], but allow the called back function to modify the hash.
   * If a value is modified, it is undefined whether the callback will see the
   * old or the new value.
   * If an entry is added the callback won't see it.
   * It an entry is removed the callback will not see it if it hasn't already. *)
  let fold_safe t ?prefix f u =
    Tree.fold_safe t.h ?prefix f u

  let iter_safe t ?prefix f =
    fold_safe t ?prefix (fun k hv () -> f k hv) ()

  let mem t k = Tree.mem t.h k

  let process_msg t = function
    | SrvMsg.AuthOk socket ->
        t.my_socket <- Some socket

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
          t.h <- Tree.add k hv t.h ;
          t.on_new t k v uid mtime false false "" 0. in
        (match Tree.get t.h k with
        | exception Not_found ->
            let hv = new_hv () in
            set_hv hv ;
            wait_is_over t k hv
        | prev ->
            (* Store the value: *)
            prev.value <- v ;
            prev.uid <- uid ;
            prev.mtime <- mtime ;
            prev.eagerly <- Nope ;
            (* Callbacks: *)
            t.on_set t k v uid mtime
        )

    | SrvMsg.NewKey { k ; v ; uid ; mtime ; can_write ; can_del ; owner ;
                      expiry } ->
        let new_hv ()  =
          { value = v ; uid ; mtime ; owner ; expiry ; eagerly = Nope } in
        let set_hv hv =
            t.h <- Tree.add k hv t.h ;
            t.on_new t k v uid mtime can_write can_del owner expiry in
        (match Tree.get t.h k with
        | exception Not_found ->
            let hv = new_hv () in
            set_hv hv ;
            wait_is_over t k hv
        | prev ->
            if prev.eagerly = Nope then
              !logger.error
                "Server create key %a that already exist, updating"
                Key.print k ;
            (* Store the value: *)
            prev.value <- v ;
            prev.uid <- uid ;
            prev.mtime <- mtime ;
            prev.owner <- owner ;
            prev.expiry <- expiry ;
            prev.eagerly <- Nope ;
            (* Callbacks *)
            t.on_set t k v uid mtime ;
            if prev.owner = "" && owner <> "" then
              t.on_lock t k owner expiry
            else if prev.owner <> "" && owner = "" then
              t.on_unlock t k
        )

    | SrvMsg.DelKey k ->
        (match Tree.get t.h k with
        | exception Not_found ->
            !logger.error "Server wanted to delete an unknown key %a"
              Key.print k
        | hv ->
            let conts = H.find_all t.waiters k in
            if conts <> [] then
              !logger.error "%d waiters were waiting for key %a at deletion"
                (List.length conts)
                Key.print k ;
            t.h <- Tree.rem k t.h ;
            t.on_del t k hv.value)

    | SrvMsg.LockKey { k ; owner ; expiry } ->
        if owner = "" then
          !logger.error "Server locked key %a with empty owner (and expiry=%f)"
            Key.print k
            expiry
        else (
          match Tree.get t.h k with
          | exception Not_found ->
              !logger.error "Server want to lock unknown key %a"
                Key.print k
          | prev ->
              prev.owner <- owner ;
              prev.expiry <- expiry ;
              t.on_lock t k owner expiry
        )

    | SrvMsg.UnlockKey k ->
        (match Tree.get t.h k with
        | exception Not_found ->
            !logger.error "Server want to unlock unknown key %a"
              Key.print k
        | prev ->
            if prev.owner = "" then (
              !logger.error "Server unlocked key %a that is not locked"
                Key.print k ;
            ) else (
              prev.owner <- "" ;
              t.on_unlock t k
            )
        )
end
