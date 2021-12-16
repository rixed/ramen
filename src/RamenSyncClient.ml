(* A sync client connects to the server with admin capacity and synchronise
 * the piece of configuration it's interested into.
 * The configuration is a hash from keys to values * valid flag (which is
 * false while the value is invalid, but we still keep the last known value.
 *)
open Batteries
open RamenLog
open RamenSyncIntf
open RamenHelpersNoLog

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
        if hv.eagerly = Deleted then (
          !logger.debug "Waiting for new value of eagerly deleted %a" Key.print k ;
          H.add t.waiters k cont
        ) else
          cont hv

  let wait_is_over t k hv =
    let conts = H.find_all t.waiters k in
    H.remove_all t.waiters k ;
    List.iter (fun cont -> cont hv) conts

  let find t k =
    let hv = Tree.get t.h k in
    if hv.eagerly = Deleted then raise Not_found else hv

  let find_option t k =
    match find t k with
    | exception Not_found -> None
    | hv -> Some hv

  let fold t ?prefix f u =
    Tree.fold t.h ?prefix (fun k hv u ->
      if hv.eagerly = Deleted then u else f k hv u
    ) u

  let iter t ?prefix f =
    fold t ?prefix (fun k hv () -> f k hv) ()

  let exists t ?prefix f =
    try
      iter t ?prefix (fun k hv ->
        if f k hv then raise Exit) ;
      false
    with Exit ->
      true

  (* Same as [fold], but allow the called back function to modify the hash.
   * If a value is modified, it is undefined whether the callback will see the
   * old or the new value.
   * If an entry is added the callback won't see it.
   * It an entry is removed the callback will not see it if it hasn't already. *)
  let fold_safe t ?prefix f u =
    Tree.fold_safe t.h ?prefix (fun k hv u ->
      if hv.eagerly = Deleted then u else f k hv u
    ) u

  let iter_safe t ?prefix f =
    fold_safe t ?prefix (fun k hv () -> f k hv) ()

  let dump_keys t =
    let lst = fold t (fun k _hv lst -> k :: lst) [] in
    !logger.debug "RamenSyncClient knows only about those keys: %a"
      (pretty_list_print Key.print) lst

  let mem t k =
    match Tree.get t.h k with
    | exception Not_found -> false
    | hv -> hv.eagerly <> Deleted

  let process_msg t = function
    | SrvMsg.AuthOk socket ->
        t.my_socket <- Some socket

    | SrvMsg.AuthErr s ->
        Printf.sprintf "Not authenticated to the server: %s" s |>
        failwith

    | SrvMsg.SetKey { setKey_k ; setKey_v ; setKey_uid ; setKey_mtime } ->
        let new_hv () =
          { value = setKey_v ; uid = setKey_uid ;
            mtime = setKey_mtime ; owner = "" ; expiry = 0. ;
            eagerly = Nope } in
        let set_hv hv =
          !logger.error
            "Server set key %a that has not been created"
            Key.print setKey_k ;
          t.h <- Tree.add setKey_k hv t.h ;
          t.on_new t setKey_k setKey_v setKey_uid setKey_mtime
                   false false "" 0. in
        (match Tree.get t.h setKey_k with
        | exception Not_found ->
            let hv = new_hv () in
            set_hv hv ;
            wait_is_over t setKey_k hv
        | prev ->
            (* Store the value: *)
            prev.value <- setKey_v ;
            prev.uid <- setKey_uid ;
            prev.mtime <- setKey_mtime ;
            prev.eagerly <- Nope ;
            (* Callbacks: *)
            t.on_set t setKey_k setKey_v setKey_uid setKey_mtime
        )

    | SrvMsg.NewKey { newKey_k ; v ; newKey_uid ; mtime ; can_write ; can_del ;
                      newKey_owner ; newKey_expiry } ->
        let new_hv ()  =
          { value = v ; uid = newKey_uid ; mtime ; owner = newKey_owner ;
            expiry = newKey_expiry ; eagerly = Nope } in
        let set_hv hv =
            t.h <- Tree.add newKey_k hv t.h ;
            t.on_new t newKey_k v newKey_uid mtime can_write can_del newKey_owner
                     newKey_expiry in
        (match Tree.get t.h newKey_k with
        | exception Not_found ->
            let hv = new_hv () in
            set_hv hv ;
            wait_is_over t newKey_k hv
        | prev ->
            if prev.eagerly = Nope then
              !logger.error
                "Server create key %a that already exist, updating"
                Key.print newKey_k ;
            (* Store the value: *)
            prev.value <- v ;
            prev.uid <- newKey_uid ;
            prev.mtime <- mtime ;
            prev.owner <- newKey_owner ;
            prev.expiry <- newKey_expiry ;
            prev.eagerly <- Nope ;
            (* Callbacks *)
            t.on_set t newKey_k v newKey_uid mtime ;
            if prev.owner = "" && newKey_owner <> "" then
              t.on_lock t newKey_k newKey_owner newKey_expiry
            else if prev.owner <> "" && newKey_owner = "" then
              t.on_unlock t newKey_k
        )

    | SrvMsg.DelKey { delKey_k ; uid } ->
        (match Tree.get t.h delKey_k with
        | exception Not_found ->
            !logger.error "%s wants to delete unknown key %a"
              uid
              Key.print delKey_k ;
            dump_keys t
        | hv ->
            if hv.eagerly <> Deleted then (
              let conts = H.find_all t.waiters delKey_k in
              if conts <> [] then
                !logger.error "%d waiters were waiting for key %a at deletion"
                  (List.length conts)
                  Key.print delKey_k) ;
            t.h <- Tree.rem delKey_k t.h ;
            t.on_del t delKey_k hv.value)

    | SrvMsg.LockKey { k ; owner ; expiry } ->
        if owner = "" then
          !logger.error "Server locked key %a with empty owner (and expiry=%f)"
            Key.print k
            expiry
        else (
          match Tree.get t.h k with
          | exception Not_found ->
              !logger.error "Server wants to lock unknown key %a"
                Key.print k ;
              dump_keys t
          | prev ->
              prev.owner <- owner ;
              prev.expiry <- expiry ;
              t.on_lock t k owner expiry
        )

    | SrvMsg.UnlockKey k ->
        (match Tree.get t.h k with
        | exception Not_found ->
            !logger.error "Server wants to unlock unknown key %a"
              Key.print k ;
            dump_keys t
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
