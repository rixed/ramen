open Batteries
open RamenHelpers
open RamenLog
open RamenSyncIntf
open RamenConsts

(* A KV store implementing sync mechanism, with still no side effects *)
module Make (Value : VALUE) (Selector : SELECTOR) =
struct
  module Key = Selector.Key
  module User = Key.User
  module Role = User.Role
  module Selector = Selector
  module H = Hashtbl.Make (Key)

  include Messages (Key) (Value) (Selector)

  type t =
    { h : hash_value H.t ;
      (* Order of appearance of each key, for ordering initial syncs: *)
      mutable next_key_seq : int ;
      user_db : User.db ;
      send_msg : (User.socket * SrvMsg.t) Enum.t -> unit ;
      (* Inverted match: who is using what: *)
      cb_selectors : Selector.set ;
      user_selectors : Selector.set ;
      subscriptions : (Selector.id, (User.socket, User.t) Map.t) Hashtbl.t }

  and hash_value =
    { mutable v : Value.t ;
      (* Order in the sequence of key creation. Also stored in snapshots. *)
      key_seq : int ;
      (* The only permissions we need are:
       * - read: to see a key and its value,
       * - write: to be able to write that value,
       * - del: to be able to delete the key. *)
      can_read : Role.t Set.t ;
      can_write : Role.t Set.t ;
      can_del : Role.t Set.t ;
      (* Locked by the user who's on top of the list. Others are waiting: *)
      (* TODO: Distinct reader locks (a set) from writer locks (that list). *)
      mutable locks : lock list ;
      (* Also some metadata: *)
      mutable set_by : User.t ;
      mutable mtime : float }

  (* that float is an absolute time at the head of the list and a
   * duration for other lockers: *)
  and lock = User.t * float

  (* Callbacks return either None, meaning the change is refused, or some
   * new (or identical) value to be written instead of the user supplied
   * one.
   * If several callbacks are registered they are played in order.
   * Not sure if this is going to be ever happen though. *)
  (* Note: To save on coding the on_del callback is passed a dummy value. *)
  and callback = Key.t -> Value.t -> Value.t option

  let make user_db ~send_msg =
    { h = H.create 99 ; next_key_seq = 0 ;
      user_db ; send_msg ;
      cb_selectors = Selector.make_set () ;
      user_selectors = Selector.make_set () ;
      subscriptions = Hashtbl.create 99 }

  let print_lockers oc = function
    | [] ->
        Printf.fprintf oc "None"
    | (current_locker, expiry) :: rest ->
        Printf.fprintf oc "%a (until %a) (then: %a)"
          User.print current_locker
          print_as_date expiry
          (List.print (Tuple2.print User.print print_as_duration)) rest

  let notify t k is_permitted m =
    let subscriber_sockets =
      Selector.matches k t.user_selectors |>
      Enum.fold (fun sockets sel_id ->
        !logger.debug "Selector %a matched key %a"
          Selector.print_id sel_id Key.print k ;
        Hashtbl.find_default t.subscriptions sel_id Map.empty |>
        Map.union sockets
      ) Map.empty in
    Map.enum subscriber_sockets //@
    (fun (socket, user) ->
      if is_permitted user then
        Some (socket, m user)
      else (
        !logger.debug "User %a cannot read %a"
          User.print user
          Key.print k ;
        None
      )) |>
    t.send_msg

  let no_such_key k =
    Printf.sprintf2 "Key %a: does not exist"
      Key.print k |>
    failwith

  let locked_by k u =
    Printf.sprintf2 "Key %a: temporarily unavailable, locked by %a"
      Key.print k
      User.print u |>
    failwith

  (* Remove the head locker and notify of lock change: *)
  let do_unlock ?(and_notify=true) t k hv =
    match List.tl hv.locks with
    | [] ->
        hv.locks <- [] ;
        if and_notify then
          notify t k (User.has_any_role hv.can_read)
                 (fun _ -> UnlockKey k)
    | (u, duration) :: rest ->
        let expiry = Unix.gettimeofday () +. duration in
        hv.locks <- (u, expiry) :: rest ;
        let owner = IO.to_string User.print_id (User.id u) in
        if and_notify then
          notify t k (User.has_any_role hv.can_read)
                 (fun _ -> LockKey { k ; owner ; expiry})

  let timeout_locks ?and_notify t k hv =
    match hv.locks with
    | [] -> ()
    | (u, expiry) :: _ ->
        let now = Unix.gettimeofday () in
        if expiry < now then (
          !logger.warning "Timing out %a's lock of config key %a"
            User.print u
            Key.print k ;
          do_unlock ?and_notify t k hv)

  (* Early cleaning of timed out locks is just for nicer visualisation in
   * clients but is not required for proper working of locks. *)
  let timeout_all_locks =
    let last_timeout = ref 0. in
    fun t ->
      let now = Unix.time () in
      if now -. !last_timeout >= 1. then (
        last_timeout := now ;
        (* FIXME: have a heap of locks *)
        H.iter (timeout_locks t) t.h
      )

  let check_unlocked t hv k u =
    timeout_locks t k hv ;
    match hv.locks with
    | (u', _) :: _ when not (User.equal u' u) ->
        locked_by k u'
    (* TODO: Think about making locking mandatory *)
    | _ -> ()


  let check_can_do what k u can =
    if not (User.has_any_role can u) then (
      Printf.sprintf2 "Key %a: not allowed to %s" Key.print k what |>
      failwith
    )

  let check_can_write t k hv u =
    check_can_do "write" k u hv.can_write ;
    check_unlocked t hv k u

  let check_can_delete t k hv u =
    check_can_do "delete" k u hv.can_del ;
    check_can_write t k hv u

  let create t u k v ?(lock_timeo=Default.sync_lock_timeout)
             ~can_read ~can_write ~can_del =
    !logger.debug "Creating config key %a with value %a, read:%a write:%a del:%a"
      Key.print k
      Value.print v
      (Set.print Role.print) can_read
      (Set.print Role.print) can_write
      (Set.print Role.print) can_del ;
    match H.find t.h k with
    | exception Not_found ->
        (* As long as there is a callback for this, that's ok: *)
        let mtime = Unix.gettimeofday () in
        let uid = IO.to_string User.print_id (User.id u) in
        (* Objects are created locked unless timeout is <= 0 (to avoid
         * spurious warnings): *)
        let locks, owner, expiry =
          if lock_timeo > 0. then
            let expiry = mtime +. lock_timeo in
            [ u, expiry ], uid, expiry
          else
            [], "", 0. in
        H.add t.h k { v ; key_seq = t.next_key_seq ;
                      can_read ; can_write ; can_del ;
                      locks ; set_by = u ; mtime } ;
        t.next_key_seq <- t.next_key_seq + 1 ;
        let msg u =
          let can_write = User.has_any_role can_write u
          and can_del = User.has_any_role can_del u in
          SrvMsg.NewKey { k ; v ; uid ; mtime ; can_write ; can_del ;
                          owner ; expiry } in
        notify t k (User.has_any_role can_read) msg
    | _ ->
        Printf.sprintf2 "Key %a: already exist"
          Key.print k |>
        failwith

  let check_same_value prev_v v u k =
    if Value.equal prev_v v then
      !logger.warning "Client %a is writting the same value to %a"
        User.print u
        Key.print k

  let update t u k v =
    match H.find t.h k with
    | exception Not_found ->
        no_such_key k
    | prev ->
        check_same_value prev.v v u k ;
        !logger.debug "Setting config key %a to value %a"
          Key.print k
          Value.print v ;
        check_can_write t k prev u ;
        prev.v <- v ;
        prev.set_by <- u ;
        prev.mtime <- Unix.gettimeofday () ;
        let uid = IO.to_string User.print_id (User.id u) in
        let msg _ = SrvMsg.SetKey { k ; v ; uid ; mtime = prev.mtime } in
        notify t k (User.has_any_role prev.can_read) msg

  let set t u k v = (* TODO: H.find and pass prev item to update *)
    if H.mem t.h k then
      update t u k v
    else
      let can_read = Set.of_list Role.[ Admin ; User ] in
      let can_write = Set.of_list Role.[ Specific (User.id u) ] in
      let can_del = can_write in
      create t u k v ~lock_timeo:0. ~can_read ~can_write ~can_del

  let del t u k =
    !logger.debug "Deleting config key %a"
      Key.print k ;
    match H.find t.h k with
    | exception Not_found ->
        ()
    | prev ->
        (* TODO: think about making locking mandatory *)
        check_can_delete t k prev u ;
        H.remove t.h k ;
        notify t k (User.has_any_role prev.can_read)
               (fun _ -> DelKey k)

  let lock t u k ~must_exist ~lock_timeo =
    !logger.debug "Locking config key %a"
      Key.print k ;
    match H.find t.h k with
    | exception Not_found ->
        (* We must allow to lock a non-existent key to reserve the key to its
         * creator. In that case a lock will create a new (Void) value. *)
        if must_exist then no_such_key k else
        let can_read = Set.of_list Role.[ Admin ; User ] in
        let can_write = Set.of_list Role.[ Specific (User.id u) ] in
        let can_del = can_write in
        create t u k Value.dummy ~can_read ~can_write ~can_del ~lock_timeo
    | prev ->
        timeout_locks t k prev ;
        !logger.debug "Current lockers: %a" print_lockers prev.locks ;
        (* only for wlocks: check_can_write t k prev u ; *)
        (match prev.locks with
        | [] ->
            (* We have a new locker: *)
            let owner = IO.to_string User.print_id (User.id u) in
            let expiry = Unix.gettimeofday () +. lock_timeo in
            prev.locks <- [ u, expiry ] ;
            let is_permitted = User.has_any_role prev.can_read in
            notify t k is_permitted
                   (fun _ -> LockKey { k ; owner ; expiry })
        | lst ->
            (* Err out if the user is already the current locker: *)
            if User.equal u (fst (List.hd lst)) then
              Printf.sprintf2 "User %a already owns %a"
                User.print u
                Key.print k |>
              failwith ;
            (* Reject it if it's already in the lockers: *)
            if List.exists (fun (u', _) -> User.equal u u') lst then
              Printf.sprintf2 "User %a is already waiting for %a lock"
                User.print u
                Key.print k |>
              failwith ;
            prev.locks <- lst @ [ u, lock_timeo ] (* FIXME *))

  let unlock t u k =
    !logger.debug "Unlocking config key %a"
      Key.print k ;
    match H.find t.h k with
    | exception Not_found ->
        no_such_key k
    | prev ->
        (match prev.locks with
        | (u', _) :: _ when User.equal u u' ->
            do_unlock t k prev
        | (u', _) :: _ ->
            locked_by k u'
        | [] ->
            Printf.sprintf2 "Key %a: not locked" Key.print k |>
            failwith)

  let create_or_update srv k v ~can_read ~can_write ~can_del =
    match H.find srv.h k with
    | exception Not_found ->
        create srv User.internal k v ~lock_timeo:0. ~can_read ~can_write ~can_del
    | hv ->
        (* create_or_update is only for internal use, no client will wait
         * an answer; So it's OK to skip NOPs: *)
        if not (Value.equal hv.v v) then
          set srv User.internal k v

  let subscribe_user t socket u sel =
    (* Add this selection to the known selectors, and add this selector
     * ID for this user to the subscriptions: *)
    let id = Selector.add t.user_selectors sel in
    !logger.debug "User %a has selection %a for %a"
      User.print u
      Selector.print_id id
      Selector.print sel ;
    (* Note: [Map.add] will replace any previous mapping for [socket]: *)
    Hashtbl.modify_def Map.empty id (Map.add socket u) t.subscriptions

  let owner_of_hash_value hv =
    match hv.locks with
    | [] -> "", 0.
    | (owner, expiry) :: _ ->
        IO.to_string User.print_id (User.id owner),
        expiry

  let initial_sync t socket u sel =
    !logger.debug "Initial synchronisation for user %a: Starting!" User.print u ;
    let s = Selector.make_set () in
    let _ = Selector.add s sel in
    let sorted =
      H.enum t.h //
      (fun (k, hv) ->
        User.has_any_role hv.can_read u &&
        not (Enum.is_empty (Selector.matches k s))
      ) |> Array.of_enum in
    let key_seq_cmp (_, hv1) (_, hv2) = Int.compare hv1.key_seq hv2.key_seq in
    Array.fast_sort key_seq_cmp sorted ;
    Array.iter (fun (k, hv) ->
      timeout_locks ~and_notify:false t k hv ;
      let uid = IO.to_string User.print_id (User.id hv.set_by)
      and owner, expiry = owner_of_hash_value hv
      and can_write = User.has_any_role hv.can_write u
      and can_del = User.has_any_role hv.can_del u in
      let msg = SrvMsg.NewKey { k ; v = hv.v ; uid ; mtime = hv.mtime ;
                                can_write ; can_del ; owner ; expiry } in
      t.send_msg (Enum.singleton (socket, msg))
    ) sorted ;
    !logger.debug "Initial synchronisation for user %a: Complete!" User.print u

  let set_user_err t u socket i str =
    let k = Key.user_errs u socket
    and v = Value.err_msg i str in
    set t User.internal k v

  let process_msg t socket u clt_pub_key (i, cmd) =
    try
      let u =
        match cmd with
        | CltMsg.Auth uid ->
            (* Auth is special: as we have no user yet, errors must be
             * returned directly. *)
            (try
              let u' = User.authenticate t.user_db u uid clt_pub_key in
              !logger.info "User %a authenticated out of user %a"
                User.print u'
                User.print u ;
              (* Must create this user's error object if not already there.
               * Value will be set below: *)
              let k = Key.user_errs u' socket in
              let can_read = Set.of_list Role.[ Specific (User.id u') ] in
              let can_write = Set.empty in
              let can_del = can_read in
              create_or_update t k (Value.err_msg i "")
                               ~can_read ~can_write ~can_del ;
              t.send_msg (Enum.singleton (socket, SrvMsg.AuthOk socket)) ;
              u'
            with e ->
              let err = Printexc.to_string e in
              !logger.info "While authenticating %a: %s" User.print u err ;
              t.send_msg (Enum.singleton (socket, SrvMsg.AuthErr err)) ;
              u)

        | CltMsg.StartSync sel ->
            subscribe_user t socket u sel ;
            (* Then send everything that matches this selection and that the
             * user can read: *)
            initial_sync t socket u sel ;
            u

        | CltMsg.SetKey (k, v) ->
            set t u k v ;
            u

        | CltMsg.NewKey (k, v, lock_timeo) ->
            let can_read = Set.of_list Role.[ Admin ; User ] in
            let can_write = Set.of_list Role.[ Specific (User.id u) ] in
            let can_del = can_write in
            create t u k v ~can_read ~can_write ~can_del ~lock_timeo ;
            u

        | CltMsg.UpdKey (k, v) ->
            update t u k v ;
            u

        | CltMsg.DelKey k ->
            del t u k ;
            u

        | CltMsg.LockKey (k, lock_timeo) ->
            lock t u k ~must_exist:true ~lock_timeo ;
            u

        | CltMsg.LockOrCreateKey (k, lock_timeo) ->
            lock t u k ~must_exist:false ~lock_timeo ;
            u

        | CltMsg.UnlockKey k ->
            unlock t u k ;
            u
      in
      if User.is_authenticated u then set_user_err t u socket i "" ;
      u
    with e ->
      set_user_err t u socket i (Printexc.to_string e) ;
      u
end
