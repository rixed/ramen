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
  module Capa = User.Capa
  module Selector = Selector
  module H = Hashtbl.Make (Key)

  include Messages (Key) (Value) (Selector)

  type t =
    { h : hash_value H.t ;
      send_msg : SrvMsg.t -> User.socket Enum.t -> unit ;
      (* Inverted match: who is using what: *)
      cb_selectors : Selector.set ;
      on_sets : (Selector.id, callback) Hashtbl.t ;
      on_news : (Selector.id, callback) Hashtbl.t ;
      on_dels : (Selector.id, callback) Hashtbl.t ;
      user_selectors : Selector.set ;
      subscriptions : (Selector.id, (User.socket, User.t) Map.t) Hashtbl.t }

  and hash_value =
    { mutable v : Value.t ;
      r : Capa.t ; (* Read perm *)
      w : Capa.t ; (* Write perm *)
      s : bool (* Sticky, ie cannot be deleted *) ;
      (* Locked by the user who's on top of the list. Others are waiting: *)
      (* TODO: Distinct reader locks (a set) from /writer locks (that list). *)
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

  let make ~send_msg =
    { h = H.create 99 ; send_msg ;
      cb_selectors = Selector.make_set () ;
      on_sets = Hashtbl.create 10 ;
      on_news = Hashtbl.create 10 ;
      on_dels = Hashtbl.create 10 ;
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

  let do_cbs cbs t k v =
    match
      Selector.matches k t.cb_selectors |>
      Enum.fold (fun v sel_id ->
        let cbs = Hashtbl.find_all cbs sel_id in
        List.fold_left (fun v cb ->
          match cb k v with
          | Some v -> v
          | None -> raise Exit
        ) v cbs
      ) v with
    | exception Exit ->
        Printf.sprintf2 "Key %a: change denied"
          Key.print k |>
        failwith
    | v ->
        v

  let notify t k has_capa m =
    let subscriber_sockets =
      Selector.matches k t.user_selectors |>
      Enum.fold (fun sockets sel_id ->
        Hashtbl.find_default t.subscriptions sel_id Map.empty |>
        Map.union sockets
      ) Map.empty in
    Map.enum subscriber_sockets //@
    (fun (socket, user) ->
      if has_capa user then Some socket else (
        !logger.debug "User %a has no capa" User.print user ;
        None
      )) |>
    t.send_msg m

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
  let do_unlock t k hv =
    match List.tl hv.locks with
    | [] ->
        hv.locks <- [] ;
        notify t k (User.has_capa hv.r) (UnlockKey k)
    | (u, duration) :: rest ->
        let expiry = Unix.gettimeofday () +. duration in
        hv.locks <- (u, expiry) :: rest ;
        let owner = IO.to_string User.print_id (User.id u) in
        notify t k (User.has_capa hv.r) (LockKey { k ; owner ; expiry})

  let timeout_locks t k hv =
    match hv.locks with
    | [] -> ()
    | (u, expiry) :: _ ->
        let now = Unix.gettimeofday () in
        if expiry < now then (
          !logger.warning "Timing out %a's lock of config key %a"
            User.print u
            Key.print k ;
          do_unlock t k hv)

  let check_unlocked t hv k u =
    timeout_locks t k hv ;
    match hv.locks with
    | (u', _) :: _ when not (User.equal u' u) ->
        locked_by k u'
    (* TODO: Think about making locking mandatory *)
    | _ -> ()

  let check_can_write t k hv u =
    if not (User.has_capa hv.w u) then (
      Printf.sprintf2 "Key %a: read only" Key.print k |>
      failwith
    ) ;
    check_unlocked t hv k u

  let check_can_delete t k hv u =
    if hv.s then (
      Printf.sprintf2 "Key %a: is sticky" Key.print k |>
      failwith
    ) ;
    check_can_write t k hv u

  let create t u k v ?(lock_timeo=Default.sync_lock_timeout) ~r ~w ~s =
    !logger.debug "Creating config key %a with value %a, r:%a w:%a%s"
      Key.print k
      Value.print v
      Capa.print r
      Capa.print w
      (if s then " sticky" else "") ;
    match H.find t.h k with
    | exception Not_found ->
        (* As long as there is a callback for this, that's ok: *)
        let v = do_cbs t.on_news t k v in
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
        H.add t.h k { v ; r ; w ; s ; locks ; set_by = u ; mtime } ;
        let msg = SrvMsg.NewKey { k ; v ; uid ; mtime ; owner ; expiry } in
        notify t k (User.has_capa r) msg
    | _ ->
        Printf.sprintf2 "Key %a: already exist"
          Key.print k |>
        failwith

  let update t u k v =
    match H.find t.h k with
    | exception Not_found ->
        no_such_key k
    | prev ->
        if not (Value.equal prev.v v) then (
          !logger.debug "Setting config key %a to value %a"
            Key.print k
            Value.print v ;
          check_can_write t k prev u ;
          let v = do_cbs t.on_sets t k v in
          prev.v <- v ;
          prev.set_by <- u ;
          prev.mtime <- Unix.gettimeofday () ;
          let uid = IO.to_string User.print_id (User.id u) in
          let msg = SrvMsg.SetKey { k ; v ; uid ; mtime = prev.mtime } in
          notify t k (User.has_capa prev.r) msg
        )

  let set t u k v = (* TODO: H.find and pass prev item to update *)
    if H.mem t.h k then
      update t u k v
    else
      let r = Capa.anybody
      and w = User.only_me u
      and s = false in
      create t u k v ~lock_timeo:0. ~r ~w ~s

  let del t u k =
    !logger.debug "Deleting config key %a"
      Key.print k ;
    match H.find t.h k with
    | exception Not_found ->
        ()
    | prev ->
        (* TODO: think about making locking mandatory *)
        check_can_delete t k prev u ;
        let _ = do_cbs t.on_dels t k prev.v in
        H.remove t.h k ;
        notify t k (User.has_capa prev.r) (DelKey k)

  let lock t u k ~must_exist ~lock_timeo =
    !logger.debug "Locking config key %a"
      Key.print k ;
    match H.find t.h k with
    | exception Not_found ->
        (* We must allow to lock a non-existent key to reserve the key to its
         * creator. In that case a lock will create a new (Void) value. *)
        if must_exist then no_such_key k else
        let me = User.only_me u in
        create t u k Value.dummy ~r:Capa.anybody ~w:me ~s:false ~lock_timeo
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
            notify t k (User.has_capa prev.r) (LockKey { k ; owner ; expiry })
        | lst ->
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

  (* Helper for internal user: *)
  let create_unlocked srv k v ~r ~w ~s =
    create srv User.internal k v ~lock_timeo:0. ~r ~w ~s

  let create_or_update srv k v ~r ~w ~s =
    match H.find srv.h k with
    | exception Not_found ->
        create_unlocked srv k v ~r ~w ~s
    | hv ->
        if not (Value.equal hv.v v) then (
          set srv User.internal k v
        )

  let subscribe_user t socket u sel =
    (* Add this selection to the known selectors, and add this selector
     * ID for this user to the subscriptions: *)
    let id = Selector.add t.user_selectors sel in
    let def = Map.singleton socket u in
    Hashtbl.modify_def def id (Map.add socket u) t.subscriptions

  let register_callback t cbs f sel =
    let id = Selector.add t.cb_selectors sel in
    Hashtbl.add cbs id f

  let owner_of_hash_value hv =
    match hv.locks with
    | [] -> "", 0.
    | (owner, expiry) :: _ ->
        IO.to_string User.print_id (User.id owner),
        expiry

  let initial_sync t socket u sel =
    !logger.info "Initial synchronisation for user %a" User.print u ;
    let s = Selector.make_set () in
    let _ = Selector.add s sel in
    H.iter (fun k hv ->
      if User.has_capa hv.r u &&
         not (Enum.is_empty (Selector.matches k s))
      then (
        timeout_locks t k hv ;
        let uid = IO.to_string User.print_id (User.id hv.set_by) in
        let owner, expiry = owner_of_hash_value hv in
        let msg = SrvMsg.NewKey { k ; v = hv.v ; uid ; mtime = hv.mtime ;
                                  owner ; expiry } in
        t.send_msg msg (Enum.singleton socket)
      )
    ) t.h ;
    !logger.info "...done"

  let set_user_err t u socket i str =
    let k = Key.user_errs u socket
    and v = Value.err_msg i str in
    set t User.internal k v

  let process_msg t socket u (i, cmd as msg) =
    try
      !logger.debug "Received msg %a from %a"
        CltMsg.print msg
        User.print u ;
      (match cmd with
      | CltMsg.Auth creds ->
          (* Auth is special: as we have no user yet, errors must be
           * returned directly. *)
          (try
            let u' = User.authenticate u creds socket in
            !logger.info "User %a authenticated out of user %a"
              User.print u'
              User.print u ;
            (* Must create this user's error object if not already there.
             * Value will be set below: *)
            let k = Key.user_errs u' socket in
            create_or_update t k (Value.err_msg i "")
                             ~r:(User.only_me u') ~w:Capa.nobody ~s:false ;
            t.send_msg (SrvMsg.AuthOk k) (Enum.singleton socket)
          with e ->
            let err = Printexc.to_string e in
            !logger.info "While authenticating %a: %s" User.print u err ;
            t.send_msg (SrvMsg.AuthErr err) (Enum.singleton socket))

      | CltMsg.StartSync sel ->
          subscribe_user t socket u sel ;
          (* Then send everything that matches this selection and that the
           * user can read: *)
          initial_sync t socket u sel

      | CltMsg.SetKey (k, v) ->
          set t u k v

      | CltMsg.NewKey (k, v, lock_timeo) ->
          let r = Capa.anybody
          and w = User.only_me u
          and s = false in
          create t u k v ~r ~w ~s ~lock_timeo

      | CltMsg.UpdKey (k, v) ->
          update t u k v

      | CltMsg.DelKey k ->
          del t u k

      | CltMsg.LockKey (k, lock_timeo) ->
          lock t u k ~must_exist:true ~lock_timeo

      | CltMsg.LockOrCreateKey (k, lock_timeo) ->
          lock t u k ~must_exist:false ~lock_timeo

      | CltMsg.UnlockKey k ->
          unlock t u k
      ) ;
      if User.authenticated u then set_user_err t u socket i ""
    with e ->
      set_user_err t u socket i (Printexc.to_string e)
end
