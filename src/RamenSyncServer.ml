open Batteries
open RamenHelpers
open RamenLog
open RamenSyncIntf

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
      mutable l : User.t option (* Locked by that user. TODO: add a recursion count? *) ;
      (* Also some metadata: *)
      mutable set_by : User.t ;
      mutable mtime : float }

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
      if has_capa user then Some socket else None) |>
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

  let check_unlocked hv k u =
    match hv.l with
    | Some u' when not (User.equal u' u) ->
        locked_by k u'
    | _ -> ()

  let check_can_update k hv u =
    if not (User.has_capa hv.w u) then (
      Printf.sprintf2 "Key %a: read only" Key.print k |>
      failwith
    ) ;
    check_unlocked hv k u

  let check_can_delete k hv u =
    if hv.s then (
      Printf.sprintf2 "Key %a: is sticky" Key.print k |>
      failwith
    ) ;
    check_can_update k hv u

  let create t u k v ~r ~w ~s =
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
        let l = Some u in (* objects are created locked *)
        let mtime = Unix.gettimeofday () in
        H.add t.h k { v ; r ; w ; s ; l ; set_by = u ; mtime } ;
        let uid = IO.to_string User.print_id (User.id u) in
        notify t k (User.has_capa r) (NewKey (k, v, uid, mtime))
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
          (* TODO: Think about making locking mandatory *)
          check_can_update k prev u ;
          let v = do_cbs t.on_sets t k v in
          prev.v <- v ;
          prev.set_by <- u ;
          prev.mtime <- Unix.gettimeofday () ;
          let uid = IO.to_string User.print_id (User.id u) in
          notify t k (User.has_capa prev.r) (SetKey (k, v, uid, prev.mtime))
        )

  let set t u k v = (* TODO: H.find and pass prev item to update *)
    if H.mem t.h k then update t u k v
    else
      let r = Capa.anybody
      and w = User.only_me u
      and s = false in
      create t u k v ~r ~w ~s

  let del t u k =
    !logger.debug "Deleting config key %a"
      Key.print k ;
    match H.find t.h k with
    | exception Not_found ->
        ()
    | prev ->
        (* TODO: think about making locking mandatory *)
        check_can_delete k prev u ;
        let _ = do_cbs t.on_dels t k prev.v in
        H.remove t.h k ;
        notify t k (User.has_capa prev.r) (DelKey k)

  let lock t u k ~must_exist =
    !logger.debug "Locking config key %a"
      Key.print k ;
    match H.find t.h k with
    | exception Not_found ->
        (* We must allow to lock a non-existent key to reserve the key to its
         * creator. In that case a lock will create a new (Void) value. *)
        if must_exist then no_such_key k else
        let me = User.only_me u in
        create t u k Value.dummy ~r:Capa.anybody ~w:me ~s:false
    | prev ->
        check_can_update k prev u ;
        (match prev.l with
        | Some u' when User.equal u u' -> ()
        | _ ->
            let uid = IO.to_string User.print_id (User.id u) in
            notify t k (User.has_capa prev.r) (LockKey (k, uid)) ;
            prev.l <- Some u)

  let unlock t u k =
    !logger.debug "Unlocking config key %a"
      Key.print k ;
    match H.find t.h k with
    | exception Not_found ->
        no_such_key k
    | prev ->
        check_can_update k prev u ;
        (match prev.l with
        | Some u' when User.equal u u' ->
            notify t k (User.has_capa prev.r) (UnlockKey k) ;
            prev.l <- None
        | Some u' ->
            locked_by k u'
        | None ->
            Printf.sprintf2 "Key %a: not locked" Key.print k |>
            failwith)

  (* Helper for internal user: *)
  let create_unlocked srv k v ~r ~w ~s =
    create srv User.internal k v ~r ~w ~s ;
    unlock srv User.internal k

  let create_or_update srv k v ~r ~w ~s =
    match H.find srv.h k with
    | exception Not_found ->
        create_unlocked srv k v ~r ~w ~s
    | hv ->
        if not (Value.equal hv.v v) then (
          (* We have to disregard locks, as config file has priority
           * (until it goes away) *)
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

  let initial_sync t socket u sel =
    !logger.info "Initial synchronisation for user %a" User.print u ;
    let s = Selector.make_set () in
    let _ = Selector.add s sel in
    H.iter (fun k hv ->
      if User.has_capa hv.r u &&
         not (Enum.is_empty (Selector.matches k s))
      then (
        let uid =
          IO.to_string User.print_id (User.id hv.set_by) in
        t.send_msg (SrvMsg.NewKey (k, hv.v, uid, hv.mtime))
                   (Enum.singleton socket) ;
        (* Will be created locked by client: *)
        if hv.l = None then
          t.send_msg (SrvMsg.UnlockKey k) (Enum.singleton socket)
      )
    ) t.h ;
    !logger.info "...done"

  let set_user_err t u i str =
    let k = Key.user_errs u
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
          let err =
            try
              let u' = User.authenticate u creds socket in
              !logger.info "User %a authenticated out of user %a"
                User.print u'
                User.print u ;
              (* Must create this user's error object if not already there.
               * Value will be set below: *)
              let k = Key.user_errs u' in
              create_or_update t k (Value.err_msg i "")
                               ~r:(User.only_me u') ~w:Capa.nobody ~s:false ;
              ""
            with e ->
              let err = Printexc.to_string e in
              !logger.info "While authenticating %a: %s" User.print u err ;
              err
          in
          t.send_msg (SrvMsg.Auth err) (Enum.singleton socket)

      | CltMsg.StartSync sel ->
          subscribe_user t socket u sel ;
          (* Then send everything that matches this selection and that the
           * user can read: *)
          initial_sync t socket u sel

      | CltMsg.SetKey (k, v) ->
          set t u k v

      | CltMsg.NewKey (k, v) ->
          let r = Capa.anybody
          and w = User.only_me u
          and s = false in
          create t u k v ~r ~w ~s

      | CltMsg.UpdKey (k, v) ->
          update t u k v

      | CltMsg.DelKey k ->
          del t u k

      | CltMsg.LockKey k ->
          lock t u k ~must_exist:true

      | CltMsg.LockOrCreateKey k ->
          lock t u k ~must_exist:false

      | CltMsg.UnlockKey k ->
          unlock t u k
      ) ;
      if User.authenticated u then  set_user_err t u i ""
    with e ->
      set_user_err t u i (Printexc.to_string e)
end
