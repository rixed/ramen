open Batteries
open RamenHelpersNoLog
open RamenLog
open RamenSyncIntf
open RamenConsts
module Metric = RamenConstsMetric

(*
 * Stats on the synchronisation process
 *)

open Binocle

let stats_resp_time =
  Histogram.make Metric.Names.sync_resp_time_server
    "Response time per command to the confserver."
    Histogram.powers_of_two

(* A KV store implementing sync mechanism, with still no side effects *)
module Make (Value : VALUE) (Selector : SELECTOR) =
struct
  module Key = Selector.Key
  module User = Key.User
  module Role = User.Role
  module Selector = Selector
  module H = Hashtbl.Make (Key)

  module MapOfSockets = Map.Make (struct
    type t = User.socket
    let compare = User.compare_sockets
  end)

  include Messages (Key) (Value) (Selector)

  type t =
    { h : hash_value H.t ;
      (* Order of appearance of each key, for ordering initial syncs: *)
      mutable next_key_seq : int ;
      user_db : User.db ;
      send_msg : (User.socket * SrvMsg.t) Enum.t -> unit ;
      subscriptions :
        (Selector.id, Selector.t * User.t MapOfSockets.t) Hashtbl.t }

  and hash_value =
    { mutable v : Value.t ;
      (* Order in the sequence of key creation. Also stored in snapshots. *)
      key_seq : int ;
      (* Also cache the prepared_key: *)
      prepared_key : Selector.prepared_key ;
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

  (* That float is an absolute time at the head of the list and a
   * duration for other lockers. The optional int is the recursive count. *)
  and lock = User.t * float * int ref option

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
      subscriptions = Hashtbl.create 99 }

  let print_lockers oc = function
    | [] ->
        Printf.fprintf oc "None"
    | (current_locker, expiry, rec_count) :: rest ->
        let rec_count_print oc = function
          | None -> ()
          | Some n -> Printf.fprintf oc "x%d" !n in
        Printf.fprintf oc "%a%a (until %a) (then: %a)"
          User.print current_locker
          rec_count_print rec_count
          print_as_date expiry
          (List.print (fun oc (u, d, c) ->
            Printf.fprintf oc "%a%a for %a"
              User.print u
              rec_count_print c
              print_as_duration d)) rest

  let notify t k prepared_key is_permitted m =
    let subscriber_sockets =
      Hashtbl.values t.subscriptions //
      (fun (sel, _map) -> Selector.matches sel prepared_key) |>
      Enum.fold (fun sockets (sel, map) ->
        !logger.debug "Selector %a matched key %a"
          Selector.print sel Key.print k ;
        (* Same as MapOfSockets.union socket map (FIXME: add to batteries) *)
        MapOfSockets.fold MapOfSockets.add map sockets
      ) MapOfSockets.empty in
    MapOfSockets.enum subscriber_sockets //@
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

  (* Remove the head locker (regardless of the recursion count) and notify
   * of lock change: *)
  let do_unlock ?(and_notify=true) t k hv =
    match List.tl hv.locks with
    | [] ->
        hv.locks <- [] ;
        if and_notify then
          notify t k hv.prepared_key (User.has_any_role hv.can_read)
                 (fun _ -> UnlockKey k)
    | (u, duration, rec_count) :: rest ->
        let expiry = Unix.gettimeofday () +. duration in
        hv.locks <- (u, expiry, rec_count) :: rest ;
        let owner = IO.to_string User.print_id (User.id u) in
        if and_notify then
          notify t k hv.prepared_key (User.has_any_role hv.can_read)
                 (fun _ -> LockKey { k ; owner ; expiry})

  let timeout_locks ?and_notify t k hv =
    match hv.locks with
    | [] -> ()
    | (u, expiry, rec_count) :: _ ->
        let now = Unix.gettimeofday () in
        if expiry < now then (
          !logger.warning "Timing out %a's %slock on %a (expired %a ago)"
            User.print u
            (if rec_count = None then "" else "recursive ")
            Key.print k
            print_as_duration (now -. expiry) ;
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
    | (u', _, _) :: _ when not (User.equal u' u) ->
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

  let create t u k v ~lock_timeo ~recurs ~can_read ~can_write ~can_del ~echo =
    !logger.debug "Creating %a with value %a, read:%a write:%a del:%a"
      Key.print k
      Value.print v
      (Set.print Role.print) can_read
      (Set.print Role.print) can_write
      (Set.print Role.print) can_del ;
    match H.find t.h k with
    | exception Not_found ->
        (* As long as there is a callback for this, that's ok: *)
        let mtime = Unix.gettimeofday ()
        and uid = IO.to_string User.print_id (User.id u)
        and prepared_key = Selector.prepare_key k in
        (* Objects are created locked unless timeout is <= 0 or no username
         * is set (to avoid spurious warnings): *)
        let locks, owner, expiry =
          if lock_timeo > 0. && uid <> "" then
            let expiry = mtime +. lock_timeo
            and rec_count = if recurs then Some (ref 1) else None in
            [ u, expiry, rec_count ], uid, expiry
          else
            [], "", 0. in
        H.add t.h k { v ; key_seq = t.next_key_seq ; prepared_key ;
                      can_read ; can_write ; can_del ;
                      locks ; set_by = u ; mtime } ;
        t.next_key_seq <- t.next_key_seq + 1 ;
        let msg u =
          let can_write = User.has_any_role can_write u
          and can_del = User.has_any_role can_del u in
          SrvMsg.NewKey { k ; v ; uid ; mtime ; can_write ; can_del ;
                          owner ; expiry } in
        let is_permitted user =
          User.has_any_role can_read user &&
          (echo || not (User.equal user u)) in
        notify t k prepared_key is_permitted msg
    | _ ->
        Printf.sprintf2 "Key %a: already exist"
          Key.print k |>
        failwith

  let update t u k v ~echo =
    match H.find t.h k with
    | exception Not_found ->
        no_such_key k
    | prev ->
        !logger.debug "Setting %a to value %a"
          Key.print k
          Value.print v ;
        check_can_write t k prev u ;
        prev.v <- v ;
        prev.set_by <- u ;
        prev.mtime <- Unix.gettimeofday () ;
        let uid = IO.to_string User.print_id (User.id u) in
        let msg _ = SrvMsg.SetKey { k ; v ; uid ; mtime = prev.mtime } in
        let is_permitted user =
          User.has_any_role prev.can_read user &&
          (echo || not (User.equal user u)) in
        notify t k prev.prepared_key is_permitted msg

  let set t u k v ~echo = (* TODO: H.find and pass prev item to update *)
    if H.mem t.h k then
      update t u k v ~echo
    else
      let can_read, can_write, can_del =
        Key.permissions (User.id u) k in
      create t u k v ~lock_timeo:0. ~recurs:false ~can_read ~can_write ~can_del
             ~echo

  let del t u k ~echo =
    !logger.debug "Deleting %a" Key.print k ;
    match H.find t.h k with
    | exception Not_found ->
        !logger.warning "Cannot delete non existent key %a" Key.print k
    | prev ->
        (* TODO: think about making locking mandatory *)
        check_can_delete t k prev u ;
        H.remove t.h k ;
        let is_permitted user =
          User.has_any_role prev.can_read user &&
          (echo || not (User.equal user u)) in
        notify t k prev.prepared_key is_permitted (fun _ -> DelKey k)

  let lock t u k ~must_exist ~lock_timeo ~recurs =
    !logger.debug "Locking %a to user %a"
      Key.print k
      User.print_id (User.id u) ;
    match H.find t.h k with
    | exception Not_found ->
        (* We must allow to lock a non-existent key to reserve the key to its
         * creator. In that case a lock will create a new (Void) value. *)
        if must_exist then no_such_key k else
        let can_read, can_write, can_del =
          Key.permissions (User.id u) k in
        create t u k Value.dummy ~can_read ~can_write ~can_del ~lock_timeo ~recurs
               ~echo:true (* Locks/Unlocks are always echoed *)
    | prev ->
        let do_notify owner expiry =
          let is_permitted = User.has_any_role prev.can_read in
          notify t k prev.prepared_key is_permitted
                 (fun _ -> LockKey { k ; owner ; expiry }) in
        timeout_locks t k prev ;
        !logger.debug "Current lockers: %a" print_lockers prev.locks ;
        let one_count = if recurs then Some (ref 1) else None in
        (* only for wlocks: check_can_write t k prev u ; *)
        (match prev.locks with
        | [] ->
            (* We have a new locker: *)
            let owner = IO.to_string User.print_id (User.id u) in
            (* As empty owner means no owner, prevent empty usernames to lock
             * a key: *)
            if owner = "" then
              failwith "Cannot lock with no username" ;
            let expiry = Unix.gettimeofday () +. lock_timeo in
            prev.locks <- [ u, expiry, one_count ] ;
            do_notify owner expiry
        | (owner, expiry, rec_count) :: rest as lst ->
            let incr_some = function
              | None -> assert false
              | Some iref -> incr iref in
            (* Err out if the user is already the current locker: *)
            if User.equal u owner then (
              if not recurs || rec_count = None then (
                Printf.sprintf2 "User %a already owns %a"
                  User.print u
                  Key.print k |>
                failwith
              ) else (
                (* Although she will receive an OK response, the client might
                 * still want to know when she become the actual owner as the
                 * result of this new lock. There is no way to communicate that
                 * though, since sending a Lock notification would no tell her
                 * anything she does not know already (she knows already that
                 * she owns that key, and the Lock notification would not be
                 * unambiguously connected to this very Lock command.
                 * The client could have a look at the current owner but that
                 * not very robust and convoluted. So here we do again send a
                 * notification in that case; by the way, clients will then
                 * learn what's the new expiry (which should be made a bit
                 * longer, cf https://github.com/rixed/ramen/issues/1288). *)
                incr_some rec_count ;
                let owner = IO.to_string User.print_id (User.id u) in
                do_notify owner expiry
              )
            ) else (
              (* Reject it if it's already in the lockers: *)
              match List.find (fun (u', _, _) -> User.equal u u') rest with
              | exception Not_found ->
                  (* Only when user is not already in the lockers, add it.
                   * Client will be notified when she becomes the owner. *)
                  prev.locks <- lst @ [ u, lock_timeo, one_count ] (* FIXME: faster*)
              | _, _duration, rec_count ->
                  if not recurs || rec_count = None then
                    Printf.sprintf2 "User %a is already waiting for %a lock"
                      User.print u
                      Key.print k |>
                    failwith
                  else
                    (* Client will be notified when she becomes the owner *)
                    incr_some rec_count))

  let unlock t u k =
    match H.find t.h k with
    | exception Not_found ->
        no_such_key k
    | prev ->
        (match prev.locks with
        | (u', _, (None | Some { contents = 1 })) :: _ when User.equal u u' ->
            !logger.debug "Unlocking %a from %a"
              Key.print k
              User.print_id (User.id u) ;
            do_unlock t k prev
        | (u', _, Some c) :: _ when User.equal u u' ->
            (* There is no notification of unlocking in this case;
             * client might want to use the on_ok callback rather than on_done
             * in order to proceed. *)
            decr c ;
            !logger.debug "Unlocking %a from %a (still owns %d refs)"
              Key.print k
              User.print_id (User.id u)
              !c ;
            assert (!c > 0) ;
        | (u', _, _) :: _ ->
            locked_by k u'
        | [] ->
            Printf.sprintf2 "Key %a: not locked" Key.print k |>
            failwith)

  let create_or_update srv k v ~can_read ~can_write ~can_del ~echo =
    match H.find srv.h k with
    | exception Not_found ->
        create srv User.internal k v ~lock_timeo:0. ~recurs:false
               ~can_read ~can_write ~can_del ~echo
    | hv ->
        (* create_or_update is only for internal use, no client will wait
         * an answer; So it's OK to skip NOPs: *)
        if not (Value.equal hv.v v) then
          set srv User.internal k v ~echo

  let subscribe_user t socket u sel =
    (* Add this selection to the known selectors, and add this selector
     * ID for this user to the subscriptions: *)
    let id = Selector.to_id sel in
    !logger.debug "User %a has selection %a for %a"
      User.print u
      Selector.print_id id
      Selector.print sel ;
    (* Note: [Map.add] will replace any previous mapping for [socket]: *)
    Hashtbl.modify_def (sel, MapOfSockets.empty) id (fun (sel, map) ->
      sel, MapOfSockets.add socket u map
    ) t.subscriptions

  let owner_of_hash_value hv =
    match hv.locks with
    | [] -> "", 0.
    | (owner, expiry, _) :: _ ->
        IO.to_string User.print_id (User.id owner),
        expiry

  let initial_sync t socket u sel =
    !logger.debug "Initial synchronisation for user %a: Starting!" User.print u ;
    let sorted =
      H.enum t.h //
      (fun (_k, hv) ->
        User.has_any_role hv.can_read u &&
        Selector.matches sel hv.prepared_key
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
    set t User.internal k v ~echo:true

  let label_of_command = function
    | CltMsg.Auth _ -> "Auth"
    | StartSync _ -> "StartSync"
    | SetKey _ -> "SetKey"
    | NewKey _ -> "NewKey"
    | UpdKey _ -> "UpdKey"
    | DelKey _ -> "DelKey"
    | LockKey _ -> "LockKey"
    | LockOrCreateKey _ -> "LockOrCreateKey"
    | UnlockKey _ -> "UnlockKey"
    | Bye -> "Bye"

  let process_msg t socket u clt_pub_key msg =
    let start_time = Unix.gettimeofday () in
    let u', status_label =
      match msg.CltMsg.cmd with
      | CltMsg.Auth (uid, _timeout) ->
          (* Auth is special: as we have no user yet, errors must be
           * returned directly. *)
          (try
            let u' = User.authenticate t.user_db u uid clt_pub_key in
            !logger.info "User %a authenticated out of user %a on socket %a"
              User.print u'
              User.print u
              User.print_socket socket ;
            (* Must create this user's error object if not already there.
             * Value will be set below: *)
            let k = Key.user_errs u' socket in
            let can_read = Set.of_list Role.[ Specific (User.id u') ] in
            let can_write = Set.empty in
            let can_del = can_read in
            (* Original creation of the error file is sent regardless of
             * msg.confirm_success: *)
            create_or_update t k (Value.err_msg msg.seq "")
                             ~can_read ~can_write ~can_del ~echo:true ;
            t.send_msg (Enum.singleton (socket, SrvMsg.AuthOk socket)) ;
            u', "ok"
          with e ->
            let err = Printexc.to_string e in
            !logger.warning "While authenticating %a: %s" User.print u err ;
            t.send_msg (Enum.singleton (socket, SrvMsg.AuthErr err)) ;
            u, "error" (* [err] might have high cardinality *))
      | _ ->
          if not (User.is_authenticated u) then (
            let err = "Must authenticate" in
            t.send_msg (Enum.singleton (socket, SrvMsg.AuthErr err)) ;
            u, "no-auth"
          ) else (
            let echo = msg.echo in
            try
              (match msg.cmd with
              | CltMsg.Auth _ ->
                  assert false (* Handled above *)
              | CltMsg.StartSync sel ->
                  subscribe_user t socket u sel ;
                  (* Then send everything that matches this selection and that the
                   * user can read: *)
                  initial_sync t socket u sel
              | CltMsg.SetKey (k, v) ->
                  set t u k v ~echo
              | CltMsg.NewKey (k, v, lock_timeo, recurs) ->
                  let can_read, can_write, can_del =
                    Key.permissions (User.id u) k in
                  create t u k v ~can_read ~can_write ~can_del
                         ~lock_timeo ~recurs ~echo
              | CltMsg.UpdKey (k, v) ->
                  update t u k v ~echo
              | CltMsg.DelKey k ->
                  del t u k ~echo
              | CltMsg.LockKey (k, lock_timeo, recurs) ->
                  lock t u k ~must_exist:true ~lock_timeo ~recurs
              | CltMsg.LockOrCreateKey (k, lock_timeo, recurs) ->
                  lock t u k ~must_exist:false ~lock_timeo ~recurs
              | CltMsg.UnlockKey k ->
                  unlock t u k
              | CltMsg.Bye ->
                  (* A disconnected user keep its locks, but maybe they should be
                   * shortened? *)
                  (* TODO: Delete user form the conftree below "users/socket" *)
                  ()) ;
              if msg.confirm_success then
                set_user_err t u socket msg.seq "" ;
              u, "ok"
            with e ->
              let err = Printexc.to_string e in
              set_user_err t u socket msg.seq err ;
              u, "error" (* [err] might be high cardinality *)
          ) ;
      in
    (* Update stats_resp_time: *)
    let resp_time = Unix.gettimeofday () -. start_time in
    let cmd_label = label_of_command msg.cmd in
    let labels = [ "command", cmd_label ; "status", status_label ] in
    Histogram.add stats_resp_time ~labels resp_time ;
    u'
end
