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
  module Capa = User.Capa
  module Selector = Selector
  module H = Hashtbl.Make (Key)

  include Messages (Key) (Value) (Selector)

  type t =
    { h : hash_value H.t ;
      on_new : t -> Key.t -> Value.t -> string -> unit ;
      on_set : t -> Key.t -> Value.t -> unit ;
      on_del : t -> Key.t -> unit ;
      on_lock : t -> Key.t -> string -> unit ;
      on_unlock : t -> Key.t -> unit }

  and hash_value =
    { mutable v : Value.t ;
      mutable locked : string option }

  let make ~on_new ~on_set ~on_del ~on_lock ~on_unlock =
    { h = H.create 99 ;
      on_new ; on_set ; on_del ; on_lock ; on_unlock }

  let process_msg t = function
    | SrvMsg.Auth err ->
        if err <> "" then
          Printf.sprintf "Cannot authenticate to the server: %s" err |>
          failwith

    | SrvMsg.SetKey (k, v) ->
        (match H.find t.h k with
        | exception Not_found ->
            !logger.error
              "Server set key %a that has not been created"
              Key.print k ;
            let hv = { v ; locked = None } in
            H.add t.h k hv ;
            t.on_new t k v "unknown"
        | prev ->
            if Value.equal prev.v v then (
              !logger.error
                "Server wanted to replace a value of key %a by the same: %a"
                Key.print k
                Value.print v
              (* TODO: count this *)
            ) else (
              prev.v <- v ;
              t.on_set t k v
            )
        )

    | SrvMsg.NewKey (k, v, uid) ->
        (match H.find t.h k with
        | exception Not_found ->
            let hv = { v ; locked = Some uid } in
            H.add t.h k hv ;
            t.on_new t k v uid
        | prev ->
            !logger.error
              "Server create key %a that already exist, updating"
              Key.print k ;
            prev.v <- v ;
            t.on_set t k v
        )

    | SrvMsg.DelKey k ->
        if H.mem t.h k then (
          H.remove t.h k ;
          t.on_del t k
        ) else
          !logger.error "Server wanted to delete an unknown key %a"
            Key.print k

    | SrvMsg.LockKey (k, u) ->
        (match H.find t.h k with
        | exception Not_found ->
            !logger.error "Server want to lock unknown key %a"
              Key.print k
        | prev ->
            if prev.locked <> None then (
              !logger.error "Server locked key %a that is already locked"
                Key.print k ;
            ) else (
              prev.locked <- Some u ;
              t.on_lock t k u
            )
        )

    | SrvMsg.UnlockKey k ->
        (match H.find t.h k with
        | exception Not_found ->
            !logger.error "Server want to unlock unknown key %a"
              Key.print k
        | prev ->
            if prev.locked = None then (
              !logger.error "Server unlocked key %a that is not locked"
                Key.print k ;
            ) else (
              prev.locked <- None ;
              t.on_unlock t k
            )
        )
end
