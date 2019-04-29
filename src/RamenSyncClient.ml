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
      send_msg : CltMsg.t -> unit ;
      on_new : t -> Key.t -> Value.t -> unit ;
      on_set : t -> Key.t -> Value.t -> unit ;
      on_del : t -> Key.t -> unit ;
      on_lock : t -> Key.t -> unit ;
      on_unlock : t -> Key.t -> unit }

  and hash_value =
    { mutable v : Value.t ;
      mutable locked : bool }

  let make ~send_msg ~on_new ~on_set ~on_del ~on_lock ~on_unlock =
    { h = H.create 99 ; send_msg ;
      on_new ; on_set ; on_del ; on_lock ; on_unlock }

  let process_msg t = function
    | SrvMsg.BadAuth e ->
        Printf.sprintf "Cannot authenticate to the server: %s" e |>
        failwith

    | SrvMsg.SetKey (k, v) ->
        (match H.find t.h k with
        | exception Not_found ->
            let hv = { v ; locked = true } in
            H.add t.h k hv ;
            t.on_new t k v
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

    | SrvMsg.DelKey k ->
        if H.mem t.h k then (
          H.remove t.h k ;
          t.on_del t k
        ) else
          !logger.error "Server wanted to delete an unknown key %a"
            Key.print k

    | SrvMsg.LockKey k ->
        (match H.find t.h k with
        | exception Not_found ->
            !logger.error "Server want to lock unknown key %a"
              Key.print k
        | prev ->
            if prev.locked then (
              !logger.error "Server locked key %a that is already locked"
                Key.print k ;
            ) else (
              prev.locked <- true ;
              t.on_lock t k
            )
        )

    | SrvMsg.UnlockKey k ->
        (match H.find t.h k with
        | exception Not_found ->
            !logger.error "Server want to unlock unknown key %a"
              Key.print k
        | prev ->
            if not prev.locked then (
              !logger.error "Server unlocked key %a that is not locked"
                Key.print k ;
            ) else (
              prev.locked <- false ;
              t.on_unlock t k
            )
        )
end
