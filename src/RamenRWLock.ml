(* TODO: Shouldn't there be a rwlock in batteries? *)
(* Mostly copy-pasta from
 * http://www.cs.cornell.edu/courses/cs3110/2011fa/recitations/rec16.html
 * and uglified with Lwt. *)
open Lwt

type t =
  { mutable num_readers : int ;
    mutable has_writer : bool ;
    mutex : Lwt_mutex.t ;
    can_be_read : unit Lwt_condition.t ;
    can_be_written : unit Lwt_condition.t }

let make () =
  { num_readers = 0 ; has_writer = false ;
    mutex = Lwt_mutex.create () ;
    can_be_read = Lwt_condition.create () ;
    can_be_written = Lwt_condition.create () }

let r_lock t =
  Lwt_mutex.with_lock t.mutex (fun () ->
    while%lwt t.has_writer do
      Lwt_condition.wait ~mutex:t.mutex t.can_be_read
    done ;%lwt
    t.num_readers <- t.num_readers + 1 ;
    return_unit)

let r_unlock t =
  Lwt_mutex.with_lock t.mutex (fun () ->
    t.num_readers <- t.num_readers - 1 ;
    if t.num_readers = 0 then
      Lwt_condition.signal t.can_be_written () ;
    return_unit)

let with_r_lock t f =
  r_lock t ;%lwt
  finalize f (fun () -> r_unlock t)

let w_lock t =
  Lwt_mutex.with_lock t.mutex (fun () ->
    while%lwt t.num_readers > 0 || t.has_writer do
      Lwt_condition.wait ~mutex:t.mutex t.can_be_written
    done ;%lwt
    t.has_writer <- true ;
    return_unit)

let w_unlock t =
  Lwt_mutex.with_lock t.mutex (fun () ->
    t.has_writer <- false ;
    Lwt_condition.signal t.can_be_written () ;
    Lwt_condition.broadcast t.can_be_read () ;
    return_unit)

let with_w_lock t f =
  w_lock t ;%lwt
  finalize f (fun () -> w_unlock t)
