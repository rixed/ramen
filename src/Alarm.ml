(* An alarm is a tool that will call back a function at some point in time. You
 * can reprogram it whenever you want, by changing the time or the callback.
 * The alarm will fire only once, but you can reuse it (including: you can
 * reprogram it from your callback to be called again).
 *
 * It works by having a clock event called from within the main event loop.
 *)

type t =
  { (* true if in all_alarms *)
    mutable queued : bool ;

    (* time must not be changed once it's in the Heap or the heap invariant
     * would be broken *)
    mutable time : float ;

    (* since the callback is likely to update time, do not call it when the
     * alarm is queued. *)
    mutable callback : unit -> unit }

let make () =
  { queued = false ; time = 0. ; callback = ignore }

(* FIXME: Turn this into a heap ordered by time. *)
let all_alarms = ref []

(* Gives the time to all operations that needs it. Avoid plenty of slow
 * syscalls. *)
let now = ref (Unix.gettimeofday ())
let timestep = ref 1.

(* Set the alarm to go off at the given time. *)
let at alarm time callback =
  assert (not alarm.queued) ;
  alarm.time <- time ;
  alarm.callback <- callback ;
  (* Enqueue regardless of time because caller expect his callback to be
   * called no matter what: *)
  alarm.queued <- true ;
  all_alarms := alarm :: !all_alarms

(* Helper to get a callback called regularly. *)
let every n =
  if n < !timestep then
    Printf.eprintf "WARNING: timestep (%f) is less than some periodic alarms (%f)\n%!"
      !timestep n ;
  fun f ->
    let alarm = make () in
    let rec reschedule () =
      at alarm (!now +. n) (fun () ->
        f () ;
        reschedule ())
    in
    reschedule ()

(* This must be called repeatedly *)
let run_until =
  every 60. (fun () ->
    Printf.printf "%d alarms queued\n%!" (List.length !all_alarms)) ;
  fun t ->
    now := t ;
    (* Beware that callbacks can add/modify alarms *)
    let all = !all_alarms in
    all_alarms := [] ;
    let rem = List.filter (fun alarm ->
        assert(alarm.queued) ;
        if alarm.time > t then true else (
          alarm.queued <- false ;
          alarm.callback () ;
          alarm.queued
        )
      ) all in
    (* New alarms might have been added. Alarms that were already
     * on the list have not been added again. *)
    all_alarms := List.rev_append !all_alarms rem

(* Main loop that just call run_until *)
let rec main_loop () =
  let%lwt () = Lwt_unix.sleep !timestep in
  run_until (Unix.gettimeofday ()) ;
  main_loop ()
