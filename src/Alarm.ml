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
let every n f =
  let alarm = make () in
  let rec reschedule () =
    at alarm (!now +. float_of_int n) (fun () ->
      f () ;
      reschedule ())
  in
  reschedule ()

(* This must be called repeatedly *)
let run_until =
  every 60 (fun () ->
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

(* Main loop. You can have your own but don't forget run_until! *)
let main_loop get_next_event input =
  (* Periodically report the event rate: *)
  let event_count = ref 0 in
  let last_report_time = ref (Unix.gettimeofday ()) in
  every 5 (fun () ->
    let dt = !now -. !last_report_time in
    let rate = float_of_int !event_count /. dt in
    Printf.printf "%.0f events/sec\n%!" rate ;
    event_count := 0 ;
    last_report_time := !now) ;
  (* Process that many events before updating the clock
   * to save costly syscalls: *)
  let event_batch_size = 10 in
  let rec loop t batch_sz =
    run_until t ;
    get_next_event () |> input ;
    incr event_count ;
    let t', batch_sz' =
      if batch_sz = event_batch_size then (
        Unix.gettimeofday (), 0
      ) else (
        t, batch_sz + 1
      ) in
    loop t' batch_sz' in
  let start = Unix.gettimeofday () in
  loop start 0

