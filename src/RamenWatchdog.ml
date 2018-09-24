(* Watchdog: exit the process if not reset regularly.
 *
 * Used by all daemon and workers.
 *
 * The process is terminated first with the quit flag, then by calling
 * exit. *)
open RamenLog

type t =
  { name : string ;
    (* The thread that monitors the watchdog, started by make once and
     * for all: *)
    mutable monitor : Thread.t ;
    (* So that a watchdog can be disabled when we have to block for an
     * undetermined amount of time. Watchdogs start enabled though. *)
    mutable enabled : bool ;
    quit_flag : int option ref ;
    (* Time of last reset: *)
    mutable last_reset : float ;
    (* Set to the time when we have witnessed the quit flag was on for
     * the first time (0. for never): *)
    mutable quitting_since : float ;
    (* Loops can be legitimately slower at the beginning, so allow for
     * a longer timeout initially (this adds to the normal timeout the
     * first time): *)
    mutable grace_period : float ;
    (* The duration after which we begin the termination if the watchdog
     * has not been reset: *)
    timeout : float ;
    (* The duration it normally take to terminate after the quit flag is
     * set: *)
    quit_timeout : float }

let rec monitor t =
  if not t.enabled then (
    Unix.sleepf (t.timeout /. 2.)
  ) else if !(t.quit_flag) = None then (
    let now = Unix.time () in
    if now -. t.last_reset > t.timeout +. t.grace_period then (
      !logger.error "Forced quit from watchdog %S \
                     (last reset was %fs ago)!"
        t.name (now -. t.last_reset) ;
      t.quit_flag := Some RamenConsts.ExitCodes.watchdog ;
      Unix.sleepf t.quit_timeout
    ) else (
      Unix.sleepf (t.timeout /. 4.)
    )
  ) else ( (* t.quit_flag is set *)
    let now = Unix.time () in
    if t.quitting_since = 0. then
      t.quitting_since <- now
    else if now -. t.quitting_since > t.quit_timeout then (
      !logger.error "Forced exit from watchdog %S \
                     (trying to quit for %fs)!"
        t.name (now -. t.quitting_since) ;
      exit RamenConsts.ExitCodes.watchdog
    ) ;
    Unix.sleepf ((t.quitting_since +. t.quit_timeout) -. now)
  ) ;
  monitor t

let make ?(grace_period=60.) ?(timeout=30.) ?(quit_timeout=30.)
         name quit_flag =
  let t =
    { name ; monitor = Thread.self () ; quit_flag ; last_reset = 0. ;
      quitting_since = 0. ; enabled = false ; grace_period ; timeout ;
      quit_timeout } in
  t.monitor <- Thread.create monitor t ;
  t

let reset t =
  !logger.debug "Reset watchdog %S" t.name ;
  t.last_reset <- Unix.time () ;
  t.grace_period <- 0.

let disable t =
  !logger.debug "Disabling watchdog %S" t.name ;
  t.enabled <- false

let enable t =
  !logger.debug "Enabling watchdog %S" t.name ;
  t.last_reset <- Unix.time () ;
  t.enabled <- true
