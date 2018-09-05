(* Watchdog: exit the process if not reset regularly.
 *
 * Used by all daemon and workers.
 *
 * The process is terminated first with the quit flag, then by calling
 * exit. *)
open Lwt
open RamenLog

type t =
  { name : string ;
    (* A flag to avoid having several threads monitoring the same watchdog
     * in case it is started several times: *)
    mutable is_running : bool ;
    (* So that a watchdog can be disabled when we have to block for an
     * undetermined amount of time. Watchdogs start enabled though. *)
    mutable enabled : bool ;
    quit_flag : int option ref ;
    (* Time of last reset: *)
    mutable last_reset : float ;
    (* Set to the time when we have witnessed the quit flag was on for
     * the first time: *)
    mutable quitting_since : float option ;
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

let make ?(grace_period=60.) ?(timeout=30.) ?(quit_timeout=30.)
         name quit_flag =
  { name ; is_running = false ;
    quit_flag ; last_reset = 0. ; quitting_since = None ;
    enabled = true ; grace_period ; timeout ; quit_timeout }

let reset t =
  !logger.debug "Reset watchdog %S" t.name ;
  t.last_reset <- Unix.gettimeofday () ;
  t.grace_period <- 0.

let disable t =
  !logger.debug "Disabling watchdog %S" t.name ;
  t.enabled <- false

let enable t =
  !logger.debug "Enabling watchdog %S" t.name ;
  t.last_reset <- Unix.gettimeofday () ;
  t.enabled <- true

let run t =
  let rec loop () =
    let now = Unix.time () in
    (if !(t.quit_flag) <> None then (
      (match t.quitting_since with
      | None -> t.quitting_since <- Some now
      | Some start_quit ->
          if now -. start_quit > t.quit_timeout then (
            !logger.error "Forced exit from watchdog %S \
                           (trying to quit for %fs)!"
              t.name (now -. start_quit) ;
            exit RamenConsts.ExitCodes.watchdog)) ;
      Lwt_unix.sleep (t.quit_timeout /. 4.)
    ) else (
      if t.enabled && now -. t.last_reset > t.timeout +. t.grace_period then (
        !logger.error "Forced quit from watchdog %S \
                       (last reset was %fs ago)!"
          t.name (now -. t.last_reset) ;
        t.quit_flag := Some RamenConsts.ExitCodes.watchdog) ;
      Lwt_unix.sleep (t.timeout /. 4.)
    )) >>= loop in
  if t.is_running then
    !logger.warning "Ignoring request to run %S again" t.name
  else (
    !logger.debug "Starting watchdog %S" t.name ;
    t.is_running <- true ;
    t.last_reset <- Unix.gettimeofday () ;
    async loop)
