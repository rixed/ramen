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
    (* So that a watchdog can be disabled when we have to block for an
     * undetermined amount of time. Watchdogs start enabled though. *)
    mutable enabled : bool ;
    quit_flag : bool ref ;
    (* Time of last reset: *)
    mutable last_reset : float ;
    (* Set to the time when we have witnessed the quit flag was on for
     * the first time: *)
    mutable quitting_since : float option ;
    (* The duration after which we begin the termination if the watchdog
     * has not been reset: *)
    timeout : float ;
    (* The duration it normally take to terminate after the quit flag is
     * set: *)
    quit_timeout : float }

let make ?(timeout=10.) ?(quit_timeout=5.) name quit_flag =
  { name ; quit_flag ; last_reset = 0. ; quitting_since = None ;
    enabled = true ; timeout ; quit_timeout }

let reset t =
  !logger.debug "Reset watchdog %S" t.name ;
  t.last_reset <- Unix.time ()

let disable t =
  !logger.debug "Disabling watchdog %S" t.name ;
  t.enabled <- false

let enable t =
  !logger.debug "Enabling watchdog %S" t.name ;
  t.enabled <- true

let run t =
  let rec loop () =
    let now = Unix.time () in
    if !(t.quit_flag) then (
      (match t.quitting_since with
      | None -> t.quitting_since <- Some now
      | Some start_quit ->
          if now -. start_quit > t.quit_timeout then (
            !logger.error "Forced exit from watchdog!" ;
            exit 5)) ;
      Lwt_unix.sleep (t.quit_timeout /. 4.)
    ) else (
      if t.enabled && now -. t.last_reset > t.timeout then (
        !logger.error "Forced quit from watchdog!" ;
        t.quit_flag := true) ;
      Lwt_unix.sleep (t.timeout /. 4.)
    ) >>= loop in
  !logger.info "Starting watchdog %S" t.name ;
  reset t ;
  async loop
