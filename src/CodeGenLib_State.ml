(* Internal state of a function.
 * We had state in some aggregate functions already.
 * Some non-aggregate functions (for instance, smoothing) might need internal
 * state as well. Currently, the state can be only in the aggregate record (as
 * the aggregate value) and the function closure. This has several issues:
 *
 * - The state is lost when the program is restarted;
 * - the state, or some part of it, might be both large and infrequently used
 *   (suggesting it should be kept on disc);
 *
 * This library makes state management a bit more abstract to offer transparent
 * save/restore across runs and offloading on disc. *)
open RamenLog
open RamenHelpers

(* The "state" is a value of some type known only to the function. The module
 * used to save/restore has to be chosen depending on the knowledge the user
 * has about the state size and volatility. *)
open Batteries

let marshal_into_fd fd v =
  try marshal_into_fd fd v
  with e ->
    !logger.error "Cannot marshal_into_fd %d: %s, skipping"
      (int_of_fd fd) (Printexc.to_string e)

let create_or_read fname v =
  let open Unix in
  RamenHelpers.mkdir_all ~is_file:true fname ;
  let init_restore () =
    !logger.info "Will have my state in file %s" fname ;
    let fd = openfile fname [O_RDWR] 0o640 in
    fd, marshal_from_fd fd
  and init_create () =
    let fd = openfile fname [O_RDWR; O_CREAT; O_TRUNC] 0o640 in
    marshal_into_fd fd v ;
    fd, v
  in
  if RamenHelpers.file_exists ~maybe_empty:false ~has_perms:0o400 fname then
    try init_restore () with _ -> init_create ()
  else
    init_create ()

(* Use this if you just want to save the state on disc from time to time, but
 * are fine with keeping it in memory all the time: *)

module Persistent =
struct
  type 'a handle =
    { v : 'a ;
      fd : Unix.file_descr ;
      last_saved : float ;
      calls_since_saved : int }

  (* When the program start we must "register" the state, ie. give it a unique
   * name. Given a single binary will run a single operation and will be given
   * its own temp directory we just need a name that's unique to the operation.
   * Note that the initial value that's given is supposed to be used only for
   * ids that are new regardless of restarts. *)
  let make fname v =
    (* FIXME: if save_every>0 || save_timeout>0., register an at_exit call that
     * will backup the last value. *)
    let fd, v = create_or_read fname v in
    !logger.debug "Will save/restore state from fd %d" (int_of_fd fd) ;
    { v ; fd ;
      last_saved = !CodeGenLib_IO.now ;
      calls_since_saved = 0 }

  (* Retrieve the lastly saved value (or the initial value): *)
  let restore h = h.v

  (* Save the new value.
   * The save_every parameters help control when exactly the value is backed
   * up on disc. By default it should be saved every time.
   * If save_every is 0 then no backup based on number of calls will be performed.
   * Similarly, if save_timeout is 0 no backup based on time.
   *)
  let save ?(save_every=1) ?(save_timeout=0.) h v =
    if
      save_every > 0 && h.calls_since_saved >= save_every ||
      save_timeout > 0. && !CodeGenLib_IO.now >= h.last_saved +. save_timeout
    then (
      !logger.debug "Save state into fd %d (after %d calls, now=%f while \
                     last_saved=%f)"
        (int_of_fd h.fd) h.calls_since_saved !CodeGenLib_IO.now h.last_saved ;
      marshal_into_fd h.fd v ;
      { h with v ; last_saved = !CodeGenLib_IO.now ; calls_since_saved = 1 }
    ) else (
      { h with calls_since_saved = h.calls_since_saved + 1 }
    )
end

(* Similar one that does not keep the value in memory: *)

module File =
struct
  type handle = Unix.file_descr

  let make fname v =
    (* FIXME: if save_every>0 || save_timeout>0., register an at_exit call that
     * will backup the last value. *)
    let fd, _v = create_or_read fname v in fd

  let restore = marshal_from_fd

  let save = marshal_into_fd
end

(* TODO: variant of the above without keeping the file_descr but keeping the
 * file name instead, for when there are too many states (aka id depends on
 * some large group-by key. *)
