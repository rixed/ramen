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

(* The "state" is a value of some type known only to the function. The module
 * used to save/restore has to be chosen depending on the knowledge the user
 * has about the state size and volatility. *)
let int_of_fd fd : int = Obj.magic fd

let do_save fd v =
  let open Unix in
  lseek fd 0 SEEK_SET |> ignore ;
  (* Leak memory for some reason / and do not write anything to the file
   * is we Marshal.to_channel directly. :-/ *)
  let bytes = Marshal.to_bytes v [] in
  let len = Bytes.length bytes in
  write fd bytes 0 len |> ignore

let do_restore fd =
  let open Unix in
  lseek fd 0 SEEK_SET |> ignore ;
  Marshal.from_channel (in_channel_of_descr fd)

open Batteries

let create_or_read persist_dir id v =
  let open Unix in
  let fname = persist_dir ^"/states/"^ id in
  Helpers.mkdir_all ~is_file:true fname ;
  let init_restore () =
    let fd = openfile fname [O_RDWR] 0o640 in
    fd, do_restore fd
  and init_create () =
    let fd = openfile fname [O_RDWR; O_CREAT; O_TRUNC] 0o640 in
    do_save fd v ;
    fd, v
  in
  if Helpers.file_exists ~maybe_empty:false ~has_perms:0o400 fname then
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
  let make persist_dir id v =
    (* FIXME: if save_every>0 || save_timeout>0., register an at_exit call that
     * will backup the last value. *)
    let fd, v = create_or_read persist_dir id v in
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
      do_save h.fd v ;
      { h with v ; last_saved = !CodeGenLib_IO.now ; calls_since_saved = 1 }
    ) else (
      { h with calls_since_saved = h.calls_since_saved + 1 }
    )
end

(* Similar one that does not keep the value in memory: *)

module File =
struct
  type handle = Unix.file_descr

  let make persist_dir id v =
    (* FIXME: if save_every>0 || save_timeout>0., register an at_exit call that
     * will backup the last value. *)
    let fd, _v = create_or_read persist_dir id v in fd

  let restore = do_restore

  let save = do_save
end

(* TODO: variant of the above without keeping the file_descr but keeping the
 * file name instead, for when there are too many states (aka id depends on
 * some large group-by key. *)
