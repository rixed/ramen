(* OutRef files are the files describing where a node should send its
 * output. It's basically a list of ringbuf files, but soon we will have
 * more info in there.
 *
 * We want to lock those files, both internally (same process) and externally
 * (other processes), although we are fine with advisory locking.
 * Unfortunately lockf will only lock other processes out so we have to combine
 * RWLocks and lockf.
 *)
open Batteries
open Lwt
open Helpers
open RamenLog

module Lock =
struct
  open Lwt_unix

  (* For simplicity we use a single RW lock for all out_ref, here it is: *)
  let internal_lock = RWLock.make ()

  let with_lock with_int_lock op fname f =
    (* Of course we lock ourself before locking other processes. *)
    let%lwt fd = openfile fname [O_RDWR; O_CREAT] 0o640 in
    with_int_lock internal_lock (fun () ->
      (* Just grab the first "byte", probably simpler than the whole file *)
      let%lwt () = lockf fd op 1 in
      finalize f (fun () ->
        let%lwt () = lockf fd F_ULOCK 1 in
        close fd))

  let with_r_lock fname = with_lock RWLock.with_r_lock F_RLOCK fname
  let with_w_lock fname = with_lock RWLock.with_w_lock F_LOCK fname
end

type out_spec = string

(* Used by ramen when starting a new worker to initialize (or reset) its
 * output: *)
let set_ fname outs =
  mkdir_all ~is_file:true fname ;
  File.write_lines fname (Set.enum outs)

let set fname outs =
  Lock.with_w_lock fname (fun () ->
    wrap (fun () -> set_ fname outs))

let read_ fname =
  File.lines_of fname |> Set.of_enum

let read fname =
  Lock.with_r_lock fname (fun () ->
    wrap (fun () -> read_ fname))

(* Used by ramen when starting a new worker to add it to its parents outref: *)
let add_ fname out =
  let lines =
    try read_ fname
    with Sys_error _ ->
      set_ fname Set.empty ;
      Set.empty
    in
  if not (Set.mem out lines) then (
    let outs = Set.add out lines in
    set_ fname outs ;
    !logger.info "Adding %s into %s, now outputting to %a"
      out fname (Set.print String.print) outs)

let add fname out =
  Lock.with_w_lock fname (fun () ->
    wrap (fun () -> add_ fname out))

(* Used by ramen when stopping a node to remove its input from its parents
 * out_ref: *)
let remove_ fname out =
  let out_files = read_ fname in
  set_ fname (Set.remove out out_files) ;
  !logger.info "Removed %s from %s, now output only to: %a"
    out fname (Set.print String.print) out_files

let remove fname out =
  Lock.with_w_lock fname (fun () ->
    remove_ fname out ;
    return_unit)

(* Check that fname is listed in outbuf_ref_fname: *)
let mem_ fname out =
  read_ fname |> Set.mem out

let mem fname out =
  Lock.with_r_lock fname (fun () ->
    mem_ fname out |> return)
