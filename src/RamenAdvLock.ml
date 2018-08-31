open Batteries
open Lwt
open Lwt_unix
open RamenHelpers
open RamenLog

(* RW locks working both internally (mandatory locking) and externally
 * (advisory locking) *)

(* To exclude ourselves we use a normal RamenRWLock per file.
 * Note that the key is the file name. We do simplify the path but if
 * for any reason it's possible to access the same lockfile with two
 * different paths then the lockfile will be freed several times.
 * Along the lock, this hash also store the kind of file lock we already
 * hold on that file, and a count, so that we make this lockf recursive.*)
type lock_entry =
  { rw_lock : RamenRWLock.t ;
    (* Protects the following of concurrent readers: *)
    count_mutex : Lwt_mutex.t ;
    mutable ext_lock : (Lwt_unix.file_descr * Unix.lock_command) option ;
    mutable ext_lock_count : int }

let locks = Hashtbl.create 3

let with_lock with_int_lock op fname f =
  let fname = simplified_path fname in
  let lock =
    try Hashtbl.find locks fname
    with Not_found ->
      !logger.debug "Create new lock for file %S" fname ;
      let lock =
        { rw_lock = RamenRWLock.make () ;
          ext_lock = None ;
          count_mutex = Lwt_mutex.create () ;
          ext_lock_count = 0 } in
      Hashtbl.add locks fname lock ;
      lock in
  (* Of course we lock ourself before locking other processes. *)
  with_int_lock lock.rw_lock (fun () ->
    (* If we already have this lock on this file, just increment the
     * counter: *)
    (match lock.ext_lock with
    | Some (_fd, op') ->
        Lwt_mutex.with_lock lock.count_mutex (fun () ->
          (* We cannot arrive here wanting a R lock if another thread already
           * held a W lock, nor the other way around: *)
          assert (op = op') ;
          lock.ext_lock_count <- lock.ext_lock_count + 1 ;
          return_unit) ;
    | None ->
        Lwt_mutex.with_lock lock.count_mutex (fun () ->
          lock.ext_lock_count <- 1 ;
          mkdir_all ~is_file:true fname ;
          let%lwt fd = openfile fname [O_RDWR; O_CREAT] 0o640 in
          lock.ext_lock <- Some (fd, op) ;
          (* Just grab the first "byte", probably simpler than the whole
           * file *)
          lockf fd op 1)) ;%lwt
        (*!logger.debug "Got internal lock" ;*)
    (*!logger.debug "Got lockf on %s" fname ;*)
    finalize f (fun () ->
      Lwt_mutex.with_lock lock.count_mutex (fun () ->
        lock.ext_lock_count <- lock.ext_lock_count - 1 ;
        if lock.ext_lock_count = 0 then (
          let fd, _op = Option.get lock.ext_lock in
          lock.ext_lock <- None ;
          lockf fd F_ULOCK 1 ;%lwt
          close fd
        ) else return_unit)))

let with_r_lock fname = with_lock RamenRWLock.with_r_lock F_RLOCK fname
let with_w_lock fname = with_lock RamenRWLock.with_w_lock F_LOCK fname
