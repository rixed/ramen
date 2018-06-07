open Batteries
open Lwt
open Lwt_unix
open RamenHelpers
open RamenLog

(* RW locks working both internally (mandatory locking) and externally
 * (advisory locking) *)

(* To exclude ourselves we use a normal RamenRWLock per file.
 * Note that the key is the file name as given, not some canonical file
 * name, although it should probably be. *)
let internal_locks = Hashtbl.create 3

let with_lock with_int_lock op fname f =
  let internal_lock =
    try Hashtbl.find internal_locks fname
    with Not_found ->
      !logger.debug "Create new lock for file %S" fname ;
      let l = RamenRWLock.make () in
      Hashtbl.add internal_locks fname l ;
      l in
  (* Of course we lock ourself before locking other processes. *)
  with_int_lock internal_lock (fun () ->
    mkdir_all ~is_file:true fname ;
    let%lwt fd = openfile fname [O_RDWR; O_CREAT] 0o640 in
    (*!logger.debug "Got internal lock" ;*)
    (* Just grab the first "byte", probably simpler than the whole file *)
    let%lwt () = lockf fd op 1 in
    (*!logger.debug "Got lockf on %s" fname ;*)
    finalize f (fun () ->
      let%lwt () = lockf fd F_ULOCK 1 in
      close fd))

let with_r_lock fname = with_lock RamenRWLock.with_r_lock F_RLOCK fname
let with_w_lock fname = with_lock RamenRWLock.with_w_lock F_LOCK fname
