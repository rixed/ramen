open Batteries
open Lwt
open Lwt_unix
open RamenHelpers
open RamenLog

let mutex = Lwt_mutex.create ()

let with_lock op fname f =
  mkdir_all ~is_file:true fname ;
  (* File must have been created with initial content of at least 1 byte
   * beforehand: *)
  let%lwt fd = openfile fname [O_RDWR; O_CLOEXEC] 0o640 in
  (* Of course we lock ourself before locking other processes. *)
  Lwt_mutex.with_lock mutex (fun () ->
    (* Just grab the first "byte", probably simpler than the whole file *)
    lockf fd op 1) ;%lwt
  finalize f (fun () ->
    lockf fd F_ULOCK 1 ;%lwt
    close fd)

let with_r_lock fname = with_lock F_RLOCK fname
let with_w_lock fname = with_lock F_LOCK fname
