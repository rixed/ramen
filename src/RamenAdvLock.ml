open Batteries
open Legacy.Unix
open RamenLog
open RamenHelpers
open RamenAtomic
module N = RamenName
module Files = RamenFiles

let write_locked = Hashtbl.create 11

let with_lock op fname f =
  let mutex =
    if op = F_LOCK then Some (
      match Hashtbl.find write_locked fname with
      | exception Not_found ->
          let mutex = Mutex.create () in
          Mutex.lock mutex ;
          Hashtbl.add write_locked fname mutex ;
          mutex
      | mutex ->
          if not (Mutex.try_lock mutex) then (
            !logger.warning "File %a is already locked, waiting!"
              N.path_print fname ;
            Mutex.lock mutex (* Wait for that other thread... *)) ;
          mutex
    ) else None in
  finally (fun () -> Option.may Mutex.unlock mutex)
          (fun () ->
    Files.mkdir_all ~is_file:true fname ;
    let flags =
      O_CLOEXEC :: (
        if op = F_LOCK then
          [ O_RDWR; O_CREAT ] else [ O_RDONLY ]
      ) in
    let fd = openfile (fname :> string) flags 0o640 in
    finally
      (fun () -> Files.safe_close fd)
      (fun () ->
        Unix.restart_on_EINTR (lockf fd op) 0 ;
        finally
          (fun () ->
            Unix.restart_on_EINTR (lseek fd 0) SEEK_SET |> ignore ;
            Unix.restart_on_EINTR (lockf fd F_ULOCK) 0)
          f fd
      ) ()
  ) ()

let with_r_lock fname = with_lock F_RLOCK fname
let with_w_lock fname = with_lock F_LOCK fname
