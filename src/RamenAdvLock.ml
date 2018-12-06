open Batteries
open Legacy.Unix
open RamenLog
open RamenHelpers
open RamenAtomic

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
            !logger.warning "File %S is already locked, waiting!"
              fname ;
            Mutex.lock mutex (* Wait for that other thread... *)) ;
          mutex
    ) else None in
  finally (fun () -> Option.may Mutex.unlock mutex)
          (fun () ->
    mkdir_all ~is_file:true fname ;
    let fd = openfile fname [O_RDWR; O_CLOEXEC; O_CREAT] 0o640 in
    finally (fun () -> close fd) (fun () ->
      lockf fd op 0 ;
      finally
        (fun () ->
          lseek fd 0 SEEK_SET |> ignore ;
          lockf fd F_ULOCK 0)
        f fd
    ) ()
  ) ()

let with_r_lock fname = with_lock F_RLOCK fname
let with_w_lock fname = with_lock F_LOCK fname
