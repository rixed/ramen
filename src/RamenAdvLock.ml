open Batteries
open Legacy.Unix
open RamenLog
open RamenHelpers
open RamenAtomic

(* TODO: some kind of error when we recurse would be great *)

let write_locked = ref BatSet.String.empty

let with_lock op fname f =
  assert (not (BatSet.String.mem fname !write_locked)) ;
  if op = F_LOCK then
    write_locked := BatSet.String.add fname !write_locked ;
  mkdir_all ~is_file:true fname ;
  let fd = openfile fname [O_RDWR; O_CLOEXEC; O_CREAT] 0o640 in
  finally
    (fun () ->
      if op = F_LOCK then
        write_locked := BatSet.String.remove fname !write_locked ;
      close fd)
    (fun () ->
      lockf fd op 0 ;
      finally
        (fun () ->
          lseek fd 0 SEEK_SET |> ignore ;
          lockf fd F_ULOCK 0)
        f fd) ()

let with_r_lock fname = with_lock F_RLOCK fname
let with_w_lock fname = with_lock F_LOCK fname
