open Batteries
open Legacy.Unix
open RamenLog
open RamenHelpers
open RamenAtomic

let with_lock op fname f =
  mkdir_all ~is_file:true fname ;
  let fd = openfile fname [O_RDWR; O_CLOEXEC; O_CREAT] 0o640 in
  finally
    (fun () -> close fd)
    (fun () ->
      lockf fd op 0 ;
      finally
        (fun () ->
          lseek fd 0 SEEK_SET |> ignore ;
          lockf fd F_ULOCK 0)
        f fd) ()

let with_r_lock fname = with_lock F_RLOCK fname
let with_w_lock fname = with_lock F_LOCK fname
