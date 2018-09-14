open Batteries
open Legacy.Unix
open RamenHelpers
open RamenLog

let with_lock op fname f =
  mkdir_all ~is_file:true fname ;
  (* File must have been created with initial content of at least 1 byte
   * beforehand: *)
  let fd = openfile fname [O_RDWR; O_CLOEXEC] 0o640 in
  (* Just grab the first "byte", probably simpler than the whole file *)
  lockf fd op 1 ;
  finally
    (fun () -> close fd)
    (fun () ->
      let res = f () in
      lockf fd F_ULOCK 1 ;
      res) ()

let with_r_lock fname = with_lock F_RLOCK fname
let with_w_lock fname = with_lock F_LOCK fname
