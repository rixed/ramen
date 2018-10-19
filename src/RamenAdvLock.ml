open Batteries
open Legacy.Unix
open RamenLog
open RamenHelpers
open RamenAtomic

(* Hash of fnames we have locked already (write for true, read only if false.
 * Note: we take fname as given, assuming the caller will always access the same
 * file using the same fname. *)
let locks = Hashtbl.create 7
(* Protects the above: *)
let locks_mutex = Mutex.create ()

let with_lock op fname f =
  let want_write = op = F_LOCK in
  let need_flock =
    with_mutex locks_mutex (fun () ->
      let lst = Hashtbl.find_default locks fname [] in
      Hashtbl.add locks fname (want_write :: lst) ;
      (* Do we already have a lock on that file that's good enough? *)
      lst = [] || (want_write && List.for_all not lst)) ()
  in
  finally
    (fun () ->
      (* Remove that lock from the hash: *)
      with_mutex locks_mutex (fun () ->
        let rec loop prevs = function
        | [] ->
            (* We must be able to find what we are looking for! *)
            assert false
        | have_write::rest when have_write = want_write ->
            Hashtbl.replace locks fname (List.rev_append prevs rest)
        | x :: rest ->
            loop (x :: prevs) rest
        in
        loop [] (Hashtbl.find_default locks fname [])) ())
    (fun () ->
      let fd =
        if need_flock then (
          mkdir_all ~is_file:true fname ;
          Some (openfile fname [O_RDWR; O_CLOEXEC] 0o640)
        ) else None
      in
      finally
        (fun () -> Option.may close fd)
        (fun () ->
          Option.may (fun fd -> lockf fd op 0) fd ;
          finally
            (fun () ->
              Option.may (fun fd ->
                lseek fd 0 SEEK_SET |> ignore ;
                lockf fd F_ULOCK 0
              ) fd)
            f ()) ()) ()

let with_r_lock fname = with_lock F_RLOCK fname
let with_w_lock fname = with_lock F_LOCK fname
