(* Tools for LWT IOs *)
open Lwt
open Log

let dying task =
  Lwt.fail_with (Printf.sprintf "Committing suicide while %s\n%!" task)

let always_true () = true

let read_file_lines ?(do_unlink=false) ?(alive=always_true) filename k =
  match%lwt Lwt_unix.(openfile filename [ O_RDONLY ] 0x644) with
  | exception e ->
    !logger.error "Cannot open file %S: %s, skipping."
      filename (Printexc.to_string e) ;
    return_unit
  | fd ->
    let%lwt () =
      if do_unlink then Lwt_unix.unlink filename else return_unit in
    let chan = Lwt_io.(of_fd ~mode:input fd) in
    let rec read_next_line () =
      if alive () then (
        match%lwt Lwt_io.read_line chan with
        | exception End_of_file -> return_unit
        | line -> k line >>= read_next_line
      ) else (
        dying (Printf.sprintf "reading %S" filename)
      )
    in
    let%lwt () = read_next_line () in
    !logger.info "done reading %S" filename ;
    return_unit

let read_ringbuf rb f =
  let open RingBuf in
  let rec read_next () =
    match dequeue_alloc rb with
    | exception Failure _ -> Lwt_unix.sleep 1. >>= read_next
    | tx -> f tx >>= read_next
  in
  read_next ()
