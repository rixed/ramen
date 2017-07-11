(* Tools for LWT IOs *)
open Lwt
open Log
open Stdint

let dying task =
  Lwt.fail_with (Printf.sprintf "Committing suicide while %s\n%!" task)

let always_true () = true

let tuple_count = ref Uint64.zero

let on_each_input () =
  tuple_count := Uint64.succ !tuple_count

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
        | line ->
          let%lwt () = k line in
          on_each_input () ;
          read_next_line ()
      ) else (
        dying (Printf.sprintf "reading %S" filename)
      )
    in
    let%lwt () = read_next_line () in
    !logger.info "Done reading %S" filename ;
    return_unit

let retry ~on ?(first_delay=1.0) ?(min_delay=0.000001) ?(max_delay=10.0) ?(delay_adjust_ok=0.2) ?(delay_adjust_nok=1.1) f =
  let next_delay = ref first_delay in
  let rec loop x =
    (match f x with
    | exception e ->
      if on e then (
        let delay = !next_delay in
        let delay = min delay max_delay in
        let delay = max delay min_delay in
        next_delay := !next_delay *. delay_adjust_nok ;
        !logger.debug "Retryable error: %s, pausing %gs"
          (Printexc.to_string e) delay ;
        let%lwt () = Lwt_unix.sleep delay in
        loop x
      ) else (
        !logger.error "Non-retryable error: %s"
          (Printexc.to_string e) ;
        fail e
      )
    | r ->
      next_delay := !next_delay *. delay_adjust_ok ;
      return r)
  in
  loop

let retry_for_ringbuf ~on f = retry ~on ~first_delay:0.001 ~max_delay:0.01 f

let read_ringbuf rb f =
  let open RingBuf in
  let on = function
    | Failure _ ->
      !logger.debug "Nothing left to read in the ring buffer, sleeping..." ;
      true
    | _ ->
      false
  in
  let rec read_next () =
    let%lwt tx = retry_for_ringbuf ~on dequeue_alloc rb in
    let%lwt () = f tx in
    on_each_input () ;
    read_next ()
  in
  read_next ()
