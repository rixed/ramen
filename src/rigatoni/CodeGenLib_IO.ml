(* Tools for LWT IOs *)
open Lwt
open Log
open Stdint
open Batteries

let tuple_count = ref Uint64.zero

let on_each_input () =
  tuple_count := Uint64.succ !tuple_count

let read_file_lines ?(do_unlink=false) filename k =
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
      match%lwt Lwt_io.read_line chan with
      | exception End_of_file -> return_unit
      | line ->
        let%lwt () = k line in
        on_each_input () ;
        read_next_line ()
    in
    let%lwt () = read_next_line () in
    !logger.info "Done reading %S" filename ;
    return_unit

let check_file_exist kind kind_name path =
  !logger.debug "Checking %S is a %s..." path kind_name ;
  let open Lwt_unix in
  let%lwt stats = stat path in
  if stats.st_kind <> kind then
    fail_with (Printf.sprintf "Path %S is not a %s" path kind_name)
  else return_unit

let check_dir_exist = check_file_exist Lwt_unix.S_DIR "directory"

let read_glob_lines ?do_unlink path k =
  let dirname = Filename.dirname path
  and glob = Filename.basename path in
  let glob = Globs.compile glob in
  let import_file_if_match filename =
    if Globs.matches glob filename then
      read_file_lines ?do_unlink (dirname ^"/"^ filename) k
    else (
      !logger.debug "File %S is not interesting." filename ;
      return_unit
    ) in
  (* inotify will silently do nothing if that path does not exist: *)
  let%lwt () = check_dir_exist dirname in
  let%lwt handler = Lwt_inotify.create () in
  let mask = Inotify.[ S_Create ; S_Onlydir ] in
  let%lwt _ = Lwt_inotify.add_watch handler dirname mask in
  (* Before paying attention to the notifications, scan all files that
   * are already waiting there. There is a race condition but soon we
   * will do both simultaneously *)
  !logger.debug "Import all files in dir %S..." dirname ;
  let stream = Lwt_unix.files_of_directory dirname in
  let%lwt () = Lwt_stream.iter_s import_file_if_match stream in
  !logger.debug "...done. Now import any new file in %S..." dirname ;
  while%lwt true do
    match%lwt Lwt_inotify.read handler with
    | _watch, kinds, _cookie, Some filename
      when List.mem Inotify.Create kinds
        && not (List.mem Inotify.Isdir kinds) ->
      !logger.debug "New file %S in dir %S!" filename dirname ;
      import_file_if_match filename
    | _watch, _kinds, _cookie, filename_opt ->
      !logger.debug "Received a useless inotification for %a"
        (Option.print String.print) filename_opt ;
      return_unit
  done

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
