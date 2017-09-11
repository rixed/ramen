(* Tools for LWT IOs *)
open Lwt
open RamenLog
open Stdint
open Batteries

let tuple_count = ref Uint64.zero
let now = ref 0.

let on_each_input_pre () =
  now := Unix.gettimeofday ()
let on_each_input_post () =
  tuple_count := Uint64.succ !tuple_count

let read_file_lines ?(do_unlink=false) filename preprocessor k =
  let open_file =
    if preprocessor = "" then (
      fun () ->
        let%lwt fd = Lwt_unix.(openfile filename [ O_RDONLY ] 0x644) in
        return Lwt_io.(of_fd ~mode:Input fd)
    ) else (
      fun () ->
        let f = Helpers.shell_quote filename in
        let s =
          if String.exists preprocessor "%s" then
            String.nreplace preprocessor "%s" f
          else
            preprocessor ^" "^ f
        in
        let cmd = Lwt_process.shell s in
        return (Lwt_process.open_process_in cmd)#stdout
    ) in
  match%lwt open_file () with
  | exception e ->
    !logger.error "Cannot open file %S%s: %s, skipping."
      filename
      (if preprocessor = "" then ""
       else (Printf.sprintf " through %S" preprocessor))
      (Printexc.to_string e) ;
    return_unit
  | chan ->
    !logger.debug "Start reading %S" filename ;
    let%lwt () =
      (* If we used a preprocessor we must wait for EOF before
       * unlinking the file. *)
      if do_unlink && preprocessor = "" then
        Lwt_unix.unlink filename else return_unit in
    let rec read_next_line () =
      match%lwt Lwt_io.read_line chan with
      | exception End_of_file ->
        let%lwt () = Lwt_io.close chan in
        if do_unlink && preprocessor <> "" then
          Lwt_unix.unlink filename else return_unit
      | line ->
        on_each_input_pre () ;
        let%lwt () = k line in
        on_each_input_post () ;
        read_next_line ()
    in
    read_next_line ()

let check_file_exist kind kind_name path =
  !logger.debug "Checking %S is a %s..." path kind_name ;
  let open Lwt_unix in
  let%lwt stats = stat path in
  if stats.st_kind <> kind then
    fail_with (Printf.sprintf "Path %S is not a %s" path kind_name)
  else return_unit

let check_dir_exist = check_file_exist Lwt_unix.S_DIR "directory"

let read_glob_lines ?do_unlink path preprocessor k =
  let dirname = Filename.dirname path
  and glob = Filename.basename path in
  let glob = Globs.compile glob in
  let import_file_if_match filename =
    if Globs.matches glob filename then
      read_file_lines ?do_unlink (dirname ^"/"^ filename) preprocessor k
    else (
      !logger.debug "File %S is not interesting." filename ;
      return_unit
    ) in
  (* inotify will silently do nothing if that path does not exist: *)
  let%lwt () = check_dir_exist dirname in
  let%lwt handler = Lwt_inotify.create () in
  let mask = Inotify.[ S_Create ; S_Moved_to ; S_Onlydir ] in
  let%lwt _ = Lwt_inotify.add_watch handler dirname mask in
  (* Before paying attention to the notifications, scan all files that
   * are already waiting there. There is a race condition but soon we
   * will do both simultaneously *)
  !logger.debug "Import all files in dir %S..." dirname ;
  let%lwt files = Lwt_unix.files_of_directory dirname |>
                  Lwt_stream.to_list in
  let%lwt () = List.fast_sort String.compare files |>
               Lwt_list.iter_s import_file_if_match in
  !logger.debug "...done. Now import any new file in %S..." dirname ;
  while%lwt true do
    match%lwt Lwt_inotify.read handler with
    | _watch, kinds, _cookie, Some filename
      when (List.mem Inotify.Create kinds ||
            List.mem Inotify.Moved_to kinds) &&
           not (List.mem Inotify.Isdir kinds) ->
      !logger.debug "New file %S in dir %S!" filename dirname ;
      import_file_if_match filename
    | ev ->
      !logger.debug "Received a useless inotification: %s"
        (Inotify.string_of_event ev) ;
      return_unit
  done

let read_ringbuf rb f =
  let open RingBuf in
  let rec read_next () =
    on_each_input_pre () ;
    let%lwt tx = RingBufLib.retry_for_ringbuf dequeue_alloc rb in
    let%lwt () = f tx in
    on_each_input_post () ;
    read_next ()
  in
  read_next ()

let http_notify url =
  (* TODO: time this and add a stat *)
  !logger.debug "Send HTTP notification to %S" url ;
  let open Cohttp in
  let open Cohttp_lwt_unix in
  let headers = Header.init_with "Connection" "close" in
  let%lwt resp, _body = Client.get ~headers (Uri.of_string url) in
  let code = resp |> Response.status |> Code.code_of_status in
  if code <> 200 then
    !logger.error "Received code %d from %S" code url ;
  return_unit
