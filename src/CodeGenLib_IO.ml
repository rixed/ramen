(* Tools for LWT IOs *)
open Lwt
open RamenLog
open Stdint
open Batteries

let tuple_count = ref Uint64.zero
let now = ref 0.

let on_each_input_pre () =
  now := Unix.gettimeofday ();
  tuple_count := Uint64.succ !tuple_count

let read_file_lines ?(while_=(fun () -> true)) ?(do_unlink=false)
                    filename preprocessor k =
  let open_file =
    if preprocessor = "" then (
      fun () ->
        let%lwt fd = Lwt_unix.(openfile filename [ O_RDONLY ] 0x644) in
        return Lwt_io.(of_fd ~mode:Input fd)
    ) else (
      fun () ->
        let f = RamenHelpers.shell_quote filename in
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
       * unlinking the file. And in case we crash before the end
       * of the file it is safer to skip the file rather than redo
       * the lines that have been read already. *)
      if do_unlink && preprocessor = "" then
        Lwt_unix.unlink filename else return_unit in
    let rec read_next_line () =
      let on_eof () =
        !logger.debug "Finished reading %S" filename ;
        let%lwt () = Lwt_io.close chan in
        if do_unlink && preprocessor <> "" then
          Lwt_unix.unlink filename else return_unit in
      if while_ () then (
        match%lwt Lwt_io.read_line chan with
        | exception End_of_file -> on_eof ()
        | line ->
          on_each_input_pre () ;
          let%lwt () = k line in
          read_next_line ()
      ) else on_eof ()
    in
    read_next_line ()

let check_file_exists kind kind_name path =
  !logger.debug "Checking %S is a %s..." path kind_name ;
  let open Lwt_unix in
  match%lwt stat path with
  | exception Unix.Unix_error (Unix.ENOENT, _, _) ->
    fail_with (path ^" does not exist")
  | stats ->
    if stats.st_kind <> kind then
      fail_with (Printf.sprintf "Path %S is not a %s" path kind_name)
    else return_unit

let check_dir_exists = check_file_exists Lwt_unix.S_DIR "directory"

let read_glob_lines ?while_ ?do_unlink path preprocessor k =
  let dirname = Filename.dirname path
  and glob = Filename.basename path in
  let glob = Globs.compile glob in
  let import_file_if_match filename =
    if Globs.matches glob filename then
      try%lwt
        read_file_lines ?while_ ?do_unlink (dirname ^"/"^ filename) preprocessor k
      with exn ->
        !logger.error "Exception while reading file %s: %s\n%s"
          filename
          (Printexc.to_string exn)
          (Printexc.get_backtrace ()) ;
        return_unit
    else (
      !logger.debug "File %S is not interesting." filename ;
      return_unit
    ) in
  let%lwt () = check_dir_exists dirname in
  let%lwt handler = RamenFileNotify.make ?while_ dirname in
  !logger.debug "Import all files in dir %S..." dirname ;
  RamenFileNotify.for_each (fun filename ->
    !logger.debug "New file %S in dir %S!" filename dirname ;
    import_file_if_match filename) handler

let url_encode =
  let char_encode c =
    let c = Char.code c in
    Printf.sprintf "%%%X%X" (c lsr 4) (c land 0xf) in
  let reserved_chars = "!*'();:@&=+$,/?#[]" in
  let is_in_set set c =
    try ignore (String.index set c); true with Not_found -> false in
  let is_reserved = is_in_set reserved_chars in
  fun s ->
    let rep c =
      (if is_reserved c then char_encode else String.of_char) c in
    String.replace_chars rep s
