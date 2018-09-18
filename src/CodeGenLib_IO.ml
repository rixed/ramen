open RamenLog
open Stdint
open Batteries
open Legacy.Unix
open RamenHelpers

let now = ref (gettimeofday ())

let on_each_input_pre () =
  now := gettimeofday ()

let read_file_lines ?(while_=(fun () -> true)) ?(do_unlink=false)
                    filename preprocessor watchdog k =
  let open_file =
    if preprocessor = "" then (
      fun () ->
        let fd = openfile filename [ O_RDONLY ] 0o644 in
        fd, (fun () -> close fd)
    ) else (
      fun () ->
        let f = RamenHelpers.shell_quote filename in
        let cmd =
          if String.exists preprocessor "%s" then
            String.nreplace preprocessor "%s" f
          else
            preprocessor ^" "^ f
        in
        (* We need to keep the chan for later destruction, as
         * in_channel_of_descr would create another one *)
        let chan = open_process_in cmd in
        descr_of_in_channel chan,
        fun () ->
          match close_process_in chan with
          | Unix.WEXITED 0 -> ()
          | s ->
              !logger.warning "CSV preprocessor %S %s"
                preprocessor (string_of_process_status s)
    ) in
  match open_file () with
  | exception e ->
    !logger.error "Cannot open file %S%s: %s, skipping."
      filename
      (if preprocessor = "" then ""
       else (Printf.sprintf " through %S" preprocessor))
      (Printexc.to_string e)
  | fd, close_file ->
    !logger.debug "Start reading %S" filename ;
    finally close_file
      (fun () ->
        (* If we used a preprocessor we must wait for EOF before
         * unlinking the file. And in case we crash before the end
         * of the file it is safer to skip the file rather than redo
         * the lines that have been read already. *)
        if do_unlink && preprocessor = "" then
          unlink filename ;
        RamenWatchdog.enable watchdog ;
        let lines = read_lines fd in
        (try
          Enum.iter (fun line ->
            if not (while_ ()) then raise Exit ;
            on_each_input_pre () ;
            k line ;
            RamenWatchdog.reset watchdog
          ) lines
        with Exit -> ()) ;
        RamenWatchdog.disable watchdog ;
        !logger.debug "Finished reading %S" filename ;
        if do_unlink && preprocessor <> "" then
          unlink filename) ()

let check_file_exists kind kind_name path =
  !logger.debug "Checking %S is a %s..." path kind_name ;
  match stat path with
  | exception Unix_error (ENOENT, _, _) ->
    failwith (path ^" does not exist")
  | stats ->
    if stats.st_kind <> kind then
      failwith (Printf.sprintf "Path %S is not a %s" path kind_name)

let check_dir_exists = check_file_exists S_DIR "directory"

(* Try hard not to create several instances of the same watchdog: *)
let watchdog = ref None

let read_glob_lines ?while_ ?do_unlink path preprocessor quit_flag k =
  let dirname = Filename.dirname path
  and glob = Filename.basename path in
  let glob = Globs.compile glob in
  if !watchdog = None then
    watchdog := Some (RamenWatchdog.make ~timeout:300. "read lines"
                                         quit_flag) ;
  let watchdog = Option.get !watchdog in
  RamenWatchdog.enable watchdog ;
  let import_file_if_match filename =
    if Globs.matches glob filename then
      try
        read_file_lines ?while_ ?do_unlink (dirname ^"/"^ filename)
                        preprocessor watchdog k
      with exn ->
        !logger.error "Exception while reading file %s: %s\n%s"
          filename
          (Printexc.to_string exn)
          (Printexc.get_backtrace ())
    else (
      !logger.debug "File %S is not interesting." filename
    ) in
  check_dir_exists dirname ;
  let handler = RamenFileNotify.make ?while_ dirname in
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
