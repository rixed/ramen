open RamenLog
open Stdint
open Batteries
open Legacy.Unix
open RamenHelpers
module N = RamenName
module Files = RamenFiles

let now = ref (gettimeofday ())
let first_input = ref None
let last_input = ref None

let on_each_input_pre () =
  let t = gettimeofday () in
  now := t ;
  if !first_input = None then first_input := Some t ;
  last_input := Some t

let read_file_lines ?(while_=always) ?(do_unlink=false)
                    (filename : N.path) preprocessor watchdog k =
  let open_file =
    if preprocessor = "" then (
      fun () ->
        let fd = openfile (filename :> string) [ O_RDONLY ] 0o644 in
        fd, (fun () -> close fd)
    ) else (
      fun () ->
        let f = RamenHelpers.shell_quote (filename :> string) in
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
    !logger.error "Cannot open file %a%s: %s, skipping."
      N.path_print filename
      (if preprocessor = "" then ""
       else (Printf.sprintf " through %S" preprocessor))
      (Printexc.to_string e)
  | fd, close_file ->
    !logger.debug "Start reading %a" N.path_print filename ;
    finally close_file
      (fun () ->
        (* If we used a preprocessor we must wait for EOF before
         * unlinking the file. And in case we crash before the end
         * of the file it is safer to skip the file rather than redo
         * the lines that have been read already. *)
        if do_unlink && preprocessor = "" then
          Files.safe_unlink filename ;
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
        !logger.debug "Finished reading %a" N.path_print filename ;
        if do_unlink && preprocessor <> "" then Files.safe_unlink filename ;
        ignore (Gc.major_slice 0)) ()

let check_file_exists kind kind_name path =
  !logger.debug "Checking %a is a %s..." N.path_print path kind_name ;
  match Files.safe_stat path with
  | exception Unix_error (ENOENT, _, _) ->
    Printf.sprintf2 "%a does not exist" N.path_print path |>
    failwith
  | stats ->
    if stats.st_kind <> kind then
      Printf.sprintf2 "Path %a is not a %s" N.path_print path kind_name |>
      failwith

let check_dir_exists = check_file_exists S_DIR "directory"

(* Try hard not to create several instances of the same watchdog: *)
let watchdog = ref None

let read_glob_lines ?while_ ?do_unlink path preprocessor quit_flag k =
  let dirname = Filename.dirname path |> N.path
  and glob = Filename.basename path in
  let glob = Globs.compile glob in
  if !watchdog = None then
    watchdog := Some (RamenWatchdog.make ~timeout:300. "read lines"
                                         quit_flag) ;
  let watchdog = Option.get !watchdog in
  let import_file_if_match (filename : N.path) =
    if Globs.matches glob (filename :> string) then
      try
        read_file_lines ?while_ ?do_unlink (N.path_cat [dirname ; filename ])
                        preprocessor watchdog k
      with exn ->
        !logger.error "Exception while reading file %a: %s\n%s"
          N.path_print filename
          (Printexc.to_string exn)
          (Printexc.get_backtrace ())
    else (
      !logger.debug "File %a is not interesting." N.path_print filename
    ) in
  check_dir_exists dirname ;
  let handler = RamenFileNotify.make ?while_ dirname in
  !logger.debug "Import all files in dir %a..." N.path_print dirname ;
  RamenFileNotify.for_each (fun filename ->
    !logger.debug "New file %a in dir %a!"
      N.path_print filename N.path_print dirname ;
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
