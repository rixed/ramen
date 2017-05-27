(* Given a command line that sets a pattern for files to read, starts to
 * inotify those directories and for each file pass its name to each reader
 * plugins. Once one accept it then use it.  file reader plugins register one
 * function: val accept_filename: string -> (unit -> 'a option) option where 'a
 * is the event. The idea of course is that we want to be able to no depend on
 * PV CSV format.  Alternatively, when we register a reader we also configure
 * this reader with a file pattern. *)
open Batteries
open Lwt

let debug = ref false

let () = Printexc.register_printer (function
  | Failure x -> Some x
  | End_of_file -> Some "End of file"
  | _ -> None)

let dying task =
  Printf.eprintf "Committing suicide while %s\n%!" task ;
  Lwt.fail_with (Printf.sprintf "Committing suicide while %s\n%!" task)

let import_file ?(do_unlink=false) ~alive filename ppp k =
  Printf.eprintf "Importing file %S...\n%!" filename ;
  match%lwt Lwt_unix.(openfile filename [ O_RDONLY ] 0x644) with
  | exception e ->
    Printf.eprintf "Cannot open file %S: %s, skipping.\n%!"
      filename (Printexc.to_string e) ;
    return_unit
  | fd ->
    let%lwt () =
      if do_unlink then Lwt_unix.unlink filename else return_unit in
    let chan = Lwt_io.(of_fd ~mode:input fd) in
    (* FIXME: since we force a line per line format, this is really just
     * for CSV *)
    let rec read_next_line () =
      if alive () then (
        match%lwt Lwt_io.read_line chan with
        | exception End_of_file -> return_unit
        | line ->
          (* FIXME: wouldn't it be nice if PPP_CSV.tuple was not depending on this "\n"? *)
          (match PPP.of_string ppp (line ^"\n") 0 with
          | exception e ->
            Printf.eprintf "Exception %s!\n%!" (Printexc.to_string e) ;
            read_next_line ()
          | Some (e, l) when l = String.length line + 1 ->
            k e >>=
            read_next_line
          | _ ->
            Printf.eprintf "Cannot parse line %S\n%!" line ;
            read_next_line ())
      ) else dying (Printf.sprintf "reading %S" filename)
    in
    let%lwt () = read_next_line () in
    Printf.printf "done reading %S\n%!" filename ;
    return_unit

let nb_imported_files = ref 0

let register_file_reader ~alive filename ppp k =
  let reading_thread () =
    incr nb_imported_files ;
    let%lwt () = import_file ~alive filename ppp k in
    decr nb_imported_files ;
    if !nb_imported_files <= 0 then (
      if !debug then Printf.eprintf "No more file to read, exiting.\n%!" ;
      Alarm.quit := true
    ) ;
    return_unit
  in
  (* When we use this option we want to exit once all files are imported *)
  async reading_thread

let check_file_exist kind kind_name path =
  Printf.eprintf "Checking %S is a %s...\n%!" path kind_name ;
  let open Lwt_unix in
  let%lwt stats = stat path in
  if stats.st_kind <> kind then
    fail_with (Printf.sprintf "Path %S is not a %s" path kind_name)
  else return_unit

let check_dir_exist = check_file_exist Lwt_unix.S_DIR "directory"

let register_dir_reader ~alive path glob ppp k =
  let glob = Globs.compile glob in
  let import_file_if_match filename =
    if Globs.matches glob filename then
      (* FIXME: we probably want to read it asynchronously (async)
       * to keep processing notifications before this file is processed,
       * but for now let's not do too many things simultaneously. *)
      import_file ~alive ~do_unlink:true (path ^"/"^ filename) ppp k
    else (
      Printf.eprintf "File %S is not interesting.\n%!" filename ;
      return_unit
    ) in
  let reading_thread () =
    (* inotify will silently do nothing if that path does not exist: *)
    let%lwt () = check_dir_exist path in
    let%lwt handler = Lwt_inotify.create () in
    let mask = Inotify.[ S_Create ; S_Onlydir ] in
    let%lwt _ = Lwt_inotify.add_watch handler path mask in
    (* Before paying attention to the notifications, scan all files that
     * are already waiting there. There is a race condition but soon we
     * will to both simultaneously *)
    Printf.eprintf "Import all files in dir %S...\n%!" path ;
    let stream = Lwt_unix.files_of_directory path in
    let%lwt () = Lwt_stream.iter_s import_file_if_match stream in
    Printf.eprintf "...done. Now import any new file in %S...\n%!" path ;
    let rec notify_loop () =
      if alive () then
        match%lwt Lwt_inotify.read handler with
        | _watch, kinds, _cookie, Some filename
          when List.mem Inotify.Create kinds
            && not (List.mem Inotify.Isdir kinds) ->
          Printf.eprintf "New file %S in dir %S!\n%!" filename path ;
          import_file_if_match filename >>=
          notify_loop
        | _watch, _kinds, _cookie, filename_opt ->
          Printf.eprintf "Received a useless inotification for %a\n%!"
            (Option.print String.print) filename_opt ;
          notify_loop ()
      else dying (Printf.sprintf "monitoring %S" path)
    in
    notify_loop() in
  async reading_thread

let start debug_ =
  debug := debug_ ;
  (* Make thread failure more verbose: *)
  let print_exn exn =
    Printf.eprintf "Thread died: %s\n%!" (Printexc.to_string exn) in
  let nagger th =
    catch th (fun exn -> print_exn exn ; return_unit) in
  async_exception_hook := print_exn ;
  Lwt_main.run (nagger Alarm.main_loop) ;
  if !debug then Printf.printf "... Done execution.\n%!"
