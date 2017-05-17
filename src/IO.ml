(* Given a command line that sets a pattern for files to read, starts to
 * inotify those directories and for each file pass its name to each reader
 * plugins. Once one accept it then use it.  file reader plugins register one
 * function: val accept_filename: string -> (unit -> 'a option) option where 'a
 * is the event. The idea of course is that we want to be able to no depend on
 * PV CSV format.  Alternatively, when we register a reader we also configure
 * this reader with a file pattern. *)
open Batteries
open Lwt

(*
type watcher =
  { pattern : Globs.pattern ;
    reader : string -> unit Lwt.t }

type watch =
  { handler : Lwt_inotify.t;
    mutable watchers : watcher list }

(* Monitor the directories and call readers on all matching new files: *)
let rec notify_loop watch () =
  match%lwt Lwt_inotify.read watch.handler with
  | _watch, kinds, _cookie, Some filename
      when List.mem Inotify.Create kinds &&
           not (List.mem Inotify.Isdir kinds) ->
    (try List.find_map (fun watcher ->
            if Globs.matches watcher.pattern filename then
              Some (watcher.reader filename)
            else None) watch.watchers |>
        async
    with Not_found ->
      Printf.eprintf "New file %s is not interesting.\n%!" filename) ;
    notify_loop watch ()


(* File reader registration *)

let watches = Hashtbl.create 7

let add_watch_for_path path pattern reader =
  let watcher = { pattern ; reader } in
  match Hashtbl.find watches path with
  | exception Not_found ->
    let%lwt handler = Lwt_inotify.create () in
    let mask = Inotify.[ S_Create ; S_Onlydir ] in
    let%lwt _ = Lwt_inotify.add_watch handler path mask in
    let watch = { handler ; watchers = [ watcher ] } in
    Hashtbl.add watches path watch ;
    Lwt.asyn (notify_loop watch)
  | watch ->
    watch.watchers <- watcher :: watch.watchers ;
    return_unit

let register reader pattern =
  match Globs.compile pattern with
  | { anchored_start = true ;
      anchored_end = true ;
      chunks = [ f ] } ->
    (* Single file, go for it: *)
    with_input_file f reader ()
  | { anchored_start = true ;
      chunks = path :: rest ; _ } as pat ->
    let pattern = { pat with anchored_start = false ;
                             chunks = rest } in
    add_watch_for_path path pattern reader
*)

let all_threads = ref []

let register_file_reader filename ppp k =
  let reading_thread =
    let%lwt fd = Lwt_unix.(openfile filename [ O_RDONLY ] 0x644) in
    (* TODO: Optionally delete the filename here *)
    let chan = Lwt_io.(of_fd ~mode:input fd) in
    let stream = Lwt_io.read_lines chan in
    let%lwt () = Lwt_stream.iter_s (fun line ->
      (* FIXME: wouldn't it be nice if PPP_CSV.tuple was not depending on this "\n"? *)
      match PPP.of_string ppp (line ^"\n") 0 with
      | exception e ->
        Printf.eprintf "Exception %s!\n%!" (Printexc.to_string e) ;
        return_unit
      | Some (e, l) when l = String.length line + 1 ->
          k e
      | _ ->
          Printf.eprintf "Cannot parse line %S\n%!" line ;
          return_unit) stream in
    Printf.printf "done reading %S\n%!" filename ;
    exit 0 ;
    return_unit in
  all_threads := reading_thread :: !all_threads

let start th =
  Printf.printf "Start %d threads...\n%!" (List.length !all_threads) ;
  Lwt_main.run (Lwt.join (th :: !all_threads)) ;
  Printf.printf "... Done execution.\n%!"
