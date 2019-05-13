open Batteries
open RamenLog
open RamenHelpers
module N = RamenName
module Files = RamenFiles

type dir_notifier =
  { mutable already_present : N.path list ;
    dirname : N.path ;
    handler : Unix.file_descr ;
    while_ : unit -> bool }

let make ?(while_=always) dirname =
  Files.mkdir_all ~is_file:false dirname ;
  let handler = Inotify.create () in
  let mask = Inotify.[ S_Create ; S_Moved_to ; S_Onlydir ] in
  let _ = Inotify.add_watch handler (dirname :> string) mask in
  let already_present =
    (Sys.files_of (dirname :> string) /@ N.path) |>
    List.of_enum in
  let already_present = List.fast_sort N.compare already_present in
  !logger.info "%d files already present when starting inotifier"
    (List.length already_present) ;
  { already_present ; dirname ; handler ; while_ }

(* Call f on each new file in the directory.
 * At start, call f on each file already present. *)
let for_each f n =
  List.iter (fun fname ->
    if n.while_ () then f fname
  ) n.already_present ;
  let rec loop () =
    if n.while_ () then (
      match restart_on_eintr ~while_:n.while_ Inotify.read n.handler with
      | exception exn ->
        !logger.error "Cannot Inotify.read: %s"
          (Printexc.to_string exn) ;
        Unix.sleep 1 ;
        loop ()
      | lst ->
          List.iter (function
          | _watch, kinds, _cookie, Some filename
              when (List.mem Inotify.Create kinds ||
                    List.mem Inotify.Moved_to kinds) &&
                   not (List.mem Inotify.Isdir kinds) ->
              f (N.path filename)
          | ev ->
            !logger.debug "Received a useless inotification: %s"
              (Inotify.string_of_event ev)) lst ;
          loop ()) in
  loop ()

type file_notifier =
  { files : (Inotify.watch * N.path) list ;
    handler : Unix.file_descr ;
    alarm_file : N.path }

let make_file_notifier files =
  let handler = Inotify.create () in
  (* To be used as an alarm, to stop Inotify.read after some time: *)
  let alarm_file =
    Filename.temp_file "ramen_alarm_file_" ".tmp" |>
    N.path in
  let files =
    List.map (fun (fname : N.path) ->
      Files.mkdir_all ~is_file:true fname ;
      let mask = Inotify.[ S_Close_write ; S_Moved_to ] in
      Inotify.add_watch handler (fname :> string) mask, fname
    ) (alarm_file :: files) in
  { handler ; alarm_file ; files }

let wait_file_changes ?(while_=always) ~max_wait n =
  match Unix.select [ n.handler ] [] [] max_wait with
  | exception Unix.(Unix_error (EINTR, _, _)) ->
      None
  | [], _, _ ->
      None
  | _, _, _ ->
      if while_ () then (
        match BatUnix.restart_on_EINTR Inotify.read n.handler with
        | exception exn ->
            if exn <> Exit then
              !logger.error "Cannot Inotify.read: %s"
                (Printexc.to_string exn) ;
            raise exn
        | lst ->
            (try Some (
              List.find_map (function
                | watch, kinds, _cookie, _ as ev (* The file is not given *)
                    when (List.mem Inotify.Close_write kinds ||
                          List.mem Inotify.Moved_to kinds) &&
                         not (List.mem Inotify.Isdir kinds) ->
                    (match List.assoc watch n.files with
                    | exception Not_found ->
                        !logger.error "Received notification %S about unknown \
                                       watch (known watches: %a)"
                          (Inotify.string_of_event ev)
                          (pretty_enum_print Int.print)
                            (List.enum n.files /@ fst /@ Inotify.int_of_watch) ;
                        None
                    | fname ->
                        Some fname)
                | watch, kinds, _cookie, None
                    when Inotify.int_of_watch watch = -1 &&
                         List.mem Inotify.Q_overflow kinds ->
                    !logger.error "Inotify overflow!" ;
                    None
                | ev ->
                    !logger.debug "Received a useless inotification: %s"
                      (Inotify.string_of_event ev) ;
                    None
              ) lst)
            with Not_found -> None)
      ) else None
