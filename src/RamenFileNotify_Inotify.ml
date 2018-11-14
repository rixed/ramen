open Batteries
open RamenLog
open RamenHelpers

type notifier =
  { mutable already_present : string list ;
    dirname : string ;
    handler : Unix.file_descr ;
    while_ : unit -> bool }

let make ?(while_=always) dirname =
  let handler = Inotify.create () in
  let mask = Inotify.[ S_Create ; S_Moved_to ; S_Onlydir ] in
  let _ = Inotify.add_watch handler dirname mask in
  let already_present =
    Sys.files_of dirname |>
    List.of_enum in
  let already_present = List.fast_sort String.compare already_present in
  !logger.info "%d files already present when starting inotifier"
    (List.length already_present) ;
  { already_present ; dirname ; handler ; while_ }

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
              f filename
          | ev ->
            !logger.debug "Received a useless inotification: %s"
              (Inotify.string_of_event ev)) lst ;
          loop ()) in
  loop ()
