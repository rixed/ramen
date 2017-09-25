open Lwt
open RamenLog

type notifier =
  { mutable already_present : string list ;
    dirname : string ;
    handler : Lwt_inotify.t }

let make dirname =
  let%lwt handler = Lwt_inotify.create () in
  let mask = Inotify.[ S_Create ; S_Moved_to ; S_Onlydir ] in
  let%lwt _ = Lwt_inotify.add_watch handler dirname mask in
  let%lwt already_present =
    Lwt_unix.files_of_directory dirname |>
    Lwt_stream.to_list in
  let already_present = List.fast_sort String.compare already_present in
  return { already_present ; dirname ; handler }

let for_each f n =
  let%lwt () = Lwt_list.iter_s f n.already_present in
  let rec loop () =
    match%lwt Lwt_inotify.read n.handler with
    | _watch, kinds, _cookie, Some filename
      when (List.mem Inotify.Create kinds ||
            List.mem Inotify.Moved_to kinds) &&
           not (List.mem Inotify.Isdir kinds) ->
      f filename >>= loop
    | ev ->
      !logger.debug "Received a useless inotification: %s"
        (Inotify.string_of_event ev) ;
      loop () in
  loop ()
