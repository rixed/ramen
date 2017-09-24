open RamenLog

type notifier =
  { mutable already_present : string list ;
    dirname : string ;
    handler : Lwt_inotify.t }

let make dirname =
  (* inotify will silently do nothing if that directory does not exist: *)
  let%lwt handler = Lwt_inotify.create () in
  let mask = Inotify.[ S_Create ; S_Moved_to ; S_Onlydir ] in
  let%lwt _ = Lwt_inotify.add_watch handler dirname mask in
  let%lwt already_present =
    Lwt_unix.files_of_directory dirname |>
    Lwt_stream.to_list in
  Lwt.return { already_present ; dirnmae ; handler }

let rec next n =
  match n.already_present with
  | f :: rest ->
    n.already_present <- rest ;
    Lwt.return f
  | [] ->
    match%lwt Lwt_inotify.read n.handler with
    | _watch, kinds, _cookie, Some filename
      when (List.mem Inotify.Create kinds ||
            List.mem Inotify.Moved_to kinds) &&
           not (List.mem Inotify.Isdir kinds) ->
      Lwt.return filename
    | ev ->
      !logger.debug "Received a useless inotification: %s"
        (Inotify.string_of_event ev) ;
      next handler
