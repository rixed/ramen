open Batteries
open Lwt
open RamenLog

type notifier =
  { mutable reported : (string * float (* time *)) list ;
    dirname : string ;
    while_ : unit -> bool }

let make ?(while_=(fun () -> true)) dirname =
  return { reported = [] ; dirname ; while_ }

let rec for_each f n =
  if not (n.while_ ()) then return_unit else
  let%lwt files = Lwt_unix.files_of_directory n.dirname |>
                  Lwt_stream.to_list in
  let files = Array.of_list files in (* FIXME *)
  (*!logger.debug "%d files in %s" (Array.length files) n.dirname ;*)
  Array.fast_sort String.compare files ;
  let rec merge prev i next_reported =
    if not (n.while_ ()) then (
      !logger.info "Stop listening to directory %s" n.dirname ;
      return_unit
    ) else if i >= Array.length files then (
      (*!logger.debug "No new files in %s" n.dirname ;*)
      n.reported <- List.rev_append prev next_reported ;
      let%lwt () = Lwt_unix.sleep 1. in
      for_each f n
    ) else (
      let s = Unix.stat (n.dirname ^"/"^ files.(i)) in
      if s.Unix.st_kind = Unix.S_DIR then
        merge prev (i + 1) next_reported
      else (
        let f_mtime = s.Unix.st_mtime in
        match next_reported with
        | (r, r_mtime as rpair)::rest ->
          (match String.compare r files.(i) with
          | 0 ->
            if f_mtime > r_mtime then  (
              !logger.debug "File %S has changed" files.(i) ;
              let%lwt () = f files.(i) in
              merge ((r, f_mtime) :: prev) (i + 1) rest
            ) else (
              (* Still the same, keep going *)
              merge (rpair :: prev) (i + 1) rest
            )
          | 1 ->
            (* files.(i) is new: insert and notify *)
            !logger.debug "File %S is new" files.(i) ;
            let%lwt () = f files.(i) in
            merge ((files.(i), f_mtime) :: prev) (i + 1) next_reported
          | _ ->
            !logger.debug "File %S is new" files.(i) ;
            let%lwt () = f files.(i) in
            merge ((files.(i), f_mtime) :: rpair :: prev) (i + 1) rest
          )
        | [] ->
          !logger.debug "File %S is new" files.(i) ;
          let%lwt () = f files.(i) in
          merge ((files.(i), f_mtime) :: prev) (i + 1) []
      )
    ) in
  merge [] 0 n.reported
