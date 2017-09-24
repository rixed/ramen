open Batteries
open Lwt
open RamenLog

type notifier =
  { mutable reported : (string * float (* time *)) list ;
    dirname : string }

let make dirname = return { reported = [] ; dirname }

let next n =
  let%lwt files = Lwt_unix.files_of_directory n.dirname |>
                  Lwt_stream.to_list in
  let files = Array.of_list files in (* FIXME *)
  !logger.debug "%d files in %s" (Array.length files) n.dirname ;
  Array.fast_sort String.compare files ;
  let rec merge prev i next_reported =
    if i >= Array.length files then (
      !logger.debug "No new files in %s" n.dirname ;
      n.reported <- List.rev_append prev next_reported ;
      None
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
              !logger.debug "File %S have changed" files.(i) ;
              n.reported <- List.rev_append prev ((r, f_mtime)::rest) ;
              Some i
            ) else (
              (* Still the same, keep going *)
              merge (rpair :: prev) (i + 1) rest
            )
          | 1 ->
            (* files.(i) is new: insert and notify *)
            !logger.debug "File %S is new" files.(i) ;
            n.reported <- List.rev_append ((files.(i), f_mtime) :: prev) next_reported ;
            Some i
          | _ ->
            !logger.debug "File %S is new" files.(i) ;
            n.reported <- List.rev_append (rpair :: prev) ((files.(i), f_mtime) :: rest) ;
            Some i
          )
        | [] ->
          (* If we came that far, all previously reported haven't been modified.
           * Report a new one: *)
          !logger.debug "File %S is new" files.(i) ;
          n.reported <- List.rev ((files.(i), f_mtime) :: prev) ;
          Some i
      )
    ) in
  let rec loop () =
    match merge [] 0 n.reported with
    | None ->
      Lwt_unix.sleep 1. >>= loop
    | Some i ->
      Lwt.return files.(i) in
  loop ()
