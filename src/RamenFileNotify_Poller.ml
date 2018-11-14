open Batteries
open RamenLog
open RamenHelpers

type notifier =
  { mutable reported : (string * float (* time *)) list ;
    dirname : string ;
    while_ : unit -> bool }

let make ?(while_=always) dirname =
  { reported = [] ; dirname ; while_ }

let rec for_each f n =
  if n.while_ () then (
    let files = Sys.files_of n.dirname |>
                List.of_enum in
    let files = List.fast_sort String.compare files in
    let num_files = List.length files in
    !logger.debug "%d files to monitor." num_files ;
    (* Merge the list of files with the previously known files (in
     * [n.reported]). Both are ordered alphabetically. The result is then
     * stored in [n.reported]. *)
    let rec merge prev next files =
      if not (n.while_ ()) then (
        !logger.info "Stop listening to directory %s" n.dirname
      ) else match files with
      | [] ->
          List.iter (fun (r, _) -> !logger.debug "File %S is gone" r) next ;
          n.reported <- List.rev prev ;
          Unix.sleep 1 ;
          for_each f n
      | (file :: files') as files ->
          (match Unix.stat (n.dirname ^"/"^ file) with
          | exception Unix.Unix_error (Unix.ENOENT, _, _) ->
              (* File might have been read and deleted by another worker
               * already *)
              merge prev next files'
          | s ->
              if s.Unix.st_kind = Unix.S_DIR then
                merge prev next files'
              else (
                let f_mtime = s.Unix.st_mtime in
                match next with
                | (r, r_mtime as rpair)::rest ->
                  let cmp = String.compare r file in
                  if cmp = 0 then (
                    if f_mtime > r_mtime then  (
                      !logger.debug "File %S has changed" file ;
                      f file ;
                      merge ((r, f_mtime) :: prev) rest files'
                    ) else (
                      (* Still the same, keep going *)
                      merge (rpair :: prev) rest files'
                    )
                  ) else if cmp > 0 then (
                    (* file is new: insert and notify *)
                    !logger.debug "File %S is new" file ;
                    f file ;
                    merge ((file, f_mtime) :: prev) next files'
                  ) else (
                    !logger.debug "File %S is gone" r ;
                    merge prev rest files
                  )
                | [] ->
                  !logger.debug "File %S is new" file ;
                  f file ;
                  merge ((file, f_mtime) :: prev) [] files'
              )
          ) in
    merge [] n.reported files)
