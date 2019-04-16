open Batteries
open RamenLog
open RamenHelpers
module N = RamenName
module Files = RamenFiles

type dir_notifier =
  { mutable reported : (N.path * float (* time *)) list ;
    dirname : N.path ;
    while_ : unit -> bool }

let make ?(while_=always) dirname =
  Files.mkdir_all ~is_file:false dirname ;
  { reported = [] ; dirname ; while_ }

(* Call f on each new file in the directory.
 * At start, call f on each file already present. *)
let rec for_each f n =
  if n.while_ () then (
    let files = ((Sys.files_of (n.dirname :> string)) /@ N.path) |>
                List.of_enum in
    let files = List.fast_sort N.compare files in
    let num_files = List.length files in
    !logger.debug "%d files to monitor." num_files ;
    (* Merge the list of files with the previously known files (in
     * [n.reported]). Both are ordered alphabetically. The result is then
     * stored in [n.reported]. *)
    let rec merge prev next files =
      if not (n.while_ ()) then (
        !logger.info "Stop listening to directory %a" N.path_print n.dirname
      ) else match files with
      | [] ->
          List.iter (fun (r, _) ->
            !logger.debug "File %a is gone" N.path_print r
          ) next ;
          n.reported <- List.rev prev ;
          Unix.sleep 1 ;
          for_each f n
      | (file :: files') as files ->
          (match Files.safe_stat (N.path_cat [ n.dirname ; file ]) with
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
                  let cmp = N.compare r file in
                  if cmp = 0 then (
                    if f_mtime > r_mtime then  (
                      !logger.debug "File %a has changed" N.path_print file ;
                      f file ;
                      merge ((r, f_mtime) :: prev) rest files'
                    ) else (
                      (* Still the same, keep going *)
                      merge (rpair :: prev) rest files'
                    )
                  ) else if cmp > 0 then (
                    (* file is new: insert and notify *)
                    !logger.debug "File %a is new" N.path_print file ;
                    f file ;
                    merge ((file, f_mtime) :: prev) next files'
                  ) else (
                    !logger.debug "File %a is gone" N.path_print r ;
                    merge prev rest files
                  )
                | [] ->
                  !logger.debug "File %a is new" N.path_print file ;
                  f file ;
                  merge ((file, f_mtime) :: prev) [] files'
              )
          ) in
    merge [] n.reported files)

(* Wait until any of the files are changed. [files] is an assoc list of file
 * name to last modification time stamp. Returns the new list of files, with
 * the changed one on top, or the empty list if not [while_]. *)
type file_notifier =
  { files : (N.path * float ref) list }

let make_file_notifier files =
  { files =
      List.map (fun fname ->
        Files.mkdir_all ~is_file:true fname ;
        fname, ref 0.
      ) files }

let wait_file_changes ?(while_=always) ?max_wait n =
  let rec loop tot_wait = function
    | (fname, mtime) :: rest ->
        (match Files.mtime fname with
        | exception _ ->
            loop tot_wait rest
        | t ->
            if t > !mtime then (
              mtime := t ;
              Some fname
            ) else
              loop tot_wait rest)
    | [] ->
        if Option.map_default ((<) tot_wait) true max_wait &&
           while_ ()
        then (
          let delay = 1. in
          Unix.sleepf delay ;
          loop (tot_wait +. delay) n.files
        ) else None in
  loop 0. n.files
