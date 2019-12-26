open Batteries
open RamenHelpers
open RamenLog
module Files = RamenFiles
module N = RamenName
module T = RamenTypes

(*
 * Factors possible values:
 *)

type possible_values_index =
  { min_time : float ;
    (* Name of the previous file where that set was saved.
     * This file is renamed after every write. *)
    fname : N.path ;
    mutable values : T.value Set.t }

(* Just a placeholder to init the initial array. *)
let possible_values_empty =
  { min_time = max_float ;
    fname = N.path "" ; values = Set.empty }

let possible_values_file dir factor min_time =
  N.path_cat [ dir ; Files.quote (N.path factor) ;
               N.path (Printf.sprintf "%h" min_time) ]

let save_possible_values prev_fname pvs =
  (* We are going to write a new file and delete the former one.
   * We only need a lock when that's the same file. *)
  let do_write fd = Files.marshal_into_fd fd pvs.values in
  if prev_fname = pvs.fname then (
    !logger.debug "Updating index %a" N.path_print prev_fname ;
    RamenAdvLock.with_w_lock prev_fname do_write
  ) else (
    !logger.debug "Creating new index %a" N.path_print pvs.fname ;
    Files.mkdir_all ~is_file:true pvs.fname ;
    let flags = Unix.[ O_CREAT; O_EXCL; O_WRONLY; O_CLOEXEC ] in
    (match Unix.openfile (pvs.fname :> string) flags 0o644 with
    | exception Unix.(Unix_error (EEXIST, _, _)) ->
        (* Although we though we would create a new file for a singleton,
         * it turns out this file exists already. This could happen when
         * event time goes back and forth, which is allowed. So we have
         * to merge the indices now: *)
        !logger.warning "Stumbled upon preexisting index %a, merging..."
          N.path_print pvs.fname ;
        RamenAdvLock.with_w_lock pvs.fname (fun fd ->
          let prev_set : T.value Set.t =
            Files.marshal_from_fd ~default:Set.empty pvs.fname fd in
          let s = Set.union prev_set pvs.values in
          (* Keep past values for the next write: *)
          pvs.values <- s ;
          do_write fd)
    | fd -> do_write fd) ;
    if not (N.is_empty prev_fname) then
      log_and_ignore_exceptions Files.safe_unlink prev_fname)


