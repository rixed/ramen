(* OutRef files are the files describing where a func should send its
 * output. It's basically a list of ringbuf files, with a bitmask of fields
 * that must be written, and an optional timestamp  after which the export
 * should stop (esp. useful for non-ring buffers).
 * Ring and Non-ring buffers are distinguished with their filename
 * extensions, which are either ".r" (ring) or ".b" (buffer).
 * This has the nice consequence that you cannot have a single file name
 * repurposed into a different kind of storage.
 *
 * In case of non-ring buffers, on completion the file is renamed with the
 * min and max sequence numbers and timestamps (if known), and a new one is
 * created.
 *
 * We want to lock those files, both internally (same process) and externally
 * (other processes), although we are fine with advisory locking.
 * Unfortunately lockf will only lock other processes out so we have to combine
 * RWLocks and lockf.
 *)
open Batteries
open RamenHelpers
open RamenLog

(* How are data represented on disk: *)
type out_ref_conf =
  (string (* dest file *), file_spec_conf) Hashtbl.t [@@ppp PPP_OCaml]
and file_spec_conf =
  string (* field mask *) * float (* timeout *) [@@ppp PPP_OCaml]

(* ...and internally, where the field mask is a proper list of booleans: *)
type file_spec =
  { field_mask : bool list ; timeout : float (* 0 for no timeout *) }

let print_out_specs oc =
  Hashtbl.print String.print (fun _oc _s -> ()) oc

let string_of_field_mask mask =
  List.map (function true -> 'X' | false -> '_') mask |>
  String.of_list

let write_ fname c =
  ppp_to_file fname out_ref_conf_ppp_ocaml c

let read_ =
  ppp_of_file ~error_ok:false out_ref_conf_ppp_ocaml

let read fname =
  ensure_file_exists ~contents:"{}" fname ;
  RamenAdvLock.with_r_lock fname (fun () ->
    (*!logger.debug "Got read lock for read on %s" fname ;*)
    let now = Unix.gettimeofday () in
    let field_mask_of_string s =
      String.to_list s |> List.map ((=) 'X') in
    let still_valid timeout = timeout <= 0. || timeout > now in
    read_ fname |>
    Hashtbl.filter_map (fun _p (mask_str, timeout) ->
      if still_valid timeout then
        Some { field_mask = field_mask_of_string mask_str ;
               timeout }
      else None))

let add_ fname out_fname file_spec =
  let file_spec =
    string_of_field_mask file_spec.field_mask,
    file_spec.timeout in
  let h =
    try read_ fname
    with Sys_error _ ->
      Hashtbl.create 1
    in
  let rewrite () =
    Hashtbl.replace h out_fname file_spec ;
    write_ fname h ;
    !logger.debug "Adding %s into %s" out_fname fname in
  match Hashtbl.find h out_fname with
  | exception Not_found -> rewrite ()
  | prev_spec ->
    if prev_spec <> file_spec then rewrite ()

let add fname (out_fname, file_spec) =
  ensure_file_exists ~contents:"{}" fname ;
  RamenAdvLock.with_w_lock fname (fun () ->
    (*!logger.debug "Got write lock for add on %s" fname ;*)
    add_ fname out_fname file_spec)

let remove_ fname out_fname =
  let h = read_ fname in
  if Hashtbl.mem h out_fname then (
    Hashtbl.remove h out_fname ;
    write_ fname h ;
    !logger.debug "Removed %s from %s"
      out_fname fname)

let remove fname out_fname =
  ensure_file_exists ~contents:"{}" fname ;
  RamenAdvLock.with_w_lock fname (fun () ->
    (*!logger.debug "Got write lock for remove on %s" fname ;*)
    remove_ fname out_fname)

(* Check that fname is listed in outbuf_ref_fname: *)
let mem_ fname out_fname =
  let c = read_ fname in
  Hashtbl.mem c out_fname

let mem fname out_fname =
  ensure_file_exists ~contents:"{}" fname ;
  RamenAdvLock.with_r_lock fname (fun () ->
    (*!logger.debug "Got read lock for mem on %s" fname ;*)
    mem_ fname out_fname)
