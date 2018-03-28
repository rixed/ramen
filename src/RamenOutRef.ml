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
open Lwt
open Helpers
open RamenLog
module Lock = RamenAdvLock

type file_spec =
  { field_mask : bool list ; timeout : float (* 0 for no timeout *) }

let string_of_field_mask mask =
  List.map (function true -> 'X' | false -> '_') mask |>
  String.of_list

let string_of_file_spec file_spec =
  string_of_field_mask file_spec.field_mask ^"|"^
  string_of_float file_spec.timeout

let file_spec_print oc file_spec =
  String.print oc (string_of_file_spec file_spec)

type out_spec = string * file_spec

let string_of_out_spec (fname, file_spec) =
  fname ^"|"^ string_of_file_spec file_spec

let out_spec_of_string str =
  let make fname mask timeout =
    fname,
    { field_mask = String.to_list mask |> List.map ((=) 'X') ; timeout }
  in
  match String.split_on_char '|' str with
  | [ fname ; mask ; t ] -> make fname mask (float_of_string t)
  | _ ->
      !logger.error "Invalid line in out-ref: %S" str ;
      invalid_arg "out_spec_of_string"

(* For debug only: *)
let print_out_specs oc outs =
  Map.print ~sep:"; " String.print file_spec_print oc outs

(* Used by ramen when starting a new worker to initialize (or reset) its
 * output: *)
let set_ fname outs =
  File.write_lines fname (Map.enum outs /@ string_of_out_spec)

let set fname outs =
  Lock.with_w_lock fname (fun () ->
    (*!logger.debug "Got write lock for set on %s" fname ;*)
    wrap (fun () -> set_ fname outs))

let is_still_valid now file_spec =
  file_spec.timeout <= 0. || file_spec.timeout > now

let read_ fname =
  let now = Unix.gettimeofday () in
  File.lines_of fname /@ out_spec_of_string //
  (fun (_, file_spec) -> is_still_valid now file_spec) |>
  Map.of_enum

let read fname =
  Lock.with_r_lock fname (fun () ->
    (*!logger.debug "Got read lock for read on %s" fname ;*)
    wrap (fun () -> read_ fname))

(* Used by ramen when starting a new worker to add it to its parents outref: *)
let add_ fname (out_fname, out_fields) =
  let lines =
    try read_ fname
    with Sys_error _ ->
      set_ fname Map.empty ;
      Map.empty
    in
  let rewrite () =
    let outs = Map.add out_fname out_fields lines in
    set_ fname outs ;
    !logger.debug "Adding %s into %s, now outputting to %a"
      out_fname fname print_out_specs outs in
  match Map.find out_fname lines with
  | exception Not_found -> rewrite ()
  | prev_fields ->
    if prev_fields <> out_fields then rewrite ()

let add fname out =
  if String.length fname = 0 ||
     (let c = fname.[String.length fname - 1] in c <> 'r' && c <> 'b')
  then invalid_arg "RamenOutRef.add" ;
  Lock.with_w_lock fname (fun () ->
    (*!logger.debug "Got write lock for add on %s" fname ;*)
    wrap (fun () -> add_ fname out))

(* Used by ramen when stopping a func to remove its input from its parents
 * out_ref: *)
let remove_ fname out_fname =
  let out_files = read_ fname in
  let out_files' = Map.remove out_fname out_files in
  set_ fname out_files' ;
  !logger.debug "Removed %s from %s, now output only to: %a"
    out_fname fname print_out_specs out_files'

let remove fname out_fname =
  Lock.with_w_lock fname (fun () ->
    (*!logger.debug "Got write lock for remove on %s" fname ;*)
    wrap (fun () -> remove_ fname out_fname))

(* Check that fname is listed in outbuf_ref_fname: *)
let mem_ fname out_fname =
  read_ fname |> Map.mem out_fname

let mem fname out_fname =
  Lock.with_r_lock fname (fun () ->
    (*!logger.debug "Got read lock for mem on %s" fname ;*)
    wrap (fun () -> mem_ fname out_fname))
