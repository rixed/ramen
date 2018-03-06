(* OutRef files are the files describing where a func should send its
 * output. It's basically a list of ringbuf files, but soon we will have
 * more info in there.
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

module Lock =
struct
  open Lwt_unix

  (* For simplicity we use a single RW lock for all out_ref, here it is: *)
  let internal_lock = RWLock.make ()

  let with_lock with_int_lock op fname f =
    mkdir_all ~is_file:true fname ;
    (* Of course we lock ourself before locking other processes. *)
    let%lwt fd = openfile fname [O_RDWR; O_CREAT] 0o640 in
    with_int_lock internal_lock (fun () ->
      (*!logger.debug "Got internal lock" ;*)
      (* Just grab the first "byte", probably simpler than the whole file *)
      let%lwt () = lockf fd op 1 in
      (*!logger.debug "Got lockf on %s" fname ;*)
      finalize f (fun () ->
        let%lwt () = lockf fd F_ULOCK 1 in
        close fd))

  let with_r_lock fname = with_lock RWLock.with_r_lock F_RLOCK fname
  let with_w_lock fname = with_lock RWLock.with_w_lock F_LOCK fname
end

type out_spec = string * bool list

let string_of_out_spec (fname, keep) =
  fname ^"|"^ String.of_list (List.map (fun keep ->
    if keep then 'X' else '_') keep)

let out_spec_of_string str =
  let fname, skiplist = String.split str ~by:"|" in
  fname,
  String.to_list skiplist |> List.map ((=) 'X')

(* For debug only: *)
let print_out_specs oc outs =
  Map.print ~sep:"; " String.print (List.print Bool.print) oc outs

(* Used by ramen when starting a new worker to initialize (or reset) its
 * output: *)
let set_ fname outs =
  File.write_lines fname (Map.enum outs /@ string_of_out_spec)

let set fname outs =
  Lock.with_w_lock fname (fun () ->
    (*!logger.debug "Got write lock for set on %s" fname ;*)
    wrap (fun () -> set_ fname outs))

let read_ fname =
  File.lines_of fname /@ out_spec_of_string |> Map.of_enum

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
