(* OutRef files are the files describing where a node should send its
 * output. It's basically a list of ringbuf files, but soon we will have
 * more info in there. *)
open Batteries
open Helpers
open RamenLog

type out_spec = string

(* Used by ramen when starting a new worker to initialize (or reset) its
 * output: *)
let set fname outs =
  mkdir_all ~is_file:true fname ;
  File.write_lines fname (Set.enum outs)

let read fname =
  File.lines_of fname |> Set.of_enum

(* Used by ramen when starting a new worker to add it to its parents outref: *)
let add fname out =
  let lines =
    try read fname
    with Sys_error _ ->
      set fname Set.empty ;
      Set.empty
    in
  if not (Set.mem out lines) then (
    let outs = Set.add out lines in
    set fname outs ;
    !logger.info "Adding %s into %s, now outputting to %a"
      out fname (Set.print String.print) outs)

(* Used by ramen when stopping a node to remove its input from its parents
 * out_ref: *)
let remove fname out =
  let out_files = read fname in
  set fname (Set.remove out out_files) ;
  !logger.info "Removed %s from %s, now output only to: %a"
    out fname (Set.print String.print) out_files

(* Check that fname is listed in outbuf_ref_fname: *)
let mem fname out =
  read fname |> Set.mem out
