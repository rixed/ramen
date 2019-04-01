(* In distributed mode we need to know the list of all possible servers so
 * we can filter them by Globs, which we cannot do with DNS. Also, it helps
 * to run several tunneld servers in the same TCP stack.
 *
 * This is used by ramen services but not by workers, which are passed all
 * the configuration they need via envvars.
 *
 * This implementation just read a static file.
 * TODO: Alternative implementations. *)
open Batteries
open RamenHelpers
module C = RamenConf
module N = RamenName
module Files = RamenFiles

type entry =
  { host : N.host ; port : int } [@@ppp PPP_OCaml]

let print_entry oc e =
  Printf.fprintf oc "%a:%d" N.host_print e.host e.port

type services =
  (string, entry) Hashtbl.t [@@ppp PPP_OCaml]

let services_file persist_dir =
  N.path_cat [ persist_dir ; N.path "services" ;
               N.path RamenVersions.services ; N.path "services" ]

(* FIXME: cache it *)
let load conf =
  let fname = services_file conf.C.persist_dir in
  fail_with_context "Reading services file" (fun () ->
    Files.ppp_of_file ~default:"{}" services_ppp_ocaml fname)

let resolve conf name =
  let services = load conf in
  Hashtbl.find services name

let lookup conf glob =
  if Globs.has_wildcard glob then
    let services = load conf in
    hashtbl_find_all (fun k _ ->
      Globs.matches glob k
    ) services |> List.map snd
  else
    [ resolve conf (Globs.decompile glob) ]

