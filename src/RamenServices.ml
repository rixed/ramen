(* In distributed mode we need to know the list of all possible servers so
 * we can filter them by Globs, which we cannot do with DNS. Also, it helps
 * to run several tunneld servers in the same TCP stack.
 *
 * This is used by ramen services but not by workers, which are passed all
 * the configuration they need via envvars.
 *
 * This implementation just reads a static file.
 * TODO: Alternative implementations.
 *
 * Better design making use of the confserver:
 * The tunneld register themselves. For this they just need to be given the
 * confserver url (already the case) and the site name (also already the case)
 * and then they just register themselves.
 * The client side might be more work if they are not already sharing the
 * conftree.
 *)
open Batteries
open RamenHelpersNoLog
module C = RamenConf
module N = RamenName
module Files = RamenFiles

type entry =
  { host : N.host ; port : int } [@@ppp PPP_OCaml]

let print_entry oc e =
  Printf.fprintf oc "%a:%d" N.host_print e.host e.port

type site_directory =
  (N.service, entry) Hashtbl.t [@@ppp PPP_OCaml]

type directory =
  (N.site, site_directory) Hashtbl.t [@@ppp PPP_OCaml]

let directory_file persist_dir =
  N.path_cat [ persist_dir ; N.path "services" ;
               N.path RamenVersions.services ; N.path "services" ]

let load =
  let ppp_of_file =
    Files.ppp_of_file ~default:"{}" directory_ppp_ocaml in
  fun conf ->
    let fname = directory_file conf.C.persist_dir in
    fail_with_context "Reading directory file" (fun () ->
      ppp_of_file fname)

let resolve conf site service =
  let directory = load conf in
  Hashtbl.find (Hashtbl.find directory site) service

let resolve_every_site conf service =
  let directory = load conf in
  Hashtbl.fold (fun site h lst ->
    match Hashtbl.find h service with
    | exception Not_found -> lst
    | e -> (site, e) :: lst
  ) directory []

let lookup conf site_glob service =
  if Globs.has_wildcard site_glob then
    let directory = load conf in
    hashtbl_find_all (fun (site : N.site) _ ->
      Globs.matches site_glob (site :> string)
    ) directory |>
    List.fold_left (fun lst (_site, site_dir) ->
      List.rev_append
        (Hashtbl.find_all site_dir service)
        lst
    ) []
  else
    let site = N.site (Globs.decompile site_glob) in
    [ resolve conf site service ]

let all_sites conf =
  let directory = load conf in
  let s = Hashtbl.keys directory |> Set.of_enum in
  (* For single-site runs, for instance tests, that does not want to bother
   * with a service directory: *)
  Set.add conf.C.site s
