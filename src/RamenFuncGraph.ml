(* Build an object representing the graph of the whole map of sites, programs
 * and functions. *)
open Batteries
open RamenLog
open RamenHelpers
open RamenConsts
module C = RamenConf
module RC = C.Running
module F = C.Func
module P = C.Program
module O = RamenOperation
module N = RamenName
module Services = RamenServices
module OutRef = RamenOutRef

open Binocle

let stats_graph_build_time =
  RamenWorkerStats.ensure_inited (fun save_dir ->
    Histogram.make ~save_dir:(save_dir :> string)
      Metric.Names.worker_graph_build_time
      "Workers graph build time"
      Histogram.powers_of_two)

type t =
  { h : (N.site, (N.program * N.func, entry) Hashtbl.t) Hashtbl.t ;
    (* Hack: parents of used entries not on the graph yet queue here
     * waiting for [fix_used]. *)
    mutable delay_used : parent Set.t }

and entry =
  { rce : RC.entry ;
    prog : P.t ;
    func : F.t ;
    parents : parent Set.t ;
    (* False if the node has no child, no notification, no active
     * export... *)
    mutable used : bool }

and parent = N.site * N.program * N.func

let print_parents oc parents =
  let print_parent oc (ps, pp, pf) =
    Printf.fprintf oc "%a:%a/%a"
      N.site_print ps
      N.program_print pp
      N.func_print pf in
  Set.print print_parent oc parents

let print oc t =
  let print_entry oc ge =
    Printf.fprintf oc "%a%s"
      print_parents ge.parents
      (if ge.used then "" else " (unused)") in
  let print_key oc (pn, fn) =
    Printf.fprintf oc "%a/%a" N.program_print pn N.func_print fn in
  Hashtbl.print
    N.site_print_quoted
    (Hashtbl.print print_key print_entry) oc t.h

let empty () =
  { h = Hashtbl.create 31 ;
    delay_used = Set.empty }

let find t site pn fn =
  let h = Hashtbl.find t.h site in
  Hashtbl.find h (pn, fn)

let rec make_used t parents =
  Set.iter (fun (ps, pp, pf as parent) ->
    match find t ps pp pf with
    | exception Not_found ->
        t.delay_used <- Set.add parent t.delay_used (* later! *)
    | pge ->
        if not pge.used then (
          pge.used <- true ;
          make_used t pge.parents)
  ) parents

let fix_used t =
  make_used t t.delay_used ;
  t.delay_used <- Set.empty

let make_entry conf t rce prog func parents =
  let has_export () =
    let out_ref =
      C.out_ringbuf_names_ref conf func |>
      OutRef.read in
    not (Hashtbl.is_empty out_ref) in
  let used =
    not func.F.is_lazy ||
    O.has_notifications func.F.operation ||
    has_export () in
  if used then make_used t parents ;
  { rce ; prog ; func ; parents ; used }

let sites_matching p =
  Set.filter (fun (s : N.site) -> Globs.matches p (s :> string))

let parents_sites local_site programs sites func =
  List.fold_left (fun s (psite, rel_pprog, pfunc) ->
    let pprog = F.program_of_parent_prog func.F.program_name rel_pprog in
    match Hashtbl.find programs pprog with
    | exception Not_found ->
        s
    | rce, _get_rc ->
        if rce.RC.status <> RC.MustRun then s else
        (* Where the parents are running: *)
        let where_running = sites_matching rce.RC.on_site sites in
        (* Restricted to where [func] selects from: *)
        let psites =
          match psite with
          | O.AllSites ->
              where_running
          | O.ThisSite ->
              if Set.mem local_site where_running then
                Set.singleton local_site
              else
                Set.empty
          | O.TheseSites p ->
              sites_matching p where_running in
        Set.union s (Set.map (fun h -> h, pprog, pfunc) psites)
  ) Set.empty func.F.parents

(* Build a map from site name to the map of (prog, func names) to
 * prog*func*parents, where parents is a set of site*prog*func: *)
let make conf =
  let t = empty () in
  let sites = Services.all_sites conf in
  let programs = RC.with_rlock conf identity in
  with_time (fun () ->
    Hashtbl.iter (fun _ (rce, get_rc) ->
      if rce.RC.status = RC.MustRun then (
        let where_running = sites_matching rce.RC.on_site sites in
        !logger.debug "%a must run on sites matching %a: %a"
          N.path_print rce.RC.bin
          Globs.print rce.RC.on_site
          (Set.print N.site_print_quoted) where_running ;
        match get_rc () with
        | exception _ ->
            (* Errors have been logged already, nothing more can
             * be done about this. *)
            ()
        | prog ->
            List.iter (fun func ->
              Set.iter (fun local_site ->
                let parents =
                  parents_sites local_site programs sites func in
                let h =
                  hashtbl_find_option_delayed
                    (fun () -> Hashtbl.create 5) t.h local_site in
                let k = func.F.program_name, func.F.name in
                let ge = make_entry conf t rce prog func parents in
                Hashtbl.add h k ge
              ) where_running
            ) prog.P.funcs
      ) (* else this program is not running *)
    ) programs ;
    fix_used t)
    (Histogram.add (stats_graph_build_time conf.C.persist_dir)) ;
  t
