(* A service of its own, the archivist job is to monitor everything
 * that's running and, guided by some user configuration, to find out
 * which function should be asked to archive its history and for all
 * long (this being used by the GC eventually). *)
open Stdint
open Batteries
open RamenHelpers
open RamenLog
open RamenSmt
module C = RamenConf
module F = C.Func
module P = C.Program

(* We want to serialize globs as strings: *)
type glob = Globs.pattern
let glob_ppp_ocaml : glob PPP.t =
  let star = '*' and placeholder = '?' and escape = '\\' in
  let s2g = Globs.compile ~star ~placeholder ~escape
  and g2s = Globs.decompile in
  PPP.(string >>: (g2s, s2g))

type user_conf = per_node_conf array [@@ppp PPP_OCaml]

and per_node_conf =
  { (* Functions which FQ match [pattern] will be collectively allowed to
       use [size] bytes of storage. If nothing match then no size
       restriction applies. *)
    pattern : glob [@ppp_default Globs.all] ;
    size : int }
    [@@ppp PPP_OCaml]

let get_user_conf fname =
  ppp_of_file ~error_ok:true user_conf_ppp_ocaml fname

let user_conf_file conf =
  conf.C.persist_dir ^"/archiving/"
                     ^ RamenVersions.archiving_conf
                     ^"/config"

(* We need to listen to all stats and based on that we build an understanding
 * of each function characteristics such as output size / time, CPU spent and
 * life expectancy.
 * This could be done with a dedicated worker but for now we just tail on
 * #notifs manually.
 *
 * Then we also need the RC as we also need to know the graph to be able to
 * compute distance between nodes, and also to find binary so that we can
 * measure their "complexity".
 *
 * All these informations are then used to compute the disk shares that are
 * then stored in a file as a mapping from FQ to number of bytes allowed on
 * disk: *)

type per_func_quota = (RamenName.fq, int) Hashtbl.t
  [@@ppp PPP_OCaml]

let get_per_func_quota fname =
  ppp_of_file per_func_quota_ppp_ocaml fname

(* Global per-func stats that are updated by the thread reading #notifs and
 * the one reading the RC, and also saved on disk while ramen is not running:
 *)

type per_func_stats_ser = (RamenName.fq, func_stats) Hashtbl.t
  [@@ppp PPP_OCaml]

and func_stats =
  { startup_time : float ;
    tuples : int64 ; (* We sacrifice one bit for convenience *)
    bytes : int64 ;
    cpu : float (* Cumulated seconds *) ;
    ram : int64 (* Max observed heap size *) }
  [@@ppp PPP_OCaml]

let get_per_func_stats fname =
  ppp_of_file per_func_stats_ser_ppp_ocaml fname

let func_stats_empty =
  { startup_time = 0. ; tuples = 0L ;
    bytes = 0L ; cpu = 0. ; ram = 0L }

let func_stats_of_ps_stats s =
  { startup_time = s.RamenPs.startup_time ;
    tuples = Uint64.(to_int64 (s.out_count |? zero)) ;
    bytes = Uint64.(to_int64 (s.bytes_out |? zero)) ;
    cpu = s.cpu ;
    ram = Uint64.(to_int64 s.max_ram) }

let add_func_stats a b =
  { startup_time = a.startup_time ;
    tuples = Int64.(a.tuples + b.tuples) ;
    bytes = Int64.(a.bytes + b.bytes) ;
    cpu = a.cpu +. b.cpu ;
    ram = Int64.(a.ram + b.ram) }

(* Actually, when running we keep both a total of all past stats and the
 * stats since last worker startup: *)
let per_func_stats : (RamenName.fq, (func_stats * func_stats)) Hashtbl.t =
  Hashtbl.create 29

let per_func_stats_lock = Mutex.create ()

let per_func_stats_sync f =
  BatMutex.synchronize ~lock:per_func_stats_lock f

(* tail -f the #notifs stream and update per_func_stats: *)
let notification_reader ?while_ conf =
  !logger.info "Starting thread reading all notifications..." ;
  try forever (fun () ->
    RamenPs.read_stats ?while_ conf |>
    Hashtbl.iter (fun fq s ->
      per_func_stats_sync
        (Hashtbl.modify_opt fq (function
          | None ->
              Some (func_stats_empty, func_stats_of_ps_stats s)
          | Some (tot, last) ->
              if s.RamenPs.startup_time = last.startup_time then (
                Some (tot, func_stats_of_ps_stats s)
              ) else (
                (* worker have restarted. We assume it's still mostly the
                 * same operation. Maybe consider the function signature
                 * (and add it to the stats?) *)
                Some (add_func_stats tot last, func_stats_of_ps_stats s)
              )
        )) per_func_stats
    ) ;
    Option.may (fun w -> if not (w ()) then raise Exit) while_ ;
    RamenProcesses.sleep_or_exit ?while_ (jitter 10.)
  ) ()
  with Exit -> ()

(* Returns v relative to r as an integer from 0 to 10 inclusive. *)
let scale_to r v =
  if v >= r then 10 else
  if v <= 0. then 0 else
  int_of_float (11. *. v /. r)

(* Return the "processing cost" per tuple, ie the CPU time rate. *)
let cost_of_stats now (tot, last) =
  let s = add_func_stats tot last in
  let dt = now -. tot.startup_time in
  s.cpu /. dt

(* The cost of running fq per unit of time, from 0 to 10. *)
let cost_of_func () =
  let now = Unix.gettimeofday () in
  let max_cost, avg_cost =
    let ma, sum, count =
      per_func_stats_sync
        (Hashtbl.fold (fun _fq s (ma, sum, count) ->
          let c = cost_of_stats now s in
          max ma c, sum +. c, count + 1
        ) per_func_stats) (0., 0., 0) in
    ma,
    if count = 0 then 0.001 (* wtv at this point *) else
    sum /. float_of_int count
  in
  fun fq ->
    (match per_func_stats_sync
            (Hashtbl.find per_func_stats) fq with
    | exception Not_found -> avg_cost
    | s -> cost_of_stats now s) |>
    scale_to max_cost

(* Return a graph of the running config with vertices weighted according to
 * their computing cost: *)
let rc_complexity conf =
  let vertices, edges = RamenPs.func_graph conf in
  let cost_of_func = cost_of_func () in
  let vertices =
    Hashtbl.map (fun fq func ->
      func, cost_of_func fq
    ) vertices in
  vertices, edges

(* For any two functions, return the shortest path (reversed) in between
 * them or raise Not_found: *)
let path edges src dst =
  let fold v f u =
    Hashtbl.find edges v |>
    List.fold_left (fun u d ->
      let d' = (RamenName.string_of_fq d).[0] in
      f u d d') u
  in
  path_in_graph ~src ~dst { fold }

(*
 * Optimising storage for a duration of one year (actually a parameter):
 *
 * Premises:
 *
 * - We have hard constraints given by user configuration that set a higher
 *   bound of the sum of some history sizes. We need those constraints named
 *   in case we have to report non-satisfiability;
 *
 * - Assuming we will query each persistent function complete history once,
 *   the total cost of doing so must be as small as possible ; the cost being
 *   the IO cost of reading the required history and the CPU (and ideally, RAM)
 *   cost of all functions involved to recompute the desired output;
 *
 * The processing cost of each function is a given constant.
 *
 * The total storage cost of the solution is the storage cost of each and
 * every function. We also want it as small as possible.
 *
 * Each individual cost is represented as an integer from 0 to 10.
 *)

let list_print oc =
  List.print ~first:"" ~last:"" ~sep:" " oc

let hashkeys_print p =
  Hashtbl.print ~first:"" ~last:"" ~sep:" " ~kvsep:"" p (fun _ _ -> ())

let const pref fq =
  pref ^ scramble (RamenName.string_of_fq fq)

let histo = const "h_"
let size = const "s_"
let query_cost = const "q_"

(* For each function, declare the boolean h_f: *)
let emit_all_vars oc vertices =
  Hashtbl.iter (fun fq _ ->
    Printf.fprintf oc
      "; Storage of %s\n\
       (declare-const %s Bool) ; archive history?\n\
       (declare-const %s Int) ; archive size\n\
       (declare-const %s Int) ; query cost\n"
      (RamenName.string_of_fq fq)
      (histo fq) (size fq) (query_cost fq)
  ) vertices

let emit_constraints vertices oc user_conf =
  Array.iteri (fun i node_conf ->
    let err_name = "user_"^ string_of_int i in
    let matching =
      Hashtbl.enum vertices //
      (fun (fq, _) ->
        Globs.matches node_conf.pattern (RamenName.string_of_fq fq)) |>
      List.of_enum in
    if matching = [] then
      !logger.warning "User constraint %a matches no function"
        Globs.print_pattern node_conf.pattern
    else
      Printf.fprintf oc
        "(assert (! (+ 0 %a) : named %s))\n"
        (list_print (fun oc (fq, _) -> String.print oc (size fq))) matching
        err_name
  ) user_conf

let cost_reading_history _fq =
  (* TODO *)
  5

let parents_of inv_edges fq =
  Hashtbl.find_default inv_edges fq []

(* The cost for querying the function f is:
 *
 *   if f is archiving its history, then it's the cost of reading the history.
 *   otherwise, it's the cost of querying each of its parent + the processing
 *   cost of f.  *)
let emit_query_costs vertices oc inv_edges =
  Hashtbl.iter (fun fq (_, proc_cost) ->
    Printf.fprintf oc
      "(assert (or (and %s (= %s %d))\n\
                   (and (not %s) "
      (histo fq) (query_cost fq) (cost_reading_history fq)
      (histo fq) ;
    let parents = parents_of inv_edges fq in
    Printf.fprintf oc "(= %s (+ %a %d)))))\n"
      (query_cost fq)
      (list_print (fun oc parent -> String.print oc (query_cost parent))) parents
      proc_cost
  ) vertices

(* The storage cost of f is:
 *
 *   if f is archiving its history, then that's the estimated size after one
 *   year. Otherwise, that's zero.  *)
let emit_storage_costs oc vertices =
  Hashtbl.iter (fun fq _ ->
    Printf.fprintf oc "%a\n"
      (emit_imply_not (histo fq)) ("(= 0 "^ size fq ^")")
  ) vertices

(* The cost of querying the solution is the cost of querying each persistent
 * function. We want it as small as possible.  *)
let emit_persistents_query_costs oc vertices =
  let persistents =
    Hashtbl.enum vertices //@
    (fun (fq, (func, _proc_cost)) ->
      if func.F.persistent then Some fq else None) |>
    List.of_enum in
  Printf.fprintf oc "(+ 0%a)"
    (list_print (fun oc fq -> String.print oc (query_cost fq))) persistents

(* We also want to store as little as possible, so we try to minimize the
 * total size of storage on disk.  *)
let emit_total_storage_costs oc vertices =
  Printf.fprintf oc "(+ 0%a)"
    (hashkeys_print (fun oc fq -> String.print oc (size fq))) vertices

let emit_smt2 conf (vertices, inv_edges) oc ~optimize =
  let user_conf =
    try get_user_conf (user_conf_file conf)
    with Unix.(Unix_error (ENOENT, _, _)) | Sys_error _ ->
      !logger.info "No user configuration found" ;
      [||]
  in
  Printf.fprintf oc
    "%a\
     ; What we aim to know: should each function archi e or not?\n\
     %a\n\
     ; User specified constraints:\n\
     %a\n\
     ; Query costs:\n\
     %a\n\
     ; Storage costs:\n\
     %a\n\
     ; Minimize the cost of querying each persistent function +\n\
     ; the total storage cost:\n\
     (minimize (+ %a %a))\n\
     %t"
    preamble optimize
    emit_all_vars vertices
    (emit_constraints vertices) user_conf
    (emit_query_costs vertices) inv_edges
    emit_storage_costs vertices
    emit_persistents_query_costs vertices
    emit_total_storage_costs vertices
    post_scriptum

let allocate_storage conf =
  let vertices, edges = rc_complexity conf in
  (* Create a map from vertices to parents: *)
  let inv_edges = Hashtbl.create (Hashtbl.length edges) in
  Hashtbl.iter (fun par children ->
    List.iter (fun child ->
      Hashtbl.modify_def [ par ] child (List.cons par) inv_edges
    ) children
  ) edges ;
  let fname = "/tmp/archiving.smt2"
  and emit = emit_smt2 conf (vertices, inv_edges)
  and parse_result _sym _vars _sort _term = ()
  and unsat _syms _output = ()
  in
  run_smt2 ~fname ~emit ~parse_result ~unsat
