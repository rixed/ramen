(* A service of its own, the archivist job is to monitor everything
 * that's running and, guided by some user configuration, to find out
 * which function should be asked to archive its history and for all
 * long (this being used by the GC eventually). *)
open Stdint
open Batteries
open RamenHelpers
open RamenLog
open RamenSmt
open RamenConsts
module C = RamenConf
module F = C.Func
module P = C.Program

let tot_archive_size = ref Default.archive_size

(* We want to serialize globs as strings: *)
type glob = Globs.pattern
let glob_ppp_ocaml : glob PPP.t =
  let star = '*' and placeholder = '?' and escape = '\\' in
  let s2g = Globs.compile ~star ~placeholder ~escape
  and g2s = Globs.decompile in
  PPP.(string >>: (g2s, s2g))

type user_conf =
  { (* Global size limit, in byte (although the SMT uses coarser grained
       sizes): *)
    size_limit : int ;
    (* The cost to retrieve one byte of archived data, expressed in the
     * unit of CPU time (ie. the time it takes to retrieve that byte if
     * you value the IO time as much as the CPU time): *)
    recall_cost : float [@ppp_default 100.] ;
    (* Individual nodes we want to keep some history, none by default.
     * TODO: replaces or override the persist flag + retention length
     * that should go with it): *)
    retentions : (glob, retention) Hashtbl.t
      [@ppp_default
        let h = Hashtbl.create 1 in
        Hashtbl.add h Globs.all
          { duration = 86400. *. 365. ; query_freq = 1. /. 600. } ;
        h ] }
  [@@ppp PPP_OCaml]

and retention =
  { duration : float ;
    (* How frequently we intend to query it, in Hertz (TODO: we could
     * approximate a better value if absent): *)
    query_freq : float [@ppp_default 1. /. 600.] }
  [@@ppp PPP_OCaml]

let get_user_conf fname =
  ensure_file_exists ~min_size:14 ~contents:"{size_limit=104857600}" fname ;
  ppp_of_file user_conf_ppp_ocaml fname

let user_conf_file conf =
  conf.C.persist_dir ^"/archiving/"
                     ^ RamenVersions.archiving_conf
                     ^"/config"

let retention_of_fq conf fq =
  try
    Hashtbl.enum conf.retentions |>
    Enum.find_map (fun (pat, ret) ->
      if Globs.matches pat (RamenName.string_of_fq fq) then Some ret
      else None)
  with Not_found -> { duration = 0. ; query_freq = 0. }


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

(* Global per-func stats that are updated by the thread reading #notifs and
 * the one reading the RC, and also saved on disk while ramen is not running:
 * (TODO) *)

type per_func_stats_ser = (RamenName.fq, func_stats) Hashtbl.t
  [@@ppp PPP_OCaml]

and func_stats =
  { running_time : float ;
    tuples : int64 ; (* We sacrifice one bit for convenience *)
    bytes : int64 ;
    cpu : float (* Cumulated seconds *) ;
    ram : int64 (* Max observed heap size *) }
  [@@ppp PPP_OCaml]

let get_per_func_stats fname =
  ppp_of_file per_func_stats_ser_ppp_ocaml fname

let func_stats_empty =
  { running_time = 0. ; tuples = 0L ; bytes = 0L ; cpu = 0. ; ram = 0L }

(* Returns the func_stat resulting of adding the RamenPs.stats to the
 * previous func_stat [a]: *)
(* FIXME: should take now as argument *)
let add_ps_stats a s default_dt =
  let etime_diff =
    match s.RamenPs.min_etime, s.max_etime with
    | Some t1, Some t2 -> t2 -. t1
    | _ -> default_dt
  in
  { running_time = a.running_time +. etime_diff ;
    tuples = Int64.add a.tuples Uint64.(to_int64 (s.out_count |? zero)) ;
    bytes = Int64.add a.bytes Uint64.(to_int64 (s.bytes_out |? zero)) ;
    cpu = a.cpu +. s.cpu ;
    ram = Int64.add a.ram Uint64.(to_int64 s.max_ram) }

(* When running we keep both the stats and the last received health report
 * as well as the last startup_time (to detect restarts): *)
let per_func_stats : (RamenName.fq, (func_stats * float * RamenPs.t)) Hashtbl.t =
  Hashtbl.create 29

let per_func_stats_lock = Mutex.create ()

let per_func_stats_sync f =
  BatMutex.synchronize ~lock:per_func_stats_lock f

let print_func_stats oc fq =
  match per_func_stats_sync
          (Hashtbl.find per_func_stats) fq with
  | exception Not_found ->
      String.print oc "no statistics"
  | tot, startup, last ->
      let default_dt = Unix.gettimeofday () -. startup in
      let s = add_ps_stats tot last default_dt in
      Printf.fprintf oc
        "running time: %s, tuples: %Ld, bytes: %Ld, cpu: %fs, ram: %Ld"
        (string_of_duration s.running_time) s.tuples s.bytes s.cpu s.ram

(* tail -f the #notifs stream and update per_func_stats: *)
let notification_reader ?while_ conf =
  !logger.info "Starting thread reading all notifications..." ;
  try forever (fun () ->
    RamenPs.read_stats ?while_ conf |>
    Hashtbl.iter (fun fq s ->
      per_func_stats_sync
        (Hashtbl.modify_opt fq (function
          | None ->
              Some (func_stats_empty, s.RamenPs.startup_time, s)
          | Some (tot, startup_time, _) ->
              if s.RamenPs.startup_time = startup_time then (
                Some (tot, startup_time, s)
              ) else (
                (* Worker has restarted. We assume it's still mostly the
                 * same operation. Maybe consider the function signature
                 * (and add it to the stats?) *)
                let default_dt = Unix.gettimeofday () -. startup_time in
                let tot = add_ps_stats tot s default_dt in
                Some (tot, s.RamenPs.startup_time, s)
              )
        )) per_func_stats
    ) ;
    Option.may (fun w -> if not (w ()) then raise Exit) while_ ;
    RamenProcesses.sleep_or_exit ?while_ (jitter 10.)
  ) ()
  with Exit -> ()

(* Return the "processing cost" per second, ie the CPU time it takes to
 * process one second worth of data, the "storage size" per second in bytes.
 * The recall cost of a second worth of output is this size times the
 * recall_cost. *)
let costs_of_stats (tot, startup_time, last) =
  let default_dt = Unix.gettimeofday () -. startup_time in
  let s = add_ps_stats tot last default_dt in
  s.cpu /. s.running_time,
  Int64.to_float s.bytes /. s.running_time

(* The costs (cpu * storage) of recomputing and reading fq output per unit of time,
 * from 0 to 10. *)
let costs_of_func fq =
  match per_func_stats_sync
           (Hashtbl.find per_func_stats) fq with
  | exception Not_found -> 0.5, 100e3 (* Beware of unknown functions! *)
  | s -> costs_of_stats s

(* Return a graph of the running config with costs: *)
let rc_complexity conf =
  let vertices, edges = RamenPs.func_graph conf in
  let vertices =
    Hashtbl.map (fun fq func ->
      func, costs_of_func fq
    ) vertices in
  vertices, edges


(*
 * Optimising storage:
 *)

(* We have constraints given by the user configuration that set a higher
 * bound on some history sizes. We need those constraints named
 * in case we have to report non-satisfiability, so we name them according
 * to their index in the file: *)

let constraint_name i =
  scramble ("user_" ^ string_of_int i)

(* Now we want to minimize the cost of querying the whole history of all
 * persistent functions in proportion to their query frequency, while still
 * remaining within the bounds allocated to storage.
 *
 * The cost to retrieve length L at frequency H of any function output is
 * either:
 *
 * - the IO cost of reading it: L * H * storage cost
 * - or the CPU (+ RAM?) cost of recomputing it: L * H * cpu cost
 *
 * ...depending on whether the output is archived or not.
 * Notice that here L is coming from the persistent function that's a child of
 * that one (or that very one).
 *
 * We know each cost individually, and how to relate storage and computing
 * cost thanks to user configuration.
 * We want to make the sum of all those costs as small as possible.
 *
 * Notice that we want the total storage to be below the provided limits but
 * not necessarily as small as possible (although the smaller the storage the
 * faster it is to read).
 *
 * The parameters are the history length of each function, perc_i, as a
 * percentage of available storage space between 0 (no archival whatsoever)
 * to 100 (archive only that function). The connection between the actual
 * length (in days) and that share of storage is constant and known for
 * each function.
 *
 * The constraints are thus:
 *
 * - Total sum of perc_i <= 100 (we may not use exactly 100% but certainly
 *   the solution will come close);
 *
 * - The archive size of a function is:
 *   its size per second * perc_i * size_limit/100;
 *   while its archive duration is:
 *   its archive size / its size per second, or:
 *   perc_i * (size_limit/(100 * size per second))
 *
 * - The query cost of function F for a duration L is:
 *   - if its archive length if longer than L:
 *     its read cost * L
 *   - otherwise:
 *     the query cost of each of its parent for duration L +
 *     its own cpu cost * L.
 *
 * - Note that for functions with no parent, the cpu cost is infinite, as
 *   there are actually no way to recompute it. If a function with no parent
 *   is queried directly there is no way around archiving.
 *
 * - The cost of the solution it the sum of all query costs for each function
 *   with a retention, that we want to minimize.
 *
 * TODO: Make it costly to radically change his mind!
 *)

let list_print oc =
  List.print ~first:"" ~last:"" ~sep:" " oc

let hashkeys_print p =
  Hashtbl.print ~first:"" ~last:"" ~sep:" " ~kvsep:"" p (fun _ _ -> ())

let const pref fq =
  pref ^ scramble (RamenName.string_of_fq fq)

let perc = const "perc_"
let cost i fq = (const "cost_" fq) ^"_"^ string_of_int i

(* For each function, declare the boolean perc_f, that must be between 0
 * and 100: *)
let emit_all_vars durations oc vertices =
  Hashtbl.iter (fun fq (_func, (cpu_cost, sto_cost)) ->
    Printf.fprintf oc
      "; Storage share of %s (%a, query cost: %f, storage cost: %f)\n\
       (declare-const %s Int)\n\
       (assert (>= %s 0))\n\
       (assert (<= %s 100)) ; should not be required but helps\n"
      (RamenName.string_of_fq fq) print_func_stats fq cpu_cost sto_cost
      (perc fq) (perc fq) (perc fq) ;
    List.iteri (fun i _ ->
      Printf.fprintf oc
        "(declare-const %s Int)\n"
        (cost i fq)) durations
  ) vertices

let emit_sum_of_percentages oc vertices =
  Printf.fprintf oc "(+ 0 %a)"
    (hashkeys_print (fun oc fq -> String.print oc (perc fq))) vertices

let parents_of inv_edges fq =
  Hashtbl.find_default inv_edges fq []

let secs_per_day = 86400.
let invalid_cost = "99999999999"

let emit_query_costs user_conf durations vertices oc inv_edges =
  String.print oc "; Durations: " ;
  List.iteri (fun i d ->
    Printf.fprintf oc "%s%d:%fs"
      (if i > 0 then ", " else "") i d
  ) durations ;
  String.print oc "\n" ;
  (* Now for each of these durations, instruct the solver what the query cost
   * will be: *)
  Hashtbl.iter (fun fq (_, (cpu_cost, sto_size)) ->
    let parents = parents_of inv_edges fq in
    Printf.fprintf oc "; Query cost of %s (parents: %a)\n"
      (RamenName.string_of_fq fq)
      (list_print (fun oc p ->
        String.print oc (RamenName.string_of_fq p))) parents ;
    List.iteri (fun i d ->
      let recall_cost =
        ceil_to_int (sto_size *. d *. user_conf.recall_cost) in
      let compute_cost =
        if parents = [] then invalid_cost else
        Printf.sprintf2 "(+ %d (+ 0 %a))"
          (ceil_to_int (cpu_cost *. d))
          (* cost of all parents for that duration: *)
          (list_print (fun oc parent ->
             Printf.fprintf oc "%s" (cost i parent))) parents
      in
      !logger.info "Cost to recall %a output for duration %.0fs: %d (or cpu=%d)"
        RamenName.fq_print fq d recall_cost
        (ceil_to_int (cpu_cost *. d)) ;
      Printf.fprintf oc
        "(assert (= %s\n\
            (ite (>= %s %d)\n\
                 %d\n\
                 %s)))\n"
      (cost i fq)
      (perc fq)
        (* Percentage of size_limit required to hold duration [d] of
         * archives: *)
        (ceil_to_int (d *. sto_size *. 100. /.
         float_of_int user_conf.size_limit))
      recall_cost
      compute_cost
    ) durations
  ) vertices

let emit_no_invalid_cost user_conf durations oc vertices =
  Hashtbl.iter (fun fq _ ->
    let retention = retention_of_fq user_conf fq in
    if retention.duration > 0. then (
      (* Which index is that? *)
      let i = List.index_of retention.duration durations |> Option.get in
      Printf.fprintf oc "(assert (< %s %s))\n"
        (cost i fq) invalid_cost)
  ) vertices

let emit_total_query_costs user_conf durations oc vertices =
  Printf.fprintf oc "(+ 0 %a)"
    (hashkeys_print (fun oc fq ->
      let retention = retention_of_fq user_conf fq in
      if retention.duration > 0. then
        (* Which index is that? *)
        let i = List.index_of retention.duration durations |> Option.get in
        (* The cost is a whole day of queries: *)
        let queries_per_days =
          ceil_to_int (retention.query_freq *. secs_per_day) in
        !logger.info
          "Must be able to query %a for a duration %s, at %d queries per day"
          RamenName.fq_print fq
          (string_of_duration retention.duration)
          queries_per_days ;
        Printf.fprintf oc "(* %s %d)"
          (cost i fq) queries_per_days))
      vertices

let emit_smt2 conf (vertices, inv_edges) oc ~optimize =
  let user_conf = get_user_conf (user_conf_file conf) in
  (* To begin with, what retention durations are we interested about? *)
  let durations =
    Hashtbl.enum user_conf.retentions /@
    (fun (_fq, ret) -> ret.duration) |>
    List.of_enum |>
    List.sort_uniq Float.compare in
  Printf.fprintf oc
    "%a\
     ; What we aim to know: the percentage of available storage to be used\n\
     ; for each function:\n\
     %a\n\
     ; Of course the sum of those shares cannot exceed 100:
     (assert (<= %a 100))\n\
     ; Query costs of each function:\n\
     %a\n\
     ; No actually used cost must be < invalid_cost\n\
     %a\n\
     ; Minimize the cost of querying each function with retention:\n\
     (minimize %a)\n\
     %t"
    preamble optimize
    (emit_all_vars durations) vertices
    emit_sum_of_percentages vertices
    (emit_query_costs user_conf durations vertices) inv_edges
    (emit_no_invalid_cost user_conf durations) vertices
    (emit_total_query_costs user_conf durations) vertices
    post_scriptum

let allocate_storage conf =
  let vertices, edges = rc_complexity conf in
  (* Create a map from vertices to parents: *)
  let inv_edges = Hashtbl.create (Hashtbl.length edges) in
  Hashtbl.iter (fun par children ->
    List.iter (fun child ->
      Hashtbl.modify_def [] child (List.cons par) inv_edges
    ) children
  ) edges ;
  let open RamenSmtParser in
  let fname = "/tmp/archiving.smt2"
  and emit = emit_smt2 conf (vertices, inv_edges)
  and parse_result sym vars sort term =
    try Scanf.sscanf sym "perc_%s%!" (fun s ->
      let s = unscramble s in
      match vars, sort, term with
      | [], NonParametricSort (Identifier "Int"),
        ConstantTerm perc ->
          let perc = int_of_constant perc in
          if perc <> 0 then
            !logger.info "%S: %d%%" s perc
      | _ ->
          !logger.warning "  of some sort...?")
    with Scanf.Scan_failure _ -> ()
  and unsat _syms _output = ()
  in
  run_smt2 ~fname ~emit ~parse_result ~unsat
