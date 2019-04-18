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
module RC = C.Running
module FS = C.FuncStats
module F = C.Func
module P = C.Program
module N = RamenName
module O = RamenOperation
module OutRef = RamenOutRef
module Files = RamenFiles
module Processes = RamenProcesses

let conf_dir conf =
  N.path_cat [ conf.C.persist_dir ; N.path "archivist" ;
               N.path RamenVersions.archivist_conf ]

(*
 * User configuration provides what functions we want to be able to retrieve
 * the output in the future (either directly via reading the archived output
 * or indirectly via recomputing from parents output) and what total storage
 * space we have at disposal.
 *)

type user_conf =
  { (* Global size limit, in byte (although the SMT uses coarser grained
       sizes): *)
    size_limit : int ;
    (* The cost to retrieve one byte of archived data, expressed in the
     * unit of CPU time (ie. the time it takes to retrieve that byte if
     * you value the IO time as much as the CPU time): *)
    recall_cost : float [@ppp_default 1e-6] ;
    (* Individual nodes we want to keep some history, none by default.
     * TODO: replaces or override the persist flag + retention length
     * that should go with it): *)
    retentions : (Globs.t, F.retention) Hashtbl.t
      [@ppp_default Hashtbl.create 0] }
  [@@ppp PPP_OCaml]

let user_conf_file conf =
  N.path_cat [ conf_dir conf ; N.path "config" ]

let retention_of_user_conf user_conf (fq : N.fq) =
  Hashtbl.enum user_conf.retentions |>
  Enum.find_map (fun (pat, ret) ->
    if Globs.matches pat (fq :> string) then Some ret
    else None)

let retention_of_source src_retention (fq : N.fq) =
  Hashtbl.find src_retention fq

(* FIXME: retention conf should take into account the site. *)
let retention_of_fq src_retention user_conf (fq : N.fq) =
  try retention_of_user_conf user_conf fq
  with Not_found ->
    (try retention_of_source src_retention fq
    with Not_found ->
      { duration = 0. ; period = 0. })

(*
 * Then the first stage is to gather statistics about all running workers.
 * We do this by continuously listening to the health reports and maintaining
 * a "stats" file with the best idea of the size of the output of each worker
 * and its resource consumption.
 *
 * This could be done with a dedicated worker but for now we just tail on
 * #notifs "manually".
 *)

let get_user_conf fname per_func_stats =
  let default = "{size_limit=1073741824}" in
  let user_conf = Files.ppp_of_file ~default user_conf_ppp_ocaml fname in
  (* In case no retention was provided, keep the roots for 1 hour: *)
  if Hashtbl.length user_conf.retentions = 0 then (
    let save_short = F.{ duration = 3600. ; period = Default.query_period }
    and no_save = F.{ duration = 0. ; period = 0. } in
    (* Empty configuration: save the roots for 10 minutes. *)
    if Hashtbl.length per_func_stats = 0 then
      (* No worker, then do not save anything: *)
      Hashtbl.add user_conf.retentions (Globs.compile "*") no_save
    else
      Hashtbl.iter (fun (_, (fq : N.fq)) s ->
        if s.FS.parents = [] then
          let pat = Globs.(escape (fq :> string)) in
          Hashtbl.add user_conf.retentions pat save_short
      ) per_func_stats) ;
  assert (Hashtbl.length user_conf.retentions > 0) ;
  user_conf

let get_retention_from_src programs =
  let src_retention = Hashtbl.create 11 in
  Hashtbl.iter (fun _prog_name (_rce, get_rc) ->
    match get_rc () with
    | exception _ -> ()
    | prog ->
        List.iter (fun func ->
          Option.may (fun r ->
            let fq = N.fq_of_program func.F.program_name func.F.name in
            Hashtbl.add src_retention fq r
          ) func.F.retention
        ) prog.P.funcs
  ) programs ;
  src_retention

(* Build a FS.t from a unique stats received for the first time: *)
let func_stats_of_stat s =
  let bytes =
    match s.RamenPs.avg_full_bytes, s.out_count with
    | None, _ | _, None -> Uint64.zero
    | Some b, Some c -> Uint64.(b * c)
  in
  FS.{
    startup_time = s.RamenPs.startup_time ;
    min_etime = s.RamenPs.min_etime ;
    max_etime = s.RamenPs.max_etime ;
    tuples = Uint64.(to_int64 (s.out_count |? zero)) ;
    bytes = Uint64.(to_int64 bytes) ;
    cpu = s.RamenPs.cpu ;
    ram = Uint64.(to_int64 s.max_ram) ;
    parents = [] ;
    archives = [] ;
    is_running = true }

(* Adds two FS.t together, favoring a *)
let add_ps_stats a b =
  FS.{
    startup_time = a.startup_time ;
    min_etime = option_map2 min a.min_etime b.min_etime ;
    max_etime = option_map2 max a.max_etime b.max_etime ;
    tuples = Int64.add a.tuples b.tuples ;
    bytes = Int64.add a.bytes b.bytes ;
    cpu = a.cpu +. b.cpu ;
    ram = Int64.add a.ram b.ram ;
    parents = a.parents ;
    archives = a.archives ;
    is_running = a.is_running }

(* Those stats are saved on disk: *)

type per_func_stats_ser = (N.fq, FS.t) Hashtbl.t
  [@@ppp PPP_OCaml]

let stat_file ?site conf =
  let site = site |? conf.C.site in
  N.path_cat
    [ conf_dir conf ; N.path "stats" ;
      N.path (
        if N.is_empty site then "local" else (site :> string)) ]

let load_stats =
  let ppp_of_file =
    Files.ppp_of_file ~default:"{}" per_func_stats_ser_ppp_ocaml in
  fun ?site conf ->
    let fname = stat_file ?site conf in
    RamenAdvLock.with_r_lock fname (fun _fd -> ppp_of_file fname)

let save_stats conf stats =
  let fname = stat_file conf in
  RamenAdvLock.with_w_lock fname (fun fd ->
    Files.ppp_to_fd ~pretty:true per_func_stats_ser_ppp_ocaml fd stats)

let get_global_stats_no_refresh conf =
  let sites = RamenServices.all_sites conf in
  let h = Hashtbl.create (Set.cardinal sites) in
  Set.iter (fun site ->
    match load_stats ~site conf with
    | exception e ->
        !logger.warning "Cannot read stats for site %a: %s"
          N.site_print site
          (Printexc.to_string e)
    | s ->
        Hashtbl.iter (fun fq stats ->
          Hashtbl.add h (site, fq) stats
        ) s
  ) sites ;
  h

(* Then we also need the RC as we also need to know the workers relationships
 * in order to estimate how expensive it is to rely on parents as opposed to
 * archival.
 *
 * So this function enriches the per_func_stats hash with parent-children
 * relationships and also compute and sets the various costs we need. *)

let sites_matching_identifier conf all_sites = function
  | O.AllSites ->
      all_sites
  | O.ThisSite ->
      Set.singleton conf.C.site
  | O.TheseSites p ->
      Set.filter (fun (s : N.site) ->
        Globs.matches p (s :> string)
      ) all_sites

let update_parents conf programs s all_sites program_name func =
  s.FS.parents <-
    List.fold_left (fun parents (psite_id, pprog, pfunc) ->
      let pprog =
        F.program_of_parent_prog program_name pprog in
      match Hashtbl.find programs pprog with
      | exception Not_found ->
          !logger.warning "Unknown parent %a of %a"
            N.program_print pprog
            N.fq_print (F.fq_name func) ;
          parents
      | prce, _ ->
          let psites =
            (* Parent sites are the intersection of the site identifier of the
             * FROM clause and the RC specification for that program: *)
            sites_matching_identifier conf all_sites psite_id |>
            Set.filter (fun (psite : N.site) ->
              Globs.matches prce.RC.on_site (psite :> string)) in
          Set.fold (fun psite parents ->
            (psite, N.fq_of_program pprog pfunc) :: parents
          ) psites parents
    ) [] func.F.parents

let update_archives conf s func =
  (* We are going to scan the current archive, which is always in RingBuf
   * format. arc_dir_of_bname would return the same directory for an Orc
   * file anyway: *)
  let bname = C.archive_buf_name ~file_type:OutRef.RingBuf conf func in
  let lst =
    RingBufLib.(arc_dir_of_bname bname |> arc_files_of) //@
    (fun (_seq_mi, _seq_ma, t1, t2, _typ, _f) ->
      if Float.(is_nan t1 || is_nan t2) then None else Some (t1, t2)) |>
    List.of_enum in
  (* We might also have a current archive: *)
  let lst =
    match RingBuf.load bname with
    | exception _ -> lst (* nope *)
    | rb ->
        finally (fun () -> RingBuf.unload rb) (fun () ->
          let st = RingBuf.stats rb in
          if st.t_min <> 0. || st.t_max <> 0. then
            (st.t_min, st.t_max) :: lst
          else  lst) () in
  let lst =
    List.sort (fun (ta,_) (tb,_) -> Float.compare ta tb) lst in
  (* Compress that list: when a gap in between two file is smaller than
   * one tenth of the duration of those two files then assume there is no
   * gap: *)
  let rec loop prev rest =
    match prev, rest with
    | (t11, t12)::prev', (t21, t22)::rest' ->
        assert (t12 >= t11 && t22 >= t21) ;
        let gap = t21 -. t12 in
        if gap < 0.1 *. abs_float (t22 -. t11) then
          loop ((t11, t22) :: prev') rest'
        else
          loop ((t21, t22) :: prev) rest'
    | [], t::rest' ->
        loop [t] rest'
    | prev, [] ->
        List.rev prev in
  s.FS.archives <- loop [] lst

let enrich_local_stats conf programs per_func_stats =
  let all_sites = RamenServices.all_sites conf in
  Hashtbl.iter (fun program_name (rce, get_rc) ->
    if Globs.matches rce.RC.on_site (conf.C.site :> string) then
      match get_rc () with
      | exception _ -> ()
      | prog ->
          List.iter (fun func ->
            let fq = N.fq_of_program func.F.program_name func.F.name in
            match Hashtbl.find per_func_stats fq with
            | exception Not_found -> ()
            | s ->
                s.FS.is_running <- rce.RC.status = MustRun ;
                update_parents conf programs s all_sites program_name func ;
                update_archives conf s func
          ) prog.P.funcs
  ) programs

(* tail -f the #notifs stream and update per_func_stats: *)
let update_local_worker_stats ?while_ conf programs =
  (* When running we keep both the stats and the last received health report
   * as well as the last startup_time (to detect restarts). We start by
   * loading the file as the current stats. We will shift it into the total
   * if we receive a new startup_time: *)
  let per_func_stats :
    (N.fq, (FS.t option * FS.t)) Hashtbl.t =
    load_stats ~site:conf.C.site conf |>
    Hashtbl.map (fun _fq s -> None, s)
  in
  let max_etime =
    Hashtbl.fold (fun _fq (_, s) t ->
      option_map2 max t s.FS.max_etime
    ) per_func_stats None in
  RamenPs.read_stats ?while_ ?since:max_etime conf |>
  Hashtbl.iter (fun (fq, is_top_half) s ->
    if not is_top_half then (
      Hashtbl.modify_opt fq (function
      | None ->
          Some (None, func_stats_of_stat s)
      | Some (tot, cur) ->
          if Distance.float s.RamenPs.startup_time cur.startup_time < 1.
          then (
            (* Just replace cur: *)
            Some (tot, func_stats_of_stat s)
          ) else (
            (* Worker has restarted. We assume it's still mostly the
             * same operation. Maybe consider the function signature
             * (and add it to the stats?) *)
            let print_date =
              print_as_date ?rel:None ?right_justified:None in
            !logger.warning
              "Merging stats of a new instance of worker %a that started \
               at %a with the previous run that started at %a"
              N.fq_print fq
              print_date s.RamenPs.startup_time
              print_date cur.startup_time ;
            let tot =
              match tot with
              | None ->
                  (* If we had no tot yet, now we have one: *)
                  cur
              | Some tot ->
                  (* Accumulate tot + cur and let s be the new cur: *)
                  add_ps_stats tot cur in
            Some (Some tot, func_stats_of_stat s)
          )
    ) per_func_stats)
  ) ;
  let stats =
    Hashtbl.map (fun _fq -> function
      | Some tot, s -> add_ps_stats tot s
      | None, s -> s
    ) per_func_stats in
  enrich_local_stats conf programs stats ;
  save_stats conf stats

(*
 * Optimising storage:
 *
 * All the information in the stats file can then be used to compute the
 * disk shares per running functions ; also to be stored in a file as a mapping
 * from FQ to number of bytes allowed on disk. This will then be read and used
 * by the GC.
 *)

(* We have constraints given by the user configuration that set a higher
 * bound on some history sizes. We need those constraints named
 * in case we have to report non-satisfiability, so we name them according
 * to their index in the file.
 * The only other constraint we have is that no function must cost more
 * than the invalid cost, which arrives only when there is no other
 * solution than to "recompute" the original values, ie there is not
 * enough storage space. *)

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
 * TODO: Make it costly to radically change one's mind!
 *)

let list_print oc =
  List.print ~first:"" ~last:"" ~sep:" " oc

let hashkeys_print p =
  Hashtbl.print ~first:"" ~last:"" ~sep:" " ~kvsep:"" p (fun _ _ -> ())

let const pref ((site_id : N.site), (fq : N.fq)) =
  pref ^ scramble ((site_id :> string) ^":"^ (fq :> string))

let perc = const "perc_"
let cost i site_fq =
  (const "cost_" site_fq) ^"_"^ string_of_int i

(* The "compute cost" per second is the CPU time it takes to
 * process one second worth of data. *)
let compute_cost s =
  match s.FS.min_etime, s.FS.max_etime with
  | Some mi, Some ma ->
      let running_time = ma -. mi in
      s.FS.cpu /. running_time
  | _ ->
      Default.compute_cost

(* The "recall size" is the total size per second in bytes.
 * The recall cost of a second worth of output will be this size
 * times the user_conf.recall_cost. *)
let recall_size s =
  match s.FS.min_etime, s.FS.max_etime with
  | Some mi, Some ma ->
      let running_time = ma -. mi in
      Int64.to_float s.FS.bytes /. running_time
  | _ ->
      Default.recall_size

(* For each function, declare the boolean perc_f, that must be between 0
 * and 100. From now on, [per_func_stats] is keyed by site*fq and has all
 * functions running everywhere. Obviously this is intended for the master
 * instance, to compute the disk allocations. *)
let emit_all_vars durations oc per_func_stats =
  Hashtbl.iter (fun ((site : N.site), (fq : N.fq) as site_fq) s ->
    Printf.fprintf oc
      "; Storage share of %a:%a (compute cost: %f, recall size: %f)\n\
       (declare-const %s Int)\n\
       (assert (>= %s 0))\n\
       (assert (<= %s 100)) ; should not be required but helps\n"
      N.site_print site
      N.fq_print fq
      (compute_cost s) (recall_size s)
      (perc site_fq) (perc site_fq) (perc site_fq) ;
    List.iteri (fun i _ ->
      Printf.fprintf oc "(declare-const %s Int)\n"
        (cost i site_fq)
    ) durations
  ) per_func_stats

let emit_sum_of_percentages oc per_func_stats =
  Printf.fprintf oc "(+ 0 %a)"
    (hashkeys_print (fun oc fq -> String.print oc (perc fq))) per_func_stats

let secs_per_day = 86400.
let invalid_cost = "99999999999999999999"

let emit_query_costs user_conf durations oc per_func_stats =
  String.print oc "; Durations: " ;
  List.iteri (fun i d ->
    Printf.fprintf oc "%s%d:%s"
      (if i > 0 then ", " else "") i (string_of_duration d)
  ) durations ;
  String.print oc "\n" ;
  (* Now for each of these durations, instruct the solver what the query
   * cost will be: *)
  Hashtbl.iter (fun (site, fq as site_fq) s ->
    Printf.fprintf oc "; Query cost of %a:%a (parents: %a)\n"
      N.site_print site
      N.fq_print fq
      (list_print (fun oc (site, fq) ->
        Printf.fprintf oc "%a:%a"
          N.site_print site
          N.fq_print fq)) s.FS.parents ;
    List.iteri (fun i d ->
      let recall_size = recall_size s in
      let recall_cost =
        if recall_size < 0. then invalid_cost else
        string_of_int (
          ceil_to_int (user_conf.recall_cost *. recall_size *. d)) in
      if String.length recall_cost > String.length invalid_cost then
        (* Poor man arbitrary size integers :> *)
        !logger.error "Archivist: Got a cost of %s which is greater than invalid!"
          recall_cost ;
      let compute_cost = compute_cost s in
      let compute_cost =
        if s.parents = [] || compute_cost < 0. then invalid_cost else
        Printf.sprintf2 "(+ %d (+ 0 %a))"
          (ceil_to_int (compute_cost *. d))
          (* cost of all parents for that duration: *)
          (list_print (fun oc parent ->
             Printf.fprintf oc "%s" (cost i parent))) s.parents
      in
      Printf.fprintf oc
        "(assert (= %s\n\
            \t(ite (>= %s %d)\n\
                 \t\t%s\n\
                 \t\t%s)))\n"
        (cost i site_fq)
        (perc site_fq)
          (* Percentage of size_limit required to hold duration [d] of
           * archives: *)
          (ceil_to_int (d *. recall_size *. 100. /.
             float_of_int user_conf.size_limit))
        recall_cost
        compute_cost
    ) durations
  ) per_func_stats

let emit_no_invalid_cost
      src_retention user_conf durations oc per_func_stats =
  Hashtbl.iter (fun (_, (fq : N.fq) as site_fq) _ ->
    let retention = retention_of_fq src_retention user_conf fq in
    if retention.duration > 0. then (
      (* Which index is that? *)
      let i = List.index_of retention.duration durations |> Option.get in
      Printf.fprintf oc "(assert (< %s %s))\n"
        (cost i site_fq) invalid_cost)
  ) per_func_stats

let emit_total_query_costs
      src_retention user_conf durations oc per_func_stats =
  Printf.fprintf oc "(+ 0 %a)"
    (hashkeys_print (fun oc ((site : N.site), (fq : N.fq) as site_fq) ->
      let retention = retention_of_fq src_retention user_conf fq in
      if retention.duration > 0. then
        (* Which index is that? *)
        let i = List.index_of retention.duration durations |> Option.get in
        (* The cost is a whole day of queries: *)
        let queries_per_days =
          ceil_to_int (secs_per_day /. (max 1. retention.period)) in
        !logger.info
          "Must be able to query %a:%a for a duration %s, at %d queries per day"
          N.site_print site
          N.fq_print fq
          (string_of_duration retention.duration)
          queries_per_days ;
        Printf.fprintf oc "(* %s %d)"
          (cost i site_fq) queries_per_days))
      per_func_stats

let emit_smt2 src_retention user_conf per_func_stats oc ~optimize =
  (* We want to consider only the running functions, but also need their
   * non running parents! *)
  let per_func_stats =
    let h =
      Hashtbl.filter (fun s -> s.FS.is_running) per_func_stats in
    let rec loop () =
      let mia =
        Hashtbl.fold (fun _site_fq s mia ->
          List.fold_left (fun mia psite_fq ->
            if Hashtbl.mem h psite_fq then mia else psite_fq::mia
          ) mia s.FS.parents
        ) h [] in
      if mia <> [] then (
        List.iter (fun (psite, pfq as psite_fq) ->
          match Hashtbl.find per_func_stats psite_fq with
          | exception Not_found ->
              Printf.sprintf2 "No statistics about %a:%a"
                N.site_print psite
                N.fq_print pfq |>
              failwith (* No better idea *)
          | s ->
              Hashtbl.add h psite_fq s
        ) mia ;
        loop ()
      ) in
    loop () ;
    h in
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
     ; Of course the sum of those shares cannot exceed 100:\n\
     (assert (<= %a 100))\n\
     ; Query costs of each _running_ function:\n\
     %a\n\
     ; No actually used cost must be greater than invalid_cost\n\
     %a\n\
     ; Minimize the cost of querying each _running_ function with retention:\n\
     (minimize %a)\n\
     %t"
    preamble optimize
    (emit_all_vars durations) per_func_stats
    emit_sum_of_percentages per_func_stats
    (emit_query_costs user_conf durations) per_func_stats
    (emit_no_invalid_cost src_retention user_conf durations) per_func_stats
    (emit_total_query_costs src_retention user_conf durations) per_func_stats
    post_scriptum

(*
 * The results are stored in the file "allocs", which is a map from names
 * to size.
 *)

type per_func_allocs_ser = ((N.site * N.fq), int) Hashtbl.t
  [@@ppp PPP_OCaml]

let save_allocs conf allocs =
  let fname = N.path_cat [ conf_dir conf ; N.path "allocs" ] in
  Files.ppp_to_file ~pretty:true fname per_func_allocs_ser_ppp_ocaml allocs

let load_allocs =
  let ppp_of_file =
    Files.ppp_of_file ~default:"{}" per_func_allocs_ser_ppp_ocaml in
  fun conf ->
    N.path_cat [ conf_dir conf ; N.path "allocs" ] |>
    ppp_of_file

let update_storage_allocation conf programs =
  let open RamenSmtParser in
  let solution = Hashtbl.create 17 in
  let per_func_stats = get_global_stats_no_refresh conf in
  let user_conf = get_user_conf (user_conf_file conf) per_func_stats
  and src_retention = get_retention_from_src programs in
  let fname = N.path_cat [ conf_dir conf ; N.path "allocations.smt2" ]
  and emit = emit_smt2 src_retention user_conf per_func_stats
  and parse_result sym vars sort term =
    try Scanf.sscanf sym "perc_%s%!" (fun s ->
      let site_fq = unscramble s in
      let site, fq = String.split ~by:":" site_fq in
      let site, fq = N.site site, N.fq fq in
      match vars, sort, term with
      | [], NonParametricSort (Identifier "Int"),
        ConstantTerm perc ->
          let perc = int_of_constant perc in
          if perc <> 0 then !logger.info "%a:%a: %d%%"
            N.site_print site
            N.fq_print fq
            perc ;
          Hashtbl.replace solution (site, fq) perc
      | _ ->
          !logger.warning "  of some sort...?")
    with Scanf.Scan_failure _ -> ()
  (* TODO! *)
  and unsat _syms _output =
    (* Ideally, name the asserts from the user config and report errors
     * as for typing. For now, just complain loudly giving generic advices. *)
    !logger.error
      "Cannot satisfy archival constraints. Try reducing history length or \
       allocate more disk space."
  in
  run_smt2 ~fname ~emit ~parse_result ~unsat ;
  (* It might happen that the solution is empty if no mentioned functions had
   * stats or if the solver decided that all functions were equally important
   * and give them each 0% of storage.
   * In that case, we'd rather have each function share the available space: *)
  let site_fq_print oc (site, fq) =
    Printf.fprintf oc "%a:%a" N.site_print site N.fq_print fq in
  let tot_perc = Hashtbl.fold (fun _ p s -> s + p) solution 0 in
  !logger.debug "solution: %a, tot-perc = %d"
    (Hashtbl.print site_fq_print Int.print) solution
    tot_perc ;
  let solution, tot_perc =
    if tot_perc = 0 then (
      let retention_globs = Hashtbl.keys user_conf.retentions |>
                            List.of_enum in
      let mentionned_in_user_conf (_site, (fq : N.fq)) =
        List.exists (fun p ->
          Globs.matches p (fq :> string)
        ) retention_globs in
      let persist_nodes =
        Hashtbl.keys per_func_stats //
        mentionned_in_user_conf |>
        Set.of_enum in
      !logger.warning "No solution due to lack of stats(?), \
                       sharing available space between %a"
        (Set.print site_fq_print) persist_nodes ;
      (* Next scale will scale this properly *)
      let solution =
        Hashtbl.filter_map (fun site_fq s ->
          if s.FS.is_running && mentionned_in_user_conf site_fq
          then Some 1 else None
        ) per_func_stats in
      (* Have to recompute [tot_perc]: *)
      let tot_perc =
        Hashtbl.fold (fun _ p s -> s + p) solution 0 in
      solution, tot_perc
    ) else solution, tot_perc in
  (* Scale it up to 100% and convert to bytes: *)
  let scale =
    if tot_perc > 0 then
      float_of_int user_conf.size_limit /. float_of_int tot_perc
    else 1. in
  let allocs =
    Hashtbl.map (fun _ p ->
      round_to_int (float_of_int p *. scale)
    ) solution in
  (* Warn of any large change: *)
  let prev_allocs = load_allocs conf in
  Hashtbl.iter (fun site_fq p ->
    let prev_p = Hashtbl.find_default allocs site_fq 0 in
    if reldiff (float_of_int p) (float_of_int prev_p) > 0.5 then
      !logger.warning "Allocation for %a is jumping from %d%% to %d%%"
        site_fq_print site_fq prev_p p
  ) prev_allocs ;
  save_allocs conf allocs

(*
 * The allocs are used to update the workers out_ref to make them archive.
 * If not refreshed periodically (see
 * [RamenConst.Defaults.archivist_export_duration]) any worker will stop
 * exporting at some point.
 *)

let update_local_workers_export
    ?(export_duration=Default.archivist_export_duration) conf programs =
  load_allocs conf |>
  Hashtbl.iter (fun (site, fq) max_size ->
    if site = conf.C.site then
      match RC.find_func programs fq with
      | exception e ->
          !logger.debug "Cannot find function %a: %s, skipping"
            N.fq_print fq
            (Printexc.to_string e)
      | _mre, _prog, func ->
          let file_type =
            if RamenExperiments.archive_in_orc.variant = 0 then
              OutRef.RingBuf
            else
              OutRef.Orc {
                with_index = false ;
                batch_size = Default.orc_rows_per_batch ;
                num_batches = Default.orc_batches_per_file } in
          if max_size > 0 then
            Processes.start_export
              ~file_type ~duration:export_duration conf func |> ignore)

(*
 * CLI
 *)

let run_once conf ?while_ ?export_duration
             stats allocs reconf =
  let programs = RC.with_rlock conf identity in (* Best effort *)
  (* Start by gathering (more) workers stats: *)
  if stats then (
    !logger.info "Updating workers stats" ;
    update_local_worker_stats ?while_ conf programs) ;
  (* Then use those to answer the big questions about queries, the storage
   * and everything: *)
  if allocs then (
    !logger.info "Updating storage allocations" ;
    update_storage_allocation conf programs) ;
  (* Now update the archiving configuration of running workers: *)
  if reconf then (
    !logger.info "Updating workers export configuration" ;
    update_local_workers_export ?export_duration conf programs)

let run_loop conf ?while_ sleep_time stats allocs reconf =
  (* Export instructions are only valid for twice as long as the archivist
   * loop. Consequence to keep in mind: if the archivist is not running then
   * exports will soon stop! *)
  let export_duration = sleep_time *. 2. in
  let watchdog =
    let timeout = sleep_time *. 2. in
    RamenWatchdog.make ~timeout "Archiver" Processes.quit in
  RamenWatchdog.enable watchdog ;
  while (while_ |? always) () do
    log_and_ignore_exceptions ~what:"archivist run_once"
      (run_once conf ?while_ ~export_duration stats allocs) reconf ;
    RamenWatchdog.reset watchdog ;
    Processes.sleep_or_exit ?while_ (jitter sleep_time)
  done

(* Helpers: get the stats (maybe refreshed) *)

let maybe_refresh_local_stats ?while_ conf =
  let programs = RC.with_rlock conf identity in
  match Files.age (stat_file conf) with
  | exception Unix.Unix_error (Unix.ENOENT, _, _) ->
      update_local_worker_stats ?while_ conf programs
  | stat_file_age ->
      if stat_file_age > max_archivist_stat_file_age ||
         stat_file_age > RC.age conf
      then
        update_local_worker_stats ?while_ conf programs

(* Returns a hash keyed by FQ: *)
let get_local_stats ?while_ conf =
  maybe_refresh_local_stats ?while_ conf ;
  load_stats conf

(* Returns a hash keyed by (site * FQ): *)
let get_global_stats ?while_ conf =
  maybe_refresh_local_stats ?while_ conf ;
  get_global_stats_no_refresh conf
