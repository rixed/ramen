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
open RamenSyncHelpers
module C = RamenConf
module FS = C.FuncStats
module VSI = RamenSync.Value.SourceInfo
module VTC = RamenSync.Value.TargetConfig
module VOS = RamenSync.Value.OutputSpecs
module N = RamenName
module O = RamenOperation
module OutRef = RamenOutRef
module Files = RamenFiles
module Paths = RamenPaths
module Processes = RamenProcesses
module Retention = RamenRetention
module ZMQClient = RamenSyncZMQClient

(*
 * User configuration provides what functions we want to be able to retrieve
 * the output in the future (either directly via reading the archived output
 * or indirectly via recomputing from parents output) and what total storage
 * space we have at disposal.
 *)

type user_conf =
  { (* Global size limit, in byte (although the SMT uses coarser grained
       sizes): *)
    size_limit : int64 ;
    (* The cost to retrieve one byte of archived data, expressed in the
     * unit of CPU time (ie. the time it takes to retrieve that byte if
     * you value the IO time as much as the CPU time): *)
    recall_cost : float ;
    (* Individual nodes we want to keep some history, none by default.
     * TODO: replaces or override the persist flag + retention length
     * that should go with it): *)
    retentions : (Globs.t, Retention.t) Hashtbl.t }

let conf_dir conf =
  N.path_cat [ conf.C.persist_dir ; N.path "archivist" ;
               N.path RamenVersions.archivist_conf ]

let user_conf_file conf =
  N.path_cat [ conf_dir conf ; N.path "config" ]

let retention_of_user_conf user_conf (site, fq) =
  let sfq = Printf.sprintf2 "%a:%a" N.site_print site N.fq_print fq in
  Hashtbl.enum user_conf.retentions |>
  Enum.find_map (fun (pat, ret) ->
    if Globs.matches pat sfq then Some ret
    else None)

let retention_of_source src_retention (fq : N.fq) =
  Hashtbl.find src_retention fq

(* Returns both the retention setting and a numeric identifier *)
let retention_of_site_fq src_retention user_conf (_, fq as site_fq) =
  try retention_of_user_conf user_conf site_fq
  with Not_found ->
    (try retention_of_source src_retention fq
    with Not_found ->
      { duration = 0. ; period = 0. })

(* The stats we need about each worker to compute: *)

type arc_stats =
  { min_etime : float option ;
    max_etime : float option ;
    bytes : int64 ; (* Average size per tuple times number of output *)
    cpu : float ;
    parents : (N.site * N.fq) list }

let arc_stats_of_runtime_stats parents s =
  { min_etime = s.RamenSync.Value.RuntimeStats.min_etime ;
    max_etime = s.max_etime ;
    bytes =
      (if Uint64.(compare s.tot_full_bytes_samples zero) > 0 then
        let avg = Uint64.to_float s.tot_full_bytes /.
                  Uint64.to_float s.tot_full_bytes_samples in
        Int64.of_float (avg *. Uint64.to_float s.tot_out_tuples)
      else 0L) ;
    cpu = s.tot_cpu ; parents }

(*
 * Then the first stage is to gather statistics about all running workers.
 * We do this by continuously listening to the health reports and maintaining
 * a "stats" file with the best idea of the size of the output of each worker
 * and its resource consumption.
 *
 * This could be done with a dedicated worker but for now we just tail on
 * #notifs "manually".
 *)

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
    num_arc_files = 0 ;
    num_arc_bytes = 0L }

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
    num_arc_files = a.num_arc_files + b.num_arc_files ;
    num_arc_bytes = Int64.add a.num_arc_bytes b.num_arc_bytes }

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
  let parents = O.parents_of_operation func.VSI.operation in
  s.FS.parents <-
    List.fold_left (fun parents (psite_id, pprog, pfunc) ->
      let pprog =
        O.program_of_parent_prog program_name pprog in
      match Hashtbl.find programs pprog with
      | exception Not_found ->
          !logger.warning "Unknown parent %a of %a"
            N.program_print pprog
            N.func_print func.VSI.name ;
          parents
      | prce, _ ->
          let psites =
            (* Parent sites are the intersection of the site identifier of the
             * FROM clause and the RC specification for that program: *)
            let on_site = Globs.compile prce.VTC.on_site in
            sites_matching_identifier conf all_sites psite_id |>
            Set.filter (fun (psite : N.site) ->
              Globs.matches on_site (psite :> string)) in
          Set.fold (fun psite parents ->
            (psite, N.fq_of_program pprog pfunc) :: parents
          ) psites parents
    ) [] parents

let compute_archives conf prog_name func =
  (* We are going to scan the current archive, which is always in RingBuf
   * format. arc_dir_of_bname would return the same directory for an Orc
   * file anyway: *)
  let fq = VSI.fq_name prog_name func in
  let bname =
    Paths.archive_buf_name ~file_type:VOS.RingBuf conf prog_name func in
  !logger.debug "Computing archive size of function %a, from dir %a"
    N.fq_print fq N.path_print bname ;
  let lst =
    RingBufLib.(arc_dir_of_bname bname |> arc_files_of) //@
    (fun (_seq_mi, _seq_ma, t1, t2, _typ, fname) ->
      if Float.(is_nan t1 || is_nan t2) then
        None
      else (
        Some (t1, t2, false, 1, Files.size fname)
      )) |>
    List.of_enum in
  (* We might also have a current archive (if the worker is actually running): *)
  let lst =
    if Files.exists bname then
      match RingBuf.load bname with
      | exception _ -> lst (* nope *)
      | rb ->
          finally (fun () -> RingBuf.unload rb) (fun () ->
            let st = RingBuf.stats rb in
            if st.t_min <> 0. || st.t_max <> 0. then
              let sz = st.alloced_words * RingBuf.rb_word_bytes in
              (st.t_min, st.t_max, true, 1, sz) :: lst
            else lst) ()
    else lst in
  let lst =
    List.sort (fun (ta, _, _, _, _) (tb, _, _, _, _) ->
      Float.compare ta tb
    ) lst in
  (* Compress that list: when a gap in between two files is smaller than
   * one tenth of the duration of those two files then assume there is no
   * gap: *)
  let rec loop prev rest =
    match prev, rest with
    | (t11, t12, oe1, nf1, sz1)::prev', (t21, t22, oe2, nf2, sz2 as tr2)::rest' ->
        assert (t12 >= t11 && t22 >= t21) ;
        let gap = t21 -. t12 in
        if not oe1 && gap < 0.1 *. abs_float (t22 -. t11) then
          loop ((t11, t22, oe2, nf1+nf2, sz1+sz2) :: prev') rest'
        else
          loop (tr2 :: prev) rest'
    | [], t::rest' ->
        loop [t] rest'
    | prev, [] ->
        List.rev prev in
  let ranges, num_files, num_bytes =
    loop [] lst |>
    List.fold_left (fun (ranges, num_files, num_bytes) (t1, t2, oe, nf, sz) ->
      (t1, t2, oe) :: ranges,
      num_files + nf,
      Int64.(add num_bytes (of_int sz))
    ) ([], 0, 0L) in
  !logger.debug "Function %a has %Ld bytes of archive in %d files"
    N.fq_print fq num_bytes num_files ;
  List.rev ranges, num_files, num_bytes

(*
 * Optimising storage:
 *
 * All the information in the stats file can then be used to compute the
 * disk shares per running functions ; also to be stored in a file as a mapping
 * from FQ to number of bytes allowed on disk. This will then be read and used
 * by the GC.
 *)

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
 *   is queried directly there is no way around archival.
 *
 * - The cost of the solution it the sum of all query costs for each function
 *   with a retention, that we want to minimize.
 *
 * TODO: Make it costly to radically change one's mind!
 *)

let list_print oc =
  List.print ~first:"" ~last:"" ~sep:" " oc

let hashkeys_print p =
  Hashtbl.print ~first:"" ~last:"" ~sep:"\n" ~kvsep:"" p (fun _ _ -> ())

let const pref ((site_id : N.site), (fq : N.fq)) =
  pref ^ scramble ((site_id :> string) ^":"^ (fq :> string))

(* (variable name of the) percentage of total storage allocated to this
 * worker: *)
let perc = const "perc_"

(* (variable name of the) cost of archiving this worker for the duration
 * [i]: *)
let cost i site_fq =
  (const "cost_" site_fq) ^"_"^ string_of_int i

(* The "compute cost" per second is the CPU time it takes to
 * process one second worth of data. *)
let compute_cost s =
  match s.min_etime, s.max_etime with
  | Some mi, Some ma ->
      let running_time = ma -. mi in
      s.cpu /. running_time
  | _ ->
      Default.compute_cost

(* The "recall size" is the total size per second in bytes.
 * The recall cost of a second worth of output will be this size
 * times the user_conf.recall_cost. *)
let recall_size fq s =
  match s.min_etime, s.max_etime with
  | Some mi, Some ma ->
      let running_time = ma -. mi in
      Int64.to_float s.bytes /. running_time
  | _ ->
      (* If that node has no output then a default value has to be used.
       * We cannot merely forbid archival, as computation might not be
       * an option (in absence of parents). The idea is that if this function
       * ends up archiving, then it will thus run and then runtime stats will
       * eventually be collected, leading to a better decision later. *)
      !logger.info
        "Function %a has no stats, assuming default archival cost."
        N.fq_print fq ;
      Default.recall_size

(* For each function, declare the boolean perc_f, that must be between 0
 * and 100, and as many cost_f as there are defined durations.
 * From now on, [per_func_stats] is keyed by site*fq and has all functions
 * running everywhere. Obviously this is intended for the master instance to
 * compute the disk allocations. *)
let emit_all_vars durations oc per_func_stats =
  Hashtbl.iter (fun ((site : N.site), (fq : N.fq) as site_fq) s ->
    Printf.fprintf oc
      "; Storage share of %a:%a (compute cost: %f, recall size: %f)\n\
       (declare-const %s Int)\n\
       (assert (>= %s 0))\n\
       (assert (<= %s 100)) ; should not be required but helps\n"
      N.site_print site
      N.fq_print fq
      (compute_cost s) (recall_size fq s)
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
          N.fq_print fq)) s.parents ;
    List.iteri (fun i d ->
      let recall_size = recall_size fq s in
      let recall_cost =
        string_of_int (
          ceil_to_int (user_conf.recall_cost *. recall_size *. d)) in
      if String.length recall_cost > String.length invalid_cost then
        (* Poor man arbitrary size integers :> *)
        !logger.error
          "Archivist: Got a cost of %s which is greater than invalid!"
          recall_cost ;
      let compute_cost = compute_cost s in
      let compute_cost =
        if s.parents = [] || compute_cost < 0.
        then
          invalid_cost
        else
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
        (* Percentage of size_limit required to hold duration [d] of archives: *)
        (ceil_to_int (d *. recall_size *. 100. /.
                      Int64.to_float user_conf.size_limit))
        recall_cost
        compute_cost
    ) durations
  ) per_func_stats

let emit_no_invalid_cost
      src_retention user_conf durations oc per_func_stats =
  Hashtbl.iter (fun site_fq _ ->
    let retention = retention_of_site_fq src_retention user_conf site_fq in
    if retention.duration > 0. then (
      (* Which index is that? *)
      let i = List.index_of retention.duration durations |>
              option_get "retention.duration" __LOC__ in
      Printf.fprintf oc "(assert (< %s %s))\n"
        (cost i site_fq) invalid_cost)
  ) per_func_stats

let emit_total_query_costs
      src_retention user_conf durations oc per_func_stats =
  Printf.fprintf oc "(+ 0 %a)"
    (hashkeys_print (fun oc ((site : N.site), (fq : N.fq) as site_fq) ->
      let retention = retention_of_site_fq src_retention user_conf site_fq in
      if retention.duration > 0. then (
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
          (cost i site_fq) queries_per_days
      ) else
        !logger.debug "No retention for %a" N.site_fq_print site_fq))
      per_func_stats

let emit_smt2 src_retention user_conf per_func_stats oc ~optimize =
  (* To begin with, what retention durations are we interested about? *)
  let durations =
    Enum.append
      (* Durations defined in user_conf: *)
      (Hashtbl.values user_conf.retentions)
      (* Durations defined in function source: *)
      (Hashtbl.values src_retention) /@
    (fun ret -> ret.Retention.duration) |>
    List.of_enum |>
    List.sort_uniq Float.compare in
  Printf.fprintf oc
    "%a\
     ; What we aim to know: the percentage of available storage to be used\n\
     ; for each function:\n\
     %a\n\
     ; Query costs of each _running_ function:\n\
     %a\n\
     ; No actually used cost must be greater than invalid_cost\n\
     %a\n\
     ; Minimize the cost of querying each _running_ function with retention:\n\
     (minimize %a)\n\
     ; Minimize the cost of archival:\n\
     (minimize %a)\n\
     %t"
    preamble optimize
    (emit_all_vars durations) per_func_stats
    (emit_query_costs user_conf durations) per_func_stats
    (emit_no_invalid_cost src_retention user_conf durations) per_func_stats
    (emit_total_query_costs src_retention user_conf durations) per_func_stats
    emit_sum_of_percentages per_func_stats
    post_scriptum

exception Unsat

let update_storage_allocation
      conf user_conf per_func_stats src_retention ignore_unsat =
  let open RamenSmtParser in
  let solution = Hashtbl.create 17 in
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
  and unsat _syms _output =
    if not ignore_unsat then raise Unsat
  in
  run_smt2 ~fname ~emit ~parse_result ~unsat ;
  (* It might happen that the solution is empty if no mentioned functions had
   * stats or if the solver decided that all functions were equally important
   * and give them each 0% of storage.
   * In that case, we'd rather have each function share the available space: *)
  let tot_perc = Hashtbl.fold (fun _ p s -> s + p) solution 0 in
  !logger.debug "solution: %a, tot-perc = %d"
    (Hashtbl.print N.site_fq_print Int.print) solution
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
        (Set.print N.site_fq_print) persist_nodes ;
      (* Next scale will scale this properly *)
      let solution =
        Hashtbl.filter_map (fun site_fq _s ->
          if mentionned_in_user_conf site_fq then Some 1 else None
        ) per_func_stats in
      (* Have to recompute [tot_perc]: *)
      let tot_perc =
        Hashtbl.fold (fun _ p s -> s + p) solution 0 in
      solution, tot_perc
    ) else solution, tot_perc in
  (* Scale it up to 100% and convert to bytes: *)
  let scale =
    if tot_perc > 0 then
      Int64.to_float user_conf.size_limit /. float_of_int tot_perc
    else 1. in
  let allocs =
    Hashtbl.map (fun _ p ->
      round_to_int (float_of_int p *. scale)
    ) solution in
  allocs

(*
 * The allocs are used to update the workers out_ref to make them archive.
 * If not refreshed periodically (see
 * [RamenConst.Defaults.archivist_export_duration]) any worker will stop
 * exporting at some point.
 *)

let reconf_workers
    ~while_ ?(export_duration=Default.archivist_export_duration)
    conf session =
  let open RamenSync in
  let prefix = "sites/"^ (conf.C.site :> string) ^"/" in
  Client.iter session.ZMQClient.clt ~prefix (fun k hv ->
    match k, hv.Client.value with
    | Key.PerSite (site, PerWorker (fq, AllocedArcBytes)),
      Value.RamenValue T.(VI64 size)
      when site = conf.C.site && size > 0L ->
        (* Start the export: *)
        (match function_of_fq session.clt fq with
        | exception e ->
            !logger.debug "Cannot find function %a: %s, skipping"
              N.fq_print fq
              (Printexc.to_string e)
        | _prog, prog_name, func ->
            let file_type =
              if RamenExperiments.archive_in_orc.variant = 0 then
                VOS.RingBuf
              else
                VOS.Orc {
                  with_index = false ;
                  batch_size = Default.orc_rows_per_batch ;
                  num_batches = Default.orc_batches_per_file } in
            !logger.info "Make %a to archive"
              N.fq_print fq ;
            Processes.start_export
              ~while_ ~file_type ~duration:export_duration conf session
              site prog_name func)
    | _ -> ())

(*
 * CLI
 *)

let realloc conf session ~while_ =
  (* Collect all stats and retention info: *)
  !logger.debug "Recomputing storage allocations" ;
  let per_func_stats : (C.N.site * N.fq, arc_stats) Hashtbl.t =
    Hashtbl.create 100 in
  let size_limit = ref (Uint64.of_int64 1073741824L)
  and recall_cost = ref Default.archive_recall_cost
  and retentions = Hashtbl.create 11
  and src_retention = Hashtbl.create 11
  and prev_allocs = Hashtbl.create 11 in
  let open RamenSync in
  Client.iter session.ZMQClient.clt (fun k v ->
    match k, v.Client.value with
    | Key.PerSite (site, PerWorker (fq, RuntimeStats)),
      Value.RuntimeStats stats ->
        let worker_key = Key.PerSite (site, PerWorker (fq, Worker)) in
        (* Update program runtime stats: *)
        (match (Client.find session.clt worker_key).value with
        | exception Not_found ->
            !logger.debug "Ignoring stats %a for missing worker %a"
              Key.print k
              Key.print worker_key
        | Value.Worker worker ->
            if worker.Value.Worker.role = Whole then (
              let parents =
                worker.Value.Worker.parents |>
                List.map (fun ref ->
                  ref.Value.Worker.site,
                  N.fq_of_program ref.program ref.func) in
              let stats =
                arc_stats_of_runtime_stats parents stats in
              if stats.bytes = 0L then (
                let min_et = ref (
                  IO.to_string (Option.print print_as_date) stats.min_etime) in
                !logger.info
                  "Function %a hasn't output any byte for etimes %s..%a"
                  N.fq_print fq
                  !min_et
                  (Option.print (print_as_date_rel ~rel:min_et)) stats.max_etime
              ) ;
              Hashtbl.add per_func_stats (site, fq) stats
            ) else
              !logger.debug "Ignoring %a" Value.Worker.print worker
        | v ->
            invalid_sync_type worker_key v "a Worker")
    | Key.PerSite (_site, PerWorker (fq, Worker)),
      Value.Worker _worker ->
        (* Update function retention: *)
        let prog_name, _func_name = N.fq_parse fq in
        let src_path = N.src_path_of_program prog_name in
        (match program_of_src_path session.clt src_path with
        | exception e ->
            !logger.error
              "Cannot find program for worker %a: %s, assuming no retention"
              N.fq_print fq
              (Printexc.to_string e)
        | prog ->
            List.iter (fun func ->
              let fq = N.fq_of_program prog_name func.VSI.name in
              Option.may (Hashtbl.replace src_retention fq)
                         func.VSI.retention
            ) prog.VSI.funcs)
    | Key.Storage TotalSize,
      Value.RamenValue T.(VU64 v) ->
        size_limit := v
    | Key.Storage RecallCost,
      Value.RamenValue T.(VFloat v) ->
        recall_cost := v
    | Key.Storage (RetentionsOverride pat),
      Value.Retention v ->
        Hashtbl.replace retentions pat v
    | Key.PerSite (site, PerWorker (fq, AllocedArcBytes)),
      Value.RamenValue T.(VI64 bytes) ->
        Hashtbl.add prev_allocs (site, fq) bytes
    | _ ->
        ()) ;
  let user_conf =
    { size_limit = Uint64.to_int64 !size_limit ;
      recall_cost = !recall_cost ;
      retentions } in
  let allocs =
    let rec retry_with_reduced_retentions ratio =
      let user_conf, src_retention, ignore_unsat =
        if ratio < 1e-3 then (
          (* Ideally, name the asserts from the user config and report errors
           * as for typing. For now, just complain loudly giving generic advices. *)
          !logger.error
            "Cannot satisfy archival constraints. Try reducing history length or \
             allocate more disk space." ;
          user_conf, src_retention, true
        ) else (
          let reduce h =
            Hashtbl.map (fun _ r ->
              Retention.{ r with duration = r.duration *. ratio }
            ) h in
          { user_conf with retentions = reduce user_conf.retentions },
          reduce src_retention,
          false
        ) in
      (try update_storage_allocation
            conf user_conf per_func_stats src_retention ignore_unsat
      with Unsat ->
            let ratio = ratio *. 0.5 in
            !logger.warning
              "Cannot satisfy the archival constraints, retrying with only
               %g of all retentions" ratio ;
            retry_with_reduced_retentions (ratio *. 0.5)) in
    retry_with_reduced_retentions 1. in
  (* Write new allocs and warn of any large change: *)
  Hashtbl.iter (fun (site, fq as hk) bytes ->
    let k = Key.PerSite (site, PerWorker (fq, AllocedArcBytes))
    and v = Value.of_int bytes in
    (match Hashtbl.find prev_allocs hk with
    | exception Not_found ->
        !logger.info "Newly allocated storage: %d bytes for %a"
          bytes
          N.site_fq_print (site, fq) ;
        ZMQClient.send_cmd ~while_ session (NewKey (k, v, 0.)) ;
    | prev_bytes ->
        if reldiff (float_of_int bytes) (Int64.to_float prev_bytes) > 0.5
        then
          !logger.warning "Allocation for %a shifted from %Ld to %d bytes"
            N.site_fq_print (site, fq) prev_bytes bytes ;
        ZMQClient.send_cmd ~while_ session (UpdKey (k, v)) ;
        Hashtbl.remove prev_allocs hk)
  ) allocs ;
  (* Delete what's left in prev_allocs: *)
  Hashtbl.iter (fun (site, fq as site_fq) _ ->
    let k = Key.PerSite (site, PerWorker (fq, AllocedArcBytes)) in
    !logger.info "No more allocated storage for %a"
      N.site_fq_print site_fq ;
    ZMQClient.send_cmd ~while_ session (DelKey k)
  ) prev_allocs

let run conf ~while_ loop allocs reconf =
  (* We need retentions (that we get from the info files), user config,
   * runtime stats and workers (to get src_path and running flag).
   * Results are written in PerWorker AllocedArcBytes, that we must also read
   * to warn about large changes. *)
  let topics =
    [ "sources/*/info" ;
      "storage/*" ;
      "sites/*/workers/*/stats/runtime" ;
      "sites/*/workers/*/archives/alloc_size" ;
      "sites/*/workers/*/worker" ;
      OutRef.topics ] in
  (* We listen to all of those and any change will reset an alarm after which
   * the allocations are recomputed. *)
  (* Try to have the first realloc as soon as possible while still giving
   * time for first stats to arrive: *)
  let last_change = ref 0.
  and last_realloc = ref (Unix.time () -. min_duration_between_storage_alloc
                                       +. Default.report_period *. 2.)
  and last_reconf = ref 0. in
  let on_del _session k _v =
    let open RamenSync in
    match k with
    | Key.PerSite (_, PerWorker (_, RuntimeStats))
    | Key.Storage _ ->
        last_change := Unix.gettimeofday ()
    | _ ->
        () in
  let on_set session k v _uid _mtime = on_del session k v
  and on_new session k v _uid _mtime _can_write _can_del _owner _expiry =
    on_del session k v in
  start_sync conf ~while_ ~on_set ~on_new ~on_del ~topics ~recvtimeo:5.
             (fun session ->
    let do_once () =
      ZMQClient.process_in ~while_ ~single:true session ;
      let now = Unix.gettimeofday () in
      !logger.debug "now=%a, last_change=%a, last_realloc=%a, last_reconf=%a"
        print_as_date now
        print_as_date !last_change
        print_as_date !last_realloc
        print_as_date !last_reconf ;
      if allocs &&
         (
          now > !last_change +. archivist_settle_delay &&
          !last_change > !last_realloc &&
          now > !last_realloc +. min_duration_between_storage_alloc
        ) || (
          now > !last_realloc +. max_duration_between_storage_alloc
        ) || (
          (* If we run archivist in one-shot mode, just do it: *)
          now > !last_realloc && loop <= 0.
        )
      then (
        last_realloc := now ;
        let what = "Updating storage allocations" in
        !logger.info "%s" what ;
        log_and_ignore_exceptions ~what (realloc conf ~while_) session) ;
      (* Note: for now we update the outref files (thus the restriction
       * to local workers and the need to run this on all sites). In the
       * future we'd rather have the outref content on the config tree,
       * and then a single archivist will be enough. *)
      (* FIXME: rather reconf the workers whenever a change of the allocs
       * is received. *)
      if reconf &&
         now > !last_reconf +. min_duration_between_archive_reconf ||
         now > !last_reconf && loop <= 0.
      then (
        last_reconf := now ;
        let what = "Updating workers export configuration" in
        !logger.info "%s" what ;
        log_and_ignore_exceptions ~what (reconf_workers ~while_ conf) session)
    in
    if loop <= 0. then
      do_once ()
    else
      while while_ () do
        do_once ()
      done
  )
