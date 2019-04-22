(* Process supervisor which task is to start and stop workers, connect them
 * properly and make sure they keep working. *)
open Batteries
open RamenLog
open RamenHelpers
open RamenConsts
module C = RamenConf
module RC = C.Running
module FS = C.FuncStats
module F = C.Func
module P = C.Program
module E = RamenExpr
module O = RamenOperation
module N = RamenName
module OutRef = RamenOutRef
module Files = RamenFiles
module Services = RamenServices

(*$inject open Batteries *)

(* Global quit flag, set (to some ExitCodes) when the term signal
 * is received or some other bad condition happen: *)
let quit = ref None

let until_quit f =
  let rec loop () =
    if !quit = None && f () then loop () in
  loop ()

let dummy_nop () =
  !logger.warning "Running in dummy mode" ;
  until_quit (fun () -> Unix.sleep 3 ; true)

let rec sleep_or_exit ?(while_=always) t =
  if t > 0. && while_ () then (
    let delay = min 1. t in
    Unix.sleepf delay ;
    sleep_or_exit ~while_ (t -. delay)
  )

(* Seed to pass to workers to init their random generator: *)
let rand_seed = ref None

(*
 * Helpers:
 *)

(* To be called before synchronize_running *)
let repair_and_warn what rb =
  if RingBuf.repair rb then
    !logger.warning "Ringbuf for %s was damaged." what

(* Prepare ringbuffer for notifications *)
let prepare_notifs conf =
  let rb_name = C.notify_ringbuf conf in
  RingBuf.create ~wrap:false rb_name ;
  let notify_rb = RingBuf.load rb_name in
  repair_and_warn "notifications" notify_rb ;
  notify_rb

(* Prepare ringbuffer for reports. *)
let prepare_reports conf =
  let rb_name = C.report_ringbuf conf in
  RingBuf.create ~wrap:false rb_name ;
  let report_rb = RingBuf.load rb_name in
  repair_and_warn "instrumentation" report_rb ;
  report_rb

(* Many messages that are exceptional in supervisor are quite expected in tests: *)
let info_or_test conf =
  if conf.C.test then !logger.debug else !logger.info

(* Install signal handlers.
 * Beware: do not use BatPrintf in there, as there is a mutex in there that
 * might be held already when the OCaml runtime decides to run this in the
 * same thread: *)
let prepare_signal_handlers conf =
  ignore conf ;
  set_signals Sys.[sigterm; sigint] (Signal_handle (fun s ->
    (*info_or_test conf "Received signal %s" (name_of_signal s) ;*)
    ignore s ;
    quit :=
      Some (if s = Sys.sigterm then ExitCodes.terminated
                               else ExitCodes.interrupted))) ;
  (* Dump stats on sigusr1: *)
  set_signals Sys.[sigusr1] (Signal_handle (fun s ->
    (* This log also useful to rotate the logfile. *)
    ignore s ;
    (*!logger.info "Received signal %s" (name_of_signal s) ;*)
    Binocle.display_console ()))

(*
 * Machinery to spawn other programs.
 *)

let fd_of_int : int -> Unix.file_descr = Obj.magic

let close_fd i =
  Unix.close (fd_of_int i)

let run_background ?cwd ?(and_stop=false) cmd args env =
  let open Unix in
  let quoted oc s = Printf.fprintf oc "%S" s in
  !logger.debug "Running %a as: /usr/bin/env %a %a %a"
    N.path_print cmd
    (Array.print ~first:"" ~last:"" ~sep:" " quoted) env
    N.path_print_quoted cmd
    (Array.print ~first:"" ~last:"" ~sep:" " quoted) args ;
  flush_all () ;
  match fork () with
  | 0 ->
    (try
      if and_stop then RingBufLib.kill_myself Sys.sigstop ;
      Option.may (fun (d : N.path) -> chdir (d :> string)) cwd ;
      let null = openfile "/dev/null" [O_RDONLY] 0 in
      dup2 null stdin ;
      close null ;
      for i = 3 to 255 do
        try close_fd i with Unix.Unix_error (Unix.EBADF, _, _) -> ()
      done ;
      execve (cmd :> string) args env
    with e ->
      Printf.eprintf "Cannot execve: %s\n%!" (Printexc.to_string e) ;
      sys_exit 127)
  | pid -> pid

(*$inject
  open Unix
  module N = RamenName

  let check_status pid expected =
    let rep_pid, status = waitpid [ WNOHANG; WUNTRACED ] pid in
    let status =
      if rep_pid = 0 then None else Some status in
    let printer = function
      | None -> "no status"
      | Some st -> RamenHelpers.string_of_process_status st in
    assert_equal ~printer expected status

  let run ?and_stop args =
    let pid = run_background ?and_stop (N.path args.(0)) args [||] in
    sleepf 0.1 ;
    pid
*)

(*$R run
  let pid = run [| "tests/test_false" |] in
  check_status pid (Some (WEXITED 1))
 *)

(*$R run
  let pid = run ~and_stop:true [| "/bin/sleep" ; "1" |] in
  check_status pid (Some (WSTOPPED Sys.sigstop)) ;
  kill pid Sys.sigcont ;
  check_status pid None ;
  sleepf 1.1 ;
  check_status pid (Some (WEXITED 0))
 *)

let run_worker ?and_stop (bin : N.path) args env =
  (* Better have the workers CWD where the binary is, so that any file name
   * mentioned in the program is relative to the program. *)
  let cwd = Files.dirname bin in
  let cmd = Files.basename bin in
  run_background ~cwd ?and_stop cmd args env

(*
 * Process Supervisor: Start and stop according to a mere file listing which
 * programs to run.
 *
 * Also monitor quit.
 *)

(* FIXME: parent_num is not good enough because a parent num might change
 * when another parent is added/removed. *)
type top_half_spec =
  { tunneld : Services.entry ; parent_num : int }

(* What is run from a worker: *)
type worker_part =
  | Whole
  (* Top half: only the filtering part of that function is run, once for
   * every local parent; output is forwarded to another site. *)
  | TopHalf of top_half_spec list

let print_worker_part oc = function
  | Whole -> String.print oc "whole worker"
  | TopHalf _ -> String.print oc "top half"

(* We use the same key for both [must_run] and [running] to make the
 * comparison easier, although strictly speaking the signature is useless
 * in [must_run]: *)
type key =
  { program_name : N.program ;
    func_name : N.func ;
    func_signature : string ;
    params : RamenTuple.params ;
    part : worker_part }

(* What we store in the [must_run] hash: *)
type must_run_entry =
  { key : key ;
    rce : RC.entry ;
    func : F.t ;
    parents : (N.site * P.t * F.t) list }

let print_must_run_entry oc mre =
  Printf.fprintf oc "%a/%a"
    N.program_print mre.key.program_name
    N.func_print mre.key.func_name

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

let print_parents oc parents =
  let print_parent oc (ps, pp, pf) =
    Printf.fprintf oc "%a:%a/%a"
      N.site_print ps
      N.program_print pp
      N.func_print pf in
  Set.print print_parent oc parents

(* Description of a running worker that is stored in the [running] hash.
 * Not persisted on disk.
 * Some of this is updated as the configuration change. *)
type running_process =
  { key : key ;
    params : RamenParams.t ; (* The ones in RCE only! *)
    bin : N.path ;
    func : C.Func.t ;
    parents : (N.site * P.t * F.t) list ;
    children : F.t list ;
    log_level : log_level ;
    report_period : float ;
    mutable pid : int option ;
    last_killed : float ref (* 0 for never *) ;
    mutable continued : bool ;
    (* purely for reporting: *)
    mutable last_exit : float ;
    mutable last_exit_status : string ;
    mutable succ_failures : int ;
    mutable quarantine_until : float }

let print_running_process oc proc =
  Printf.fprintf oc "%s/%s (%a) (parents=%a)"
    (proc.func.F.program_name :> string)
    (proc.func.F.name :> string)
    print_worker_part proc.key.part
    (List.print F.print_parent) proc.func.parents

let make_running_process conf must_run mre =
  let log_level =
    if mre.rce.RC.debug then Debug else conf.C.log_level in
  (* All children running locally, including top-halves: *)
  let children =
    Hashtbl.fold (fun _ (mre' : must_run_entry) children ->
      List.fold_left (fun children (_h, _pprog, pfunc) ->
        if pfunc == mre.func then
          mre'.func :: children
        else
          children
      ) children mre.parents
    ) must_run [] in
  { key = mre.key ;
    params = mre.rce.RC.params ;
    bin = mre.rce.RC.bin ;
    func = mre.func ;
    parents = mre.parents ;
    children ;
    log_level ;
    report_period = mre.rce.RC.report_period ;
    pid = None ; last_killed = ref 0. ; continued = false ;
    last_exit = 0. ; last_exit_status = "" ; succ_failures = 0 ;
    quarantine_until = 0. }

(* Returns the name of func input ringbuf for the given parent (if func is
 * merging, each parent uses a distinct one) and the file_spec. *)
let input_ringbuf_fname conf parent child =
  (* In case of merge, ringbufs are numbered as the node parents: *)
  if child.F.merge_inputs then
    match List.findi (fun _ (_, pprog, pname) ->
            let pprog_name =
              F.program_of_parent_prog child.F.program_name pprog in
            pprog_name = parent.F.program_name && pname = parent.name
          ) child.parents with
    | exception Not_found ->
        !logger.error "Operation %S is not a child of %S"
          (child.name :> string)
          (parent.name :> string) ;
        invalid_arg "input_ringbuf_fname"
    | i, _ ->
        C.in_ringbuf_name_merging conf child i
  else C.in_ringbuf_name_single conf child

let make_fieldmask parent child =
  let out_typ =
    O.out_type_of_operation ~with_private:false parent.F.operation in
  RamenFieldMaskLib.fieldmask_of_operation ~out_typ child.F.operation

let check_is_subtype t1 t2 =
  (* For t1 to be a subtype of t2, all fields of t1 must be present and
   * public in t2. And since there is no more extension from scalar types at
   * this stage, those fields must have the exact same types. *)
  List.iter (fun f1 ->
    let f2_typ =
      RamenFieldMaskLib.find_type_of_path t2 f1.RamenFieldMaskLib.path in
    if f1.typ <> f2_typ then
      Printf.sprintf2 "Fields %a have different types"
        N.field_print (E.id_of_path f1.path) |>
      failwith
  ) t1

(*
 * Replay
 *
 * This module knows how to:
 * - find the required sources to reconstruct any function output in a
 *   given time range;
 * - connect all those sources for a dedicated channel, down to the function
 *   which output we are interested in (the target);
 * - tear down all these connections.
 *
 * Temporarily also:
 * - load and save the replays configuration from disk.
 *)

module Replay =
struct
  (*$< Replay *)

  module Range = struct
    (*$< Range *)
    type t = (float * float) list

    let empty = []

    let is_empty = (=) []

    let make t1 t2 =
      let t1 = min t1 t2 and t2 = max t1 t2 in
      if t1 < t2 then [ t1, t2 ] else []

    (*$T make
      is_empty (make 1. 1.)
      not (is_empty (make 1. 2.))
    *)

    let rec merge l1 l2 =
      match l1, l2 with
      | [], l | l, [] -> l
      | (a1, b1)::r1, (a2, b2)::r2 ->
          let am = min a1 a2 and bm = max b1 b2 in
          if bm -. am <= (b1 -. a1) +. (b2 -. a2) then
            (am, bm) :: merge r1 r2
          else if a1 <= a2 then
            (a1, b1) :: merge r1 l2
          else
            (a2, b2) :: merge l1 r2

    (*$= merge & ~printer:(IO.to_string print)
      [ 1.,2. ; 3.,4. ] (merge [ 1.,2. ] [ 3.,4. ])
      [ 1.,2. ; 3.,4. ] (merge [ 3.,4. ] [ 1.,2. ])
      [ 1.,4. ] (merge [ 1.,3. ] [ 2.,4. ])
      [ 1.,4. ] (merge [ 1.,2. ] [ 2.,4. ])
      [ 1.,4. ] (merge [ 1.,4. ] [ 2.,3. ])
      [] (merge [] [])
      [ 1.,2. ] (merge [ 1.,2. ] [])
      [ 1.,2. ] (merge [] [ 1.,2. ])
    *)

    let print oc =
      let rel = ref "" in
      let p = print_as_date ~right_justified:false ~rel in
      List.print (Tuple2.print p p) oc

    (*$>*)
  end

  (* Helper function: *)

  let cartesian_product f lst =
    (* We have a list of list of things. Call [f] with a all possible lists of
     * thing, one by one. (technically, the lists are reversed) *)
    let rec loop selection = function
      | [] -> f selection
      | lst::rest ->
          (* For each x of list, try all possible continuations: *)
          List.iter (fun x ->
            loop (x::selection) rest
          ) lst in
    if lst <> [] then loop [] lst

  (*$inject
    let test_cp lst =
      let ls l = List.fast_sort compare l in
      let s = ref Set.empty in
      cartesian_product (fun l -> s := Set.add (ls l) !s) lst ;
      Set.to_list !s |> ls
  *)

  (*$= test_cp & ~printer:(IO.to_string (List.print (List.print Int.print)))
    [ [1;4;5] ; [1;4;6] ; \
      [2;4;5] ; [2;4;6] ; \
      [3;4;5] ; [3;4;6] ] (test_cp [ [1;2;3]; [4]; [5;6] ])

   [] (test_cp [])

   [] (test_cp [ []; [] ])

   [] (test_cp [ [1;2;3]; []; [5;6] ])
  *)

  open C.Replays

  exception NotInStats of (N.site * N.fq)
  exception NoData

  (* Find a way to get the data out of fq for that time range.
   * Note that there could be several ways to obtain those. For instance,
   * we could ask for a 1year retention of some node that queried very
   * infrequently, and that is also a parent of some other node for which
   * we asked for a much shorter retention but that is queried more often:
   * in that case the archiver could well decide to archive both the
   * parent and, as an optimisation, this child. Now which best (or only)
   * path to query that child depends on since/until.
   * But the shortest path form fq to its archiving parents that have the
   * required data is always the best. So start from fq and progress
   * through parents until we have found all required sources with the whole
   * archived content, recording the all the functions on the way up there
   * in a set.
   * Return all the possible ways to get some data in that range, with the
   * sources, their distance, and the list of covered ranges. Caller
   * will pick what it likes best (or compose a mix).
   *
   * Note regarding distributed mode:
   * We keep assuming for now that fq is unique and runs locally.
   * But its sources might not. In particular, for each parent we have to
   * take into account the optional host identifier and follow there. During
   * recursion we will have to keep track of the current local site. *)
  let find_sources stats local_site fq since until =
    let find_fq_stats site_fq =
      try Hashtbl.find stats site_fq
      with Not_found -> raise (NotInStats site_fq)
    in
    let cost (sources, links) =
      Set.cardinal sources + Set.cardinal links in
    let merge_ways ways =
      (* We have a list of, for each parent, one time range and one replay
       * configuration. Compute the union of all: *)
      List.fold_left (fun (range, (sources, links))
                          (range', (sources', links')) ->
        Range.merge range range',
        (Set.union sources sources', Set.union links links')
      ) (Range.empty, (Set.empty, Set.empty)) ways in
    let rec find_parent_ways since until links fqs =
      (* When asked for several parent fqs, all the possible ways to retrieve
       * any range of data is the cartesian product. But we are not going to
       * return the full product, only the best alternative for any distinct
       * time range. *)
      let per_parent_ways =
        List.map (find_ways since until links) fqs in
      (* For each parent, we got an alist from time ranges to a set of
       * sources/links (from which we assess a cost).
       * Now the result is an alist from time ranges to ways unioning all
       * possible ways. *)
      let h = Hashtbl.create 11 in
      cartesian_product (fun ways ->
        (* ranges is a list (one item per parent) of all possible ways.
         * Compute the resulting range.
         * If it's better that what we already have (or if
         * we have nothing for that range yet) then compute the actual
         * set of sources and links and store it in [ways]: *)
        let range, v = merge_ways ways in
        Hashtbl.modify_opt range (function
          | None -> Some v
          | Some v' as prev ->
              if cost v < cost v' then Some v else prev
        ) h
      ) per_parent_ways ;
      Hashtbl.enum h |> List.of_enum
    (* Find all ways up to some data sources required for [fq] in the time
     * range [since]..[until]. Returns a list of pairs composed of the Range.t
     * and a pair of the set of fqs and the set of links, a link being a pair
     * of parent and child fq. So for instance, this is a possible result:
     * [
     *   [ (123.456, 125.789) ], (* the range *)
     *   (
     *     { source1; source2; ... }, (* The set of data sources *)
     *     { (source1, fq2); (fq2, fq3); ... } (* The set of links *)
     *   )
     * ]
     *
     * Note regarding distributed mode:
     * Everywhere we have a fq in that return type, what we actually have is a
     * site * fq.
     *)
    and find_ways since until links (local_site, fq as site_fq) =
      (* When given a single function, the possible ways are the one from
       * the archives of this node, plus all possible ways from its parents: *)
      let s = find_fq_stats site_fq in
      let local_range =
        List.fold_left (fun range (t1, t2) ->
          if t1 > until || t2 < since then range else
          Range.merge range (Range.make (max t1 since) (min t2 until))
        ) Range.empty s.FS.archives in
      !logger.debug "From %a:%a, range from archives = %a"
        N.site_print local_site
        N.fq_print fq
        Range.print local_range ;
      (* Take what we can from here and the rest from the parents: *)
      (* Note: we are going to ask all the parents to export the replay
       * channel to this function. Although maybe some of those we are
       * going to spawn a replayer. That's normal, the replayer is reusing
       * the out_ref of the function it substitutes to. Which will never
       * receive this channel (unless loops but then we actually want
       * the worker to process the looping tuples!) *)
      let links =
        List.fold_left (fun links pfq ->
          Set.add (pfq, (local_site, fq)) links
        ) links s.parents in
      let from_parents =
        find_parent_ways since until links s.parents in
      if Range.is_empty local_range then from_parents else
        let local_way = local_range, (Set.singleton (local_site, fq), links) in
        local_way :: from_parents
    and pick_best_way ways =
      let span_of_range =
        List.fold_left (fun s (t1, t2) -> s +. (t2 -. t1)) 0. in
      (* For now, consider only the covered range.
       * TODO: Maybe favor also shorter paths? *)
      List.fold_left (fun (best_span, _ as prev) (range, _ as way) ->
        let span = span_of_range range in
        assert (span >= 0.) ;
        if span > best_span then span, Some way else prev
      ) (0., None) ways |>
      snd |>
      option_get "no data in best path"
    in
    (* We assume all functions are running for now but this is not required
     * for the replayer itself. Later we could add the required workers in the
     * RC for just that channel. *)
    match find_ways since until Set.empty (local_site, fq) with
    | exception NotInStats _ ->
        Printf.sprintf2 "Cannot find %a in the stats"
          N.fq_print fq |>
        failwith
    | [] ->
        raise NoData
    | ways ->
        !logger.debug "Found those ways: %a"
          (List.print (Tuple2.print
            Range.print
            (Tuple2.print (Set.print site_fq_print)
                          (Set.print link_print)))) ways ;
        pick_best_way ways

  (* Create the replay structure but does not start it (nor create the
   * final_rb).
   * Notice that the target is always local, which free us from the
   * site identifier that would otherwise be necessary. If one wants
   * to replay remote (or a combination of local and remote) workers
   * then one can easily replay from an immediate expression, expressing
   * this complex selection in ramen language directly.
   * So, here [func] is supposed to mean the local instance of it only. *)
  let create conf stats ?(timeout=Default.replay_timeout) func since until =
    let timeout = Unix.gettimeofday () +. timeout in
    let fq = F.fq_name func in
    let out_type =
      O.out_type_of_operation ~with_private:true func.F.operation in
    let ser = RingBufLib.ser_tuple_typ_of_tuple_typ out_type |>
              List.map fst in
    (* Ask to export only the fields we want. From now on we'd better
     * not fail and retry as we would hammer the out_ref with temp
     * ringbufs.
     * Maybe we should use the channel in the rb name, and pick a
     * channel large enough to be a hash of FQ, output fields and
     * dates? But that mean 256bits integers (2xU128?). *)
    (* TODO: for now, we ask for all fields. Ask only for field_names,
     * but beware of with_event_type! *)
    let target_fieldmask = RamenFieldMaskLib.fieldmask_all ~out_typ:ser in
    let range, (sources, links) =
      find_sources stats conf.C.site fq since until in
    (* Pick a channel. They are cheap, we do not care if we fail
     * in the next step: *)
    let channel = RamenChannel.make () in
    let final_rb =
      let tmpdir = getenv ~def:"/tmp" "TMPDIR" in
      Printf.sprintf2 "%s/replay_%a_%d.rb"
        tmpdir RamenChannel.print channel (Unix.getpid ()) |>
      N.path in
    !logger.debug
      "Creating replay channel %a, with sources=%a, links=%a, \
       covered time slices=%a, final rb=%a"
      RamenChannel.print channel
      (Set.print site_fq_print) sources
      (Set.print link_print) links
      Range.print range
      N.path_print final_rb ;
    { channel ; target = (conf.C.site, fq) ; target_fieldmask ;
      since ; until ; final_rb ; sources ; links ; timeout }

  let teardown_links conf programs t =
    let rem_out_from site_fq =
      let site, fq = site_fq in
      if site = conf.C.site then
        match RC.find_func programs fq with
        | exception Not_found -> (* Not a big deal really *)
            !logger.warning
              "While tearing down channel %a, cannot find function %a"
              RamenChannel.print t.channel N.fq_print fq
        | _rce, _prog, func ->
            let out_ref = C.out_ringbuf_names_ref conf func in
            OutRef.remove_channel out_ref t.channel
    in
    (* Start by removing the links from the graph, then the last one
     * from the target: *)
    Set.iter (fun (psite_fq, _) -> rem_out_from psite_fq) t.links ;
    rem_out_from t.target

  let settup_links conf programs t =
    (* Connect the target first, then the graph: *)
    let connect_to_rb func fname fieldmask =
      let out_ref = C.out_ringbuf_names_ref conf func in
      OutRef.add out_ref ~timeout:t.timeout ~channel:t.channel
                 fname fieldmask
    in
    let target_site, target_fq = t.target in
    let what = Printf.sprintf2 "Setting up links for channel %a"
                 RamenChannel.print t.channel in
    log_and_ignore_exceptions ~what (fun () ->
      if conf.C.site = target_site then (
        let _rce, _prog, func = RC.find_func_or_fail programs target_fq in
        connect_to_rb func t.final_rb t.target_fieldmask)) () ;
    Set.iter (fun ((psite, pfq), (_, cfq)) ->
      if conf.C.site = psite then
        log_and_ignore_exceptions ~what (fun () ->
          let _rce, _prog, cfunc = RC.find_func_or_fail programs cfq in
          let _rce, _prog, pfunc = RC.find_func_or_fail programs pfq in
          let fname = input_ringbuf_fname conf pfunc cfunc
          and fieldmask = make_fieldmask pfunc cfunc in
          connect_to_rb pfunc fname fieldmask) ()
    ) t.links

  (* Spawn a source.
   * Note: there is no top-half for sources. We assume the required
   * top-halves are already running as their full remote children are.
   * Pass to each replayer the name of the function, the out_ref files to
   * obey, the channel id to tag tuples with, and since/until dates.
   * Returns the pid. *)
  let spawn_source_replay conf programs sfq t replayer_id =
    let rce, _prog, func = RC.find_func_or_fail programs sfq in
    let fq = F.fq_name func in
    let args = [| Worker_argv0.replay ; (fq :> string) |]
    and out_ringbuf_ref = C.out_ringbuf_names_ref conf func
    and rb_archive =
      (* We pass the name of the current ringbuf archive if
       * there is one, that will be read after the archive
       * directory. Notice that if we cannot read the current
       * ORC file before it's archived, nothing prevent a
       * worker to write both a non-wrapping, non-archive
       * worthy ringbuf in addition to an ORC file. *)
      C.archive_buf_name ~file_type:OutRef.RingBuf conf func
    in
    let env =
      [| "name="^ (func.F.name :> string) ;
         "fq_name="^ (fq :> string) ;
         "log_level="^ string_of_log_level conf.C.log_level ;
         "output_ringbufs_ref="^ (out_ringbuf_ref :> string) ;
         "rb_archive="^ (rb_archive :> string) ;
         "since="^ string_of_float t.since ;
         "until="^ string_of_float t.until ;
         "channel_id="^ RamenChannel.to_string t.channel ;
         "replayer_id="^ string_of_int replayer_id |] in
    let pid = run_worker rce.RC.bin args env in
    !logger.debug "Replay for %a is running under pid %d"
      N.fq_print fq pid ;
    pid

  (* Spawn all (local) replayers and return the set of pids and the
   * set of replayer ids. *)
  let spawn_all_local_sources conf programs t =
    let what = Printf.sprintf2 "Spawning local replayers for channel %a"
                 RamenChannel.print t.channel in
    Set.fold (fun (ssite, sfq) pids ->
      if conf.C.site = ssite then
        let replayer_id = Random.int RingBufLib.max_replayer_id in
        match spawn_source_replay conf programs sfq t replayer_id with
        | exception e ->
            print_exception ~what e ;
            pids
        | pid ->
            Set.add (pid, ref 0.) pids
      else pids
    ) t.sources Set.empty

  (*$>*)
end


open Binocle

let stats_worker_crashes =
  RamenWorkerStats.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.worker_crashes
      "Number of workers that have crashed (or exited with non 0 status).")

let stats_worker_deadloopings =
  RamenWorkerStats.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.worker_deadloopings
      "Number of time a worker has been found to deadloop.")

let stats_worker_count =
  IntGauge.make Metric.Names.worker_count
    "Number of workers configured to run."

let stats_worker_running =
  IntGauge.make Metric.Names.worker_running
    "Number of workers actually running."

let stats_ringbuf_repairs =
  RamenWorkerStats.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.ringbuf_repairs
      "Number of times a worker ringbuf had to be repaired.")

let stats_outref_repairs =
  RamenWorkerStats.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.outref_repairs
      "Number of times a worker outref had to be repaired.")

let stats_worker_sigkills =
  RamenWorkerStats.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.worker_sigkills
      "Number of times a worker had to be sigkilled instead of sigtermed.")

let stats_replayer_crashes =
  RamenWorkerStats.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.replayer_crashes
      "Number of replayers that have crashed (or exited with non 0 status).")

let stats_replayer_sigkills =
  RamenWorkerStats.ensure_inited (fun save_dir ->
    IntCounter.make ~save_dir:(save_dir :> string)
      Metric.Names.replayer_sigkills
      "Number of times a replayer had to be sigkilled instead of sigtermed.")

let stats_graph_build_time =
  RamenWorkerStats.ensure_inited (fun save_dir ->
    Histogram.make ~save_dir:(save_dir :> string)
      Metric.Names.worker_graph_build_time
      "Workers graph build time" Histogram.powers_of_two)


(* When a worker seems to crashloop, assume it's because of a bad file and
 * delete them! *)
let rescue_worker conf func params =
  (* Maybe the state file is poisoned? At this stage it's probably safer
   * to move it away: *)
  !logger.info "Worker %s is deadlooping. Deleting its state file and \
                input ringbuffers."
    ((F.fq_name func) :> string) ;
  let state_file = C.worker_state conf func params in
  Files.move_away state_file ;
  (* At this stage there should be no writers since this worker is stopped. *)
  let input_ringbufs = C.in_ringbuf_names conf func in
  List.iter Files.move_away input_ringbufs

(* Then this function is cleaning the running hash: *)
let process_workers_terminations conf running =
  let open Unix in
  let now = gettimeofday () in
  Hashtbl.iter (fun _ proc ->
    Option.may (fun pid ->
      let what =
        Printf.sprintf2 "Operation %a (pid %d)"
          print_running_process proc pid in
      (match restart_on_EINTR (waitpid [ WNOHANG ; WUNTRACED ]) pid with
      | exception exn ->
          !logger.error "%s: waitpid: %s" what (Printexc.to_string exn)
      | 0, _ -> () (* Nothing to report *)
      | _, (WSIGNALED s | WSTOPPED s) when s = Sys.sigstop ->
          !logger.debug "%s got stopped" what
      | _, status ->
          let status_str = string_of_process_status status in
          let is_err =
            status <> WEXITED ExitCodes.terminated in
          (if is_err then !logger.error else info_or_test conf)
            "%s %s." what status_str ;
          proc.last_exit <- now ;
          proc.last_exit_status <- status_str ;
          if is_err then (
            proc.succ_failures <- proc.succ_failures + 1 ;
            IntCounter.inc (stats_worker_crashes conf.C.persist_dir) ;
            if proc.succ_failures = 5 then (
              IntCounter.inc (stats_worker_deadloopings conf.C.persist_dir) ;
              rescue_worker conf proc.func proc.params)
          ) else (
            proc.succ_failures <- 0
          ) ;
          (* Wait before attempting to restart a failing worker: *)
          let max_delay = float_of_int proc.succ_failures in
          proc.quarantine_until <-
            now +. Random.float (min 90. max_delay) ;
          proc.pid <- None)
    ) proc.pid
  ) running

let process_replayers_terminations conf replayers =
  let open Unix in
  let get_programs = memoize (fun () -> RC.with_rlock conf identity) in
  Hashtbl.filter_map_inplace (fun channel (replay, pids) ->
    let pids =
      Set.filter (fun (pid, _last_killed) ->
        let what =
          Printf.sprintf2 "Replayer for channel %a (pid %d)"
            RamenChannel.print channel pid in
        (match restart_on_EINTR (waitpid [ WNOHANG ; WUNTRACED ]) pid with
        | exception exn ->
            !logger.error "%s: waitpid: %s" what (Printexc.to_string exn) ;
            (* assume the replayers is safe *)
            true
        | 0, _ ->
            true (* Nothing to report *)
        | _, (WSIGNALED s | WSTOPPED s) when s = Sys.sigstop ->
            !logger.debug "%s got stopped" what ;
            true
        | _, status ->
            let status_str = string_of_process_status status in
            let is_err =
              status <> WEXITED ExitCodes.terminated in
            (if is_err then !logger.error else info_or_test conf)
              "%s %s." what status_str ;
            if is_err then
              IntCounter.inc (stats_replayer_crashes conf.C.persist_dir) ;
            false)
      ) pids in
    if Set.is_empty pids then (
      let programs = get_programs () in
      Replay.teardown_links conf programs replay ;
      None
    ) else Some (replay, pids)
  ) replayers

(* Returns the buffer name: *)
let start_export ?(file_type=OutRef.RingBuf)
                 ?(duration=Default.export_duration) conf func =
  let bname = C.archive_buf_name ~file_type conf func in
  if file_type = OutRef.RingBuf then
    RingBuf.create ~wrap:false bname ;
  (* Add that name to the function out-ref *)
  let out_ref = C.out_ringbuf_names_ref conf func in
  let out_typ =
    O.out_type_of_operation ~with_private:false func.F.operation in
  (* Negative durations, yielding a timestamp of 0, means no timeout ;
   * while duration = 0 means to actually not export anything (and we have
   * a cli-test that relies on the spec not being present in the out_ref
   * in that case): *)
  if duration <> 0. then (
    let timeout =
      if duration < 0. then 0. else Unix.gettimeofday () +. duration in
    let fieldmask = RamenFieldMaskLib.fieldmask_all ~out_typ in
    OutRef.add out_ref ~timeout ~file_type bname fieldmask
  ) ;
  bname

let really_start conf proc =
  (* Create the input ringbufs.
   * We now start the workers one by one in no
   * particular order. The input and out-ref ringbufs are created when the
   * worker start, and the out-ref is filled with the running (or should
   * be running) children. Therefore, if a children fails to run the
   * parents might block. Also, if a child has not been started yet its
   * inbound ringbuf will not exist yet, again implying this worker will
   * block.
   * Each time a new worker is started or stopped the parents outrefs
   * are updated. *)
  let fq_str = (F.fq_name proc.func :> string) in
  let is_top_half =
    match proc.key.part with Whole -> false | TopHalf _ -> true in
  !logger.debug "Creating in buffers..." ;
  let input_ringbufs =
    if is_top_half then
      [ C.in_ringbuf_name_single conf proc.func ]
    else
      C.in_ringbuf_names conf proc.func in
  List.iter (fun rb_name ->
    RingBuf.create rb_name ;
    let rb = RingBuf.load rb_name in
    finally (fun () -> RingBuf.unload rb)
      (fun () ->
        repair_and_warn fq_str rb ;
        IntCounter.inc (stats_ringbuf_repairs conf.C.persist_dir)) ()
  ) input_ringbufs ;
  (* And the pre-filled out_ref: *)
  let out_ringbuf_ref =
    if is_top_half then None else (
      !logger.debug "Updating out-ref buffers..." ;
      let out_ringbuf_ref = C.out_ringbuf_names_ref conf proc.func in
      List.iter (fun cfunc ->
        let fname = input_ringbuf_fname conf proc.func cfunc
        and fieldmask = make_fieldmask proc.func cfunc in
        (* The destination ringbuffer must exist before it's referenced in an
         * out-ref, or the worker might err and throw away the tuples: *)
        RingBuf.create fname ;
        OutRef.add out_ringbuf_ref fname fieldmask
      ) proc.children ;
      Some out_ringbuf_ref
    ) in
  (* Export for a little while at the beginning (help with both automatic
   * and manual tests): *)
  if not is_top_half then (
    start_export ~duration:conf.initial_export_duration conf proc.func |>
    ignore
  ) ;
  (* Now actually start the binary *)
  let notify_ringbuf =
    (* Where that worker must write its notifications. Normally toward a
     * ringbuffer that's read by Ramen, unless it's a test programs.
     * Tests must not send their notifications to Ramen with real ones,
     * but instead to a ringbuffer specific to the test_id. *)
    C.notify_ringbuf conf in
  let ocamlrunparam =
    let def = if proc.log_level = Debug then "b" else "" in
    getenv ~def "OCAMLRUNPARAM" in
  let env = [|
    "OCAMLRUNPARAM="^ ocamlrunparam ;
    "log_level="^ string_of_log_level proc.log_level ;
    (* To know what to log: *)
    "is_test="^ string_of_bool conf.C.test ;
    (* Select the function to be executed: *)
    "name="^ (proc.func.F.name :> string) ;
    "fq_name="^ fq_str ; (* Used for monitoring *)
    "report_ringbuf="^ (C.report_ringbuf conf :> string) ;
    "report_period="^ string_of_float proc.report_period ;
    "notify_ringbuf="^ (notify_ringbuf :> string) ;
    "rand_seed="^ (match !rand_seed with None -> ""
                  | Some s -> string_of_int s) ;
    "site="^ (conf.C.site :> string) ;
    (match !logger.output with
      | Directory _ ->
        let dir = N.path_cat [ conf.C.persist_dir ; N.path "log/workers" ;
                               F.path proc.func ] in
        "log="^ (dir :> string)
      | Stdout -> "no_log" (* aka stdout/err *)
      | Syslog -> "log=syslog") |] in
  let more_env =
    match proc.key.part with
    | Whole ->
        List.enum
          [ "output_ringbufs_ref="^ (Option.get out_ringbuf_ref :> string) ;
            (* We need to change this dir whenever the func signature or
             * params change to prevent it to reload an incompatible state *)
            "state_file="^ (C.worker_state conf proc.func proc.params :>
                              string) ;
            "factors_dir="^ (C.factors_of_function conf proc.func :>
                              string) ]
    | TopHalf ths ->
        Enum.append
          (List.enum ths |>
          Enum.mapi (fun i th ->
            "tunneld_host_"^ string_of_int i ^"="^
              (th.tunneld.Services.host :> string)))
          (List.enum ths |>
          Enum.mapi (fun i th ->
            "tunneld_port_"^ string_of_int i ^"="^
              string_of_int th.tunneld.Services.port)) |>
        Enum.append
          (List.enum ths |>
          Enum.mapi (fun i th ->
            "parent_num_"^ string_of_int i ^"="^
              string_of_int th.parent_num)) in
  (* Pass each input ringbuffer in a sequence of envvars: *)
  let more_env =
    List.enum input_ringbufs |>
    Enum.mapi (fun i (n : N.path) ->
      "input_ringbuf_"^ string_of_int i ^"="^ (n :> string)) |>
    Enum.append more_env in
  (* Pass each individual parameter as a separate envvar; envvars are just
   * non interpreted strings (but for the first '=' sign that will be
   * interpreted by the OCaml runtime) so it should work regardless of the
   * actual param name or value, and make it easier to see what's going
   * on from the shell. Notice that we pass all the parameters including
   * those omitted by the user. *)
  let more_env =
    P.env_of_params_and_exps conf proc.params |>
    Enum.append more_env in
  (* Also add all envvars that are defined and used in the operation: *)
  let envvars = O.envvars_of_operation proc.func.operation in
  let more_env =
    List.enum envvars //@
    (fun (n : N.field) ->
      try Some ((n :> string) ^"="^ Sys.getenv (n :> string))
      with Not_found -> None) |>
    Enum.append more_env in
  let env = Array.append env (Array.of_enum more_env) in
  let args =
    [| if proc.key.part = Whole then Worker_argv0.full_worker
                                else Worker_argv0.top_half ;
       fq_str |] in
  let pid = run_worker ~and_stop:conf.C.test proc.bin args env in
  !logger.debug "Function %a now runs under pid %d"
    print_running_process proc pid ;
  proc.pid <- Some pid ;
  proc.last_killed := 0. ;
  (* Update the parents out_ringbuf_ref: *)
  List.iter (fun (_, _, pfunc) ->
    let out_ref =
      C.out_ringbuf_names_ref conf pfunc in
    let fname = input_ringbuf_fname conf pfunc proc.func
    and fieldmask = make_fieldmask pfunc proc.func in
    OutRef.add out_ref fname fieldmask
  ) proc.parents

(* Try to start the given proc.
 * Check links (ie.: do parents and children have the proper types?) *)
let really_try_start conf now proc =
  info_or_test conf "Starting operation %a"
    print_running_process proc ;
  assert (proc.pid = None) ;
  let check_linkage p c =
    let out_type =
      O.out_type_of_operation ~with_private:false p.F.operation in
    try check_is_subtype c.F.in_type out_type
    with Failure msg ->
      Printf.sprintf2
        "Input type of %s (%a) is not compatible with \
         output type of %s (%a): %s"
        (F.fq_name c :> string)
        RamenFieldMaskLib.print_in_type c.in_type
        (F.fq_name p :> string)
        RamenTuple.print_typ_names out_type
        msg |>
      failwith in
  let parents_ok = ref false in (* This is benign *)
  try
    parents_ok := true ;
    List.iter (fun (_, _, pfunc) ->
      check_linkage pfunc proc.func
    ) proc.parents ;
    List.iter (fun cfunc ->
      check_linkage proc.func cfunc
    ) proc.children ;
    really_start conf proc
  with e ->
    print_exception ~what:"Cannot start worker" e ;
    (* Anything goes wrong when starting a node? Quarantine it! *)
    let delay =
      if not !parents_ok then 20. else 600. in
    proc.quarantine_until <-
      now +. Random.float (max 90. delay)

let try_start conf proc =
  let now = Unix.gettimeofday () in
  if proc.quarantine_until > now then (
    !logger.debug "Operation %a still in quarantine"
      print_running_process proc
  ) else (
    really_try_start conf now proc
  )

let kill_politely conf last_killed what pid stats_sigkills =
  (* Start with a TERM (if last_killed = 0.) and then after 5s send a
   * KILL if the worker is still running. *)
  let now = Unix.gettimeofday () in
  if !last_killed = 0. then (
    (* Ask politely: *)
    info_or_test conf "Terminating %s" what ;
    log_and_ignore_exceptions ~what:("Terminating "^ what)
      (Unix.kill pid) Sys.sigterm ;
    last_killed := now
  ) else if now -. !last_killed > 10. then (
    !logger.warning "Killing %s with bigger guns" what ;
    log_and_ignore_exceptions ~what:("Killing "^ what)
      (Unix.kill pid) Sys.sigkill ;
    last_killed := now ;
    IntCounter.inc (stats_sigkills conf.C.persist_dir)
  )

let try_kill conf proc =
  let pid = Option.get proc.pid in
  (* There is no reason to wait before we remove this worker from its
   * parent out-ref: if it's not replaced then the last unprocessed
   * tuples are lost. If it's indeed a replacement then the new version
   * will have a chance to process the left overs. *)
  let input_ringbufs = C.in_ringbuf_names conf proc.func in
  List.iter (fun (_, _, pfunc) ->
    let parent_out_ref =
      C.out_ringbuf_names_ref conf pfunc in
    List.iter (fun this_in ->
      OutRef.remove parent_out_ref this_in RamenChannel.live
    ) input_ringbufs
  ) proc.parents ;
  (* If it's still stopped, unblock first: *)
  log_and_ignore_exceptions ~what:"Continuing worker (before kill)"
    (Unix.kill pid) Sys.sigcont ;
  let what = Printf.sprintf2 "worker %a (pid %d)"
               N.fq_print (F.fq_name proc.func) pid in
  kill_politely conf proc.last_killed what pid stats_worker_sigkills

(* This is used to check that we do not check too often nor too rarely: *)
let last_checked_outref = ref 0.

(* Need also running to check which workers are actually running *)
let check_out_ref conf must_run running =
  !logger.debug "Checking out_refs..." ;
  (* Build the set of all wrapping ringbuf that are being read.
   * Note that input rb of a top-half must be treated the same as input
   * rb of a full worker: *)
  let rbs =
    Hashtbl.fold (fun _k (mre : must_run_entry) s ->
      C.in_ringbuf_names conf mre.func |>
      List.fold_left (fun s rb_name -> Set.add rb_name s) s
    ) must_run (Set.singleton (C.notify_ringbuf conf)) in
  Hashtbl.iter (fun _ proc ->
    (* Iter over all running functions and check they do not output to a
     * ringbuf not in this set: *)
    if proc.pid <> None then (
      let out_ref = C.out_ringbuf_names_ref conf proc.func in
      let outs = OutRef.read_live out_ref in
      Hashtbl.iter (fun fname _ ->
        if Files.has_ext "r" fname && not (Set.mem fname rbs) then (
          !logger.error "Operation %a outputs to %a, which is not read, fixing"
            print_running_process proc
            N.path_print fname ;
          log_and_ignore_exceptions ~what:("fixing "^ (fname :> string))
            (fun () ->
              IntCounter.inc (stats_outref_repairs conf.C.persist_dir) ;
              OutRef.remove out_ref fname RamenChannel.live) ())
      ) outs ;
      (* Conversely, check that all children are in the out_ref of their
       * parent: *)
      let in_rbs = C.in_ringbuf_names conf proc.func |> Set.of_list in
      List.iter (fun (_, _, pfunc) ->
        let out_ref = C.out_ringbuf_names_ref conf pfunc in
        let outs = OutRef.read_live out_ref in
        let outs = Hashtbl.keys outs |> Set.of_enum in
        if Set.disjoint in_rbs outs then (
          !logger.error "Operation %a must output to %a but does not, fixing"
            N.fq_print (F.fq_name pfunc)
            print_running_process proc ;
          log_and_ignore_exceptions ~what:("fixing "^(out_ref :> string))
            (fun () ->
              let fname = input_ringbuf_fname conf pfunc proc.func
              and fieldmask = make_fieldmask pfunc proc.func in
              OutRef.add out_ref fname fieldmask) ())
      ) proc.parents
    )
  ) running

let signal_all_cont running =
  Hashtbl.iter (fun _ proc ->
    if proc.pid <> None && not proc.continued then (
      proc.continued <- true ;
      !logger.debug "Signaling %a to continue" print_running_process proc ;
      log_and_ignore_exceptions ~what:"Signaling worker to continue"
        (Unix.kill (Option.get proc.pid)) Sys.sigcont) ;
  ) running

module FuncGraph =
struct
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

  let make () =
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
end

(* Build the list of workers that must run on this site.
 * Start by getting a list of sites and then populate it with all workers
 * that must run there, with for each the set of their parents. *)

let build_must_run conf =
  let sites = Services.all_sites conf in
  let programs = RC.with_rlock conf identity in
  (* Start by rebuilding what needs to be before calling any get_rc() : *)
  Hashtbl.iter (fun program_name (rce, _get_rc) ->
    if rce.RC.status = RC.MustRun && not (N.is_empty rce.RC.src_file) then (
      !logger.debug "Trying to build %a" N.path_print rce.RC.bin ;
      log_and_ignore_exceptions
        ~what:("rebuilding "^ (rce.RC.bin :> string))
        (fun () ->
          let get_parent =
            RamenCompiler.parent_from_programs programs in
          RamenMake.build
            conf get_parent program_name rce.RC.src_file
            rce.RC.bin) ())
  ) programs ;
  (* Build a map from site name to the map of (prog, func names) to
   * prog*func*parents, where parents is a set of site*prog*func: *)
  let graph = FuncGraph.make () in
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
                    (fun () -> Hashtbl.create 5) graph.h local_site in
                let k = func.F.program_name, func.F.name in
                let ge =
                  FuncGraph.make_entry conf graph rce prog func parents in
                Hashtbl.add h k ge
              ) where_running
            ) prog.P.funcs
      ) (* else this program is not running *)
    ) programs ;
    FuncGraph.fix_used graph)
    (Histogram.add (stats_graph_build_time conf.C.persist_dir)) ;
  !logger.debug "Graph of functions: %a" FuncGraph.print graph ;
  (* Now building the hash of functions that must run from graph is easy: *)
  let must_run =
    match Hashtbl.find graph.h conf.C.site with
    | exception Not_found ->
        Hashtbl.create 0
    | h ->
        Hashtbl.enum h //@
        (fun (_, ge) ->
          if ge.used then
            (* Use the mount point + signature + params as the key.
             * Notice that we take all the parameter values (from
             * prog.params), not only the explicitly set values (from
             * rce.params), so that if a default value that is
             * unset is changed in the program then that's considered a
             * different program. *)
            let parents =
              Set.fold (fun (ps, pp, pf) lst ->
                (* Guaranteed to be in the graph: *)
                let pge = FuncGraph.find graph ps pp pf in
                (ps, pge.prog, pge.func) :: lst
              ) ge.parents [] in
            let key =
              { program_name = ge.func.F.program_name ;
                func_name = ge.func.F.name ;
                func_signature = ge.func.F.signature ;
                params = ge.prog.P.params ;
                part = Whole } in
            let v =
              { key ; rce = ge.rce ; func = ge.func ; parents } in
            Some (key, v)
          else
            None) |>
        Hashtbl.of_enum in
  !logger.info "%d workers must run in full" (Hashtbl.length must_run) ;
  (* We need a top half for every function running locally with a child
   * running remotely.
   * If we shared the same top-half for several local parents, then we would
   * have to deal with merging/sorting in the top-half.
   * If we have N such children with the same program name/func names and
   * signature (therefore the same WHERE filter) then we need only one
   * top-half. *)
  let top_halves = Hashtbl.create 10 in
  Hashtbl.iter (fun site h ->
    if site <> conf.C.site then
      Hashtbl.iter (fun _ ge ->
        if ge.FuncGraph.used then
          (* Then parent will be used as well, no need to check *)
          Set.iter (fun (ps, pp, pf) ->
            if ps = conf.C.site then
              let service = N.service "tunneld" in
              match Services.resolve conf site service with
              | exception Not_found ->
                  !logger.error "No service matching %a:%a"
                    N.site_print site
                    N.service_print service
              | srv ->
                  let k =
                    (* local part *)
                    pp, pf,
                    (* remote part *)
                    ge.FuncGraph.func.F.program_name, ge.func.F.name,
                    ge.func.F.signature, ge.prog.P.params in
                  (* We could meet several times the same func on different
                   * sites, but that would be the same rce and func! *)
                  Hashtbl.modify_opt k (function
                    | Some (rce, func, srvs) ->
                        Some (rce, func, (srv :: srvs))
                    | None ->
                        Some (ge.rce, ge.func, [ srv ])
                  ) top_halves
          ) ge.parents
      ) h
  ) graph.h ;
  let l = Hashtbl.length top_halves in
  if l > 0 then !logger.info "%d workers must run in half" l ;
  (* Now that we have aggregated all top-halves children, actually adds them
   * to [must_run]: *)
  Hashtbl.iter (fun (pp, pf, cprog, cfunc, sign, params)
                    (rce, func, tunnelds) ->
    let part =
      TopHalf (List.mapi (fun i tunneld ->
        { tunneld ; parent_num = i }) tunnelds) in
    let key =
      { program_name = cprog ;
        func_name = cfunc ;
        func_signature = sign ;
        params ; part } in
    let parents =
      let pge = FuncGraph.find graph conf.C.site pp pf in
      [ conf.C.site, pge.prog, pge.func ] in
    let v =
      { key ; rce ; func ; parents } in
    assert (not (Hashtbl.mem must_run key)) ;
    Hashtbl.add must_run key v
  ) top_halves ;
  must_run

(* Have a single watchdog even when supervisor is restarted: *)
let watchdog = ref None

(*
 * Synchronisation of the rc file of programs we want to run and the
 * actually running workers. Also synchronise the configured replays
 * and the actually running replayers and established replay channels.
 *
 * [autoreload_delay]: even if the rc file hasn't changed, we want to re-read
 * it from time to time and refresh the [must_run] hash with new signatures
 * from the binaries that might have changed.  If some operations from a
 * program indeed has a new signature, then the former one will disappear from
 * [must_run] and the new one will enter it, and the new one will replace it.
 * We do not have to wait until the previous worker dies before starting the
 * new version: If they have the same input type they will share the same
 * input ringbuf and steal some work from each others, which is still better
 * than having only the former worker doing all the work.  And if they have
 * different input types then the tuples will switch toward the new instance
 * as the parent out-ref gets updated. Similarly, they use different state
 * files.  They might both be present in a parent out_ref though, so some
 * duplication of tuple is possible (or conversely: some input tuples might
 * be missing if we kill the previous before starting the new one).
 * If they do share the same input ringbuf though, it is important that we
 * remove the former one before we add the new one (or the parent out-ref
 * will end-up empty). This is why we first do all the kills, then all the
 * starts (Note that even if a worker does not terminate immediately on
 * kill, its parent out-ref is cleaned immediately).
 *
 * Regarding [conf.test]:
 * In that mode, add a parameter to the workers so that they stop before doing
 * anything. Then, when nothing is left to be started, the synchronizer will
 * unblock all workers. This mechanism enforces that CSV readers do not start
 * to read tuples before all their children are attached to their out_ref file.
 *)
let synchronize_running conf autoreload_delay =
  (* Avoid memoizing this at every call to build_must_run: *)
  if !watchdog = None then
    watchdog :=
      (* In the first run we might have *plenty* of workers to start, thus
       * the extended grace_period (it's not unheard of >1min to start all
       * workers on a small VM) *)
      Some (RamenWatchdog.make ~grace_period:180. ~timeout:30.
                               "supervisor" quit) ;
  let watchdog = Option.get !watchdog in
  (* Stop/Start processes so that [running] corresponds to [must_run].
   * [must_run] is a hash from the function mount point (program and function
   * name), signature, parameters and top-half info, to the
   * [C.must_run_entry], prog and func.
   *
   * [running] is a hash from the same key to its running_process (mutable
   * pid, cleared asynchronously when the worker terminates).
   *
   * FIXME: it would be nice if all parents were resolved once and for all
   * in [must_run] as well, as a list of optional function mount point (FQ).
   * When a function is reparented we would then detect it and could fix the
   * outref immediately.
   * Similarly, [running] should keep the previous set of parents (or rather,
   * the name of their out_ref). *)
  let synchronize_workers must_run running =
    (* First, remove from running all terminated processes that must not run
     * any longer. Send a kill to those that are still running. *)
    let prev_num_running = Hashtbl.length running in
    IntGauge.set stats_worker_count (Hashtbl.length must_run) ;
    IntGauge.set stats_worker_running prev_num_running ;
    let to_kill = ref [] and to_start = ref []
    and (+=) r x = r := x :: !r in
    Hashtbl.filteri_inplace (fun k proc ->
      if Hashtbl.mem must_run k then true else
      if proc.pid <> None then (to_kill += proc ; true) else
      false
    ) running ;
    (* Then, add/restart all those that must run. *)
    Hashtbl.iter (fun k (mre : must_run_entry) ->
      match Hashtbl.find running k with
      | exception Not_found ->
          let proc = make_running_process conf must_run mre in
          Hashtbl.add running k proc ;
          to_start += proc
      | proc ->
          (* If it's dead, restart it: *)
          if proc.pid = None then to_start += proc else
          (* If we were killing it, it's safer to keep killing it until it's
           * dead and then restart it. *)
          if !(proc.last_killed) <> 0. then to_kill += proc
    ) must_run ;
    let num_running = Hashtbl.length running in
    if !quit <> None && num_running > 0 && num_running <> prev_num_running then
      info_or_test conf "Still %d processes running"
        (Hashtbl.length running) ;
    (* See preamble discussion about autoreload for why workers must be
     * started only after all the kills: *)
    if !to_kill <> [] then !logger.debug "Starting the kills" ;
    List.iter (try_kill conf) !to_kill ;
    if !to_start <> [] then !logger.debug "Starting the starts" ;
    (* FIXME: sort it so that parents are started before children,
     * so that in case of linkage error we do not end up with orphans
     * preventing parents to be run. *)
    List.iter (fun proc ->
      try_start conf proc ;
      (* If we had to compile a program this is worth resetting the watchdog: *)
      RamenWatchdog.reset watchdog
    ) !to_start ;
    (* Try to fix any issue with out_refs: *)
    let now = Unix.time () in
    if (now > !last_checked_outref +. 30. ||
        now > !last_checked_outref +. 5. && !to_start = [] && !to_kill = []) &&
       !quit = None
    then (
      last_checked_outref := now ;
      log_and_ignore_exceptions ~what:"checking out_refs"
        (check_out_ref conf must_run) running) ;
    if !to_start = [] && conf.C.test then signal_all_cont running ;
    (* Return if anything changed: *)
    !to_kill <> [] || !to_start <> []
  (* Similarly, try to make [replayers] the same as [must_replay]: *)
  and synchronize_replays must_replay replayers =
    (* Kill the replayers of channels that are not configured any longer,
     * and start new replayers for new channels: *)
    let run_chans = Hashtbl.keys replayers |> Set.of_enum
    and def_chans = Hashtbl.keys must_replay |> Set.of_enum in
    let kill_replayer chan (pid, last_killed) =
      let what = Printf.sprintf2 "replayer for %a (pid %d)"
                   RamenChannel.print chan pid in
      kill_politely conf last_killed what pid stats_replayer_sigkills in
    let to_rem chan =
      (* Just kill the replayers and wait for them to finish before tearing
       * down the replay chanel: *)
      let replayer, pids = Hashtbl.find replayers chan in
      Set.iter (kill_replayer replayer.C.Replays.channel) pids in
    let get_programs = memoize (fun () -> RC.with_rlock conf identity) in
    let start_replay replay =
      let programs = get_programs () in
      Replay.settup_links conf programs replay ;
      (* Do not start the replay at once or the worker won't have reread
       * its out-ref. TODO: signal it. *)
      !logger.info "Sleeping %fs to allow nodes to read out-ref"
        Default.min_delay_restats ;
      Unix.sleepf Default.min_delay_restats ;
      Replay.spawn_all_local_sources conf programs replay in
    let to_add chan =
      let replay = Hashtbl.find must_replay chan in
      match start_replay replay with
      | exception e ->
          let what = Printf.sprintf2 "Starting replay for channel %a"
                       RamenChannel.print replay.C.Replays.channel in
          print_exception ~what e
      | pids ->
          (* If this replay does not concern our site, the pids will be
           * empty, process_replayers_terminations will then remove the
           * entry, and it will be added again next time. We can save
           * a bit of this useless work by not adding it at all: *)
          if not (Set.is_empty pids) then
            Hashtbl.add replayers chan (replay, pids)
    in
    Set.diff run_chans def_chans |> Set.iter to_rem ;
    Set.diff def_chans run_chans |> Set.iter to_add
  in
  (* The workers that are currently running: *)
  let running = Hashtbl.create 307 in
  (* The replayers (hash from channel to (pids * replay)) that are currently
   * running: *)
  let replayers = Hashtbl.create 307 in
  let rc_file = RC.file_name conf in
  Files.ensure_exists ~contents:"{}" rc_file ;
  let replays_file = C.Replays.file_name conf in
  Files.ensure_exists ~contents:"{}" replays_file ;
  let fnotifier =
    RamenFileNotify.make_file_notifier [ rc_file ; replays_file ] in
  let rec loop last_must_run last_read_rc last_replays last_read_replays =
    (* Once we have forked some workers we must not allow an exception to
     * terminate this function or we'd leave unsupervised workers behind: *)
    restart_on_failure "process supervisor" (fun () ->
      if !quit <> None &&
         Hashtbl.length running + Hashtbl.length replayers = 0
      then (
        !logger.info "All processes stopped, quitting."
      ) else (
        let max_wait =
          ref (if autoreload_delay > 0. then
                 autoreload_delay else 5.) in
        let set_max_wait d =
          if d < !max_wait then max_wait := d in
        let must_reread fname last_read autoreload_delay now =
          let granularity = 0.1 in
          let last_mod =
            try Some (Files.mtime fname)
            with Unix.(Unix_error (ENOENT, _, _)) -> None in
          let reread =
            match last_mod with
            | None -> false
            | Some lm ->
                if lm >= last_read ||
                   autoreload_delay > 0. &&
                   now -. last_read >= autoreload_delay
                then (
                  (* To prevent missing the last writes when the file is
                   * updated faster than mtime granularity, refuse to refresh
                   * the file unless last mod time is old enough.
                   * As [now] will be the next [last_read] we are guaranteed to
                   * have [lm < last_read] in the next calls in the absence of
                   * writes.  But we also want to make sure that all writes
                   * occurring after we do read that file will push the mtime
                   * after (>) [lm], which is true only if now is greater than
                   * lm + mtime granularity: *)
                  let age_modif = now -. lm in
                  if age_modif > granularity then
                    true
                  else (
                    set_max_wait (granularity -. age_modif) ;
                    false
                  )
                ) else false in
          let ret = last_mod <> None && reread in
          if ret then !logger.debug "%a has changed %gs ago"
            N.path_print fname (now -. Option.get last_mod) ;
          ret in
        let must_run, last_read_rc =
          if !quit <> None then (
            !logger.debug "No more workers should run" ;
            Hashtbl.create 0, last_read_rc
          ) else (
            let now = Unix.gettimeofday () in
            if must_reread rc_file last_read_rc autoreload_delay now then
              build_must_run conf, now
            else
              last_must_run, last_read_rc
          ) in
        process_workers_terminations conf running ;
        let changed = synchronize_workers must_run running in
        (* Touch the rc file if anything changed (esp. autoreload) since that
         * mtime is used to signal cache expirations etc. *)
        if changed then Files.touch rc_file last_read_rc ;
        let must_replay, last_read_replays =
          if !quit <> None then (
            !logger.debug "No more replays should run" ;
            Hashtbl.create 0, last_read_replays
          ) else (
            let now = Unix.gettimeofday () in
            if must_reread replays_file last_read_replays 0. now then
              (* FIXME: We need to start lazy nodes right *after* having
               * written into their out-ref but *before* replayers are started
               *)
              C.Replays.load conf, now
            else
              last_replays, last_read_replays
          ) in
        process_replayers_terminations conf replayers ;
        synchronize_replays must_replay replayers ;
        Gc.minor () ;
        !logger.debug "Waiting for file changes (max %a)"
          print_as_duration !max_wait ;
        let fname = RamenFileNotify.wait_file_changes
                      ~while_:(fun () -> !quit = None)
                      ~max_wait:!max_wait fnotifier in
        !logger.debug "Done. %a changed." (Option.print N.path_print) fname ;
        RamenWatchdog.reset watchdog ;
        loop must_run last_read_rc must_replay last_read_replays)) ()
  in
  RamenWatchdog.enable watchdog ;
  loop (Hashtbl.create 0) 0. (Hashtbl.create 0) 0. ;
  RamenWatchdog.disable watchdog
