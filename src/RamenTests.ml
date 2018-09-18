open Batteries
open Stdint
open RamenHelpers
open RamenLog
open RamenNullable
module C = RamenConf
module F = C.Func
module P = C.Program

type tuple_spec = (string, string) Hashtbl.t [@@ppp PPP_OCaml]

module Input = struct
  type spec =
    { pause : float [@ppp_default 0.] ;
      operation : RamenName.fq ;
      tuple : tuple_spec }
    [@@ppp PPP_OCaml]
end

module Output = struct
  type spec =
    { present : tuple_spec list [@ppp_default []] ;
      absent : tuple_spec list [@ppp_default []] ;
      timeout : float [@ppp_default 7.] }
    [@@ppp PPP_OCaml]
end

module Notifs = struct
  type spec =
    { present : string list [@ppp_default []] ;
      absent : string list [@ppp_default []] ;
      timeout : float [@ppp_default 7.] }
    [@@ppp PPP_OCaml]
end

type test_spec =
  { programs : (RamenName.program, program_spec) Hashtbl.t ;
    inputs : Input.spec list [@ppp_default []] ;
    outputs : (RamenName.fq, Output.spec) Hashtbl.t
      [@ppp_default Hashtbl.create 0] ;
    until : (RamenName.fq, tuple_spec) Hashtbl.t
      [@ppp_default Hashtbl.create 0] ;
    (* Notifications likely useless now that we can tests #notifs: *)
    notifications : Notifs.spec
      [@ppp_default Notifs.{ present=[]; absent=[]; timeout=0. }] }
    [@@ppp PPP_OCaml]

and program_spec =
  { bin : string ; params : RamenName.params [@ppp_default Hashtbl.create 0] }
  [@@ppp PPP_OCaml]

(* Read a tuple described by the given type, and return a hash of fields
 * to string values *)

let fail_and_quit msg =
  RamenProcesses.quit := Some 1 ;
  failwith msg

let compare_miss bad1 bad2 =
  (* TODO: also look at the values *)
  Int.compare (List.length bad1) (List.length bad2)

let field_index_of_name fq ser field =
  match List.findi (fun _ ftyp ->
          ftyp.RamenTuple.typ_name = field
        ) ser with
  | exception Not_found ->
      Printf.sprintf2 "Unknown field %S in %s, which has only %a"
        field
        (RamenName.fq_color fq)
        RamenTuple.print_typ_names ser |>
      fail_and_quit
  | idx, _ -> idx

let field_name_of_index ser idx =
  (List.nth ser idx).RamenTuple.typ_name

(* The configuration file gives us tuple spec as a hash, which is
 * convenient to serialize, but for filtering it's more convenient to
 * have a list of field index to values, and a best_miss: *)
let filter_spec_of_spec fq ser spec =
  Hashtbl.enum spec /@
  (fun (field, value) ->
    field_index_of_name fq ser field, value) |>
  List.of_enum, ref []

(* Do not use RamenExport.read_output filter facility because of
 * best_miss: *)
let filter_of_tuple_spec (spec, best_miss) tuple =
  let miss =
    List.fold_left (fun miss (idx, value) ->
      (* FIXME: instead of comparing in string we should try to parse
       * the expected value (once and for all -> faster) so that we
       * also check its type. *)
      let s = RamenTypes.to_string tuple.(idx) in
      let ok = s = value in
      if ok then miss else (
        !logger.debug "found %S instead of %S" s value ;
        (idx, s)::miss)
    ) [] spec in
  if miss = [] then true
  else (
    if !best_miss = [] || compare_miss miss !best_miss < 0 then
      best_miss := miss ;
    false
  )

let file_spec_print ser best_miss oc (idx, value) =
  (* Retrieve actual field name: *)
  let n = field_name_of_index ser idx in
  Printf.fprintf oc "%s=%s" n value ;
  match List.find (fun (idx', s) -> idx = idx') best_miss with
  | exception Not_found -> ()
  | _idx, s -> Printf.fprintf oc " (had %S)" s

let tuple_spec_print ser oc (spec, best_miss) =
  List.fast_sort (fun (i1, _) (i2, _) -> Int.compare i1 i2) spec |>
  List.print (file_spec_print ser !best_miss) oc

let test_output conf fq output_spec test_ended =
  (* Notice that although we do not provide a filter read_output can
   * return one, to select the worker in well-known functions: *)
  let bname, filter, _typ, ser, _params, _event_time =
    RamenExport.read_output conf fq [] in
  let nullmask_sz = RingBufLib.nullmask_bytes_of_tuple_type ser in
  (* Change the hashtable of field to value into a list of field index
   * and value: *)
  let field_indices_of_tuples =
    List.map (filter_spec_of_spec fq ser) in
  let tuples_to_find = ref (
    field_indices_of_tuples output_spec.Output.present) in
  let tuples_must_be_absent =
    field_indices_of_tuples output_spec.Output.absent in
  let tuples_to_not_find = ref [] in
  let start = Unix.gettimeofday () in
  (* With tuples that must be absent, when to stop listening?
   * For now the rule is simple:
   * for as long as we have not yet received some tuples that
   * must be present and the time did not ran out. *)
  let while_ () =
    not !test_ended &&
    !tuples_to_find <> [] &&
    !tuples_to_not_find = [] &&
    !RamenProcesses.quit = None &&
    Unix.gettimeofday () -. start < output_spec.timeout in
  let unserialize = RamenSerialization.read_tuple ser nullmask_sz in
  !logger.debug "Enumerating tuples from %s" bname ;
  let num_tuples =
    RamenSerialization.fold_seq_range ~wait_for_more:true ~while_ bname 0 (fun count _seq tx ->
      let tuple = unserialize tx in
      if filter tuple then (
        !logger.debug "Read a tuple out of operation %S"
          (RamenName.string_of_fq fq) ;
        tuples_to_find :=
          List.filter (fun filter_spec ->
            not (filter_of_tuple_spec filter_spec tuple)
          ) !tuples_to_find ;
        tuples_to_not_find :=
          List.filter (fun (spec, _) ->
            List.for_all (fun (idx, value) ->
              RamenTypes.to_string tuple.(idx) = value) spec
          ) tuples_must_be_absent |>
          List.rev_append !tuples_to_not_find) ;
      (* Count all tuples including those filtered out: *)
      count + 1) in
  let success = !tuples_to_find = [] && !tuples_to_not_find = []
  in
  let err_string_of_tuples lst =
    let len = List.length lst in
    let pref =
      if len <= 1 then "this tuple: "
      else Printf.sprintf "these %d tuples: " len in
    pref ^
    IO.to_string (List.print ~first:"\n  " ~last:"\n" ~sep:"\n  "
                    (tuple_spec_print ser)) lst in
  let msg =
    if success then "" else
    (Printf.sprintf "Enumerated %d tuple%s from %s"
      num_tuples (if num_tuples > 0 then "s" else "")
      (RamenName.string_of_fq fq)) ^
    (if !tuples_to_find = [] then "" else
      " but could not find "^ err_string_of_tuples !tuples_to_find) ^
    (if !tuples_to_not_find = [] then "" else
      " and found "^ err_string_of_tuples !tuples_to_not_find)
  in
  success, msg

(* Wait for the given tuple: *)
let test_until conf fq spec =
  let bname, filter, _typ, ser, _params, _event_time =
    RamenExport.read_output conf fq [] in
  let filter_spec = filter_spec_of_spec fq ser spec in
  let nullmask_sz = RingBufLib.nullmask_bytes_of_tuple_type ser in
  let unserialize = RamenSerialization.read_tuple ser nullmask_sz in
  let got_it = ref false in
  let while_ () = not !got_it && !RamenProcesses.quit = None in
  !logger.debug "Enumerating tuples from %s for early termination" bname ;
  RamenSerialization.fold_seq_range ~wait_for_more:true ~while_ bname 0 (fun count _seq tx ->
    let tuple = unserialize tx in
    if filter tuple && filter_of_tuple_spec filter_spec tuple then (
      !logger.info "Got terminator tuple from function %S"
        (RamenName.string_of_fq fq) ;
      got_it := true) ;
    (* Count all tuples including those filtered out: *)
    count + 1) |> ignore

let test_notifications notify_rb notif_spec test_ended =
  (* We keep pat in order to be able to print it later: *)
  let to_regexp pat = pat, Str.regexp pat in
  let notifs_must_be_absent = List.map to_regexp notif_spec.Notifs.absent
  and notifs_to_find = ref (List.map to_regexp notif_spec.Notifs.present)
  and notifs_to_not_find = ref []
  and start = Unix.gettimeofday () in
  let while_ () =
    not !test_ended &&
    !notifs_to_find <> [] &&
    !notifs_to_not_find = [] &&
    Unix.gettimeofday () -. start < notif_spec.timeout in
  RamenSerialization.read_notifs ~while_ notify_rb
    (fun (worker, sent_time, event_time, notif_name, firing, certainty,
          parameters) ->
      let firing = option_of_nullable firing in
      !logger.debug "Got %snotification from %s: %S"
        (if firing = Some false then "stopping " else "firing ")
        worker notif_name ;
      notifs_to_find :=
        List.filter (fun (_pat, re) ->
          Str.string_match re notif_name 0 |>  not) !notifs_to_find ;
      notifs_to_not_find :=
        List.filter (fun (_pat, re) ->
          Str.string_match re notif_name 0) notifs_must_be_absent |>
        List.rev_append !notifs_to_not_find) ;
  let success = !notifs_to_find = [] && !notifs_to_not_find = [] in
  let re_print oc (pat, _re) = String.print oc pat in
  let msg =
    if success then "" else
    (if !notifs_to_find = [] then "" else
      "Could not find these notifs: "^
        IO.to_string (List.print re_print) !notifs_to_find) ^
    (if !notifs_to_not_find = [] then "" else
      "Found these notifs: "^
        IO.to_string (List.print re_print) !notifs_to_not_find)
  in
  success, msg

(* Perform all find of checks before spawning testing threads, such as
 * check the existence of all mentioned programs and functions: *)
let check_test_spec conf test =
  let fold_funcs i f =
    let maybe_f fq (s, i as prev) =
      if Set.mem fq s then prev else (
        Set.add fq s,
        f i fq)
    in
    let s_i =
      List.fold_left (fun s_i in_spec ->
        maybe_f in_spec.Input.operation s_i
      ) (Set.empty, i) test.inputs in
    let s_i =
      Hashtbl.fold (fun fq _ s_i ->
        maybe_f fq s_i
      ) test.outputs s_i in
    let s, i =
      Hashtbl.fold (fun fq _ s_i ->
        maybe_f fq s_i
      ) test.until s_i in
    i in
  let fold_programs i f =
    let maybe_f prog_name (s, i as prev) =
      if Set.mem prog_name s then prev else (
        Set.add prog_name s,
        f i prog_name) in
    let s_i =
      Hashtbl.fold (fun prog_name _ s_i ->
        maybe_f prog_name s_i
      ) test.programs (Set.empty, i) in
    let _, i =
      fold_funcs s_i (fun s_i fq ->
        let prog_name, _ = RamenName.fq_parse fq in
        maybe_f prog_name s_i
      ) in
    i in
  C.with_rlock conf (fun programs ->
    fold_programs () (fun () prog_name ->
      if not (Hashtbl.mem programs prog_name) then
        Printf.sprintf "Unknown program %s"
          (RamenName.program_color prog_name) |>
        failwith))

let run_test conf notify_rb dirname test =
  (* Hash from func fq name to its rc and mmapped input ring-buffer: *)
  let workers = Hashtbl.create 11 in
  (* The only sure way to know when to stop the workers is: when the test
   * succeeded, or timeouted. So we start three threads at the same time:
   * the process synchronizer, the worker feeder, and the output evaluator: *)
  (* First, write the list of programs that must run and fill workers
   * hash-table: *)
  C.with_wlock conf (fun programs ->
    Hashtbl.clear programs ;
    Hashtbl.iter (fun program_name p ->
      (* The path to the binary is relative to the test file: *)
      if p.bin = "" then failwith "Binary file must not be empty" ;
      let bin =
        (if p.bin.[0] = '/' then p.bin else dirname ^"/"^ p.bin) |>
        absolute_path_of in
      let prog = P.of_bin p.params bin in
      Hashtbl.add programs program_name
        C.{ bin ; params = p.params ; killed = false ; debug = false } ;
      List.iter (fun func ->
        Hashtbl.add workers (F.fq_name func) (func, ref None)
      ) prog.P.funcs
    ) test.programs) ;
  check_test_spec conf test ;
  let worker_feeder () =
    let feed_input input =
      match Hashtbl.find workers input.Input.operation with
      | exception Not_found ->
          let msg =
            Printf.sprintf2 "Unknown operation: %S (must be one of: %a)"
              (RamenName.string_of_fq input.operation)
              (Enum.print RamenName.fq_print) (Hashtbl.keys workers) in
          fail_and_quit msg
      | func, rbr ->
          if !rbr = None then (
            if func.F.merge_inputs then
              (* TODO: either specify a parent number or pick the first one? *)
              let err = "Writing to merging operations is not \
                         supported yet!" in
              fail_and_quit err
            else (
              let in_rb = C.in_ringbuf_name_single conf func in
              (* It might not exist already. Instead of waiting for the
               * worker to start, create it: *)
              RingBuf.create in_rb ;
              let rb = RingBuf.load in_rb in
              rbr := Some rb)) ;
          let rb = Option.get !rbr in
          RamenSerialization.write_record conf func.F.in_type rb input.tuple
    in
    List.iter (fun input ->
      if !RamenProcesses.quit = None then feed_input input
    ) test.inputs ;
    Hashtbl.iter (fun _ (_, rbr) ->
      Option.may RingBuf.unload !rbr ;
      rbr := None
    ) workers in
  (* This flag will signal the end to either both tester and early_termination
   * threads: *)
  let test_ended = ref false in
  let stop_workers =
    let all = [ Globs.compile "*" ] in
    fun () ->
      ignore (RamenRun.kill conf all) ;
      test_ended := true in
  (* One tester thread per operation *)
  let tester_threads =
    Hashtbl.fold (fun user_fq_name output_spec thds ->
      test_output conf user_fq_name output_spec :: thds
    ) test.outputs [] in
  (* Similarly, test the notifications: *)
  let tester_threads =
    (test_notifications notify_rb test.notifications) ::
    tester_threads in
  (* Wrap the testers into threads that update this status and set
   * the quit flag: *)
  let all_good = ref true in
  let num_tests_left = ref (List.length tester_threads) in
  let tester_threads =
    List.map (fun thd ->
      Thread.create (fun () ->
        let success, msg = thd test_ended in
        if not success then (
          all_good := false ;
          !logger.error "Failure: %s\n" msg
        ) ;
        decr num_tests_left ;
        if !num_tests_left <= 0 then (
          !logger.info "Finished all tests" ;
          stop_workers ())) ()
    ) tester_threads in
  (* Finally, a thread that tests the ending condition: *)
  let early_terminator =
    if Hashtbl.is_empty test.until then
      Thread.create RamenProcesses.until_quit (fun () ->
        if !test_ended then false else (Unix.sleep 1 ; true))
    else (
      Thread.create (fun () ->
        Hashtbl.fold (fun user_fq_name tuple_spec thds ->
          Thread.create (test_until conf user_fq_name) tuple_spec :: thds
        ) test.until [] |>
        List.iter Thread.join ;
        !logger.info "Early termination." ;
        all_good := false ;
        stop_workers ()) ()
    ) in
  !logger.debug "Waiting for test threads..." ;
  List.iter Thread.join
    ((Thread.create worker_feeder ()) :: tester_threads) ;
  !logger.debug "Waiting for thread early_terminator..." ;
  Thread.join early_terminator ;
  !all_good

let run conf server_url api graphite test () =
  let conf = C.{ conf with
    persist_dir =
      Filename.get_temp_dir_name ()
        ^"/ramen_test."^ string_of_int (Unix.getpid ()) |>
      uniquify_filename ;
    test = true } in
  logger := make_logger conf.C.log_level ;
  if server_url <> "" || api <> None || graphite <> None then
    RamenHttpd.run_httpd conf server_url api graphite 0.0 ;
  (* Parse tests so that we won't have to clean anything if it's bogus *)
  !logger.info "Parsing test specification in %S..." test ;
  let test_spec = ppp_of_file test_spec_ppp_ocaml test in
  let name = Filename.(basename test |> remove_extension) in
  (* Start Ramen *)
  !logger.info "Starting ramen, using temp dir %s" conf.persist_dir ;
  mkdir_all conf.persist_dir ;
  RamenProcesses.prepare_signal_handlers () ;
  let notify_rb = RamenProcesses.prepare_notifs conf in
  let report_rb = RamenProcesses.prepare_reports conf in
  RingBuf.unload report_rb ;
  (* Run all tests: *)
  (* Note: The workers states must be cleaned in between 2 tests ; the
   * simpler is to draw a new test_id. *)
  let res = ref false in
  Thread.create (
    restart_on_failure "wait_all_pids_loop"
      RamenProcesses.wait_all_pids_loop) true |> ignore ;
  let sync =
    Thread.create (
      restart_on_failure "synchronize_running"
        (RamenProcesses.synchronize_running conf)) 0. in
  finally (fun () ->
            (* Terminate the other thread cleanly: *)
            RamenProcesses.quit := Some 0)
    (fun () ->
      let r =
        run_test conf notify_rb (Filename.dirname test) test_spec in
      res := r) () ;
  !logger.debug "Waiting for thread sync..." ;
  Thread.join sync ;
  RingBuf.unload notify_rb ;
  (* Show resources consumption: *)
  let stats = RamenPs.read_stats conf in
  !logger.info "Resources:%a"
    (Hashtbl.print ~first:"\n\t" ~last:"" ~kvsep:"\t" ~sep:"\n\t"
      String.print
      (fun oc (_min_etime, _max_etime, in_count, selected_count, out_count,
               _group_count, cpu, ram, max_ram, _wait_in, _wait_out, _bytes_in,
               _bytes_out, _last_out, _stime) ->
        Printf.fprintf oc "cpu:%fs\tmax ram:%s" cpu (Uint64.to_string max_ram)))
      stats ;
  if !res then !logger.info "Test %s: Success" name
  else failwith ("Test "^ name ^": FAILURE")
