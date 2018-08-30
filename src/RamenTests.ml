open Batteries
open Lwt
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
  { programs : (string * RamenName.params) list ;
    inputs : Input.spec list [@ppp_default []] ;
    outputs : (RamenName.fq, Output.spec) Hashtbl.t
      [@ppp_default Hashtbl.create 0] ;
    notifications : Notifs.spec
      [@ppp_default Notifs.{ present=[]; absent=[]; timeout=0. }] }
    [@@ppp PPP_OCaml]

(* Read a tuple described by the given type, and return a hash of fields
 * to string values *)

let fail_and_quit msg =
  RamenProcesses.quit := Some 1 ;
  failwith msg

let lwt_fail_and_quit msg =
  wrap (fun () -> fail_and_quit msg)

let compare_miss bad1 bad2 =
  (* TODO: also look at the values *)
  Int.compare (List.length bad1) (List.length bad2)

let test_output func bname output_spec =
  let ser = RingBufLib.ser_tuple_typ_of_tuple_typ func.F.out_type in
  let nullmask_sz = RingBufLib.nullmask_bytes_of_tuple_type ser in
  (* Change the hashtable of field to value into a list of field index
   * and value: *)
  let field_index_of_name field =
    match List.findi (fun _ ftyp ->
            ftyp.RamenTuple.typ_name = field
          ) ser with
    | exception Not_found ->
        let msg = Printf.sprintf "Unknown field %s in %s" field
                    (IO.to_string RamenTuple.print_typ_names ser) in
        fail_and_quit msg
    | idx, _ -> idx in
  (* The other way around to print the results: *)
  let field_name_of_index idx =
    (List.nth ser idx).RamenTuple.typ_name in
  let field_indices_of_tuples =
    List.map (fun spec ->
      Hashtbl.enum spec /@
      (fun (field, value) ->
        field_index_of_name field, value) |>
      List.of_enum, ref []) in
  let%lwt tuples_to_find = wrap (fun () -> ref (
    field_indices_of_tuples output_spec.Output.present)) in
  let%lwt tuples_must_be_absent = wrap (fun () ->
    field_indices_of_tuples output_spec.Output.absent) in
  let tuples_to_not_find = ref [] in
  let start = Unix.gettimeofday () in
  (* With tuples that must be absent, when to stop listening?
   * For now the rule is simple:
   * for as long as we have not yet received some tuples that
   * must be present and the time did not ran out. *)
  let while_ () =
    return (
      !tuples_to_find <> [] &&
      !tuples_to_not_find = [] &&
      !RamenProcesses.quit = None &&
      Unix.gettimeofday () -. start < output_spec.timeout) in
  let unserialize = RamenSerialization.read_tuple ser nullmask_sz in
  !logger.debug "Enumerating tuples from %s" bname ;
  let%lwt num_tuples =
    RamenSerialization.fold_seq_range ~wait_for_more:true ~while_ bname 0 (fun count _seq tx ->
      let tuple = unserialize tx in
      !logger.debug "Read a tuple out of operation %S"
        (RamenName.string_of_func func.F.name) ;
      tuples_to_find :=
        List.filter (fun (spec, best_miss) ->
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
          if miss = [] then false
          else (
            if !best_miss = [] || compare_miss miss !best_miss < 0 then
              best_miss := miss ;
            true
          )
        ) !tuples_to_find ;
      tuples_to_not_find :=
        List.filter (fun (spec, _) ->
          List.for_all (fun (idx, value) ->
            RamenTypes.to_string tuple.(idx) = value) spec
        ) tuples_must_be_absent |>
        List.rev_append !tuples_to_not_find ;
      return (count + 1)) in
  let success = !tuples_to_find = [] && !tuples_to_not_find = [] in
  let file_spec_print best_miss oc (idx, value) =
    (* Retrieve actual field name: *)
    let n = field_name_of_index idx in
    Printf.fprintf oc "%s=%s" n value ;
    match List.find (fun (idx', s) -> idx = idx') best_miss with
    | exception Not_found -> ()
    | _idx, s -> Printf.fprintf oc " (had %S)" s
  in
  let tuple_spec_print oc (spec, best_miss) =
    List.fast_sort (fun (i1, _) (i2, _) -> Int.compare i1 i2) spec |>
    List.print (file_spec_print !best_miss) oc in
  let err_string_of_tuples lst =
    let len = List.length lst in
    let pref =
      if len <= 1 then "this tuple: "
      else Printf.sprintf "these %d tuples: " len in
    pref ^
    IO.to_string (List.print ~first:"\n  " ~last:"\n" ~sep:"\n  "
                    tuple_spec_print) lst in
  let msg =
    if success then "" else
    (Printf.sprintf "Enumerated %d tuple%s from %s"
      num_tuples (if num_tuples > 0 then "s" else "")
      (RamenName.string_of_fq (F.fq_name func)))^
    (if !tuples_to_find = [] then "" else
      " but could not find "^ err_string_of_tuples !tuples_to_find)^
    (if !tuples_to_not_find = [] then "" else
      " and found "^ err_string_of_tuples !tuples_to_not_find)
  in
  return (success, msg)

let test_notifications notify_rb notif_spec =
  (* We keep pat in order to be able to print it later: *)
  let to_regexp pat = pat, Str.regexp pat in
  let notifs_must_be_absent = List.map to_regexp notif_spec.Notifs.absent
  and notifs_to_find = ref (List.map to_regexp notif_spec.Notifs.present)
  and notifs_to_not_find = ref []
  and start = Unix.gettimeofday () in
  let while_ () =
    if !notifs_to_find <> [] &&
       !notifs_to_not_find = [] &&
       Unix.gettimeofday () -. start < notif_spec.timeout
    then return_true else return_false in
  let%lwt () =
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
        List.rev_append !notifs_to_not_find ;
      return_unit) in
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
  return (success, msg)

let test_one conf root_path notify_rb dirname test =
  (* Hash from func fq name to its rc, bname and mmapped input ring-buffer: *)
  let workers = Hashtbl.create 11 in
  (* The only sure way to know when to stop the workers is: when the test
   * succeeded, or timeouted. So we start three threads at the same time:
   * the process synchronizer, the worker feeder, and the output evaluator: *)
  (* First, write the list of programs that must run and fill workers
   * hash-table: *)
  let%lwt () =
    C.with_wlock conf (fun programs ->
      Hashtbl.clear programs ;
      Lwt_list.iter_p (fun (bin, params) ->
        (* The path to the binary is relative to the test file: *)
        let bin = absolute_path_of ~rel_to:dirname bin in
        let prog = P.of_bin params bin in
        let program_name = (List.hd prog.P.funcs).F.program_name in
        Hashtbl.add programs program_name C.{ bin ; params } ;
        Lwt_list.iter_s (fun func ->
          (* Each function will archive its output: *)
          let%lwt bname = RamenExport.make_temp_export conf func in
          Hashtbl.add workers (F.fq_name func) (func, bname, ref None) ;
          return_unit
        ) prog.P.funcs ;
      ) test.programs) in
  let worker_feeder =
    let feed_input input =
      match Hashtbl.find workers input.Input.operation with
      | exception Not_found ->
          let msg =
            Printf.sprintf2 "Unknown operation: %S (must be one of: %a)"
              (RamenName.string_of_fq input.operation)
              (Enum.print RamenName.fq_print) (Hashtbl.keys workers) in
          lwt_fail_and_quit msg
      | func, _, rbr ->
          let%lwt () =
            if !rbr = None then (
              if func.F.merge_inputs then
                (* TODO: either specify a parent number or pick the first one? *)
                let err = "Writing to merging operations is not \
                           supported yet!" in
                lwt_fail_and_quit err
              else (
                let in_rb = C.in_ringbuf_name_single conf func in
                (* It might not exist already. Instead of waiting for the
                 * worker to start, create it: *)
                RingBuf.create in_rb ;
                let rb = RingBuf.load in_rb in
                rbr := Some rb ;
                return_unit)
            ) else return_unit in
          let rb = Option.get !rbr in
          RamenSerialization.write_record conf func.F.in_type rb input.tuple
    in
    let%lwt () =
      Lwt_list.iter_s (fun input ->
        if !RamenProcesses.quit <> None then return_unit
        else feed_input input
      ) test.inputs in
    Hashtbl.iter (fun _ (_, _, rbr) ->
      Option.may RingBuf.unload !rbr ;
      rbr := None
    ) workers ;
    return_unit in
  (* One tester thread per operation *)
  let%lwt tester_threads =
    hash_fold_s test.outputs (fun user_fq_name output_spec thds ->
      let tester_thread =
        match Hashtbl.find workers user_fq_name with
        | exception Not_found ->
            fun () ->
              lwt_fail_and_quit
                ("Unknown operation "^ RamenName.string_of_fq user_fq_name)
        | tested_func, bname, _rbr ->
            fun () -> test_output tested_func bname output_spec in
      return (tester_thread :: thds)
    ) [] in
  (* Similarly, test the notifications: *)
  let tester_threads =
    (fun () -> test_notifications notify_rb test.notifications) ::
    tester_threads in
  (* Wrap the testers into threads that update this status and set
   * the quit flag: *)
  let all_good = ref true in
  let num_tests_left = ref (List.length tester_threads) in
  let tester_threads =
    List.map (fun thd ->
      let%lwt success, msg = thd () in
      if not success then (
        all_good := false ;
        !logger.error "Failure: %s\n" msg
      ) ;
      decr num_tests_left ;
      if !num_tests_left <= 0 then (
        !logger.info "Finished all tests" ;
        (* Stop all workers *)
        C.with_wlock conf (fun running_programs ->
          Hashtbl.clear running_programs ;
          return_unit)
      ) else return_unit
    ) tester_threads in
  let%lwt () =
    join (worker_feeder :: tester_threads) in
  return !all_good

let run conf root_path test () =
  let conf = { conf with C.persist_dir =
    Filename.get_temp_dir_name ()
      ^"/ramen_test."^ string_of_int (Unix.getpid ()) |>
    uniquify_filename } in
  logger := make_logger conf.C.log_level ;
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
  Lwt_main.run (
    async (fun () ->
      restart_on_failure "wait_all_pids_loop"
        RamenProcesses.wait_all_pids_loop true) ;
    join [
      restart_on_failure "synchronize_running"
        (RamenProcesses.synchronize_running conf) 0. ;
      (
        let%lwt r = test_one conf root_path notify_rb (Filename.dirname test) test_spec in
        res := r ;
        RamenProcesses.quit := Some 0 ;
        return_unit
      ) ]) ;
  RingBuf.unload notify_rb ;
  if !res then !logger.info "Test %s: Success" name
  else failwith ("Test "^ name ^": FAILURE")
