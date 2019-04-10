open Batteries
open Stdint
open RamenHelpers
open RamenLog
open RamenNullable
module C = RamenConf
module F = C.Func
module P = C.Program
module O = RamenOperation
module T = RamenTypes
module N = RamenName
module Files = RamenFiles

type tuple_spec = (N.field, string) Hashtbl.t [@@ppp PPP_OCaml]

module Input = struct
  type spec =
    { pause : float [@ppp_default 0.] ;
      operation : N.fq ;
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
  { programs : (N.program, program_spec) Hashtbl.t ;
    inputs : Input.spec list [@ppp_default []] ;
    outputs : (N.fq, Output.spec) Hashtbl.t
      [@ppp_default Hashtbl.create 0] ;
    until : (N.fq, tuple_spec) Hashtbl.t
      [@ppp_default Hashtbl.create 0] ;
    (* Notifications likely useless now that we can tests #notifs: *)
    notifications : Notifs.spec
      [@ppp_default Notifs.{ present=[]; absent=[]; timeout=0. }] }
    [@@ppp PPP_OCaml]

and program_spec =
  { bin : N.path [@ppp_default N.path ""] ;
    code : string [@ppp_default ""] ;
    params : RamenParams.t [@ppp_default Hashtbl.create 0] }
  [@@ppp PPP_OCaml]

(* Read a tuple described by the given type, and return a hash of fields
 * to string values *)

let fail_and_quit msg =
  RamenProcesses.quit := Some 1 ;
  failwith msg

let rec miss_distance exp actual =
  let open RamenTypes in
  (* Fast path: *)
  if exp = actual then 0. else
  if exp = VNull || actual = VNull then 1. else
  if is_a_num (structure_of exp) &&
     is_a_num (structure_of actual)
  then
    let fos s =
      (* When a float can't be associated with that value just use 0 for now. *)
      try float_of_scalar s |> option_get "float_of_scalar of tested value"
      with Invalid_argument _ -> 0. in
    Distance.float (fos exp) (fos actual)
  else match exp, actual with
  | VString e, VString a -> Distance.string e a
  | VEth e, VEth a -> Distance.string (RamenEthAddr.to_string e)
                                      (RamenEthAddr.to_string a)
  | VIpv4 e, VIpv4 a -> Distance.string (RamenIpv4.to_string e)
                                        (RamenIpv4.to_string a)
  | VIpv6 e, VIpv6 a -> Distance.string (RamenIpv6.to_string e)
                                        (RamenIpv6.to_string a)
  | VIp e, VIp a -> Distance.string (RamenIp.to_string e)
                                    (RamenIp.to_string a)
  | VCidrv4 e, VCidrv4 a -> Distance.string (RamenIpv4.Cidr.to_string e)
                                            (RamenIpv4.Cidr.to_string a)
  | VCidrv6 e, VCidrv6 a -> Distance.string (RamenIpv6.Cidr.to_string e)
                                            (RamenIpv6.Cidr.to_string a)
  | VCidr e, VCidr a -> Distance.string (RamenIp.Cidr.to_string e)
                                        (RamenIp.Cidr.to_string a)
  | VNull, VNull -> 0.
  | (VTuple es, VTuple as_)
  | (VVec es, VVec as_)
  | (VList es, VList as_) ->
      Array.map2 miss_distance es as_ |>
      Array.reduce (+.)
  | _ ->
      Printf.sprintf2 "Cannot compare %a with %a"
        print exp
        print actual |>
      fail_and_quit

let compare_miss bad1 bad2 =
  (* Favor having the less possible wrong values, and then look at the
   * distance between expected and actual values: *)
  match Int.compare (List.length bad1) (List.length bad2) with
  | 0 ->
      let tot_err = List.fold_left (fun s (_, _, err) -> s +. err) 0. in
      Float.compare (tot_err bad1) (tot_err bad2)
  | c -> c

let field_index_of_name fq typ field =
  try
    List.findi (fun _ ftyp ->
      ftyp.RamenTuple.name = field
    ) typ
  with Not_found ->
    Printf.sprintf2 "Unknown field %a in %a, which has only %a"
      N.field_print field
      N.fq_print fq
      RamenTuple.print_typ_names typ |>
    fail_and_quit

let field_name_of_index typ idx =
  (List.nth typ idx).RamenTuple.name

(* The configuration file gives us tuple spec as a hash, which is
 * convenient to serialize, but for filtering it's more convenient to
 * have a list of field index to values, and a best_miss. While at it
 * replace the given string by an actual RamenTypes.value: *)
let filter_spec_of_spec fq typ spec =
  Hashtbl.enum spec /@
  (fun (field, value) ->
    let idx, field_typ = field_index_of_name fq typ field in
    let what = Printf.sprintf2 "value %S for field %a"
                               value N.field_print field in
    match T.of_string ~what ~typ:field_typ.RamenTuple.typ value with
    | Result.Ok v -> idx, v
    | Result.Bad e -> fail_and_quit e) |>
  List.of_enum, ref []

(* Do not use RamenExport.read_output filter facility because of
 * best_miss: *)
let filter_of_tuple_spec (spec, best_miss) tuple =
  (* [miss] is a list of index, value and error (from 0 to 1): *)
  let miss =
    List.fold_left (fun miss (idx, expected) ->
      let actual = tuple.(idx) in
      (* Better not compare float values directly as expected values are
       * entered as strings. But then miss_distance is required to be fast
       * when actual = expected! *)
      let err = miss_distance expected actual in
      let ok = err < 1e-7 in
      if ok then miss else (
        !logger.debug "found %a instead of %a (err=%f)"
          T.print actual
          T.print expected
          err ;
        (idx, actual, err)::miss)
    ) [] spec in
  if miss = [] then true
  else (
    if !best_miss = [] || compare_miss miss !best_miss < 0 then
      best_miss := miss ;
    false
  )

let file_spec_print typ best_miss oc (idx, value) =
  (* Retrieve actual field name: *)
  let n = field_name_of_index typ idx in
  Printf.fprintf oc "%a => %a"
    N.field_print n
    T.print value ;
  match List.find (fun (idx', _, _) -> idx = idx') best_miss with
  | exception Not_found -> ()
  | _, a, _ -> Printf.fprintf oc " (had %a)" T.print a

let tuple_spec_print typ oc (spec, best_miss) =
  List.fast_sort (fun (i1, _) (i2, _) -> Int.compare i1 i2) spec |>
  List.print ~first:"{ " ~last:" }" (file_spec_print typ !best_miss) oc

let tuple_print typ oc vs =
  String.print oc "{ " ;
  List.iteri (fun i ft ->
    if i > 0 then String.print oc "; " ;
    Printf.fprintf oc "%a => %a"
      N.field_print ft.RamenTuple.name
      T.print vs.(i)
  ) typ ;
  String.print oc " }"

let test_output conf fq output_spec end_flag =
  (* Notice that although we do not provide a filter read_output can
   * return one, to select the worker in well-known functions: *)
  let bname, _is_temp_export, filter, _typ, ser, _params, _event_time =
    RamenExport.read_output conf fq [] in
  (* Change the hashtable of field to string value into a list of field
   * index and value: *)
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
    Atomic.Flag.is_unset end_flag &&
    !tuples_to_find <> [] &&
    !tuples_to_not_find = [] &&
    !RamenProcesses.quit = None &&
    Unix.gettimeofday () -. start < output_spec.timeout in
  let unserialize = RamenSerialization.read_array_of_values ser in
  !logger.debug "Enumerating tuples from %a" N.path_print bname ;
  let num_tuples =
    RamenSerialization.fold_seq_range
      ~wait_for_more:true ~while_ bname 0 (fun count _seq tx ->
      match RamenSerialization.read_tuple unserialize tx with
      | RingBufLib.DataTuple chan, Some tuple ->
        (* We do no replay on test instance of ramen: *)
        assert (chan = RamenChannel.live) ;
        if filter tuple then (
          !logger.debug "Read a tuple out of operation %S"
            (fq :> string) ;
          tuples_to_find :=
            List.filter (fun filter_spec ->
              not (filter_of_tuple_spec filter_spec tuple)
            ) !tuples_to_find ;
          tuples_to_not_find :=
            List.filter_map (fun (spec, _) ->
              if List.for_all (fun (idx, value) ->
                   tuple.(idx) = value
                 ) spec
              then (* Store the whole input for easier debugging *)
                Some tuple
              else None
            ) tuples_must_be_absent |>
            List.rev_append !tuples_to_not_find) ;
        (* Count all tuples including those filtered out: *)
        count + 1
      | _ -> count) in
  let success = !tuples_to_find = [] && !tuples_to_not_find = []
  in
  let err_string_of_tuples p lst =
    let len = List.length lst in
    let pref =
      if len <= 1 then "this tuple: "
      else Printf.sprintf "these %d tuples: " len in
    pref ^
    IO.to_string
      (List.print ~first:"\n  " ~last:"\n" ~sep:"\n  " (p ser)) lst in
  let msg =
    if success then "" else
    (Printf.sprintf "Enumerated %d tuple%s from %s"
      num_tuples (if num_tuples > 0 then "s" else "")
      (fq :> string)) ^
    (if !tuples_to_not_find <> [] then
      " and found "^
        (err_string_of_tuples tuple_print) !tuples_to_not_find
    else
      " but could not find "^
        (err_string_of_tuples tuple_spec_print) !tuples_to_find)
  in
  success, msg

(* Wait for the given tuple: *)
let test_until conf count end_flag fq spec =
  let bname, _is_temp_export, filter, _typ, typ, _params, _event_time =
    RamenExport.read_output conf fq [] in
  let filter_spec = filter_spec_of_spec fq typ spec in
  let unserialize = RamenSerialization.read_array_of_values typ in
  let got_it = ref false in
  let while_ () =
    not !got_it &&
    !RamenProcesses.quit = None &&
    Atomic.Flag.is_unset end_flag in
  !logger.debug "Enumerating tuples from %a for early termination"
    N.path_print bname ;
  RamenSerialization.fold_seq_range ~wait_for_more:true ~while_ bname ()
    (fun () _ tx ->
    match RamenSerialization.read_tuple unserialize tx with
    | RingBufLib.DataTuple chan, Some tuple ->
      assert (chan = RamenChannel.live) ;
      if filter tuple && filter_of_tuple_spec filter_spec tuple then (
        !logger.info "Got terminator tuple from function %S"
          (fq :> string) ;
        got_it := true ;
        Atomic.Counter.incr count)
    | _ -> ())

let test_notifications notify_rb notif_spec end_flag =
  (* We keep pat in order to be able to print it later: *)
  let to_regexp pat = pat, Str.regexp pat in
  let notifs_must_be_absent = List.map to_regexp notif_spec.Notifs.absent
  and notifs_to_find = ref (List.map to_regexp notif_spec.Notifs.present)
  and notifs_to_not_find = ref []
  and start = Unix.gettimeofday () in
  let while_ () =
    Atomic.Flag.is_unset end_flag &&
    !notifs_to_find <> [] &&
    !notifs_to_not_find = [] &&
    Unix.gettimeofday () -. start < notif_spec.timeout in
  RamenSerialization.read_notifs ~while_ notify_rb
    (fun (site, worker, _sent_time, _event_time, notif_name, firing, _certainty,
          _parameters) ->
      let firing = option_of_nullable firing in
      !logger.debug "Got %snotification from %s%s: %S"
        (if firing = Some false then "stopping " else "firing ")
        (if site <> "" then site ^":" else "")
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

(* Perform all kind of checks before spawning testing threads, such as
 * check the existence of all mentioned programs and functions: *)
let check_test_spec conf test =
  let fold_funcs i f =
    let maybe_f fq tuples (s, i as prev) =
      if Set.mem fq s then prev else (
        Set.add fq s,
        f i fq tuples)
    in
    let s_i =
      List.fold_left (fun s_i in_spec ->
        maybe_f in_spec.Input.operation [ in_spec.Input.tuple ] s_i
      ) (Set.empty, i) test.inputs in
    let s_i =
      Hashtbl.fold (fun fq out_spec s_i ->
        let tuples =
          out_spec.Output.present @ out_spec.Output.absent in
        maybe_f fq tuples s_i
      ) test.outputs s_i in
    let _, i =
      Hashtbl.fold (fun fq tuple s_i ->
        maybe_f fq [ tuple ] s_i
      ) test.until s_i in
    i in
  let iter_programs ~per_prog ~per_func =
    let maybe_f prog_name s =
      if Set.mem prog_name s then s else (
        per_prog prog_name ;
        Set.add prog_name s) in
    let s =
      Hashtbl.fold (fun prog_name _ s ->
        maybe_f prog_name s
      ) test.programs Set.empty in
    fold_funcs s (fun s fq tuples ->
      let prog_name, func_name = N.fq_parse fq in
      (* Check the existence of program first: *)
      let s = maybe_f prog_name s in
      List.iter (per_func prog_name func_name) tuples ;
      s
    ) |> ignore
  in
  C.with_rlock conf (fun programs ->
    iter_programs
      ~per_prog:(fun pn ->
        if not (Hashtbl.mem programs pn) then
          Printf.sprintf2 "Unknown program %s (have %a)"
            (N.program_color pn)
            (pretty_enum_print N.program_print) (Hashtbl.keys programs) |>
          failwith)
      ~per_func:(fun pn fn tuple ->
        let _mre, get_rc = Hashtbl.find programs pn in
        let prog = get_rc () in
        match List.find (fun func -> func.F.name = fn) prog.P.funcs with
        | exception Not_found ->
            Printf.sprintf "Unknown function %s in program %s"
              (N.func_color fn)
              (N.program_color pn) |>
            failwith ;
        | func ->
            let out_type =
              O.out_type_of_operation func.F.operation in
            Hashtbl.iter (fun field_name _ ->
              if not (List.exists (fun ft ->
                        ft.RamenTuple.name = field_name
                      ) out_type) then
                Printf.sprintf2 "Unknown field %a in %a (have %a)"
                  N.field_print_quoted field_name
                  N.fq_print_quoted (N.fq_of_program pn fn)
                  RamenTuple.print_typ_names out_type |>
                failwith
            ) tuple))

let bin_of_program conf get_parent program_name program_code =
  let exec_file =
    N.path_cat [ C.test_literal_programs_root conf ;
                 Files.add_ext (N.path_of_program program_name) "x" ]
  and source_file =
    N.path_cat [ C.test_literal_programs_root conf ;
                 Files.add_ext (N.path_of_program program_name) "ramen" ] in
  File.with_file_out ~mode:[`create; `text ; `trunc] (source_file :> string)
    (fun oc -> String.print oc program_code) ;
  RamenCompiler.compile conf get_parent ~exec_file
                        source_file program_name ;
  exec_file

let run_test conf notify_rb dirname test =
  (* Hash from func fq name to its rc and mmapped input ring-buffer: *)
  let workers = Hashtbl.create 11 in
  let dirname = Files.absolute_path_of dirname in
  (* The only sure way to know when to stop the workers is: when the test
   * succeeded, or timeouted. So we start three threads at the same time:
   * the process synchronizer, the worker feeder, and the output evaluator: *)
  (* First, write the list of programs that must run and fill workers
   * hash-table: *)
  C.with_wlock conf (fun programs ->
    Hashtbl.clear programs ;
    Hashtbl.iter (fun program_name p ->
      let bin =
        if not (N.is_empty p.bin) then (
          (* The path to the binary is relative to the test file: *)
          if Files.is_absolute p.bin then p.bin
          else N.path_cat [ dirname ; p.bin ]
        ) else (
          if p.code = "" then failwith "Either the binary file or the code of \
                                        a program must be specified" ;
          let get_parent n =
            match Hashtbl.find test.programs n with
            | exception Not_found ->
                Printf.sprintf "Cannot find program %s" (N.program_color n) |>
                fail_and_quit
            | par -> P.of_bin n par.params par.bin
          in
          bin_of_program conf get_parent program_name p.code) in
      let prog = P.of_bin program_name p.params bin in
      Hashtbl.add programs program_name
        C.{ bin ; params = p.params ; status = MustRun ; debug = false ;
            report_period = RamenConsts.Default.report_period ;
            src_file = N.path "" ; on_site = Globs.all ; automatic = false } ;
      List.iter (fun func ->
        Hashtbl.add workers (F.fq_name func) (func, ref None)
      ) prog.P.funcs
    ) test.programs) ;
  check_test_spec conf test ;
  (* Now that the running config is complete we can start the worker
   * supervisor: *)
  let sync =
    let open RamenProcesses in
    Thread.create (
      restart_on_failure "synchronize_running"
        (synchronize_running conf)) 0. in
  (* Start the test proper: *)
  let worker_feeder () =
    let feed_input input =
      match Hashtbl.find workers input.Input.operation with
      | exception Not_found ->
          let msg =
            Printf.sprintf2 "Unknown operation: %S (must be one of: %a)"
              (input.operation :> string)
              (Enum.print N.fq_print) (Hashtbl.keys workers) in
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
          RamenSerialization.write_record func.F.in_type rb input.tuple
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
  let end_flag = Atomic.Flag.make false in
  let stop_workers =
    let all = [ Globs.compile "*" ] in
    fun () ->
      ignore (RamenRun.kill conf all) ;
      Atomic.Flag.set end_flag in
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
  let num_tests_left = Atomic.Counter.make (List.length tester_threads) in
  let tester_threads =
    List.map (fun thd ->
      Thread.create (fun () ->
        (match thd end_flag with
        | exception e ->
            all_good := false ;
            !logger.error "Exception: %s" (Printexc.to_string e)
        | success, msg ->
            if not success then (
              all_good := false ;
              !logger.error "Failure: %s" msg)) ;
        Atomic.Counter.decr num_tests_left ;
        if Atomic.Counter.get num_tests_left <= 0 then
          stop_workers ()
      ) ()
    ) tester_threads in
  (* Finally, a thread that tests the ending condition: *)
  let until_count = Atomic.Counter.make 0 in
  let early_terminator =
    if Hashtbl.is_empty test.until then
      Thread.create RamenProcesses.until_quit (fun () ->
        if Atomic.Flag.is_set end_flag then false else (Unix.sleep 1 ; true))
    else (
      Thread.create (fun () ->
        Hashtbl.fold (fun user_fq_name tuple_spec thds ->
          Thread.create (test_until conf until_count end_flag user_fq_name) tuple_spec :: thds
        ) test.until [] |>
        List.iter Thread.join ;
        !logger.debug "Early termination detectors ended." ;
        if Atomic.Counter.get until_count = Hashtbl.length test.until then
          all_good := false ;
        stop_workers ()) ()
    ) in
  !logger.debug "Waiting for test threads..." ;
  List.iter Thread.join
    ((Thread.create worker_feeder ()) :: tester_threads) ;
  !logger.debug "Waiting for thread early_terminator..." ;
  Thread.join early_terminator ;
  !all_good, sync

let run conf server_url api graphite
        use_external_compiler max_simult_compils smt_solver
        test () =
  let conf = C.{ conf with
    persist_dir =
      Filename.get_temp_dir_name ()
        ^"/ramen_test."^ string_of_int (Unix.getpid ()) |>
      N.path |> Files.uniquify ;
    test = true } in
  init_logger conf.C.log_level ;
  RamenCompiler.init use_external_compiler max_simult_compils smt_solver ;
  !logger.info "Using temp dir %a" N.path_print conf.persist_dir ;
  Files.mkdir_all conf.persist_dir ;
  let httpd_thread =
    if server_url = "" && api = None && graphite = None then None
    else Some (
      Thread.create (fun () ->
        RamenHttpd.run_httpd conf server_url api graphite 0.0
      ) ()) in
  (* Parse tests so that we won't have to clean anything if it's bogus *)
  !logger.info "Parsing test specification in %a..."
    N.path_print_quoted test ;
  let test_spec = Files.ppp_of_file test_spec_ppp_ocaml test in
  let name = (Files.(basename test |> remove_ext) :> string) in
  (* Start Ramen *)
  RamenProcesses.prepare_signal_handlers conf ;
  let notify_rb = RamenProcesses.prepare_notifs conf in
  let report_rb = RamenProcesses.prepare_reports conf in
  RingBuf.unload report_rb ;
  (* Run all tests. Also return the syn thread that's still running: *)
  !logger.info "Starting tests..." ;
  let res, sync = run_test conf notify_rb (Files.dirname test) test_spec in
  !logger.debug "Finished tests" ;
  RingBuf.unload notify_rb ;
  (* Show resources consumption: *)
  let stats = RamenPs.read_stats conf in
  !logger.info "Resources:%a"
    (Hashtbl.print ~first:"\n\t" ~last:"" ~kvsep:"\t" ~sep:"\n\t"
      (fun oc (fq, is_top_half) ->
        assert (not is_top_half) ;
        N.fq_print oc fq)
      (fun oc s ->
        Printf.fprintf oc "cpu:%fs\tmax ram:%s"
          s.RamenPs.cpu (Uint64.to_string s.max_ram)))
      stats ;
  if res then !logger.info "Test %s: Success" name
  else failwith ("Test "^ name ^": FAILURE") ;
  if httpd_thread = None then
    RamenProcesses.quit := Some 0 ;
  (* else wait for the user to kill *)
  !logger.debug "Waiting for workers supervisor..." ;
  Thread.join sync ;
  Option.may (fun thd ->
    !logger.debug "Waiting for http server..." ;
    Thread.join thd
  ) httpd_thread
