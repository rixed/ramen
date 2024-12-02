open Batteries
open Stdint
open RamenHelpersNoLog
open RamenHelpers
open RamenLog
open RamenSync
open RamenSyncHelpers
module C = RamenConf
module Default = RamenConstsDefault
module DT = DessserTypes
module DM = DessserMasks
module VTC = Value.TargetConfig
module VSI = Value.SourceInfo
module O = RamenOperation
module T = RamenTypes
module N = RamenName
module Export = RamenExport
module Files = RamenFiles
module OutRef = RamenOutRef
module Paths = RamenPaths
module Processes = RamenProcesses
module Services = RamenServices

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
      timeout : float [@ppp_default 20.] }
    [@@ppp PPP_OCaml]
end

module Notifs = struct
  type spec =
    { present : string list [@ppp_default []] ;
      absent : string list [@ppp_default []] ;
      timeout : float [@ppp_default 20.] }
    [@@ppp PPP_OCaml]
end

type program_spec =
  { src : N.path [@ppp_default N.path ""] ;
    ext : string [@ppp_default ""] ;
    params : RamenParams.t [@ppp_default Hashtbl.create 0] }
  [@@ppp PPP_OCaml]

type test_spec =
  { programs : program_spec list ;
    outputs : (N.fq, Output.spec) Hashtbl.t
      [@ppp_default Hashtbl.create 0] }
  [@@ppp PPP_OCaml]

(* Read a tuple described by the given type, and return a hash of fields
 * to string values *)

let end_time = ref 0.

let finish status =
  (* Watch the time before stopping the workers so we can wait for fresh
   * stats: *)
  end_time := Unix.gettimeofday () ;
  RamenProcesses.quit := Some status

let fail_and_quit msg =
  finish 1 ;
  failwith msg

let rec miss_distance exp actual =
  let open RamenTypes in
  (* Fast path: *)
  if exp = actual then 0. else
  match exp, actual with
  | Raql_value.VString e, VString a -> Distance.string e a
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
  | VChar e, VChar a -> Distance.char e a
  | VNull, VNull -> 0.
  | (VTup es, VTup as_)
  | (VVec es, VVec as_)
  | (VArr es, VArr as_) ->
      let len = max (Array.length es) (Array.length as_) in
      let dist = ref 0. in
      for i = 0 to len - 1 do
        match es.(i), as_.(i) with
        | exception _ -> dist := !dist +. 42. (* ? *)
        | a, b -> dist := !dist +. miss_distance a b
      done ;
      !dist
  (* Some large numeric types cannot (always) be reliably compared as floats: *)
  | VI64 e, VI64 a -> Distance.int64 e a
  | VU64 e, VU64 a -> Distance.uint64 e a
  | VI128 e, VI128 a -> Distance.int128 e a
  | VU128 e, VU128 a -> Distance.uint128 e a
  | _ ->
      if exp = VNull || actual = VNull then 1. else
      if is_num (type_of_value exp) &&
         is_num (type_of_value actual) then
        let fos s =
          (* When a float can't be associated with that value just use 0 for now. *)
          try float_of_scalar s |>
              option_get "float_of_scalar of tested value" __LOC__
          with Invalid_argument _ -> 0. in
        Distance.float (fos exp) (fos actual)
      else
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

(* The configuration file gives us tuple spec as a hash, which is
 * convenient to serialize, but for filtering it's more convenient to
 * have a list of field index to values, and a best_miss. While at it
 * replace the given string by an actual RamenTypes.value: *)
let filter_spec_of_spec fq mn spec =
  Hashtbl.enum spec /@
  (fun (field, value) ->
    let idx, field_typ =
      try RamenSerialization.find_field mn field
      with e ->
        Printf.sprintf2 "In function %a: %s"
          N.fq_print fq
          (Printexc.to_string e) |>
        fail_and_quit in
    let what = Printf.sprintf2 "value %S for field %a"
                               value N.field_print field in
    match T.of_string ~what ~mn:field_typ value with
    | Ok v -> idx, v
    | Error e -> fail_and_quit e) |>
  List.of_enum, ref []

let filter_of_tuple_spec (spec, best_miss) tuple =
  (* [miss] is a list of index, value and error (from 0 to 1): *)
  let miss =
    List.fold_left (fun miss (idx, expected) ->
      let actual = tuple.(idx) in
      (* Better not compare float values directly as expected values are
       * entered as strings. But then miss_distance is required to be fast
       * when actual = expected! *)
      let err = miss_distance expected actual in
      !logger.debug "Distance between actual %a and expected %a = %g"
        T.print actual T.print expected err ;
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

let file_spec_print mn best_miss oc (idx, value) =
  (* Retrieve actual field name: *)
  let field_name_of_index i =
    N.field (
      match mn with
      | DT.{ typ = TRec mns ; _ } -> fst (mns.(i))
      | _ -> string_of_int i) in
  let n = field_name_of_index idx in
  Printf.fprintf oc "%a => %a"
    N.field_print n
    T.print value ;
  match List.find (fun (idx', _, _) -> idx = idx') best_miss with
  | exception Not_found -> ()
  | _, a, _ -> Printf.fprintf oc " (had %a)" T.print a

let tuple_spec_print mn oc (spec, best_miss) =
  List.fast_sort (fun (i1, _) (i2, _) -> Int.compare i1 i2) spec |>
  List.print ~first:"{ " ~last:" }" (file_spec_print mn !best_miss) oc

let tuple_print mn oc vs =
  String.print oc "{ " ;
  T.fold_columns (fun i fn _mn ->
    if i > 0 then String.print oc "; " ;
    Printf.fprintf oc "%a => %a"
      N.field_print fn
      T.print vs.(i) ;
      i + 1
  ) 0 mn |> ignore ;
  String.print oc " }"

(* Add a new output ringbuffer to the worker for [fq] and return both its
 * filename, and this worker serialized type: *)
let add_output conf session clt ~while_ fq =
  let out_fname =
    N.path_cat [
      conf.C.persist_dir ; N.path "tests" ;
      N.path_of_fq ~suffix:true fq ; N.path "output.b" ] in
  (* As ramen test can be a bit slow to read ringbuffer, make sure writers get
   * more patience than usual: *)
  RingBuf.create ~timeout:300. out_fname ;
  let _prog, _prog_name, func = function_of_fq clt fq in
  let fieldmask = RamenFieldMaskLib.all_public func.VSI.operation in
  !logger.debug "Fieldmask for %a: %a"
    N.func_print func.VSI.name
    DM.print_mask fieldmask ;
  let now = Unix.time () in
  OutRef.add ~now ~while_ session conf.C.site fq
             (Output_specs_wire.DessserGen.DirectFile out_fname) fieldmask ;
  let ser = O.out_record_of_operation ~with_priv:false func.VSI.operation in
  out_fname, ser

let test_output ~while_ fq output_spec out_fname ser end_flag =
  (* Change the hashtable of field to string value into a list of field
   * indices and values: *)
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
    while_ () &&
    !tuples_to_find <> [] &&
    !tuples_to_not_find = [] &&
    Atomic.Flag.is_unset end_flag &&
    Unix.gettimeofday () -. start < output_spec.timeout in
  let unserialize = RamenSerialization.read_array_of_values ser in
  !logger.debug "Enumerating tuples from %a" N.path_print out_fname ;
  let num_tuples = ref 0 in
  let rb = RingBuf.load out_fname in
  RingBufLib.read_ringbuf ~while_ rb (fun tx ->
    match RamenSerialization.read_tuple unserialize tx with
    | RingBufLib.DataTuple chan, Some tuple ->
        RingBuf.dequeue_commit tx ;
        (* We do no replay on test instance of ramen: *)
        assert (chan = RamenChannel.live) ;
        !logger.debug "Read a tuple out of operation %S" (fq :> string) ;
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
          List.rev_append !tuples_to_not_find ;
        (* Count all tuples including those filtered out: *)
        incr num_tuples
    | _ ->
        RingBuf.dequeue_commit tx) ;
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
      !num_tuples (if !num_tuples > 0 then "s" else "")
      (fq :> string)) ^
    (if !tuples_to_not_find <> [] then
      " and found "^
        (err_string_of_tuples tuple_print) !tuples_to_not_find
    else
      " but could not find "^
        (err_string_of_tuples tuple_spec_print) !tuples_to_find)^
        " (have you tried raising the timeout?)"
  in
  success, msg

(* Perform all kind of checks before spawning testing threads, such as
 * check the existence of all mentioned programs and functions: *)

let check_positive what x =
  if x < 0. then
    Printf.sprintf "%s must be positive (not %g)"
      what x |>
    failwith

let check_unique_names what lst =
  if List.(length lst <> length (unique lst)) then
    Printf.sprintf "Duplicate entries in %s" what |>
    failwith

let check_tuple_spec what tuple =
  let names = Hashtbl.keys tuple |> List.of_enum in
  check_unique_names what names

let check_output_spec spec =
  List.iter (check_tuple_spec "present output fields") spec.Output.present ;
  List.iter (check_tuple_spec "absent output fields") spec.absent ;
  check_positive "timeout" spec.timeout

let src_path_of_src src =
  N.src_path (Files.remove_ext (N.simplified_path src) :> string)

let program_name_of_src src ext =
  let src = (src_path_of_src src :> string) in
  let src = if ext = "" then src else src ^"#"^ ext in
  N.program src

let check_test_spec test session =
  let clt = option_get "check_test" __LOC__ session.ZMQClient.clt in
  (* Start with simple checks that values are positive and names unique: *)
  let names = Hashtbl.keys test.outputs |> List.of_enum in
  check_unique_names "output function names" names ;
  Hashtbl.iter (fun _ spec -> check_output_spec spec) test.outputs ;
  (* Check the specs against actual programs: *)
  let fold_funcs i f =
    let maybe_f fq tuples (s, i as prev) =
      if Set.mem fq s then prev else (
        Set.add fq s,
        f i fq tuples)
    in
    let _, i =
      Hashtbl.fold (fun fq out_spec s_i ->
        let tuples =
          out_spec.Output.present @ out_spec.Output.absent in
        maybe_f fq tuples s_i
      ) test.outputs (Set.empty, i) in
    i in
  let iter_programs ~per_prog ~per_func =
    let maybe_f prog_name s =
      if Set.mem prog_name s then s else (
        per_prog prog_name ;
        Set.add prog_name s) in
    let s =
      List.fold_left (fun s p ->
        let prog_name = program_name_of_src p.src p.ext in
        maybe_f prog_name s
      ) Set.empty test.programs in
    fold_funcs s (fun s fq tuples ->
      let prog_name, func_name = N.fq_parse fq in
      (* Check the existence of program first: *)
      let s = maybe_f prog_name s in
      List.iter (per_func prog_name func_name) tuples ;
      s
    ) |> ignore
  in
  let programs = Hashtbl.create 10 in
  Client.iter ~prefix:"sources/" clt (fun k hv ->
    match k, hv.Client.value with
    | Key.(Sources (src_path, "info")),
      Value.(SourceInfo { detail = Compiled prog ; _ }) ->
        Hashtbl.add programs src_path prog
    | _ -> ()) ;
  iter_programs
    ~per_prog:(fun pn ->
      let src_path = N.src_path_of_program pn in
      let info_k = Key.(Sources (src_path, "info")) in
      if not (Client.mem clt info_k) then
        Printf.sprintf2 "Unknown source %a (have %a)"
          N.src_path_print src_path
          (pretty_enum_print N.src_path_print) (Hashtbl.keys programs) |>
        failwith)
    ~per_func:(fun pn fn tuple ->
      let src_path = N.src_path_of_program pn in
      match Hashtbl.find programs src_path with
      | exception Not_found ->
          Printf.sprintf2 "Cannot find source %a!" N.src_path_print src_path |>
          failwith
      | prog ->
          (match List.find (fun func -> func.VSI.name = fn) prog.VSI.funcs with
          | exception Not_found ->
              Printf.sprintf "Unknown function %s in program %s"
                (N.func_color fn)
                (N.program_color pn) |>
              failwith ;
          | func ->
              let out_fields =
                O.out_type_of_operation ~with_priv:false func.VSI.operation in
              Hashtbl.iter (fun field_name _ ->
                let has_field =
                  List.exists (fun ft ->
                    ft.RamenTuple.name = field_name
                  ) out_fields in
                if not has_field then
                  Printf.sprintf2 "Unknown field %a in %a (have %a)"
                    N.field_print_quoted field_name
                    N.fq_print_quoted (N.fq_of_program pn fn)
                    RamenTuple.print_typ_names out_fields |>
                  failwith
              ) tuple))

let test_literal_programs_root conf =
  N.path_cat [ conf.C.persist_dir ; N.path "tests" ]

let prog_info clt src_path =
  let k = Key.Sources (src_path, "info") in
  match (Client.find clt k).value with
  | Value.(SourceInfo { detail = Compiled prog ; _ }) ->
      prog
  | _ ->
      Printf.sprintf2 "prog_info: %a is not compiled"
        N.src_path_print src_path |>
      invalid_arg

let num_infos clt =
  Client.fold clt ~prefix:"sources/" (fun k hv num ->
    match k, hv.Client.value with
    | Key.Sources (_, "info"),
      Value.(SourceInfo { detail = Compiled _ ; _ }) ->
        num + 1
    (* Abort on fatal errors: *)
    | Key.Sources (src_path, "info"),
      Value.(SourceInfo { detail = Failed { errors = e :: _ ;
                                            depends_on = None } ; _ }) ->
        Printf.sprintf2 "Cannot compile %a: %a"
          N.src_path_print src_path
          RamenRaqlError.print e |>
        failwith
    | _ ->
        num
  ) 0

let num_running site clt =
  let prefix = "sites/"^ (site : N.site :> string) ^"/workers/" in
  Client.fold clt ~prefix (fun k _hv num ->
    match k with
    | Key.PerSite (_, PerWorker (_, PerInstance (_, Pid))) -> num + 1
    | _ -> num
  ) 0

let process_until what ~while_ session cond =
  !logger.info "Waiting until %s" what ;
  let while_ () = while_ () && not (cond ()) in
  ZMQClient.process_until ~while_ session

(* Read all workers stats directly from the confserver internal hash (ie.
 * confserver must run in this program).
 * Returns a hash of N.fq to runtime stats *)
let read_stats session =
  let clt = option_get "read_stats" __LOC__ session.ZMQClient.clt in
  let h = Hashtbl.create 57 in
  Client.iter clt ~prefix:"sites/" (fun k hv ->
    match k, hv.Client.value with
    | Key.PerSite (_, PerWorker (fq, RuntimeStats)),
      Value.RuntimeStats s ->
        Hashtbl.replace h fq s
    | _ ->
        ()) ;
  h

(* We now that the confserver has the latest stats but our client may not have
 * them yet so wait for them: *)
let wait_for_stats session =
  let is_fresh (fq, s) =
    let ok = s.Value.RuntimeStats.stats_time >= !end_time in
    if not ok then
      !logger.info "Stats is too old (%f (%a) < %f (%a)) for worker %a"
        s.stats_time
        print_as_date s.stats_time
        !end_time
        print_as_date !end_time
        N.fq_print fq ;
    ok in
  let rec loop tot_delay =
    ZMQClient.process_in session ;
    let stats = read_stats session in
    if Enum.for_all is_fresh (Hashtbl.enum stats) then
      stats
    else if tot_delay > 5. then (
      !logger.error "Timing out workers stats!" ;
      stats
    ) else (
      let delay = 0.1 in
      Unix.sleepf delay ;
      loop (tot_delay +. delay)
    ) in
  loop 0.

let run_test conf session ~while_ dirname test =
  let clt = option_get "runt_test" __LOC__ session.ZMQClient.clt in
  (* Hash from func fq name to its rc and mmapped input ring-buffer: *)
  let dirname = Files.absolute_path_of dirname in
  let src_file_of_src src =
    (* The path to the source is relative to the test file: *)
    N.path_cat [ dirname ; src ] in
  (* Prepare the list of programs to upload/compile. Notice test.programs
   * can feature several times the same source (with different ext/params) *)
  let programs =
    List.fold_left (fun set p ->
      Set.add p.src set
    ) Set.empty test.programs in
  (* The only sure way to know when to stop the workers is: when the test
   * succeeded, or timeouted. So we start three threads at the same time:
   * the process synchronizer, the worker feeder, and the output evaluator: *)
  (* First, write the list of programs that must run and fill workers
   * hash-table: *)
  Set.iter (fun (src : N.path) ->
    let src_file = src_file_of_src src in
    let src_path = src_path_of_src src in
    (* Upload that source *)
    !logger.debug "Uploading %a" N.src_path_print src_path ;
    let ext = Files.ext src_file in
    if ext = "" then
      failwith "Need an extension to build a source file." ;
    let key = Key.(Sources (src_path, ext))
    and value = Value.of_string (Files.read_whole_file src_file) in
    ZMQClient.send_cmd session (NewKey (key, value, 0., false))
  ) programs ;
  (* Wait until all programs are type-checked: *)
  let num_programs = Set.cardinal programs in
  process_until "all programs are type-checked" ~while_ session (fun () ->
    num_infos clt >= num_programs) ;
  check_test_spec test session ;
  (* Run all of them *)
  let debug = !logger.log_level = Debug in
  let target_config =
    (List.enum test.programs /@
    fun p ->
      let program_name = program_name_of_src p.src p.ext in
      VTC.{ program = program_name ;
            params =
              Hashtbl.enum p.params /@
              (fun (name, value) ->
                Program_run_parameter.DessserGen.{ name ; value }) |>
              Array.of_enum ;
            enabled = true ; debug ;
            report_period = Default.report_period ;
            cwd = Files.dirname (src_file_of_src p.src) ;
            on_site = "*" ; automatic = false }) |>
    Array.of_enum in
  let key = Key.TargetConfig
  and value = Value.TargetConfig target_config in
  ZMQClient.send_cmd session (SetKey (key, value)) ;
  (* Wait until the programs are running *)
  let num_funcs =
    List.fold_left (fun num p ->
      let prog = prog_info clt (src_path_of_src p.src) in
      num + List.length prog.funcs
    ) 0 test.programs in
  let what = string_of_int num_funcs ^" workers are running" in
  process_until what ~while_ session (fun () ->
    let n = num_running conf.C.site clt in
    !logger.debug "%d workers are currently running" n ;
    n >= num_funcs) ;
  (* Add an output to monitored workers, then signal all workers to start,
   * then start monitoring the workers: *)
  (* One tester thread per operation *)
  let tester_threads =
    Hashtbl.fold (fun fq output_spec thds ->
      (* test must be configured in the main thread (ZMQ...) *)
      let out_fname, ser =
        add_output conf session clt ~while_ fq in
      test_output ~while_ fq output_spec out_fname ser :: thds
    ) test.outputs [] in
  !logger.info "Signaling all workers to continue" ;
  let prefix = "sites/"^ (conf.C.site : N.site :> string) ^"/workers/" in
  Client.iter clt ~prefix (fun k hv ->
    match k, hv.Client.value with
    | Key.PerSite (_, PerWorker (fq, PerInstance (_, Pid))),
      Value.(RamenValue T.(VU32 v)) ->
        let pid = Uint32.to_int v in
        !logger.debug "Signaling %a (pid %d) to continue" N.fq_print fq pid ;
        Unix.kill pid Sys.sigcont
    | _ -> ()) ;
  (* This flag will signal the end to either both tester and early_termination
   * threads: *)
  let end_flag = Atomic.Flag.make false in
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
          Atomic.Flag.set end_flag
      ) ()
    ) tester_threads in
  process_until "all tests are finished" ~while_ session (fun () ->
    Atomic.Flag.is_set end_flag) ;
  List.iter Thread.join tester_threads ;
  !all_good

let run conf server_url api graphite use_external_compiler max_simult_compils
        smt_solver dessser_codegen opt_level test_file =
  (* Tweak the configuration specifically for running tests: *)
  RamenCliCheck.non_empty "test file name" (test_file : N.path :> string) ;
  let persist_dir =
    Filename.get_temp_dir_name ()
      ^"/ramen_test."^ string_of_int (Unix.getpid ()) |>
    N.path |> Files.uniquify in
  let confserver_port = random_port () in
  let sync_url = "localhost:"^ string_of_int confserver_port in
  let username = "_test" in
  (* Note: the test flag will make supervisor stop the workers.
   * TODO: have a test flag in VTC.entry instead. *)
  let conf =
    C.{ conf with persist_dir ; username ; sync_url ; test = true } in
  (* Init various modules: *)
  init_logger ~with_time:conf.C.log_with_time conf.log_level ;
  RamenSmt.solver := smt_solver ;
  RamenCompiler.init use_external_compiler max_simult_compils dessser_codegen
                     opt_level ;
  !logger.info "Using temp dir %a" N.path_print conf.persist_dir ;
  Files.mkdir_all conf.persist_dir ;
  RamenProcesses.prepare_signal_handlers conf ;
  (* Parse tests so that we won't have to clean anything if it's bogus *)
  !logger.debug "Parsing test specification in %a..."
    N.path_print_quoted test_file ;
  let test_spec = Files.ppp_of_file test_spec_ppp_ocaml test_file in
  let name = (Files.(basename test_file |> remove_ext) :> string) in
  (*
   * Start all services as threads
   *)
  let while_ () = !RamenProcesses.quit = None in
  let no_key = N.path "" in
  !logger.info "Running local confserver on port %d..." confserver_port ;
  let thread_create f =
    Thread.create (fun () ->
      try f () with Exit -> ()) () in
  (* The confserver must be the last standing so it can transmit the workers
   * stats: *)
  let quit_confserver = ref false in
  let confserver_thread =
    thread_create (fun () ->
      set_thread_name "confserver" ;
      let bind_addr = "*:"^ string_of_int confserver_port in
      RamenSyncZMQServer.start
        ~while_:(fun () -> not !quit_confserver)
        conf [ bind_addr ] [] no_key no_key true true 0 0. 0. 0 10 false) in
  (* FIXME: wait until confserver has a chance to create the initial keys: *)
  Unix.sleep 1 ;
  !logger.info "Running local supervisor..." ;
  let supervisor_thread =
    thread_create (fun () ->
      set_thread_name "supervisor" ;
      let conf = { conf with username = "_supervisor" } in
      RamenSupervisor.synchronize_running conf ~while_ true) in
  !logger.info "Running local choreographer..." ;
  let choreographer_thread =
    thread_create (fun () ->
      set_thread_name "choreographer" ;
      let conf = { conf with username = "_choreographer" } in
      RamenChoreographer.start conf ~while_) in
  !logger.info "Running local precompserver..." ;
  let precompserver_thread =
    thread_create (fun () ->
      set_thread_name "precompserver" ;
      let conf = { conf with username = "_precompserver" } in
      RamenPrecompserver.start conf ~while_) in
  let execompserver_thread =
    thread_create (fun () ->
      set_thread_name "execompserver" ;
      let conf = { conf with username = "_execompserver" } in
      RamenExecompserver.start conf ~while_) in
  (* httpd is special: it is run on demand, and then will prevent exit
   * at the end. The idea is to allow user to check test results and stats
   * via the graphite API. Soon to be replaced with the GUI dashboards. *)
  let httpd_thread =
    if server_url = "" && api = None && graphite = None then None
    else Some (
      !logger.info "Running local httpd..." ;
      thread_create (fun () ->
        set_thread_name "httpd" ;
        let conf = { conf with username = "_httpd" } in
        RamenHttpd.run_httpd conf server_url api "" graphite 0.0)) in
  (* Helps with logs mangling: *)
  Unix.sleepf 0.5 ;
  (*
   * Run all tests. Also return the syn thread that's still running:
   *)
  !logger.info "Starting tests..." ;
  let topics =
    [ (* To know when sources are precompiled: *)
      "sources/*/info" ;
      (* To know which workers are running and to signal them: *)
      "sites/*/workers/*/instances/*/pid" ;
      (* To modify the output specs of workers: *)
      "sites/*/workers/*/outputs" ;
      (* To display resources *)
      "sites/*/workers/*/stats/runtime" ] in
  let join_thread what thd =
    !logger.debug "Waiting for %s termination..." what ;
    Thread.join thd in
  let ok = ref false in
  log_exceptions ~what:"run test" (fun () ->
    start_sync conf ~while_ ~topics ~recvtimeo:1. (fun session ->
      ok := run_test conf session ~while_ (Files.dirname test_file) test_spec ;
      !logger.debug "Finished tests" ;
      (* Stop the services: *)
      if httpd_thread = None then finish 0 ;
      (* else wait for the user to kill *)
      Option.may (join_thread "httpd") httpd_thread ;
      join_thread "supervisor" supervisor_thread ;
      join_thread "choreographer" choreographer_thread ;
      join_thread "precompserver" precompserver_thread ;
      join_thread "execompserver" execompserver_thread ;
      (* Show resources consumption: *)
      let stats = wait_for_stats session in
      !logger.info "Resources:%a"
        (Hashtbl.print ~first:"\n\t" ~last:"" ~kvsep:"\t" ~sep:"\n\t"
          N.fq_print
          (fun oc s ->
            let err_count = s.Value.RuntimeStats.tot_out_errs in
            let err_report =
              if Uint64.(err_count > zero) then
                red (" ("^ Uint64.to_string err_count ^" errors!)")
              else
                "" in
            Printf.fprintf oc "cpu:%fs\tmax ram:%s%s"
              s.Value.RuntimeStats.tot_cpu
              (Uint64.to_string s.max_ram)
              err_report))
          stats)) ;
  if !ok then !logger.info "Test %s: Success" name
  else !logger.error "Test %s: FAILURE" name ;
  quit_confserver := true ;
  join_thread "confserver" confserver_thread ;
  if not !ok then exit 1
