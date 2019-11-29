open Batteries
open Stdint
open RamenHelpers
open RamenConsts
open RamenLog
open RamenNullable
open RamenSync
module C = RamenConf
module RC = C.Running
module F = C.Func
module P = C.Program
module O = RamenOperation
module T = RamenTypes
module N = RamenName
module Files = RamenFiles
module OutRef = RamenOutRef
module Supervisor = RamenSupervisor
module Processes = RamenProcesses
module FuncGraph = RamenFuncGraph
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
  match exp, actual with
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
  | VChar e, VChar a -> Distance.char e a
  | VNull, VNull -> 0.
  | (VTuple es, VTuple as_)
  | (VVec es, VVec as_)
  | (VList es, VList as_) ->
      Array.map2 miss_distance es as_ |>
      Array.reduce (+.)
  (* Some large numeric types cannot (always) be reliably compared as floats: *)
  | VI64 e, VI64 a -> Distance.int64 e a
  | VU64 e, VU64 a -> Distance.uint64 e a
  | VI128 e, VI128 a -> Distance.int128 e a
  | VU128 e, VU128 a -> Distance.uint128 e a
  | _ ->
      if exp = VNull || actual = VNull then 1. else
      if is_a_num (structure_of exp) &&
         is_a_num (structure_of actual) then
        let fos s =
          (* When a float can't be associated with that value just use 0 for now. *)
          try float_of_scalar s |> option_get "float_of_scalar of tested value"
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

let test_output conf fq output_spec programs end_flag =
  (* Notice that although we do not provide a filter read_output can
   * return one, to select the worker in well-known functions: *)
  let bname, _is_temp_export, filter, ser, _params, _event_time =
    RamenExport.read_output conf fq [] programs in
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
let test_until conf count end_flag fq spec programs =
  let bname, _is_temp_export, filter, typ, _params, _event_time =
    RamenExport.read_output conf fq [] programs in
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

let check_input_spec spec =
  check_positive "input pause" spec.Input.pause ;
  check_tuple_spec "input field names" spec.tuple

let check_output_spec spec =
  List.iter (check_tuple_spec "present output fields") spec.Output.present ;
  List.iter (check_tuple_spec "absent output fields") spec.absent ;
  check_positive "timeout" spec.timeout

let check_notification spec =
  check_unique_names "present notifications" spec.Notifs.present ;
  check_unique_names "absent notifications" spec.absent ;
  check_positive "timeout" spec.timeout

let check_test_spec test programs =
  (* Start with simple checks that values are positive and names unique: *)
  List.iter check_input_spec test.inputs ;
  let names = Hashtbl.keys test.outputs |> List.of_enum in
  check_unique_names "output function names" names ;
  Hashtbl.iter (fun _ spec -> check_output_spec spec) test.outputs ;
  check_notification test.notifications ;
  (* Check the specs against actual programs: *)
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
            O.out_type_of_operation ~with_private:false
                                    func.F.operation in
          Hashtbl.iter (fun field_name _ ->
            if not (List.exists (fun ft ->
                      ft.RamenTuple.name = field_name
                    ) out_type) then
              Printf.sprintf2 "Unknown field %a in %a (have %a)"
                N.field_print_quoted field_name
                N.fq_print_quoted (N.fq_of_program pn fn)
                RamenTuple.print_typ_names out_type |>
              failwith
          ) tuple)

let bin_of_program conf get_parent program_name program_code =
  let exec_file =
    N.path_cat
      [ C.test_literal_programs_root conf ;
        Files.add_ext (N.path_of_program ~suffix:false program_name) "x" ]
  and source_file =
    N.path_cat
      [ C.test_literal_programs_root conf ;
        Files.add_ext (N.path_of_program ~suffix:false program_name) "ramen" ] in
  File.with_file_out ~mode:[`create; `text ; `trunc] (source_file :> string)
    (fun oc -> String.print oc program_code) ;
  RamenMake.(apply_rule conf ~force_rebuild:true get_parent program_name
                        source_file exec_file bin_rule) ;
  exec_file

(*
 * Variant of Supervisor functions that works without the confserver:
 *)

(* We use the same key for both [must_run] and [running] to make the
 * comparison easier, although strictly speaking the signature is useless
 * in [must_run]: *)
type key =
  { program_name : N.program ;
    func_name : N.func ;
    func_signature : string ;
    params : RamenTuple.params ;
    role : Value.Worker.role }

(* What we store in the [must_run] hash: *)
type must_run_entry =
  { key : key ;
    rce : RC.entry ;
    func : F.t ;
    (* Actual workers not only logical parents as in func.parent: *)
    parents : (N.site * P.t * F.t) list }

(* Description of a running worker that is stored in the [running] hash.
 * Not persisted on disk.
 * Some of this is updated as the configuration change. *)
type running_process =
  { key : key ;
    params : RamenParams.t ; (* The ones in RCE only! *)
    bin : N.path ;
    func : F.t ;
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
    Value.Worker.print_role proc.key.role
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
          (if is_err then !logger.error else Supervisor.info_or_test conf)
            "%s %s." what status_str ;
          proc.last_exit <- now ;
          proc.last_exit_status <- status_str ;
          let input_ringbufs = C.in_ringbuf_names conf proc.func in
          if is_err then
            proc.succ_failures <- proc.succ_failures + 1
          else
            proc.succ_failures <- 0 ;
          (* In case the worker stopped on its own, remove it from its
           * parents out_ref: *)
          let out_refs =
            List.map (fun (_, _, pfunc) ->
              C.out_ringbuf_names_ref conf pfunc
            ) proc.parents in
          Supervisor.cut_from_parents_outrefs input_ringbufs out_refs pid ;
          (* Wait before attempting to restart a failing worker: *)
          let max_delay = float_of_int proc.succ_failures in
          proc.quarantine_until <-
            now +. Random.float (min 90. max_delay) ;
          proc.pid <- None)
    ) proc.pid
  ) running

let really_start conf proc =
  let envvars =
    O.envvars_of_operation proc.func.operation |>
    List.map (fun (n : N.field) ->
      n, Sys.getenv_opt (n :> string))
  in
  let input_ringbufs =
    Supervisor.input_ringbufs conf proc.func proc.key.role
  and state_file =
    C.worker_state conf proc.func (RamenParams.signature proc.params)
  and out_ringbuf_ref =
    if Value.Worker.is_top_half proc.key.role then None
    else Some (C.out_ringbuf_names_ref conf proc.func)
  and parent_links =
    List.map (fun (_, _, pfunc) ->
      C.out_ringbuf_names_ref conf pfunc,
      C.input_ringbuf_fname conf pfunc proc.func,
      F.make_fieldmask pfunc proc.func
    ) proc.parents in
  let pid =
    Supervisor.start_worker
      conf proc.func proc.params envvars proc.key.role proc.log_level
      proc.report_period "test_instance" proc.bin parent_links
      proc.children input_ringbufs state_file out_ringbuf_ref in
  proc.pid <- Some pid ;
  proc.last_killed := 0.

(* Try to start the given proc.
 * Check links (ie.: do parents and children have the proper types?) *)
let really_try_start conf now proc =
  Supervisor.info_or_test conf "Starting operation %a"
    print_running_process proc ;
  assert (proc.pid = None) ;
  let check_linkage p c =
    let out_type =
      O.out_type_of_operation ~with_private:false p.F.operation in
    try Processes.check_is_subtype c.F.in_type out_type
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
  Hashtbl.iter (fun _ (proc : running_process) ->
    (* Iter over all running functions and check they do not output to a
     * ringbuf not in this set: *)
    if proc.pid <> None then (
      let out_ref = C.out_ringbuf_names_ref conf proc.func in
      let outs = OutRef.read_live out_ref in
      let open OutRef in
      Hashtbl.iter (fun key _ ->
        match key with
        | File fname ->
            if Files.has_ext "r" fname && not (Set.mem fname rbs) then (
              !logger.error
                "Operation %a outputs to %a, which is not read, fixing"
                print_running_process proc
                N.path_print fname ;
              log_and_ignore_exceptions ~what:("fixing "^ (fname :> string))
                (fun () ->
                  remove out_ref (File fname) Channel.live) ())
        | SyncKey _ ->
            ()
      ) outs ;
      (* Conversely, check that all children are in the out_ref of their
       * parent: *)
      let in_rbs = C.in_ringbuf_names conf proc.func |> Set.of_list in
      List.iter (fun (_, _, pfunc) ->
        let out_ref = C.out_ringbuf_names_ref conf pfunc in
        let outs = (read_live out_ref |>
                    Hashtbl.keys) //@
                   (function File f -> Some f
                           | SyncKey _ -> None) |>
                   Set.of_enum in
        if Set.disjoint in_rbs outs then (
          !logger.error "Operation %a must output to %a but does not, fixing"
            N.fq_print (F.fq_name pfunc)
            print_running_process proc ;
          log_and_ignore_exceptions ~what:("fixing "^(out_ref :> string))
            (fun () ->
              let fname = C.input_ringbuf_fname conf pfunc proc.func
              and fieldmask = F.make_fieldmask pfunc proc.func in
              add out_ref (File fname) fieldmask) ())
      ) proc.parents
    )
  ) running

let signal_all_cont running =
  Hashtbl.iter (fun _ (proc : running_process) ->
    if proc.pid <> None && not proc.continued then (
      proc.continued <- true ;
      !logger.debug "Signaling %a to continue" print_running_process proc ;
      log_and_ignore_exceptions ~what:"Signaling worker to continue"
        (Unix.kill (Option.get proc.pid)) Sys.sigcont) ;
  ) running

let try_kill conf pid func parents last_killed =
  (* There is no reason to wait before we remove this worker from its
   * parent out-ref: if it's not replaced then the last unprocessed
   * tuples are lost. If it's indeed a replacement then the new version
   * will have a chance to process the left overs. *)
  let input_ringbufs = C.in_ringbuf_names conf func in
  let out_refs =
    List.map (fun (_, _, pfunc) ->
      C.out_ringbuf_names_ref conf pfunc
    ) parents in
  Supervisor.cut_from_parents_outrefs input_ringbufs out_refs pid ;
  (* If it's still stopped, unblock first: *)
  log_and_ignore_exceptions ~what:"Continuing worker (before kill)"
    (Unix.kill pid) Sys.sigcont ;
  let what = Printf.sprintf2 "worker %a (pid %d)"
               N.fq_print (F.fq_name func) pid in
  Supervisor.(kill_politely conf last_killed what pid stats_worker_sigkills)

(* This is used to check that we do not check too often nor too rarely: *)
let last_checked_outref = ref 0.

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
let synchronize_workers conf must_run running =
  (* First, remove from running all terminated processes that must not run
   * any longer. Send a kill to those that are still running. *)
  let to_kill = ref [] and to_start = ref []
  and (+=) r x = r := x :: !r in
  Hashtbl.filteri_inplace (fun k (proc : running_process) ->
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
  if !to_kill <> [] then !logger.debug "Starting the kills" ;
  List.iter (fun (proc : running_process) ->
    try_kill conf (Option.get proc.pid) proc.func proc.parents proc.last_killed
  ) !to_kill ;
  if !to_start <> [] then !logger.debug "Starting the starts" ;
  (* FIXME: sort it so that parents are started before children,
   * so that in case of linkage error we do not end up with orphans
   * preventing parents to be run. *)
  List.iter (try_start conf) !to_start ;
  (* Try to fix any issue with out_refs: *)
  let now = Unix.time () in
  if (now > !last_checked_outref +. 30. ||
      now > !last_checked_outref +. 5. && !to_start = [] && !to_kill = []) &&
     !Processes.quit = None
  then (
    last_checked_outref := now ;
    log_and_ignore_exceptions ~what:"checking out_refs"
      (check_out_ref conf must_run) running) ;
  if !to_start = [] && conf.C.test then signal_all_cont running ;
  (* Return if anything changed: *)
  !to_kill <> [] || !to_start <> []

(* A version of Supervisor.synchronize_running that runs on a distinct
 * thread and synchronize against a fixed hash of programs that has to run: *)
let synchronize_running_static conf must_run end_flag =
  let prev_num_running = ref 0 in
  (* The workers that are currently running: *)
  let running = Hashtbl.create 10 in
  let rec loop () =
    let the_end =
      if !Processes.quit <> None || Atomic.Flag.is_set end_flag then (
        let num_running = Hashtbl.length running in
        if num_running = 0 then (
          !logger.info "Every worker has stopped, quitting." ;
          true
        ) else (
          if num_running <> !prev_num_running then (
            prev_num_running := num_running ;
            !logger.info "Still %d workers running" (Hashtbl.length running)) ;
          false
        )
      ) else false in
    if not the_end then (
      if !Processes.quit <> None then (
        !logger.debug "No more workers should run" ;
        Hashtbl.clear must_run
      ) ;
      process_workers_terminations conf running ;
      let _changed = synchronize_workers conf must_run running in
      let delay = if !Processes.quit = None then 1. else 0.3 in
      Unix.sleepf delay ;
      (loop [@tailcall]) ()
    ) in
  (* Once we have forked some workers we must not allow an exception to
   * terminate this function or we'd leave unsupervised workers behind: *)
  restart_on_failure "process supervisor" loop ()

(* Build the list of workers that must run on this site.
 * Start by getting a list of sites and then populate it with all workers
 * that must run there, with for each the set of their parents.
 *
 * Note: only used by RamenTests. *)

let build_must_run conf programs =
  let graph = FuncGraph.make conf programs in
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
                params = ge.prog.P.default_params ;
                role = Whole } in
            let v =
              { key ; rce = ge.rce ; func = ge.func ; parents } in
            Some (key, v)
          else
            None) |>
        Hashtbl.of_enum in
  !logger.debug "%d workers must run in full" (Hashtbl.length must_run) ;
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
              let service = ServiceNames.tunneld in
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
                    ge.func.F.signature, ge.prog.P.default_params in
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
    let role =
      Value.Worker.TopHalf (
        List.mapi (fun i tunneld ->
          Value.Worker.{
            tunneld_host = tunneld.Services.host ;
            tunneld_port = tunneld.Services.port ;
            parent_num = i }
        ) tunnelds) in
    let key =
      { program_name = cprog ;
        func_name = cfunc ;
        func_signature = sign ;
        params ; role } in
    let parents =
      let pge = FuncGraph.find graph conf.C.site pp pf in
      [ conf.C.site, pge.prog, pge.func ] in
    let v =
      { key ; rce ; func ; parents } in
    assert (not (Hashtbl.mem must_run key)) ;
    Hashtbl.add must_run key v
  ) top_halves ;
  must_run

let run_test conf notify_rb dirname test =
  (* Hash from func fq name to its rc and mmapped input ring-buffer: *)
  let workers = Hashtbl.create 11 in
  let dirname = Files.absolute_path_of dirname in
  (* The only sure way to know when to stop the workers is: when the test
   * succeeded, or timeouted. So we start three threads at the same time:
   * the process synchronizer, the worker feeder, and the output evaluator: *)
  (* First, write the list of programs that must run and fill workers
   * hash-table: *)
  let programs = Hashtbl.create 10 in
  Hashtbl.iter (fun program_name (p : program_spec) ->
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
    (* Mimic the rc file structure.
     * TODO: Make the synchronizer work directly from the set of bins *)
    let prog = P.of_bin program_name p.params bin in
    let get_rc () = prog in
    let rce =
      RC.{ bin ; params = p.params ; status = MustRun ; debug = false ;
           report_period = RamenConsts.Default.report_period ;
          src_file = N.path "" ; on_site = Globs.all ; automatic = false } in
    Hashtbl.add programs program_name (rce, get_rc) ;
    List.iter (fun func ->
      Hashtbl.add workers (F.fq_name func) (func, ref None)
    ) prog.P.funcs
  ) test.programs ;
  check_test_spec test programs ;
  (* This flag will signal the end to either both tester and early_termination
   * threads: *)
  let end_flag = Atomic.Flag.make false in
  let stop_workers () = Atomic.Flag.set end_flag in
  (* Now that the running config is complete we can start the worker
   * supervisor: *)
  let must_run = build_must_run conf programs in
  let supervisor =
    Thread.create (
      restart_on_failure "synchronize_running"
        (synchronize_running_static conf must_run)) end_flag in
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
  (* One tester thread per operation *)
  let tester_threads =
    Hashtbl.fold (fun user_fq_name output_spec thds ->
      test_output conf user_fq_name output_spec programs :: thds
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
          Thread.create
            (test_until conf until_count end_flag user_fq_name tuple_spec)
            programs :: thds
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
  !all_good, supervisor

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
  let res, supervisor =
    run_test conf notify_rb (Files.dirname test) test_spec in
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
  Thread.join supervisor ;
  Option.may (fun thd ->
    !logger.debug "Waiting for http server..." ;
    Thread.join thd
  ) httpd_thread
