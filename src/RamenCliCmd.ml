(* For each ramen command, check arguments and mostly transfer the control
 * further to more specialized modules. *)
open Batteries
open Stdint
open RamenLog
open RamenHelpers
open RamenConsts
module C = RamenConf
module F = C.Func
module P = C.Program

let () =
  Printexc.register_printer (function
    | Failure msg -> Some msg
    | _ -> None)

let make_copts debug quiet persist_dir rand_seed keep_temp_files
               forced_variants initial_export_duration =
  (match rand_seed with
  | None -> Random.self_init ()
  | Some seed ->
      RamenProcesses.rand_seed := Some seed ;
      Random.init seed) ;
  (* As the RAMEN_VARIANTS envar can only take a list as a single string,
   * let's consider each value can be a list: *)
  let forced_variants =
    List.fold_left (fun lst s ->
      List.rev_append (String.split_on_char ',' s) lst
    ) [] forced_variants in
  C.make_conf ~debug ~quiet ~keep_temp_files ~forced_variants
              ~initial_export_duration persist_dir

(*
 * `ramen supervisor`
 *
 * Start the process supervisor, which will keep running the programs
 * present in the configuration/rc file (and kill the others).
 * This does not return (under normal circumstances).
 *
 * The actual work is done in module RamenProcesses.
 *)

let check_binocle_errors () =
  Option.may raise !Binocle.last_error

let while_ () = !RamenProcesses.quit = None

let supervisor conf daemonize to_stdout to_syslog autoreload
               use_external_compiler bundle_dir max_simult_compils
               smt_solver fail_for_good_ () =
  RamenCompiler.init use_external_compiler bundle_dir max_simult_compils
                     smt_solver ;
  if to_stdout && daemonize then
    failwith "Options --daemonize and --stdout are incompatible." ;
  if to_stdout && to_syslog then
    failwith "Options --syslog and --stdout are incompatible." ;
  if to_syslog then
    init_syslog conf.C.log_level
  else (
    (* Controls all calls to restart_on_failure: *)
    fail_for_good := fail_for_good_ ;
    let logdir =
      if to_stdout then None
      else Some (conf.C.persist_dir ^"/log/supervisor") in
    Option.may mkdir_all logdir ;
    init_logger ?logdir conf.C.log_level) ;
  check_binocle_errors () ;
  if daemonize then do_daemonize () ;
  let open RamenProcesses in
  (* Also attempt to repair the report/notifs ringbufs.
   * This is OK because there can be no writer right now, and the report
   * ringbuf being a non-wrapping buffer then reader part cannot be damaged
   * any way. For notifications we could have the notifier reading though,
   * so FIXME: smarter ringbuf_repair that spins before repairing. *)
  prepare_signal_handlers conf ;
  let reports_rb = prepare_reports conf in
  RingBuf.unload reports_rb ;
  let notify_rb = prepare_notifs conf in
  RingBuf.unload notify_rb ;
  (* The main job of this process is to make what's actually running
   * in accordance to the running program list: *)
  restart_on_failure ~while_ "synchronize_running"
    RamenExperiments.(specialize the_big_one) [|
      RamenProcesses.dummy_nop ;
      (fun () -> synchronize_running conf autoreload) |] ;
  Option.may exit !RamenProcesses.quit

(*
 * `ramen notifier`
 *
 * Start the notifier process, which will read the notifications ringbuf
 * and perform whatever action it takes, most likely reaching out to
 * external systems.
 *
 * The actual work is done in module RamenNotifier.
 *)

let notifier conf notif_conf_file max_fpr daemonize to_stdout
             to_syslog () =
  if to_stdout && daemonize then
    failwith "Options --daemonize and --stdout are incompatible." ;
  if to_stdout && to_syslog then
    failwith "Options --syslog and --stdout are incompatible." ;
  if max_fpr < 0. || max_fpr > 1. then
    failwith "False-positive rate is a rate is a rate." ;
  (* The configuration file better exists, unless it's the default one in
   * which case it will be created with the default configuration: *)
  if notif_conf_file = Default.notif_conf_file then (
    RamenNotifier.ensure_conf_file_exists notif_conf_file
  ) else if file_check ~min_size:1 notif_conf_file <> FileOk then (
    failwith ("Configuration file "^ notif_conf_file ^" does not exist.")
  ) else (
    (* Try to parse that file while we have the user attention: *)
    ignore (RamenNotifier.load_config notif_conf_file)
  ) ;
  if to_syslog then
    init_syslog conf.C.log_level
  else (
    let logdir =
      if to_stdout then None
      else Some (conf.C.persist_dir ^"/log/notifier") in
    Option.may mkdir_all logdir ;
    init_logger ?logdir conf.C.log_level) ;
  check_binocle_errors () ;
  if daemonize then do_daemonize () ;
  RamenProcesses.prepare_signal_handlers conf ;
  let notify_rb = RamenProcesses.prepare_notifs conf in
  restart_on_failure ~while_ "process_notifications"
    RamenExperiments.(specialize the_big_one) [|
      RamenProcesses.dummy_nop ;
      (fun () ->
        RamenNotifier.start conf notif_conf_file notify_rb max_fpr) |] ;
  Option.may exit !RamenProcesses.quit

let notify conf parameters notif_name () =
  init_logger conf.C.log_level ;
  let rb = RamenProcesses.prepare_notifs conf in
  let start = Unix.gettimeofday () in
  let firing, certainty, parameters =
    RingBufLib.normalize_notif_parameters parameters in
  let parameters = Array.of_list parameters in
  RingBufLib.write_notif rb
    ("CLI", start, None, notif_name, firing, certainty, parameters)

(*
 * `ramen compile`
 *
 * Turn a ramen program into an executable binary.
 * Actual work happens in RamenCompiler.
 *)

(* Note: We need a program name to identify relative parents. *)
let compile conf root_path use_external_compiler bundle_dir
            max_simult_compils smt_solver source_files
            output_file_opt program_name_opt parents_from_rc () =
  let many_source_files = List.length source_files > 1 in
  if many_source_files && program_name_opt <> None then
    failwith "Cannot specify the program name for several source files" ;
  init_logger conf.C.log_level ;
  (* There is a long way to calling the compiler so we configure it from
   * here: *)
  RamenCompiler.init use_external_compiler bundle_dir max_simult_compils
                     smt_solver ;
  let root_path = absolute_path_of root_path in
  let get_parent =
    if parents_from_rc then
      let programs = C.with_rlock conf identity in
      RamenCompiler.parent_from_programs programs
    else
      RamenCompiler.parent_from_root_path root_path in
  let all_ok = ref true in
  let compile_file source_file =
    let program_name =
      Option.default_delayed (fun () ->
        try
          rel_path_from root_path (Filename.remove_extension source_file) |>
          RamenName.program_of_string
        with Failure s ->
          !logger.error "%s" s ;
          !logger.error "No program name given and cannot find out from the \
                         file name, giving up!" ;
          exit 1
      ) program_name_opt in
    let output_file =
      Option.default_delayed (fun () ->
        Filename.remove_extension source_file ^".x"
      ) output_file_opt in
    RamenMake.build conf get_parent program_name source_file output_file
  in
  List.iter (fun source_file ->
    try
      compile_file source_file
    with
    | Failure msg ->
        !logger.error "Error: %s" msg ;
        all_ok := false
    | e ->
        print_exception e ;
        all_ok := false
  ) source_files ;
  if not !all_ok then exit 1

(*
 * `ramen run`
 *
 * Ask the ramen daemon to start a compiled program.
 *)

let run conf params replace kill_if_disabled report_period as_ src_file
        bin_file () =
  (* By default use the given path as the program name: *)
  let program_name =
    Option.default_delayed (fun () ->
      Filename.remove_extension bin_file |>
      RamenName.program_of_string
    ) as_ in
  let params = List.enum params |> Hashtbl.of_enum in
  init_logger conf.C.log_level ;
  (* If we run in --debug mode, also set that worker in debug mode: *)
  let debug = conf.C.log_level = Debug in
  RamenRun.run conf ~params ~debug ~replace ~kill_if_disabled ~report_period
               ?src_file bin_file program_name

(*
 * `ramen kill`
 *
 * Remove that program from the list of running programs.
 * This time the program is identified by its name not its executable file.
 *)

let kill conf program_names purge () =
  init_logger conf.C.log_level ;
  let program_names =
    List.map Globs.compile program_names in
  let num_kills = RamenRun.kill conf ~purge program_names in
  Printf.printf "Killed %d program%s\n"
    num_kills (if num_kills > 1 then "s" else "")

(*
 * `ramen gc`
 *
 * Delete old or unused files.
 *)

let gc conf max_archives dry_run loop daemonize to_stdout to_syslog () =
  if to_stdout && daemonize then
    failwith "Options --daemonize and --stdout are incompatible." ;
  if to_stdout && to_syslog then
    failwith "Options --syslog and --stdout are incompatible." ;
  if daemonize && loop = 0. then
    failwith "It makes no sense to --daemonize without --loop." ;
  if to_syslog then
    init_syslog conf.C.log_level
  else (
    let logdir =
      if to_stdout then None
      else Some (conf.C.persist_dir ^"/log/gc") in
    Option.may mkdir_all logdir ;
    init_logger ?logdir conf.C.log_level) ;
  if loop <= 0. then
    RamenGc.cleanup_once conf dry_run max_archives
  else (
    check_binocle_errors () ;
    if daemonize then do_daemonize () ;
    RamenGc.cleanup_loop conf dry_run loop max_archives)

(*
 * `ramen ps`
 *
 * Display information about running programs and quit.
 *)

let ps conf short pretty with_header sort_col top pattern all () =
  init_logger conf.C.log_level ;
  let pattern = Globs.compile pattern in
  (* Start by reading the last minute of instrumentation data: *)
  let stats = RamenPs.read_stats conf in
  (* Now iter over all workers and display those stats: *)
  let open TermTable in
  let head, lines =
    if short then
      (* For --short, we sum everything by program: *)
      let h = RamenPs.per_program stats in
      [| "program" ; "parameters" ; "#in" ; "#selected" ; "#out" ; "#groups" ;
         "CPU" ; "wait in" ; "wait out" ; "heap" ; "max heap" ; "volume in" ;
         "volume out" |],
      C.with_rlock conf (fun programs ->
        Hashtbl.fold (fun program_name (mre, _get_rc) lines ->
          if (all || mre.C.status = C.MustRun) &&
             Globs.matches pattern
               (RamenName.string_of_program program_name)
          then (
            let s = Hashtbl.find_default h program_name RamenPs.no_stats in
            [| Some (ValStr (RamenName.string_of_program program_name)) ;
               Some (ValStr (RamenName.string_of_params mre.C.params)) ;
               int_or_na s.in_count ;
               int_or_na s.selected_count ;
               int_or_na s.out_count ;
               int_or_na s.group_count ;
               Some (ValFlt s.cpu) ;
               flt_or_na s.wait_in ;
               flt_or_na s.wait_out ;
               Some (ValInt (Uint64.to_int s.ram)) ;
               Some (ValInt (Uint64.to_int s.max_ram)) ;
               flt_or_na (Option.map Uint64.to_float s.bytes_in) ;
               flt_or_na (Option.map Uint64.to_float s.bytes_out) |] :: lines
          ) else lines
        ) programs [])
    else
      (* Otherwise we want to display all we can about individual workers *)
      [| "operation" ; "#in" ; "#selected" ; "#out" ; "#groups" ; "last out" ;
         "min event time" ; "max event time" ; "CPU" ; "wait in" ; "wait out" ;
         "heap" ; "max heap" ; "volume in" ; "volume out" ; "startup time" ;
         "#parents" ; "#children" ; "signature" |],
      C.with_rlock conf (fun programs ->
        (* First pass to get the children: *)
        let children_of_func = Hashtbl.create 23 in
        Hashtbl.iter (fun _prog_name (mre, get_rc) ->
          if all || mre.C.status = C.MustRun then match get_rc () with
          | exception _ -> ()
          | prog ->
              List.iter (fun func ->
                List.iter (fun (pp, pf) ->
                  (* We could use the pattern to filter out uninteresting
                   * parents but there is not much to save at this point. *)
                  let k =
                    F.program_of_parent_prog func.F.program_name pp, pf in
                  Hashtbl.add children_of_func k func
                ) func.F.parents
              ) prog.P.funcs
        ) programs ;
        Hashtbl.fold (fun program_name (mre, get_rc) lines ->
          if not all && mre.C.status <> MustRun then lines
          else match get_rc () with
          | exception _ -> (* which has been logged already *) lines
          | prog ->
            List.fold_left (fun lines func ->
              let fq = RamenName.fq program_name func.F.name in
              if Globs.matches pattern (RamenName.string_of_fq fq) then
                let s = Hashtbl.find_default stats fq RamenPs.no_stats
                and num_children = Hashtbl.find_all children_of_func
                                     (func.F.program_name, func.F.name) |>
                                     List.length in
                [| Some (ValStr (RamenName.string_of_fq fq)) ;
                   int_or_na s.in_count ;
                   int_or_na s.selected_count ;
                   int_or_na s.out_count ;
                   int_or_na s.group_count ;
                   date_or_na s.last_out ;
                   date_or_na s.min_etime ;
                   date_or_na s.max_etime ;
                   Some (ValFlt s.cpu) ;
                   flt_or_na s.wait_in ;
                   flt_or_na s.wait_out ;
                   Some (ValInt (Uint64.to_int s.ram)) ;
                   Some (ValInt (Uint64.to_int s.max_ram)) ;
                   flt_or_na (Option.map Uint64.to_float s.bytes_in) ;
                   flt_or_na (Option.map Uint64.to_float s.bytes_out) ;
                   Some (ValDate s.startup_time) ;
                   Some (ValInt (List.length func.F.parents)) ;
                   Some (ValInt num_children) ;
                   Some (ValStr func.signature) |] :: lines
              else lines
            ) lines prog.P.funcs
        ) programs []) in
  print_table ~pretty ~sort_col ~with_header ?top head lines

(*
 * `ramen tail`
 *
 * Display the last tuple(s) output by an operation.
 *
 * This first create a non-wrapping buffer file and then asks the operation
 * to write in there for 1 hour (by default).
 * This buffer name is standard so that other clients wishing to read those
 * tuples can reuse the same and benefit from a shared history.
 *)

(* Check the entered field names are correct: *)
let check_field_names typ field_names =
  List.iter (fun fn ->
    if not (List.exists (fun h -> fn = h.RamenTuple.name) typ) then
      Printf.sprintf2 "Unknown field %a, should be one of %a"
        RamenName.field_print fn
        RamenTuple.print_typ_names typ |>
      failwith
  ) field_names

let tail conf fq field_names with_header with_units sep null raw
         last min_seq max_seq continuous where since until
         with_seqnums with_event_time duration pretty flush () =
  init_logger conf.C.log_level ;
  if (last <> None || continuous) && (min_seq <> None || max_seq <> None) then
    failwith "Options --{last,continuous} and \
              --{min,max}-seq are incompatible." ;
  if continuous && Option.map_default (fun l -> l < 0) false last then
    failwith "Option --last must be >0 if used with --continuous." ;
  if with_units && not with_header then
    failwith "Option --with-units makes no sense without --with-header" ;
  (* Do something useful by default: display the 10 last lines *)
  let last =
    if last = None && min_seq = None && max_seq = None then Some 10
    else last in
  let flush = flush || continuous in
  let bname, is_temp_export, filter, typ, ser, params, event_time =
    RamenExport.read_output conf ~duration fq where
  in
  (* Find out which seqnums we want to scan: *)
  let mi, ma = match last with
    | None ->
        min_seq,
        Option.map succ max_seq (* max_seqnum is in *)
    | Some l when l >= 0 ->
        let _mi, ma = RingBufLib.seq_range bname in
        Some (cap_add ma ~-l),
        Some (if continuous then max_int else ma)
    | Some l ->
        assert (l < 0) ;
        let _mi, ma = RingBufLib.seq_range bname in
        Some ma, Some (cap_add ma (cap_neg l)) in
  !logger.debug "Will display tuples from %a (incl) to %a (excl)"
    (Option.print Int.print) mi
    (Option.print Int.print) ma ;
  (* Then, scan all present ringbufs in the requested range (either
   * the last N tuples or, TBD, since ts1 [until ts2]) and display
   * them *)
  let reorder_column = RingBufLib.reorder_tuple_to_user typ ser in
  check_field_names typ field_names ;
  let header = typ |> Array.of_list in
  let display_field =
    Array.map (fun h ->
      field_names = [] || List.mem h.RamenTuple.name field_names
    ) header in
  if with_header then (
    let first = if with_seqnums then "Seq"^ sep else "" in
    let first = if with_event_time then "Event time"^ sep else first in
    Printf.printf "#%s" first ;
    let need_sep = ref false in
    for i = 0 to Array.length header - 1 do
      if display_field.(i) then (
        if !need_sep then String.print stdout sep ;
        RamenName.field_print stdout header.(i).name ;
        if with_units then
          Option.may (fun u -> RamenUnits.print stdout u) header.(i).units ;
        need_sep := true)
    done ;
    Char.print stdout '\n' ;
    (*if flush then BatIO.flush stdout*)) ;
  (* Pick a printer for each column according to the field type: *)
  let float_printer p oc =
    let open RamenTypes in
    function
    | VFloat t -> p oc t
    | t -> print_custom ~null ~quoting:(not raw) oc t
  in
  let printer_of ?rel ft =
    let open RamenTypes in
      match pretty, ft.RamenTuple.typ.structure, ft.RamenTuple.units with
      | true, TFloat, Some units
        when units = RamenUnits.seconds_since_epoch ->
          float_printer (print_as_date ?rel)
      | true, TFloat, Some units
        when units = RamenUnits.seconds ->
          float_printer print_as_duration
      | _ ->
          print_custom ~null ~quoting:(not raw)
  in
  let printers = Array.map (printer_of ?rel:None) header in
  let et_printer =
    let rel = ref "" in
    float_printer (print_as_date ~rel) in
  if is_temp_export then (
    let rec reset_export_timeout () =
      let _mre, _prog, func =
        C.with_rlock conf (fun programs ->
          C.find_func programs fq) in
      let _ = RamenExport.start conf ~duration func in
      (* Start by sleeping as we've just set the temp export above: *)
      Unix.sleepf (max 1. (duration -. 1.)) ;
      reset_export_timeout () in
    Thread.create (
      restart_on_failure "reset_export_timeout"
        reset_export_timeout) () |> ignore) ;
  let open RamenSerialization in
  let event_time_of_tuple = match event_time with
    | None ->
      if with_event_time then
        failwith "Function has no event time information"
      else (fun _ -> 0., 0.)
    | Some et -> event_time_of_tuple typ params et
  in
  let unserialize = RamenSerialization.read_array_of_values ser in
  fold_seq_range ~wait_for_more:true bname ?mi ?ma () (fun () m tx ->
    match RamenSerialization.read_tuple unserialize tx with
    | RingBufLib.DataTuple chan, Some tuple
      when chan = RamenChannel.live && filter tuple ->
        let t1, t2 = event_time_of_tuple tuple in
        if Option.map_default (fun since -> t2 > since) true since &&
           Option.map_default (fun until -> t1 <= until) true until
        then (
          let need_sep = ref false in
          let may_print_sep () =
            if !need_sep then String.print stdout sep ;
            need_sep := true in
          if with_event_time then (
            let s1 = IO.to_string et_printer (VFloat t1) in
            let s2 = IO.to_string et_printer (VFloat t2) in
            Printf.printf "%s..%s" s1 s2 ;
            need_sep := true) ;
          if with_seqnums then (
            may_print_sep () ;
            Int.print stdout m) ;
          reorder_column tuple |>
          Array.iteri (fun i v ->
            if display_field.(i) then (
              may_print_sep () ;
              printers.(i) stdout v)) ;
          Char.print stdout '\n' ;
          if flush then BatIO.flush stdout)
    | _ -> ())

(*
 * `ramen replay`
 *
 * Like tail, but replays history.
 * Mainly for testing purposes for now.
 * `tail` ask a worker to archive its output and then display this archive.
 * Non-live channels are never archived though. So replay has to create its
 * own receiving ring-buffer like a new worker would do, create a channel,
 * ask for the operation to output this channel to this ringbuf,
 * choose the replayers according to the stats file, setup all the out_ref
 * from the replayers to its own ringbuf (no the outref currently in place
 * should be ok), and finally launch the replayers.
 *
 * Note: refactor the CSV dumper to accommodate for tail, replay and
 * timeseries.
 *)

let replay conf fq field_names with_header with_units _sep _null _raw
           _where since until _with_event_time _pretty _flush () =
  init_logger conf.C.log_level ;
  if with_units && not with_header then
    failwith "Option --with-units makes no sense without --with-header" ;
  (* Start with the most hazardeous and interesting part: find a way to
   * get the data that's being asked: *)
  (* First, make sure the operation actually exist: *)
  let mre, _prog, func =
    C.with_rlock conf (fun programs ->
      C.find_func programs fq) in
  check_field_names func.F.out_type field_names ;
  (* Then, get the runtime stats: *)
  let stats = RamenArchivist.load_stats conf in
  (* Find a way to get the data from func in between from and until.
   * Note that there could be several ways to obtain those. For instance,
   * we could ask for a 1year retention of some node that queried very
   * infrequently, and that is also a parent of some other node for which
   * we asked for a much shorter retention but that is queried more often:
   * in that case the archiver could well decide to archive both the
   * parent and, as an optimisation, this child. Now which best (or only)
   * path to query that child depends on since/until.
   * But the shortest path form func to its archiving parents that have the
   * required data is always the best. So start from func and progress
   * through parents until we have found all required sources with the whole
   * archived content. As a first attempt, just look for a parent that has
   * all the data (avoid "roaming"). Then gaps could be queried independently
   * (narrowed since/until might find the data). Or we could actually query all
   * the gaps while the "main" query is going. TBD.  *)
  (* Using stats, append to lst the list of all sources required to recall
   * data from since to until, and also return the best times we have
   * found so far: *)
  (* TODO: handle gaps in archives (for instance, best_since and best_until
   * could also come with a ratio of the coverage of the asked time
   * slice) *)
  let rec find_sources lst fq =
    let s = Hashtbl.find stats fq in
    let best_opt =
      List.fold_left (fun best_opt (t1, t2) ->
        if t1 > until || t2 < since then best_opt else
        Some (max t1 since, min t2 until)
      ) None s.RamenArchivist.archives in
    match best_opt with
    | Some (best_since, best_until)
      when best_since <= since && best_until >= until ->
        fq :: lst, Some (best_since, best_until)
    | _ ->
        List.fold_left (fun (lst, best_opt) pfq ->
          let lst, best_opt' = find_sources lst pfq in
          lst,
          (* We have to take the intersection of our parents time ranges *)
          match best_opt, best_opt' with
          | x, None -> x
          | None, (Some _ as x) -> x
          | Some (bf', bu'), Some (bf, bu) ->
              Some (max bf bf', min bu bu')
        ) (lst, None) s.parents
  in
  match find_sources [] fq with
  | exception Not_found ->
      !logger.error "Cannot find some parents in the stats?!"
  | _, None ->
      !logger.info "No archive found. Game over."
  | sources, Some (best_since, best_until) ->
      !logger.info "List of required sources: %a"
        (pretty_list_print RamenName.fq_print) sources ;
      let rel = ref "" in
      let p = print_as_date ~right_justified:false ~rel in
      !logger.info "Time slice covered: %a..%a"
        p best_since p best_until ;
      (* Then create a ringbuffer for reception: *)
      let rb_name =
        Printf.sprintf "/tmp/replay_test_%d" (Unix.getpid ()) in
      finally (fun () -> safe_unlink rb_name) (fun () ->
        RingBuf.create rb_name ;
        let rb = RingBuf.load rb_name in
        finally (fun () -> RingBuf.unload rb) (fun () ->
          (* Pick a channel. They are cheap, we do not care if we fail
           * in the next step: *)
          let chn = RamenChannel.make conf in
          (* Ask to export only the fields we want. From no on we'd better
           * not fail and retry as we would hammer the out_ref with temp
           * ringbufs.
           * Maybe we should use the channel in the rb name, and pick a
           * channel_id large enough to be a hash of FQ, output fields and
           * dates? But that mean 256bits integers (2xU128?). *)
          let out_type =
            RingBufLib.ser_tuple_typ_of_tuple_typ func.F.out_type
          and in_type =
            if field_names = [] then func.F.out_type
            else
              List.filter (fun ft ->
                List.mem ft.RamenTuple.name field_names
              ) func.F.out_type in
          let field_mask = RingBufLib.skip_list ~out_type ~in_type in
          let file_spec =
            RamenOutRef.{ field_mask ; timeout = 300. ;
                          channel = Some chn } in
          let out_ref = C.out_ringbuf_names_ref conf func in
          RamenOutRef.add out_ref (rb_name, file_spec) ;
          (* Now spawn the replayers.
           * For each source, spawn a replayer, passing it the name of the
           * function, the out_ref files to obey, the channel id to tag tuples
           * with, and since/until dates. *)
          List.iter (fun sfq ->
            let args = [| replay_argv0 ; RamenName.string_of_fq sfq |]
            and env = [||] in
            let pid = RamenProcesses.run_worker mre.C.bin args env in
            !logger.info "Replay for %a is running under pid %d"
              RamenName.fq_print sfq pid
          ) sources
          ) ()) ()


(*
 * `ramen timeseries`
 *
 * Similar to tail, but output only two columns: time and a value, and
 * make sure to provide as many data samples as asked for, consolidating
 * the actual samples as needed.
 *
 * This works only on operations with time-event information and uses the
 * same output archive files as the `ramen tail` command does.
 *)

let timeseries conf since until with_header where factors num_points
               time_step sep null func_name data_fields consolidation
               bucket_time duration pretty () =
  init_logger conf.C.log_level ;
  let num_points =
    if num_points <= 0 && time_step <= 0. then 100 else num_points in
  let until = until |? Unix.gettimeofday () in
  let since = since |? until -. 600. in
  (* Obtain the data: *)
  let open RamenTimeseries in
  let num_points, since, until =
    compute_num_points time_step num_points since until in
  let columns, timeseries =
    get conf ~duration num_points since until where factors
        ~consolidation ~bucket_time func_name data_fields in
  (* Display results: *)
  let single_data_field = List.length data_fields = 1 in
  if with_header then (
    let column_names =
      Array.fold_left (fun res sc ->
        let v =
          List.map RamenTypes.to_string sc |>
          String.concat "." in
        if single_data_field then
          (if v = "" then
            List.hd data_fields |> RamenName.string_of_field
           else v) :: res
        else
          List.fold_left (fun res df ->
            let df = RamenName.string_of_field df in
            (df ^(if v = "" then "" else "("^ v ^")")) :: res
          ) res data_fields
      ) [] columns |> List.rev in
    List.print ~first:("#Time"^ sep) ~last:"\n" ~sep
                String.print stdout column_names) ;
  let t_rel = ref "" in
  Enum.iter (fun (t, vs) ->
    Printf.printf "%a%s%a"
      (if pretty then print_as_date ~right_justified:true ~rel:t_rel
                 else print_nice_float) t
      sep
      (Array.print ~first:"" ~last:"\n" ~sep
        (Array.print ~first:"" ~last:"" ~sep
          (fun oc -> function
            | None -> String.print oc null
            | Some v -> Float.print oc v))) vs
  ) timeseries

(*
 * `ramen timerange`
 *
 * Obtain information about the time range available for timeseries.
 *)

let timerange conf fq () =
  init_logger conf.C.log_level ;
  C.with_rlock conf (fun programs ->
    match C.find_func programs fq with
    | exception _ -> exit 1
    | _mre, prog, func ->
        let mi_ma =
          (* We need the func to know its buffer location.
           * Nothing better to do in case of error than to exit. *)
          let bname = C.archive_buf_name conf func in
          let typ = func.F.out_type in
          let ser = RingBufLib.ser_tuple_typ_of_tuple_typ typ in
          let params = prog.P.params in
          RamenSerialization.time_range bname ser params func.F.event_time
        in
        match mi_ma with
          | None -> Printf.printf "No time info or no output yet.\n"
          | Some (mi, ma) -> Printf.printf "%f %f\n" mi ma)

(*
 * `ramen httpd`
 *
 * Starts an HTTP daemon that will serve (and maybe one day also accept)
 * timeseries, either impersonating Graphite API (https://graphiteapp.org/)
 * or any other API of our own.
 *)

let httpd conf daemonize to_stdout to_syslog fault_injection_rate
          server_url api graphite
          (* The API might compile some code: *)
          use_external_compiler bundle_dir max_simult_compils smt_solver
          () =
  RamenCompiler.init use_external_compiler bundle_dir max_simult_compils
                     smt_solver ;
  if to_stdout && daemonize then
    failwith "Options --daemonize and --stdout are incompatible." ;
  if to_stdout && to_syslog then
    failwith "Options --syslog and --stdout are incompatible." ;
  if fault_injection_rate > 1. then
    failwith "Fault injection rate is a rate is a rate." ;
  if to_syslog then
    init_syslog conf.C.log_level
  else (
    let logdir =
      (* In case we serve several API from several daemon we will need an
       * additional option to set a different logdir for each. *)
      if to_stdout then None
      else Some (conf.C.persist_dir ^"/log/httpd") in
    Option.may mkdir_all logdir ;
    init_logger ?logdir conf.C.log_level) ;
  check_binocle_errors () ;
  if daemonize then do_daemonize () ;
  RamenProcesses.prepare_signal_handlers conf ;
  RamenHttpd.run_httpd conf server_url api graphite fault_injection_rate ;
  Option.may exit !RamenProcesses.quit

let graphite_expand conf for_render all query () =
  init_logger conf.C.log_level ;
  let query = RamenGraphite.split_query query in
  let te =
    RamenGraphite.full_enum_tree_of_query
      conf ~anchor_right:for_render ~only_running:(not all) query in
  let rec display indent te =
    let e = RamenGraphite.get te in
    list_iter_first_last (fun is_first is_last (pv, c) ->
      let prefix =
        if is_first then
          if indent = "" then "" else
          if is_last then "-" else "┬"
        else
          if is_last then "└" else "├" in
      let value = RamenGraphite.(fix_quote pv.value) in
      Printf.printf "%s%s%s"
        (if is_first then "" else "\n"^indent)
        prefix value ;
      let indent' =
        indent
          ^ (if prefix <> "" then
              if is_last then " " else "│"
            else "")
          ^ String.make (String.length value) ' '
      in
      display indent' c
    ) e
  in
  display "" te ;
  Printf.printf "\n"

(*
 * `ramen archivist`
 *
 * A daemon that listen to notifier and a user configuration file
 * and decides what worker should save its history in order to be
 * able to retrieve or rebuild the output of all persistent functions.
 *)

let archivist conf loop daemonize no_stats no_allocs no_reconf
              to_stdout to_syslog () =
  if to_stdout && daemonize then
    failwith "Options --daemonize and --stdout are incompatible." ;
  if to_stdout && to_syslog then
    failwith "Options --syslog and --stdout are incompatible." ;
  if no_stats && no_allocs && no_reconf then
    failwith "Nothing to do then?" ;
  if to_syslog then
    init_syslog conf.C.log_level
  else (
    let logdir =
      if to_stdout then None
      else Some (conf.C.persist_dir ^"/log/archivist") in
    Option.may mkdir_all logdir ;
    init_logger ?logdir conf.C.log_level) ;
  RamenProcesses.prepare_signal_handlers conf ;
  if loop <= 0. then
    RamenArchivist.run_once conf ~while_ no_stats no_allocs no_reconf
  else (
    check_binocle_errors () ;
    if daemonize then do_daemonize () ;
    RamenArchivist.run_loop conf ~while_ loop no_stats no_allocs no_reconf)


(*
 * Display various internal informations
 *)

let variants conf () =
  init_logger conf.C.log_level ;
  let open RamenExperiments in
  let experimenter_id = get_experimenter_id conf.C.persist_dir in
  Printf.printf "Experimenter Id: %d\n" experimenter_id ;
  Printf.printf "Experiments (legend: %s | %s | unselected):\n"
    (green "forced") (yellow "selected") ;
  all_experiments conf.C.persist_dir |>
  List.iter (fun (name, e) ->
    Printf.printf "  %s:\n" name ;
    for i = 0 to Array.length e.variants - 1 do
      let v = e.variants.(i) in
      Printf.printf "    %s (%s%%):\n%s\n"
        ((if e.variant = i then
           if e.forced then green else yellow
         else identity) v.Variant.name)
        (nice_string_of_float (100. *. v.Variant.share))
        (reindent "      " v.Variant.descr)
    done ;
    Printf.printf "\n")

let stats conf () =
  init_logger conf.C.log_level ;
  (* Initialize all metrics so that they register to Binocle: *)
  List.iter (fun initer ->
    initer conf.C.persist_dir
  ) !RamenBinocle.all_saved_metrics ;
  Binocle.display_console ()
