(* For each ramen command, check arguments and mostly transfer the control
 * further to more specialized modules. *)
open Batteries
open Lwt
open Stdint
open RamenLog
open RamenHelpers
module C = RamenConf
module F = C.Func
module P = C.Program

let () =
  async_exception_hook := (fun exn ->
    !logger.error "Received exception %s\n%s"
      (Printexc.to_string exn)
      (Printexc.get_backtrace ()))

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

let dummy_nop () =
  !logger.warning "Running in dummy mode" ;
  let until_quit f =
    let rec loop () =
      if !RamenProcesses.quit <> None then return_unit
      else f () >>= loop in
    loop () in
  until_quit (fun () -> Lwt_unix.sleep 3.)

let supervisor conf daemonize to_stdout to_syslog autoreload
               report_period () =
  if to_stdout && daemonize then
    failwith "Options --daemonize and --stdout are incompatible." ;
  if to_stdout && to_syslog then
    failwith "Options --syslog and --stdout are incompatible." ;
  if to_syslog then
    logger := make_syslog conf.C.log_level
  else (
    let logdir =
      if to_stdout then None
      else Some (conf.C.persist_dir ^"/log/supervisor") in
    Option.may mkdir_all logdir ;
    logger := make_logger ?logdir conf.C.log_level) ;
  RamenProcesses.report_period := report_period ;
  if daemonize then do_daemonize () ;
  let open RamenProcesses in
  (* Also attempt to repair the report/notifs ringbufs.
   * This is OK because there can be no writer right now, and the report
   * ringbuf being a non-wrapping buffer then reader part cannot be damaged
   * any way. For notifications we could have the notifier reading though,
   * so FIXME: smarter ringbuf_repair that spins before repairing. *)
  prepare_signal_handlers () ;
  let reports_rb = prepare_reports conf in
  RingBuf.unload reports_rb ;
  let notify_rb = prepare_notifs conf in
  RingBuf.unload notify_rb ;
  Lwt_main.run (
    join [
      (let%lwt () = Lwt_unix.sleep 1. in
       async (fun () ->
         restart_on_failure "wait_all_pids_loop"
           RamenProcesses.wait_all_pids_loop true) ;
       return_unit) ;
      (* The main job of this process is to make what's actually running
       * in accordance to the running program list: *)
      restart_on_failure "synchronize_running"
        RamenExperiments.(specialize conf.C.persist_dir the_big_one) [|
          dummy_nop ;
          (fun () -> synchronize_running conf autoreload) |] ]) ;
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
  if notif_conf_file = RamenConsts.Default.notif_conf_file then (
    RamenNotifier.ensure_conf_file_exists notif_conf_file
  ) else if file_check ~min_size:1 notif_conf_file <> FileOk ||
            is_failing RamenNotifier.load_config notif_conf_file then
    failwith ("Configuration file "^ notif_conf_file ^" does not exist.") ;
  if to_syslog then
    logger := make_syslog conf.C.log_level
  else (
    let logdir =
      if to_stdout then None
      else Some (conf.C.persist_dir ^"/log/notifier") in
    Option.may mkdir_all logdir ;
    logger := make_logger ?logdir conf.C.log_level) ;
  if daemonize then do_daemonize () ;
  RamenProcesses.prepare_signal_handlers () ;
  let notify_rb = RamenProcesses.prepare_notifs conf in
  Lwt_main.run (
    async (fun () ->
      restart_on_failure "wait_all_pids_loop"
        RamenProcesses.wait_all_pids_loop false) ;
    restart_on_failure "process_notifications"
      RamenExperiments.(specialize conf.C.persist_dir the_big_one) [|
        dummy_nop ;
        (fun () ->
          RamenNotifier.start conf notif_conf_file notify_rb max_fpr) |]) ;
  Option.may exit !RamenProcesses.quit

let notify conf parameters notif_name () =
  logger := make_logger conf.C.log_level ;
  let rb = RamenProcesses.prepare_notifs conf in
  let sent_time = Unix.gettimeofday () in
  let firing, certainty, parameters =
    RingBufLib.normalize_notif_parameters parameters in
  let parameters = Array.of_list parameters in
  Lwt_main.run (
    RingBufLib.write_notif rb
      ("CLI", sent_time, None, notif_name, firing, certainty, parameters))

(*
 * `ramen compile`
 *
 * Turn a ramen program into an executable binary.
 * Actual work happens in RamenCompiler.
 *)

let compile conf root_path use_external_compiler bundle_dir
            max_simult_compils smt_solver source_files program_name_opt () =
  logger := make_logger conf.C.log_level ;
  (* There is a long way to calling the compiler so we configure it from
   * here: *)
  RamenCompiler.init use_external_compiler bundle_dir max_simult_compils
                     smt_solver ;
  let root_path = absolute_path_of root_path in
  let get_parent = RamenCompiler.parent_from_root_path root_path in
  let all_ok = ref true in
  let compile_file source_file =
    let program_name =
      Option.default_delayed (fun () ->
        (try
          rel_path_from root_path (Filename.remove_extension source_file) |>
          RamenName.program_of_string
        with Failure s ->
          !logger.error "%s" s ;
          !logger.error "No program name given and cannot find out from the \
                         file name, giving up!" ;
          exit 1)
      ) program_name_opt
    and program_code = read_whole_file source_file in
    RamenCompiler.compile conf root_path get_parent program_name program_code
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

let run conf params replace as_ bin_file () =
  logger := make_logger conf.C.log_level ;
  Lwt_main.run (RamenRun.run conf params replace ?as_ bin_file)

(*
 * `ramen kill`
 *
 * Remove that program from the list of running programs.
 * This time the program is identified by its name not its executable file.
 *)

let kill conf program_names () =
  logger := make_logger conf.C.log_level ;
  let program_names =
    List.map Globs.compile program_names in
  let num_kills =
    Lwt_main.run (RamenRun.kill conf program_names) in
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
  if daemonize && loop = 0 then
    failwith "It makes no sense to --daemonize without --loop." ;
  if to_syslog then
    logger := make_syslog conf.C.log_level
  else (
    let logdir =
      if to_stdout then None
      else Some (conf.C.persist_dir ^"/log/gc") in
    Option.may mkdir_all logdir ;
    logger := make_logger ?logdir conf.C.log_level) ;
  if daemonize then do_daemonize () ;
  if loop = 0 then
    RamenGc.cleanup_once conf dry_run max_archives
  else
    RamenGc.cleanup_loop conf dry_run loop max_archives

(*
 * `ramen ps`
 *
 * Display information about running programs and quit.
 *)

let no_stats =
  None, None, None, None, None, 0., Uint64.zero, None, None, None, None, None

let add_stats (etime', in_count', selected_count', out_count', group_count',
               cpu', ram', wait_in', wait_out', bytes_in', bytes_out',
               last_out')
              (etime, in_count, selected_count, out_count, group_count, cpu,
               ram, wait_in, wait_out, bytes_in, bytes_out, last_out) =
  let combine_opt f a b =
    match a, b with None, b -> b | a, None -> a
    | Some a, Some b -> Some (f a b) in
  let add_nu64 = combine_opt Uint64.add
  and add_nfloat = combine_opt (+.)
  and max_nfloat = combine_opt Float.max
  in
  max_nfloat etime' etime,
  add_nu64 in_count' in_count,
  add_nu64 selected_count' selected_count,
  add_nu64 out_count' out_count,
  add_nu64 group_count' group_count,
  cpu' +. cpu,
  Uint64.add ram' ram,
  add_nfloat wait_in' wait_in,
  add_nfloat wait_out' wait_out,
  add_nu64 bytes_in' bytes_in,
  add_nu64 bytes_out' bytes_out,
  max_nfloat last_out' last_out

let read_stats conf pattern =
  let h = Hashtbl.create 57 in
  let open RamenTypes in
  let bname = C.report_ringbuf conf in
  let typ = RamenBinocle.tuple_typ in
  let event_time = RamenBinocle.event_time in
  let now = Unix.gettimeofday () in
  let%lwt until =
    match%lwt RamenSerialization.time_range bname typ [] event_time with
    | None ->
        !logger.warning "No time range information for instrumentation" ;
        return now
    | Some (_, ma) ->
        if ma < now -. 120. then
          !logger.warning "Instrumentation info is %ds old"
            (int_of_float (now -. ma)) ;
        return ma in
  (* FIXME: Not OK because we don't know if report-period has been
   * overridden on `ramen supervisor` command line. Maybe at least make
   * `ramen ps` accept that option too? *)
  let since = until -. 2. *. !RamenProcesses.report_period in
  let get_string = function VString s -> s [@@ocaml.warning "-8"]
  and get_u64 = function VU64 n -> n [@@ocaml.warning "-8"]
  and get_nu64 = function VNull -> None | VU64 n -> Some n [@@ocaml.warning "-8"]
  and get_float = function VFloat f -> f [@@ocaml.warning "-8"]
  and get_nfloat = function VNull -> None | VFloat f -> Some f [@@ocaml.warning "-8"]
  in
  Lwt_main.run (
    let while_ () = (* Do not wait more than 1s: *)
      return (Unix.gettimeofday () -. now < 1.) in
    RamenSerialization.fold_time_range ~while_ bname typ [] event_time
                         since until ()  (fun () tuple t1 t2 ->
    let worker = get_string tuple.(0) in
    if Globs.matches pattern worker then
      let time = get_float tuple.(1)
      and etime = get_nfloat tuple.(2)
      and in_count = get_nu64 tuple.(3)
      and selected_count = get_nu64 tuple.(4)
      and out_count = get_nu64 tuple.(5)
      and group_count = get_nu64 tuple.(6)
      and cpu = get_float tuple.(7)
      and ram = get_u64 tuple.(8)
      and wait_in = get_nfloat tuple.(9)
      and wait_out = get_nfloat tuple.(10)
      and bytes_in = get_nu64 tuple.(11)
      and bytes_out = get_nu64 tuple.(12)
      and last_out = get_nfloat tuple.(13)
      in
      let stats = etime, in_count, selected_count, out_count, group_count, cpu,
                  ram, wait_in, wait_out, bytes_in, bytes_out, last_out in
      (* Keep only the latest stat line per worker: *)
      Hashtbl.modify_opt worker (function
        | None -> Some (time, stats)
        | Some (time', stats') as prev ->
            if time' > time then prev else Some (time, stats)
      ) h)) ;
  return h

let int_or_na = function
  | None -> TermTable.ValStr "n/a"
  | Some i -> TermTable.ValInt (Uint64.to_int i)

let flt_or_na = function
  | None -> TermTable.ValStr "n/a"
  | Some f -> TermTable.ValFlt f

let date_or_na = function
  | None -> TermTable.ValStr "n/a"
  | Some t -> TermTable.ValDate t

let str_or_na = function
  | None -> TermTable.ValStr "n/a"
  | Some s -> TermTable.ValStr s

let ps conf short with_header sort_col top pattern () =
  logger := make_logger conf.C.log_level ;
  let pattern = Globs.compile pattern in
  (* Start by reading the last minute of instrumentation data: *)
  let stats = Lwt_main.run (read_stats conf pattern) in
  (* Now iter over all workers and display those stats: *)
  let open TermTable in
  let head, lines =
    if short then
      (* For --short, we sum everything by program: *)
      let h = Hashtbl.create 17 in
      Hashtbl.iter (fun worker (time, stats) ->
        let program, _ = C.program_func_of_user_string worker in
        Hashtbl.modify_opt program (function
          | None -> Some (time, stats)
          | Some (time', stats') -> Some (time, add_stats stats' stats)
        ) h
      ) stats ;
      [| "program" ; "parameters" ; "#in" ; "#selected" ; "#out" ; "#groups" ;
         "CPU" ; "wait in" ; "wait out" ; "heap" ; "volume in" ;
         "volume out" |],
      Lwt_main.run (
        C.with_rlock conf (fun programs ->
          Hashtbl.fold (fun program_name (mre, _get_rc) lines ->
            if Globs.matches pattern
                 (RamenName.string_of_program program_name) then
              let _, (_etime, in_count, selected_count, out_count, group_count,
                      cpu, ram, wait_in, wait_out, bytes_in, bytes_out, _last_out) =
                Hashtbl.find_default h program_name (0., no_stats) in
              [| ValStr (RamenName.string_of_program program_name) ;
                 ValStr (RamenName.string_of_params mre.C.params) ;
                 int_or_na in_count ;
                 int_or_na selected_count ;
                 int_or_na out_count ;
                 int_or_na group_count ;
                 ValFlt cpu ;
                 flt_or_na wait_in ;
                 flt_or_na wait_out ;
                 ValInt (Uint64.to_int ram) ;
                 flt_or_na (Option.map Uint64.to_float bytes_in) ;
                 flt_or_na (Option.map Uint64.to_float bytes_out) |] :: lines
            else lines
          ) programs [] |> return))
    else
      (* Otherwise we want to display all we can about individual workers *)
      [| "operation" ; "#in" ; "#selected" ; "#out" ; "#groups" ; "last out" ;
         "max event time" ; "CPU" ; "wait in" ; "wait out" ; "heap" ;
         "volume in" ; "volume out" ; "#parents" ; "#children"; "signature" |],
      Lwt_main.run (
        C.with_rlock conf (fun programs ->
          (* First pass to get the childrens: *)
          let children_of_func = Hashtbl.create 23 in
          Hashtbl.iter (fun program_name (_mre, get_rc) ->
            match get_rc () with
            | exception e -> ()
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
          Hashtbl.fold (fun program_name (_mre, get_rc) lines ->
            match get_rc () with
            | exception e ->
              let fq_name =
                red (RamenName.string_of_program program_name ^"/*") in
              [| ValStr fq_name ; ValStr (Printexc.to_string e) |] :: lines
            | prog ->
              List.fold_left (fun lines func ->
                let fq_name = RamenName.string_of_program program_name
                              ^"/"^ RamenName.string_of_func func.F.name in
                if Globs.matches pattern fq_name then
                  let _, (etime, in_count, selected_count, out_count,
                          group_count, cpu, ram, wait_in, wait_out,
                          bytes_in, bytes_out, last_out) =
                    Hashtbl.find_default stats fq_name (0., no_stats)
                  and num_children = Hashtbl.find_all children_of_func
                                       (func.F.program_name, func.F.name) |>
                                       List.length in
                  [| ValStr fq_name ;
                     int_or_na in_count ;
                     int_or_na selected_count ;
                     int_or_na out_count ;
                     int_or_na group_count ;
                     date_or_na last_out ;
                     date_or_na etime ;
                     ValFlt cpu ;
                     flt_or_na wait_in ;
                     flt_or_na wait_out ;
                     ValInt (Uint64.to_int ram) ;
                     flt_or_na (Option.map Uint64.to_float bytes_in) ;
                     flt_or_na (Option.map Uint64.to_float bytes_out) ;
                     ValInt (List.length func.F.parents) ;
                     ValInt num_children ;
                     ValStr func.signature |] :: lines
                else lines
              ) lines prog.P.funcs
          ) programs [] |> return)) in
  print_table ~sort_col ~with_header ?top head lines

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

let tail conf func_name with_header sep null raw
         last min_seq max_seq continuous where with_seqnums with_event_time
         duration () =
  logger := make_logger conf.C.log_level ;
  if last <> None && (min_seq <> None || max_seq <> None) then
    failwith "Options --last  and --{min,max}-seq are incompatible." ;
  if continuous && (min_seq <> None || max_seq <> None) then
    failwith "Options --continuous and --{min,max}-seq are incompatible." ;
  if continuous && Option.map_default (fun l -> l < 0) false last then
    failwith "Option --last must be >0 if used with --continuous." ;
  (* Do something useful by default: display the 10 last lines *)
  let last =
    if last = None && min_seq = None && max_seq = None then Some 10
    else last in
  Lwt_main.run (
    let%lwt bname, filter, typ, ser, params, event_time =
      RamenTimeseries.read_output conf ~duration func_name where
    in
    (* Find out which seqnums we want to scan: *)
    let mi, ma = match last with
      | None ->
          min_seq,
          Option.map succ max_seq (* max_seqnum is in *)
      | Some l when l >= 0 ->
          let mi, ma = RingBufLib.seq_range bname in
          Some (cap_add ma ~-l),
          Some (if continuous then max_int else ma)
      | Some l ->
          assert (l < 0) ;
          let mi, ma = RingBufLib.seq_range bname in
          Some ma, Some (cap_add ma (cap_neg l)) in
    !logger.debug "Will display tuples from %a (incl) to %a (excl)"
      (Option.print Int.print) mi
      (Option.print Int.print) ma ;
    (* Then, scan all present ringbufs in the requested range (either
     * the last N tuples or, TBD, since ts1 [until ts2]) and display
     * them *)
    let nullmask_size =
      RingBufLib.nullmask_bytes_of_tuple_type ser in
    let reorder_column = RingBufLib.reorder_tuple_to_user typ ser in
    if with_header then (
      let header = typ |> Array.of_list in
      let first = if with_seqnums then "Seq"^ sep else "" in
      let first = if with_event_time then "Event time"^ sep else first in
      let first = "#"^ first in
      Array.print ~first ~last:"\n" ~sep
        (fun oc ft -> String.print oc ft.RamenTuple.typ_name)
        stdout header ;
      BatIO.flush stdout) ;
    async (fun () ->
      restart_on_failure "wait_all_pids_loop"
        RamenProcesses.wait_all_pids_loop false) ;
    let rec reset_export_timeout () =
      (* Start by sleeping as we've just set the temp export above: *)
      let%lwt () = Lwt_unix.sleep (max 1. (duration -. 1.)) in
      let%lwt _ =
        RamenExport.make_temp_export_by_name conf ~duration func_name in
      reset_export_timeout () in
    async (fun () ->
      restart_on_failure "reset_export_timeout"
        reset_export_timeout ()) ;
    let open RamenSerialization in
    let%lwt event_time_of_tuple = match event_time with
      | None ->
        if with_event_time then
          fail_with "Function has no event time information"
        else return (fun _ -> 0., 0.)
      | Some et -> return (event_time_of_tuple typ params et) in
    fold_seq_range ~wait_for_more:true bname ?mi ?ma () (fun () m tx ->
      let tuple = read_tuple ser nullmask_size tx in
      if filter tuple then (
        if with_event_time then (
          let t1, t2 = event_time_of_tuple tuple in
          Printf.printf "%f..%f%s" t1 t2 sep) ;
        if with_seqnums then (
          Int.print stdout m ; String.print stdout sep) ;
        reorder_column tuple |>
        Array.print ~first:"" ~last:"\n" ~sep
          (RamenTypes.print_custom ~null ~quoting:(not raw)) stdout ;
        BatIO.flush stdout) ;
      return_unit))

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

let timeseries conf since until with_header where factors max_data_points
               sep null func_name data_fields consolidation duration
               () =
  logger := make_logger conf.C.log_level ;
  if max_data_points < 1 then failwith "invalid max_data_points" ;
  let since = since |? until -. 600. in
  if since >= until then failwith "since must come strictly before until" ;
  (* Obtain the data: *)
  let columns, timeseries =
    Lwt_main.run (
      RamenTimeseries.get conf ~duration max_data_points since until where
                          factors ~consolidation func_name data_fields) in
  (* Display results: *)
  let single_data_field = List.length data_fields = 1 in
  if with_header then (
    let column_names =
      Array.fold_left (fun res sc ->
        let v =
          List.map RamenTypes.to_string sc |>
          String.concat "." in
        if single_data_field then
          (if v = "" then List.hd data_fields else v) :: res
        else List.fold_left (fun res df ->
          (df ^(if v = "" then "" else "("^ v ^")")) :: res) res data_fields
      ) [] columns |> List.rev in
    List.print ~first:("#Time"^ sep) ~last:"\n" ~sep
                String.print stdout column_names) ;
  Enum.iter (fun (t, vs) ->
    Printf.printf "%f%s%a"
      t sep
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

let timerange conf func_name () =
  logger := make_logger conf.C.log_level ;
  match C.program_func_of_user_string func_name with
  | exception _ -> exit 1
  | program_name, func_name ->
      let mi_ma =
        Lwt_main.run (
          C.with_rlock conf (fun programs ->
            (* We need the func to know its buffer location.
             * Nothing better to do in case of error than to exit. *)
            let prog, func = C.find_func programs program_name func_name in
            let bname = C.archive_buf_name conf func in
            let typ = func.F.out_type in
            let ser = RingBufLib.ser_tuple_typ_of_tuple_typ typ in
            let params = prog.P.params in
            RamenSerialization.time_range bname ser params func.F.event_time))
      in
      match mi_ma with
        | None -> Printf.printf "No time info or no output yet.\n"
        | Some (mi, ma) -> Printf.printf "%f %f\n" mi ma

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
    logger := make_syslog conf.C.log_level
  else (
    let logdir =
      (* In case we serve several API from several daemon we will need an
       * additional option to set a different logdir for each. *)
      if to_stdout then None
      else Some (conf.C.persist_dir ^"/log/httpd") in
    Option.may mkdir_all logdir ;
    logger := make_logger ?logdir conf.C.log_level) ;
  (* We take the port and URL prefix from the given URL but does not take
   * into account the hostname or the scheme. *)
  let uri = Uri.of_string server_url in
  (* In a user-supplied URL string the default port should be as usual for
   * HTTP scheme: *)
  let port =
    match Uri.port uri with
    | Some p -> p
    | None ->
      (match Uri.scheme uri with
      | Some "https" -> 443
      | _ -> 80) in
  let url_prefix = Uri.path uri in
  if daemonize then do_daemonize () ;
  let (++) rout1 rout2 =
    fun meth path params headers body ->
      try%lwt rout1 meth path params headers body
      with RamenHttpHelpers.BadPrefix ->
        rout2 meth path params headers body in
  let router _ path _ _ _ =
    let path = String.join "/" path in
    RamenHttpHelpers.not_found (Printf.sprintf "Unknown resource %S" path)
  in
  let router = Option.map_default (fun prefix ->
    !logger.info "Starting Graphite impersonator on %S" prefix ;
    RamenGraphite.router conf prefix) router graphite ++ router in
  let router = Option.map_default (fun prefix ->
    !logger.info "Serving custom API on %S" prefix ;
    RamenApi.router conf prefix) router api ++ router in
  Lwt_main.run (
    async (fun () ->
      restart_on_failure "wait_all_pids_loop"
        RamenProcesses.wait_all_pids_loop false) ;
    restart_on_failure "http server"
      RamenExperiments.(specialize conf.C.persist_dir the_big_one) [|
        dummy_nop ;
        (fun () ->
          RamenHttpHelpers.http_service
            port url_prefix router fault_injection_rate) |]) ;
  Option.may exit !RamenProcesses.quit

let graphite_expand conf for_render query () =
  logger := make_logger conf.C.log_level ;
  let query = String.nsplit ~by:"." query in
  let te = Lwt_main.run (
    RamenGraphite.full_enum_tree_of_query conf ~anchor_right:for_render
                                               query) in
  let rec display indent te =
    let e = RamenGraphite.get te in
    let len = List.length e in
    List.iteri (fun i ((n, _), c) ->
      let first = i = 0
      and last = i = len - 1 in
      let prefix =
        if first then
          if indent = "" then "" else
          if last then "-" else "┬"
        else
          if last then "└" else "├" in
      Printf.printf "%s%s%s"
        (if first then "" else "\n"^indent)
        prefix n ;
      let indent' =
        indent
          ^ (if prefix <> "" then
              if last then " " else "│"
            else "")
          ^ String.make (String.length n) ' '
      in
      display indent' c
    ) e
  in
  display "" te ;
  Printf.printf "\n"

(*
 * Display various internal informations
 *)

let variants conf () =
  let open RamenExperiments in
  let experimenter_id = get_experimenter_id conf.C.persist_dir in
  Printf.printf "Experimenter Id: %d\n" experimenter_id ;
  Printf.printf "Experiments (legend: %s | %s | unselected):\n"
    (green "forced") (yellow "selected") ;
  List.iter (fun e ->
    Printf.printf "  %s:\n" e.name ;
    for i = 0 to Array.length e.variants - 1 do
      let v = e.variants.(i) in
      Printf.printf "    %s (%s%%):\n%s\n"
        ((if e.variant = i then
           if e.forced then green else yellow
         else identity) v.Variant.name)
        (nice_string_of_float (100. *. v.Variant.share))
        (reindent "      " v.Variant.descr)
    done ;
    Printf.printf "\n"
  ) all_experiments

let stats conf () =
  Binocle.display_console ()
