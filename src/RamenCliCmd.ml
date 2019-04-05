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
module O = RamenOperation
module T = RamenTypes
module N = RamenName
module OutRef = RamenOutRef
module Files = RamenFiles

let () =
  Printexc.register_printer (function
    | Failure msg -> Some msg
    | _ -> None)

let make_copts debug quiet persist_dir rand_seed keep_temp_files
               forced_variants initial_export_duration site =
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
              ~initial_export_duration ~site persist_dir

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
      else Some (N.path_cat [ conf.C.persist_dir ;
                              N.path "log/supervisor" ]) in
    Option.may Files.mkdir_all logdir ;
    init_logger ?logdir:(logdir :> string option) conf.C.log_level) ;
  check_binocle_errors () ;
  if daemonize then do_daemonize () ;
  let open RamenProcesses in
  (* Also attempt to repair the report/notifs ringbufs.
   * This is OK because there can be no writer right now, and the report
   * ringbuf being a non-wrapping buffer then reader part cannot be damaged
   * any way. For notifications we could have the alerter reading though,
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
 * `ramen alerter`
 *
 * Start the alerter process, which will read the notifications ringbuf
 * and perform whatever action it takes, most likely reaching out to
 * external systems.
 *
 * The actual work is done in module RamenAlerter.
 *)

let alerter conf notif_conf_file max_fpr daemonize to_stdout
             to_syslog () =
  if to_stdout && daemonize then
    failwith "Options --daemonize and --stdout are incompatible." ;
  if to_stdout && to_syslog then
    failwith "Options --syslog and --stdout are incompatible." ;
  if max_fpr < 0. || max_fpr > 1. then
    failwith "False-positive rate is a rate is a rate." ;
  (* The configuration file better exists, unless it's the default one in
   * which case it will be created with the default configuration: *)
  let notif_conf_file =
    match notif_conf_file with
    | None ->
        let notif_conf_file =
          N.path_cat [ conf.C.persist_dir ; N.path "alerter.conf" ] in
        RamenAlerter.ensure_conf_file_exists notif_conf_file ;
        notif_conf_file
    | Some notif_conf_file ->
        if Files.check ~min_size:1 notif_conf_file <> FileOk then (
          Printf.sprintf2 "Configuration file %a does not exist."
            N.path_print notif_conf_file |>
          failwith
        ) else (
          (* Try to parse that file while we have the user attention: *)
          ignore (RamenAlerter.load_config notif_conf_file)
        ) ;
        notif_conf_file in
  if to_syslog then
    init_syslog conf.C.log_level
  else (
    let logdir =
      if to_stdout then None
      else Some (N.path_cat [ conf.C.persist_dir ;
                              N.path "log/alerter" ]) in
    Option.may Files.mkdir_all logdir ;
    init_logger ?logdir:(logdir :> string option) conf.C.log_level) ;
  check_binocle_errors () ;
  if daemonize then do_daemonize () ;
  RamenProcesses.prepare_signal_handlers conf ;
  let notify_rb = RamenProcesses.prepare_notifs conf in
  restart_on_failure ~while_ "process_notifications"
    RamenExperiments.(specialize the_big_one) [|
      RamenProcesses.dummy_nop ;
      (fun () ->
        RamenAlerter.start conf notif_conf_file notify_rb max_fpr) |] ;
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
 * `ramen tunneld`
 *
 * Runs a service allowing other sites to transfer tuples to workers running
 * on this one.
 *)

let tunneld conf daemonize to_stdout to_syslog port () =
  if to_stdout && daemonize then
    failwith "Options --daemonize and --stdout are incompatible." ;
  if to_stdout && to_syslog then
    failwith "Options --syslog and --stdout are incompatible." ;
  if to_syslog then
    init_syslog conf.C.log_level
  else (
    let logdir =
      (* In case we serve several API from several daemon we will need an
       * additional option to set a different logdir for each. *)
      if to_stdout then None
      else Some (N.path_cat [ conf.C.persist_dir ;
                              N.path "log/tunneld" ]) in
    Option.may Files.mkdir_all logdir ;
    init_logger ?logdir:(logdir :> string option) conf.C.log_level) ;
  check_binocle_errors () ;
  if daemonize then do_daemonize () ;
  RamenProcesses.prepare_signal_handlers conf ;
  RamenCopySrv.copy_server conf port ;
  Option.may exit !RamenProcesses.quit

(*
 * `ramen compile`
 *
 * Turn a ramen program into an executable binary.
 * Actual work happens in RamenCompiler.
 *)

(* Note: We need a program name to identify relative parents. *)
let compile conf lib_path use_external_compiler bundle_dir
            max_simult_compils smt_solver source_files
            output_file_opt program_name_opt () =
  let many_source_files = List.length source_files > 1 in
  if many_source_files && program_name_opt <> None then
    failwith "Cannot specify the program name for several source files" ;
  init_logger conf.C.log_level ;
  (* There is a long way to calling the compiler so we configure it from
   * here: *)
  RamenCompiler.init use_external_compiler bundle_dir max_simult_compils
                     smt_solver ;
  let get_parent =
    if lib_path = [] then
      let programs = C.with_rlock conf identity in
      RamenCompiler.parent_from_programs programs
    else
      List.map Files.absolute_path_of lib_path |>
      RamenCompiler.parent_from_lib_path in
  let all_ok = ref true in
  let compile_file source_file =
    let program_name_opt =
      if program_name_opt <> None then
        program_name_opt
      else
        (* Try to get an idea from the lib-path: *)
        match lib_path with
        | p::_ ->
          (try
            let p =
              Files.remove_ext source_file |>
              Files.rel_path_from (Files.absolute_path_of p) in
            Some (N.program (p :> string))
          with Failure s ->
            !logger.debug "%s" s ;
            None)
        | [] -> None in
    let program_name =
      match program_name_opt with
      | Some p -> p
      | None -> RamenRun.default_program_name source_file
    in
    let output_file =
      Option.default_delayed (fun () ->
        Files.change_ext "x" source_file
      ) output_file_opt in
    RamenMake.build conf ~force_rebuild:true get_parent program_name
                    source_file output_file
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

let run conf params replace kill_if_disabled report_period program_name_opt
        src_file on_site bin_file () =
  let params = List.enum params |> Hashtbl.of_enum in
  init_logger conf.C.log_level ;
  (* If we run in --debug mode, also set that worker in debug mode: *)
  let debug = conf.C.log_level = Debug in
  RamenRun.run conf ~params ~debug ~replace ~kill_if_disabled ~report_period
               ?src_file ~on_site bin_file program_name_opt

(*
 * `ramen kill`
 *
 * Remove that program from the list of running programs.
 * This time the program is identified by its name not its executable file.
 *)

let kill conf program_names purge () =
  init_logger conf.C.log_level ;
  let num_kills = RamenRun.kill conf ~purge program_names in
  Printf.printf "Killed %d program%s\n"
    num_kills (if num_kills > 1 then "s" else "")

(*
 * `ramen info`
 *
 * Display a program or function meta information from the binary.
 *)

let info _conf params program_name_opt bin_file opt_func_name () =
  let params = List.enum params |> Hashtbl.of_enum in
  let program_name =
    Option.default_delayed (fun () ->
      RamenRun.default_program_name bin_file
    ) program_name_opt in
  let prog = P.of_bin program_name params bin_file in
  if prog.params <> [] then (
    TermTable.print_head 0 "Parameters" ;
    TermTable.print 1 "%a" RamenTuple.print_params prog.params) ;
  Option.may (fun run_cond ->
    TermTable.print_head 0 "Running condition" ;
    TermTable.print_string 1 run_cond
  ) prog.condition ;
  let info_func i func =
    TermTable.print i "%s" (N.func_color func.F.name) ;
    if func.doc <> "" then TermTable.print_abstract i func.doc ;
    TermTable.print_head (i+1) "Parents" ;
    if func.parents = [] then
      TermTable.print (i+2) "None"
    else
      List.iter (function
        | site, None, f ->
            TermTable.print (i+2) "%a%s"
              O.print_site_identifier site
              (N.func_color f)
        | site, Some rp, f ->
            TermTable.print (i+2) "%a%s/%s"
              O.print_site_identifier site
              (N.rel_program_color rp)
              (N.func_color f)
      ) func.parents ;
    TermTable.print_head (i+1) "Input type" ;
    TermTable.print (i+2) "%a" RamenFieldMaskLib.print_in_type func.in_type ;
    let out_type =
      O.out_type_of_operation ~with_private:false func.operation in
    TermTable.print_head (i+1) "Output type" ;
    TermTable.print (i+2) "Ramen: %a" RamenTuple.print_typ out_type ;
    TermTable.print (i+2) "ORC: %a"
      RamenOrc.print (RamenOrc.of_tuple out_type) ;
    O.event_time_of_operation func.operation |>
    Option.may (fun et ->
      TermTable.print_head (i+1) "Event time" ;
      TermTable.print (i+2) "%a" RamenEventTime.print et) ;
    print_endline "" ;
    if func.F.persistent || func.F.is_lazy then (
      let lst = [] in
      let lst =
        if func.F.persistent then "persistent"::lst else lst in
      let lst =
        if func.F.is_lazy then "lazy"::lst else lst in
      TermTable.print_head (i+1) "Misc" ;
      TermTable.print (i+2) "%a"
        (List.print ~first:"" ~last:"" ~sep:", " String.print) lst
    )
  in
  match opt_func_name with
  | None ->
      TermTable.print_head 0 "Functions" ;
      List.iter (info_func 1) prog.funcs
  | Some func_name ->
      (match List.find (fun func ->
               func.F.name = func_name) prog.funcs with
      | exception Not_found ->
          Printf.sprintf2 "No such function %a"
            N.func_print func_name |>
          failwith
      | func -> info_func 0 func)

(*
 * `ramen gc`
 *
 * Delete old or unused files.
 *)

let gc conf dry_run del_ratio compress_older loop daemonize
       to_stdout to_syslog () =
  if to_stdout && daemonize then
    failwith "Options --daemonize and --stdout are incompatible." ;
  if to_stdout && to_syslog then
    failwith "Options --syslog and --stdout are incompatible." ;
  if daemonize && loop = Some 0. then
    failwith "It makes no sense to --daemonize without --loop." ;
  let loop = loop |? Default.gc_loop in
  if to_syslog then
    init_syslog conf.C.log_level
  else (
    let logdir =
      if to_stdout then None
      else Some (N.path_cat [ conf.C.persist_dir ;
                              N.path "log/gc" ]) in
    Option.may Files.mkdir_all logdir ;
    init_logger ?logdir:(logdir :> string option) conf.C.log_level) ;
  if loop <= 0. then
    RamenGc.cleanup_once conf dry_run del_ratio compress_older
  else (
    check_binocle_errors () ;
    if daemonize then do_daemonize () ;
    RamenGc.cleanup_loop conf dry_run del_ratio compress_older loop)

(*
 * `ramen ps`
 *
 * Display information about running programs and quit.
 *)

let sort_col_of_string spec str =
  let str = String.(trim str |> lowercase) in
  try int_of_string str
  with Failure _ ->
    let matching =
      Array.enum spec |> Enum.foldi (fun i c l ->
        let c = String.lowercase c in
        if String.starts_with c str then (i, c) :: l else l) [] in
    (match matching with
    | [ i, _ ] -> i
    | [] ->
        Printf.sprintf2 "Unknown column %S (must be one of %a)"
          str
          (pretty_array_print String.print_quoted) spec |>
        failwith
    | l ->
        Printf.sprintf2 "Ambiguous: %S could be %a"
          str
          (pretty_list_print (fun oc (_, s) ->
            String.print_quoted oc s)) l |>
        failwith)

let ps_ profile conf short pretty with_header sort_col top pattern all () =
  init_logger conf.C.log_level ;
  let must_run_here rce =
    rce.C.status = C.MustRun && C.match_localsite conf rce.C.on_site in
  (* Start by reading the last minute of instrumentation data: *)
  let stats = RamenPs.read_stats conf in
  (* Now iter over all workers and display those stats: *)
  let open TermTable in
  if short then
    let head =
      if profile then
        [| "program" ; "CPU" ; "wait in" ; "wait out" ;
           "tot_per_tuple" ; "where_fast" ; "find_group" ; "where_slow" ;
           "update_group" ; "commit_incoming" ; "select_others" ;
           "finalize_others" ; "commit_others" ; "flush_others" |]
      else
        [| "program" ; "parameters" ; "#in" ; "#selected" ; "#out" ; "#groups" ;
           "CPU" ; "wait in" ; "wait out" ; "heap" ; "max heap" ; "volume in" ;
           "volume out" |]  in
    let sort_col = sort_col_of_string head sort_col in
    let print = print_table ~pretty ~sort_col ~with_header ?top head in
    (* For --short, we sum everything by program: *)
    let h = RamenPs.per_program stats in
    C.with_rlock conf (fun programs ->
      Hashtbl.iter (fun (program_name : N.program) (rce, _get_rc) ->
        if (all || must_run_here rce) &&
           Globs.matches pattern (program_name :> string)
        then (
          let s = Hashtbl.find_default h program_name RamenPs.no_stats in
          (if profile then
            [| Some (ValStr (program_name :> string)) ;
               Some (ValFlt s.cpu) ;
               flt_or_na s.wait_in ;
               flt_or_na s.wait_out ;
               perf s.profile.tot_per_tuple ;
               perf s.profile.where_fast ;
               perf s.profile.find_group ;
               perf s.profile.where_slow ;
               perf s.profile.update_group ;
               perf s.profile.commit_incoming ;
               perf s.profile.select_others ;
               perf s.profile.finalize_others ;
               perf s.profile.commit_others ;
               perf s.profile.flush_others |]
          else
            [| Some (ValStr (program_name :> string)) ;
               Some (ValStr (RamenParams.to_string rce.C.params)) ;
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
               flt_or_na (Option.map Uint64.to_float s.bytes_out) |]) |>
          print
        )
      ) programs) ;
    print [||]
  else
    (* Otherwise we want to display all we can about individual workers *)
    let head =
      if profile then
        [| "operation" ; "CPU" ; "wait in" ; "wait out" ;
           "tot_per_tuple" ; "where_fast" ; "find_group" ; "where_slow" ;
           "update_group" ; "commit_incoming" ; "select_others" ;
           "finalize_others" ; "commit_others" ; "flush_others" |]
      else
        [| "operation" ; "#in" ; "#selected" ; "#out" ; "#groups" ;
           "last out" ; "min event time" ; "max event time" ; "CPU" ;
           "wait in" ; "wait out" ; "heap" ; "max heap" ; "volume in" ;
           "volume out" ; "avg out sz" ; "startup time" ;
           "#parents" ; "#children" ; "signature" |] in
    let sort_col = sort_col_of_string head sort_col in
    let print = print_table ~pretty ~sort_col ~with_header ?top head in
    C.with_rlock conf (fun programs ->
      (* First pass to get the children: *)
      let children_of_func = Hashtbl.create 23 in
      Hashtbl.iter (fun _prog_name (rce, get_rc) ->
        if all || must_run_here rce then match get_rc () with
        | exception _ -> ()
        | prog ->
            List.iter (fun func ->
              List.iter (fun (_, pp, pf) ->
                (* We could use the pattern to filter out uninteresting
                 * parents but there is not much to save at this point. *)
                let k =
                  F.program_of_parent_prog func.F.program_name pp, pf in
                Hashtbl.add children_of_func k func
              ) func.F.parents
            ) prog.P.funcs
      ) programs ;
      Hashtbl.iter (fun program_name (rce, get_rc) ->
        if all || must_run_here rce then match get_rc () with
        | exception _ -> (* which has been logged already *) ()
        | prog ->
          List.iter (fun func ->
            let fq = N.fq_of_program program_name func.F.name in
            if Globs.matches pattern (fq :> string) then
              (* TODO: also lookup for the top-half and... display it on
               * a separate line? *)
              let s =
                Hashtbl.find_default stats (fq, false) RamenPs.no_stats in
              (if profile then
                [| Some (ValStr (fq :> string)) ;
                   Some (ValFlt s.cpu) ;
                   flt_or_na s.wait_in ;
                   flt_or_na s.wait_out ;
                   perf s.profile.tot_per_tuple ;
                   perf s.profile.where_fast ;
                   perf s.profile.find_group ;
                   perf s.profile.where_slow ;
                   perf s.profile.update_group ;
                   perf s.profile.commit_incoming ;
                   perf s.profile.select_others ;
                   perf s.profile.finalize_others ;
                   perf s.profile.commit_others ;
                   perf s.profile.flush_others |]
               else
                let num_children = Hashtbl.find_all children_of_func
                                     (func.F.program_name, func.F.name) |>
                                     List.length in
                [| Some (ValStr (fq :> string)) ;
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
                   flt_or_na (Option.map Uint64.to_float s.avg_full_bytes) ;
                   Some (ValDate s.startup_time) ;
                   Some (ValInt (List.length func.F.parents)) ;
                   Some (ValInt num_children) ;
                   Some (ValStr func.signature) |]) |>
              print
          ) prog.P.funcs
      ) programs) ;
      print [||]

let ps = ps_ false
let profile = ps_ true

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

let table_formatter pretty raw null units =
  (* TODO: reuse RamenTypes in TermTable *)
  let open RamenTypes in
  let open TermTable in
  function
  | VFloat t ->
      if units = Some RamenUnits.seconds_since_epoch && pretty then
        Some (ValDate t)
      else if units = Some RamenUnits.seconds && pretty then
        Some (ValDuration t)
      else
        Some (ValFlt t)
  | VNull -> None
  | t ->
    Some (ValStr (IO.to_string (print_custom ~null ~quoting:(not raw)) t))

(* [func_name_or_code] is a list of strings as present in the command
 * line. It could be a function name followed by a list of field names, or
 * it could be the code of a function which output we want to display.
 * We find out by picking the first that works. In case both approach
 * fails we have to display the two error messages though. *)
let parse_func_name_of_code conf what func_name_or_code =
  let parse_as_names () =
    match func_name_or_code with
    | func_name :: field_names ->
        let fq = N.fq func_name
        and field_names = List.map N.field field_names in
        let ret = fq, field_names, [] in
        (* First, is it any of the special ringbuf? *)
        if String.ends_with func_name ("#"^ SpecialFunctions.stats) ||
           String.ends_with func_name ("#"^ SpecialFunctions.notifs) then
          ret
        else (
          (* Check this function exists: *)
          C.with_rlock conf (fun programs ->
            C.find_func programs fq |> ignore) ;
          ret)
    | _ -> assert false (* As the command line parser prevent this *)
  and parse_as_code () =
    let program_name = C.make_transient_program () in
    let func_name = N.func "f" in
    let programs = C.with_rlock conf identity in (* best effort *)
    let get_parent = RamenCompiler.parent_from_programs programs in
    let oc, src_file =
      BatFile.open_temporary_out ~prefix:what ~suffix:".ramen" () in
    let src_file = N.path src_file in
    !logger.info "Going to use source file %a"
      N.path_print_quoted src_file ;
    let exec_file = Files.change_ext "x" src_file in
    on_error (fun () ->
      if not conf.C.keep_temp_files then (
        Files.safe_unlink src_file ;
        Files.safe_unlink exec_file))
             (fun () ->
      Printf.fprintf oc
        "-- Temporary file created on %a for %s\n\
         DEFINE '%s' AS %a\n"
        (print_as_date ?rel:None ?right_justified:None) (Unix.time ())
        what
        (func_name :> string)
        (List.print ~first:"" ~last:"" ~sep:" " String.print)
          func_name_or_code ;
      close_out oc ;
      Files.safe_unlink exec_file ; (* hahaha! *)
      RamenMake.build conf get_parent program_name src_file exec_file ;
      (* Run it, making sure it archives its history straight from the start: *)
      let debug = conf.C.log_level = Debug in
      RamenRun.run conf ~report_period:0. ~src_file ~debug
                   exec_file (Some program_name)) ;
    let fq = N.fq_of_program program_name func_name in
    fq, [], [ program_name ]
  in
  try
    parse_as_names ()
  with e1 ->
    try parse_as_code ()
    with e2 ->
      let cmd_line = String.join " " func_name_or_code in
      Printf.sprintf
        "Cannot parse %S as a function name (%s) nor as a program (%s)"
        cmd_line
        (Printexc.to_string e1)
        (Printexc.to_string e2) |>
      failwith

let head_of_types ~with_units head_typ =
  Array.map (fun t ->
    (t.RamenTuple.name :> string) ^
       (if with_units then
         Option.map_default RamenUnits.to_string "" t.units
       else "")
  ) head_typ

let tail_ conf fq field_names with_header with_units sep null raw
          last next min_seq max_seq continuous where since until
          with_event_time duration pretty flush =
  if (last <> None || next <> None || continuous) &&
     (min_seq <> None || max_seq <> None) then
    failwith "Options --{last,next,continuous} and \
              --{min,max}-seq are incompatible." ;
  if continuous && next <> None then
    failwith "Option --next and --continuous are incompatible." ;
  if with_units && with_header = 0 then
    failwith "Option --with-units makes no sense without --with-header" ;
  (* Do something useful by default: display the 10 last lines *)
  let last =
    if last = None && next = None && min_seq = None && max_seq = None &&
       since = None && until = None
    then Some 10 else last in
  let flush = flush || continuous in
  let next = if continuous then Some max_int else next in
  let bname, is_temp_export, filter, _typ, ser, params, event_time =
    RamenExport.read_output conf ~duration fq where
  in
  (* Find out which seqnums we want to scan: *)
  let mi, ma = match last, next with
    | None, None ->
        min_seq,
        Option.map succ max_seq (* max_seqnum is in *)
    | Some l, None ->
        let _mi, ma = RingBufLib.seq_range bname in
        Some (cap_add ma ~-l),
        Some ma
    | None, Some n ->
        let _mi, ma = RingBufLib.seq_range bname in
        Some ma, Some (cap_add ma n)
    | Some l, Some n ->
        let _mi, ma = RingBufLib.seq_range bname in
        Some (cap_add ma ~-l), Some (cap_add ma n)
  in
  !logger.debug "Will display tuples from %a (incl) to %a (excl)"
    (Option.print Int.print) mi
    (Option.print Int.print) ma ;
  let field_names = RamenExport.check_field_names ser field_names in
  let head_idx, head_typ =
    RamenExport.header_of_type ~with_event_time field_names ser in
  let open TermTable in
  let head_typ = Array.of_list head_typ in
  let head = head_of_types ~with_units head_typ in
  let print = print_table ~sep ~pretty ~with_header ~flush head in
  (* Pick a "printer" for each column according to the field type: *)
  let formatter = table_formatter pretty raw null
  in
  if is_temp_export then (
    let rec reset_export_timeout () =
      let _mre, _prog, func =
        C.with_rlock conf (fun programs ->
          C.find_func_or_fail programs fq) in
      let _ = RamenProcesses.start_export conf ~duration func in
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
    | Some et ->
        event_time_of_tuple ser params et
  in
  let unserialize = RamenSerialization.read_array_of_values ser in
  (* Then, scan all present ringbufs in the requested range (either
   * the last N tuples or, TBD, since ts1 [until ts2]) and display
   * them *)
  fold_seq_range ~wait_for_more:true bname ?mi ?ma () (fun () _m tx ->
    match RamenSerialization.read_tuple unserialize tx with
    | RingBufLib.DataTuple chan, Some tuple
      when chan = RamenChannel.live && filter tuple ->
        let t1, t2 = event_time_of_tuple tuple in
        if Option.map_default (fun since -> t2 > since) true since &&
           Option.map_default (fun until -> t1 <= until) true until
        then (
          let cols =
            Array.mapi (fun i idx ->
              match idx with
              | -2 -> Some (ValDate t2)
              | -1 -> Some (ValDate t1)
              | idx -> formatter head_typ.(i).units tuple.(idx)
            ) head_idx in
          print cols)
    | _ -> ()) ;
  print [||]

let purge_transient conf to_purge () =
  if to_purge <> [] then
    let patterns =
      List.map (fun (p : N.program) ->
        Globs.(compile (escape (p :> string)))
      ) to_purge in
    let nb_kills = RamenRun.kill conf ~purge:true patterns in
    !logger.debug "Killed %d programs" nb_kills

let tail conf func_name_or_code with_header with_units sep null raw
         last next min_seq max_seq continuous where since until
         with_event_time duration pretty flush
         (* We might compile the command line: *)
         use_external_compiler bundle_dir max_simult_compils smt_solver
         () =
  init_logger conf.C.log_level ;
  RamenCompiler.init use_external_compiler bundle_dir max_simult_compils
                     smt_solver ;
  let fq, field_names, to_purge =
    parse_func_name_of_code conf "ramen tail" func_name_or_code in
  finally (purge_transient conf to_purge)
    (tail_ conf fq field_names with_header with_units sep null raw
           last next min_seq max_seq continuous where since until
           with_event_time duration pretty) flush

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
 * time series.
 *)

let replay_ conf fq field_names with_header with_units sep null raw
            where since until with_event_time pretty flush =
  if with_units && with_header = 0 then
    failwith "Option --with-units makes no sense without --with-header" ;
  let until = until |? Unix.gettimeofday () in
  let formatter = table_formatter pretty raw null in
  RamenExport.replay conf ~while_ fq field_names where since until
                     ~with_event_time (fun head ->
      let head = Array.of_list head in
      let head' = head_of_types ~with_units head in
      let print = TermTable.print_table ~na:null ~sep ~pretty ~with_header
                                        ~flush head' in
      (fun _t1 _t2 tuple ->
        let vals =
          Array.mapi (fun i -> formatter head.(i).units) tuple in
        print vals), ignore)

let replay conf func_name_or_code with_header with_units sep null raw
           where since until with_event_time pretty flush
           (* We might compile the command line: *)
           use_external_compiler bundle_dir max_simult_compils smt_solver
           () =
  init_logger conf.C.log_level ;
  RamenCompiler.init use_external_compiler bundle_dir max_simult_compils
                     smt_solver ;
  let fq, field_names, to_purge =
    parse_func_name_of_code conf "ramen tail" func_name_or_code in
  finally (purge_transient conf to_purge)
    (replay_ conf fq field_names with_header with_units sep null raw
             where since until with_event_time pretty) flush

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

let timeseries_ conf fq data_fields
                since until with_header where factors num_points
                time_step sep null consolidation
                bucket_time pretty =
  let num_points =
    if num_points <= 0 && time_step <= 0. then 100 else num_points in
  let until = until |? Unix.gettimeofday () in
  let since = since |? until -. 600. in
  (* Obtain the data: *)
  let num_points, since, until =
    RamenTimeseries.compute_num_points time_step num_points since until in
  let columns, timeseries =
    RamenTimeseries.get conf num_points since until where factors
        ~consolidation ~bucket_time fq data_fields in
  (* Display results: *)
  let single_data_field = List.length data_fields = 1 in
  let head =
    Array.fold_left (fun res sc ->
      let v =
        Array.enum sc /@ T.to_string |>
        List.of_enum |>
        String.concat "." in
      if single_data_field then
        (if v = "" then (List.hd data_fields :> string) else v) :: res
      else
        List.fold_left (fun res (df : N.field) ->
          ((df :> string) ^ (if v = "" then "" else "("^ v ^")")) :: res
        ) res data_fields
    ) [] columns |> List.rev in
  let head = "Time" :: head |> Array.of_list in
  let open TermTable in
  let print = print_table ~na:null ~sep ~pretty ~with_header head in
  Enum.iter (fun (t, vs) ->
    Array.append
      [| Some (ValDate t) |]
      (Array.concat
        (List.map
          (Array.map (function
            | None -> None
            | Some v -> Some (ValFlt v))) (Array.to_list vs))) |>
    print
  ) timeseries ;
  print [||]

let timeseries conf func_name_or_code
               since until with_header where factors num_points
               time_step sep null consolidation
               bucket_time pretty
               (* We might compile the command line: *)
               use_external_compiler bundle_dir max_simult_compils smt_solver
               () =
  init_logger conf.C.log_level ;
  RamenCompiler.init use_external_compiler bundle_dir max_simult_compils
                     smt_solver ;
  let fq, field_names, to_purge =
    parse_func_name_of_code conf "ramen tail" func_name_or_code in
  finally (purge_transient conf to_purge)
    (timeseries_ conf fq field_names
                 since until with_header where factors num_points
                 time_step sep null consolidation
                 bucket_time) pretty

(*
 * `ramen timerange`
 *
 * Obtain information about the time range available for time series.
 *)

let timerange conf fq () =
  init_logger conf.C.log_level ;
  C.with_rlock conf (fun programs ->
    let _mre, prog, func = C.find_func_or_fail programs fq in
    let mi_ma =
      (* We need the func to know its buffer location.
       * Nothing better to do in case of error than to exit.
       * The archive dir is going to be scanned, then this file in case
       * it exists: *)
      let bname = C.archive_buf_name ~file_type:OutRef.RingBuf conf func in
      let typ =
        O.out_type_of_operation func.F.operation in
      let ser = RingBufLib.ser_tuple_typ_of_tuple_typ typ |>
                List.map fst in
      let params = prog.P.params in
      let event_time =
        O.event_time_of_operation func.operation in
      RamenSerialization.time_range bname ser params event_time
    in
    match mi_ma with
      | None -> Printf.printf "No time info or no output yet.\n"
      | Some (mi, ma) -> Printf.printf "%f %f\n" mi ma)

(*
 * `ramen httpd`
 *
 * Starts an HTTP daemon that will serve (and maybe one day also accept)
 * time series, either impersonating Graphite API (https://graphiteapp.org/)
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
      else Some (N.path_cat [ conf.C.persist_dir ;
                              N.path "log/httpd" ]) in
    Option.may Files.mkdir_all logdir ;
    init_logger ?logdir:(logdir :> string option) conf.C.log_level) ;
  check_binocle_errors () ;
  if daemonize then do_daemonize () ;
  RamenProcesses.prepare_signal_handlers conf ;
  RamenHttpd.run_httpd conf server_url api graphite fault_injection_rate ;
  Option.may exit !RamenProcesses.quit

(* TODO: allow several queries as in the API *)
let graphite_expand conf for_render since until query () =
  init_logger conf.C.log_level ;
  if not for_render then
    RamenGraphite.metrics_find conf ?since ?until query |>
    List.of_enum |>
    PPP.to_string ~pretty:true RamenGraphite.metrics_ppp_json |>
    Printf.printf "%s\n"
  else
    RamenGraphite.targets_for_render conf ?since ?until [ query ] |>
    Enum.iter (fun (_func, fq, fvals, data_field) ->
      Printf.printf "%a %a with %a"
        N.fq_print fq
        N.field_print data_field
        (Set.print (fun oc (factor, opt_val) ->
          Printf.fprintf oc "%a:" N.field_print factor ;
          match opt_val with
          | None -> String.print oc "*"
          | Some v -> T.print oc v)) fvals)

(*
 * `ramen archivist`
 *
 * A daemon that react to RC stats and a user configuration file
 * and decides what worker should save its history in order to be
 * able to retrieve or rebuild the output of all persistent functions.
 *)

let archivist conf loop daemonize stats allocs reconf
              to_stdout to_syslog () =
  if to_stdout && daemonize then
    failwith "Options --daemonize and --stdout are incompatible." ;
  if to_stdout && to_syslog then
    failwith "Options --syslog and --stdout are incompatible." ;
  if not stats && not allocs && not reconf then
    failwith "Must specify at least one of --stats, --allocs or --reconf" ;
  if daemonize && loop = Some 0. then
    failwith "It makes no sense to --daemonize without --loop." ;
  let loop = loop |? Default.archivist_loop in
  if to_syslog then
    init_syslog conf.C.log_level
  else (
    let logdir =
      if to_stdout then None
      else Some (N.path_cat [ conf.C.persist_dir ;
                              N.path "log/archivist" ]) in
    Option.may Files.mkdir_all logdir ;
    init_logger ?logdir:(logdir :> string option) conf.C.log_level) ;
  RamenProcesses.prepare_signal_handlers conf ;
  if loop <= 0. then
    RamenArchivist.run_once conf ~while_ stats allocs reconf
  else (
    check_binocle_errors () ;
    if daemonize then do_daemonize () ;
    RamenArchivist.run_loop conf ~while_ loop stats allocs reconf)

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
