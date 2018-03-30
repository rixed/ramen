(* For each ramen command, check arguments and mostly transfer the control
 * further to more specialized modules. *)
open Batteries
open Lwt
open RamenLog
open RamenHelpers
module C = RamenConf
module F = RamenConf.Func
module P = RamenConf.Program

let () =
  async_exception_hook := (fun exn ->
    !logger.error "Received exception %s\n%s"
      (Printexc.to_string exn)
      (Printexc.get_backtrace ()))

let make_copts debug persist_dir max_archives use_embedded_compiler
               bundle_dir max_simult_compilations rand_seed keep_temp_files =
  (match rand_seed with
  | None -> Random.self_init ()
  | Some seed -> Random.init seed) ;
  C.make_conf ~debug ~max_simult_compilations ~max_archives
              ~use_embedded_compiler ~bundle_dir ~keep_temp_files persist_dir

(*
 * `ramen start`
 *
 * Start the process supervisor, which will keep running the programs
 * present in the configuration/rc file (and kill the others).
 * This does not return (under normal circumstances).
 *
 * The actual work is done in module RamenProcesses.
 *)

let start conf daemonize to_stderr () =
  if to_stderr && daemonize then
    failwith "Options --daemonize and --to-stderr are incompatible." ;
  let logdir =
    if to_stderr then None else Some (conf.C.persist_dir ^"/log") in
  Option.may mkdir_all logdir ;
  logger := make_logger ?logdir conf.C.debug ;
  if daemonize then do_daemonize () ;
  (* Prepare ringbuffers for reports and notifications: *)
  let rb_name = C.report_ringbuf conf in
  RingBuf.create ~wrap:false rb_name RingBufLib.rb_default_words ;
  let rb_name = C.notify_ringbuf conf in
  RingBuf.create rb_name RingBufLib.rb_default_words ;
  let notify_rb = RingBuf.load rb_name in
  (* Install signal handlers *)
  set_signals Sys.[sigterm; sigint] (Signal_handle (fun s ->
    !logger.info "Received signal %s" (name_of_signal s) ;
    RamenProcesses.quit := true)) ;
  Lwt_main.run (
    join [
      (let%lwt () = Lwt_unix.sleep 1. in
       (* TODO: Also a separate command to do the cleaning? *)
       async (fun () ->
         restart_on_failure RamenProcesses.cleanup_old_files conf) ;
       async (fun () ->
         restart_on_failure RamenProcesses.process_notifications notify_rb) ;
       return_unit) ;
      (* The main job of this process is to make what's actually running
       * in accordance to the running program list: *)
      restart_on_failure RamenProcesses.synchronize_running conf ])

(*
 * `ramen compile`
 *
 * Turn a ramen program into an executable binary.
 * Actual work happens in RamenCompiler.
 *)

let compile conf root_path source_files () =
  logger := make_logger conf.C.debug ;
  let all_ok = ref true in
  let comp_file source_file =
    let program_name = Filename.remove_extension source_file |>
                       rel_path_from root_path
    and program_code = read_whole_file source_file in
    RamenCompiler.compile conf root_path program_name program_code
  in
  List.iter (fun source_file ->
    try
      comp_file source_file
    with e ->
      print_exception e ;
      all_ok := false
  ) source_files ;
  if not !all_ok then exit 1

(*
 * `ramen run`
 *
 * Ask the ramen daemon to start a compiled program.
 *)

let run conf parameters bin_files () =
  logger := make_logger conf.C.debug ;
  Lwt_main.run (
    C.with_wlock conf (fun running_programs ->
      List.iter (fun bin ->
        let bin = absolute_path_of bin in
        let rc = P.of_bin bin in
        let program_name = (List.hd rc).F.program_name in
        Hashtbl.add running_programs program_name C.{ bin ; parameters }
      ) bin_files ;
      return_unit))

(*
 * `ramen kill`
 *
 * Remove that program from the list of running programs.
 * This time the program is identified by its name not its executable file.
 * TODO: warn if this orphans some children.
 *)

let kill conf prog_name () =
  logger := make_logger conf.C.debug ;
  let nb_kills =
    Lwt_main.run (
      C.with_wlock conf (fun running_programs ->
        let before = Hashtbl.length running_programs in
        Hashtbl.filteri_inplace (fun name _mre ->
          name <> prog_name
        ) running_programs ;
        return (Hashtbl.length running_programs - before))) in
  Printf.printf "Killed %d program%s\n"
    nb_kills (if nb_kills > 1 then "s" else "")

(*
 * `ramen ps`
 *
 * Display information about running programs and quit.
 *)

let int_or_na = function
  | None -> TermTable.ValStr "n/a"
  | Some i -> TermTable.ValInt i

let flt_or_na = function
  | None -> TermTable.ValStr "n/a"
  | Some f -> TermTable.ValFlt f

let str_or_na = function
  | None -> TermTable.ValStr "n/a"
  | Some s -> TermTable.ValStr s

let time_or_na = function
  | None -> TermTable.ValStr "n/a"
  | Some f -> TermTable.ValStr (string_of_time f)

(* TODO: The supervisor should save the last report somewhere in a file in
 * persist_dir *)
let ps conf short () =
  logger := make_logger conf.C.debug ;
  let open TermTable in
  let head, lines =
    Lwt_main.run (
      C.with_rlock conf (fun programs ->
        return (
          if short then
            [| "name" ; "#ops" ; "status" ; "started" ; "stopped" ; "executable" |],
            Hashtbl.fold (fun program_name get_rc lst ->
              let bin, rc = get_rc () in
              [| ValStr program_name ;
                 ValInt (List.length rc) ;
                 ValStr "TODO" ;
                 time_or_na None ;
                 time_or_na None ;
                 ValStr bin |] :: lst
            ) programs []
          else
            [| "operation" ; "#in" ; "#selected" ; "#out" ;
               "#groups" ; "CPU" ; "wait in" ; "wait out" ; "heap" ;
               "volume in" ; "volume out" ; "#parents" ; "signature" |],
            Hashtbl.fold (fun program_name get_rc lines ->
              let bin, rc = get_rc () in
              List.fold_left (fun lines func ->
                let stats =
                  (* Fake it: *)
                  RamenProcesses.{
                    time = 0. ; in_tuple_count = None ;
                    selected_tuple_count = None ; out_tuple_count = None ;
                    group_count = None ; cpu_time = 0. ; ram_usage = 0 ;
                    in_sleep = None ; out_sleep = None ; in_bytes = None ;
                    out_bytes = None } in
                [| ValStr (program_name ^"/"^ func.F.name) ;
                   int_or_na stats.in_tuple_count ;
                   int_or_na stats.selected_tuple_count ;
                   int_or_na stats.out_tuple_count ;
                   int_or_na stats.group_count ;
                   ValFlt stats.cpu_time ;
                   flt_or_na stats.in_sleep ;
                   flt_or_na stats.out_sleep ;
                   ValInt stats.ram_usage ;
                   flt_or_na stats.in_bytes ;
                   flt_or_na stats.out_bytes ;
                   ValInt (List.length func.F.parents) ;
                   ValStr func.signature |] :: lines
              ) lines rc
            ) programs []))) in
  print_table head lines

(*
 * `ramen tail`
 *
 * Display the last tuple output by an operation.
 *
 * This first create a non-wrapping buffer file and then asks the operation
 * to write in there for 1 hour (by default).
 * This buffer name is standard so that other clients wishing to read those
 * tuples can reuse the same and benefit from a shared history.
 *)

let always_true _ = true

let tail conf func_name with_header separator null
         last min_seq max_seq with_seqnums duration () =
  logger := make_logger conf.C.debug ;
  (* Do something useful by default: tail forever *)
  let last =
    if last = None && min_seq = None && max_seq = None then Some min_int
    else last in
  let bname, filter, typ =
    (* Read directly from the instrumentation ringbuf when func_name ends
     * with "#stats" *)
    if func_name = "stats" || String.ends_with func_name "#stats" then
      let typ = RamenBinocle.tuple_typ in
      let wi = RamenSerialization.find_field_index typ "worker" in
      let filter =
        if func_name = "stats" then always_true else
        let func_name, _ = String.rsplit func_name ~by:"#" in
        fun tuple -> tuple.(wi) = RamenScalar.VString func_name in
      let bname = C.report_ringbuf conf in
      bname, filter, typ
    else
      (* Create the non-wrapping RingBuf (under a standard name given
       * by RamenConf *)
      Lwt_main.run (
        let%lwt _, bname, typ =
          RamenExport.make_temp_export_by_name conf ~duration func_name in
        return (bname, always_true, typ))
  in
  (* Find out which seqnums we want to scan: *)
  let mi, ma = RingBuf.seq_range bname in
  let mi = match min_seq with None -> mi | Some m -> max mi m in
  let ma = match max_seq with None -> ma | Some m -> m + 1 (* max_seqnum is in *) in
  let mi, ma = match last with
    | Some l when l >= 0 ->
        let mi = max mi (cap_add ma ~-l) in
        let ma = mi + l in
        mi, ma
    | Some l ->
        assert (l < 0) ;
        let mi = ma
        and ma = cap_add ma (cap_neg l) in
        mi, ma
    | None -> mi, ma in
  !logger.debug "Will display tuples from %d to %d" mi ma ;
  (* Then, scan all present ringbufs in the requested range (either
   * the last N tuples or, TBD, since ts1 [until ts2]) and display
   * them *)
  let nullmask_size =
    RingBufLib.nullmask_bytes_of_tuple_type typ in
  if with_header then (
    let first = if with_seqnums then "#Seq"^ separator else "#" in
    List.print ~first ~last:"\n" ~sep:separator
      (fun fmt ft -> String.print fmt ft.RamenTuple.typ_name)
      stdout typ ;
    BatIO.flush stdout) ;
  Lwt_main.run (
    let rec loop m =
      if m >= ma then return_unit else
      let%lwt m =
        let open RamenSerialization in
        fold_seq_range bname m ma m (fun m tx ->
          let tuple =
            read_tuple typ nullmask_size tx in
          if filter tuple then (
            if with_seqnums then (
              Int.print stdout m ; String.print stdout separator) ;
            Array.print ~first:"" ~last:"\n" ~sep:separator
              (RamenScalar.print_custom ~null) stdout tuple ;
            BatIO.flush stdout ;
            return (m + 1)
          ) else return m) in
      if m >= ma then return_unit else
        (* TODO: If we tail for a long time in continuous mode, we might
         * need to refresh the out-ref timeout from time to time. *)
        let delay = 1. +. Random.float 1. in
        let%lwt () = Lwt_unix.sleep delay in
        loop m
    in
    loop mi)

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

let timeseries conf since until max_data_points separator null
               func_name data_field consolidation duration () =
  logger := make_logger conf.C.debug ;
  if max_data_points < 1 then failwith "invalid max_data_points" ;
  let since = since |? until -. 600. in
  if since >= until then failwith "since must come strictly before until" ;
  let dt = (until -. since) /. float_of_int max_data_points in
  let open RamenExport in
  let buckets = make_buckets max_data_points in
  let bucket_of_time = bucket_of_time since dt in
  let consolidation =
    match String.lowercase consolidation with
    | "min" -> bucket_min | "max" -> bucket_max | _ -> bucket_avg in
  let bname, filter, typ, event_time =
    (* Read directly from the instrumentation ringbuf when func_name ends
     * with "#stats" *)
    if func_name = "stats" || String.ends_with func_name "#stats" then
      let typ = RamenBinocle.tuple_typ in
      let wi = RamenSerialization.find_field_index typ "worker" in
      let filter =
        if func_name = "stats" then always_true else
        let func_name, _ = String.rsplit func_name ~by:"#" in
        fun tuple -> tuple.(wi) = RamenScalar.VString func_name in
      let bname = C.report_ringbuf conf in
      bname, filter, typ, RamenBinocle.event_time
    else
      (* Create the non-wrapping RingBuf (under a standard name given
       * by RamenConf *)
      Lwt_main.run (
        let%lwt func, bname, typ =
          make_temp_export_by_name conf ~duration func_name in
        return (bname, always_true, typ, func.F.event_time))
  in
  Lwt_main.run (
    let open RamenSerialization in
    let%lwt vi =
      wrap (fun () -> find_field_index typ data_field) in
    fold_time_range bname typ event_time since until () (fun () tuple t1 t2 ->
      if t1 >= until then (), false else (
        if filter tuple && t2 >= since then (
          let v = float_of_scalar_value tuple.(vi) in
          let bi1 = bucket_of_time t1 and bi2 = bucket_of_time t2 in
          for bi = bi1 to bi2 do add_into_bucket buckets bi v done) ;
        (), true))) ;
  (* Display results: *)
  for i = 0 to Array.length buckets - 1 do
    let t = since +. dt *. (float_of_int i +. 0.5)
    and v = consolidation buckets.(i) in
    Float.print stdout t ;
    String.print stdout separator ;
    (match v with None -> String.print stdout null
                | Some v -> Float.print stdout v) ;
    String.print stdout "\n"
  done

(*
 * `ramen timerange`
 *
 * Obtain information about the time range available for timeseries.
 *)

let timerange conf func_name () =
  logger := make_logger conf.C.debug ;
  match C.program_func_of_user_string func_name with
  | exception Not_found ->
      !logger.error "Cannot find function %S" func_name ;
      exit 1
  | program_name, func_name ->
      let mi_ma =
        Lwt_main.run (
          C.with_rlock conf (fun programs ->
            (* We need the func to know its buffer location *)
            let func = C.find_func programs program_name func_name in
            let bname = C.archive_buf_name conf func in
            let typ = func.F.out_type.ser in
            RamenSerialization.time_range bname typ func.F.event_time))
      in
      match mi_ma with
        | None -> Printf.printf "No time info or no output yet.\n"
        | Some (mi, ma) -> Printf.printf "%f %f\n" mi ma
