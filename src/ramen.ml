(* This is where the command line arguments are processed.
 * This then mostly transfer control to various specialized functions
 * of the RamenCliCmd module.
 *)
open Cmdliner
open Batteries

(*
 * Common options
 *)

let copts =
  let docs = Manpage.s_common_options in
  let debug =
    let env = Term.env_info "RAMEN_DEBUG" in
    let i = Arg.info ~doc:"Increase verbosity."
                     ~docs ~env [ "d"; "debug" ] in
    Arg.(value (flag i))
  and persist_dir =
    let env = Term.env_info "RAMEN_PERSIST_DIR" in
    let i = Arg.info ~doc:"Directory where are stored data persisted on disc."
                     ~docs ~env [ "persist-dir" ] in
    Arg.(value (opt string RamenConsts.default_persist_dir i))
  and rand_seed =
    let env = Term.env_info "RAMEN_RANDOM_SEED" in
    let i = Arg.info ~doc:"Seed to initialize the random generator with. \
                           (will use a random one if unset)."
                     ~docs ~env [ "seed"; "rand-seed" ] in
    Arg.(value (opt (some int) None i))
  and keep_temp_files =
    let env = Term.env_info "RAMEN_KEEP_TEMP_FILES" in
    let i = Arg.info ~doc:"Keep temporary files."
                     ~docs ~env [ "keep-temp-files" ] in
    Arg.(value (flag i))
  in
  Term.(const RamenCliCmd.make_copts
    $ debug
    $ persist_dir
    $ rand_seed
    $ keep_temp_files)

(*
 * Start the event processor
 *)

let daemonize =
  let env = Term.env_info "RAMEN_DAEMONIZE" in
  let i = Arg.info ~doc:"Daemonize."
                   ~env [ "daemon"; "daemonize"] in
  Arg.(value (flag i))

let to_stderr =
  let env = Term.env_info "RAMEN_LOG_TO_STDERR" in
  let i = Arg.info ~doc:"Log onto stderr."
                   ~env [ "log-to-stderr"; "to-stderr"; "stderr" ] in
  Arg.(value (flag i))

let max_archives =
  let env = Term.env_info "RAMEN_MAX_HISTORY_ARCHIVES" in
  let i = Arg.info ~doc:"Max number of archive files to keep per operation; \
                         0 would disable archiving altogether."
                   ~env ["max-archives"] in
  Arg.(value (opt int 20 i))

let start =
  Term.(
    (const RamenCliCmd.start
      $ copts
      $ daemonize
      $ to_stderr
      $ max_archives),
    info ~doc:RamenConsts.start_info "start")

(*
 * Examine the ringbuffers
 *)

(* TODO: check that this is actually the name of a ringbuffer file *)
let rb_file =
  let i = Arg.info ~doc:"File with the ring buffer."
                   ~docv:"FILE" [] in
  Arg.(required (pos 0 (some string) None i))

let nb_tuples =
  let i = Arg.info ~doc:"How many entries to dequeue."
                   [ "n"; "nb-entries" ] in
  Arg.(value (opt int 1 i))

let dequeue =
  Term.(
    (const RingBufCmd.dequeue
      $ copts
      $ rb_file
      $ nb_tuples),
    info ~doc:"Dequeue a message from a ringbuffer." "dequeue")

let rb_files =
  let i = Arg.info ~doc:"The ring buffers to display information about.."
                   ~docv:"FILE" [] in
  Arg.(non_empty (pos_all string [] i))

let summary =
  Term.(
    (const RingBufCmd.summary
      $ copts
      $ rb_files),
    info ~doc:"Dump info about a ring-buffer." "ringbuf-summary")

(*
 * Compiling/Running/Stopping
 *)

let use_embedded_compiler =
  let env = Term.env_info "RAMEN_USE_EMBEDDED_COMPILER" in
  let i = Arg.info ~doc:"Use embedded compiler rather than calling system one."
                   ~env [ "use-embedded-compiler"; "use-internal-compiler";
                          "embedded-compiler"; "internal-compiler" ] in
  Arg.(value (flag i))

let bundle_dir =
  let env = Term.env_info "RAMEN_BUNDLE_DIR" in
  let i = Arg.info ~doc:"Directory where to find libraries for the embedded \
                         compiler."
                   ~env [ "bundle-dir" ] in
  Arg.(value (opt string RamenCompilConfig.default_bundle_dir i))

let max_simult_compilations =
  let env = Term.env_info "RAMEN_MAX_SIMULT_COMPILATIONS" in
  let i = Arg.info ~doc:"Max number of compilations to perform \
                         simultaneously."
                   ~env [ "max-simult-compilations" ;
                          "max-simultaneous-compilations" ] in
  Arg.(value (opt int 4 i))

let param =
  let parse s =
    match String.split s ~by:"=" with
    | exception Not_found ->
        Pervasives.Error (
          `Msg "You must specify the parameter name, followed by an equal \
                sign (=), followed by the parameter value.")
    | pname, pval ->
        let open RamenParsing in
        let p = allow_surrounding_blanks RamenScalar.Parser.p in
        let stream = stream_of_string pval in
        let m = [ "parameter value from command line" ] in
        (match p m  None Parsers.no_error_correction stream |>
              to_result with
        | Bad e ->
            let err =
              IO.to_string (print_bad_result RamenScalar.print) e in
            Pervasives.Error (`Msg err)
        | Ok (v, _) ->
            Pervasives.Ok (pname, v))
  and print fmt (pname, pval) =
    Format.fprintf fmt "%s=%s" pname (RamenScalar.to_string pval)
  in
  Arg.conv ~docv:"PARAM=VALUE" (parse, print)

(* Note: parameter with same name in different functions will all take
 * their value from this. Easy to add a prefix with function name when
 * it causes troubles. *)
let params =
  let i = Arg.info ~doc:"Override parameter's P default value with V."
                   ~docv:"P=V"
                   ["p"; "parameter"] in
  Arg.(value (opt_all param [] i))

let program_name =
  let i = Arg.info ~doc:"Program unique name."
                   ~docv:"PROGRAM" [] in
  Arg.(required (pos 0 (some string) None i))

let root_path =
  let env = Term.env_info "RAMEN_ROOT" in
  let i = Arg.info ~doc:"Path where to find other programs."
                   ~env [ "root" ] in
  Arg.(value (opt string "" i))

let source_files =
  let i = Arg.info ~doc:"Source files to compile."
                   ~docv:"FILE" [] in
  Arg.(non_empty (pos_all string [] i))

let bin_files =
  let i = Arg.info ~doc:"Binary files to run."
                   ~docv:"FILE" [] in
  Arg.(non_empty (pos_all string [] i))

let compile =
  Term.(
    (const RamenCliCmd.compile
      $ copts
      $ root_path
      $ use_embedded_compiler
      $ bundle_dir
      $ max_simult_compilations
      $ source_files),
    info ~doc:RamenConsts.compile_info "compile")

let run =
  Term.(
    (const RamenCliCmd.run
      $ copts
      $ params
      $ bin_files),
    info ~doc:RamenConsts.run_info "run")

let prog_name p =
  let i = Arg.info ~doc:"Program name."
                   ~docv:"PROGRAM" [] in
  Arg.(required (pos p (some string) None i))

let kill =
  Term.(
    (const RamenCliCmd.kill
      $ copts
      $ prog_name 0),
    info ~doc:RamenConsts.kill_info "kill")

(*
 * Display the timeseries
 *)

let with_header =
  let i = Arg.info ~doc:"Output the header line in CSV."
                   [ "with-header"; "header" ] in
  Arg.(value (flag i))

let csv_separator =
  let env = Term.env_info "RAMEN_CSV_SEPARATOR" in
  let i = Arg.info ~doc:"Field separator."
                   ~env [ "separator" ] in
  Arg.(value (opt string "," i))

let csv_null =
  let env = Term.env_info "RAMEN_CSV_NULL" in
  let i = Arg.info ~doc:"Representation of NULL values."
                   ~env [ "null" ] in
  Arg.(value (opt string "<NULL>" i))

let last =
  let i = Arg.info ~doc:"Output only the last N tuples (or only the \
                         next -N, if N is negative)."
                   [ "last" ] in
  Arg.(value (opt (some int) None i))

let min_seq =
  let i = Arg.info ~doc:"Output only tuples with greater sequence number."
                   ["min-seqnum"] in
  Arg.(value (opt (some int) None i))

let max_seq =
  let i = Arg.info ~doc:"Output only tuples with smaller sequence number."
                   ["max-seqnum"] in
  Arg.(value (opt (some int) None i))

let print_seqnums =
  let i = Arg.info ~doc:"Prepend tuples with their sequence number."
                   ["with-seqnums"] in
  Arg.(value (flag i))

(* TODO: returns directly the program and function names to spare some
 * calls to C.program_func_of_user_string *)
let func_name p =
  let i = Arg.info ~doc:"Operation unique name."
                   ~docv:"OPERATION" [] in
  Arg.(required (pos p (some string) None i))

let duration =
  let i = Arg.info ~doc:"Operation will stop archiving its output after \
                         that duration if nobody ask for it."
                   ["timeout"] in
  Arg.(value (opt float 3600. i))

(* TODO: from/until timestamps *)
let tail =
  Term.(
    (const RamenCliCmd.tail
      $ copts
      $ func_name 0
      $ with_header
      $ csv_separator
      $ csv_null
      $ last
      $ min_seq
      $ max_seq
      $ print_seqnums
      $ duration),
    info ~doc:RamenConsts.tail_info "tail")

(*
 * Timeseries
 *)

let since =
  let i = Arg.info ~doc:"Timestamp of the first point."
                   ~docv:"SINCE" ["since"] in
  Arg.(value (opt (some float) None i))

let until =
  let i = Arg.info ~doc:"Timestamp of the last point."
                   ~docv:"UNTIL" ["until"] in
  Arg.(value (opt float (Unix.gettimeofday ()) i))

let max_data_points =
  let i = Arg.info ~doc:"Max number of points returned."
                   ~docv:"POINTS" ["nb-points"] in
  Arg.(value (opt int 100 i))

let data_field p =
  let i = Arg.info ~doc:"Field to retrieve values from."
                   ~docv:"FIELD" [] in
  Arg.(required (pos p (some string) None i))

let consolidation =
  let i = Arg.info ~doc:"Consolidation function."
                   ~docv:"min|max|avg" ["consolidation"] in
  let cons_func =
    let p x = x, x in
    [ p "min" ; p "max" ; p "avg" ] in
  Arg.(value (opt (enum cons_func) "avg" i))

let timeseries =
  Term.(
    (const RamenCliCmd.timeseries
      $ copts
      $ since
      $ until
      $ max_data_points
      $ csv_separator
      $ csv_null
      $ func_name 0
      $ data_field 1
      $ consolidation
      $ duration),
    info ~doc:RamenConsts.timeseries_info "timeseries")

(*
 * Time Ranges
 *)

let timerange =
  Term.(
    (const RamenCliCmd.timerange
      $ copts
      $ func_name 0),
    info ~doc:RamenConsts.timerange_info "timerange")

(*
 * Info
 *)

let short =
  let i = Arg.info ~doc:"Display only a short summary."
                   [ "short" ; "p" ] in
  Arg.(value (flag i))

let sort_col =
  let i = Arg.info ~doc:"Sort the operation list according to this column \
                         (first column -name- is 1, then #in is 2...)."
                   ~docv:"COL" [ "sort" ; "s" ] in
  Arg.(value (opt int 1 i))

let top =
  let i = Arg.info ~doc:"Truncate the list of operations after the first N \
                         entries."
                   ~docv:"N" [ "top" ; "t" ] in
  Arg.(value (opt (some int) None i))

let ps =
  Term.(
    (const RamenCliCmd.ps
      $ copts
      $ short
      $ sort_col
      $ top),
    info ~doc:RamenConsts.ps_info "ps")

(*
 * Autocompletion
 *)

let command =
  let i = Arg.info ~doc:"Ramen command line to be completed."
                   ~docv:"STRING" [] in
  Arg.(value (pos 0 string "" i))

let autocomplete =
  Term.(
    (const RamenCompletion.complete
      $ command),
    info ~doc:"Autocomplete the given command." "_completion")

(*
 * Tests
 *)

let test_files =
  let i = Arg.info ~doc:"Definition of a test to run."
                   ~docv:"file.test" [] in
  Arg.(non_empty (pos_all string [] i))

let test =
  Term.(
    (const RamenTests.run
      $ copts
      $ root_path
      $ test_files),
    info ~doc:RamenConsts.test_info "test")

(*
 * Command line evaluation
 *)

let default =
  let sdocs = Manpage.s_common_options in
  let doc = "Ramen Stream Processor" in
  let version = RamenVersions.release_tag in
  Term.((ret (const (`Help (`Pager, None)))),
        info "Ramen" ~version ~doc ~sdocs)

let () =
  match Term.eval_choice default [
    start ; compile ; run ; kill ; tail ; timeseries ; timerange ;
    ps ; dequeue ; summary ; autocomplete ; test
  ] with `Error _ -> exit 1
       | `Version | `Help -> exit 0
       | `Ok f -> (
          try f ()
          with Exit -> exit 0
             | RamenHelpers.Timeout ->
                 Printf.eprintf "Timed out\n" ;
                 exit 1
             | Failure msg ->
                 Printf.eprintf "%s\n" msg ;
                 exit 1)
