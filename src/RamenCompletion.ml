open Batteries
open RamenLog
open RamenHelpers
module C = RamenConf
module F = C.Func
module P = C.Program

let propose (l, h) =
  String.print stdout l ;
  if h <> "" then (
    Char.print stdout '\t' ;
    String.print stdout h) ;
  print_endline ""

let complete lst s =
  List.filter (fun (l, _) -> String.starts_with l s) lst |>
  List.iter propose

let complete_commands s =
  let commands =
    [ "supervisor", RamenConsts.CliInfo.supervisor ;
      "notifier", RamenConsts.CliInfo.notifier ;
      "notify", RamenConsts.CliInfo.notify ;
      "compile", RamenConsts.CliInfo.compile ;
      "run", RamenConsts.CliInfo.run ;
      "kill", RamenConsts.CliInfo.kill ;
      "tail", RamenConsts.CliInfo.tail ;
      "timeseries", RamenConsts.CliInfo.timeseries ;
      "timerange", RamenConsts.CliInfo.timerange ;
      "ps", RamenConsts.CliInfo.ps ;
      "links", RamenConsts.CliInfo.links ;
      "test", RamenConsts.CliInfo.test ;
      "httpd", RamenConsts.CliInfo.httpd ;
      "variants", RamenConsts.CliInfo.variants ;
      "gc", RamenConsts.CliInfo.gc ;
      "stats", RamenConsts.CliInfo.stats ] in
  complete commands s

let complete_global_options s =
  let options =
    [ "--help", RamenConsts.CliInfo.help ;
      "--version", RamenConsts.CliInfo.version ] in
  complete options s

let find_opt o =
  let opt_value s =
    String.split s ~by:"=" |> snd in
  let o_eq = o ^ "=" in
  let find_opt_1 s =
    if String.starts_with s o_eq then opt_value s else
    raise Not_found in
  let rec loop = function
  | [] -> raise Not_found
  | [s] -> find_opt_1 s
  | s::(n::_ as rest) ->
      if s = o then n else
      try find_opt_1 s
      with Not_found -> loop rest in
  fun lst -> loop lst

let persist_dir toks =
  try find_opt "--persist-dir" toks
  with Not_found ->
    try Sys.getenv "RAMEN_PERSIST_DIR"
    with Not_found -> RamenConsts.Default.persist_dir

let complete_file select str =
  let count = ref 0 in
  let res = ref [] in
  let root, pref =
    if str = "" then ".", ""
    else if str.[String.length str - 1] = '/' then str, str
    else let d = Filename.dirname str in d, if d = "." && str.[0] <> '.' then "" else d^"/" in
  let start =
    (* [basename ""] would be ".", and [basename "toto/"] would be toto ?! *)
    if str = "" || str.[String.length str - 1] = '/' then ""
    else Filename.basename str in
  let on_file fname rel_fname =
    if select fname && String.starts_with rel_fname start then (
      incr count ;
      if !count > 500 then raise Exit ;
      res := (pref ^ rel_fname, "") :: !res) in
  if str <> "" && str.[0] = '-' then [] else (
    (try dir_subtree_iter ~on_file root
    with Exit -> ()) ;
    !res)

let extension_is ext fname =
  String.ends_with fname ext

let complete_program_files str =
  complete_file (extension_is ".ramen") str

let complete_binary_files str =
  complete_file (extension_is ".x") str

let complete_test_file str =
  complete_file (extension_is ".test") str

let empty_help s = s, ""

let complete_running_function persist_dir =
  let conf = C.make_conf persist_dir in
  (
    Hashtbl.values (C.with_rlock conf identity) //@
    (fun (_mre, get_rc) ->
      try Some (get_rc ())
      with _ -> None) /@
    (fun prog ->
      List.enum prog.P.funcs |>
      Enum.map (fun func ->
        RamenName.string_of_program func.F.program_name ^"/"^
        RamenName.string_of_func func.F.name)) |>
    Enum.flatten
  ) /@
  empty_help |> List.of_enum

let complete_running_program persist_dir =
  let conf = C.make_conf persist_dir in
  Hashtbl.enum (C.with_rlock conf identity) //@
  (fun (p, (mre, _)) ->
    if mre.C.status = C.MustRun then
      Some (RamenName.string_of_program p)
    else None) /@
  empty_help |> List.of_enum

let complete str () =
  (* Tokenize str, find where we are: *)
  let last_tok_is_complete =
    let len = String.length str in
    len > 0 && Char.is_whitespace str.[len - 1] in
  let toks =
    String.split_on_char ' ' str |>
    List.filter (fun s -> String.length s > 0) in
  let toks =
    match toks with
    | s :: rest when String.ends_with s "ramen" -> rest
    | r -> r (* ?? *) in
  let num_toks = List.length toks in
  let command_idx, command =
    try List.findi (fun _ s -> s.[0] <> '-') toks
    with Not_found -> -1, "" in
  let last_tok =
    if num_toks > 0 then List.nth toks (num_toks-1)
    else "" in
  (*!logger.info "num_toks=%d, command_idx=%d, command=%s, last_tok_complete=%b"
    num_toks command_idx command last_tok_is_complete ;*)

  (match num_toks, command_idx, last_tok_is_complete with
  | 0, _, true -> (* "ramen<TAB>" *)
    complete_commands ""
  | 0, _, false -> (* "ramen <TAB>" *)
    complete_commands ""
  | _, -1, false -> (* "ramen [[other options]] --...<TAB>" *)
    complete_global_options last_tok
  | _, c_idx, false when c_idx = num_toks-1 -> (* "ramen ... comm<TAB>" *)
    complete_commands last_tok
  | _ -> (* "ramen ... command ...? <TAB>" *)
    let toks = List.drop (command_idx+1) toks in
    let copts =
      [ "--help", RamenConsts.CliInfo.help ;
        "--debug", RamenConsts.CliInfo.debug ;
        "--quiet", RamenConsts.CliInfo.quiet ;
        "--rand-seed", RamenConsts.CliInfo.rand_seed ;
        "--persist-dir=", RamenConsts.CliInfo.persist_dir ;
        "--variant", RamenConsts.CliInfo.variant ] in
    let completions =
      (match command with
      | "supervisor" ->
          [ "--daemonize", RamenConsts.CliInfo.daemonize ;
            "--to-stdout", RamenConsts.CliInfo.to_stdout ;
            "--syslog", RamenConsts.CliInfo.to_syslog ;
            "--autoreload=", RamenConsts.CliInfo.autoreload ;
            "--report-period=", RamenConsts.CliInfo.report_period ;
            "--bundle-dir=", RamenConsts.CliInfo.bundle_dir ;
            "--external-compiler=", RamenConsts.CliInfo.external_compiler ;
            "--max-simult-compilations",
              RamenConsts.CliInfo.max_simult_compilations ;
            "--solver=", RamenConsts.CliInfo.smt_solver ] @
          copts
       | "notifier" ->
          [ "--daemonize", RamenConsts.CliInfo.daemonize ;
            "--to-stdout", RamenConsts.CliInfo.to_stdout ;
            "--syslog", RamenConsts.CliInfo.to_syslog ;
            "--config", RamenConsts.CliInfo.conffile ] @
          copts
       | "notify" ->
          ("--parameter=", RamenConsts.CliInfo.param) ::
          copts
      | "compile" ->
          [ "--bundle-dir=", RamenConsts.CliInfo.bundle_dir ;
            "--external-compiler=", RamenConsts.CliInfo.external_compiler ;
            "--max-simult-compilations",
              RamenConsts.CliInfo.max_simult_compilations ;
            "--solver=", RamenConsts.CliInfo.smt_solver ;
            "--keep-temp-files", RamenConsts.CliInfo.keep_temp_files ;
            "--root-path=", RamenConsts.CliInfo.root_path ;
            "--external-compiler", RamenConsts.CliInfo.external_compiler ;
            "--as-program=", RamenConsts.CliInfo.program_name ] @
          copts @
          (complete_program_files last_tok)
      | "run" ->
          ("--parameter=", RamenConsts.CliInfo.param) ::
          ("--as=", RamenConsts.CliInfo.as_) ::
          ("--replace", RamenConsts.CliInfo.replace) ::
          ("--kill-if-disabled", RamenConsts.CliInfo.kill_if_disabled) ::
          ("--source-file=", RamenConsts.CliInfo.src_file) ::
          copts @
          (complete_binary_files last_tok)
      | "kill" ->
          let persist_dir = persist_dir toks in
          ("--purge", RamenConsts.CliInfo.purge) ::
          copts @
          (complete_running_program persist_dir)
      | "tail" ->
          let persist_dir = persist_dir toks in
          ("--last=", RamenConsts.CliInfo.last) ::
          ("--max-seqnum=", RamenConsts.CliInfo.max_seq) ::
          ("--min-seqnum=", RamenConsts.CliInfo.min_seq) ::
          ("--continuous", RamenConsts.CliInfo.continuous) ::
          ("--where=", RamenConsts.CliInfo.where) ::
          ("--null=", RamenConsts.CliInfo.csv_null) ::
          ("--separator=", RamenConsts.CliInfo.csv_separator) ::
          ("--with-header", RamenConsts.CliInfo.with_header) ::
          ("--with-seqnums", RamenConsts.CliInfo.with_seqnums) ::
          ("--with-times", RamenConsts.CliInfo.with_seqnums) ::
          ("--with-units", RamenConsts.CliInfo.with_seqnums) ::
          ("--pretty", RamenConsts.CliInfo.pretty) ::
          ("--flush", RamenConsts.CliInfo.flush) ::
          copts @
          ((RamenConsts.SpecialFunctions.stats, "Activity statistics") ::
           (RamenConsts.SpecialFunctions.notifs, "Internal instrumentation") ::
           (complete_running_function persist_dir))
      | "timeseries" ->
          let persist_dir = persist_dir toks in
          (* TODO: get the function name from toks and autocomplete
           * field names! *)
          ("--since=", RamenConsts.CliInfo.since) ::
          ("--until=", RamenConsts.CliInfo.until) ::
          ("--where=", RamenConsts.CliInfo.where) ::
          ("--factor=", RamenConsts.CliInfo.factors) ::
          ("--num-points=", RamenConsts.CliInfo.num_points) ::
          ("--time-step=", RamenConsts.CliInfo.time_step) ::
          ("--consolidation=", RamenConsts.CliInfo.consolidation) ::
          ("--separator=", RamenConsts.CliInfo.csv_separator) ::
          ("--null=", RamenConsts.CliInfo.csv_null) ::
          ("--pretty", RamenConsts.CliInfo.pretty) ::
          copts @
          (("stats", "Internal instrumentation") ::
           (complete_running_function persist_dir))
      | "timerange" ->
          let persist_dir = persist_dir toks in
          copts @
          (complete_running_function persist_dir)
      | "ps" ->
          let persist_dir = persist_dir toks in
          ("--short", RamenConsts.CliInfo.short) ::
          ("--pretty", RamenConsts.CliInfo.pretty) ::
          ("--with-header", RamenConsts.CliInfo.with_header) ::
          ("--sort", RamenConsts.CliInfo.sort_col) ::
          ("--top", RamenConsts.CliInfo.top) ::
          ("--all", RamenConsts.CliInfo.all) ::
          copts @
          (complete_running_function persist_dir)
      | "links" ->
          let persist_dir = persist_dir toks in
          ("--no-abbrev", RamenConsts.CliInfo.no_abbrev) ::
          ("--show-all", RamenConsts.CliInfo.show_all) ::
          ("--as-tree", RamenConsts.CliInfo.as_tree) ::
          ("--pretty", RamenConsts.CliInfo.pretty) ::
          ("--with-header", RamenConsts.CliInfo.with_header) ::
          ("--sort", RamenConsts.CliInfo.sort_col) ::
          ("--top", RamenConsts.CliInfo.top) ::
          copts @
          (complete_running_function persist_dir)
      | "test" ->
          [ "--help", RamenConsts.CliInfo.help ;
            "--url=", RamenConsts.CliInfo.server_url ;
            "--api", RamenConsts.CliInfo.api ;
            "--graphite", RamenConsts.CliInfo.graphite ;
            "--bundle-dir=", RamenConsts.CliInfo.bundle_dir ;
            "--external-compiler=", RamenConsts.CliInfo.external_compiler ;
            "--max-simult-compilations",
              RamenConsts.CliInfo.max_simult_compilations ;
            "--solver=", RamenConsts.CliInfo.smt_solver ] @
          copts @
          (complete_test_file last_tok)
      | "httpd" ->
          [ "--daemonize", RamenConsts.CliInfo.daemonize ;
            "--to-stdout", RamenConsts.CliInfo.to_stdout ;
            "--syslog", RamenConsts.CliInfo.to_syslog ;
            "--url=", RamenConsts.CliInfo.server_url ;
            "--api", RamenConsts.CliInfo.api ;
            "--graphite", RamenConsts.CliInfo.graphite ;
            "--bundle-dir=", RamenConsts.CliInfo.bundle_dir ;
            "--external-compiler=", RamenConsts.CliInfo.external_compiler ;
            "--max-simult-compilations",
              RamenConsts.CliInfo.max_simult_compilations ;
            "--solver=", RamenConsts.CliInfo.smt_solver ] @
          copts
      | "variants" ->
          copts
      | "gc" ->
          [ "--max-archives", RamenConsts.CliInfo.max_archives ;
            "--loop", RamenConsts.CliInfo.loop ;
            "--dry-run", RamenConsts.CliInfo.dry_run ] @
          copts
      | "stats" ->
          copts
      | _ -> []) in
    complete completions (if last_tok_is_complete then "" else last_tok))
