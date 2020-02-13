open Batteries
open RamenLog
open RamenHelpers
open RamenConsts
module C = RamenConf
module N = RamenName
module Files = RamenFiles

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
    [ "supervisor", CliInfo.supervisor ;
      "alerter", CliInfo.alerter ;
      "notify", CliInfo.notify ;
      "compile", CliInfo.compile ;
      "run", CliInfo.run ;
      "kill", CliInfo.kill ;
      "info", CliInfo.info ;
      "tail", CliInfo.tail ;
      "replay", CliInfo.replay ;
      "timeseries", CliInfo.timeseries ;
      "ps", CliInfo.ps ;
      "links", CliInfo.links ;
      "test", CliInfo.test ;
      "httpd", CliInfo.httpd ;
      "tunneld", CliInfo.tunneld ;
      "variants", CliInfo.variants ;
      "gc", CliInfo.gc ;
      "stats", CliInfo.stats ;
      "archivist", CliInfo.archivist ;
      "ringbuf-summary", CliInfo.summary ;
      "dequeue", CliInfo.dequeue ;
      "repair-ringbuf", CliInfo.repair ;
      "dump-ringbuf", CliInfo.dump ;
      "confserver", CliInfo.confserver ;
      "confclient", CliInfo.confclient ;
      "precompserver", CliInfo.precompserver ;
      "execompserver", CliInfo.execompserver ;
      "choreographer", CliInfo.choreographer ;
      "useradd", CliInfo.useradd ;
      "userdel", CliInfo.userdel ;
      "usermod", CliInfo.usermod ] in
  complete commands s

let complete_global_options s =
  let options =
    [ "--help", CliInfo.help ;
      "--version", CliInfo.version ] in
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
  try find_opt "--persist-dir" toks |> N.path
  with Not_found ->
    try Sys.getenv "RAMEN_DIR" |> N.path
    with Not_found -> Default.persist_dir

let complete_file select str =
  let count = ref 0 in
  let res = ref [] in
  let root, pref =
    if str = "" then
      N.path ".", N.path ""
    else if str.[String.length str - 1] = '/' then
      N.path str, N.path str
    else
      let d = Filename.dirname str in
      N.path d, N.path (if d = "." && str.[0] <> '.' then "" else d^"/") in
  let start =
    (* [basename ""] would be ".", and [basename "toto/"] would be toto ?! *)
    if str = "" || str.[String.length str - 1] = '/' then N.path ""
    else Files.basename (N.path str) in
  let on_file fname rel_fname =
    if select fname && N.starts_with rel_fname start then (
      incr count ;
      if !count > 500 then raise Exit ;
      res := (N.cat pref rel_fname, "") :: !res) in
  if str <> "" && str.[0] = '-' then [] else (
    (try Files.dir_subtree_iter ~on_file root
    with Exit -> ()) ;
    (!res :> (string * string) list)
  )

let complete_program_files str =
  complete_file (Files.has_ext "ramen") str

let complete_binary_files str =
  complete_file (Files.has_ext "x") str

let complete_test_file str =
  complete_file (Files.has_ext "test") str

let complete_rb_file str =
  complete_file (fun f -> Files.(has_ext "r" f || has_ext "b" f)) str

(*let empty_help s = s, ""*)

let complete_running_function _persist_dir =
  (* TODO: run `ramen ps` in the background *)
  []
  (*let conf = C.make_conf persist_dir ~site:(N.site "completion") in
  (
    Hashtbl.values (RC.with_rlock conf identity) //@
    (fun (_mre, get_rc) ->
      try Some (get_rc ())
      with _ -> None) /@
    (fun prog ->
      List.enum prog.P.funcs |>
      Enum.map (fun func ->
        (func.F.program_name :> string) ^"/"^
        (func.F.name :> string))) |>
    Enum.flatten
  ) /@
  empty_help |> List.of_enum *)

let complete_running_program _persist_dir =
  (* TODO: run `ramen ps` in the background *)
  []
  (*let conf = C.make_conf persist_dir ~site:(N.site "completion") in
  Hashtbl.enum (RC.with_rlock conf identity) //@
  (fun (p, (rce, _)) ->
    if rce.RC.status = RC.MustRun then
      Some (p :> string)
    else None) /@
  empty_help |> List.of_enum*)

let complete str () =
  (* Tokenize str, find where we are: *)
  let last_tok_is_complete =
    let len = String.length str in
    len > 0 && Char.is_whitespace str.[len - 1] in
  let toks =
    string_split_on_char ' ' str |>
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
    let copts with_sync =
      [ "--help", CliInfo.help ;
        "--debug", CliInfo.debug ;
        "--quiet", CliInfo.quiet ;
        "--rand-seed", CliInfo.rand_seed ;
        "--persist-dir=", CliInfo.persist_dir ;
        "--variant", CliInfo.variant ;
        "--site", CliInfo.site ;
        "--master=", CliInfo.site ;
        "--bundle-dir=", CliInfo.bundle_dir ] @
      (if with_sync then 
        [ "--confserver", CliInfo.confserver_url ;
          "--confserver-key", CliInfo.confserver_key ;
          "--username", CliInfo.username ;
          "--pub-key", CliInfo.client_pub_key ;
          "--priv-key", CliInfo.client_priv_key ;
          "--identity", CliInfo.identity_file ]
      else [])
      in
    let completions =
      (match command with
      | "supervisor" ->
          [ "--daemonize", CliInfo.daemonize ;
            "--to-stdout", CliInfo.to_stdout ;
            "--syslog", CliInfo.to_syslog ;
            "--external-compiler=", CliInfo.external_compiler ;
            "--max-simult-compilations",
              CliInfo.max_simult_compilations ;
            "--solver=", CliInfo.smt_solver ;
            "--kill-at-exit", CliInfo.kill_at_exit ;
            "--fail-for-good", CliInfo.fail_for_good ;
            "--test-notifs", CliInfo.test_notifs_every ] @
          copts true
       | "alerter" ->
          [ "--daemonize", CliInfo.daemonize ;
            "--to-stdout", CliInfo.to_stdout ;
            "--syslog", CliInfo.to_syslog ;
            "--config", CliInfo.conffile ] @
          copts true
       | "notify" ->
          ("--parameter=", CliInfo.param) ::
          ("--test", CliInfo.is_test_alert) ::
          copts false
      | "compile" ->
          [ "--external-compiler=", CliInfo.external_compiler ;
            "--max-simult-compilations",
              CliInfo.max_simult_compilations ;
            "--solver=", CliInfo.smt_solver ;
            "--keep-temp-files", CliInfo.keep_temp_files ;
            "--reuse-prev-files", CliInfo.reuse_prev_files ;
            "--lib-path=", CliInfo.lib_path ;
            "--external-compiler", CliInfo.external_compiler ;
            "--as-program=", CliInfo.program_name ] @
          copts true @
          (complete_program_files last_tok)
      | "run" ->
          ("--parameter=", CliInfo.param) ::
          ("--as=", CliInfo.as_) ::
          ("--replace", CliInfo.replace) ::
          ("--report-period=", CliInfo.report_period) ::
          ("--on-site=", CliInfo.on_site) ::
          copts true @
          (complete_program_files last_tok)
      | "kill" ->
          let persist_dir = persist_dir toks in
          ("--purge", CliInfo.purge) ::
          copts true @
          (complete_running_program persist_dir)
      | "info" ->
          ("--parameter=", CliInfo.param) ::
          copts true @
          (complete_binary_files last_tok)
      | "tail" ->
          let persist_dir = persist_dir toks in
          ("--last=", CliInfo.last) ::
          ("--next=", CliInfo.next) ::
          ("--follow", CliInfo.follow) ::
          ("--where=", CliInfo.where) ::
          ("--since=", CliInfo.since) ::
          ("--until=", CliInfo.until) ::
          ("--null=", CliInfo.csv_null) ::
          ("--separator=", CliInfo.csv_separator) ::
          ("--with-header", CliInfo.with_header) ::
          ("--with-times", CliInfo.with_event_time) ::
          ("--with-units", CliInfo.with_units) ::
          ("--pretty", CliInfo.pretty) ::
          ("--raw", CliInfo.csv_raw) ::
          ("--flush", CliInfo.flush) ::
          ("--external-compiler=", CliInfo.external_compiler) ::
          ("--max-simult-compilations", CliInfo.max_simult_compilations) ::
          ("--solver=", CliInfo.smt_solver) ::
          copts true @
          ((SpecialFunctions.stats, "Activity statistics") ::
           (SpecialFunctions.notifs, "Internal instrumentation") ::
           (complete_running_function persist_dir))
      | "replay" ->
          let persist_dir = persist_dir toks in
          ("--where=", CliInfo.where) ::
          ("--since=", CliInfo.since) ::
          ("--until=", CliInfo.until) ::
          ("--null=", CliInfo.csv_null) ::
          ("--separator=", CliInfo.csv_separator) ::
          ("--with-header", CliInfo.with_header) ::
          ("--with-times", CliInfo.with_event_time) ::
          ("--with-units", CliInfo.with_units) ::
          ("--pretty", CliInfo.pretty) ::
          ("--raw", CliInfo.csv_raw) ::
          ("--flush", CliInfo.flush) ::
          ("--external-compiler=", CliInfo.external_compiler) ::
          ("--max-simult-compilations", CliInfo.max_simult_compilations) ::
          ("--solver=", CliInfo.smt_solver) ::
          ("--via=", CliInfo.via) ::
          copts true @
          ((SpecialFunctions.stats, "Activity statistics") ::
           (SpecialFunctions.notifs, "Internal instrumentation") ::
           (complete_running_function persist_dir))
      | "timeseries" ->
          let persist_dir = persist_dir toks in
          (* TODO: get the function name from toks and autocomplete
           * field names! *)
          ("--since=", CliInfo.since) ::
          ("--until=", CliInfo.until) ::
          ("--where=", CliInfo.where) ::
          ("--factor=", CliInfo.factors) ::
          ("--num-points=", CliInfo.num_points) ::
          ("--time-step=", CliInfo.time_step) ::
          ("--consolidation=", CliInfo.consolidation) ::
          ("--separator=", CliInfo.csv_separator) ::
          ("--null=", CliInfo.csv_null) ::
          ("--pretty", CliInfo.pretty) ::
          copts true @
          (("stats", "Internal instrumentation") ::
           (complete_running_function persist_dir))
      | "ps" ->
          let persist_dir = persist_dir toks in
          ("--pretty", CliInfo.pretty) ::
          ("--with-header", CliInfo.with_header) ::
          ("--sort", CliInfo.sort_col) ::
          ("--top", CliInfo.top) ::
          ("--show-all", CliInfo.show_all_workers) ::
          copts true @
          (complete_running_function persist_dir)
      | "links" ->
          let persist_dir = persist_dir toks in
          ("--no-abbrev", CliInfo.no_abbrev) ::
          ("--show-all", CliInfo.show_all_links) ::
          ("--as-tree", CliInfo.as_tree) ::
          ("--pretty", CliInfo.pretty) ::
          ("--with-header", CliInfo.with_header) ::
          ("--sort", CliInfo.sort_col) ::
          ("--top", CliInfo.top) ::
          copts false @
          (complete_running_function persist_dir)
      | "test" ->
          [ "--help", CliInfo.help ;
            "--url=", CliInfo.server_url ;
            "--api", CliInfo.api ;
            "--graphite", CliInfo.graphite ;
            "--external-compiler=", CliInfo.external_compiler ;
            "--max-simult-compilations",
              CliInfo.max_simult_compilations ;
            "--solver=", CliInfo.smt_solver ] @
          copts false @
          (complete_test_file last_tok)
      | "httpd" ->
          [ "--daemonize", CliInfo.daemonize ;
            "--to-stdout", CliInfo.to_stdout ;
            "--syslog", CliInfo.to_syslog ;
            "--url=", CliInfo.server_url ;
            "--api", CliInfo.api ;
            "--graphite", CliInfo.graphite ;
            "--external-compiler=", CliInfo.external_compiler ;
            "--max-simult-compilations",
              CliInfo.max_simult_compilations ;
            "--solver=", CliInfo.smt_solver ] @
          copts true
       | "tunneld" ->
          [ "--daemonize", CliInfo.daemonize ;
            "--to-stdout", CliInfo.to_stdout ;
            "--syslog", CliInfo.to_syslog ;
            "--port=", CliInfo.tunneld_port ] @
          copts true
      | "variants" ->
          copts false
      | "gc" ->
          [ "--del-ratio", CliInfo.del_ratio ;
            "--loop", CliInfo.loop ;
            "--dry-run", CliInfo.dry_run ] @
          copts true
      | "stats" ->
          copts false
      | "archivist" ->
          [ "--loop", CliInfo.loop ;
            "--stats", CliInfo.update_stats ;
            "--allocs", CliInfo.update_allocs ;
            "--reconf-workers", CliInfo.reconf_workers ] @
          copts true
      | "ringbuf-summary" ->
          ("--max-bytes", CliInfo.max_bytes) ::
          copts false @
          (complete_rb_file last_tok)
      | "dequeue" ->
          ("--num-entries", CliInfo.num_tuples) ::
          copts false @
          (complete_rb_file last_tok)
      | "repair-ringbuf" ->
          copts false @
          (complete_rb_file last_tok)
      | "dump-ringbuf" ->
          [ "--start", CliInfo.start_word ;
            "--stop", CliInfo.stop_word ] @
          copts false @
          (complete_rb_file last_tok)
      | "confserver" ->
          [ "--daemonize", CliInfo.daemonize ;
            "--to-stdout", CliInfo.to_stdout ;
            "--syslog", CliInfo.to_syslog ;
            "--secure", CliInfo.confserver_port_sec ;
            "--insecure", CliInfo.confserver_port ] @
          copts false
      | "confclient" ->
          copts true
      | "precompserver" ->
          [ "--daemonize", CliInfo.daemonize ;
            "--to-stdout", CliInfo.to_stdout ;
            "--syslog", CliInfo.to_syslog ;
            "--solver=", CliInfo.smt_solver ] @
          copts true
      | "execompserver" ->
          [ "--daemonize", CliInfo.daemonize ;
            "--to-stdout", CliInfo.to_stdout ;
            "--syslog", CliInfo.to_syslog ;
            "--external-compiler=", CliInfo.external_compiler ;
            "--max-simult-compilations",
              CliInfo.max_simult_compilations ] @
          copts true
      | "choreographer" ->
          [ "--daemonize", CliInfo.daemonize ;
            "--to-stdout", CliInfo.to_stdout ;
            "--syslog", CliInfo.to_syslog ] @
          copts true
      | "useradd" ->
          [ "--username", CliInfo.username ;
            "--role", CliInfo.role ;
            "-o", CliInfo.output_file] @
          copts false
      | "userdel" ->
          [ "--username", CliInfo.username ] @
          copts false
      | "usermod" ->
          [ "--username", CliInfo.username ;
            "--role", CliInfo.role ] @
          copts false
      | _ -> []) in
    complete completions (if last_tok_is_complete then "" else last_tok))
