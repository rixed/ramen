open Batteries
open RamenLog
open RamenHelpersNoLog
module C = RamenConf
module CliInfo = RamenConstsCliInfo
module Default = RamenConstsDefault
module Files = RamenFiles
module N = RamenName

let propose (l, h) =
  String.print stdout l ;
  if h <> "" then (
    Char.print stdout '\t' ;
    String.print stdout h) ;
  print_endline ""

let complete lst s =
  List.filter (fun (l, _) -> String.starts_with l s) lst |>
  List.iter propose

let commands =
  CliInfo.[
    supervisor ;
    alerter ;
    notify ;
    compile ;
    run ;
    kill ;
    info ;
    tail ;
    replay ;
    timeseries ;
    ps ;
    links ;
    test ;
    httpd ;
    tunneld ;
    variants ;
    gc ;
    stats ;
    archivist ;
    summary ;
    dequeue ;
    repair ;
    dump_ringbuf ;
    confserver ;
    confclient ;
    precompserver ;
    execompserver ;
    choreographer ;
    useradd ;
    userdel ;
    usermod ]

let complete_commands =
  let commands =
    ("--help", CliInfo.help) ::
    ("--version", CliInfo.version) ::
    (List.map (fun cmd -> cmd.CliInfo.name, cmd.doc) commands)
  in
  fun s ->
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

let remove_ext = List.map (fun (f, empty) ->
  (Filename.remove_extension f, empty))

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

let complete str =
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
    let completions =
      match List.find (fun cmd -> cmd.CliInfo.name = command) commands with
      | exception Not_found ->
          []
      | cmd ->
          List.filter_map (fun opt ->
            match opt.CliInfo.names with
            | [] ->
                None
            | name :: _ ->
                (* This works because the first option is always a long
                 * option: *)
                Some ("--"^ name, opt.doc)
          ) cmd.CliInfo.opts @
          (
            if cmd.CliInfo.name = "compile" then
              complete_program_files last_tok
            else if cmd.name = "run" then
              complete_program_files last_tok |> remove_ext
            else if cmd.name = "kill" then
              let persist_dir = persist_dir toks in
              complete_running_program persist_dir
            else if cmd.name = "info" then
              complete_binary_files last_tok
            else if List.mem cmd.name [
                      "tail" ; "replay" ; "timeseries" ; "ps" ; "links" ] then
              let persist_dir = persist_dir toks in
              complete_running_function persist_dir
            else if cmd.name = "test" then
              complete_test_file last_tok
            else if List.mem cmd.name [
                      "ringbuf-summary" ; "dequeue" ; "repair-ringbuf" ;
                      "dump-ringbuf" ] then
              complete_rb_file last_tok
            else
              []
          )
    in
    complete completions (if last_tok_is_complete then "" else last_tok))
