open Batteries

type 'a printer =
  ('a, unit BatIO.output, unit) format -> 'a

type log_output = Directory of string | Stdout | Syslog

type logger =
  { error : 'a. 'a printer ;
    warning : 'a. 'a printer ;
    info : 'a. 'a printer ;
    debug : 'a. 'a printer ;
    output : log_output ;
    prefix : string ref }

let colored ansi =
  Printf.sprintf "\027[%sm%s\027[0m" ansi

let red = colored "1;31"
let yellow = colored "1;33"
let green = colored "1;32"

let log_file tm =
  Printf.sprintf "%04d-%02d-%02d"
    (tm.Unix.tm_year+1900) (tm.Unix.tm_mon+1) tm.Unix.tm_mday

let do_output =
  let ocr = ref None and fname = ref "" in
  fun output tm is_err ->
    match output with
    | Directory logdir ->
      let fname' = log_file tm in
      if fname' <> !fname then (
        let open Legacy.Unix in
        fname := fname' ;
        Option.may (ignore_exceptions @@ close_out) !ocr ;
        let path = logdir ^"/"^ !fname in
        let fd = BatUnix.openfile path
                   [O_WRONLY; O_APPEND; O_CREAT; O_CLOEXEC] 0o644 in
        (* Make sure everything that gets written to stdout/err end up
         * logged in that file too: *)
        dup2 fd stderr ;
        dup2 fd stdout ;
        let oc = BatUnix.out_channel_of_descr fd in
        ocr := Some oc
      ) ;
      Option.get !ocr
    | Stdout -> if is_err then stderr else stdout
    | Syslog -> assert false

let make_prefix s =
  if s = "" then s else (colored "1;34" (" "^s)) ^":"

let rate_limit max_rate =
  let last_sec = ref 0 and count = ref 0 in
  fun now ->
    let sec = int_of_float now in
    if sec = !last_sec then (
      incr count ;
      !count > max_rate
    ) else (
      last_sec := sec ;
      count := 0 ;
      false
    )

let make_logger ?logdir ?(prefix="") dbg =
  let output = match logdir with Some s -> Directory s | _ -> Stdout in
  let prefix = ref (make_prefix prefix) in
  let rate_limit = rate_limit 10 in
  let skip = ref 0 in
  let do_log is_err col fmt =
    let open Unix in
    let now = time () in
    let tm = localtime now in
    let time_pref =
      Printf.sprintf "%02dh%02dm%02d:"
        tm.tm_hour tm.tm_min tm.tm_sec in
    let oc = do_output output tm is_err in
    let p =
      if is_err && rate_limit now then (
        incr skip ;
        Printf.ifprintf
      ) else (
        if !skip > 0 then (
          Printf.fprintf oc "%d skipped" !skip ;
          skip := 0
        ) ;
        Printf.fprintf
      ) in
    p oc ("%s%s " ^^ fmt ^^ "\n%!") (col time_pref) !prefix
  in
  let error fmt = do_log true red fmt
  and warning fmt = do_log true yellow fmt
  and info fmt = do_log false green fmt
  and debug fmt =
    if dbg then do_log false identity fmt
    else Printf.ifprintf stderr fmt
  in
  { error ; warning ; info ; debug ; output ; prefix }

let syslog =
  try Some (Syslog.openlog ~facility:`LOG_USER "ramen")
  with _ -> None

let make_syslog ?(prefix="") dbg =
  let prefix = ref (make_prefix prefix) in
  match syslog with
  | None ->
      failwith "No syslog facility on this host."
  | Some slog ->
      let do_log lvl fmt =
        Printf.ksprintf2 (fun str ->
          Syslog.syslog slog lvl str) fmt in
      let error fmt = do_log `LOG_ERR fmt
      and warning fmt = do_log `LOG_WARNING fmt
      and info fmt = do_log `LOG_INFO fmt
      and debug fmt =
        if dbg then do_log `LOG_DEBUG fmt
        else Printf.ifprintf stderr fmt
      in
      { error ; warning ; info ; debug ; output = Syslog ; prefix }

let set_prefix logger prefix =
  logger.prefix := make_prefix prefix

let logger = ref (make_logger false)
