open Batteries

type 'a printer =
  ('a, unit BatIO.output, unit) format -> 'a

type logger =
  { error : 'a. 'a printer ;
    warning : 'a. 'a printer ;
    info : 'a. 'a printer ;
    debug : 'a. 'a printer ;
    logdir : string option ;
    prefix : string ref }

let colored ansi =
  Printf.sprintf "\027[%sm%s\027[0m" ansi

let red = colored "1;31"
let yellow = colored "1;33"
let green = colored "1;32"

let log_file tm =
  Printf.sprintf "%04d-%02d-%02d"
    (tm.Unix.tm_year+1900) (tm.Unix.tm_mon+1) tm.Unix.tm_mday

let output =
  let ocr = ref None and fname = ref "" in
  fun ?logdir tm is_err ->
    match logdir with
    | Some logdir ->
      let fname' = log_file tm in
      if fname' <> !fname then (
        let open Legacy.Unix in
        fname := fname' ;
        Option.may (ignore_exceptions @@ close_out) !ocr ;
        let path = logdir ^"/"^ !fname in
        let fd = BatUnix.openfile path [O_WRONLY; O_APPEND; O_CREAT; O_CLOEXEC] 0o644 in
        (* Make sure everything that gets written to stdout/err end up
         * logged in that file too: *)
        dup2 fd stderr ;
        dup2 fd stdout ;
        let oc = BatUnix.out_channel_of_descr fd in
        ocr := Some oc
      ) ;
      Option.get !ocr
    | None -> if is_err then stderr else stdout

let make_prefix s =
  if s = "" then s else (colored "1;34" s) ^": "

let make_logger ?logdir ?(prefix="") dbg =
  let prefix = ref (make_prefix prefix) in
  let do_log is_err col fmt =
    let open Unix in
    let tm = gettimeofday () |> localtime in
    let time_pref =
      Printf.sprintf "%02dh%02dm%02d: "
        tm.tm_hour tm.tm_min tm.tm_sec in
    let oc = output ?logdir tm is_err in
    Printf.fprintf oc ("%s%s" ^^ fmt ^^ "\n%!") (col time_pref) !prefix
  in
  let error fmt = do_log true red fmt
  and warning fmt = do_log true yellow fmt
  and info fmt = do_log false green fmt
  and debug fmt =
    if dbg then do_log false identity fmt
    else Printf.ifprintf stderr fmt
  in
  { error ; warning ; info ; debug ; logdir ; prefix }

let set_prefix logger prefix =
  logger.prefix := make_prefix prefix

let logger = ref (make_logger false)
