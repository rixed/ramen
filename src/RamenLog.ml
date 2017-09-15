open Batteries

type 'a printer =
  ('a, unit BatIO.output, unit) format -> 'a

type logger =
  { error : 'a. 'a printer ;
    warning : 'a. 'a printer ;
    info : 'a. 'a printer ;
    debug : 'a. 'a printer ;
    logdir : string option }

let colored ansi =
  Printf.sprintf "\027[%sm%s\027[0m" ansi

let red = colored "1;31"
let yellow = colored "1;33"
let green = colored "1;32"

let make_logger ?logdir ?(prefix="") dbg =
  let prefix = colored "1;34" prefix in
  let fd = ref None and fname = ref "" in
  let do_log col fmt =
    let open Unix in
    let tm = gettimeofday () |> localtime in
    let time_pref =
      Printf.sprintf "%02dh%02dm%02d: "
        tm.tm_hour tm.tm_min tm.tm_sec in
    let oc =
      match logdir with
      | Some logdir ->
        let fname' =
          Printf.sprintf "%04d-%02d-%02d"
            (tm.tm_year+1900) (tm.tm_mon+1) tm.tm_mday in
        if fname' <> !fname then (
          fname := fname' ;
          Option.may IO.close_out !fd ;
          let path = logdir ^"/"^ !fname in
          fd := Some (File.open_out ~mode:[`append;`create;`text] path)
        ) ;
        Option.get !fd
      | None -> IO.stderr
    in
    Printf.fprintf oc ("%s%s" ^^ fmt ^^ "\n%!") (col time_pref) prefix
  in
  let error fmt = do_log red fmt
  and warning fmt = do_log yellow fmt
  and info fmt = do_log green fmt
  and debug fmt =
    if dbg then do_log identity fmt
    else Printf.ifprintf stderr fmt
  in
  { error ; warning ; info ; debug ; logdir }

let logger = ref (make_logger false)

