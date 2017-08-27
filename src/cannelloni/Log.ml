open Batteries

type 'a printer =
  ('a, unit BatIO.output, unit) format -> 'a

type logger =
  { error : 'a. 'a printer ;
    info : 'a. 'a printer ;
    debug : 'a. 'a printer }

let colored ansi =
  Printf.sprintf "\027[%sm%s\027[0m" ansi

let red = colored "1;31"
let yellow = colored "1;33"
let green = colored "1;32"

let time () =
  let open Unix in
  let tm = gettimeofday () |> localtime in
  Printf.sprintf "%02dh%02dm%02d: "
    tm.tm_hour tm.tm_min tm.tm_sec

let make_logger ?(prefix="") dbg =
  let prefix = colored "1;34" prefix in
  let error fmt =
    Printf.fprintf stderr ("%s%s" ^^ fmt ^^ "\n%!") (red (time ())) prefix
  and info fmt =
    Printf.fprintf stderr ("%s%s" ^^ fmt ^^ "\n%!") (yellow (time ())) prefix
  and debug fmt =
    if dbg then
      Printf.fprintf stderr ("%s%s" ^^ fmt ^^ "\n%!") (green (time ())) prefix
    else Printf.ifprintf stderr fmt
  in
  { error ; info ; debug }

let logger = ref (make_logger false)

