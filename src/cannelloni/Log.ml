open Batteries

type 'a printer =
  ('a, unit BatIO.output, unit) format -> 'a

type logger =
  { error : 'a. 'a printer ;
    info : 'a. 'a printer ;
    debug : 'a. 'a printer }

let make_logger ?(prefix="") dbg =
  let error fmt =
    Printf.fprintf stderr ("%s" ^^ fmt ^^ "\n%!") prefix
  and info fmt =
    Printf.fprintf stderr ("%s" ^^ fmt ^^ "\n%!") prefix
  and debug fmt =
    if dbg then Printf.fprintf stderr ("%s" ^^ fmt ^^ "\n%!") prefix
    else Printf.ifprintf stderr fmt
  in
  { error ; info ; debug }

let logger = ref (make_logger false)

