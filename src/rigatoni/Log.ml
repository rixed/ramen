open Batteries

type 'a printer =
  ('a, unit BatIO.output, unit) format -> 'a

type logger =
  { info : 'a. 'a printer ;
    debug : 'a. 'a printer }

let make_logger dbg =
  let info fmt =
    Printf.fprintf stdout (fmt ^^ "\n%!")
  and debug fmt =
    if dbg then Printf.fprintf stdout (fmt ^^ "\n%!")
    else Printf.ifprintf stdout fmt
  in
  { info ; debug }

