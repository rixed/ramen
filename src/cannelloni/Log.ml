open Batteries

type 'a printer =
  ('a, unit BatIO.output, unit) format -> 'a

type logger =
  { error : 'a. 'a printer ;
    info : 'a. 'a printer ;
    debug : 'a. 'a printer }

let make_logger dbg =
  let error fmt =
    Printf.fprintf stderr (fmt ^^ "\n%!")
  and info fmt =
    Printf.fprintf stderr (fmt ^^ "\n%!")
  and debug fmt =
    if dbg then Printf.fprintf stderr (fmt ^^ "\n%!")
    else Printf.ifprintf stderr fmt
  in
  { error ; info ; debug }

let logger = ref (make_logger false)

