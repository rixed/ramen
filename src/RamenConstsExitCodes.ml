(* These first 3 are not us to define: *)
let terminated = 0
let interrupted = 1
let lwt_uncaught_exception = 2
let cannot_parse_param = 3
let watchdog = 4
let uncaught_exception = 5
let damaged_ringbuf = 6
let other_error = 7

let string_of_code = function
  | 0 -> "terminated"
  | 1 -> "interrupted"
  | 2 -> "uncaught exception"
  | 3 -> "cannot parse a parameter"
  | 4 -> "killed by watchdog"
  | 5 -> "crashed"
  | 6 -> "suffered ringbuffer damage"
  | 7 -> "other error"
  | _ -> "unknown"
