(* These first 3 are not us to define: *)
let terminated = 0
let interrupted = 1
let test_failed = 1
let lwt_uncaught_exception = 2
let cannot_parse_param = 3
let watchdog = 4
let uncaught_exception = 5
let damaged_ringbuf = 6
let confserver_unreachable = 7
let confserver_migrated = 8
let other_error = 9

let string_of_code = function
  | 0 -> "terminated"
  | 1 -> "interrupted/failure"
  | 2 -> "uncaught exception"
  | 3 -> "cannot parse a parameter"
  | 4 -> "killed by watchdog"
  | 5 -> "crashed"
  | 6 -> "suffered ringbuffer damage"
  | 7 -> "cannot reach the confserver"
  | 8 -> "confserver restarted after migration"
  | 9 -> "other error"
  | _ -> "unknown"
