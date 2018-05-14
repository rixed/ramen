(* Modules and helpers related to parsing *)
open Batteries

module PConfig = ParsersPositions.LineCol (Parsers.SimpleConfig (Char))
module P = Parsers.Make (PConfig)
module ParseUsual = ParsersUsual.Make (P)
include P
include ParseUsual

let strinG = ParseUsual.string ~case_sensitive:false

let that_string s =
  strinG s >>: fun () -> s (* because [string] returns () *)

let blank = ParseUsual.blank >>: ignore
let newline = ParseUsual.newline >>: ignore

let comment =
  let all_but_newline =
    cond "anything until newline" (fun c -> c <> '\n' && c <> '\r') 'x'
  in
  char '-' -- char '-' --
  repeat_greedy ~sep:none ~what:"comment" all_but_newline

let blanks =
  repeat_greedy ~min:1 ~sep:none ~what:"whitespaces"
    (blank ||| newline ||| comment) >>: ignore

let opt_blanks =
  optional_greedy ~def:() blanks

let allow_surrounding_blanks ppp =
  opt_blanks -+ ppp +- opt_blanks +- eof

let slash = char ~what:"slash" '/'
let star = char ~what:"star" '*'

let id_quote = char ~what:"quote" '\''

(* program_allowed: if true, the function name can be prefixed with a program
 * name. *)
let func_identifier ?(globs_allowed=false) ~program_allowed =
  let first_char = letter ||| underscore in
  let first_char =
    if program_allowed then first_char ||| slash
    else first_char in
  let first_char =
    if globs_allowed then first_char ||| star
    else first_char in
  let any_char = first_char ||| decimal_digit in
  (first_char ++
     repeat_greedy ~sep:none ~what:"function identifier" any_char >>:
   fun (c, s) -> String.of_list (c :: s)) |||
  (id_quote -+
   repeat_greedy ~sep:none ~what:"function identifier" (
     cond "quoted function identifier" (fun c ->
       c <> '\'' && (program_allowed || c <> '/')) 'x') +-
   id_quote >>:
  fun s -> String.of_list s)

let pos_integer what =
  decimal_number >>: fun n ->
    if Num.sign_num n < 0 then
      raise (Reject (what ^" must be greater than zero"))
    else Num.int_of_num n

let number =
  floating_point ||| (decimal_number >>: Num.to_float)

(* TODO: "duration and duration" -> add the durations *)
let duration m =
  let m = "duration" :: m in
  let strinGs s = strinG s ||| strinG (s ^"s") in
  (optional ~def:1. (number +- blanks) ++
   ((strinGs "microsecond" >>: fun () -> 0.000_001) |||
    (strinGs "millisecond" >>: fun () -> 0.001) |||
    (strinGs "second" >>: fun () -> 1.) |||
    (strinGs "minute" >>: fun () -> 60.) |||
    (strinGs "hour" >>: fun () -> 3600.)) >>:
   fun (dur, scale) ->
     let d = dur *. scale in
     if d < 0. then
       raise (Reject "durations must be greater than zero") ;
     d) m
