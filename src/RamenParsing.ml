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

let strinGs s = strinG s ||| strinG (s ^"s")

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

let not_in_range what ?min ?max n =
  let e =
    what ^" must be "^ match min, max with
    | None, None -> "all right, so what's the problem?"
    | Some m, None -> "greater than or equal to "^ Num.to_string m
    | None, Some m -> "less than or equal to "^ Num.to_string m
    | Some mi, Some ma -> "between "^ Num.to_string mi ^" and "^
                          Num.to_string ma ^" (inclusive)" in
  raise (Reject e)

(* Accept any notation (decimal, hexa, etc) only in the given range. Returns
 * a Num. *)
let integer_range ?min ?max =
  integer >>: fun n ->
    if Option.map_default (Num.ge_num n) true min &&
       Option.map_default (Num.le_num n) true max
    then n
    else not_in_range "integer" ?min ?max n

(* Only accept decimal integers from min to max (inclusive). *)
let decimal_integer_range ?min ?max what =
  let decimal_integer what m =
    let m = what :: m in
    (decimal_number >>: fun n ->
      try Num.int_of_num n
      with Failure _ ->
        raise (Reject "too big for an OCaml int")) m
  in
  decimal_integer what >>: fun n ->
    if Option.map_default ((>=) n) true min &&
       Option.map_default ((<=) n) true max
    then n
    else not_in_range what ?min:(Option.map Num.of_int min)
                           ?max:(Option.map Num.of_int max) n

let pos_decimal_integer what =
  decimal_integer_range ~min:0 ?max:None what

let number =
  floating_point ||| (decimal_number >>: Num.to_float)

(* TODO: "duration and duration" -> add the durations *)
let duration m =
  let m = "duration" :: m in
  (
    (number >>: fun n -> n, 1.) ||| (* unitless number are seconds *)
    (optional ~def:1. (number +- blanks) ++
     ((strinGs "microsecond" >>: fun () -> 0.000_001) |||
      (strinGs "millisecond" >>: fun () -> 0.001) |||
      (strinGs "second" >>: fun () -> 1.) |||
      (strinGs "minute" >>: fun () -> 60.) |||
      (strinGs "hour" >>: fun () -> 3600.))) >>:
   fun (dur, scale) ->
     let d = dur *. scale in
     if d < 0. then
       raise (Reject "durations must be greater than zero") ;
     d
  ) m

let list_sep m =
  let m = "list separator" :: m in
  (opt_blanks -- char ',' -- opt_blanks) m

let list_sep_and m =
  let m = "list separator" :: m in
  (
    (blanks -- strinG "and" -- blanks) |||
    (opt_blanks -- char ',' -- opt_blanks)
  ) m
