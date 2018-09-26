(* Modules and helpers related to parsing *)
open Batteries

module PConfig = ParsersPositions.LineCol (Parsers.SimpleConfig (Char))
module P = Parsers.Make (PConfig)
module ParseUsual = ParsersUsual.Make (P)
include P
include ParseUsual

(*$inject
  open TestHelpers
*)

let blank = ParseUsual.blank >>: ignore
let newline = ParseUsual.newline >>: ignore

let all_but_newline =
  cond "anything until newline" (fun c -> c <> '\n' && c <> '\r') 'x'

let comment =
  char '-' -- char '-' --
  repeat_greedy ~sep:none ~what:"comment" all_but_newline

let blanks =
  repeat_greedy ~min:1 ~sep:none ~what:"whitespaces"
    (blank ||| newline ||| comment) >>: ignore

let opt_blanks =
  optional_greedy ~def:() blanks

let allow_surrounding_blanks ppp =
  opt_blanks -+ ppp +- opt_blanks +- eof

(* strinG will match the given string regardless of the case and
 * regardless of the surrounding (ie even if followed by other letters). *)
let strinG = ParseUsual.string ~case_sensitive:false

let that_string s =
  strinG s >>: fun () -> s (* because [string] returns () *)

let strinGs s = strinG s ||| strinG (s ^"s")

(* but word would match only if the string is not followed by other
 * letters: *)
let word ?case_sensitive s =
  ParseUsual.string ?case_sensitive s -- nay letter

let worD = word ~case_sensitive:false

let worDs s = worD s ||| worD (s ^"s")

(* Given we frequently encode strings with "%S", we need to parse them back
 * with numerical escape sequence in base 10: *)
let quoted_string = quoted_string ~base_num:10

(*$= quoted_string & ~printer:(test_printer BatString.print)
  (Ok ("→", (14, []))) \
    (test_p quoted_string "\"\\226\\134\\146\"")
 *)

let slash = char ~what:"slash" '/'
let star = char ~what:"star" '*'
let dot = char ~what:"dot" '.'
let char_ ?what x = char ?what x >>: fun _ -> ()

let id_quote = char_ ~what:"quote" '\''

let not_in_range ?min ?max what =
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
    else not_in_range ?min ?max "integer"

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
    else not_in_range ?min:(Option.map Num.of_int min)
                      ?max:(Option.map Num.of_int max) what

let pos_decimal_integer what =
  decimal_integer_range ~min:0 ?max:None what

let number =
  floating_point ||| (decimal_number >>: Num.to_float)

let rec duration m =
  let m = "duration" :: m in
  let single_duration =
    number +- opt_blanks ++
    (((worDs "microsecond" ||| word "μs") >>: fun () -> 0.000_001) |||
     ((worDs "millisecond" ||| word "ms") >>: fun () -> 0.001) |||
     ((worDs "second" ||| word "s") >>: fun () -> 1.) |||
     ((worDs "minute" ||| word "min" ||| word "m") >>: fun () -> 60.) |||
     ((worDs "hour" ||| word "h") >>: fun () -> 3600.)) >>:
   fun (dur, scale) ->
     let d = dur *. scale in
     if d < 0. then
       raise (Reject "durations must be greater than zero") ;
     d
  in
  (
    let sep = (blanks -- strinG "and" -- blanks) |||
              (opt_blanks -- char ',' -- blanks) in
    let sep = optional ~def:() sep in
    several ~sep single_duration >>: List.reduce (+.)
  ) m

(*$= duration & ~printer:(test_printer BatFloat.print)
  (Ok (42., (3, []))) \
    (test_p duration "42s")
  (Ok (123.4, (41, []))) \
    (test_p duration "2 minutes, 3 seconds and 400 milliseconds")
  (Ok (62., (4, []))) \
    (test_p duration "1m2s")
  (Ok (1.234, (26,[]))) \
    (test_p duration "1 second, 234 milliseconds")
  (Ok (121.5, (6,[]))) \
    (test_p duration "2m1.5s")
 *)

(* TODO: use more appropriate units *)
let print_duration oc d =
  Printf.fprintf oc "%f seconds" d

let list_sep m =
  let m = "list separator" :: m in
  (opt_blanks -- char ',' -- opt_blanks) m

let list_sep_and m =
  let m = "list separator" :: m in
  (
    (blanks -- strinG "and" -- blanks) |||
    (opt_blanks -- char ',' -- opt_blanks)
  ) m

let keyword =
  (
    (* Some values that must not be parsed as field names: *)
    strinG "true" ||| strinG "false" ||| strinG "null" |||
    strinG "all" ||| strinG "as" |||
    (* Or "X in top" could also be parsed as an independent expression: *)
    strinG "top" ||| strinG "group" |||
    (* Some functions with possibly no arguments that must not be
     * parsed as field names: *)
    strinG "now" ||| strinG "random"
  ) -- check (nay (letter ||| underscore ||| decimal_digit))

let non_keyword =
  (check ~what:"no quoted identifier" (nay id_quote) -+
   check ~what:"no keyword" (nay keyword) -+
   identifier) |||
  (id_quote -+ identifier +- id_quote)
