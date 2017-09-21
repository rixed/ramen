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

let slash = char ~what:"slash" '/'

let id_quote = char ~what:"quote" '\''

let node_identifier =
  let first_char = letter ||| underscore ||| slash in
  let any_char = first_char ||| decimal_digit in
  (first_char ++
     repeat_greedy ~sep:none ~what:"node identifier" any_char >>:
   fun (c, s) -> String.of_list (c :: s)) |||
  (id_quote -+
   repeat_greedy ~sep:none ~what:"node identifier" (
     cond "quoted node identifier" (fun c -> c <> '\'') 'x') +-
   id_quote >>:
  fun s -> String.of_list s)
