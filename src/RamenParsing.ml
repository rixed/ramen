(* Modules and helpers related to parsing *)
open Batteries

module PConfig = ParsersPositions.LineCol (Parsers.SimpleConfig (Char))
module P = Parsers.Make (PConfig)
module ParseUsual = ParsersUsual.Make (P)
include P
include ParseUsual

(*$inject
  open TestHelpers
  open Batteries
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

let string_parser ~what ~print p =
  let p = allow_surrounding_blanks p in
  fun s ->
    let stream = stream_of_string s in
    (* TODO: enable error correction *)
    match p [what] None Parsers.no_error_correction stream |> to_result with
    | Bad e ->
      Printf.sprintf2 "Parse error: %a" (print_bad_result print) e |>
      failwith
    | Ok (res, _) -> res

(* strinG will match the given string regardless of the case and
 * regardless of the surrounding (ie even if followed by other letters). *)
let strinG = ParseUsual.string ~case_sensitive:false

let that_string s =
  strinG s >>: fun () -> s (* because [string] returns () *)

let strinGs s = strinG s ||| strinG (s ^"s")

let legit_identifier_chars = letter ||| underscore ||| decimal_digit

(* but word would match only if the string is not followed by other
 * letters (trailing numbers are OK, and it's actually used by the
 * duration parser): *)
let word ?case_sensitive s =
  ParseUsual.string ?case_sensitive s +- nay letter

let worD = word ~case_sensitive:false

let worDs s = worD s ||| worD (s ^"s")

(* Given we frequently encode strings with "%S", we need to parse them back
 * with numerical escape sequence in base 10: *)
let quoted_string = quoted_string ~base_num:10

(*$= quoted_string & ~printer:identity
  "\226\134\146" \
    (test_expr ~printer:BatString.print quoted_string "\"\\226\\134\\146\"")
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

(* Only accept decimal integers from min to max (inclusive) with no scale suffix,
 * and returns an int (used for IP addresses, port numbers... *)
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

let num_scale =
  optional ~def:Num.one (opt_blanks -+
    let str ?(case_sensitive=true) s scale =
      ParseUsual.string ~case_sensitive s >>: fun () -> scale in
    let stR = str ~case_sensitive:false in
    let pico = Num.(div one (of_int 1_000_000_000))
    and micro = Num.(div one (of_int 1_000_000))
    and milli = Num.(div one (of_int 1_000))
    and kilo = Num.(of_int 1_000)
    and mega = Num.(of_int 1_000_000)
    and giga = Num.(of_int 1_000_000_000)
    in
    (str "p" pico) |||
    (stR "pico" pico) |||
    (str "µ" micro) |||
    (stR "micro" micro) |||
    (str "m" milli ) |||
    (stR "milli" milli ) |||
    (str "k" kilo) |||
    (stR "kilo" kilo) |||
    (str "M" mega) |||
    (stR "mega" mega) |||
    (str "G" giga) |||
    (stR "giga" giga) |||
    (str "Ki" (Num.of_int 1024)) |||
    (str "Mi" (Num.of_int 1_048_576)) |||
    (str "Gi" (Num.of_int 1_073_741_824)) |||
    (str "Ti" (Num.of_int 1_099_511_627_776)))

let float_scale =
  num_scale >>: Num.to_float

let number m =
  let m = "number" :: m in
  (
    (floating_point ||| (decimal_number >>: Num.to_float)) ++
    float_scale >>: fun (n, scale) -> n *. scale
  ) m

(*$= number & ~printer:identity
  "42." (test_expr ~printer:BatFloat.print number "42")
  "42." (test_expr ~printer:BatFloat.print number "0.042k")
  "42." (test_expr ~printer:BatFloat.print number "0.042 k")
  "42." (test_expr ~printer:BatFloat.print number "0.042 kilo")
  "42." (test_expr ~printer:BatFloat.print number "42000m")
  "42." (test_expr ~printer:BatFloat.print number "42000milli")
*)

let rec duration m =
  let m = "duration" :: m in
  let single_duration =
    number +- opt_blanks ++ (
      ((worDs "second" ||| worDs "sec" ||| word "s") >>: fun () -> 1.) |||
      ((worDs "minute" ||| worDs "min") >>: fun () -> 60.) |||
      ((worDs "hour" ||| word "h") >>: fun () -> 3600.)
      (* Length of a day is already not doable due to DST *)
    ) >>: fun (dur, scale) ->
      let d = dur *. scale in
      if d < 0. then
        raise (Reject "durations must be greater than zero") ;
      d
  in
  (
    let sep = (blanks -- strinG "and" -- blanks) |||
              (opt_blanks -- char ',' -- blanks) in
    let sep = optional ~def:() sep in
    (
      several ~sep single_duration >>: List.reduce (+.)
    ) ||| (
      (* We must not allow "m" for minutes above because it would be ambiguous with
       * "milli", but we'd really like it to work when used with "h" or "s", so there
       * you go: *)
      optional ~def:None (
        some (number +- opt_blanks +- word "h" +- opt_blanks)) ++
      number +- opt_blanks +- word "m" ++
      optional ~def:None (
        some (opt_blanks -+ number +- opt_blanks +- word "s")) >>:
      function
        | (None, _), None ->
            raise (Reject "Ambiguous \"m\": is it \"minutes\" or \"milli\"?")
        | (h, m), s ->
            let h = h |? 0. and s = s |? 0. in
            h *. 3600. +. m *. 60. +. s
    )
  ) m

(*$= duration & ~printer:identity
  "42." \
    (test_expr ~printer:BatFloat.print duration "42s")
  "123.4" \
    (test_expr ~printer:BatFloat.print duration "2 minutes, 3 seconds and 400 milliseconds")
  "62." \
    (test_expr ~printer:BatFloat.print duration "1m2s")
  "1.234" \
    (test_expr ~printer:BatFloat.print duration "1 second, 234 milliseconds")
  "121.5" \
    (test_expr ~printer:BatFloat.print duration "2m1.5s")
  "3720." \
    (test_expr ~printer:BatFloat.print duration "1h2m")
*)

(* TODO: use more appropriate units *)
let print_duration oc d =
  Printf.fprintf oc "%f seconds" d

(* Accept any notation (decimal, hexa, etc) only in the given range. Returns
 * a Num. *)
let integer_range ?min ?max =
  integer ++ num_scale >>: fun (n, s) ->
    let n = Num.mul n s in
    if Num.is_integer_num n then
      if Option.map_default (Num.ge_num n) true min &&
         Option.map_default (Num.le_num n) true max
      then n
      else not_in_range ?min ?max "integer"
    else raise (Reject "not an integer")

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
    strinG "keep" ||| strinG "all" ||| strinG "as" |||
    (* Or "X in top" could also be parsed as an independent expression: *)
    strinG "top" ||| strinG "group" |||
    (* Some functions with possibly no arguments that must not be
     * parsed as field names: *)
    strinG "now" ||| strinG "random"
  ) -- check (nay legit_identifier_chars)

let non_keyword =
  (check ~what:"no quoted identifier" (nay id_quote) -+
   check ~what:"no keyword" (nay keyword) -+
   identifier) |||
  (
    id_quote -+ (
    repeat_greedy ~sep:none (
      cond "quoted identifier" (fun c -> c <> '\'') 'x') >>: String.of_list) +-
    id_quote
  )
