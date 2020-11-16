(* Depends on Ipv4, Ipv6 and friends.
 * Used by CodeGenLib and Ramen. *)
open Batteries
open Stdint
open RamenHelpersNoLog

(*$inject open Stdint *)

(* Converters from string to values *)

(* When we convert a string to a string we quote it, so now we must
 * unquote. But this is also used when we read CSV, where we try to
 * unquote every fields, and for command line parameters, etc. Be
 * lenient in what you receive...
 * But being lenient only goes that far; what if the quotes are actually
 * part of the value? The [quotes] boolean parameter, if false, let us
 * know that no quoting must be going on.
 * In case the value is unquoted, we use fins as a list of terminating
 * characters. *)
let string_of_string ?(fins=[]) ?(may_quote=true) s o =
  let l = String.length s in
  (* Skip leading spaces if the string is quoted: *)
  let o =
    if may_quote then
      let maybe_start =
        let rec loop n =
          if n < l && Char.is_whitespace s.[n] then loop (n + 1) else n in
        loop o in
      if maybe_start < l && s.[maybe_start] = '"' then maybe_start else o
    else o
  in
  (* If the string is unquoted, grab everything until the end: *)
  if not may_quote || l < o + 2 || s.[o] <> '"' then (
    (* Until end of s or any of fins: *)
    let end_ =
      if fins = [] then l else
      let rec loop n =
        if n >= l || (let c = s.[n] in List.exists ((=) c) fins) then n
        else loop (n + 1) in
      loop o in
    String.sub s o (end_ - o), end_
  ) else
    (* FIXME: skip escaped quotes, and more generally convert escaped chars: *)
    match String.index_from s (o + 1) '"' with
    | exception Not_found ->
        String.sub s o (l - o), l
    | o' ->
        String.sub s (o + 1) (o' - o - 1), (o' + 1)

(*$= string_of_string & ~printer:(BatIO.to_string (BatTuple.Tuple2.print BatString.print BatInt.print))
  ("", 0)       (string_of_string "" 0)
  ("", 1)       (string_of_string "x" 1)
  ("x", 1)      (string_of_string "x" 0)
  ("xy", 2)     (string_of_string "xy" 0)
  ("xy", 4)     (string_of_string "\"xy\"" 0)
  ("\"xy", 3)   (string_of_string "\"xy" 0)
  ("xy\"", 3)   (string_of_string "xy\"" 0)
  ("\"xy\"", 4) (string_of_string ~may_quote:false "\"xy\"" 0)
  ("xy", 6)     (string_of_string "ab\"xy\"cd" 2)
  ("xy", 6)     (string_of_string "  \"xy\"cd" 0)
  (" xy", 3)    (string_of_string ~fins:[';';','] " xy,X" 0)
  ("x,y", 6)    (string_of_string ~fins:[';';','] " \"x,y\"X" 0)
*)

let bool_of_string b o =
  let o = string_skip_blanks b o in
  if string_sub_eq ~case_sensitive:false b o "true" 0 4 then
    true, (o + 4) else
  if string_sub_eq ~case_sensitive:false b o "false" 0 5 then
    false, (o + 5) else
  if string_sub_eq ~case_sensitive:false b o "on" 0 2 then
    true, (o + 2) else
  if string_sub_eq ~case_sensitive:false b o "off" 0 3 then
    false, (o + 3) else
  if string_sub_eq ~case_sensitive:false b o "#t" 0 2 then
    true, (o + 2) else
  if string_sub_eq ~case_sensitive:false b o "#f" 0 2 then
    false, (o + 2) else
  if String.length b > o then
    (match b.[o] with
    | 't' | 'y' | '1' -> true, o + 1
    | 'f' | 'n' | '0' -> false, o + 1
    | _ -> Printf.sprintf "Invalid boolean at offset %d" o |> failwith)
  else failwith "end of string while expecting boolean"

(*$= bool_of_string & ~printer:(BatIO.to_string (BatTuple.Tuple2.print BatBool.print BatInt.print))
  (true, 4)   (bool_of_string "true" 0)
  (false, 5)  (bool_of_string "false" 0)
  (false, 6)  (bool_of_string " false" 1)
*)

let float_of_string s o =
  (* FIXME: same without string copy (via C?) *)
  let s' = String.lchop ~n:o s in
  (* Reminder: a space in the format string matches any amount of space
   * (including none): *)
  Scanf.sscanf s' " %f%n" (fun f n -> f, o + n)

(*$= float_of_string & ~printer:(BatIO.to_string (BatTuple.Tuple2.print BatFloat.print BatInt.print))
  (1.2, 3)  (float_of_string "1.2" 0)
  (1.2, 3)  (float_of_string "1.2x" 0)
  (1.2, 4)  (float_of_string "x1.2y" 1)
  (-1.2, 5) (float_of_string "x-1.2y z" 1)
  (-0.1, 6) (float_of_string "x-1e-1y z" 1)
  (0.1, 5)  (float_of_string "x.1e0y z" 1)
*)

let char_of_string s o =
  let short () =
    if o >= String.length s then
      Printf.sprintf "Cannot parse %S as a char: too short" s |>
      failwith ;
    s.[o], o + 1
  and long () =
    if o + 3 > String.length s then
      Printf.sprintf "Cannot parse %S at %d as a char: too short" s o |>
      failwith ;
    if s.[o] <> '#' || s.[o+1] <> '\\' then
      Printf.sprintf "Cannot parse %S at %d as a char: missing prefix" s o |>
      failwith ;
    if Char.is_latin1 s.[o+2] then
      s.[o+2], o + 3
    else
      if o + 5 > String.length s then
        Printf.sprintf "Cannot parse %S at %d as a char: too short" s o |>
        failwith
      else
        let int_of_char i =
          if s.[i] < '0' || s.[i] > '9' then
            Printf.sprintf "Cannot parse %S as a char at %d" s i |>
            failwith ;
          Char.code s.[i] - Char.code '0' in
        let hi = int_of_char (o + 2)
        and mi = int_of_char (o + 3)
        and lo = int_of_char (o + 4) in
        Char.chr (hi * 64 + mi * 8 + lo), o + 5
  in
  if String.length s >= o + 2 && s.[o] = '#' && s.[o+1] = '\\' then
    long ()
  else
    short ()

(*$= char_of_string & ~printer:(BatIO.to_string (BatTuple.Tuple2.print BatChar.print BatInt.print))
  ('a', 3)  (char_of_string "#\\a" 0)
  ('a', 5)  (char_of_string "#\\141" 0)
  ('a', 1)  (char_of_string "a" 0)
*)

let rec integer_of_string p s o =
  let o = string_skip_blanks s o in
  if o >= String.length s then
    Printf.sprintf "Cannot parse %S at %d" s o |>
    failwith ;
  p s ~pos:o

let u8_of_string = integer_of_string Uint8.of_substring
let u16_of_string = integer_of_string Uint16.of_substring
let u24_of_string = integer_of_string Uint24.of_substring
let u32_of_string = integer_of_string Uint32.of_substring
let u40_of_string = integer_of_string Uint40.of_substring
let u48_of_string = integer_of_string Uint48.of_substring
let u56_of_string = integer_of_string Uint56.of_substring
let u64_of_string = integer_of_string Uint64.of_substring
let u128_of_string = integer_of_string Uint128.of_substring
let i8_of_string = integer_of_string Int8.of_substring
let i16_of_string = integer_of_string Int16.of_substring
let i24_of_string = integer_of_string Int24.of_substring
let i32_of_string = integer_of_string Int32.of_substring
let i40_of_string = integer_of_string Int40.of_substring
let i48_of_string = integer_of_string Int48.of_substring
let i56_of_string = integer_of_string Int56.of_substring
let i64_of_string = integer_of_string Int64.of_substring
let i128_of_string = integer_of_string Int128.of_substring

(*$= i32_of_string & ~printer:(BatIO.to_string (BatTuple.Tuple2.print (fun oc n -> BatInt.print oc (Int32.to_int n)) BatInt.print))
  (Int32.of_int 3600, 4) (i32_of_string "3600" 0)
  (Int32.of_int 3600, 5) (i32_of_string "+3600 " 0)
  (Int32.of_int ~-3600, 5) (i32_of_string "-3600  " 0)
  (Int32.of_int ~-3600, 7) (i32_of_string "  -3600 " 0)
 *)

let null_of_string _ o = (), o

let eth_of_string = RamenEthAddr.of_string
let ip4_of_string = RamenIpv4.of_string
let ip6_of_string = RamenIpv6.of_string
let ip_of_string = RamenIp.of_string
let cidr4_of_string = RamenIpv4.Cidr.of_string
let cidr6_of_string = RamenIpv6.Cidr.of_string
let cidr_of_string = RamenIp.Cidr.of_string
