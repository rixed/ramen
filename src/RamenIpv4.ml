(* Function related to IPv4 addresses *)
open Batteries
open Stdint
open RamenHelpers

let to_string =
  let mask = Uint32.of_int 255 in
  let digit n shf =
    Uint32.(shift_right_logical n shf |>
            logand mask |>
            to_int) in
  let digit_sz n =
    if n < 10 then 1 else if n < 100 then 2 else 3 in
  let zero = Char.code '0' in
  let char_of n =
    Char.chr (zero + n) in
  fun n ->
    let ns = List.map (digit n) [24; 16; 8; 0] in
    let sz = List.fold_left (fun s n -> digit_sz n + s) 3 ns in
    let s = String.create sz in
    List.fold_left (fun si n ->
      let si =
        if si = 0 then 0 else (s.[si] <- '.' ; si + 1) in
      let si =
        if n < 100 then si else (
          s.[si] <- char_of (n / 100) ; si + 1) in
      let si =
        if n < 10 then si else (
          s.[si] <- char_of ((n / 10) mod 10) ; si + 1) in
      let si =
        s.[si] <- char_of (n mod 10) ; si + 1 in
      si) 0 ns |> ignore ;
    Bytes.to_string s

(*$= to_string & ~printer:(fun x -> x)
  "123.45.67.89" \
    (to_string (Stdint.Uint32.of_string "0x7B2D4359"))
 *)

let print oc n =
  String.print oc (to_string n)

module Parser =
struct
  open RamenParsing

  let p m =
    let m = "IPv4" :: m in
    let small_uint =
      decimal_integer_range ~min:0 ~max:255 "IPv4 component" in
    (
      dismiss_error_if (parsed_fewer_than 6) (
        repeat ~min:4 ~max:4 ~sep:(char '.') small_uint >>: fun lst ->
          List.fold_left (fun (s, shf) n ->
            Uint32.(add s (shift_left (of_int n) shf)),
            shf - 8
          ) (Uint32.zero, 24) lst |> fst
      )
    ) m
end

(* Fast parser from string for reading CSVs, command line etc... *)
let of_string s o =
  let check_dot o =
    if o >= String.length s then
      failwith "Missing dot in IPv4 at end of string" ;
    if s.[o] <> '.' then
      Printf.sprintf "Missing dot in IPv4 at offset %d" o |>
      failwith
  and check_digit n =
    if n < 0 || n > 255 then
      Printf.sprintf "Invalid %d in IPv4" n |>
      failwith
  in
  let o = string_skip_blanks s o in
  let n1, o = unsigned_of_string s o in
  check_dot o ;
  let n2, o = unsigned_of_string s (o + 1) in
  check_dot o ;
  let n3, o = unsigned_of_string s (o + 1) in
  check_dot o ;
  let n4, o = unsigned_of_string s (o + 1) in
  check_digit n1 ; check_digit n2 ; check_digit n3 ; check_digit n4 ;
  let open Uint32 in
  (
    shift_left (of_int n1) 24 |>
    add (of_int (n2 lsl 16)) |>
    add (of_int (n3 lsl 8)) |>
    add (of_int n4)
  ), o

(*$= of_string & ~printer:(BatIO.to_string (BatTuple.Tuple2.print print BatInt.print))
   (Stdint.Uint32.of_int32 0x01020304l, 7)  (of_string "1.2.3.4" 0)
   (Stdint.Uint32.of_int32 0x0b0c0d0el, 11) (of_string "11.12.13.14" 0)
 *)

(*$T
   try (ignore (of_string "1.2.3.400" 0)) ; false \
   with Failure _ -> true
 *)

(* Helper to parse ip/mask: *)
let cidr_of_string p max s o =
  let ip, o = p s o in
  if o >= String.length s || s.[o] <> '/' then
    Printf.sprintf "Missing netmask at offset %d" o |>
    failwith ;
  let mask, o' = unsigned_of_string s (o + 1) in
  if mask > max then
    Printf.sprintf "Invalid netmask /%d at offset %d" mask o |>
    failwith ;
  (ip, mask), o'


module Cidr =
struct
  (*$< Cidr *)
  type t = uint32 * int [@@ppp PPP_OCaml]

  let netmask_of_len len =
    let shf = 32 - len in
    Uint32.(shift_left max_int shf)

  let and_to_len len net =
    Uint32.(logand (netmask_of_len len) net)

  let or_to_len len net =
    let shf = 32 - len in
    Uint32.(logor net ((shift_left one shf) - one))

  let first (net, len) = and_to_len len net
  let last (net, len) = or_to_len len net
  let is_in ip cidr =
    Uint32.compare ip (first cidr) >= 0 &&
    Uint32.compare ip (last cidr) <= 0

  module Parser =
  struct
    open RamenParsing

    let mask =
      decimal_integer_range ~min:0 ~max:31 "CIDRv4 mask"

    let p m =
      let m = "CIDRv4" :: m in
      (
        Parser.p +- char '/' ++ mask >>: fun (net, len) ->
        and_to_len len net, len
      ) m
  end

  let of_string s o =
    let (net, len), o = cidr_of_string of_string 32 s o in
    (and_to_len len net, len), o

  (*$= of_string & ~printer:(BatIO.to_string (BatTuple.Tuple2.print print BatInt.print))
     ((Stdint.Uint32.of_int32 0x0A0A0100l, 24), 12) \
        (of_string "10.10.1.0/24" 0)
     ((Stdint.Uint32.of_int32 0x0A0A0100l, 24), 13) \
        (of_string "10.10.1.42/24" 0)
  *)

  let to_string (net, len) =
    let net = Uint32.(logand (netmask_of_len len) net) in
    to_string net ^"/"^ string_of_int len

  (*$= to_string & ~printer:(fun x -> x)
     "192.168.10.0/24" \
       (to_string (Stdint.Uint32.of_string "0xC0A80A00", 24))
   *)

  let print oc t = String.print oc (to_string t)

  (*$>*)
end
