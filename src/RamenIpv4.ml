(* Function related to IPv4 addresses *)
open Batteries
open Stdint

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

let print fmt n =
  String.print fmt (to_string n)

module Parser =
struct
  open RamenParsing

  let p m =
    let m = "IPv4" :: m in
    let small_uint =
      decimal_integer_range ~min:0 ~max:255 "IPv4 component" in
    (repeat ~min:4 ~max:4 ~sep:(char '.') small_uint >>: fun lst ->
       List.fold_left (fun (s, shf) n ->
           Uint32.(add s (shift_left (of_int n) shf)),
           shf - 8
         ) (Uint32.zero, 24) lst |> fst) m
end

let of_string = RamenParsing.of_string_exn Parser.p
(*$= of_string & ~printer:(BatIO.to_string print)
   (Stdint.Uint32.of_int32 0x01020304l) (of_string "1.2.3.4")
 *)
(*$T
   try (ignore (of_string "1.2.3.400")) ; false \
   with RamenParsing.P.ParseError _ -> true
 *)

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

  module Parser =
  struct
    open RamenParsing

    let mask =
      decimal_integer_range ~min:0 ~max:31 "CIDRv4 mask"

    let p m =
      let m = "CIDRv4" :: m in
      (Parser.p +- char '/' ++ mask >>: fun (net, len) ->
       and_to_len len net, len) m
  end

  let of_string = RamenParsing.of_string_exn Parser.p
  (*$= of_string & ~printer:(BatIO.to_string print)
     (Stdint.Uint32.of_int32 0x0A0A0100l, 24) (of_string "10.10.1.0/24")
     (Stdint.Uint32.of_int32 0x0A0A0100l, 24) (of_string "10.10.1.42/24")
   *)

  let to_string (net, len) =
    let net = Uint32.(logand (netmask_of_len len) net) in
    to_string net ^"/"^ string_of_int len
  (*$= to_string & ~printer:(fun x -> x)
     "192.168.10.0/24" \
       (to_string (Stdint.Uint32.of_string "0xC0A80A00", 24))
   *)

  let print fmt t = String.print fmt (to_string t)

  (*$>*)
end
