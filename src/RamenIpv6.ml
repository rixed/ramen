(* Function related to IPv6 addresses *)
open Batteries
open Stdint
open RamenHelpers

(* Used to compress 0s in IPv6: *)
type word_type = Zeros of int | Word of int

let string_of_words lst =
  let s = Bytes.create (8*(4+1)) in
  let rec loop i (* last index in bytes s *) need_sep = function
    | [] -> i
    | Word v :: lst ->
        let i =
          if need_sep then (
            s.[i] <- ':' ; i + 1
          ) else i in
        let i =
          if v < 0x1000 then i else (
            s.[i] <- hex_of (v lsr 12) ; i + 1) in
        let i =
          if v < 0x100 then i else (
            s.[i] <- hex_of ((v lsr 8) land 0xf) ; i + 1) in
        let i =
          if v < 0x10 then i else (
            s.[i] <- hex_of ((v lsr 4) land 0xf) ; i + 1) in
        let i =
          s.[i] <- hex_of (v land 0xf) ; i + 1 in
        loop i true lst
    | Zeros _ :: lst ->
        s.[i] <- ':' ; s.[i+1] <- ':' ;
        loop (i + 2) false lst
  in
  let len = loop 0 false lst in
  Bytes.sub_string s 0 len

(*$= string_of_words & ~printer:(fun x -> x)
  "1" (string_of_words [ Word 1 ])
  "1:2" (string_of_words [ Word 1 ; Word 2 ])
  "1::2" (string_of_words [ Word 1 ; Zeros 42 ; Word 2 ])
  "::2a" (string_of_words [ Zeros 32 ; Word 42 ])
 *)

let to_string =
  let mask = Uint128.of_int 65535 in
  let word n shf =
    Uint128.(shift_right_logical n shf |>
             logand mask |>
             to_int) in
  fun n ->
    let rec loop lst max_zeros zeros shf =
      if shf < 0 then
        let lst = if zeros > 0 then Zeros zeros :: lst else lst in
        (* replace all Zeros by Word 0s but the first one with max_zeros: *)
        let _, lst =
          List.fold_left (fun (max_zeros, res) -> function
            | Zeros z as zeros ->
                if z = max_zeros then (* The chosen one *)
                  max_int, zeros :: res
                else
                  let rec loop res z =
                    if z <= 0 then res else
                    loop (Word 0 :: res) (z - 1) in
                  max_zeros, loop res z
            | x -> max_zeros, x :: res
          ) (max_zeros, []) lst in
        string_of_words lst
      else
        let w = word n shf in
        let shf = shf - 16 in
        if w = 0 then
          let zeros = zeros + 1 in
          loop lst (max max_zeros zeros) zeros shf
        else
          let lst = if zeros > 0 then Zeros zeros :: lst else lst in
          let lst = Word w :: lst in
          loop lst max_zeros 0 shf
    in
    loop [] 0 0 112

(*$= to_string & ~printer:(fun x -> x)
  "2001:db8::ff00:42:8329" \
    (to_string (Stdint.Uint128.of_string \
      "0x20010DB8000000000000FF0000428329"))
  "::2001" \
    (to_string (Stdint.Uint128.of_string "0x2001"))
  "2001::" \
    (to_string (Stdint.Uint128.of_string \
      "0x20010000000000000000000000000000"))
  "1:23:456::7:0:8" \
    (to_string (Stdint.Uint128.of_string \
      "0x00010023045600000000000700000008"))
 *)

let print oc n = String.print oc (to_string n)

module Parser =
struct
  open RamenParsing

  let p m =
    let m = "IPv6" :: m in
    let group =
      let max_num = Num.of_int 65535 in
      fun m ->
        let m = "IPv6 group" :: m in
        (check hexadecimal_digit -+ (* to avoid empty strings *)
         non_decimal_integer 16 (return ()) hexadecimal_digit >>: fun n ->
          if Num.gt_num n max_num then
            raise (Reject "IPv6 group too large") ;
          Num.to_int n) m in
    let sep = char ':' in
    let ipv6_of_list lst =
      let rec loop n shf = function
      | [] -> assert (shf < 0) ; n
      | g :: rest ->
        let n = Uint128.(logor n (shift_left (of_int g) shf)) in
        loop n (shf - 16) rest
      in
      loop Uint128.zero 112 lst
    in
    (repeat ~min:8 ~max:8 ~sep group |||
     (repeat ~max:7 ~sep group +- string "::" ++
      repeat ~max:7 ~sep group >>: fun (bef, aft) ->
        let len = List.length bef + List.length aft in
        if len > 8 then
          raise (Reject "IPv6 address too large") ;
        bef @ List.make (8 - len) 0 @ aft) >>: ipv6_of_list
    ) m
end

let of_string = RamenParsing.of_string_exn Parser.p

(*$= of_string & ~printer:(BatIO.to_string print)
   (Stdint.Uint128.of_string "0x20010DB8000000000000FF0000428329") \
     (of_string "2001:db8::ff00:42:8329")
 *)

module Cidr =
struct
  (*$< Cidr *)
  type t = uint128 * int [@@ppp PPP_OCaml]

  let netmask_of_len len =
    let shf = 128 - len in
    Uint128.(shift_left max_int shf)

  let and_to_len len net =
    Uint128.(logand (netmask_of_len len) net)

  let or_to_len len net =
    let shf = 128 - len in
    Uint128.(logor net ((shift_left one shf) - one))

  let first (net, len) = and_to_len len net
  let last (net, len) = or_to_len len net
  let is_in ip cidr =
    Uint128.compare ip (first cidr) >= 0 &&
    Uint128.compare ip (last cidr) <= 0

  let to_string (net, len) =
    let net = and_to_len len net in
    to_string net ^"/"^ string_of_int len

  (*$= to_string & ~printer:(fun x -> x)
     "2001:db8::ff00:42:8300/120" \
       (to_string (Stdint.Uint128.of_string \
         "0x20010DB8000000000000FF0000428300", 120))
   *)

  let print oc t = String.print oc (to_string t)

  module Parser =
  struct
    open RamenParsing

    let small_int =
      decimal_integer_range ~min:0 ~max:127 "CIDRv6 mask"

    let p m =
      let m = "CIDRv6" :: m in
      (Parser.p +- char '/' ++ small_int >>: fun (net, len) ->
       and_to_len len net, len) m
  end

  let of_string = RamenParsing.of_string_exn Parser.p

  (*$= of_string & ~printer:(BatIO.to_string print)
     (Stdint.Uint128.of_string "0x20010DB8000000000000FF0000428300", 120) \
       (of_string "2001:db8::ff00:42:8300/120")
     (Stdint.Uint128.of_string "0x20010DB8000000000000FF0000428300", 120) \
       (of_string "2001:db8::ff00:42:8342/120")
   *)
  (*$>*)
end
