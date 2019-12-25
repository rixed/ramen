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
    (
      dismiss_error_if (parsed_fewer_than 6) (
        repeat ~min:8 ~max:8 ~sep group |||
        (
          repeat ~max:7 ~sep group +- string "::" ++
          repeat ~max:7 ~sep group >>: fun (bef, aft) ->
            let len = List.length bef + List.length aft in
            if len > 8 then
              raise (Reject "IPv6 address too large") ;
            bef @ List.make (8 - len) 0 @ aft
        ) >>: ipv6_of_list
      )
    ) m
end

let of_string s o =
  let is_double_colon o had_zeros =
    let r = s.[o] = ':' && o < String.length s - 1 && s.[o+1] = ':' in
    if r && had_zeros then
      failwith "Cannot have several '::' in IPv6" ;
    r in
  let is_colon o =
    o < String.length s && s.[o] = ':' in
  let rec loop had_zeros l prevs o =
    if l >= 8 then had_zeros, l, prevs, o else
    if o >= String.length s then had_zeros, l, prevs, o else
    let i, o = unsigned_of_hexstring s o in
    let l, prevs = l + 1, Some i :: prevs in
    if o >= String.length s then had_zeros, l, prevs, o else
    if is_double_colon o had_zeros then (
      loop true l (None :: prevs) (o + 2)
    ) else (
      if is_colon o then loop had_zeros l prevs (o + 1)
      else had_zeros, l, prevs, o
    ) in
  let had_zeros, l, ns, o =
    if is_double_colon o false then
      loop true 0 [ None ] (o + 2)
    else
      loop false 0 [] o
    in
  (* Check we either have 8 components or we had the double colon: *)
  if (not had_zeros && l <> 8) || l > (if had_zeros then 7 else 8) then
    Printf.sprintf "Invalid IPv6 address at %d" o |>
    failwith ;
  (* Complete the None: *)
  let ip =
    let rec loop ls num_zeros ip = function
    | [] ->
        assert (num_zeros = 0) ;
        ip
    | None :: rest as lst ->
        if num_zeros > 0 then
          loop (ls + 16) (num_zeros - 1) ip lst
        else
          loop ls 0 ip rest
    | Some n :: rest ->
        let ip = Uint128.(shift_left (of_int n) ls |> add ip) in
        loop (ls + 16) num_zeros ip rest in
    loop 0 (8 - l) Uint128.zero ns in
  ip, o

(*$= of_string & ~printer:(BatIO.to_string (BatTuple.Tuple2.print print BatInt.print))
   (Stdint.Uint128.of_string "0x20010DB8000000000000FF0000428329", 22) \
     (of_string "2001:db8::ff00:42:8329" 0)
   (Stdint.Uint128.of_string "0xffffffffffffffffffffffffffffffff", 39) \
     (of_string "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff" 0)
   (Stdint.Uint128.of_string "0x00010000000000000000000000000000", 3) \
     (of_string "1::" 0)
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
      (
        Parser.p +- char '/' ++ small_int >>: fun (net, len) ->
        and_to_len len net, len
      ) m
  end

  let of_string s o =
    let (net, len), o = RamenIpv4.cidr_of_string of_string 128 s o in
    (and_to_len len net, len), o

  (*$= of_string & ~printer:(BatIO.to_string (BatTuple.Tuple2.print print BatInt.print))
     ((Stdint.Uint128.of_string "0x20010DB8000000000000FF0000428300", 120), 26) \
       (of_string "2001:db8::ff00:42:8300/120" 0)
     ((Stdint.Uint128.of_string "0x20010DB8000000000000FF0000428300", 120), 26) \
       (of_string "2001:db8::ff00:42:8342/120" 0)
  *)

  (*$>*)
end
