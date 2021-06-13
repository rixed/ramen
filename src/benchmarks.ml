(* Slow functions come here to die *)
open Batteries
open Benchmark
open Stdint

let uuid_of_u128_old s =
  let buffer = Buffer.create 36 in
  let buffer_without_minus = Buffer.create 32 in
  let hex_str = Uint128.to_string_hex s in
  let number_of_zero_to_add = 32 - (String.length hex_str - 2) in
  for i = 1 to number_of_zero_to_add do
    Buffer.add_char buffer_without_minus '0'
  done ;
  Buffer.add_substring buffer_without_minus hex_str 2 (String.length hex_str - 2) ;
  let buffer_without_minus_str = Buffer.contents buffer_without_minus in
  Buffer.add_substring buffer buffer_without_minus_str 0 8 ;
  Buffer.add_char buffer '-' ;
  Buffer.add_substring buffer buffer_without_minus_str 8 4 ;
  Buffer.add_char buffer '-' ;
  Buffer.add_substring buffer buffer_without_minus_str 12 4 ;
  Buffer.add_char buffer '-' ;
  Buffer.add_substring buffer buffer_without_minus_str 16 4 ;
  Buffer.add_char buffer '-' ;
  Buffer.add_substring buffer buffer_without_minus_str 20 12 ;
  Buffer.contents buffer

let uuid_of_u128_simple n =
  let to_str n =
    let s = Uint64.to_string_hex n |> String.lchop ~n:2 in
    let num_zeros = 16 - String.length s in
    String.sub "0000000000000000" 0 num_zeros ^ s in
  let hi = to_str (Uint128.(to_uint64 (shift_right n 64)))
  and lo = to_str (Uint128.(to_uint64 n)) in
  String.sub hi 0 8 ^"-"^
  String.sub hi 8 4 ^"-"^
  String.sub hi 12 4 ^"-"^
  String.sub lo 0 4 ^"-"^
  String.sub lo 4 12

let uuid_of_u128_concat n =
  let to_str n =
    let s = Uint64.to_string_hex n |> String.lchop ~n:2 in
    let num_zeros = 16 - String.length s in
    String.sub "0000000000000000" 0 num_zeros ^ s in
  let hi = to_str (Uint128.(to_uint64 (shift_right n 64)))
  and lo = to_str (Uint128.(to_uint64 n)) in
  String.concat "-" [
    String.sub hi 0 8 ;
    String.sub hi 8 4 ;
    String.sub hi 12 4 ;
    String.sub lo 0 4 ;
    String.sub lo 4 12 ]

let uuid_of_u128_prealloc_zeros n =
  let zeros =
    [| "" ;
       "0" ;
       "00" ;
       "000" ;
       "0000" ;
       "00000" ;
       "000000" ;
       "0000000" ;
       "00000000" ;
       "000000000" ;
       "0000000000" ;
       "00000000000" ;
       "000000000000" ;
       "0000000000000" ;
       "00000000000000" ;
       "000000000000000" ;
       "0000000000000000" |] in
  let to_str n =
    let s = Uint64.to_string_hex n |> String.lchop ~n:2 in
    let num_zeros = 16 - String.length s in
    zeros.(num_zeros) ^ s in
  let hi = to_str (Uint128.(to_uint64 (shift_right n 64)))
  and lo = to_str (Uint128.(to_uint64 n)) in
  String.sub hi 0 8 ^"-"^
  String.sub hi 8 4 ^"-"^
  String.sub hi 12 4 ^"-"^
  String.sub lo 0 4 ^"-"^
  String.sub lo 4 12

external uuid_of_u128_small : Uint128.t -> string = "wrap_uuid_of_u128_small"

let () =
  Printf.printf "uuid_of_u128...\n" ;
  let n = Uint128.of_string "0x00112233445566778899aabbccddeeff" in
  let res = throughputN 2 [ "old", uuid_of_u128_old, n ;
                            "simple", uuid_of_u128_simple, n ;
                            "concat", uuid_of_u128_concat, n ;
                            "prealloc_0s", uuid_of_u128_prealloc_zeros, n ;
                            "C-small", uuid_of_u128_small, n ;
                            "C-big", CodeGenLib.uuid_of_u128, n ] in
  tabulate res
