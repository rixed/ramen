(* Depends on Ipv4, Ipv6 and friends.
 * Used by CodeGenLib and Ramen. *)
open Stdint

(* Converters from string to values *)

let float_of_string = Pervasives.float_of_string
let string_of_string x = x
let bool_of_string = function
  | "true" | "TRUE" | "t" | "T" | "y" | "Y" | "on" | "ON" | "#t" | "1" -> true
  | _ -> false
let u8_of_string = Uint8.of_string
let u16_of_string = Uint16.of_string
let u32_of_string = Uint32.of_string
let u64_of_string = Uint64.of_string
let u128_of_string = Uint128.of_string
let i8_of_string = Int8.of_string
let i16_of_string = Int16.of_string
let i32_of_string = Int32.of_string
let i64_of_string = Int64.of_string
let i128_of_string = Int128.of_string
let null_of_string = ()
let eth_of_string s = Uint48.of_string ("0x"^ RamenHelpers.string_remove ':' s)
let ip4_of_string = RamenIpv4.of_string
let ip6_of_string = RamenIpv6.of_string
let cidr4_of_string = RamenIpv4.Cidr.of_string
let cidr6_of_string = RamenIpv6.Cidr.of_string
