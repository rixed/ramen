open Stdint

external of_ipv4 : uint32 -> string = "wrap_country_of_ipv4"
external of_ipv6 : uint128 -> string = "wrap_country_of_ipv6"

let of_ip = function
  | RamenIp.V4 u -> of_ipv4 u
  | RamenIp.V6 u -> of_ipv6 u
