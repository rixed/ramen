open Stdint
open DessserOCamlBackEndHelpers

external of_ipv4_exn : uint32 -> string = "wrap_country_of_ipv4"
external of_ipv6_exn : uint128 -> string = "wrap_country_of_ipv6"

let of_ipv4 ip =
  try Some (of_ipv4_exn ip)
  with Not_found -> None

let of_ipv6 ip =
  try Some (of_ipv6_exn ip)
  with Not_found -> None

let of_ip = function
  | RamenIp.V4 u -> of_ipv4 u
  | RamenIp.V6 u -> of_ipv6 u
