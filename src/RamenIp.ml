(* Either v4 or v6 *)
open Batteries
open Stdint

type t =
  V4 of uint32 | V6 of uint128
  [@@ppp PPP_OCaml]

let to_string = function
  | V4 n -> RamenIpv4.to_string n
  | V6 n -> RamenIpv6.to_string n

let print oc n = String.print oc (to_string n)

let of_string n =
  try V4 (RamenIpv4.of_string n)
  with _ -> V6 (RamenIpv6.of_string n)

let of_unix_addr = of_string % Unix.string_of_inet_addr

module Cidr =
struct
  type t =
    V4 of RamenIpv4.Cidr.t | V6 of RamenIpv6.Cidr.t
    [@@ppp PPP_OCaml]

  let to_string = function
    | V4 n -> RamenIpv4.Cidr.to_string n
    | V6 n -> RamenIpv6.Cidr.to_string n

  let print oc t = String.print oc (to_string t)

  let of_string n =
    try V4 (RamenIpv4.Cidr.of_string n)
    with _ -> V6 (RamenIpv6.Cidr.of_string n)
end

let first = function
  | Cidr.V4 n -> V4 (RamenIpv4.Cidr.first n)
  | Cidr.V6 n -> V6 (RamenIpv6.Cidr.first n)

let last = function
  | Cidr.V4 n -> V4 (RamenIpv4.Cidr.last n)
  | Cidr.V6 n -> V6 (RamenIpv6.Cidr.last n)

let is_in ip cidr =
  match ip, cidr with
  | V4 ip, Cidr.V4 cidr -> RamenIpv4.Cidr.is_in ip cidr
  | V6 ip, Cidr.V6 cidr -> RamenIpv6.Cidr.is_in ip cidr
  | _ -> false
