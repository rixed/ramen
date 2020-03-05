(* Either v4 or v6 *)
open Batteries
open Stdint
open RamenNullable
open RamenHelpersNoLog

type t =
  V4 of uint32 | V6 of uint128
  [@@ppp PPP_OCaml]

let to_string = function
  | V4 n -> RamenIpv4.to_string n
  | V6 n -> RamenIpv6.to_string n

let print oc n = String.print oc (to_string n)

let of_string s o =
  try
    let ip, o = RamenIpv4.of_string s o in
    V4 ip, o
  with _ ->
    (try
      let ip, o = RamenIpv6.of_string s o in
      V6 ip, o
    with _ ->
      Printf.sprintf "%S is neither a v4 or v6 IP address"
        (abbrev 10 (String.sub s o (String.length s - o))) |>
      failwith)

let of_unix_addr s =
  fst (of_string (Unix.string_of_inet_addr s) 0)

module Cidr =
struct
  type t =
    V4 of RamenIpv4.Cidr.t | V6 of RamenIpv6.Cidr.t
    [@@ppp PPP_OCaml]

  let to_string = function
    | V4 n -> RamenIpv4.Cidr.to_string n
    | V6 n -> RamenIpv6.Cidr.to_string n

  let print oc t = String.print oc (to_string t)

  let of_string s o =
    try
      let cidr, o = RamenIpv4.Cidr.of_string s o in
      V4 cidr, o
    with _ ->
      let cidr, o = RamenIpv6.Cidr.of_string s o in
      V6 cidr, o
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

(* [ip] and [cidrs] can be nulls, but the individual cidr are not: *)
let is_in_list ip cidrs =
  match ip, cidrs with
  | Null, NotNull [||] -> NotNull false (* Whatever IP is not in the empty list *)
  | Null, _ -> Null  (* If list is unknown or not empty, then we don't know *)
  | _, Null -> Null
  | NotNull ip, NotNull cidrs ->
      NotNull (Array.exists (fun cidr -> is_in ip cidr) cidrs)

(* [ip] and [cidrs] can be nulls, and so can be each of the individual
 * cidrs: *)
let is_in_list_of_nullable ip cidrs =
  match ip, cidrs with
  | Null, NotNull [||] -> NotNull false (* Whatever IP is not in the empty list *)
  | Null, _ -> Null  (* If list is unknown or not empty, then we don't know *)
  | _, Null -> Null
  | NotNull ip, NotNull cidrs ->
      (* Look for ip in the cidrs. If we cannot find the ip but found some
       * null then we must return that we don't know. *)
      let rec loop if_not_found i =
        if i >= Array.length cidrs then
          if_not_found
        else
          match cidrs.(i) with
          | NotNull cidr  ->
              if is_in ip cidr then NotNull true
              else loop if_not_found (i + 1)
          | Null ->
              loop Null (i + 1) in
      loop (NotNull false) 0
