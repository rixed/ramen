(* Shhh, look away! *)
open Batteries

type t = int64 * int64 [@@ppp PPP_OCaml]

let rand max = 0L, Random.int64 max

let to_string (hi, lo) = Int64.to_string hi ^ Int64.to_string lo

