(* Shhh, look away! *)
open Batteries

type t = Int64.t * Int64.t

let rand max = 0L, Random.int64 max

let to_string (hi, lo) = Int64.to_string hi ^ Int64.to_string lo

let ppp = PPP_OCaml.(int64 ++ int64)
