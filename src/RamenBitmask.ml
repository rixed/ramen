(* Simple bitmask *)
open Batteries

type t = { num_bits : int ; bits : Bytes.t }

let make num_bits =
  { num_bits ;
    bits = Bytes.make ((num_bits + 7) / 8) '\000' }

let bit_loc_of_bit b =
  b lsr 3, b land 7

let get t b =
  let idx, b_off = bit_loc_of_bit b in
  let n = Bytes.get t.bits idx |> Char.code in
  (n lsr b_off) land 1 = 1

(* returns if the bit was already set *)
let getset t b =
  let idx, b_off = bit_loc_of_bit b in
  let n = Bytes.get t.bits idx |> Char.code in
  let mask = 1 lsl b_off in
  if n land mask = 0 then (
    Bytes.set t.bits idx (Char.chr (n lor (1 lsl b_off))) ;
    false
  ) else true

let set t b =
  ignore (getset t b)

let to_bools t = Array.init t.num_bits (get t)
