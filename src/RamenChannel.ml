(* A channel is an identifier that is used to segregate the various replays
 * that are going on. In theory, tuples from different channels do never
 * see each others. *)
open Batteries
open RamenHelpers
open Stdint

type t = Uint16.t

let live = Uint16.zero

let print oc c =
  Uint16.to_string c |>
  String.print oc

(* TODO! *)
let make () =
  Random.int 0xFFFF |>
  Uint16.of_int

let of_string = Uint16.of_string
let to_string = Uint16.to_string

let of_int n =
  assert (n <= 0xFFFF) ;
  Uint16.of_int n
