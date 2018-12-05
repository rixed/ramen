(* A channel is an identifier that is used to segregate the various replays
 * that are going on. In theory, tuples from different channels do never
 * see each others. *)
open Batteries
open RamenHelpers

type t = int [@@ppp PPP_OCaml]
let live = 0

(* TODO! *)
let make _conf = Random.int 0xFFFF

let of_string = int_of_string
let to_string = string_of_int
