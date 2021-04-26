(* The bits that are required on workers. See RamenFieldMaskLib for the real
 * thing. *)
open Batteries
open RamenConsts
open DessserMasks

let print = print_mask

(* Note: recursive strings are compatible with the older format as long as
 * there is no recursion. *)
let to_string = string_of_mask

let of_string s = DessserMasks.Parser.mask_of_string s

(*$< Batteries *)

(*$= of_string & ~printer:identity
  "_" (of_string "_" |> to_string)
  "_" (of_string "(__)" |> to_string)
  "(_X)" (of_string "(_X)" |> to_string)
  "(X_)" (of_string "(X_)" |> to_string)
  "(X(_XX))" (of_string "(X(_XX))" |> to_string)
 *)

(* FIXME: when the output type is a single value, just [| Copy |]: *)
let all_fields = Recurse (Array.make num_all_fields Copy)

(*$>*)
