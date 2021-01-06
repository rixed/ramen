(* The bits that are required on workers. See RamenFieldMaskLib for the real
 * thing. *)
open Batteries
open RamenConsts
open DessserMasks

let print = print_mask

(* Note: recursive strings are compatible with the older format as long as
 * there is no recursion. *)
let to_string = string_of_mask

(* Quicker than DessserMasks.Parser but supports only the mask-actions used
 * within ramen (TODO: do use DessserMask.Parser): *)
let of_string s =
  let to_array = Array.of_list % List.rev in
  (* returns both the fieldmask and the next offset in the string: *)
  let rec of_sub prev i =
    if i >= String.length s then
      to_array prev, i
    else
      match s.[i] with
      | '_' -> of_sub (Skip :: prev) (i + 1)
      | 'X' -> of_sub (Copy :: prev) (i + 1)
      | '(' ->
          let fm, j = of_sub [] (i + 1) in
          of_sub (Recurse fm :: prev) j
      | ')' ->
          to_array prev, i + 1
      | _ ->
          Printf.sprintf "Invalid fieldmask string: %S" s |>
          failwith
  in
  let fm, i = of_sub [] 0 in
  if i <> String.length s then
    Printf.sprintf "Junk after character %d in fieldmask %S" i s |>
    failwith ;
  fm

(*$< Batteries *)

(*$= of_string & ~printer:identity
  "" (of_string "" |> to_string)
  "__" (of_string "__" |> to_string)
  "_X" (of_string "_X" |> to_string)
  "X_" (of_string "X_" |> to_string)
  "X(_XX)" (of_string "X(_XX)" |> to_string)
 *)

(* FIXME: when the output type is a single value, just [| Copy |]: *)
let all_fields = Array.make num_all_fields Copy

(*$>*)
