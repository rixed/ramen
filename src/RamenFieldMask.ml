(* The bits that are required on workers. See RamenFieldMaskLib for the real
 * thing. *)
open Batteries
open RamenConsts

type mask =
  | Skip (* Skip this field and any subfields *)
  | Copy (* Copy this field and any subfields *)
  | Null (* This fields contains only null values *)
  (* Copy this constructed field but only the subfields specified by the given
   * submask. [Rec [Copy;Copy;...etc]] is the same as [Copy]: *)
  | Rec of fieldmask
and fieldmask = mask array

let rec print_mask oc = function
  | Skip -> String.print oc "_"
  | Copy -> String.print oc "X"
  | Null -> String.print oc "N"
  | Rec fm -> Printf.fprintf oc "(%a)" print fm

and print oc =
  Array.print ~first:"" ~last:"" ~sep:"" print_mask oc

(* Note: recursive strings are compatible with the older format as long as
 * there is no recursion. *)
let to_string = IO.to_string print

let of_string s =
  (* returns both the fieldmask and the next offset in the string: *)
  let rec of_sub prev i =
    let fm_of_prev () = Array.of_list (List.rev prev) in
    if i >= String.length s then
      fm_of_prev (), i
    else
      match s.[i] with
      | '_' -> of_sub (Skip :: prev) (i + 1)
      | 'X' -> of_sub (Copy :: prev) (i + 1)
      | 'N' -> of_sub (Null :: prev) (i + 1)
      | '(' ->
          let fm, j = of_sub [] (i + 1) in
          of_sub (Rec fm :: prev) j
      | ')' -> fm_of_prev (), i + 1
      | _ ->
          Printf.sprintf "Invalid fieldmask string: %S" s |>
          failwith
  in
  let fm, i = of_sub [] 0 in
  if i <> String.length s then
    Printf.sprintf "Junk after character %d in fieldmask %S" i s |>
    failwith ;
  fm

let fieldmask_ppp_ocaml =
  PPP.(>>:)
    PPP_OCaml.string
    (to_string, of_string)

(*$< Batteries *)

(*$= of_string & ~printer:identity
  "" (of_string "" |> to_string)
  "__" (of_string "__" |> to_string)
  "_X" (of_string "_X" |> to_string)
  "X_" (of_string "X_" |> to_string)
  "X(_XX)" (of_string "X(_XX)" |> to_string)
  "N" (of_string "N" |> to_string)
 *)

(* FIXME: when the output type is a single value, just [| Copy |]: *)
let all_fields = Array.make num_all_fields Copy

(*$>*)
