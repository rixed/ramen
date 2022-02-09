(* Make use of external library Dessser for code generation *)
open Batteries
open Dessser
open Stdint

open RamenConsts
open RamenHelpers
open RamenHelpersNoLog
open RamenLang
open RamenLog

module DT = DessserTypes
module T = RamenTypes

let print type_of_column oc filter =
  if array_is_empty filter then String.print oc "true" else
  Array.print ~first:"" ~sep:" AND " ~last:"" (fun oc w ->
    (* Get the proper right-type according to left-type and operator: *)
    let lft = type_of_column w.Simple_filter.DessserGen.lhs in
    let rft =
      if w.op = "in" || w.op = "not in" then
        DT.(optional (TArr lft))
      else lft in
    let v = RamenSerialization.value_of_string rft w.rhs in
    (* Turn 'in [x]' into '= x': *)
    let op, v =
      match w.op, v with
      | "in", (VVec [| x |] | VArr [| x |]) -> "=", x
      | "not in", (VVec [| x |] | VArr [| x |]) -> "<>", x
      | _ -> w.op, v in
    let s =
      Printf.sprintf2 "%s %s %a"
        (ramen_quote (w.lhs :> string))
        op
        T.print v in
    if lft.nullable then
      Printf.fprintf oc "COALESCE(%s, false)" s
    else
      String.print oc s
  ) oc filter
