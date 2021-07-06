open Batteries
open Stdint

open RamenHelpers
open RamenLog

include Raql_variable.DessserGen

let to_string = function
  | Unknown -> "unknown"
  | In -> "in"
  | GroupState -> "group_state"
  | GlobalState -> "global_state"
  | OutPrevious -> "out_previous"
  | Out -> "out"
  | SortFirst -> "sort_first"
  | SortSmallest -> "sort_smallest"
  | SortGreatest -> "sort_greatest"
  | Param -> "param"
  | Env -> "env"
  | Record -> "record"
  | GlobalVar -> "global"

let print oc p =
  Printf.fprintf oc "%s" (to_string p)

let parse m =
  let open RamenParsing in
  let m = "variable name" :: m in
  let w s = ParseUsual.string ~case_sensitive:false s +-
            nay legit_identifier_chars in
  (
    (w "unknown" >>: fun () -> Unknown) |<|
    (w "in" >>: fun () -> In) |<|
    (w "group_state" >>: fun () -> GroupState) |<|
    (w "global_state" >>: fun () -> GlobalState) |<|
    (w "out_previous" >>: fun () -> OutPrevious) |<|
    (w "previous" >>: fun () -> OutPrevious) |<|
    (w "out" >>: fun () -> Out) |<|
    (w "sort_first" >>: fun () -> SortFirst) |<|
    (w "sort_smallest" >>: fun () -> SortSmallest) |<|
    (w "sort_greatest" >>: fun () -> SortGreatest) |<|
    (w "smallest" >>: fun () -> SortSmallest) |<|
    (w "greatest" >>: fun () -> SortGreatest) |<|
    (w "param" >>: fun () -> Param) |<|
    (w "env" >>: fun () -> Env) |<|
    (* Not for public consumption: *)
    (w "record" >>: fun () -> Record) |<|
    (w "global" >>: fun () -> GlobalVar)
  ) m

(* Variables that has the fields of this func input type *)
let has_type_input = function
  | In | SortFirst | SortSmallest | SortGreatest -> true
  | _ -> false

(* Variables that has the fields of this func output type *)
let has_type_output = function
  | OutPrevious | Out -> true
  | _ -> false
