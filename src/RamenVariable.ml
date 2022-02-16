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
  | GlobalLastOut -> "global_last_out"
  | LocalLastOut -> "local_last_out"
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
    (w "global_last_out" >>: fun () -> GlobalLastOut) |<|
    (w "global_last" >>: fun () -> GlobalLastOut) |<|
    (w "local_last_out" >>: fun () -> LocalLastOut) |<|
    (w "local_last" >>: fun () -> LocalLastOut) |<|
    (w "previous" >>: fun () -> LocalLastOut) |<| (* Historic keyword *)
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
  | GlobalLastOut | LocalLastOut | Out -> true
  | _ -> false
