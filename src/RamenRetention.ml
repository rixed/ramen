(* Describes how long function output should be available (either by storing
 * it directly, or alternatively making the functions it depends on
 * available). *)
open Batteries
open RamenHelpersNoLog
module N = RamenName

type duration = Const of float | Param of N.field

let print_duration oc = function
  | Const d -> print_as_duration oc d
  | Param n -> "param."^ ramen_quote (n :> string) |> String.print oc

type t =
  { duration : duration ;
    (* How frequently we intend to query it, in Hertz (TODO: we could
     * approximate a better value if absent): *)
    period : float }

(* For the ramen language printer, see RamenProgram.print_retention *)
let print oc r =
  Printf.fprintf oc "duration:%a, period:%a"
    print_duration r.duration
    print_as_duration r.period

(* Helper: return the "set" of used parameters: *)
let used_parameters = function
  | None
  | Some { duration = Const _ ; _ } -> Set.empty
  | Some { duration = Param p ; _ } -> Set.singleton p
