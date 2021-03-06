(* Describes how long function output should be available (either by storing
 * it directly, or alternatively making the functions it depends on
 * available). *)
open Batteries
open RamenHelpers
open RamenHelpersNoLog
module E = RamenExpr
module N = RamenName

include Retention.DessserGen

(* For the ramen language printer, see RamenProgram.print_retention *)
let print oc r =
  Printf.fprintf oc "duration:%a, period:%a"
    (E.print false) r.duration
    print_as_duration r.period

(* Helper: fold over all "expressions" *)
let fold_expr u f = function
  | Some { duration ; _ } -> f u duration
  | _ -> u

(* Helper: return the "set" of used parameters: *)
let used_parameters = function
  | Some { duration = E.{ text = Stateless (SL2 (Get, n, _)) ; _ } ; _ } ->
      let p = E.string_of_const n |> option_get "retention" __LOC__ in
      N.SetOfFields.singleton (N.field p)
  | _ ->
      N.SetOfFields.empty
