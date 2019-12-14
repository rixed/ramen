(* Describes how long function output should be available (either by storing
 * it directly, or alternatively making the functions it depends on
 * available). *)
open Batteries
open RamenHelpers

type t =
  { duration : float ;
    (* How frequently we intend to query it, in Hertz (TODO: we could
     * approximate a better value if absent): *)
    period : float [@ppp_default 600.] }

(* For the ramen language printer, see RamenProgram.print_retention *)
let print oc r =
  Printf.fprintf oc "duration:%a, period:%a"
    print_as_duration r.duration
    print_as_duration r.period
