(* Event time.
 *)
open Batteries

(* Event time info *)

(* The fields used in event time description can either come from the output
 * tuple or from parameters: *)
type field_source = OutputField | Parameter [@@ppp PPP_OCaml]

type field = string * field_source ref * float [@@ppp PPP_OCaml]

let string_of_field (n, _, s) =
  let string_of_scale f = "*"^ string_of_float f in
  n ^ string_of_scale s

type event_start = field [@@ppp PPP_OCaml]

type event_duration =
  | DurationConst of float (* seconds *)
  | DurationField of field
  | StopField of field
  [@@ppp PPP_OCaml]

type t = (event_start * event_duration) [@@ppp PPP_OCaml]

let print oc (start_field, duration) =
  Printf.fprintf oc "EVENT STARTING AT %s AND %s"
    (string_of_field start_field)
    (match duration with
     (* FIXME: uses RamenExpr.print_duration: *)
     | DurationConst f -> "DURATION "^ string_of_float f
     | DurationField f -> "DURATION "^ string_of_field f
     | StopField f -> "STOPPING AT "^ string_of_field f)
