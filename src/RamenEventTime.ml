(* Event time.
 *)
open Batteries

(* Event time info *)

type event_start = string * float [@@ppp PPP_OCaml]
type event_duration = DurationConst of float (* seconds *)
                    | DurationField of (string * float)
                    | StopField of (string * float) [@@ppp PPP_OCaml]
type t = (event_start * event_duration) [@@ppp PPP_OCaml]

let print fmt (start_field, duration) =
  let string_of_scale f = "*"^ string_of_float f in
  Printf.fprintf fmt "EVENT STARTING AT %s%s AND %s"
    (fst start_field)
    (string_of_scale (snd start_field))
    (match duration with
     (* FIXME: uses RamenExpr.print_duration: *)
     | DurationConst f -> "DURATION "^ string_of_float f
     | DurationField (n, s) -> "DURATION "^ n ^ string_of_scale s
     | StopField (n, s) -> "STOPPING AT "^ n ^ string_of_scale s)
