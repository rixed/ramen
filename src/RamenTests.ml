open Batteries
open Lwt
open Helpers

type tuple_spec = (string, string) Hashtbl.t [@@ppp PPP_JSON]

type input_spec =
  { pause : float [@ppp_default 0.] ;
    operation : string ;
    tuple : tuple_spec } [@@ppp PPP_JSON]

type output_spec =
  { operation : string ;
    tuples : tuple_spec array ;
    notifications : string array } [@@ppp PPP_JSON]

type test_spec =
  { inputs : input_spec array ;
    outputs : output_spec array } [@@ppp PPP_JSON]

(* Return a random unique test identifier *)
let get_id () =
  Random.full_range_int () |>
  packed_string_of_int |>
  Base64.str_encode

let reprogram _test_id funcs = funcs (* TODO *)

(* For a change we do it directly rather than sending everything to some server: *)
let run conf tests =
  Printf.printf "conf=%s, tests=%a\n" conf
    (List.print String.print) tests ;
  return_unit
