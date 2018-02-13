open Batteries
open Lwt
open Helpers
open RamenLog
module C = RamenConf
module N = RamenConf.Func
module L = RamenConf.Program

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

(* Change the FROMs from program_name to new_program_name,
 * warn if time-related functions are used. *)
let reprogram old_program_name new_program_name func =
  let open Lang in
  let rename s =
    if String.starts_with s old_program_name then
      let len = String.length old_program_name in
      if String.length s = len then new_program_name
      else if s.[len] = '/' then
        new_program_name ^ String.lchop ~n:len s
      else s
    else s
  in
  Operation.iter_expr (function
    | Expr.(StatelessFun (_, (Age _ | Now))) ->
        !logger.warning "Test uses time related functions"
    | _ -> ()) func.Program.operation ;
  match func.operation with
  | Operation.Yield { every ; _ } ->
      if every > 0. then !logger.warning "Test uses YIELD EVERY" ;
      func
  | Operation.ReadCSVFile _ | Operation.ListenFor _ -> func
  | Operation.Aggregate ({ from ; _ } as r) ->
      { func with operation =
          Operation.Aggregate { r with from = List.map rename from } }

(* For a change we do it directly rather than sending everything to some server: *)
let run conf tests =
  Printf.printf "conf=%s, tests=%a\n" conf
    (List.print String.print) tests ;
  return_unit
