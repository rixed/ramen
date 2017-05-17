(* The type of a configuration module. Actually a functor taking an
 * implementation and returning a module with a runnable configuration, for
 * some definition of "runnable" depending on the applied implementation. *)
module type MAKER = functor (I : Engine.S) ->
sig
  type input
  val configuration : (input, unit) I.result
end

(* Helper to go from a PPP to a result type, useful for implementing
 * input_of_string: *)

let input_of_string_of_ppp ppp str =
  match PPP.of_string ppp str 0 with
  | Some (e, l) ->
    if l < String.length str then
      Error (Printf.sprintf "Garbage at end of line, starting at offset %d" l)
    else
      Ok e
  | None ->
    Error "Cannot parse input"

(* Registry of all loaded configurations. this is unlikely we will ever handle
 * more than one but it doesn't hurt neither: *)
let registered_configs : (string, (module MAKER)) Hashtbl.t = Hashtbl.create 3

let register name m =
  Hashtbl.add registered_configs name m
