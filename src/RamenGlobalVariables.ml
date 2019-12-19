(* Compile time global variables representation *)
open Batteries
open RamenHelpers
module N = RamenName
module T = RamenTypes

type t = { scope : scope ; name : N.field ; typ : T.t }

and scope = Program | Site | Global

let string_of_scope = function
  | Program -> "PROGRAM"
  | Site -> "SITE"
  | Global -> "GLOBAL"

let print oc g =
  Printf.fprintf oc "%a (scope %s)"
    N.field_print g.name
    (string_of_scope g.scope)

module MakeMap (Conf : sig type k type v val program_name : string end) =
struct
  let init () =
    let h : (Conf.k, Conf.v) Hashtbl.t = Hashtbl.create 10 in
    let get k = Hashtbl.find h k
    and set k v = Hashtbl.add h k v in
    get, set
end
