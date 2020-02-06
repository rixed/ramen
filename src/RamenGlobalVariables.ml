(* Compile time global variables representation.
 *
 * Note on LMDB usage:
 *
 * - We use a single environment with one DB per global variable (as opposed
 *   to, say, one environment per global variable), so that different
 *   read/write accesses to different global variables happening in a single
 *   message can be isolated from those happening for different tuples in
 *   different workers, even if those global variables have different scope.
 *   Database names must therefore be unique regardless of their scope.
 *
 * - Because we can not know in advance if map keys will be added or set we
 *   cannot know at database creation whether the database should be created
 *   with the multikey flag or not. Therefore, all adds are treated as sets.
 *
 * -  *)

open Batteries
open RamenHelpers
open RamenConsts
module N = RamenName
module T = RamenTypes
module Files = RamenFiles

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

(* Notice that although the "program" scope is called Program it is actually
 * a full src_path, as for now the scope_id is an immediate string in the
 * compiled program and can therefore not depend on the program name.
 * FIXME: make that compiled scope_id a function of the program name. *)
let scope_id t src_path =
  match t.scope with
  | Program -> (src_path : N.src_path :> string)
  | Site -> ""
  | Global -> todo "Globals of global scope"
