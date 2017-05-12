(* This is the engine implementation that builds a representation (as a
 * directed graph) of the configuration.  No need for speed here.
 *
 * Executing the configuration implemented by this engine will return the
 * graph of the nodes.
 *)

open Batteries
open Model

module Impl :
  Engine.S with type ('i, 'k) result = ?from_node:Node.t -> Graph.t -> Graph.t =
struct
  type ('i, 'k) result = ?from_node:Node.t -> Graph.t -> Graph.t

  (* Add myself and then all subnodes *)
  let add_sub ?from_node ?id g ks name =
    let id = Option.get id in
    let n = Node.make name id in
    let g = Graph.add_node g ?from_node n in
    List.fold_left (fun g k -> k ?from_node:(Some n) g) g ks

  let replicate ?(name="replicate") ?id ks ?from_node g =
    add_sub ?from_node ?id g ks name

  let filter ?(name="filter") ?id ~by ks ?from_node g =
    ignore by ;
    add_sub ?from_node ?id g ks name

  let on_change ?(name="on-change") ?id ks ?from_node g =
    add_sub ?from_node ?id g ks name

  let aggregate ?(name="aggregate") ?id ~key_of_event ~make_aggregate ~aggregate
                ?is_complete ?timeout_sec ?timeout_events
                ks ?from_node g =
    ignore key_of_event ; ignore make_aggregate ; ignore aggregate ;
    ignore is_complete ; ignore timeout_sec ; ignore timeout_events ;
    add_sub ?from_node ?id g ks name

  let sliding_window ?(name="sliding-window") ?id ~cmp ~is_complete
                     ks ?from_node g =
    ignore cmp ; ignore is_complete ;
    add_sub ?from_node ?id g ks name

  let all ?(name="all") ?id ~cond ks ?from_node g =
    ignore cond ;
    add_sub ?from_node ?id g ks name

  let alert ?(name="alert") ?id ~team ~title ~text () ?from_node g =
    ignore team ; ignore title ; ignore text ;
    add_sub ?from_node ?id g [] name

  let save ?(name="save") ?id ~retention () ?from_node g =
    ignore retention ;
    add_sub ?from_node ?id g [] name
end
