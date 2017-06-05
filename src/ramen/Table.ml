(* We want to be able to visualize what's happening in the stream processor.
 * If we had only tuples of known type as events then we would be able to directly plot
 * any stream (picking the X, Y, type of plot, etc). Since we have arbitrary
 * events we need to expose possible scalars and factors. That's what this
 * series operation does: build a table, or the equivalent of an R frame, from
 * a sequence of events.
 *
 * We could force all ops to accept only tuples as input so that we could
 * easily store/display anything; the cons would be that we'd have to declare
 * those tuples each time we introduce a new type (for each aggregate, etc) or
 * implement a kind of type inference, and then we'd have to implement type
 * checking on our own to make sure the fields used are actually present in the
 * input. *)

type 'e field = FloatVal of ('e -> float)
              | IntVal of ('e -> int)
              | BoolVal of ('e -> bool)
              | FactVal of ('e -> string)

type vector = FloatVec of float array
            | IntVec of int array
            | BoolVec of bool array
            | FactVec of string array

type t = { name : string list ;
           node_id : int ;
           capacity : int ;
           vectors : (string * vector) array ;
           mutable oldest_idx : int ;
           mutable length : int }

let vector_of_spec n = function
  | FloatVal _ -> FloatVec (Array.make n 0.)
  | IntVal _ -> IntVec (Array.make n 0)
  | BoolVal _ -> BoolVec (Array.make n false)
  | FactVal _ -> FactVec (Array.make n "")

let all_tables : t list ref = ref []

let make =
  let series_ns = ["series"] in
  fun name node_id nb_values vectors ->
    let t = {
      name = name::series_ns ;
      node_id ; capacity = nb_values ;
      vectors =
        Array.map (fun (name, f) ->
          name, vector_of_spec nb_values f) vectors ;
      oldest_idx = 0 ; length = 0 } in
    all_tables := t :: !all_tables ;
    t
