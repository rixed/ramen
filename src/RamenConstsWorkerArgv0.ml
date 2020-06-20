let full_worker = "ramen worker:"
(* The top half of a worker executes the fast-where and then sends the
 * filtered tuples into the children. In the future, when we can transmit
 * aggregation state, it could go as far as aggregating.
 * The tunneld service then enqueue the tuples in the proper worker input
 * queue. *)
let top_half = "ramen worker (top-half):"
let replay = "ramen replay:"
