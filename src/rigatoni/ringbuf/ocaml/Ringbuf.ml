type t (* abstract, implemented in wrap_ringbuf.c *)

external create : string -> int -> t = "wrap_ringbuf_create"
external load : string -> t = "wrap_ringbuf_load"
external enqueue : t -> bytes -> int -> unit = "wrap_ringbuf_enqueue"
external dequeue : t -> bytes = "wrap_ringbuf_dequeue"

(* Note: each primitive operation is generic but for a few things:
 *
 * - how to bind values from the incoming tuple to a local variables (a tuple)
 *
 * - functions returning values from the tuple (for instance, the function to
 * extract a key of the tuple)
 *
 * - function to stringify a tuple to write it into an output ring buffer (in
 * field name order).
 *
 * We need to generate the OCaml code for all of these (all unserializers and
 * all configuration functions). The we could write the final program that
 * would implement either a single operation or several.
 *
 * Notice that if we encode two successive operations in the same program then
 * we can save the ringbuffer and serialization between them, in addition to
 * the additional inlining opportunities. This require those two operations to
 * be implemented in the same language and that we won't need to restart one
 * without restarting the other.
 *
 * The simple operations (projection, column renaming, extensions) are good
 * candidates for this bundling.
 *
 * The API to this code generator is composed of: operation name, input format,
 * parameters (including function expressions). The output format is given by
 * this and need not be given. *)
