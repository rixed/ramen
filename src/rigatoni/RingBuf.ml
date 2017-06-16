open Stdint

type t (* abstract, represents a ring buffer mmapped file *)
type tx (* abstract, represents an ongoing (de)queueing operation *)

external create : string -> int -> t = "wrap_ringbuf_create"
external load : string -> t = "wrap_ringbuf_load"
external enqueue_alloc : t -> int -> tx = "wrap_ringbuf_enqueue_alloc"
external enqueue_commit : tx -> unit = "wrap_ringbuf_enqueue_commit"
external enqueue : t -> bytes -> int -> unit = "wrap_ringbuf_enqueue"
external dequeue_alloc : t -> tx = "wrap_ringbuf_dequeue_alloc"
external dequeue_commit : tx -> unit = "wrap_ringbuf_dequeue_commit"
external dequeue : t -> bytes = "wrap_ringbuf_dequeue"

external write_float : tx -> int -> float -> unit = "write_boxed_64"
external write_string : tx -> int -> string -> unit = "write_boxed_str"
external write_u8 : tx -> int -> Uint8.t -> unit = "write_boxed_8"
external write_i8 : tx -> int -> Int8.t -> unit = "write_boxed_8"
external write_u16 : tx -> int -> Uint16.t -> unit = "write_boxed_16"
external write_i16 : tx -> int -> Int16.t -> unit = "write_boxed_16"
external write_u32 : tx -> int -> Uint32.t -> unit = "write_boxed_32"
external write_i32 : tx -> int -> Int32.t -> unit = "write_boxed_32"
external write_u64 : tx -> int -> Uint64.t -> unit = "write_boxed_64"
external write_i64 : tx -> int -> Int64.t -> unit = "write_boxed_64"
external write_u128 : tx -> int -> Uint128.t -> unit = "write_boxed_128"
external write_i128 : tx -> int -> Int128.t -> unit = "write_boxed_128"
(* Everything passed as an int and which occupancy is <= word *)
external write_bool : tx -> int -> bool -> unit = "write_word"

external read_float : tx -> int -> float = "read_float"
external read_string : tx -> int -> string = "read_str"
external read_u8 : tx -> int -> Uint8.t = "read_uint8"
external read_i8 : tx -> int -> Int8.t = "read_int8"
external read_u16 : tx -> int -> Uint16.t = "read_uint16"
external read_i16 : tx -> int -> Int16.t = "read_int16"
external read_u32 : tx -> int -> Uint32.t = "read_uint32"
external read_i32 : tx -> int -> Int32.t = "read_int32"
external read_u64 : tx -> int -> Uint64.t = "read_uint64"
external read_i64 : tx -> int -> Int64.t = "read_int64"
external read_u128 : tx -> int -> Uint128.t = "read_uint128"
external read_i128 : tx -> int -> Int128.t = "read_int128"
external read_bool : tx -> int -> bool = "read_word"
external read_word : tx -> int -> int = "read_word"

let set_bit _addr _bit = assert false (* TODO *)
let get_bit _addr = assert false (* TODO *)

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
 * parameters (including function expressions). The output format is inferred
 * and need not be given. *)
