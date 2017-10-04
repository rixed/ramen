open Stdint

type t (* abstract, represents a ring buffer mmapped file *)

external create_ : string -> int -> unit = "wrap_ringbuf_create"
let create fname =
  Helpers.mkdir_all ~is_file:true fname ;
  create_ fname

type stats = {
  capacity : int ; (* in words *)
  nb_entries : int ; (* in words *)
  mem_size : int ; (* the number of bytes that were mapped *)
  prod_head : int ;
  cons_head : int }

external load : string -> t = "wrap_ringbuf_load"
external unload : t -> unit = "wrap_ringbuf_unload"
external stats : t -> stats = "wrap_ringbuf_stats"

type tx (* abstract, represents an ongoing (de)queueing operation *)

external tx_size : tx -> int = "wrap_ringbuf_tx_size"
external enqueue_alloc : t -> int -> tx = "wrap_ringbuf_enqueue_alloc"
external enqueue_commit : tx -> unit = "wrap_ringbuf_enqueue_commit"
external enqueue : t -> bytes -> int -> unit = "wrap_ringbuf_enqueue"
external dequeue_alloc : t -> tx = "wrap_ringbuf_dequeue_alloc"
external dequeue_commit : tx -> unit = "wrap_ringbuf_dequeue_commit"
external dequeue : t -> bytes = "wrap_ringbuf_dequeue"

external write_float : tx -> int -> float -> unit = "write_float"
external write_string : tx -> int -> string -> unit = "write_boxed_str"
external write_u8 : tx -> int -> Uint8.t -> unit = "write_boxed_8"
external write_u16 : tx -> int -> Uint16.t -> unit = "write_boxed_16"
external write_u32 : tx -> int -> Uint32.t -> unit = "write_boxed_32"
external write_i32 : tx -> int -> Int32.t -> unit = "write_boxed_32"
external write_u64 : tx -> int -> Uint64.t -> unit = "write_boxed_64"
external write_i64 : tx -> int -> Int64.t -> unit = "write_boxed_64"
external write_u128 : tx -> int -> Uint128.t -> unit = "write_boxed_128"
external write_i128 : tx -> int -> Int128.t -> unit = "write_boxed_128"
external write_eth : tx -> int -> Uint48.t -> unit = "write_boxed_48"
external write_ip4 : tx -> int -> Uint32.t -> unit = "write_boxed_32"
external write_ip6 : tx -> int -> Uint128.t -> unit = "write_boxed_128"
(* Everything passed as an int and which occupancy is <= word *)
external write_bool : tx -> int -> bool -> unit = "write_word"
(* Special to zero the nullmask *)
external zero_bytes : tx -> int -> int -> unit = "zero_bytes"

(* Integers of 8, 16 and 24 bits are stored as normal ocaml integers.
 * But signed int8, int16 and int24 are shifted to the left so that
 * ocaml see them with the proper sign so that arithmetic works.
 * When we encode them using write_u{8,16,24} we must therefore shift
 * them back, as those functions assume only the low bits are relevant.
 * For this, it is enough to call the to_int function, since that's what
 * those functions do: *)
external write_i8_ : tx -> int -> int -> unit = "write_boxed_8"
external write_i16_ : tx -> int -> int -> unit = "write_boxed_16"
let write_i8 tx offs i = write_i8_ tx offs (Int8.to_int i)
let write_i16 tx offs i = write_i16_ tx offs (Int16.to_int i)

let write_cidr4 tx offs (n, l) =
  write_u32 tx offs n ;
  write_u8 tx (offs + RingBufLib.round_up_to_rb_word 4) l

let write_cidr6 tx offs (n, l) =
  write_u128 tx offs n ;
  write_u16 tx (offs + RingBufLib.round_up_to_rb_word 16) l

external read_float : tx -> int -> float = "read_float"
external read_string : tx -> int -> string = "read_str"
external read_u8 : tx -> int -> Uint8.t = "read_uint8"
external read_u16 : tx -> int -> Uint16.t = "read_uint16"
external read_u32 : tx -> int -> Uint32.t = "read_uint32"
external read_i32 : tx -> int -> Int32.t = "read_int32"
external read_u64 : tx -> int -> Uint64.t = "read_uint64"
external read_i64 : tx -> int -> Int64.t = "read_int64"
external read_u128 : tx -> int -> Uint128.t = "read_uint128"
external read_i128 : tx -> int -> Int128.t = "read_int128"
external read_eth : tx -> int -> Uint48.t = "read_uint48"
external read_ip4 : tx -> int -> Uint32.t = "read_uint32"
external read_ip6 : tx -> int -> Uint128.t = "read_uint128"
external read_bool : tx -> int -> bool = "read_word"
external read_word : tx -> int -> int = "read_word"

external set_bit : tx -> int -> unit = "set_bit"
external get_bit : tx -> int -> bool = "get_bit"

(* See above as to why int8 and int16 are special: *)
external read_i8_ : tx -> int -> int = "read_int8"
external read_i16_ : tx -> int -> int = "read_int16"
let read_i8 tx offs = Int8.of_int (read_i8_ tx offs)
let read_i16 tx offs = Int16.of_int (read_i16_ tx offs)

let read_cidr4 tx offs =
  let addr = read_u32 tx offs in
  let len = read_u8 tx (offs + RingBufLib.round_up_to_rb_word 4) in
  addr, Uint8.to_int len

let read_cidr6 tx offs =
  let addr = read_u128 tx offs in
  let len = read_u16 tx (offs + RingBufLib.round_up_to_rb_word 16) in
  addr, Uint16.to_int len

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
