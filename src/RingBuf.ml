open Batteries
open Stdint
open RamenHelpers
open RamenConsts

exception NoMoreRoom
exception Empty
let () =
  Callback.register_exception "ringbuf full exception" NoMoreRoom ;
  Callback.register_exception "ringbuf empty exception" Empty

type t (* abstract, represents a ring buffer mmapped file *)

let prepend_rb_name f fname =
  try f fname
  with Failure msg -> failwith (fname ^": "^ msg)

external create_ : string -> bool -> bool -> int -> string -> unit =
  "wrap_ringbuf_create"

let create ?(wrap=true) ?(archive=false)
           ?(words=Default.ringbuffer_word_length) fname =
  mkdir_all ~is_file:true fname ;
  prepend_rb_name (create_ RamenVersions.ringbuf wrap archive words) fname

type stats = {
  capacity : int ; (* in words *)
  wrap : bool ;
  archive : bool ;
  alloced_words : int ; (* in words *)
  alloc_count : int ;
  t_min : float ; (* Will be 0 when unset *)
  t_max : float ;
  mem_size : int ; (* the number of bytes that were mapped *)
  prod_head : int ;
  prod_tail : int ;
  cons_head : int ;
  cons_tail : int ;
  first_seq : int (* taken from arc/max file *) }

external load_ : string -> string -> t = "wrap_ringbuf_load"
let load = prepend_rb_name (load_ RamenVersions.ringbuf)
external unload : t -> unit = "wrap_ringbuf_unload"
external stats : t -> stats = "wrap_ringbuf_stats"
external repair : t -> bool = "wrap_ringbuf_repair"

type tx (* abstract, represents an ongoing (de)queueing operation *)

external tx_size : tx -> int = "wrap_ringbuf_tx_size"
external enqueue_alloc : t -> int -> tx = "wrap_ringbuf_enqueue_alloc"
external enqueue_commit : tx -> float -> float -> unit = "wrap_ringbuf_enqueue_commit"
external enqueue : t -> bytes -> int -> float -> float -> unit = "wrap_ringbuf_enqueue"
external dequeue_alloc : t -> tx = "wrap_ringbuf_dequeue_alloc"
external dequeue_commit : tx -> unit = "wrap_ringbuf_dequeue_commit"
external dequeue : t -> bytes = "wrap_ringbuf_dequeue"
external read_raw : t -> int -> int -> bytes = "wrap_ringbuf_read_raw"
external read_first : t -> tx = "wrap_ringbuf_read_first"
external read_next : tx -> tx = "wrap_ringbuf_read_next"
external empty_tx : unit -> tx = "wrap_empty_tx"

external write_float : tx -> int -> float -> unit = "write_float"
external write_string : tx -> int -> string -> unit = "write_str"
external write_u8 : tx -> int -> Uint8.t -> unit = "write_unboxed_8"
external write_u16 : tx -> int -> Uint16.t -> unit = "write_unboxed_16"
external write_u32 : tx -> int -> Uint32.t -> unit = "write_boxed_32"
external write_i32 : tx -> int -> Int32.t -> unit = "write_boxed_32"
external write_u64 : tx -> int -> Uint64.t -> unit = "write_boxed_64"
external write_i64 : tx -> int -> Int64.t -> unit = "write_boxed_64"
external write_u128 : tx -> int -> Uint128.t -> unit = "write_boxed_128"
external write_i128 : tx -> int -> Int128.t -> unit = "write_boxed_128"
external write_eth : tx -> int -> Uint48.t -> unit = "write_boxed_48"
external write_ip4 : tx -> int -> Uint32.t -> unit = "write_boxed_32"
external write_ip6 : tx -> int -> Uint128.t -> unit = "write_boxed_128"
external write_ip : tx -> int -> RamenIp.t -> unit = "write_ip"
(* Everything passed as an int and which occupancy is <= word *)
external write_bool : tx -> int -> bool -> unit = "write_word"
(* Special to zero the nullmask *)
external zero_bytes : tx -> int (* offs *) -> int (* size *) -> unit =
  "zero_bytes"

(* Integers of 8, 16 and 24 bits are stored as normal ocaml integers.
 * But signed int8, int16 and int24 are shifted to the left so that
 * ocaml sees them with the proper sign so that arithmetic and wrap around
 * works.
 * When we encode them using write_u{8,16,24} we must therefore shift
 * them back, as those functions assume only the low bits are relevant.
 * For this, it is enough to call the to_int function, since that's what
 * those functions do: *)
external write_i8_ : tx -> int -> int -> unit = "write_unboxed_8"
external write_i16_ : tx -> int -> int -> unit = "write_unboxed_16"
let write_i8 tx offs i = write_i8_ tx offs (Int8.to_int i)
let write_i16 tx offs i = write_i16_ tx offs (Int16.to_int i)

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
external read_ip : tx -> int -> RamenIp.t = "read_ip"
external read_bool : tx -> int -> bool = "read_word"
external read_word : tx -> int -> int = "read_word"

external set_bit : tx -> int -> int -> unit = "set_bit"
external get_bit : tx -> int -> int -> bool = "get_bit"

(* See above as to why int8 and int16 are special: *)
external read_i8_ : tx -> int -> int = "read_int8"
external read_i16_ : tx -> int -> int = "read_int16"
let read_i8 tx offs = Int8.of_int (read_i8_ tx offs)
let read_i16 tx offs = Int16.of_int (read_i16_ tx offs)

(* Compromise between size and efficient reading of data, TBD: *)
let rb_word_bytes = 4
let rb_word_bits = rb_word_bytes * 8
let rb_word_mask = (1 lsl rb_word_bits) - 1

let bytes_for_bits n =
  n / 8 + (if n land 7 = 0 then 0 else 1)

let round_up_to_rb_word bytes =
  let low = bytes land (rb_word_bytes-1) in
  if low = 0 then bytes else bytes - low + rb_word_bytes

let write_cidr4 tx offs (n, l) =
  write_u32 tx offs n ;
  write_u8 tx (offs + round_up_to_rb_word 4) (Uint8.of_int l)

let write_cidr6 tx offs (n, l) =
  write_u128 tx offs n ;
  write_u16 tx (offs + round_up_to_rb_word 16) (Uint16.of_int l)

let write_cidr tx offs = function
  | RamenIp.Cidr.V4 n ->
      write_u8 tx offs (Uint8.of_int 4) ;
      write_cidr4 tx (offs + round_up_to_rb_word 1) n
  | RamenIp.Cidr.V6 n ->
      write_u8 tx offs (Uint8.of_int 6) ;
      write_cidr6 tx (offs + round_up_to_rb_word 1) n

let read_cidr4 tx offs =
  let addr = read_u32 tx offs in
  let len = read_u8 tx (offs + round_up_to_rb_word 4) in
  addr, Uint8.to_int len

let read_cidr6 tx offs =
  let addr = read_u128 tx offs in
  let len = read_u16 tx (offs + round_up_to_rb_word 16) in
  addr, Uint16.to_int len

let read_cidr tx offs =
  match read_u8 tx offs |> Uint8.to_int with
  | 4 -> RamenIp.Cidr.V4 (read_cidr4 tx (offs + round_up_to_rb_word 1))
  | 6 -> RamenIp.Cidr.V6 (read_cidr6 tx (offs + round_up_to_rb_word 1))
  | _ -> assert false
