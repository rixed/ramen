(* RingBuffer are used to pass data from one worker to the next(s) in the
 * stream.
 *
 * Each worker reads its inputs from a single input ringbuffer into which all
 * of its parents write, and then itself write its output to the input
 * ringbuffer of each of its children.
 *
 * The main advantage of this design over one where workers have a single
 * output and read from each of their parents, is back-pressure: the stream
 * naturally blocks when a worker is slower than its parents, and wait for it
 * to catch up. If children were reading from each of their parents output then
 * a slow child would loose data. Blocking would be harder to implement.
 *
 * This design also comes will several drawbacks, the most obvious being that a
 * worker must know all its children, which would otherwise not be needed
 * (indeed, the RaQL language selects _from_ parents, not _into_ children).
 * This requires the worker to monitor the configuration to detect when
 * children are attached/detached.
 *
 * The other important drawback is that, although the number of ringbuffers is
 * the same, there are typically more writes (one per children).
 *
 * But this also opens an interesting opportunity: Since a parent writes a
 * specific output for each of the children, this output can be trimmed down to
 * the only fields the children actually care about. Indeed it is not rare that
 * a children uses only but a small fraction of the data structure computed by
 * a parent. If all children of a parent are like that it may end up writing
 * less bytes than it would, should it writes its full output into a single
 * output ringbuffer. This is not that easy to implement though, as now not
 * only must the parent know their children, but they must know what data to
 * export (especially: we want to be able to select only some fields in a
 * deeply nested data structure). Each worker thus has a list of output
 * specifications in the configuration tree, indicating not only the location
 * of the output ringbuffer (or ORC file), but also a tree-shaped field-mask
 * selecting which fields to write. All this while trying to serialize data as
 * quick as possible using specialized native code (this part is handled by the
 * dessser library).
 *
 * Although this is safer to have no data loss as the default behavior, in
 * practice parents will not wait forever for a slow child: if an input
 * ringbuffer is full they will spin for a while and then give up for some time
 * (quarantining that full ringbuffer).
 *
 * Ringbuffers are ordinary files on the file system but they are accessed only
 * via mmap. They are all of a fixed size of 100k 32bits words, plus a small
 * header. Ringbuffers can have several simultaneous writers and readers,
 * although in practice there is always a unique reader (the worker whose input
 * ringbuffer it is).
 *
 * Every data in the ringbuffer is 32bits padded.
 *
 * Each input, or message, is appended in the ringbuffer until the buffer can
 * not accomodate the next message, in which case a special end-of-buffer
 * marker word is written (0xffff) indicating that the reader must look for the
 * next message at the buffer start.
 *
 * A message starts with its size, in 32bits words, writen as a 32bits word
 * (the size does not account for that storage, so an empty message of size 0
 * will be encoded as a word which value is 0, occupying 32 bits in the
 * ringbuffer).
 *
 * Then, the content itself starts with a 1 word header indicating whether it
 * is output data (and if so, for which channel), or if it's an end-of-replay
 * indicator (and if so, for which replay).  If the highest byte of that 32bit
 * word is 0, it is a tuple, and the channel number is given by the remaining
 * lower 24 bits (remember that channel zero is the live channel, so most of
 * the time this header will be just a plain 0 word).  If the highest byte is
 * 1, then it is an end-of-replay marker, and the remaining bits encode the
 * replay number.
 *
 * For end-of-replay indicator, there is no more data and the message is
 * already over.
 *
 * If that's output data, then it is encoded as follow:
 *
 * first, compound types (vectors, records, tuples...) starts with a nullmask,
 * which total size must be a multiple of 32bits, and cannot be 0 because it
 * itself starts with an 8bits length (giving the nullmask size in words). So
 * even a compound type having no nullable fields come with a one-word header
 * starting with the byte 1 (and the other 3 bytes being unused and arbitrary).
 *
 * If the compound type do have some nullable fields, the nullmask will be long
 * enough to have one bit per such fields (although it can have more, to
 * simplify and thus speed up the writer's job). Then when reading the fields
 * of the compound type the reader must refer back to that mask: if bit N is 0
 * then the Nth nullable field of that compound type is NULL and must be
 * skipped.
 *
 * Although a bit more complicated than prefixing each nullable value with a
 * flag, it allows us to more compactly store nulls in most cases while still
 * maintaining a nice 32bits alignment for data.
 *
 * Notice that outside of compound data it is not possible to encode null bits.
 * Therefore, it is not possible for a worker to output a single nullable
 * value.  This pose no problem in practice (as it is easy enough to output a
 * single field record or a one dimensional vector instead, when needed)
 *
 * Scalar types are encoded in the machine "natural" encoding, padded to 32bits
 * words.
 *
 * Strings are prefixed with a one word prefix for the length (in bytes).
 *
 * Lists are similarly prefixed with the length of the list.
 *
 * Sum types are a bit special: They come with a 1 word header that's composed
 * of a 16 bits nullmask (of which only the first bit can ever be used) and
 * then 16bits to specify the constructor. The constructed value follows as
 * usual.
 *
 * Record fields are *not* encoded in order they are defined, but in
 * alphabetical order. For instance, the type "{foo : i16? ; bar : u32 }" will
 * be encoded as:
 *
 * - a nullmask of one word (with the first bit assigned to "foo")
 *
 * - a word for the value of the "bar" field
 *
 * - an optional word for the value of "foo" (if the nullmask says so), padded
 * in a 32bits word.
 *
 * This is so that a child can select the same fields from parents outputing
 * different records, as long as they have the same names and types: both will
 * end up being encoded the same.
 *)

open Batteries
open Stdint
open RamenHelpers
module Default = RamenConstsDefault
module N = RamenName
module Files = RamenFiles

exception NoMoreRoom
exception Empty
exception Damaged
let () =
  Callback.register_exception "ringbuf full exception" NoMoreRoom ;
  Callback.register_exception "ringbuf empty exception" Empty ;
  Callback.register_exception "ringbuf damaged exception" Damaged

type t (* abstract, represents a ring buffer mmapped file *)

let prepend_rb_name f (fname : N.path) =
  try f fname
  with Failure msg -> failwith ((fname :> string) ^": "^ msg)

external create_ : string -> bool -> int -> float -> N.path -> unit =
  "wrap_ringbuf_create"

let create ?(wrap=true)
           ?(words=Default.ringbuffer_word_length)
           ?(timeout=Default.ringbuffer_timeout)
           fname =
  Files.mkdir_all ~is_file:true fname ;
  prepend_rb_name (create_ RamenVersions.ringbuf wrap words timeout) fname

type stats = {
  capacity : int ; (* in words *)
  wrap : bool ;
  alloced_words : int ; (* in words *)
  alloc_count : int ;
  t_min : float ; (* Will be 0 when unset *)
  t_max : float ;
  mem_size : int ; (* the number of bytes that were mapped *)
  prod_head : int ;
  prod_tail : int ;
  cons_head : int ;
  cons_tail : int ;
  first_seq : int (* taken from arc/max file *) ;
  timeout : float }

external load_ : string -> N.path -> t = "wrap_ringbuf_load"
let load = prepend_rb_name (load_ RamenVersions.ringbuf)
external unload : t -> unit = "wrap_ringbuf_unload"
external stats : t -> stats = "wrap_ringbuf_stats"
external repair : t -> bool = "wrap_ringbuf_repair"
(* Same as unload, but if the ringbuf is non wrapping tries to archive it: *)
external may_archive_and_unload : t -> unit = "wrap_ringbuf_may_archive"

type tx (* abstract, represents an ongoing (de)queueing operation *)

(* Return the size in bytes: *)
external tx_size : tx -> int = "wrap_ringbuf_tx_size" [@@noalloc]
external tx_start : tx -> int = "wrap_ringbuf_tx_start" [@@noalloc]
external tx_fname : tx -> string = "wrap_ringbuf_tx_fname"
external tx_address : tx -> Uint64.t = "wrap_ringbuf_tx_address"
external enqueue_alloc : t -> int -> tx = "wrap_ringbuf_enqueue_alloc"
external enqueue_commit : tx -> float -> float -> unit = "wrap_ringbuf_enqueue_commit" [@@noalloc]
external enqueue : t -> bytes -> int -> float -> float -> unit = "wrap_ringbuf_enqueue"
external dequeue_alloc : t -> tx = "wrap_ringbuf_dequeue_alloc"
external dequeue_commit : tx -> unit = "wrap_ringbuf_dequeue_commit" [@@noalloc]
external dequeue : t -> bytes = "wrap_ringbuf_dequeue"
external read_raw : t -> int -> int -> bytes = "wrap_ringbuf_read_raw"
external read_raw_tx : tx -> bytes = "wrap_ringbuf_read_raw_tx"
external write_raw_tx : tx -> int -> bytes -> unit = "wrap_ringbuf_write_raw_tx"
external read_first : t -> tx = "wrap_ringbuf_read_first"
external read_next : tx -> tx = "wrap_ringbuf_read_next"
(* A TX that serialize things in an internal buffer of the given size (in
 * bytes) and which is effectively independent of any ringbuffer.
 * Do not enqueue_alloc in there but write directly!
 * Then use [read_raw_tx] to get the value. *)
external bytes_tx : int -> tx = "wrap_bytes_tx"
(* Build a special TX from the given bytes, that can then be used to
 * deserialize. Independent of any ringbuffer. *)
external tx_of_bytes : Bytes.t -> tx = "wrap_tx_of_bytes"

external write_float : tx -> int -> float -> unit = "write_float"
external write_char : tx -> int -> char -> unit = "write_word"
external write_string : tx -> int -> string -> unit = "write_str"
external write_u8 : tx -> int -> Uint8.t -> unit = "write_unboxed_8"
external write_u16 : tx -> int -> Uint16.t -> unit = "write_unboxed_16"
external write_u24 : tx -> int -> Uint24.t -> unit = "write_unboxed_24"
external write_u32 : tx -> int -> Uint32.t -> unit = "write_boxed_32"
external write_u40 : tx -> int -> Uint40.t -> unit = "write_boxed_40"
external write_u48 : tx -> int -> Uint48.t -> unit = "write_boxed_48"
external write_u56 : tx -> int -> Uint56.t -> unit = "write_boxed_56"
external write_i32 : tx -> int -> Int32.t -> unit = "write_boxed_32"
external write_i40 : tx -> int -> Int40.t -> unit = "write_boxed_40"
external write_i48 : tx -> int -> Int48.t -> unit = "write_boxed_48"
external write_i56 : tx -> int -> Int56.t -> unit = "write_boxed_56"
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
external write_i24_ : tx -> int -> int -> unit = "write_unboxed_24"
let write_i8 tx offs i = write_i8_ tx offs (Int8.to_int i)
let write_i16 tx offs i = write_i16_ tx offs (Int16.to_int i)
let write_i24 tx offs i = write_i24_ tx offs (Int24.to_int i)

external read_float : tx -> int -> float = "read_float"
external read_char : tx -> int -> char = "read_word"
external read_string : tx -> int -> string = "read_str"
external read_u8 : tx -> int -> Uint8.t = "read_uint8"
external read_u16 : tx -> int -> Uint16.t = "read_uint16"
external read_u24 : tx -> int -> Uint24.t = "read_uint24"
external read_u32 : tx -> int -> Uint32.t = "read_uint32"
external read_u40 : tx -> int -> Uint40.t = "read_uint40"
external read_u48 : tx -> int -> Uint48.t = "read_uint48"
external read_u56 : tx -> int -> Uint56.t = "read_uint56"
external read_u64 : tx -> int -> Uint64.t = "read_uint64"
external read_u128 : tx -> int -> Uint128.t = "read_uint128"
external read_i32 : tx -> int -> Int32.t = "read_int32"
external read_i40 : tx -> int -> Int40.t = "read_int40"
external read_i48 : tx -> int -> Int48.t = "read_int48"
external read_i56 : tx -> int -> Int56.t = "read_int56"
external read_i64 : tx -> int -> Int64.t = "read_int64"
external read_i128 : tx -> int -> Int128.t = "read_int128"
external read_eth : tx -> int -> Uint48.t = "read_uint48"
external read_ip4 : tx -> int -> Uint32.t = "read_uint32"
external read_ip6 : tx -> int -> Uint128.t = "read_uint128"
external read_ip : tx -> int -> RamenIp.t = "read_ip"
external read_bool : tx -> int -> bool = "read_word"
external read_word : tx -> int -> int = "read_word"

external set_bit : tx -> int -> int -> unit = "set_bit" [@@noalloc]
external get_bit : tx -> int -> int -> bool = "get_bit" [@@noalloc]

(* See above as to why int8, int16 and int24 are special: *)
external read_i8_ : tx -> int -> int = "read_int8"
external read_i16_ : tx -> int -> int = "read_int16"
external read_i24_ : tx -> int -> int = "read_int24"
let read_i8 tx offs = Int8.of_int (read_i8_ tx offs)
let read_i16 tx offs = Int16.of_int (read_i16_ tx offs)
let read_i24 tx offs = Int24.of_int (read_i24_ tx offs)

let round_up_to_rb_word bytes =
  let low = bytes land (DessserRamenRingBuffer.word_size - 1) in
  if low = 0 then bytes else bytes - low + DessserRamenRingBuffer.word_size

(* As a record, first the "ip" then the "mask", with an empty nullmask: *)
let write_cidr4 tx offs (n, l) =
  write_u32 tx offs Uint32.zero ;
  write_u32 tx (offs + round_up_to_rb_word 4) n ;
  write_u8 tx (offs + round_up_to_rb_word 8) l

let write_cidr6 tx offs (n, l) =
  write_u32 tx offs Uint32.zero ;
  write_u128 tx (offs + round_up_to_rb_word 4) n ;
  write_u8 tx (offs + round_up_to_rb_word 20) l

let ip_head n =
  Uint32.of_int (n lsl 16)

let write_cidr tx offs = function
  | RamenIp.Cidr.V4 n ->
      write_u32 tx offs (ip_head 0) ;
      write_cidr4 tx (offs + round_up_to_rb_word 1) n
  | RamenIp.Cidr.V6 n ->
      write_u32 tx offs (ip_head 1) ;
      write_cidr6 tx (offs + round_up_to_rb_word 1) n

let read_cidr4 tx offs =
  let offs = offs + round_up_to_rb_word 4 in
  let addr = read_u32 tx offs in
  let len = read_u8 tx (offs + round_up_to_rb_word 4) in
  addr, len

let read_cidr6 tx offs =
  let offs = offs + round_up_to_rb_word 4 in
  let addr = read_u128 tx offs in
  let len = read_u8 tx (offs + round_up_to_rb_word 16) in
  addr, len

(* Dessser sum encoding, with unused nullmask: *)
let read_cidr tx offs =
  let head = read_u32 tx offs |> Uint32.to_int in
  let tag = head lsr 16 in
  match tag with
  | 0 -> RamenIp.Cidr.V4 (read_cidr4 tx (offs + DessserRamenRingBuffer.word_size))
  | 1 -> RamenIp.Cidr.V6 (read_cidr6 tx (offs + DessserRamenRingBuffer.word_size))
  | _ -> assert false
