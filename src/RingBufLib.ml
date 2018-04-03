(* Have this outside of RingBuf so that we can easily link this with the
 * tests without bringing in the whole ringbuf libs *)
open Batteries
open RamenLog
open RamenHelpers
open RamenScalar

exception NoMoreRoom
exception Empty
let () =
  Callback.register_exception "ringbuf full exception" NoMoreRoom ;
  Callback.register_exception "ringbuf empty exception" Empty

(* Compromise between size and efficient reading of data, TBD: *)
let rb_word_bytes = 4
let rb_word_bits = rb_word_bytes * 8
let rb_word_mask = (1 lsl rb_word_bits) - 1

let rb_words = 1_000_000

let bytes_for_bits n =
  n / 8 + (if n land 7 = 0 then 0 else 1)

let round_up_to_rb_word bytes =
  let low = bytes land (rb_word_bytes-1) in
  if low = 0 then bytes else bytes - low + rb_word_bytes

let nullmask_bytes_of_tuple_type tuple_typ =
  List.fold_left (fun s field_typ ->
    if not RamenTuple.(is_private_field field_typ.typ_name) && field_typ.nullable
    then s+1 else s) 0 tuple_typ |>
  bytes_for_bits |>
  round_up_to_rb_word

let retry_for_ringbuf ?while_ ?delay_rec ?max_retry_time f =
  let on = function
    | NoMoreRoom | Empty -> Lwt.return_true
    | _ -> Lwt.return_false
  in
  retry ?while_ ~on ~first_delay:0.001 ~max_delay:0.1 ?delay_rec
        ?max_retry_time (fun x -> Lwt.wrap (fun () -> f x))

let sersize_of_string s =
  rb_word_bytes + round_up_to_rb_word (String.length s)

let sersize_of_float = round_up_to_rb_word 8
let sersize_of_bool = round_up_to_rb_word 1
let sersize_of_u8 = round_up_to_rb_word 1
let sersize_of_i8 = round_up_to_rb_word 1
let sersize_of_u16 = round_up_to_rb_word 2
let sersize_of_i16 = round_up_to_rb_word 2
let sersize_of_u32 = round_up_to_rb_word 4
let sersize_of_i32 = round_up_to_rb_word 4
let sersize_of_ipv4 = round_up_to_rb_word 4
let sersize_of_u64 = round_up_to_rb_word 8
let sersize_of_i64 = round_up_to_rb_word 8
let sersize_of_u128 = round_up_to_rb_word 16
let sersize_of_i128 = round_up_to_rb_word 16
let sersize_of_ipv6 = round_up_to_rb_word 16
let sersize_of_null = 0
let sersize_of_eth = round_up_to_rb_word 6
let sersize_of_cidrv4 = sersize_of_ipv4 + sersize_of_u8
let sersize_of_cidrv6 = sersize_of_ipv6 + sersize_of_u16

let rec sersize_of_fixsz_typ = function
  | TFloat -> sersize_of_float
  | TBool -> sersize_of_bool
  | TU8 -> sersize_of_u8
  | TI8 -> sersize_of_i8
  | TU16 -> sersize_of_u16
  | TI16 -> sersize_of_i16
  | TU32 -> sersize_of_u32
  | TI32 -> sersize_of_i32
  | TIpv4 -> sersize_of_ipv4
  | TU64 -> sersize_of_u64
  | TI64 -> sersize_of_i64
  | TU128 -> sersize_of_u128
  | TI128 -> sersize_of_i128
  | TIpv6 -> sersize_of_ipv6
  | TNull -> sersize_of_null
  | TEth -> sersize_of_eth
  | TCidrv4 -> sersize_of_cidrv4
  | TCidrv6 -> sersize_of_cidrv6
  | TString | TNum | TAny -> assert false

let sersize_of_value = function
  | VString s -> sersize_of_string s
  | VFloat _ -> sersize_of_float
  | VBool _ -> sersize_of_bool
  | VU8 _ -> sersize_of_u8
  | VI8 _ -> sersize_of_i8
  | VU16 _ -> sersize_of_u16
  | VI16 _ -> sersize_of_i16
  | VU32 _ -> sersize_of_u32
  | VI32 _ -> sersize_of_i32
  | VIpv4 _ -> sersize_of_ipv4
  | VU64 _ -> sersize_of_u64
  | VI64 _ -> sersize_of_i64
  | VU128 _ -> sersize_of_u128
  | VI128 _ -> sersize_of_i128
  | VIpv6 _ -> sersize_of_ipv6
  | VNull -> sersize_of_null
  | VEth _ -> sersize_of_eth
  | VCidrv4 _ -> sersize_of_cidrv4
  | VCidrv6 _ -> sersize_of_cidrv6

let out_ringbuf_names outbuf_ref_fname =
  let open Lwt in
  let last_touched fname =
    let open Lwt_unix in
    match%lwt stat fname with
    | exception Unix.Unix_error (Unix.ENOENT, _, _) ->
        !logger.warning "last_touched: file %S is missing" fname ;
        return 0.
    | s ->
        return s.st_mtime in
  let last_read = ref 0. in
  fun () ->
    let%lwt t = last_touched outbuf_ref_fname in
    if t > !last_read then (
      if !last_read <> 0. then
        !logger.info "Have to re-read %s" outbuf_ref_fname ;
      last_read := t ;
      let%lwt lines = RamenOutRef.read outbuf_ref_fname in
      return (Some lines)
    ) else return_none

(* To allow a func to select only some fields from its parent and write only
 * a skip list in the out_ref (to makes serialization easier not out_ref
 * smaller) we serialize all fields in the same order: *)
let ser_tuple_field_cmp t1 t2 =
  String.compare t1.RamenTuple.typ_name t2.RamenTuple.typ_name

let ser_tuple_typ_of_tuple_typ tuple_typ =
  tuple_typ |>
  List.filter (fun t -> not RamenTuple.(is_private_field t.typ_name)) |>
  List.fast_sort ser_tuple_field_cmp

let skip_list ~out_type ~in_type =
  let ser_out = ser_tuple_typ_of_tuple_typ out_type
  and ser_in = ser_tuple_typ_of_tuple_typ in_type in
  let rec loop v = function
    | [], [] -> List.rev v
    | _::os', [] -> loop (false :: v) (os', [])
    | o::os', (i::is' as is) ->
      let c = ser_tuple_field_cmp o i in
      if c < 0 then
        (* not interested in o *)
        loop (false :: v) (os', is)
      else if c = 0 then
        (* we want o *)
        loop (true :: v) (os', is')
      else (
        (* not possible: i must be in o *)
        !logger.error "Field %s is not in its parent output" i.typ_name ;
        assert false)
    | [], _ ->
      !logger.error "More inputs than parent outputs?" ;
      assert false
  in
  loop [] (ser_out, ser_in)
