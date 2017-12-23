(* Have this outside of RingBuf so that we can easily link this with the
 * tests without bringing in the whole ringbuf libs *)
open Batteries
open RamenSharedTypes
open RamenSharedTypesJS
open RamenLog
open Helpers

exception NoMoreRoom
exception Empty
let () =
  Callback.register_exception "ringbuf full exception" NoMoreRoom ;
  Callback.register_exception "ringbuf empty exception" Empty

(* Compromise between size and efficient reading of data, TBD: *)
let rb_word_bytes = 4
let rb_word_bits = rb_word_bytes * 8
let rb_word_mask = (1 lsl rb_word_bits) - 1

let bytes_for_bits n =
  n / 8 + (if n land 7 = 0 then 0 else 1)

let round_up_to_rb_word bytes =
  let low = bytes land (rb_word_bytes-1) in
  if low = 0 then bytes else bytes - low + rb_word_bytes

let nullmask_bytes_of_tuple_type tuple_typ =
  List.fold_left (fun s field_typ ->
    if not (is_private_field field_typ.typ_name) && field_typ.nullable
    then s+1 else s) 0 tuple_typ |>
  bytes_for_bits |>
  round_up_to_rb_word

let retry_for_ringbuf ?(while_=(fun () -> Lwt.return_true)) ?delay_rec f =
  let on = function
    | NoMoreRoom | Empty -> while_ ()
    | _ -> Lwt.return_false
  in
  retry ~on ~first_delay:0.001 ~max_delay:0.1 ?delay_rec
    (fun x -> Lwt.return (f x))

let sersize_of_string s =
  rb_word_bytes + round_up_to_rb_word (String.length s)

let rec sersize_of_fixsz_typ =
  function
  | TFloat -> round_up_to_rb_word 8
  | TBool | TU8 | TI8 -> round_up_to_rb_word 1
  | TU16 | TI16 -> round_up_to_rb_word 2
  | TU32 | TI32 | TIpv4 -> round_up_to_rb_word 4
  | TU64 | TI64 -> round_up_to_rb_word 8
  | TU128 | TI128 | TIpv6 -> round_up_to_rb_word 16
  | TNull -> 0
  | TEth -> round_up_to_rb_word 6
  | TCidrv4 -> sersize_of_fixsz_typ TIpv4 + sersize_of_fixsz_typ TU8
  | TCidrv6 -> sersize_of_fixsz_typ TIpv6 + sersize_of_fixsz_typ TU16
  | TString -> assert false
  | TNum | TAny -> assert false

let out_ringbuf_names outbuf_ref_fname =
  let open Lwt in
  let last_touched fname =
    let open Lwt_unix in
    let%lwt s = stat fname in return s.st_mtime in
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

(* To allow a node to select only some fields from its parent and write only
 * a skip list in the out_ref (to makes serialization easier not out_ref
 * smaller) we serialize all fields in the same order: *)
let ser_tuple_field_cmp t1 t2 =
  String.compare t1.typ_name t2.typ_name

let ser_tuple_typ_of_tuple_typ tuple_typ =
  tuple_typ |>
  List.filter (fun t -> not (is_private_field t.typ_name)) |>
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
        !logger.error "Field %s is not in it's parent output" i.typ_name ;
        assert false)
    | [], _ ->
      !logger.error "More inputs than parent outputs?" ;
      assert false
  in
  loop [] (ser_out, ser_in)
