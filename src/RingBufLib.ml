(* Have this outside of RingBuf so that we can easily link this with the
 * tests without bringing in the whole ringbuf libs *)
open Batteries
open RamenSharedTypes
open RamenLog

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
    if field_typ.nullable then s+1 else s) 0 tuple_typ |>
  bytes_for_bits |>
  round_up_to_rb_word

let retry_for_ringbuf f =
  let on = function
    | NoMoreRoom | Empty -> true
    | _ -> false
  in
  Helpers.retry ~on ~first_delay:0.001 ~max_delay:0.01
    (fun x -> Lwt.return (f x))

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
  let lines = ref Set.empty in
  fun () ->
    let%lwt t = last_touched outbuf_ref_fname in
    if t > !last_read then (
      if !last_read <> 0. then
        !logger.info "Have to re-read %s" outbuf_ref_fname ;
      last_read := t ;
      lines := File.lines_of outbuf_ref_fname |> Set.of_enum ;
      return (Some !lines)
    ) else return_none
