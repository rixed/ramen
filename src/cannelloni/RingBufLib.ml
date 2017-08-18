(* Have this outside of RingBuf so that we can easily link this with the
 * tests without bringing in the whole ringbuf libs *)
open Batteries
open Lwt

(* Compromise between size and efficient reading of data, TBD: *)
let rb_word_bytes = 4
let rb_word_bits = rb_word_bytes * 8
let rb_word_mask = (1 lsl rb_word_bits) - 1

let bytes_for_bits n =
  n / 8 + (if n land 7 = 0 then 0 else 1)

let round_up_to_rb_word bytes =
  let low = bytes land (rb_word_bytes-1) in
  if low = 0 then bytes else bytes - low + rb_word_bytes

let retry_for_ringbuf f =
  let on = function
    (* FIXME: a dedicated RingBuf.NoMoreRoom exception *)
    | Failure _ -> true
    | _ -> false
  in
  Helpers.retry ~on ~first_delay:0.001 ~max_delay:0.01
    (fun x -> return (f x))

let rec sersize_of_fixsz_typ =
  let open RamenSharedTypes in
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
  | TNum -> assert false

(* Get ones input ringbuf and output ringbufs given the node "signature",
 * which is a string identifying both the operation of a node and its
 * input and output types: *)

let tmp_dir = "/tmp"

let in_ringbuf_name signature =
  tmp_dir ^"/ringbuf_in_"^ signature

let exp_ringbuf_name signature =
  tmp_dir ^"/ringbuf_exp_"^ signature

let out_ringbuf_names_ref signature =
  tmp_dir ^"/ringbuf_out_ref_"^ signature

let last_touched fname =
  let open Lwt_unix in
  let%lwt s = stat fname in return s.st_mtime

let out_ringbuf_names outbuf_ref_fname =
  let last_read = ref 0. in
  let lines = ref Set.empty in
  fun () ->
    let%lwt t = last_touched outbuf_ref_fname in
    if t > !last_read then (
      last_read := t ;
      lines := File.lines_of outbuf_ref_fname |> Set.of_enum ;
      return (Some !lines)
    ) else return_none
