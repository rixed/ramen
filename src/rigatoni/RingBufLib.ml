(* Have this outside of RingBuf so that we can easily link this with the
 * tests without bringing in the whole ringbuf libs *)

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
  Helpers.retry ~on ~first_delay:0.001 ~max_delay:0.01 f
