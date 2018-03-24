open RingBuf
open RingBufLib
open Stdint
open Batteries

let debug = false

let () =
  if debug then Printf.printf "Mmapping ringbuf...\n%!" ;
  let rb_fname = "/tmp/ringbuf_test.r" in
  ignore_exceptions Unix.unlink rb_fname ;
  create rb_fname 100 ;
  let rb = load ~rotate:true rb_fname in
  if debug then Printf.printf "Allocating bytes...\n%!" ;
  let tx = enqueue_alloc rb (4+4+4+16) in
  if debug then Printf.printf "Write u32...\n%!" ;
  write_u32 tx 0 (Uint32.of_int32 0x11223344l) ;
  if debug then Printf.printf "Write bool...\n%!" ;
  write_bool tx 4 true ;
  if debug then Printf.printf "Write u8...\n%!" ;
  write_u8 tx 8 (Uint8.of_int 0x12) ;
  if debug then Printf.printf "Write i128...\n%!" ;
  write_i128 tx 12 (Int128.of_string "22690724232397191156128999734549562180") ;
  if debug then Printf.printf "Commit...\n%!" ;
  enqueue_commit tx 0. 0. ;
  let str = "glopi" in
  let tx = enqueue_alloc rb (
            round_up_to_rb_word(String.length str)
            (* variable sized field will be prepended with its length *)
          + round_up_to_rb_word(1)) in
  if debug then Printf.printf "Write %S...\n%!" str ;
  write_string tx 0 str ;
  enqueue_commit tx 0. 0.
