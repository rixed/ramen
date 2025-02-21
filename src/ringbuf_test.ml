open RingBuf
open RingBufLib
open Stdint
open Batteries
module Files = RamenFiles

let debug = true

let rb_fname = N.path (Files.tmp_dir ^"/ringbuf_test.r")

(* Basic ser/des tests *)
let test1 () =
  if debug then Printf.printf "Ser/Des test...\n%!" ;
  if debug then Printf.printf "Mmapping ringbuf...\n%!" ;
  ignore_exceptions Files.unlink rb_fname ;
  create ~words:100 rb_fname ;
  let rb = load rb_fname in
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
            round_up_to_rb_word (String.length str)
            (* variable sized field will be prepended with its length *)
          + round_up_to_rb_word 1) in
  if debug then Printf.printf "Write %S...\n%!" str ;
  write_string tx 0 str ;
  enqueue_commit tx 0. 0.

(* Concurrent access tests *)
let test2 () =
  if debug then Printf.printf "Concurrency test...\n%!" ;
  if debug then Printf.printf "Mmapping ringbuf...\n%!" ;
  ignore_exceptions Files.unlink rb_fname ;
  let msg_min_len = 4
  and msg_max_len = 64 in
  create ~words:((msg_max_len / 4) * 100_000) rb_fname ;
  let rb = load rb_fname in
  let rec do_until t f =
    let now = Unix.gettimeofday () in
    f (now > t) ;
    do_until t f
  and wait () =
    Unix.sleepf 1e-7
  and die () =
    Printf.eprintf "%d: Dying now\n" (Unix.getpid ()) ;
    exit 0
  and make_msg len =
    let msg = Bytes.make len 'a' in
    Bytes.unsafe_set msg 0 'B' ;
    Bytes.unsafe_set msg (len - 1) 'R' ;
    msg in
  let reader die_after =
    do_until die_after (fun enough ->
      match dequeue_alloc rb with
      | exception Empty ->
          if enough then die () else
          wait ()
      | tx ->
          (* Check the message *)
          let msg = read_raw_tx tx in
          let msg_len = Bytes.length msg in
          if msg_len < msg_min_len || msg_len > msg_max_len then
            Printf.eprintf "msg_len = %d!?\n" msg_len ;
          assert (msg_len >= msg_min_len && msg_len <= msg_max_len) ;
          (*if debug then Printf.printf "%d: < %d bytes\n" pid msg_len ;*)
          if Bytes.unsafe_get msg 0 <> 'B' ||
             Bytes.unsafe_get msg 1 <> 'a' ||
             Bytes.(unsafe_get msg (msg_len - 1)) <> 'R' then
            Printf.eprintf "wrong msg: %S\n" (Bytes.unsafe_to_string msg |> String.quote) ;
(*          assert (Bytes.unsafe_get msg 0 = 'B') ;
          assert (Bytes.unsafe_get msg 1 = 'a') ;
          assert (Bytes.(unsafe_get msg (msg_len - 1)) = 'R') ;*)
          if enough then die () (* Quit with the TX allocated *) else
          dequeue_commit tx)
  and writer die_after =
    do_until die_after (fun enough ->
      let msg_len =
        ((msg_min_len + Random.int (msg_max_len - msg_min_len + 1)) + 3) land (lnot 3) in
      (*if debug then Printf.printf "%d: > %d bytes\n" pid msg_len ;*)
      match enqueue_alloc rb msg_len with
      | exception NoMoreRoom ->
          if enough then die () else
          wait ()
      | tx ->
          if enough then die () (* Die with uninitialized message *) else
          let msg = make_msg msg_len in
          write_raw_tx tx 0 msg ;
          enqueue_commit tx 0. 0.)
  in
  let num_processes = 10 in
  let now = Unix.gettimeofday () in
  List.init num_processes (fun i ->
    let pid = Unix.fork () in
    if pid = 0 then (
      let is_writer = i mod 2 = 0 in
      let die_after =
        now +. 1. +. 2. *. float_of_int (i + 1) in
      (if is_writer then writer else reader) die_after)
    else pid) |>
  List.iter (fun pid ->
    let open Unix in
    match waitpid [] pid with
    | _pid, WEXITED 0 ->
        (* All good *)
        if debug then Printf.printf "Child %d exited OK\n%!" pid
    | _pid, WEXITED x ->
        Printf.eprintf "Child %d exited with code %d\n" pid x
    | _pid, WSIGNALED x ->
        Printf.eprintf "Chilld %d killed by signal %d\n" pid x
    | _ ->
        assert false (* Should not report stops *))

let () =
  Random.self_init () ;
  test1 () ;
  test2 ()
