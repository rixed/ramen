open Batteries
open RamenLog

(* Dequeue command *)

let dequeue copts file n () =
  logger := make_logger copts.ApiCmd.debug ;
  if file = "" then invalid_arg "dequeue" ;
  let rotate = file.[String.length file - 1] = 'r' in
  let open RingBuf in
  let rb = load ~rotate file in
  let rec dequeue_loop n =
    if n > 0 then (
      (* TODO: same automatic retry-er as in CodeGenLib_IO *)
      let bytes = dequeue rb in
      Printf.printf "dequeued %d bytes\n%!" (Bytes.length bytes) ;
      dequeue_loop (n - 1)
    )
  in
  dequeue_loop n

(* Summary command *)

let summary copts file () =
  logger := make_logger copts.ApiCmd.debug ;
  if file = "" then invalid_arg "dequeue" ;
  let rotate = file.[String.length file - 1] = 'r' in
  let open RingBuf in
  let rb = load ~rotate file in
  let s = stats rb in
  Printf.printf "%s:\n\
                 %d objects\n\
                 time range: %f..%f\n\
                 %d/%d words used\n\
                 mmapped bytes: %d\n\
                 prod/cons heads: %d/%d\n"
    file s.alloced_objects s.t_min s.t_max
    s.alloced_words s.capacity s.mem_size s.prod_head s.cons_head
