open Batteries
open RamenLog
module C = RamenConf

(* Dequeue command *)

let dequeue conf file n () =
  logger := make_logger conf.C.debug ;
  if file = "" then invalid_arg "dequeue" ;
  let open RingBuf in
  let rb = load file in
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

let summary conf file () =
  logger := make_logger conf.C.debug ;
  if file = "" then invalid_arg "dequeue" ;
  let open RingBuf in
  let rb = load file in
  let s = stats rb in
  Printf.printf "%s:\n\
                 Flags:%s\n\
                 %d objects\n\
                 first seq: %d\n\
                 time range: %f..%f\n\
                 %d/%d words used (%3.1f%%)\n\
                 mmapped bytes: %d\n\
                 prod/cons heads: %d/%d\n"
    file (if s.wrap then " Wrap" else "") s.alloced_objects
    s.first_seq s.t_min s.t_max
    s.alloced_words s.capacity
    (float_of_int s.alloced_words *. 100. /. (float_of_int s.capacity))
    s.mem_size s.prod_head s.cons_head
