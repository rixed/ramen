open Batteries
open Log

(* Dequeue command *)

let dequeue debug file n () =
  logger := make_logger debug ;
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

let summary debug file () =
  logger := make_logger debug ;
  let open RingBuf in
  let rb = load file in
  Printf.printf "%s: %d/%d words used\n"
    file (nb_entries rb) (capacity rb)
