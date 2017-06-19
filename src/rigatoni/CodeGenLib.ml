(* Tools used by the generated OCaml code *)
open Batteries
open Stdint
open Log
open Lwt

(* Converters from string to values *)

let float_of_string = Pervasives.float_of_string
let string_of_string x = x
let bool_of_string = Pervasives.bool_of_string
let u8_of_string = Uint8.of_string
let u16_of_string = Uint16.of_string
let u32_of_string = Uint32.of_string
let u64_of_string = Uint64.of_string
let u128_of_string = Uint128.of_string
let i8_of_string = Int8.of_string
let i16_of_string = Int16.of_string
let i32_of_string = Int32.of_string
let i64_of_string = Int64.of_string
let i128_of_string = Int128.of_string

let getenv ?def n =
  try Sys.getenv n
  with Not_found ->
    match def with
    | Some d -> d
    | None ->
      Printf.sprintf "Cannot find envvar %s" n |>
      failwith

let output rb sersize_of_tuple serialize_tuple tuple =
  let open RingBuf in
  let sersize = sersize_of_tuple tuple in
  let tx = enqueue_alloc rb sersize in
  let offs = serialize_tuple tx tuple in
  enqueue_commit tx ;
  assert (offs = sersize)

let retry ~on ?(first_delay=1.0) ?(min_delay=0.001) ?(max_delay=10.0) ?(delay_adjust_ok=0.2) ?(delay_adjust_nok=1.5) f =
  let next_delay = ref first_delay in
  let rec loop x =
    (match f x with
    | exception e ->
      if on e then (
        let delay = !next_delay in
        let delay = min delay max_delay in
        let delay = max delay min_delay in
        next_delay := !next_delay *. delay_adjust_nok ;
        !logger.error "Retryable error: %s, pausing %gs"
          (Printexc.to_string e) delay ;
        let%lwt () = Lwt_unix.sleep delay in
        loop x
      ) else (
        !logger.error "Something went wrong: %s"
          (Printexc.to_string e) ;
        fail e
      )
    | r ->
      next_delay := !next_delay *. delay_adjust_ok ;
      return r)
  in
  loop

let outputer_of rb_out sersize_of_tuple serialize_tuple =
  let once = output rb_out sersize_of_tuple serialize_tuple in
  let on = function
    (* FIXME: a dedicated RingBuf.NoMoreRoom exception *)
    | Failure _ ->
      !logger.debug "No more space in the ring buffer, sleeping..." ;
      true
    | _ -> false
  in
  retry ~on once

let read_csv_file filename separator sersize_of_tuple serialize_tuple tuple_of_strings =
  !logger.info "Starting READ CSV FILE process..." ;
  (* For tests, allow to overwrite what's specified in the operation: *)
  let filename = getenv ~def:filename "csv_filename"
  and separator = getenv ~def:separator "csv_separator"
  and rb_out_fname = getenv ~def:"/tmp/ringbuf_out" "output_ringbuf"
  and rb_out_sz = getenv ~def:"100" "input_ringbuf_size" |> int_of_string
  in
  !logger.debug "Will read CSV file %S using separator %S, and write \
                 output to ringbuffer %S (size is %d words)"
                filename separator rb_out_fname rb_out_sz ;
  let of_string line =
    let strings = String.nsplit line separator |> Array.of_list in
    tuple_of_strings strings
  in
  let rb_out = RingBuf.create rb_out_fname rb_out_sz in (* create? *)
  let outputer =
    outputer_of rb_out sersize_of_tuple serialize_tuple in
  CodeGenLib_IO.read_file_lines filename (fun line ->
    match of_string line with
    | exception e ->
      !logger.error "Cannot parse line %S: %s"
        line (Printexc.to_string e) ;
      return_unit ;
    | tuple -> outputer tuple)

let select read_tuple sersize_of_tuple serialize_tuple where select =
  !logger.info "Starting SELECT process..." ;
  let rb_in_fname = getenv ~def:"/tmp/ringbuf_in" "input_ringbuf"
  and rb_out_fname = getenv ~def:"/tmp/ringbuf_out" "output_ringbuf"
  and rb_out_sz = getenv ~def:"100" "output_ringbuf_size" |> int_of_string
  in
  !logger.debug "Will read ringbuffer %S and write output to \
                 ringbuffer %S (size is %d words)"
                rb_in_fname
                rb_out_fname rb_out_sz ;
  let%lwt rb_in = retry ~on:(fun _ -> true) ~min_delay:1.0 RingBuf.load rb_in_fname in
  let rb_out = RingBuf.create rb_out_fname rb_out_sz in (* create? *)
  let outputer =
    outputer_of rb_out sersize_of_tuple serialize_tuple in
  CodeGenLib_IO.read_ringbuf rb_in (fun tx ->
    let tuple = read_tuple tx in
    RingBuf.dequeue_commit tx ;
    if where tuple then outputer (select tuple) else return_unit)

type 'a aggr_value =
  { first_touched : float ;
    mutable last_touched : float ;
    mutable nb_entries : int ;
    mutable nb_successive : int ;
    mutable last_ev_count : int ; (* used for others.successive *)
    fields : 'a (* the record of aggregation values *) }

let aggregate read_tuple sersize_of_tuple serialize_aggr where key_of_input
              commit_when aggr_of_input update_aggr =
  !logger.info "Starting GROUP BY process..." ;
  let rb_in_fname = getenv ~def:"/tmp/ringbuf_in" "input_ringbuf"
  and rb_out_fname = getenv ~def:"/tmp/ringbuf_out" "output_ringbuf"
  and rb_out_sz = getenv ~def:"100" "output_ringbuf_size" |> int_of_string
  in
  !logger.debug "Will read ringbuffer %S and write output to \
                 ringbuffer %S (size is %d words)"
                rb_in_fname
                rb_out_fname rb_out_sz ;
  let%lwt rb_in = retry ~on:(fun _ -> true) ~min_delay:1.0 RingBuf.load rb_in_fname in
  let rb_out = RingBuf.create rb_out_fname rb_out_sz in (* create? *)
  let outputer =
    outputer_of rb_out sersize_of_tuple serialize_aggr in
  let h = Hashtbl.create 701
  and event_count = ref 0 (* used to fake others.count etc *)
  and last_key = ref None (* used for successive count *)
  in
  CodeGenLib_IO.read_ringbuf rb_in (fun tx ->
    let in_tuple = read_tuple tx in
    RingBuf.dequeue_commit tx ;
    if where in_tuple then (
      (* TODO: update any aggr *)
      let k = key_of_input in_tuple in
      let now = Unix.gettimeofday () in (* haha lol! *)
      (match Hashtbl.find h k with
      | exception Not_found ->
        Hashtbl.add h k {
            first_touched = now ;
            last_touched = now ;
            nb_entries = 1 ; nb_successive = 1 ;
            last_ev_count = !event_count ;
            fields = aggr_of_input in_tuple }
      | aggr ->
        aggr.last_touched <- now ;
        aggr.last_ev_count <- !event_count ;
        update_aggr aggr.fields in_tuple  ;
        if !last_key = Some k then
          aggr.nb_successive <- aggr.nb_successive + 1) ;
      last_key := Some k ;
      (* haha lol: TODO a heap of timeouts *)
      let to_output = ref [] in
      Hashtbl.filteri_inplace (fun _k aggr ->
          if commit_when in_tuple aggr then (
            to_output := aggr.fields :: !to_output ;
            false
          ) else true
        ) h ;
      Lwt_list.iter_p outputer !to_output
    ) else
      return_unit)
