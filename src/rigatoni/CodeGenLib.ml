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
  with e ->
    (match def with
    | Some d -> d
    | None ->
      Printf.sprintf "Cannot find envvar %s: %s" n (Printexc.to_string e) |>
      failwith)

let serialize rb sersize_of_tuple serialize_tuple tuple =
  let open RingBuf in
  let sersize = sersize_of_tuple tuple in
  let tx = enqueue_alloc rb sersize in
  let offs = serialize_tuple tx tuple in
  enqueue_commit tx ;
  assert (offs = sersize)

let retry f =
  let rec loop x =
    (match f x with
    (* FIXME: a dedicated RingBuf.NoMoreRoom exception *)
    | exception (Failure _) ->
      !logger.debug "No more space in the ring buffer, sleeping..." ;
      (* TODO: an automatic retry-er that tries to find out the best
       * amount of time to sleep based on successive errors *)
      let%lwt () = Lwt_unix.sleep 1. in
      loop x
    | exception e ->
      !logger.error "Something went wrong, skipping: %s"
        (Printexc.to_string e) ;
      return_unit
    | () -> return_unit)
  in
  loop

let read_csv_file filename separator sersize_of_tuple serialize_tuple tuple_of_strings =
  (* For tests, allow to overwrite what's specified in the operation: *)
  let filename = getenv ~def:filename "csv_filename"
  and separator = getenv ~def:separator "csv_separator"
  and rb_out_fname = getenv ~def:"/tmp/ringbuf_csv_file_out" "input_ringbuf"
  and rb_out_sz = getenv ~def:"10000" "input_ringbuf_size" |> int_of_string
  in
  !logger.info "Will read CSV file %S using separator %S, and write output to \
                ringbuffer %S (size is %d words)"
               filename separator rb_out_fname rb_out_sz ;
  let of_string line =
    let strings = String.nsplit line separator |> Array.of_list in
    tuple_of_strings strings
  in
  let rb_out = RingBuf.create rb_out_fname rb_out_sz in (* create? *)
  let serializer =
    let once = serialize rb_out sersize_of_tuple serialize_tuple in
    retry once in
  CodeGenLib_IO.read_file_lines filename (fun line ->
    match of_string line with
    | exception e ->
      !logger.error "Cannot parse line %S: %s"
        line (Printexc.to_string e) ;
      return_unit ;
    | tuple -> serializer tuple)

let select read_tuple sersize_of_tuple serialize_tuple where select =
  let rb_in_fname = getenv ~def:"/tmp/ringbuf_select_in" "input_ringbuf"
  and rb_out_fname = getenv ~def:"/tmp/ringbuf_select_out" "output_ringbuf"
  and rb_out_sz = getenv ~def:"10000" "output_ringbuf_size" |> int_of_string
  in
  !logger.info "Will read ringbuffer %S and write output to \
                ringbuffer %S (size is %d words)"
               rb_in_fname
               rb_out_fname rb_out_sz ;
  let rb_in = RingBuf.load rb_in_fname in
  let rb_out = RingBuf.create rb_out_fname rb_out_sz in (* create? *)
  let serializer =
    let once = serialize rb_out sersize_of_tuple serialize_tuple in
    retry once in
  CodeGenLib_IO.read_ringbuf rb_in (fun tx ->
    let tuple = read_tuple tx in
    if where tuple then serializer (select tuple) else return_unit)
