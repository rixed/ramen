(* Tools used by the generated OCaml code *)
open Batteries
open Stdint
open Log

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

let read_csv_file filename separator sersize_of_tuple serialize_tuple tuple_of_strings =
  (* For tests, allow to overwrite what's specified in the operation: *)
  let filename = getenv ~def:filename "csv_filename"
  and separator = getenv ~def:separator "csv_separator"
  and rb_fname = getenv ~def:"/tmp/ringbuf_csv_file" "input_ringbuf"
  and rb_word_sz = getenv ~def:"10000" "ringbuf_word_size" |> int_of_string
  in
  !logger.info "Will read CSV file %S using separator %S, and write output to \
                ringbuffer %S (which size is %d words)"
               filename separator rb_fname rb_word_sz ;
  let of_string line =
    let strings = String.nsplit line separator |> Array.of_list in
    tuple_of_strings strings
  in
  let rb = RingBuf.create rb_fname rb_word_sz in (* create? *)
  let serializer = serialize rb sersize_of_tuple serialize_tuple in
  CodeGenLib_IO.read_file_lines filename of_string serializer

