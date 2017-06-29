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

(* Functions *)

(* We are not allowed to have any state specific to this function.
 * Consequently we must compute the sequence number from the start
 * and increment and the global tuple count. *)
let sequence start inc =
  Int128.(start + !CodeGenLib_IO.tuple_count * inc)

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

let outputer_of rb_outs sersize_of_tuple serialize_tuple =
  let outputers_with_retry = List.map (fun rb_out ->
        let once = output rb_out sersize_of_tuple serialize_tuple in
        let on = function
          (* FIXME: a dedicated RingBuf.NoMoreRoom exception *)
          | Failure _ ->
            !logger.debug "No more space in the ring buffer, sleeping..." ;
            true
          | _ -> false
        in
        CodeGenLib_IO.retry_for_ringbuf ~on once
      ) rb_outs in
  fun tuple ->
    List.map (fun out -> out tuple) outputers_with_retry |>
    Lwt.join

(* Each node can write in several ringbuffers (one per children) which
 * names are given by the output_ringbuf envvar followed by the child number
 * as an extension. *)
let load_out_ringbufs () =
  let rb_out_fnames = getenv ~def:"/tmp/ringbuf_out" "output_ringbufs" |> String.split_on_char ','
  in
  !logger.info "Will output into %a" (List.print String.print) rb_out_fnames ;
  List.map (fun fname -> RingBuf.load fname) rb_out_fnames

let read_csv_file filename separator sersize_of_tuple serialize_tuple tuple_of_strings =
  !logger.info "Starting READ CSV FILE process..." ;
  (* For tests, allow to overwrite what's specified in the operation: *)
  let filename = getenv ~def:filename "csv_filename"
  and separator = getenv ~def:separator "csv_separator"
  in
  !logger.debug "Will read CSV file %S using separator %S"
                filename separator ;
  let of_string line =
    let strings = String.nsplit line separator |> Array.of_list in
    tuple_of_strings strings
  in
  let rb_outs = load_out_ringbufs () in
  let outputer =
    outputer_of rb_outs sersize_of_tuple serialize_tuple in
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
  in
  !logger.debug "Will read ringbuffer %S" rb_in_fname ;
  let rb_outs = load_out_ringbufs () in
  let%lwt rb_in =
    CodeGenLib_IO.retry ~on:(fun _ -> true) ~min_delay:1.0 RingBuf.load rb_in_fname in
  let outputer =
    outputer_of rb_outs sersize_of_tuple serialize_tuple in
  CodeGenLib_IO.read_ringbuf rb_in (fun tx ->
    let tuple = read_tuple tx in
    RingBuf.dequeue_commit tx ;
    if where tuple then outputer (select tuple) else return_unit)

let yield sersize_of_tuple serialize_tuple select =
  !logger.info "Starting YIELD process..." ;
  let rb_outs = load_out_ringbufs () in
  let outputer =
    outputer_of rb_outs sersize_of_tuple serialize_tuple in
  let rec loop () =
    outputer (select ()) >>= loop in
  loop ()

type ('a, 'b, 'c) aggr_value =
  { first_touched : float ;
    (* used to compute the actual selected field when outputing the
     * aggregate: *)
    first_in : 'b ; (* first in-tuple of this aggregate *)
    mutable last_in : 'b ; (* last in-tuple of this aggregate *)
    mutable previous_out : 'c ; (* previously computed temp out tuple, if any *)
    mutable last_touched : float ;
    mutable nb_entries : int ;
    mutable nb_successive : int ;
    mutable last_ev_count : int ; (* used for others.successive *)
    fields : 'a (* the record of aggregation values *) }

let aggregate (read_tuple : RingBuf.tx -> 'tuple_in)
              (sersize_of_tuple : 'tuple_out -> int)
              (serialize_tuple : RingBuf.tx -> 'tuple_out -> int)
              (tuple_of_aggr : 'aggr -> 'tuple_in -> 'tuple_in -> 'tuple_in -> 'tuple_out)
              (* Where_fast/slow: premature optimisation: if the where filter
               * uses the aggregate then we need where_slow (checked after
               * the aggregate look up) but if it uses only the incoming
               * tuple then we can use only where_fast. *)
              (where_fast : 'tuple_in -> bool)
              (where_slow : 'aggr -> 'tuple_in -> 'tuple_in -> 'tuple_in -> bool)
              (key_of_input : 'tuple_in -> 'key)
              (commit_when : 'aggr -> 'tuple_in -> 'tuple_in -> 'tuple_in -> 'tuple_out -> 'tuple_out -> bool)
              (flush_when : 'aggr -> 'tuple_in -> 'tuple_in -> 'tuple_in -> 'tuple_out -> 'tuple_out -> bool)
              (aggr_init : 'tuple_in -> 'aggr)
              (update_aggr : 'aggr -> 'tuple_in -> unit) =
  !logger.info "Starting GROUP BY process..." ;
  let rb_in_fname = getenv ~def:"/tmp/ringbuf_in" "input_ringbuf"
  in
  !logger.debug "Will read ringbuffer %S" rb_in_fname ;
  let rb_outs = load_out_ringbufs () in
  let%lwt rb_in =
    CodeGenLib_IO.retry ~on:(fun _ -> true) ~min_delay:1.0 RingBuf.load rb_in_fname in
  let commit =
    outputer_of rb_outs sersize_of_tuple serialize_tuple in
  let h = Hashtbl.create 701
  and event_count = ref 0 (* used to fake others.count etc *)
  and last_key = ref None (* used for successive count *)
  in
  CodeGenLib_IO.read_ringbuf rb_in (fun tx ->
    let in_tuple = read_tuple tx in
    RingBuf.dequeue_commit tx ;
    if where_fast in_tuple then (
      (* TODO: update any aggr *)
      let k = key_of_input in_tuple in
      let now = Unix.gettimeofday () in (* haha lol! *)
      let prev_last_key = !last_key in
      last_key := Some k ;
      match Hashtbl.find h k with
      | exception Not_found ->
        let fields = aggr_init in_tuple in
        if where_slow fields in_tuple in_tuple in_tuple then (
          let out_tuple =
            tuple_of_aggr fields in_tuple in_tuple in_tuple in
          let do_commit, do_flush =
            if commit_when fields in_tuple in_tuple in_tuple out_tuple out_tuple then (
              true,
              flush_when == commit_when ||
              flush_when fields in_tuple in_tuple in_tuple out_tuple out_tuple
            ) else (
              false,
              not (flush_when == commit_when) &&
              flush_when fields in_tuple in_tuple in_tuple out_tuple out_tuple
            ) in
          if not do_flush then (
            let aggr = {
              first_touched = now ;
              first_in = in_tuple ;
              last_in = in_tuple ;
              previous_out = out_tuple ;
              last_touched = now ;
              nb_entries = 1 ; nb_successive = 1 ;
              last_ev_count = !event_count ;
              fields = aggr_init in_tuple } in
            Hashtbl.add h k aggr
          ) ;
          if do_commit then commit out_tuple  else return_unit
        ) else return_unit
      | aggr ->
        if where_slow aggr.fields in_tuple aggr.first_in aggr.last_in then (
          update_aggr aggr.fields in_tuple ;
          aggr.last_touched <- now ;
          aggr.last_ev_count <- !event_count ;
          if prev_last_key = Some k then
            aggr.nb_successive <- aggr.nb_successive + 1 ;
          let out_tuple =
            tuple_of_aggr aggr.fields in_tuple aggr.first_in aggr.last_in in
          let do_commit, do_flush =
            if commit_when aggr.fields in_tuple aggr.first_in aggr.last_in out_tuple aggr.previous_out then (
              true,
              flush_when == commit_when ||
              flush_when aggr.fields in_tuple aggr.first_in aggr.last_in out_tuple aggr.previous_out
            ) else (
              false,
              not (flush_when == commit_when) &&
              flush_when aggr.fields in_tuple aggr.first_in aggr.last_in out_tuple aggr.previous_out
            ) in
          if do_flush then
            Hashtbl.remove h k
          else (
            aggr.last_in <- in_tuple ;
            aggr.previous_out <- out_tuple ;
          ) ;
          if do_commit then commit out_tuple else return_unit
        ) else return_unit
    ) else return_unit
    (* FIXME: some commit conditions require much more thoughts than that *)
  )

let alert read_tuple field_of_tuple team subject text =
  !logger.info "Starting ALERT process..." ;
  let rb_in_fname = getenv ~def:"/tmp/ringbuf_in" "input_ringbuf"
  in
  !logger.debug "Will read ringbuffer %S" rb_in_fname ;
  let%lwt rb_in =
    CodeGenLib_IO.retry ~on:(fun _ -> true) ~min_delay:1.0 RingBuf.load rb_in_fname in
  let expand_fields =
    let open Str in
    let re = regexp "\\${\\(in\\.\\)?\\([_a-zA-Z0-9]+\\)}" in
    fun text tuple ->
      global_substitute re (fun s ->
          let field_name = matched_group 2 s in
          try field_of_tuple tuple field_name
          with Not_found ->
            !logger.error "Field %S used in alert text substitution is not \
                           present in the input!" field_name ;
            "??"^ field_name ^"??"
        ) text
  in
  CodeGenLib_IO.read_ringbuf rb_in (fun tx ->
    let tuple = read_tuple tx in
    RingBuf.dequeue_commit tx ;
    let team = expand_fields team tuple
    and subject = expand_fields subject tuple
    and text = expand_fields text tuple in
    (* TODO: send this to the alert manager *)
    Printf.printf "ALERT!\nTo: %s\nSubject: %s\n%s\n\n"
      team subject text ;
    return_unit)
