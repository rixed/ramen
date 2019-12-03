open RamenLog
open Stdint
open Batteries
open Legacy.Unix
open RamenHelpers
open RamenConsts
module N = RamenName
module Files = RamenFiles

(* [k] is the reader that's passed the chunks of external data
 * as a byte string, the offset and length to consume, and a flag
 * telling if more data is to be expected; And it answers with the
 * offset of the first byte not consumed, or raise Exit *)
let read_file ~while_ ~do_unlink filename preprocessor watchdog k =
  !logger.debug "read_file: Importing file %a" N.path_print filename ;
  let open_file =
    if preprocessor = "" then (
      fun () ->
        let fd = openfile (filename : N.path :> string) [ O_RDONLY ] 0o644 in
        fd, (fun () -> close fd)
    ) else (
      fun () ->
        let f = RamenHelpers.shell_quote (filename :> string) in
        let cmd =
          if String.exists preprocessor "%s" then
            String.nreplace preprocessor "%s" f
          else
            preprocessor ^" "^ f
        in
        (* We need to keep the chan for later destruction, as
         * in_channel_of_descr would create another one *)
        let chan = open_process_in cmd in
        descr_of_in_channel chan,
        fun () ->
          match close_process_in chan with
          | Unix.WEXITED 0 -> ()
          | s ->
              !logger.warning "CSV preprocessor %S %s"
                preprocessor (string_of_process_status s)
    ) in
  match open_file () with
  | exception e ->
    !logger.error "Cannot open file %a%s: %s, skipping."
      N.path_print filename
      (if preprocessor = "" then ""
       else (Printf.sprintf " through %S" preprocessor))
      (Printexc.to_string e)
  | fd, close_file ->
    !logger.debug "read_file: Start reading %a" N.path_print filename ;
    (* Everything is stored into a circular buffer of fixed size that is
     * rearranged from time to time to keep it simple for the callback. *)
    let buffer = Bytes.create read_buffer_size in
    finally
      (fun () ->
        !logger.debug "read_file: Finished reading %a" N.path_print filename ;
        close_file () ;
        RamenWatchdog.disable watchdog ;
        if do_unlink then Files.safe_unlink filename ;
        ignore (Gc.major_slice 0))
      (fun () ->
        (* Try to unlink the file as early as possible, because if the
         * worker crashes before the end its actually safer to skip some
         * messages than to process some twice, although an option to
         * control this would be nice (TODO).
         * If we used a preprocessor we must wait for EOF before
         * unlinking the file. *)
        RamenWatchdog.enable watchdog ;
        let rec read_more start stop has_more =
          (* TODO: read in a loop until buffer is full or not has_more *)
          let has_more, stop =
            let len = Bytes.length buffer - stop in
            if has_more && len > 0 then (
              !logger.debug "read_file: Unix.read @%d..+%d" stop len ;
              let sz = Unix.read fd buffer stop len in
              !logger.debug "read_file: Read %d bytes" sz ;
              sz > 0, stop + sz
            ) else
              has_more, stop
          in
          let consumed =
            if stop > start then
              try
                let consumed = k buffer start stop has_more in
                if consumed = 0 && not has_more then
                  raise (Failure "Reader is deadlooping") ;
                consumed
              with e ->
                let bt = Printexc.get_raw_backtrace () in
                let filename_save = N.cat filename (N.path ".bad") in
                !logger.error "While reading file %a: %s. Saving as %a."
                  N.path_print filename
                  (Printexc.to_string e)
                  N.path_print filename_save ;
                Files.cp filename filename_save ;
                Printexc.raise_with_backtrace e bt
            else
              0 in
          !logger.debug "read_file: consumed %d bytes" consumed ;
          let start = start + consumed in
          if while_ () && (has_more || stop > start) then
            let start, stop =
              if has_more &&
                 Bytes.length buffer - stop < max_external_msg_size
              then (
                Bytes.blit buffer start buffer 0 (stop - start) ;
                0, stop - start
              ) else
                start, stop in
            read_more start stop has_more in
        read_more 0 0 true
      ) ()

let check_file_exists kind kind_name path =
  !logger.debug "Checking %a is a %s..." N.path_print path kind_name ;
  match Files.safe_stat path with
  | exception Unix_error (ENOENT, _, _) ->
    Printf.sprintf2 "%a does not exist" N.path_print path |>
    failwith
  | stats ->
    if stats.st_kind <> kind then
      Printf.sprintf2 "Path %a is not a %s" N.path_print path kind_name |>
      failwith

let check_dir_exists = check_file_exists S_DIR "directory"

(* Helper to read lines out of data chunks, each line being then sent
 * to [k]. Return the number of consumed bytes: *)
let lines_of_chunks k buffer start stop has_more =
  match Bytes.index_from buffer start '\n' with
  | exception Not_found ->
      if not has_more then (
        (* Assume eol at eof: *)
        k buffer start stop ;
        stop - start
      ) else 0
  | i when i >= stop ->
      if not has_more then (
        k buffer start stop ;
        stop - start
      ) else 0
  | i ->
      k buffer start i ;
      (i + 1) - start

(* Helper to turn a CSV line into a tuple: *)
let tuple_of_csv_line separator may_quote escape_seq tuple_of_strings =
  let of_bytes buffer start stop =
    let consumed, strs =
      strings_of_csv separator may_quote escape_seq buffer start stop in
    if consumed < stop - start then
      !logger.warning "Consumed only %d bytes over %d"
        consumed
        (Bytes.length buffer) ;
    tuple_of_strings strs
  in
  fun k buffer start stop ->
    match of_bytes buffer start stop with
    | exception e ->
        let line = Bytes.(sub buffer start (stop - start) |> to_string) in
        !logger.error "Cannot parse line %S: %s"
          line (Printexc.to_string e)
    | tuple ->
        k tuple

(* Try hard not to create several instances of the same watchdog: *)
let watchdog = ref None

(* Calls [k] with buffered data repeatedly *)
let read_glob_file path preprocessor do_unlink quit_flag while_ k =
  let dirname = Filename.dirname path |> N.path
  and glob_str = Filename.basename path in
  let glob = Globs.compile glob_str in
  if !watchdog = None then watchdog :=
    Some (RamenWatchdog.make ~timeout:300. "read file" quit_flag) ;
  let watchdog = Option.get !watchdog in
  let import_file_if_match (filename : N.path) =
    if Globs.matches glob (filename :> string) then
      try
        read_file ~while_ ~do_unlink (N.path_cat [dirname ; filename ])
                  preprocessor watchdog k
      with exn ->
        !logger.error "Exception while reading file %a: %s\n%s"
          N.path_print filename
          (Printexc.to_string exn)
          (Printexc.get_backtrace ())
    else (
      !logger.debug "File %a is not interesting (glob is %S)."
        N.path_print filename glob_str
    ) in
  check_dir_exists dirname ;
  let handler = RamenFileNotify.make ~while_ dirname in
  !logger.debug "Import all files in dir %a..." N.path_print dirname ;
  RamenFileNotify.for_each (fun filename ->
    !logger.debug "New file %a in dir %a!"
      N.path_print filename N.path_print dirname ;
    import_file_if_match filename) handler

let read_kafka_topic consumer topic partitions offset quit_flag while_ k =
  !logger.info "Import all messages from %d Kafka partitions, topic %S..."
    (List.length partitions) (Kafka.topic_name topic) ;
  if !watchdog = None then watchdog :=
    Some (RamenWatchdog.make ~timeout:300. "read Kafka" quit_flag) ;
  let watchdog = Option.get !watchdog in
  let queue = Kafka.new_queue consumer in
  List.iter (fun partition ->
    Kafka.consume_start_queue queue topic partition offset
  ) partitions ;
  (* Default max Kafka message is ~1Mb.
   * We allow a single tuple to be cut in half by a message boundary, but do
   * not allow a tuple to be larger than a single message (some progress is
   * required at every received message). *)
  let buffer = ref (Bytes.create 0) in
  let buffer_len = ref 0 in  (* used size, as opposed to capacity *)
  let capacity () = Bytes.length !buffer in
  let append_msg str =
    let capa = capacity () in
    let str_len = String.length str in
    let new_len = !buffer_len + str_len in
    if new_len > capa then (
      let new_capa = capa + new_len * 2 in
      if new_capa > 50_000_000 then
        failwith "Reached max buffer size for a single tuple" ;
      !logger.info "New Kafka read buffer capacity: %d" new_capa ;
      let new_buffer = Bytes.create new_capa in
      Bytes.blit !buffer 0 new_buffer 0 !buffer_len ;
      buffer := new_buffer ;
    ) ;
    Bytes.blit_string str 0 !buffer !buffer_len str_len ;
    buffer_len := new_len in
  let consume_message str =
    if String.length str > 0 then (
      append_msg str ;
      let rec loop i =
        let rem_bytes = !buffer_len - i in
        if rem_bytes > 0 && while_ () then (
          (* As long as we managed to consume something, then it's
           * OK to fail now: *)
          let has_more = i > 0 in
          let consumed = k !buffer i !buffer_len has_more in
          !logger.debug "consume_message: consumed %d/%d bytes"
            consumed rem_bytes ;
          if consumed > 0 then
            loop (i + consumed)
          else i
        ) else i in
      RamenWatchdog.enable watchdog ;
      let consumed = loop 0 in
      RamenWatchdog.disable watchdog ;
      if consumed = 0 then
        failwith "Cannot decode anything from that whole message" ;
      let new_len = !buffer_len - consumed in
      Bytes.blit !buffer consumed !buffer 0 new_len ;
      buffer_len := new_len
    )
  in
  let timeout_ms = int_of_float (kafka_consume_timeout *. 1000.) in
  let rec read_more () =
    (match Kafka.consume_queue ~timeout_ms queue with
    | exception Kafka.Error (TIMED_OUT, _) ->
        ()
    | exception Kafka.Error (_, msg) ->
        !logger.error "Kafka consumer error: %s" msg ;
        Unix.sleepf (Random.float 1.)
    | Kafka.PartitionEnd (_topic, partition, offset) ->
        !logger.info "Reached the end of partition %d at offset %Ld"
          partition offset
    | Kafka.Message (_topic, partition, offset, value, key_opt) ->
        !logger.debug
          "New Kafka value @%Ld of size %d for key %a on partition %d"
          offset
          (String.length value)
          (Option.print String.print) key_opt
          partition ;
        log_and_ignore_exceptions ~what:"Reading a Kafka message"
          consume_message value) ;
    (* TODO: store the offset *)
    if while_ () then read_more ()
  in
  read_more () ;
  Kafka.destroy_queue queue ;
  Kafka.destroy_handler consumer ;
  Kafka.destroy_topic topic
