open RamenLog
open Stdint
open Batteries
open Legacy.Unix
open RamenHelpers
open RamenConsts
module N = RamenName
module Files = RamenFiles

let now = ref (gettimeofday ())
let first_input = ref None
let last_input = ref None

let on_each_input_pre () =
  let t = gettimeofday () in
  now := t ;
  if !first_input = None then first_input := Some t ;
  last_input := Some t

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
     * rearranged from time to time to keep it simple for the callback. TODO *)
    let buffer = Bytes.create max_external_msg_size in
    let buffer_stop = ref 0 in
    finally
      (fun () ->
        !logger.debug "read_file: Finished reading %a" N.path_print filename ;
        close_file () ;
        RamenWatchdog.disable watchdog ;
        if do_unlink && preprocessor <> "" then Files.safe_unlink filename ;
        ignore (Gc.major_slice 0))
      (fun () ->
        (* Try to unlink the file as early as possible, because if the
         * worker crashes before the end its actually safer to skip some
         * messages than to process some twice, although an option to
         * control this would be nice (TODO).
         * If we used a preprocessor we must wait for EOF before
         * unlinking the file. *)
        if do_unlink && preprocessor = "" then
          Files.safe_unlink filename ;
        RamenWatchdog.enable watchdog ;
        let rec read_more has_more =
          let has_more =
            if has_more then
              let len = Bytes.length buffer - !buffer_stop in
              assert (len > 0) ;  (* Or buffer is too small *)
              !logger.debug "read_file: Unix.read @%d..+%d" !buffer_stop len ;
              let sz = Unix.read fd buffer !buffer_stop len in
              !logger.debug "read_file: Read %d bytes" sz ;
              buffer_stop := !buffer_stop + sz ;
              sz > 0
            else has_more
          in
          let consumed = k buffer 0 !buffer_stop has_more in
          !logger.debug "read_file: consumed %d bytes" consumed ;
          buffer_stop := !buffer_stop - consumed ;
          Bytes.blit buffer consumed buffer 0 !buffer_stop ;
          if while_ () && (has_more || !buffer_stop > 0) then
            read_more has_more in
        read_more true
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
  let of_string line =
    strings_of_csv separator may_quote escape_seq line |>
    tuple_of_strings
  in
  fun k buffer start stop ->
    (* FIXME: make strings_of_csv works on bytes from start to stop *)
    let line = Bytes.(sub buffer start (stop - start) |> to_string) in
    !logger.debug "tuple_of_csv_line: new line: %S" line ;
    match of_string line with
    | exception e ->
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
  !logger.debug "Import all messages from Kafka..." ;
  if !watchdog = None then watchdog :=
    Some (RamenWatchdog.make ~timeout:300. "read file" quit_flag) ;
  let watchdog = Option.get !watchdog in
  let queue = Kafka.new_queue consumer in
  List.iter (fun partition ->
    Kafka.consume_start_queue queue topic partition offset
  ) partitions ;
  let consume_message str =
    let buffer = Bytes.of_string str in
    let rec loop i =
      if i < Bytes.length buffer && while_ () then (
        let consumed = k buffer i (Bytes.length buffer) false in
        !logger.debug "consume_message: consumed %d bytes" consumed ;
        loop (i + consumed)
      ) in
    RamenWatchdog.enable watchdog ;
    loop 0 ;
    RamenWatchdog.disable watchdog
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
        !logger.debug "Reached the end of partition %d at offset %Ld"
          partition offset
    | Kafka.Message (_topic, partition, offset, value, key_opt) ->
        !logger.debug
          "New Kafka value @%Ld of size %d for key %a on partition %d"
          offset
          (String.length value)
          (Option.print String.print) key_opt
          partition ;
        consume_message value) ;
    (* TODO: store the offset *)
    if while_ () then read_more ()
  in
  read_more () ;
  Kafka.destroy_queue queue ;
  Kafka.destroy_handler consumer ;
  Kafka.destroy_topic topic
