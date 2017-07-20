open Batteries
open BatOption.Infix
open Log
open RamenSharedTypes
module C = RamenConf

(* Possible solutions:
 *
 * 1. Simple for the node, complex for ramen:
 *
 * The export is a normal children for the exporting node, it mirror the
 * output tuple there as it would for any other child node. Ramen, thus, must
 * read a normal ringbuf although it has no generated code for this. So the
 * ringbuf header must be enriched with field names and types, and ramen must
 * be able to process the ringbuf fast enough.
 *
 * Pros:
 *
 * - we could use the same techno to display ringbuf content in ringbuf_ctl;
 * - ringbuf is smaller than if the client converted into another format, and
 *   we have a single format (the ringbuf message itself) for all client and
 *   the history we want to keep;
 * - does not slow down the nodes even a bit;
 * - does not make them any more complex;
 *
 * Cons:
 *
 * - slower than dedicated code to read the ringbuf from ramen;
 *
 * 2. Simple for ramen, complex for the nodes:
 *
 * The nodes know that they export and have a specific code to output the tuple
 * as a string in this special ringbuf. From there ramen can read, store and
 * serve those strings.
 *
 * Pros:
 *
 * - Fast for ramen;
 * - no need to mess with ringbuf header nor to try to understand it.
 *
 * Cons:
 *
 * - Bigger ringbufs;
 * - more work for the nodes;
 * - can have only one syntax for the websocket data (likely json).
 *
 * Let's go with 1.
 *)

(* Convert a tuple, given its type, into a JSON string *)
let json_of_tuple tuple_type tuple =
  (List.fold_lefti (fun s i typ ->
      s ^ (if i > 0 then "," else "") ^
      typ.typ_name ^":"^
      IO.to_string Lang.Scalar.print tuple.(i)
    ) "{" tuple_type) ^ "}"

(* Store history of past tuple output by a given node: *)
let history_length = 8096

type history =
  { tuple_type : Lang.Tuple.typ ;
    (* Store arrays of Scalar.values not hash of names to values ! *)
    tuples : Lang.Scalar.t array array ;
    (* Gives us both the position of the last tuple in the array and an index
     * in the stream of tuples to help polling *)
    mutable count : int }

let imported_tuples : (string, history) Hashtbl.t = Hashtbl.create 11

let add_tuple node_name tuple_type tuple =
  match Hashtbl.find imported_tuples node_name with
  | exception Not_found ->
    let history = { tuples = Array.init history_length (fun i ->
                      if i = 0 then tuple else [||]) ;
                    tuple_type ;
                    count = 0 } in
    Hashtbl.add imported_tuples node_name history
  | history ->
    let idx = history.count mod Array.length history.tuples in
    history.tuples.(idx) <- tuple ;
    history.count <- history.count + 1

let read_tuple tuple_type tx =
  let open Lang.Scalar in
  (* First read the nullmask *)
  let nullmask_size =
    CodeGen_OCaml.nullmask_bytes_of_tuple_type tuple_type in
  let read_single_value offs =
    let open RingBuf in
    function
    | TFloat  -> VFloat (read_float tx offs)
    | TString -> VString (read_string tx offs)
    | TBool   -> VBool (read_bool tx offs)
    | TU8     -> VU8 (read_u8 tx offs)
    | TU16    -> VU16 (read_u16 tx offs)
    | TU32    -> VU32 (read_u32 tx offs)
    | TU64    -> VU64 (read_u64 tx offs)
    | TU128   -> VU128 (read_u128 tx offs)
    | TI8     -> VI8 (read_i8 tx offs)
    | TI16    -> VI16 (read_i16 tx offs)
    | TI32    -> VI32 (read_i32 tx offs)
    | TI64    -> VI64 (read_i64 tx offs)
    | TI128   -> VI128 (read_i128 tx offs)
    | TNull   -> VNull
    | TNum    -> assert false
  and sersize_of =
    function
    | _, VString s ->
      RingBufLib.(rb_word_bytes + round_up_to_rb_word(String.length s))
    | typ, _ ->
      CodeGen_OCaml.sersize_of_fixsz_typ typ
  in
  (* Read all fields one by one *)
  let tuple_len = List.length tuple_type in
  let tuple = Array.make tuple_len VNull in
  let sz, _ =
    List.fold_lefti (fun (offs, b) i typ ->
        let value, offs', b' =
          if typ.nullable && not (RingBuf.get_bit tx b) then (
            None, offs, b+1
          ) else (
            let value = read_single_value offs typ.typ in
            let offs' = offs + sersize_of (typ.typ, value) in
            Some value, offs', b
          ) in
        Option.may (Array.set tuple i) value ;
        offs', b'
      ) (nullmask_size, 0) tuple_type in
  tuple, sz

let import_tuples rb_name node_name tuple_type =
  !logger.info "Starting to import output from node %S (in ringbuf %S)"
    node_name rb_name ;
  let rb = RingBuf.load rb_name in
  let dequeue =
    RingBufLib.retry_for_ringbuf RingBuf.dequeue_alloc in
  while%lwt true do
    let%lwt tx = dequeue rb in
    let tuple, sz = read_tuple tuple_type tx in
    RingBuf.dequeue_commit tx ;
    add_tuple node_name tuple_type tuple ;
    !logger.debug "Importing a tuple of %d bytes from %S: %s"
      sz node_name (json_of_tuple tuple_type tuple) ;
    Lwt_main.yield ()
  done

let get_history node_name =
  try Hashtbl.find imported_tuples node_name
  with Not_found ->
    raise (C.InvalidCommand ("Unknown node "^node_name))

let get_field_types node_name =
  let history = get_history node_name in
  Array.of_list history.tuple_type

let fold_tuples ?(since=0) ?max_res node_name init f =
  let history = get_history node_name in
  let max_res = max_res |? Array.length history.tuples - 1 in
  let since = max since (history.count - max_res) in
  let first_idx = since mod Array.length history.tuples in
  let last_idx = history.count mod Array.length history.tuples in
  let rec loop prev i =
    let i = if i < Array.length history.tuples then i else 0 in
    let tuple = history.tuples.(i) in
    let prev =
      if Array.length tuple > 0 then f prev tuple else prev in
    if i = last_idx then prev else loop prev (i + 1) in
  if
  first_idx = last_idx then init else loop init first_idx
