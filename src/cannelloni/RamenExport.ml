open Batteries
open RamenLog
open RamenSharedTypes
module C = RamenConf
module N = RamenConf.Node
open Stdint

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

let add_tuple node tuple_type tuple =
  match node.N.history with
  | None ->
    node.N.history <- Some
      C.{ tuples = Array.init history_length (fun i ->
              if i = 0 then tuple else [||]) ;
              tuple_type ;
          count = 1 }
  | Some history ->
    let idx = history.C.count mod Array.length history.C.tuples in
    history.C.tuples.(idx) <- tuple ;
    history.C.count <- history.C.count + 1

let read_tuple tuple_type tx =
  (* First read the nullmask *)
  let nullmask_size =
    RingBufLib.nullmask_bytes_of_tuple_type tuple_type in
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
    | TEth    -> VEth (read_eth tx offs)
    | TIpv4   -> VIpv4 (read_u32 tx offs)
    | TIpv6   -> VIpv6 (read_u128 tx offs)
    | TCidrv4 -> VCidrv4 (read_cidr4 tx offs)
    | TCidrv6 -> VCidrv6 (read_cidr6 tx offs)
    | TNull   -> VNull
    | TNum | TAny -> assert false
  and sersize_of =
    function
    | _, VString s ->
      RingBufLib.(rb_word_bytes + round_up_to_rb_word(String.length s))
    | typ, _ ->
      RingBufLib.sersize_of_fixsz_typ typ
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

let import_tuples rb_name node tuple_type =
  let open Lwt in
  !logger.info "Starting to import output from node %s (in ringbuf %S)"
    (N.fq_name node) rb_name ;
  let rb = RingBuf.load rb_name in
  catch
    (fun () ->
      let dequeue =
        RingBufLib.retry_for_ringbuf RingBuf.dequeue_alloc in
      while%lwt true do
        let%lwt tx = dequeue rb in
        let tuple, _sz = read_tuple tuple_type tx in
        RingBuf.dequeue_commit tx ;
        add_tuple node tuple_type tuple ;
        Lwt_main.yield ()
      done)
    (function Canceled ->
      !logger.info "Import from %s was cancelled" rb_name ;
      RingBuf.unload rb ;
      return_unit
            | e -> fail e)

let get_history node =
  Option.default_delayed (fun () ->
      (* Build a fake empty history - this requires the node to be typed. *)
      C.{ tuple_type = C.tup_typ_of_temp_tup_type node.C.Node.out_type ;
          tuples = [||] ; count = 0 }
    ) node.N.history

let get_field_types history =
  history.C.tuple_type

let fold_tuples ?(since=0) ?max_res history init f =
  if Array.length history.C.tuples = 0 then (
    since, init
  ) else (
    let nb_tuples = min history.C.count (Array.length history.C.tuples) in
    let nb_res = match max_res with
      | Some r -> min r nb_tuples
      | None -> nb_tuples in
    let since = max since (history.C.count - Array.length history.C.tuples) in
    let first_idx = since mod Array.length history.C.tuples in
    let rec loop prev i r =
      if r <= 0 then prev else (
        let i = if i < Array.length history.C.tuples then i else 0 in
        let tuple = history.C.tuples.(i) in
        let prev =
          if Array.length tuple > 0 then f tuple prev else prev in
        loop prev (i + 1) (r - 1)
      ) in
    since, loop init first_idx nb_res
  )

let scalar_column_init typ len f =
  match typ with
  | TNull -> ANull len
  | TFloat -> AFloat (Array.init len (fun i -> match f i with VFloat x -> x | _ -> assert false))
  | TString -> AString (Array.init len (fun i -> match f i with VString x -> x | _ -> assert false))
  | TBool -> ABool (Array.init len (fun i -> match f i with VBool x -> x | _ -> assert false))
  | TU8 -> AU8 (Array.init len (fun i -> match f i with VU8 x -> x | _ -> assert false))
  | TU16 -> AU16 (Array.init len (fun i -> match f i with VU16 x -> x | _ -> assert false))
  | TU32 -> AU32 (Array.init len (fun i -> match f i with VU32 x -> x | _ -> assert false))
  | TU64 -> AU64 (Array.init len (fun i -> match f i with VU64 x -> x | _ -> assert false))
  | TU128 -> AU128 (Array.init len (fun i -> match f i with VU128 x -> x | _ -> assert false))
  | TI8 -> AI8 (Array.init len (fun i -> match f i with VI8 x -> x | _ -> assert false))
  | TI16 -> AI16 (Array.init len (fun i -> match f i with VI16 x -> x | _ -> assert false))
  | TI32 -> AI32 (Array.init len (fun i -> match f i with VI32 x -> x | _ -> assert false))
  | TI64 -> AI64 (Array.init len (fun i -> match f i with VI64 x -> x | _ -> assert false))
  | TI128 -> AI128 (Array.init len (fun i -> match f i with VI128 x -> x | _ -> assert false))
  | TEth -> AEth (Array.init len (fun i -> match f i with VEth x -> x | _ -> assert false))
  | TIpv4 -> AIpv4 (Array.init len (fun i -> match f i with VIpv4 x -> x | _ -> assert false))
  | TIpv6 -> AIpv6 (Array.init len (fun i -> match f i with VIpv6 x -> x | _ -> assert false))
  | TCidrv4 -> ACidrv4 (Array.init len (fun i -> match f i with VCidrv4 x -> x | _ -> assert false))
  | TCidrv6 -> ACidrv6 (Array.init len (fun i -> match f i with VCidrv6 x -> x | _ -> assert false))
  | TNum | TAny -> assert false

(* Note: the list of values is ordered latest to oldest *)
let columns_of_tuples fields values =
  let values = Array.of_list values in
  List.mapi (fun ci ft ->
      ft.typ_name, ft.nullable,
      scalar_column_init ft.typ (Array.length values) (fun i ->
        let inv_i = Array.length values - 1 - i in
        values.(inv_i).(ci))
    ) fields

(* Garbage in / garbage out *)
let float_of_scalar_value = function
  | VFloat x -> x
  | VBool x -> if x then 1. else 0.
  | VU8 x -> Uint8.to_float x
  | VU16 x -> Uint16.to_float x
  | VU32 x -> Uint32.to_float x
  | VU64 x -> Uint64.to_float x
  | VU128 x -> Uint128.to_float x
  | VI8 x -> Int8.to_float x
  | VI16 x -> Int16.to_float x
  | VI32 x -> Int32.to_float x
  | VI64 x -> Int64.to_float x
  | VI128 x -> Int128.to_float x
  | VEth x -> Uint48.to_float x
  | VIpv4 x -> Uint32.to_float x
  | VIpv6 x -> Uint128.to_float x
  | VNull | VString _ | VCidrv4 _ | VCidrv6 _ -> 0.

(* Building timeseries with points at regular times *)

type timeserie_bucket =
  (* Hopefully count will be small enough that sum can be tracked accurately *)
  { mutable count : int ; mutable sum : float ;
    mutable min : float ; mutable max : float }

let add_into_bucket b i v =
  if i > 0 && i < Array.length b then (
    b.(i).count <- succ b.(i).count ;
    b.(i).min <- min b.(i).min v ;
    b.(i).max <- max b.(i).max v ;
    b.(i).sum <- b.(i).sum +. v)

let bucket_avg b =
  if b.count = 0 then None else Some (b.sum /. float_of_int b.count)
let bucket_min b =
  if b.count = 0 then None else Some b.min
let bucket_max b =
  if b.count = 0 then None else Some b.max

let build_timeseries node start_field start_scale data_field duration_info
                     max_data_points from to_ consolidation =
  let history = get_history node in
  let find_field n =
    try (
      List.findi (fun _i (ft : field_typ) ->
        ft.typ_name = n) history.C.tuple_type |> fst
    ) with Not_found ->
      failwith ("field "^ n ^" does not exist") in
  let ti = find_field start_field
  and vi = find_field data_field in
  if max_data_points < 1 then failwith "invalid max_data_points" ;
  let dt = (to_ -. from) /. float_of_int max_data_points in
  let buckets = Array.init max_data_points (fun _ ->
    { count = 0 ; sum = 0. ; min = max_float ; max = min_float }) in
  let bucket_of_time t = int_of_float ((t -. from) /. dt) in
  let _ =
    fold_tuples history () (fun tup () ->
      let t, v = float_of_scalar_value tup.(ti),
                 float_of_scalar_value tup.(vi) in
      let t1 = t *. start_scale in
      let t2 =
        let open Lang.Operation in
        match duration_info with
        | DurationConst f -> t1 +. f
        | DurationField (f, s) ->
          let fi = find_field f in
          t1 +. float_of_scalar_value tup.(fi) *. s
        | StopField (f, s) ->
          let fi = find_field f in
          float_of_scalar_value tup.(fi) *. s
      in
      (* We allow duration to be < 0 *)
      let t1, t2 = if t2 >= t1 then t1, t2 else t2, t1 in
      (* t1 and t2 are in secs. But the API is in milliseconds (thanks to
       * grafana) *)
      let t1, t2 = t1 *. 1000., t2 *. 1000. in
      if t1 < to_ && t2 >= from then
        let bi1 = bucket_of_time t1 and bi2 = bucket_of_time t2 in
        for bi = bi1 to bi2 do
          add_into_bucket buckets bi v
        done) in
  Array.mapi (fun i _ ->
    from +. dt *. (float_of_int i +. 0.5)) buckets,
  Array.map consolidation buckets
