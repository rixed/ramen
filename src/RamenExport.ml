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

(* History of past tuples:
 *
 * We store tuples output by exporting nodes both in RAM (for the last ones)
 * on disk (in a "history" subdir). Each file is numbered and a cursor in the
 * history of some node might be given by file number, tuple offset (the
 * count below). Since only the ramen process ever write or read those files
 * there is not much to worry about. For now those are merely Marshalled
 * array or array of scalar value (tuples in the record below). *)

let clean_archives history =
  while history.C.max_filenum - history.C.min_filenum > C.max_history_archives do
    let fname = history.C.dir ^"/"^ string_of_int history.C.min_filenum in
    !logger.debug "Deleting history archive %S" fname ;
    Unix.unlink fname ;
    Hashtbl.remove history.C.ts_cache history.C.min_filenum ;
    history.C.min_filenum <- history.C.min_filenum + 1
  done

let archive_history history =
  let fname = history.C.dir ^"/"^ string_of_int (history.C.max_filenum + 1) in
  !logger.debug "Saving history in %S" fname ;
  File.with_file_out ~mode:[`create; `trunc] fname (fun oc ->
    (* Since we do not "clean" the array but only the count field, it is
     * important not to save anything beyond count: *)
    (if history.C.count == Array.length history.C.tuples then
       history.C.tuples
     else (
      !logger.info "Saving an incomplete history of only %d tuples in %S"
        history.C.count fname ;
      Array.sub history.C.tuples 0 history.C.count)
    ) |>
    Marshal.output oc) ;
  history.C.max_filenum <- history.C.max_filenum + 1 ;
  if history.C.min_filenum = -1 then
    history.C.min_filenum <- history.C.max_filenum ;
  clean_archives history

let read_archive dir filenum : scalar_value array array option =
  let fname = dir ^"/"^ string_of_int filenum in
  !logger.debug "Reading history from %S" fname ;
  try Some (File.with_file_in fname Marshal.input)
  with Sys_error _ -> None

let add_tuple node tuple =
  let history = Option.get node.N.history in
  if history.C.count >= Array.length history.C.tuples then (
    archive_history history ; (* Will update filenums *)
    history.C.count <- 0
  ) ;
  let idx = history.C.count in
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

let import_tuples rb_name node =
  let open Lwt in
  let tuple_type = C.tup_typ_of_temp_tup_type node.N.out_type in
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
        add_tuple node tuple ;
        Lwt_main.yield ()
      done)
    (function Canceled ->
      !logger.info "Import from %s was cancelled" rb_name ;
      RingBuf.unload rb ;
      return_unit
            | e -> fail e)

let fold_tuples ?min_filenum ?max_filenum ?(since=0) ?(max_res=1000) node init f =
  let history = Option.get node.N.history in
  let min_filenum = Option.default history.C.min_filenum min_filenum
  and max_filenum = Option.default (history.C.max_filenum+1) max_filenum in
  let min_filenum = max history.C.min_filenum min_filenum
  and max_filenum = min (history.C.max_filenum+1) max_filenum in

  let rec loop_tup tuples filenum last_idx idx nb_res x =
    if idx >= last_idx || nb_res >= max_res then
      idx, nb_res, x else
    let tuple = tuples.(idx) in
    loop_tup tuples filenum last_idx (idx+1) (nb_res+1) (f filenum tuple x)
  in

  let rec loop_file has_gap filenum first_idx nb_res x =
    !logger.debug "loop_file has_gap=%b filenum=%d first_idx=%d nb_res=%d"
      has_gap filenum first_idx nb_res ;
    if nb_res >= max_res || filenum > max_filenum then
      filenum, first_idx, x else
    let has_gap, tuples, last_idx =
      if filenum = history.C.max_filenum + 1 then
        has_gap, history.C.tuples, history.C.count
      else
        match read_archive history.C.dir filenum with
        | None -> true, [||], 0
        | Some tuples -> has_gap, tuples, Array.length tuples in
    let idx, nb_res, x = loop_tup tuples filenum last_idx first_idx nb_res x in
    (* If we are done we want to return with current idx and filenum in
     * case it is not completed yet: *)
    if nb_res >= max_res then filenum, idx, x else
    loop_file has_gap (filenum+1) 0 nb_res x
  in

  let filenum = since / C.max_history_block_length
  and first_idx = since mod C.max_history_block_length in
  let has_gap, filenum, first_idx =
    if filenum < min_filenum then
      true, min_filenum, 0
    else false, filenum, first_idx in
  let filenum, idx, x =
    loop_file has_gap filenum first_idx 0 init in
  assert (idx < C.max_history_block_length) ;
  filenum * C.max_history_block_length + idx, x

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
let columns_of_tuples tuple_type values =
  let values = Array.of_list values in
  let nb_values = Array.length values in
  let get_val ci i =
    let inv_i = Array.length values - 1 - i in
    values.(inv_i).(ci)
  in
  List.mapi (fun ci ft ->
      (* If the values are nullable then we add a column of bits (1=value is
       * present). We first check the presence of values and set this bitmask
       * so that we know how long our vector of values needs to be. *)
      if ft.nullable then (
        let nullmask = RamenBitmask.make nb_values in
        let val_idx_to_tuple_idx = Array.create nb_values ~-1 in
        let nb_set =
          Array.fold_lefti (fun nb_set i _value ->
              match get_val ci i with
              | VNull -> nb_set
              | _ ->
                RamenBitmask.set nullmask i ;
                val_idx_to_tuple_idx.(nb_set) <- i ;
                nb_set + 1
            ) 0 values in
        let column =
          scalar_column_init ft.typ nb_set (fun i ->
            get_val ci val_idx_to_tuple_idx.(i)) in
        ft.typ_name, Some nullmask, column
      ) else (
        ft.typ_name, None,
        scalar_column_init ft.typ nb_values (get_val ci)
      )
    ) tuple_type

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
  let find_field n =
    match Hashtbl.find node.N.out_type.C.fields n with
    | exception Not_found ->
      failwith ("field "^ n ^" does not exist")
    | rank, _field_typ -> Option.get !rank in
  let ti = find_field start_field
  and vi = find_field data_field in
  if max_data_points < 1 then failwith "invalid max_data_points" ;
  let dt = (to_ -. from) /. float_of_int max_data_points in
  let buckets = Array.init max_data_points (fun _ ->
    { count = 0 ; sum = 0. ; min = max_float ; max = min_float }) in
  let bucket_of_time t = int_of_float ((t -. from) /. dt) in
  let history = Option.get node.N.history in
  let f_opt f x y = match x, y with
    | None, y -> Some y
    | Some x, y -> Some (f x y) in
  let min_filenum, max_filenum =
    Hashtbl.enum history.C.ts_cache |>
    Enum.fold (fun (mi, ma) (filenum, (ts_min, ts_max)) ->
      (if from >= ts_min then f_opt max mi filenum else mi),
      (if to_ <= ts_max then f_opt min ma filenum else ma))
      (None, None) in
  let _ =
    fold_tuples ?min_filenum ?max_filenum
      node (-1, max_float, min_float)
      (fun filenum tup (prev_filenum, tmin, tmax) ->
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
        (* Maybe update ts_cache *)
        let tmin, tmax =
          if filenum = prev_filenum then (
            min tmin t1, max tmax t2
          ) else (
            (* Do save only if the filenum is an archive: *)
            if filenum >= history.C.min_filenum &&
               filenum <= history.C.max_filenum
            then (
              Hashtbl.modify_opt filenum (function
                | None ->
                  !logger.debug "Caching times for filenum %d to %g..%g"
                    filenum tmin tmax ;
                  Some (tmin, tmax)
                | x -> x) history.C.ts_cache
            ) ;
            t1, t2
          ) in
        (* t1 and t2 are in secs. But the API is in milliseconds (thanks to
         * grafana) *)
        let t1, t2 = t1 *. 1000., t2 *. 1000. in
        if t1 < to_ && t2 >= from then (
          let bi1 = bucket_of_time t1 and bi2 = bucket_of_time t2 in
          for bi = bi1 to bi2 do
            add_into_bucket buckets bi v
          done
        ) ;
        filenum, tmin, tmax) in
  Array.mapi (fun i _ ->
    from +. dt *. (float_of_int i +. 0.5)) buckets,
  Array.map consolidation buckets
