open Batteries
open RamenLog
open RamenSharedTypes
open RamenSharedTypesJS
module C = RamenConf
module N = RamenConf.Node
open Helpers
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
 * and on disk (in a "history" subdir). Each file is numbered and a cursor in
 * the history of some node might be given by file number, tuple offset (the
 * count below). Since only the ramen process ever write or read those files
 * there is not much to worry about. For now those are merely Marshalled
 * array or array of scalar value (tuples in the record below). *)

let history_block_length = 10_000 (* TODO: make it a parameter? *)

type history =
  { (* Store arrays of Scalar.values not hash of names to values !
     * TODO: ideally storing columns would be even better *)
    tuples : scalar_value array array ;
    (* Start seqnum of the next block: *)
    mutable block_start : int ;
    (* How many tuples we already have in memory: *)
    mutable count : int ;
    (* Files are saved with a name composed of block_start-block_end (aka
     * next block_start). *)
    (* dir is named after the node output type so that we won't run the risk
     * to read data that have been archived in a different format. Other than
     * that, we make no attempt to clean these archives even when the node is
     * edited. It's best if deletion is explicit (TODO: add an option in the
     * put layer command). Another way to get rid of the history is to rename
     * the node. Since we need output type to locate the history, a node
     * cannot have a history before it's typed. *)
    dir : string ;
    (* A cache to save first/last timestamps of each archive file we've
     * visited.  This assume the timestamps are always increasing. If it's
     * not the case the cache performances will be poor but returned data
     * should still be correct. *)
    ts_cache : (int * int, float * float) Hashtbl.t ;
    (* Not necessarily up to date but gives a lower bound: *)
    mutable nb_files : int ; (* Count items in the list below *)
    (* Filenum = starting * stopping sequence number, the list being ordered
     * by starting timestamp *)
    mutable filenums : (int * int) list }

(* Store history of past tuple output by a given node. Not stored in the
 * node itself because we do not want to serialize such a big data structure
 * for every HTTP query, nor "rollback" it when a graph change fails.
 * Indexed by node fq_name and output signature: *)
let imported_tuples : (string * string, history) Hashtbl.t =
  Hashtbl.create 11

let history_key node =
  let fq_name = N.fq_name node in
  let type_sign = C.type_signature_hash node.N.out_type in
  fq_name, type_sign

let clean_archives conf history =
  while history.nb_files > 0 &&
        history.nb_files > conf.C.max_history_archives do
    assert (history.filenums <> []) ;
    let filenum = List.hd history.filenums in
    let fname = C.archive_file history.dir filenum in
    !logger.debug "Deleting history archive %S" fname ;
    Unix.unlink fname ;
    Hashtbl.remove history.ts_cache filenum ;
    history.filenums <- List.tl history.filenums ;
    history.nb_files <- history.nb_files - 1
  done

let archive_history conf history =
  if conf.C.max_history_archives <= 0 then () else
  let filenum =
    history.block_start, history.block_start + history.count in
  let fname = C.archive_file history.dir filenum in
  !logger.debug "Saving history in %S" fname ;
  Helpers.mkdir_all ~is_file:true fname ;
  File.with_file_out ~mode:[`create; `trunc] fname (fun oc ->
    (* Since we do not "clean" the array but only the count field, it is
     * important not to save anything beyond count: *)
    (if history.count == Array.length history.tuples then
       history.tuples
     else (
      !logger.info "Saving an incomplete history of only %d tuples in %S"
        history.count fname ;
      Array.sub history.tuples 0 history.count)
    ) |>
    Marshal.output oc) ;
  (* FIXME: a ring data structure: *)
  history.filenums <- history.filenums @ [ filenum ] ;
  history.nb_files <- history.nb_files + 1 ;
  history.block_start <- history.block_start + history.count ;
  history.count <- 0 ;
  clean_archives conf history

let read_archive dir filenum =
  let fname = C.archive_file dir filenum in
  !logger.debug "Reading history from %S" fname ;
  try Some (File.with_file_in fname Marshal.input)
  with Sys_error _ -> None

let make_history conf node =
  let type_sign = C.type_signature_hash node.N.out_type in
  let dir = conf.C.persist_dir ^"/workers/history/"^ N.fq_name node
                               ^"/"^ type_sign in
  !logger.info "Creating history for node %S" (N.fq_name node) ;
  mkdir_all dir ;
  (* Note: this is OK to share this [||] since we use it only as a placeholder *)
  let tuples = Array.make history_block_length [||] in
  let nb_files, filenums, max_seqnum =
    Sys.readdir dir |>
    Array.fold_left (fun (nb_files, arcs, ma as prev) fname ->
        match Scanf.sscanf fname "%d-%d" (fun a b -> a, b) with
        | exception _ -> prev
        | _, stop as m ->
          nb_files + 1, m :: arcs, max stop ma
      ) (0, [], min_int) in
  let filenums =
    List.fast_sort (fun (m1, _) (m2, _) -> Int.compare m1 m2) filenums in
  let block_start =
    if nb_files = 0 then 0
    else (
      !logger.debug "%d archive files from %d up to %d"
        nb_files (fst (List.hd filenums)) max_seqnum ;
      max_seqnum) in
  { tuples ; block_start ; count = 0 ; dir ; nb_files ; filenums ;
    ts_cache = Hashtbl.create (conf.C.max_history_archives / 8) }

let add_tuple conf node tuple =
	let k = history_key node in
  let history =
    try Hashtbl.find imported_tuples k
    with Not_found ->
      let history = make_history conf node in
      Hashtbl.add imported_tuples k history ;
      history in
  if history.count >= Array.length history.tuples then
    archive_history conf history ;
  let idx = history.count in
  history.tuples.(idx) <- tuple ;
  history.count <- history.count + 1

let read_tuple tuple_type =
  (* First read the nullmask *)
  let nullmask_size =
    RingBufLib.nullmask_bytes_of_tuple_type tuple_type in
  fun tx ->
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
    (* This works because the node is running and therefore the tuples
     * has been cleared from private fields already. *)
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
              Some value, offs', if typ.nullable then b+1 else b
            ) in
          Option.may (Array.set tuple i) value ;
          offs', b'
        ) (nullmask_size, 0) tuple_type in
    tuple, sz

let import_tuples conf rb_name node =
  let open Lwt in
  let tuple_type = C.tup_typ_of_temp_tup_type node.N.out_type in
  !logger.debug "Starting to import output from node %s (in ringbuf %S), which outputs %a"
    (N.fq_name node) rb_name C.print_temp_tup_typ node.N.out_type ;
  let%lwt rb = wrap (fun () -> RingBuf.load rb_name) in
  catch
    (fun () ->
      let dequeue =
        RingBufLib.retry_for_ringbuf RingBuf.dequeue_alloc in
      while%lwt true do
        let%lwt tx = dequeue rb in
        let tuple, _sz = read_tuple tuple_type tx in
        RingBuf.dequeue_commit tx ;
        wrap (fun () -> add_tuple conf node tuple) >>=
        Lwt_main.yield
      done)
    (function Canceled ->
      !logger.info "Import from %s was cancelled" rb_name ;
      wrap (fun () -> RingBuf.unload rb)
            | e ->
      !logger.error "Importing tuples failed with %s,\n%s"
        (Printexc.to_string e)
        (Printexc.get_backtrace ()) ;
      fail e)

let filenum_print oc (sta, sto) = Printf.fprintf oc "%d-%d" sta sto

(* Return the first and last filenums (or current block limits), or None
 * if we haven't received any value yet *)
let hist_min_max history =
  let current_filenum =
    history.block_start,
    history.block_start + history.count in
  if history.filenums = [] then
    if history.count = 0 then None
    else Some (current_filenum, current_filenum)
  else
    Some (
      List.hd history.filenums,
      if history.count = 0 then
        List.last history.filenums
      else current_filenum)

(* filenums are archive files (might not exist anymore though.
 * do_fold_tuples will also look into live tuples if required to reach max_res. *)
let do_fold_tuples filenums first_idx max_res history init f =
  !logger.debug "will fold over filenums=%a, first_idx=%d, up to %d results"
    (List.print filenum_print) filenums first_idx max_res ;
  let rec loop_tup tuples filenum block_start last_idx idx nb_res x =
    if idx >= last_idx || nb_res >= max_res then
      nb_res, x else
    let tuple = tuples.(idx) in
    let x = f filenum (block_start + idx) tuple x in
    loop_tup tuples filenum block_start last_idx (idx+1) (nb_res+1) x
  in
  let rec loop_file filenums first_idx nb_res x =
    !logger.debug "loop_file first_idx=%d nb_res=%d" first_idx nb_res ;
    match filenums with
    | [] ->
      (* Look into live tuples *)
      let _nb_res, x =
        loop_tup history.tuples None history.block_start history.count first_idx nb_res x in
      x
    | filenum :: filenums' ->
      match read_archive history.dir filenum with
      | None -> loop_file (filenums') 0 nb_res x
      | Some tuples ->
        let nb_res, x =
          loop_tup tuples (Some filenum) (fst filenum) (Array.length tuples) first_idx nb_res x in
        if nb_res >= max_res then x
        else loop_file filenums' 0 nb_res x
  in
  loop_file filenums first_idx 0 init

let drop_until history seqnum max_res =
  let rec loop = function
    | [] ->
      if seqnum >= history.block_start then
        [], seqnum - history.block_start, max_res
      else
        (* gap *)
        [], 0, max_res
    | ((sta, sto) :: filenums') as filenums ->
      if seqnum <= sta then
        (* gap *)
        filenums, 0, max_res
      else if seqnum < sto then
        filenums, seqnum - sta, max_res
      else
        loop filenums'
  in
  loop history.filenums

let fold_tuples_since ?since ?(max_res=1000)
                      history init f =
  !logger.debug "fold_tuples_since since=%a max_res=%d"
    (Option.print Int.print) since max_res ;
  !logger.debug "history has block_start=%d, count=%d"
    history.block_start history.count ;
  let filenums, first_idx, max_res =
    match since with
    | None ->
      if max_res >= 0 then history.filenums, 0, max_res
      else drop_until history (history.block_start + history.count + max_res) ~-max_res
    | Some since ->
      if max_res >= 0 then drop_until history since max_res
      else drop_until history (since + max_res) ~-max_res in
  do_fold_tuples filenums first_idx max_res history init f

let fold_tuples_from_files ?min_filenum ?max_filenum ?(max_res=1000)
                           history init f =
  !logger.debug "fold_tuples_from_files min_filenum=%a max_filenum=%a max_res=%d"
    (Option.print filenum_print) min_filenum
    (Option.print filenum_print) max_filenum
    max_res ;
  !logger.debug "history has block_start=%d, count=%d"
    history.block_start history.count ;
  match hist_min_max history with
  | None -> init
  | Some (hist_min, hist_max) ->
    let min_filenum = Option.default hist_min min_filenum
    and max_filenum = Option.default hist_max max_filenum in
    assert (max_res > 0) ;
    let max_res = min max_res (snd max_filenum - fst min_filenum) in
    let filenums, first_idx, max_res = drop_until history (fst min_filenum) max_res in
    do_fold_tuples filenums first_idx max_res history init f

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
  | TEth -> AEth (Array.init len (fun i -> match f i with VEth x -> EthAddr.to_string x | _ -> assert false))
  | TIpv4 -> AIpv4 (Array.init len (fun i -> match f i with VIpv4 x -> Ipv4.to_string x | _ -> assert false))
  | TIpv6 -> AIpv6 (Array.init len (fun i -> match f i with VIpv6 x -> Ipv6.to_string x | _ -> assert false))
  | TCidrv4 -> ACidrv4 (Array.init len (fun i -> match f i with VCidrv4 x -> Ipv4.Cidr.to_string x | _ -> assert false))
  | TCidrv6 -> ACidrv6 (Array.init len (fun i -> match f i with VCidrv6 x -> Ipv6.Cidr.to_string x | _ -> assert false))
  | TNum | TAny -> assert false

(* Note: the list of values is ordered latest to oldest *)
let export_columns_of_tuples tuple_type values =
  let values = Array.of_list values in
  let nb_values = Array.length values in
  let get_val ci i =
    let inv_i = Array.length values - 1 - i in
    values.(inv_i).(ci)
  in
  (* This works because private fields have been expunged from tuple_type
   * already. *)
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
  if i >= 0 && i < Array.length b then (
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

exception NodeHasNoEventTimeInfo of string
let () =
  Printexc.register_printer (function
    | NodeHasNoEventTimeInfo n -> Some (
      Printf.sprintf "Node %S has no event-time information" n)
    | _ -> None)

(* Return the rank of field named [n] in [node] out_type tuple. *)
let find_field node n =
  let rec loop i = function
  | [] -> failwith ("field "^ n ^" does not exist")
  | (name, _typ) :: rest ->
    if name = n then i else loop (i+1) rest in
  loop 0 node.N.out_type.C.fields

let fold_tuples_and_update_ts_cache
      ?min_filenum ?max_filenum ?max_res
      node history init f =
  let open Lang.Operation in
  match export_event_info node.N.operation with
  | None -> raise (NodeHasNoEventTimeInfo node.N.name)
  | Some ((start_field, start_scale), duration_info) ->
    let ti = find_field node start_field in
    let t2_of_tup =
      match duration_info with
      | DurationConst f -> fun _tup t1 -> t1 +. f
      | DurationField (f, s) ->
        let fi = find_field node f in
        fun tup t1 ->
          t1 +. float_of_scalar_value tup.(fi) *. s
      | StopField (f, s) ->
        let fi = find_field node f in
        fun tup _t1 ->
          float_of_scalar_value tup.(fi) *. s
    in
    let _, _, _, user_val =
      fold_tuples_from_files ?min_filenum ?max_filenum ?max_res
        history (None, max_float, min_float, init)
        (fun filenum _ tup (prev_filenum, tmin, tmax, user_val) ->
          let t = float_of_scalar_value tup.(ti) in
          let t1 = t *. start_scale in
          let t2 = t2_of_tup tup t1 in
          (* Allow duration to be < 0 *)
          let t1, t2 = if t2 >= t1 then t1, t2 else t2, t1 in
          (* Maybe update ts_cache *)
          let tmin, tmax =
            if filenum = prev_filenum then (
              min tmin t1, max tmax t2
            ) else (
              (* New filenum. Save the min/max we computed for the previous one
               * in the ts_cache: *)
              (match prev_filenum with
              | None -> ()
              | Some fn ->
                Hashtbl.modify_opt fn (function
                  | None ->
                    !logger.debug "Caching times for filenum %a to %g..%g"
                      filenum_print fn tmin tmax ;
                    Some (tmin, tmax)
                  | x -> x) history.ts_cache
              ) ;
              (* And start computing new extremes starting from the first points: *)
              t1, t2
            ) in
          filenum, tmin, tmax, f user_val t1 t2 tup) in
    user_val

let build_timeseries node data_field max_data_points
                     since until consolidation =
  if max_data_points < 1 then failwith "invalid max_data_points" ;
  let dt = (until -. since) /. float_of_int max_data_points in
  let buckets = Array.init max_data_points (fun _ ->
    { count = 0 ; sum = 0. ; min = max_float ; max = min_float }) in
  let bucket_of_time t = int_of_float ((t -. since) /. dt) in
  match Hashtbl.find imported_tuples (history_key node) with
  | exception Not_found ->
    !logger.info "Node %s has no history" (N.fq_name node) ;
    [||], [||]
  | history ->
    let f_opt f x y = match x, y with
      | None, y -> Some y
      | Some x, y -> Some (f x y) in
    !logger.debug "timeseries since=%f and until=%f" since until ;
    (* Since the cache is just a cache and is no comprehensive, we must use
     * it as a necessary but not sufficient condition, to prevent us from
     * exploring too far. As time goes it should become the same though. *)
    let min_filenum, max_filenum =
      Hashtbl.enum history.ts_cache |>
      Enum.fold (fun (mi, ma) (filenum, (ts_min, ts_max)) ->
        !logger.debug "filenum %a goes from TS=%f to %f" filenum_print filenum ts_min ts_max ;
        (if since >= ts_min then f_opt max mi filenum else mi),
        (if until <= ts_max then f_opt min ma filenum else ma))
        (None, None) in
    (* As we already have max_data_points, no need for max_res *)
    let vi = find_field node data_field in
    fold_tuples_and_update_ts_cache
      ?min_filenum ?max_filenum ~max_res:max_int node history ()
      (fun () t1 t2 tup ->
        let v = float_of_scalar_value tup.(vi) in
        if t1 < until && t2 >= since then (
          let bi1 = bucket_of_time t1 and bi2 = bucket_of_time t2 in
          for bi = bi1 to bi2 do
            add_into_bucket buckets bi v
          done)) ;
    Array.mapi (fun i _ ->
      since +. dt *. (float_of_int i +. 0.5)) buckets,
    Array.map consolidation buckets

let timerange_of_filenum node history filenum =
  try Hashtbl.find history.ts_cache filenum
  with Not_found ->
    fold_tuples_and_update_ts_cache
      ~min_filenum:filenum ~max_filenum:filenum ~max_res:max_int
      node history None (fun prev t1 t2 _tup ->
        match prev with
        | None -> Some (t1, t2)
        | Some (mi, ma) -> Some (min mi t1, max ma t2)) |>
      (* hist_min_max goes out of his way to never return an empty
       * filenum. Maybe we have an empty storage file? *)
     Option.get
