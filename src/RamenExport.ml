open Batteries
open RamenLog
open RamenSharedTypes
open RamenSharedTypesJS
module C = RamenConf
module N = RamenConf.Func
open Helpers
open Stdint
open Lwt

(* Possible solutions:
 *
 * 1. Simple for the func, complex for ramen:
 *
 * The export is a normal children for the exporting func, it mirror the
 * output tuple there as it would for any other child func. Ramen, thus, must
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
 * - does not slow down the funcs even a bit;
 * - does not make them any more complex;
 *
 * Cons:
 *
 * - slower than dedicated code to read the ringbuf from ramen;
 *
 * 2. Simple for ramen, complex for the funcs:
 *
 * The funcs know that they export and have a specific code to output the tuple
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
 * - more work for the funcs;
 * - can have only one syntax for the websocket data (likely json).
 *
 * Let's go with 1.
 *)

(* History of past tuples:
 *
 * We store tuples output by exporting funcs both in RAM (for the last ones)
 * and on disk (in a "history" subdir). Each file is numbered and a cursor in
 * the history of some func might be given by file number, tuple offset (the
 * count below). Since only the ramen process ever write or read those files
 * there is not much to worry about. For now those are merely Marshalled
 * array or array of scalar value (tuples in the record below). *)

let history_block_length = 10_000 (* TODO: make it a parameter? *)

type history =
  { (* The type stored in there, which must correspond to the func
       out_type serialized type, and will since the history is associated
       with the func name and output signature. *)
    ser_tuple_type : field_typ list ;
    (* List of the serialized fields in order of appearance in the user
     * provided tuple so that we can reorder exported tuples: *)
    pos_in_user_tuple : string array ;
    (* Store arrays of RamenScalar.values not hash of names to values !
     * TODO: ideally storing columns would be even better *)
    tuples : scalar_value array array ;
    (* Start seqnum of the next block: *)
    mutable block_start : int ;
    (* How many tuples we already have in memory: *)
    mutable count : int ;
    (* Files are saved with a name composed of block_start-block_end (aka
     * next block_start). *)
    (* dir is named after the func output type so that we won't run the risk
     * to read data that have been archived in a different format. Other than
     * that, we make no attempt to clean these archives even when the func is
     * edited. It's best if deletion is explicit (TODO: add an option in the
     * put program command). Another way to get rid of the history is to rename
     * the func. Since we need output type to locate the history, a func
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

let history_key func =
  let fq_name = N.fq_name func in
  let type_sign = C.type_signature_hash func.N.out_type in
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
  if history.count <= 0 || conf.C.max_history_archives <= 0 then () else
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

let make_history conf func =
  let history_version_tag = "~1" in
  let type_sign = C.type_signature_hash func.N.out_type in
  let dir = conf.C.persist_dir ^"/workers/history/"
                               ^ RamenVersions.history
                               ^"/"^ Config.version
                               ^"/"^ N.fq_name func
                               ^"/"^ type_sign ^ history_version_tag in
  !logger.info "Creating history for func %S" (N.fq_name func) ;
  mkdir_all dir ;
  let ser_tuple_type, pos_in_user_tuple =
    match func.N.out_type with
    | UntypedTuple _ -> assert false
    | TypedTuple { user ; ser } ->
      ser,
      (* The pos_in_user_tuple is merely user without private fields: *)
      List.filter_map (fun f ->
        if is_private_field f.typ_name then None
        else Some f.typ_name) user |>
      Array.of_list in
  (* Note: this is OK to share this [||] since we use it only as a placeholder *)
  let tuples = Array.make history_block_length [||] in
  let nb_files, filenums, max_seqnum =
    (* TODO: use Lwt for scanning the dir *)
    Sys.readdir dir |>
    Array.fold_left (fun (nb_files, arcs, ma as prev) fname ->
        match Scanf.sscanf fname "%d-%d" (fun a b -> a, b) with
        | exception _ -> prev
        | start, stop as m ->
          if stop > start then
            nb_files + 1, m :: arcs, max stop ma
          else prev
      ) (0, [], min_int) in
  let filenums =
    List.fast_sort (fun (m1, _) (m2, _) -> Int.compare m1 m2) filenums in
  let block_start =
    if nb_files = 0 then 0
    else (
      !logger.debug "%d archive files from %d up to %d"
        nb_files (fst (List.hd filenums)) max_seqnum ;
      max_seqnum) in
  { ser_tuple_type ; pos_in_user_tuple ; tuples ; block_start ;
    count = 0 ; dir ; nb_files ; filenums ;
    ts_cache = Hashtbl.create (conf.C.max_history_archives / 8) }

let add_tuple conf history tuple =
  if history.count >= Array.length history.tuples then
    archive_history conf history ;
  let idx = history.count in
  history.tuples.(idx) <- tuple ;
  history.count <- history.count + 1

let import_tuples conf history rb_name ser_tuple_typ =
  let nullmask_size =
    RingBufLib.nullmask_bytes_of_tuple_type ser_tuple_typ in
  (* We will store tuples in serialized column order but will reorder
   * the columns when answering user queries. *)
  !logger.debug "Starting to import output from ringbuf %S, \
                 which outputs %a."
    rb_name RamenTuple.print_typ ser_tuple_typ ;
  let%lwt rb = wrap (fun () -> RingBuf.load rb_name) in
  try%lwt
    let dequeue =
      RingBufLib.retry_for_ringbuf RingBuf.dequeue_alloc in
    while%lwt true do
      let%lwt tx = dequeue rb in
      let ser_tuple =
        RamenSerialization.read_tuple ser_tuple_typ nullmask_size tx in
      RingBuf.dequeue_commit tx ;
      wrap (fun () -> add_tuple conf history ser_tuple) >>=
      Lwt_main.yield
    done
  with Canceled ->
        !logger.info "Import from %s was cancelled" rb_name ;
        wrap (fun () -> RingBuf.unload rb)
     | e ->
        !logger.error "Importing tuples failed with %s,\n%s"
          (Printexc.to_string e)
          (Printexc.get_backtrace ()) ;
        fail e

(* Store history of past tuple output by a given func. Not stored in the
 * func itself because we do not want to serialize such a big data structure
 * for every HTTP query, nor "rollback" it when a graph change fails.
 * Indexed by func fq_name and output signature (see history_key): *)
type export =
  { importer : unit Lwt.t ;
    history : history ;
    fqn : string ;
    rb_name : string ;
    out_rb_ref : string ;
    mutable last_used : float ;
    force_export : bool }
let make_export importer history fqn rb_name out_rb_ref force_export =
  { importer ; history ; fqn ; rb_name ; out_rb_ref ; force_export ;
    last_used = Unix.gettimeofday () }
(* Key can be obtained from history_key *)
let exports : (string (* FQN *) * string (* out signature *), export)
                Hashtbl.t = Hashtbl.create 31
(* To protect the above exports hashtbl: *)
let exports_mutex = Lwt_mutex.create ()

let is_exporting k =
  Lwt_mutex.with_lock exports_mutex (fun () ->
    return (Hashtbl.mem exports k))

let is_func_exporting func =
  (* func needs to have out_type typed, at least: *)
  if not (C.tuple_is_typed func.N.out_type) then return_false
  else
    let k = history_key func in
    is_exporting k

(* Caller need no wlock on the programs since func is not modified *)
(* func must be typed though! *)
let get_or_start conf func =
  let (fqn, _ as k) = history_key func in
  Lwt_mutex.with_lock exports_mutex (fun () ->
    match Hashtbl.find exports k with
    | exception Not_found ->
        !logger.info "Function %s starts exporting" fqn ;
        let%lwt typ = wrap (fun () -> C.tuple_ser_type func.N.out_type) in
        let rb_name = C.exp_ringbuf_name conf func in
        let out_rb_ref = C.out_ringbuf_names_ref conf func in
        RingBuf.create rb_name RingBufLib.rb_default_words ;
        let history = make_history conf func in
        let importer = import_tuples conf history rb_name typ in
        let export = make_export importer history fqn rb_name out_rb_ref
                                 func.N.force_export in
        Hashtbl.add exports k export ;
        let skip_none =
          RingBufLib.skip_list ~out_type:typ ~in_type:typ in
        let%lwt () =
          RamenOutRef.add out_rb_ref (rb_name, skip_none) in
        return history
    | exp ->
        !logger.debug "Function %s is already exporting" fqn ;
        exp.last_used <- Unix.gettimeofday () ;
        return exp.history)

(* Caller need no wlock on the programs since func is not modified *)
let stop_export conf exp =
  (* Start by asking the worker to stop exporting, then kill the
   * importer, then get rid of the ringbuf.
   * TODO: Ideally these steps are synchronous. *)
  let%lwt () = RamenOutRef.remove exp.out_rb_ref exp.rb_name in
  cancel exp.importer ;
  archive_history conf exp.history ;
  log_exceptions Unix.unlink exp.rb_name ;
  return_unit

let stop conf func =
  let (fqn, _ as k) = history_key func in
  Lwt_mutex.with_lock exports_mutex (fun () ->
    match Hashtbl.find exports k with
    | exception Not_found ->
        !logger.debug "Operation %s was not exporting" fqn ;
        return_unit
    | exp ->
        !logger.info "Function %s stops exporting" fqn ;
        Hashtbl.remove exports k ;
        stop_export conf exp)

let timeout_exports conf =
  let now = Unix.gettimeofday () in
  (* timeout exports oldest that that: *)
  let oldest = now -. 10. in
  Lwt_mutex.with_lock exports_mutex (fun () ->
    let to_stop = ref [] in
    Hashtbl.filter_inplace (fun exp ->
      if not exp.force_export && exp.last_used < oldest then (
        !logger.info "Timeouting export from %s" exp.fqn ;
        to_stop := exp :: !to_stop ;
        false
      ) else true
    ) exports ;
    (* TODO: iter_p *)
    Lwt_list.iter_s (stop_export conf) !to_stop)

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
        let val_idx_to_tuple_idx = Array.make nb_values ~-1 in
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

let reorder_columns_to_user history =
  let idx_of_field n =
    Array.findi ((=) n) history.pos_in_user_tuple in
  fun (n1, _, _) (n2, _, _) ->
    try
      let i1 = idx_of_field n1
      and i2 = idx_of_field n2 in
      Int.compare i1 i2
    with Not_found ->
      !logger.error "Cannot find fields %s and %s in %a, \
                     user ordering will be wrong!"
        n1 n2
        RamenTuple.print_typ history.ser_tuple_type ;
      String.compare n1 n2 (* Still we want List.sort to terminate *)

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

exception FuncHasNoEventTimeInfo of string
let () =
  Printexc.register_printer (function
    | FuncHasNoEventTimeInfo n -> Some (
      Printf.sprintf "Function %S has no event-time information" n)
    | _ -> None)

(* Return the rank of field named [n] in the serialized tuple. *)
let find_ser_field tuple_type n =
  match tuple_type with
  | C.UntypedTuple _ ->
      !logger.error "Asked for serialized rank of an untyped output" ;
      assert false
  | C.TypedTuple { ser ; _ } ->
    (match List.findi (fun _i field -> field.typ_name = n) ser with
    | exception Not_found ->
        failwith ("field "^ n ^" does not exist")
    | i, _ -> i)

let fold_tuples_and_update_ts_cache
      ?min_filenum ?max_filenum ?max_res
      func history init f =
  let open RamenOperation in
  match event_time_of_operation func.N.operation with
  | None -> raise (FuncHasNoEventTimeInfo func.N.name)
  | Some ((start_field, start_scale), duration_info) ->
    let ti = find_ser_field func.N.out_type start_field in
    let t2_of_tup =
      match duration_info with
      | DurationConst f -> fun _tup t1 -> t1 +. f
      | DurationField (f, s) ->
        let fi = find_ser_field func.N.out_type f in
        fun tup t1 ->
          t1 +. float_of_scalar_value tup.(fi) *. s
      | StopField (f, s) ->
        let fi = find_ser_field func.N.out_type f in
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

let build_timeseries func history data_field max_data_points
                     since until consolidation =
  if max_data_points < 1 then failwith "invalid max_data_points" ;
  let dt = (until -. since) /. float_of_int max_data_points in
  let buckets = Array.init max_data_points (fun _ ->
    { count = 0 ; sum = 0. ; min = max_float ; max = min_float }) in
  let bucket_of_time t = int_of_float ((t -. since) /. dt) in
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
  let vi = find_ser_field func.N.out_type data_field in
  fold_tuples_and_update_ts_cache
    ?min_filenum ?max_filenum ~max_res:max_int func history ()
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

let timerange_of_filenum func history filenum =
  try Hashtbl.find history.ts_cache filenum
  with Not_found ->
    fold_tuples_and_update_ts_cache
      ~min_filenum:filenum ~max_filenum:filenum ~max_res:max_int
      func history None (fun prev t1 t2 _tup ->
        match prev with
        | None -> Some (t1, t2)
        | Some (mi, ma) -> Some (min mi t1, max ma t2)) |>
    (* hist_min_max goes out of his way to never return an empty
     * filenum. Maybe we have an empty storage file? *)
    Option.get
