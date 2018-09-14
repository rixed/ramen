(* This program builds timeseries for the requested time range out of any
 * operation field.
 *
 * The operation event-time has to be known, though.
 *)
open Batteries
open RamenLog
open RamenHelpers
module C = RamenConf
module F = C.Func
module P = C.Program

(* Building timeseries with points at regular times *)

type timeserie_bucket =
  (* Hopefully count will be small enough that sum can be tracked accurately *)
  { mutable count : int ; mutable sum : float ;
    mutable min : float ; mutable max : float }

let make_buckets nt nc =
  Array.init nt (fun _ ->
    Array.init nc (fun _ ->
      { count = 0 ; sum = 0. ; min = max_float ; max = min_float }))

let pour_into_bucket b bi ci v =
  b.(bi).(ci).count <- succ b.(bi).(ci).count ;
  b.(bi).(ci).min <- min b.(bi).(ci).min v ;
  b.(bi).(ci).max <- max b.(bi).(ci).max v ;
  b.(bi).(ci).sum <- b.(bi).(ci).sum +. v

let bucket_of_time since dt t = int_of_float ((t -. since) /. dt)

let bucket_sum b =
  if b.count = 0 then None else Some b.sum
let bucket_avg b =
  if b.count = 0 then None else Some (b.sum /. float_of_int b.count)
let bucket_min b =
  if b.count = 0 then None else Some b.min
let bucket_max b =
  if b.count = 0 then None else Some b.max

(* Enumerates all the time*values.
 * Returns the array of factor-column and the Enum.t of data.
 *
 * Each factor-column is the list of values for each given factor (for
 * instance, if you ask factoring by ["ip"; "port"] then for each column
 * you will have ["ip_value"; "port_value"] for that column. If you
 * ask for no factor then you have only one column which values is the empty
 * list [].
 *
 * The enumeration of data is composed of one entry per time value in
 * increasing order, each entry being composed of the time and an array
 * of data-columns.
 *
 * A data-column is itself an array with one optional float per selected
 * field (each factor-column thus has one data-column per selected field).
 *)
(* TODO: (consolidation * data_field) list instead of a single consolidation
 * for all fields *)
let get conf ?duration max_data_points since until where factors
        ?(consolidation="avg") fq data_fields =
  !logger.debug "Build timeseries for %s, data=%a, where=%a, factors=%a"
    (RamenName.string_of_fq fq)
    (List.print String.print) data_fields
    (List.print (fun oc (field, op, value) ->
      Printf.fprintf oc "%s%s%a" field op RamenTypes.print value)) where
    (List.print String.print) factors ;
  let num_data_fields = List.length data_fields in
  let bname, filter, typ, ser, params, event_time =
    RamenExport.read_output conf ?duration fq where in
  let open RamenSerialization in
  let fis =
    List.map (find_field_index ser) factors in
  let key_of_factors tuple =
    List.map (fun fi -> tuple.(fi)) fis in
  let vis =
    List.map (fun data_field ->
      find_field_index ser data_field
    ) data_fields in
  let dt = (until -. since) /. float_of_int max_data_points in
  let per_factor_buckets = Hashtbl.create 11 in
  let bucket_of_time = bucket_of_time since dt in
  let consolidation =
    match String.lowercase consolidation with
    | "min" -> bucket_min | "max" -> bucket_max | "sum" -> bucket_sum
    | _ -> bucket_avg in
  fold_time_range bname ser params event_time since until ()
    (fun () tuple t1 t2 ->
    if filter tuple then (
      let k = key_of_factors tuple in
      let buckets =
        try Hashtbl.find per_factor_buckets k
        with Not_found ->
          !logger.debug "New timeseries for column key %a"
            (List.print RamenTypes.print) k ;
          let buckets = make_buckets max_data_points num_data_fields in
          Hashtbl.add per_factor_buckets k buckets ;
          buckets in
      let bi1 = bucket_of_time t1 and bi2 = bucket_of_time t2 in
      List.iteri (fun i vi ->
        let v = RamenTypes.float_of_scalar tuple.(vi) in
        for bi = max bi1 0 to min bi2 (Array.length buckets - 1) do
          pour_into_bucket buckets bi i v done
      ) vis)) ;
  (* Extract the results as an Enum, one value per key *)
  let indices = Enum.range 0 ~until:(max_data_points - 1) in
  (* Assume keys and values will enumerate keys in the same orders: *)
  let columns =
    Hashtbl.keys per_factor_buckets |>
    Array.of_enum in
  let ts =
    Hashtbl.values per_factor_buckets |>
    Array.of_enum in
  columns,
  indices /@
  (fun i ->
    let t = since +. dt *. (float_of_int i +. 0.5)
    and v =
      Array.map (fun buckets ->
        Array.map consolidation buckets.(i)
      ) ts in
    t, v)

(*
 * Factors.
 *
 * For now let's compute the possible values cache lazily, so we can afford
 * to lose/delete them in case of need. In the future we'd like to compute
 * the caches on the fly though.
 *)

(* Scan the given file for all possible values for each of the factors *)
let scan_possible_values factors bname typ =
  let ser = RingBufLib.ser_tuple_typ_of_tuple_typ typ in
  let fis =
    List.map (RamenSerialization.find_field_index ser) factors in
  let possible_values =
    Array.init (List.length fis) (fun _ -> Set.empty) in
  RamenSerialization.fold_buffer_tuple bname ser () (fun () tuple ->
    List.iteri (fun i fidx ->
      let v = tuple.(fidx) in
      possible_values.(i) <- Set.add v possible_values.(i)
    ) fis ;
    ((), true)) ;
  possible_values

let all_seq_bnames conf ?since ?until func =
  let bname = C.archive_buf_name conf func in
  Enum.append
    (RingBufLib.(arc_dir_of_bname bname |> arc_files_of) //@
     (fun (_, _, t1, t2, fname) ->
       if Option.map_default (fun t -> t <= t2) true since &&
          Option.map_default (fun t -> t >= t1) true until
       then Some fname else None))
    (Enum.singleton bname)

(* What we save in factors cache files: *)
type cached_factors = RamenTypes.value list [@@ppp PPP_OCaml]

let factors_of_file =
  let get = ppp_of_file ~error_ok:true cached_factors_ppp_ocaml in
  fun fname ->
    get fname |> Set.of_list

let factors_to_file fname factors =
  let lst = Set.to_list factors in
  ppp_to_file fname cached_factors_ppp_ocaml lst

(* Scan all stored values for this operation and return the set of all
 * possible values for that factor (if we need to actually scan a file,
 * all factors will be refreshed). *)
let get_possible_values conf ?since ?until func factor =
  if not (List.mem factor func.F.factors) then
    invalid_arg "get_possible_values: not a factor"
  else
    let typ = func.F.out_type in
    let bnames = all_seq_bnames conf ?since ?until func |>
                 List.of_enum (* FIXME *) in
    List.fold_left (fun set bname ->
      let pvs =
        try
          let cache = C.factors_of_ringbuf bname factor in
          (* If fname is newer than cache and cache is older than X seconds
           * then raise *)
          let b_mtime = mtime_of_file_def 0. bname
          and c_mtime = mtime_of_file cache in
          if b_mtime >= c_mtime &&
             c_mtime < Unix.time () -. RamenConsts.cache_factors_ttl then
              raise Exit ;
          let pvs = factors_of_file cache in
          !logger.debug "Got factors from cache %s" cache ;
          pvs
        with e ->
          if e <> Exit then
            !logger.debug "Cannot read cached factor values for %s.%s: %s, \
                           scanning." bname factor (Printexc.to_string e) ;
          !logger.debug "Have to recompute factor values cache." ;
          let all_pvs = scan_possible_values func.F.factors bname typ in
          let pvs = ref Set.empty in
          (* Save them all: *)
          List.iteri (fun i factor' ->
            if factor' = factor then (* The one that was asked for *)
              pvs := all_pvs.(i) ;
            let cache = C.factors_of_ringbuf bname factor' in
            factors_to_file cache all_pvs.(i)
          ) func.F.factors ;
          !pvs
      in
      Set.union pvs set
    ) Set.empty bnames

(* Possible values per factor are precalculated to cut down on promises.
 * Indexed by FQ names, then factor name: *)
(* FIXME: change the tree type to make the children enumerator a promise :-( *)
let possible_values_cache = Hashtbl.create 31

let cache_possible_values conf programs =
  Hashtbl.iter (fun _ (_mre, get_rc) ->
    match get_rc () with
    | exception _ -> ()
    | prog ->
        List.iter (fun func ->
          let h = Hashtbl.create (List.length func.F.factors) in
          List.iter (fun factor ->
            let pvs = get_possible_values conf func factor in
            Hashtbl.add h factor pvs
          ) func.F.factors ;
          Hashtbl.replace possible_values_cache (F.fq_name func) h
        ) prog.P.funcs
  ) programs

(* Enumerate the possible values of a factor: *)
let possible_values func factor =
  match Hashtbl.find possible_values_cache (F.fq_name func) with
  | exception Not_found ->
      !logger.error "%S possible values are not cached?!"
        (RamenName.string_of_func func.F.name) ;
      raise Not_found
  | h ->
    (match Hashtbl.find h factor with
    | exception Not_found ->
        !logger.error "%S is not a factor of %S"
          factor
          (RamenName.string_of_func func.F.name) ;
        raise Not_found
    | x -> x)
