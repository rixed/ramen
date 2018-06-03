(* This program builds timeseries for the requested time range out of any
 * operation field.
 *
 * The operation event-time has to be known, though.
 *)
open Lwt
open Batteries
open RamenLog
open RamenHelpers
module C = RamenConf
module F = C.Func

(* Building timeseries with points at regular times *)

type timeserie_bucket =
  (* Hopefully count will be small enough that sum can be tracked accurately *)
  { mutable count : int ; mutable sum : float ;
    mutable min : float ; mutable max : float }

let make_buckets nt nc =
  Array.init nt (fun _ ->
    Array.init nc (fun _ ->
      { count = 0 ; sum = 0. ; min = max_float ; max = min_float }))

let add_into_bucket b bi ci v =
  if bi >= 0 && bi < Array.length b then (
    b.(bi).(ci).count <- succ b.(bi).(ci).count ;
    b.(bi).(ci).min <- min b.(bi).(ci).min v ;
    b.(bi).(ci).max <- max b.(bi).(ci).max v ;
    b.(bi).(ci).sum <- b.(bi).(ci).sum +. v)

let bucket_of_time since dt t = int_of_float ((t -. since) /. dt)

let bucket_sum b =
  if b.count = 0 then None else Some b.sum
let bucket_avg b =
  if b.count = 0 then None else Some (b.sum /. float_of_int b.count)
let bucket_min b =
  if b.count = 0 then None else Some b.min
let bucket_max b =
  if b.count = 0 then None else Some b.max


(* Enumerates all the time*values *)
(* TODO: (consolidation * data_field) list instead of a single consolidation
 * for all fields *)
let get conf ?duration max_data_points since until where factors
        ?(consolidation="avg") func_name data_fields =
  !logger.debug "Build timeseries for %s, data=%a, where=%a, factors=%a"
    func_name
    (List.print String.print) data_fields
    (List.print (fun oc (factor, value) ->
      Printf.fprintf oc "%s=%a" factor RamenTypes.print value)) where
    (List.print String.print) factors ;
  let nb_data_fields = List.length data_fields in
  let%lwt bname, filter, typ, event_time =
    (* Read directly from the instrumentation ringbuf when func_name ends
     * with "#stats" *)
    if func_name = "stats" || String.ends_with func_name "#stats" then
      let typ = RamenTuple.{ user = RamenBinocle.tuple_typ ;
                             ser = RamenBinocle.tuple_typ } in
      let where_filter = RamenSerialization.filter_tuple_by typ.ser where in
      let wi = RamenSerialization.find_field_index typ.ser "worker" in
      let filter =
        if func_name = "stats" then where_filter else
        let func_name, _ = String.rsplit func_name ~by:"#" in
        fun tuple ->
          tuple.(wi) = RamenTypes.VString func_name &&
          where_filter tuple in
      let bname = C.report_ringbuf conf in
      return (bname, filter, typ, RamenBinocle.event_time)
    else
      (* Create the non-wrapping RingBuf (under a standard name given
       * by RamenConf *)
      let%lwt func, bname =
        RamenExport.make_temp_export_by_name conf ?duration func_name in
      let typ = func.F.out_type in
      let filter = RamenSerialization.filter_tuple_by typ.ser where in
      return (bname, filter, typ, func.F.event_time)
  in
  let open RamenSerialization in
  let fis =
    List.map (find_field_index typ.ser) factors in
  let key_of_factors tuple =
    List.fold_left (fun k fi -> tuple.(fi) :: k) [] fis in
  let%lwt vis =
    Lwt_list.map_s (fun data_field ->
      Lwt.wrap (fun () -> find_field_index typ.ser data_field)
    ) data_fields in
  let dt = (until -. since) /. float_of_int max_data_points in
  let per_factor_buckets = Hashtbl.create 11 in
  let bucket_of_time = bucket_of_time since dt in
  let consolidation =
    match String.lowercase consolidation with
    | "min" -> bucket_min | "max" -> bucket_max | "sum" -> bucket_sum
    | _ -> bucket_avg in
  let%lwt () =
    fold_time_range bname typ.ser event_time since until () (fun () tuple t1 t2 ->
      if filter tuple then (
        let k = key_of_factors tuple in
        let buckets =
          try Hashtbl.find per_factor_buckets k
          with Not_found ->
            !logger.debug "New timeseries for column key %a"
              (List.print RamenTypes.print) k ;
            let buckets = make_buckets max_data_points nb_data_fields in
            Hashtbl.add per_factor_buckets k buckets ;
            buckets in
        let bi1 = bucket_of_time t1 and bi2 = bucket_of_time t2 in
        List.iteri (fun i vi ->
          let v = RamenTypes.float_of_scalar_value tuple.(vi) in
          for bi = bi1 to bi2 do add_into_bucket buckets bi i v done
        ) vis)) in
  (* Extract the results as an Enum, one value per key *)
  let indices = Enum.range 0 ~until:(max_data_points - 1) in
  (* Assume keys and values will enumerate keys in the same orders: *)
  let columns =
    Hashtbl.keys per_factor_buckets |>
    Array.of_enum in
  let ts =
    Hashtbl.values per_factor_buckets |>
    Array.of_enum in
  return (
    columns,
    indices /@
    (fun i ->
      let t = since +. dt *. (float_of_int i +. 0.5)
      and v =
        Array.map (fun buckets ->
          Array.map consolidation buckets.(i)
        ) ts in
      t, v))

(*
 * Factors.
 *
 * For now let's compute the possible values cache lazily, so we can afford
 * to lose/delete them in case of need. In the future we'd like to compute
 * the caches on the fly though.
 *)

(* Scan the given file for all possible values for each of the factors *)
let scan_possible_values factors bname typ =
  let ser = typ.RamenTuple.ser in
  let fis =
    List.map (RamenSerialization.find_field_index ser) factors in
  let possible_values =
    Array.init (List.length fis) (fun _ -> Set.empty) in
  let%lwt () =
    RamenSerialization.fold_buffer_tuple bname ser () (fun () tuple ->
      List.iteri (fun i fidx ->
        let v = tuple.(fidx) in
        possible_values.(i) <- Set.add v possible_values.(i)
      ) fis ;
      ((), true)) in
  return possible_values

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

let factors_of_file fname =
  let lst = C.ppp_of_file ~error_ok:true fname cached_factors_ppp_ocaml in
  Set.of_list lst

let factors_to_file fname factors =
  let lst = Set.to_list factors in
  C.ppp_to_file fname cached_factors_ppp_ocaml lst

(* Scan all stored values for this operation and return the set of all
 * possible values for that factor (if we need to actually scan a file,
 * all factors will be refreshed). *)
let get_possible_values conf ?since ?until func factor =
  if not (List.mem factor func.F.factors) then
    fail_invalid_arg "get_possible_values: not a factor"
  else
    let typ = func.F.out_type in
    let bnames = all_seq_bnames conf ?since ?until func |>
                 List.of_enum (* FIXME *) in
    Lwt_list.fold_left_s (fun set bname ->
      let%lwt pvs =
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
          return pvs
        with e ->
          if e <> Exit then
            !logger.debug "Cannot read cached factor values for %s.%s: %s, \
                           scanning." bname factor (Printexc.to_string e) ;
          !logger.debug "Have to recompute factor values cache." ;
          let%lwt all_pvs = scan_possible_values func.F.factors bname typ in
          let pvs = ref Set.empty in
          (* Save them all: *)
          List.iteri (fun i factor' ->
            if factor' = factor then (* The one that was asked for *)
              pvs := all_pvs.(i) ;
            let cache = C.factors_of_ringbuf bname factor' in
            factors_to_file cache all_pvs.(i)
          ) func.F.factors ;
          return !pvs
      in
      return (Set.union pvs set)
    ) Set.empty bnames

(* Possible values per factor are precalculated to cut down on promises: *)
(* FIXME: change the tree type to make the children enumerator a promise :-( *)
let possible_values_cache = Hashtbl.create 31

let cache_possible_values conf programs =
  Hashtbl.values programs |>
  List.of_enum |> (* FIXME *)
  Lwt_list.iter_p (fun get_rc ->
    let _bin, funcs = get_rc () in
    Lwt_list.iter_p (fun func ->
      let h = Hashtbl.create (List.length func.F.factors) in
      let%lwt () =
        Lwt_list.iter_p (fun factor ->
          let%lwt pvs = get_possible_values conf func factor in
          Hashtbl.add h factor pvs ;
          return_unit
        ) func.F.factors in
      Hashtbl.replace possible_values_cache func.F.name h ;
      return_unit
    ) funcs)

(* Enumerate the possible values of a factor: *)
let possible_values func factor =
  let h = Hashtbl.find possible_values_cache func.F.name in
  Hashtbl.find h factor
