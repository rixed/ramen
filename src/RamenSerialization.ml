open Batteries
open RingBuf
open RingBufLib
open RamenLog
open RamenHelpers
open RamenTypes
open Lwt

let verbose_serialization = false

let read_tuple ser_tuple_typ nullmask_size tx =
  (* Read all fields one by one *)
  if verbose_serialization then
    !logger.debug "Importing the serialized tuple %a"
      RamenTuple.print_typ ser_tuple_typ ;
  let tuple_len = List.length ser_tuple_typ in
  let tuple = Array.make tuple_len VNull in
  let _ =
    List.fold_lefti (fun (offs, b) i typ ->
        assert (not (is_private_field typ.RamenTuple.typ_name)) ;
        let value, offs', b' =
          if typ.nullable && not (get_bit tx b) then (
            None, offs, b+1
          ) else (
            let value = RingBufLib.read_value tx offs typ.typ in
            if verbose_serialization then
              !logger.debug "Importing a single value for %a at offset %d: %a"
                RamenTuple.print_field_typ typ
                offs RamenTypes.print value ;
            let offs' = offs + RingBufLib.sersize_of_value value in
            Some value, offs', if typ.nullable then b+1 else b
          ) in
        Option.may (Array.set tuple i) value ;
        offs', b'
      ) (nullmask_size, 0) ser_tuple_typ in
  tuple

let read_tuples ?while_ unserialize rb f =
  read_ringbuf ?while_ rb (fun tx ->
    let tuple = unserialize tx in
    dequeue_commit tx ;
    f tuple)

let read_notifs ?while_ rb f =
  let unserialize tx =
    let offs = 0 in (* Nothing can be null in this tuple *)
    let worker = read_string tx offs in
    let offs = offs + RingBufLib.sersize_of_string worker in
    let url = read_string tx offs in
    worker, url
  in
  read_tuples ?while_ unserialize rb f

let value_of_string typ s =
  let open RamenParsing in
  (* First parse the string as any immediate value: *)
  let p = allow_surrounding_blanks
            (RamenTypes.Parser.p ~only_narrowest:false) in
  let stream = stream_of_string s in
  match p ["value"] None Parsers.no_error_correction stream |>
        to_result with
  | Bad ((NoSolution _ | Approximation _) as e) ->
      let msg =
        Printf.sprintf2 "Cannot parse %S: %a"
          s (print_bad_result print) e in
      failwith msg
  | Ok (v, _s (* no rest since we ends with eof *)) ->
      let vt = type_of v in
      if vt = typ then v else
      let msg =
        Printf.sprintf2 "%S has type %a instead of expected %a"
          s RamenTypes.print_typ vt RamenTypes.print_typ typ in
      failwith msg
  | Bad (Ambiguous lst) ->
      match List.filter (fun (v, _c, _s) ->
              type_of v = typ
            ) lst with
      | [] ->
          let msg =
            Printf.sprintf2 "%S could have type %a but not the expected %a"
              s
              (List.print ~first:"" ~last:"" ~sep:" or "
                (fun oc (v, _c, _s) ->
                  RamenTypes.print_typ oc (type_of v))) lst
              RamenTypes.print_typ typ in
          failwith msg
      | [v, _, _] -> v
      | lst ->
          let msg =
            Printf.sprintf2 "%S is ambiguous: is it %a?"
              s
              (List.print ~first:"" ~last:"" ~sep:", or "
                (fun oc (v, _, _) -> RamenTypes.print oc v)) lst in
          failwith msg

(*$inject open Stdint *)
(*$= value_of_string & ~printer:(RamenTypes.to_string)
  (VString "glop")             (value_of_string TString "\"glop\"")
  (VString "glop")             (value_of_string TString " \"glop\"  ")
  (VU16 (Uint16.of_int 15042)) (value_of_string TU16    "15042")
  (VU32 (Uint32.of_int 15042)) (value_of_string TU32    "15042")
  (VI64 (Int64.of_int  15042)) (value_of_string TI64    "15042")
  (VFloat 15042.)              (value_of_string TFloat  "15042")
 *)

let write_record conf ser_in_type rb tuple =
  let nullmask_sz, values = (* List of nullable * scalar *)
    List.fold_left (fun (null_i, lst) ftyp ->
      if ftyp.RamenTuple.nullable then
        match Hashtbl.find tuple ftyp.typ_name with
        | exception Not_found ->
            (* Unspecified nullable fields are just null. *)
            null_i + 1, lst
        | s ->
            null_i + 1,
            (Some null_i, value_of_string ftyp.typ s) :: lst
      else
        match Hashtbl.find tuple ftyp.typ_name with
        | exception Not_found ->
            null_i, (None, RamenTypes.any_value_of_type ftyp.typ) :: lst
        | s ->
            null_i, (None, value_of_string ftyp.typ s) :: lst
    ) (0, []) ser_in_type |>
    fun (null_i, lst) ->
      RingBufLib.(round_up_to_rb_word (bytes_for_bits null_i)),
      List.rev lst in
  let sz =
    List.fold_left (fun sz (_, v) ->
      sz + RingBufLib.sersize_of_value v
    ) nullmask_sz values in
  !logger.debug "Sending an input tuple of %d bytes" sz ;
  with_enqueue_tx rb sz (fun tx ->
    zero_bytes tx 0 nullmask_sz ; (* zero the nullmask *)
    (* Loop over all values: *)
    List.fold_left (fun offs (null_i, v) ->
      Option.may (set_bit tx) null_i ;
      RingBufLib.write_value  tx offs v ;
      offs + RingBufLib.sersize_of_value v
    ) nullmask_sz values |> ignore ;
    (* For tests we won't archive the ringbufs so no need for time info: *)
    0., 0.)

(* Garbage in / garbage out *)
let float_of_scalar_value =
  let open Stdint in
  function
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
  | VIp (V4 x) -> Uint32.to_float x
  | VIp (V6 x) -> Uint128.to_float x
  | VNull | VString _ | VCidrv4 _ | VCidrv6 _ | VCidr _ | VTuple _ -> 0.

let find_field_index typ n =
  match List.findi (fun _i f -> f.RamenTuple.typ_name = n) typ with
  | exception Not_found ->
      let err_msg =
        Printf.sprintf2 "Field %s does not exist (possible fields are: %a)" n
          (List.print ~first:"" ~last:"" ~sep:", "
             (fun oc f -> String.print oc f.RamenTuple.typ_name)) typ in
      failwith err_msg
  | i, _ -> i

(* Build a filter function for tuples of the given type: *)
let filter_tuple_by typ where =
  (* Find the indices of all the involved fields, and parse the values: *)
  let where =
    List.map (fun (n, v) ->
      let idx = find_field_index typ n in
      idx, v
    ) where in
  fun tuple ->
    List.for_all (fun (idx, v) ->
      tuple.(idx) = v
    ) where

(* By default, this loops until we reach ma and while_ yields true.
 * Set ~wait_for_more to false if you want to stop once the end of data
 * is reached.
 * Note: mi is inclusive, ma exclusive *)
let rec fold_seq_range ?while_ ?wait_for_more ?(mi=0) ?ma bname init f =
  let fold_rb from rb usr =
    !logger.debug "fold_rb: from=%d, mi=%d" from mi ;
    read_buf ?while_ ?wait_for_more rb (usr, from) (fun (usr, seq) tx ->
      !logger.debug "fold_seq_range: read_buf seq=%d" seq ;
      if seq < mi then return ((usr, seq + 1), true) else
      match ma with Some m when seq >= m ->
        return ((usr, seq + 1), false)
      | _ ->
        let%lwt usr = f usr seq tx in
        (* Try to save the last sleep: *)
        let more_to_come =
          match ma with None -> true | Some m -> seq < m - 1 in
        return ((usr, seq + 1), more_to_come)) in
  !logger.debug "fold_seq_range: mi=%d, ma=%a" mi (Option.print Int.print) ma ;
  match ma with Some m when mi >= m -> return init
  | _ -> (
    let%lwt keep_going =
      match while_ with Some w -> w () | _ -> return_true in
    if not keep_going then return init else (
      let dir = arc_dir_of_bname bname in
      let entries =
        arc_files_of dir //
        (fun (from, to_, _t1, _t2, _fname) ->
          (* in file names, to_ is inclusive *)
          to_ >= mi && Option.map_default (fun ma -> from < ma) true ma) |>
        Array.of_enum in
      Array.fast_sort arc_file_compare entries ;
      let%lwt usr, next_seq =
        Array.to_list entries |> (* FIXME *)
        Lwt_list.fold_left_s (fun (usr, _) (from, to_, _t1, _t2, fname) ->
          let rb = load fname in
          finalize (fun () -> fold_rb from rb usr)
                   (fun () -> unload rb ; return_unit)
        ) (init, 0 (* unused if there are some entries *)) in
      !logger.debug "After archives, next_seq is %d" next_seq ;
      (* Of course by the time we reach here, new archives might have been
       * created. We will know after opening the current rb, if its starting
       * seqnum is > then max_seq then we should recurse into the archive ... *)
      (* Finish with the current rb: *)
      let rb = load bname in
      let%lwt s = wrap (fun () -> stats rb) in
      if next_seq > 0 && s.first_seq > next_seq && s.first_seq > mi then (
        !logger.debug "fold_seq_range: current starts at %d > %d, \
                       going through the archive again"
          s.first_seq next_seq ;
        unload rb ;
        fold_seq_range ?while_ ?wait_for_more ~mi:next_seq ?ma bname usr f
      ) else (
        !logger.debug "fold_seq_range: current starts at %d, lgtm"
          s.first_seq ;
        let%lwt usr, next_seq =
          finalize (fun () -> fold_rb s.first_seq rb usr)
                   (fun () -> unload rb ; return_unit) in
        !logger.debug "After current, next_seq is %d" next_seq ;
        (* And of course, by the time we reach this point this ringbuf might
         * have been archived already. *)
        fold_seq_range ?while_ ?wait_for_more ~mi:next_seq ?ma bname usr f)))

let fold_buffer ?wait_for_more ?while_ bname init f =
  match load bname with
  | exception Failure msg ->
      !logger.debug "Cannot fold_buffer: %s" msg ;
      (* Therefore there is nothing to fold: *)
      return init
  | rb ->
      let%lwt usr = finalize
        (fun () -> read_buf ?wait_for_more ?while_ rb init f)
        (fun () -> unload rb ; return_unit) in
      return usr

(* Like fold_buffer but call f with the tuple rather than the tx: *)
let fold_buffer_tuple ?while_ ?(early_stop=true) bname typ init f =
  !logger.debug "Going to fold over %s" bname ;
  let nullmask_size =
    RingBufLib.nullmask_bytes_of_tuple_type typ in
  let f usr tx =
    let tuple =
      read_tuple typ nullmask_size tx in
    return (
      let usr, more_to_come as res = f usr tuple in
      if early_stop then res
      else (usr, true))
  in
  fold_buffer ~wait_for_more:false ?while_ bname init f

(* As tuples are not necessarily ordered by time we want the possibility
 * to override the more_to_come decision.
 * Notice that contrary to `ramen tail`, `ramen timeseries` must never
 * wait for data and must return as soon as we've reached the end of what's
 * available. *)
let fold_buffer_with_time ?while_ ?(early_stop=true) bname typ event_time init f =
  !logger.debug "Folding over %s" bname ;
  let%lwt event_time_of_tuple =
    match event_time with
    | None ->
        fail_with "Function has no time information"
    | Some ((start_field, start_scale), duration_info) ->
        let t1i = find_field_index typ start_field in
        let t2i = match duration_info with
          | RamenEventTime.DurationConst _ -> 0 (* won't be used *)
          | RamenEventTime.DurationField (f, _) -> find_field_index typ f
          | RamenEventTime.StopField (f, _) -> find_field_index typ f in
        return (fun tup ->
          let t1 = float_of_scalar_value tup.(t1i) *. start_scale in
          let t2 = match duration_info with
            | RamenEventTime.DurationConst k -> t1 +. k
            | RamenEventTime.DurationField (_, s) -> t1 +. float_of_scalar_value tup.(t2i) *. s
            | RamenEventTime.StopField (_, s) -> float_of_scalar_value tup.(t2i) *. s in
          (* Allow duration to be < 0 *)
          if t2 >= t1 then t1, t2 else t2, t1) in
  let f usr tuple =
    (* Get the times from tuple: *)
    let t1, t2 = event_time_of_tuple tuple in
    f usr tuple t1 t2
  in
  fold_buffer_tuple ?while_ bname typ init f

let time_range ?while_ bname typ event_time =
  let dir = arc_dir_of_bname bname in
  let max_range mi_ma t1 t2 =
    match mi_ma with
    | None -> Some (t1, t2)
    | Some (mi, ma) -> Some (Float.min mi t1, Float.max ma t2) in
  let mi_ma =
    RingBufLib.arc_files_of dir |>
    Enum.fold (fun mi_ma (_s1, _s2, t1, t2, _fname) ->
      max_range mi_ma t1 t2
    ) None in
  (* Also look into the current rb: *)
  fold_buffer_with_time ?while_ bname typ event_time mi_ma (fun mi_ma _tup t1 t2 ->
    max_range mi_ma t1 t2, true)

let fold_time_range ?while_ bname typ event_time since until init f =
  let dir = arc_dir_of_bname bname in
  let entries =
    RingBufLib.arc_files_of dir //
    (fun (_s1, _s2, t1, t2, fname) -> since < t2 && until >= t1) in
  let f usr tuple t1 t2 =
    (if t1 >= until || t2 < since then usr else f usr tuple t1 t2), true in
  let rec loop usr =
    match Enum.get_exn entries with
    | exception Enum.No_more_elements -> return usr
    | _s1, _s2, _t1, _t2, fname ->
        fold_buffer_with_time ?while_ ~early_stop:false
                              fname typ event_time usr f >>=
        loop
  in
  let%lwt usr = loop init in
  (* finish with the current rb: *)
  fold_buffer_with_time ?while_ bname typ event_time usr f
