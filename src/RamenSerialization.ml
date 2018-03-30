open Batteries
open RingBuf
open RamenLog
open RamenHelpers
open RamenScalar

let verbose_serialization = false

let read_tuple ser_tuple_typ nullmask_size tx =
  let read_single_value offs = function
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
  if verbose_serialization then
    !logger.debug "Importing the serialized tuple %a"
      RamenTuple.print_typ ser_tuple_typ ;
  let tuple_len = List.length ser_tuple_typ in
  let tuple = Array.make tuple_len VNull in
  let _ =
    List.fold_lefti (fun (offs, b) i typ ->
        assert (not (is_private_field typ.RamenTuple.typ_name)) ;
        let value, offs', b' =
          if typ.nullable && not (RingBuf.get_bit tx b) then (
            None, offs, b+1
          ) else (
            let value = read_single_value offs typ.typ in
            if verbose_serialization then
              !logger.debug "Importing a single value for %a at offset %d: %a"
                RamenTuple.print_field_typ typ
                offs RamenScalar.print value ;
            let offs' = offs + sersize_of (typ.typ, value) in
            Some value, offs', if typ.nullable then b+1 else b
          ) in
        Option.may (Array.set tuple i) value ;
        offs', b'
      ) (nullmask_size, 0) ser_tuple_typ in
  tuple

let read_tuples ?while_ unserialize rb f =
  RingBuf.read_ringbuf ?while_ rb (fun tx ->
    let tuple = unserialize tx in
    RingBuf.dequeue_commit tx ;
    f tuple)

let read_notifs ?while_ rb f =
  let unserialize tx =
    let offs = 0 in (* Nothing can be null in this tuple *)
    let worker = RingBuf.read_string tx offs in
    let offs = offs + RingBufLib.sersize_of_string worker in
    let url = RingBuf.read_string tx offs in
    worker, url
  in
  read_tuples ?while_ unserialize rb f

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
  | VNull | VString _ | VCidrv4 _ | VCidrv6 _ -> 0.

let find_field_index typ n =
  match List.findi (fun _i f -> f.RamenTuple.typ_name = n) typ with
  | exception Not_found -> failwith ("field "^ n ^" does not exist")
  | i, _ -> i

let fold_seq_range bname mi ma init f =
  let dir = seq_dir_of_bname bname in
  let entries =
    seq_files_of dir //
    (fun (from, to_, _fname) -> from < ma && to_ >= mi) |>
    Array.of_enum in
  Array.fast_sort seq_file_compare entries ;
  let fold_rb from rb usr =
    let%lwt _, usr =
      read_buf rb (usr, 0) (fun (usr, i) tx ->
        let seq = from + i in
        if seq < mi then Lwt.return ((usr, i+1), true) else
        let%lwt usr = f usr tx in
        Lwt.return ((usr, i+1), seq < ma-1)) in
    Lwt.return usr in
  let%lwt usr =
    Array.to_list entries |> (* FIXME *)
    Lwt_list.fold_left_s (fun usr (from, to_, fname) ->
      let rb = load fname in
      Lwt.finalize (fun () -> fold_rb from rb usr)
                   (fun () -> unload rb ; Lwt.return_unit)
    ) init in
  (* finish with the current rb: *)
  let rb = load bname in
  Lwt.finalize
    (fun () ->
      let%lwt s = Lwt.wrap (fun () -> stats rb) in
      fold_rb s.first_seq rb usr)
    (fun () -> unload rb ; Lwt.return_unit)

let fold_buffer bname init f =
  let rb = load bname in
  let%lwt usr = Lwt.finalize
    (fun () -> read_buf rb init f)
    (fun () -> unload rb ; Lwt.return_unit) in
  Lwt.return usr

let fold_buffer_with_time bname typ event_time init f =
  let nullmask_size =
    RingBufLib.nullmask_bytes_of_tuple_type typ in
  let%lwt event_time_of_tuple =
    match event_time with
    | None ->
        Lwt.fail_with "Function has no time information"
    | Some ((start_field, start_scale), duration_info) ->
        let t1i = find_field_index typ start_field in
        let t2i = match duration_info with
          | RamenEventTime.DurationConst _ -> 0 (* won't be used *)
          | RamenEventTime.DurationField (f, _) -> find_field_index typ f
          | RamenEventTime.StopField (f, _) -> find_field_index typ f in
        Lwt.return (fun tup ->
          let t1 = float_of_scalar_value tup.(t1i) *. start_scale in
          let t2 = match duration_info with
            | RamenEventTime.DurationConst k -> t1 +. k
            | RamenEventTime.DurationField (_, s) -> t1 +. float_of_scalar_value tup.(t2i) *. s
            | RamenEventTime.StopField (_, s) -> float_of_scalar_value tup.(t2i) *. s in
          (* Allow duration to be < 0 *)
          if t2 >= t1 then t1, t2 else t2, t1) in
  let f usr tx =
    let tuple =
      read_tuple typ nullmask_size tx in
    (* Get the times from tuple: *)
    let t1, t2 = event_time_of_tuple tuple in
    f usr tuple t1 t2
  in
  fold_buffer bname init f

let time_range bname typ event_time =
  let dir = time_dir_of_bname bname in
  let max_range mi_ma t1 t2 =
    match mi_ma with
    | None -> Some (t1, t2)
    | Some (mi, ma) -> Some (Float.min mi t1, Float.max ma t2) in
  let mi_ma =
    time_files_of dir |>
    Enum.fold (fun mi_ma (t1, t2, _fname) ->
      max_range mi_ma t1 t2
    ) None in
  (* Also look into the current rb: *)
  fold_buffer_with_time bname typ event_time mi_ma (fun mi_ma _tup t1 t2 ->
    Lwt.return (max_range mi_ma t1 t2, true))

let fold_time_range bname typ event_time since until init f =
  let dir = time_dir_of_bname bname in
  let entries =
    time_files_of dir //
    (fun (t1, t2, _fname) -> since < t2 && until >= t1) in
  let loop usr =
    match Enum.get_exn entries with
    | exception Enum.No_more_elements -> Lwt.return usr
    | _t1, _t2, fname ->
        fold_buffer_with_time bname typ event_time usr f
  in
  let%lwt usr = loop init in
  (* finish with the current rb: *)
  fold_buffer_with_time bname typ event_time usr f
