open Batteries
open RingBuf
open RamenSharedTypesJS
open RamenSharedTypes
open RamenLog
open Helpers

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
        assert (not (is_private_field typ.typ_name)) ;
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
