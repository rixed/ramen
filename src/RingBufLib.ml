open Batteries
open RamenLog
open RamenHelpers
open RamenTypes
open Stdint
open RingBuf

(* Note regarding nullmask and constructed types:
 * For list, we cannot know in advance the number of values so the nullmask
 * size can not be known in advance.
 * For Vec and Tuples, when given only the value we cannot retrieve the
 * type in depth, so we cannot serialize a value without being given the
 * full type.
 * For all these reasons, structured types will be serialized with their
 * own nullmask as a prefix. And we will consider all values are nullable
 * so we do not have to pass the types in practice. That's not a big waste
 * of space considering how rare it is that we want to serialize a
 * constructed type. Maybe we should do the same for the whole output
 * tuple BTW. *)

let nullmask_bytes_of_tuple_type ser =
  List.fold_left (fun s field_typ ->
    if is_private_field field_typ.RamenTuple.typ_name then s
    else s + (if field_typ.RamenTuple.typ.nullable then 1 else 0)
  ) 0 ser |>
  bytes_for_bits |>
  round_up_to_rb_word

let nullmask_sz_of_tuple ts =
  Array.length ts |> bytes_for_bits |> round_up_to_rb_word

let nullmask_sz_of_vector d =
  bytes_for_bits d |> round_up_to_rb_word

let sersize_of_string s =
  rb_word_bytes + round_up_to_rb_word (String.length s)

let sersize_of_float = round_up_to_rb_word 8
let sersize_of_bool = round_up_to_rb_word 1
let sersize_of_u8 = round_up_to_rb_word 1
let sersize_of_i8 = round_up_to_rb_word 1
let sersize_of_u16 = round_up_to_rb_word 2
let sersize_of_i16 = round_up_to_rb_word 2
let sersize_of_u32 = round_up_to_rb_word 4
let sersize_of_i32 = round_up_to_rb_word 4
let sersize_of_ipv4 = round_up_to_rb_word 4
let sersize_of_u64 = round_up_to_rb_word 8
let sersize_of_i64 = round_up_to_rb_word 8
let sersize_of_u128 = round_up_to_rb_word 16
let sersize_of_i128 = round_up_to_rb_word 16
let sersize_of_ipv6 = round_up_to_rb_word 16
let sersize_of_null = 0
let sersize_of_eth = round_up_to_rb_word 6
let sersize_of_cidrv4 = sersize_of_ipv4 + sersize_of_u8
let sersize_of_cidrv6 = sersize_of_ipv6 + sersize_of_u16

let sersize_of_ip = function
  | RamenIp.V4 _ -> rb_word_bytes + sersize_of_ipv4
  | RamenIp.V6 _ -> rb_word_bytes + sersize_of_ipv6

let sersize_of_cidr = function
  | RamenIp.Cidr.V4 _ -> rb_word_bytes + sersize_of_cidrv4
  | RamenIp.Cidr.V6 _ -> rb_word_bytes + sersize_of_cidrv6

let rec sersize_of_fixsz_typ = function
  | TFloat -> sersize_of_float
  | TBool -> sersize_of_bool
  | TU8 -> sersize_of_u8
  | TI8 -> sersize_of_i8
  | TU16 -> sersize_of_u16
  | TI16 -> sersize_of_i16
  | TU32 -> sersize_of_u32
  | TI32 -> sersize_of_i32
  | TIpv4 -> sersize_of_ipv4
  | TU64 -> sersize_of_u64
  | TI64 -> sersize_of_i64
  | TU128 -> sersize_of_u128
  | TI128 -> sersize_of_i128
  | TIpv6 -> sersize_of_ipv6
  | TEth -> sersize_of_eth
  | TCidrv4 -> sersize_of_cidrv4
  | TCidrv6 -> sersize_of_cidrv6
  (* FIXME: TVec (d, t) should be a fixsz typ if t is one. *)
  | TString | TIp | TCidr | TTuple _ | TVec _ | TList _
  | TNum | TAny | TEmpty -> assert false

let rec sersize_of_value = function
  | VString s -> sersize_of_string s
  | VFloat _ -> sersize_of_float
  | VBool _ -> sersize_of_bool
  | VU8 _ -> sersize_of_u8
  | VI8 _ -> sersize_of_i8
  | VU16 _ -> sersize_of_u16
  | VI16 _ -> sersize_of_i16
  | VU32 _ -> sersize_of_u32
  | VI32 _ -> sersize_of_i32
  | VIpv4 _ -> sersize_of_ipv4
  | VU64 _ -> sersize_of_u64
  | VI64 _ -> sersize_of_i64
  | VU128 _ -> sersize_of_u128
  | VI128 _ -> sersize_of_i128
  | VIpv6 _ -> sersize_of_ipv6
  | VIp x -> sersize_of_ip x
  | VNull -> sersize_of_null
  | VEth _ -> sersize_of_eth
  | VCidrv4 _ -> sersize_of_cidrv4
  | VCidrv6 _ -> sersize_of_cidrv6
  | VCidr x -> sersize_of_cidr x
  | VTuple vs -> sersize_of_tuple vs
  | VVec vs -> sersize_of_vector vs
  | VList vs -> sersize_of_list vs

and sersize_of_tuple vs =
  (* Tuples are serialized in field order, unserializer will know which
   * fields are present so there is no need for meta-data: *)
  (* The bitmask for the null values is stored first (remember that all
   * values are considered nullable). *)
  let nullmask_sz = nullmask_sz_of_tuple vs in
  Array.fold_left (fun s v -> s + sersize_of_value v) nullmask_sz vs

and sersize_of_vector vs =
  (* Vectors are serialized in field order, unserializer will also know
   * the size of the vector and the type of its elements: *)
  sersize_of_tuple vs

and sersize_of_list vs =
  (* Lists are prefixed with their number of elements (before the
   * nullmask): *)
  sersize_of_u32 + sersize_of_tuple vs

let has_fixed_size = function
  | TString
  (* Technically, those could have a fixed size, but we always treat them as
   * variable: *)
  | TTuple _ | TVec _ | TList _ -> false
  | _ -> true

let tot_fixsz tuple_typ =
  List.fold_left (fun c t ->
    let open RamenTypes in
    if not (has_fixed_size t.RamenTuple.typ.structure) then c else
    c + sersize_of_fixsz_typ t.typ.structure
  ) 0 tuple_typ

let rec write_value tx offs = function
  | VFloat f -> write_float tx offs f
  | VString s -> write_string tx offs s
  | VBool b -> write_bool tx offs b
  | VU8 i -> write_u8 tx offs i
  | VU16 i -> write_u16 tx offs i
  | VU32 i -> write_u32 tx offs i
  | VU64 i -> write_u64 tx offs i
  | VU128 i -> write_u128 tx offs i
  | VI8 i -> write_i8 tx offs i
  | VI16 i -> write_i16 tx offs i
  | VI32 i -> write_i32 tx offs i
  | VI64 i -> write_i64 tx offs i
  | VI128 i -> write_i128 tx offs i
  | VEth e -> write_eth tx offs e
  | VIpv4 i -> write_ip4 tx offs i
  | VIpv6 i -> write_ip6 tx offs i
  | VIp i -> write_ip tx offs i
  | VCidrv4 c -> write_cidr4 tx offs c
  | VCidrv6 c -> write_cidr6 tx offs c
  | VCidr c -> write_cidr tx offs c
  | VTuple vs -> write_tuple tx offs vs
  | VVec vs -> write_vector tx offs vs
  | VList vs -> write_list tx offs vs
  | VNull -> assert false

(* Tuples are serialized as the succession of the values, after the local
 * nullmask. Since we don't know any longer if the values are nullable,
 * the nullmask has one bit per value in any cases. *)
and write_tuple tx offs vs =
  let nullmask_sz = nullmask_sz_of_tuple vs in
  zero_bytes tx offs nullmask_sz ;
  let bi = offs * 8 in
  Array.fold_lefti (fun o i v ->
    if v = VNull then offs else (
      set_bit tx (bi + i) ;
      write_value tx o v ;
      o + sersize_of_value v
    )
  ) (offs + nullmask_sz) vs |>
  ignore

and write_vector tx = write_tuple tx

and write_list tx offs vs =
  write_u32 tx offs (Array.length vs |> Uint32.of_int) ;
  write_vector tx (offs + sersize_of_u32) vs

let rec read_value tx offs structure =
  match structure with
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
  | TIp     -> VIp (read_ip tx offs)
  | TCidrv4 -> VCidrv4 (read_cidr4 tx offs)
  | TCidrv6 -> VCidrv6 (read_cidr6 tx offs)
  | TCidr   -> VCidr (read_cidr tx offs)
  | TTuple ts -> VTuple (read_tuple ts tx offs)
  | TVec (d, t) -> VVec (read_vector d t tx offs)
  | TList t -> VList (read_list t tx offs)
  | TNum | TAny | TEmpty -> assert false

and read_constructed_value tx t o bi =
  let v =
    if t.nullable && not (get_bit tx bi) then VNull
    else read_value tx !o t.structure in
  o := !o + sersize_of_value v ;
  v

(* Tuples, vectors and lists come with a separate nullmask as a prefix,
 * with one bit per element regardless of their nullability (because
 * although we knoe the type, write_tuple do not): *)
and read_tuple ts tx offs =
  let nullmask_sz = nullmask_sz_of_tuple ts in
  let o = ref (offs + nullmask_sz) in
  let bi = offs * 8 in
  Array.mapi (fun i t ->
    read_constructed_value tx t o (bi + i)
  ) ts

and read_vector d t tx offs =
  let nullmask_sz = nullmask_sz_of_vector d in
  let bi = offs * 8 in
  let o = ref (offs + nullmask_sz) in
  Array.init d (fun i ->
    read_constructed_value tx t o (bi + i))

and read_list t tx offs =
  let d = read_u32 tx offs |> Uint32.to_int in
  let bi = (offs + sersize_of_u32) * 8 in
  let nullmask_sz = nullmask_sz_of_vector d in
  let o = ref (offs + sersize_of_u32 + nullmask_sz) in
  Array.init d (fun i ->
    read_constructed_value tx t o (bi + i))

(* Unless wait_for_more, this will raise Empty when out of data *)
let retry_for_ringbuf ?(wait_for_more=true) ?while_ ?delay_rec ?max_retry_time f =
  let on = function
    | NoMoreRoom -> Lwt.return_true
    | Empty -> Lwt.return wait_for_more
    | _ -> Lwt.return_false
  in
  retry ?while_ ~on ~first_delay:0.001 ~max_delay:1. ?delay_rec
        ?max_retry_time (fun x -> Lwt.wrap (fun () -> f x))

let out_ringbuf_names outbuf_ref_fname =
  let open Lwt in
  let last_touched fname =
    let open Lwt_unix in
    mtime_of_file_def 0. fname |> return in
  let last_read = ref 0. in
  fun () ->
    let%lwt t = last_touched outbuf_ref_fname in
    if t > !last_read then (
      if !last_read <> 0. then
        !logger.info "Have to re-read %s" outbuf_ref_fname ;
      last_read := t ;
      let%lwt lines = RamenOutRef.read outbuf_ref_fname in
      return (Some lines)
    ) else return_none

(* To allow a func to select only some fields from its parent and write only
 * a skip list in the out_ref (to makes serialization easier not out_ref
 * smaller) we serialize all fields in the same order: *)
let ser_tuple_field_cmp t1 t2 =
  String.compare t1.RamenTuple.typ_name t2.RamenTuple.typ_name

let ser_tuple_typ_of_tuple_typ tuple_typ =
  tuple_typ |>
  List.filter (fun t -> not (is_private_field t.RamenTuple.typ_name)) |>
  List.fast_sort ser_tuple_field_cmp

(* Given a tuple type and its serialized type, return a function that reorder
 * a tuple (as an array) into the same column order as in the tuple type: *)
let reorder_tuple_to_user typ ser =
  (* Start by building the array of indices in the ser tuple of fields of
   * the user (minus private) tuple. *)
  let indices =
    List.filter_map (fun f ->
      if is_private_field f.RamenTuple.typ_name then None
      else Some (
        List.findi (fun _ f' ->
          f'.RamenTuple.typ_name = f.typ_name) ser |> fst)
    ) typ |>
    Array.of_list in
  (* Now reorder a list of scalar values in ser order into user order: *)
  (* TODO: an inplace version *)
  fun vs ->
    Array.map (fun idx -> vs.(idx)) indices

let skip_list ~out_type ~in_type =
  let ser_out = ser_tuple_typ_of_tuple_typ out_type
  and ser_in = ser_tuple_typ_of_tuple_typ in_type in
  let rec loop v = function
    | [], [] -> List.rev v
    | _::os', [] -> loop (false :: v) (os', [])
    | o::os', (i::is' as is) ->
      let c = ser_tuple_field_cmp o i in
      if c < 0 then
        (* not interested in o *)
        loop (false :: v) (os', is)
      else if c = 0 then
        (* we want o *)
        loop (true :: v) (os', is')
      else (
        (* not possible: i must be in o *)
        !logger.error "Field %s is not in its parent output" i.typ_name ;
        assert false)
    | [], _ ->
      !logger.error "More inputs than parent outputs?" ;
      assert false
  in
  loop [] (ser_out, ser_in)

let dequeue_ringbuf_once ?while_ ?delay_rec ?max_retry_time rb =
  retry_for_ringbuf ?while_ ?delay_rec ?max_retry_time
                    dequeue_alloc rb

let read_ringbuf ?while_ ?delay_rec rb f =
  let open Lwt in
  let rec loop () =
    match%lwt dequeue_ringbuf_once ?while_ ?delay_rec rb with
    | exception (Exit | Timeout) -> return_unit
    | tx ->
      (* f has to call dequeue_commit on the passed tx (as soon as
       * possible): *)
      f tx >>= loop in
  loop ()

let read_buf ?wait_for_more ?while_ ?delay_rec rb init f =
  (* Read tuples by hoping from one to the next using tx_next.
   * Note that we may reach the end of the written content, and will
   * have to wait unless we reached the EOF mark (special value
   * returned by tx_next). *)
  let open Lwt in
  let rec loop usr tx_ =
    match%lwt tx_ with
    | exception (Exit | Timeout | End_of_file) -> return usr
    | exception Empty ->
        assert (wait_for_more <> Some true) ;
        return usr
    | tx ->
        (* Contrary to the wrapping case, f must not call dequeue_commit.
         * Caller must know in which case it is: *)
        let%lwt usr, more_to_come = f usr tx in
        if more_to_come then
          let tx_ = retry_for_ringbuf ?while_ ?wait_for_more
                                      ?delay_rec read_next tx in
          loop usr tx_
        else
          return usr
  in
  let tx_ = retry_for_ringbuf ?while_ ?wait_for_more ?delay_rec
                              read_first rb in
  loop init tx_

let with_enqueue_tx rb sz f =
  let open Lwt in
  let%lwt tx =
    retry_for_ringbuf (enqueue_alloc rb) sz in
  try
    let tmin, tmax = f tx in
    enqueue_commit tx tmin tmax ;
    return_unit
  with exn ->
    (* There is no such thing as enqueue_rollback. We cannot make the rb
     * pointer go backward (or... can we?) but we could have a 1 bit header
     * indicating if an entry is valid or not. *)
    fail exn

let arc_dir_of_bname fname = fname ^".arc"

let int_of_hex s = int_of_string ("0x"^ s)

external strtod : string -> float = "wrap_strtod"
external kill_myself : int -> unit = "wrap_raise"

let parse_archive_file_name fname =
  let mi, rest = String.split ~by:"_" fname in
  let ma, rest = String.split ~by:"_" rest in
  let tmi, rest = String.split ~by:"_" rest in
  let tma, rest = String.rsplit ~by:"." rest in
  if rest <> "b" then failwith ("not an archive file ("^ rest ^")") ;
  int_of_hex mi, int_of_hex ma,
  strtod tmi, strtod tma
(*$= parse_archive_file_name & ~printer:BatPervasives.dump
  (10, 16, 0x1.6bbcc4b69ae36p+30, 0x1.6bbcf3df4c0dbp+30) \
    (parse_archive_file_name \
      "00A_010_0x1.6bbcc4b69ae36p+30_0x1.6bbcf3df4c0dbp+30.b")
 *)

let arc_files_of dir =
  (try Sys.files_of dir
  with Sys_error _ -> Enum.empty ()) //@
  (fun fname ->
    match parse_archive_file_name fname with
    | exception (Not_found | Failure _) -> None
    | mi, ma, t1, t2 -> Some (mi, ma, t1, t2, dir ^"/"^ fname))

let arc_file_compare (s1, _, _, _, _) (s2, _, _, _, _) =
  Int.compare s1 s2

let seq_range bname =
  (* Returns the first and last available seqnums.
   * Takes first from the per.seq subdir names and last from same subdir +
   * rb->stats. *)
  (* Note: in theory we should take that ringbuf lock for reading while
   * enumerating the arc files to prevent rotation to happen, but we
   * consider this operation a best-effort. *)
  let dir = arc_dir_of_bname bname in
  let mi_ma =
    arc_files_of dir |>
    Enum.fold (fun mi_ma (from, to_, _t1, _t2, _fname) ->
      match mi_ma with
      | None -> Some (from, to_)
      | Some (mi, ma) -> Some (min mi from, max ma to_)
    ) None in
  let rb = load bname in
  let s = finally (fun () -> unload rb) stats rb in
  match mi_ma with
  | None -> s.first_seq, s.alloc_count
  | Some (mi, _ma) -> mi, s.first_seq + s.alloc_count

(* All the following functions are here rather than in RamenSerialization
 * or RamenNotification since it has to be included by workers as well, and
 * both RamenSerialization and RamenNotification depends on too many
 * modules for the workers. *)

let notification_nullmask_sz = round_up_to_rb_word (bytes_for_bits 2)
let notification_fixsz = sersize_of_float * 3 + sersize_of_bool

let serialize_notification tx
      (worker, start, event_time, name, firing, certainty, parameters) =
  RingBuf.zero_bytes tx 0 notification_nullmask_sz ; (* zero the nullmask *)
  let write_nullable_thing w sz offs null_i = function
    | None ->
        offs
    | Some v ->
        RingBuf.set_bit tx null_i ;
        w tx offs v ;
        offs + sz in
  let write_nullable_float =
    write_nullable_thing RingBuf.write_float sersize_of_float in
  let write_nullable_bool =
    write_nullable_thing RingBuf.write_bool sersize_of_bool in
  let offs = notification_nullmask_sz in
  let offs =
    RingBuf.write_string tx offs worker ;
    offs + sersize_of_string worker in
  let offs =
    RingBuf.write_float tx offs start ;
    offs + sersize_of_float in
  let offs = write_nullable_float offs 0 event_time in
  let offs =
    RingBuf.write_string tx offs name ;
    offs + sersize_of_string name in
  let offs = write_nullable_bool offs 1 firing in
  let offs =
    RingBuf.write_float tx offs certainty ;
    offs + sersize_of_float in
  let offs =
    write_u32 tx offs (Array.length parameters |> Uint32.of_int) ;
    offs + sersize_of_u32 in
  (* Also the internal nullmask, even though the parameters cannot be
   * NULL: *)
  let int_nullmask_sz = nullmask_sz_of_vector (Array.length parameters) in
  zero_bytes tx offs int_nullmask_sz ;
  let offs = offs + int_nullmask_sz in
  (* Now the vector elements: *)
  let offs =
    Array.fold_left (fun offs (n, v) ->
      (* But wait, this is a tuple, which also requires its nullmask for 2
       * elements: *)
      let offs = offs + nullmask_sz_of_vector 2 in
      RingBuf.write_string tx offs n ;
      let offs = offs + sersize_of_string n in
      RingBuf.write_string tx offs v ;
      offs + sersize_of_string v
    ) offs parameters in
  offs

(* Types have been erased so we cannot use sersize_of_value: *)
let max_sersize_of_notification (worker, _, _, name, _, _, parameters) =
  let psz =
    Array.fold_left (fun sz (n, v) ->
      sz +
      nullmask_sz_of_vector 2 +
      sersize_of_string n + sersize_of_string v
    ) (sersize_of_u32 + nullmask_sz_of_vector (Array.length parameters))
      parameters
  in
  notification_nullmask_sz + notification_fixsz +
  sersize_of_string worker + sersize_of_string name + psz

let write_notif ?delay_rec rb (_, _, event_time, _, _, _, _ as tuple) =
  retry_for_ringbuf ?delay_rec (fun () ->
    let sersize = max_sersize_of_notification tuple in
    let tx = enqueue_alloc rb sersize in
    let tmin, tmax = event_time |? 0., 0. in
    let sz = serialize_notification tx tuple in
    assert (sz <= sersize) ;
    enqueue_commit tx tmin tmax) ()

(* In a few places we need to extract the special parameters firing and
 * certainty from the notification parameters to turn them into actual
 * columns (or give them a default value if they are not specified as
 * parameters). This is better than having special syntax in ramen
 * language for them and special command line arguments, although the
 * user could then legitimately wonder why aren't all parameters usable
 * as tuple fields. *)
let normalize_notif_parameters params =
  let firing, certainty, params =
    List.fold_left (fun (firing, certainty, params) (n ,v as param) ->
      let n' = String.lowercase_ascii n in
      try
        if n' = "firing" then
          Some (RamenTypeConverters.bool_of_string v), certainty, params
        else if n' = "certainty" then
          firing, float_of_string (String.trim v), params
        else
          firing, certainty, (param :: params)
      with e ->
        !logger.warning "Cannot convert %S into a standard %s, leaving it \
                         as a parameter" v n' ;
        firing, certainty, (param :: params)
    ) (None, 0.5, []) params in
  firing, certainty, List.rev params
