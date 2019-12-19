open Batteries
open RamenLog
open RamenHelpers
open Stdint
open RingBuf
module T = RamenTypes
module N = RamenName
module Files = RamenFiles
open RamenTypes

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

let nullmask_bytes_of_tuple_type typ =
  List.fold_left (fun s field_typ ->
    s + (if field_typ.RamenTuple.typ.nullable then 1 else 0)
  ) 0 typ |>
  bytes_for_bits |>
  round_up_to_rb_word

let nullmask_sz_of_tuple ts =
  Array.length ts |> bytes_for_bits |> round_up_to_rb_word

let nullmask_sz_of_record kts =
  Array.length kts |> bytes_for_bits |> round_up_to_rb_word

let nullmask_sz_of_vector d =
  bytes_for_bits d |> round_up_to_rb_word

let sersize_of_float = round_up_to_rb_word 8
let sersize_of_char = round_up_to_rb_word 1
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
let sersize_of_string s =
  sersize_of_u32 + round_up_to_rb_word (String.length s)

let sersize_of_ip = function
  | RamenIp.V4 _ -> rb_word_bytes + sersize_of_ipv4
  | RamenIp.V6 _ -> rb_word_bytes + sersize_of_ipv6

let sersize_of_cidr = function
  | RamenIp.Cidr.V4 _ -> rb_word_bytes + sersize_of_cidrv4
  | RamenIp.Cidr.V6 _ -> rb_word_bytes + sersize_of_cidrv6

let rec ser_array_of_record kts =
  let a =
    Array.filter_map (fun (k, t as kt) ->
      if N.(is_private (field k)) then None else
      match t with
      | { structure = TRecord kts ; nullable } ->
          let kts = ser_array_of_record kts in
          if Array.length kts = 0 then None
          else Some (k, { structure = TRecord kts ; nullable })
      | _ -> Some kt
    ) kts in
  assert (a != kts) ; (* Just checking filter_map don't try to outsmart us *)
  Array.fast_sort (fun (k1,_) (k2,_) -> String.compare k1 k2) a ;
  a

(* Given a kvs (or kts), return an array of (k, v) in serializing order and
 * without private fields. *)
let ser_order kts =
  let a =
    Array.filter (fun (k, _) ->
      not (N.(is_private (field k)))
    ) kts in
  Array.fast_sort (fun (k1, _) (k2, _) -> String.compare k1 k2) a ;
  a

let rec sersize_of_fixsz_typ = function
  | TFloat -> sersize_of_float
  | TChar -> sersize_of_char
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
  | TString | TIp | TCidr | TTuple _ | TVec _ | TList _ | TRecord _ | TMap _
  | TNum | TAny | TEmpty -> assert false

let rec sersize_of_value = function
  | VString s -> sersize_of_string s
  | VFloat _ -> sersize_of_float
  | VChar _ -> sersize_of_char
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
  | VTuple vs -> sersize_of_tuple_value vs
  | VRecord kvs -> sersize_of_record_value kvs
  | VVec vs -> sersize_of_vector_value vs
  | VList vs -> sersize_of_list_value vs
  | VMap _ -> todo "serialization of maps"

and sersize_of_tuple_value vs =
  (* Tuples are serialized in field order, unserializer will know which
   * fields are present so there is no need for meta-data: *)
  (* The bitmask for the null values is stored first (in which all
   * values are considered nullable). *)
  let nullmask_sz = nullmask_sz_of_tuple vs in
  Array.fold_left (fun s v -> s + sersize_of_value v) nullmask_sz vs

and sersize_of_record_value kvs =
  (* We serialize records as we serialize output: in alphabetical order *)
  Array.map snd kvs |> sersize_of_tuple_value

and sersize_of_vector_value vs =
  (* Vectors are serialized in field order, unserializer will also know
   * the size of the vector and the type of its elements: *)
  sersize_of_tuple_value vs

and sersize_of_list_value vs =
  (* Lists are prefixed with their number of elements (before the
   * nullmask): *)
  sersize_of_u32 + sersize_of_tuple_value vs

let has_fixed_size = function
  | TString
  (* Technically, those could have a fixed size, but we always treat them as
   * variable. FIXME: *)
  | TTuple _ | TRecord _ | TVec _ | TList _ -> false
  | _ -> true

let tot_fixsz tuple_typ =
  List.fold_left (fun c t ->
    if not (has_fixed_size t.RamenTuple.typ.structure) then c else
    c + sersize_of_fixsz_typ t.typ.structure
  ) 0 tuple_typ

let rec write_value tx offs = function
  | VFloat f -> write_float tx offs f
  | VString s -> write_string tx offs s
  | VBool b -> write_bool tx offs b
  | VChar c -> write_char tx offs c
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
  | VRecord kvs -> write_record tx offs kvs
  | VVec vs -> write_vector tx offs vs
  | VList vs -> write_list tx offs vs
  | VMap _ -> todo "serialization of maps"
  | VNull -> assert false

(* Tuples are serialized as the succession of the values, after the local
 * nullmask. Since we don't know any longer if the values are nullable,
 * the nullmask has one bit per value in any cases. *)
and write_tuple tx offs vs =
  let nullmask_sz = nullmask_sz_of_tuple vs in
  zero_bytes tx offs nullmask_sz ;
  Array.fold_lefti (fun o bi v ->
    if v = VNull then offs else (
      set_bit tx offs bi ;
      write_value tx o v ;
      o + sersize_of_value v
    )
  ) (offs + nullmask_sz) vs |>
  ignore

and write_record tx offs kvs =
  ser_order kvs |>
  Array.map snd |>
  write_tuple tx offs

and write_vector tx = write_tuple tx

and write_list tx offs vs =
  write_u32 tx offs (Array.length vs |> Uint32.of_int) ;
  write_vector tx (offs + sersize_of_u32) vs

let rec read_value tx offs structure =
  match structure with
  | TFloat  -> VFloat (read_float tx offs)
  | TString -> VString (read_string tx offs)
  | TBool   -> VBool (read_bool tx offs)
  | TChar   -> VChar (read_char tx offs)
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
  | TRecord ts -> VRecord (read_record ts tx offs)
  | TVec (d, t) -> VVec (read_vector d t tx offs)
  | TList t -> VList (read_list t tx offs)
  | TMap _ -> todo "serialization of maps"
  | TNum | TAny | TEmpty -> assert false

and read_constructed_value tx t offs o bi =
  let v =
    if t.nullable && not (get_bit tx offs bi) then VNull
    else read_value tx !o t.structure in
  o := !o + sersize_of_value v ;
  v

(* Tuples, vectors and lists come with a separate nullmask as a prefix,
 * with one bit per element regardless of their nullability (because
 * although we know the type, write_tuple do not): *)
and read_tuple ts tx offs =
  let nullmask_sz = nullmask_sz_of_tuple ts in
  let o = ref (offs + nullmask_sz) in
  Array.mapi (fun bi t ->
    read_constructed_value tx t offs o bi
  ) ts

and read_record kts tx offs =
  (* Return the array of fields and types we are supposed to have, in
   * serialized order. Private fields will not be present in the returned
   * kvs. *)
  let ser = ser_order kts in
  let ts = Array.map (fun (_, t) -> t) ser in
  let vs = read_tuple ts tx offs in
  assert (Array.length vs = Array.length ts) ;
  Array.map2 (fun (k, _) v -> k, v) ser vs

and read_vector d t tx offs =
  let nullmask_sz = nullmask_sz_of_vector d in
  let o = ref (offs + nullmask_sz) in
  Array.init d (fun bi ->
    read_constructed_value tx t offs o bi)

and read_list t tx offs =
  let d = read_u32 tx offs |> Uint32.to_int in
  let bi = (offs + sersize_of_u32) * 8 in
  let nullmask_sz = nullmask_sz_of_vector d in
  let o = ref (offs + sersize_of_u32 + nullmask_sz) in
  Array.init d (fun i ->
    read_constructed_value tx t offs o (bi + i))

(*
 * Various other Helpers:
 *)

(* Unless wait_for_more, this will raise Empty when out of data *)
let retry_for_ringbuf ?(wait_for_more=true) ?while_ ?delay_rec ?max_retry_time f =
  let on = function
    | NoMoreRoom -> true
    | Empty -> wait_for_more
    | _ -> false
  in
  retry ?while_ ~on ~first_delay:0.001 ~max_delay:1. ?delay_rec
        ?max_retry_time f

(* To allow a func to select only some fields from its parent and write only
 * a skip list in the out_ref (to makes serialization easier not out_ref
 * smaller) we serialize all fields in the same order: *)
let ser_tuple_field_cmp (t1, _) (t2, _) =
  N.compare t1.RamenTuple.name t2.RamenTuple.name

(* Reorder RamenTuple fields and skip private fields. Also return as a
 * second component the original position: *)
let ser_tuple_typ_of_tuple_typ ?(recursive=true) tuple_typ =
  tuple_typ |>
  List.fold_left (fun (lst, i as prev) ft ->
    if N.is_private ft.RamenTuple.name then prev else
    if not recursive then (ft, i)::lst, i + 1 else
    match ft.typ.structure with
    | TRecord kts ->
        let kts = ser_array_of_record kts in
        if Array.length kts = 0 then prev
        else
          ({ ft with typ = { ft.typ with structure = TRecord kts } }, i)::lst,
          i + 1
    | _ -> (ft, i)::lst, i + 1
  ) ([], 0) |> fst |>
  List.fast_sort ser_tuple_field_cmp

let dequeue_ringbuf_once ?while_ ?delay_rec ?max_retry_time rb =
  retry_for_ringbuf ?while_ ?delay_rec ?max_retry_time
                    dequeue_alloc rb

let read_ringbuf ?while_ ?delay_rec rb f =
  let rec loop () =
    match dequeue_ringbuf_once ?while_ ?delay_rec rb with
    | exception (Exit | Timeout) ->
        ()
    | tx ->
        (* f has to call dequeue_commit on the passed tx (as soon as
         * possible): *)
        f tx ;
        loop () in
  loop ()

let read_buf ?wait_for_more ?while_ ?delay_rec rb init f =
  (* Read tuples by hoping from one to the next using tx_next.
   * Note that we may reach the end of the written content, and will
   * have to wait unless we reached the EOF mark (special value
   * returned by tx_next). *)
  let read how arg usr k =
    match retry_for_ringbuf ?while_ ?wait_for_more
                            ?delay_rec how arg with
    | exception (Exit | Timeout | End_of_file) -> usr
    | exception Empty ->
        assert (wait_for_more <> Some true) ;
        usr
    | tx -> k usr tx
  in
  let rec loop usr tx =
    (* Contrary to the wrapping case, f must not call dequeue_commit. *)
    let usr, more_to_come = f usr tx in
    if more_to_come then
      read read_next tx usr loop
    else usr
  in
  read read_first rb init loop

let with_enqueue_tx rb sz f =
  let tx =
    retry_for_ringbuf (enqueue_alloc rb) sz in
  let tmin, tmax = f tx in
  (* There is no such thing as enqueue_rollback. We cannot make the rb
   * pointer go backward (or... can we?) but we could have a 1 bit header
   * indicating if an entry is valid or not. *)
  enqueue_commit tx tmin tmax

let arc_dir_of_bname fname =
  N.cat (Files.dirname fname) (N.path "/arc")

let int_of_hex s = int_of_string ("0x"^ s)

external strtod : string -> float = "wrap_strtod"
external kill_myself : int -> unit = "wrap_raise"

type arc_type = RingBuf | Orc

let parse_archive_file_name (fname : N.path) =
  let mi, rest = String.split ~by:"_" (fname :> string) in
  let ma, rest = String.split ~by:"_" rest in
  let tmi, rest = String.split ~by:"_" rest in
  let tma, rest = String.rsplit ~by:"." rest in
  let type_ =
    match rest with
    | "b" -> RingBuf
    | "orc" -> Orc
    | _ ->
        Printf.sprintf2 "not an archive file: %a"
          N.path_print_quoted fname |>
        failwith in
  int_of_hex mi, int_of_hex ma,
  strtod tmi, strtod tma, type_

(*$inject
  module N = RamenName
*)
(*$= parse_archive_file_name & ~printer:BatPervasives.dump
  (10, 16, 0x1.6bbcc4b69ae36p+30, 0x1.6bbcf3df4c0dbp+30, RingBuf) \
    (parse_archive_file_name \
      (N.path "00A_010_0x1.6bbcc4b69ae36p+30_0x1.6bbcf3df4c0dbp+30.b"))
  (10, 16, 0x1.6bbcc4b69ae36p+30, 0x1.6bbcf3df4c0dbp+30, Orc) \
    (parse_archive_file_name \
      (N.path "00A_010_0x1.6bbcc4b69ae36p+30_0x1.6bbcf3df4c0dbp+30.orc"))
*)

let filter_arc_files dir =
  Enum.filter_map (fun fname ->
    match parse_archive_file_name fname with
    | exception (Not_found | Failure _) ->
        None
    | mi, ma, t1, t2, typ ->
        let full_path = N.cat (N.cat dir (N.path "/")) fname in
        Some (mi, ma, t1, t2, typ, full_path))

let arc_files_of dir =
  (try Files.files_of dir
  with Sys_error _ -> Enum.empty ()) |>
  filter_arc_files dir

let arc_file_compare (s1, _, _, _, _, _) (s2, _, _, _, _, _) =
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
    Enum.fold (fun mi_ma (from, to_, _t1, _t2, _typ, _fname) ->
      match mi_ma with
      | None -> Some (from, to_)
      | Some (mi, ma) -> Some (min mi from, max ma to_)
    ) None in
  let rb = load bname in
  let s = finally (fun () -> unload rb) stats rb in
  match mi_ma with
  | None -> s.first_seq, s.alloc_count
  | Some (mi, _ma) -> mi, s.first_seq + s.alloc_count

(* In the ringbuf we actually pass more than tuples: also various kinds of
 * notifications. Also, tuples (and some of those meta-messages) are
 * specific to a * "channel". *)

type message_header =
  | DataTuple of RamenChannel.t (* Followed by a tuple *)
  | EndOfReplay of RamenChannel.t * int (* identifying the replayer *)
  (* Also TBD:
  | Timing of RamenChannel.t * (string * float) list
  | TimeBarrier ... *)

let channel_of_message_header = function
  | DataTuple chn -> chn
  | EndOfReplay (chn, _) -> chn

(* We encode the variant on 4 bits, channel id on 16 and replayer id on
 * 12 so that everything fits in word word for now while we still have
 * plenty of variants left: *)

(* Let's assume there will never be more than 36636 live channels and use
 * only one word for the header: *)
let message_header_sersize = function
  | DataTuple _ -> 4
  | EndOfReplay _ -> 4

let max_replayer_id = 0xFFF
let max_channel_id = 0xFFFF

let write_message_header tx offs = function
  | DataTuple chan ->
      assert ((chan :> int) <= max_channel_id) ;
      write_u32 tx offs (Uint32.of_int ((chan :> int) land max_channel_id))
  | EndOfReplay (chan, replayer_id) ->
      assert ((chan :> int) <= max_channel_id) ;
      assert (replayer_id <= max_replayer_id) ;
      let hi = 0x1000 + replayer_id land max_replayer_id in
      Uint32.(
        shift_left (of_int hi) 16 |>
        logor (of_int ((chan :> int) land max_channel_id))) |>
      write_u32 tx offs

let read_message_header tx offs =
  let u32 = read_u32 tx offs in
  let chan = Uint32.(logand u32 (of_int 0xFFFF) |> to_int) |>
             RamenChannel.of_int in
  match Uint32.(shift_right_logical u32 16 |> to_int) with
  | 0 -> DataTuple chan
  | w when w >= 0x1000 ->
      let replayer_id = w land 0xFFF in
      EndOfReplay (chan, replayer_id)
  | _ ->
      Printf.sprintf
        "read_message_header: invalid header at offset %d: %s"
        offs (Uint32.to_string u32) |>
      invalid_arg

(*
 * Notifications
 *
 * All the following functions are here rather than in RamenSerialization
 * or RamenNotification since it has to be included by workers as well, and
 * both RamenSerialization and RamenNotification depends on too many
 * modules for the workers.
 *)

let notification_nullmask_sz = round_up_to_rb_word (bytes_for_bits 2)
let notification_fixsz = sersize_of_float * 3 + sersize_of_bool

let serialize_notification tx start_offs
      (site, worker, start, event_time, name, firing, certainty, parameters) =
  (* Zero the nullmask: *)
  RingBuf.zero_bytes tx start_offs notification_nullmask_sz ;
  let write_nullable_thing w sz offs null_i = function
    | None ->
        offs
    | Some v ->
        RingBuf.set_bit tx start_offs null_i ;
        w tx offs v ;
        offs + sz in
  let write_nullable_float =
    write_nullable_thing RingBuf.write_float sersize_of_float in
  let write_nullable_bool =
    write_nullable_thing RingBuf.write_bool sersize_of_bool in
  let offs = start_offs + notification_nullmask_sz in
  let offs =
    RingBuf.write_string tx offs site ;
    offs + sersize_of_string site in
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
let max_sersize_of_notification (site, worker, _, _, name, _, _, parameters) =
  let psz =
    Array.fold_left (fun sz (n, v) ->
      sz +
      nullmask_sz_of_vector 2 +
      sersize_of_string n + sersize_of_string v
    ) (sersize_of_u32 + nullmask_sz_of_vector (Array.length parameters))
      parameters
  in
  notification_nullmask_sz + notification_fixsz +
  sersize_of_string site + sersize_of_string worker +
  sersize_of_string name + psz

let write_notif ?delay_rec rb ?(channel_id=RamenChannel.live)
                (_, _, _, event_time, _, _, _, _ as tuple) =
  retry_for_ringbuf ?delay_rec (fun () ->
    let head = DataTuple channel_id in
    let sersize =
      message_header_sersize head +
      max_sersize_of_notification tuple in
    let tx = enqueue_alloc rb sersize in
    let tmin, tmax = event_time |? 0., 0. in
    write_message_header tx 0 head ;
    let sz =
      serialize_notification tx (message_header_sersize head) tuple in
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
          let b, o = RamenTypeConverters.bool_of_string v 0 in
          let o = string_skip_blanks v o in
          if o <> String.length v then
            !logger.warning "Junk at end of firing value %S" v ;
          Some b, certainty, params
        else if n' = "certainty" then
          firing, float_of_string (String.trim v), params
        else
          firing, certainty, (param :: params)
      with e ->
        !logger.warning
          "Cannot convert %S into a standard %s (%s), \
           leaving it as a parameter"
          v n' (Printexc.to_string e) ;
        firing, certainty, (param :: params)
    ) (None, 0.5, []) params in
  firing, certainty, List.rev params
