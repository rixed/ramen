open Batteries
open RamenLog
open RamenHelpers
open RamenTypes
open Stdint
open RingBuf

exception NoMoreRoom
exception Empty
let () =
  Callback.register_exception "ringbuf full exception" NoMoreRoom ;
  Callback.register_exception "ringbuf empty exception" Empty

let nullmask_bytes_of_tuple_type tuple_typ =
  List.fold_left (fun s field_typ ->
    if not (is_private_field field_typ.RamenTuple.typ_name) &&
       field_typ.RamenTuple.nullable
    then s+1 else s) 0 tuple_typ |>
  bytes_for_bits |>
  round_up_to_rb_word

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
  | TString | TIp | TCidr | TTuple _ | TVec _ | TNum | TAny -> assert false

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
and sersize_of_tuple vs =
  (* Tuples are serialized in field order, unserializer will know which
   * fields are present so there is no need for meta-data: *)
  Array.fold_left (fun s v -> s + sersize_of_value v) 0 vs
and sersize_of_vector vs =
  (* Vectors are serialized in field order, unserializer will also know
   * the size of the vector and the type of its elements: *)
  sersize_of_tuple vs

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
  | VNull -> assert false
(* Tuples are serialized as the succession of the values. No meta data
 * required. *)
and write_tuple tx offs vs =
  Array.fold_left (fun sz v ->
    write_value tx (offs + sz) v ;
    sz + sersize_of_value v
  ) 0 vs |>
  ignore
and write_vector tx = write_tuple tx

let rec read_value tx offs = function
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
  | TNum | TAny -> assert false
and read_tuple ts tx offs =
  let offs = ref offs in
  Array.map (fun t ->
    let v = read_value tx !offs t in
    offs := !offs + sersize_of_value v ;
    v) ts
and read_vector d t tx offs =
  let offs = ref offs in
  Array.init d (fun _ ->
    let v = read_value tx !offs t in
    offs := !offs + sersize_of_value v ;
    v)

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
(* Here rather than in RamenSerialization since it has to be included by
 * workers as well, and RamenSerialization depends on too many modules for
 * the workers. *)
let write_notif ?delay_rec rb worker notif =
  retry_for_ringbuf ?delay_rec (fun () ->
    let sersize = sersize_of_string worker +
                  sersize_of_string notif in
    let tx = enqueue_alloc rb sersize in
    write_string tx 0 worker ;
    let offs = sersize_of_string worker in
    write_string tx offs notif ;
    (* Note: Surely there should be some time info for notifs. *)
    enqueue_commit tx 0. 0.) ()
