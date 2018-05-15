open Batteries
open RamenLog
open RamenHelpers
open RamenScalar
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

(* Unless wait_for_more, this will raise Empty when out of data *)
let retry_for_ringbuf ?(wait_for_more=true) ?while_ ?delay_rec ?max_retry_time f =
  let on = function
    | NoMoreRoom -> Lwt.return_true
    | Empty -> Lwt.return wait_for_more
    | _ -> Lwt.return_false
  in
  retry ?while_ ~on ~first_delay:0.001 ~max_delay:1. ?delay_rec
        ?max_retry_time (fun x -> Lwt.wrap (fun () -> f x))

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
  | TNull -> sersize_of_null
  | TEth -> sersize_of_eth
  | TCidrv4 -> sersize_of_cidrv4
  | TCidrv6 -> sersize_of_cidrv6
  | TString | TIp | TCidr | TNum | TAny -> assert false

let sersize_of_value = function
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
