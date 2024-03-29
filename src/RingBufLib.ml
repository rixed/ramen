open Batteries
open RamenLog
open RamenHelpersNoLog
open RamenHelpers
open Stdint
open RingBuf
module DT = DessserTypes
module Files = RamenFiles
module N = RamenName
module T = RamenTypes
open Raql_value

(* Return sersizes in bytes for any given type: *)
let sersize_of_unit = 0
let sersize_of_float = round_up_to_rb_word 8
let sersize_of_char = round_up_to_rb_word 1
let sersize_of_bool = round_up_to_rb_word 1
let sersize_of_u8 = round_up_to_rb_word 1
let sersize_of_i8 = round_up_to_rb_word 1
let sersize_of_u16 = round_up_to_rb_word 2
let sersize_of_i16 = round_up_to_rb_word 2
let sersize_of_u24 = round_up_to_rb_word 3
let sersize_of_i24 = round_up_to_rb_word 3
let sersize_of_u32 = round_up_to_rb_word 4
let sersize_of_i32 = round_up_to_rb_word 4
let sersize_of_u40 = round_up_to_rb_word 5
let sersize_of_i40 = round_up_to_rb_word 5
let sersize_of_u48 = round_up_to_rb_word 6
let sersize_of_i48 = round_up_to_rb_word 6
let sersize_of_u56 = round_up_to_rb_word 7
let sersize_of_i56 = round_up_to_rb_word 7
let sersize_of_u64 = round_up_to_rb_word 8
let sersize_of_i64 = round_up_to_rb_word 8
let sersize_of_u128 = round_up_to_rb_word 16
let sersize_of_i128 = round_up_to_rb_word 16
let sersize_of_ipv4 = round_up_to_rb_word 4
let sersize_of_ipv6 = round_up_to_rb_word 16
let sersize_of_null = 0
let sersize_of_eth = round_up_to_rb_word 6
(* Cidrs are encoded as records, so with a nullmask: *)
let sersize_of_cidrv4 = round_up_to_rb_word 1 + sersize_of_ipv4 + sersize_of_u8
let sersize_of_cidrv6 = round_up_to_rb_word 1 + sersize_of_ipv6 + sersize_of_u8
let sersize_of_string s =
  sersize_of_u32 + round_up_to_rb_word (String.length s)

let sersize_of_ip = function
  | RamenIp.V4 _ -> DessserRamenRingBuffer.word_size + sersize_of_ipv4
  | RamenIp.V6 _ -> DessserRamenRingBuffer.word_size + sersize_of_ipv6

let sersize_of_cidr = function
  | RamenIp.Cidr.V4 _ -> DessserRamenRingBuffer.word_size + sersize_of_cidrv4
  | RamenIp.Cidr.V6 _ -> DessserRamenRingBuffer.word_size + sersize_of_cidrv6

(* Given a kvs (or kts), return an array of (k, v) in serializing order. *)
let ser_order kts =
  let a = Array.copy kts in
  Array.fast_sort (fun (k1, _) (k2, _) -> String.compare k1 k2) a ;
  a

let rec sersize_of_fixsz_typ = function
  | DT.TVoid -> sersize_of_unit
  | TFloat -> sersize_of_float
  | TChar -> sersize_of_char
  | TBool -> sersize_of_bool
  | TU8 -> sersize_of_u8
  | TI8 -> sersize_of_i8
  | TU16 -> sersize_of_u16
  | TI16 -> sersize_of_i16
  | TU24 -> sersize_of_u24
  | TI24 -> sersize_of_i24
  | TU32 -> sersize_of_u32
  | TI32 -> sersize_of_i32
  | TU40 -> sersize_of_u40
  | TI40 -> sersize_of_i40
  | TU48 -> sersize_of_u48
  | TI48 -> sersize_of_i48
  | TU56 -> sersize_of_u56
  | TI56 -> sersize_of_i56
  | TU64 -> sersize_of_u64
  | TI64 -> sersize_of_i64
  | TU128 -> sersize_of_u128
  | TI128 -> sersize_of_i128
  | TUsr { name = "Ip4" ; _ } -> sersize_of_ipv4
  | TUsr { name = "Ip6" ; _ } -> sersize_of_ipv6
  | TUsr { name = "Eth" ; _ } -> sersize_of_eth
  | TUsr { name = "Cidr4" ; _ } -> sersize_of_cidrv4
  | TUsr { name = "Cidr6" ; _ } -> sersize_of_cidrv6
  (* FIXME: Vec (d, t) should be a fixsz typ if t is one. *)
  | t ->
      Printf.sprintf2 "Cannot sersize_of_fixsz_typ %a"
        DT.print t |>
      failwith

let has_fixed_size = function
  | DT.TString
  (* Technically, those could have a fixed size, but we always treat them as
   * variable. FIXME: *)
  | TTup _ | TRec _ | TVec _ | TArr _ | TLst _ | TSum _
  | TUsr { name = "Ip"|"Cidr" ; _ } ->
      false
  | _ ->
      true

let tot_fixsz tuple_typ =
  List.fold_left (fun c t ->
    let vt = t.RamenTuple.typ.DT.typ in
    if has_fixed_size vt then c + sersize_of_fixsz_typ vt else c
  ) 0 tuple_typ

(* Return both the value and the new offset: *)
let rec read_value tx offs vt =
  match vt with
  | DT.TVoid ->
      VUnit, offs
  | TFloat ->
      VFloat (read_float tx offs), offs + sersize_of_float
  | TString ->
      let s = read_string tx offs in
      VString s, offs + sersize_of_string s
  | TBool ->
      VBool (read_bool tx offs), offs + sersize_of_bool
  | TChar ->
      VChar (read_char tx offs), offs + sersize_of_char
  | TU8 ->
      VU8 (read_u8 tx offs), offs + sersize_of_u8
  | TU16 ->
      VU16 (read_u16 tx offs), offs + sersize_of_u16
  | TU24 ->
      VU24 (read_u24 tx offs), offs + sersize_of_u24
  | TU32 ->
      VU32 (read_u32 tx offs), offs + sersize_of_u32
  | TU40 ->
      VU40 (read_u40 tx offs), offs + sersize_of_u40
  | TU48 ->
      VU48 (read_u48 tx offs), offs + sersize_of_u48
  | TU56 ->
      VU56 (read_u56 tx offs), offs + sersize_of_u56
  | TU64 ->
      VU64 (read_u64 tx offs), offs + sersize_of_u64
  | TU128 ->
      VU128 (read_u128 tx offs), offs + sersize_of_u128
  | TI8 ->
      VI8 (read_i8 tx offs), offs + sersize_of_i8
  | TI16 ->
      VI16 (read_i16 tx offs), offs + sersize_of_i16
  | TI24 ->
      VI24 (read_i24 tx offs), offs + sersize_of_i24
  | TI32 ->
      VI32 (read_i32 tx offs), offs + sersize_of_i32
  | TI40 ->
      VI40 (read_i40 tx offs), offs + sersize_of_i40
  | TI48 ->
      VI48 (read_i48 tx offs), offs + sersize_of_i48
  | TI56 ->
      VI56 (read_i56 tx offs), offs + sersize_of_i56
  | TI64 ->
      VI64 (read_i64 tx offs), offs + sersize_of_i64
  | TI128 ->
      VI128 (read_i128 tx offs), offs + sersize_of_i128
  | TUsr { name = "Eth" ; _ } ->
      VEth (read_eth tx offs), offs + sersize_of_eth
  | TUsr { name = "Ip4"; _ } ->
      VIpv4 (read_u32 tx offs), offs + sersize_of_u32
  | TUsr { name = "Ip6" ; _ } ->
      VIpv6 (read_u128 tx offs), offs + sersize_of_u128
  | TUsr { name = "Ip" ; _ } ->
      let v = read_ip tx offs in
      VIp v, offs + sersize_of_ip v
  | TUsr { name = "Cidr4" ; _ } ->
      VCidrv4 (read_cidr4 tx offs), offs + sersize_of_cidrv4
  | TUsr { name = "Cidr6" ; _ } ->
      VCidrv6 (read_cidr6 tx offs), offs + sersize_of_cidrv6
  | TUsr { name = "Cidr" ; _ } ->
      let v = read_cidr tx offs in
      VCidr v, offs + sersize_of_cidr v
  | TTup ts ->
      let v, offs = read_tuple ts tx offs in
      VTup v, offs
  | TRec ts ->
      let v, offs = read_record ts tx offs in
      VRec v, offs
  | TVec (d, t) ->
      let v, offs = read_vector d t tx offs in
      VVec v, offs
  | TArr t ->
      let v, offs = read_array t tx offs in
      VArr v, offs
  | TSet _ ->
      todo "unserialization of sets"
  | TMap _ ->
      todo "unserialization of maps"
  | TSum _ ->
      todo "unserialization of sum values"
  | t ->
      invalid_arg ("read_value: "^ DT.to_string t)

and read_constructed_value tx t offs o bi =
  let v, o' =
    if DessserRamenRingBuffer.BitMaskWidth.has_bit t &&
       get_bit tx offs bi then VNull, !o
    else read_value tx !o t.DT.typ in
  o := o' ;
  v

(* Tuples, records and sums come with a mandatory nullmask, whereas vectors and
 * lists have a nullmask only if their items are actually nullable. *)
and read_tuple ts tx offs =
  let nullmask_words = RingBuf.read_u8 tx offs |> Uint8.to_int in
  let o = ref (offs + DessserRamenRingBuffer.word_size * nullmask_words) in
  (* Returns both the value and the new offset: *)
  let v =
    let bi = ref 8 in
    Array.map (fun t ->
      let v = read_constructed_value tx t offs o !bi in
      (* Until only null values have a bit: *)
      if DessserRamenRingBuffer.BitMaskWidth.has_bit t then incr bi ;
      v
    ) ts in
  v, !o

and read_record kts tx offs =
  (* Return the array of fields and types we are supposed to have, in
   * serialized order. *)
  let ser = ser_order kts in
  let ts = Array.map snd ser in
  let vs, offs' = read_tuple ts tx offs in
  assert (Array.length vs = Array.length ts) ;
  let v = Array.map2 (fun (k, _) v -> k, v) ser vs in
  v, offs'

(* Vectors/lists have a nullmask if their items (of type [t]) are nullable. *)
and read_vector d t tx offs =
  let has_nullmask = DessserRamenRingBuffer.BitMaskWidth.has_bit t in
  let nullmask_words =
    if not has_nullmask then 0 else
      RingBuf.read_u8 tx offs |> Uint8.to_int in
  let o = ref (offs + DessserRamenRingBuffer.word_size * nullmask_words) in
  let v =
    Array.init d (fun bi ->
      let bi = if has_nullmask then bi + 8 else 0 in
      read_constructed_value tx t offs o bi) in
  v, !o

and read_array t tx offs =
  let d = read_u32 tx offs |> Uint32.to_int in
  if d >= 999999 then !logger.error "read_array: d = %d at offset %d" d offs ;
  assert (d < 999999) ;
  let offs = offs + sersize_of_u32 in
  let has_nullmask = DessserRamenRingBuffer.BitMaskWidth.has_bit t in
  let nullmask_words =
    if not has_nullmask then 0 else
      RingBuf.read_u8 tx offs |> Uint8.to_int in
  let o = ref (offs + DessserRamenRingBuffer.word_size * nullmask_words) in
  let v =
    Array.init d (fun bi ->
      let bi = if has_nullmask then bi + 8 else 0 in
      read_constructed_value tx t offs o bi) in
  v, !o

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
 * 12 so that everything fits in one dword for now while we still have
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
      write_u32 tx offs (Uint32.of_int (chan :> int))
  | EndOfReplay (chan, replayer_id) ->
      assert ((chan :> int) <= max_channel_id) ;
      assert (replayer_id <= max_replayer_id) ;
      let hi = 0x1000 + replayer_id in
      Uint32.(
        shift_left (of_int hi) 16 |>
        logor (of_int (chan :> int))) |>
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
 * In a few places we need to extract the special parameters (firing,
 * certainty, ...) from the notification parameters to turn them into actual
 * columns (or give them a default value if they are not specified as
 * parameters). This is better than having special syntax in ramen
 * language for them and special command line arguments. *)
type normalized_params =
  { mutable firing : bool ;
    mutable certainty : float ;
    mutable debounce : float ;
    mutable timeout : float }

let normalize_notif_parameters params =
  let default =
    { firing = true ; certainty = 0.5 ; debounce = 0. ; timeout = 0. } in
  let norms, params =
    Array.fold_left (fun (norms, params) (n, v as param) ->
      let n' = String.lowercase_ascii n in
      try
        if n' = "firing" then
          let b, o = RamenTypeConverters.bool_of_string v 0 in
          let o = string_skip_blanks v o in
          if o <> String.length v then
            !logger.warning "Junk at end of firing value %S" v ;
          { norms with firing = b }, params
        else if n' = "certainty" then
          { norms with certainty = float_of_string (String.trim v) }, params
        else if n' = "debounce" then
          { norms with debounce = float_of_string (String.trim v) }, params
        else if n' = "timeout" then
          { norms with timeout = float_of_string (String.trim v) }, params
        else
          norms, (param :: params)
      with e ->
        !logger.warning
          "Cannot convert %S into a standard %s (%s), \
           leaving it as a parameter"
          v n' (Printexc.to_string e) ;
        norms, (param :: params)
    ) (default, []) params in
  norms.firing, norms.certainty, norms.debounce, norms.timeout, List.rev params
