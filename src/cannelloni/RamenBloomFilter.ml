(* Small Bloom Filter for the novelty operator. *)
open Batteries

type t =
  { bytes : Bytes.t ;
    nb_bits : int }

let make nb_bits =
  let len = (nb_bits + 7)/ 8 in
  { bytes = Bytes.make len (Char.chr 0) ; nb_bits }

let bit_loc_of_bit b =
  b lsr 3, b land 7

let get_bit t b =
  let idx, b_off = bit_loc_of_bit b in
  let n = Bytes.get t.bytes idx |> Char.code in
  (n lsr b_off) land 1 = 1

let set_bit t b =
  let idx, b_off = bit_loc_of_bit b in
  let n = Bytes.get t.bytes idx |> Char.code in
  Bytes.set t.bytes idx (Char.chr (n lor (1 lsl b_off)))

(*$= get_bit & ~printer:(BatIO.to_string BatBool.print)
  false (let t = make 1 1 in get_bit t 0)
  true (let t = make 1 1 in set_bit t 0 ; get_bit t 0)
  true (let t = make 10 1 in set_bit t 9 ; get_bit t 9)
  false (let t = make 10 1 in set_bit t 9 ; get_bit t 5)
 *)

type key = int list

let key salt x =
  let h0 = Hashtbl.hash x in
  List.fold_left (fun prev_h salt ->
    let h = Hashtbl.hash (salt :: prev_h) in
    h :: prev_h) [h0] salt

let get_by_key t k =
  List.exists (fun h ->
    let b = h mod t.nb_bits in
    not (get_bit t b)) k |> not

let get t salt x =
  let k = key salt x in
  get_by_key t k

let set_by_key t k =
  List.iter (fun h ->
    let b = h mod t.nb_bits in
    set_bit t b) k

let set t salt x =
  let k = key salt x in
  set_by_key t k

(*$inject
  let t =
    let t = make 10 3 in
    set t "foo" ;
    set t "bar" ;
    t
 *)
(*$= get & ~printer:(BatIO.to_string BatBool.print)
  true (get t [1;2] "foo")
  true (get t [1;2] "bar")
  false (get t [1;2] "baz")
 *)

(* Now for the slicing *)

type slice =
  { filter : t ;
    start_time : float }

type sliced_filter =
  { slices : slice array ;
    nb_bits : int ;
    salt : int list ;
    slice_width : float ;
    mutable current : int }

let make_slice nb_bits start_time =
  { filter = make nb_bits ; start_time }

let make_sliced start_time nb_slices slice_width nb_bits nb_keys =
  let max_int_for_random = 0x3FFFFFFF in
  { slices = Array.init nb_slices (fun i ->
      let start_time = start_time +. float_of_int i *. slice_width in
      make_slice nb_bits start_time) ;
    slice_width ; nb_bits ; current = 0 ;
    salt = List.init nb_keys (fun _ -> Random.int max_int_for_random) }

(* Tells if x has been seen earlier (and remember it). If x time is
 * before the range of remembered data returns false (not seen). *)
let remember sf time x =
  let k = key sf.salt x in
  (* TODO: early exit *)
  let rem =
    Array.fold_left (fun rem slice ->
      let rem = rem || get_by_key slice.filter k in
      if time >= slice.start_time && time < slice.start_time +. sf.slice_width then
        set_by_key slice.filter k ;
      rem) false sf.slices in
  (* Should we rotate? *)
  let rec loop () =
    let end_time =
      sf.slices.(sf.current).start_time +. sf.slice_width in
    if time >= end_time then (
      sf.current <- sf.current + 1 ;
      sf.slices.(sf.current) <- make_slice sf.nb_bits end_time ;
      loop ()
    ) in
  loop () ;
  rem
