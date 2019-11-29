(* Small Bloom Filter for the novelty operator. *)
open Batteries
open RamenLog
open RamenHelpers
open RamenConsts

(* TODO: use RamenBitmask *)
type t =
  { bytes : Bytes.t ;
    num_bits : int ; num_keys : int ;
    salt : int list ;
    mutable num_bits_set : int }

let make num_bits num_keys =
  let len = (num_bits + 7)/ 8 in
  { bytes = Bytes.make len (Char.chr 0) ;
    num_bits ; num_keys ; num_bits_set = 0 ;
    salt = List.init num_keys (fun _ -> Random.int max_int_for_random) }

let bit_loc_of_bit b =
  b lsr 3, b land 7

let get_bit t b =
  let idx, b_off = bit_loc_of_bit b in
  let n = Bytes.get t.bytes idx |> Char.code in
  (n lsr b_off) land 1 = 1

let fill_ratio t =
  float_of_int t.num_bits_set /. float_of_int t.num_bits

let false_positive_likelihood t =
  let k = float_of_int t.num_keys in
  (1. -. exp ~-.(k *. fill_ratio t)) ** k

let set_bit t b =
  let idx, b_off = bit_loc_of_bit b in
  let n = Bytes.get t.bytes idx |> Char.code in
  let mask = 1 lsl b_off in
  if n land mask = 0 then (
    t.num_bits_set <- t.num_bits_set + 1 ;
    Bytes.set t.bytes idx (Char.chr (n lor (1 lsl b_off)))
  )

(*$= get_bit & ~printer:(BatIO.to_string BatBool.print)
  false (let t = make 1 1 in get_bit t 0)
  true (let t = make 1 1 in set_bit t 0 ; get_bit t 0)
  true (let t = make 10 1 in set_bit t 9 ; get_bit t 9)
  false (let t = make 10 1 in set_bit t 9 ; get_bit t 5)
 *)

type key = int list

let key t x =
  let h0 = Hashtbl.hash x in
  List.fold_left (fun prev_h salt ->
    let h = Hashtbl.hash (salt :: prev_h) in
    h :: prev_h) [h0] t.salt

let get_by_key t k =
  List.exists (fun h ->
    let b = h mod t.num_bits in
    not (get_bit t b)) k |> not

let get t x =
  let k = key t x in
  get_by_key t k

let set_by_key t k =
  List.iter (fun h ->
    let b = h mod t.num_bits in
    set_bit t b) k

let set t x =
  let k = key t x in
  set_by_key t k

(*$inject
  let t =
    let t = make 100 2 in
    set t "foo" ;
    set t "bar" ;
    t
 *)
(*$= get & ~printer:(BatIO.to_string BatBool.print)
  true (get t "foo")
  true (get t "bar")
  false (get t "baz")
 *)

(* Now for the slicing *)

type slice =
  { filter : t ;
    start_time : float }

type sliced_filter =
  { slices : slice array ;
    slice_width : float ;
    num_keys : int ;
    num_bits_per_item : float ;
    mutable current : int }

let make_slice num_bits num_keys start_time =
  { filter = make num_bits num_keys ; start_time }

let make_sliced start_time num_slices slice_width false_positive_ratio =
  (* We aim for the given probability of false positive. *)
  let num_keys = ~- (RamenHelpers.round_to_int (
    log false_positive_ratio /. log 2.)) in
  (* num_bits_per_item: the ratio between the size of the bloom filter and
   * the number of inserted items: *)
  let num_bits_per_item = ~-.1.44 *. log false_positive_ratio /. log 2. in
  let num_bits = 65536 in (* initial guess *)
  !logger.debug "Rotating bloom-filter: starting at time %f \
                 with %d keys, %f bits per items, %d bits \
                 (%d slices of duration %f)\n"
    start_time num_keys num_bits_per_item num_bits num_slices slice_width ;
  { slices = Array.init num_slices (fun i ->
      let start_time = start_time +. float_of_int i *. slice_width in
      make_slice num_bits num_keys start_time) ;
    slice_width ; num_keys ; num_bits_per_item ; current = 0 }

(* Tells if x has been seen earlier (and remembers it). If x time is
 * before the range of remembered data returns false (not seen). *)
let remember sf time x =
  (* It is OK to stay a long time without adding a new value, but beware
   * that bogus times may deadloop here: *)
  if time > sf.slices.(sf.current).start_time +.
            1000. *. sf.slice_width *. float_of_int (Array.length sf.slices)
  then
    Printf.sprintf2 "BloomFilter.remember: bogus time %f > %f + %f * %d"
      time
      sf.slices.(sf.current).start_time
      sf.slice_width
      (Array.length sf.slices) |>
    failwith ;
  (* Should we rotate? *)
  let rec loop () =
    let end_time =
      sf.slices.(sf.current).start_time +. sf.slice_width in
    if time >= end_time then (
      sf.current <- (sf.current + 1) mod Array.length sf.slices ;
      (* Given the number of bit sets, maybe enlarge the hash *)
      let fr = fill_ratio sf.slices.(sf.current).filter in
      let fpl = false_positive_likelihood sf.slices.(sf.current).filter in
      let epsilon = 1e-9 in
      let minmax mi (x : float) ma =
        if x < mi then mi else if x > ma then ma else x in
      let num_inserted s =
        if s.filter.num_bits_set = 0 then 0. else
        ~-. (float_of_int s.filter.num_bits /. float_of_int sf.num_keys) *.
            log (1. -. (minmax epsilon (fill_ratio s.filter) (1. -. epsilon))) |>
        max (float_of_int s.filter.num_bits_set) in
      (* We don't know how many items will be inserted in the next slice so
       * we prepare for the worse: *)
      let num_inserted = Array.fold_left (fun n s ->
          max (num_inserted s) n) 0. sf.slices in
      let num_bits = sf.num_bits_per_item *. num_inserted |>
                     int_of_float |>
                     max 1024 in
      (* Avoid decreasing too fast *)
      let num_bits =
        if num_bits >= sf.slices.(sf.current).filter.num_bits then num_bits
        else (num_bits + sf.slices.(sf.current).filter.num_bits) / 2 in
      (if fr > 0.6 then !logger.info else !logger.debug)
        "Rotating bloom-filter, expunging filter %d filled up to %.02f%% \
         (%d/%d bits set, max of %d inserted items in all slices (estimated), \
         %.02f%% false positives), next size will be %d bits"
        sf.current (100. *. fr)
        sf.slices.(sf.current).filter.num_bits_set
        sf.slices.(sf.current).filter.num_bits
        (int_of_float num_inserted)
        (100. *. fpl) num_bits ;
      (* TODO: a slice_reset that does not allocate (if we keep a close size) *)
      sf.slices.(sf.current) <- make_slice num_bits sf.num_keys end_time ;
      loop ()
    ) in
  loop () ;
  (* TODO: early exit *)
  let rem =
    Array.fold_left (fun rem slice ->
      let rem = rem || get slice.filter x in
      if time >= slice.start_time &&
         time < slice.start_time +. sf.slice_width
      then set slice.filter x ;
      rem
    ) false sf.slices in
  rem
