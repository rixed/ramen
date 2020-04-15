(* Functions used by the generated OCaml code to implement various
 * operators. *)
open Batteries
open Stdint
open RamenLog
open RamenHelpersNoLog
open RamenHelpers
open RamenNullable
open RamenConsts

(*$inject open Batteries *)

let now = ref (Unix.gettimeofday ())
let first_input = ref None
let last_input = ref None

let on_each_input_pre () =
  let t = Unix.gettimeofday () in
  now := t ;
  if !first_input = None then first_input := Some t ;
  last_input := Some t

(* Get parameters from the environment.
 * This function is called at module initialization time to get the (constant)
 * value of a parameter (with default value in [def]): *)
let parameter_value ~def parser name =
  let envvar = param_envvar_prefix ^ name in
  !logger.debug "Looking for envvar %S" envvar ;
  match Sys.getenv envvar with
  | exception Not_found -> def
  | s ->
      try
        check_parse_all s (parser s)
      with e ->
        let what =
          Printf.sprintf "Cannot parse value %s for parameter %s: %s"
                         s name (Printexc.to_string e) in
        print_exception ~what e ;
        exit ExitCodes.cannot_parse_param

(* For experiment names, we don't know in advance the experiment name we are
 * going to use so we just read them all from the whole envvar vector: *)
let experiment_variants =
  let h = Hashtbl.create 17 in
  Unix.environment () |>
  Array.iter (fun str ->
    match String.split ~by:"=" str with
    | exception Not_found -> ()
    | n, v ->
        let pref = exp_envvar_prefix in
        if String.starts_with n pref then (
          let n = String.lchop ~n:(String.length pref) n in
          Hashtbl.add h n v)) ;
  h

let get_variant exp_name =
  try NotNull (Hashtbl.find experiment_variants exp_name)
  with Not_found -> Null

(* Functions *)

let and_opt a b =
  match a, b with
  | NotNull false, _ | _ , NotNull false -> NotNull false
  | NotNull a, NotNull b -> NotNull (a && b)
  | _ -> Null

let or_opt a b =
  match a, b with
  | NotNull true, _ | _, NotNull true -> NotNull true
  | NotNull a, NotNull b -> NotNull (a || b)
  | _ -> Null

let age_float x = !now -. x
let age_u8 = Uint8.of_float % age_float
let age_u16 = Uint16.of_float % age_float
let age_u32 = Uint32.of_float % age_float
let age_u64 = Uint64.of_float % age_float
let age_u128 = Uint128.of_float % age_float
let age_i8 = Int8.of_float % age_float
let age_i16 = Int16.of_float % age_float
let age_i32 = Int32.of_float % age_float
let age_i64 = Int64.of_float % age_float
let age_i128 = Int128.of_float % age_float
(* FIXME: typecheck age_eth, age_ipv4 etc out of existence *)

let aggr_min s x =
  match s with
  | Null -> NotNull x
  | NotNull y -> NotNull (min x y)

let aggr_max s x =
  match s with
  | Null -> NotNull x
  | NotNull y -> NotNull (max x y)

let aggr_first s x =
  match s with
  | Null -> NotNull x
  | y -> y

let aggr_last _ x = NotNull x

let kahan_init = 0., 0.
let kahan_add (sum, c) x =
  let t = sum +. x in
  let c = c +. if Float.abs sum >= Float.abs x then (sum -. t) +. x else (x -. t) +. sum in
  t, c
let kahan_finalize (sum, c) = sum +. c

(* State is count * sum *)
let avg_init = 0, kahan_init
let avg_add (count, kahan_state) x = count + 1, kahan_add kahan_state x
let avg_finalize (count, kahan_state) = kahan_finalize kahan_state /. float_of_int count

(* Multiply a string by an integer *)
let string_repeat s n =
  String.repeat s (Uint32.to_int n)

(* Compute the p percentile of an array of anything: *)
module Percentile = struct
  (*$< Percentile *)
  let swap a i j =
    let tmp = a.(i) in
    a.(i) <- a.(j) ;
    a.(j) <- tmp

  (* Appetizer: partition a sub-array around a given pivot value,
   * and return a position into the final array such that every
   * items up to and including that position are <= pivot and
   * every items after that are >= pivot. *)
  let partition a lo hi p =
    let rec adv_i i j =
      (* Everything that's <= i is <= p,
       * everything that's >= j is >= p,
       * i < j *)
      let i = i + 1 in
      if i >= j then i - 1
      else if a.(i) < p then (adv_i [@tailcall]) i j
      else (rec_j [@tailcall]) i j
    and rec_j i j =
      (* Everything that's < i is <= p, i is > p,
       * everything that's >= j is >= p, j > i-1.
       * If i = 0 we know we have the actual pivot value before to stop j
       * from reading before the beginning of the array. *)
      let j = j - 1 in
      if a.(j) > p then (rec_j [@tailcall]) i j
      else if j <= i then i - 1
      else (
        swap a i j ;
        (* we are back to adv_i precondition: *)
        (adv_i [@tailcall]) i j
      )
    in
    adv_i (lo - 1) (hi + 1)

  (*$inject
    let test_partition arr =
      Q.assume (Array.length arr > 0) ;
      let hi = Array.length arr - 1 in
      let m = arr.(0) in
      let pv = partition arr 0 hi m in
      let res =
        Array.fold_lefti (fun res i v ->
          res && (
            (i <= pv && v <= m) ||
            (i > pv && v >= m)
          )) true arr in
      if not res then (
        Printf.printf "ERROR: pv=%d, m=%d, arr=%a\n" pv m (Array.print Int.print) arr
      ) ;
      res
  *)
  (*$Q test_partition
    (Q.array_of_size (Q.Gen.int_range 1 10) Q.small_int) \
      test_partition
  *)

  (* Sorting a-la quicksort, but looking only at the partitions that has
   * one of the indexes we care about: *)
  let partial_sort a ks =
    let rec loop count lo hi ks =
      (* Add tot count and tot length in instrumentation ? *)
      if count > Array.length a then (
        !logger.error "DEADLOOP: loop lo=%d, hi=%d, ks=%a, a=%a"
          lo hi
          (List.print Int.print) ks
          (Array.print (fun oc v -> String.print oc (dump v))) a ;
        assert false
      ) ;
      if hi - lo > 0 && ks <> [] then (
        (* We are going to use partition in that way:
         * First, we take one random value as the pivot and put it in front.
         * Then, we partition the rest of the array around
         * that pivot value. The returned position is the last of the first
         * partition. We can then put the pivot at the end of it, so we have
         * "sorted" that value. *)
        let pv = lo + (hi - lo) / 2 in (* not that random *)
        swap a lo pv ;
        let m = a.(lo) in
        let pv = partition a (lo + 1) hi m in
        swap a lo pv ;
        let ks_lo, ks_hi = List.partition (fun k -> k <= pv) ks in
        let c = loop (count + 1) lo pv ks_lo in
        loop c (pv + 1) hi ks_hi
      ) else count in
    let count = loop 0 0 (Array.length a - 1) ks in
    (*!logger.info "partial_sort %d items for ks=%a in %d steps" (Array.length a) (List.print Int.print) ks count *)
    ignore count

  (* Check that the percentiles we obtain from [a] are the same as those
   * we would get after a full sort *)
  (*$inject
    let test_partial_sort (ps, a) =
      let a_orig = Array.copy a in
      Q.assume (Array.length a > 0) ;
      let hi = Array.length a - 1 in
      let ks =
        Array.map (fun p ->
          let k =
            RamenHelpersNoLog.round_to_int (float_of_int (p * hi) *. 0.01) in
          assert (k >= 0 && k <= hi) ;
          k
        ) ps in
      partial_sort a (Array.to_list ks) ;
      let vs = Array.map (fun k -> a.(k)) ks in
      Array.fast_sort Int.compare a ;
      let res =
        Array.fold_lefti (fun res i k ->
          res && a.(k) = vs.(i)
        ) true ks in
      if not res then (
        Printf.printf "ERROR: a_orig=%a, a=%a, ks=%a\n"
          (Array.print Int.print) a_orig
          (Array.print Int.print) a
          (Array.print Int.print) ks
      ) ;
      res
  *)
  (*$Q test_partial_sort
    (Q.pair (Q.array_of_size (Q.Gen.int_range 0 3) (Q.int_range 0 100)) \
            (Q.array_of_size (Q.Gen.int_range 1 10) Q.small_int)) \
      test_partial_sort
  *)

  let multi ps arr =
    let ks =
      Array.map (fun p ->
        assert (p >= 0.0 && p <= 100.0) ;
        round_to_int (p *. 0.01 *. float_of_int (Array.length arr - 1))
      ) ps in
    partial_sort arr (Array.to_list ks) ;
    Array.map (fun k -> arr.(k)) ks

  (* Mono-valued variant of the above: *)
  let single p arr =
    (multi [| p |] arr).(0)

  (*$>*)
end

let index s c =
  Int32.of_int (try String.index s c with Not_found -> -1)

let chr i =
  (Uint32.to_int i land 255) |> Char.chr

let substring s a b =
  let a = Int32.to_int a
  and b = Int32.to_int b in
  let l = String.length s in
  let a, b = min a l, min b l in
  let a = if a < 0 then a + l else a
  and b = if b < 0 then b + l else b in
  if a >= b then "" else
  String.sub s a (b - a)

let uuid_of_u128 (s: uint128) =
  let buffer = Buffer.create 36 in
  let buffer_without_minus = Buffer.create 32 in
  let hex_str = Uint128.to_string_hex s in
  let number_of_zero_to_add = 32 - (String.length hex_str - 2) in
  for i = 1 to number_of_zero_to_add do
    Buffer.add_char buffer_without_minus '0'
  done ;
  Buffer.add_substring buffer_without_minus hex_str 2 (String.length hex_str - 2) ;
  let buffer_without_minus_str = Buffer.contents buffer_without_minus in

  Buffer.add_substring buffer buffer_without_minus_str 0 8 ;
  Buffer.add_char buffer '-' ;
  Buffer.add_substring buffer buffer_without_minus_str 8 4 ;
  Buffer.add_char buffer '-' ;
  Buffer.add_substring buffer buffer_without_minus_str 12 4 ;
  Buffer.add_char buffer '-' ;
  Buffer.add_substring buffer buffer_without_minus_str 16 4 ;
  Buffer.add_char buffer '-' ;
  Buffer.add_substring buffer buffer_without_minus_str 20 12 ;
  Buffer.contents buffer

(*$inject open Stdint *)
(*$= uuid_of_u128 & ~printer:identity
  "00112233-4455-6677-8899-aabbccddeeff" \
    (uuid_of_u128 @@ Uint128.of_string "0x00112233445566778899aabbccddeeff")
  "00000000-0000-0000-0000-000000000000" \
    (uuid_of_u128 @@ Uint128.zero)
*)

let string_of_chars arr =
  let len = Array.length arr in
  let b = Bytes.create len in
  for i = 0 to len - 1 do
    Bytes.set b i arr.(i)
  done ;
  Bytes.unsafe_to_string b

let string_of_nullable_chars arr =
  let len = Array.length arr in
  let b = Bytes.create len in
  for i = 0 to len - 1 do
    Bytes.set b i (arr.(i) |! '?')
  done ;
  Bytes.unsafe_to_string b

let smooth prev alpha x = x *. alpha +. prev *. (1. -. alpha)

let smooth_damped_holt_init = 0.0, 0.0

let smooth_damped_holt (prev_level, prev_trend) alpha beta phi x =
  let level =
    (alpha *. x) +. (1. -. alpha) *. (prev_level +. phi *. prev_trend) in
  let trend =
    beta *. (level -. prev_level) +. ( 1. -. beta) *. phi *. prev_trend in
  level, trend

let smooth_damped_holt_finalize (prev_level, prev_trend) phi =
  prev_level +. phi *. prev_trend

let smooth_damped_holt_winter_init n =
  let n = Uint8.to_int n in
  0.0, 0.0, Array.make n 0.0, 0

let smooth_damped_holt_winter
    (prev_level, prev_trend, prev_seasons, prev_season_cycle)
    alpha beta gama total_season phi x =
  let total_season = Uint8.to_int total_season in
  let season_cycle = (prev_season_cycle + 1) mod total_season in
  let pred_season = Array.get prev_seasons prev_season_cycle in
  let level =
    (alpha *. (x -. pred_season)) +. (1. -. alpha) *. (prev_level +. phi *. prev_trend) in
  let trend =
    beta *. (level -. prev_level) +. ( 1. -. beta) *. phi *. prev_trend in
  let season =
    gama *. (x -. prev_level -. phi *. prev_trend) +. (1. -. gama) *. pred_season in
  Array.set prev_seasons prev_season_cycle season;
  level, trend, prev_seasons, season_cycle

let smooth_damped_holt_winter_finalize
    (prev_level, prev_trend, prev_seasons, prev_season_cycle) phi =
  prev_level +. phi *. prev_trend +. (Array.get prev_seasons prev_season_cycle)

let split by what k =
  string_nsplit what by |> List.iter k

module Remember = struct
(* Remember values *)
  type state =
    { mutable filter : RamenBloomFilter.sliced_filter option ;
      false_positive_ratio : float ;
      duration : float ;
      mutable last_remembered : bool }

  let init false_positive_ratio duration =
    (* We cannot init the bloom filter before we receive the first tuple
     * since we need starting time: *)
    { filter = None ;
      false_positive_ratio ;
      duration ;
      last_remembered = false }

  let really_init st tim =
    check_finite_float "start time used in REMEMBER operation" tim ;
    let num_slices = 10 in
    let start_time = tim -. st.duration
    and slice_width = st.duration /. float_of_int num_slices in
    let filter =
      RamenBloomFilter.make_sliced start_time num_slices slice_width
                                   st.false_positive_ratio in
    st.filter <- Some filter ;
    filter

  let add st tim es =
    check_finite_float "time used in REMEMBER operation" tim ;
    let filter =
      match st.filter with
      | None -> really_init st tim
      | Some f -> f in
    st.last_remembered <- RamenBloomFilter.remember filter tim es ;
    st

  let finalize st = st.last_remembered
end

module Distinct = struct
  (* Distinct op values *)
  type 'a state =
    { distinct_values : ('a, unit) Hashtbl.t ;
      mutable last_was_distinct : bool }

  let init () =
    { distinct_values = Hashtbl.create 31 ; last_was_distinct = false }

  let add st x =
    (* TODO: a Hashtbl.modify which callback also returns the return value *)
    st.last_was_distinct <- not (Hashtbl.mem st.distinct_values x) ;
    Hashtbl.add st.distinct_values x () ;
    st

  let finalize st = st.last_was_distinct
end

module Top = struct
  (* Heavy Hitters wrappers: *)

  let init max_size duration =
    assert (duration > 0.) ;
    let max_size = Uint32.to_int max_size in
    (* We want an entry weight to be halved after half [duration]: *)
    let decay = -. log 0.5 /. (duration *. 0.5) in
    HeavyHitters.make ~max_size ~decay

  let add s t w x =
    check_finite_float "time used in TOP operation" t ;
    HeavyHitters.add s t w x ;
    s

  let rank s c x =
    HeavyHitters.rank (Uint32.to_int c) x s |>
    nullable_of_option

  let is_in_top s c x =
    HeavyHitters.is_in_top (Uint32.to_int c) x s

  (* Returns the largest [c] items, in decreasing order, as an array: *)
  let to_list s c =
    let c = (Uint32.to_int c) in
    HeavyHitters.top c s |> Array.of_list
end

let hash x = Hashtbl.hash x |> Int64.of_int

let square mul x = mul x x

(* An operator used only for debugging: *)
let print strs =
  !logger.info "PRINT: %a"
    (List.print ~first:"" ~last:"" ~sep:", "
       (fun oc s -> String.print oc (s |! "<NULL>"))) strs

module Hysteresis = struct
  let add was_ok v accept max =
    let extr =
      if was_ok then max else accept in
    if max >= accept then v <= extr else v >= extr

  let finalize is_ok = is_ok
end

module Histogram = struct
  type state =
    { min : float ; span : float ; sw : float ; num_buckets : int ;
      histo : Uint32.t array }

  let init min max num_buckets =
    let histo = Array.create (num_buckets + 2) Uint32.zero in
    let span = max -. min in
    let sw = float_of_int num_buckets /. span in
    { min ; span ; sw ; num_buckets ; histo }

  let add h x =
    check_not_nan "value added to HISTOGRAM" x ;
    let x = x -. h.min in
    let bucket =
      if x < 0. then 0 else
      if x >= h.span then h.num_buckets + 1 else
      let b = int_of_float (x *. h.sw) in
      assert (b >= 0 && b < h.num_buckets) ;
      b + 1 in
    h.histo.(bucket) <- Uint32.succ h.histo.(bucket) ;
    h

  let finalize h = h.histo
end

module Largest = struct
  type ('a, 'b) state =
    { (* Ordered according to some generic value, smaller or greater first
         depending on the inv flag, and we will keep only the N largest
         values: *)
      values : ('a (* value *) * 'b (* order *)) RamenHeap.t ;
      inv : bool ;
      up_to : bool ;
      but : int ;
      max_length : int (* The number of values we want to return *) ;
      length : int (* how many values are there already *) ;
      count : int (* Count insertions, to use as default order *) }

  let init ~inv ~up_to ~but n =
    let n = Uint32.to_int n in
    let but = Uint32.to_int but in
    { values = RamenHeap.empty ; inv ; up_to ; but ; max_length = n ;
      length = 0 ; count = 0 }

  let cmp (_, by1) (_, by2) = compare by1 by2
  let cmp_inv (_, by1) (_, by2) = compare by2 by1

  let add state x by =
    let cmp = if state.inv then cmp_inv else cmp in
    let values = RamenHeap.add cmp (x, by) state.values in
    assert (state.length <= state.max_length) ;
    if state.length < state.max_length then
      { state with
          values ;
          length = state.length + 1 ;
          count = state.count + 1 }
    else
      { state with
          values = RamenHeap.del_min cmp values ;
          count = state.count + 1 }

  let add_on_count state x =
    add state x state.count

  (* Also used by Past.
   * TODO: faster conversion from heap to array. *)
  let array_of_heap_fst cmp h but =
    RamenHeap.fold_left cmp (fun lst (x, _) ->
      x :: lst
    ) [] h |>
    List.drop but |>
    List.rev |>
    Array.of_list

  (* Must return an optional vector of max_length values: *)
  let finalize state =
    if state.length < state.max_length &&
       (not state.up_to || state.length <= state.but)
    then Null
    else
      let cmp = if state.inv then cmp_inv else cmp in
      NotNull (array_of_heap_fst cmp state.values state.but)
end

module Past = struct

  type 'a state =
    { (* Ordered according to time, smaller on top: *)
      values : ('a * float) RamenHeap.t ;
      (*         ^     ^-- this is time
                 |-------- this is the value *)
      max_age : float ;
      (* used to keep track values that will be removed from state.values.
       *
       * Each time we add a value from state.values, we add it from state.sample too.
       * If an old value is removed from state.sample we also remove it from state.values
       * with also all other old values.
       *)
      sample : ('a * float) RamenSampling.reservoir option }

  let init max_age sample_size any_value =
    check_finite_float "max age used in PAST operation" max_age ;
    { sample =
        Option.map (fun sz ->
          RamenSampling.init sz (any_value, 0.)
        ) sample_size ;
      values = RamenHeap.empty ; max_age }

  let cmp (_, t1) (_, t2) = Float.compare t1 t2

  let add state x t =
    check_finite_float "time added in PAST operation" t ;
    let rec out_the_olds h =
      match RamenHeap.min h with
      | exception Not_found -> h
      | _, t' ->
          if t -. t' < state.max_age then h else
            out_the_olds (RamenHeap.del_min cmp h) in
    let add values =
      let values = out_the_olds values in
      let values = RamenHeap.add cmp (x, t) values in
      { state with values }
    in
    match state.sample with
    | None -> add state.values
    | Some r ->
        (match RamenSampling.swap_in r (x, t) with
        | None -> state
        | Some prev ->
          (* When the reservoir was initially empty it will replace
           * any_value with time 0., which is not in the heap.
           * Therefore it is OK that rem fails silently: *)
          (* TODO: try to make RamenHeap.rem destructive *)
          let values = RamenHeap.rem cmp prev state.values in
          add values)

  (* Must return an optional vector of max_length values: *)
  let finalize state =
    NotNull (Largest.array_of_heap_fst cmp state.values 0)
end

module Group = struct
  type 'a state = 'a list
  let add lst x = x :: lst
  let finalize = Array.of_list
end

let strftime ?(gmt=false) str tim =
  let open Unix in
  let tm = (if gmt then gmtime else localtime) tim in
  let replacements =
    [ "%Y", string_of_int (tm.tm_year + 1900) ;
      "%d", Printf.sprintf "%02d" tm.tm_mday ;
      "%H", Printf.sprintf "%02d" tm.tm_hour ;
      "%j", string_of_int tm.tm_yday ;
      "%M", Printf.sprintf "%02d" tm.tm_min ;
      "%m", Printf.sprintf "%02d" (tm.tm_mon + 1) ;
      "%n", "\n" ; "%t", "\t" ;
      "%S", Printf.sprintf "%05.2f"
              (float_of_int tm.tm_sec +. mod_float tim 1.) ;
      "%s", string_of_float tim ;
      "%u", string_of_int tm.tm_wday ] in
  List.fold_left (fun str (sub, by) ->
    String.nreplace ~str ~sub ~by
  ) str replacements

module Truncate =
struct
  (*$< Truncate *)
  (* In all the trunc operations we truncate toward minus infinity, à la
   * Ruby or Python, for the very same practical reason: Basically, we want
   * it to work to resample values (such as timestamps) regardless of their
   * origin. *)

  (* Round the float [n] to the given scale [s] (which must be positive): *)
  let float n s =
    check_not_nan "dividend of TRUNCATE operation" n ;
    check_not_nan "divisor of TRUNCATE operation" s ;
    Float.floor (n /. s) *. s

  (*$= float & ~printer:string_of_float
     0. (float 0. 1.)
     0. (float 0.1 1.)
     0. (float 0.9 1.)
     1. (float 1.0 1.)
     1. (float 1.1 1.)
     1. (float 1.9 1.)
     2. (float 2.0 1.)
     2. (float 2.1 1.)
     5. (float 5.7 1.)
     0. (float 1.0 10.)
     0. (float 9.9 10.)
    10. (float 11.0 10.)
     0. (float 11.0 12.)
    24. (float 25.3 12.)
   (-1.) (float (-0.1) 1.)
   (-1.) (float (-0.9) 1.)
   (-1.) (float (-1.0) 1.)
   (-2.) (float (-1.1) 1.)
  (-24.) (float (-23.3) 12.)
  *)

  (* Same as above but for one of the unsigned integer type. We must be
   * given the div and mut functions: *)
  let uint div mul n s = mul (div n s) s

  (* Same as above but for one of the signed integer types: *)
  let int sub compare zero div mul n s =
    let r = mul (div n s) s in
    if compare n zero >= 0 then r else sub r s

  (*$inject
    module type INT = sig
      type t
      val of_int : int -> t
      val to_int : t -> int
      val bits : int
      val zero : t
      val div : t -> t -> t
      val mul : t -> t -> t
      val sub : t -> t -> t
      val compare : t -> t -> int
    end
    let unsigned_mods =
      let open Stdint in
      [ (module Uint8 : INT) ; (module Uint16 : INT) ;
        (module Uint32 : INT) ; (module Uint64 : INT) ;
        (module Uint128 : INT) ]
    let signed_mods =
      let open Stdint in
      [ (module Int8 : INT) ; (module Int16 : INT) ;
        (module Int32 : INT) ; (module Int64 : INT) ;
        (module Int128 : INT) ]
    let test_trunc signed expected n s m =
      let module M = (val m : INT) in
      let t = if signed then int M.sub M.compare M.zero else uint in
      let r = M.to_int (t M.div M.mul (M.of_int n) (M.of_int s)) in
      let w = M.bits and ui = if signed then 'i' else 'u' in
      let msg = Printf.sprintf "truncate(%d%c%d, %d%c%d)" n ui w s ui w in
      assert_equal ~msg ~printer:string_of_int expected r
  *)
  (*$R
    List.iter (fun (e, n, s) ->
      List.iter (test_trunc false e n s) unsigned_mods)
      [ 0, 0, 10 ; 0, 1, 10 ; 0, 9, 10 ;
        10, 10, 10 ; 10, 19, 10 ; 40, 47, 10 ;
        250, 255, 10 ] ;

    List.iter (fun (e, n, s) ->
      List.iter (test_trunc true e n s) signed_mods)
      [ 0, 0, 10 ; 0, 1, 10 ; 0, 9, 10 ;
        10, 10, 10 ; 10, 19, 10 ; 40, 47, 10 ;
        120, 127, 10 ;
        -10, -1, 10 ; -10, -9, 10 ; -20, -11, 10 ;
        -120, -118, 10 ]
  *)
  (*$>*)
end

(* We often want functions that work on the last k elements, or the last k
 * periods of length p for seasonal data. So we often need a small sliding
 * window as a function internal state. If we could join between two different
 * streams we would have an aggregate running with a sliding window in one hand
 * and then join it with the non-aggregated values, but that would be complex
 * to setup and prone to error. The only advantage would be that we would have
 * just one sliding window for computing all seasonal stats, instead of having
 * individual internal states for each computation. This could be later
 * optimized, though, by sharing the sliding windows of same n and p. *)
module Seasonal =
struct
  (* All that is needed, provided the finalizers supply n and p, is an array
   * of past values and a tuple counter: *)
  type 'a t = 'a array * int

  (* We initialize this internal state with a random value [x]: *)
  let init p n x =
    let p = Uint32.to_int p and n = Uint32.to_int n in
    Array.make (n * p + 1) x, 0

  (* Then adding a value: *)
  let add (prevs, count) x =
    prevs.(count mod Array.length prevs) <- x ;
    prevs, count + 1

  (* Now various operations differ only by their finalizers,
   * most of them will want to iterate over the last season: *)
  let fold p n (prevs, count) v0 f =
    (* ex: n=3, p=4 (quarterly), array of size n*p+1 = 13, after 42 entries
       (counted from 0 to 41, last was 41 and next one will be 42):

       count:            last--v   v--count=42
       array index:    0   1   2   3   4   5   6   7   8   9  10  11  12
       value time:    39  40  41  29  30  31  32  33  34  35  36  37  38
       wanted values:              0               1               2

       Start from the oldest entry (which is at index count) then skip p values,
       etc, and stop when we reach count-1 (excluded). Since the order does not
       matter for an average we could proceed differently and avoid the modulo
       but this approach is simpler. *)
    let rec loop v c idx =
      if c >= n then v else
      loop (f v prevs.(idx mod Array.length prevs)) (c+1) (idx+p)
    in
    loop v0 0 count

  let foldi p n t v0 f =
    fold p n t (0, v0) (fun (i, v) x -> i+1, f v i x) |> snd

  let iter p n t f =
    fold p n t () (fun () x -> f x)

  let iteri p n t f =
    foldi p n t () (fun () i x -> f i x)

  let current (prevs, count) =
    prevs.((count - 1) mod Array.length prevs)

  let lag (prevs, count) =
    let idx =
      if count < Array.length prevs then 0
      else count mod Array.length prevs in
    prevs.(idx)

  let avg t p n =
    let p = Uint32.to_int p and n = Uint32.to_int n in
    (fold p n t 0. (+.)) /. float_of_int n

  (* For multi variable linear regression we store in the array a pair with
   * the predicted value and an array of all predictors value. *)
  let init_multi_linreg p n x preds = init p n (x, preds)
  let add_multi_linreg t x preds = add t (x, preds)
  let multi_linreg t p n =
    let p = Uint32.to_int p and n = Uint32.to_int n in
    let open Lacaml.D in
    (* We first want to know how many observations and predictors we have: *)
    let num_preds, num_obs =
      fold p n t (-1, 0) (fun (nbp, nbo) (_y, xs) ->
        let nbp' = Array.length xs in
        assert (nbp = -1 || nbp = nbp') ;
        nbp', nbo+1) in
    (* Build the x and y matrices *)
    let xm = Mat.create num_obs num_preds
    and ym = Mat.create_mvec num_obs in (* 1 column of num_obs rows *)
    iteri p n t (fun i (y, xs) ->
      (* Fortran flavors. Indices start at 1 and first index is row: *)
      ym.{i+1, 1} <- y ;
      for j = 0 to num_preds-1 do
        xm.{i+1, j+1} <- xs.(j)
      done) ;
    (* Now ask for the "best" parameters: *)
    match gels xm ym with
    | exception _ ->
      let print_mat oc mat =
        let arr = Mat.to_array mat in
        Array.print (Array.print Float.print) oc arr in
      !logger.error "Cannot multi-fit! xm=%a, ym=%a"
        print_mat xm print_mat ym ;
      0.
    | () -> (* Results are in ym *)
      (* And use that to predict the new y given the new xs *)
      let _cury, cur_preds = current t in
      Array.fold_lefti (fun y i x ->
        y +. ym.{i+1, 1} *. x) 0. cur_preds
end

module Shift = struct
  module type IS = sig
    type t
    val shift_left : t -> int -> t
    val shift_right : t -> int -> t
  end

  module Make (I : IS) = struct
    let shift x s =
      let s = Int16.to_int s in
      if s > 0 then I.shift_left x s
      else I.shift_right x (~-s)
  end

  module Uint8 = Make (Uint8)
  module Uint16 = Make (Uint16)
  module Uint32 = Make (Uint32)
  module Uint64 = Make (Uint64)
  module Uint128 = Make (Uint128)
  module Int8 = Make (Int8)
  module Int16 = Make (Int16)
  module Int32 = Make (Int32)
  module Int64 = Make (Int64)
  module Int128 = Make (Int128)
end

module Count = struct
  let count_true s p =
    if p then Uint32.succ s else s

  let count_anything = Uint32.succ
end

(* Linear regressions *)
module LinReg = struct
  (* These stateless functions perform linear regressions given only,
   * as the only parameter [es], an array of observations which are
   * nullable arrays of floats, the first of which is the fitted value.
   * The last array is the current observation and is not used for the
   * regression. The first value of that last observations will not be
   * looked at at all. The returned value is the estimation for that
   * value, or Null. *)

  (* When the matrix of observations contain only one value we assume
   * observations are evenly placed and perform a simple linear regression
   * with this simpler and faster (for small vectors) function is used: *)
  (* Returns a float or raise ImNull: *)
  let one_dimension es =
    (* Exclude the last observation: *)
    let num_obs = Array.length es - 1 in
    if num_obs < 1 then raise ImNull ;
    let x_avg =
      float_of_int (num_obs - 1) /. 2. in
    let b1n, b1d, num_not_null, last =
      Array.fold_lefti (fun (b1n, b1d, nnn, _ as prev) i obs ->
        if i >= num_obs then
          (* Ignore the last observation *)
          prev
        else
          match obs with
          | NotNull obs ->
              let x = float_of_int i in
              let xd = x -. x_avg in
              b1n +. obs.(0) *. xd,
              b1d +. sq xd,
              nnn + 1,
              obs.(0)
          | Null ->
              prev
      ) (0., 0., 0, 0. (* wtv *)) es in
    match num_not_null with
    | 0 -> raise ImNull
    | 1 -> last
    | _ ->
        last +. b1n /. b1d

  let fit es =
    let fit_exn es =
      let num_obs, first_non_null =
        Array.fold_lefti (fun (num, first_non_null as prev) i -> function
          | NotNull _ ->
              num + 1,
              if first_non_null = None then Some i else first_non_null
          | Null ->
              prev
        ) (0, None) es in
      if first_non_null = None then raise ImNull ;
      let first_non_null = Option.get first_non_null in
      (* Get the origin of all observed value as the first observation: *)
      let origin = nullable_get es.(first_non_null) in
      (* And remove this observation from the set: *)
      let num_obs = num_obs - 1 in
      (* If the first non null is also the last one, we are done: *)
      if first_non_null = Array.length es - 1 then origin.(0) else
      (* First value in es.(x) is the fitted value, others are the predictors *)
      let num_preds = Array.length origin - 1 in
      if num_preds < 0 then raise ImNull else
      if num_preds = 0 then one_dimension es else
  (*    if num_preds = 1 then (
      ) else (* More than 1 predictor, do the real thing: *) *)
      (* TODO: if num_preds = 1, simpler linear regression formula *)
      let iter_obs f =
        let rec loop ai oi =
          (* Last observation is not used for the regression: *)
          if ai < Array.length es - 1 then
            match es.(ai) with
            | Null ->
                loop (ai + 1) oi
            | NotNull x ->
                f oi x ;
                loop (ai + 1) (oi + 1) in
        (* Skip the origin by start right after first_non_null: *)
        loop (first_non_null + 1) 0 in
      match es.(Array.length es - 1) with
      | Null ->
          raise ImNull
      | NotNull last_obs ->
          (* The last observation is not used for regression: *)
          let num_obs = num_obs - 1 in
          (* Because we already returned if the first non null was the last
           * observation, and the last observation is non null: *)
          assert (num_obs >= 0) ;
          let check_obs obs =
            if Array.length obs <> num_preds + 1 then (
              !logger.error "fit: an observation has %d predictors instead of %d"
                (Array.length obs - 1) num_preds ;
              raise ImNull) in
          check_obs last_obs ;
          let open Lacaml.D in
          let xm = Mat.create num_obs num_preds
          and ym = Mat.create_mvec num_obs in (* 1 column of num_obs rows *)
          !logger.debug "fit: mat dim = %dx%d" num_obs num_preds ;
          iter_obs (fun i obs ->
            check_obs obs ;
            (* Fortran flavors. Indices start at 1 and first index is row: *)
            ym.{i+1, 1} <- obs.(0) -. origin.(0) ;
            !logger.debug "fit: ym.{%d, 1} = %g - %g" (i+1) obs.(0) origin.(0) ;
            for j = 1 to num_preds do
              xm.{i+1, j} <- obs.(j) -. origin.(j) ;
              !logger.debug "fit: xm.{%d, %d} = %g - %g" (i+1) j obs.(j) origin.(j)
            done) ;
          (* Now ask for the "best" parameters: *)
          match gels xm ym with
          | exception _ ->
              let print_mat oc mat =
                let arr = Mat.to_array mat in
                Array.print (Array.print Float.print) oc arr in
              !logger.error "Cannot multi-fit! xm=%a, ym=%a"
                print_mat xm print_mat ym ;
              raise ImNull
          | () ->
              (* Results are in the first num_preds values of ym.
               * Use that to predict the new y given the new xs *)
              let rec loop i y =
                if i >= num_preds then
                  y
                else
                  let predictor = last_obs.(i+1) -. origin.(i+1) in
                  let y' = y +. ym.{i+1, 1} *. predictor in
                  !logger.debug "y = %f + %f * %f" y ym.{i+1, 1} predictor ;
                  loop (i + 1) y' in
              loop 0 origin.(0)
    in
    try NotNull (fit_exn es) with ImNull -> Null
end

let begin_of_range_cidr4 (n, l) = RamenIpv4.Cidr.and_to_len l n
let end_of_range_cidr4 (n, l) = RamenIpv4.Cidr.or_to_len l n
let begin_of_range_cidr6 (n, l) = RamenIpv6.Cidr.and_to_len l n
let end_of_range_cidr6 (n, l) = RamenIpv6.Cidr.or_to_len l n

module OneOutOf =
struct
  type state =
    { count : int ; period : int }

  let init period =
    let period = Uint32.to_int period in
    { count = -1 ; period }

  let add state =
    { state with count = (state.count + 1) mod state.period }

  (* Is is forced nullable: *)
  let finalize state x =
    if state.count = 0 then x else Null
end

module OnceEvery =
struct
  type state =
    { last : float ; duration : float ; must_emit : bool }

  let init duration =
    { last = 0. ; duration ; must_emit = false (* wtv. *) }

  let add state now =
    if now >= state.last +. state.duration then
      { state with last = now ; must_emit = true }
    else
      { state with must_emit = false }

  (* Is is forced nullable: *)
  let finalize state x =
    if state.must_emit then x else Null
end

module IntOfArray =
struct
  (* Helps with converting arrays of integers into larger integers, in little
   * endian *)
  (* - [arr] is the input array of unsigned integers,
   * - [lshift] the left-shift * operator,
   * - [width] the width in bits of the integers in the array,
   * - [res_width] the width of the integer result,
   * - [zero] is the initial value of the reduction,
   * - [enlarge] converts from the integer type of the array elements into
   *   that of the result.
   * Reading the array stops when its end is reached or at least res_width
   * bits have been read. *)
  let little logor lshift width res_width zero enlarge arr =
    let rec loop accum i read_width =
      if i >= Array.length arr || read_width >= res_width then
        accum
      else
        let accum = logor (lshift accum width) (enlarge arr.(i)) in
        loop accum (i + 1) (read_width + width) in
    loop zero 0 0

  let big logor lshift width res_width zero enlarge arr =
    let rec loop accum i read_width =
      if i < 0 || read_width >= res_width then
        accum
      else
        let accum = logor (lshift accum width) (enlarge arr.(i)) in
        loop accum (i - 1) (read_width + width) in
    loop zero (Array.length arr - 1) 0
end

module Globals =
struct
  type map =
    unit ->
      (?txn:[ `Read ] Lmdb.Txn.t -> string -> string) *
      (?txn:[ `Write ] Lmdb.Txn.t -> string -> string -> unit)

  let map_set (map : map) k v =
    let _, set = map () in
    set k v ;
    v

  let map_get (map : map) k =
    let get, _ = map () in
    try NotNull (get k) with Not_found -> Null
end
