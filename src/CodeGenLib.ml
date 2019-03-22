(* Functions used by the generated OCaml code to implement various
 * operators. *)
open Batteries
open Stdint
open RamenLog
open RamenHelpers
open RamenNullable
open RamenConsts

(*$inject open Batteries *)

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

let age_float x = !CodeGenLib_IO.now -. x
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

(* State is count * sum *)
let avg_init = 0, 0.
let avg_add (count, sum) x = count + 1, sum +. x
let avg_finalize (count, sum) = sum /. float_of_int count

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
          lo hi (List.print Int.print) ks (Array.print (fun oc v -> String.print oc (dump v))) a ;
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
            RamenHelpers.round_to_int (float_of_int (p * hi) *. 0.01) in
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

let substring s a b =
  let a = Int32.to_int a
  and b = Int32.to_int b in
  let l = String.length s in
  let a, b = min a l, min b l in
  let a = if a < 0 then a + l else a
  and b = if b < 0 then b + l else b in
  if a >= b then "" else
  String.sub s a (b - a)

let smooth prev alpha x = x *. alpha +. prev *. (1. -. alpha)

let split by what k =
  if what = "" then k what else
  String.nsplit ~by what |> List.iter k

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
    let num_slices = 10 in
    let start_time = tim -. st.duration
    and slice_width = st.duration /. float_of_int num_slices in
    let filter =
      RamenBloomFilter.make_sliced start_time num_slices slice_width
                                   st.false_positive_ratio in
    st.filter <- Some filter ;
    filter

  let add st tim es =
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
    HeavyHitters.add s t w x ;
    s

  let rank s n x =
    HeavyHitters.rank (Uint32.to_int n) x s |>
    nullable_of_option

  let is_in_top s n x =
    HeavyHitters.is_in_top (Uint32.to_int n) x s
end

let hash x = Hashtbl.hash x |> Int64.of_int

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

module Last = struct
  type ('a, 'b) state =
    { (* Ordered according to some generic value, smaller first, and we
         will keep only the N bigger values: *)
      values : ('a * 'b) RamenHeap.t ;
      max_length : int (* The number of values we want to return *) ;
      length : int (* how many values are there already *) ;
      count : int (* Count insertions, to use as default order *) }

  let init n =
    let n = Uint32.to_int n in
    { values = RamenHeap.empty ; max_length = n ; length = 0 ; count = 0 }

  let cmp (_, by1) (_, by2) = compare by1 by2

  let add state x by =
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
   * FIXME: faster conversion from heap to array. *)
  let array_of_heap_fst cmp h =
    RamenHeap.fold_left cmp (fun lst (x, _) ->
      x :: lst
    ) [] h |>
    List.rev |>
    Array.of_list

  (* Must return an optional vector of max_length values: *)
  let finalize state =
    if state.length < state.max_length then Null
    else NotNull (array_of_heap_fst cmp state.values)
end

module Past = struct

  type 'a state =
    { (* Ordered according to time, smaller on top: *)
      values : ('a * float) RamenHeap.t ;
      max_age : float ;
      sample : ('a * float) RamenSampling.reservoir option }

  let init max_age sample_size any_value =
    { sample =
        Option.map (fun sz ->
          RamenSampling.init sz (any_value, 0.)
        ) sample_size ;
      values = RamenHeap.empty ; max_age }

  let cmp (_, t1) (_, t2) = Float.compare t1 t2

  let add state x t =
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
    NotNull (Last.array_of_heap_fst cmp state.values)
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

let reldiff a b =
  let d = abs_float (a -. b) and a = max a b in
  if a = d then 0. else if d < a then d /. a else a /. d

module Truncate =
struct
  (*$< Truncate *)
  (* In all the trunc operations we truncate toward minus infinity, à la
   * Ruby or Python, for the very same practical reason: Basically, we want
   * it to work to resample values (such as timestamps) regardless of their
   * origin. *)

  (* Round the float [n] to the given scale [s] (which must be positive): *)
  let float n s = Float.floor (n /. s) *. s

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

  let linreg t p n =
    let p = Uint32.to_int p and n = Uint32.to_int n in
    let b1n, b1d, last =
      let x_avg = float_of_int (n - 1) /. 2. in
      let sq x = x *. x in
      foldi p n t (0., 0., 0.) (fun (b1n, b1d, _) i y ->
        let x = float_of_int i in
        let xd = x -. x_avg in
        b1n +. y *. xd,
        b1d +. sq xd,
        y) in
    let b1 =
      if n > 1 then b1n /. b1d else 0. in
    last +. b1

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
      (* Fortran flavors. Indices start at 0 and first index is row: *)
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

let begin_of_range_cidr4 (n, l) = RamenIpv4.Cidr.and_to_len l n
let end_of_range_cidr4 (n, l) = RamenIpv4.Cidr.or_to_len l n
let begin_of_range_cidr6 (n, l) = RamenIpv6.Cidr.and_to_len l n
let end_of_range_cidr6 (n, l) = RamenIpv6.Cidr.or_to_len l n
