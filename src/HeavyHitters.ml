(* Simple implementation of a polymorphic set that keeps only the most
 * important entries using the "heavy hitters" selection technique.
 * This technique is an approximation. For an item to be guaranteed to be
 * featured in the top N, its total contribution must be >= 1/N of the total.
 * Other than that, it depends on the actual sequence.
 * To avoid noise, every item which guaranteed minimal weight is below that of
 * the Nth item of the top will be discarded.
 *
 * It is therefore recommended to track more than N items. By default, this
 * tracks 10 times more items than N. So for instance, to obtain the top 10
 * contributors this would actually track the top 100, and build reliably the
 * list of all items which contribution is larger than 1/100th of the total,
 * then returning the top 10. *)
open Batteries
open RamenLog
open RamenHelpersNoLog

(*$inject
  open Batteries
  open TestHelpers
*)

let debug = false

(* Weight map: from weight to anything, ordered bigger weights first: *)
module WMap = Map.Make (struct
  type t = float ref (* ref so we can downscale *)
  let compare w1 w2 = Float.compare !w2 !w1
end)

type 'a t =
  { max_size : int ;
    mutable cur_size : int ;
    (* Fade off contributors by decaying weights in time (actually, inflating
     * new weights as time passes) *)
    decay : float ; (* decay factor (0 for no decay) *)
    mutable time_origin : float option ;
    (* value to weight and overestimation: *)
    mutable w_of_x : ('a, float * float) Map.t ;
    (* max weight to value to overestimation. Since we need iteration to go
     * from bigger to smaller weight we need a custom map: *)
    mutable xs_of_w : (('a, float) Map.t) WMap.t }

let make ~max_size ~decay =
  { max_size ; decay ; time_origin = None ; cur_size = 0 ;
    w_of_x = Map.empty ; xs_of_w = WMap.empty }

(* Downscale all stored weight by [d] and reset time_origin.
 * That's OK to modify the map keys because relative ordering is not going
 * to change: *)
(* TODO: stats about rescale frequency *)
let downscale s t d =
  !logger.debug "HeavyHitters: downscaling %d entries by %g"
    s.cur_size d ;
  s.w_of_x <-
    Map.map (fun (w, o) -> w *. d, o *. d) s.w_of_x ;
  WMap.iter (fun w _xs -> w := !w *. d) s.xs_of_w ;
  s.time_origin <- Some t

let add s t w x =
  (* Decaying old weights is the same as inflating new weights.
   * But then after a while new inflated weights will become too big to
   * be accurately tracked, so when this happen we _rescale_ the top,
   * that is we actually decay the history and reset the origin of time
   * so that new entries will only be inflated by exp(0), and so on. *)
  (* Shall we downscale? *)
  let inflation =
    match s.time_origin with
    | None -> s.time_origin <- Some t ; 1.
    | Some t0 ->
        let infl = exp ((t -. t0) *. s.decay) in
        (* Make this a parameter for trading off CPU vs accuracy? *)
        let max_infl = 1e6 in
        if infl < max_infl then infl else (
          downscale s t infl ;
          1.
        ) in
  let w = w *. inflation in
  let add_in_xs_of_w x w o m =
    if debug then Printf.printf "TOP: add entry %s of weight %f\n" (dump x) w ;
    WMap.modify_opt (ref w) (function
      | None -> Some (Map.singleton x o)
      | Some xs ->
          assert (not (Map.mem x xs)) ;
          Some (Map.add x o xs)
    ) m
  and rem_from_xs_of_w x w m =
    WMap.modify_opt (ref w) (function
      | None -> assert false
      | Some xs ->
          (match Map.extract x xs with
          | exception Not_found ->
              !logger.error "xs_of_w for w=%f does not have x=%s (only %a)"
                w (dump x)
                (Map.print print_dump Float.print) xs ;
              assert false
          | _, xs ->
              if Map.is_empty xs then None else Some xs)
    ) m
  in
  (* Shortcut for the frequent case when w=0: *)
  if w <> 0. then (
    let victim_x = ref None in
    s.w_of_x <-
      Map.modify_opt x (function
        | None ->
            let victim_w = ref 0. in
            if s.cur_size >= s.max_size then (
              (* pick the victim and remove it from xs_of_w: *)
              let victim_w', xs = WMap.max_binding s.xs_of_w in
              let (victim_x', _victim_o), xs' = Map.pop xs in
              victim_w := !victim_w' ;
              victim_x := Some victim_x' ;
              s.xs_of_w <-
                if Map.is_empty xs' then
                  WMap.remove victim_w' s.xs_of_w
                else
                  WMap.update victim_w' victim_w' xs' s.xs_of_w
            ) else s.cur_size <- s.cur_size + 1 ;
            let w = w +. !victim_w in
            s.xs_of_w <- add_in_xs_of_w x w !victim_w s.xs_of_w ;
            Some (w, !victim_w)
        | Some (w', o) ->
            let w = w +. w' in
            s.xs_of_w <-
              rem_from_xs_of_w x w' s.xs_of_w |>
              add_in_xs_of_w x w o ;
            Some (w, o)
      ) s.w_of_x ;
    Option.may (fun x ->
      match Map.extract x s.w_of_x with
      | exception Not_found ->
          !logger.error "w_of_x does not have x=%s, only %a"
            (dump x)
            (Enum.print print_dump) (Map.keys s.w_of_x) ;
          assert false
      | _, w_of_x ->
          s.w_of_x <- w_of_x
    ) !victim_x ;
    assert (s.cur_size <= s.max_size) (*;
    assert (Map.cardinal s.w_of_x = s.cur_size) ;
    assert (WMap.cardinal s.xs_of_w <= s.cur_size)*)
  ) (* w <> 0. *)

(* For each monitored item of rank k <= n, we must ask ourselves: could there
 * be an item with rank k > n, or a non-monitored items, with more weight?  For
 * this we must compare guaranteed weight of items with the max weight of item
 * of rank n+1; but we don't know which item that is unless we order them. *)
(* FIXME: super slow, maintain the entries in increased max weight order. *)

(* Iter the entries in decreasing weight order.
 * Note: BatMap iterates in increasing keys order despite de doc says it's
 * unspecified, but since we reverse the comparison operator we fold from
 * heaviest to lightest. *)
let fold u f s =
  WMap.fold (fun w xs u ->
    if debug then Printf.printf "TOP: folding over all entries of weight %f\n" !w ;
    Map.foldi (fun x o u ->
      if debug then Printf.printf "TOP:   ... %s\n" (dump x) ;
      f !w x o u
    ) xs u
  ) s.xs_of_w u

(* Iter over the top [n'] entries (<= [n] but close) in order of weight,
 * lightest first (so that it's easy to build the reverse list): *)
let fold_top n u f s =
  let res = ref []
  and cutoff = ref None in
  (try
    let _ =
      fold 1 (fun w x o rank ->
        (* We need item at rank n+1 to find top-n *)
        if rank <= n then (
          if debug then
            Printf.printf "TOP rank=%d<=%d is %s, weight %f\n" rank n (dump x) w ;
          (* May be filtered once we know the cutoff: *)
          res := (w, (w -. o), x) :: !res ; (* res is lightest to heaviest *)
          rank + 1
        ) else (
          assert (rank = n + 1) ;
          if debug then
            Printf.printf "TOP rank=%d>%d is %s, weight %f\n" rank n (dump x) w ;
          cutoff := Some w ;
          raise Exit
        )
      ) s in
    (* We reach here when we had less entries than n, in which case we do not
     * need a cut-off since we know all the entries: *)
    if debug then
      Printf.printf "TOP: Couldn't reach rank %d, cur_size=%d\n" n s.cur_size ;
  with Exit -> ()) ;
  (* Now filter the entries if we have a cutoff, and build the result: *)
  List.fold_left (fun u (_w, min_w, x) ->
    match !cutoff with
    | None -> f u x
    | Some c -> if min_w >= c then f u x else u
  ) u !res

(* Returns the top as a list ordered by weight (heavier first) *)
let top n s =
  fold_top n [] (fun lst x -> x :: lst) s

(* Tells the rank of a given value in the top, or None: *)
let rank n x s =
  if n < 1 then invalid_arg "rank" ;
  let res = ref None in
  (try
    fold_top n 1 (fun k x' ->
      if x = x' then (
        res := Some k ;
        raise Exit
      ) else k + 1
    ) s |> ignore
  with Exit -> ()) ;
  !res

(* Tells if x is in the top [n]: *)
let is_in_top n x s =
  rank n x s <> None

(*$R is_in_top
  (* Check that what we add into an empty top is in the top: *)
  let top_size = 100 in
  let s = make ~max_size:(top_size * 10) ~decay:8.3e-5 in
  add s 1. 1. 42 ;
  assert_bool "42 is in top" (is_in_top top_size 42 s)
*)

(*$R add
  let (++) = Enum.append in
  let xs = Enum.(
    repeat ~times:10 42 ++
    repeat ~times:10 43 ++
    repeat ~times:10 44 ++
    take 70 (Random.enum_int 999999)) |>
    Array.of_enum in
  Array.shuffle xs ;
  let s = make ~max_size:30 ~decay:0. in
  let now = Unix.time () in
  Array.iteri (fun i x ->
    let t = now +. float_of_int i in
    add s t 1. x
  ) xs ;
  let s = top 3 s in
  (*Printf.printf "Solution: %a\n%!" (List.print Int.print) s ;*)
  assert_bool "Result size is limited" (List.length s <= 10) ;
  assert_bool "42 is present" (List.mem 42 s) ;
  assert_bool "43 is present" (List.mem 43 s) ;
  assert_bool "44 is present" (List.mem 44 s)
*)

(*$R add
  let k = 5 in
  let retry = 10 in

  let test_once k =
    let max_size = k * 10 and zc = 1. in
    let s = make ~max_size ~decay:0. in
    zipf_distrib 1000 zc |> Enum.take 10_000 |> Enum.iter (add s 0. 1.) ;
    let s = top k s in
    assert_bool "Result size is limited" (List.length s <= k) ;
    (* All items from 0 to k-1 (included) are present most of the time: *)
    let missing = ref 0 in
    for i = 0 to k-1 do
      if not (List.mem i s) then incr missing
    done ;
    if !missing > 0 then
      Printf.printf "Solution: %a\n%!" (List.print Int.print) s ;
    assert_bool "no more than 2 items are missing" (!missing <= 2) ;
    !missing = 0 in
  let num_find_all = ref 0 in
  for i = 0 to retry-1 do
    if test_once k then incr num_find_all
  done ;
  let success_rate = float_of_int !num_find_all /. float_of_int retry in
  assert_bool "must be accurate most of the times" (success_rate > 0.6)
*)
