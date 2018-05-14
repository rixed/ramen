(* Simple implementation of a polymorphic set that keeps only the most
 * important entries. *)
open Batteries

(* Weight map: from weight to anything, ordered bigger weights first: *)
module WMap = Map.Make (struct
  type t = float
  let compare w1 w2 = Float.compare w2 w1
end)

type 'a t =
  { max_size : int ;
    mutable cur_size : int ;
    decay : float ;
    (* value to weight and overestimation: *)
    mutable w_of_x : ('a, float * float) Map.t ;
    (* max weight to value to overestimation. Since we need iteration to go
     * from bigger to smaller weight we need a custom map: *)
    mutable xs_of_w : (('a, float) Map.t) WMap.t }

let make ~max_size ~decay =
  { max_size ; decay ; cur_size = 0 ;
    w_of_x = Map.empty ; xs_of_w = WMap.empty }

let add s t w x =
  let add_in_xs_of_w x w o m =
    WMap.modify_opt w (function
      | None -> Some (Map.singleton x o)
      | Some xs ->
          assert (not (Map.mem x xs)) ;
          Some (Map.add x o xs)
    ) m
  and rem_from_xs_of_w x w m =
    WMap.modify_opt w (function
      | None -> assert false
      | Some xs ->
          assert (Map.mem x xs) ;
          let xs = Map.remove x xs in
          if Map.is_empty xs then None else Some xs
    ) m
  in
  let victim_x = ref None in
  s.w_of_x <-
    Map.modify_opt x (function
      | None ->
          let victim_w = ref 0. in
          if s.cur_size >= s.max_size then (
            (* pick the victim and remove it from xs_of_w: *)
            let victim_w', xs = WMap.max_binding s.xs_of_w in
            let (victim_x', _victim_o), xs' = Map.pop xs in
            victim_w := victim_w' ;
            victim_x := Some victim_x' ;
            s.xs_of_w <-
              if Map.is_empty xs' then
                WMap.remove victim_w' s.xs_of_w
              else
                WMap.update victim_w' victim_w' xs' s.xs_of_w ;
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
    assert (Map.mem x s.w_of_x) ;
    s.w_of_x <- Map.remove x s.w_of_x
  ) !victim_x ;
  assert (s.cur_size <= s.max_size) ;
  assert (Map.cardinal s.w_of_x = s.cur_size) ;
  assert (WMap.cardinal s.xs_of_w <= s.cur_size)

(* For each monitored item of rank k <= n, we must ask ourselves: could there
 * be an item with rank k > n, or a non-monitored items, with more weight?  For
 * this we must compare guaranteed weight of items with the max weight of item
 * of rank n+1; but we don't know which item that is unless we order them. *)
(* FIXME: super slow, maintain the entries in increased max weight order. *)

(* Iter the entries in max weight order.
 * Note: BatMap iterates in increasing keys order despite de doc says it's
 * unspecified. *)
let fold u f s =
  WMap.fold (fun w xs u ->
    Map.foldi (fun x o u ->
      f w x o u
    ) xs u
  ) s.xs_of_w u

(* Iter over the top [n'] entries (<= [n] but close) in order of weight,
 * lightest first (so that it's easy to build the reverse list): *)
let fold_top n u f s =
  (* We need item at rank n+1 to find top-n *)
  let n = min n (s.cur_size - 1) in
  let res = ref u in
  (try
    let _ =
      fold (1, []) (fun w x o (rank, firsts) ->
        if rank <= n then (
          rank + 1,
          (w, (w -. o), x) :: firsts (* will be filtered later *)
        ) else (
          assert (rank = n + 1) ;
          let cutoff = w in
          (* first is now lightest to heaviest: *)
          res :=
            List.fold_left (fun u (w, min_w, x) ->
              if min_w >= cutoff then f u x else u
            ) u firsts ;
          raise Exit)
      ) s in
    (* We reach here only when we had no entries at all, in which case empty
     * is the right answer *)
    assert (s.cur_size = 0)
  with Exit -> ()) ;
  !res

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

(*$inject
  open Batteries
  open TestHelpers
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
    zipf_distrib 10000 zc |> Enum.take 100_000 |> Enum.iter (add s 0. 1.) ;
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
  let nb_find_all = ref 0 in
  for i = 0 to retry-1 do
    if test_once k then incr nb_find_all
  done ;
  let success_rate = float_of_int !nb_find_all /. float_of_int retry in
  assert_bool "must be accurate most of the times" (success_rate > 0.6)
*)
