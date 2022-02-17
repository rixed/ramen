(* FIXME: This has not moved to dessser *)
(* Simple leftist heap.
 * We need a heap that works on universal types (since we will store
 * tuples in there and don't bother monomorphize either BatHeap or
 * any of Pfds.heaps), but we want to provide a custom comparison
 * function.
 * This does not collapse duplicate entries (= entries for which the
 * comparison function returns 0), unlike BatMap.PMap.
 * Smallest elements are nearest to the root.
 * When two elements compare equal the relative order is unspecified. *)
open Batteries

type 'a t =
  | E
  | T of {
      value : 'a ;
      rank : int ;
      left : 'a t ;
      right : 'a t }

let empty = E

let rec is_empty = function
  | E -> true
  | T _ -> false

let singleton value =
  T { value ; rank = 1 ; left = E ; right = E }

let rec print p oc = function
  | E -> String.print oc "∅"
  | T t ->
      Printf.fprintf oc "[ %a <- %a -> %a ]"
        (print p) t.left
        p t.value
        (print p) t.right

let rank = function
  | E -> 0
  | T t -> t.rank

(* Build a tree once we already know the min value and two subtrees: *)
let makeT value left right =
  let rank_l, rank_r = rank left, rank right in
  if rank_l >= rank_r then
    T { value ; rank = rank_r + 1 ; left ; right }
  else
    T { value ; rank = rank_l + 1 ; left = right ; right = left }

let min = function
  | E -> raise Not_found
  | T t -> t.value

let min_opt t =
  try Some (min t)
  with Not_found -> None

let rec merge cmp a b =
  match a with
  | E -> b
  | T ta -> (match b with
      | E -> a
      | T tb ->
          if cmp ta.value tb.value < 0 then
            makeT ta.value ta.left (merge cmp ta.right b)
          else
            makeT tb.value tb.left (merge cmp tb.right a))

let add cmp x a =
  merge cmp a (singleton x)

let del_min cmp = function
  | E ->
      raise Not_found
  | T t ->
      merge cmp t.left t.right

let pop_min cmp t =
  min t, del_min cmp t

(* Iterate over items, smallest to greatest: *)
let rec fold_left cmp f init = function
  | E ->
      init
  | t ->
      let mi, t' = pop_min cmp t in
      let init' = f init mi in
      fold_left cmp f init' t'

(* Note: cmp is used to navigate in the heap, while equality is used to spot
 * which item to delete. *)
let rec rem cmp x = function
  | E -> E
  | T t (* as a *) ->
      (* FIXME: no need for the cmp check: *)
      if x = t.value && cmp x t.value = 0 then
        merge cmp t.left t.right
      (* FIXME: give up when we reached a value biger than the one we are looking for *)
      (* else if cmp x t.value < 0 then a *)
      else
        makeT t.value (rem cmp x t.left) (rem cmp x t.right)

(* Same as above, destructively, using physical equality to select the
 * item to be removed. Returns true if the value was indeed removed: *)
let rec rem_phys cmp x = function
  | E ->
      E
  | T t as a ->
      (* If the entry we want to delete is smaller than the value, give up: *)
      if x == t.value then (
        merge cmp t.left t.right
      ) else if cmp x t.value < 0 then
        a
      else
        makeT t.value (rem_phys cmp x t.left) (rem_phys cmp x t.right)

(* Returns the number of items in that heap.
 * Slow and non recursive, for debugging only: *)
let length t =
  let rec loop tot = function
    | E -> tot
    | T t ->
        let tot = loop (tot + 1) t.left in
        loop tot t.right in
  loop 0 t

(* A function that goes through the heap from top to bottom, offering the
 * caller to either collect, keep or stop at each item. It returns the
 * list of collected items as well as the resulting heap.
 * As a special treatment for groups that must be kept in the heap but still
 * output, [PickAndKeep] instruct the heap to stays the same (as in Keep)
 * yet to return the group with collected ones. *)
type collect_action = SelectAndRemove | SelectAndKeep | Ignore | IgnoreAll
let collect cmp f h =
  let rec loop collected h =
    match h with
    | E -> collected, h
    | T t ->
        let res = f t.value in
        if res = IgnoreAll then
            collected, h
        else
          let collected, a = loop collected t.left in
          let collected, b = loop collected t.right in
          if res = Ignore then
            collected, makeT t.value a b
          else if res = SelectAndKeep then
            (t.value :: collected), makeT t.value a b
          else (
            assert (res = SelectAndRemove) ;
            (t.value :: collected), merge cmp a b
          ) in
  loop [] h
