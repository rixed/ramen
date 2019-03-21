(* Simple leftist heap.
 * We need a heap that works on universal types (since we will store
 * tuples in there and don't bother monomorphize either BatHeap or
 * any of Pfds.heaps), but we want to provide a custom comparison
 * function.
 * This does not collapse duplicate entries (= entries for which the
 * comparison function returns 0).
 * Smallest elements are nearest to the root.
 * When two elements compare equal the relative order is not specified. *)
open Batteries

type 'a t =
  | E
  | T of {
      mutable deleted : bool ;
      value : 'a ;
      rank : int ;
      left : 'a t ;
      right : 'a t }

let empty = E

let rec is_empty = function
  | E ->
      true
  | T { deleted = false ; _ } ->
      false
  | T { deleted = true ; left ; right ; _ } ->
      is_empty left && is_empty right

let singleton value =
  T { deleted = false ; value ; rank = 1 ; left = E ; right = E }

let rec print p oc = function
  | E -> String.print oc "∅"
  | T t ->
      Printf.fprintf oc "[ %a <- %a%s -> %a ]"
        (print p) t.left
        p t.value
        (if t.deleted then "(DELETED)" else "")
        (print p) t.right

let rank = function
  | E -> 0
  | T t -> t.rank

let makeT value left right =
  let rank_l, rank_r = rank left, rank right in
  if rank_l >= rank_r then
    T { deleted = false ; value ; rank = rank_r + 1 ; left ; right }
  else
    T { deleted = false ; value ; rank = rank_l + 1 ; left = right ; right = left }

let rec min cmp = function
  | E ->
      raise Not_found
  | T { deleted = false ; value ; _ } ->
      value
  | T { deleted = true ; left ; right ; _ } ->
      (match min cmp left with
      | exception Not_found -> min cmp right
      | l ->
          (match min cmp right with
          | exception Not_found -> l
          | r ->
              if cmp l r <= 0 then l else r))

let min_opt cmp t =
  try Some (min cmp t)
  with Not_found -> None

let rec pop_deleted cmp a =
  match a with
  | E -> a
  | T t ->
      if t.deleted then merge cmp t.left t.right
      else a

and merge cmp a b =
  let a = pop_deleted cmp a
  and b = pop_deleted cmp b in
  match a with
  | E -> b
  | T ta -> (match b with
      | E -> a
      | T tb ->
          (* Note: It is forbidden to call cmp on a deleted item: *)
          if cmp ta.value tb.value < 0 then
            makeT ta.value ta.left (merge cmp ta.right b)
          else
            makeT tb.value tb.left (merge cmp tb.right a))

let add cmp x a =
  merge cmp a (singleton x)

let del_min cmp = function
  | E ->
      raise Not_found
  | a ->
      (match pop_deleted cmp a with
      | E -> raise Not_found
      | T t ->
          merge cmp t.left t.right)

let pop_min cmp t =
  min cmp t, del_min cmp t

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
  | T t ->
      (* Note: it is forbidden to call cmp on a deleted value: *)
      (* FIXME: no need for the cmp check: *)
      if not t.deleted && x = t.value && cmp x t.value = 0 then
        merge cmp t.left t.right
      else
        makeT t.value (rem cmp x t.left) (rem cmp x t.right)

(* Same as above, destructively, using physical equality to select the
 * item to be removed. Returns true if the value was indeed removed: *)
let rec rem_phys cmp x = function
  | E ->
      false
  | T t ->
      (* If the entry we want to delete is smaller than the value, give up: *)
      if t.deleted then
        rem_phys cmp x t.left ||
        rem_phys cmp x t.right
      else
        if x == t.value then (
          t.deleted <- true ;
          true
        ) else if cmp x t.value < 0 then
          false
        else
          rem_phys cmp x t.left ||
          rem_phys cmp x t.right

(* Returns the number of items in that heap (total and non-deleted).
 * Slow and non recursive, for debugging only: *)
let length t =
  let rec loop tot nd = function
    | E -> tot, nd
    | T t ->
        let tot = tot + 1
        and nd = if t.deleted then nd else nd + 1 in
        let tot, nd = loop tot nd t.left in
        loop tot nd t.right in
  loop 0 0 t
