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
  E | T of { value : 'a ; rank : int ; left : 'a t ; right : 'a t }

let empty = E

let is_empty = function
  | E -> true
  | _ -> false

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

let makeT value left right =
  let rank_l, rank_r = rank left, rank right in
  if rank_l >= rank_r then
    T { value ; rank = rank_l + 1 ; left ; right }
  else
    T { value ; rank = rank_r + 1 ; left = right ; right = left }

let rec merge cmp a b = match a with
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

let min = function
  | E -> raise Not_found
  | T t -> t.value

let min_opt t =
  try Some (min t)
  with Not_found -> None

let del_min cmp = function
  | E -> raise Not_found
  | T t -> merge cmp t.left t.right

let pop_min cmp t =
  min t, del_min cmp t

(* Iterate over items, smallest to greatest: *)
let rec fold_left cmp f init = function
  | E -> init
  | t ->
      let init' = f init (min t) in
      fold_left cmp f init' (del_min cmp t)

let rec rem cmp x = function
  | E -> E
  | T t ->
      if x = t.value && cmp x t.value = 0 then
        merge cmp t.left t.right
      else
        makeT t.value (rem cmp x t.left) (rem cmp x t.right)

(* Same as above, but use physical equality to locate the item to remove: *)
let rec rem_phys cmp x = function
  | E -> E
  | T t ->
      if x == t.value then
        merge cmp t.left t.right
      else
        makeT t.value (rem_phys cmp x t.left) (rem_phys cmp x t.right)

(* Returns the number of items in that heap. Slow and non recursive, for
 * debugging only: *)
let length t =
  let rec loop s = function
    | E -> s
    | T t ->
        loop (loop (1 + s) t.left) t.right in
  loop 0 t
