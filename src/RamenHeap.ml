(* Simple leftist heap.
 * We need a heap that works on universal types (since we will store
 * tuples in there and don't bother monomorphize either BatHeap or
 * any of Pfds.heaps), but we want to provide a custom comparison
 * function.
 * This does not collapse duplicate entries (= entries for which the
 * comparison function returns 0).
 * Smallest elements are nearest to the root.
 * When two elements compare equal the relative order is not specified. *)
type 'a t = E | T of (* rank *) int * 'a * 'a t * 'a t

let empty = E
let is_empty = function E -> true | _ -> false
let singleton x = T (1, x, E, E)

let rank = function
    | E -> 0
    | T (r, _, _, _) -> r

let makeT v l r =
    let rank_l, rank_r = rank l, rank r in
    if rank_l >= rank_r then T (rank_l + 1, v, l, r)
    else T (rank_r + 1, v, r, l)

let rec merge cmp a b = match a with
    | E -> b
    | T (_, x, a_l, a_r) -> (match b with
        | E -> a
        | T (_, y, b_l, b_r) ->
            if cmp x y < 0 then makeT x a_l (merge cmp a_r b)
            else makeT y b_l (merge cmp a b_r))

let add cmp x a = merge cmp a (singleton x)

let min = function E -> raise Not_found | T (_, x, _, _) -> x

let min_opt t = try Some (min t) with Not_found -> None

let del_min cmp = function
  | E -> raise Not_found
  | T (_, _, l, r) -> merge cmp l r

let pop_min cmp h = min h, del_min cmp h

(* Iterate over items, smallest to greatest: *)
let rec fold_left cmp f init = function
  | E -> init
  | t ->
      let init' = f init (min t) in
      fold_left cmp f init' (del_min cmp t)

let rec rem cmp x = function
  | E -> E
  | T (_, y, l, r) ->
      let c = cmp x y in
      if c < 0 then makeT y (rem cmp x l) r else
      if c = 0 && x = y then merge cmp l r else
      makeT y l (rem cmp x r)
