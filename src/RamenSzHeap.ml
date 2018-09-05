(* Same as RamenHeap, with an added entry count to answer `cardinal`
 * in O(1). *)
open RamenHeap

type 'a t = 'a RamenHeap.t * int

let empty = empty, 0

let is_empty (h, c) =
  assert (is_empty h = (c = 0)) ;
  is_empty h

let singleton x = singleton x, 1

(* RamenHeap does not collapse duplicate entries: *)
let merge cmp (h1, c1) (h2, c2) = merge cmp h1 h2, c1 + c2

let add cmp x (h, c) = add cmp x h, c + 1

let min (h, _) = min h

let min_opt (h, _) = min_opt h

let del_min cmp (h, c) = del_min cmp h, c - 1

let pop_min cmp t = min t, del_min cmp t

let fold_left cmp f init (h, c) = fold_left cmp f init h

let cardinal (_, c) = c
