(* A Heap storing also the first, last, and greatest element, used as a
 * FIFO. Functional interface. *)
open Batteries

(* To begin with, we need a Heap of elements of any types, ordered by
 * a custom comparison function: *)
module Heap =
struct
  type 'a t = E | T of (* rank *) int * 'a * 'a t * 'a t

  let empty = E
  let singleton x = T (1, x, E, E)

  let rank = function
      | E -> 0
      | T (r, _, _, _) -> r

  let rec merge cmp a b = match a with
      | E -> b
      | T (_, x, a_l, a_r) -> (match b with
          | E -> a
          | T (_, y, b_l, b_r) ->
              let makeT v l r =
                  let rank_l, rank_r = rank l, rank r in
                  if rank_l >= rank_r then T (rank_l + 1, v, l, r)
                  else T (rank_r + 1, v, r, l) in
              if cmp x y <= 0 then makeT x a_l (merge cmp a_r b)
              else makeT y b_l (merge cmp a b_r))

  let add cmp x a = merge cmp a (singleton x)

  let min = function E -> invalid_arg "min" | T (_, x, _, _) -> x

  let del_min cmp = function
    | E -> invalid_arg "delete_min"
    | T (_, _, l, r) -> merge cmp l r

  (* Iterate over items, smallest to greatest: *)
  let rec fold_left cmp f init = function
    | E -> init
    | t ->
        let init' = f init (min t) in
        fold_left cmp f init' (del_min cmp t)
end

module LL = Dllist

type ('k, 'x) t = ('k, 'x) non_empty option
and ('k, 'x) non_empty =
  (* Both the heap and the linked-list use the same physical 'a: *)
  { heap : ('k * 'x) LL.node_t Heap.t ; (* to get the min *)
    llist : ('k * 'x) LL.t ; (* to track first/last *)
    length : int ;
    greatest : 'x }

let print p oc = function
  | None -> Printf.fprintf oc "[]"
  | Some { llist ; _ } -> LL.print p oc llist

(* Comparison function used with heaps of ('a, 'b) LL.node_t, where 'a is
 * the key. Use generic compare with the keys: *)
let cmp_nodes a b =
  compare (fst (LL.get a)) (fst (LL.get b))

let empty = None

let add k x = function
  | None ->
      let llist = LL.create (k, x) in
      Some {
        heap = Heap.singleton llist ;
        llist ; length = 1 ; greatest = x }
  | Some t ->
      (* Add this node as last position in the llist, ie just
       * before head: : *)
      let x_node = LL.prepend t.llist (k, x) in
      Some {
        heap = Heap.add cmp_nodes x_node t.heap ;
        llist = t.llist ;
        length = t.length + 1 ;
        greatest = max x t.greatest }

let to_list = function
  | None -> []
  | Some sb ->
      Heap.fold_left cmp_nodes (fun lst x_node ->
        snd (LL.get x_node) :: lst
      ) [] sb.heap |> List.rev

(* Note: Can't use = with Dllists so convert sortbufs into lists: *)
(*$= add & ~printer:(BatIO.to_string (BatList.print BatInt.print))
  ([42]) (add 42 42 empty |> to_list)
  ([42; 57]) (add 42 42 (add 57 57 empty) |> to_list)
  ([42; 57]) (add 57 57 (add 42 42 empty) |> to_list)
 *)

let length = function
  | None -> 0
  | Some t -> t.length

(*$= length & ~printer:string_of_int
  2 (length (add 42 42 (add 57 57 empty)))
 *)

let pop_min t =
  match length t with
  | 0 -> invalid_arg "pop_min"
  | 1 -> (Option.get t).greatest, empty
  | _ ->
      let t = Option.get t in
      let x_node = Heap.min t.heap in
      (* We don't want this item to be the one t.llist points to: *)
      let llist =
        if x_node == t.llist then
          LL.next t.llist
        else t.llist in
      (* Unlink x_node wherever it is: *)
      LL.remove x_node ;
      snd (LL.get x_node),
      Some {
        heap = Heap.del_min cmp_nodes t.heap ;
        llist ; length = t.length - 1 ;
        (* Since length was > 1 we know the greatest stays the same: *)
        greatest = t.greatest }

(*$= pop_min & ~printer:(fun (i, l) -> Printf.sprintf "(%d, %s)" i (BatIO.to_string (BatList.print BatInt.print) l))
  (42, []) \
    (let m, sb = pop_min (add 42 42 empty) in m, to_list sb)

  (42, [57]) \
    (let m, sb = pop_min (add 42 42 (add 57 57 empty)) in m, to_list sb)

  (42, [57]) \
    (let m, sb = pop_min (add 57 57 (add 42 42 empty)) in m, to_list sb)
 *)

let first = function
  | None -> invalid_arg "first"
  | Some t -> snd (LL.get t.llist)

let last = function
  | None -> invalid_arg "last"
  | Some t -> snd (LL.get (LL.prev t.llist))

let smallest = function
  | None -> invalid_arg "smallest"
  | Some t -> snd (LL.get (Heap.min t.heap))

let greatest = function
  | None -> invalid_arg "greatest"
  | Some t -> t.greatest
