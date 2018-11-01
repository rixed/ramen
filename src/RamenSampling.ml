(* Sampling function: build a sized down version of the list of all entries.
 * Returns a list, as the number of elements is unknown (only known to be
 * between 1 and max, inclusive) *)
open Stdint

type 'a reservoir =
  { mutable cur_size : int ;
    (* Count number of incoming items: *)
    mutable count : int ;
    (* The size of this array gives the reservoir max size.
     * Uninitialized values are initialized to a random value given by the
     * user: *)
    arr : 'a array  }

let init max_size any_value =
  let max_size = Uint32.to_int max_size in
  (* Because empty lists are invalid, the language makes sure we cannot ask
   * for max 0 samples: *)
  assert (max_size > 0) ;
  { cur_size = 0 ; count = 0 ;
    arr = Array.make max_size any_value }

let add t x =
  let i = t.count in
  t.count <- i + 1 ;
  if t.cur_size < Array.length t.arr then (
    t.arr.(t.cur_size) <- x ;
    t.cur_size <- t.cur_size + 1
  ) else (
    let max_size = float_of_int (Array.length t.arr) in
    let keep = Random.float 1. < max_size /. float_of_int (i + 1) in
    if keep then
      t.arr.(Random.int (Array.length t.arr)) <- x
  ) ;
  t

(* Because empty lists are invalid, if we had no entries at all we must return
 * None (will be turned into NULL): *)
let finalize t =
  if t.cur_size = 0 then None else
  if t.cur_size < Array.length t.arr then (
    (* TODO: Lists should be represented by slices not arrays *)
    Some (Array.sub t.arr 0 t.cur_size)
  ) else Some t.arr
