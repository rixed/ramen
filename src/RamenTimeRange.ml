open Batteries
open RamenHelpers

type t = (float * float) list [@@ppp PPP_OCaml]

let empty = []

let is_empty t = t = []

let make t1 t2 =
  let t1 = min t1 t2 and t2 = max t1 t2 in
  if t1 < t2 then [ t1, t2 ] else []

(*$T make
  is_empty (make 1. 1.)
  not (is_empty (make 1. 2.))
*)

let rec merge l1 l2 =
  match l1, l2 with
  | [], l | l, [] -> l
  | (a1, b1)::r1, (a2, b2)::r2 ->
      let am = min a1 a2 and bm = max b1 b2 in
      if bm -. am <= (b1 -. a1) +. (b2 -. a2) then
        (am, bm) :: merge r1 r2
      else if a1 <= a2 then
        (a1, b1) :: merge r1 l2
      else
        (a2, b2) :: merge l1 r2

(*$= merge & ~printer:(BatIO.to_string print)
  [ 1.,2. ; 3.,4. ] (merge [ 1.,2. ] [ 3.,4. ])
  [ 1.,2. ; 3.,4. ] (merge [ 3.,4. ] [ 1.,2. ])
  [ 1.,4. ] (merge [ 1.,3. ] [ 2.,4. ])
  [ 1.,4. ] (merge [ 1.,2. ] [ 2.,4. ])
  [ 1.,4. ] (merge [ 1.,4. ] [ 2.,3. ])
  [] (merge [] [])
  [ 1.,2. ] (merge [ 1.,2. ] [])
  [ 1.,2. ] (merge [] [ 1.,2. ])
*)

let single_inter (a1, b1) (a2, b2) =
  let a = max a1 a2 and b = min b1 b2 in
  if a < b then (a, b) else raise Not_found

let rec inter l1 l2 =
  match l1, l2 with
  | [], _ | _, [] -> []
  | (a1, b1 as i1)::r1, (a2, b2 as i2)::r2 ->
      match single_inter i1 i2 with
      | exception Not_found ->
          if a1 <= a2 then inter r1 l2 else inter l1 r2
      | (_, b) as i ->
          i :: (inter ((b, b1)::r1) ((b, b2)::r2))

(*$= inter & ~printer:(BatIO.to_string print)
  [] (inter [ 1.,2. ] [ 3.,4. ])
  [] (inter [ 3.,4. ] [ 1.,2. ])
  [ 2.,3. ] (inter [ 1.,3. ] [ 2.,4. ])
  [] (inter [ 1.,2. ] [ 2.,4. ])
  [ 2.,3. ] (inter [ 1.,4. ] [ 2.,3. ])
  [] (inter [] [])
  [] (inter [ 1.,2. ] [])
  [] (inter [] [ 1.,2. ])
  [ 2.,3. ; 4.,5. ] (inter [ 1.,3. ; 4., 6.] [ 2., 5. ])
*)

let rec span = function
  | [] -> 0.
  | (a, b) :: r -> (b -. a) +. span r

let print oc =
  let rel = ref "" in
  let p = print_as_date_rel ~rel ~right_justified:false in
  List.print (Tuple2.print p p) oc

let approx_eq l1 l2 =
  (* [l1] is approx_eq [l2] if the intersection of [l1] and [l2] has
   * approximately the same length as that of [l1] and that of [l2].
   * This is used to determine if two replays can be merged. The time
   * range will be the union of both of course, but if they are too
   * distinct it's better to start two replayers than to share a
   * single one. *)
  let i = span (inter l1 l2) in
  i >= 0.7 *. span l1 &&
  i >= 0.7 *. span l2

let bounds = function
  | [] ->
      invalid_arg "Range.bounds"
  | (a, _) :: _ as l ->
      a, snd (List.last l)
