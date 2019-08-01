open Batteries
open RamenHelpers

(* The last bool tells if that range is still growing. Should be false for
 * all non tail range. *)
type t = (float * float * bool) list [@@ppp PPP_OCaml]

let empty = []

let is_empty t = t = []

let make t1 t2 oe =
  let t1 = min t1 t2 and t2 = max t1 t2 in
  if t1 < t2 then [ t1, t2, oe ] else []

(*$T make
  is_empty (make 1. 1. false)
  not (is_empty (make 1. 2. false))
*)

let rec merge l1 l2 =
  match l1, l2 with
  | [], l | l, [] -> l
  | (a1, b1, oe1 as i1)::r1, (a2, b2, oe2 as i2)::r2 ->
      if oe1 then
        (a1, max b1 b2, true) :: merge r1 r2 else
      let am = min a1 a2 and bm = max b1 b2 in
      if bm -. am <= (b1 -. a1) +. (b2 -. a2) then
        (am, bm, oe2) :: merge r1 r2
      else if a1 <= a2 then
        i1 :: merge r1 l2
      else
        i2 :: merge l1 r2

(*$= merge & ~printer:(BatIO.to_string print)
  [ 1.,2.,false ; 3.,4.,false ] (merge [ 1.,2.,false ] [ 3.,4.,false ])
  [ 1.,2.,false ; 3.,4.,false ] (merge [ 3.,4.,false ] [ 1.,2.,false ])
  [ 1.,4.,false ] (merge [ 1.,3.,false ] [ 2.,4.,false ])
  [ 1.,4.,false ] (merge [ 1.,2.,false ] [ 2.,4.,false ])
  [ 1.,4.,false ] (merge [ 1.,4.,false ] [ 2.,3.,false ])
  [] (merge [] [])
  [ 1.,2.,false ] (merge [ 1.,2.,false ] [])
  [ 1.,2.,false ] (merge [] [ 1.,2.,false ])
  [ 1.,4.,true ] (merge [ 1.,2.,true ] [ 3.,4.,false])
*)

let single_inter (a1, b1, oe1) (a2, b2, oe2) =
  let a = max a1 a2
  and b, oe =
    if b1 <= b2 then b1, oe1
                else b2, oe2 in
  if a < b then (a, b, oe) else raise Not_found

let rec inter l1 l2 =
  match l1, l2 with
  | [], _ | _, [] -> []
  | (a1, b1, oe1 as i1)::r1, (a2, b2, oe2 as i2)::r2 ->
      match single_inter i1 i2 with
      | exception Not_found ->
          if a1 <= a2 then inter r1 l2 else inter l1 r2
      | (_, b, _) as i ->
          i :: (inter ((b, b1, oe1)::r1) ((b, b2, oe2)::r2))

(*$= inter & ~printer:(BatIO.to_string print)
  [] (inter [ 1.,2.,false ] [ 3.,4.,false ])
  [] (inter [ 3.,4.,false ] [ 1.,2.,false ])
  [ 2.,3.,false ] (inter [ 1.,3.,false ] [ 2.,4.,false ])
  [] (inter [ 1.,2.,false ] [ 2.,4.,false ])
  [ 2.,3.,false ] (inter [ 1.,4.,false ] [ 2.,3.,false ])
  [] (inter [] [])
  [] (inter [ 1.,2.,false ] [])
  [] (inter [] [ 1.,2.,false ])
  [ 2.,3.,false ; 4.,5.,false ] \
    (inter [ 1.,3.,false ; 4.,6.,false ] [ 2.,5.,false ])
*)

(* Ignoring open-endedness: *)
let rec span = function
  | [] -> 0.
  | (a, b, _) :: r -> (b -. a) +. span r

let print oc =
  let rel = ref "" in
  let p = print_as_date_rel ~rel ~right_justified:false in
  List.print (fun oc (t1, t2, oe) ->
    Printf.fprintf oc "%a..%a%s" p t1 p t2 (if oe then "+" else "")) oc

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
  | (a, _, _) :: _ as l ->
      a, Tuple3.second (List.last l)
