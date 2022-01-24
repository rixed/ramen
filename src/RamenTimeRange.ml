open Batteries
open RamenHelpers
open RamenHelpersNoLog

(* The last bool tells if that range is still growing. Should be false for
 * all non tail range. *)
include Time_range.DessserGen

let empty = [||]

let is_empty = array_is_empty

let make t1 t2 growing =
  let since = min t1 t2 and until = max t1 t2 in
  if since < until then [| { since ; until ; growing } |]
  else empty

(*$T make
  is_empty (make 1. 1. false)
  not (is_empty (make 1. 2. false))
*)

let rec merge l1 l2 =
  match l1, l2 with
  | [], l | l, [] -> l
  | i1::r1, i2::r2 ->
      if i1.growing then
        { since = i1.since ; until = max i1.until i2.until ; growing = true } ::
        merge r1 r2 else
      let am = min i1.since i2.since and bm = max i1.until i2.until in
      if bm -. am <= (i1.until -. i1.since) +. (i2.until -. i2.since) then
        { since = am ; until = bm ; growing = i2.growing } :: merge r1 r2
      else if i1.since <= i2.since then
        i1 :: merge r1 l2
      else
        i2 :: merge l1 r2

(* TODO: serialization of slist in dessser *)
let merge a1 a2 =
  let l1 = Array.to_list a1
  and l2 = Array.to_list a2 in
  merge l1 l2 |>
  Array.of_list

(*$= merge & ~printer:(BatIO.to_string print)
  [| { since = 1.; until = 2.; growing = false } ;\
     { since = 3.; until = 4.; growing = false } |] \
        (merge [| { since = 1.; until = 2.; growing = false } |] \
               [| { since = 3.; until = 4.; growing = false } |])
  [| { since = 1.; until = 2.; growing = false } ;\
     { since = 3.; until = 4.; growing = false } |] \
        (merge [| { since = 3.; until = 4.; growing = false } |] \
               [| { since = 1.; until = 2.; growing = false } |])
  [| { since = 1.; until = 4.; growing = false } |] \
        (merge [| { since = 1.; until = 3.; growing = false } |] \
               [| { since = 2.; until = 4.; growing = false } |])
  [| { since = 1.; until = 4.; growing = false } |] \
        (merge [| { since = 1.; until = 2.; growing = false } |] \
               [| { since = 2.; until = 4.; growing = false } |])
  [| { since = 1.; until = 4.; growing = false } |] \
        (merge [| { since = 1.; until = 4.; growing = false } |] \
               [| { since = 2.; until = 3.; growing = false } |])
  [||] (merge [||] [||])
  [| { since = 1.; until = 2.; growing = false } |] \
        (merge [| { since = 1.; until = 2.; growing = false } |] [||])
  [| { since = 1.; until = 2.; growing = false } |] \
        (merge [||] [| { since = 1.; until = 2.; growing = false } |])
  [| { since = 1.; until = 4.; growing = true } |] \
        (merge [| { since = 1.; until = 2.; growing = true } |] \
               [| { since = 3.; until = 4.; growing = false } |])
*)

let single_inter i1 i2 =
  let a = max i1.since i2.since
  and b, oe =
    if i1.until <= i2.until then i1.until, i1.growing
                else i2.until, i2.growing in
  if a < b then { since = a ; until = b ; growing = oe }
           else raise Not_found

let rec inter l1 l2 =
  match l1, l2 with
  | [], _ | _, [] -> []
  | i1::r1, i2::r2 ->
      match single_inter i1 i2 with
      | exception Not_found ->
          if i1.since <= i2.since then inter r1 l2 else inter l1 r2
      | i ->
          i :: (inter ({ i1 with since = i.until }::r1)
                      ({ i2 with since = i.until }::r2))

let inter r1 r2 =
  let l1 = Array.to_list r1
  and l2 = Array.to_list r2 in
  inter l1 l2 |>
  Array.of_list

(*$= inter & ~printer:(BatIO.to_string print)
  [||] (inter [| { since = 1.; until = 2.; growing = false } |] \
              [| { since = 3.; until = 4.; growing = false } |])
  [||] (inter [| { since = 3.; until = 4.; growing = false } |] \
              [| { since = 1.; until = 2.; growing = false } |])
  [| { since = 2.; until = 3.; growing = false } |] \
        (inter [| { since = 1.; until = 3.; growing = false } |] \
               [| { since = 2.; until = 4.; growing = false } |])
  [||] (inter [| { since = 1.; until = 2.; growing = false } |] \
              [| { since = 2.; until = 4.; growing = false } |])
  [| { since = 2.; until = 3.; growing = false } |] \
        (inter [| { since = 1.; until = 4.; growing = false } |] \
               [| { since = 2.; until = 3.; growing = false } |])
  [||] (inter [||] [||])
  [||] (inter [| { since = 1.; until = 2.; growing = false } |] [||])
  [||] (inter [||] [| { since = 1.; until = 2.; growing = false } |])
  [| { since = 2.; until = 3.; growing = false } ; \
     { since = 4.; until = 5.; growing = false } |] \
    (inter [| { since = 1.; until = 3.; growing = false } ;\
              { since = 4.; until = 6.; growing = false } |] \
           [| { since = 2.; until = 5.; growing = false } |])
*)

(* Ignoring open-endedness: *)
let rec span r =
  Array.fold (fun s t ->
    (t.until -. t.since) +. s
  ) 0. r

let print oc =
  let rel = ref "" in
  let p = print_as_date_rel ~rel ~right_justified:false in
  Array.print (fun oc i ->
    Printf.fprintf oc "%a..%a%s"
      p i.since
      p i.until
      (if i.growing then "+" else "")) oc

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

(* Returns the ends of the TimeRange, ignoring open-endedness: *)
let bounds r =
  if array_is_empty r then
    invalid_arg "Range.bounds" ;
  r.(0).since,
  r.(Array.length r - 1).until
