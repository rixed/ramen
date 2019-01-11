(* Units of measurements.
 * For dimensional analysis, documentation and data visualization of
 * numeric values.
 *
 * A dimension is merely known by its name, which is a mere string.
 * We assume that any newly introduced dimension is independent of all
 * others, so that we can enforce basic dimensional analysis rules.
 *
 * Any fields can then be given units, which is kept as a map from
 * dimension names to the corresponding exponent (as a float).
 *
 * To this, we add the notion of "relative dimension", which merely
 * means a measurement, given in some unit, relative to some unknown
 * reference point. For instance, a distance is measured in meters,
 * whereas a position is measured in meters relative to some reference point;
 * a duration is measured in seconds whereas a date is measured in seconds
 * relative to some origin of time; molecular kinetic energy is measured
 * in Kelvin whereas temperatures are measured in Kelvins relative to some
 * reference points...
 *
 * It is assumed that the reference point used for some dimension is always
 * the same.
 *
 * Rules governing those "relative" units are somewhat more limited than the
 * usual rules of dimensional analysis: one cannot multiply or divide
 * relative measurements at all, or add two relative measurements, but
 * an "absolute" measure can be added to or subtracted from a relative one,
 * and obtain either a new relative measurement or a difference given in
 * the "absolute" unit. Also, relative measurement cannot be multiplied
 * or divided by anything.
 *
 * Those rules are checked but not enforced so that ultimately the user is
 * in control. *)
open Batteries
open RamenHelpers

(*$inject
  open Batteries
  open TestHelpers

  let p2s = test_printer RamenUnits.print
*)

module MapUnit = struct
  include Map.String

  let t_ppp_ocaml ppp =
    let map_of_hash = of_enum % Hashtbl.enum
    and hash_of_map = Hashtbl.of_enum % enum
    in
    let open PPP in
    PPP_OCaml.hashtbl string ppp >>:
      (hash_of_map, map_of_hash)
end

type t = (float * bool) MapUnit.t
  [@@ppp PPP_OCaml]

let compare =
  MapUnit.compare (Tuple2.compare ~cmp1:Float.compare ~cmp2:Bool.compare)

let eq =
  MapUnit.equal (Tuple2.eq Float.equal Bool.equal)

let empty = MapUnit.empty

let make ?(rel=false) n = MapUnit.singleton n (1., rel)

let dimensionless = MapUnit.empty
let seconds = make "seconds"
let seconds_since_epoch = make ~rel:true "seconds"
let bytes = make "bytes"
let packets = make "packets"
let tuples = make "tuples"
let groups = make "groups"
let processes = make "processes"
let chars = make "chars"

let is_relative u =
  MapUnit.exists (fun _ (_e, rel) -> rel) u

let print oc =
  let p oc (e, rel) =
    Printf.fprintf oc "%s"
      (if rel then "(rel)" else "") ;
    if e <> 1. then
      Printf.fprintf oc "^%g" e
  in
  MapUnit.print ~first:"{" ~last:"}" ~sep:"*" ~kvsep:""
    String.print p oc

let to_string u = IO.to_string print u

let binop u1 c u2 =
  Printf.sprintf2 "%a %c %a"
    print u1 c print u2

let func n u1 u2 =
  Printf.sprintf2 "%s %a %a"
    n print u1 print u2

let fail ~what msg =
  Printf.sprintf "In %s: %s" what msg |>
  failwith

let add u1 u2 =
  let fail = fail ~what:(binop u1 '+' u2) in
  MapUnit.merge (fun _u e1 e2 ->
    match e1, e2 with
    | Some (e1, r1), Some (e2, r2) when e1 = e2 ->
        if r1 && r2 then fail "cannot add relative units" ;
        Some (e2, r1 || r2)
    | _ -> fail "not the same units"
  ) u1 u2

let sub ?what u1 u2 =
  let fail = fail ~what:(what |? binop u1 '-' u2) in
  MapUnit.merge (fun _u e1 e2 ->
    match e1, e2 with
    | Some (e1, r1), Some (e2, r2) when e1 = e2 ->
        if not r1 && r2 then fail "cannot subtract a relative unit" ;
        (* Only way to get a relative unit is to do relative - absolute: *)
        Some (e1, r1 && not r2)
    | _ -> fail "not the same units"
  ) u1 u2

let mul ?what u1 u2 =
  let fail = fail ~what:(what |? binop u1 '*' u2) in
  MapUnit.merge (fun _u e1 e2 ->
    let e1, r1 = e1 |? (0., false) and e2, r2 = e2 |? (0., false) in
    if r1 && r2 then
      fail "cannot multiply or divide two relative units" ;
    let e = e1 +. e2 in
    if e = 0. then None else Some (e, r1 || r2)
  ) u1 u2

let pow u n =
  MapUnit.map (fun (e, r) -> e *. n, r) u

let div u1 u2 =
  let u2' = MapUnit.map (fun (e, r) -> ~-.e, r) u2 in
  mul ~what:(binop u1 '/' u2) u1 u2'

let min u1 u2 = sub ~what:(func "min" u1 u2) u1 u2
let max u1 u2 = sub ~what:(func "max" u1 u2) u1 u2

(* For everything else: *)
let check_unitless u =
  if u <> MapUnit.empty then
    Printf.sprintf2 "%a must be dimensionless" print u |>
    failwith

(* Same or unset actually.
 * FIXME: as None means both unknown and _being_computed_, we might accept
 * a None as non-conflicting with some other before it's actually fully
 * typed, and then determine units that are actually incompatible but without
 * noticing it. We should distinguish between unknown_for_good and
 * not_yet_known. *)
let check_same_units ~what =
  Enum.fold (fun units_opt u ->
    match units_opt, u with
    | None, b -> b
    | a, None -> a
    | Some a, Some b ->
        if eq a b then units_opt
        else Printf.sprintf2 "%s have units %a and %a"
               what print a print b |>
             failwith)

module Parser =
struct
  (*$< Parser *)
  open RamenParsing

  let u m =
    let m = "unit" :: m in
    (
      identifier ++
      optional ~def:false
        (opt_blanks -+ strinG "(rel)" >>: fun () -> true) ++
      optional ~def:1. (opt_blanks -- char '^' -- opt_blanks -+ number)
    ) m

  let p m =
    let m = "units" :: m in
    let sep =
      (* TODO: also '/' *)
      opt_blanks -- (char '*' ||| char '.') -- opt_blanks in
    (
      char '{' -+ repeat ~sep u +- char '}' >>:
        List.fold_left (fun us ((n, r), u) ->
          MapUnit.modify_opt n (function
          | None -> Some (u, r)
          | Some (u', r') ->
              if r <> r' then
                raise (Reject "Cannot multiply absolute and relative unit") ;
              Some (u' +. u, r)
          ) us
        ) MapUnit.empty
    ) m

  (*$= p & ~printer:identity
    "{seconds}" (test_p p "{seconds}" |> p2s)
    "{}"        (test_p p "{}" |> p2s)
  *)
  (*$>*)
end
