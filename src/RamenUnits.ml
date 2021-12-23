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
open RamenHelpersNoLog
open RamenHelpers

include Units.DessserGen

(*$inject
  open Batteries
  open TestHelpers

  let p2s = test_printer RamenUnits.print
*)

let eq u1 u2 =
  let eq1 (n1, (p1, r1)) (n2, (p2, r2)) =
    n1 = n2 && p1 = p2 && r1 = r2 in
  try
    Array.for_all2 eq1 u1 u2
  with Invalid_argument _ ->
    false

let make ?(rel=false) name =
  [| name, (1., rel) |]

let empty = [||]

let dimensionless = empty
let seconds = make "seconds"
let seconds_since_epoch = make ~rel:true "seconds"
let bytes = make "bytes"
let packets = make "packets"
let tuples = make "tuples"
let groups = make "groups"
let processes = make "processes"
let chars = make "chars"
let operations = make "operations"

let is_relative u =
  Array.exists (fun (_, (_, rel)) -> rel) u

let print oc =
  Array.print ~first:"{" ~last:"}" ~sep:"*"
    (fun oc (name, (p, rel)) ->
      Printf.fprintf oc "%s%s"
        name (if rel then "(rel)" else "") ;
      if p <> 1. then
        Printf.fprintf oc "^%g" p) oc

let to_string u =
  IO.to_string print u

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
  assoc_array_merge (fun u1 u2 ->
    match u1, u2 with
    | Some (p1, r1), Some (p2, r2) when p1 = p2 ->
        if r1 && r2 then fail "Cannot add relative units" ;
        Some (p1, r1 || r2)
    | _ -> fail "not the same units"
  ) u1 u2

let sub ?what u1 u2 =
  let fail = fail ~what:(what |? binop u1 '-' u2) in
  assoc_array_merge (fun u1 u2 ->
    match u1, u2 with
    | Some (p1, r1), Some (p2, r2) when p1 = p2 ->
        (* relative - relative -> absolute
         * relative - absolute -> relative
         * absolute - relative -> meaningless
         * absolute - absolute -> absolute *)
        let r =
          match r1, r2 with
          | true, true ->
              false
          | true, false ->
              true
          | false, true ->
              fail "cannot subtract a relative from an absolute unit"
          | false, false ->
              false in
        Some (p1, r)
    | _ -> fail "not the same units"
  ) u1 u2

let mul ?what u1 u2 =
  let fail = fail ~what:(what |? binop u1 '*' u2) in
  assoc_array_merge (fun u1 u2 ->
    let nul = 0., false in
    let p1, r1 = u1 |? nul
    and p2, r2 = u2 |? nul in
    if r1 && r2 then
      fail "cannot multiply or divide two relative units" ;
    let power = p1 +. p2 in
    if power = 0. then
      None
    else
      Some (power, r1 || r2)
  ) u1 u2

let pow u i =
  Array.map (fun (n, (p, r)) ->
    n, (p *. i, r)
  ) u

let div u1 u2 =
  let u2' = Array.map (fun (n, (p, r)) -> n, (0. -. p, r)) u2 in
  mul ~what:(binop u1 '/' u2) u1 u2'

let min u1 u2 = sub ~what:(func "min" u1 u2) u1 u2
let max u1 u2 = sub ~what:(func "max" u1 u2) u1 u2

(* For everything else: *)
let check_unitless u =
  if not (array_is_empty u) then
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
      opt_blanks -- (char '*' |<| char '.') -- opt_blanks in
    (
      char '{' -+ repeat ~sep u +- char '}' >>:
        List.fold_left (fun us ((n, r), p) ->
          assoc_array_modify_opt n (function
          | None -> Some (p, r)
          | Some (p', r') ->
              if r' <> r then
                raise (Reject "Cannot multiply absolute and relative unit") ;
              Some (p' +. p, r)
          ) us
        ) [||]
    ) m

  (*$= p & ~printer:identity
    "{seconds}" (test_p p "{seconds}" |> p2s)
    "{seconds(rel)}" (test_p p "{seconds (rel)}" |> p2s)
    "{}"        (test_p p "{}" |> p2s)
  *)
  (*$>*)
end
