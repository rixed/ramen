(* Small tools used in the WWW-UI that does not depend in Js_of_ocaml
 * and that we can therefore link with the tests *)

(* Stdlib complement: *)

let identity x = x

let apply f = f ()

let option_may f = function
  | None -> ()
  | Some x -> f x

let option_map f = function
  | None -> None
  | Some x -> Some (f x)

let option_def x = function None -> x | Some v -> v
let (|?) a b = option_def b a

let list_init n f =
  let rec loop prev i =
    if i >= n then List.rev prev else
    loop (f i :: prev) (i + 1) in
  loop [] 0

let replace_assoc n v l = (n, v) :: List.remove_assoc n l

let string_starts_with p s =
  let open String in
  length s >= length p &&
  sub s 0 (length p) = p

let rec string_times n s =
  if n = 0 then "" else s ^ string_times (n - 1) s

let abbrev len s =
  if String.length s <= len then s else
  String.sub s 0 (len-3) ^"..."

let rec short_string_of_float f =
  if f = 0. then "0" else  (* take good care of ~-.0. *)
  if f < 0. then "-"^ short_string_of_float (~-.f) else
  (* SVG don't like digits ending with a dot *)
  let s = Printf.sprintf "%.5f" f in (* limit number of significant digits to reduce page size *)
  (* chop trailing zeros and trailing dot *)
  let rec chop last l =
    let c = s.[l] in
    if last || l < 1 || c <> '0' && c <> '.' then (
      if l = String.length s - 1 then s else
      String.sub s 0 (l + 1)
    ) else
      chop (c = '.') (l - 1) in
  chop false (String.length s - 1)

let log_base base n = log n /. log base

let grid_interval ?(base=10.) n start stop =
  let dv = stop -. start in
  (* find the round value closest to dv/n (by round we mean 1, 5, 10...) *)
  let l = dv /. float_of_int n in (* l = length if we split dv in n equal parts *)
  let f = base ** floor (log_base base l) in (* f closest power of 10 below l *)
  let i = floor (l /. f) in (* i >= 1, how much f is smaller than l *)
  if i < 2.5 || 5. *. f >= dv then f else (* if it's less than 2.5 times smaller, use it *)
  if i < 7.5 || 10. *. f >= dv then 5. *. f else (* if it's around 5 times smaller, use 5*f *)
  10. *. f

(*$Q grid_interval
  (Q.triple Q.small_int Q.float Q.pos_float) (fun (n, start, width) -> \
    n = 0 || width = 0. || \
    let interval = grid_interval n start (start +. width) in \
    interval >= 0. && interval <= width)
 *)

(* Given a range of values [start:stop], returns an enum of approximatively [n]
 * round intermediate values *)

let grid ?base n start stop =
  let interval = grid_interval ?base n start stop in
  let lo = interval *. floor (start /. interval) in
  let lo = if lo >= start then lo else lo +. interval in
  let rec loop prev i =
    if stop >= i then
      loop (i :: prev) (i +. interval)
    else List.rev prev in
  loop [] lo

(*$Q grid
  (Q.triple Q.small_int Q.float Q.pos_float) (fun (n, start, width) -> \
    n = 0 || width = 0. || \
    let stop = start +. width in \
    grid n start stop |> List.for_all (fun x -> x >= start && x <= stop))
 *)
