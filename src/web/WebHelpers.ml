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

let string_of_timestamp t =
  let rec len l s =
    if String.length s >= l then s else len l ("0"^s) in
  let jst = new%js Js.date_fromTimeValue (1000. *. t) in
  (string_of_int jst##getFullYear |> len 4) ^"-"^
  (string_of_int (jst##getMonth + 1) |> len 2) ^"-"^
  (string_of_int jst##getDate |> len 2) ^" "^
  (string_of_int jst##getHours |> len 2) ^"h"^
  (string_of_int jst##getMinutes |> len 2) ^"m"^
  (string_of_int jst##getSeconds |> len 2) ^"s"

(* JS efficient string hash table: *)
let shash =
  RamenChart.{
    create = Jstable.create ;
    find = (fun h k ->
      let k = Js.string k in
      Jstable.find h k |>
      Js.Optdef.to_option) ;
    add = (fun h k v ->
      let k = Js.string k in
      Jstable.add h k v) }
