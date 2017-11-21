(* Some more helpers that depend on Js. *)
open WebHelpers

let with_debug = false

let print a = if with_debug then Firebug.console##log a
let print_2 a b = if with_debug then Firebug.console##log_2 a b
let print_3 a b c = if with_debug then Firebug.console##log_3 a b c
let print_4 a b c d = if with_debug then Firebug.console##log_4 a b c d
let fail msg =
  Firebug.console##log (Js.string ("Failure: "^ msg)) ;
  Firebug.console##assert_ Js._false ;
  assert false
let fail_2 msg x =
  Firebug.console##log_2 (Js.string ("Failure: "^ msg)) x ;
  Firebug.console##assert_ Js._false ;
  assert false

let option_get = function Some x -> x | None -> fail "Invalid None"
let optdef_get x = Js.Optdef.get x (fun () -> fail "Invalid undef")
let opt_get x = Js.Opt.get x (fun () -> fail "Invalid None")
let to_int x = Js.float_of_number x |> int_of_float

let date_of_ts ts =
  let d = new%js Js.date_fromTimeValue (1000. *. ts) in
  Js.to_string d##toLocaleString

(* Conversion to/from JS values *)

let js_of_list j lst =
  let a = new%js Js.array_empty in
  List.iter (fun i -> a##push (j i) |> ignore) lst ;
  a
let js_of_option j = function None -> Js.null | Some v -> Js.some (j v)
let js_of_float = Js.number_of_float

let list_of_js c js =
  list_init js##.length (fun i ->
    let j = Js.array_get js i |> optdef_get in
    c j)
let of_field js n c = Js.Unsafe.get js n |> c
let of_opt_field js (n : string) c =
  (* Value can be unset or undef *)
  Js.Optdef.case (Js.Unsafe.get js n)
    (fun () -> None)
    (fun v ->
      Js.Opt.case v
        (fun () -> None)
        (fun v -> Some (c v)))
let variant_of_js vars js =
  let rec loop = function
  | [] -> fail_2 "Unknown variant" js
  | (name, conv)::vars ->
    Js.Optdef.case (Js.Unsafe.get js name)
      (fun () -> loop vars)
      (fun v -> conv v) in
  loop vars
let pair_of_js c1 c2 js =
  c1 (Js.array_get js 0 |> optdef_get),
  c2 (Js.array_get js 1 |> optdef_get)
let array_of_js c js =
  Array.init js##.length (fun i ->
    Js.array_get js i |> optdef_get |> c)

(* For a smaller JS: *)
let string_of_float x =
  (Js.number_of_float x)##toString |> Js.to_string
let string_of_int x = string_of_float (float_of_int x)

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
