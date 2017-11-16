(* Converts from floats to string for label *)
open Engine
open WebHelpers

type t = {
  to_label : float -> string ;
  name : string ;
  base : float ;
}

let numeric = {
  name = "numeric" ;
  base = 10. ;
  to_label = short_string_of_float ;
}

(* We must reset this state after each axis... *)
let last_s = ref ""

let label_of_timestamp t =
  let rec len l s =
    if String.length s >= l then s else len l ("0"^s) in
  let jst = new%js Js.date_fromTimeValue (1000. *. t) in
  let s = (string_of_int jst##getFullYear |> len 4) ^"-"^
          (string_of_int (jst##getMonth + 1) |> len 2) ^"-"^
          (string_of_int jst##getDate |> len 2) ^" "^
          (string_of_int jst##getHours |> len 2) ^"h"^
          (string_of_int jst##getMinutes |> len 2) ^"m"^
          (string_of_int jst##getSeconds |> len 2) ^"s" in
  let rec prefix_len i =
    if i >= String.length !last_s ||
       s.[i] <> !last_s.[i] then i
    else prefix_len (i+1) in
  let pl = prefix_len 0 in
  (* Avoid axing the string in the middle of a meaningful unit: *)
  let pl = if pl < 11 then 0 else
           if pl < 14 then 11 else
           if pl < 17 then 14 else
           if pl < 20 then 17 else pl in
  last_s := s ;
  (* Make it even shorter by removing 00 endings: *)
  let rec suffix_start i =
    let zero = "XXXX-XX-XX 00h00m00s" in
    if zero.[i] <> s.[i] then i
    else suffix_start (i-1) in
  let e = suffix_start (String.length s - 1) in
  (* Avoid chopping the unit: *)
  let e = if e > 16 then 19 else
          if e > 13 then 16 else
          if e > 10 then 13 else e in
  String.sub s pl (e + 1 - pl)

let timestamp = {
  name = "timestamp" ;
  base = 60. ;
  to_label = label_of_timestamp 
}

let reset_all_states () = last_s := ""

let all =
  [ "numeric", numeric ;
    "timestamp", timestamp ]
