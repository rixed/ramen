(* Functions related to eth addresses handling *)
open Batteries
open Stdint
open RamenHelpers

let to_string =
  let ff = Uint48.of_int 0xff in
  fun n ->
    let s = String.create 17 in
    let rec loop shf i =
      if shf >= 0 then (
        let v = Uint48.(shift_right_logical n shf |> logand ff |> to_int) in
        s.[i] <- hex_of (v lsr 4) ;
        s.[i+1] <- hex_of (v land 0xf) ;
        let i' = if shf = 0 then i+2 else (s.[i+2] <- ':' ; i+3) in
        loop (shf - 8) i'
      ) in
    loop 40 0 ;
    Bytes.to_string s

(*$= to_string & ~printer:(fun x -> x)
  "01:23:45:67:89:ab" \
    (to_string (Stdint.Uint48.of_string "0x123456789AB"))
 *)

let print oc n =
  String.print oc (to_string n)

module Parser =
struct
  open RamenParsing

  let append_num base n m = Num.add (Num.mul base n) m

  let hex_byte =
    let base = Num.num_of_int 16 in
    hexadecimal_digit ++ hexadecimal_digit >>: fun (hi, lo) ->
    append_num base (num_of_char hi) (num_of_char lo)

  let p m =
    let m = "eth address" :: m in
    let base = Num.num_of_int 256 in
    (
      dismiss_error_if (parsed_fewer_than 6) (
        repeat ~min:6 ~max:6 ~sep:(char ':') hex_byte >>: fun bytes ->
          List.fold_left (append_num base) Num.zero bytes |>
          (* FIXME: Add to_int{32,64} in stdlib *)
          Num.to_string |> Uint48.of_string
      )
    ) m
end

(* Fast (hum) parser from string for reading CSVs, command line, etc... *)
let of_string s o =
  let invalid o =
    Printf.sprintf "Invalid ethernet address at offset %d" o |>
    failwith in
  let rec loop a n o =
    if n >= 6 then a, o else (
      if o >= String.length s then invalid o ;
      let i, o = unsigned_of_hexstring s o in
      let o =
        if n < 5 && o < String.length s then (
          if s.[o] <> ':' then invalid o ;
          o + 1
        ) else o in
      loop Uint48.(shift_left a 8 + of_int i) (n + 1) o
    ) in
  loop Uint48.zero 0 o

(*$= of_string & ~printer:(BatIO.to_string (BatTuple.Tuple2.print print BatInt.print))
  (Stdint.Uint48.of_int64 0x12_34_56_78_9a_bcL, 17) (of_string "12:34:56:78:9a:bc" 0)
  (Stdint.Uint48.of_int64 0x12_34_56_78_9a_bcL, 17) (of_string "12:34:56:78:9A:BC" 0)
  (Stdint.Uint48.of_int64 0x01_00_00_00_00_00L, 17) (of_string "01:00:00:00:00:00" 0)
*)
