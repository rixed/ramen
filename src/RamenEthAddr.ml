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

let print fmt n =
  String.print fmt (to_string n)

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
    (repeat ~min:6 ~max:6 ~sep:(char ':') hex_byte >>: fun bytes ->
     List.fold_left (append_num base) Num.zero bytes |>
     (* FIXME: Add to_int{32,64} in stdlib *)
     Num.to_string |> Uint48.of_string) m
end
