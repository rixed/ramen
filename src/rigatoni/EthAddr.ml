(* Functions related to eth addresses handling *)
open Batteries
open Stdint

let print fmt n =
  let ff = Uint48.of_int 0xff in
  let rec loop shf =
    if shf >= 0 then (
      let v = Uint48.(shift_right_logical n shf |> logand ff |> to_int) in
      Printf.fprintf fmt "%02x%s" v (if shf = 0 then "" else ":") ;
      loop (shf - 8)
    ) in
  loop 40

(*$= print & ~printer:(fun x -> x)
  "01:23:45:67:89:ab" \
    (BatIO.to_string print (Stdint.Uint48.of_string "0x123456789AB"))
 *)

module Parser =
struct
  open RamenParsing
  open ParseUsual
  open P

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
