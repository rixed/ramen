(* Function related to IPv4 addresses *)
open Batteries
open Stdint

let print fmt n =
  let mask = Uint32.of_int 255 in
  let digit shf =
    Uint32.(shift_right_logical n shf |>
            logand mask |>
            to_int) in
  Printf.fprintf fmt "%d.%d.%d.%d"
    (digit 24) (digit 16) (digit 8) (digit 0)

(*$= print & ~printer:(fun x -> x)
  "123.45.67.89" \
    (BatIO.to_string print (Stdint.Uint32.of_string "0x7B2D4359"))
 *)

module Parser =
struct
  open RamenParsing

  let p m =
    let m = "IPv4 address" :: m in
    (repeat ~min:4 ~max:4 ~sep:(char '.') unsigned_decimal_number >>: fun lst ->
       List.fold_left (fun (s, shf) n ->
           Uint32.(add s (shift_left (of_int (Num.to_int n)) shf)),
           shf - 8
         ) (Uint32.zero, 24) lst |> fst) m
end

let of_string = RamenParsing.of_string_exn Parser.p

(*$= of_string & ~printer:(BatIO.to_string print)
   (Stdint.Uint32.of_int32 0x01020304l) (of_string "1.2.3.4")
 *)
