(* Lifting functions for non-basic types that are used. *)
open Stdint

let lift_i8 (x : int8) =
  let x' = Int8.to_int x in .< Int8.of_int x' >.

let lift_u8 (x : Uint8.t) =
  let x' = Uint8.to_int x in .< Uint8.of_int x' >.

let lift_i16 (x : int16) =
  let x' = Int16.to_int x in .< Int16.of_int x' >.

let lift_u16 (x : Uint16.t) =
  let x' = Uint16.to_int x in .< Uint16.of_int x' >.

let lift_i32 (x : int32) =
  if x >= Int32.of_int min_int && x <= Int32.of_int max_int then
    let x' = Int32.to_int x in .< Int32.of_int x' >.
  else
    let x' = Int32.to_string x in .< Int32.of_string x' >.

let lift_u32 (x : Uint32.t) =
  if x >= Uint32.of_int min_int && x <= Uint32.of_int max_int then
    let x' = Uint32.to_int x in .< Uint32.of_int x' >.
  else
    let x' = Uint32.to_string x in .< Uint32.of_string x' >.

let lift_u48 (x : Uint48.t) =
  if x >= Uint48.of_int min_int && x <= Uint48.of_int max_int then
    let x' = Uint48.to_int x in .< Uint48.of_int x' >.
  else
    let x' = Uint48.to_string x in .< Uint48.of_string x' >.

let lift_i64 (x : int64) =
  if x >= Int64.of_int min_int && x <= Int64.of_int max_int then
    let x' = Int64.to_int x in .< Int64.of_int x' >.
  else
    let x' = Int64.to_string x in .< Int64.of_string x' >.

let lift_u64 (x : Uint64.t) =
  if x >= Uint64.of_int min_int && x <= Uint64.of_int max_int then
    let x' = Uint64.to_int x in .< Uint64.of_int x' >.
  else
    let x' = Uint64.to_string x in .< Uint64.of_string x' >.

let lift_i128 (x : Int128.t) =
  if x >= Int128.of_int min_int && x <= Int128.of_int max_int then
    let x' = Int128.to_int x in .< Int128.of_int x' >.
  else
    let x' = Int128.to_string x in .< Int128.of_string x' >.

let lift_u128 (x : Uint128.t) =
  if x >= Uint128.of_int min_int && x <= Uint128.of_int max_int then
    let x' = Uint128.to_int x in .< Uint128.of_int x' >.
  else
    let x' = Uint128.to_string x in .< Uint128.of_string x' >.

let lift_int (x : int) = .< x >.

let lift_bool = function
  | true -> .< true >.
  | false -> .< false >.

let lift_option (lifter : 'a -> 'a code) (x : 'a option) =
  match x with
  | None -> .< None >.
  | Some x -> .< Some .~(lifter x) >.

