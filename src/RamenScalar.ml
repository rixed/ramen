(* This module parses scalar types and values.
 *)
open Batteries
open Stdint
open RamenLang

(*$inject
  open TestHelpers
  open RamenLang
*)

(* TNum is not an actual type used by any value, but it's used as a default
 * type for numeric operands that can be "promoted" to any other numerical
 * type. TAny is meant to be replaced by an actual type during compilation:
 * all TAny types in an expression will be changed to a specific type that's
 * large enough to accommodate all the values at hand. *)
type typ =
  | TNull | TFloat | TString | TBool | TNum | TAny
  | TU8 | TU16 | TU32 | TU64 | TU128
  | TI8 | TI16 | TI32 | TI64 | TI128
  | TEth (* 48bits unsigned integers with funny notation *)
  | TIpv4 | TIpv6 | TCidrv4 | TCidrv6
  [@@ppp PPP_OCaml]

let string_of_typ = function
  | TNull   -> "NULL"
  | TFloat  -> "FLOAT"
  | TString -> "STRING"
  | TBool   -> "BOOL"
  | TNum    -> "ANY_NUM" (* This one not for consumption *)
  | TAny    -> "ANY" (* same *)
  | TU8     -> "U8"
  | TU16    -> "U16"
  | TU32    -> "U32"
  | TU64    -> "U64"
  | TU128   -> "U128"
  | TI8     -> "I8"
  | TI16    -> "I16"
  | TI32    -> "I32"
  | TI64    -> "I64"
  | TI128   -> "I128"
  | TEth    -> "Eth"
  | TIpv4   -> "IPv4"
  | TIpv6   -> "IPv6"
  | TCidrv4 -> "CIDRv4"
  | TCidrv6 -> "CIDRv6"

let print_typ fmt typ =
  String.print fmt (string_of_typ typ)

type type_class = KNum | KBool | KString | KNull | KCidrv4 | KCidrv6
let compare_typ typ1 typ2 =
  let rank_of_typ = function
    | TFloat  -> KNum, 200
    | TU128   -> KNum, 128
    | TIpv6   -> KNum, 128
    | TI128   -> KNum, 127
    | TNum    -> KNum, 0
    | TU64    -> KNum, 64
    | TI64    -> KNum, 63
    (* We consider Eth and IPs numbers so we can cast directly to/from ints
     * and use comparison operators. *)
    | TEth    -> KNum, 48
    | TU32    -> KNum, 32
    | TIpv4   -> KNum, 32
    | TI32    -> KNum, 31
    | TU16    -> KNum, 16
    | TI16    -> KNum, 15
    | TU8     -> KNum, 8
    | TI8     -> KNum, 7
    | TString -> KString, 1
    | TBool   -> KBool, 1
    | TNull   -> KNull, 0
    | TCidrv4 -> KCidrv4, 0
    | TCidrv6 -> KCidrv6, 0
    | TAny    -> assert false
  in
  let k1, r1 = rank_of_typ typ1
  and k2, r2 = rank_of_typ typ2 in
  if k1 <> k2 then invalid_arg "types not comparable" ;
  compare r1 r2

let larger_type (t1, t2) =
  if compare_typ t1 t2 >= 0 then t1 else t2

let largest_type = function
  | fst :: rest ->
    List.fold_left (fun l t ->
      larger_type (l, t)) fst rest
  | _ -> invalid_arg "largest_type"

(* stdint types are implemented as custom blocks, therefore are slower than
 * ints.  But we do not care as we merely represents code here, we do not run
 * the operators. *)
type value =
  | VFloat of float | VString of string | VBool of bool
  | VU8 of uint8 | VU16 of uint16 | VU32 of uint32
  | VU64 of uint64 | VU128 of uint128
  | VI8 of int8 | VI16 of int16 | VI32 of int32
  | VI64 of int64 | VI128 of int128 | VNull
  | VEth of uint48
  | VIpv4 of uint32 | VIpv6 of uint128
  | VCidrv4 of (uint32 * int) | VCidrv6 of (uint128 * int)
  [@@ppp PPP_OCaml]

let type_of = function
  | VFloat _ -> TFloat | VString _ -> TString | VBool _ -> TBool
  | VU8 _ -> TU8 | VU16 _ -> TU16 | VU32 _ -> TU32 | VU64 _ -> TU64
  | VU128 _ -> TU128 | VI8 _ -> TI8 | VI16 _ -> TI16 | VI32 _ -> TI32
  | VI64 _ -> TI64 | VI128 _ -> TI128 | VNull -> TNull
  | VEth _ -> TEth | VIpv4 _ -> TIpv4 | VIpv6 _ -> TIpv6
  | VCidrv4 _ -> TCidrv4 | VCidrv6 _ -> TCidrv6

(* The original Float.to_string adds a useless dot at the end of
 * round numbers: *)
let my_float_to_string v =
  let s = Float.to_string v in
  assert (String.length s > 0) ;
  if s.[String.length s - 1] <> '.' then s else String.rchop s

(* Used for debug, value expansion within strings, output values in tail
 * and timeseries commands, test immediate values.., but not for code
 * generation. *)
let to_string ?(null="NULL") = function
  | VFloat f  -> my_float_to_string f
  | VString s -> Printf.sprintf "%S" s
  | VBool b   -> Bool.to_string b
  | VU8 i     -> Uint8.to_string i
  | VU16 i    -> Uint16.to_string i
  | VU32 i    -> Uint32.to_string i
  | VU64 i    -> Uint64.to_string i
  | VU128 i   -> Uint128.to_string i
  | VI8 i     -> Int8.to_string i
  | VI16 i    -> Int16.to_string i
  | VI32 i    -> Int32.to_string i
  | VI64 i    -> Int64.to_string i
  | VI128 i   -> Int128.to_string i
  | VNull     -> null
  | VEth i    -> RamenEthAddr.to_string i
  | VIpv4 i   -> RamenIpv4.to_string i
  | VIpv6 i   -> RamenIpv6.to_string i
  | VCidrv4 i -> RamenIpv4.Cidr.to_string i
  | VCidrv6 i -> RamenIpv6.Cidr.to_string i

let print_custom ?null fmt v = String.print fmt (to_string ?null v)
let print fmt v = print_custom fmt v

let is_round_integer = function
  | VFloat f  -> fst(modf f) = 0.
  | VString _ | VBool _ | VNull | VEth _ | VIpv4 _ | VIpv6 _
  | VCidrv4 _ | VCidrv6 _ -> false
  | _ -> true

let value_of_string typ s =
  let open RamenTypeConverters in
  match typ with
  | TFloat -> VFloat (float_of_string s)
  | TString -> VString (string_of_string s)
  | TBool -> VBool (bool_of_string s)
  | TU8 -> VU8 (u8_of_string s)
  | TU16 -> VU16 (u16_of_string s)
  | TU32 -> VU32 (u32_of_string s)
  | TU64 -> VU64 (u64_of_string s)
  | TU128 -> VU128 (u128_of_string s)
  | TI8 -> VI8 (i8_of_string s)
  | TI16 -> VI16 (i16_of_string s)
  | TI32 -> VI32 (i32_of_string s)
  | TI64 -> VI64 (i64_of_string s)
  | TI128 -> VI128 (i128_of_string s)
  | TEth -> VEth (eth_of_string s)
  | TIpv4 -> VIpv4 (ip4_of_string s)
  | TIpv6 -> VIpv6 (ip6_of_string s)
  | TCidrv4 -> VCidrv4 (cidr4_of_string s)
  | TCidrv6 -> VCidrv6 (cidr6_of_string s)
  | TNull -> VNull
  | TNum | TAny -> assert false

let any_value_of_type = function
  | TFloat -> VFloat 0.
  | TString -> VString "hello"
  | TBool -> VBool true
  | TU8 -> VU8 Uint8.zero
  | TU16 -> VU16 Uint16.zero
  | TU32 -> VU32 Uint32.zero
  | TU64 -> VU64 Uint64.zero
  | TU128 -> VU128 Uint128.zero
  | TI8 -> VI8 Int8.zero
  | TI16 -> VI16 Int16.zero
  | TI32 -> VI32 Int32.zero
  | TI64 -> VI64 Int64.zero
  | TI128 -> VI128 Int128.zero
  | TEth -> VEth Uint48.zero
  | TIpv4 -> VIpv4 Uint32.zero
  | TIpv6 -> VIpv6 Uint128.zero
  | TCidrv4 -> VCidrv4 (Uint32.zero, 0)
  | TCidrv6 -> VCidrv6 (Uint128.zero, 0)
  | TNull -> VNull
  | TNum | TAny -> assert false

module Parser =
struct
  (*$< Parser *)
  open RamenParsing

  let narrowest_int_scalar =
    let min_i32 = Num.of_string "-2147483648"
    and max_i32 = Num.of_string "2147483647"
    and max_u32 = Num.of_string "4294967295"
    and min_i64 = Num.of_string "-9223372036854775808"
    and max_i64 = Num.of_string "9223372036854775807"
    and max_u64 = Num.of_string "18446744073709551615"
    and min_i128 = Num.of_string "-170141183460469231731687303715884105728"
    and max_i128 = Num.of_string "170141183460469231731687303715884105727"
    and max_u128 = Num.of_string "340282366920938463463374607431768211455"
    and zero = Num.zero
    in fun i ->
      let s = Num.to_string i in
      if Num.le_num min_i32 i && Num.le_num i max_i32  then VI32 (Int32.of_string s) else
      if Num.le_num zero i && Num.le_num i max_u32  then VU32 (Uint32.of_string s) else
      if Num.le_num min_i64 i && Num.le_num i max_i64  then VI64 (Int64.of_string s) else
      if Num.le_num zero i && Num.le_num i max_u64  then VU64 (Uint64.of_string s) else
      if Num.le_num min_i128 i && Num.le_num i max_i128  then VI128 (Int128.of_string s) else
      if Num.le_num zero i && Num.le_num i max_u128  then VU128 (Uint128.of_string s) else
      assert false

  let narrowest_typ_for_int n =
    narrowest_int_scalar (Num.of_int n) |> type_of

  (* TODO: Here and elsewhere, we want the location (start+length) of the
   * thing in addition to the thing *)
  let p =
    (integer >>: narrowest_int_scalar) |||
    (integer +- strinG "i8" >>: fun i -> VI8 (Int8.of_string (Num.to_string i))) |||
    (integer +- strinG "i16" >>: fun i -> VI16 (Int16.of_string (Num.to_string i))) |||
    (integer +- strinG "i32" >>: fun i -> VI32 (Int32.of_string (Num.to_string i))) |||
    (integer +- strinG "i64" >>: fun i -> VI64 (Int64.of_string (Num.to_string i))) |||
    (integer +- strinG "i128" >>: fun i -> VI128 (Int128.of_string (Num.to_string i))) |||
    (integer +- strinG "u8" >>: fun i -> VU8 (Uint8.of_string (Num.to_string i))) |||
    (integer +- strinG "u16" >>: fun i -> VU16 (Uint16.of_string (Num.to_string i))) |||
    (integer +- strinG "u32" >>: fun i -> VU32 (Uint32.of_string (Num.to_string i))) |||
    (integer +- strinG "u64" >>: fun i -> VU64 (Uint64.of_string (Num.to_string i))) |||
    (integer +- strinG "u128" >>: fun i -> VU128 (Uint128.of_string (Num.to_string i))) |||
    (floating_point >>: fun f -> VFloat f) |||
    (strinG "false" >>: fun _ -> VBool false) |||
    (strinG "true" >>: fun _ -> VBool true) |||
    (quoted_string >>: fun s -> VString s) |||
    (RamenEthAddr.Parser.p >>: fun v -> VEth v) |||
    (RamenIpv4.Parser.p >>: fun v -> VIpv4 v) |||
    (RamenIpv6.Parser.p >>: fun v -> VIpv6 v) |||
    (RamenIpv4.Cidr.Parser.p >>: fun v -> VCidrv4 v) |||
    (RamenIpv6.Cidr.Parser.p >>: fun v -> VCidrv6 v)

  (*$= p & ~printer:(test_printer print)
    (Ok (VI32 (Int32.of_int 31000), (5,[])))   (test_p p "31000")
    (Ok (VI32 (Int32.of_int 61000), (5,[])))   (test_p p "61000")
    (Ok (VFloat 3.14, (4,[])))                 (test_p p "3.14")
    (Ok (VFloat ~-.3.14, (5,[])))              (test_p p "-3.14")
    (Ok (VBool false, (5,[])))                 (test_p p "false")
    (Ok (VBool true, (4,[])))                  (test_p p "true")
    (Ok (VString "glop", (6,[])))              (test_p p "\"glop\"")
  *)

  let typ =
    (strinG "float" >>: fun () -> TFloat) |||
    (strinG "string" >>: fun () -> TString) |||
    ((strinG "bool" ||| strinG "boolean") >>: fun () -> TBool) |||
    (strinG "u8" >>: fun () -> TU8) |||
    (strinG "u16" >>: fun () -> TU16) |||
    (strinG "u32" >>: fun () -> TU32) |||
    (strinG "u64" >>: fun () -> TU64) |||
    (strinG "u128" >>: fun () -> TU128) |||
    (strinG "i8" >>: fun () -> TI8) |||
    (strinG "i16" >>: fun () -> TI16) |||
    (strinG "i32" >>: fun () -> TI32) |||
    (strinG "i64" >>: fun () -> TI64) |||
    (strinG "i128" >>: fun () -> TI128) |||
    (strinG "null" >>: fun () -> TNull) |||
    (strinG "eth" >>: fun () -> TEth) |||
    (strinG "ip4" >>: fun () -> TIpv4) |||
    (strinG "ip6" >>: fun () -> TIpv6) |||
    (strinG "cidr4" >>: fun () -> TCidrv4) |||
    (strinG "cidr6" >>: fun () -> TCidrv6)

  (*$>*)
end
