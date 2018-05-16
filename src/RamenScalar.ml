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
  | TFloat | TString | TBool | TNum | TAny
  | TU8 | TU16 | TU32 | TU64 | TU128
  | TI8 | TI16 | TI32 | TI64 | TI128
  | TEth (* 48bits unsigned integers with funny notation *)
  | TIpv4 | TIpv6 | TIp | TCidrv4 | TCidrv6 | TCidr
  [@@ppp PPP_OCaml]

let string_of_typ = function
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
  | TIp     -> "IP"
  | TCidrv4 -> "CIDRv4"
  | TCidrv6 -> "CIDRv6"
  | TCidr   -> "CIDR"

let print_typ fmt typ =
  String.print fmt (string_of_typ typ)

let can_enlarge ~from ~to_ =
  (* Beware: it looks backward but it's not. [from] is the current
   * type of the expression and [to_] is the type of its
   * operands; and we want to know if we could change the type of the global
   * expression into the type of its operands. *)
  (* On TBool and Integer conversions:
   * We want to convert easily from bool to int to ease the usage of
   * booleans in arithmetic operations (for instance, summing how many times
   * something is true). But we still want to disallow int to bool automatic
   * conversions to keep conditionals clean of integers (use an IF to convert
   * in this direction).
   * If we merely allowed a TNum to be "enlarged" into a TBool then an
   * expression using a boolean as an integer would have a boolean result,
   * which is certainly not what we want. So we want to disallow such an
   * automatic conversion. Instead, user must manually cast to some integer
   * type. *)
  let compatible_types =
    match from with
    | TNum -> [ TU8 ; TU16 ; TU32 ; TU64 ; TU128 ; TI8 ; TI16 ; TI32 ; TI64 ; TI128 ; TFloat ]
    | TU8 -> [ TU8 ; TU16 ; TU32 ; TU64 ; TU128 ; TI16 ; TI32 ; TI64 ; TI128 ; TFloat ; TNum ]
    | TU16 -> [ TU16 ; TU32 ; TU64 ; TU128 ; TI32 ; TI64 ; TI128 ; TFloat ; TNum ]
    | TU32 -> [ TU32 ; TU64 ; TU128 ; TI64 ; TI128 ; TFloat ; TNum ]
    | TU64 -> [ TU64 ; TU128 ; TI128 ; TFloat ; TNum ]
    | TU128 -> [ TU128 ; TFloat ; TNum ]
    | TI8 -> [ TI8 ; TI16 ; TI32 ; TI64 ; TI128 ; TU16 ; TU32 ; TU64 ; TU128 ; TFloat ; TNum ]
    | TI16 -> [ TI16 ; TI32 ; TI64 ; TI128 ; TU32 ; TU64 ; TU128 ; TFloat ; TNum ]
    | TI32 -> [ TI32 ; TI64 ; TI128 ; TU64 ; TU128 ; TFloat ; TNum ]
    | TI64 -> [ TI64 ; TI128 ; TU128 ; TFloat ; TNum ]
    | TI128 -> [ TI128 ; TFloat ; TNum ]
    | TBool -> [ TBool ; TU8 ; TU16 ; TU32 ; TU64 ; TU128 ; TI8 ; TI16 ; TI32 ; TI64 ; TI128 ; TFloat ; TNum ]
    (* Any specific type can be turned into its generic variant: *)
    | TIpv4 -> [ TIpv4 ; TIp ]
    | TIpv6 -> [ TIpv6 ; TIp ]
    | TCidrv4 -> [ TCidrv4 ; TCidr ]
    | TCidrv6 -> [ TCidrv6 ; TCidr ]
    | x -> [ x ] in
  List.mem to_ compatible_types

let larger_type t1 t2 =
  if can_enlarge ~from:t1 ~to_:t2 then t2 else
  if can_enlarge ~from:t2 ~to_:t1 then t1 else
  invalid_arg ("types "^ string_of_typ t1 ^" and "^ string_of_typ t2 ^
               " are not comparable")

(* Enlarge a type in search for a common ground for type combinations. *)
let enlarge_type = function
  | TU8  | TI8  -> TI16
  | TU16 | TI16 -> TI32
  | TU32 | TI32 -> TI64
  | TU64 | TI64 -> TI128
  | TIpv4   -> TIp
  | TIpv6   -> TIp
  | TCidrv4 -> TCidr
  | TCidrv6 -> TCidr
  | t -> invalid_arg ("Type "^ string_of_typ t ^" cannot be enlarged")

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
  | VIpv4 of uint32 | VIpv6 of uint128 | VIp of RamenIp.t
  | VCidrv4 of RamenIpv4.Cidr.t | VCidrv6 of RamenIpv6.Cidr.t
  | VCidr of RamenIp.Cidr.t
  [@@ppp PPP_OCaml]

let type_of = function
  | VFloat _ -> TFloat | VString _ -> TString | VBool _ -> TBool
  | VU8 _ -> TU8 | VU16 _ -> TU16 | VU32 _ -> TU32 | VU64 _ -> TU64
  | VU128 _ -> TU128 | VI8 _ -> TI8 | VI16 _ -> TI16 | VI32 _ -> TI32
  | VI64 _ -> TI64 | VI128 _ -> TI128
  | VEth _ -> TEth | VIpv4 _ -> TIpv4 | VIpv6 _ -> TIpv6 | VIp _ -> TIp
  | VCidrv4 _ -> TCidrv4 | VCidrv6 _ -> TCidrv6 | VCidr _ -> TCidr
  | VNull -> assert false

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
  | VIp i     -> RamenIp.to_string i
  | VCidrv4 i -> RamenIpv4.Cidr.to_string i
  | VCidrv6 i -> RamenIpv6.Cidr.to_string i
  | VCidr i   -> RamenIp.Cidr.to_string i

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
  | TIp -> VIp (ip_of_string s)
  | TCidrv4 -> VCidrv4 (cidr4_of_string s)
  | TCidrv6 -> VCidrv6 (cidr6_of_string s)
  | TCidr -> VCidr (cidr_of_string s)
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
  | TIpv4 | TIp -> VIpv4 Uint32.zero
  | TIpv6 -> VIpv6 Uint128.zero
  | TCidrv4 | TCidr -> VCidrv4 (Uint32.zero, 0)
  | TCidrv6 -> VCidrv6 (Uint128.zero, 0)
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
    (* Note: we do not parse an IP or a CIDR as a generic RamenIP.t etc.
     * Indeed, that would lead to an ambiguous grammar and also what's the
     * point in losing typing accuracy? IPs will be cast to generic IPs
     * as required. *)

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
    (strinG "eth" >>: fun () -> TEth) |||
    (strinG "ip4" >>: fun () -> TIpv4) |||
    (strinG "ip6" >>: fun () -> TIpv6) |||
    (strinG "ip" >>: fun () -> TIp) |||
    (strinG "cidr4" >>: fun () -> TCidrv4) |||
    (strinG "cidr6" >>: fun () -> TCidrv6) |||
    (strinG "cidr" >>: fun () -> TCidr)

  (*$>*)
end
