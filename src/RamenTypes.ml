(* This module parses scalar types and values.
 *)
open Batteries
open Stdint
open RamenHelpers

(*$inject
  open TestHelpers
*)

(*
 * Types and Values
 *)

(* TNum is not an actual type used by any value, but it's used as a default
 * type for numeric operands that can be "promoted" to any other numerical
 * type. TAny is meant to be replaced by an actual type during typing:
 * all TAny types in an expression will be changed to a specific type that's
 * large enough to accommodate all the values at hand. *)
type typ =
  | TFloat | TString | TBool | TNum | TAny
  | TU8 | TU16 | TU32 | TU64 | TU128
  | TI8 | TI16 | TI32 | TI64 | TI128
  | TEth (* 48bits unsigned integers with funny notation *)
  | TIpv4 | TIpv6 | TIp | TCidrv4 | TCidrv6 | TCidr
  (* TODO: Support for tuples/vectors of nullable types?
   *       But then, serialization is harder since we need several bits in
   *       the nullmask for constructed types. *)
  | TTuple of typ array
  | TVec of int * typ
  [@@ppp PPP_OCaml]

let rec print_typ oc = function
  | TFloat  -> String.print oc "FLOAT"
  | TString -> String.print oc "STRING"
  | TBool   -> String.print oc "BOOL"
  | TNum    -> String.print oc "ANY_NUM" (* This one not for consumption *)
  | TAny    -> String.print oc "ANY" (* same *)
  | TU8     -> String.print oc "U8"
  | TU16    -> String.print oc "U16"
  | TU32    -> String.print oc "U32"
  | TU64    -> String.print oc "U64"
  | TU128   -> String.print oc "U128"
  | TI8     -> String.print oc "I8"
  | TI16    -> String.print oc "I16"
  | TI32    -> String.print oc "I32"
  | TI64    -> String.print oc "I64"
  | TI128   -> String.print oc "I128"
  | TEth    -> String.print oc "Eth"
  | TIpv4   -> String.print oc "IPv4"
  | TIpv6   -> String.print oc "IPv6"
  | TIp     -> String.print oc "IP"
  | TCidrv4 -> String.print oc "CIDRv4"
  | TCidrv6 -> String.print oc "CIDRv6"
  | TCidr   -> String.print oc "CIDR"
  | TTuple ts -> Array.print ~first:"(" ~last:")" ~sep:";" print_typ oc ts
  | TVec (0, t) -> Printf.fprintf oc "%a[]" print_typ t
  | TVec (d, t) -> Printf.fprintf oc "%a[%d]" print_typ t d

let rec string_of_typ t = IO.to_string print_typ t

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
  | VTuple of value array
  | VVec of value array (* All values must have same type *)
  [@@ppp PPP_OCaml]

let rec type_of = function
  | VFloat _ -> TFloat | VString _ -> TString | VBool _ -> TBool
  | VU8 _ -> TU8 | VU16 _ -> TU16 | VU32 _ -> TU32 | VU64 _ -> TU64
  | VU128 _ -> TU128 | VI8 _ -> TI8 | VI16 _ -> TI16 | VI32 _ -> TI32
  | VI64 _ -> TI64 | VI128 _ -> TI128
  | VEth _ -> TEth | VIpv4 _ -> TIpv4 | VIpv6 _ -> TIpv6 | VIp _ -> TIp
  | VCidrv4 _ -> TCidrv4 | VCidrv6 _ -> TCidrv6 | VCidr _ -> TCidr
  | VTuple vs -> TTuple (Array.map type_of vs)
  | VVec vs ->
      let d = Array.length vs in
      TVec (d, if d > 0 then type_of vs.(0) else TAny)
  | VNull -> assert false

(*
 * Printers
 *)

(* Used for debug, value expansion within strings, output values in tail
 * and timeseries commands, test immediate values.., but not for code
 * generation. *)
let rec print_custom ?(null="NULL") ?(quoting=true) oc = function
  | VFloat f  -> nice_string_of_float f |> String.print oc
  | VString s -> Printf.fprintf oc (if quoting then "%S" else "%s") s
  | VBool b   -> Bool.print oc b
  | VU8 i     -> Uint8.to_string i |> String.print oc
  | VU16 i    -> Uint16.to_string i |> String.print oc
  | VU32 i    -> Uint32.to_string i |> String.print oc
  | VU64 i    -> Uint64.to_string i |> String.print oc
  | VU128 i   -> Uint128.to_string i |> String.print oc
  | VI8 i     -> Int8.to_string i |> String.print oc
  | VI16 i    -> Int16.to_string i |> String.print oc
  | VI32 i    -> Int32.to_string i |> String.print oc
  | VI64 i    -> Int64.to_string i |> String.print oc
  | VI128 i   -> Int128.to_string i |> String.print oc
  | VNull     -> String.print oc null
  | VEth i    -> RamenEthAddr.to_string i |> String.print oc
  | VIpv4 i   -> RamenIpv4.to_string i |> String.print oc
  | VIpv6 i   -> RamenIpv6.to_string i |> String.print oc
  | VIp i     -> RamenIp.to_string i |> String.print oc
  | VCidrv4 i -> RamenIpv4.Cidr.to_string i |> String.print oc
  | VCidrv6 i -> RamenIpv6.Cidr.to_string i |> String.print oc
  | VCidr i   -> RamenIp.Cidr.to_string i |> String.print oc
  | VTuple vs -> Array.print ~first:"(" ~last:")" ~sep:";" (print_custom ~null ~quoting) oc vs
  | VVec vs   -> Array.print ~first:"[" ~last:"]" ~sep:";" (print_custom ~null ~quoting) oc vs

let to_string ?null ?quoting v =
  IO.to_string (print_custom ?null ?quoting) v

(* Allow to elude ~null while currying: *)
let print oc v = print_custom oc v

(*
 * Promotions
 *)

let can_enlarge_scalar ~from ~to_ =
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

(* Note: This is based on type only. If you have the actual value, see
 * enlarge_value below. *)
let rec can_enlarge ~from ~to_ =
  match from, to_ with
  | _, TAny -> true
  | TTuple ts1, TTuple ts2 ->
      (* TTuple [||] means "any tuple", so we can "enlarge" any actual tuple
       * into "any tuple": *)
      ts2 = [||] ||
      (* Otherwise we must have the same size and each item must be
       * enlargeable: *)
      Array.length ts1 = Array.length ts2 &&
      Array.for_all2 (fun from_item to_item ->
        can_enlarge ~from:from_item ~to_:to_item
      ) ts1 ts2
  | TVec (d1, t1), TVec (d2, t2) ->
      (* Similarly, TVec (0, _) means "any vector", so we can enlarge any
       * actual vector into that: *)
      d2 = 0 ||
      (* Otherwise, vectors must have the same dimension and t1 must be
       * enlargeable to t2: *)
      d1 = d2 && can_enlarge ~from:t1 ~to_:t2
  | TTuple _, _ | _, TTuple _ | TVec _, _ | _, TVec _ ->
      false
  | _ -> can_enlarge_scalar ~from ~to_

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
  (* We also consider floats to be larger than 128 bits integers: *)
  | TU128 | TI128 -> TFloat
  | TIpv4   -> TIp
  | TIpv6   -> TIp
  | TCidrv4 -> TCidr
  | TCidrv6 -> TCidr
  | t -> invalid_arg ("Type "^ string_of_typ t ^" cannot be enlarged")

(* Important note: Sometime a value can be enlarged from one type to
 * another, while can_enlarge would have denied the promotion. That's
 * because can_enlarge base its decision on types only. *)
let rec enlarge_value t v =
  let vt = type_of v in
  if vt = t then v else
  match v, t with
  | VI8 x, _ when Int8.(compare x zero) >= 0 ->
      enlarge_value t (VU8 (Uint8.of_int8 x))
  | VI8 x, _ ->
      enlarge_value t (VI16 (Int16.of_int8 x))
  | VU8 x, _ ->
      enlarge_value t (VI16 (Int16.of_uint8 x))
  | VI16 x, _ when Int16.(compare x zero) >= 0 ->
      enlarge_value t (VU16 (Uint16.of_int16 x))
  | VI16 x, _ ->
      enlarge_value t (VI32 (Int32.of_int16 x))
  | VU16 x, _ ->
      enlarge_value t (VI32 (Int32.of_uint16 x))
  | VI32 x, _ when Int32.(compare x zero) >= 0 ->
      enlarge_value t (VU32 (Uint32.of_int32 x))
  | VI32 x, _ ->
      enlarge_value t (VI64 (Int64.of_int32 x))
  | VU32 x, _ ->
      enlarge_value t (VI64 (Int64.of_uint32 x))
  | VI64 x, _ when Int64.(compare x zero) >= 0 ->
      enlarge_value t (VU64 (Uint64.of_int64 x))
  | VI64 x, _ ->
      enlarge_value t (VI128 (Int128.of_int64 x))
  | VU64 x, _ ->
      enlarge_value t (VI128 (Int128.of_uint64 x))
  | VI128 x, _ when Int128.(compare x zero) >= 0 ->
      enlarge_value t (VU128 (Uint128.of_int128 x))
  | VI128 x, _ ->
      enlarge_value t (VFloat (Int128.to_float x))
  | VU128 x, _ ->
      enlarge_value t (VFloat (Uint128.to_float x))
  | VIpv4 x, _ ->
      enlarge_value t (VIp RamenIp.(V4 x))
  | VIpv6 x, _ ->
      enlarge_value t (VIp RamenIp.(V6 x))
  | VCidrv4 x, _ ->
      enlarge_value t (VCidr RamenIp.Cidr.(V4 x))
  | VCidrv6 x, _ ->
      enlarge_value t (VCidr RamenIp.Cidr.(V6 x))
  | VTuple vs, TTuple [||] ->
      v (* Nothing to do *)
  | VTuple vs, TTuple ts when Array.length ts = Array.length vs ->
      VTuple (Array.map2 enlarge_value ts vs)
  | VVec vs, TVec (d, t) when d = 0 || d = Array.length vs ->
      VVec (Array.map (enlarge_value t) vs)
  | _ ->
      invalid_arg ("Value "^ to_string v ^" cannot be enlarged into a "^
                   string_of_typ t)

(* From the list of operand types, return the largest type able to accomodate
 * all operands. Most of the time it will be the largest in term of "all
 * others can be enlarged to that one" but for special cases where we want
 * an even larger type; For instance, if we combine an i8 and an u8 then we
 * want the result to be an i16, or if we combine an IPv4 and an IPv6 then
 * we want the result to be an IP. *)
let rec large_enough_for t1 t2 =
  try larger_type t1 t2
  with Invalid_argument _ as e ->
    (* Try to enlarge t1 a bit further *)
    (match enlarge_type t1 with
    | exception _ -> raise e
    | t1 -> large_enough_for t1 t2)

(* From the list of operand types, return the largest type able to accommodate
 * all operands. Most of the time it will be the largest in term of "all
 * others can be enlarged to that one", but for special cases where we want
 * an even larger type; For instance, if we combine an i8 and an u8 then we
 * want the result to be an i16, or if we combine an IPv4 and an IPv6 then
 * we want the result to be an IP. *)
let largest_type = function
  | fst :: rest ->
    List.fold_left large_enough_for fst rest
  | _ -> invalid_arg "largest_type"

(*
 * Tools
 *)

let rec any_value_of_type = function
  | TString -> VString ""
  | TNum -> assert false
  | TAny -> assert false
  | TCidr | TCidrv4 -> VCidrv4 (Uint32.of_int 0, 0)
  | TCidrv6 -> VCidrv6 (Uint128.of_int 0, 0)
  | TFloat -> VFloat 0.
  | TBool -> VBool false
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
  | TIp | TIpv4 -> VIpv4 Uint32.zero
  | TIpv6 -> VIpv6 Uint128.zero
  | TTuple ts -> VTuple (Array.map any_value_of_type ts)
  | TVec (d, t) -> VVec (Array.create d (any_value_of_type t))

let is_round_integer = function
  | VFloat f  -> fst(modf f) = 0.
  | VString _ | VBool _ | VNull | VEth _ | VIpv4 _ | VIpv6 _
  | VCidrv4 _ | VCidrv6 _ | VTuple _ | VVec _ -> false
  | _ -> true

(* Garbage in / garbage out *)
let float_of_scalar =
  let open Stdint in
  function
  | VFloat x -> x
  | VBool x -> if x then 1. else 0.
  | VU8 x -> Uint8.to_float x
  | VU16 x -> Uint16.to_float x
  | VU32 x -> Uint32.to_float x
  | VU64 x -> Uint64.to_float x
  | VU128 x -> Uint128.to_float x
  | VI8 x -> Int8.to_float x
  | VI16 x -> Int16.to_float x
  | VI32 x -> Int32.to_float x
  | VI64 x -> Int64.to_float x
  | VI128 x -> Int128.to_float x
  | VEth x -> Uint48.to_float x
  | VIpv4 x -> Uint32.to_float x
  | VIpv6 x -> Uint128.to_float x
  | VIp (V4 x) -> Uint32.to_float x
  | VIp (V6 x) -> Uint128.to_float x
  | VNull | VString _ | VCidrv4 _ | VCidrv6 _ | VCidr _
  | VTuple _ | VVec _ -> 0.

let int_of_scalar =
  int_of_float % float_of_scalar

(*
 * Parsing
 *)

module Parser =
struct
  (*$< Parser *)
  open RamenParsing

  let narrowest_int_scalar =
    let min_i8 = Num.of_string "-128"
    and max_i8 = Num.of_string "127"
    and max_u8 = Num.of_string "255"
    and min_i16 = Num.of_string "-32766"
    and max_i16 = Num.of_string "32767"
    and max_u16 = Num.of_string "65535"
    and min_i32 = Num.of_string "-2147483648"
    and max_i32 = Num.of_string "2147483647"
    and max_u32 = Num.of_string "4294967295"
    and min_i64 = Num.of_string "-9223372036854775808"
    and max_i64 = Num.of_string "9223372036854775807"
    and max_u64 = Num.of_string "18446744073709551615"
    and min_i128 = Num.of_string "-170141183460469231731687303715884105728"
    and max_i128 = Num.of_string "170141183460469231731687303715884105727"
    and max_u128 = Num.of_string "340282366920938463463374607431768211455"
    and zero = Num.zero
    in fun ?(min_int_width=32) i ->
      let s = Num.to_string i in
      if min_int_width <= 8 && Num.le_num min_i8 i && Num.le_num i max_i8 then VI8 (Int8.of_string s) else
      if min_int_width <= 8 && Num.le_num zero i && Num.le_num i max_u8 then VU8 (Uint8.of_string s) else
      if min_int_width <= 16 && Num.le_num min_i16 i && Num.le_num i max_i16 then VI16 (Int16.of_string s) else
      if min_int_width <= 16 && Num.le_num zero i && Num.le_num i max_u16 then VU16 (Uint16.of_string s) else
      if min_int_width <= 32 && Num.le_num min_i32 i && Num.le_num i max_i32 then VI32 (Int32.of_string s) else
      if min_int_width <= 32 && Num.le_num zero i && Num.le_num i max_u32 then VU32 (Uint32.of_string s) else
      if min_int_width <= 64 && Num.le_num min_i64 i && Num.le_num i max_i64 then VI64 (Int64.of_string s) else
      if min_int_width <= 64 && Num.le_num zero i && Num.le_num i max_u64 then VU64 (Uint64.of_string s) else
      if min_int_width <= 128 && Num.le_num min_i128 i && Num.le_num i max_i128 then VI128 (Int128.of_string s) else
      if min_int_width <= 128 && Num.le_num zero i && Num.le_num i max_u128 then VU128 (Uint128.of_string s) else
      assert false

  let narrowest_typ_for_int ?min_int_width n =
    narrowest_int_scalar ?min_int_width (Num.of_int n) |> type_of

  (* TODO: Here and elsewhere, we want the location (start+length) of the
   * thing in addition to the thing *)
  let narrowest_int ?min_int_width =
    (integer >>: narrowest_int_scalar ?min_int_width) |||
    (integer_range ~min:(Num.of_int ~-128) ~max:(Num.of_int 127) +-
      strinG "i8" >>: fun i -> VI8 (Int8.of_string (Num.to_string i))) |||
    (integer_range ~min:(Num.of_int ~-32768) ~max:(Num.of_int 32767) +-
      strinG "i16" >>: fun i -> VI16 (Int16.of_string (Num.to_string i))) |||
    (integer_range ~min:(Num.of_int ~-2147483648) ~max:(Num.of_int 2147483647) +-
      strinG "i32" >>: fun i -> VI32 (Int32.of_string (Num.to_string i))) |||
    (integer_range ~min:(Num.of_string "-9223372036854775808") ~max:(Num.of_string "9223372036854775807") +-
      strinG "i64" >>: fun i -> VI64 (Int64.of_string (Num.to_string i))) |||
    (integer_range ~min:(Num.of_string "-170141183460469231731687303715884105728") ~max:(Num.of_string "170141183460469231731687303715884105728") +-
      strinG "i128" >>: fun i -> VI128 (Int128.of_string (Num.to_string i))) |||
    (integer_range ~min:Num.zero ~max:(Num.of_int 255) +-
      strinG "u8" >>: fun i -> VU8 (Uint8.of_string (Num.to_string i))) |||
    (integer_range ~min:Num.zero ~max:(Num.of_int 65535) +-
      strinG "u16" >>: fun i -> VU16 (Uint16.of_string (Num.to_string i))) |||
    (integer_range ~min:Num.zero ~max:(Num.of_string "4294967295") +-
      strinG "u32" >>: fun i -> VU32 (Uint32.of_string (Num.to_string i))) |||
    (integer_range ~min:Num.zero ~max:(Num.of_string "18446744073709551615") +-
      strinG "u64" >>: fun i -> VU64 (Uint64.of_string (Num.to_string i))) |||
    (integer_range ~min:Num.zero ~max:(Num.of_string "340282366920938463463374607431768211455") +-
      strinG "u128" >>: fun i -> VU128 (Uint128.of_string (Num.to_string i)))

  let all_possible_ints =
    (integer_range ~min:(Num.of_int ~-128) ~max:(Num.of_int 127) +-
      optional ~def:() (strinG "i8") >>: fun i -> VI8 (Int8.of_string (Num.to_string i))) |||
    (integer_range ~min:(Num.of_int ~-32768) ~max:(Num.of_int 32767) +-
      optional ~def:() (strinG "i16") >>: fun i -> VI16 (Int16.of_string (Num.to_string i))) |||
    (integer_range ~min:(Num.of_int ~-2147483648) ~max:(Num.of_int 2147483647) +-
      optional ~def:() (strinG "i32") >>: fun i -> VI32 (Int32.of_string (Num.to_string i))) |||
    (integer_range ~min:(Num.of_string "-9223372036854775808") ~max:(Num.of_string "9223372036854775807") +-
      optional ~def:() (strinG "i64") >>: fun i -> VI64 (Int64.of_string (Num.to_string i))) |||
    (integer_range ~min:(Num.of_string "-170141183460469231731687303715884105728") ~max:(Num.of_string "170141183460469231731687303715884105728") +-
      optional ~def:() (strinG "i128") >>: fun i -> VI128 (Int128.of_string (Num.to_string i))) |||
    (integer_range ~min:Num.zero ~max:(Num.of_int 255) +-
      optional ~def:() (strinG "u8") >>: fun i -> VU8 (Uint8.of_string (Num.to_string i))) |||
    (integer_range ~min:Num.zero ~max:(Num.of_int 65535) +-
      optional ~def:() (strinG "u16") >>: fun i -> VU16 (Uint16.of_string (Num.to_string i))) |||
    (integer_range ~min:Num.zero ~max:(Num.of_string "4294967295") +-
      optional ~def:() (strinG "u32") >>: fun i -> VU32 (Uint32.of_string (Num.to_string i))) |||
    (integer_range ~min:Num.zero ~max:(Num.of_string "18446744073709551615") +-
      optional ~def:() (strinG "u64") >>: fun i -> VU64 (Uint64.of_string (Num.to_string i))) |||
    (integer_range ~min:Num.zero ~max:(Num.of_string "340282366920938463463374607431768211455") +-
      optional ~def:() (strinG "u128") >>: fun i -> VU128 (Uint128.of_string (Num.to_string i))) |||
    (* Also in "all_possible" mode, accept an integer as a float: *)
    (decimal_number >>: fun i -> VFloat (Num.to_float i))

  (* when parsing expressions we'd rather keep literal tuples/vectors to be
   * expressions, to disambiguate the syntax. So then we only look for
   * scalars: *)
  let scalar ?min_int_width =
    (if min_int_width <> None then narrowest_int ?min_int_width
     else all_possible_ints) |||
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

  (* We do not allow to add explicit NULL values as immediate values in an
   * expression, but in some places this could be used: *)
  let null =
    strinG "null" >>: fun () -> VNull

  let tup_sep = opt_blanks -- char ';' -- opt_blanks (* TODO: consider functions as taking a single tuple *)

  (* But in general when parsing user provided values (such as in parameters
   * or test files), we want to allow any literal: *)
  (* [p_] returns all possible interpretation of a literal (ie. for an
   * integer, all it's possible sizes); while [p_ ~min_int_width] returns
   * only the smaller (that have at least the specified width). *)
  let rec p_ ?min_int_width =
    scalar ?min_int_width |||
    (* Also literals of constructed types: *)
    (tuple ?min_int_width >>: fun vs -> VTuple vs) |||
    (vector ?min_int_width >>: fun vs -> VVec vs)

  (* By default, we want only one value of at least 32 bits: *)
  and p m = p_ ~min_int_width:32 m

  (* Empty tuples and tuples of arity 1 are disallowed in order not to
   * conflict with parentheses used as grouping symbols: *)
  and tuple ?min_int_width m =
    let m = "tuple" :: m in
    (
      char '(' -- opt_blanks -+
      (repeat ~min:2 ~sep:tup_sep (p_ ?min_int_width) >>:
       fun vs -> Array.of_list vs) +-
      opt_blanks +- char ')'
    ) m

  (* Empty vectors are disallowed so we cannot not know the element type: *)
  and vector ?min_int_width m =
    let m = "vector" :: m in
    (
      char '[' -- opt_blanks -+
      (several ~sep:tup_sep (p_ ?min_int_width) >>: fun vs ->
         match largest_type (List.map type_of vs) with
         | exception Invalid_argument _ ->
            raise (Reject "Cannot find common type")
         | t -> List.map (enlarge_value t) vs |>
                Array.of_list) +-
      opt_blanks +- char ']'
    ) m

  (*$= p & ~printer:(test_printer print)
    (Ok (VI32 (Int32.of_int 31000), (5,[])))   (test_p p "31000")
    (Ok (VI32 (Int32.of_int 61000), (5,[])))   (test_p p "61000")
    (Ok (VFloat 3.14, (4,[])))                 (test_p p "3.14")
    (Ok (VFloat ~-.3.14, (5,[])))              (test_p p "-3.14")
    (Ok (VBool false, (5,[])))                 (test_p p "false")
    (Ok (VBool true, (4,[])))                  (test_p p "true")
    (Ok (VString "glop", (6,[])))              (test_p p "\"glop\"")
    (Ok (VFloat 15042., (6,[])))               (test_p p "15042.")
    (Ok (VTuple [| VFloat 3.14; VBool true |], (12,[]))) \
                                               (test_p p "(3.14; true)")
    (Ok (VVec [| VFloat 3.14; VFloat 1. |], (9,[]))) \
                                               (test_p p "[3.14; 1]")
  *)

  let rec typ m =
    let m = "type" :: m in
    (scalar_typ ||| tuple_typ ||| vector_typ) m

  and scalar_typ m =
    let m = "scalar type" :: m in
    (
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
    ) m

  and tuple_typ m =
    let m = "tuple type" :: m in
    (
      char '(' -- opt_blanks -+
        several ~sep:tup_sep typ
      +- opt_blanks +- char ')' >>: fun ts -> TTuple (Array.of_list ts)
    ) m

  and vector_typ m =
    let m = "vector type" :: m in
    (
      scalar_typ +- opt_blanks +- char '[' +- opt_blanks ++
      optional ~def:0 (pos_decimal_integer "vector dimensions") +-
      opt_blanks +- char ']' >>:
      fun (t, d) -> TVec (d, t)
    ) m

  (*$>*)
end
