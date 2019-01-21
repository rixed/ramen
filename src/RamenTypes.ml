(* This module parses scalar types and values.
 *)
open Batteries
open Stdint
open RamenHelpers

(*$inject
  open TestHelpers
  open Stdint
  open RamenHelpers
*)

(*
 * Types and Values
 *)

(* TNum is not an actual type used by any value, but it's used as a default
 * type for numeric operands that can be "promoted" to any other numerical
 * type. TAny is meant to be replaced by an actual type during typing:
 * all TAny types in an expression will be changed to a specific type that's
 * large enough to accommodate all the values at hand *)
type t =
  { structure : structure ;
    nullable : bool } [@@ppp PPP_OCaml]

and structure =
  | TEmpty (* There is no value of this type. Used to denote bad types. *)
  | TFloat | TString | TBool | TNum | TAny
  | TU8 | TU16 | TU32 | TU64 | TU128
  | TI8 | TI16 | TI32 | TI64 | TI128
  | TEth (* 48bits unsigned integers with funny notation *)
  | TIpv4 | TIpv6 | TIp | TCidrv4 | TCidrv6 | TCidr
  | TTuple of t array
  | TVec of int * t (* Fixed length arrays *)
  | TList of t (* Variable length arrays, aka lists *)
  | TRecord of (string, t) Hashtbl.t
  [@@ppp PPP_OCaml]

let is_an_int = function
  | TNum|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 -> true
  | _ -> false

(* What can be plotted (ie converted to float), and could have a unit: *)
let is_a_num x = is_an_int x || x = TFloat || x = TBool

let is_an_ip = function
  | TIpv4|TIpv6|TIp -> true
  | _ -> false

let is_scalar = function
  | TEmpty | TAny -> assert false
  | TFloat | TString | TBool | TNum
  | TU8 | TU16 | TU32 | TU64 | TU128 | TI8 | TI16 | TI32 | TI64 | TI128
  | TEth (* 48bits unsigned integers with funny notation *)
  | TIpv4 | TIpv6 | TIp | TCidrv4 | TCidrv6 | TCidr -> true
  | TTuple _ | TRecord _ | TVec _ | TList _ -> false

let is_typed t = t <> TNum && t <> TAny

let rec print_structure oc = function
  | TEmpty  -> String.print oc "INVALID"
  | TFloat  -> String.print oc "FLOAT"
  | TString -> String.print oc "STRING"
  | TBool   -> String.print oc "BOOL"
  | TNum    -> String.print oc "ANY_NUM" (* Not for consumption! *)
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
  | TTuple ts -> Array.print ~first:"(" ~last:")" ~sep:";"
                   print_typ oc ts
  | TVec (d, t) -> Printf.fprintf oc "%a[%d]" print_typ t d
  | TList t -> Printf.fprintf oc "%a[]" print_typ t
  | TRecord h -> Hashtbl.print ~first:"{" ~last:"}" ~kvsep:" " ~sep:";"
                   String.print print_typ oc h

and print_typ oc t =
  print_structure oc t.structure ;
  if t.nullable then Char.print oc '?'

let string_of_typ t = IO.to_string print_typ t
let string_of_structure t = IO.to_string print_structure t

(* stdint types are implemented as custom blocks, therefore are slower than
 * ints.  But we do not care as we merely represents code here, we do not run
 * the operators.
 * For NULL values we are doomed to loose the type information, at least for
 * constructed types, unless we always keep the type alongside the value,
 * which we do not want to (we want to erase types in serialization etc). So
 * if we are given only a NULL tuple there is no way to know its type.
 * Tough life. *)
type value =
  | VFloat of float
  | VString of string
  | VBool of bool
  | VU8 of uint8
  | VU16 of uint16
  | VU32 of uint32
  | VU64 of uint64
  | VU128 of uint128
  | VI8 of int8
  | VI16 of int16
  | VI32 of int32
  | VI64 of int64
  | VI128 of int128
  | VEth of uint48
  | VIpv4 of uint32
  | VIpv6 of uint128
  | VIp of RamenIp.t
  | VCidrv4 of RamenIpv4.Cidr.t
  | VCidrv6 of RamenIpv6.Cidr.t
  | VCidr of RamenIp.Cidr.t
  | VNull
  | VTuple of value array
  | VVec of value array (* All values must have the same type *)
  | VList of value array (* All values must have the same type *)
  | VRecord of (string, value) Hashtbl.t
  [@@ppp PPP_OCaml]

let rec structure_of =
  let sub_types_of_array vs =
    (if Array.length vs > 0 then structure_of vs.(0) else TAny),
    Array.fold_left (fun sub_nullable v ->
      if v = VNull then true else sub_nullable
    ) false vs in
  function
  | VFloat _  -> TFloat
  | VString _ -> TString
  | VBool _   -> TBool
  | VU8 _     -> TU8
  | VU16 _    -> TU16
  | VU32 _    -> TU32
  | VU64 _    -> TU64
  | VU128 _   -> TU128
  | VI8 _     -> TI8
  | VI16 _    -> TI16
  | VI32 _    -> TI32
  | VI64 _    -> TI64
  | VI128 _   -> TI128
  | VEth _    -> TEth
  | VIpv4 _   -> TIpv4
  | VIpv6 _   -> TIpv6
  | VIp _     -> TIp
  | VCidrv4 _ -> TCidrv4
  | VCidrv6 _ -> TCidrv6
  | VCidr _   -> TCidr
  | VNull     -> TAny
  (* Note regarding NULL and constructed types: We aim for non nullable
   * values, unless one of the value is actually null. *)
  | VTuple vs ->
      TTuple (Array.map (fun v ->
        { structure = structure_of v ; nullable = v = VNull }) vs)
  (* Note regarding type of zero length arrays:
   * Vec of size 0 are not super interesting, and can be of any type,
   * ideally all the time (ie if a parent exports a value of type 0-length
   * array of t1, we should be able to use it in a context requiring a
   * 0-length array of t2<>t1). *)
  | VVec vs ->
      let sub_structure, sub_nullable = sub_types_of_array vs in
      TVec (Array.length vs, { structure = sub_structure ;
                               nullable = sub_nullable })
  (* Note regarding empty lists:
   * If we receive from a parent a value from a
   * list of t1 that happens to be empty, we cannot use it in another context
   * where another list is expected of course. But empty list literal can still
   * be assigned any type. *)
  | VList vs ->
      let sub_structure, sub_nullable = sub_types_of_array vs in
      TList { structure = sub_structure ; nullable = sub_nullable }
  | VRecord h ->
      TRecord (Hashtbl.map (fun _k v ->
        { structure = structure_of v ; nullable = v = VNull }) h)

(*
 * Printers
 *)

(* Used for debug, value expansion within strings, output values in tail
 * and timeseries commands, test immediate values.., but not for code
 * generation. For this, see CodeGen_ocaml.emit_value *)
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
  | VEth i    -> RamenEthAddr.to_string i |> String.print oc
  | VIpv4 i   -> RamenIpv4.to_string i |> String.print oc
  | VIpv6 i   -> RamenIpv6.to_string i |> String.print oc
  | VIp i     -> RamenIp.to_string i |> String.print oc
  | VCidrv4 i -> RamenIpv4.Cidr.to_string i |> String.print oc
  | VCidrv6 i -> RamenIpv6.Cidr.to_string i |> String.print oc
  | VCidr i   -> RamenIp.Cidr.to_string i |> String.print oc
  | VTuple vs -> Array.print ~first:"(" ~last:")" ~sep:";"
                   (print_custom ~null ~quoting) oc vs
  (* For now, mimick the "value AS name" syntax: *)
  | VRecord h ->
      String.print oc "{" ;
      Hashtbl.iter (fun k v ->
        Printf.fprintf oc "%a AS %s"
          (print_custom ~null ~quoting) v
          (ramen_quote k)
      ) h ;
      String.print oc "}"
  | VVec vs   -> Array.print ~first:"[" ~last:"]" ~sep:";"
                   (print_custom ~null ~quoting) oc vs
  (* It is more user friendly to write lists as arrays and blur the line
   * between those for the user: *)
  | VList vs  -> Array.print ~first:"[" ~last:"]" ~sep:";"
                   (print_custom ~null ~quoting) oc vs
  | VNull     -> String.print oc null

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
    | TFloat -> [ TFloat ; TNum ]
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
  | x, TAny when x <> TEmpty -> true
  | TTuple ts1, TTuple ts2 ->
      (* TTuple [||] means "any tuple", so we can "enlarge" any actual tuple
       * into "any tuple": *)
      ts2 = [||] ||
      (* Otherwise we must have the same size and each item must be
       * enlargeable: *)
      Array.length ts1 = Array.length ts2 &&
      Array.for_all2 (fun from_item to_item ->
        can_enlarge ~from:from_item.structure ~to_:to_item.structure
      ) ts1 ts2
  | TVec (d1, t1), TVec (d2, t2) ->
      (* Similarly, TVec (0, _) means "any vector", so we can enlarge any
       * actual vector into that: *)
      d2 = 0 ||
      (* Otherwise, vectors must have the same dimension and t1 must be
       * enlargeable to t2: *)
      d1 = d2 &&
      can_enlarge ~from:t1.structure ~to_:t2.structure
  | TList t1, TList t2 ->
      can_enlarge ~from:t1.structure ~to_:t2.structure
  | TRecord h1, TRecord h2 ->
      (* We can enlarge a record into another if each field can be enlarged
       * and no more fields are present in the larger version (but fields may
       * be missing, ie the enlargement is a projection - notice that a
       * narrower record is larger in the sense of "more general" like a
       * supertype. *)
      Hashtbl.enum h1 |> Enum.for_all (fun (k, t1) ->
        match Hashtbl.find h2 k with
        | exception Not_found -> true
        | t2 -> can_enlarge ~from:t1.structure ~to_:t2.structure) &&
      (* No more fields in h2: *)
      Hashtbl.enum h2 |> Enum.for_all (fun (k, _) ->
        Hashtbl.mem h1 k)
  | TTuple _, _ | _, TTuple _
  | TVec _, _ | _, TVec _
  | TList _, _ | _, TList _
  | TRecord _, _ | _, TRecord _ ->
      false
  | _ -> can_enlarge_scalar ~from ~to_

let larger_structure s1 s2 =
  if can_enlarge ~from:s1 ~to_:s2 then s2 else
  if can_enlarge ~from:s2 ~to_:s1 then s1 else
  invalid_arg ("types "^ string_of_structure s1 ^
               " and "^ string_of_structure s2 ^
               " are not comparable")

(* Enlarge a type in search for a common ground for type combinations. *)
let enlarge_structure = function
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
  | s -> invalid_arg ("Type "^ string_of_structure s ^" cannot be enlarged")

let enlarge_type t =
  { t with structure = enlarge_structure t.structure }

(* Important note: Sometime a _value_ can be enlarged from one type to
 * another, while can_enlarge would have denied the promotion. That's
 * because can_enlarge bases its decision on types only. *)
let rec enlarge_value t v =
  let rec loop v =
    let vt = structure_of v in
    if vt = t then v else
    match v, t with
    (* Any signed integer that is >= 0 can be enlarged to the corresponding
     * untyped integer: *)
    | VI8 x, _ when Int8.(compare x zero) >= 0 ->
        loop (VU8 (Uint8.of_int8 x))
    | VI8 x, _ ->
        loop (VI16 (Int16.of_int8 x))
    | VU8 x, _ ->
        loop (VI16 (Int16.of_uint8 x))
    | VI16 x, _ when Int16.(compare x zero) >= 0 ->
        loop (VU16 (Uint16.of_int16 x))
    | VI16 x, _ ->
        loop (VI32 (Int32.of_int16 x))
    | VU16 x, _ ->
        loop (VI32 (Int32.of_uint16 x))
    | VI32 x, _ when Int32.(compare x zero) >= 0 ->
        loop (VU32 (Uint32.of_int32 x))
    | VI32 x, _ ->
        loop (VI64 (Int64.of_int32 x))
    | VU32 x, _ ->
        loop (VI64 (Int64.of_uint32 x))
    | VI64 x, _ when Int64.(compare x zero) >= 0 ->
        loop (VU64 (Uint64.of_int64 x))
    | VI64 x, _ ->
        loop (VI128 (Int128.of_int64 x))
    | VU64 x, _ ->
        loop (VI128 (Int128.of_uint64 x))
    | VI128 x, _ when Int128.(compare x zero) >= 0 ->
        loop (VU128 (Uint128.of_int128 x))
    | VI128 x, _ ->
        loop (VFloat (Int128.to_float x))
    | VU128 x, _ ->
        loop (VFloat (Uint128.to_float x))
    | VIpv4 x, _ ->
        loop (VIp RamenIp.(V4 x))
    | VIpv6 x, _ ->
        loop (VIp RamenIp.(V6 x))
    | VCidrv4 x, _ ->
        loop (VCidr RamenIp.Cidr.(V4 x))
    | VCidrv6 x, _ ->
        loop (VCidr RamenIp.Cidr.(V6 x))
    | VTuple _, TTuple [||] ->
        v (* Nothing to do *)
    | VTuple vs, TTuple ts when Array.length ts = Array.length vs ->
        (* Assume we won't try to enlarge to an unknown type: *)
        VTuple (
          Array.map2 (fun t v -> enlarge_value t.structure v) ts vs)
    | VRecord vh, TRecord th ->
        VRecord (
          Hashtbl.map (fun k t ->
            match Hashtbl.find vh k with
            | exception Not_found ->
                Printf.sprintf2
                  "value %a (%s) cannot be enlarged into %s: \
                   missing field %s"
                  print v
                  (string_of_structure (structure_of v))
                  (string_of_structure t.structure)
                  k |>
                invalid_arg
            | v -> enlarge_value t.structure v
          ) th)
    | VVec vs, TVec (d, t) when d = 0 || d = Array.length vs ->
        VVec (Array.map (enlarge_value t.structure) vs)
    | (VVec vs | VList vs), TList t ->
        VList (Array.map (enlarge_value t.structure) vs)
    | _ ->
        Printf.sprintf2 "value %a (%s) cannot be enlarged into a %s"
          print v
          (string_of_structure (structure_of v))
          (string_of_structure t) |>
        invalid_arg
  in
  (* When growing along the enlargement ladder in [loop] we go from signed to
   * unsigned of same width (if the value is positive). But we also want to
   * accept "enlarging" from unsigned to signed whenever it fits, but does this
   * only if the target type is that signed type to save the continuously
   * growing scale and avoid looping. So we test this case here first. *)
  match v, t with
  | VU8 x, TI8 when Uint8.(compare x (of_int 128)) < 0 ->
      VI8 (Int8.of_uint8 x)
  | VU16 x, TI16 when Uint16.(compare x (of_int 32768)) < 0 ->
      VI16 (Int16.of_uint16 x)
  | VU32 x, TI32 when Uint32.(compare x (of_int64 2147483648L)) < 0 ->
      VI32 (Int32.of_uint32 x)
  | VU64 x, TI64 when Uint64.(compare x (of_string "9223372036854775808")) < 0 ->
      VI64 (Int64.of_uint64 x)
  | VU128 x, TI128 when Uint128.(compare x (of_string "170141183460469231731687303715884105728")) < 0 ->
      VI128 (Int128.of_uint128 x)
  | _ -> loop v

(* Return a type that is large enough for both s1 and s2, assuming
 * s1 could be made larger itself. *)
let rec large_enough_for s1 s2 =
  try larger_structure s1 s2
  with Invalid_argument _ as e ->
    (* Try to enlarge t1 a bit further *)
    (match enlarge_structure s1 with
    | exception _ -> raise e
    | s1 -> large_enough_for s1 s2)

(* From the list of operand types, return the largest type able to accommodate
 * all operands. Most of the time it will be the largest in term of "all
 * others can be enlarged to that one", but for special cases where we want
 * an even larger type; For instance, if we combine an i8 and an u8 then we
 * want the result to be an i16, or if we combine an IPv4 and an IPv6 then
 * we want the result to be an IP. *)
let largest_structure = function
  | fst :: rest ->
    List.fold_left large_enough_for fst rest
  | _ -> invalid_arg "largest_structure"

(*
 * Tools
 *)

let rec any_value_of_type = function
  | TNum | TAny | TEmpty -> assert false
  | TString -> VString ""
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
  | TTuple ts ->
      VTuple (
        Array.map (fun t -> any_value_of_type t.structure) ts)
  | TRecord h ->
      VRecord (
        Hashtbl.map (fun _ t -> any_value_of_type t.structure) h)
  | TVec (d, t) ->
      VVec (Array.create d (any_value_of_type t.structure))
  (* Avoid loosing type info by returning a non-empty list: *)
  | TList t ->
      VList [| any_value_of_type t.structure |]

let is_round_integer = function
  | VFloat f ->
      fst (modf f) = 0.
  | VString _ | VBool _ | VNull | VEth _ | VIpv4 _ | VIpv6 _
  | VCidrv4 _ | VCidrv6 _ | VTuple _ | VVec _ | VList _ ->
      false
  | _ ->
      true

let float_of_scalar s =
  let open Stdint in
  if s = VNull then None else
  Some (
    match s with
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
    | _ -> invalid_arg "float_of_scalar")

let int_of_scalar s =
  Option.map int_of_float (float_of_scalar s)

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
      if min_int_width <= 8 && Num.le_num zero i && Num.le_num i max_u8
      then VU8 (Uint8.of_string s) else
      if min_int_width <= 8 && Num.le_num min_i8 i && Num.le_num i max_i8
      then VI8 (Int8.of_string s) else
      if min_int_width <= 16 && Num.le_num zero i && Num.le_num i max_u16
      then VU16 (Uint16.of_string s) else
      if min_int_width <= 16 && Num.le_num min_i16 i && Num.le_num i max_i16
      then VI16 (Int16.of_string s) else
      if min_int_width <= 32 && Num.le_num zero i && Num.le_num i max_u32
      then VU32 (Uint32.of_string s) else
      if min_int_width <= 32 && Num.le_num min_i32 i && Num.le_num i max_i32
      then VI32 (Int32.of_string s) else
      if min_int_width <= 64 && Num.le_num zero i && Num.le_num i max_u64
      then VU64 (Uint64.of_string s) else
      if min_int_width <= 64 && Num.le_num min_i64 i && Num.le_num i max_i64
      then VI64 (Int64.of_string s) else
      if min_int_width <= 128 && Num.le_num zero i && Num.le_num i max_u128
      then VU128 (Uint128.of_string s) else
      if min_int_width <= 128 && Num.le_num min_i128 i && Num.le_num i max_i128
      then VI128 (Int128.of_string s) else
      assert false

  let narrowest_typ_for_int ?min_int_width n =
    narrowest_int_scalar ?min_int_width (Num.of_int n) |> structure_of

  (* TODO: Here and elsewhere, we want the location (start+length) of the
   * thing in addition to the thing *)
  let narrowest_int ?min_int_width ?(all_possible=false) () =
    let ostrinG s =
      if all_possible then optional ~def:() (strinG s)
      else strinG s in
    (if all_possible then (
      (* Also in "all_possible" mode, accept an integer as a float: *)
      decimal_number ++ float_scale >>: fun (n, s) -> VFloat ((Num.to_float n) *. s)
     ) else (
      integer ++ num_scale >>: fun (n, s) ->
        let n = Num.mul n s in
        if Num.is_integer_num n then
          narrowest_int_scalar ?min_int_width n
        else
          raise (Reject "Not an integer")
     )) |||
    (integer_range ~min:(Num.of_int ~-128) ~max:(Num.of_int 127) +-
      ostrinG "i8" >>: fun i -> VI8 (Int8.of_string (Num.to_string i))) |||
    (integer_range ~min:(Num.of_int ~-32768) ~max:(Num.of_int 32767) +-
      ostrinG "i16" >>: fun i -> VI16 (Int16.of_string (Num.to_string i))) |||
    (integer_range ~min:(Num.of_int ~-2147483648) ~max:(Num.of_int 2147483647) +-
      ostrinG "i32" >>: fun i -> VI32 (Int32.of_string (Num.to_string i))) |||
    (integer_range ~min:(Num.of_string "-9223372036854775808") ~max:(Num.of_string "9223372036854775807") +-
      ostrinG "i64" >>: fun i -> VI64 (Int64.of_string (Num.to_string i))) |||
    (integer_range ~min:(Num.of_string "-170141183460469231731687303715884105728") ~max:(Num.of_string "170141183460469231731687303715884105728") +-
      ostrinG "i128" >>: fun i -> VI128 (Int128.of_string (Num.to_string i))) |||
    (integer_range ~min:Num.zero ~max:(Num.of_int 255) +-
      ostrinG "u8" >>: fun i -> VU8 (Uint8.of_string (Num.to_string i))) |||
    (integer_range ~min:Num.zero ~max:(Num.of_int 65535) +-
      ostrinG "u16" >>: fun i -> VU16 (Uint16.of_string (Num.to_string i))) |||
    (integer_range ~min:Num.zero ~max:(Num.of_string "4294967295") +-
      ostrinG "u32" >>: fun i -> VU32 (Uint32.of_string (Num.to_string i))) |||
    (integer_range ~min:Num.zero ~max:(Num.of_string "18446744073709551615") +-
      ostrinG "u64" >>: fun i -> VU64 (Uint64.of_string (Num.to_string i))) |||
    (integer_range ~min:Num.zero ~max:(Num.of_string "340282366920938463463374607431768211455") +-
      ostrinG "u128" >>: fun i -> VU128 (Uint128.of_string (Num.to_string i)))

  (*$= narrowest_int & ~printer:(test_printer print)
    (Ok (VU8 (Uint8.of_int 12), (2,[]))) \
        (test_p (narrowest_int ~min_int_width:0 ()) "12")
    (Ok (VU8 (Uint8.of_int 12), (6,[]))) \
        (test_p (narrowest_int ~min_int_width:0 ()) "12000m")
  *)

  let all_possible_ints =
    narrowest_int ~all_possible:true ()

  (* when parsing expressions we'd rather keep literal tuples/vectors to be
   * expressions, to disambiguate the syntax. So then we only look for
   * scalars: *)
  let scalar ?min_int_width =
    (if min_int_width = None then all_possible_ints
     else narrowest_int ?min_int_width ()) |||
    (floating_point ++ float_scale >>: fun (f, s) -> VFloat (f *. s)) |||
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
  let null m =
    let m = "NULL" :: m in
    (strinG "null" >>: fun () -> VNull) m

  (* TODO: consider functions as taking a single tuple *)
  let tup_sep =
    opt_blanks -- char ';' -- opt_blanks

  (* For now we stay away from the special syntax for SELECT ("as"): *)
  let kv_sep =
    blanks -- strinG "az" -- blanks

  (* But in general when parsing user provided values (such as in parameters
   * or test files), we want to allow any literal: *)
  (* [p_] returns all possible interpretation of a literal (ie. for an
   * integer, all it's possible sizes); while [p_ ~min_int_width] returns
   * only the smaller (that have at least the specified width). *)
  let rec p_ ?min_int_width =
    scalar ?min_int_width |||
    (* Also literals of constructed types: *)
    (tuple ?min_int_width >>: fun vs -> VTuple vs) |||
    (vector ?min_int_width >>: fun vs -> VVec vs) |||
    (record ?min_int_width >>: fun h -> VRecord h)
    (* Note: there is no way to enter a litteral list, as it's the same
     * representation than an array. And, given the functions that work
     * on arrays would also work on list, and that arrays are more efficient
     * (because there is no additional NULL check), there is no reason to
     * do that but to test lists. For that, we could use a conversion
     * function from arrays to lists. *)

  (* By default, we want only one value of at least 32 bits: *)
  and p m = p_ ~min_int_width:32 m

  (* Do we need literal value-only tuples/vectors/records for
   * anything since we have literal tuple/vectors/records expressions?
   * Yes, to be able to parse for instance command line arguments into
   * immediate values - but we could also instead use the expression parser,
   * and early-evaluate the result. We could then be able to do away with
   * the following parsers, and even maybe early-evaluate simple arithmetic
   * functions so that we would be allowed to enter "1+1" instead of 2
   * for instance. TODO *)

  (* Empty tuples and tuples of arity 1 are disallowed in order not to
   * conflict with parentheses used as grouping symbols: *)
  and tuple ?min_int_width m =
    let m = "tuple" :: m in
    (
      char '(' -- opt_blanks -+
      (repeat ~min:2 ~sep:tup_sep (p_ ?min_int_width) >>: Array.of_list) +-
      opt_blanks +- char ')'
    ) m

  (* Like tuples but with mandatory field names: *)
  and record ?min_int_width m =
    let m = "record" :: m in
    (
      char '(' -- opt_blanks -+
      (repeat ~min:1 ~sep:tup_sep (
        p_ ?min_int_width +- kv_sep ++ non_keyword) >>: fun l ->
          let h = Hashtbl.create 9 in
          List.iter (fun (v, k) -> Hashtbl.add h k v) l ;
          h) +-
      opt_blanks +- char ')'
    ) m

  (* Empty vectors are disallowed so we cannot not know the element type: *)
  and vector ?min_int_width m =
    let m = "vector" :: m in
    (
      char '[' -- opt_blanks -+
      (several ~sep:tup_sep (p_ ?min_int_width) >>: fun vs ->
         match largest_structure (List.map structure_of vs) with
         | exception Invalid_argument _ ->
            raise (Reject "Cannot find common type")
         | s -> List.map (enlarge_value s) vs |>
                Array.of_list) +-
      opt_blanks +- char ']'
    ) m

  (*$= p & ~printer:(test_printer print)
    (Ok (VU32 (Uint32.of_int 31000), (5,[]))) \
                                  (test_p p "31000")
    (Ok (VU32 (Uint32.of_int 61000), (5,[]))) \
                                  (test_p p "61000")
    (Ok (VFloat 3.14, (4,[])))    (test_p p "3.14")
    (Ok (VFloat ~-.3.14, (5,[]))) (test_p p "-3.14")
    (Ok (VBool false, (5,[])))    (test_p p "false")
    (Ok (VBool true, (4,[])))     (test_p p "true")
    (Ok (VString "glop", (6,[]))) (test_p p "\"glop\"")
    (Ok (VFloat 15042., (6,[])))  (test_p p "15042.")
    (Ok (VTuple [| VFloat 3.14; VBool true |], (12,[]))) \
                                  (test_p p "(3.14; true)")
    (Ok (VVec [| VFloat 3.14; VFloat 1. |], (9,[]))) \
                                  (test_p p "[3.14; 1]")
    (Ok (VVec [| VU32 Uint32.zero; VU32 Uint32.one; \
                 VU32 (Uint32.of_int 2) |], (9,[]))) \
                                  (test_p p "[0; 1; 2]")
  *)

  let rec typ m =
    let m = "type" :: m in
    (scalar_typ ||| tuple_typ ||| vector_typ ||| list_typ) m

  and scalar_typ m =
    let m = "scalar type" :: m in
    let st n structure =
      (strinG (n ^"?") >>: fun () ->
        { structure ; nullable = true }) |||
      (strinG n >>: fun () ->
        { structure ; nullable = false })
    in
    (
      (st "float" TFloat) |||
      (st "string" TString) |||
      (st "bool" TBool) |||
      (st "boolean" TBool) |||
      (st "u8" TU8) |||
      (st "u16" TU16) |||
      (st "u32" TU32) |||
      (st "u64" TU64) |||
      (st "u128" TU128) |||
      (st "i8" TI8) |||
      (st "i16" TI16) |||
      (st "i32" TI32) |||
      (st "i64" TI64) |||
      (st "i128" TI128) |||
      (st "eth" TEth) |||
      (st "ip4" TIpv4) |||
      (st "ip6" TIpv6) |||
      (st "ip" TIp) |||
      (st "cidr4" TCidrv4) |||
      (st "cidr6" TCidrv6) |||
      (st "cidr" TCidr)
    ) m

  and opt_question_mark =
    optional ~def:false (char '?' >>: fun _ -> true)

  and tuple_typ m =
    let m = "tuple type" :: m in
    (
      char '(' -- opt_blanks -+
        several ~sep:tup_sep typ
      +- opt_blanks +- char ')' ++
      opt_question_mark >>: fun (ts, n) ->
        { structure = TTuple (Array.of_list ts) ; nullable = n }
    ) m

  and vector_typ m =
    let m = "vector type" :: m in
    (
      (* FIXME: we can't do vectors of constructed types with this.
       * We need to start with the repetition count so that we can
       * then use [typ] without deadlooping. *)
      scalar_typ +- opt_blanks +- char '[' +- opt_blanks ++
      pos_decimal_integer "vector dimensions" +-
      opt_blanks +- char ']' ++
      opt_question_mark >>: fun ((t, d), n) ->
        if d <= 0 then raise (Reject "Vector must have dimension > 0") ;
        { structure = TVec (d, t) ; nullable = n }
    ) m

  and list_typ m =
    let m = "vector type" :: m in
    (
      (* FIXME: we can't do vectors of constructed types with this.
       * We need to start with the repetition count so that we can
       * then use [typ] without deadlooping. *)
      scalar_typ +- opt_blanks +- char '[' +- opt_blanks +- char ']' ++
      opt_question_mark >>: fun (t, n) ->
        { structure = TList t ; nullable = n }
    ) m

  (*$>*)
end

(* Use the above parser to get a value from a string.
 * Pass the expected type if you know it. *)
let of_string ?what ?typ s =
  let open RamenParsing in
  let what = what |? ("value of "^ String.quote s) in
  let p = allow_surrounding_blanks Parser.(
            (* Parse the string as narrowly as possible; values
             * will be enlarged later as required: *)
            p_ ~min_int_width:0 ||| null) in
  let stream = stream_of_string s in
  let m = [ what ] in
  (match p m None Parsers.no_error_correction stream |>
        to_result with
  | Bad e ->
      let err =
        IO.to_string (print_bad_result print) e in
      Result.Bad err
  | Ok (v, _) ->
      let v =
        match typ with
        | None -> v
        | Some typ ->
            if v = VNull then VNull (* TODO: check typ.nullable *) else
            enlarge_value typ.structure v in
      Result.Ok v)

(*$= of_string & ~printer:(BatIO.to_string (result_print print BatString.print))
  (BatResult.Ok (VI8 (Int8.of_int 42))) \
    (of_string ~typ:{ structure = TI8 ; nullable = false } "42")
*)

let scalar_of_int n =
  Parser.narrowest_int_scalar ~min_int_width:0 (Num.of_int n)

(*$= scalar_of_int & ~printer:(BatIO.to_string print)
  (VU8 (Uint8.of_int 42)) (scalar_of_int 42)
  (VI8 (Int8.of_int (-42))) (scalar_of_int (-42))
  (VU16 (Uint16.of_int 45678)) (scalar_of_int 45678)
*)
