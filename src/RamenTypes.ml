(* This module defines values of any dessser types.
 * FIXME: rename into RamenValues
 *)
open Batteries
open Stdint
open RamenHelpersNoLog
open DessserTypes
module DT = DessserTypes
module N = RamenName

(*$inject
  open TestHelpers
  open Stdint
  open RamenHelpersNoLog
  module DT = DessserTypes
*)

(*
 * Types and Values
 *)

(* Records are particularly important in RQL, as they convey what's usually
 * called "tuple" in DB contexts.
 * Order of record fields is significant in a few places:
 * - when defining the fields, the value of previous fields can be used;
 * - when converting to string it is more polite to the user to present
 *   the fields in the order of definition;
 * - marginally useful: in the generated code, store the fields in the
 *   definition order for simplicity.
 * But for serialization in the ringbuffers we need to order them in such
 * a way that several parents with different records can be selected from
 * and serialize a subset of their fields in the same order.
 * Therefore when serializing a record to a ringbuffer we handle the
 * fields in alphabetical order, and when unserializing we also read
 * them in alphabetical order (note that no copy is involved of course).
 * when storing records in an ORC file we are free to serialize in any
 * order, so preserve the definition order. *)
(* FIXME: to be able to deprecate RamenTuples we will need to add
 * documentation to any types, units to any scalar type and default aggr: *)
type t = mn

let eth = get_user_type "Eth"
let ipv4 = get_user_type "Ip4"
let ipv6 = get_user_type "Ip6"
let ip = get_user_type "Ip"
let cidrv4 = get_user_type "Cidr4"
let cidrv6 = get_user_type "Cidr6"
let cidr = get_user_type "Cidr"

(* What can be plotted (ie converted to float), and could have a unit: *)
let is_num x =
  is_numeric x || x = TBool

let is_ip = function
  | TUsr { name = ("Ip4"|"Ip6"|"Ip") ; _ } -> true
  | _ -> false

let rec is_scalar = function
  | TBool | TChar | TFloat | TString
  | TU8 | TU16 | TU24 | TU32 | TU40 | TU48 | TU56 | TU64 | TU128
  | TI8 | TI16 | TI24 | TI32 | TI40 | TI48 | TI56 | TI64 | TI128 ->
      true
  | TUsr { name = "Eth"|"Ip4"|"Ip6"|"Ip"|"Cidr4"|"Cidr6"|"Cidr" ; _ } ->
      true
  | TUsr { def ; _ } ->
      is_scalar def
  | TSum mns ->
      Array.for_all (fun (_, mn) -> is_scalar mn.typ) mns
  | _ ->
      false

let width_of_int = function
  | TU8 | TI8 -> 8
  | TU16 | TI16 -> 16
  | TU24 | TI24 -> 24
  | TU32 | TI32 -> 32
  | TU40 | TI40 -> 40
  | TU48 | TI48 -> 48
  | TU56 | TI56 -> 56
  | TU64 | TI64 -> 64
  | TU128 | TI128 -> 128
  | _ ->
      invalid_arg "width_of_int"

(* Given a record type, return a list of the output of [f] for each of its
 * fields. Returns [[]] if the passed type is not a record. *)
let map_fields f = function
  | TRec mns -> Array.map (fun (n, mn) -> f n mn) mns
  | _ -> [||]

(* Tells is mn has any private field, including deep in [mn]: *)
let rec has_private_fields mn =
  match mn.typ with
  | TRec mns ->
      Array.exists (fun (n, mn) ->
        N.is_private (N.field n) || has_private_fields mn
      ) mns
  | TTup mns ->
      Array.exists has_private_fields mns
  | TVec (_, mn) | TArr mn | TSet (_, mn) ->
      has_private_fields mn
  | TSum mns ->
      Array.exists (fun (_n, mn) ->
        has_private_fields mn
      ) mns
  | _ ->
      false (* Note: TUsr types are opaque *)

let fold_columns f u = function
  | { typ = TRec mns ; _ } ->
      Array.fold_left (fun u (fn, mn) ->
        f u (N.field fn) mn
      ) u mns
  | { typ = TTup mns ; _ } ->
      Array.fold_lefti (fun u i mn ->
        let fn = string_of_int i in
        f u (N.field fn) mn
      ) u mns
  | mn ->
      f u (N.field "") mn

let iter_columns f mn =
  fold_columns (fun () -> f) () mn

let num_columns = function
  | { typ = TRec mns ; _ } -> Array.length mns
  | { typ = TTup mns ; _ } -> Array.length mns
  | _ -> 1

(* FIXME: simpler: forbid records made of only private fields *)
let filter_out_private mn =
  let rec aux mn =
    match mn.typ with
    | TRec kts ->
        let kts =
          Array.filter_map (fun (k, mn') ->
            if N.(is_private (field k)) then None
            else (
              aux mn' |> Option.map (fun mn' -> k, mn')
            )
          ) kts in
        if Array.length kts = 0 then None
        else Some { mn with typ = TRec kts }
    | TTup ts ->
        let ts = Array.filter_map aux ts in
        if Array.length ts = 0 then None
        else Some { mn with typ = TTup ts }
    | TVec (d, mn') ->
        aux mn' |>
        Option.map (fun mn' -> { mn with typ = TVec (d, mn') })
    | TArr mn' ->
        aux mn' |>
        Option.map (fun mn' -> { mn with typ = TArr mn' })
    | _ ->
        Some mn
  in
  aux mn |? void

open Raql_value
type value = Raql_value.t

let rec type_of_value =
  let sub_types_of_array vs =
    if Array.length vs = 0 then
      required TUnknown  (* Can be cast into a nullable if desired *)
    else
      let typ = type_of_value vs.(0) in
      let nullable = Array.exists ((=) VNull) vs in
      maybe_nullable ~nullable typ
  and sub_types_of_map m =
    match m.(0) with
    | exception Invalid_argument _ ->
        invalid_arg "empty map"
    | k, v ->
        let nullable = Array.exists (((=) VNull) % fst) m in
        maybe_nullable ~nullable (type_of_value k),
        let nullable = Array.exists (((=) VNull) % snd) m in
        maybe_nullable ~nullable (type_of_value v)
  in
  function
  | VUnit     -> TVoid
  | VFloat _  -> TFloat
  | VString _ -> TString
  | VBool _   -> TBool
  | VChar _   -> TChar
  | VU8 _     -> TU8
  | VU16 _    -> TU16
  | VU24 _    -> TU24
  | VU32 _    -> TU32
  | VU40 _    -> TU40
  | VU48 _    -> TU48
  | VU56 _    -> TU56
  | VU64 _    -> TU64
  | VU128 _   -> TU128
  | VI8 _     -> TI8
  | VI16 _    -> TI16
  | VI24 _    -> TI24
  | VI32 _    -> TI32
  | VI40 _    -> TI40
  | VI48 _    -> TI48
  | VI56 _    -> TI56
  | VI64 _    -> TI64
  | VI128 _   -> TI128
  | VEth _    -> eth
  | VIpv4 _   -> ipv4
  | VIpv6 _   -> ipv6
  | VIp _     -> ip
  | VCidrv4 _ -> cidrv4
  | VCidrv6 _ -> cidrv6
  | VCidr _   -> cidr
  | VNull     -> TUnknown
  (* Note regarding NULL and constructed types: We aim for non nullable
   * values, unless one of the value is actually null. *)
  | VTup vs ->
      TTup (Array.map (fun v -> required (type_of_value v)) vs)
  | VRec kvs ->
      TRec (Array.map (fun (k, v) -> k, required (type_of_value v)) kvs)
  (* Note regarding type of zero length arrays:
   * TVec of size 0 are not super interesting, and can be of any type,
   * ideally all the time (ie if a parent exports a value of type 0-length
   * array of t1, we should be able to use it in a context requiring a
   * 0-length array of t2<>t1). *)
  | VVec vs ->
      TVec (Array.length vs, sub_types_of_array vs)
  (* Note regarding empty lists:
   * If we receive from a parent a value from a
   * list of t1 that happens to be empty, we cannot use it in another context
   * where another list is expected of course. But empty list literal can still
   * be assigned any type. *)
  | VArr vs ->
      TArr (sub_types_of_array vs)
  | VMap m ->
      let k, v = sub_types_of_map m in
      TMap (k, v)

(*
 * Printers
 *)

(* Used for debug, value expansion within strings, output values in tail
 * and timeseries commands, test immediate values.., but not for code
 * generation. For this, see CodeGen_ocaml.emit_value *)
(* Some use cases prefer shorter representation of values, such as
 * Graphite legends and raw tail output. In that case they turn
 * [quoting] off. *)
let rec print_custom ?(null="NULL") ?(quoting=true) ?(hex_floats=false) oc =
  function
  | VUnit     -> String.print oc "()"
  | VFloat f  ->
      (if hex_floats then hex_string_of_float else nice_string_of_float) f |>
      String.print oc
  | VString s -> Printf.fprintf oc (if quoting then "%S" else "%s") s
  | VBool b   -> Bool.print oc b
  | VChar c   -> RamenParsing.print_char oc c
  | VU8 i     -> Uint8.to_string i |> String.print oc
  | VU16 i    -> Uint16.to_string i |> String.print oc
  | VU24 i    -> Uint24.to_string i |> String.print oc
  | VU32 i    -> Uint32.to_string i |> String.print oc
  | VU40 i    -> Uint40.to_string i |> String.print oc
  | VU48 i    -> Uint48.to_string i |> String.print oc
  | VU56 i    -> Uint56.to_string i |> String.print oc
  | VU64 i    -> Uint64.to_string i |> String.print oc
  | VU128 i   -> Uint128.to_string i |> String.print oc
  | VI8 i     -> Int8.to_string i |> String.print oc
  | VI16 i    -> Int16.to_string i |> String.print oc
  | VI24 i    -> Int24.to_string i |> String.print oc
  | VI32 i    -> Int32.to_string i |> String.print oc
  | VI40 i    -> Int40.to_string i |> String.print oc
  | VI48 i    -> Int48.to_string i |> String.print oc
  | VI56 i    -> Int56.to_string i |> String.print oc
  | VI64 i    -> Int64.to_string i |> String.print oc
  | VI128 i   -> Int128.to_string i |> String.print oc
  | VEth i    -> RamenEthAddr.to_string i |> String.print oc
  | VIpv4 i   -> RamenIpv4.to_string i |> String.print oc
  | VIpv6 i   -> RamenIpv6.to_string i |> String.print oc
  | VIp i     -> RamenIp.to_string i |> String.print oc
  | VCidrv4 i -> RamenIpv4.Cidr.to_string i |> String.print oc
  | VCidrv6 i -> RamenIpv6.Cidr.to_string i |> String.print oc
  | VCidr i   -> RamenIp.Cidr.to_string i |> String.print oc
  | VTup vs -> Array.print ~first:"(" ~last:")" ~sep:";"
                   (print_custom ~null ~quoting ~hex_floats) oc vs
  (* For now, mimic the "value AS name" syntax: *)
  | VRec kvs ->
      Array.print ~first:"{" ~last:"}" ~sep:"," (fun oc (k, v) ->
        Printf.fprintf oc "%a AS %s"
          (print_custom ~null ~quoting ~hex_floats) v
          (ramen_quote k)
      ) oc kvs
  | VVec vs   -> Array.print ~first:"[" ~last:"]" ~sep:";"
                   (print_custom ~null ~quoting ~hex_floats) oc vs
  (* It is more user friendly to write lists as arrays and blur the line
   * between those for the user: *)
  | VArr vs  -> Array.print ~first:"[" ~last:"]" ~sep:";"
                   (print_custom ~null ~quoting ~hex_floats) oc vs
  (* Print maps as association lists: *)
  | VMap m ->
      Array.print ~first:"{" ~last:"}" ~sep:"," (fun oc (k, v) ->
        Printf.fprintf oc "%a => %a"
          (print_custom ~null ~quoting ~hex_floats) k
          (print_custom ~null ~quoting ~hex_floats) v
      ) oc m
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
  (* On Bool and Integer conversions:
   * We want to convert easily from bool to int to ease the usage of
   * booleans in arithmetic operations (for instance, summing how many times
   * something is true). But we still want to disallow int to bool automatic
   * conversions to keep conditionals clean of integers (use an IF to convert
   * in this direction).
   * If we merely allowed an int to be "enlarged" into a Bool then an
   * expression using a boolean as an integer would have a boolean result,
   * which is certainly not what we want. So we want to disallow such an
   * automatic conversion. Instead, user must manually cast to some integer
   * type. *)
  let compatible_types =
    match from with
    | TU8 ->
        [ TU8 ; TU16 ; TU24 ; TU32 ; TU40 ; TU48 ; TU56 ; TU64 ; TU128 ;
          TI16 ; TI24 ; TI32 ; TI40 ; TI48 ; TI56 ; TI64 ; TI128 ; TFloat ]
    | TU16 ->
        [ TU16 ; TU24 ; TU32 ; TU40 ; TU48 ; TU56 ; TU64 ; TU128 ;
          TI24 ; TI32 ; TI40 ; TI48 ; TI56 ; TI64 ; TI128 ; TFloat ]
    | TU24 ->
        [ TU24 ; TU32 ; TU40 ; TU48 ; TU56 ; TU64 ; TU128 ;
          TI32 ; TI40 ; TI48 ; TI56 ; TI64 ; TI128 ; TFloat ]
    | TU32 ->
        [ TU32 ; TU40 ; TU48 ; TU56 ; TU64 ; TU128 ;
          TI40 ; TI48 ; TI56 ; TI64 ; TI128 ; TFloat ]
    | TU40 ->
        [ TU40 ; TU48 ; TU56 ; TU64 ; TU128 ;
          TI48 ; TI56 ; TI64 ; TI128 ; TFloat ]
    | TU48 ->
        [ TU48 ; TU56 ; TU64 ; TU128 ;
          TI56 ; TI64 ; TI128 ; TFloat ]
    | TU56 ->
        [ TU56 ; TU64 ; TU128 ;
          TI64 ; TI128 ; TFloat ]
    | TU64 ->
        [ TU64 ; TU128 ;
          TI128 ; TFloat ]
    | TU128 ->
        [ TU128 ;
          TFloat ]
    | TI8 ->
        [ TI8 ; TI16 ; TI24 ; TI32 ; TI40 ; TI48 ; TI56 ; TI64 ; TI128 ;
          TU16 ; TU24 ; TU32 ; TU40 ; TU48 ; TU56 ; TU64 ; TU128 ; TFloat ]
    | TI16 ->
        [ TI16 ; TI24 ; TI32 ; TI40 ; TI48 ; TI56 ; TI64 ; TI128 ;
          TU24 ; TU32 ; TU40 ; TU48 ; TU56 ; TU64 ; TU128 ; TFloat ]
    | TI24 ->
        [ TI24 ; TI32 ; TI40 ; TI48 ; TI56 ; TI64 ; TI128 ;
          TU32 ; TU40 ; TU48 ; TU56 ; TU64 ; TU128 ; TFloat ]
    | TI32 ->
        [ TI32 ; TI40 ; TI48 ; TI56 ; TI64 ; TI128 ;
          TU40 ; TU48 ; TU56 ; TU64 ; TU128 ; TFloat ]
    | TI40 ->
        [ TI40 ; TI48 ; TI56 ; TI64 ; TI128 ;
          TU48 ; TU56 ; TU64 ; TU128 ; TFloat ]
    | TI48 ->
        [ TI48 ; TI56 ; TI64 ; TI128 ;
          TU56 ; TU64 ; TU128 ; TFloat ]
    | TI56 ->
        [ TI56 ; TI64 ; TI128 ;
          TU64 ; TU128 ; TFloat ]
    | TI64 ->
        [ TI64 ; TI128 ;
          TU128 ; TFloat ]
    | TI128 ->
        [ TI128 ;
           TFloat ]
    | TFloat ->
        [ TFloat ]
    | TBool ->
        [ TBool ;
          TU8 ; TU16 ; TU24 ; TU32 ; TU40 ; TU48 ; TU56 ; TU64 ; TU128 ;
          TI8 ; TI16 ; TI24 ; TI32 ; TI40 ; TI48 ; TI56 ; TI64 ; TI128 ;
          TFloat ]
    (* Any specific type can be turned into its generic variant: *)
    | TUsr { name = "Ip4" ; _ } ->
        [ ipv4 ; ip ]
    | TUsr { name = "Ip6" ; _ } ->
        [ ipv6 ; ip ]
    | TUsr { name = "Cidr4" ; _ } ->
        [ cidrv4 ; cidr ]
    | TUsr { name = "Cidr6" ; _ } ->
        [ cidrv6 ; cidr ]
    | x ->
        [ x ] in
  List.mem to_ compatible_types

(* Note: This is based on type only. If you have the actual value, see
 * enlarge_value below. *)
let rec can_enlarge ~from ~to_ =
  if from = TUnknown then invalid_arg "Cannot enlarge from unknown type" ;
  if to_ = TUnknown then invalid_arg "Cannot enlarge to unknown type" ;
  match from, to_ with
  | TLst _, _ | _, TLst _ ->
      assert false (* unused (TArr are used for RaQL lists) *)
  | TTup ts1, TTup ts2 ->
      (* TTup [||] means "any tuple", so we can "enlarge" any actual tuple
       * into "any tuple": *)
      (* FIXME: what is using this special case? *)
      ts2 = [||] ||
      (* Otherwise we must have the same size and each item must be
       * enlargeable: *)
      Array.length ts1 = Array.length ts2 &&
      Array.for_all2 (fun from to_ ->
        can_enlarge_maybe_nullable ~from ~to_
      ) ts1 ts2
  | TRec kts, TRec kvs ->
      (* We can enlarge a record into another if each field can be enlarged
       * and no more fields are present in the larger version (but fields may
       * be missing, ie the enlargement is a projection - notice that a
       * narrower record is larger in the sense of "more general" like a
       * supertype. *)
      Array.for_all (fun (k1, t1) ->
        match Array.find (fun (k2, _) -> k1 = k2) kvs with
        | exception Not_found -> true
        | _, t2 -> can_enlarge_maybe_nullable ~from:t1 ~to_:t2
      ) kts &&
      (* No more fields in h2: *)
      Array.for_all (fun (k2, _) ->
        Array.exists (fun (k1, _) -> k1 = k2) kts
      ) kvs
  | TVec (d1, t1), TVec (d2, t2) ->
      (* Similarly, TVec (0, _) means "any vector", so we can enlarge any
       * actual vector into that: *)
      (* FIXME: what is using this special case? *)
      d2 = 0 ||
      (* Otherwise, vectors must have the same dimension and t1 must be
       * enlargeable to t2: *)
      d1 = d2 &&
      can_enlarge_maybe_nullable ~from:t1 ~to_:t2
  | TArr t1, TArr t2 ->
      can_enlarge_maybe_nullable ~from:t1 ~to_:t2
  (* For convenience, make it possible to enlarge a scalar into a one
   * element vector or tuple: *)
  | t1, TVec ((0|1), t2)
  | t1, TTup [| t2 |] ->
      can_enlarge_maybe_nullable (required t1) t2
  (* Other non scalar conversions are not possible: *)
  | TTup _, _ | _, TTup _
  | TVec _, _ | _, TVec _
  | TArr _, _ | _, TArr _
  | TRec _, _ | _, TRec _
  | TMap _, _ | _, TMap _ ->
      false
  | _ ->
      can_enlarge_scalar ~from ~to_

and can_enlarge_maybe_nullable ~from ~to_ =
  (not from.nullable || to_.nullable) &&
  can_enlarge ~from:from.typ ~to_:to_.typ

(* Note: TUnknown is supposed to be _smaller_ than any type, as we want to allow
 * a literal NULL (of type TUnknown) to be enlarged into any other type. *)
let larger_type s1 s2 =
  if s1 = TUnknown then s2 else
  if s2 = TUnknown then s1 else
  if can_enlarge ~from:s1 ~to_:s2 then s2 else
  if can_enlarge ~from:s2 ~to_:s1 then s1 else
  invalid_arg ("types "^ DT.to_string s1 ^
               " and "^ DT.to_string s2 ^
               " are not comparable")

(* Enlarge a type in search for a common ground for type combinations. *)
let enlarge_value_type = function
  | TU8  | TI8 -> TI16
  | TU16 | TI16 -> TI24
  | TU24 | TI24 -> TI32
  | TU32 | TI32 -> TI40
  | TU40 | TI40 -> TI48
  | TU48 | TI48 -> TI56
  | TU56 | TI56 -> TI64
  | TU64 | TI64 -> TI128
  (* We also consider floats to be larger than 128 bits integers: *)
  | TU128 | TI128 -> TFloat
  | TUsr { name = "Ip4" ; _ } -> ip
  | TUsr { name = "Ip6" ; _ } -> ip
  | TUsr { name = "Cidr4" ; _ } -> cidr
  | TUsr { name = "Cidr6" ; _ } -> cidr
  | s -> invalid_arg ("Type "^ DT.to_string s ^" cannot be enlarged")

let enlarge_type mn =
  { mn with
    typ = enlarge_value_type mn.typ }

(* Important note: Sometime a _value_ can be enlarged from one type to
 * another, while can_enlarge would have denied the promotion. That's
 * because can_enlarge bases its decision on types only. *)
let rec enlarge_value t v =
  let rec loop v =
    let vt = type_of_value v in
    if vt = t then v else
    match v, t with
    (* Null can be enlarged into whatever: *)
    | VNull, _ -> VNull
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
        loop (VI24 (Int24.of_int16 x))
    | VU16 x, _ ->
        loop (VI24 (Int24.of_uint16 x))
    | VI24 x, _ when Int24.(compare x zero) >= 0 ->
        loop (VU24 (Uint24.of_int24 x))
    | VI24 x, _ ->
        loop (VI32 (Int32.of_int24 x))
    | VU24 x, _ ->
        loop (VI32 (Int32.of_uint24 x))
    | VI32 x, _ when Int32.(compare x zero) >= 0 ->
        loop (VU32 (Uint32.of_int32 x))
    | VI32 x, _ ->
        loop (VI40 (Int40.of_int32 x))
    | VU32 x, _ ->
        loop (VI40 (Int40.of_uint32 x))
    | VI40 x, _ when Int40.(compare x zero) >= 0 ->
        loop (VU40 (Uint40.of_int40 x))
    | VI40 x, _ ->
        loop (VI48 (Int48.of_int40 x))
    | VU40 x, _ ->
        loop (VI48 (Int48.of_uint40 x))
    | VI48 x, _ when Int48.(compare x zero) >= 0 ->
        loop (VU48 (Uint48.of_int48 x))
    | VI48 x, _ ->
        loop (VI56 (Int56.of_int48 x))
    | VU48 x, _ ->
        loop (VI56 (Int56.of_uint48 x))
    | VI56 x, _ when Int56.(compare x zero) >= 0 ->
        loop (VU56 (Uint56.of_int56 x))
    | VI56 x, _ ->
        loop (VI64 (Int64.of_int56 x))
    | VU56 x, _ ->
        loop (VI64 (Int64.of_uint56 x))
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
    | VTup _, TTup [||] ->
        v (* Nothing to do *)
    | VTup vs, TTup ts when Array.length ts = Array.length vs ->
        (* Assume we won't try to enlarge to an unknown type: *)
        VTup (
          Array.map2 (fun t v -> enlarge_value t.typ v) ts vs)
    | VRec kvs, TRec kts ->
        VRec (
          Array.map (fun (k, t) ->
            match Array.find (fun (k', _) -> k = k') kvs with
            | exception Not_found ->
                Printf.sprintf2
                  "value %a (%s) cannot be enlarged into %s: \
                   missing field %s"
                  print v
                  (DT.to_string (type_of_value v))
                  (DT.to_string t.typ)
                  k |>
                invalid_arg
            | _, v -> k, enlarge_value t.typ v
          ) kts)
    | VVec vs, TVec (d, t) when d = 0 || d = Array.length vs ->
        VVec (Array.map (enlarge_value t.typ) vs)
    | (VVec vs | VArr vs), TArr t ->
        VArr (Array.map (enlarge_value t.typ) vs)
    | _ ->
        Printf.sprintf2 "value %a (%s) cannot be enlarged into a %s"
          print v
          (DT.to_string (type_of_value v))
          (DT.to_string t) |>
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
  | VU24 x, TI24 when Uint24.(compare x (of_int 8388608)) < 0 ->
      VI24 (Int24.of_uint24 x)
  | VU32 x, TI32 when Uint32.(compare x (of_int64 2147483648L)) < 0 ->
      VI32 (Int32.of_uint32 x)
  | VU40 x, TI40 when Uint40.(compare x (of_int64 549755813888L)) < 0 ->
      VI40 (Int40.of_uint40 x)
  | VU48 x, TI48 when Uint48.(compare x (of_int64 140737488355328L)) < 0 ->
      VI48 (Int48.of_uint48 x)
  | VU56 x, TI56 when Uint56.(compare x (of_int64 36028797018963968L)) < 0 ->
      VI56 (Int56.of_uint56 x)
  | VU64 x, TI64 when Uint64.(compare x (of_string "9223372036854775808")) < 0 ->
      VI64 (Int64.of_uint64 x)
  | VU128 x, TI128 when Uint128.(compare x (of_string "170141183460469231731687303715884105728")) < 0 ->
      VI128 (Int128.of_uint128 x)
  (* For convenience, make it possible to enlarge a scalar into a one element
   * vector or tuple: *)
  | v, TVec ((0|1), t)
    when can_enlarge ~from:(type_of_value v) ~to_:t.typ ->
      VVec [| enlarge_value t.typ v |]
  | v, TTup [| t |]
    when can_enlarge ~from:(type_of_value v) ~to_:t.typ ->
      VTup [| enlarge_value t.typ v |]
  | _ -> loop v

(* Return a type that is large enough for both s1 and s2, assuming
 * s1 could be made larger itself. *)
let rec large_enough_for s1 s2 =
  try larger_type s1 s2
  with Invalid_argument _ as e ->
    (* Try to enlarge t1 a bit further *)
    (match enlarge_value_type s1 with
    | exception _ -> raise e
    | s1 -> large_enough_for s1 s2)

(* From the list of operand types, return the largest type able to accommodate
 * all operands. Most of the time it will be the largest in term of "all
 * others can be enlarged to that one", but for special cases where we want
 * an even larger type; For instance, if we combine an i8 and an u8 then we
 * want the result to be an i16, or if we combine an IPv4 and an IPv6 then
 * we want the result to be an IP. *)
let largest_type = function
  | fst :: rest ->
      List.fold_left large_enough_for fst rest
  | _ ->
      invalid_arg "largest_type"

(* Unlike [enlarge_value t v], this can also reduce the type of [v] to match [t],
 * as long as the value allows it. *)
let rec to_type t v =
  try enlarge_value t v
  with Invalid_argument _ ->
    (* Try to reduce the type of the value then: *)
    (match v with
    | VU8 n when Uint8.(compare n (of_int 128) < 0) ->
        to_type t (VI8 (Int8.of_uint8 n))
    | VI16 n when Int16.(compare n (of_int 256) < 0 && compare n zero >= 0) ->
        to_type t (VU8 (Uint8.of_int16 n))
    | VI16 n when Int16.(compare n (of_int 128) < 0 && compare n (of_int ~-127) >= 0) ->
        to_type t (VI8 (Int8.of_int16 n))
    | VU16 n when Uint16.(compare n (of_int 32768) < 0) ->
        to_type t (VI16 (Int16.of_uint16 n))
    | VI24 n when Int24.(compare n (of_int 65536) < 0 && compare n zero >= 0) ->
        to_type t (VU16 (Uint16.of_int24 n))
    | VI24 n when Int24.(compare n (of_int 32768) < 0 && compare n (of_int ~-32768) >= 0) ->
        to_type t (VI16 (Int16.of_int24 n))
    | VU24 n when Uint24.(compare n (of_int 8388608) < 0) ->
        to_type t (VI24 (Int24.of_uint24 n))
    | VI32 n when Int32.(compare n (of_int 16777216) < 0 && compare n zero >= 0) ->
        to_type t (VU24 (Uint24.of_int32 n))
    | VI32 n when Int32.(compare n (of_int 8388608) < 0 && compare n (of_int ~-8388608) >= 0) ->
        to_type t (VI24 (Int24.of_int32 n))
    | VU32 n when Uint32.(compare n (of_string "2147483648") < 0) ->
        to_type t (VI32 (Int32.of_uint32 n))
    | VI40 n when Int40.(compare n (of_string "4294967296") < 0 && compare n zero >= 0) ->
        to_type t (VU32 (Uint32.of_int40 n))
    | VI40 n when Int40.(compare n (of_string "2147483648") < 0 && compare n (of_string "-2147483648") >= 0) ->
        to_type t (VI32 (Int32.of_int40 n))
    | VU40 n when Uint40.(compare n (of_string "549755813888") < 0) ->
        to_type t (VI40 (Int40.of_uint40 n))
    | VI48 n when Int48.(compare n (of_string "1099511627776") < 0 && compare n zero >= 0) ->
        to_type t (VU40 (Uint40.of_int48 n))
    | VI48 n when Int48.(compare n (of_string "549755813888") < 0 && compare n (of_string "-549755813888") >= 0) ->
        to_type t (VI40 (Int40.of_int48 n))
    | VU48 n when Uint48.(compare n (of_string "140737488355328") < 0) ->
        to_type t (VI48 (Int48.of_uint48 n))
    | VI56 n when Int56.(compare n (of_string "281474976710656") < 0 && compare n zero >= 0) ->
        to_type t (VU48 (Uint48.of_int56 n))
    | VI56 n when Int56.(compare n (of_string "140737488355328") < 0 && compare n (of_string "-140737488355328") >= 0) ->
        to_type t (VI48 (Int48.of_int56 n))
    | VU56 n when Uint56.(compare n (of_string "36028797018963968") < 0) ->
        to_type t (VI56 (Int56.of_uint56 n))
    | VI64 n when Int64.(compare n (of_string "72057594037927936") < 0 && compare n zero >= 0) ->
        to_type t (VU56 (Uint56.of_int64 n))
    | VI64 n when Int64.(compare n (of_string "36028797018963968") < 0 && compare n (of_string "-36028797018963968") >= 0) ->
        to_type t (VI56 (Int56.of_int64 n))
    | VU64 n when Uint64.(compare n (of_string "9223372036854775808") < 0) ->
        to_type t (VI64 (Int64.of_uint64 n))
    | VI128 n when Int128.(compare n (of_string "18446744073709551616") < 0 && compare n zero >= 0) ->
        to_type t (VU64 (Uint64.of_int128 n))
    | VI128 n when Int128.(compare n (of_string "9223372036854775808") < 0 && compare n (of_string "-9223372036854775808") >= 0) ->
        to_type t (VI64 (Int64.of_int128 n))
    | VU128 n when Uint128.(compare n (of_string "170141183460469231731687303715884105728") < 0) ->
        to_type t (VI128 (Int128.of_uint128 n))
    | VFloat n when float_is_integer n && n < 340282366920938463463374607431768211456. && n >= 0. ->
        to_type t (VU128 (Uint128.of_float n))
    | VFloat n when float_is_integer n && n < 170141183460469231731687303715884105728. && n >= ~-.170141183460469231731687303715884105728. ->
        to_type t (VI128 (Int128.of_float n))
    | _ -> v)

(*$= to_type & ~printer:to_string
  (VIp RamenIp.(V4 (Uint32.of_int 1234))) \
    (to_type ip (VIpv4 (Uint32.of_int 1234)))
*)

(*
 * Tools
 *)

(* Returns a good default value, but avoids VNull as the caller intend is
 * often to keep track of the type. *)
let rec any_value_of_type ?avoid_null = function
  | TVoid -> VUnit
  | TString -> VString ""
  | TFloat -> VFloat 0.
  | TBool -> VBool false
  | TChar -> VChar '\x00'
  | TU8 -> VU8 Uint8.zero
  | TU16 -> VU16 Uint16.zero
  | TU24 -> VU24 Uint24.zero
  | TU32 -> VU32 Uint32.zero
  | TU40 -> VU40 Uint40.zero
  | TU48 -> VU48 Uint48.zero
  | TU56 -> VU56 Uint56.zero
  | TU64 -> VU64 Uint64.zero
  | TU128 -> VU128 Uint128.zero
  | TI8 -> VI8 Int8.zero
  | TI16 -> VI16 Int16.zero
  | TI24 -> VI24 Int24.zero
  | TI32 -> VI32 Int32.zero
  | TI40 -> VI40 Int40.zero
  | TI48 -> VI48 Int48.zero
  | TI56 -> VI56 Int56.zero
  | TI64 -> VI64 Int64.zero
  | TI128 -> VI128 Int128.zero
  | TUsr { name = "Eth" ; _ } -> VEth Uint48.zero
  | TUsr { name = "Ip4" ; _ } -> VIpv4 Uint32.zero
  | TUsr { name = "Ip6" ; _ } -> VIpv6 Uint128.zero
  | TUsr { name = "Ip" ; _ } -> VIp RamenIp.(V4 (Uint32.zero))
  | TUsr { name = "Cidr4" ; _ } -> VCidrv4 (Uint32.zero, Uint8.zero)
  | TUsr { name = "Cidr6" ; _ } -> VCidrv6 (Uint128.zero, Uint8.zero)
  | TUsr { name = "Cidr" ; _ } -> VCidr RamenIp.Cidr.(V4 (Uint32.zero, Uint8.zero))
  | TUsr d ->
      invalid_arg ("no known value of unknown user type "^ d.name)
  | TTup ts ->
      VTup (
        Array.map (fun t -> any_value_of_maybe_nullable ?avoid_null t) ts)
  | TRec kts ->
      VRec (
        Array.map (fun (k, t) -> k, any_value_of_maybe_nullable ?avoid_null t) kts)
  | TVec (d, t) ->
      VVec (Array.create d (any_value_of_maybe_nullable ?avoid_null t))
  (* Avoid loosing type info by returning a non-empty list: *)
  | TArr t ->
      VArr [| any_value_of_maybe_nullable ?avoid_null t |]
  | TMap (k, v) -> (* Represent maps as association lists: *)
      VMap [| any_value_of_maybe_nullable ?avoid_null k,
              any_value_of_maybe_nullable ?avoid_null v |]
  | t ->
      invalid_arg ("any_value_of_type:" ^ (DT.to_string t))

and any_value_of_maybe_nullable ?(avoid_null=false) t =
  if t.nullable && not avoid_null then VNull
  else any_value_of_type ~avoid_null t.typ

let is_round_integer = function
  | VFloat f ->
      fst (modf f) = 0.
  | VU8 _ | VU16 _ | VU24 _ | VU32 _ | VU40 _ | VU48 _ | VU56 _ | VU64 _ | VU128 _
  | VI8 _ | VI16 _ | VI24 _ | VI32 _ | VI40 _ | VI48 _ | VI56 _ | VI64 _ | VI128 _ ->
      true
  | _ ->
      false

let float_of_scalar s =
  let open Stdint in
  if s = VNull then None else
  Some (
    match s with
    | VFloat x -> x
    | VBool x -> if x then 1. else 0.
    | VU8 x -> Uint8.to_float x
    | VU16 x -> Uint16.to_float x
    | VU24 x -> Uint24.to_float x
    | VU32 x -> Uint32.to_float x
    | VU40 x -> Uint40.to_float x
    | VU48 x -> Uint48.to_float x
    | VU56 x -> Uint56.to_float x
    | VU64 x -> Uint64.to_float x
    | VU128 x -> Uint128.to_float x
    | VI8 x -> Int8.to_float x
    | VI16 x -> Int16.to_float x
    | VI24 x -> Int24.to_float x
    | VI32 x -> Int32.to_float x
    | VI40 x -> Int40.to_float x
    | VI48 x -> Int48.to_float x
    | VI56 x -> Int56.to_float x
    | VI64 x -> Int64.to_float x
    | VI128 x -> Int128.to_float x
    | VEth x -> Uint48.to_float x
    | VIpv4 x -> Uint32.to_float x
    | VIpv6 x -> Uint128.to_float x
    | VIp (V4 x) -> Uint32.to_float x
    | VIp (V6 x) -> Uint128.to_float x
    | v ->
        Printf.sprintf2 "float_of_scalar for %a" print v |>
        invalid_arg)

let int_of_scalar s =
  Option.map int_of_float (float_of_scalar s)

let bool_of_scalar s =
  if s = VNull then None else
  match s with
  | VBool x -> Some x
  | _ -> Option.map ((<>) 0) (int_of_scalar s)

(*
 * Parsing
 *)

module Parser =
struct
  (*$< Parser *)
  type key_type = VecDim of int | ListDim | MapKey of t

  let min_i8 = Num.of_string "-128"
  let max_i8 = Num.of_string "127"
  let max_u8 = Num.of_string "255"
  let min_i16 = Num.of_string "-32766"
  let max_i16 = Num.of_string "32767"
  let max_u16 = Num.of_string "65535"
  let min_i24 = Num.of_string "-8388608"
  let max_i24 = Num.of_string "8388607"
  let max_u24 = Num.of_string "16777215"
  let min_i32 = Num.of_string "-2147483648"
  let max_i32 = Num.of_string "2147483647"
  let max_u32 = Num.of_string "4294967295"
  let min_i40 = Num.of_string "-549755813888"
  let max_i40 = Num.of_string "549755813887"
  let max_u40 = Num.of_string "1099511627776"
  let min_i48 = Num.of_string "-140737488355328"
  let max_i48 = Num.of_string "140737488355327"
  let max_u48 = Num.of_string "281474976710655"
  let min_i56 = Num.of_string "-36028797018963968"
  let max_i56 = Num.of_string "36028797018963967"
  let max_u56 = Num.of_string "72057594037927935"
  let min_i64 = Num.of_string "-9223372036854775808"
  let max_i64 = Num.of_string "9223372036854775807"
  let max_u64 = Num.of_string "18446744073709551615"
  let min_i128 = Num.of_string "-170141183460469231731687303715884105728"
  let max_i128 = Num.of_string "170141183460469231731687303715884105727"
  let max_u128 = Num.of_string "340282366920938463463374607431768211455"
  let zero = Num.zero

  open RamenParsing

  (* Default scalars are parsed as small integers of usually at least 32 bits,
   * preferable unsigned. But the type checker have some allowance to relax
   * shit decision. *)
  let narrowest_int_scalar ?(min_int_width=32) i =
    let s = Num.to_string i in
    if min_int_width <= 8 && Num.le_num zero i && Num.le_num i max_u8
    then VU8 (Uint8.of_string s) else
    if min_int_width <= 8 && Num.le_num min_i8 i && Num.le_num i max_i8
    then VI8 (Int8.of_string s) else
    if min_int_width <= 16 && Num.le_num zero i && Num.le_num i max_u16
    then VU16 (Uint16.of_string s) else
    if min_int_width <= 16 && Num.le_num min_i16 i && Num.le_num i max_i16
    then VI16 (Int16.of_string s) else
    if min_int_width <= 24 && Num.le_num zero i && Num.le_num i max_u24
    then VU24 (Uint24.of_string s) else
    if min_int_width <= 24 && Num.le_num min_i24 i && Num.le_num i max_i24
    then VI24 (Int24.of_string s) else
    if min_int_width <= 32 && Num.le_num zero i && Num.le_num i max_u32
    then VU32 (Uint32.of_string s) else
    if min_int_width <= 32 && Num.le_num min_i32 i && Num.le_num i max_i32
    then VI32 (Int32.of_string s) else
    if min_int_width <= 40 && Num.le_num zero i && Num.le_num i max_u40
    then VU40 (Uint40.of_string s) else
    if min_int_width <= 40 && Num.le_num min_i40 i && Num.le_num i max_i40
    then VI40 (Int40.of_string s) else
    if min_int_width <= 48 && Num.le_num zero i && Num.le_num i max_u48
    then VU48 (Uint48.of_string s) else
    if min_int_width <= 48 && Num.le_num min_i48 i && Num.le_num i max_i48
    then VI48 (Int48.of_string s) else
    if min_int_width <= 56 && Num.le_num zero i && Num.le_num i max_u56
    then VU56 (Uint56.of_string s) else
    if min_int_width <= 56 && Num.le_num min_i56 i && Num.le_num i max_i56
    then VI56 (Int56.of_string s) else
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
    narrowest_int_scalar ?min_int_width (Num.of_int n) |> type_of_value

  (* TODO: Here and elsewhere, we want the location (start+length) of the
   * thing in addition to the thing *)
  let narrowest_int ?min_int_width ?(all_possible=false) () =
    let ostrinG s =
      if all_possible then optional ~def:() (strinG s)
      else strinG s in
    (if all_possible then (
      (* Also in "all_possible" mode, accept an integer as a float: *)
      decimal_number ++ float_scale >>: fun (n, s) ->
        VFloat ((Num.to_float n) *. s)
     ) else (
      integer ++ num_scale >>: fun (n, s) ->
        let n = Num.mul n s in
        if Num.is_integer_num n then
          narrowest_int_scalar ?min_int_width n
        else
          raise (Reject "Not an integer")
     )) |||
    (* Ignore min_int_width when an explicit suffix is given: *)
    (integer_range ~min:min_i8 ~max:max_i8 +-
      ostrinG "i8" >>: fun i -> VI8 (Int8.of_string (Num.to_string i))) |||
    (integer_range ~min:min_i16 ~max:max_i16 +-
      ostrinG "i16" >>: fun i -> VI16 (Int16.of_string (Num.to_string i))) |||
    (integer_range ~min:min_i24 ~max:max_i24 +-
      ostrinG "i24" >>: fun i -> VI24 (Int24.of_string (Num.to_string i))) |||
    (integer_range ~min:min_i32 ~max:max_i32 +-
      ostrinG "i32" >>: fun i -> VI32 (Int32.of_string (Num.to_string i))) |||
    (integer_range ~min:min_i40 ~max:max_i40 +-
      ostrinG "i40" >>: fun i -> VI40 (Int40.of_string (Num.to_string i))) |||
    (integer_range ~min:min_i48 ~max:max_i48 +-
      ostrinG "i48" >>: fun i -> VI48 (Int48.of_string (Num.to_string i))) |||
    (integer_range ~min:min_i56 ~max:max_i56 +-
      ostrinG "i56" >>: fun i -> VI56 (Int56.of_string (Num.to_string i))) |||
    (integer_range ~min:min_i64 ~max:max_i64 +-
      ostrinG "i64" >>: fun i -> VI64 (Int64.of_string (Num.to_string i))) |||
    (integer_range ~min:min_i128 ~max:max_i128 +-
      ostrinG "i128" >>: fun i -> VI128 (Int128.of_string (Num.to_string i))) |||
    (integer_range ~min:Num.zero ~max:max_u8 +-
      ostrinG "u8" >>: fun i -> VU8 (Uint8.of_string (Num.to_string i))) |||
    (integer_range ~min:Num.zero ~max:max_u16 +-
      ostrinG "u16" >>: fun i -> VU16 (Uint16.of_string (Num.to_string i))) |||
    (integer_range ~min:Num.zero ~max:max_u24 +-
      ostrinG "u24" >>: fun i -> VU24 (Uint24.of_string (Num.to_string i))) |||
    (integer_range ~min:Num.zero ~max:max_u32 +-
      ostrinG "u32" >>: fun i -> VU32 (Uint32.of_string (Num.to_string i))) |||
    (integer_range ~min:Num.zero ~max:max_u40 +-
      ostrinG "u40" >>: fun i -> VU40 (Uint40.of_string (Num.to_string i))) |||
    (integer_range ~min:Num.zero ~max:max_u48 +-
      ostrinG "u48" >>: fun i -> VU48 (Uint48.of_string (Num.to_string i))) |||
    (integer_range ~min:Num.zero ~max:max_u56 +-
      ostrinG "u56" >>: fun i -> VU56 (Uint56.of_string (Num.to_string i))) |||
    (integer_range ~min:Num.zero ~max:max_u64 +-
      ostrinG "u64" >>: fun i -> VU64 (Uint64.of_string (Num.to_string i))) |||
    (integer_range ~min:Num.zero ~max:max_u128 +-
      ostrinG "u128" >>: fun i -> VU128 (Uint128.of_string (Num.to_string i)))

  (* Note that min_int_width must not prevent a type suffix to take effect *)
  (*$= narrowest_int & ~printer:(test_printer print)
    (Ok (VU8 (Uint8.of_int 12), (2,[]))) \
        (test_p (narrowest_int ~min_int_width:0 ()) "12")
    (Ok (VU8 (Uint8.of_int 12), (6,[]))) \
        (test_p (narrowest_int ~min_int_width:0 ()) "12000m")
    (Ok (VU8 (Uint8.of_int 12), (4,[]))) \
        (test_p (narrowest_int ~min_int_width:32 ()) "12u8")
  *)

  let all_possible_ints =
    narrowest_int ~all_possible:true ()

  (* When parsing expressions we'd rather keep literal tuples/vectors to be
   * expressions, to disambiguate the syntax. So then we only look for
   * scalars: *)
  let scalar ?min_int_width m =
    let m = "scalar" :: m in
    (
      (worD "false" >>: fun () -> VBool false) |<|
      (worD "true" >>: fun () -> VBool true) |<|
      (quoted_char >>: fun c -> VChar c) |<|
      (quoted_string >>: fun s -> VString s) |<|
      (RamenEthAddr.Parser.p >>: fun v -> VEth v) |<|
      (* Note: Ipv4 dotted notation must come before floating point values
       * and CIDRs before IPs: *)
      (RamenIpv4.Cidr.Parser.p >>: fun v -> VCidrv4 v) |<|
      (RamenIpv6.Cidr.Parser.p >>: fun v -> VCidrv6 v) |<|
      (RamenIpv4.Parser.p >>: fun v -> VIpv4 v) |<|
      (RamenIpv6.Parser.p >>: fun v -> VIpv6 v) |<|
      (* Note: we do not parse an IP or a CIDR as a generic RamenIP.t etc.
       * Indeed, that would lead to an ambiguous grammar and also what's the
       * point in losing typing accuracy? IPs will be cast to generic IPs
       * as required. *)
      (
        (if min_int_width = None then all_possible_ints
         else narrowest_int ?min_int_width ()) |||
        (floating_point ++ float_scale >>: fun (f, s) -> VFloat (f *. s))
      ) |<|
      (string "()" >>: fun () -> VUnit)
    ) m

  (* We do not allow to add explicit NULL values as immediate values in scalar
   * expressions, but in some places this could be used: *)
  let null m =
    let m = "NULL" :: m in
    (worD "null" >>: fun () -> VNull) m

  let empty_list m =
    let m = "empty list" :: m in
    (string "[]" >>: fun () -> VArr [||]) m

  (* TODO: consider functions as taking a single tuple *)
  let tup_sep =
    opt_blanks -- char ';' -- opt_blanks

  (* For now we stay away from the special syntax for SELECT ("as"): *)
  let kv_sep =
    opt_blanks -- char ':' -- opt_blanks

  (* By default, we want only one value of at least 32 bits: *)
  let rec p m = p_ ~min_int_width:32 m

  (* But in general when parsing user provided values (such as in parameters
   * or test files), we want to allow any literal: *)
  (* [p_] returns all possible interpretation of a literal (ie. for an
   * integer, all its possible sizes); while [p_ ~min_int_width] returns
   * only the smaller (that have at least the specified width). *)
  and p_ ?min_int_width =
    null |<|
    scalar ?min_int_width |<|
    (* Also literals of constructed types: *)
    empty_list |<|
    (tuple ?min_int_width >>: fun vs -> VTup vs) |<|
    (vector ?min_int_width >>: fun vs -> VVec vs) |<|
    (record ?min_int_width >>: fun h -> VRec h)
    (* Note: there is no way to enter a literal list, as it's the same
     * representation than an array. And, given the functions that work
     * on arrays would also work on list, and that arrays are more efficient
     * (because there is no additional NULL check), there is no reason to
     * do that but to test lists. For that, we could use a conversion
     * function from arrays to lists. *)

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
      (repeat_greedy ~min:2 ~sep:tup_sep (p_ ?min_int_width) >>: Array.of_list) +-
      opt_blanks +- char ')'
    ) m

  (* Like tuples but with mandatory field names: *)
  and record ?min_int_width m =
    let m = "record" :: m in
    (
      char '{' -- opt_blanks -+
      (several_greedy ~sep:tup_sep (
        non_keyword +- kv_sep ++ p_ ?min_int_width) >>:
        Array.of_list) +-
      opt_blanks +- char '}'
    ) m

  (* Empty vectors are disallowed so we cannot not know the element type: *)
  (* FIXME: What about non-empty vectors with only NULLs ? *)
  and vector ?min_int_width m =
    let m = "vector" :: m in
    (
      char '[' -- opt_blanks -+
      (several ~sep:tup_sep (p_ ?min_int_width) >>: fun vs ->
         match largest_type (List.map type_of_value vs) with
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
    (Ok (VChar 'c', (3,[])))      (test_p p "#\\c")
    (Ok (VTup [| VFloat 3.14; VBool true |], (12,[]))) \
                                  (test_p p "(3.14; true)")
    (Ok (VVec [| VFloat 3.14; VFloat 1. |], (9,[]))) \
                                  (test_p p "[3.14; 1]")
    (Ok (VVec [| VU32 Uint32.zero; VU32 Uint32.one; \
                 VU32 (Uint32.of_int 2) |], (9,[]))) \
                                  (test_p p "[0; 1; 2]")
    (Ok (VVec [| VChar 't'; VChar 'e'; VChar 's'; \
                 VChar 't' |], (20, []))) \
                                  (test_p p "[#\\t; #\\e; #\\s; #\\t]")
    (Ok (VVec [| VU32 (Uint32.of_int 42); VNull |], (13, []))) \
                                  (test_p p "[ 42 ; null ]")
  *)

  (* Also check string escape characters: *)
  (*$= p & ~printer:(test_printer print)
     (Ok (VString "glop", (8,[]))) (test_p p "\"gl\\o\\p\"")
     (Ok (VString "new\nline", (11,[]))) (test_p p "\"new\\nline\"")
  *)

  let typ =
    DessserParser.mn >>: fun mn ->
      let rec check_valid = function
        (* Filter out sum types to make grammar less ambiguous *)
        | TUnknown | TVoid | TExt _ | TSet _ | TSum _ ->
            raise (Reject "No such types in RaQL")
        | TTup mns ->
            Array.iter (fun mn -> check_valid mn.typ) mns
        | TRec mns ->
            Array.iter (fun (_, mn) -> check_valid mn.typ) mns
        | TVec (_, mn) | TArr mn ->
            check_valid mn.typ
        | _ ->
            () in
      check_valid mn.typ ;
      mn

  (*$= typ & ~printer:(test_printer DT.print_mn)
    (Ok (DT.u8, (6,[]))) (test_p typ "((u8))")
  *)

  (*$>*)
end

(* Use the above parser to get a value from a string.
 * Pass the expected type if you know it. *)
let of_string ?what ?mn s =
  let open RamenParsing in
  let what = what |? ("value of "^ String.quote s) in
  let p = allow_surrounding_blanks Parser.(
            (* Parse the string as narrowly as possible; values
             * will be enlarged later as required: *)
            p_ ~min_int_width:0) in
  let stream = stream_of_string s in
  let m = [ what ] in
  match p m None Parsers.no_error_correction stream |>
        to_result with
  | Error e ->
      let err =
        IO.to_string (print_bad_result print) e in
      Error err
  | Ok (v, _) ->
      (match mn with
      | None ->
          Ok v
      | Some mn ->
          if v = VNull then (
            if mn.nullable then Ok VNull
            else
              let err_msg =
                Printf.sprintf2 "Cannot convert NULL into non nullable type %a"
                  print_mn mn in
              Error err_msg
          ) else (
            try Ok (enlarge_value mn.typ v)
            with exn -> Error (Printexc.to_string exn)
          ))

(*$= of_string & ~printer:(BatIO.to_string (result_print print BatString.print))
  (Ok (VI8 (Int8.of_int 42))) \
    (of_string ~mn:DT.(required (TI8)) "42")
  (Ok VNull) \
    (of_string ~mn:DT.(optional (TI8)) "Null")
  (Ok (VVec [| VI8 (Int8.of_int 42); VNull |])) \
    (of_string ~mn:DT.(required (TVec (2, optional (TI8)))) "[42; Null]")
  (Ok (VVec [| VChar 't'; VChar 'e'; VChar 's'; VChar 't' |] )) \
    (of_string ~mn:DT.(required (TVec (4, optional (TChar)))) \
      "[#\\t; #\\e; #\\s; #\\t]")
*)

(*$T
  false < true
  of_string "false" < of_string "true"
*)

let scalar_of_int n =
  Parser.narrowest_int_scalar ~min_int_width:0 (Num.of_int n)

(*$= scalar_of_int & ~printer:(BatIO.to_string print)
  (VU8 (Uint8.of_int 42)) (scalar_of_int 42)
  (VI8 (Int8.of_int (-42))) (scalar_of_int (-42))
  (VU16 (Uint16.of_int 45678)) (scalar_of_int 45678)
*)
