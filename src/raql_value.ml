(* Manually written impostor to raql_value, converting from/to Ramen's internal
 * value types. *)
open Stdint
open DessserOCamlBackEndHelpers

module Wire = Raql_value_wire.DessserGen

(* Stdint types are implemented as custom blocks, therefore are slower than
 * ints.  But we do not care as we merely represents code here, we do not run
 * the operators.
 * For NULL values we are doomed to loose the type information, at least for
 * constructed types, unless we always keep the type alongside the value,
 * which we do not want to (we want to erase types in serialization etc). So
 * if we are given only a NULL tuple there is no way to know its type.
 * Tough life. *)
(* Refined from Raql_value because of our IP type. FIXME. *)
type t =
  | VNull
  | VUnit
  | VFloat of float
  | VString of string
  | VBool of bool
  | VChar of char
  | VU8 of uint8
  | VU16 of uint16
  | VU24 of uint24
  | VU32 of uint32
  | VU40 of uint40
  | VU48 of uint48
  | VU56 of uint56
  | VU64 of uint64
  | VU128 of uint128
  | VI8 of int8
  | VI16 of int16
  | VI24 of int24
  | VI32 of int32
  | VI40 of int40
  | VI48 of int48
  | VI56 of int56
  | VI64 of int64
  | VI128 of int128
  | VEth of uint48
  | VIpv4 of uint32
  | VIpv6 of uint128
  | VIp of RamenIp.t
  | VCidrv4 of RamenIpv4.Cidr.t
  | VCidrv6 of RamenIpv6.Cidr.t
  | VCidr of RamenIp.Cidr.t
  | VTup of t array
  | VVec of t array (* All values must have the same type *)
  | VLst of t array (* All values must have the same type *)
  (* Note: The labels are only needed for pretty printing the values. *)
  | VRec of (string * t) array
  | VMap of (t * t) array
  [@@ppp PPP_OCaml]

(* Temporarily, convert a dessser raql_value representation into a value: *)
let rec of_wire = function
  | Wire.VNull -> (VNull : t)
  | VUnit -> VUnit
  | VFloat x -> VFloat x
  | VString x -> VString x
  | VBool x -> VBool x
  | VChar x -> VChar x
  | VU8 x -> VU8 x
  | VU16 x -> VU16 x
  | VU24 x -> VU24 x
  | VU32 x -> VU32 x
  | VU40 x -> VU40 x
  | VU48 x -> VU48 x
  | VU56 x -> VU56 x
  | VU64 x -> VU64 x
  | VU128 x -> VU128 x
  | VI8 x -> VI8 x
  | VI16 x -> VI16 x
  | VI24 x -> VI24 x
  | VI32 x -> VI32 x
  | VI40 x -> VI40 x
  | VI48 x -> VI48 x
  | VI56 x -> VI56 x
  | VI64 x -> VI64 x
  | VI128 x -> VI128 x
  | VEth x -> VEth x
  | VIpv4 x -> VIpv4 x
  | VIpv6 x -> VIpv6 x
  | VIp (Ip_v4 x) -> VIp (RamenIp.V4 x)
  | VIp (Ip_v6 x) -> VIp (RamenIp.V6 x)
  | VCidrv4 { cidr4_ip ; cidr4_mask } -> VCidrv4 (cidr4_ip, cidr4_mask)
  | VCidrv6 { ip ; mask } -> VCidrv6 (ip, mask)
  | VCidr (V4 { cidr4_ip ; cidr4_mask }) -> VCidr (V4 (cidr4_ip, cidr4_mask))
  | VCidr (V6 { ip ; mask }) -> VCidr (V6 (ip, mask))
  | VTup x -> VTup (Array.map of_wire x)
  | VVec x -> VVec (Array.map of_wire x)
  | VLst x -> VLst (Array.map of_wire x)
  | VRec x -> VRec (Array.map (fun (n, v) -> n, of_wire v) x)
  | VMap x -> VMap (Array.map (fun (k, v) -> of_wire k, of_wire v) x)

let rec to_wire = function
  | VNull -> Wire.VNull
  | VUnit -> VUnit
  | VFloat x -> VFloat x
  | VString x -> VString x
  | VBool x -> VBool x
  | VChar x -> VChar x
  | VU8 x -> VU8 x
  | VU16 x -> VU16 x
  | VU24 x -> VU24 x
  | VU32 x -> VU32 x
  | VU40 x -> VU40 x
  | VU48 x -> VU48 x
  | VU56 x -> VU56 x
  | VU64 x -> VU64 x
  | VU128 x -> VU128 x
  | VI8 x -> VI8 x
  | VI16 x -> VI16 x
  | VI24 x -> VI24 x
  | VI32 x -> VI32 x
  | VI40 x -> VI40 x
  | VI48 x -> VI48 x
  | VI56 x -> VI56 x
  | VI64 x -> VI64 x
  | VI128 x -> VI128 x
  | VEth x -> VEth x
  | VIpv4 x -> VIpv4 x
  | VIpv6 x -> VIpv6 x
  | VIp (RamenIp.V4 x) -> VIp (Ip_v4 x)
  | VIp (RamenIp.V6 x) -> VIp (Ip_v6 x)
  | VCidrv4 (cidr4_ip, cidr4_mask) -> VCidrv4 { cidr4_ip ; cidr4_mask }
  | VCidrv6 (ip, mask) -> VCidrv6 { ip ; mask }
  | VCidr (V4 (cidr4_ip, cidr4_mask)) -> VCidr (V4 { cidr4_ip ; cidr4_mask })
  | VCidr (V6 (ip, mask)) -> VCidr (V6 { ip ; mask })
  | VTup x -> VTup (Array.map to_wire x)
  | VVec x -> VVec (Array.map to_wire x)
  | VLst x -> VLst (Array.map to_wire x)
  | VRec x -> VRec (Array.map (fun (n, v) -> n, to_wire v) x)
  | VMap x -> VMap (Array.map (fun (k, v) -> to_wire k, to_wire v) x)

module DessserGen = struct
  type out_t = t
  type t = out_t

  let to_row_binary_with_mask m t p =
    Wire.to_row_binary_with_mask m (to_wire t) p

  let to_row_binary t p =
    Wire.to_row_binary (to_wire t) p

  let sersize_of_row_binary_with_mask m t =
    Wire.sersize_of_row_binary_with_mask m (to_wire t)

  let sersize_of_row_binary t =
    Wire.sersize_of_row_binary (to_wire t)

  let of_row_binary p =
    let t, p = Wire.of_row_binary p in
    of_wire t, p
end
