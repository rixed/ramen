(* Manually written impostor converting from DessserTypes.t into
 * automatically generated raql_type_wire: *)
open Stdint
open DessserOCamlBackEndHelpers

module DT = DessserTypes
module Wire = Raql_type_wire.DessserGen

let rec wire_of_type = function
  | DT.TVoid -> Wire.Void
  | DT.TBool -> Wire.Bool
  | DT.TChar -> Wire.Char
  | DT.TFloat -> Wire.Float
  | DT.TString -> Wire.String
  | DT.TU8 -> Wire.U8
  | DT.TU16 -> Wire.U16
  | DT.TU24 -> Wire.U24
  | DT.TU32 -> Wire.U32
  | DT.TU40 -> Wire.U40
  | DT.TU48 -> Wire.U48
  | DT.TU56 -> Wire.U56
  | DT.TU64 -> Wire.U64
  | DT.TU128 -> Wire.U128
  | DT.TI8 -> Wire.I8
  | DT.TI16 -> Wire.I16
  | DT.TI24 -> Wire.I24
  | DT.TI32 -> Wire.I32
  | DT.TI40 -> Wire.I40
  | DT.TI48 -> Wire.I48
  | DT.TI56 -> Wire.I56
  | DT.TI64 -> Wire.I64
  | DT.TI128 -> Wire.I128
  | DT.TUsr { name = "Eth"; _ } -> Wire.Eth
  | DT.TUsr { name = "Ip4" ; _ } -> Wire.Ipv4
  | DT.TUsr { name = "Ip6" ; _ } -> Wire.Ipv6
  | DT.TUsr { name = "Ip" ; _ } -> Wire.Ip
  | DT.TUsr { name = "Cidr4" ; _ } -> Wire.Cidrv4
  | DT.TUsr { name = "Cidr6" ; _ } -> Wire.Cidrv6
  | DT.TUsr { name = "Cidr" ; _ } -> Wire.Cidr
  | DT.TTup mns -> Wire.Tup (Array.map wire_of_mn mns)
  | DT.TVec (d, mn) -> Wire.Vec (Uint32.of_int d, wire_of_mn mn)
  | DT.TArr mn -> Wire.Arr (wire_of_mn mn)
  | DT.TRec mns -> Wire.Rec (Array.map (fun (n, mn) -> n, wire_of_mn mn) mns)
  | DT.TSum mns -> Wire.Sum (Array.map (fun (n, mn) -> n, wire_of_mn mn) mns)
  | DT.TMap (kmn, vmn) -> Wire.Map (wire_of_mn kmn, wire_of_mn vmn)
  | t -> invalid_arg ("wire_of_type: "^ DT.to_string t)

and wire_of_mn mn =
  Wire.{ type_ = wire_of_type mn.DT.typ ;
         nullable = mn.nullable }

let rec type_of_wire = function
  | Wire.Void -> DT.TVoid
  | Wire.Bool -> DT.TBool
  | Wire.Char -> DT.TChar
  | Wire.Float -> DT.TFloat
  | Wire.String -> DT.TString
  | Wire.U8 -> DT.TU8
  | Wire.U16 -> DT.TU16
  | Wire.U24 -> DT.TU24
  | Wire.U32 -> DT.TU32
  | Wire.U40 -> DT.TU40
  | Wire.U48 -> DT.TU48
  | Wire.U56 -> DT.TU56
  | Wire.U64 -> DT.TU64
  | Wire.U128 -> DT.TU128
  | Wire.I8 -> DT.TI8
  | Wire.I16 -> DT.TI16
  | Wire.I24 -> DT.TI24
  | Wire.I32 -> DT.TI32
  | Wire.I40 -> DT.TI40
  | Wire.I48 -> DT.TI48
  | Wire.I56 -> DT.TI56
  | Wire.I64 -> DT.TI64
  | Wire.I128 -> DT.TI128
  | Wire.Eth -> DT.get_user_type "Eth"
  | Wire.Ipv4 -> DT.get_user_type "Ip4"
  | Wire.Ipv6 -> DT.get_user_type "Ip6"
  | Wire.Ip -> DT.get_user_type "Ip"
  | Wire.Cidrv4 -> DT.get_user_type "Cidr4"
  | Wire.Cidrv6 -> DT.get_user_type "Cidr6"
  | Wire.Cidr -> DT.get_user_type "Cidr"
  | Wire.Tup mns -> DT.TTup (Array.map mn_of_wire mns)
  | Wire.Vec (d, mn) -> DT.TVec (Uint32.to_int d, mn_of_wire mn)
  | Wire.Arr mn -> DT.TArr (mn_of_wire mn)
  | Wire.Rec mns -> DT.TRec (Array.map (fun (n, mn) -> n, mn_of_wire mn) mns)
  | Wire.Sum mns -> DT.TSum (Array.map (fun (n, mn) -> n, mn_of_wire mn) mns)
  | Wire.Map (kmn, vmn) -> DT.TMap (mn_of_wire kmn, mn_of_wire vmn)

and mn_of_wire mn =
  DT.{ typ = type_of_wire mn.Wire.type_ ;
       nullable = mn.nullable ;
       default = None }

module DessserGen = struct
  type t = DT.mn

  let to_row_binary_with_mask m t p =
    Wire.to_row_binary_with_mask m (wire_of_mn t) p

  let to_row_binary t p =
    Wire.to_row_binary (wire_of_mn t) p

  let sersize_of_row_binary_with_mask m t =
    Wire.sersize_of_row_binary_with_mask m (wire_of_mn t)

  let sersize_of_row_binary t =
    Wire.sersize_of_row_binary (wire_of_mn t)

  let of_row_binary p =
    let t, p = Wire.of_row_binary p in
    mn_of_wire t, p
end
