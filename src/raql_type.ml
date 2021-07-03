(* Manually written impostor converting from DessserTypes.t into
 * automatically generated raql_type_wire: *)
open Stdint
open DessserOCamlBackEndHelpers

module DT = DessserTypes
module Wire = Raql_type_wire.DessserGen

let rec wire_of_type = function
  | DT.Base Bool -> Wire.Bool
  | DT.Base Char -> Wire.Char
  | DT.Base Float -> Wire.Float
  | DT.Base String -> Wire.String
  | DT.Base U8 -> Wire.U8
  | DT.Base U16 -> Wire.U16
  | DT.Base U24 -> Wire.U24
  | DT.Base U32 -> Wire.U32
  | DT.Base U40 -> Wire.U40
  | DT.Base U48 -> Wire.U48
  | DT.Base U56 -> Wire.U56
  | DT.Base U64 -> Wire.U64
  | DT.Base U128 -> Wire.U128
  | DT.Base I8 -> Wire.I8
  | DT.Base I16 -> Wire.I16
  | DT.Base I24 -> Wire.I24
  | DT.Base I32 -> Wire.I32
  | DT.Base I40 -> Wire.I40
  | DT.Base I48 -> Wire.I48
  | DT.Base I56 -> Wire.I56
  | DT.Base I64 -> Wire.I64
  | DT.Base I128 -> Wire.I128
  | DT.Usr { name = "Eth"; _ } -> Wire.Eth
  | DT.Usr { name = "Ip4" ; _ } -> Wire.Ipv4
  | DT.Usr { name = "Ip6" ; _ } -> Wire.Ipv6
  | DT.Usr { name = "Ip" ; _ } -> Wire.Ip
  | DT.Usr { name = "Cidr4" ; _ } -> Wire.Cidrv4
  | DT.Usr { name = "Cidr6" ; _ } -> Wire.Cidrv6
  | DT.Usr { name = "Cidr" ; _ } -> Wire.Cidr
  | DT.Tup mns -> Wire.Tup (Array.map wire_of_mn mns)
  | DT.Vec (d, mn) -> Wire.Vec (Uint32.of_int d, wire_of_mn mn)
  | DT.Lst mn -> Wire.Lst (wire_of_mn mn)
  | DT.Rec mns -> Wire.Rec_ (Array.map (fun (n, mn) -> n, wire_of_mn mn) mns)
  | DT.Sum mns -> Wire.Sum (Array.map (fun (n, mn) -> n, wire_of_mn mn) mns)
  | DT.Map (kmn, vmn) -> Wire.Map (wire_of_mn kmn, wire_of_mn vmn)
  | t -> invalid_arg ("wire_of_type: "^ DT.to_string t)

and wire_of_mn mn =
  Wire.{ type_ = wire_of_type mn.DT.typ ;
         nullable = mn.nullable }

let rec type_of_wire = function
  | Wire.Bool -> DT.Base Bool
  | Wire.Char -> DT.Base Char
  | Wire.Float -> DT.Base Float
  | Wire.String -> DT.Base String
  | Wire.U8 -> DT.Base U8
  | Wire.U16 -> DT.Base U16
  | Wire.U24 -> DT.Base U24
  | Wire.U32 -> DT.Base U32
  | Wire.U40 -> DT.Base U40
  | Wire.U48 -> DT.Base U48
  | Wire.U56 -> DT.Base U56
  | Wire.U64 -> DT.Base U64
  | Wire.U128 -> DT.Base U128
  | Wire.I8 -> DT.Base I8
  | Wire.I16 -> DT.Base I16
  | Wire.I24 -> DT.Base I24
  | Wire.I32 -> DT.Base I32
  | Wire.I40 -> DT.Base I40
  | Wire.I48 -> DT.Base I48
  | Wire.I56 -> DT.Base I56
  | Wire.I64 -> DT.Base I64
  | Wire.I128 -> DT.Base I128
  | Wire.Eth -> DT.get_user_type "Eth"
  | Wire.Ipv4 -> DT.get_user_type "Ip4"
  | Wire.Ipv6 -> DT.get_user_type "Ip6"
  | Wire.Ip -> DT.get_user_type "Ip"
  | Wire.Cidrv4 -> DT.get_user_type "Cidr4"
  | Wire.Cidrv6 -> DT.get_user_type "Cidr6"
  | Wire.Cidr -> DT.get_user_type "Cidr"
  | Wire.Tup mns -> DT.Tup (Array.map mn_of_wire mns)
  | Wire.Vec (d, mn) -> DT.Vec (Uint32.to_int d, mn_of_wire mn)
  | Wire.Lst mn -> DT.Lst (mn_of_wire mn)
  | Wire.Rec_ mns -> DT.Rec (Array.map (fun (n, mn) -> n, mn_of_wire mn) mns)
  | Wire.Sum mns -> DT.Sum (Array.map (fun (n, mn) -> n, mn_of_wire mn) mns)
  | Wire.Map (kmn, vmn) -> DT.Map (mn_of_wire kmn, mn_of_wire vmn)

and mn_of_wire mn =
  DT.{ typ = type_of_wire mn.Wire.type_ ; nullable = mn.nullable }

module DessserGen = struct
  type t = DT.mn

  let to_row_binary m t p =
    Wire.to_row_binary m (wire_of_mn t) p

  let sersize_of_row_binary m t =
    Wire.sersize_of_row_binary m (wire_of_mn t)

  let of_row_binary p =
    let t, p = Wire.of_row_binary p in
    mn_of_wire t, p
end
