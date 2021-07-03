(* Manually written impostor converting from DessserTypes.t into
 * automatically generated raql_type_wire: *)
open Stdint
open DessserOCamlBackEndHelpers

module DT = DessserTypes

module DessserGen : sig
  type t = DT.mn
  val to_row_binary : (DessserMasks.t -> t -> Pointer.t -> Pointer.t)
  val sersize_of_row_binary : (DessserMasks.t -> t -> Size.t)
  val of_row_binary : (Pointer.t -> (t * Pointer.t))
end
