(* Manually written impostor to function_name_wire, converting to N.func. *)
open DessserOCamlBackEndHelpers

module N = RamenName
module Wire = Function_name_wire.DessserGen

module DessserGen : sig
  type t = N.func
  val to_row_binary_with_mask : (DessserMasks.t -> t -> Pointer.t -> Pointer.t)
  val sersize_of_row_binary_with_mask : (DessserMasks.t -> t -> Size.t)
  val of_row_binary : (Pointer.t -> (t * Pointer.t))
end
