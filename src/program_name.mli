(* Manually written impostor to program_name_wire, converting to N.program. *)
open DessserOCamlBackEndHelpers

module N = RamenName
module Wire = Program_name_wire.DessserGen

module DessserGen : sig
  type t = N.program
  val to_row_binary_with_mask : (DessserMasks.t -> t -> Pointer.t -> Pointer.t)
  val sersize_of_row_binary_with_mask : (DessserMasks.t -> t -> Size.t)
  val of_row_binary : (Pointer.t -> (t * Pointer.t))
end
