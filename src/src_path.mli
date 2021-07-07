(* Manually written impostor to src_path_wire, converting to N.src_path. *)
open DessserOCamlBackEndHelpers

module N = RamenName
module Wire = Src_path_wire.DessserGen

module DessserGen : sig
  type t = N.src_path
  val to_row_binary_with_mask : (DessserMasks.t -> t -> Pointer.t -> Pointer.t)
  val sersize_of_row_binary_with_mask : (DessserMasks.t -> t -> Size.t)
  val of_row_binary : (Pointer.t -> (t * Pointer.t))
end
