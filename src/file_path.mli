(* Manually written impostor to file_path_wire, converting to N.path. *)
open DessserOCamlBackEndHelpers

module N = RamenName
module Wire = File_path_wire.DessserGen

module DessserGen : sig
  type t = N.path
  val to_row_binary_with_mask : (DessserMasks.t -> t -> Pointer.t -> Pointer.t)
  val sersize_of_row_binary_with_mask : (DessserMasks.t -> t -> Size.t)
  val of_row_binary : (Pointer.t -> (t * Pointer.t))
end
