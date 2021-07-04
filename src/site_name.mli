(* Manually written impostor to site_name_wire, converting to N.site. *)
open DessserOCamlBackEndHelpers

module N = RamenName
module Wire = Site_name_wire.DessserGen

module DessserGen : sig
  type t = N.site
  val to_row_binary_with_mask : (DessserMasks.t -> t -> Pointer.t -> Pointer.t)
  val sersize_of_row_binary_with_mask : (DessserMasks.t -> t -> Size.t)
  val of_row_binary : (Pointer.t -> (t * Pointer.t))
end
