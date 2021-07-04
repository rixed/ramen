(* Manually written impostor to site_name_wire, converting to N.site. *)
open DessserOCamlBackEndHelpers

module N = RamenName
module Wire = Site_name_wire.DessserGen

module DessserGen = struct
  type t = N.site

  let to_row_binary_with_mask m t p =
    Wire.to_row_binary_with_mask m (t : N.site :> string) p

  let sersize_of_row_binary_with_mask m t =
    Wire.sersize_of_row_binary_with_mask m (t : N.site :> string)

  let of_row_binary p =
    let t, p = Wire.of_row_binary p in
    N.site t, p
end
