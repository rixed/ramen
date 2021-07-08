(* Manually written impostor to fieldmask_wire, converting from/to a
 * DessserMasks.t. *)
open DessserOCamlBackEndHelpers

module Wire = Fieldmask_wire.DessserGen

module DessserGen = struct
  type t = DessserMasks.t

  let to_row_binary_with_mask m t p =
    Wire.to_row_binary_with_mask m (DessserMasks.string_of_mask t) p

  let sersize_of_row_binary_with_mask m t =
    Wire.sersize_of_row_binary_with_mask m (DessserMasks.string_of_mask t)

  let of_row_binary p =
    let t, p = Wire.of_row_binary p in
    DessserMasks.Parser.mask_of_string t, p
end
