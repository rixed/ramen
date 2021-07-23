(* Manually written impostor to field_name_wire, converting to N.field. *)
open DessserOCamlBackEndHelpers

module N = RamenName
module Wire = Field_name_wire.DessserGen

module DessserGen = struct
  type t = N.field

  let to_row_binary_with_mask m t p =
    Wire.to_row_binary_with_mask m (t : N.field :> string) p

  let sersize_of_row_binary_with_mask m t =
    Wire.sersize_of_row_binary_with_mask m (t : N.field :> string)

  let of_row_binary p =
    let t, p = Wire.of_row_binary p in
    N.field t, p

  let to_json_with_mask m t p =
    Wire.to_json_with_mask m (t : N.field :> string) p

  let sersize_of_json_with_mask m t =
    Wire.sersize_of_json_with_mask m (t : N.field :> string)

  let of_json p =
    let t, p = Wire.of_json p in
    N.field t, p
end
