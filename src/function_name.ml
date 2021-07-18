(* Manually written impostor to function_name_wire, converting to N.func. *)
open DessserOCamlBackEndHelpers

module N = RamenName
module Wire = Function_name_wire.DessserGen

module DessserGen = struct
  type t = N.func

  let to_row_binary_with_mask m t p =
    Wire.to_row_binary_with_mask m (t : N.func :> string) p

  let sersize_of_row_binary_with_mask m t =
    Wire.sersize_of_row_binary_with_mask m (t : N.func :> string)

  let of_row_binary p =
    let t, p = Wire.of_row_binary p in
    N.func t, p

  let to_json_with_mask m t p =
    Wire.to_json_with_mask m (t : N.func :> string) p

  let sersize_of_json_with_mask m t =
    Wire.sersize_of_json_with_mask m (t : N.func :> string)

  let of_json p =
    let t, p = Wire.of_json p in
    N.func t, p
end
