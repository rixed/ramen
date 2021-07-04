(* Manually written impostor to program_name_wire, converting to N.program. *)
open DessserOCamlBackEndHelpers

module N = RamenName
module Wire = Program_name_wire.DessserGen

module DessserGen = struct
  type t = N.program

  let to_row_binary_with_mask m t p =
    Wire.to_row_binary_with_mask m (t : N.program :> string) p

  let sersize_of_row_binary_with_mask m t =
    Wire.sersize_of_row_binary_with_mask m (t : N.program :> string)

  let of_row_binary p =
    let t, p = Wire.of_row_binary p in
    N.program t, p
end
