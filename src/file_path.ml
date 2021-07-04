(* Manually written impostor to file_path_wire, converting to N.path. *)
open DessserOCamlBackEndHelpers

module N = RamenName
module Wire = File_path_wire.DessserGen

module DessserGen = struct
  type t = N.path

  let to_row_binary_with_mask m t p =
    Wire.to_row_binary_with_mask m (t : N.path :> string) p

  let sersize_of_row_binary_with_mask m t =
    Wire.sersize_of_row_binary_with_mask m (t : N.path :> string)

  let of_row_binary p =
    let t, p = Wire.of_row_binary p in
    N.path t, p
end
