(* Manually written impostor to src_path_wire, converting to N.src_path. *)
open DessserOCamlBackEndHelpers

module N = RamenName
module Wire = Src_path_wire.DessserGen

module DessserGen = struct
  type t = N.src_path

  let to_row_binary_with_mask m t p =
    Wire.to_row_binary_with_mask m (t : N.src_path :> string) p

  let to_row_binary t p =
    Wire.to_row_binary (t : N.src_path :> string) p

  let sersize_of_row_binary_with_mask m t =
    Wire.sersize_of_row_binary_with_mask m (t : N.src_path :> string)

  let sersize_of_row_binary t =
    Wire.sersize_of_row_binary (t : N.src_path :> string)

  let of_row_binary p =
    let t, p = Wire.of_row_binary p in
    N.src_path t, p
end
