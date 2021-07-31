(* Manually written impostor to fq_name_wire, converting to N.fq. *)
open DessserOCamlBackEndHelpers

module N = RamenName
module Wire = Fq_name_wire.DessserGen

module DessserGen = struct
  type t = N.fq

  let to_row_binary_with_mask m t p =
    Wire.to_row_binary_with_mask m (t : N.fq :> string) p

  let to_row_binary t p =
    Wire.to_row_binary (t : N.fq :> string) p

  let sersize_of_row_binary_with_mask m t =
    Wire.sersize_of_row_binary_with_mask m (t : N.fq :> string)

  let sersize_of_row_binary t =
    Wire.sersize_of_row_binary (t : N.fq :> string)

  let of_row_binary p =
    let t, p = Wire.of_row_binary p in
    N.fq t, p

  let to_json_with_mask m t p =
    Wire.to_json_with_mask m (t : N.fq :> string) p

  let to_json t p =
    Wire.to_json (t : N.fq :> string) p

  let sersize_of_json_with_mask m t =
    Wire.sersize_of_json_with_mask m (t : N.fq :> string)

  let sersize_of_json t =
    Wire.sersize_of_json (t : N.fq :> string)

  let of_json p =
    let t, p = Wire.of_json p in
    N.fq t, p
end
