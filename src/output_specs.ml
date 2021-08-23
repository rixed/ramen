(* Manually written impostor to output_specs_wire, converting from/to hashes *)
open Batteries
open Stdint
open DessserOCamlBackEndHelpers

module Wire = Output_specs_wire.DessserGen

module DessserGen = struct
  type file_spec =
    { file_type : Wire.file_type ;
      fieldmask : DessserMasks.t ;
      filters : (Uint16.t * Raql_value.t array) array ;
      mutable channels :
        (Uint16.t, Wire.channel_specs) Hashtbl.t }

  type t = (Wire.recipient, file_spec) Hashtbl.t

  let to_wire (t : t) =
    Hashtbl.enum t /@
    (fun (r, s) ->
      r,
      Wire.{
        file_type = s.file_type ;
        fieldmask = s.fieldmask ;
        filters = s.filters ;
        channels =
          Hashtbl.enum s.channels |>
          Array.of_enum }) |>
    Array.of_enum

  let of_wire w =
    Array.enum w /@
    (fun (r, s) ->
      r,
      { file_type = s.Wire.file_type ;
        fieldmask = s.Wire.fieldmask ;
        filters = s.Wire.filters ;
        channels =
          Array.enum s.Wire.channels |>
          Hashtbl.of_enum }) |>
    Hashtbl.of_enum

  let to_row_binary_with_mask m t p =
    Wire.to_row_binary_with_mask m (to_wire t) p

  let to_row_binary t p =
    Wire.to_row_binary (to_wire t) p

  let sersize_of_row_binary_with_mask m t =
    Wire.sersize_of_row_binary_with_mask m (to_wire t)

  let sersize_of_row_binary t =
    Wire.sersize_of_row_binary (to_wire t)

  let of_row_binary p =
    let t, p = Wire.of_row_binary p in
    of_wire t, p
end
