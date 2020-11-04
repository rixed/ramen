(* Reads a csv file and convert all its IPs v4 into u32 (TODO: v6).
 * Initially used to convert test CSV to use with the simpler Dessser CSV
 * deserializer. *)
open Batteries

let separator = ','


let rec loop =
  let cs = String.of_char separator in
  fun () ->
    match read_line () with
    | exception End_of_file -> ()
    | "" -> ()
    | s ->
        String.split_on_char separator s |>
        List.map (fun s ->
          try Scanf.sscanf s "%u.%u.%u.%u" (fun a b c d ->
            a lsl 24 lor b lsl 16 lor c lsl 8 lor d |>
            string_of_int)
          with _ -> s
        ) |>
        String.join cs |>
        print_endline ;
        loop ()

let () =
  loop ()
