open Stdint

open Raql_error.DessserGen

let print_gen oc line column message =
  match line, column with
  | Some l, Some c ->
      BatPrintf.fprintf oc "Line %s, column %s: %s"
        (Uint32.to_string l)
        (Uint32.to_string c)
        message
  | Some l, None ->
      BatPrintf.fprintf oc "Line %s: %s"
        (Uint32.to_string l)
        message
  | _ ->
      BatPrintf.fprintf oc "%s" message

let print oc e =
  print_gen oc e.line e.column e.message

let make ?line ?column message =
  { line ; column ; message }
