open Stdint

open Raql_warning.DessserGen

let print oc w =
  RamenRaqlError.print_gen oc w.line w.column w.message

let make ?line ?column message =
  { line ; column ; message }
