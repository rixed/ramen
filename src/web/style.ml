(* Some html generation helpers common to both ramen_app and alerter_app. *)
open RamenHtml

let icon_class ?(actionable=true) ?help sel =
  let attrs =
    [ clss ("icon"^ (if actionable then " actionable" else "")
                  ^ (if sel then " selected" else "")) ] in
  match help with
  | None -> attrs
  | Some h -> attr "title" h :: attrs
