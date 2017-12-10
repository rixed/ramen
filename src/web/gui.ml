(* Helpers for user interaction.
 * Model: a single param is editable via several inputs.
 * Modifications of each of those is handled by a function returning
 * the new value from the old one and the input value. *)
open Engine
open RamenHtml

let input_text ?label ?width ?height ?placeholder ~param ~next_state a txt =
  let action v =
    next_state param.value v |>
    set param in
  let a = (match height with Some h -> attr "height" (string_of_int h)
                           | None -> attr "type" "text") :: a in
  let a = match width, height with
          | Some w, Some _ -> attr "width" (string_of_int w) :: a
          | Some w, None -> attr "size" (string_of_int w) :: a
          | None, _ -> a in
  let a = match placeholder with
          | Some p -> attr "placeholder" p :: a
          | None -> a in
  let i = match height with
          | Some _ -> textarea ~action a [ text txt ]
          | None -> input ~action (attr "value" txt :: a) in
  match label with
  | Some l -> elmt "label" [] [ text l ; i ]
  | None -> i

let button ~param ~next_state a subs =
  let action _ =
    next_state param.value |>
    set param in
  button ~action a subs

let select_box ~param ~next_state ?selected opts =
  let action v =
    next_state param.value v |>
    set param in
  select_box ~action ?selected opts
