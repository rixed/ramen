(* Small tools used in the WWW-UI that does not depend in Js_of_ocaml
 * and that we can therefore link with the tests *)

(* Stdlib complement: *)

let identity x = x

let apply f = f ()

let option_may f = function
  | None -> ()
  | Some x -> f x

let option_map f = function
  | None -> None
  | Some x -> Some (f x)

let option_def x = function None -> x | Some v -> v
let (|?) a b = option_def b a

let list_init n f =
  let rec loop prev i =
    if i >= n then List.rev prev else
    loop (f i :: prev) (i + 1) in
  loop [] 0

let replace_assoc n v l = (n, v) :: List.remove_assoc n l

let string_starts_with p s =
  let open String in
  length s >= length p &&
  sub s 0 (length p) = p

let string_contains p s = s##indexOf p <> -1

let list_fold_lefti f init l =
  List.fold_left (fun (i, prev) x -> i+1, f i prev x) (0, init) l |>
  snd

let list_map_filter f l =
  let rec loop prev = function
    | [] -> List.rev prev
    | x :: rest ->
      let prev =
        match f x with None -> prev
                     | Some y -> y :: prev in
      loop prev rest in
  loop [] l
