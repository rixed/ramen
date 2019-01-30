open Batteries
open RamenParsing
module E = RamenExpr
module T = RamenTypes

let test_printer res_printer = function
  | Ok (res, (_, [])) ->
    Printf.sprintf "%s" (IO.to_string res_printer res)
  | Ok (res, (len, rest)) ->
    Printf.sprintf "%S, parsed_len=%d, rest=%s"
      (IO.to_string res_printer res) len
      (IO.to_string (List.print Char.print) rest)
  | Bad (Approximation _) ->
    "Approximation"
  | Bad (NoSolution e) ->
    Printf.sprintf "No solution (%s)" (IO.to_string print_error e)
  | Bad (Ambiguous lst) ->
    Printf.sprintf "%d solutions: %s"
      (List.length lst)
      (IO.to_string
        (List.print (fun oc (res, _corr, (_stream, lin, col)) ->
          Printf.fprintf oc "res=%a, pos=%d,%d"
            res_printer res
            lin col)) lst)

let strip_linecol = function
  | Ok (res, (x, _line, _col)) -> Ok (res, x)
  | Bad x -> Bad x

let test_p ?(postproc=identity) p s =
  (p +- eof) [] None Parsers.no_error_correction (PConfig.stream_of_string s) |>
  to_result |>
  strip_linecol |>
  Result.map (fun (r, rest) -> postproc r, rest)

let test_exn ?postproc p s =
  match test_p ?postproc p s with
  | Ok (r, _) -> r
  | Bad (Approximation _) -> failwith "approximation"
  | Bad (NoSolution e) ->
      failwith ("No solution ("^ IO.to_string print_error e ^")")
  | Bad (Ambiguous lst) ->
      failwith ("Ambiguous: "^ string_of_int (List.length lst) ^" results")

(* Same as above but output the pretty printed result to save us the many
 * changing minute details of the Expr/Types data types: *)
let test_expr ~printer p s =
  let str = PConfig.stream_of_string s in
  (p +- eof) [] None Parsers.no_error_correction str |>
  to_result |>
  strip_linecol |>
  test_printer printer

(* Given an alphabet of size [n], a zipf coefficient [s] (positive float, 0
 * being uniform and >1 being highly skewed), output an infinite text in
 * which characters appear at random with their zipfian frequency (numbers
 * from [0] to [n-1] are actually output, 0 being the most frequent one and
 * so on: *)
let zipf_distrib n s =
  let z k = 1. /. (float_of_int k)**s in
  let eta =
    let rec loop m eta =
      if m >= n then eta else loop (m + 1) (eta +. z m) in
    loop 1 0. in
  let f = Array.init n (fun i -> let k = i + 1 in z k /. eta) in
  (* Check than the sum of freqs is close to 1: *)
  let sum = Array.fold_left (+.) 0. f in
  assert (abs_float (sum -. 1.) < 0.01) ;
  let rand () =
    let rec lookup r i =
      if i >= Array.length f - 1 || r < f.(i) then i
      else lookup (r -. f.(i)) (i + 1) in
    lookup (Random.float 1.) 0
  in
  Enum.from rand
