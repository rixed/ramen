open Batteries
open RamenParsing

let test_printer res_printer = function
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

let test_p p s =
  (p +- eof) [] None Parsers.no_error_correction (PConfig.stream_of_string s) |>
  to_result |>
  strip_linecol

let test_op p s =
  match test_p p s with
  | Ok (res, _) as ok_res ->
    RamenOperation.check ["avg_window", RamenTypes.VI32 10l] res ; ok_res
  | x -> x

let typ = RamenExpr.make_typ "replaced for tests"
let typn = RamenExpr.make_typ ~nullable:true "replaced for tests"

let replace_typ e =
  RamenExpr.map_type (fun t -> if t.nullable = Some true then typn
                               else typ) e

let replace_typ_in_expr = function
  | Ok (expr, rest) -> Ok (replace_typ expr, rest)
  | x -> x

let replace_typ_in_operation =
  let open RamenOperation in
  function
  | Aggregate { fields ; and_all_others ; merge ; sort ; where ; event_time ;
                notifications ; key ; commit_before ;
                commit_when ; flush_how ; from ; every ; factors } ->
    Aggregate {
      fields =
        List.map (fun sf ->
          { sf with expr = replace_typ sf.expr }) fields ;
      and_all_others ;
      merge = List.map replace_typ (fst merge), snd merge ;
      sort =
        Option.map (fun (n, u, b) ->
          n, Option.map replace_typ u, List.map replace_typ b) sort ;
      where = replace_typ where ;
      event_time ; notifications ; from ;
      key = List.map replace_typ key ;
      commit_when = replace_typ commit_when ;
      commit_before = commit_before ;
      flush_how = (match flush_how with
        | Reset | Never | Slide _ -> flush_how
        | RemoveAll e -> RemoveAll (replace_typ e)
        | KeepOnly e -> KeepOnly (replace_typ e)) ;
      every ; factors }
  | x -> x

let replace_typ_in_op = function
  | Ok (op, rest) -> Ok (replace_typ_in_operation op, rest)
  | x -> x

let replace_typ_in_program =
  function
  | Ok (prog, rest) ->
    Ok (
      List.map (fun func ->
        RamenProgram.{ func with operation = replace_typ_in_operation func.RamenProgram.operation }
      ) prog,
      rest)
  | x -> x

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
