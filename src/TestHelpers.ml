open Batteries
open RamenParsing

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

let test_op ?postproc p s =
  match test_p ?postproc p s with
  | Ok (res, _) as ok_res ->
    let params =
      [ RamenTuple.{
          ptyp = { typ_name = "avg_window" ;
                   typ = { structure = RamenTypes.TI32 ;
                           nullable = false } ;
                   units = None ; doc = "" ; aggr = None } ;
          value = RamenTypes.VI32 10l }] in
    RamenOperation.check params res ; ok_res
  | x -> x

let typ =
  RamenExpr.make_typ "replaced for tests"

let replace_typ e =
  RamenExpr.map_type (fun _ -> typ) e

let replace_typ_in_expr = function
  | Ok (expr, rest) -> Ok (replace_typ expr, rest)
  | x -> x

let replace_typ_in_operation =
  let open RamenOperation in
  function
  | Aggregate { fields ; and_all_others ; merge ; sort ; where ; event_time ;
                notifications ; key ; commit_before ;
                commit_cond ; flush_how ; from ; every ; factors } ->
    Aggregate {
      fields =
        List.map (fun sf ->
          { sf with expr = replace_typ sf.expr }) fields ;
      and_all_others ;
      merge = { merge with on = List.map replace_typ merge.on } ;
      sort =
        Option.map (fun (n, u, b) ->
          n, Option.map replace_typ u, List.map replace_typ b) sort ;
      where = replace_typ where ;
      event_time ;
      notifications =
        List.map (fun n ->
          { notif_name = replace_typ n.notif_name ;
            parameters = List.map (fun (n, v) ->
                           n, replace_typ v
                         ) n.parameters }
        ) notifications ;
      from ;
      key = List.map replace_typ key ;
      commit_cond = replace_typ commit_cond ;
      commit_before = commit_before ;
      flush_how ; every ; factors }

  | ReadCSVFile csv ->
      ReadCSVFile { csv with
        preprocessor = Option.map replace_typ csv.preprocessor ;
        where = { fname = replace_typ csv.where.fname ;
                  unlink = replace_typ csv.where.unlink } }

  | x -> x

let replace_typ_in_op = function
  | Ok (op, rest) -> Ok (replace_typ_in_operation op, rest)
  | x -> x

let replace_typ_in_program =
  function
  | Ok ((params, prog), rest) ->
    Ok ((
      params,
      List.map (fun func ->
        RamenProgram.{
          func with operation =
            replace_typ_in_operation func.RamenProgram.operation }
      ) prog),
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
