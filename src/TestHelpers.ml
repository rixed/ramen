open Batteries
open RamenSharedTypes
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
        (List.print (fun fmt (res,_c,_s) ->
          res_printer fmt res)) lst)

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
    RamenOperation.check res ; ok_res
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
                force_export ; notify_url ; key ; top ; commit_before ;
                commit_when ; flush_how ; from } ->
    Aggregate {
      fields =
        List.map (fun sf ->
          { sf with expr = replace_typ sf.expr }) fields ;
      and_all_others ;
      merge = List.map replace_typ merge ;
      sort =
        Option.map (fun (n, u, b) ->
          n, Option.map replace_typ u, List.map replace_typ b) sort ;
      where = replace_typ where ;
      event_time ; force_export ; notify_url ; from ;
      key = List.map replace_typ key ;
      top = Option.map (fun (n, e) -> replace_typ n, replace_typ e) top ;
      commit_when = replace_typ commit_when ;
      commit_before = commit_before ;
      flush_how = (match flush_how with
        | Reset | Never | Slide _ -> flush_how
        | RemoveAll e -> RemoveAll (replace_typ e)
        | KeepOnly e -> KeepOnly (replace_typ e)) }
  | Yield { fields ; every ; force_export ; event_time } ->
    Yield { fields = List.map (fun sf -> { sf with expr = replace_typ sf.expr }) fields ;
            every ; force_export ; event_time }
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
