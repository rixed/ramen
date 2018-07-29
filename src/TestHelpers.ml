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

let test_exn p s =
  match test_p p s with
  | Ok (r, _) -> r
  | Bad (Approximation _) -> failwith "approximation"
  | Bad (NoSolution e) ->
      failwith ("No solution ("^ IO.to_string print_error e ^")")
  | Bad (Ambiguous lst) ->
      failwith ("Ambiguous: "^ string_of_int (List.length lst) ^" results")

let test_op p s =
  match test_p p s with
  | Ok (res, _) as ok_res ->
    let params =
      [ RamenTuple.{
          ptyp = { typ_name = "avg_window" ;
                   typ = { structure = RamenTypes.TI32 ;
                           nullable = Some false } } ;
          value = RamenTypes.VI32 10l }] in
    RamenOperation.check params res ; ok_res
  | x -> x

let typ = RamenExpr.make_typ "replaced for tests"
let typn = RamenExpr.make_typ ~nullable:true "replaced for tests"

let rec map_type ?(recurs=true) f =
  let open RamenExpr in
  function
  | Const (t, a) -> Const (f t, a)
  | Tuple (t, es) ->
    Tuple (f t,
           if recurs then List.map (map_type ~recurs f) es else es)
  | Vector (t, es) ->
    Vector (f t,
            if recurs then List.map (map_type ~recurs f) es else es)
  | Field (t, a, b) -> Field (f t, a, b)
  | StateField _ as e -> e

  | Case (t, alts, else_) ->
    Case (f t,
          (if recurs then List.map (fun alt ->
             { case_cond = map_type ~recurs f alt.case_cond ;
               case_cons = map_type ~recurs f alt.case_cons }) alts
           else alts),
          if recurs then Option.map (map_type ~recurs f) else_ else else_)
  | Coalesce (t, es) ->
    Coalesce (f t,
              if recurs then List.map (map_type ~recurs f) es else es)

  | StatefulFun (t, g, AggrMin a) ->
    StatefulFun (f t, g, AggrMin (if recurs then map_type ~recurs f a else a))
  | StatefulFun (t, g, AggrMax a) ->
    StatefulFun (f t, g, AggrMax (if recurs then map_type ~recurs f a else a))
  | StatefulFun (t, g, AggrSum a) ->
    StatefulFun (f t, g, AggrSum (if recurs then map_type ~recurs f a else a))
  | StatefulFun (t, g, AggrAvg a) ->
    StatefulFun (f t, g, AggrAvg (if recurs then map_type ~recurs f a else a))
  | StatefulFun (t, g, AggrAnd a) ->
    StatefulFun (f t, g, AggrAnd (if recurs then map_type ~recurs f a else a))
  | StatefulFun (t, g, AggrOr a) ->
    StatefulFun (f t, g, AggrOr (if recurs then map_type ~recurs f a else a))
  | StatefulFun (t, g, AggrFirst a) ->
    StatefulFun (f t, g, AggrFirst (if recurs then map_type ~recurs f a else a))
  | StatefulFun (t, g, AggrLast a) ->
    StatefulFun (f t, g, AggrLast (if recurs then map_type ~recurs f a else a))
  | StatefulFun (t, g, AggrHistogram (a, min, max, num_buckets)) ->
    StatefulFun (f t, g, AggrHistogram (
        (if recurs then map_type ~recurs f a else a), min, max, num_buckets))
  | StatefulFun (t, g, AggrPercentile (a, b)) ->
    StatefulFun (f t, g, AggrPercentile (
        (if recurs then map_type ~recurs f a else a),
        (if recurs then map_type ~recurs f b else b)))
  | StatefulFun (t, g, Lag (a, b)) ->
    StatefulFun (f t, g, Lag (
        (if recurs then map_type ~recurs f a else a),
        (if recurs then map_type ~recurs f b else b)))
  | StatefulFun (t, g, MovingAvg (a, b, c)) ->
    StatefulFun (f t, g, MovingAvg (
        (if recurs then map_type ~recurs f a else a),
        (if recurs then map_type ~recurs f b else b),
        (if recurs then map_type ~recurs f c else c)))
  | StatefulFun (t, g, LinReg (a, b, c)) ->
    StatefulFun (f t, g, LinReg (
        (if recurs then map_type ~recurs f a else a),
        (if recurs then map_type ~recurs f b else b),
        (if recurs then map_type ~recurs f c else c)))
  | StatefulFun (t, g, MultiLinReg (a, b, c, d)) ->
    StatefulFun (f t, g, MultiLinReg (
        (if recurs then map_type ~recurs f a else a),
        (if recurs then map_type ~recurs f b else b),
        (if recurs then map_type ~recurs f c else c),
        (if recurs then List.map (map_type ~recurs f) d else d)))
  | StatefulFun (t, g, Remember (fpr, tim, dur, es)) ->
    StatefulFun (f t, g, Remember (
        (if recurs then map_type ~recurs f fpr else fpr),
        (if recurs then map_type ~recurs f tim else tim),
        (if recurs then map_type ~recurs f dur else dur),
        (if recurs then List.map (map_type ~recurs f) es else es)))
  | StatefulFun (t, g, Distinct es) ->
    StatefulFun (f t, g, Distinct
        (if recurs then List.map (map_type ~recurs f) es else es))
  | StatefulFun (t, g, ExpSmooth (a, b)) ->
    StatefulFun (f t, g, ExpSmooth (
        (if recurs then map_type ~recurs f a else a),
        (if recurs then map_type ~recurs f b else b)))
  | StatefulFun (t, g, Hysteresis (a, b, c)) ->
    StatefulFun (f t, g, Hysteresis (
        (if recurs then map_type ~recurs f a else a),
        (if recurs then map_type ~recurs f b else b),
        (if recurs then map_type ~recurs f c else c)))
  | StatefulFun (t, g, Top { want_rank ; n ; what ; by ; duration ; time }) ->
    StatefulFun (f t, g, Top {
      want_rank ;
      n = (if recurs then map_type ~recurs f n else n) ;
      duration = (if recurs then map_type ~recurs f duration else duration) ;
      what = (if recurs then List.map (map_type ~recurs f) what else what) ;
      by = (if recurs then map_type ~recurs f by else by) ;
      time = (if recurs then map_type ~recurs f time else time) })
  | StatefulFun (t, g, Last (n, e, es)) ->
    StatefulFun (f t, g, Last (n,
      (if recurs then map_type ~recurs f e else e),
      (if recurs then List.map (map_type ~recurs f) es else es)))

  | StatelessFun0 (t, cst) ->
    StatelessFun0 (f t, cst)

  | StatelessFun1 (t, cst, a) ->
    StatelessFun1 (f t, cst, (if recurs then map_type ~recurs f a else a))
  | StatelessFun2 (t, cst, a, b) ->
    StatelessFun2 (f t, cst,
        (if recurs then map_type ~recurs f a else a),
        (if recurs then map_type ~recurs f b else b))
  | StatelessFunMisc (t, Like (e, p)) ->
    StatelessFunMisc (f t, Like (
        (if recurs then map_type ~recurs f e else e), p))
  | StatelessFunMisc (t, Max es) ->
    StatelessFunMisc (f t, Max
      (if recurs then List.map (map_type ~recurs f) es else es))
  | StatelessFunMisc (t, Min es) ->
    StatelessFunMisc (f t, Min
      (if recurs then List.map (map_type ~recurs f) es else es))
  | StatelessFunMisc (t, Print es) ->
    StatelessFunMisc (f t, Print
      (if recurs then List.map (map_type ~recurs f) es else es))

  | GeneratorFun (t, Split (a, b)) ->
    GeneratorFun (f t, Split (
        (if recurs then map_type ~recurs f a else a),
        (if recurs then map_type ~recurs f b else b)))


let replace_typ e =
  map_type (fun t -> if t.nullable = Some true then typn
                               else typ) e

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
      merge = List.map replace_typ (fst merge), snd merge ;
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
      flush_how = (match flush_how with
        | Reset | Never | Slide _ -> flush_how
        | RemoveAll e -> RemoveAll (replace_typ e)
        | KeepOnly e -> KeepOnly (replace_typ e)) ;
      every ; factors }

  | ReadCSVFile ({ preprocessor ; _ } as csv) ->
      ReadCSVFile { csv with
        preprocessor = Option.map replace_typ csv.preprocessor ;
        where = { csv.where with fname = replace_typ csv.where.fname } }

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
