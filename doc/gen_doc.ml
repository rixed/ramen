(* Given a textual content, returns an HTML page *)
open Batteries
open Html

let html_of_limitation l =
  p l

let html_of_example (input, output) =
  Html.Block [
    p [
      bold ("SELECT "^ input ^"…") ] ;
    p [ text ("  "^ output) ] ]

let of_expr e =
  let name = String.uppercase e.Expr.name in
  let see_also =
    try List.find_all (List.mem e.name) Expr.see_also |> List.flatten |>
        List.sort String.compare |> List.unique
    with Not_found -> [] in
  if not (String.ends_with e.short_descr ".") then
    Printf.eprintf "WARNING: %s: short description does not end with '.'!\n"
      name ;
  html [ title (e.name ^ " (RaQL expression)") ] (
    [ h1 name ;
      p [ text e.short_descr ] ;
      h2 "Syntax" ] @
    List.map (fun s -> p s) e.syntaxes @
    [ h2 "Typing" ] @
    List.map (fun s -> p s) e.typing @
    ( h2 "Description" ::
      e.long_descr @ (
        if e.deterministic then [] else [
          p [ text (
            "Warning: Parents of a function using "^ name ^" can not be \
             archived, as the "^ name ^" function is not deterministic. \
             For better results, use "^ name ^" as early as possible in the \
             processing stream.") ]
        ]) @
    (if e.limitations = [] then [] else [ h2 "Limitations" ]) @
    e.limitations @
    (if e.examples = [] then [] else [ h2 "Examples" ]) @
    List.map html_of_example e.examples @
    (if see_also = [] then [] else [ h2 "See Also" ]) @
    List.map (fun n ->
      if n <> e.name then p [ text n ] else Block []
    ) see_also))

let print_html_of_expr e oc =
  print_xml_head oc ;
  print oc (of_expr e)

(* Output the testto check the examples: *)
let print_test_of_expr e oc =
  Printf.fprintf oc "{\n" ;
  Printf.fprintf oc "  programs = [ { src = \"%s.ramen\" } ];\n" e.Expr.name ;
  Printf.fprintf oc "  outputs = {\n" ;
  Printf.fprintf oc "    \"%s/f\" => {\n" e.name ;
  Printf.fprintf oc "      timeout = 3;\n" ;
  Printf.fprintf oc "      present = [\n" ;
  List.mapi (fun i (_, output) ->
    Printf.sprintf "        { \"output_%d\" => %S }" i output
  ) e.examples |> String.join ";\n" |> String.print oc ;
  Printf.fprintf oc "\n      ]\n" ;
  Printf.fprintf oc "    }\n" ;
  Printf.fprintf oc "  }\n" ;
  Printf.fprintf oc "}\n"

(* Output the ramen file to use with the above test *)
let print_raql_of_expr e oc =
  Printf.fprintf oc "DEFINE f AS\n" ;
  Printf.fprintf oc "  SELECT\n" ;
  List.mapi (fun i (input, _) ->
    Printf.sprintf "    %s AS output_%d" input i
  ) e.Expr.examples |> String.join ",\n" |> String.print oc ;
  Printf.fprintf oc "\n" ;
  Printf.fprintf oc "  EVERY 0.1s;\n"

let with_file target f =
  Printf.printf "Generating %s...\n" target ;
  File.with_file_out target f

let expr_of_name name =
  try
    List.find (fun e -> e.Expr.name = name) Expr.exprs
  with Not_found ->
    Printf.eprintf "No such expression: %S\n" name ;
    exit 1

let process source filename ext target =
  match source, ext with
  | "raql", "html" ->
      with_file target (print_html_of_expr (expr_of_name filename))
  | "raql", "test" ->
      let e = expr_of_name filename in
      assert (e.Expr.deterministic) ;
      assert (e.examples <> []) ;
      with_file target (print_test_of_expr e)
  | "raql", "ramen" ->
      let e = expr_of_name filename in
      assert (e.Expr.deterministic) ;
      assert (e.examples <> []) ;
      with_file target (print_raql_of_expr e)
  | _ ->
      Printf.eprintf "Unknown object %S or target %S\n" source ext ;
      exit 1

let () =
  if Array.length Sys.argv <> 2 then (
    Printf.eprintf "gen_doc file.html\n" ;
    exit 1) ;
  let target = Sys.argv.(1) in
  match String.rsplit ~by:"/" target with
  | exception Not_found ->
      Printf.eprintf "Missing directory, don't know what to do.\n" ;
      exit 1
  | dirname, filename ->
      (match String.rsplit ~by:"." filename with
      | exception Not_found ->
          Printf.eprintf "Missing file extension.\n" ;
          exit 1
      | basename, ext ->
          process (Filename.basename dirname) basename ext target)
