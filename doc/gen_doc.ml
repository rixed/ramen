(* Given a textual content, returns an HTML page *)
open Batteries
open Html

let html_of_limitation l =
  p l

let html_of_example (input, output) =
  Block [
    p [
      bold ("SELECT "^ input ^"…") ] ;
    p [ cdata ("  "^ output) ] ]

let expr_of_name name =
  let filenamify s =
    String.map (fun c ->
      if Char.is_whitespace c then '_' else c
    ) s in
  try
    List.find (fun e -> filenamify e.Expr.name = name) Expr.exprs
  with Not_found ->
    Printf.eprintf "No such expression: %S\n" name ;
    exit 1

let link_to e =
  let href = e.Expr.name ^".html" in
  a href [ cdata e.short_descr ]

let html_of_expr e =
  let name = String.uppercase e.Expr.name in
  let state_expl =
    [ p [ cdata "this is an aggregate function. As such, it accepts a \
                 single operand which can wither be a scalar, in which \
                 case it will operate in turn on each item of the group, \
                 or an array or a vector in which case it will operate on \
                 each value in sequence and return the result (in practice, \
                 this process is delayed until the group is submitted for \
                 performance reasons)" ] ;
      p [ cdata "User can choose to skip over NULL values or to include \
                 them in the computation with one of the modifiers " ;
          emph "SKIP NULLS" ; cdata " to skip NULL values (the default) and " ;
          emph "KEEP NULLS" ; cdata " to include them." ] ;
      p [ cdata "In the first case the result will still be NULL if all input \
                 values are NULL, and in the last case any NULL value will
                 make the result NULL." ] ;
      p [ cdata "The other modifier tells whether the state used to compute \
                 the aggregate must be local (each group has its own \
                 independent state) or global (all groups share a single \
                 state). In general when using a GROUP-BY clause the former \
                 behavior is intended, and it is thus the default when an \
                 explicit GROUP-BY clause is present. Otherwise, the default \
                 is to use only one global state." ] ;
      p [ cdata "One can choose between those two with the modifier " ;
          emph "LOCALLY" ; cdata " to force a group-wise state and " ;
          emph "GLOBALLY" ; cdata " to force a global state." ] ;
      p [ cdata "This choice of the state lifespan is only meaningful when \
                 the operation is applied to a single scalar value, since \
                 the state required to compute the end result over a \
                 literal array or vector lives only as long as that \
                 computation." ] ] in
  let see_also =
    try List.find_all (List.mem e.name) Expr.see_also |> List.flatten |>
        List.sort String.compare |> List.unique
    with Not_found -> [] in
  if String.ends_with e.short_descr "." then
    Printf.eprintf "WARNING: %s: short description ends with '.'!\n"
      name ;
  html [ title (e.name ^ " (RaQL expression)") ;
         tag ~attrs:["charset", "utf-8"] "meta" [] ] (
    ( h1 e.short_descr ::
      if e.has_state then state_expl else [] ) @
    [ h2 "Syntax" ] @
    List.map (fun s -> p s) e.syntaxes @
    [ h2 "Typing" ] @
    List.map (fun s -> p s) e.typing @
    ( h2 "Description" ::
      e.long_descr @ (
        if e.deterministic then [] else [
          p [ cdata (
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
      let e' = expr_of_name n in
      if e'.name <> e.name then link_to e' else Block []
    ) see_also))

let print_html_of_expr e oc =
  print_xml_head oc ;
  print oc (html_of_expr e)

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

let print_html_toc oc =
  let sections = Hashtbl.create 50 in
  List.iter (fun e ->
    List.iter (fun section ->
      Hashtbl.modify_opt section (function
        | None -> Some [ e.Expr.name ]
        | Some names -> Some (e.Expr.name :: names)
      ) sections
    ) e.Expr.sections
  ) Expr.exprs ;
  print_xml_head oc ;
  let blocks =
    Hashtbl.fold (fun section_name section_exprs blocks ->
      (
        section_name,
        ul (
          List.map (fun name ->
            let e = expr_of_name name in
            li [ link_to e ]
          ) section_exprs)
      ) :: blocks
    ) sections [] in
  html [ title "Table Of Content" ;
         tag ~attrs:["charset", "utf-8"] "meta" [] ] (
    List.sort (fun (a, _) (b, _) -> String.compare a b) blocks |>
    List.map (fun (name, blk) ->
      Block [ h1 (String.lchop ~n:3 name) ; blk ])
  ) |>
  print oc

let print_raql_qt_resources oc =
  Printf.fprintf oc "<RCC>\n" ;
  Printf.fprintf oc "  <qresource>\n" ;
  Printf.fprintf oc "    <file>toc.html</file>\n" ;
  List.iter (fun e ->
    Printf.fprintf oc "    <file>%s.html</file>\n" e.Expr.name
  ) Expr.exprs ;
  Printf.fprintf oc "  </qresource>\n" ;
  Printf.fprintf oc "</RCC>\n"

let with_file target f =
  Printf.printf "Generating %s...\n" target ;
  File.with_file_out target f

let process source name ext target =
  match source, name, ext with
  | "raql", "toc", "html" ->
      with_file target print_html_toc
  | "raql", _, "html" ->
      with_file target (print_html_of_expr (expr_of_name name))
  | "raql", _, "test" ->
      let e = expr_of_name name in
      assert (e.Expr.deterministic) ;
      assert (e.examples <> []) ;
      with_file target (print_test_of_expr e)
  | "raql", _, "ramen" ->
      let e = expr_of_name name in
      assert (e.Expr.deterministic) ;
      assert (e.examples <> []) ;
      with_file target (print_raql_of_expr e)
  | _, _, "qrc" ->
      (* Qt resource file *)
      with_file target print_raql_qt_resources
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
