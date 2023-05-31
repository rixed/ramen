(* Poor man's templating engine for alerting message texts: *)
open Batteries
open RamenLog
open RamenHelpersNoLog

(*$< Batteries *)
(*$< RamenHelpersNoLog *)

exception UndefVar of string

(* Replace a map of keys by their values in a string.
 * Keys are delimited in the string with "${" and "}".
 * Used to replace notification parameters by their values.
 * Optionally, one (or several) formatter(s) may be appended after a pipe, as
 * in "${yadayada|int|date}" *)
let subst_dict =
  let open Str in
  let re =
    regexp "\\${\\([-_a-zA-Z0-9|?:,.*/+= ]*\\)}" in
  fun dict ?null text ->
    let to_value var_name =
      try Some (List.assoc var_name dict)
      with Not_found ->
        (* Maybe this is actually an immediate value of some sort? *)
        let var_name = String.trim var_name in
        (match float_of_string var_name with
        | exception Failure _ ->
            None
        | _ ->
            Some var_name) in
    let force var_name = function
      | None -> raise (UndefVar var_name)
      | Some v -> v in
    let foreach f = List.map (fun (n, v) -> n, Option.map f v) in
    let arithmetic op f =
      let b = String.lchop ~n:2 f |> float_of_string in
      foreach (fun v -> string_of_float (op (float_of_string v) b)) in
    let binary_filter name op = function
      | [ _, Some v1 ; _, Some v2 ] ->
          [ name, Some (nice_string_of_float (op (float_of_string v1)
                                                 (float_of_string v2))) ]
      | [ _ ; _ ] ->
          [ name, None ]
      | _ ->
          failwith ("bad arity for operator "^ name) in
    let var_is_null v = v = Some "" || v = Some "0" || v = Some "false" || v = None in
    let filter_of_name = function
      | "int" ->
          foreach (string_of_int % int_of_float % float_of_string)
      | "float" ->
          foreach (nice_string_of_float % float_of_string)
      | "round" ->
          foreach (nice_string_of_float % Float.round % float_of_string)
      | "ceil" ->
          foreach (nice_string_of_float % Float.ceil % float_of_string)
      | "floor" ->
          foreach (nice_string_of_float % Float.floor % float_of_string)
      | "date" ->
          foreach (string_of_time % float_of_string)
      | "trim" ->
          foreach String.trim
      | "percent" ->
          foreach (nice_string_of_float % (( *. ) 100.) % float_of_string)
      (* Some binary operators: *)
      | "sum" ->
          binary_filter "sum" (+.)
      | "diff" ->
          binary_filter "diff" (-.)
      | "coalesce" ->
          let coalesce = List.find_map (fun (n, v) ->
            if var_is_null v then None else Some (n, v)) in
          fun vars -> [coalesce vars]
      (* Special syntax for trinary operator: ${a|?b:c} will be b or c
       * depending on the truth-ness of a: (ocaml parser: |a})*)
      | f when String.length f > 1 && f.[0] = '?' ->
          (match String.(split ~by:":" (lchop f)) with
          | exception Not_found ->
              failwith "syntax of ternary filter is: \"?if_true:if_false\""
          | if_true, if_false ->
              List.map (fun (n, v) ->
                n,
                if var_is_null v then Some if_false else Some if_true))
      (* Some arithmetic operations useful to manipulate scales: *)
      | f when String.length f > 2 && f.[0] = '*' && f.[1] = '=' ->
          arithmetic ( *. ) f
      | f when String.length f > 2 && f.[0] = '/' && f.[1] = '=' ->
          arithmetic ( /. ) f
      | f when String.length f > 2 && f.[0] = '+' && f.[1] = '=' ->
          arithmetic ( +. ) f
      | f when String.length f > 2 && f.[0] = '-' && f.[1] = '=' ->
          arithmetic ( -. ) f
      (* Escaping is explicit: *)
      | "sql" ->
          foreach sql_quote
      | "shell" ->
          foreach shell_quote
      | "json" ->
          foreach json_quote
      (* A way to write indiscriminately every fields as a dictionary in a given
       * format (json...) *)
      | "json-dict" ->
          fun vars ->
            [ "json", Some ("{"^
                String.join "," (
                  List.map (fun (n, v) ->
                    json_quote n ^":"^
                    (match v with None -> force n null | Some v -> json_quote v)
                  ) vars)
              ^"}") ]
      | n ->
          failwith ("unknown filter '"^ n ^"'") in
    let missings = ref Set.String.empty in
    let rec substitute_inner text =
      let text' =
        global_substitute re (fun s ->
          let var_exprs = matched_group 1 s in
          let var_names, filters =
            let lst = string_split_on_char '|' var_exprs in
            assert (lst <> []) ;
            List.hd lst, List.tl lst in
          let var_names =
            if var_names = "*" then
              List.map fst dict
            else
              string_split_on_char ',' var_names in
          try
            let vars = List.map (fun n -> n, to_value n) var_names in
            List.fold_left (fun vars filter_name ->
              let filter = filter_of_name filter_name in
              try filter vars
              with e ->
                !logger.warning "Cannot filter %a through %s: %s"
                  (List.print String.print_quoted) (List.map fst vars)
                  filter_name (Printexc.to_string e) ;
                (* Fallback: keep input values: *)
                vars
            ) vars filters |>
            (* Drop the var names at that point: *)
            List.map (fun (n, v) -> force n v) |>
            String.join ","
          with UndefVar var_name ->
            missings := Set.String.add var_name !missings ;
            null |? "??"^ var_name ^"??"
        ) text in
      if text' = text then text else substitute_inner text'
    in
    let text' = substitute_inner text in
    if not (Set.String.is_empty !missings) then
      !logger.warning "Unknown parameter%s: %a"
        (if Set.String.cardinal !missings > 1 then "s" else "")
        (pretty_enum_print String.print) (Set.String.enum !missings) ;
    text'

(*$= subst_dict & ~printer:(fun x -> x)
  "glop 'pas' glop" \
      (subst_dict ["glop", "pas"] "glop ${glop|shell} glop")
  "pas"           (subst_dict ["glop", "pas"] "${glop}")
  "?"             (subst_dict ~null:"?" ["glop", "pas"] "${gloup}")
  "?"             (subst_dict ~null:"?" ["glop", "pas"] "${gloup|trim}")
  "{\"gloup\":null}" \
                  (subst_dict ~null:"null" ["glop", "pas"] "${gloup|json-dict}")
  "{\"gloup\":null,\"glop\":\"pas\"}" \
                  (subst_dict ~null:"null" ["glop", "pas"] "${gloup,glop|json-dict}")
  "123"           (subst_dict ["f", "123.456"] "${f|int}")
  "2019-11-29"    (let s = subst_dict ["t", "1575039473.9"] "${t|int|date}" in \
                   String.sub s 0 10)
  "glop"          (subst_dict ["f", "1"] "${f|?glop:pas glop}")
  "pas glop"      (subst_dict ["f", "0"] "${f|?glop:pas glop}")
  "pas glop"      (subst_dict ["f", ""] "${f|?glop:pas glop}")
  "glop"          (subst_dict ["f", " \tglop  "] "${f|trim}")
  "{\"a\":\"1\",\"b\":\"2\"}" \
                  (subst_dict ["a", "1"; "b", "2"] "${a,b|json-dict}")
  "{\"a\":\"1\",\"b\":\"2\"}" \
                  (subst_dict ["a", "1"; "b", "2"] "${*|json-dict}")
  "{\"a\":\"pas\",\"b\":\"glop\"}" \
                  (subst_dict ["a", " pas "; "b", " \tglop "] "${a,b|trim|json-dict}")
  "{\"a\":\"pas\",\"b\":\"glop\"}" \
                  (subst_dict ["a", " pas "; "b", " \tglop "] "${*|trim|json-dict}")
  "1.2,2.4"       (subst_dict ["a", "1.2"; "b", "2.4"] "${a,b}")
  "1,2"           (subst_dict ["a", "1.2"; "b", "2.4"] "${a,b|int}")
  "25"            (subst_dict ["f", ".25"] "${f|percent}")
  "?"             (subst_dict ~null:"?" ["a", "1"] "${b|int}")
  "unset"         (subst_dict ~null:"?" ["a", "1"] "${b|int|?set:unset}")
  "42"            (subst_dict ["a", "6"] "${a|*=7|int}")
  "42"            (subst_dict ["a", "50"; "b", "8"] "${a,b|diff|int}")
  "42"            (subst_dict ["a", "40"; "b", "2"] "${a,b|sum|int}")
  "42"            (subst_dict [] "${42}")
  "42"            (subst_dict [] "${42|int}")
  "42"            (subst_dict [] "${ 42 |int}")
  "42"            (subst_dict [] "${42.1|round}")
  "42"            (subst_dict [] "${41.9|round}")
  "42"            (subst_dict [] "${41.5|ceil}")
  "42"            (subst_dict [] "${42.9|floor}")
  "42"            (subst_dict ["a", "21"] "${a,21|sum|int}")
  "42"            (subst_dict ["a", "21"] "${a,${a}|sum|int}")
  "glop"          (subst_dict ["a", "glop"] "${a|?${a}:pas glop}")
  "'glop'"        (subst_dict ["a", "glop"] "${a|?${a}:pas glop|shell}")
  "pas glop"      (subst_dict [] "${a|?${a}:pas glop}")
  "'pas glop'"    (subst_dict [] "${a|?${a}:pas glop|shell}")
  "42"            (subst_dict ["gl.op", "42"] "${gl.op|int}")
  "a"             (subst_dict ["a", "X"] "${a|?a:b}")
  "b"             (subst_dict ["b", "X"] "${a|?a:b}")
  "X"             (subst_dict ["a", "X"] "${${a|?a:b}}")
  "X"             (subst_dict ["b", "X"] "${${a|?a:b}}")
  "42"            (subst_dict ["a", " 42 "] "${${${a|?a:b}}|int}")
  "42"            (subst_dict ["b", " 42 "] "${${${a|?a:b}}|int}")
  "42"            (subst_dict ["a", " 42 "] "${${a|?${a}:${b}}|int}")
  "42"            (subst_dict ["b", " 42 "] "${${a|?${a}:${b}}|int}")
  "X"             (subst_dict ["a", "X"] "${a|coalesce}")
  "Y"             (subst_dict ["b", "Y"] "${a,b|coalesce}")
  "Z"             (subst_dict ["c", "Z"] "${a,b,c|coalesce}")
  "Y"             (subst_dict ["b", "Y"; "c" , "Z"] "${a,b,c|coalesce}")
  "?"             (subst_dict ~null:"?" [] "${a,b,c|coalesce}")
 *)
