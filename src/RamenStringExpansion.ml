(* Poor man's templating engine for alerting message texts: *)
open Batteries
open RamenLog
open RamenHelpers

(*$< Batteries *)
(*$< RamenHelpers *)

(* Replace a map of keys by their values in a string.
 * Keys are delimited in the string with "${" and "}".
 * Used to replace notification parameters by their values.
 * Optionally, one (or several) formatter(s) may be appended after a pipe, as
 * in "${yadayada|int|date}" *)
let subst_dict =
  let open Str in
  let re =
    regexp "\\${\\([_a-zA-Z*][-_a-zA-Z0-9|?:, ]*\\)}" in
  fun dict ?null text ->
    let to_value var_name =
      try List.assoc var_name dict
      with Not_found ->
        !logger.warning "Unknown parameter %S" var_name ;
        null |? "??"^ var_name ^"??" in
    let foreach f = List.map (fun (n, v) -> n, f v) in
    let filter_of_name ?null = function
      | "int" ->
          foreach (string_of_int % int_of_float % float_of_string)
      | "date" ->
          foreach (string_of_time % float_of_string)
      | "trim" ->
          foreach String.trim
      (* Special syntax for trinary operator: ${a?b:c} will be b or c depending
       * on the truth-ness of a: *)
      | f when String.length f > 1 && f.[0] = '?' ->
          (match String.(split ~by:":" (lchop f)) with
          | exception Not_found ->
              failwith "syntax of ternary filter is: \"?if_true:if_false\""
          | if_true, if_false ->
              foreach (fun v ->
                if v = "" || v = "0" || v = "false" || null = Some v
                then if_false else if_true))
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
            [ "json", "{"^
                String.join "," (
                  List.map (fun (n, v) ->
                    json_quote n ^":"^ json_quote v
                  ) vars)
              ^"}" ]
      | _ ->
          failwith "unknown filter" in
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
      let vars = List.map (fun n -> n, to_value n) var_names in
      List.fold_left (fun vars filter_name ->
        let filter = filter_of_name ?null filter_name in
        try filter vars
        with e ->
          !logger.warning "Cannot filter %a through %s: %s"
            (List.print String.print_quoted) (List.map fst vars)
            filter_name (Printexc.to_string e) ;
          (* Fallback: keep input values: *)
          vars
      ) vars filters |>
      List.map snd |> (* drop the var names at that point *)
      String.join ","
    ) text

(*$= subst_dict & ~printer:(fun x -> x)
  "glop 'pas' glop" \
      (subst_dict ["glop", "pas"] "glop ${glop|shell} glop")
  "pas"           (subst_dict ["glop", "pas"] "${glop}")
  "??"            (subst_dict ~null:"??" ["glop", "pas"] "${gloup}")
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
 *)
