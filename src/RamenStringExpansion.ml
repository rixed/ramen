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
  let list_uniq name = function
    | [ v ] -> v
    | _ ->
        Printf.sprintf2 "Function %s takes only 1 argument" name |>
        failwith in
  let filter_of_name ?null = function
    | "int" ->
        List.singleton % string_of_int % int_of_float %
          float_of_string % list_uniq "int"
    | "date" ->
        List.singleton % string_of_time % float_of_string % list_uniq "date"
    | "trim" ->
        List.singleton % String.trim % list_uniq "trim"
    (* Special syntax for trinary operator: ${a?b:c} will be b or c depending
     * on the truth-ness of a: *)
    | f when String.length f > 1 && f.[0] = '?' ->
        (match String.(split ~by:":" (lchop f)) with
        | exception Not_found ->
            failwith "syntax of ternary filter is: \"?if_true:if_false\""
        | if_true, if_false ->
            fun vs ->
              let v = list_uniq ":?" vs in
              [ if v = "" || v = "0" || v = "false" || null = Some v
              then if_false else if_true ])
    | _ -> failwith "unknown filter" in
  let open Str in
  let re =
    regexp "\\${\\([_a-zA-Z][-_a-zA-Z0-9|?: ]*\\)}" in
  fun dict ?(quote=identity) ?null text ->
    global_substitute re (fun s ->
      let var_exprs = matched_group 1 s in
      let var_names, filters =
        let lst = string_split_on_char '|' var_exprs in
        assert (lst <> []) ;
        List.hd lst, List.tl lst in
      let var_names = string_split_on_char ',' var_names in
      let var_vals =
        List.map (fun var_name ->
          try List.assoc var_name dict
          with Not_found ->
            !logger.warning "Unknown parameter %S" var_name ;
            null |? "??"^ var_name ^"??"
        ) var_names in
      List.fold_left (fun vs filter_name ->
        let filter = filter_of_name ?null filter_name in
        try filter vs
        with e ->
          !logger.warning "Cannot filter %a through %s: %s"
            (List.print String.print_quoted) vs
            filter_name (Printexc.to_string e) ;
          vs
      ) var_vals filters |>
      String.join "," |>
      quote
    ) text

(*$= subst_dict & ~printer:(fun x -> x)
  "glop 'pas' glop" \
      (subst_dict ~quote:shell_quote ["glop", "pas"] "glop ${glop} glop")
  "pas"           (subst_dict ["glop", "pas"] "${glop}")
  "??"            (subst_dict ~null:"??" ["glop", "pas"] "${gloup}")
  "123"           (subst_dict ["f", "123.456"] "${f|int}")
  "2019-11-29"    (let s = subst_dict ["t", "1575039473.9"] "${t|int|date}" in \
                   String.sub s 0 10)
  "glop"          (subst_dict ["f", "1"] "${f|?glop:pas glop}")
  "pas glop"      (subst_dict ["f", "0"] "${f|?glop:pas glop}")
  "pas glop"      (subst_dict ["f", ""] "${f|?glop:pas glop}")
  "glop"          (subst_dict ["f", " \tglop  "] "${f|trim}")
 *)
