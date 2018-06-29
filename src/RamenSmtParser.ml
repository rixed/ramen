(*
 * See http://smtlib.cs.uiowa.edu/papers/smt-lib-reference-v2.6-r2017-07-18.pdf,
 * esp. chapters 3.1 and 3.2
 *)
open Batteries
open RamenParsing

(*$inject
  open Batteries
  open TestHelpers
*)

let comment =
  char ';' -- repeat_greedy ~sep:none ~what:"comment" all_but_newline

let is_whitespace c = c = ' ' || c = '\t' || c = '\r' || c = '\n'

let whitespace = cond "white space" is_whitespace ' '

let is_digit c = c >= '0' && c <= '9'

let digit_chr = cond "digit" is_digit '0'

let num_of_char c = Char.code c - Char.code '0'

let digit = digit_chr >>: num_of_char

let hex_digit =
  (cond "hex-digit-lo" (fun c -> c >= 'a' && c <= 'f') 'a' >>: fun c ->
    10 + Char.code c - Char.code 'a') |||
  (cond "hex-digit-hi" (fun c -> c >= 'A' && c <= 'F') 'A' >>: fun c ->
    10 + Char.code c - Char.code 'A')

let letter = cond "letter" (fun c ->
  (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) 'A'

let int_of_lst lst =
  List.fold_left (fun n d -> n * 10 + d) 0 lst

let zero = char '0' >>: fun _ -> 0
let one = char '1' >>: fun _ -> 1

let numeral_lst m =
  let m = "numeral" :: m in
  let non_zero =
    cond "non-zero" (fun c -> c >= '1' && c <= '9') '1' >>: num_of_char in
  (
    (zero >>: fun _ -> [0]) |||
    (non_zero ++ repeat ~sep:none digit >>: fun (h, t) -> h :: t)
  ) m

let numeral = numeral_lst >>: int_of_lst

(*$= numeral & ~printer:string_of_int
    314 (test_exn numeral "314")
  12345 (test_exn numeral "12345")
  12005 (test_exn numeral "12005")
*)

let decimal m =
  let m = "decimal" :: m in
  (
    numeral +- char '.' ++ repeat ~sep:none zero ++ numeral_lst >>:
      fun ((n, zs), f) ->
        float_of_int n +.
        float_of_int (int_of_lst f) *.
        10. ** (~-. (float_of_int (List.length zs + List.length f)))
  ) m

(*$= decimal & ~printer:string_of_float
    3.14 (test_exn decimal "3.14")
  1234.5 (test_exn decimal "1234.5")
  12.005 (test_exn decimal "12.005")
*)

let hexadecimal m =
  let m = "hexadecimal" :: m in
  (
    char '#' -- char 'x' -+ several ~sep:none (digit ||| hex_digit) >>:
      List.fold_left (fun s d -> s * 16 + d) 0
  ) m

(*$= hexadecimal & ~printer:string_of_int
      0 (test_exn hexadecimal "#x0")
    427 (test_exn hexadecimal "#x01Ab")
   2564 (test_exn hexadecimal "#xA04")
  25087 (test_exn hexadecimal "#x61ff")
*)

let binary m =
  let m = "binary" :: m in
  (
    char '#' -- char 'b' -+ several ~sep:none (zero ||| one) >>:
      List.fold_left (fun s d -> s * 2 + d) 0
  ) m

(*$= binary & ~printer:string_of_int
   0 (test_exn binary "#b0")
   1 (test_exn binary "#b1")
   1 (test_exn binary "#b001")
  43 (test_exn binary "#b101011")
*)

(* Note: no support for UTF-8 *)
let is_printable c =
  let c = Char.code c in
  (c >= 32 && c <= 126) || (c > 128)

let printable_char = cond "printable char" is_printable 'x'

let string_literal m =
  let quo = char '"' in
  let printable_char_no_quo =
    cond "printable char*" (fun c ->
      c <> '"' && c <> ' ' && is_printable c) 'x' in
  let m = "string literal" :: m in
  (
    quo -+
      repeat ~sep:none (printable_char_no_quo ||| whitespace ||| (quo -+ quo)) +-
    (quo -- nay quo) >>: String.of_list
  ) m

(*$= string_literal & ~printer:(fun x -> x)
  "this is a string literal" \
    (test_exn string_literal "\"this is a string literal\"")
  "" (test_exn string_literal "\"\"")
  "She said: \"Bye bye\" and left." \
    (test_exn string_literal "\"She said: \"\"Bye bye\"\" and left.\"")
  "This is a string literal\nwith a line break in it" \
    (test_exn string_literal \
      "\"This is a string literal\nwith a line break in it\"")
*)

let simple_symbol =
  several ~sep:none (
    letter ||| digit_chr |||
    cond "symbol special char" (String.contains "~!@$%^&*_-+=<>.?/") '_'
  ) >>: fun lst ->
    if is_digit (List.hd lst) then
      raise (Reject "symbol can't start with a digit") ;
    String.of_list lst

(*$= simple_symbol & ~printer:(fun x -> x)
  "+" (test_exn simple_symbol "+")
  "<=" (test_exn simple_symbol "<=")
  "x" (test_exn simple_symbol "x")
  "plus" (test_exn simple_symbol "plus")
  "**" (test_exn simple_symbol "**")
  "$" (test_exn simple_symbol "$")
  "<sas" (test_exn simple_symbol "<sas")
  "<adf>" (test_exn simple_symbol "<adf>")
  "abc77" (test_exn simple_symbol "abc77")
  "*" (test_exn simple_symbol "*")
  "$s" (test_exn simple_symbol "$s")
  "&6" (test_exn simple_symbol "&6")
  ".kkk" (test_exn simple_symbol ".kkk")
  ".8" (test_exn simple_symbol ".8")
  "+34" (test_exn simple_symbol "+34")
  "-32" (test_exn simple_symbol "-32")
*)

let quoted_symbol =
  char '|' -+
    repeat ~sep:none (cond "quoted symbol char" (fun c ->
      (is_whitespace c || is_printable c) && c <> '|' && c <> '\\') 'x') +-
  char '|' >>: String.of_list

(*$= quoted_symbol & ~printer:(fun x -> x)
  "this is a quoted symbol" (test_exn quoted_symbol "|this is a quoted symbol|")
  "so is\nthis one" (test_exn quoted_symbol "|so is\nthis one|")
  "" (test_exn quoted_symbol "||")
  "\" can occur too" (test_exn quoted_symbol "|\" can occur too|")
  "af klj^*0asfe2(&)*&(#^$>>>?\"']]984a" \
    (test_exn quoted_symbol "|af klj^*0asfe2(&)*&(#^$>>>?\"']]984a|")
*)

type symbol = string

let print_symbol = String.print

let symbol m =
  let m = "symbol" :: m in
  (simple_symbol ||| quoted_symbol) m

let keyword m =
  let m = "keyword" :: m in
  (char ':' -+ simple_symbol) m

type spec_constant =
  | Numeral of int | Decimal of float | Hexadecimal of int | Binary of int
  | String of string

let spec_constant m =
  let m = "spec constant" :: m in
  (
    (numeral >>: fun n -> Numeral n) |||
    (decimal >>: fun n -> Decimal n) |||
    (hexadecimal >>: fun n -> Hexadecimal n) |||
    (binary >>: fun n -> Binary n) |||
    (string_literal >>: fun n -> String n)
  ) m

type s_expr =
  | Constant of spec_constant | Symbol of string | Keyword of string
  | Group of s_expr list

let blanks =
  several ~sep:none ~what:"blanks"
    ((whitespace >>: ignore) ||| comment) >>: ignore

let opt_blanks =
  optional_greedy ~def:() blanks

let sep = blanks ||| check (char '(' ||| char ')')

let par p =
  (* The specs do not say so but let's assume comments are allowed: *)
  char '(' -- opt_blanks -+ p +- opt_blanks +- char ')'

let list p =
  (char '(' -- opt_blanks -- char ')' >>: fun () -> []) |||
  par (several ~sep p)

let rec s_expr m =
  let m = "S-Expression" :: m in
  (
    (spec_constant >>: fun n -> Constant n) |||
    (symbol >>: fun n -> Symbol n) |||
    (keyword >>: fun n -> Keyword n) |||
    (list s_expr >>: fun lst -> Group lst)
  ) m

type index = NumericIndex of int | SymbolicIndex of string

let index m =
  let m = "index" :: m in
  (
    (numeral >>: fun n -> NumericIndex n) |||
    (symbol >>: fun n -> SymbolicIndex n)
  ) m

let print_index oc = function
  | NumericIndex n -> Int.print oc n
  | SymbolicIndex s -> String.print oc s

type identifier =
  | Identifier of string
  | IndexedIdentifier of string * index list (* non empty *)

let identifier m =
  let m = "identifier" :: m in
  (
    (symbol >>: fun n -> Identifier n) |||
    (par (char '_' -- blanks -+ symbol +- blanks ++ several ~sep index) >>:
      fun (s, is) -> IndexedIdentifier (s, is))
  ) m

let print_list oc = List.print ~first:"" ~last:"" ~sep:" " oc

let print_identifier oc = function
  | Identifier s -> String.print oc s
  | IndexedIdentifier (s, idxs) ->
      Printf.fprintf oc "(%s (%a))" s
        (print_list print_index) idxs

(*$= identifier & ~printer:(IO.to_string print_identifier)
  (Identifier "plus") (test_exn identifier "plus")
  (Identifier "+")    (test_exn identifier "+")
  (Identifier "<=")   (test_exn identifier "<=")
  (Identifier "Real") (test_exn identifier "Real")
  (Identifier "John Brown") \
                      (test_exn identifier "|John Brown|")
  (IndexedIdentifier ("vector-add", [NumericIndex 4; NumericIndex 5])) \
                      (test_exn identifier "(_ vector-add 4 5)")
  (IndexedIdentifier ("BitVec", [NumericIndex 32])) \
                      (test_exn identifier "(_ BitVec 32)")
  (IndexedIdentifier ("move", [SymbolicIndex "up"])) \
                      (test_exn identifier "(_ move up)")
*)

type attribute_value =
  | ConstantValue of spec_constant
  | SymbolicValue of string
  | SExprValue of s_expr list

let attribute_value m =
  let m = "attribute value" :: m in
  (
    (spec_constant >>: fun n -> ConstantValue n) |||
    (symbol >>: fun n -> SymbolicValue n) |||
    (list s_expr >>: fun n -> SExprValue n)
  ) m

type attribute = string * attribute_value option

let attribute m =
  let m = "attribute" :: m in
  (keyword ++ optional ~def:None (blanks -+ some attribute_value)) m

let print_attribute oc (s, v) =
  Printf.fprintf oc ":%s" s ;
  Option.may (fun _ -> String.print oc "some value (TODO)") v

(*$= attribute & ~printer:(IO.to_string print_attribute)
  ("left-assoc", None) (test_exn attribute ":left-assoc")
  ("status", Some (SymbolicValue "unsat")) \
                       (test_exn attribute ":status unsat")
  ("my_attribute", \
   Some (SExprValue [Symbol "humpty"; Symbol "dumpty"])) \
                       (test_exn attribute ":my_attribute (humpty dumpty)")
  ("authors", Some (ConstantValue (String "Jack and Jill"))) \
                       (test_exn attribute ":authors \"Jack and Jill\"")
*)

type sort = NonParametricSort of identifier
          | ParametricSort of identifier * sort list (* parameters *)

let rec sort m =
  let m = "sort" :: m in
  (
    (identifier >>: fun i -> NonParametricSort i) |||
    (par (identifier +- blanks ++ several ~sep sort) >>: fun (i, s) ->
      ParametricSort (i, s))
  ) m

let rec print_sort oc = function
  | NonParametricSort s -> print_identifier oc s
  | ParametricSort (s, params) ->
      Printf.fprintf oc "(%a %a)"
        print_identifier s
        (print_list print_sort) params

(*$= sort & ~printer:(IO.to_string print_sort)
  (NonParametricSort (Identifier "Int"))  (test_exn sort "Int")
  (NonParametricSort (Identifier "Bool")) (test_exn sort "Bool")
  (NonParametricSort (IndexedIdentifier ("BitVec", [NumericIndex 3]))) \
                          (test_exn sort "(_ BitVec 3)")
  (ParametricSort (\
    Identifier "List", \
    [ParametricSort (\
      Identifier "Array", [NonParametricSort (Identifier "Int") ;\
                           NonParametricSort (Identifier "Real")])])) \
                          (test_exn sort "(List (Array Int Real))")
  (ParametricSort (\
    IndexedIdentifier ("FixedSizeList", [NumericIndex 4]), \
    [NonParametricSort (Identifier "Real")])) \
                          (test_exn sort "((_ FixedSizeList 4) Real)")
  (ParametricSort (\
    Identifier "Set", \
    [NonParametricSort (IndexedIdentifier ("Bitvec", [NumericIndex 3]))])) \
                          (test_exn sort "(Set (_ Bitvec 3))")
*)

type pattern = symbol list
let pattern m =
  let m = "pattern" :: m in
  (
    (symbol >>: fun s -> [s]) |||
    (par (repeat ~min:2 ~sep symbol))
  ) m

type qual_identifier = identifier * sort option

let qual_identifier m =
  let m = "qual-identifier" :: m in
  (
    (identifier >>: fun i -> i, None) |||
    (par (char 'a' -- char 's' -- blanks -+ identifier +- blanks ++ sort) >>:
      fun (i, s) -> i, Some s)
  ) m

type sorted_var = symbol * sort

let print_sorted_var oc (sym, sort) =
  Printf.fprintf oc "(%a %a)"
    print_symbol sym
    print_sort sort

let sorted_var m =
  let m = "sorted var" :: m in
  (par (symbol +- blanks ++ sort)) m

type match_case = pattern * term
and var_binding = symbol * term
and term =
  | ConstantTerm of spec_constant
  | QualIdentifier of qual_identifier * term list
  | Let of var_binding list * term
  | ForAll of sorted_var list * term
  | Exists of sorted_var list * term
  | Match of term * match_case list
  | Tagged of term * attribute list

let rec print_term oc _x = Printf.fprintf oc "some_term(TODO)"

let rec match_case m =
  let m = "match case" :: m in
  (par (pattern +- blanks ++ term)) m

and var_binding m =
  let m = "var binding" :: m in
  (par (symbol ++ term)) m

and term m =
  let m = "term" :: m in
  (
    (spec_constant >>: fun n -> ConstantTerm n) |||
    (qual_identifier >>: fun n -> QualIdentifier (n, [])) |||
    (par (qual_identifier +- blanks ++ several ~sep term) >>: fun (n, ts) ->
      QualIdentifier (n, ts)) |||
    (par (string "let" -- blanks -+ par (several ~sep var_binding) +-
          opt_blanks ++ term) >>: fun (vbs, t) -> Let (vbs, t)) |||
    (par (string "forall" -- blanks -+ par (several ~sep sorted_var) +-
          opt_blanks ++ term) >>: fun (svs, t) -> ForAll (svs, t)) |||
    (par (string "exists" -- blanks -+ par (several ~sep sorted_var) +-
          opt_blanks ++ term) >>: fun (svs, t) -> Exists (svs, t)) |||
    (par (string "match" -- blanks -+ term +- opt_blanks ++
          par (several ~sep match_case)) >>: fun (t, ps) -> Match (t, ps)) |||
    (par (char '!' -- blanks -+ term +- blanks ++ several ~sep attribute) >>:
      fun (t, attrs) -> Tagged (t, attrs))
  ) m

type function_def =
  symbol * sorted_var list * sort * term

let print_function_def oc (sym, vars, sort, term) =
  Printf.fprintf oc "(define-fun %a (%a) %a %a)"
    print_symbol sym
    (print_list print_sorted_var) vars
    print_sort sort
    print_term term

let function_def m =
  let m = "function definition" :: m in
  (
    symbol +- opt_blanks ++ list sorted_var +- opt_blanks ++
    sort +- blanks ++ term >>: fun (((sym, vars), s), t) -> sym, vars, s, t
  ) m

type function_dec =
  symbol * sorted_var list * sort

let function_dec m =
  let m = "function declaration" :: m in
  (
    par (symbol +- opt_blanks ++
         par (several ~sep sorted_var) +- opt_blanks ++
         sort) >>: fun ((sym, vars), s) -> sym, vars, s
  ) m

type model =
  (function_def * bool (* recursive *)) list

let print_model oc lst =
  (* We don't care that much about that rec flag when printing *)
  print_list (fun oc (def, _rec_flag) ->
    Printf.fprintf oc "(define-fun %a)"
      print_function_def def) oc lst

let model_resp m =
  let m = "model response" :: m in
  (
    (par (string "define-fun" -- blanks -+ function_def) >>:
      fun d -> [ d, false ]) |||
    (par (string "define-fun-rec" -- blanks -+ function_def) >>:
      fun d -> [ d, true ]) |||
    (par (string "define-funs-rec" -- opt_blanks -+
          par (several ~sep function_dec) +- opt_blanks ++
          par (several ~sep term)) >>: fun (decs, terms) ->
      try
        List.map2 (fun (sym, vars, sort) term ->
          (sym, vars, sort, term), true
        ) decs terms
      with Invalid_argument _ ->
        raise (Reject "Must have as many declarations than terms"))
  ) m

let get_model_resp m =
  let m = "get-model response" :: m in
  (
    ( (* This annoying "model" variant may be z3 specific *)
      (par (string "model") >>: fun () -> []) |||
      par (string "model" -- blanks -+ several ~sep model_resp) |||
      list model_resp
    ) >>: List.flatten
  ) m

(*$= get_model_resp & ~printer:dump
  [] (test_exn get_model_resp "(model \n)")
*)

let get_unsat_core_resp m =
  let m = "get-unsat-core response" :: m in
  (list symbol) m

type check_sat_resp = Sat | Unsat | Unknown

let check_sat_resp m =
  let m = "check-sat response" :: m in
  (
    (string "sat" >>: fun () -> Sat) |||
    (string "unsat" >>: fun () -> Unsat) |||
    (string "unknown" >>: fun () -> Unknown)
  ) m

let error_resp m =
  let m = "error response" :: m in
  (par (string "error" -- blanks -+ string_literal)) m

type response = Solved of model
              | Unsolved of symbol list

let sat_resp m =
  let m = "sat response" :: m in
  (check_sat_resp +- opt_blanks ++ error_resp +- opt_blanks ++
   get_model_resp) m

let unsat_resp m =
  let m = "unsat response" :: m in
  (check_sat_resp +- opt_blanks ++ get_unsat_core_resp +- opt_blanks ++
   error_resp) m

let response m =
  let m = "response" :: m in
  (
    (sat_resp >>: fun ((sat, _err), model) ->
      if sat <> Sat then raise (Reject "check-sat is not sat") ;
      Solved model) |||
    (unsat_resp >>: fun ((sat, syms), _err) ->
      if sat <> Unsat then raise (Reject "check-sat is not unsat") ;
      Unsolved syms)
  ) m

let print_response oc = function
  | Solved model -> print_model oc model
  | Unsolved syms ->
      print_list print_symbol oc syms

(*$= response & ~printer:dump
  (Solved []) \
       (test_exn response \
        "sat\n\
        (error \"line 17 column 15: unsat core is not available\")\n\
        (model \n\
        )")

  (Solved [\
    ("e14", [], NonParametricSort (Identifier "Type"), \
     QualIdentifier ((Identifier "string", None), [])), false ;\
    ("e18", [], NonParametricSort (Identifier "Type"), \
     QualIdentifier ((Identifier "float", None), [])), false ]) \
      (test_exn response \
        "sat\n\
         (error \"line 32 column 15: unsat core is not available\")\n\
         (model \n\
           (define-fun e14 () Type\n\
             string)\n\
           (define-fun e18 () Type\n\
             float)\n\
         )")

  (Unsolved []) \
      (test_exn response \
        "unsat\n\
         ()\n\
         (error \"line 418 column 10: model is not available\")")
*)
