(* For now this merely describes what's implemented in RamenExpr.Parser and
 * RamenTyping. Maybe one day it will be authoritative. *)
open Html

type expr =
  { name : string ;
    infix : bool ;
    has_state : bool ;
    deterministic : bool ;
    short_descr : string ;
    long_descr : html ;
    syntaxes : html list ;  (* what function names are accepted *)
    typing : html list ;
    examples : example list }

and example =
  { inputs : string list ;
    output : string }

let make ?(infix = false) ?(has_state = false) ?(deterministic = true) name
         short_descr long_descr syntaxes typing examples =
  { name ; infix ; has_state ; deterministic ; short_descr ; long_descr ;
    syntaxes ; typing ; examples }

let example inputs output = { inputs ; output }

let exprs =
  [ make "now" "Return the current time as a UNIX timestamp."
         ~deterministic:false
      [ p [ text "Floating point number of seconds since 1970-01-01 00:00 \
                  UTC." ] ]
      [ [ text "NOW" ] ]
      [ [ text "float" ] ]
      [ example [] (string_of_float (Unix.gettimeofday ())) ] ;
    make "random" "Return a random number."
         ~deterministic:false
      [ p [ text "Results are uniformly distributed between 0 and 1." ] ]
      [ [ text "RANDOM" ] ]
      [ [ text "float" ] ]
      [ example [] (string_of_float (Random.float 1.)) ] ;
    make "pi" "The constant π."
      []
      [ [ text "PI" ] ]
      [ [ text "float" ] ]
      [ example [] "3.14159265358979312" ] ;
    make "age" "Return the time elapsed since a past date."
         ~deterministic:false
      [ p [ bold "AGE D" ; text " is equivalent to " ; bold "NOW - D" ] ]
      [ [ text "AGE D" ] ; [ text "AGE(D)"] ]
      [ [ text "float" ] ]
      [ example [ "NOW - 3s" ] "3" ;
        example [ "0" ] "1645380250.123524" ;
        example [ "NOW" ] "0" ] ;
    make "cast" "Cast an expression into a specific type."
      [ p [ text "Any type can be cast into any other one. \
                  For instance, strings can be parsed as numbers or numbers \
                  printed as strings. \
                  Some conversion makes no sense and might lead to surprising \
                  results." ] ;
        p [ text "The result is the requested type, forced nullable if the \
                  expression is nullable. See FORCE to cast away nullability." ] ]
      [ [ text "CAST(…expr… AS …type…)" ] ; [ text "…type…(…expr…)" ] ]
      [ [ text "t1 -> t2" ] ]
      [ example [ "PI AS U8" ] "3" ;
        example [ "PI AS STRING" ] "\"3.14159265359\"" ] ;
    make "force" "Convert to not-null or crash."
      [ p [ text "Only force a value when you are certain it is not NULL, as the \
                  worker will abort otherwise." ] ;
        p [ text "Does not accept non nullable arguments." ] ]
      [ [ text "FORCE(…expr…)" ] ]
      [ [ text "t? -> t" ] ]
      [ example [ "U8?(42)" ] "42" ]
]
