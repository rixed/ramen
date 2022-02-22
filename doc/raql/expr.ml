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
    examples : (string * string) list ;
    limitations : html }

let make ?(infix = false) ?(has_state = false) ?(deterministic = true)
         ?(limitations = []) name
         short_descr long_descr syntaxes typing examples =
  { name ; infix ; has_state ; deterministic ; short_descr ; long_descr ;
    syntaxes ; typing ; examples ; limitations }

let exprs =
  let warn_utf8 =
    p [ text "For now, strings are just variable length sequences of \
              bytes. However, the plan is to make them UTF8 though." ] in
  [ make "now" "Return the current time as a UNIX timestamp."
         ~deterministic:false
      [ p [ text "Floating point number of seconds since 1970-01-01 00:00 \
                  UTC." ] ]
      [ [ text "NOW" ] ]
      [ [ text "float" ] ]
      [ "NOW", string_of_float (Unix.gettimeofday ()) ] ;
    make "random" "Return a random number."
         ~deterministic:false
      [ p [ text "Results are uniformly distributed between 0 and 1." ] ]
      [ [ text "RANDOM" ] ]
      [ [ text "float" ] ]
      [ "RANDOM", string_of_float (Random.float 1.) ] ;
    make "pi" "The constant π."
      []
      [ [ text "PI" ] ]
      [ [ text "float" ] ]
      [ "PI", "3.14159265358979312" ] ;
    make "age" "Return the time elapsed since a past date."
         ~deterministic:false
      [ p [ bold "AGE D" ; text " is equivalent to " ; bold "NOW - D" ] ]
      [ [ text "AGE …float-expr…" ] ]
      [ [ text "float" ] ]
      [ "AGE(NOW - 3s)", "3" ;
        "AGE(0)", "1645380250.123524" ;
        "AGE(NOW)", "0" ] ;
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
      [ "CAST(PI AS U8)", "3" ;
        "CAST(PI AS STRING)", "\"3.14159265359\"" ] ;
    make "force" "Convert to not-null or crash."
      [ p [ text "Only force a value when you are certain it is not NULL, as the \
                  worker will abort otherwise." ] ;
        p [ text "Does not accept non nullable arguments." ] ]
      [ [ text "FORCE …nullable-expr…" ] ]
      [ [ text "t? -> t" ] ]
      [ "FORCE(U8?(42))", "42" ] ;
    make "peek" "Read some bytes into a wider integer."
      ~limitations:[ p [ text "Some integer widths are not yet implemented." ] ]
      [ p [ text "Either read some bytes from a string into an integer, \
                  or convert a vector of small unsigned integers into a large \
                  integer. \
                  Can come handy when receiving arrays of integers from \
                  external systems that have no better ways to encode large \
                  integers." ] ;
        p [ text "The endianness can be specified, and little-endian is the \
                  default." ] ;
        p [ text "If the destination type is actually narrower than required \
                  then the result is merely truncated. But if the destination \
                  type is wider than the number of provided bytes then the \
                  result will be NULL." ] ;
        p [ text "The result is not nullable only when reading from a non \
                  nullable vector of non nullable integers." ] ]
      [ [ text "PEEK …int-type… [[BIG | LITTLE] ENDIAN] …string…" ] ;
        [ text "PEEK …int-type… [[BIG | LITTLE] ENDIAN] …int_vector…" ] ]
      [ [ text "STRING -> int?" ] ; [ text "int1[] -> int2" ] ]
      [ "PEEK U32 LITTLE ENDIAN \"\\002\\001\\000\\000\"", "258" ;
        "PEEK U32 LITTLE ENDIAN \"\\002\\001\"", "NULL" ;
        "PEEK I16 BIG ENDIAN \"\\002\\001\"", "513" ;
        "PEEK U8 \"\\004\\003\\002\\001\"", "4" ;
        (* Notice the arrays must be cast as U8s because default integer size
         * is 32bits: *)
        "PEEK U32 LITTLE ENDIAN [0xC0u8; 0xA8u8; 0x00u8; 0x01u8]", "16820416" ;
        "PEEK U32 BIG ENDIAN CAST([0xC0; 0xA8; 0x00; 0x01] AS U8[4])", "3232235521" ] ;
    make "length" "Compute the length of a string or array."
      [ p [ text "Returns the number of " ; bold "bytes" ; text " in a \
                  string." ] ;
        warn_utf8 ;
        p [ text "Can also return the length of an array." ] ]
      [ [ text "LENGTH …string-expr…" ] ; [ text "LENGTH …array…" ] ]
      [ [ text "STRING -> U32" ] ; [ text "t[] -> U32" ] ]
      [ "LENGTH \"foo\"", "3" ;
        "LENGTH \"\"", "0" ;
        "LENGTH CAST([42] AS U8[])", "1" ] ;
    make "lower" "Return the lowercase version of a string."
      [ warn_utf8 ]
      [ [ text "LOWER …string-expr…" ] ]
      [ [ text "STRING -> STRING" ] ]
      [ "LOWER \"Foo Bar Baz\"", "\"foo bar baz\"" ] ;
    make "upper" "Return the uppercase version of a string."
      [ warn_utf8 ]
      [ [ text "UPPER …string-expr…" ] ]
      [ [ text "STRING -> STRING" ] ]
      [ "UPPER \"Foo Bar Baz\"", "\"FOO BAR BAZ\"" ] ;
    make "uuid_of_u128" "Print a U128 as an UUID."
      [ p [ text "Convert a U128 into a STRING using the traditional \
                  notation for UUIDs." ] ]
      [ [ text "UUID_OF_U128 …u128-expr…" ] ]
      [ [ text "U128 -> STRING" ] ]
      [ "UUID_OF_U128 0x123456789abcdeffedcba098765431",
        "\"00123456-789a-bcde-ffed-cba098765431\"" ] ;
    make "not" "Negation." [ p [ text "boolean negation." ] ]
      [ [ text "NOT …bool-expr…" ] ]
      [ [ text "BOOL -> BOOL" ] ]
      [ "NOT TRUE", "FALSE" ;
        "NOT (0 > 1)", "TRUE" ] ;
    make "abs" "Absolute value."
      [ p [ text "Return the absolute value of an expression of any numeric \
                  type." ] ]
      [ [ text "ABS …numeric-expr…" ] ]
      [ [ text "numeric -> numeric" ] ]
        (* Note: parentheses are required to disambiguate: *)
      [ "ABS(-1.2)", "1.2" ] ;
    make "neg" "Negation."
      [ p [ text "Negation of the numeric argument." ] ;
        p [ text "The return type is either the same as that of the argument, \
                  or a non smaller signed integer type" ] ]
      [ [ text "-…numerix-expr…" ] ]
      [ [ text "numeric -> signed-numeric" ] ]
      [ "-(1+1)", "-2" ] ;
    make "is-null" "Check for NULL"
      [ p [ text "Test any nullable argument for NULL. Always returns a \
                  non-nullable boolean." ] ;
        p [ text "Notice that this is not equivalent to comparing a value \
                  to NULL using the equality operator. Indeed, NULL propagates \
                  through the equality operator and the result of such a \
                  comparison would merely be NULL." ] ]
      [ [ text "…nullable-expr… IS NULL" ] ;
        [ text "…nullable-expr… IS NOT NULL" ] ]
      [ [ text "t? -> BOOL" ] ]
      [ "NULL IS NULL", "TRUE" ;
        "NULL IS NOT NULL", "FALSE" ;
        "(NULL = 1) IS NULL", "TRUE" ] ;
    make "exp" "Exponential function."
      [ p [ text "Compute the exponential function (as a FLOAT)." ] ]
      [ [ text "EXP …numeric-expr…" ] ]
      [ [ text "numeric -> FLOAT" ] ]
      [ "EXP 0", "1" ; "EXP 1", "2.71828182846" ] ;
    make "log" "Natural logarithm."
      [ p [ text "Compute the natural logarithm of the argument." ] ;
        p [ text "Returns NaN on negative arguments." ] ]
      [ [ text "LOG …numeric-expr…" ] ]
      [ [ text "numeric -> FLOAT" ] ]
      [ "LOG 1", "0" ] ;
    make "log10" "Decimal logarithm."
      [ p [ text "Compute the decimal logarithm of the argument." ] ;
        p [ text "Returns NaN on negative arguments." ] ]
      [ [ text "LOG10 …numeric-expr…" ] ]
      [ [ text "numeric -> FLOAT" ] ]
      [ "LOG10 100", "2" ] ;
    make "sqrt" "Square root."
      [ p [ text "Square root function." ] ;
        p [ text "Returns NULL on negative arguments." ] ]
      [ [ text "SQRT …numeric-expr…" ] ]
      [ [ text "numeric -> FLOAT" ] ]
      [ "SQRT 16", "4" ;
        (* Note: parentheses are required to disambiguate: *)
        "SQRT(-1)", "NULL" ] ;
    make "sq" "Square."
      [ p [ text "Square function." ] ;
        p [ text "Returns the same type as the argument." ] ]
      [ [ text "SQ …numeric-expr…" ] ]
      [ [ text "numeric -> numeric" ] ]
      [ "SQ 4", "16" ] ;
    make "ceil" "Ceiling function."
      [ p [ text "Return the round value just greater or equal to the argument." ] ;
        p [ text "The result has the same type than the argument." ] ]
      [ [ text "CEIL …numeric-expr…" ] ]
      [ [ text "numeric -> numeric" ] ]
      [ "CEIL 41.2", "42" ] ;
    make "floor" "Floor function."
      [ p [ text "Return the round value just smaller or equal to the argument." ] ;
        p [ text "The result has the same type than the argument." ] ]
      [ [ text "FLOOR …numeric-expr…" ] ]
      [ [ text "numeric -> numeric" ] ]
      [ "FLOOR 42.7", "42" ] ;
    make "round" "Rounding."
      [ p [ text "Return the closest round value." ] ;
        p [ text "The result has the same type than the argument." ] ]
      [ [ text "ROUND …numeric-expr…" ] ]
      [ [ text "numeric -> numeric" ] ]
      [ "ROUND 42.4", "42" ] ;
    make "cos" "Cosine."
      [ p [ text "Return the cosine of the argument." ] ]
      [ [ text "COS …numeric-expr…" ] ]
      [ [ text "numeric -> FLOAT" ] ]
      [ "COS PI", "-1" ] ;
    make "sin" "Sine."
      [ p [ text "Return the sine of the argument." ] ]
      [ [ text "SIN …numeric-expr…" ] ]
      [ [ text "numeric -> FLOAT" ] ]
      [ "SIN PI", "0" ] ;
    make "tan" "Tangent."
      [ p [ text "Return the tangent of the argument." ] ]
      [ [ text "TAN …numeric-expr…" ] ]
      [ [ text "numeric -> FLOAT" ] ]
      [ "TAN 0", "0" ] ;
    make "acos" "Arc-Cosine."
      [ p [ text "Return the arc-cosine of the argument." ] ]
      [ [ text "ACOS …numeric-expr…" ] ]
      [ [ text "numeric -> FLOAT" ] ]
      (* Must use parenths: *)
      [ "ACOS(-1)", "3.14" ] ;
    make "asin" "Arc-Sine."
      [ p [ text "Return the arc-sine of the argument." ] ]
      [ [ text "ASIN …numeric-expr…" ] ]
      [ [ text "numeric -> FLOAT" ] ]
      [ "2 * ASIN 1", "3.14" ] ;
    make "atan" "ArcTangent."
      [ p [ text "Return the arc-tangent of the argument." ] ]
      [ [ text "ATAN …numeric-expr…" ] ]
      [ [ text "numeric -> FLOAT" ] ]
      [ "4 * ATAN 1", "3.14" ] ;
    make "cosh" "Hyperbolic Cosine."
      [ p [ text "Return the hyperbolic cosine of the argument." ] ]
      [ [ text "COSH …numeric-expr…" ] ]
      [ [ text "numeric -> FLOAT" ] ]
      [ "COSH 0", "0" ] ;
    make "sinh" "Hyperbolic Sine."
      [ p [ text "Return the hyperbolic sine of the argument." ] ]
      [ [ text "SINH …numeric-expr…" ] ]
      [ [ text "numeric -> FLOAT" ] ]
      [ "SINH 0", "0" ] ;
    make "tanh" "Hyperbolic Tangent."
      [ p [ text "Return the hyperbolic tangent of the argument." ] ]
      [ [ text "TANH …numeric-expr…" ] ]
      [ [ text "numeric -> FLOAT" ] ]
      [ "TANH 0", "0" ] ;
    make "hash" "Hash any value."
      [ p [ text "Compute a integer hash of a value of any type." ] ;
        p [ text "The hash function is deterministic." ] ]
      [ [ text "HASH …expr…" ] ]
      [ [ text "t -> I64" ] ]
      [ "HASH NULL", "NULL" ;
        "HASH (\"foo\"; \"bar\")", "731192513" ] ;
    make "parse_time" "Format a date."
      [ p [ text "This function takes a date as a string and convert it to a \
                  timestamp. It accepts various common encodings (similar to \
                  the UNIX at(1) command." ] ;
        p [ text "Beware that PARSE_TIME assumes all dates are in the local \
                  time zone." ] ;
        p [ text "The result is always nullable." ] ]
      [ [ text "PARSE_TIME …string-expr…" ] ]
      [ [ text "STRING -> FLOAT?" ] ]
      (* Divide by 24 so that this does not depend on the time zone: *)
      [ "(PARSE_TIME \"1976-01-28 12:00:00.9\") // 24h", "2218" ;
        "(PARSE_TIME \"12/25/2005\") // 24h", "13141" ] ;
    make "chr" "ASCII character of a given code."
      [ p [ text "Convert the given integer (must be below 255) into the \
                  corresponding ASCII character." ] ]
      [ [ text "CHR …int-expr…" ]]
      [ [ text "integer -> CHR" ] ]
      [ "CHR 65", "#\\A" ]
]

let see_also =
  [ [ "now" ; "age" ] ;
    [ "exp" ; "log" ; "log10" ] ;
    [ "force" ; "is-null" ; "coalesce" ] ;
    [ "lower" ; "upper" ] ;
    [ "sqrt" ; "sq" ] ;
    [ "ceil" ; "floor" ; "round" ] ;
    [ "cos" ; "sin" ; "tan" ; "acos" ; "asin" ; "atan" ; "cosh" ; "sinh" ;
      "tanh" ] ]
