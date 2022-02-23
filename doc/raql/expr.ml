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
    make "is-null" "Check for NULL."
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
      [ "FLOOR 42.7", "42" ;
        "FLOOR(-42.7)", "-43" ] ;
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
      [ "CHR 65", "#\\A" ] ;
    make "like" "Comparing strings with patterns."
      [ p [ text "LIKE compares a string expression with a pattern, expressed \
                  as a string that can contain the special character " ;
            emph "percent" ;
            text " ('%') that can replace any substring including \
                  the empty substring, and the special character " ;
            emph "underscore" ;
            text " ('_') that can match any single character." ] ;
        p [ text "If you need to match precisely for any of those special \
                  characters you can escape them with the " ;
            emph "backslach" ;
            text " prefix ('\\'). Notice though that since backslash is also \
                  the default escaping character for strings, and pattern is \
                  written as a string, it has to be doubled in practice to \
                  serve as a pattern escape character!" ] ;
        p [ text "The result is true if the left string matches the pattern, \
                  or false otherwise." ] ;
        p [ text "Note that the pattern cannot be any string expression but \
                  must be a string constant." ] ]
      [ [ text "…string-expr… LIKE \"pattern\"" ] ]
      [ [ text "STRING -> BOOL" ] ]
      [ "\"foobar\" LIKE \"foo%\"", "true" ;
        (* Notice the doubled escape character: *)
        "\"foobar\" LIKE \"foo\\\\%\"", "false" ;
        "\"foobar\" LIKE \"f%r\"", "true" ;
        "\"foobar\" LIKE \"f__b_r\"", "true" ;
        "\"foobar\" LIKE \"fo_b%\"", "true" ;
        "\"foobar\" LIKE \"%baz\"", "false" ;
        "\"foobar\" LIKE \"\"", "false" ] ;
    make "fit" "(Multi)Linear regression."
      [ p [ text "General fitting function that takes one arguments: an \
                  array or vector of observations of either unique numeric \
                  values or tuples of numeric values." ] ;
        p [ text "In the former case the unique value is supposed to be the \
                  value to predict (the " ; emph "dependent variable" ;
            text " in math jargon), using the start event time as the \
                  predictor (if available)." ] ;
        p [ text "In the later case, the first value of the tuple is the \
                  variable to predict and the others are the predictors \
                  (aka. the " ; emph "explanatory variables" ; text ")." ] ;
        p [ text "Notice that the value that is predicted is the last of the \
                  observations rather than the next observations, which \
                  makes that function more handy when comparing the last \
                  observation with the prediction. Obviously, that last \
                  observed value is not taken into account to compute the \
                  prediction (but the last predictors are of course)." ] ;
        p [ text "The result will be the predicted value." ] ]
      [ [ text "FIT …sequence-expr…" ] ]
      [ [ text "numeric[] -> FLOAT" ] ;
        [ text "numeric[n] -> FLOAT" ] ;
        [ text "(numeric; …; numeric)[] -> FLOAT" ] ;
        [ text "(numeric; …; numeric)[n] -> FLOAT" ] ]
      [ "FIT [1; 2; 3; 99]", "4" ;
        "FIT [(2; 1); (4; 2); (6; 3); (99; 4)]", "8" ] ;
    make "countrycode" "Country code of an IP."
      [ p [ text "Return the country-code (as a string) of an IP (v4 or \
                  v6)." ] ;
        p [ text "Ramen relies on an embedded, constant database of IP \
                  geo-locations that's designed for speed. It therefore \
                  makes no external request. As a downside, the information \
                  can be outdated or missing (in which case NULL is \
                  returned)." ] ;
        p [ text "Given the embedded database might change from one version \
                  of Ramen to the next, this function is not deterministic \
                  across upgrades." ] ;
        p [ text "This function should be only used as a hint." ] ]
      [ [ text "COUNTRYCODE …ip-expr…" ] ]
      [ [ text "IPv4 -> STRING?" ] ;
        [ text "IPv6 -> STRING?" ] ;
        [ text "Ip -> STRING?" ] ]
      [ "COUNTRYCODE 5.182.236.0", "\"AT\"" ;
        "COUNTRYCODE 2a00:1450:400f:804::2004", "\"IE\"" ] ;
    make "ipfamily" "Returns the version of an IP."
      [ p [ text "Returns either 4 if the IP is an IPv4 or 6 if the IP is an \
                  IPv6." ] ]
      [ [ text "IPFAMILY …ip-expr…" ] ]
      [ [ text "IPv4 -> unsigned-int" ] ;
        [ text "IPv6 -> unsigned-int" ] ;
        [ text "Ip -> unsigned-int" ] ]
      [ "IPFAMILY 135.181.17.92", "4" ;
        "IPFAMILY 2a01:4f9:4b:55b0::2", "6" ] ;
    make "basename" "Strip the directory part of a path."
      [ p [ text "Similar to the UNIX tool basename, this removes from the \
                  passed argument everything before the last slash ('/'), \
                  to keep only the filename portion of a path." ] ;
        p [ text "Does nothing if the argument contains no slash." ] ]
      [ [ text "BASENAME …string-expr…" ] ]
      [ [ text "STRING -> STRING" ] ]
      [ "BASENAME \"/foo/bar/baz\"", "\"baz\"" ;
        "BASENAME \"foo\"", "\"foo\"" ] ;
    make "min" "Minimum."
      [ p [ text "Return the minimum value of all the arguments." ] ;
        p [ text "All arguments must have compatible types (ie. it is \
                  possible to convert one into another)." ] ;
        p [ text "The result will be NULL if any of the arguments is." ] ]
      [ [ text "MIN(…expr1…, …expr2…, …)" ] ]
      [ [ text "t1, t2, … -> largest(t1, t2, …)" ] ]
      [ "MIN(1, 2, 3)", "1" ;
        "MIN(\"foo\", \"bar\")", "\"bar\"" ;
        "MIN([5; 6], [3; 9])", "[3; 9]" ] ;
    make "max" "Maximum."
      [ p [ text "Return the maximum value of all the arguments." ] ;
        p [ text "All arguments must have compatible types (ie. it is \
                  possible to convert one into another)." ] ;
        p [ text "The result will be NULL if any of the arguments is." ] ]
      [ [ text "MAX(…expr1…, …expr2…, …)" ] ]
      [ [ text "t1, t2, … -> largest(t1, t2, …)" ] ]
      [ "MAX(1, 2, 3)", "3" ;
        "MAX(\"foo\", \"bar\")", "\"foo\"" ;
        "MAX([5; 6], [3; 9])", "[5; 6]" ] ;
    make "coalesce" "Selects the first non null argument."
      [ p [ text "Accept a list of alternative values of the same type and \
                  returns the first one that is not NULL." ] ;
        p [ text "All alternatives but the last must be nullable." ] ;
        p [ text "If all alternatives are NULL then returns NULL." ] ]
      [ [ text "COALESCE(…expr1…, …expr2…, …)" ] ]
      [ [ text "(t?, t?, …, t?) -> t?" ] ;
        [ text "(t?, t?, …, t) -> t" ] ]
      [ "COALESCE(NULL, 1)", "1" ;
        "COALESCE(IF RANDOM > 1 THEN \"can't happen\", \"ok\")", "\"ok\"" ] ;
    make "add" "Addition."
      [ p [ text "Adds two numeric values." ] ;
        p [ text "The result have the type of the largest argument." ] ]
      [ [ text "…num-expr… + …num-expr…" ] ]
      [ [ text "num1, num2 -> largest(num1, num2)" ] ]
      [ "27 + 15", "42" ;
        "1.5 + 1u8", "2.5" ] ;
    make "sub" "Subtraction."
      [ p [ text "Subtracts two numeric values." ] ;
        p [ text "The result type is the largest of the argument types, and \
                  always signed." ] ]
      [ [ text "…num-expr… - …num-expr…" ] ]
      [ [ text "num1, num2 -> signed(largest(num1, num2))" ] ]
      [ "1u8 - 2u8", "-1" ] ;
    make "mul" "Multiplication."
      [ p [ text "Multiplies two numeric values, or repeat a string." ] ;
        p [ text "If the two arguments are numeric, then the result type \
                  is the largest of the argument types." ] ;
        p [ text "Alternatively, can also be used to repeat a string if the \
                  first argument is an integer and the second a string. In \
                  that case the return type is a string." ] ]
      [ [ text "…num-expr… * …num-expr…" ] ;
        [ text "…int-expr… * …string-expr…" ] ]
      [ [ text "num1, num2 -> largest(num1, num2)" ] ;
        [ text "int, STRING -> STRING" ] ]
      [ "6 * 7", "42" ;
        "2 * \"foo\"", "\"foofoo\"" ] ;
    make "div" "Division."
      [ p [ text "Divides two numeric values." ] ;
        p [ text "The result is always a FLOAT. If the divisor and dividend \
                  are non-zero constants then the result is not nullable, \
                  otherwise it is, and the result of 0/0 is NULL." ] ]
      [ [ text "…num-expr… / …num-expr…" ] ]
      [ [ text "num1, num2 -> FLOAT?" ] ;
        [ text "num1, non-zero-const -> FLOAT" ] ]
      [ "84/2", "42" ;
        "1/0", "Inf" ;
        "0/0", "NULL" ] ;
    make "idiv" "Integer division."
      [ p [ text "Divides two numeric values, truncating toward zero." ] ;
        p [ text "The result type is the largest of the argument types." ] ;
        p [ text "For FLOAT arguments the result will still be a FLOAT, \
                  floored." ] ]
      [ [ text "…num-expr… // …num-expr…" ] ]
      [ [ text "num1, num2 -> largest(num1, num2)" ] ]
      [ "10//3", "3" ;
        "-10//3", "-3" ;
        "10.5//3.1", "3" ] ;
    make "mod" "Modulo."
      [ p [ text "Compute the modolus of the two given arguments." ] ;
        p [ text "The result type is the largest of the argument types." ] ]
      [ [ text "…num-expr… % …num-expr…" ] ]
      [ [ text "num1, num2 -> largest(num1, num2)" ] ]
      [ "3 % 2", "1" ;
        "-3 % 2", "-1" ;
        "3 % -2", "1" ] ;
    make "pow" "Power."
      [ p [ text "Compute the first argument to the power of the second." ] ;
        p [ text "The result type is the largest of the argument types." ] ]
      [ [ text "…num-expr… ^ …num-expr…" ] ]
      [ [ text "num1, num2 -> largest(num1, num2)" ] ]
      [ "2 ^ 3", "8" ;
        "PI ^ PI", "36.4621596072079" ] ;
    make "truncate" "Rounding to selected precision."
      [ p [ text "Truncate the first argument to the nearest (from below) \
                  multiple of the second, or to the nearest integer if no \
                  second argument is provided (it is then equivalent to " ;
            bold "floor" ; text "." ] ]
      [ [ text "TRUNCATE(…num-expr…, …num-expr…)" ] ;
        [ text "TRUNCATE …num-expr…" ] ]
      [ [ text "num1, num2 -> largest(num1, num2)" ] ]
      [ "TRUNCATE(153.6, 10)", "150" ;
        "TRUNCATE 5.8", "5" ;
        "TRUNCATE(-2.3)", "-3" ] ;
    make "reldiff" "Relative difference."
      [ p [ text "Compare the two arguments A and B by computing:" ] ;
        p [ text "MIN(ABS(A-B), MAX(A, B)) / MAX(ABS(A-B), MAX(A, B))" ] ;
        p [ text "Returns 0 when A = B." ] ]
      [ [ text "RELDIFF(…num-expr…, …num-expr…)" ] ]
      [ [ text "num1, num2 -> FLOAT" ] ]
      [ "RELDIFF(1, 1)", "0" ;
        "RELDIFF(10, 9)", "0.1" ;
        "RELDIFF(9, 10)", "0.1" ;
        "RELDIFF(-9, -10)", "0.1" ;
        "RELDIFF(1, -10)", "1.1" ]
]

let see_also =
  [ [ "now" ; "age" ] ;
    [ "exp" ; "log" ; "log10" ] ;
    [ "force" ; "is-null" ; "coalesce" ] ;
    [ "lower" ; "upper" ] ;
    [ "sqrt" ; "sq" ] ;
    [ "ceil" ; "floor" ; "round" ; "truncate" ] ;
    [ "cos" ; "sin" ; "tan" ; "acos" ; "asin" ; "atan" ; "cosh" ; "sinh" ;
      "tanh" ] ;
    [ "fit" ; "group" ; "past" ] ;
    [ "countrycode" ; "ipfamily" ] ;
    [ "min" ; "max" ] ;
    [ "floor" ; "idiv" ] ;
    [ "add" ; "sub" ; "mul" ; "div" ; "idiv" ] ;
    [ "div" ; "idiv" ; "mod" ] ;
    [ "mul" ; "pow" ] ;
    [ "reldiff" ; "sub" ] ]
