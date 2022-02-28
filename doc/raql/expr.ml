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
    p [ cdata "For now, strings are just variable length sequences of \
               bytes. However, the plan is to make them UTF8 though." ] in
  [ make "now" "Return the current time as a UNIX timestamp"
         ~deterministic:false
      [ p [ cdata "Floating point number of seconds since 1970-01-01 00:00 \
                   UTC." ] ]
      [ [ cdata "NOW" ] ]
      [ [ cdata "float" ] ]
      [ "NOW", string_of_float (Unix.gettimeofday ()) ] ;
    make "random" "Return a random number"
         ~deterministic:false
      [ p [ cdata "Results are uniformly distributed between 0 and 1." ] ]
      [ [ cdata "RANDOM" ] ]
      [ [ cdata "float" ] ]
      [ "RANDOM", string_of_float (Random.float 1.) ] ;
    make "pi" "The constant π"
      []
      [ [ cdata "PI" ] ]
      [ [ cdata "float" ] ]
      [ "PI", "3.14159265358979312" ] ;
    make "age" "Return the time elapsed since a past date"
         ~deterministic:false
      [ p [ bold "AGE D" ; cdata " is equivalent to " ; bold "NOW - D" ] ]
      [ [ cdata "AGE …float-expr…" ] ]
      [ [ cdata "float" ] ]
      [ "AGE(NOW - 3s)", "3" ;
        "AGE(0)", "1645380250.123524" ;
        "AGE(NOW)", "0" ] ;
    make "cast" "Cast an expression into a specific type"
      [ p [ cdata "Any type can be cast into any other one. \
                   For instance, strings can be parsed as numbers or numbers \
                   printed as strings. \
                   Some conversion makes no sense and might lead to surprising \
                   results." ] ;
        p [ cdata "The result is the requested type, forced nullable if the \
                   expression is nullable. See FORCE to cast away nullability." ] ]
      [ [ cdata "CAST(…expr… AS …type…)" ] ; [ cdata "…type…(…expr…)" ] ]
      [ [ cdata "t1 -> t2" ] ]
      [ "CAST(PI AS U8)", "3" ;
        "CAST(PI AS STRING)", "\"3.14159265359\"" ] ;
    make "force" "Convert to not-null or crash"
      [ p [ cdata "Only force a value when you are certain it is not NULL, as the \
                   worker will abort otherwise." ] ;
        p [ cdata "Does not accept non nullable operands." ] ]
      [ [ cdata "FORCE …nullable-expr…" ] ]
      [ [ cdata "t? -> t" ] ]
      [ "FORCE(U8?(42))", "42" ] ;
    make "peek" "Read some bytes into a wider integer"
      ~limitations:[ p [ cdata "Some integer widths are not yet implemented." ] ]
      [ p [ cdata "Either read some bytes from a string into an integer, \
                   or convert a vector of small unsigned integers into a large \
                   integer. \
                   Can come handy when receiving arrays of integers from \
                   external systems that have no better ways to encode large \
                   integers." ] ;
        p [ cdata "The endianness can be specified, and little-endian is the \
                   default." ] ;
        p [ cdata "If the destination type is actually narrower than required \
                   then the result is merely truncated. But if the destination \
                   type is wider than the number of provided bytes then the \
                   result will be NULL." ] ;
        p [ cdata "The result is not nullable only when reading from a non \
                   nullable vector of non nullable integers." ] ]
      [ [ cdata "PEEK …int-type… [[BIG | LITTLE] ENDIAN] …string…" ] ;
        [ cdata "PEEK …int-type… [[BIG | LITTLE] ENDIAN] …int_vector…" ] ]
      [ [ cdata "STRING -> int?" ] ; [ cdata "int1[] -> int2" ] ]
      [ "PEEK U32 LITTLE ENDIAN \"\\002\\001\\000\\000\"", "258" ;
        "PEEK U32 LITTLE ENDIAN \"\\002\\001\"", "NULL" ;
        "PEEK I16 BIG ENDIAN \"\\002\\001\"", "513" ;
        "PEEK U8 \"\\004\\003\\002\\001\"", "4" ;
        (* Notice the arrays must be cast as U8s because default integer size
         * is 32bits: *)
        "PEEK U32 LITTLE ENDIAN [0xC0u8; 0xA8u8; 0x00u8; 0x01u8]", "16820416" ;
        "PEEK U32 BIG ENDIAN CAST([0xC0; 0xA8; 0x00; 0x01] AS U8[4])", "3232235521" ] ;
    make "length" "Compute the length of a string or array"
      [ p [ cdata "Returns the number of " ; bold "bytes" ; cdata " in a \
                   string." ] ;
        warn_utf8 ;
        p [ cdata "Can also return the length of an array." ] ]
      [ [ cdata "LENGTH …string-expr…" ] ; [ cdata "LENGTH …array…" ] ]
      [ [ cdata "STRING -> U32" ] ; [ cdata "t[] -> U32" ] ]
      [ "LENGTH \"foo\"", "3" ;
        "LENGTH \"\"", "0" ;
        "LENGTH CAST([42] AS U8[])", "1" ] ;
    make "lower" "Return the lowercase version of a string"
      [ warn_utf8 ]
      [ [ cdata "LOWER …string-expr…" ] ]
      [ [ cdata "STRING -> STRING" ] ]
      [ "LOWER \"Foo Bar Baz\"", "\"foo bar baz\"" ] ;
    make "upper" "Return the uppercase version of a string"
      [ warn_utf8 ]
      [ [ cdata "UPPER …string-expr…" ] ]
      [ [ cdata "STRING -> STRING" ] ]
      [ "UPPER \"Foo Bar Baz\"", "\"FOO BAR BAZ\"" ] ;
    make "uuid_of_u128" "Print a U128 as an UUID"
      [ p [ cdata "Convert a U128 into a STRING using the traditional \
                   notation for UUIDs." ] ]
      [ [ cdata "UUID_OF_U128 …u128-expr…" ] ]
      [ [ cdata "U128 -> STRING" ] ]
      [ "UUID_OF_U128 0x123456789abcdeffedcba098765431",
        "\"00123456-789a-bcde-ffed-cba098765431\"" ] ;
    make "not" "Negation"
      [ p [ cdata "boolean negation." ] ]
      [ [ cdata "NOT …bool-expr…" ] ]
      [ [ cdata "BOOL -> BOOL" ] ]
      [ "NOT TRUE", "FALSE" ;
        "NOT (0 > 1)", "TRUE" ] ;
    make "abs" "Absolute value"
      [ p [ cdata "Return the absolute value of an expression of any numeric \
                   type." ] ]
      [ [ cdata "ABS …numeric-expr…" ] ]
      [ [ cdata "numeric -> numeric" ] ]
        (* Note: parentheses are required to disambiguate: *)
      [ "ABS(-1.2)", "1.2" ] ;
    make "neg" "Negation"
      [ p [ cdata "Negation of the numeric operand." ] ;
        p [ cdata "The return type is either the same as that of the operand, \
                   or a non smaller signed integer type" ] ]
      [ [ cdata "-…numerix-expr…" ] ]
      [ [ cdata "numeric -> signed-numeric" ] ]
      [ "-(1+1)", "-2" ] ;
    make "is-null" "Check for NULL"
      [ p [ cdata "Test any nullable operand for NULL. Always returns a \
                   non-nullable boolean." ] ;
        p [ cdata "Notice that this is not equivalent to comparing a value \
                   to NULL using the equality operator. Indeed, NULL propagates \
                   through the equality operator and the result of such a \
                   comparison would merely be NULL." ] ]
      [ [ cdata "…nullable-expr… IS NULL" ] ;
        [ cdata "…nullable-expr… IS NOT NULL" ] ]
      [ [ cdata "t? -> BOOL" ] ]
      [ "NULL IS NULL", "TRUE" ;
        "NULL IS NOT NULL", "FALSE" ;
        "(NULL = 1) IS NULL", "TRUE" ] ;
    make "exp" "Exponential function"
      [ p [ cdata "Compute the exponential function (as a FLOAT)." ] ]
      [ [ cdata "EXP …numeric-expr…" ] ]
      [ [ cdata "numeric -> FLOAT" ] ]
      [ "EXP 0", "1" ; "EXP 1", "2.71828182846" ] ;
    make "log" "Natural logarithm"
      [ p [ cdata "Compute the natural logarithm of the operand." ] ;
        p [ cdata "Returns NaN on negative operands." ] ]
      [ [ cdata "LOG …numeric-expr…" ] ]
      [ [ cdata "numeric -> FLOAT" ] ]
      [ "LOG 1", "0" ] ;
    make "log10" "Decimal logarithm"
      [ p [ cdata "Compute the decimal logarithm of the operand." ] ;
        p [ cdata "Returns NaN on negative operands." ] ]
      [ [ cdata "LOG10 …numeric-expr…" ] ]
      [ [ cdata "numeric -> FLOAT" ] ]
      [ "LOG10 100", "2" ] ;
    make "sqrt" "Square root"
      [ p [ cdata "Square root function." ] ;
        p [ cdata "Returns NULL on negative operands." ] ]
      [ [ cdata "SQRT …numeric-expr…" ] ]
      [ [ cdata "numeric -> FLOAT" ] ]
      [ "SQRT 16", "4" ;
        (* Note: parentheses are required to disambiguate: *)
        "SQRT(-1)", "NULL" ] ;
    make "sq" "Square"
      [ p [ cdata "Square function." ] ;
        p [ cdata "Returns the same type as the operand." ] ]
      [ [ cdata "SQ …numeric-expr…" ] ]
      [ [ cdata "numeric -> numeric" ] ]
      [ "SQ 4", "16" ] ;
    make "ceil" "Ceiling function"
      [ p [ cdata "Return the round value just greater or equal to the operand." ] ;
        p [ cdata "The result has the same type than the operand." ] ]
      [ [ cdata "CEIL …numeric-expr…" ] ]
      [ [ cdata "numeric -> numeric" ] ]
      [ "CEIL 41.2", "42" ] ;
    make "floor" "Floor function"
      [ p [ cdata "Return the round value just smaller or equal to the operand." ] ;
        p [ cdata "The result has the same type than the operand." ] ]
      [ [ cdata "FLOOR …numeric-expr…" ] ]
      [ [ cdata "numeric -> numeric" ] ]
      [ "FLOOR 42.7", "42" ;
        "FLOOR(-42.7)", "-43" ] ;
    make "round" "Rounding"
      [ p [ cdata "Return the closest round value." ] ;
        p [ cdata "The result has the same type than the operand." ] ]
      [ [ cdata "ROUND …numeric-expr…" ] ]
      [ [ cdata "numeric -> numeric" ] ]
      [ "ROUND 42.4", "42" ] ;
    make "cos" "Cosine"
      [ p [ cdata "Return the cosine of the operand." ] ]
      [ [ cdata "COS …numeric-expr…" ] ]
      [ [ cdata "numeric -> FLOAT" ] ]
      [ "COS PI", "-1" ] ;
    make "sin" "Sine"
      [ p [ cdata "Return the sine of the operand." ] ]
      [ [ cdata "SIN …numeric-expr…" ] ]
      [ [ cdata "numeric -> FLOAT" ] ]
      [ "SIN PI", "0" ] ;
    make "tan" "Tangent"
      [ p [ cdata "Return the tangent of the operand." ] ]
      [ [ cdata "TAN …numeric-expr…" ] ]
      [ [ cdata "numeric -> FLOAT" ] ]
      [ "TAN 0", "0" ] ;
    make "acos" "Arc-Cosine"
      [ p [ cdata "Return the arc-cosine of the operand." ] ]
      [ [ cdata "ACOS …numeric-expr…" ] ]
      [ [ cdata "numeric -> FLOAT" ] ]
      (* Must use parenths: *)
      [ "ACOS(-1)", "3.14159265359" ] ;
    make "asin" "Arc-Sine"
      [ p [ cdata "Return the arc-sine of the operand." ] ]
      [ [ cdata "ASIN …numeric-expr…" ] ]
      [ [ cdata "numeric -> FLOAT" ] ]
      [ "2 * ASIN 1", "3.14159265359" ] ;
    make "atan" "ArcTangent"
      [ p [ cdata "Return the arc-tangent of the operand." ] ]
      [ [ cdata "ATAN …numeric-expr…" ] ]
      [ [ cdata "numeric -> FLOAT" ] ]
      [ "4 * ATAN 1", "3.14159265359" ] ;
    make "cosh" "Hyperbolic Cosine"
      [ p [ cdata "Return the hyperbolic cosine of the operand." ] ]
      [ [ cdata "COSH …numeric-expr…" ] ]
      [ [ cdata "numeric -> FLOAT" ] ]
      [ "COSH 0", "1" ] ;
    make "sinh" "Hyperbolic Sine"
      [ p [ cdata "Return the hyperbolic sine of the operand." ] ]
      [ [ cdata "SINH …numeric-expr…" ] ]
      [ [ cdata "numeric -> FLOAT" ] ]
      [ "SINH 0", "0" ] ;
    make "tanh" "Hyperbolic Tangent"
      [ p [ cdata "Return the hyperbolic tangent of the operand." ] ]
      [ [ cdata "TANH …numeric-expr…" ] ]
      [ [ cdata "numeric -> FLOAT" ] ]
      [ "TANH 0", "0" ] ;
    make "hash" "Hash any value"
      [ p [ cdata "Compute a integer hash of a value of any type." ] ;
        p [ cdata "The hash function is deterministic." ] ]
      [ [ cdata "HASH …expr…" ] ]
      [ [ cdata "t -> I64" ] ]
      [ "HASH NULL", "NULL" ;
        "HASH (\"foo\"; \"bar\")", "731192513" ] ;
    make "parse_time" "Format a date"
      [ p [ cdata "This function takes a date as a string and convert it to a \
                   timestamp. It accepts various common encodings (similar to \
                   the UNIX at(1) command." ] ;
        p [ cdata "Beware that PARSE_TIME assumes all dates are in the local \
                   time zone." ] ;
        p [ cdata "The result is always nullable." ] ]
      [ [ cdata "PARSE_TIME …string-expr…" ] ]
      [ [ cdata "STRING -> FLOAT?" ] ]
      (* Divide by 24 so that this does not depend on the time zone: *)
      [ "(PARSE_TIME \"1976-01-28 12:00:00.9\") // 24h", "2218" ;
        "(PARSE_TIME \"12/25/2005\") // 24h", "13141" ] ;
    make "chr" "ASCII character of a given code"
      [ p [ cdata "Convert the given integer (must be below 255) into the \
                   corresponding ASCII character." ] ]
      [ [ cdata "CHR …int-expr…" ]]
      [ [ cdata "integer -> CHR" ] ]
      [ "CHR 65", "#\\A" ] ;
    make "like" "Comparing strings with patterns"
      [ p [ cdata "LIKE compares a string expression with a pattern, expressed \
                   as a string that can contain the special character " ;
            emph "percent" ;
            cdata " ('%') that can replace any substring including \
                   the empty substring, and the special character " ;
            emph "underscore" ;
            cdata " ('_') that can match any single character." ] ;
        p [ cdata "If you need to match precisely for any of those special \
                   characters you can escape them with the " ;
            emph "backslach" ;
            cdata " prefix ('\\'). Notice though that since backslash is also \
                   the default escaping character for strings, and pattern is \
                   written as a string, it has to be doubled in practice to \
                   serve as a pattern escape character!" ] ;
        p [ cdata "The result is true if the left string matches the pattern, \
                   or false otherwise." ] ;
        p [ cdata "Note that the pattern cannot be any string expression but \
                   must be a string constant." ] ]
      [ [ cdata "…string-expr… LIKE \"pattern\"" ] ]
      [ [ cdata "STRING -> BOOL" ] ]
      [ "\"foobar\" LIKE \"foo%\"", "true" ;
        (* Notice the doubled escape character: *)
        "\"foobar\" LIKE \"foo\\\\%\"", "false" ;
        "\"foobar\" LIKE \"f%r\"", "true" ;
        "\"foobar\" LIKE \"f__b_r\"", "true" ;
        "\"foobar\" LIKE \"fo_b%\"", "true" ;
        "\"foobar\" LIKE \"%baz\"", "false" ;
        "\"foobar\" LIKE \"\"", "false" ] ;
    make "fit" "(Multi)Linear regression"
      [ p [ cdata "General fitting function that takes one operands: an \
                   array or vector of observations of either unique numeric \
                   values or tuples of numeric values." ] ;
        p [ cdata "In the former case the unique value is supposed to be the \
                   value to predict (the " ; emph "dependent variable" ;
            cdata " in math jargon), using the start event time as the \
                   predictor (if available)." ] ;
        p [ cdata "In the later case, the first value of the tuple is the \
                   variable to predict and the others are the predictors \
                   (aka. the " ; emph "explanatory variables" ; cdata ")." ] ;
        p [ cdata "Notice that the value that is predicted is the last of the \
                   observations rather than the next observations, which \
                   makes that function more handy when comparing the last \
                   observation with the prediction. Obviously, that last \
                   observed value is not taken into account to compute the \
                   prediction (but the last predictors are of course)." ] ;
        p [ cdata "The result will be the predicted value." ] ]
      [ [ cdata "FIT …sequence-expr…" ] ]
      [ [ cdata "numeric[] -> FLOAT" ] ;
        [ cdata "numeric[n] -> FLOAT" ] ;
        [ cdata "(numeric; …; numeric)[] -> FLOAT" ] ;
        [ cdata "(numeric; …; numeric)[n] -> FLOAT" ] ]
      [ "FIT [1; 2; 3; 99]", "4" ;
        "FIT [(2; 1); (4; 2); (6; 3); (99; 4)]", "8" ] ;
    make "countrycode" "Country code of an IP"
      [ p [ cdata "Return the country-code (as a string) of an IP (v4 or \
                   v6)." ] ;
        p [ cdata "Ramen relies on an embedded, constant database of IP \
                   geo-locations that's designed for speed. It therefore \
                   makes no external request. As a downside, the information \
                   can be outdated or missing (in which case NULL is \
                   returned)." ] ;
        p [ cdata "Given the embedded database might change from one version \
                   of Ramen to the next, this function is not deterministic \
                   across upgrades." ] ;
        p [ cdata "This function should be only used as a hint." ] ]
      [ [ cdata "COUNTRYCODE …ip-expr…" ] ]
      [ [ cdata "IPv4 -> STRING?" ] ;
        [ cdata "IPv6 -> STRING?" ] ;
        [ cdata "Ip -> STRING?" ] ]
      [ "COUNTRYCODE 5.182.236.0", "\"AT\"" ;
        "COUNTRYCODE 2a00:1450:400f:804::2004", "\"IE\"" ] ;
    make "ipfamily" "Returns the version of an IP"
      [ p [ cdata "Returns either 4 if the IP is an IPv4 or 6 if the IP is an \
                   IPv6." ] ]
      [ [ cdata "IPFAMILY …ip-expr…" ] ]
      [ [ cdata "IPv4 -> unsigned-int" ] ;
        [ cdata "IPv6 -> unsigned-int" ] ;
        [ cdata "Ip -> unsigned-int" ] ]
      [ "IPFAMILY 135.181.17.92", "4" ;
        "IPFAMILY 2a01:4f9:4b:55b0::2", "6" ] ;
    make "basename" "Strip the directory part of a path"
      [ p [ cdata "Similar to the UNIX tool basename, this removes from the \
                   passed operand everything before the last slash ('/'), \
                   to keep only the filename portion of a path." ] ;
        p [ cdata "Does nothing if the operand contains no slash." ] ]
      [ [ cdata "BASENAME …string-expr…" ] ]
      [ [ cdata "STRING -> STRING" ] ]
      [ "BASENAME \"/foo/bar/baz\"", "\"baz\"" ;
        "BASENAME \"foo\"", "\"foo\"" ] ;
    make "min" "Minimum"
      [ p [ cdata "Return the minimum value of all the operands." ] ;
        p [ cdata "All operands must have compatible types (ie. it is \
                   possible to convert one into another)." ] ;
        p [ cdata "The result will be NULL if any of the operands is." ] ]
      [ [ cdata "MIN(…expr1…, …expr2…, …)" ] ]
      [ [ cdata "t1, t2, … -> largest(t1, t2, …)" ] ]
      [ "MIN(1, 2, 3)", "1" ;
        "MIN(\"foo\", \"bar\")", "\"bar\"" ;
        "MIN([5; 6], [3; 9])", "[3; 9]" ] ;
    make "max" "Maximum"
      [ p [ cdata "Return the maximum value of all the operands." ] ;
        p [ cdata "All operands must have compatible types (ie. it is \
                   possible to convert one into another)." ] ;
        p [ cdata "The result will be NULL if any of the operands is." ] ]
      [ [ cdata "MAX(…expr1…, …expr2…, …)" ] ]
      [ [ cdata "t1, t2, … -> largest(t1, t2, …)" ] ]
      [ "MAX(1, 2, 3)", "3" ;
        "MAX(\"foo\", \"bar\")", "\"foo\"" ;
        "MAX([5; 6], [3; 9])", "[5; 6]" ] ;
    make "coalesce" "Selects the first non null operand"
      [ p [ cdata "Accept a list of alternative values of the same type and \
                   returns the first one that is not NULL." ] ;
        p [ cdata "All alternatives but the last must be nullable." ] ;
        p [ cdata "If all alternatives are NULL then returns NULL." ] ]
      [ [ cdata "COALESCE(…expr1…, …expr2…, …)" ] ;
        (* Two operands variant: *)
        [ cdata "…expr1… |? …expr2…" ] ]
      [ [ cdata "(t?, t?, …, t?) -> t?" ] ;
        [ cdata "(t?, t?, …, t) -> t" ] ]
      [ "NULL |? 1", "1" ;
        "COALESCE(IF RANDOM > 1 THEN \"can't happen\", \"ok\")", "\"ok\"" ] ;
    make "add" "Addition"
      [ p [ cdata "Adds two numeric values." ] ;
        p [ cdata "The result have the type of the largest operand." ] ]
      [ [ cdata "…num-expr… + …num-expr…" ] ]
      [ [ cdata "num1, num2 -> largest(num1, num2)" ] ]
      [ "27 + 15", "42" ;
        "1.5 + 1u8", "2.5" ] ;
    make "sub" "Subtraction"
      [ p [ cdata "Subtracts two numeric values." ] ;
        p [ cdata "The result type is the largest of the operand types, and \
                   always signed." ] ]
      [ [ cdata "…num-expr… - …num-expr…" ] ]
      [ [ cdata "num1, num2 -> signed(largest(num1, num2))" ] ]
      [ "1u8 - 2u8", "-1" ] ;
    make "mul" "Multiplication"
      [ p [ cdata "Multiplies two numeric values, or repeat a string." ] ;
        p [ cdata "If the two operands are numeric, then the result type \
                   is the largest of the operand types." ] ;
        p [ cdata "Alternatively, can also be used to repeat a string if the \
                   first operand is an integer and the second a string. In \
                   that case the return type is a string." ] ]
      [ [ cdata "…num-expr… * …num-expr…" ] ;
        [ cdata "…int-expr… * …string-expr…" ] ]
      [ [ cdata "num1, num2 -> largest(num1, num2)" ] ;
        [ cdata "int, STRING -> STRING" ] ]
      [ "6 * 7", "42" ;
        "2 * \"foo\"", "\"foofoo\"" ] ;
    make "div" "Division"
      [ p [ cdata "Divides two numeric values." ] ;
        p [ cdata "The result is always a FLOAT. If the divisor and dividend \
                   are non-zero constants then the result is not nullable, \
                   otherwise it is, and the result of 0/0 is NULL." ] ]
      [ [ cdata "…num-expr… / …num-expr…" ] ]
      [ [ cdata "num1, num2 -> FLOAT?" ] ;
        [ cdata "num1, non-zero-const -> FLOAT" ] ]
      [ "84/2", "42" ;
        "1/0", "Inf" ;
        "0/0", "NULL" ] ;
    make "idiv" "Integer division"
      [ p [ cdata "Divides two numeric values, truncating toward zero." ] ;
        p [ cdata "The result type is the largest of the operand types." ] ;
        p [ cdata "For FLOAT operands the result will still be a FLOAT, \
                   floored." ] ]
      [ [ cdata "…num-expr… // …num-expr…" ] ]
      [ [ cdata "num1, num2 -> largest(num1, num2)" ] ]
      [ "10//3", "3" ;
        "-10//3", "-3" ;
        "10.5//3.1", "3" ] ;
    make "mod" "Modulo"
      [ p [ cdata "Compute the modolus of the two given operands." ] ;
        p [ cdata "The result type is the largest of the operand types." ] ]
      [ [ cdata "…num-expr… % …num-expr…" ] ]
      [ [ cdata "num1, num2 -> largest(num1, num2)" ] ]
      [ "3 % 2", "1" ;
        "-3 % 2", "-1" ;
        "3 % -2", "1" ] ;
    make "pow" "Power"
      [ p [ cdata "Compute the first operand to the power of the second." ] ;
        p [ cdata "The result type is the largest of the operand types." ] ]
      [ [ cdata "…num-expr… ^ …num-expr…" ] ]
      [ [ cdata "num1, num2 -> largest(num1, num2)" ] ]
      [ "2 ^ 3", "8" ;
        "PI ^ PI", "36.4621596072079" ] ;
    make "truncate" "Rounding to selected precision"
      [ p [ cdata "Truncate the first operand to the nearest (from below) \
                   multiple of the second, or to the nearest integer if no \
                   second operand is provided (it is then equivalent to " ;
            bold "floor" ; cdata "." ] ]
      [ [ cdata "TRUNCATE(…num-expr…, …num-expr…)" ] ;
        [ cdata "TRUNCATE …num-expr…" ] ]
      [ [ cdata "num1, num2 -> largest(num1, num2)" ] ]
      [ "TRUNCATE(153.6, 10)", "150" ;
        "TRUNCATE 5.8", "5" ;
        "TRUNCATE(-2.3)", "-3" ] ;
    make "reldiff" "Relative difference"
      [ p [ cdata "Compare the two operands A and B by computing:" ] ;
        p [ cdata "MIN(ABS(A-B), MAX(A, B)) / MAX(ABS(A-B), MAX(A, B))" ] ;
        p [ cdata "Returns 0 when A = B." ] ]
      [ [ cdata "RELDIFF(…num-expr…, …num-expr…)" ] ]
      [ [ cdata "num1, num2 -> FLOAT" ] ]
      [ "RELDIFF(1, 1)", "0" ;
        "RELDIFF(10, 9)", "0.1" ;
        "RELDIFF(9, 10)", "0.1" ;
        "RELDIFF(-9, -10)", "0.1" ;
        "RELDIFF(1, -10)", "1.1" ] ;
    make "and" "Boolean And"
      [ p [ cdata "Boolean AND operator." ] ]
      [ [ cdata "…bool-expr… AND …bool-expr…" ] ]
      [ [ cdata "BOOL, BOOL -> BOOL" ] ]
      [ "FALSE AND FALSE", "FALSE" ;
        "FALSE AND TRUE", "FALSE" ;
        "TRUE AND FALSE", "FALSE" ;
        "TRUE AND TRUE", "TRUE" ] ;
    make "or" "Boolean Or"
      [ p [ cdata "Boolean OR operator." ] ]
      [ [ cdata "…bool-expr… OR …bool-expr…" ] ]
      [ [ cdata "BOOL, BOOL -> BOOL" ] ]
      [ "FALSE OR FALSE", "FALSE" ;
        "FALSE OR TRUE", "TRUE" ;
        "TRUE OR FALSE", "TRUE" ;
        "TRUE OR TRUE", "TRUE" ] ;
    make "comparison" "Comparison operators"
      [ p [ cdata "Non-strict comparison operators." ] ;
        p [ cdata "Values of any type can be compared, but both values must \
                   have the same type." ] ]
      [ [ cdata "…expr… >= …expr…" ] ;
        (* Syntactic sugar for reversed operands: *)
        [ cdata "…expr… <= …expr…" ] ]
      [ [ cdata "t, t -> BOOL" ] ]
      [ "1 >= 0", "TRUE" ;
        "1 >= 1", "TRUE" ;
        "TRUE >= FALSE", "TRUE" ;
        "\"foo\" <= \"bar\"", "FALSE" ;
        "(5; 1) <= (5; 2)", "TRUE" ] ;
    make "strict-comparison" "Comparison operators"
      [ p [ cdata "Strict comparison operators." ] ;
        p [ cdata "Values of any type can be compared, but both values must \
                   have the same type." ] ]
      [ [ cdata "…expr… > …expr…" ] ;
        (* Syntactic sugar for reversed operands: *)
        [ cdata "…expr… < …expr…" ] ]
      [ [ cdata "t, t -> BOOL" ] ]
      [ "1 > 0", "TRUE" ;
        "1 > 1", "FALSE" ;
        "TRUE > FALSE", "TRUE" ;
        "\"foo\" < \"bar\"", "FALSE" ;
        "(5; 1) < (5; 2)", "TRUE" ] ;
    make "equality" "Equality test"
      [ p [ cdata "Test for equality." ] ;
        p [ cdata "Values of any type can be tested for equality, but both \
                   values must have the same type." ] ]
      [ [ cdata "…expr… = …expr…" ] ;
        [ cdata "…expr… != …expr…" ] ;
        [ cdata "…expr… <> …expr…" ] ]
      [ [ cdata "t, t -> BOOL" ] ]
      [ "\"foo\" = \"FOO\"", "FALSE" ;
        "[1; 2] <> [2; 1]", "TRUE" ;
        "PI != 3.14", "TRUE" ] ;
    make "concat" "Concatenation operation"
      [ p [ cdata "Concatenate two strings." ] ]
      [ [ cdata "…string-expr… || …string-expr…" ] ]
      [ [ cdata "STRING, STRING -> STRING" ] ]
      [ "\"foo\" || \"bar\"", "\"foobar\"" ] ;
    make "startswith" "String-starts-with operator"
      [ p [ cdata "Test if a string starts with a given substring." ] ;
        p [ cdata "Unlike the LIKE operator, does not require any of its \
                   operands to be a literal string value." ] ]
      [ [ cdata "…string-expr… STARTS WITH …string-expr…" ] ]
      [ [ cdata "STRING, STRING -> BOOL" ] ]
      [ "\"foobar\" STARTS WITH \"foo\"", "TRUE" ;
        "\"foo\"||\"bar\" STARTS WITH 2*\"x\"", "FALSE" ] ;
    make "endswith" "String-ends-with operator"
      [ p [ cdata "Test if a string ends with a given substring." ] ;
        p [ cdata "Unlike the LIKE operator, does not require any of its \
                   operands to be a literal string value." ] ]
      [ [ cdata "…string-expr… ENDS WITH …string-expr…" ] ]
      [ [ cdata "STRING, STRING -> BOOL" ] ]
      [ "\"foobar\" ENDS WITH \"bar\"", "TRUE" ;
        "\"foo\"||\"bar\" STARTS WITH 2*\"x\"", "FALSE" ] ;
    make "bit-and" "Bitwise AND operator"
      [ p [ cdata "Perform a bitwise AND operation with its integer \
                   operands." ] ]
      [ [ cdata "…int-expr… & …int-expr…" ] ]
      [ [ cdata "int, int -> int" ] ]
      [ "1029 & 15", "5" ] ;
    make "bit-or" "Bitwise OR operator"
      [ p [ cdata "Perform a bitwise OR operation with its integer \
                   operands." ] ]
      [ [ cdata "…int-expr… | …int-expr…" ] ]
      [ [ cdata "int, int -> int" ] ]
      [ "1025 | 5", "1029" ] ;
    make "bit-xor" "Bitwise XOR operator"
      [ p [ cdata "Perform a bitwise XOR operation with its integer \
                   operands." ] ]
      [ [ cdata "…int-expr… # …int-expr…" ] ]
      [ [ cdata "int, int -> int" ] ]
      [ "1029 # 15", "1034" ] ;
    make "bit-shift" "Bit shift operator"
      [ p [ cdata "Shift its first integer operand by the number of bits \
                   indicated by its second operand, to the left or to the \
                   right." ] ;
        p [ cdata "For right shifts, sign will be extended if the first \
                   operand is signed." ] ]
      [ [ cdata "…int-expr… << …int-expr…" ] ;
        [ cdata "…int-expr… >> …int-expr…" ] ]
      [ [ cdata "int, int -> int" ] ]
      [ "1029 >> 3", "128" ;
        "5 << 3", "40" ;
        "-4 >> 1", "-2" ] ;
    make "in" "Membership test"
      [ p [ cdata "Test if the first operand is a member of the second one." ] ;
        p [ cdata "The second operand can be either an array or vector, a \
                   STRING (in which case the first operand must be a string \
                   and s substring search will be performed) or a CIDR (in \
                   which case the first operand must be an IP address)." ] ]
      [ [ cdata "…expr… IN …expr…" ] ]
      [ [ cdata "t, t[] -> BOOL" ] ;
        [ cdata "t, t[n] -> BOOL" ] ;
        [ cdata "STRING, STRING -> BOOL" ] ;
        [ cdata "IP, CIDR -> BOOL" ] ]
      [ "42 IN [1; 2; 3]", "FALSE" ;
        "\"oo\" IN \"foobar\"", "TRUE" ;
        "192.168.10.21 IN 192.168.00.0/16", "TRUE" ] ;
    make "format_time" "format a timestamp as a date and time"
      [ p [ cdata "FORMAT_TIME turns a timestamp (second operand) into a \
                   string formatted according to a given template (first \
                   operand)." ] ;
        p [ cdata "The template, which need not be a literal string, can \
                   contain special character sequences that will be replaced \
                   with date and time elements from the timestamp." ] ;
        p [ cdata "The special sequences and what they stand for:" ] ;
        ul [ li [ cdata "%Y: year (4 digits)" ] ;
             li [ cdata "%d: day of month (2 digits)" ] ;
             li [ cdata "%H: hour of day (2 digits)" ] ;
             li [ cdata "%j: day of year" ] ;
             li [ cdata "%M: minutes in hour (2 digits)" ] ;
             li [ cdata "%m: month number (2 digits)" ] ;
             li [ cdata "%n: newline" ] ;
             li [ cdata "%t: tabulation" ] ;
             li [ cdata "%S: seconds in minute (with millisecond resolution)" ] ;
             li [ cdata "%s: the timestamp" ] ;
             li [ cdata "%u: day of week (as a number, 0 being Sunday)" ] ] ]
      [ [ cdata "FORMAT_TIME(…string-expr…, …num-expr…)" ] ]
      [ [ cdata "STRING -> num -> STRING" ] ]
      [ "FORMAT_TIME(\"Sunday was day #%u\", 1645354800)",
          "\"Sunday was day #0\"" ;
        "FORMAT_TIME(\"%Y-%m-%dT%H:%M:%S\", 1645354800)",
          "\"2022-02-20T12:00:00.00\"" ] ;
    make "index" "Find the first or last occurrence of a character"
      [ p [ cdata "Returns the position of the first (or last) occurrence of \
                   the given character in the given string." ] ;
        p [ cdata "The position of the first character is 0." ] ;
        p [ cdata "Notice that the search is case sensitive." ] ;
        p [ cdata "If the character is not present in the string then -1 is \
                   returned" ] ]
      [ [ cdata "INDEX(…string-expr…, …char-expr…)" ] ;
        [ cdata "INDEX FROM START(…string-expr…, …char-expr…)" ] ;
        [ cdata "INDEX FROM END(…string-expr…, …char-expr…)" ] ]
      [ [ cdata "STRING -> CHAR -> BOOL" ] ]
      [ "INDEX(\"foobar\", #\\o)", "1" ;
        "INDEX(\"foobar\", #\\O)", "-1" ;
        "INDEX FROM END(\"foobar\", #\\o)", "2" ] ;
    make "percentile" "Compute percentiles"
      [ p [ cdata "This function takes as first operand an array or vector \
                   and the second operand can be either a numeric constant \
                   or a immediate vector of non numeric constants. It then \
                   compute the requested percentile(s)." ] ;
        p [ cdata "When several percentiles are requested, the return value \
                   is a vector of the corresponding requested percentiles. \
                   If only one percentile was asked for, then it is returned \
                   directly." ] ;
        p [ cdata "Note that if you have several percentiles to compute it \
                   is much more efficient to make use of the vector syntax \
                   to compute all in one go than to perform several \
                   individual percentile operations." ] ]
      [ [ cdata "…num-const…th PERCENTILE …expr…" ;
          cdata "[ …num1…; …num2; … ] PERCENTILE …expr…" ] ]
      [ [ cdata "num1, num2[] -> num2" ] ;
        [ cdata "num1, num2[N] -> num2" ] ;
        [ cdata "num1[N1], num2[] -> num2[N1]" ] ;
        [ cdata "num1[N1], num2[N2] -> num2[N1]" ] ]
      [ "90th PERCENTILE [3; 5; 0; 2; 7; 8; 1; 9; 6; 10; 4]", "9" ;
        "[10th; 90th] PERCENTILE [3; 5; 0; 2; 7; 8; 1; 9; 6; 10; 4]", "[1; 9]" ] ;
    make "substring" "Extract a substring"
      [ p [ cdata "The first operand is the original string, and the two \
                   others are positions in that string where the substring \
                   starts and stops (with the usual convention that start \
                   is inclusive and stop is exclusive)." ] ;
        p [ cdata "If one of the position is negative then it is meant to \
                   be relative to the end of the string." ] ]
      [ [ cdata "SUBSTRING(…string-expr…, …int-expr…, …int-expr…)" ] ]
      [ [ cdata "STRING, int, int -> STRING" ] ]
      [ "SUBSTRING(\"foobar\", 2, 4)", "\"ob\"" ;
        "SUBSTRING(\"foobar\", -2, 6)", "\"ar\"" ;
        "SUBSTRING(\"foobar\", -4, -2)", "\"ob\"" ;
        "SUBSTRING(\"foobar\", 20, 40)", "\"\"" ;
        "SUBSTRING(\"foobar\", -20, -40)", "\"\"" ] ;
    make "aggrmin" "Minimum (aggregate)" ~has_state:true
      [ p [ cdata "Selects the minimum value." ] ]
      [ [ cdata "MIN …expr…" ] ]
      [ [ cdata "t sequence -> t" ] ]
      [ "MIN [ 3; 6; 2; 4; 5 ]", "2" ;
        "MIN SKIP NULLS [ u32?(3) ; NULL ]", "3" ;
        "MIN KEEP NULLS [ u32?(3) ; NULL ]", "NULL" ] ;
    make "aggrmax" "Maximum (aggregate)" ~has_state:true
      [ p [ cdata "Selects the maximum value." ] ]
      [ [ cdata "MAX …expr…" ] ]
      [ [ cdata "t sequence -> t" ] ]
      [ "MAX [ 3; 6; 2; 4; 5 ]", "6" ;
        "MAX SKIP NULLS [ u32?(3) ; NULL ]", "3" ;
        "MAX KEEP NULLS [ u32?(3) ; NULL ]", "NULL" ] ;
    make "aggrsum" "Sum (aggregate)" ~has_state:true
      [ p [ cdata "Compute the sum of the values." ] ]
      [ [ cdata "SUM …expr…" ] ]
      [ [ cdata "num sequence -> num" ] ]
      [ "SUM [ 3; 6; 2; 4; 5 ]", "20" ;
        "SUM SKIP NULLS [ u32?(3) ; NULL ]", "3" ;
        "SUM KEEP NULLS [ u32?(3) ; NULL ]", "NULL" ] ;
    make "aggravg" "Average (aggregate)" ~has_state:true
      [ p [ cdata "Compute the average value." ] ]
      [ [ cdata "AVG …expr…" ] ]
      [ [ cdata "num sequence -> FLOAT" ] ]
      [ "AVG [ 3; 6; 2; 4; 5 ]", "4" ;
        "AVG SKIP NULLS [ u32?(3) ; NULL ]", "3" ;
        "AVG KEEP NULLS [ u32?(3) ; NULL ]", "NULL" ] ;
    make "aggrand" "Logical AND (aggregate)" ~has_state:true
      [ p [ cdata "AND all the values together." ] ]
      [ [ cdata "AND …expr…" ] ]
      [ [ cdata "BOOL sequence -> BOOL" ] ]
      [ "AND [ TRUE ; TRUE ; FALSE ; TRUE ]", "FALSE" ;
        "AND KEEP NULLS [ bool?(TRUE) ; NULL ; bool?(TRUE) ]", "NULL" ] ;
    make "aggror" "Logical OR (aggregate)" ~has_state:true
      [ p [ cdata "OR all the values together." ] ]
      [ [ cdata "OR …expr…" ] ]
      [ [ cdata "BOOL sequence -> BOOL" ] ]
      [ "OR [ TRUE ; TRUE ; FALSE ; TRUE ]", "TRUE" ;
        "OR KEEP NULLS [ bool?(TRUE) ; NULL ; bool?(TRUE) ]", "NULL" ] ;
    make "aggrbitand" "Bitwise AND (aggregate)" ~has_state:true
      [ p [ cdata "Bitwise AND all values together." ] ]
      [ [ cdata "BITAND …int-expr…" ] ]
      [ [ cdata "int sequence -> int" ] ]
      [ "BITAND [ 12; 5; 4 ]", "4" ] ;
    make "aggrbitor" "Bitwise OR (aggregate)" ~has_state:true
      [ p [ cdata "Bitwise OR all values together." ] ]
      [ [ cdata "BITOR …int-expr…" ] ]
      [ [ cdata "int sequence -> int" ] ]
      [ "BITOR [ 3; 2; 4 ]", "7" ] ;
    make "aggrbitxor" "Bitwise XOR (aggregate)" ~has_state:true
      [ p [ cdata "Bitwise XOR all values together." ] ]
      [ [ cdata "BITXOR …int-expr…" ] ]
      [ [ cdata "int sequence -> int" ] ]
      [ "BITXOR [ 1; 2; 5 ]", "6" ] ;
    make "aggrfirst" "First value" ~has_state:true
      [ p [ cdata "Selects the first value of a sequence." ] ]
      [ [ cdata "FIRST …expr…" ] ]
      [ [ cdata "t sequence -> t" ] ]
      [ "FIRST [ 1; 2; 3 ]", "1" ] ;
    make "aggrlast" "Last value" ~has_state:true
      [ p [ cdata "Selects the last value of a sequence." ] ]
      [ [ cdata "LAST …expr…" ] ]
      [ [ cdata "t sequence -> t" ] ]
      [ "LAST [ 1; 2; 3 ]", "3" ] ;
    make "aggrhistogram" "Build an histogram" ~has_state:true
      [ p [ cdata "Given a sequence of numeric values, build an histogram \
                   for values between the provided minimum and maximum \
                   values." ] ;
        p [ cdata "The third operand is the number of buckets." ] ;
        p [ cdata "The operation returns a vector of this number of buclets \
                   unsigned counters, with one extra bucket at each end to \
                   account for values respectively lesser and greater than \
                   the limits." ] ]
      ~limitations:[ p [ cdata "The histogram limits and number of nuckets \
                                must be constants." ] ]
      [ [ cdata "HISTOGRAM(…expr…, …const-float…, …const-float…, …const-int…)" ] ]
      [ [ cdata "t, FLOAT, FLOAT, N:uint -> u32[N+2]" ] ]
      [ "HISTOGRAM([ 5.1; 5.3; 3.2; 2.1; 3.7; 5.6; 1.4 ], 0, 6, 6)",
        "[ 0; 0; 1; 1; 2; 0; 3; 0 ]" ] ;
    make "group" "Collect values into an array" ~has_state:true
      [ p [ cdata "This operator aggregates all values into an array." ] ]
      ~limitations:[ p [ cdata "The order of values in the array is undefined." ] ]
      [ [ cdata "GROUP …expr…" ] ]
      [ [ cdata "t sequence -> t[]" ] ]
      (* FIXME: have a sort function to clean this *)
      [ "GROUP [1; 2; 3]", "[3; 2; 1]" ] ;
    make "count" "Count" ~has_state:true
      [ p [ cdata "If the counted expression is a boolean, count how many are \
                   true. Otherwise, is equivalent to " ; bold "SUM 1" ;
            cdata "." ] ]
      [ [ cdata "COUNT …expr…" ] ]
      [ [ cdata "t sequence -> u32" ] ]
      [ "COUNT [ 1; 1; 1 ]", "3" ;
        "COUNT [ TRUE ; FALSE ; TRUE ]", "2" ] ;
    make "distinct" "Tells if each item is distinct" ~has_state:true
      [ p [ cdata "Accurately tells if the same item was already met in \
                   the aggregate." ] ;
        p [ cdata "See " ; bold "REMEMBER" ;
            cdata " for a safer approximation of this." ] ]
      [ [ cdata "DISTINCT …expr…" ] ]
      [ [ cdata "t sequence -> BOOL" ] ]
      (* Tells whether the *last value* is distinct from the previous ones: *)
      [ "DISTINCT [ 1; 2; 3 ]", "TRUE" ;
        "DISTINCT [ 1; 2; 1 ]", "FALSE" ;
        (* FIXME: Distinct result type will be a single BOOL that is FALSE
         * and so COUNT will be 0 not 2 as one might expect. :-(
         * Instead: distinct should not be an aggregate function at all. If
         * we want it to be, it's easy enough to write "DISTINCT GROUP(X)"
         * instead of just DISTINCT X.*)
        "COUNT DISTINCT [ 1; 2; 1 ]", "0" ] ;
    make "lag" "Delayed value" ~has_state:true
      [ p [ bold "LAG" ; cdata " refers to the value received k steps ago. \
            Therefore " ; bold "LAG 1 x" ; cdata " refers to previous value \
            of " ; bold "x" ; cdata ", " ; bold "LAG 2 x" ; cdata " refers \
            to the value before the previous one, and so on." ] ;
        p [ cdata "When less than k values have been received then " ;
            bold "LAG k" ; cdata " is NULL." ] ]
      [ [ cdata "LAG …unsigned-int-expr… …expr…" ] ;
        [ cdata "LAG …expr…" ; tag "br" [] ;
          cdata "Equivalent to: LAG 1 …expr…" ] ]
      [ [ cdata "int, t -> t" ] ]
      [] ;
    make "smooth" "Exponential smoothing" ~has_state:true
      [ p [ cdata "Blend a value with the latest blend using various \
                   techniques, the simplest of which uses only one coefficient \
                   α according to the formula: " ; emph "blended = new × α + \
                   previous-blend × (1 - α)" ; cdata "." ] ;
        p [ cdata "The first operand, α, is a floating point number between \
                   0 and 1." ] ]
      [ [ cdata "SMOOTH …float-expr… …num-expr…" ] ]
      [ [ cdata "FLOAT -> num -> FLOAT" ] ]
      [] ;
    make "sample" "Reservoir sampling" ~has_state:true
      [ p [ cdata "Build a random set of values of a given maximum size." ] ;
        p [ cdata "The first operand " ; emph "k" ; cdata " is the maximum \
                   size of the set and the second is the value to be \
                   sampled." ] ;
        p [ cdata "If fewer than " ; emph "k" ; cdata " values are received \
                   then the result will contain them all. If more than " ;
            emph "k" ; cdata " values are received then the result will have \
                   exactly " ; emph "k" ; cdata " values (not taking into \
                   consideration skipping over NULL values). Every \
                   received value has the same probability to be part of \
                   the resulting set." ] ;
        p [ cdata "This function comes handy to reduce the input size of \
                   a memory expensive operation such as a percentile \
                   computation." ] ]
      [ [ cdata "SAMPLE …unsigned-int-expr… …expr…" ] ]
      [ [ cdata "N:uint, t -> t[<=N]" ] ]
      [] ;
    make "one-out-of" "Nullifies all values but one out of N" ~has_state:true
      [ p [ cdata "The first operand, which must be a constant unsigned \
                   integer, gives the periodicity of non-null values. The \
                   second operand is the expression to return when its time \
                   for a non NULL result." ] ]
      [ [ cdata "ONE OUT OF …unsigned-int-expr… …expr…"] ]
      [ [ cdata "uint, t -> t?" ] ]
      [] ;
    make "moveavg" "Moving average" ~has_state:true
      [ p [ cdata "Average of the last " ; emph "k" ; cdata " values." ] ]
      [ [ emph "k" ; cdata "-MOVEAVG …num-expr…" ] ;
        [ emph "k" ; cdata "-MA …num-expr…" ] ]
      [ [ cdata "uint, num -> num" ] ]
      [] ;
    make "hysteresis" "Tells if a value exceeded a threshold or recovered"
         ~has_state:true
      [ p [ cdata "Becomes false when the monitored value reaches the given \
                   threshold, and remains false until the value goes \
                   back to the given recovery value." ] ;
        p [ cdata "Notice that if the threshold is greater than the recovery \
                   then it is a maximum. Otherwise it acts as a minimum." ] ;
        p [ cdata "The first operand is the monitored value." ] ;
        p [ cdata "The second operand is the threshold." ] ;
        p [ cdata "Finally, the third operand is the recovery value." ] ]
      [ [ cdata "HYSTERESIS(…num-expr…, …num-expr…, …num-expr…)" ] ]
      [ [ cdata "num, num, num -> BOOL" ] ]
      [] ;
    make "once-every" "" ~has_state:true
      [ p [ bold "EVERY" ; cdata " is similar to " ; bold "ONE OUT OF" ;
            cdata " but based on time rather than number of inputs." ] ;
        p [ cdata "If " ; emph "TUMBLING" ;
            cdata " is selected (or the second operand of the functional \
                   syntax is true) then time is divided into windows \
                   of the given duration (aligned such that timestamp modulo \
                   duration is NULL), and the first value of each new \
                   such window is selected (and all others are NULL), \
                   whereas if " ;
            emph "SLIDING" ; cdata " is selected then the next value \
            after at least the selected duration is selected (regardless \
            of time alignment)." ] ]
      ~limitations:[ p [ cdata "The period must be a constant." ] ]
      [ [ cdata "EVERY …num-expr… [ SLIDING | TUMBLING ] …expr…" ] ;
        [ cdata "EVERY(…num-expr…, …bool-expr…, …expr…)" ] ]
      [ [ cdata "num, BOOL, t -> t?" ] ]
      [] ;
    make "remember" "Remember values for some time" ~has_state:true
      [ p [ cdata "Uses a series of Bloom-filters to remember any value for \
                   a given duration." ] ;
        (* TODO: let's get rid of 2nd operand (time) *)
        p [ cdata "Returns TRUE if the value have been already encountered \
                   up to a configurable time " ; emph "D" ;
            cdata " in the past (3rd operand)." ] ;
        p [ cdata "The rate of false positive is provided (as the first \
                   operand) so that the trade-off between memory consumption \
                   and accuracy is easy to configure." ] ;
        p [ cdata "The 4th and last operand is the value to be remembered." ] ;
        p [ cdata "When a new value is remembered, the " ; bold "REMEMBER" ;
            cdata " function will refresh its memory so that this value will \
                   be remembered for the next " ; emph "D" ;
            cdata " duration, whereas the " ; bold "RECALL" ;
            cdata " function will not, so that values will be reminded only \
                   for a duration " ; emph "D" ; cdata " after it's first \
                   encountered." ] ]
      [ [ cdata "REMEMBER(…float-expr…, …time-expr…, …num-expr…, …expr…)" ] ;
        [ cdata "RECALL(…float-expr…, …time-expr…, …num-expr…, …expr…)" ] ]
      [ [ cdata "FLOAT, FLOAT, num, t -> BOOL" ] ]
      [] ;
    make "largest" "Select largest (or smallest values" ~has_state:true
      [ p [ cdata "Selects the " ; emph "N" ; cdata " largest or smallest \
                   values, possibly skipping the first ones." ] ;
        p [ cdata "The weight of each value in the comparison can be \
                   specified as a last operand (by default the value \
                   itself is used)." ] ;
        p [ cdata "Result will be NULL  whenever the number of received items \
                   is less than the requested number, unless " ; bold "UP TO" ;
            cdata " is specified, or if all items have been skipped as NULL." ] ]
      ~limitations:[ p [ cdata "The number of selected values (as well as the \
                                number of skipped values) must be constants" ] ]
      [ [ cdata "[LARGEST | SMALLEST] [BUT …num-expr…] [UP TO] …num-expr… …expr… \
                 [BY …num-expr…, …num-expr…, …]" ] ;
        [ cdata "[LATEST | OLDEST] [BUT …num-expr…] [UP TO] …num-expr… …expr…" ] ;
        (* Order by time: *)
        [ cdata "EARLIER [UP TO] …num-expr… …expr…" ] ]
      [ [ cdata "uint, uint, t, BY:[t1, t2, …] -> t[]" ] ]
      [] ;
    make "top" "Detect the top contributors" ~has_state:true
      [ p [ cdata "The " ; bold "TOP" ; cdata " operation has several use \
            cases. In each of those, the operator computes an estimation of \
            the top " ; emph "N" ; cdata " contributors " ; emph "C" ;
            cdata " according to some metric " ; emph "W" ;
            cdata " (weight)." ] ;
        p [ cdata "In its simplest form, the operator merely returns that \
                   list of contributors." ] ;
        p [ cdata "But oftentimes one just want to know it some value is in \
                   the top, so the operator can then return a single \
                   boolean." ] ;
        p [ cdata "Finally, one might also want to know what rank, if any, \
                   some contributor occupies in the top, and in this last \
                   form the operator returns a nullable unsigned integer." ] ;
        p [ cdata "Parameters control not only the accuracy of the top \
                   approximation but also how quickly top contributors will \
                   fade to make room for newer ones in long running \
                   aggregations." ] ;
        p [ cdata "Such " ; bold "TOP" ; cdata " operation may return \
                   insignificant contributors if not that many really big \
                   contributors exist. To filter those out, a last parameter \
                   sets a threshold as a multiple of the standard deviation \
                   of all the weights that any contributor must met to be \
                   considered note-worthy." ] ]
      [ [ cdata "LIST TOP …int-expr… [ OVER …int-expr… ] \
                 …expr… [ BY …num-expr… ] [ FOR THE LAST …float-expr… ] \
                 [ ABOVE …num-expr… SIGMAS ]" ] ;
        [ cdata "IS …expr… IN TOP …int-expr… [ OVER …int-expr… ] \
                 [ BY …num-expr… ] [ FOR THE LAST …float-expr… ] \
                 [ ABOVE …num-expr… SIGMAS ]" ] ;
        [ cdata "RANK OF …expr… IN TOP …int-expr… [ OVER …int-expr… ] \
                 [ BY …num-expr… ] [ FOR THE LAST …float-expr… ] \
                 [ ABOVE …num-expr… SIGMAS ]" ] ]
      [ [ cdata "int, int, t, num, FLOAT, num -> t[]" ] ;
        [ cdata "t, int, int, num, FLOAT, num -> BOOL" ] ;
        [ cdata "t, int, int, num, FLOAT, num -> uint" ] ]
      [] ;
    make "past" "Group past values based on time" ~has_state:true
      [ p [ bold "PAST" ; cdata " is similar to " ; bold "GROUP" ;
            cdata " but it regroups values based on time rather than \
                   number of incoming values. Also, it contains an internal \
                   sampling mechanism." ] ;
        p [ cdata "If " ; emph "TUMBLING" ;
            cdata " is selected then time is divided into windows \
                   of the given duration aligned such that timestamp modulo \
                   duration is NULL, and those whole windows worth of values \
                   are returned in one go once they are completed." ] ;
        p [ cdata "Otherwise, if " ; emph "SLIDING" ;
            cdata " if selected (or if nothing is specified) then at every \
                   step the last values within the specified duration are \
                   returned. This is the default behavior." ] ]
      [ [ cdata "[ SAMPLE [ OF SIZE ] …num-expr… [ OF THE ] ] PAST …num-expr… \
                 [ SLIDING | TUMBLING ] [ OF ] …expr…" ] ]
      ~limitations:[ p [ cdata "The duration of the time window as well as \
                                the sampling size must be constants." ] ]
      [ [ cdata "num, num, t -> t[]" ] ]
      [] ;
    make "split" "Split a string" ~has_state:true
      [ p [ cdata "split a string by some substring and produce the resulting \
                   words." ] ;
        p [ cdata "The first operand is the delimiter and the second operand \
                   is the source string to be split." ] ;
        p [ cdata "Contrary to all other operations documented in this manual, \
                   split is not a function but a generator: given a single \
                   input string it can generate from zero to N values." ] ;
        p [ cdata "When a generator is present in the output type of a RaQL \
                   worker then all other values are simply repeated for each \
                   generated output. For instance, if we were to execute:" ] ;
        p [ pre [ raw "SELECT SPLIT(\" \", \"foo bar\") AS word, 42 AS i\n" ] ] ;
        p [ cdata "then the output would be the two tuples:" ] ;
        p [ pre [ raw "\"foo\", 1\n\"bar\", 1\n" ] ] ;
        p [ cdata "When several generators are present then the output is \
                   the cartesian product of the output of the generators." ] ]
      [ [ cdata "SPLIT(…string-expr…, …string-expr…)" ] ]
      [ [ cdata "STRING, STRING -> STRING generator" ] ]
      []
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
    [ "fit" ; (*"group" ; "past"*) ] ;
    [ "countrycode" ; "ipfamily" ] ;
    [ "min" ; "max" ] ;
    [ "floor" ; "idiv" ] ;
    [ "add" ; "sub" ; "mul" ; "div" ; "idiv" ] ;
    [ "div" ; "idiv" ; "mod" ] ;
    [ "mul" ; "pow" ] ;
    [ "reldiff" ; "sub" ] ;
    [ "and" ; "or" ] ;
    [ "comparison" ; "strict-comparison" ; "equality" ] ;
    [ "like" ; "startswith" ; "endswith" ] ;
    [ "bit-and" ; "bit-or" ; "bit-xor" ; "bit-shift" ] ;
    [ "parse_time" ; "format_time" ] ;
    [ "index" ; "startswith" ; "endswith" ] ;
    [ "min" ; "aggrmin" ] ;
    [ "max" ; "aggrmax" ] ;
    [ "aggrsum" ; "aggravg" ] ;
    [ "aggrand" ; "aggror" ] ;
    [ "aggrand" ; "and" ] ;
    [ "aggror" ; "or" ] ;
    [ "aggrbitand" ; "aggrbitor" ; "aggrbitxor" ] ;
    [ "aggrbitand" ; "bit-and" ] ;
    [ "aggrbitor" ; "bit-or" ] ;
    [ "aggrbitxor" ; "bit-xor" ] ;
    [ "aggrfirst" ; "aggrlast" ] ;
    [ "count" ; "distinct" ; "remember" ] ;
    [ "one-out-of" ; "once-every" ; "past" ] ;
    [ "group" ; "past" ]
  ]
