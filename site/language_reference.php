<? include 'header.php' ?>
<h1>Ramen Language Reference</h1>

<p>Values are described first, then expressions, then operations, and finally programs. All these concepts reference each others so there is no reading order that would save the reader from jumping around. First reading may not be clear but everything should fall into place eventually. Starting with a quick look at the <a href="/glossary.html">glossary</a> may help.</p>

<a name="syntax">
<h2>Basic Syntax</h2>
</a>

<h3>Blanks</h3>

<p>Any space, tab, newline or comment is a separator.</p>

<h3>Comments</h3>

<p>As in SQL, two dashes introduce a line comment. Everything from those dashes and the end of that line is treated as space.</p>

<p>There is no block comments.</p>

<h3>Quotation</h3>

<p>Some rare reserved keywords cannot be used as identifiers unless surrounded by simple quotes.  Quotes can also be used around operation names if they include characters that would be illegal in an identifier, such as spaces, dots or dashes.</p>

<a name="values">
<h2>Values</h2>
</a>

<h3>NULLs</h3>

<p>Like in SQL, a field may have the NULL value. Ramen typing system knows what value can be NULL and spare the NULL checks unless necessary.</p>

<p>Users can check if a nullable value is indeed NULL using the <code>IS NULL</code> or <code>IS NOT NULL</code> operators, which turn a nullable value into a (non-nullable) boolean.</p>

<p><code>NULL</code> is both a type and a value. The <code>NULL</code> value is the only possible value of the <code>NULL</code> type. It is also a possible value for any nullable type.</p>

<p>To write a literal <code>NULL</code> value enter <code>NULL</code>.</p>

<p>For any type <code>t</code>, the type <code>t?</code> denotes the type of possibly <code>NULL</code> values.</p>

<h3>Booleans</h3>

<p>The type for booleans is called <code>boolean</code> (<code>bool</code> is also accepted). Boolean true and false are spelled <code>true</code> and <code>false</code>.</p>

<h3>Strings</h3>

<p>The type for character strings is called <code>string</code>.  A literal string is double quoted (with <code>"</code>). To include a double-quote within a string, backslash it.  Other characters can be backslashed: single quote (<code>"\'"</code>), newlines (<code>"\n"</code> and <code>"\r"</code>), horizontal tab (<code>"\t"</code>), backspace (<code>"\b"</code>) and the backslash itself (<code>"\\"</code>).</p>

<p>Some functions consider strings as UTF-8 encoded, some consider strings as mere sequence of bytes.</p>

<h3>Floats</h3>

<p>The type for real numbers is called <code>float</code>. It is the standard IEEE.754 64 bits float.  Literal values will cause minimum surprise: dot notation (<code>"3.14"</code>) and scientific notation (<code>"314e-2"</code>) are supported.</p>

<h3>Integers</h3>

<p>Ramen allows integer types of 5 different sizes from 8 to 128 bits, signed or unsigned: <code>i8</code>, <code>i16</code>, <code>i32</code>, <code>i64</code>, <code>i128</code>, that are signed, and <code>u8</code>, <code>u16</code>, <code>u32</code>, <code>u64</code> and <code>u128</code>, that are unsigned.</p>

<p>Ramen uses the conventional 2-complement encoding of integers with silent wrap-around in case of overflow.</p>

<p>When writing a literal integer it is possible to specify the intended type by suffixing it with the type name; for instance: <code>42u128</code> would be an unsigned integer 128 bits wide with value <code>42</code>. If no such suffix is present then Ramen will choose the narrowest possible type that can accommodate that integer value and that's not smaller than i32.  Thus, to get a literal integer smaller than i32 one has to suffix it. This is to avoid having non-intentionally narrow constant values that would wrap around unexpectedly.</p>

<p>In addition to the suffix, you can also use a cast, using the type name as a function: <code>u128(42)</code>. This is equivalent but more general as it can be used on other expression than simple literal integers, such as floats or booleans.</p>

<h3>Network addresses</h3>

<p>Ethernet addresses are accepted with the usual notation, such as: <code>18:d6:c7:28:71:f5</code> (without quotes; those are not strings). They are internally stored as 48bits unsigned integers and can be cast from/to other integer types.</p>

<p>IP addresses are also accepted, either v4 or v6, again without strings.</p>

<p>CIDR addresses are also accepted; for instance <code>192.168.10.0/24</code> (there is no ambiguity with integer division since arithmetic operators do not apply to IP addresses).</p>

<p>NOTE: the <code>in</code> operator can check whether an IP belongs to a CIDR.</p>

<h3>Compound types</h3>

<h4>Vectors</h4>

<p>Vectors of values of the same type can be formed with <code>[ expr1 ; expr2 ; expr3 ]</code>.</p>

<h4>Lists</h4>

<p>Lists are like vectors which dimension is variable. It is not possible to create an immediate list (that would be a vector) but lists can be obtained as the result of a function.</p>

<h4>Tuples</h4>

<p>Tuples are an ordered set of values of any type.</p>

<!--
<h3>Units</h3>

<p>In addition to a type, values can optionally have units. When values with units are combined then the combination units will be automatically computed. In addition, Ramen will perform dimensional analysis to detect meaningless computations.</p>

<p>The syntax to add units is to add them in between curly braces.<p>

<p>TODO: Better printer/parser for units</p>
-->

<a name="expressions">
<h2>Expressions</h2>
</a>

<h3>Literal values</h3>

<p>Any literal value (as described in the previous section) is a valid expression.</p>

<a name="tuple-field-names">
<h3>Tuple field names</h3>
</a>

<p>In addition to literal values one can refer to a tuple field. Which tuples are available depends on the <a href="/glossary.html#Clause">clause</a> but the general syntax is: `tuple_name.field_name`. The prefix (before the dot) can be omitted in most cases; if so, the field is understood to refer to the "in" tuple (the input tuple).</p>

<p>Here is a list of all possible tuples, in order of appearance in the data flow:</p>

<a name="input-tuple">
<h4>Input tuple</h4>
</a>

<p>The tuple that has been received as input.  Its name is <code>in</code> and that's also the default tuple when the tuple name is omitted.</p>

<p>You can use the <code>in</code> tuple in all clauses as long as there is an input.  When used in a <code>commit</code> clause, it refers to the last received tuple.</p>

<a name="last-in-tuple">
<h4>Last Input tuple</h4>
</a>

<p>Named <code>in.last</code>, it is the _previous_ input tuple.  Can be used to retrieve the field of the previous received tuple.</p>

<p>Can be used in the <code>where</code>, <code>select</code> and <code>commit/flush</code> clauses.</p>

<p>When <code>in</code> is the first tuple ever, then <code>in.last</code> is the same as <code>in</code>.  This situation can nonetheless be detected using the <code>#count</code> virtual field.</p>

<a name="selected-tuple">
<h4>Selected tuple</h4>
</a>

<p>Named <code>selected.last</code>, this is the last tuple that passed the <code>WHERE</code> filter (before <code>in</code>).</p>

<p>The <code>selected.last</code> tuple can be used anywhere but in a <code>group-by</code> clause.</p>

<p>There is also a <code>selected</code> tuple that has only virtual fields.  See <a href="/glossary.html#virtual-fields">next section about virtual fields</a> for details.</p>

<p>When <code>in</code> is the first tuple to pass the <code>WHERE</code> filter then <code>selected.last</code> is the same as <code>in</code>. This situation can nonetheless be detected using the <code>#count</code> virtual field.</p>

<a name="unselected-tuple">
<h4>Unselected tuple</h4>
</a>

<p>Named <code>unselected.last</code>, this is the last tuple that failed to pass the <code>WHERE</code> filter.</p>

<p>It can be used in the same places as the <code>selected</code> tuple, that is pretty much everywhere.</p>

<p>When no tuple failed the <code>WHERE</code> filter yet, then <code>unselected.last</code> is the same as <code>in</code> but for the virtual fields.</p>

<p>There is also a <code>unselected</code> tuple that has only virtual fields.</p>

<a name="output-tuple">
<h4>Output tuple</h4>
</a>

<p>The tuple that is going to be output (if the <code>COMMIT</code> condition holds <code>true</code>).  Its name is <code>out</code>.  The only places where it can be used is in the commit clause.</p>

<p>It is also possible to refer to fields from the out tuple in <code>select</code> clauses which creates the out tuple, but only if the referred fields has been defined earlier. So for instance this is valid:</p>

<pre>
  SELECT
    sum payload AS total,
    end - start AS duration,
    total / duration AS bps
</pre>

<p>...where we both define and reuse the fields <code>total</code> and <code>duration</code>. Notice that here the name of the tuple has been eluded -- despite "in" being the default tuple, on some conditions it is OK to leave out the "out" prefix as well.  This would be an equivalent, more explicit statement:</p>

<pre>
  SELECT
    sum in.payload AS total,
    in.end - in.start AS duration,
    out.total / out.duration AS bps
</pre>

<p>It is important to keep in mind that the input and output tuples have different types (in general).</p>

<a name="previous-tuple">
<h4>Previous tuple</h4>
</a>

<p>Named <code>out.previous</code> or just <code>previous</code>, refers to the last output tuple.</p>

<p>Can be used in <code>select</code>, <code>where</code> and <code>commit</code> clauses.</p>

<p>When no tuples have been output yet that tuple has all its field set to Null.  Therefore, if you use this tuple you must check for nulls accordingly.</p>

<p>Same type as the <code>out</code> tuple, with all fields nullable.</p>

<a name="group-first-tuple">
<h4>First tuple in group</h4>
</a>

<p>Named <code>group.first</code> or just <code>first</code>, refers to the first tuple of an aggregation.  Can be used anywhere but in the <code>group-by</code> clause itself.</p>

<p>Same type as the input tuple.</p>

<p>There is also a <code>group</code> tuple with only virtual fields.</p>

<p>NOTE: It is worth noting that it makes the operation slower to use any tuple from the <code>group</code> family in the <code>where</code> clause since it requires to build the key and retrieve the aggregate even for tuples that will end up being filtered out.</p>

<a name="group-last-tuple">
<h4>Last tuple in group</h4>
</a>

<p>Named <code>group.last</code> or just <code>last</code>.  Same as <code>first</code>, but refers to the last tuple aggregated in the current bucket.</p>

<p>Same type as the input tuple.</p>

<p>Differs from <code>previous</code> by its type (<code>previous</code> is the current result of the aggregation while <code>last</code> is the last aggregated _input_ tuple) and by the fact that it can also be used in the <code>select</code> clause and the <code>where</code> clause.</p>

<a name="group-previous-tuple">
<h4>Previous tuple out of group</h4>
</a>

<p>Named <code>group.previous</code>, refers to the previous version of the aggregation result for that group.  Notice that this is not the lastly output tuple (that would be <code>out.previous</code>) but rather the previous value for <code>out</code>, which have actually been output only if the commit expression returned true (and the aggregate haven't been flushed). There is one distinct <code>group.previous</code> per group, while there is only one <code>out.previous</code>.</p>

<p>Can be used in the <code>select</code> and <code>commit</code> clause.</p>

<p>When the aggregate is fresh new then that tuple has all its field set to Null.  Therefore, if you use this tuple you must check for nulls accordingly.</p>

<p>Same type as the <code>out</code> tuple, with all fields nullable.</p>

<p>Usage example:</p>

<pre>
  SELECT key, signal GROUP BY key
  COMMIT AND KEEP ALL WHEN signal != COALESCE(group.previous.signal, 0)
</pre>

<p>To transform a succession of <code>key, signal</code> with possibly many times the same signal value into a stream of <code>key, signal</code> omitting the repetitions.</p>

<a name="param-tuple">
<h4>Parameters</h4>
</a>

<p>In addition to the tuples read from parent operations, an operation also receive some constant parameters that can be used to customize the behavior of a compiled <a href="/glossary.html#Program">program</a>. See the <a href="#programs">section about defining programs</a> below.</p>

<p>Such parameters can be accessed unambiguously by prefixing them with the tuple name <code>param</code>.</p>

<p>There is no restriction as to where this "tuple" can be used.</p>

<a name="virtual-fields">
<h4>Virtual fields</h4>
</a>

<p>In addition to a tuple fields, some tuples have <em>virtual</em> fields, that are fields which values are computed internally.  To distinguish them from normal fields their name starts with a dash (<code>#</code>).  Here is a list of all available virtual fields and which tuple they apply to:</p>

<table>
<caption>Virtual Fields</caption>
<tr>
  <th>Field name</th><th>Content</th>
</tr><tr>
  <td>in.#count</td>
  <td>How many tuples have been received (probably useless in itself but handy for comparison or with modulus).</td>
</tr><tr>
  <td>selected.#count</td>
  <td>How many tuples have passed the WHERE filter.</td>
</tr><tr>
  <td>selected.#successive</td>
  <td>How many tuples have passed the WHERE filter without any incoming tuple failing to pass.</td>
</tr><tr>
  <td>unselected.#count</td>
  <td>How many tuples have failed the WHERE filter.</td>
</tr><tr>
  <td>unselected.#successive</td>
  <td>How many tuples have failed the WHERE filter without any incoming tuple passing it.</td>
</tr><tr>
  <td>group.#count</td>
  <td>How many tuples were added so far to form the output tuple. Can be used both in the <code>where</code> clause and in the <code>select</code> clause.</td>
</tr><tr>
  <td>group.#successive</td>
  <td>How many successive incoming tuples were assigned to that group (same <code>group by</code> key).</td>
</tr><tr>
  <td>out.#count</td>
  <td>In the <code>select</code> clause, how many tuples have been output so far. For <code>SELECT</code> operations, use <code>selected.#count</code> instead.</td>
</tr>
</table>

<p>NOTE: <code>group.#successive</code> is unchanged by an aggregate flush operation and therefore make little sense in a <code>remove/keep</code> clause.</p>

<h3>Operators and Functions</h3>

<p>Predefined functions can be applied to expressions to form more complex expressions.</p>

<p>You can use parentheses to group expressions.  A <a href="#table-of-precedence">table of precedence</a> is given at the end of this section.</p>

<p>Here we list all available functions. There is no way to define your own functions short of adding them directly into Ramen source code. Therefore, there is no real difference between <em>operators</em> and <em>functions</em>.</p>

<p>It is more useful to distinguish between stateless and stateful functions, though. Function state (for those that have one) can be chosen to have either a global lifespan or a per-group lifespan. The default lifespan for aggregate functions is the group and the default lifespan for other stateful functions is global.  Add "globally" after the function name to force it to use the global lifespan and "locally" to force the per-group lifespan. For instance, the <code>sum</code> function, being an aggregate function, use a group-wise state by default, meaning <code>sum x</code> is equivalent to <code>sum locally x</code>. To make it use a global state (and build the sum over all incoming tuples regardless of how they are grouped), write: <code>sum globally x</code>.</p>

<h4>Boolean operators</h4>

<p><code>and</code>, <code>or</code>: infix, <code>bool ⨉ bool → bool</code>.</p>

<p><code>not</code>: prefix, <code>bool → bool</code>.</p>

<h4>Arithmetic operators</h4>

<p><code>+</code>, <code>-</code>, <code>*</code>, <code>//</code>, <code>^</code>: infix, <code>num ⨉ num → num</code>, where <code>num</code> can be any numeric type (integer or float).</p>

<p>The size of the result is the largest of the size of the operands.  Both operands will also be converted to the largest of their type before proceeding to the operation. For instance, in <code>1 + 999</code>, <code>1</code> will be converted to <code>i16</code> (the type of <code>999</code>) and then a 16 bits addition will yield a 16 bits result (regardless of any overflow). If you expect an overflow then you need to explicitly cast to a larger type.</p>

<p>Notice that <code>//</code> is the integer division.</p>

<p><code>/</code>: infix, floating point division, <code>float ⨉ float → float</code>.</p>

<p><code>%</code>: infix, the integer remainder, <code>int ⨉ int → int</code>.</p>

<p><code>-</code>: prefix, the negation, <code>num → num</code>.</p>

<p><code>abs</code>, prefix, <code>num → num</code>, absolute value.</p>

<p><code>exp</code>, prefix, <code>num → float</code>, exponential.</p>

<p><code>log</code>, prefix, <code>num → float</code>, logarithm.</p>

<p><code>log10</code>, prefix, <code>num → float</code>, base 10 logarithm.</p>

<p><code>sqrt</code>, prefix, <code>num → float</code>, square root.</p>

<p><code>ceil</code>, prefix, <code>num → float</code>, ceil function.</p>

<p><code>floor</code>, prefix, <code>num → float</code>, floor function.</p>

<p><code>round</code>, prefix, <code>num → float</code>, round to the nearest integer.</p>

<p><code>trunc</code>, prefix, <code>num ⨉ num → float</code>, truncate a number to a multiple of the given interval.</p>

<p><code>reldif</code>, prefix, <code>num ⨉ num → float</code>, relative difference between two numbers. <code>reldir(a, b)</code> is equivalent to <code>min(abs(a-b), max(a, b)) / max(abs(a-b), max(a, b))</code>.</p>

<h4>Integer bitwise operators</h4>

<p><code>&amp;</code>: infix, <code>int ⨉ int → int</code>, the bitwise AND operator.</p>

<p><code>|</code>: infix, <code>int ⨉ int → int</code>, the bitwise OR operator.</p>

<p><code>#</code>: infix, <code>int ⨉ int → int</code>, the bitwise XOR operator.</p>

<p><code>&lt;&lt;</code>: infix, <code>int ⨉ int → int</code>, left shift operator.</p>

<p><code>&gt;&gt;</code>: infix, <code>int ⨉ int → int</code>, right shift operator. Sign extend for signed integers.</p>

<h4>Comparison operators</h4>

<p><code>&gt;</code>, <code>&gt;=</code>, <code>&lt;=</code>, <code>&lt;</code>: infix, <code>num ⨉ num → bool</code>.</p>

<p><code>=</code>, <code>!=</code>, <code>&lt;&gt;</code>: infix, <code>any ⨉ any → bool</code>, where <code>any</code> refers to any type.</p>

<p>Notice that <code>&lt;&gt;</code> and <code>!=</code> are synonymous.</p>

<p>As for arithmetic operators, operand types will be enlarged to the largest common type and the operation will return that same type.</p>

<h4>Time related functions</h4>

<p><code>age of ...</code> or <code>age(...)</code>. Expects its argument to be a timestamp in the <span style="font-variant: small-caps">UNIX</span> epoch and will return the difference between that timestamp and now.</p>

<p><code>now</code> returns the current timestamp as a float.</p>

<p><code>#start</code> and <code>#stop</code> are special argument less functions returning the event time (start and stop) of the current input tuple.</p>

<p><code>parse_time</code>, <code>string → float?</code>, prefix. Returns the timestamp corresponding to the given string. The string in question must follow one of the traditional time format (most of the format described in <em>man at (1)</em> are supported, and then some).</p>

<p><code>format_time</code>, <code>string ⨉ float → string</code>, prefix. Returns a string representation of the given timestamp according to the time format specification, à la <em>strftime</em>, where any occurrence of the special sequence on the left will be replaced by the time component on the right:</p>

<table>
  <caption><code>format_time</code> replacements</caption>
  <tr><th>sequence</th><th>replaced by</th></tr>
  <tr><td><code>%Y</code></td><td>year</td></tr>
  <tr><td><code>%j</code></td><td>day of the year</td></tr>
  <tr><td><code>%m</code></td><td>month</td></tr>
  <tr><td><code>%d</code></td><td>day of the month</td></tr>
  <tr><td><code>%H</code></td><td>hour</td></tr>
  <tr><td><code>%M</code></td><td>minute</td></tr>
  <tr><td><code>%S</code></td><td>seconds</td></tr>
  <tr><td><code>%s</code></td><td><span style="font-variant: small-caps">UNIX</span> timestamp</td></tr>
  <tr><td><code>%u</code></td><td>day of the week</td></tr>
  <tr><td><code>%n</code></td><td>new line character</td></tr>
  <tr><td><code>%t</code></td><td>tab character</td></tr>
</table>

<h4>Casts</h4>

<p>Any type name used as a function would convert its argument into that type.  For instance: <code>int16(42)</code> or <code>int16 of 42</code>.</p>

<h4>NULL related function</h4>

<p><code>is [not] null</code>: postfix, <code>any nullable → bool</code>.</p>

<p>Turns a nullable value into a boolean. Invalid on non-nullable values.</p>

<p>For instance: <code>null is null</code> is trivially true, while `some_field is not null` can be either true or not depending on the tuple at hand.</p>

<p><code>42 is null</code> is an error, though, as 42 is not nullable.</p>

<p><code>coalesce</code>: prefix, <code>any nullable ⨉ ... ⨉ any non nullable → any non nullable</code>.</p>

<p>Get rid of nullability by providing a fallback non-nullable value. The result will be the value of the first non-null argument, and is guaranteed to be non-nullable.</p>

<h4>String functions</h4>

<p><code>length</code>, prefix, <code>string → uint16</code>: length _in bytes_ of a string. (TODO: length in UTF-8 characters)</p>

<p><code>+</code>, infix, <code>string ⨉ string → string</code>, concatenation.</p>

<p><code>lower</code>, prefix, <code>string → string</code>, convert to lowercase.</p>

<p><code>upper</code>, prefix, <code>string → string</code>, convert to uppercase.</p>

<p>Notice that <code>lower</code> and <code>upper</code> will alter only characters that are part of the US-ASCII character set.</p>

<p><code>like</code>, prefix, <code>string ⨉ pattern → bool</code> where any <code>%</code> in pattern will match any substring and any <code>_</code> character will match any single character.</p>

<p><code>split</code>, prefix, <code>string ⨉ string → multiple strings</code> where the first string is the delimiter where to cut the second string. This function output each fragment successively.</p>

<p><code>starts</code>, prefix, <code>string ⨉ string → bool</code>, tells if a string starts with a given prefix.</p>

<p><code>ends</code>, prefix, <code>string ⨉ string → bool</code>, tells if a string ends with a given prefix.</p>

<h4>Network functions</h4>

<p><code>in</code>, infix, <code>address ⨉ cidr → bool</code>, true iif the given address belongs to the CIDR range. Notice that the address can be either IPv4 or IPv6 but the CIDR must correspond to it.</p>

<h4>Lists/Vectors related functions</h4>

<p><code>in</code>, infix, <code>any ⨉ α list → bool</code>, membership test (also for vectors). O(1) on constants.</code>yy

<p><code>get</code>, prefix, <code>int ⨉ α list → α</code> or <code>int ⨉ α vector → α</code> or <code>0 ⨉ (α ⋆ β ⋆ ...) → α</code>, <code>1 ⨉ (α ⋆ β ⋆ ...) → β</code>, and so on. Extract a value from a list, a vector or a tuple. Index is 0-based. For tuples, the index must be constant.</code>

<p><code>1st</code>|<code>2nd</code>|<code>3rd</code>|<code>nth</code> and so on, <code>int ⨉ α list → α</code>, infix equivalent of the above, with a 1-based index.</code>

<p><code>Nth-percentile</code>, infix, <code>num ⨉ num list → num</code>, returns the percentile of the elements of the list. Example: <code>95th percentile of (sample (1000, response_time + data_transfert_time))</code>.</p>

<h4>Miscellaneous stateless functions</h4>

<p><code>hash</code>, prefix, <code>any → int64</code>, turn anything into a 64 bits integer.</p>

<p><code>random</code> returns a random float between 0 and 1.</p>

<p><code>sparkline</code>, prefix, <code>α vector → string</code>, turns an histogram into an UTF-8 sparkline.</p>

<p><a name="#variant-function"><code>variant</code></a>, prefix, <code>string → string?</code>, returns the name of the current variant for the given <a href="#experiments">experiment</a>.</p>

<p><code>print</code>, prefix, <code>α ⨉ ... → α</code>, accept any number of parameter and return its first. In addition, as a side effect, print all its parameter on standard output for debugging.</p>

<h3>Aggregate functions</h3>

<p>Aggregate functions are stateful functions that combines the current value with previous values.  For instance, <code>max response_time</code> will compute the max of all the <code>response_time</code> fields of all incoming tuples (until the <code>commit</code> clause instruct Ramen to output this aggregated tuple).</p>

<h4>Min, Max, Sum, Avg</h4>

<p>Compute the <code>max</code>, <code>min</code>, <code>sum</code> and <code>avg</code> of the (numeric) input values.</p>

<p>For <code>sum</code>, beware that you may want a larger integer type than the one from the operand!</p>

<h4>And, Or</h4>

<p>Compute the logical <code>and</code> and <code>or</code> of the (boolean) input values.</p>

<h4>First, Last</h4>

<p>Remember only the <code>first</code> or the <code>last</code> value encountered in this aggregation.</p>

<h4>Histograms</h4>

<p>It is also possible to approximate a distribution of values by building an histogram with the <code>histogram</code> function:</p>

<p><code>histogram</code>, prefix, <code>num ⨉ num ⨉ num ⨉ int → int[#bucket+2]</code>, where the first expression is the values to count, and then comes the min, max and number of buckets. The first bucket will count all values smaller than the minimum and the last one will count all values above the maximum.</p>

<p>Note: it is currently not possible to specify non linear scales.</p>

<h4>Grouping/Sampling</h4>

<p>It is possible to build the list of all values in a group using the <code>all</code> function (or the synonymous <code>group</code>). It is thus possible to use this list as input for the <code>percentile</code> or <code>sparkline</code> functions.</p>

<p><code>all</code>, prefix, <code>α → α list</code>, gather all values in a list.</p>

<p>Often, it is desirable to ensure the result does not grow above some maximum:</p>

<p><code>sample</code>, prefix, <code>int ⨉ α → α list</code>, reservoir sampling of the expression to return a list of at most that many elements. Example: <code>95th percentile of (sample (1000, response_time + data_transfert_time))</code>.

<p>Finally, sometime only the last or most important values have to be cherry-picked:</p>

<p><code>last N ... [by ...]</code>, prefix, <code>int ⨉ α ⨉ β → α list</code>. Selects the N most important <code>α</code> values according to <code>β</code>. If <code>[by ...]</code> is ommitted then merely selects the last N values.</p>

<p>Notice that all aggregate functions can also accept a list/vector as input and will output the same result as if the values were encountered one by one (just slower). This is useful for instance to compute the <code>min</code> or <code>avg</code> of a few immediate values.</p>

<h3>Timeseries functions</h3>

<p><code>lag</code>, prefix, <code>int ⨉ any → any</code>, delayed value of some expression. For instance, <code>lag (3, f)</code> returns the value of f 3 steps earlier. Can be used for instance to compute a poor man's derivative <code>f - lag(1, f)</code>.</p>

<p>Following functions share the notion of _seasonality_.</p>

<p>Seasonality is like weak periodicity: a seasonal time series is one that is strongly auto-correlated for some period P without being strictly periodic. When this is the case, one often wants to compute some function over the past k same seasons. For instance, if <code>v</code> has a seasonality of <code>p</code>, one might want to know the average of the last 10 seasons: `(v(t-p) + v(t-2p) + v(t-3p) + ... + v(t-10p)) / 10`.</p>

<p>The following functions are such functions, parameterized by <code>p</code> (the seasonality) and <code>k</code> (how many seasons in the past to consider). Notice that in the example above as well as in the functions below the current value is skipped: <code>v(t)</code> is not in the average. This is because we often want to compare the past seasons with the current value.</p>

<p>Seasonality is similar to fixed length windows but implemented at the function level rather than at the aggregation level.</p>

<p><code>season_moveavg</code>, prefix, <code>int ⨉ int ⨉ num → float</code>, seasonal moving average.</p>

<p>For a time series of seasonality <code>p</code> (first parameter), returns the average of the last <code>k</code> values (second parameter), skipping the current one. The third parameter is numerical expression. The result will be a float. This is the same computation than the exemple given above.</p>

<p><code>moveavg</code>: same as <code>season_moveavg</code> with <code>p=1</code>.</p>

<p><code>k-moveavg</code> or <code>k-ma</code>: alternative infix syntax for <code>moveavg</code>.</p>

<p><code>season_fit</code>, prefix, <code>int ⨉ int ⨉ num → float</code>, linear regression (fitting).</p>

<p><code>fit</code>: same as <code>season_fit</code> with <code>p=1</code>.</p>

<p><code>season_fit_multi</code>, prefix, <code>int ⨉ int ⨉ num ⨉ ... → float</code>, multiple linear regression. This is a variadic function. The first <code>num</code> (mandatory) is the parameter to be fitted, and all other following optional numbers are regression parameters helping with the fitting.</p>

<p><code>fit_multi</code>: same as <code>season_fit_multi</code> for <code>p=1</code>.</p>

<p><code>smooth</code>, prefix, <code>float ⨉ num → float</code>, exponential smoothing of the value (second parameter). The first parameter is a constant float providing the exponent (between 0 and 1, the smaller the softer the smooth).</p>

<p><code>smooth</code>, prefix, <code>num → float</code>, same as above with a default smoothing factor of 0.5.</p>

<h3>Miscellaneous Stateful Functions</h3>

<h4>Remembering past events</h4>

<p><code>remember</code>, prefix, <code>float ⨉ float ⨉ float ⨉ any ⨉ ... → bool</code>, tells if a value (or combination of) have been seen before.</p>

<p>This uses rotating <a href="https://en.wikipedia.org/wiki/Bloom_filter">bloom filters</a>.  First parameter is the false positive rate that should be aimed at, second is how to compute the event time, third is the duration, in seconds, that the function should remember values, and finally the last argument is the value to remember. The function will return true if it remember that value (and it will memorize it for next calls).  There can be false positives (<code>remember</code> returning true while in fact that very value has never been seen) but no false negative (<code>remember</code> returning false while this value had in fact been seen earlier).</p>

<p>NOTE: When possible, it might save a lot of space to aim for a high false positive rate and account for it in the surrounding calculations, as opposed to aim for a low false positive rate.</p>

<p><code>distinct</code>, prefix, <code>any ⨉ ... → bool</code>, tells if a value have been seen before.</p>

<p>This is similar to the above <code>remember</code> but actually remember every values rather than relying on a bloom filter. So, to be used only when the number of possible distinct values is small.</p>

<p>The other difference is that it has no use for time: it merely remembers everything for the duration of its state, which is local (ie. bound to the group) by default.</p>

<p>Finally, it can take any number of values of any type instead of just one.</p>

<h4>Top elements</h4>

<p>In a stream it is very common to treat frequently occurring values specially. The <code>top</code> family of aggregate functions allows to detect those frequently occurring values.</p>

<p><code>rank of ... in top ... [over ...] [by ...] [for the last ...]</code>, infix, <code>α ⨉ int [⨉ int] [⨉ β] [⨉ float] → int</code> will return the <em>approximate</em> rank of the given expression <code>α</code>. <code>over ...</code> sets a higher limit to the number of tracked elements (10 times the size of the top by default), while <code>by ...</code> specifies the expression according to which to <em>weight</em> each value. Finally, <code>for the last ...</code> sets the duration after which remembered elements should start to decay in memory.</p>

<p>More often than not what's important is to know which values are in the top but not which place they occupy. This function is then a bit faster:</p>

<p><code>is ... in top ... [over ...] [by ...] [for the last ...]</code>, infix, <code>α ⨉ int [⨉ int] [⨉ β] [⨉ float] → bool</code>, which returns only a boolean if the value is indeed in the top.</p>

<p>Typical use case is to NULL out values that are not in the top, in order to reduce some field cardinality.</p>

<h4>Hysteresis</h4>

<p><code>hysteresis</code>, prefix, <code>float ⨉ float ⨉ float → bool</code>, tells if a measured value is within some permitted range, with an hysteresis once it has ventured outside.</p>

<p>The first parameter is the measured value, the second parameter is the acceptable value and the last one is the maximum value. Starting from a <code>true</code> position, the result of hysteresis will stay true as long as the measured value stays below the defined maximum. Once it has reached or exceeded that maximum then the hysteresis value will be <code>false</code>, and the measured value now has to return below the acceptable value for hysteresis to return <code>true</code> again.</p>

<p>For instance, <code>hysteresis(x, 8, 10)</code> starts at <code>true</code>. Then is <code>x</code> goes above <code>10</code> the returned value will turn to <code>false</code>, and will stay <code>false</code> until <code>x</code> decrease to below <code>8</code>.</p>

<p>If the accepted value is greater than the maximum, it works the other way around: the maximum is interpreted as a minimum and the acceptable value is the smallest value that the measured value must reach in order for the hysteresis to be <code>true</code> again once it had ventured below the minimum.</p>

<h3>Conditionals</h3>

<p>Conditional expressions can appear anywhere an expression can.  Conditions are evaluated from left to right. Contrary to programming languages but like in SQL language, evaluation of alternatives do not stop as soon as the consequent is determined; in particular <em>the state of stateful functions will be updated even in non-taken branches</em>.</p>

<h4>CASE Expressions</h4>

<p>The only real conditional is the case expression. Other forms of conditionals are just syntactic sugar for it. Its general syntax is:</p>

<pre>
  CASE
    WHEN cond1 THEN cons1
    WHEN cond2 THEN cons2
    ...
    ELSE alt
  END
</pre>

<p>...where you can have as many <code>when</code> clauses as you want, including 0, and the <code>else</code> clause is also optional.</p>

<p>All conditions must be of type bool. Consequents can have any type as long as they have all the same. That is also the type of the result of the CASE expression.</p>

<p>Regarding nullability: if there are no else branch, or if any of the condition or consequent is nullable, then the result is nullable. Otherwise it is not.</p>

<h4>Variants</h4>

<p><code>IF cond THEN cons</code> or <code>IF(cond, cons)</code>: simple variant that produce either <code>cons</code> (if <code>cond</code> is true) or <code>NULL</code>.</p>

<p>`IF cond THEN cons ELSE alt<code> or </code>IF(cond, cons, alt)`: same as above but with an ELSE branch.</p>

<a name="table-of-precedence">
<h3>Operator precedence</h3>
</a>

<p>From higher precedence to lower precedence:</p>

<table>
<caption>Operator precedence</caption>
<tr>
  <th>Operator</th><th>Associativity</th>
</tr><tr>
  <td>functions</td>
  <td>left to right</td>
</tr><tr>
  <td><code>not</code>, <code>is null</code>, <code>is not null</code></td>
  <td>left to right</td>
</tr><tr>
  <td><code>^</code></td>
  <td>right tot left</td>
</tr><tr>
  <td><code>*</code>, <code>//</code>, <code>/</code>, <code>%</code></td>
  <td>left to right</td>
</tr><tr>
  <td><code>+</code>, <code>-</code></td>
  <td>left to right</td>
</tr><tr>
  <td><code>&gt;</code>, <code>&gt;=</code>, <code>&lt;</code>, <code>&lt;=</code>, <code>=</code>, <code>&lt;&gt;</code>, <code>!=</code></td>
  <td>left to right</td>
</tr><tr>
  <td><code>or</code>, <code>and</code></td>
  <td>left to right</td>
</tr>
</table>

<a name="functions">
<h2>Functions</h2>
</a>

<h3>Read</h3>

<p>The simplest way to get tuples may be to read them from CSV files. The <code>READ</code> operation does just that, reading a set of files and then waiting for more files to appear.</p>

<p>Its syntax is:</p>

<pre>
  READ [AND DELETE] FILES "file_pattern"
    [ PREPROCESS WITH "preprocessor" ]
    [ SEPARATOR "separator" ] [ NULL "null" ] (
    first_field_name first_field_type [ [ NOT ] NULL ],
    second_field_name second_field_type [ [ NOT ] NULL ],
    ...
  )
</pre>

<p>If <code>AND DELETE</code> is specified then files will be deleted as soon they have been read.</p>

<p>The <code>file_pattern</code>, which must be quoted, is a file name that can use the star character ("*") as a wildcard matching any possible substring. This wildcard can only appear in the file name section of the path and not in any directory, though.</p>

<p>In case a proprocessor is given then it must accept the file content in its standard input and outputs the actual CSV in its standard output.</p>

<p>The CSV will then be read line by line, and a tuple formed from a line by splitting that line according to the delimiter (the one provided or the default coma (",")). The rules to parse each individual data types in the CSV are the same as to parse them as literal values in the function operation code.  In case a line fails to parse it will be discarded.</p>

<p>The CSV reader cannot parse headers.  CSV field values can be double-quoted to escape the CSV separator from that value.</p>

<p>If a value is equal to the string passed as NULL (the empty string by default) then the value will be assumed to be NULL.</p>

<p>Field names must be valid identifiers (aka string made of letters, underscores and digits but as the first character), field types must be one of <code>bool</code>, <code>string</code>, <code>float</code>, <code>u8</code>, <code>i8</code>, <code>u16</code>, etc...  and <code>null</code> or <code>not null</code> will specify whether that field can be NULL or not (default to <code>null</code>).</p>

<p>Examples:</p>

<pre>
  READ FILE "/tmp/test.csv" SEPARATOR "\t" NULL "<NULL>" (
    first_name string NOT NULL,
    last_name string,
    year_of_birth u16 NOT NULL,
    year_of_death u16)
</pre>

<pre>
  READ FILES "/tmp/test/*.csv.gz" PREPROCESSOR "zcat" (
    first_name string NOT NULL,
    last_name string)
</pre>

<h3>Yield</h3>

<p>If you just want a constant expression to supply data to its child functions you can use the yield expression. This is useful to build a periodic clock, or for tests.</p>

<p>Examples:</p>

<pre>
  YIELD sum globally 1 % 10 AS count
</pre>

<pre>
  YIELD 1 AS tick EVERY 10 MILLISECONDS
</pre>

<p>Yield merely produces an infinite stream of tuples. If no <code>every</code> clause is specified, then it will do so as fast as the downstream functions can consume them. With an <code>every</code> clause, it will output one tuple at that pace (useful for clocks).</p>

<p>Syntax:</p>

<pre>
  YIELD expression1 AS name1, expression2 AS name2, expression3 AS name3... [ EVERY duration ]
</pre>

<h3>Group By</h3>

<p>Group-By is the meat of Ramen operation. It performs filtering, sorting, aggregation, windowing and projection. As each of those processes are optional let's visit each of them separately before looking at the big picture.</p>

<h4>Receiving tuples from parents - the <em>from</em> clause</h4>

<p>Apart for the few functions receiving their input from external sources, most functions receive them from other functions.</p>

<p>The name of those functions are listed after the <code>FROM</code> keyword.</p>

<p>Those names can be either relative to the present program or absolute.</p>

<p>If only a function name is supplied (without a program name) then the function must be defined elsewhere in the same program. Otherwise, the source name must start with a program name. If that program name starts with <code>../</code> then it is taken relative to the current program. Otherwise, it is taken as the full name of the program.</p>

<p>Examples:</p>

<ul>
<li><code>minutely_average</code>: another function in the same program;</li>
<li><code>monitoring/hosts/httpd_stats</code>: function <code>httpd_stats</code> from the <code>monitoring/hosts</code> program;</li>
<li><code>../../csv/tcp</code>: function <code>tcp</code> from the program which name relative to the current one is <code>../../csv</code>.</li>
</ul>

<p>Notice that contrary to unix file system names, Ramen program names do not start with a slash (<code>/</code>). The slash character only special function is to allow relative names.</p>

<h4>Filtering - the <em>where</em> clause</h4>

<p>If all you want is to select tuples matching some conditions, all you need is a filter. For instance, if you have a source of persons and want to filter only men older than 40, you could create an operation consisting of a single <code>where</code> clause, such as:</p>

<pre>
  WHERE is_male AND age &gt; 40 FROM source
</pre>

<p>As is evidenced above, the syntax of the <code>where</code> clause is as simple as:</p>

<pre>
  WHERE condition FROM source
</pre>

<p>Notice that the clauses order within an operation generally doesn't matter so this would be equally valid:</p>

<pre>
  FROM source WHERE condition
</pre>

<p>The condition can be any expression which type is a non-nullable boolean.</p>

<p>NOTE: The default <code>where</code> clause is <code>WHERE true</code>.</p>

<h4>Joining sources - the <em>merge</em> clause</h4>

<p>When selecting from several operation (as in <code>FROM operation1, operation2, ...</code>) the output of all those parent operations will be mixed together.  As parents will typically run simultaneously it is quite unpredictable how their output will mix.  Sometime, we'd like to synchronize those inputs though.</p>

<p>It is easy and cheap to merge sort those outputs according to some fields, and the <code>merge</code> clause does exactly that. For instance:</p>

<pre>
  SELECT * FROM source1, source2, source3 MERGE ON timestamp
</pre>

<p>In case some parents are producing tuples at a much lower pace than the others, they might slow down the pipeline significantly. Indeed, after each tuple is merged in Ramen will have to wait for the next tuple of the slow source in order to select the smallest one.</p>

<p>In that case, you might prefer not to wait longer than a specified timeout, and then exclude the slow parent from the merge sort until it starts emitting tuples again. You can to that with the <code>TIMEOUT</code> clause:</p>

<pre>
  SELECT * FROM parent1, parent2 MERGE ON field1, field2 TIMEOUT AFTER 3 SECONDS
</pre>

<p>Whenever the timed-out parent emits a tuple again it will be injected into the merge sort, with the consequence that in that case the produced tuples might not all be ordered according to the given fields.</p>

<p>The <code>merge</code> clause syntax is:</p>

<pre>
  MERGE ON expression1, expression2, ... [ TIMEOUT AFTER duration ]
</pre>

<h4>Sorting - the <em>sort</em> clause</h4>

<p>Contrary to SQL, in Ramen sorts the query input not its output. This is because in SQL <code>ORDER BY</code> is mostly a way to present the data to the user, while in Ramen <code>SORT</code> is used to enforce some ordering required by the aggregation operations or the windowing.  Also, on a persistent query you do not necessarily know what the output of an operation will be used for, but you know if and how the operation itself needs its input to be sorted.</p>

<p>Of course, since the operations never end the sort cannot wait for all the inputs before sorting. The best we can do is to wait for some entries to arrive, and then take the smaller of those, then wait for the next one to arrive, and so on, thus sorting a sliding window.</p>

<p>The maximum length of this sliding window must be specified with a constant integer: <code>SORT LAST 42</code> for instance. It is also possible to specify a condition on that window (as an expression) that, if true, will process the next smallest tuple available, so that this sliding window is not necessarily of fixed length. For instance: <code>SORT LAST 42 OR UNTIL AGE(creation_time) &gt; 60</code> would buffer at most 42 tuples, but would also process one after reception of a tuple which <code>creation_time</code> is older than 60 seconds.</p>

<p>Finally, it must also be specified according to what expression (or list of expressions) the tuples must be ordered: <code>SORT LAST 42 BY creation_time</code>.</p>

<p>The complete <code>sort</code> clause is therefore:</p>

<pre>
  SORT LAST n [ OR UNTIL expression1 ] BY expression2, expression3, ...
</pre>

<h4>Projection - the <em>select</em> clause</h4>

<p>To follow up on previous example, maybe you are just interested in the persons name and age. So now you could create this operation to select only those:</p>

<pre>
  SELECT name, age FROM source
</pre>

<p>Instead of mere field names you can write more interesting expressions:</p>

<pre>
  SELECT (IF is_male THEN "Mr. " ELSE "Ms. ") + name AS name,
         age date_of_birth as age_in_seconds
  FROM source
</pre>

<p>The general syntax of the <code>select</code> clause is:</p>

<pre>
  SELECT expression1 AS name1, expression2 AS name2, ...
</pre>

<p>You can also replace _one_ expression anywhere in this list by a star (<code>*</code>).  All fields from the input which are not already present in the list will be copied over to the output. What is meant here by "being present" is: having the same field name and a compatible type. Since field names must be unique, this is an error if an expression of an incompatible type is aliased to the same name of an input type together with the star field selector.</p>

<p>NOTE: The default <code>select</code> clause is: <code>SELECT *</code>.</p>

<h4>Aggregation</h4>

<p>Some functions that might be used in the <code>SELECT</code> build their result from several input values, and output a result only when some condition is met. Aggregation functions are a special case of stateful functions.  Stateful functions are functions that needs to maintain an internal state in order to be able to output a result. A simple example is the <code>lag</code> function, which merely output the past value for every new value.</p>

<p>The internal state of those functions can be either global to the whole operation, or specific to a group, which is the default. A group is a set of input tuple sharing something in common. For instance, all persons with the same age and sex. Let's take an example, and compute the average salary per sex and age. <code>avg</code> is the archetypal aggregation function.</p>

<pre>
  SELECT avg salary FROM employee GROUP BY age, is_male
</pre>

<p>What happens here for each incoming tuple:</p>

<ol>
<li>Extract the fields age and is_male and makes it the <code>key</code> of this tuple;</li>
<li>Look for the group for this key.
    <ul>
    <li>If not found, create a new group made only of this tuple. Initialize its average salary with this employee's salary;</li>
    <li>If found, add this salary to the average computation.</li>
    </ul></li>
</ol>

<p>The <code>group-by</code> clause in itself is very simple: it consists merely on a list of expressions building a key from any input tuple:</p>

<pre>
  GROUP BY expression1, expression2, ...
</pre>

<p>You can mix stateful functions drawing their state from the group the tuple under consideration belongs to, with stateful functions having a global state.  Where a stateful function draws its state from depends on the presence or absence of the <code>globally</code> modifier to the function name. For instance, let's also compute the global average salary:</p>

<pre>
  SELECT avg salary, avg globally salary AS global_avg_salary
  FROM employee GROUP BY age, is_male
</pre>

<p>Each time the operation will output a result, it will have the average (so far) for the group that is output (automatically named <code>avg_salary</code> since no better name was provided) and the average (so far) globally (named explicitly <code>global_avg_salary</code>).</p>

<p>Contrary to SQL, it is not an error to select a value from the input tuple with no aggregation function specified. The output tuple will then just use the current input tuple to get the value (similarly to what the <code>last</code> aggregation function would do).</p>

<p>This is also what happens if you use the <code>*</code> (star) designation in the <code>select</code> clause. So for instance:</p>

<pre>
  SELECT avg salary, *
  FROM employee GROUP BY age, is_male
</pre>

<p>...would output tuples made of the average value of the input field <code>salary</code> and all the fields of input tuples, using the last encountered values.</p>

<p>NOTE: The default <code>group-by</code> clause is: nothing! All tuples will be assigned to the same and only group, then.</p>

<p>Hopefully all is clear so far. Now the question that's still to be addressed is: When does the operation output a result? That is controlled by the <code>commit</code> clause.</p>

<h4>Windowing: the <em>commit</em> clause</h4>

<p>Windowing is a major difference with SQL, which stops aggregating values when it has processed all the input. Since stream processors model an unbounded stream of inputs one has to give this extra piece of information.</p>

<p>Conceptually, each time a tuple is received Ramen will consider each group one by one and evaluate the <code>COMMIT</code> condition to see if an output should be emitted.</p>

<p>Obviously, Ramen tries very hard *not* to actually do this as it would be unbearably slow when the number of groups is large. Instead, it will consider only the groups for which the condition might have changed ; usually, that means only the group which current tuple belongs to.</p>

<p>So, the syntax of the <code>commit</code> clause is simple:</p>

<pre>
  COMMIT AFTER condition
</pre>

<p>...where, once again, condition can be any expression which type is a non-nullable boolean.</p>

<p>NOTE: The default <code>commit</code> clause is: <code>true</code>, to commit every selected tuples.</p>

<p>The next and final step to consider is: when a tuple is output, what to do with the group? The simplest and more sensible thing to do is to delete it so that a fresh new one will be created if we ever met the same key.</p>

<p>Indeed, the above syntax is actually a shorthand for:</p>

<pre>
  COMMIT AND FLUSH AFTER condition
</pre>

<p>This additional <code>AND FLUSH</code> means exactly that: when the condition is true, commit the tuple _and_ delete (flush) the group.</p>

<p>If this is the default, what's the alternative? It is to keep the group as-is and resume aggregation without changing the group in any way, with <code>KEEP</code>.</p>

<p>A last convenient feature is that instead of committing the tuple after a condition becomes true, it is possible to commit it <em>before</em> the condition turns true. In practice, that means to commit the tuple that would have been committed the previous time that group received an input (and maybe also flush the group) before adding the new value that made the condition true.</p>

<p>So the syntax for the <code>commit</code> clause that has been given in the previous section should really have been:</p>

<pre>
  COMMIT [ AND ( FLUSH | KEEP ) ] ] ( AFTER | BEFORE ) condition
</pre>

<p>So, as an example, suppose we want a preview of the average salaries emitted every time we added 10 persons in any aggregation group:</p>

<pre>
  SELECT avg salary, avg globally salary AS global_avg_salary
  FROM employee GROUP BY age, is_male
  COMMIT AND KEEP ALL AFTER (SUM 1) % 10 = 0
</pre>


<h4>Outputting: How Tuples Are Sent To Child Functions</h4>

<p>When Ramen commits a tuple, what tuple exactly is it?</p>

<p>The output tuple is the one that is created by the <code>select</code> clause, with no more and no less fields. The types of those fields is obviously heavily influenced by the type of the input tuple. This type itself comes mostly from the output type of the parent operations. Therefore changing an ancestor operation might change the output type of an unmodified operation.</p>

<p>The output tuple is then sent to each of the children operations, before a new input tuple is read. No batching takes place in the operations, although batching does take place in the communication in between them (the ring-buffers).  Indeed, when an operation has no tuple to read it _sleeps_ for a dynamic duration that is supposed to leave enough time for N tuples to arrive, so that next time the operation is run by the operating system there are, in average, N tuples waiting.  This behavior is designed to be efficient (minimizing syscalls when busy and avoiding trashing the cache), but offers no guaranteed batching. If a computation requires batches then those batches have to be computed using windowing, as described above.</p>

<h4>Outputting: Notifying External Tools</h4>

<p>Ramen is designed to do alerting, that is to receive a lot of information, to analyze and triage it, and eventually to send some output result to some external program. By design, there is a huge asymmetry between input and output: Ramen receives large amount of data and produces very little of it.  This explains why the mechanisms for receiving tuples are designed to be efficient while mechanisms for sending tuples outside are rather designed to be convenient.</p>

<p>In addition (or instead) of committing a tuple, Ramen can output a notification, which is a very special type of tuple. While tuples are destined to be read by children workers, notifications are destined to be read by the alerter daemon and processed according to the alerter configuration, maybe resulting in an alert or some other kind of external trigger.</p>

<p>A notification tuple has a name (that will be used by the alerter to route the notification) and some parameters supposed to give some more information about the alert.</p>

<p>So for example, given a stream of people with both a name and a location, we could send a notification each time a person named "Waldo" is spotted:</p>

<pre>
  FROM employee
  SELECT age, gender, salary
  -- The notification with its name:
  NOTIFY "Waldo Spotted" WHEN name = "Waldo"
</pre>

<p>NOTE: Notice here the <code>NOTIFY</code> clause replaces the <code>COMMIT</code> clause altogether.</p>

<p>This works because the default <code>select</code> clause is <code>SELECT *</code> and <code>WHEN</code> is an alias for <code>WHERE</code>.</p>

<h4>Timeseries and Event Times</h4>

<p>In order to retrieve <a href="/glossary.html#Timeseries">time series</a> from output tuples it is necessary to provide information about what time should be associated with each tuple.</p>

<p>Similarly, some functions need to know which time is associated with each value 9such as <code>TOP</code> or <code>REMEMBER</code>).</p>

<p>Although it is convenient at time to be able to mix events which time is specified in various ways, it would nonetheless be tedious to compute the timestamp of every event every time this is required.</p>

<p>This is why how to compute the start and stop time of events is part of every function definitions, so that Ramen can compute it whenever it is needed.</p>

<p>This is the general format of this <em>event-time</em> clause:</p>

<pre>
  EVENT STARTING AT identifier [ * scale ]
      [ ( WITH DURATION ( identifier [ * scale ] | constant ) |
          AND STOPPING AT identifier [ * scale ] ) ]
</pre>

<p>Contrary to most stream processing tools, events have not only a time but a duration, that can be specified either as an actual length or as an ending time. This is because Ramen has been originally designed to accommodate call records.</p>

<p>In the above, <code>identifier</code> represent the name of an output field where the event time (or duration) is to be found. <code>scale</code> must be a number and the field it applies to will be multiplied by this number to obtain seconds (either to build a time as a <span style="font-variant: small-caps">UNIX</span> timestamp or to obtain a duration).  <code>constant</code> is a constant number of seconds representing the duration of the event, if it's known and constant.</p>

<p>Notice that Ramen tries hard to inherit event time clauses from parents to children so they do not have to be specified over and over again.</p>

<p>As a further simplification, if no event-time clause is present but the function outputs a field named <code>start</code> then it will be assumed it contains the timestamp of the event start time; and similarly for a field names <code>stop</code>.</p>

<p>With all these information, the <a href="man/timeseries.html">timeseries</a> command will be able to produce accurate results.</p>

<p>For instance if we had minutely metric collection from sensors with a field "time" in milliseconds we could write:</p>

<pre>
  SELECT whatever FROM sensors WHERE some_condition
  EVENT STARTING AT time * 0.001 WITH DURATION 30
</pre>

<h4>The Complete Picture</h4>

<p>We are now able to give the full syntax and semantic of the <code>Group By</code> operation:</p>

<pre>
  [ SELECT expression1 AS name1, expression2 AS name2, ... ]
  [ ( WHERE | WHEN ) condition ]
  FROM source1, source2, ...
  [ GROUP BY expression1, expression2, ... ]
  [ [ COMMIT ],
    [ NOTIFY name ],
    [ ( FLUSH | KEEP ) ]
    ( AFTER | BEFORE ) condition ]
  [ EVENT STARTING AT identifier [ * scale ]
     [ ( WITH DURATION ( identifier [ * scale ] | constant ) |
         AND STOPPING AT identifier [ * scale ] ) ] ]
</pre>

<p>Each of those clauses can be specified in any order and can be omitted but for the <code>from</code> clause.</p>

<p>The semantic is:</p>

<p>For each input tuple,</p>
<ol>
<li>compute the key;</li>
<li>retrieve or create the group in charge of that key;</li>
<li>evaluate the <code>where</code> clause:
  <ul>
  <li>if it is false:
    <ol>
    <li>skip that input;</li>
    <li>discard the new aggregate that might have been created.</li>
    </ol></li>
  <li>if it is true:
    <ol>
    <li>accumulates that input into that aggregate (actual meaning depending on what functions are used in the operation);</li>
    <li>compute the current output-tuple;</li>
    <li>evaluates the <code>commit</code> clause:
      <ul>
      <li>if it is true:
        <ol>
        <li>send the output tuple to all children;</li>
        <li>also maybe store it on disc;</li>
        <li>unless <code>KEEP</code>, delete this group.</li>
        </ol></li>
      </ul></li>
    <li>consider the commit condition of other groups if they depend in the last input tuple.</li>
    </ol></li>
  </ul></li>
</ol>

<a name="programs">
<h2>Programs</h2>
</a>

<p>A program is a set of functions. The order of definitions does not matter.  The semi-colon is used as a separator (although omitting the final semi-colon is allowed).<p>

<p>Here is a simple example:</p>

<pre>
  DEFINE foo AS
    SELECT * FROM other_program/operation WHERE bar &gt; 0;

  DEFINE bar AS YIELD 1 EVERY 1 SECOND;

  DEFINE foobar AS
    SELECT glop, pas_glop FROM bazibar
    WHERE glop &gt;= 42;
</pre>

<h3>Parameters</h3>

<a name="experiments">
<h2>Experiments</h2>
</a>

<p>Internally, Ramen makes use of a system to protect some feature behind control flags. This system is usable from the Ramen language itself, which makes is easier for users to run their own experiments.</p>

<p>First, the <a href="#variant-function"><code>variant</code></a> function makes it possible to know the name of the variant the server has been assigned to. This makes it easy to adapt a function code according to some variant.</p>

<p>Then, the program-wide clause <code>run if ...</code> followed by a boolean expression instruct ramen that this program should only really be run if that condition is true.</p>

<p>Using the <code>variant</code> function in this <code>run if</code> clause therefore permits to run a program only in some variant.</p>

<p>Custom experiments can be defined in the <code>$RAMEN_DIR/experiments/v1/config</code>. An example configuration can be seen <a href="https://github.com/PerformanceVision/ramen-configurator/blob/master/experiments.config">here</a>.</p>

<? include 'footer.php' ?>
