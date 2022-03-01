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

<p>Like in SQL, some values might be NULL. Unlike SQL though, not <em>all</em> values can be NULL: indeed value types as well as nullability are inferred at compile time. This benefits performance, as many NULL checks can be done away with, and also reliability, as there is some guarantee that a NULL value will not pop up where it's least expected, such as in a WHERE clause for instance.</p>

<p>Users can check if a nullable value is indeed NULL using the <code>IS NULL</code> or <code>IS NOT NULL</code> operators, which turn a nullable value into a (non-nullable) boolean.</p>

<p>To write a literal <code>NULL</code> value enter <code>NULL</code>.</p>

<p>For any type <code>t</code>, the type <code>t?</code> denotes the type of possibly <code>NULL</code> values of type <code>t</code>.</p>

<h3>Booleans</h3>

<p>The type for booleans is called <code>boolean</code> (<code>bool</code> is also accepted). The only two boolean values are spelled <code>true</code> and <code>false</code>.</p>

<p>It is possible to (explicitly) convert integers and even floating point numbers from or to booleans. In that case, <code>0</code> is equivalent to <code>false</code> and other values to <code>true</code>.

<h3>Strings</h3>

<p>The type for character strings is called <code>string</code>.  A literal string is double quoted (with <code>"</code>). To include a double-quote within a string, backslash it.  Other characters have a special meaning when backslashed: <code>"\n"</code> stands for linefeed, <code>"\r"</code> for a carriage return, <code>"\b"</code> for a backspace, <code>"\t"</code> stands for tab and <code>"\\"</code> stands for slash itself. If another character is preceded with a backslash then the pair is replaced with that character without the backslash.</p>

<p>Some functions consider strings as UTF-8 encoded, some consider strings as mere sequence of bytes.</p>

<h3>Numbers</h3>

<h4>Floats</h4>

<p>The type for real numbers is called <code>float</code>. It is the standard IEEE.754 64 bits float.</p>

<p>Literal values will cause minimum surprise: dot notation (<code>"3.14"</code>) and scientific notation (<code>"314e-2"</code>) are supported, as well as hexadecimal notation (<code>0x1.fp3</code>) and the special values <code>nan</code>, <code>inf</code> and <code>-inf</code>.</p>

<h4>Integers</h4>

<p>Ramen allows integer types of 9 different sizes from 8 to 128 bits, signed or unsigned: <code>i8</code>, <code>i16</code>, <code>i24</code>, <code>i32</code>, <code>i40</code>, <code>i48</code>, <code>i56</code>, <code>i64</code> and <code>i128</code>, which are signed integers, and <code>u8</code>, <code>u16</code>, <code>u24</code>, <code>u32</code>, <code>u40</code>, <code>u48</code>, <code>u56</code>, <code>u64</code> and <code>u128</code>, which are unsigned integers.</p>

<p>Ramen uses the conventional 2-complement encoding of integers with silent wrap-around in case of overflow.</p>

<p>When writing a literal integer it is possible to specify the intended type by suffixing it with the type name; for instance: <code>42u128</code> would be an unsigned integer 128 bits wide with value <code>42</code>. If no such suffix is present then Ramen will choose the narrowest possible type that can accommodate that integer value and that's not smaller than i32.  Thus, to get a literal integer smaller than i32 one has to suffix it. This is to avoid having non-intentionally narrow constant values that would wrap around unexpectedly.</p>

<p>In addition to the suffix, you can also use a cast, using the type name as a function: <code>u128(42)</code>. This is equivalent but more general as it can be used on other expressions than literal integers, such as floats, booleans or strings.</p>

<h4>Scales</h4>

<p>Any number can be followed by a <em>scale</em>, which is a short string indicating a multiplier.</p>

<p>The recognized scales are listed in the table below:</p>

<table>
  <caption>Numeric suffixes</caption>
  <thead>
    <tr><th>names</th><th>multiplier</th></tr>
  </thead>
  <tbody>
    <tr><td><code>pico</code>, <code>p</code></td><td>0.000&nbsp;000&nbsp;001</td></tr>
    <tr><td><code>micro</code>, <code>µ</code></td><td>0.000&mnsp;001</td></tr>
    <tr><td><code>milli</code>, <code>m</code></td><td>0.001</td></tr>
    <tr><td><code>kilo</code>, <code>k</code></td><td>1&nbsp;000</td></tr>
    <tr><td><code>mega</code>, <code>M</code></td><td>1&nbsp;000&nbsp;000</td></tr>
    <tr><td><code>giga</code>, <code>G</code></td><td>1&nbsp;000&nbsp;000&nbsp;0000</td></tr>
    <tr><td><code>Ki</code></td><td>1&nbsp;024</td></tr>
    <tr><td><code>Mi</code></td><td>1&nbsp;048&nbsp;576</td></tr>
    <tr><td><code>Gi</code></td><td>1&nbsp;073&nbsp;741&nbsp;824</td></tr>
    <tr><td><code>Ti</code></td><td>1&nbsp;099&nbsp;511&nbsp;627&nbsp;776</td></tr>
  </tbody>
</table>

<h3>Network addresses</h3>

<h4>Ethernet</h4>

<p>Ethernet addresses are accepted with the usual notation, such as: <code>18:d6:c7:28:71:f5</code> (without quotes; those are not strings).</p>

<p>Those values are internally stored as 48bits unsigned integers (big endian) and can therefore be cast from/to other integer types.</p>

<h4>Internet addresses</h4>

<p>IP addresses are also accepted, either v4 or v6, using the conventional notations, again without strings.</p>

<p>CIDR addresses are also accepted; for instance <code>192.168.10.0/24</code>. Notice that there is no ambiguity with integer division since arithmetic operators do not apply to IP addresses.</p>

<p>NOTE: the <code>in</code> operator can check whether an IP belongs to a CIDR.</p>

<h3>Compound types</h3>

<h4>Vectors</h4>

<p>Vectors of values of the same type can be formed with <code>[ expr1 ; expr2 ; expr3 ]</code>.</p>

<p>The type of vectors of <code>N</code> values of type <code>t</code> is noted <code>t[N]</code>.

<p>There is no way to ever build a vector of dimension 0. <code>[]</code> is not a valid value. This simplifies type inference a lot while not impeding the language too much, as the usefulness of a type that can contain only one value is dubious.</p>

<p>One can access the Nth item of a vector with the function <code>GET</code> or by suffixing the vector with the index in between brackets: if <code>v</code> is the vector <code>[1; 2; 3]</code>, then <code>GET(2, v)</code> as well as <code>v[2]</code> is <code>3</code> (indexes start at 0).</p>

<!-- TODO: document out of bound access -->

<h4>Arrays</h4>

<p>An arrays is like a vector which dimension is unknown until runtime.</p>

<p>It is not possible to create an immediate array (that would be a vector) but arrays are frequently returned by functions.</p>

<p>Accessing the Nth item of an array uses the same syntax as for vectors.</p>

<h4>Tuples</h4>

<p>Tuples are an ordered set of values of any type.</p>

<p>They are written within parentheses with values separated with semicolons, such as:<code>(1; "foo"; true)</code>.</p>

<p>One can extract the Nth item of a tuple with the special notation <code>1st</code>, <code>2nd</code>, <code>3rd</code>, <code>4th</code>, and so on. For instance, <code>2nd (1; "foo"; true)</code> is <code>"foo"</code>.

<h4>Records</h4>

<p>Records are like tuples, but with names given to each item.</p>

<p>Immediate values of record type uses curly braces instead of parentheses, and each item is preceded with its name followed by a colon. For instance: <code>{ age: 64; name: "John Doe" }</code>.</p>

<p>To access a given field of a record, suffix the record with a dot (<code>.</code>) and the field name: given the above record value as <code>person</code>, then <code>person.name</code> would be <code>"John Doe"</code>.</p>

<h3>Units</h3>

<p>In addition to a type, values can optionally have units.</p>

<p>When values with units are combined then the combination units will be automatically computed. In addition, Ramen will perform dimensional analysis to detect meaningless computations and will emit a warning (but not an error) in such cases.</p>

<p>The syntax to specify units is quite rough: in between curly braces, separated with a start (<code>*</code>) (or, equivalently, with a dot (<code>.</code>)), units are identified by name. Those names can be anything; They are mere identifier to Ramen. You can also use the power notation (<code>^N</code>) which avoids multiplying several times the same unit, and allows to use negative power as there is no dedicated syntax for dividing units.<p>

<p>Finally, each individual unit can be interpreted as <em>relative</em> if it's identifier ends with <code>(rel)</code>. Relative units are expressed as a difference with some point of reference, whereas absolute units are no such reference. For instance, a duration is expressed in seconds (absolute), whereas a date can be expressed in seconds <em>since some epoch</em> (relative to the epoch). Similarly, heat can be expressed in Kelvins (absolute) whereas a temperature can be expressed in Kelvins above freezing water temperature (relative).</p>

<p>Relative or absolute units follow different rules when combined.</p>

<a name="expressions">
<h2>Expressions</h2>
</a>

<h3>Literal values</h3>

<p>Any literal value (as described in the previous section) is a valid expression.</p>

<a name="record-field-names">
<h3>Record field names</h3>
</a>

<p>In addition to literal values one can refer to a record field. Which records are available depends on the <a href="/glossary.html#Clause">clause</a> but the general syntax is: <code>record.field_name</code>.</p>

<p>The prefix (before the dot) can be omitted in many cases; if so, the field is understood to refer to the "in" record (the input record).</p>

<p>Here is a list of all possible records, in order of appearance in the data flow:</p>

<a name="input-value">
<h4>Input value</h4>
</a>

<p>The value that has been received as input.  Its name is <code>in</code> and that's also the default when a record name is omitted.</p>

<p>You can use the <code>in</code> value in all clauses as long as there is an input.  When used in a <code>commit</code> clause, it refers to the last received value.</p>

<a name="output-value">
<h4>Output value</h4>
</a>

<p>The value that is going to be output (if the <code>COMMIT</code> condition holds <code>true</code>).  Its name is <code>out</code>.  The only places where it can be used is in the commit clause.</p>

<p>It is also possible to refer to fields from the out record in <code>select</code> clauses which creates the out value, but only if the referred fields has been defined earlier. So for instance this is valid:</p>

<pre>
  SELECT
    sum payload AS total,
    end - start AS duration,
    total / duration AS bps
</pre>

<p>...where we both define and reuse the fields <code>total</code> and <code>duration</code>. Notice that here the name of the record has been eluded -- despite "in" being the default value, on some conditions it is OK to leave out the "out" prefix as well.  This would be an equivalent, more explicit statement:</p>

<pre>
  SELECT
    sum in.payload AS total,
    in.end - in.start AS duration,
    out.total / out.duration AS bps
</pre>

<p>It is important to keep in mind that the input and output values have different types (in general).</p>

<a name="local-last-out">
<h4>Previous output value</h4>
</a>

<p>Named <code>previous</code>, refers to the last output value for this group.</p>

<p>Can be used in <code>select</code>, <code>where</code> and <code>commit</code> clauses.</p>

<p>Notice that refering to this value in an expression has some runtime cost in term
of memory, since group state must be kept in memory after the group value has been
committed. So take care not to use this when grouping on potentially large dimension
key spaces.</p>

<p>When no values have been output yet, any field read from <code>previous</code> is just NULL. Therefore, using <code>previous</code> makes it mandatory to always test for NULL.</p>

<p>Technically, the <code>previous</code> value has the same type as the <code>out</code> value with all fields nullable.</p>

<p>The other name of the <code>previous</code> value is <code>local_last</code>, to contrast it with the next special value.</p>

<a name="global-last-out">
<h4>Previous output value (globally)</h4>
</a>

<p>Named <code>global_last</code>, this value is similar to <code>previous</code> aka <code>local_last</code> but it holds the last value that's been output from any aggregation group, ie. regardless of the key.</p>

<p>It therefore has the same type as <code>local_last</code>.

<p>Unlike <code>local_last</code>, <code>global_last</code> incurs no runtime penalty.</p>

<a name="param-record">
<h4>Parameters</h4>
</a>

<p>In addition to the values read from parent operations, an operation also receive some constant parameters that can be used to customize the behavior of a compiled <a href="/glossary.html#Program">program</a>. See the <a href="#programs">section about defining programs</a> below.</p>

<p>Such parameters can be accessed unambiguously by prefixing them with the value name <code>param</code>.</p>

<p>There is no restriction as to where this record can be used.</p>

<a name="environment-record">
<h4>Environment</h4>
</a>

<p>A RaQL program can also access its environment (as in the UNIX environment variables). Environment variables can be read from the <code>env</code> record.</p>

<p>Any field's value from that record is a nullable string, that will indeed be NULL whenever the environment variable is not defined.</p>

<p>There is no restriction as to where this record can be used.</p>

<h3>Conditionals</h3>

<p>Conditional expressions can appear anywhere an expression can.  Conditions are evaluated from left to right. Contrary to programming languages but like in SQL language, evaluation of alternatives do not stop as soon as the consequent is determined; in particular <em>the state of stateful functions will be updated even in non-taken branches</em>.</p>

<h4>CASE Expressions</h4>

<p>The only real conditional is the <em>case expression</em>. Other forms of conditionals are just syntactic sugar for it. Its general syntax is:</p>

<pre>
  CASE
    WHEN cond1 THEN cons1
    WHEN cond2 THEN cons2
    ...
    ELSE alt
  END
</pre>

<p>...where you can have as many <code>when</code> clauses as you want, including 0, and the <code>else</code> clause is optional.</p>

<p>Every conditions must be of type bool. Consequents can have any type as long as they have all the same. That is also the type of the result of the CASE expression.</p>

<p>Regarding nullability: if there are no <em>else branch</em>, or if any of the condition or consequent is nullable, then the result is nullable. Otherwise it is not.</p>

<h4>Variants</h4>

<p><code>IF cond THEN cons</code> or <code>IF(cond, cons)</code>: simple variant that produce either <code>cons</code> (if <code>cond</code> is true) or <code>NULL</code>.</p>

<p>`IF cond THEN cons ELSE alt<code> or </code>IF(cond, cons, alt)`: same as above but with an ELSE branch.</p>

<h3>Operators</h3>

<p>Predefined operators can be applied to expressions to form more complex expressions.</p>

<p>You can use parentheses to group expressions.  A <a href="#table-of-precedence">table of precedence</a> is given at the end of this section.</p>

<p>There is no way to define your own operator, short of adding them directly into Ramen source code.</p>

<h4>Operator States</h4>

<p>Most operators are stateless but some are stateful. Most stateful operators can also behave as stateless operator when passed a sequence of values. Many operators will aggregate values into a set in a certain way, and can thus be used as operands to other aggregate operators.</p>

<p>For instance you can write: <code>SELECT AVG v</code> to get the average value of all input values of <code>v</code>, or <code>SELECT AVG SAMPLE 100 v</code> to get the average value of a random set of 100 values of <code>v</code>. In the later case, <code>AVG</code> does not really perform any aggregation but <code>SAMPLE</code> does.</p>

<p>for every stateful operator that actually perform an aggregation, one can pick
between two possible lifespans for the operators state: local to the current aggregation group, which is the default whenever an explicit group-by clause is present, or global (a single state for every groups). Thus it is possible to compute simultaneously an aggregate over all values with the same key and all values regardless of the key, such as in this example:</p>

<pre>
  SELECT AVG LOCALLY x AS local_avg,
         AVG GLOBALLY x AS global_avg,
         local_avg &lt; global_avg / 2 AS something_is_wrong
  GROUP BY some_key;
</pre>

<p>Additionally, in some very rare cases it might be necessary to explicitly ask for the aggregate operator to operate over a given set of values, which can be enforced with the "IMMEDIATELY" keyword, such as in: <code>SELECT AVG IMMEDIATELY SAMPLE 100 x</code>.</p>

<h4>Aggregates and NULLs</h4>

<p>By default, aggregate functions will skip over NULL values. Consequently, aggregating nullable values result in a nullable result (since all input might be NULL).</p>

<p>To configure that behavior, two keywords can be added right after the operator's name: <code>SKIP NULLS</code> (the default) and <code>KEEP NULLS</code>.</p>

<a name="Operators">
<h4>Operators</h4>
</a>
<div id="operator-manual">

<?
function raql($name) {
  print '<li><a href="raql/'.$name.'.html" target="expr-manual">'.ucfirst($name).'</a></li>';
}
?>

<div id="operator-index">
<h5>Boolean operators</h5>

<ul>
<?=raql('not')?>
<?=raql('and')?>
<?=raql('or')?>
<?=raql('equality')?>
<?=raql('comparison')?>
<?=raql('strict-comparison')?>
<?=raql('aggrand')?>
<?=raql('aggror')?>
</ul>

<h5>Arithmetic operators</h5>

<ul>
<?=raql('add')?>
<?=raql('sub')?>
<?=raql('neg')?>
<?=raql('mul')?>
<?=raql('div')?>
<?=raql('idiv')?>
<?=raql('mod')?>
<?=raql('pow')?>
<?=raql('abs')?>
<?=raql('reldiff')?>
<?=raql('ceil')?>
<?=raql('floor')?>
<?=raql('round')?>
<?=raql('truncate')?>
<?=raql('aggrsum')?>
<?=raql('aggravg')?>
</ul>

<h5>Trigonometric functions</h5>

<ul>
<?=raql('pi')?>
<?=raql('cos')?>
<?=raql('sin')?>
<?=raql('tan')?>
<?=raql('acos')?>
<?=raql('asin')?>
<?=raql('atan')?>
<?=raql('cosh')?>
<?=raql('sinh')?>
<?=raql('tanh')?>
</ul>

<h5>Arithmetic functions</h5>

<ul>
<?=raql('exp')?>
<?=raql('log')?>
<?=raql('log10')?>
<?=raql('sqrt')?>
<?=raql('sq')?>
</ul>

<h5>Smoothing</h5>

<ul>
<?=raql('smooth')?>
<?=raql('moveavg')?>
</ul>

<h5>Value selection operators</h5>

<ul>
<?=raql('min')?>
<?=raql('max')?>
<?=raql('aggrmin')?>
<?=raql('aggrmax')?>
<?=raql('aggrfirst')?>
<?=raql('aggrlast')?>
<?=raql('largest')?>
<?=raql('one-out-of')?>
<?=raql('once-every')?>
<?=raql('lag')?>
</ul>

<h5>Grouping/Sketching functions</h5>

<ul>
<?=raql('top')?>
<?=raql('group')?>
<?=raql('sample')?>
<?=raql('past')?>
<?=raql('percentile')?>
<?=raql('aggrhistogram')?>
<?=raql('count')?>
<?=raql('distinct')?>
<?=raql('fit')?>
</ul>

<h5>Bit-wise operators</h5>

<ul>
<?=raql('bit-and')?>
<?=raql('bit-or')?>
<?=raql('bit-xor')?>
<?=raql('bit-shift')?>
<?=raql('aggrbitand')?>
<?=raql('aggrbitor')?>
<?=raql('aggrbitxor')?>
</ul>

<h5>String related operators</h5>

<ul>
<?=raql('length')?>
<?=raql('lower')?>
<?=raql('upper')?>
<?=raql('like')?>
<?=raql('index')?>
<?=raql('basename')?>
<?=raql('concat')?>
<?=raql('startswith')?>
<?=raql('endswith')?>
<?=raql('substring')?>
<?=raql('split')?>
<?=raql('in')?>
</ul>

<h5>Time related operators</h5>

<ul>
<?=raql('now')?>
<?=raql('age')?>
<?=raql('parse_time')?>
<?=raql('format_time')?>
</ul>

<h5>Networking related operators</h5>

<ul>
<?=raql('countrycode')?>
<?=raql('ipfamily')?>
<?=raql('in')?>
</ul>

<h5>NULLs and conversions related operators</h5>

<ul>
<?=raql('force')?>
<?=raql('is-null')?>
<?=raql('coalesce')?>
<?=raql('cast')?>
<?=raql('peek')?>
<?=raql('uuid_of_u128')?>
<?=raql('chr')?>
</ul>

<h5>Miscellaneous operators</h5>

<ul>
<?=raql('hysteresis')?>
<?=raql('hash')?>
<?=raql('random')?>
<?=raql('in')?>
<?=raql('remember')?>
</ul>

</div>  <!-- operator-index -->
<iframe id="operator-iframe" name="expr-manual" src="doc_placeholder.html"></iframe>
</div>  <!-- operator-manual -->

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

<h4>From files</h4>

<p>The simplest way to ingest values may be to read them from a CSV files. The <code>READ</code> operation does just that, reading a set of files and then waiting for more files to appear.</p>

<p>Its syntax is:</p>

<pre>
  READ FROM FILES "file_pattern"
    [ PREPROCESS WITH "preprocessor" ]
    [ THEN DELETE [ IF condition ] ]
  AS CSV
    [ SEPARATOR "separator" ]
    [ NULL "null_character_sequence" ]
    [ [ NO ] QUOTES ]
    [ ESCAPE WITH "escapement_character_sequence" ]
    [ VECTORS OF CHARS AS [ STRING | VECTOR ] ]
    [ CLICKHOUSE SYNTAX ]
    (
      first_field_name first_field_type [ [ NOT ] NULL ],
      second_field_name second_field_type [ [ NOT ] NULL ],
      ...
    )
</pre>

<p>If <code>THEN DELETE</code> is specified then files will be deleted as soon they have been read.</p>

<p>The <code>file_pattern</code>, which must be quoted, is a file name that can use the star character ("*") as a wildcard matching any possible substring. This wildcard can only appear in the file name section of the path and not in any directory, though.</p>

<p>In case a preprocessor is given then it must accept the file content in its standard input and outputs the actual CSV in its standard output.</p>

<p>The CSV will then be read line by line, and a tuple formed from a line by splitting that line according to the delimiter (the one provided or the default coma (",")). The rules to parse each individual data types in the CSV are the same as to parse them as literal values in the function operation code.  In case a line fails to parse it will be discarded.</p>

<p>The CSV reader cannot parse headers.  CSV field values can be double-quoted to escape the CSV separator from that value.</p>

<p>If a value is equal to the string passed as NULL (the empty string by default) then the value will be assumed to be NULL.</p>

<p>Field names must be valid identifiers (aka string made of letters, underscores and digits but as the first character).</p>

<p>Examples:</p>

<pre>
  READ FROM FILE "/tmp/test.csv" SEPARATOR "\t" NULL "&lt;NULL&gt;"
  AS CSV (
    first_name string,
    last_name string?,
    year_of_birth u16,
    year_of_death u16
  )
</pre>

<pre>
  READ FROM FILES "/tmp/test/*.csv" || (IF param.compression THEN ".gz" ELSE "")
    PREPROCESSOR WITH (IF param.compression THEN "zcat" ELSE "")
    THEN DELETE IF param.do_delete
  AS CSV (
    first_name string?,
    last_name string
  )
</pre>

<p>It is also possible to read from binary files in ClickHouse binary format, which is more efficient. Then instead of <code>CSV</code> the format is called <code>ROWBINARY</code> and the format specification in between parentheses must be given in Clickhouse's specific "NameAndType" format.</p>

<p>Example:</p>

<pre>
  READ FROM
    FILES "/tmp/data.chb" THEN DELETE
    AS ROWBINARY (
columns format version: 1
2 columns:
`first_name` Nullable(String)
`last_name ` String
);
</pre>

<h4>From Kafka</h4>

<p>It is also possible to read data from <em>Kafka</em>. Then, instead of the <code>FILE</code> specification, one enters a <code>KAFKA</code> specification, like so:</p>

<pre>
  READ FROM KAFKA
    TOPIC "the_topic_name"
    [ PARTITIONS [1;2;3;...] ]
    [ WITH OPTIONS
        "metadata.broker.list" = "localhost:9092",
        "max.partition.fetch.bytes" = 1000000,
        "group.id" = "kafka_group" ]
    AS ...
</pre>

<p>The options are transmitted verbatim to the Kafka client library <a href="https://github.com/edenhill/librdkafka">rdkafka</a>, refers to its documentation for more details.</p>

<h3>Yield</h3>

<p>If you just want a constant expression to supply data to its child functions you can use the yield expression. This is useful to build a periodic clock, or for tests.</p>

<p>Examples:</p>

<pre>
  YIELD sum globally 1 % 10 AS count
</pre>

<pre>
  YIELD 1 AS tick EVERY 10 MILLISECONDS
</pre>

<p>Yield merely produces an infinite stream of values. If no <code>every</code> clause is specified, then it will do so as fast as the downstream functions can consume them. With an <code>every</code> clause, it will output one tuple at that pace (useful for clocks).</p>

<p>Syntax:</p>

<pre>
  YIELD expression1 AS name1, expression2 AS name2, expression3 AS name3... [ EVERY duration ]
</pre>

<h3>Select</h3>

<p>The select operation is the meat of Ramen operation. It performs filtering, sorting, aggregation, windowing and projection. As each of those processes are optional let's visit each of them separately before looking at the big picture.</p>

<h4>Receiving values from parents - the <em>from</em> clause</h4>

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

<p>If all you want is to select incoming values matching some conditions, all you need is a filter. For instance, if you have a source of persons and want to filter only men older than 40, you could create an operation consisting of a single <code>where</code> clause, such as:</p>

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

<p>In case some parents are producing values at a much lower pace than the others, they might slow down the pipeline significantly. Indeed, after each tuple is merged in Ramen will have to wait for the next tuple of the slow source in order to select the smallest one.</p>

<p>In that case, you might prefer not to wait longer than a specified timeout, and then exclude the slow parent from the merge sort until it starts emitting values again. You can to that with the <code>TIMEOUT</code> clause:</p>

<pre>
  SELECT * FROM parent1, parent2 MERGE ON field1, field2 TIMEOUT AFTER 3 SECONDS
</pre>

<p>Whenever the timed-out parent emits a tuple again it will be injected into the merge sort, with the consequence that in that case the produced values might not all be ordered according to the given fields.</p>

<p>The <code>merge</code> clause syntax is:</p>

<pre>
  MERGE ON expression1, expression2, ... [ TIMEOUT AFTER duration ]
</pre>

<h4>Sorting - the <em>sort</em> clause</h4>

<p>Contrary to SQL, in Ramen sorts the query input not its output. This is because in SQL <code>ORDER BY</code> is mostly a way to present the data to the user, while in Ramen <code>SORT</code> is used to enforce some ordering required by the aggregation operations or the windowing.  Also, on a persistent query you do not necessarily know what the output of an operation will be used for, but you know if and how the operation itself needs its input to be sorted.</p>

<p>Of course, since the operations never end the sort cannot wait for all the inputs before sorting. The best we can do is to wait for some entries to arrive, and then take the smaller of those, then wait for the next one to arrive, and so on, thus sorting a sliding window.</p>

<p>The maximum length of this sliding window must be specified with a constant integer: <code>SORT LAST 42</code> for instance. It is also possible to specify a condition on that window (as an expression) that, if true, will process the next smallest tuple available, so that this sliding window is not necessarily of fixed length. For instance: <code>SORT LAST 42 OR UNTIL AGE(creation_time) &gt; 60</code> would buffer at most 42 values, but would also process one after reception of a tuple which <code>creation_time</code> is older than 60 seconds.</p>

<p>Finally, it must also be specified according to what expression (or list of expressions) the values must be ordered: <code>SORT LAST 42 BY creation_time</code>.</p>

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

<p>...would output records made of the average value of the input field <code>salary</code> and all the fields of input records, using the last encountered values.</p>

<p>NOTE: The default <code>group-by</code> clause is: nothing! All incoming records will be assigned to the same and only group, then.</p>

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

<p>NOTE: The default <code>commit</code> clause is: <code>true</code>, to commit every selected incoming values.</p>

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


<h4>Outputting: How Values Are Sent To Child Functions</h4>

<p>When Ramen commits a tuple, what tuple exactly is it?</p>

<p>The output tuple is the one that is created by the <code>select</code> clause, with no more and no less fields. The types of those fields is obviously heavily influenced by the type of the input tuple. This type itself comes mostly from the output type of the parent operations. Therefore changing an ancestor operation might change the output type of an unmodified operation.</p>

<p>The output tuple is then sent to each of the children operations, before a new input tuple is read. No batching takes place in the operations, although batching does take place in the communication in between them (the ring-buffers).  Indeed, when an operation has no tuple to read it _sleeps_ for a dynamic duration that is supposed to leave enough time for N values to arrive, so that next time the operation is run by the operating system there are, in average, N values waiting.  This behavior is designed to be efficient (minimizing syscalls when busy and avoiding trashing the cache), but offers no guaranteed batching. If a computation requires batches then those batches have to be computed using windowing, as described above.</p>

<h4>Outputting: Notifying External Tools</h4>

<p>Ramen is designed to do alerting, that is to receive a lot of information, to analyze and triage it, and eventually to send some output result to some external program. By design, there is a huge asymmetry between input and output: Ramen receives large amount of data and produces very little of it.  This explains why the mechanisms for receiving values are designed to be efficient while mechanisms for sending values outside are rather designed to be convenient.</p>

<p>In addition (or instead) of committing a tuple, Ramen can output a notification, which is a very special type of tuple. While output values are destined to be read by children workers, notifications are destined to be read by the alerter daemon and processed according to the alerter configuration, maybe resulting in an alert or some other kind of external trigger.</p>

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

<p>In order to retrieve <a href="/glossary.html#Timeseries">time series</a> from output values it is necessary to provide information about what time should be associated with each tuple.</p>

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

<p>TODO</p>

<a name="experiments">
<h2>Experiments</h2>
</a>

<p>Internally, Ramen makes use of a system to protect some feature behind control flags. This system is usable from the Ramen language itself, which makes is easier for users to run their own experiments.</p>

<p>First, the <a href="#variant-function"><code>variant</code></a> function makes it possible to know the name of the variant the server has been assigned to. This makes it easy to adapt a function code according to some variant.</p>

<p>Then, the program-wide clause <code>run if ...</code> followed by a boolean expression instruct ramen that this program should only really be run if that condition is true.</p>

<p>Using the <code>variant</code> function in this <code>run if</code> clause therefore permits to run a program only in some variant.</p>

<p>Custom experiments can be defined in the <code>$RAMEN_DIR/experiments/v1/config</code>. An example configuration can be seen <a href="https://github.com/PerformanceVision/ramen-configurator/blob/master/experiments.config">here</a>.</p>

<? include 'footer.php' ?>
