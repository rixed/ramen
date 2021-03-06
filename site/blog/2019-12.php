<? include 'header.php' ?>

<h1>Walk through the implementation of deep field selection</h1>
<p class="date">2019-12-07</p>

<p>This blog entry is unusual in that it documents an aspect of Ramen implementation that is amongst the hardest to comprehend.</p>

<p>Arguably, such documentation should reside with the code; But it is a well-known fact that the order of exposition that's best for an explanation does not always match how code is organized in a code base.  One typical solution to this problem is literate programming, which comes with its own flaws. Another typical solution which I'm trying here is to supplement code documentation with blog posts.</p>

<p>This one is going to describe the implementation of how subfield access is represented and compiled, with a special focus on unused fields elision.</p>

<h2>Compound types</h2>

<p>With compound types come the need to access subparts of a value. For now we have 4 possible compound types:</p>

<h3>Array like compound types</h3>

<p>The simplest compound types are <em>vectors</em> and <em>lists</em>, which are very similar.</p>

<p>All sub-items of vectors or lists are of the same type.</p>

<p>The difference between vectors and lists is that vectors have a dimension that is known at compile time and that must be at least 1, whereas lists have a dynamic length and can be empty. The distinction is useful for type checking but past that point vectors and lists are compiled into the same thing: arrays.</p>

<p>Those compound types, like all other types, are described in the very classical sum type <code>RamenTypes.structure</code>, as variants <code>TVec of int * t</code> and <code>TList of t</code>. Note that the type <code>t</code> here is simply:</p>

<pre>
type t =
  { structure : structure ;
    nullable : bool }
</pre>

<p>In this post we do not need to pay attention to NULLable values and therefore from now on you can equal <code>t</code> and <code>structure</code>.</p>

<p>Values of those types are described by the corresponding variants <code>VVec of value array</code> and <code>VList of value array</code> of the type <code>RamenTypes.value</code>.</p>

<p>Immediate values of type vectors can be created with this syntax, for instance here creating a vector of dimension 3: <code>[ 1; 4; 9 ]</code>. This will be parsed as the immediate value: <code>RamenExpr.VVec [| VI32 1l; VI32 4l; VI32 9l |]</code>.</p>

<p>It is also possible to enter immediate vector expressions such as: <code>[ 1^2; 2^2; 3^2 ]</code>, which will be parsed into an AST of type <code>RamenExpr.r</code> and value:</p>

<pre>
RamenExpr.{
  text = Vector [
    { text = Stateless (SL2 (Pow, { text = Const (VI32 1l) ; ... },
                                  { text = Const (VI32 2l) ; ... })) ; ... },
    { text = Stateless (SL2 (Pow, { text = Const (VI32 2l) ; ... },
                                  { text = Const (VI32 2l) ; ... })) ; ... },
    { text = Stateless (SL2 (Pow, { text = Const (VI32 3l) ; ... },
                                  { text = Const (VI32 2l) ; ... })) ; ... },
</pre>

<p>...omitting all fields from <code>RamenExpr.t</code> but </code>text</code>.</p>

<p>Notice that there is no way to write a constant list. Since a constant list would have a known length then it would be a vector (exception: the empty list would kind of make sense). Lists can only appear as the result of some functions.</p>

<p>Given <code>v</code> is a vector or a list and <code>n</code> is any integer, the syntax to access <code>v</code>'s <code>n</code>th item is the familiar: <code>v[n]</code>.</p>

<h3>Product types</h3>

<p>The other compound types supported by Ramen are <em>tuples</em> and <em>records</em>.</p>

<p>Unlike array-like types, tuples and records can hold together values of different types.</p>

<p>Tuple types are represented by the variant <code>RamenTypes.TTuple of t array</code> and tuple values by <code>RamenTypes.VTuple of value array</code>. Internally, tuple values will be compiled into OCaml tuples.</p>

<p>Immediate tuple values can be created with the syntax: <code>(1; "two"; 3.0)</code> similar to vectors but with parentheses instead of brackets (and no restriction that every values must have the same type).</p>

<p>Records are very similar to tuples, adding only the possibility to access fields by name. They are also compiled into OCaml tuples. Record types are described by the variant <code>RamenTypes.TRecord of (string * t) array</code> and values by <code>RamenTypes.VRecord of (string * value) array</code>.</p>

<p>Immediate record values are usually created with the SQL select clause syntax: <code>1 AS foo, "two" AS bar, 3.0 AS baz</code>. Also, to help disambiguate the parsing in case of nested records, one can use parenthesis and the <code>AZ</code> keyword instead of <code>AS</code>: <code>(1 AZ foo, "two" AZ bar, 3.0 AZ baz)</code>.</p>

<p>The syntax to access a subfield <code>f</code> of a record <code>r</code> is the familiar dotted notation <code>r.f</code>. To access the <code>n</code>th item of a tuple <code>t</code> there is no such convenient shorthand and one has to use the <code>get</code> function: <code>get(n, t)</code>.</p>

<p>Actually, that <code>get</code> function is universal and all other forms seen so far are just syntactic sugar for <code>get</code>:</p>

<p><code>v[42]</code> is equivalent to <code>get(42, v)</code>,  <code>r.f</code> is <code>get("f", r)</code>, and <code>foo.bar[7].baz</code> is <code>get("baz", get(7, get ("bar", foo)))</code>.</p>

<h2>Accessing deep fields, and field elision</h2>

<p>As far as parsing is concerned, <code>get</code> is pretty much an ordinary function, defined as the variant <code>RamenExpr.Get</code> of the <code>RamenExpr.stateless2</code> type that denotes all stateless functions with 2 parameters, with some added syntactic sugar on top.</p>

<p>But <code>get</code> is much more than a usual function.  In conjunction with the other special expressions <code>RamenExpr.Variable</code>, <code>Path</code> and <code>Binding</code>, </code>Get</code> implements subfield access.</p>

<h3>Variables</h3>

<p>What's called <em>variable</em> in this context is, not unlike in general purpose programming languages, a named handle for some value. Ramen variables are not only immutable, but they cannot even be created. There are only a handful of given variables depending on the context.</p>

<p>Variables are used to hold a function input value, its output value, the record of program parameters, the record representing the environment, etc. Some are seldom used such as the variable holding the smallest input value in the input sorting buffer.</p>

<p>Variables can be referenced by their name: <code>in</code> and <code>out</code>, for instance, are the variables holding respectively the input and output value of a function, whereas <code>smallest</code> is the name of the variable holding the smallest input value in the current sort buffer.  Those variables are usually of a record type, and then their name can sometimes be omitted, so that for instance in the SELECT clause one can write simply <code>foo</code> instead of <code>in.foo</code>.</p>

<p>When it is omitted, a variable name is represented by the value <code>Unknown</code>. Therefore, <code>foo</code> is equivalent to <code>get("foo", unknown)</code>.</p>

<p>The list of all those well-known variables can be found in <code>RamenLang.parse_variable</code>, the function that parses such a name into a variable.</p>

<p>The parser <code>RamenExpr.Parser.sugared_get</code>, which is responsible for parsing all the syntactic sugar around the <code>get</code> function, is also where omitted variable names are assigned to the variable <code>Unknown</code>.</p>

<p>Unknown variables are replaced by actual ones in <code>RamenOperation.resolve_unknown_variables</code> as part of <code>RamenOperation.checked</code>, a function that is called right after an operation has been parsed, to perform various checks, in particular to check that the accessed variables are indeed available when they are accessed (for instance, using <code>smallest<code> outside of a SORT operation will raise an error).</p>

<p>There is also a special variable named Record that is used to refer to any previously opened record (more on this later).</p>

<h3>Paths</h3>

<p><code>RamenExpr.Path</code> expressions are similar to <code>RamenExpr.Get</code> expressions, with a few important differences:</p>

<ul>

<li>To begin with, there is no way to create a path expression from the expression parser. Only later during the compilation process will some get functions be replaced by equivalent paths.</li>

<li>Then, path expressions can handle deep access directly: a chain of <code>get</code> functions such as <code>get("baz", get(7, get ("bar", in)))</code> will be replaced with a single Path expression.</li>

<li>Last but not least, paths apply only to input record.</li>

</ul>

<p>Indeed, path expressions, which variant is <code>RamenExpr.Path of path_comp list</code>, materialize deep field selection into the parent functions output.</p>

<p>For instance, <code>in.bar[7].baz</code> (or merely <code>bar[7].baz</code> whenever <code>In</code> is the default variable) will be turned into <code>RamenExpr.Path [ Name "bar"; Idx 7; Name "baz" ]</code>.</p>

<p>The function responsible for this change is <code>RamenFieldMaskLib.subst_deep_fields</code>, called as part of <code>RamenCompiler.precompile</code> (Note: what is called precompilation in this context is merely parsing + typing of ramen programs. The actual compilation, ie. generation of a native executable, takes place elsewhere both in time and place. Look for it in <code>RamenMake</code>).</p>

<p>Once all input field selections have been replaced by paths, Ramen can start to think about what it needs from the parent output values and how to obtain them.</p>

<h3>FieldMasks</h3>

<p>A <em>field mask</em> represents the selection of fields that a given function wants to receive from each of its parents.</p>

<p>For instance, in this simple program:</p>

<pre>
DEFINE parent AS YIELD 1 AS one, 2 AS two, 3 AS three EVERY 1s;
DEFINE child AS SELECT two + two AS four FROM parent;
</pre>

<p>...the parent function must send to its child the field two only, which cam be represented succinctly by the string "_X_" meaning: skip the first field, send the second field, and skip the third field. The equivalent OCaml expression is: <code>RamenFieldMask.[| Skip; Copy; Skip |]</code>.</p>

<p>Those field masks in shorter string form are stored in the out_ref files as part of the destination ring-buffer file name.</p>

<p>Deep field selection complicates this picture significantly.</p>

<p>What was basically a flat bitmask becomes a recursive structure:</p>

<pre>
DEFINE parent AS YIELD 0 AS fist, (1 AZ thumb, [2; 3; 4; 5] AZ fingers) AS hand EVERY 1s;
DEFINE child AS SELECT finger[2] AS ring, fingers[3] AS pinky;
</pre>

<p>Here the child uses only the 3rd and 4th items of the fingers vector of the hand record (of the input record), and we want to transmit only these from the parent to the child; not the whole output record nor even the whole finger vector.</p>

<p>The field mask become: "_(_(__XX))", meaning: skip the first field (fist), then enter the second field, then skip its first field (thumb), then enter its second (finger), then skip, skip, and finally copy and copy. Or the equivalent OCaml expression: <code>[| Skip; Rec; Skip; Rec; Skip; Skip; Copy; Copy |]</code>.</p>

<p>So, back to our story: Ramen has the set of all paths needed from the parent, and must be able to compute the corresponding field mask. That is the task of the <code>RamenFieldMaskLib</code> module, starting with function <code>fieldmask_of_operation</code>, and this is needed every time the out_ref specification is needed: when updating the out_ref files of course, but also when generating the code to serialize values.</p>

<p>This serialization function used in a parent function must handle all possible field masks (as we do not want to recompile a running worker every time a new child is attached to it, each with its special needs). The crux of the code generator is a field mask crawler in <code>CodeGen_OCaml.emit_for_serialized_fields</code>. The generated code will take a fully fledged output value and a field mask and serialize only the requested subfield from that value.</p>

<p>For now, there is still a small but important operation to perform in the precompilation stage: that of computing the input type of each function. This is subtly different from the set of all paths, for two reasons:</p>

<ol>
<li>If all items of a compound value are selected then the whole value is selected rather than every individual items, simplifying the code to serialize a record;</li>
<li>If a compound value is selected then there is no more need to also select any individual sub items.</li>
</ol>

<h3>Bindings</h3>

<p>Code is generated for many small functions that receive some well-known variable as parameter. For instance, the function implementing the fast variant of the <em>where clause</em> receives the <code>Param</code>, <code>Env</code>, <code>In</code>, <code>MergeGreatest</code> and <code>OutPrevious</em> variables, which hold respectively the record with all program parameters, the record with all used environment variables, the input value, the largest value form the optional merge buffer and the latest output value.</p>

<p>Instead of hard-coding in the code generator how to access each of those variables, a more general stack of accessible values is passed around. Each item of this stack is composed of a pair of a <code>RamenExpr.binding_key</code> and whatever code is supposed to be equivalent to this value at runtime. A <code>binding_key</code> identifies either a variable, a subfield of a variable, or a given stateful expression internal state, or, as a special case, any other runtime identifier.<p>

<p>This stack is initialised with all accessible variables (and all the required internal states of all present stateful expressions) before calling the code generator for a given function.</p>

<p>Provided all <code>Path</code> and <code>Get</code> expressions have been substituted with an equivalent <code>Binding of binding_key</code> expression, the AST walker can then retrieve any referenced object by looking it up in that stack. This substitution happens in <code>CodeGen_OCaml.subst_fields_for_binding</code>.</p>

<p>Beware that this stack is classically called the "environment" (<code>~env</code> named parameter), not to be confused with the UNIX process environment available via the <code>Env</code> variable.</p>

<p>This process or substitution from <code>Path</code> and <code>Get</code> expressions to bindings, then building the environment stack, and looking up in that stack how to actually transpose each access into concrete code, can look convoluted compared to hard-coding how to access each possible <code>Path</code> and <code>Get</code> expressions, but once the machinery is in place it is conceptually simpler, and allows an interesting trick: local opening of records.</p>

<p>Indeed, as soon as a record field value is known, a reference to it is pushed onto the environment stack (and popped after the record has been compiled) so that the following expressions can refer to it by name. This allows to write such things as:</p>

<pre>
SELECT 42 AS foo, foo+1 AS bar
</pre>

<p>...where at the location where <code>bar</code> is computed a binding to <code>foo</code> is present in the stack (may be shadowing another <code>foo</code> defined earlier).</p>

<h2>Conclusion</h2>

<p>Of course many things have not been covered by this short post, especially with regard to code generation, but at least these simple explanations will highlight some useful milestones and help the adventurous hacker to get the gist of some of the most obscure expressions.<p>

<? include 'footer.php' ?>
