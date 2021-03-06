<? include 'header.php' ?>

<h1>Projection of deep compound types</h1>
<p class="date">2019-01-16</p>

<h2>Compound types?</h2>

<p>Compound types are types build from other types. The basic, nearly universal compound types are lists, arrays, tuples and records. For instance an array of integers is a compound type; a list of arrays of boolean and strings is also a compound type. More sophisticated compound types can be devised: sets, maps, sum types, objects,&nbsp;... but lists and records seems to be the most important, judging by the popularity of JSON which offers only those two.</p>

<p>Ramen allows to group values into <i>vectors</i> (ie. fixed length arrays), <i>lists</i> (dynamically sized arrays) and <i>tuples</i> (ordered set of values of different types); But not records. Records are nothing more than tuples with named fields, so I though records and the required associated syntax could be added any time later easily.</p>

<h2>Software grows in cycles.</h2>

<p>The job of a programmer is twofold:</p>

<ul>
<li>On one hand, adding LOCs to grow a code base; This is the easy task that is rarely conflictual.</li>
<li>On the other hand, pruning and reorganizing that code which have grown organically in several conflictual directions, in order to strengthen a particular desired evolution; this is the part that may involve conflicts (first with the authors of the code that's being pruned and then with the management who doubt the value of reorganizing code)</li>
</ul>

<p>The evolution of data types in Ramen followed this cycle.</p>

<p>Initially, there were only scalar types, except for the input and output of functions which were records. Those records were thus treated specially. For instance, one can write <code>in.foo</code> to access the <code>foo</code> field of the input event, but it was not possible, beside the SELECT projector, to create ones own record type.</p>

<p>Then came a day when we wanted some operators to accept or return compound types, such as the <code>histogram</code> or <code>sample</code> operations which return a vector. So lists, tuples and vectors were added, with the corresponding syntax to construct them and access their components. Not records though, as the desired syntax (<code>value.subfield</code>) was already in use to access the individual fields of the input and output events.</p>

<p>Ideally, records should be normal compound types with corresponding syntax for construction and field access; And functions should input and output values of any types. Ramen would be simpler and more flexible. And it would be simpler to implement storing archives in ORC files.</p>

<p>So now is the time to implement records and do away with the special type and syntax for manipulating events. But this turned out a longer journey than expected.</p>

<h2>The burden of hierarchies</h2>

<p>It is not rare to encounter a relational data model mixed with a hierarchical data model. Who have never stored a JSON value in a text column, out of convenience?</p>

<p>This poses a problem though: a relational database knows how to project away unused columns but not how to project away unused branches of deep hierarchical values. And Ramen was no different: If a function selects a column from another, only that column is going to be transfered between the two functions; But if that column is a vector and only the first item of that vector is used, the whole vector is still going to be passed.</p>

<p>In a normal programming language one would likely pass a pointer to the structure to avoid that copy. But when the called function does not share the same memory then passing an address is no longer an option.</p>

<p>We could live with it for vectors and lists, as the use case of selecting just one item and discarding every others is probably rare. But for records this is much more likely to happen. If a function outputs a field named <code>client</code> with this value (here in JSON-like syntax):</p>

<pre>
{
  "round_trip_time": {
    "min": 0.123, "max": 0.234, "avg": 0.201,
    "95th": 0.202, "99th": 0.229
  },
  "time_to_first_byte": {
    "min": 0.21, "max": 1.517, "avg": 0.541,
    "95th": 1.493, "99th": 1.512
  }
}
</pre>

<p>...then that whole data structure is going to be sent to a child function even if that child only ever look at <code>client.round_trip_time.avg</code>.</p>

<p>This is even worse than it looks: since we would like events to be normal values (of type record), if we cannot project away unused branches of a record that means we don't know how to project away unused columns any longer!</p>

<p>It would be a shame that the convenience of records be offset by the runtime cost. So projection has to be made smarter.</p>

<p>First, Ramen must look in depth what fields and subfields (and/or what indices) are actually used by a consumer, and then instruct the producer that it must send only those elements. Currently, a flat bit mask with one bit per field is used to describe what to send in the output specification files. This mask needs to become hierarchical. And the serialization and de-serialization code needs to follow this hierarchy when coding/decoding passed events. Only then will it become possible to have large user defined records for free.</p>

<p>This is a bigger change than adding a syntax to access tuple fields, but it is a nice optimisation. Actually, event vectors, list and tuples will benefit from it.</p>

<? include 'footer.php' ?>
