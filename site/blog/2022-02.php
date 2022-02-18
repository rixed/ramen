<? include 'header.php' ?>

<h1>What happened those last two years?</h1>
<p class="date">2022-02-17</p>

<h2>About this blog</h2>

<p>Ramen project started in mid 2017. This is by now the longest programming project I ever tasked myself with.</p>

<p>My plan was to blog only about big achievements (or questionable decisions). As it turned out, the big step I was working on in 2020 took so long and had so many repercussions that this blog came to a complete halt. You will be able to read more on this big step in the next section. More or less unconsciously I was also probably glad to do away with this monthly chore: the less time spent writing those posts, the more time to work on Ramen and the merrier.</p>

<p>Long term this was not a good strategy. Indeed, posting regularly about an ongoing project helps focusing on important tasks which in turn helps with motivation. It can also come handy to keep this track record of past design decisions to refer back to it when wondering why things are the way they are.</p>

<p>So, in retrospect, I wish I had stuck to that habit.</p>

<p>Now, let's see what took so long that there have been no post for two years.</p>

<h2>A new code generator: dessser</h2>

<h3>Why?</h3>

<p>One of the starting design decision for Ramen was to compile data manipulation operations down to native machine code.</p>

<p>To get started, the initial "compiler" was simply generating OCaml source code corresponding to the operation. This was good enough and, since Ramen itself is implemented in OCaml, allowed to share lots of code with the generated workers. The end goal has always been to replace this lazy approach by a more efficient code generator though, one that does not rely on automatic memory management or boxed heap values.</p>

<p>Performance was an obvious goal, but it was not the only one.</p>

<p>Indeed, in many places Ramen serializes values that can only be deserialized by OCaml programs; especially for the shared configuration, which the graphical client program (RmAdmin) happens to connect to. Implying: RmAdmin is a C++ multithreaded program (for Qt) that's bootstrapped from OCaml (to deserialize everything received from Ramen's configuration server). Sharing information, especially complex data types, between OCaml and C++ was laborious and error prove, so I envisioned to endow Ramen with a code generator able to transpile to C or C++ in addition to OCaml, and that would also generate serializers and deserializers that could then replace had-oc serialization for the configuration and free RmAdmin from the need to be bootstrapped from OCaml, thus making it way easier to build for inferior operating systems.</p>

<p>Notice that in the future when workers will be implemented in C/C++ the very same issue would have emerged since they, too, exchange data with the configuration server.</p>

<p>So, the work started in early 2020 and took... a bit longer than expected.</p>

<p>The original plan was to use <a href="https://okmij.org/ftp/ML/MetaOCaml.html">BER MetaOCaml</a>, which looked like the perfect replacement for my makeshift printf-based OCaml code generator. Also, it's the coolest tool available and if it's the first time you hear about it consider yourself <a href="https://xkcd.com/1053/">lucky</a>, stop reading this right now and head toward Oleg's website to learn more.</p>

<p>Unfortunately, Ramen design did not seem to fit MetaOCaml very well. When generating workers, Ramen kind of starts from the data types, and although MetaOCaml shines at generating code, I couldn't find a way to make it generate types. Also, MetaOCaml ability to generate C rather than OCaml seemed limited and still in proof-of-concept territory. All in all, I eventually decided that it would be easier and safer to swap the code generator with a custom one specifically designed for data manipulation and serialization.</p>

<p>In retrospect, was it a good decision? I'll never know, but at least I've been happy enough with the new code generator so far.</p>

<h3>Early design decisions</h3>

<p>Here is the list of initial design decisions with some reason behind each of them and why some are debatable.</p>

<ol>
<li>
  <p>One of the main use case is to serialize to deserialize from various encoding formats.</p>
  <p>Unlike many recent ser/des tools, this one must be <i>mostly</i> agnostic to the encoding. Indeed, within ramen itself it could be used to manage data in the format used in the ring-buffer (designed to be quick, if wasteful with RAM) as well as the format used to share the configuration amongst clients (no need to be that fast but should make good use of network bandwidth), decoding incoming data in every supported format (ClickHouse binary files, CSV files, JSON messages...). There is no intend to support columnar encodings though, which have a much more complex structure that can be properly implemented only by a dedicated library.</p>
</li><li>
  <p>As a consequence, it could be used to generate a converter between two encodings, which might be useful in Ramen to import external data. The generated converter should not reify the intermediary value but convert it while it's being read.</p>
  <p>This later constraint proven not that useful for Ramen, which, as of now, still reads then write each value for generality. Yet, this constraints makes it much harder to implement converters for encoding formats that have no deterministic order for fields (such as JSON).</p>
</li><li>
  <p>The tool must support all types currently supported by Ramen, including null. It must also support proper sum types (so that we can add support for AVRO in some not so distant future).</p>
  <p>It is unusual, thus questionable, to have both proper sum types and still a nullable type modifier. Notice that the SQL NULL is not the same as Haskell's Maybe or OCaml's Option types, because SQL's is <i>contagious</i>. You cannot have a NOT NULL NULL: anything that touches NULL is just NULL. And I grew fond of this behavior. Maybe I'm biased, but for now NULL stays onboard.</p>
  <p>Also, Ramen has some types that are specific to network monitoring, so these must be present in some shape or form in the new code generator. Currently they are implemented as "user defined types" within the code generator library, a questionable feature that will never be really used and that I'm not convinced should stay.</p>
</li><li>
  <p>The new code generator must support static as well as dynamic field-masks.</p>
  <p>Again, this makes the implementation harder but Ramen can't make use of the generated code for workers without this optional feature.</p>
</li><li>
  <p>The tool must support various backend languages (initially OCaml and C/C++).</p>
  <p>It turned out that C++ was way easier than C, because of lambdas (hopefully no real closures were needed). Generating C++ is quite complex and brittle though, and compilation takes forever, in some cases crashing or OOMing the machine. In the future I'd like to support LLVM IR which will force me to properly resolve lambdas which may help with C++ compilation speed too.</p>
</li><li>
  <p>The tool must also be able to serialize to/from heap stored values in canonical form for the selected backend.</p>
</li><li>
  <p>Finally, it must be possible to define data manipulation operations acting on those heap stored values. Ideally, it must be possible to translate any Ramen functions into this new language.</p>
  <p>As a consequence, this new code generator library has operations to do groups, tops, etc. Those operation implementation are too complex and does not fit the code generator very well, but I couldn't find a way to implement them only with the simpler operations available.</p>
</li>
</ol>

<p>I won't go into more details about dessser's design and implementation here, saving that for later installments. Instead I will just report on the current status of that "engine swap".</p>

<h3>Current status</h3>

<p>The new code generator is a separate project, named <i>dessser</i> and hosted <a href="https://github.com/rixed/dessser">on github</a>.</p>

<p>It is still quite rough around the edges, but the minimum to support Ramen's use case has been implemented. Ramen can make use of it to compile workers (with the legacy code generator as a fallback whenever an operation is not present in dessser yet). This is still hidden behind an experiment though, and the default is to rely solely on the legacy compiler.</p>

<p>This dessser library is also used to encode the configuration exchanged by all daemons composing Ramen. All types used for that configuration are therefore now defined in dessser type specification language and then translated to OCaml or C++ by <i>dessserc</i>, the dessser command line tool (see for instance <a href="https://github.com/rixed/ramen/blob/master/src/worker.type">one such type definition</a>). This works well in practice, including for the C++ GUI (RmAdmin), which no longer needs bits of OCaml and is therefore easier to build (it's been ported to Windows without too much hassle thanks to Qt).</p>

<p>The dessser library could also be used to generate random data to load-test Ramen (see the separate project <a href="https://github.com/rixed/datasino">datasino</a>).</p>

<p>It cannot be said that dessser is user friendly but there is a lot that it can do and by and large it's quite useful, and have opened new pathways to more optimisations.</p>

<h3>Next steps</h3>

<p>In no particular order:</p>

<ul>
<li>
  <p>Given the central role occupied by dessser within Ramen it's recommended to have more exhaustive tests. Especially, I'd like to implement an <i>invertible type checker</i> that could act as a smart program generator for testing; more about that in a later installment.</p>
</li><li>
  <p>As already mentioned, LLVM is very much on the roadmap. Dessser intermediary language is already quite similar to LLVM's, but for the lambda expressions that have to be taken care of somehow.</p>
</li><li>
  <p>Currently, even when a worker is compiled with dessser, it is still compiled with dessser's OCaml backend, since the backbone of a worker is still a constant OCaml function (parameterized with generated functions). If this backbone also existed in C++ then the whole worker could be C++ and free from automatic memory management (ignoring for now how slow and inefficient the generated C++ code is).</p>
</li><li>
  <p>Some important aspects that's been left aside is provision for schema extensions. This probably should have been part of the initial set of design decisions for dessser, but given that Ramen already versions the workers (their code, their snapshots and more importantly the format of the data they exchange) it seamed a distant concern. But it has become a very urgent concern as soon as dessser got used for the configuration. As of now, if a client using a different version of the configuration schema connects to the configuration server then all bets are off. A safe way to exchange values encoded/decoded in different versions of a schema is long overdue.</p>
</li><li>
  <p>Dessser should understand other data schemas (it currently only understands its own and ClickHouse's <i>NamesAndTypes</i> format).</p>
</li><li>
  <p>A few easy optimisations are still to be implemented, such as not encoding default values.</p>
</li>
</ul>

<? include 'footer.php' ?>
