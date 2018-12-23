<? include 'header.php' ?>
<h1>Glossary</h1>

<a name="AggregationFunction"><h2>Aggregation Function</h2></a>

<p>A function that build a result value out of several input values.  The result is finalized and output when some given condition is met (see <a href="#Commit">commit</a>).</p>

<a name="Clause"><h2>Clause</h2></a>

<p>Part of an <a href="#Operation">operation</a>. Especially, the aggregate operation has a `select` clause, a `where` clause, a `group-by` clause, etc...</p>

<a name="Commit"><h2>Commit</h2></a>

<p>When several incoming <a href="#Tuple">tuples</a> are aggregated into a single <a href="#Group">group</a>, a condition must be specified that, when met, will trigger the output of the aggregated value. This is called to _commit_ the group.  What happens to that group after the result tuple has been output depends on the flush clause; By default it will merely be reset (tumbling windows), but it is also possible to resume the aggregation from where it was or to expels oldest events (sliding window) or even other behaviors.</p>

<a name="Event"><h2>Event</h2></a>

<p>An individual message to be processed. In Ramen events are <a href="#Tuples">tuples</a>. `Event`, `message` and `tuple` are used interchangeably.</p>

<a name="FQName"><h2>Fully Qualified Name</h2></a>

<p>The fully qualified name of an <a href="#Operation">operation</a> is the name of the <a href="#Program">program</a> it belongs to, followed by a slash ("/"), followed by the operation name. Those FQ names are globally unique.</p>

<a name="Group"><h2>Group</h2></a>

<p>Within the context of an operation, a `group` refers to an aggregation group, ie. the set of tuples sharing the same key and aggregated together. This group lives (in RAM) until it is committed, when it can be deleted or downsized before resuming the operation. Aggregation groups are therefore closely related to the concept of <a href="#Windowing">windows</a>.</p>

<a name="Notification"><h2>Notification</h2></a>

<p>In addition to tuples, workers also output so called `notifications` using the `notify` clause. Those follow a particular format and are not intended for other workers but for the notifier process, that may eventually send an alert to users.</p>

<a name="Nullable"><h2>Nullable</h2></a>

<p>Said of a tuple field that can hold the NULL value. Most fields should not be nullable as it implies expensive runtime checks.</p>

<a name="Operation"><h2>Operation</h2></a>

<p>An operation is a statement in the SQL-inspired specific language, describing how to produce some output from some inputs. Operation have names that must be unique within a <a href="#Program">program</a>.  Every operation has an input and an output type, which is the types of the tuples they consume and produce. Operations can receive tuples from any other operations of the same program or from any operations of any other program already compiled.</p>

<a name="Program"><h2>Program</h2></a>

<p>Containing one or several <a href="#Operation">operations</a>. This is the smallest subset of the configuration that can be edited/compiled/started/stopped.  Inside a program functions are allowed to form cycles.  In addition to a set of operations, a program also has a name. Those names must be globally unique.</p>

<a name="RingBuffer"><h2>Ring-Buffer</h2></a>

<p>Fixed sized FIFO buffer used by workers to exchange tuples.</p>

<a name="StatefulFunction"><h2>Stateful function</h2></a>

<p>A function that requires an internal state in order to output a result.  <a href="#AggregationFunction">Aggregation functions</a> are a special case of stateful functions.</p>

<a name="StatelessFunction"><h2>Stateless function</h2></a>

<p>A function that requires nothing more than its parameters to compute its result. For instance, all arithmetic functions are stateless.</p>

<a name="StreamProcessor"><h2>Stream Processor</h2></a>

<p>A software which purpose is to perform user defined computations on a theoretically infinite stream of data.  See: https://en.wikipedia.org/wiki/Data_stream_management_system.</p>

<a name="Timeseries"><h2>Timeseries</h2></a>

<p>A time series is a series of values indexed by time. Timeseries can be extracted from Ramen if additional information about event times are provided.  See: https://en.wikipedia.org/wiki/Time_series.</p>

<a name="Tuple"><h2>Tuple</h2></a>

<p>What is consumed and produced by functions.  A set of values, each of its own type. Those values are called "fields" and beside its type a field also has a name that must be unique in the tuple.  For instance, <code>{ "name": "John Dae"; "age": 42; "is_male": true }</code> is a tuple with 3 fields (here represented in JSON for convenience; Ramen does not encode tuples as JSON internally).</p>

<a name="Windowing"><h2>Windowing</h2></a>

<p>In a <a href="#StreamProcessor">stream processor</a>, _windowing_ refers to how incoming events are batched for aggregation. Usually windows are either sliding or tumbling, with variations (see for instance <a href="https://i.stack.imgur.com/mm06A.jpg">this picture</a>). Ramen is rather unique in this regard that it has no notion of a window, especially not one bound to time, since it has no preconception of time. Instead, Ramen has explicit conditions for when to stop an aggregation (to <a href="#Commit">commit</a> the group), and what to keep from one aggregation to the next. This makes it possible to implement many windowing behavior, including tumbling and sliding windows.</p>

<a name="Worker"><h2>Worker</h2></a>

<p>One of the possibly many executables generated and run by Ramen to carry out some operation on the data stream.</p>

<? include 'footer.php' ?>
