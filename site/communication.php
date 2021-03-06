<? include 'header.php' ?>
<h1>Ringbuffers and ringbuffer references</h1>

<h2>Input ringbuffers</h2>

<p>Depending on the flow of events, the number of parents, the location of children, the type of operation, and so on, we might want to use different kind of message passing mechanism in between workers. But ringbuffers are a versatile default, and as of now still the only supported mechanism.</p>

<p>In the future the following should be added:</p>

<ul>
<li>Ringbuffers should be dynamically resized according to the volume that goes through them to minimize memory pressure;</li>
<li>A special daemon should implement tunnelling of ringbuffers from one machine to another, to allow Ramen workers to be spread amongst several hosts;</li>
<li>When several functions are tightly coupled (same program, no intermediary reader) then they should be run in a single worker, and tuples should be passed using synchronous function calls, bypassing any ringbuffer;</li>
<li>Optionally, non-wrapping ringbuffers used for archiving should be replaced by ORC files.</li>
</ul>

<h2>Output specifications</h2>

<p>When starting a worker, the supervisor must also instruct its parents that they have a new child and should start sending their output also to its ringbuffer. For instance, in the example of the previous section, the worker implementing the function <code>services/web/http_logs</code> has to be instructed to copy its output (actually, only those fields that are used: <code>start</code>, <code>stop</code>, <code>hostname</code> and <code>ip_server</code>) into <code>web_resp_times</code> input ringbuffer.</p>

<p>Indeed, contrary to a design based on a message queue where children subscribe to their parents output queue, in Ramen parents send directly their output to their children. For the parents this is more work as they are forced to do a job that would have to be done by the message broker otherwise. But that gets rid of half the communications. Also, parents write only the fields that are actually used by their children, which can be much smaller than the whole tuple.</p>

<p>So, in addition to an input ringbuffer, each worker is also given an output reference file describing in which ringbuffers it should write what fields. This file is therefore not unlike a symbolic link with several targets, and more meta data attached to each of those targets.</p>

<p>In addition to the ringbuf file name and a field mask, the output ringbuf reference also has an optional timestamp (after which the output should cease) and a channel identifier (see <a href="archival.html">Archival and Replay</a>).</p>

<p>Some operations join inputs from different parents in a way that requires the inputs to be present in distinct ringbuffers. In that case the supervisor will create several input ringbuffers accordingly.</p>

<h2>Backpressure</h2>

<p>If one of those ringbuffer is full then the parent will block, therefore its own input ringbuffer will fill up, and so on, thus exercising back pressure up to the source of events.</p>

<? include 'footer.php' ?>
