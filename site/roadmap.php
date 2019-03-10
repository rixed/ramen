<? include 'header.php' ?>
<h1>Roadmap: What's to come</h1>

<a name="architecture">
<h2>Architecture</h2>
</a>

<ul>
<li>Distributed supervisor;</li>
<li>Tunnel ringbuffers from host to host;</li>
<li><code>SELECT FROM host:path ...</code>;</li>
</ul>
<p>All of the above ETA: summer 2019</p>

<a name="language">
<h2>Language</h2>
</a>

<ul>
<li>Fix the parse errors;</li>
<li>Further simplification of window management;</li>
<li>Simplified "language" requiring only sources/aggregation key/time step/filter;</li>
<li>Functions for prediction;</li>
<li>Records (ideally encoded like in/out tuples and replacing them);</li>
<li>Asynchronous functions (DNS lookups, geo-ip, ...);</li>
<li>Deal with integer overflows somehow.</li>
</ul>

<a name="performances">
<h2>Performances</h2>
</a>

<ul>
<li>Compiler should generate C rather than OCaml;</li>
<li>Make it possible to run several inline functions in the same worker;</li>
<li>Make ringbuffer size dynamic to save RAM;</li>
<li>Optimize windowing (while keeping it flexible).</li>
</ul>

<a name="storage">
<h2>Storage</h2>
</a>

<ul>
<li>storage and retrieval in ORC format (in the work right now);</li>
<li>data degradation as lossy compression?</li>
</ul>

<a name="extraction">
<h2>Extraction</h2>
</a>

<ul>
<li>Support for graphite functions when extracting a time series;</li>
<li>Send alerts to Kafka.</li>
</ul>

<a name="ingestion">
<h2>Ingestion</h2>
</a>

<ul>
<li>Read from ORC;</li>
<li>Read from netCDF?</li>
<li>Read from popular message-queues.</li>
</ul>

<a name="gui">
<h2>GUI</h2>
</a>

<ul>
<li>Grafana plugin to manage functions, configure the alerter, retention, etc...</li>
</ul>

<a name="security">
<h2>Security</h2>
</a>

<ul>
<li>Per fields access rights;</li>
</ul>

<? include 'footer.php' ?>
