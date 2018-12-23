<? include 'header.php' ?>
<h1>Roadmap: What's to come</h1>

<h2>Architecture</h2>

<ul>
<li>Distributed supervisor;</li>
<li>Tunnel ringbuffers from host to host;</li>
<li><code>SELECT FROM host:path ...</code>;</li>
</ul>

<h2>Language</h2>

<ul>
<li>Further simplification of window management;</li>
<li>Simplified "language" requiring only sources/aggregation key/time step/filter;</li>
<li>Functions for prediction;</li>
<li>Records (ideally encoded like in/out tuples and replacing them);</li>
<li>Asynchronous functions (DNS lookups, geo-ip, ...);</li>
</ul>

<h2>Performances</h2>

<ul>
<li>Compiler should generate C rather than OCaml;</li>
<li>Make it possible to run several inline functions in the same worker;</li>
<li>Make ringbuffer size dynamic to save RAM;</li>
</ul>

<h2>Storage</h2>

<ul>
<li>storage and retrieval in ORC format;</li>
<li>data degradation as lossy compression?</li>
</ul>

<h2>Extraction</h2>

<ul>
<li>Support for graphite functions when extracting a timeseries;</li>
</ul>

<h2>Ingestion</h2>

<ul>
<li>Read from ORC;</li>
<li>Read from popular message-queues;</li>
</ul>

<h2>GUI</h2>

<ul>
<li>Grafana plugin to manage functions, configure the notifier, retention, etc...</li>
</ul>

<h2>Security</h2>

<ul>
<li>Per fields access rights;</li>
</ul>

<? include 'footer.php' ?>
