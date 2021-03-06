<? include "header.php"; ?>
<h1>What?</h1>

<p><a href="https://github.com/rixed/ramen">Open source</a>, fast, non-distributed stream processing for monitoring.</p>

<h1>Why?</h1>

<p>Thanks to such large companies as Google, Facebook, Linkedin and Netflix, the culture and practice of modern infrastructure monitoring has vastly improved and many good and free tools have been released publicly. Those tools understandably focus on large distributed infrastructure.</p>

<p>Ramen is designed for the scale most mortals are dealing with.</p>

<p>Ramen has been designed to process network monitoring data in embedded appliances but can be useful in other contexts. If you need an all-purpose stream processor to turn inputs time series into dashboards or alerts but do not want to deploy Kubernetes in your three racks of hardware or have only a couple of GiB left of RAM for monitoring, then you might want to consider using Ramen.</p>

<h1>How?</h1>

<p>The key design principles are:</p>

<ul>
<li>Ramen is <em>not</em> based on a persistent message queue; Instead messages are sent directly from producers to consumers, using <b>memory-mapped ring-buffers</b> (or, as a possible optimisation, direct function calls).</li>

<li>Operations are defined using a <b>strongly typed SQL-like language</b> that's compiled down to machine code for efficiency. The type system knows about NULLs, integers up to 128 bits, network addresses, compound types such as lists and structures, etc... Available functions include the usual online algorithms and then some.</li>

<p>This is how it looks like:</p>
<pre>
DEFINE memory_alert AS
  SELECT time, host, free + used + cached AS total
  FROM memory
  GROUP BY host
  NOTIFY "Low RAM" WHEN free / total < 0.5;
</pre>

<li>An HTTP daemon mimics the <a href="https://graphite-api.readthedocs.io/">Graphite API</a> so Ramen <b>works with <a href="https://grafana.com">Grafana</a> out of the box</b>.</li>

<li>Past data can be archived so that <b>Ramen can also answer queries about the past</b> and be useful not only for real-time dashboarding and alerting but also for troubleshooting/capacity planning/...</li>
</ul>

<h1>More information</h1>

<ul>
<? foreach ($info_pages as $page => $p) { ?>
<?   if ($page == $current) continue; ?>
  <li><a href="<?=$page?>"><?=$p['title']?></a>;</li>
<? } ?>
</ul>

<? include "footer.php" ?>
