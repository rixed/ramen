<? include 'header.php' ?>
<h1>Host Monitoring in 15 minutes</h1>

<h2>Starting up</h2>

<h3>Using the Docker image</h3>

<p>The docker image is about 150MiB compressed. We are going to bind the <code>ramen</code> directory in your home, so make sure $HOME/ramen does not exist. We are also going to redirect Ramen graphite impersonator, <em>collectd</em> and <em>netflow</em> UDP ports into the docker image. Run:</p>

<pre>
$ docker run --name=ramen-test -v $HOME/ramen:/ramen -p 25826:25826/udp -p 2055:2055/udp -p 29380:29380/tcp rixed/ramen
</pre>

<p>...and continue this tutorial from another terminal.</p>

<p>Redirecting the ports is optional: The docker image comes with <a href="https://collectd.org/">collectd</a> and <a href="http://fprobe.sourceforge.net/">fprobe</a> daemons that will monitor the container itself. That is not super interesting but it's enough to get some data in. You could also point external collectd and netflow sources to these ports for real world data.</p>

<p>From now on we will run the `ramen` program through docker so often that a shortcut is in order:</p>

<pre>
$ alias ramen "docker exec ramen-test ramen"
</pre>

<h3>Installing more probes</h3>

<p>Skip that section if you are happy with using only internally generated metrics for the duration of this tutorial.</p>

<h4>Installing collectd</h4>

<p><code>aptitude install collectd</code>.</p>

<p>Edit <code>/etc/collectd/collectd.conf</code> (or the equivalent, such as <code>/usr/local/etc/collectd.conf</code> if brewed on mac OS), uncomment `LoadPlugin network` and make sure the stats will be sent to your server running ramen. For now let's say you run collectd and ramen on the same host, that means you must have this configuration for the network plugin:</p>

</pre>
<Plugin network>
  Server "127.0.0.1" "25826"
</Plugin>
</pre>

<p>WARNING: Replace "127.0.0.1" with the actual address of your host if collectd runs elsewhere.</p>

<p>Then you can restart collectd (<code>systemctl restart collectd.service</code> or <code>/usr/local/sbin/collectd -f -C /usr/local/etc/collectd.conf</code> or whatever works for you).</p>

<h4>Sending more netflow to Ramen</h4>

<p>The docker image comes with an internal netflow source that will send flow information to Ramen about the traffic taking place inside the container.  This is of course of little use for monitoring but is enough to get some data.</p>

<p>If you already have access to a switch that can emit netflow statistics then you could direct them to the container port 2055 for more interesting data.</p>

<h2>Getting some data in</h2>

<p>Ramen won't do anything at all if we do not tell it to do it. At first we are going to collect netflow statistics and turn it into a small network traffic dashboard.</p>

<p>Let's create a subdirectory <code>ramen/sources</code> and create there a file named <code>demo.ramen</code> with a few instructions:</p>

<pre>
$ mkdir $HOME/ramen/sources
$ cat &gt; $HOME/ramen/sources/demo.ramen &lt;&lt;EOF
-- Ingest some data from the outside world (this is just a comment)
DEFINE netflow AS listen for netflow;
DEFINE collectd AS listen for collectd;
EOF
$
</pre>

<p>This program defines two <em>functions</em> named <code>netflow</code> and <code>collectd</code>. The construct <code>listen for ...</code> means to open a socket and decode incoming messages for the named protocols, and turn that into a stream of data. That's how we are going to get some data in for now.</p>

<p>This file can then be compiled with (the path is as seen by Ramen from within the docker image!):</p>

<pre>
$ ramen compile sources/demo.ramen --as demo
Parsing program demo
Typing program demo
Compiling program demo
Compiling "ramen_root/demo_params_v48.ml"
Compiling "ramen_root/demo_e202c4a9d0fb42089406fe7539688b2b_v48.ml"
Compiling "ramen_root/demo_d388e90d7b555a745a23719a13b26293_v48.ml"
Linking "ramen_root/demo_casing_v48.ml"
</pre>

<p>...and you should now have a <code>demo.x</code> file. Let's ask Ramen to run it:</p>

<pre>
$ ramen run sources/demo.x
</pre>

<p>A quick look at `ramen ps` confirms that it's indeed running:</p>

<pre>
$ ramen ps --pretty
operation     | #in | #selected | #out | #groups | last out | min event time | max event time | CPU   | wait in | wait out | heap    | max heap | volume in | volume out | startup time         | #parents | #children | signature                        |
demo/collectd | n/a |       n/a |    0 |     n/a |      n/a |            n/a |            n/a | 0.012 |       0 |        0 | 3932160 |  3932160 |         0 |          0 | 2018-12-13T10h52m59s |        0 |         0 | d388e90d7b555a745a23719a13b26293 |
demo/netflow  | n/a |       n/a |    0 |     n/a |      n/a |            n/a |            n/a | 0.012 |       0 |        0 | 3932160 |  3932160 |         0 |          0 | 2018-12-13T10h52m59s |        0 |         0 | e202c4a9d0fb42089406fe7539688b2b |
</pre>

<h2>Getting data out</h2>

<p>Let's see what the netflow messages are turned into:</p>

<pre>
$ ramen tail -f --with-header demo/netflow
#source,start,stop,seqnum,engine_type,engine_id,sampling_type,sampling_rate,src,dst,next_hop,src_port,dst_port,in_iface,out_iface,packets,bytes,tcp_flags,ip_proto,ip_tos,src_as,dst_as,src_mask,dst_mask
1219,176.59.205.59,0,0,1045,0,0,0,17,0,0.0.0.0,0,2,0,0,33865509,"172.17.0.1:56098",5.135.156.187,0,0,3760,1544698921.22,1544698926.91,0
924,5.135.156.187,0,0,3760,0,0,0,17,0,0.0.0.0,0,7,0,0,33865509,"172.17.0.1:56098",76.69.117.5,0,0,51413,1544698925.27,1544698926.31,0
332,173.212.202.22,0,0,6929,0,0,0,17,0,0.0.0.0,0,1,0,0,33865509,"172.17.0.1:56098",5.135.156.187,0,0,3760,1544698925.71,1544698925.71,0
125,5.135.156.187,0,0,3760,0,0,0,17,0,0.0.0.0,0,1,0,0,33865509,"172.17.0.1:56098",173.212.202.22,0,0,6929,1544698925.7,1544698925.7,0
52,46.4.105.12,0,0,445,0,0,0,6,0,0.0.0.0,0,1,0,0,7497501,"172.17.0.1:57909",123.17.109.95,0,0,57566,1544698609.67,1544698609.67,2
40,46.4.105.4,0,0,24242,0,0,0,6,0,0.0.0.0,0,1,0,0,7497501,"172.17.0.1:57909",78.128.112.10,0,0,50976,1544698607.55,1544698607.55,2
120,46.4.104.236,0,0,80,0,0,0,6,0,0.0.0.0,0,2,0,0,7497501,"172.17.0.1:57909",148.251.75.227,0,0,26204,1544698605.16,1544698608.17,2
40,46.4.125.34,0,0,7100,0,0,0,6,0,0.0.0.0,0,1,0,0,7497501,"172.17.0.1:57909",125.64.94.197,0,0,33137,1544698609.7,1544698609.7,2
40,46.4.118.133,0,0,1281,0,0,0,6,0,0.0.0.0,0,1,0,0,7497501,"172.17.0.1:57909",176.119.7.18,0,0,57163,1544698610.01,1544698610.01,2
40,46.4.118.133,0,0,7100,0,0,0,6,0,0.0.0.0,0,1,0,0,7497501,"172.17.0.1:57909",125.64.94.197,0,0,46701,1544698606.47,1544698606.47,2
^C
$
</pre>

<p>Let's now extract a time series for the <code>bytes</code> field. Assuming you are using GNU date and a Bourne-like shell, you could type:</p>

<pre>
$ ramen timeseries \
    --since $(date -d '10 minutes ago' '+%s') \
    --until $(date -d '1 minutes ago' '+%s') \
    --with-header --num-points 9 --consolidation sum \
    demo/netflow bytes
#Time,bytes
2018-12-13T11h34m15s,&lt;NULL&gt;
2018-12-13T11h35m15s,&lt;NULL&gt;
2018-12-13T11h36m15s,37213.5588409
2018-12-13T11h37m15s,348552.509063
2018-12-13T11h38m15s,411183.370782
2018-12-13T11h39m15s,519981.395792
2018-12-13T11h40m15s,643904.305475
2018-12-13T11h41m15s,636835.757926
2018-12-13T11h42m15s,173121.564614
</pre>

<p>Note: you may want to wait a bit for netflow records t be received before running this.</p>

<p>The <code>consolidation</code> option specify how to fit events into the time buckets, and possible values are <code>min</code>, <code>max</code>, <code>avg</code> (the default) and <code>sum</code>. Here we are accumulating traffic volumes from different sources so the only meaningful way to combine those volumes is to sum them.</p>

<p>That time series of course could be piped into any dashboarding program, such as the venerable <a href="http://www.gnuplot.info">gnuplot</a> (don't worry, we will use <a href="https://grafana.com">grafana</a> in a minute):</p>

<pre>
$ while sleep 10; do \
    ramen timeseries \
      --since $(date -d '31 minutes ago' '+%s') \
      --until $(date -d '1 minute ago' '+%s') \
      --num-points 30 --separator ' ' --null 0 --consolidation sum \
      demo/netflow bytes 2>/dev/null | \
    gnuplot -p -e "set timefmt '%Y-%m-%dT%H:%M:%S'; set xdata time; set format x '%H:%M'; \
      set terminal dumb $COLUMNS,$LINES; \
      plot '&lt; cat -' using 1:2 with lines title 'Bytes'";
  done

    4e+07 +-+-+--+---+--+---+--+---+--+---+--+---+************-+---+--+---+--+---+--+---+--+---+--+---+--+---+--+-+-+
          +          +         +          +       * +        * +          +         +          +         +          +
  3.5e+07 +-+                                    *            *                                       Bytes *******-+
          |                                      *            *                                                     |
          |                                     *              *                                                    |
    3e+07 +-+                                   *              *                                                  +-+
          |                                    *                *                                                   |
  2.5e+07 +-+                                  *                **                                                +-+
          |                                   *                   **                                                |
          |                                   *                                                                     |
    2e+07 +-+                                 *                     ***                                           +-+
          |                                  *                         ***********                                  |
  1.5e+07 +-+                                *                                    *                               +-+
          |                                  *                                    *                                 |
          |                                  *                                     *                                |
    1e+07 +-+                               *                                      *                              +-+
          |                                 *                                       *                               |
    5e+06 +-+                               *                                       *                             +-+
          |                                 *                                       *                               |
          +          +         +          +*        +          +          +         +*         +         +          +
        0 +*********************************-+---+--+---+--+---+---+--+---+--+---+--+******************************-+
        11:30      11:33     11:36      11:39     11:42      11:45      11:48     11:51      11:54     11:57      12:00
</pre>

<p>Ok, now that we are confident we know how to get some data in and out, let's have a look at what we can do with the data in between.</p>

<h2>Ramen Programs and Functions</h2>

<p>Programs are sets of functions. A function can be of several types: listening to some network port for some known protocol (such as collectd or netflow) is one of them. Reading data from CSV files is another. But in general though, functions will take their input from other functions output, thus creating a tree of functions ultimately computing the signal one want to alert on. A function output (and input) is thus a never-ending stream of messages, composed of several named fields of given types and that we call <em>tuples</em> or <em>events</em>. For instance, here is a tuple:</p>

<table>
<tr>
  <th>time</th>
  <th>host</th>
  <th>interface</th>
  <th>sent</th>
  <th>received</th>
</tr><tr>
  <td>1507295705.54</td>
  <td>www45</td>
  <td>em0</td>
  <td>749998080</td>
  <td>1821294592</td>
</tr>
</table>

<p>A function can have zero, one or several parents and children. All children receive a full copy of the output.</p>

<p>Programs are the smallest unit that can be created, started and stopped. A function from a program might read the output of a function defined in another program, as long as it is already running when that new program is run.</p>

<p>Functions and programs have names. Program names must be globally unique while function names need only be unique within the program they belong to. The <em>fully qualified</em> name of an function is the name of the program it belongs to, followed by a slash ("/"), followed by the name of the function. Consequently, the slash character is not allowed in an function name.</p>

<p>For instance, "base/per_hosts/hourly_traffic" is the fully qualified name of the function "hourly_traffic" in the program named "base/per_hosts". Notice that the slash ("/") in the program name is just a convention with no particular meaning.</p>

<p>For now we have a single program named "demo" containing only two functions.</p>

<h2>Computing Memory Consumption</h2>

<p>Monitoring usually involves three phases:</p>

<ol>
<li>Collecting all possible data (that's what we have just done above);</li>
<li>Turning that data into meaningful information;</li>
<li>Finally alert on that information.</li>
</ol>

<p>We are now going to see how we could turn our netflows and collectd messages into something useful.</p>

<p>Collectd events are very fine grained and one may want to build a more synthetic view of the state of some subsystem. Let's start with memory: Instead of having individual events with various bits of information about many subsystems, let's try to build a stream representing, at a given time, how memory is allocated for various usage.</p>

<p>So to begin with, let's filter the events generated by collectd memory probes.  Let's write a new program and call it <code>hosts.ramen</code>, for we will monitor hosts health in it.</p>

<pre>
DEFINE memory AS
  SELECT * FROM demo/collectd WHERE COALESCE(plugin = "memory", false);
</pre>

<p>Without going too deep into Ramen syntax, the intended meaning of this simple function should be clear: we want to filter the tuples according to their <code>plugin</code> field and keep only those originating from the "memory" plugin. The <code>COALESCE</code> function is here because a <code>WHERE</code> clause is not allowed to be NULL, but the <code>plugin</code> field can be NULL.</p>

<p>Save, compile and run it:</p>

<pre>
$ ramen compile sources/hosts.ramen --as hosts
$ ramen run sources/hosts.x
</pre>

<p>You might notice (<code>ramen tail hosts/memory</code>) that this plugin only sets one value and also that the <code>type_instance</code> field contains the type of memory this value refers to.  Apart from that, most fields are useless. We could therefore make this more readable by changing its function into the following, enumerating the fields we want to keep (and implicitly discarding the others):</p>

<pre>
DEFINE memory AS
  SELECT start, host, type_instance, value
  FROM demo/collectd
  WHERE COALESCE(plugin = "memory", false);
</pre>

<p>The output is now easier to read; it should look something like this:</p>

<pre>
$ ramen tail hosts/memory --with-header
#time,host,type_instance,value
1522945763.3,"poum","used",4902309888
1522945763.3,"poum","cached",17255350272
1522945763.3,"poum","buffered",2819915776
1522945763.3,"poum","free",763043840
1522945763.3,"poum","slab_unrecl",97742848
1522945763.3,"poum","slab_recl",7081136128
1522945773.3,"poum","used",4902801408
1522945773.3,"poum","cached",17255350272
1522945773.3,"poum","buffered",2819915776
1522945773.3,"poum","slab_recl",7081103360
1522945773.3,"poum","slab_unrecl",97460224
1522945773.3,"poum","free",762867712
...
</pre>

<p>On your own system, other "type instances" might appear; please adapt accordingly as you read along.</p>

<p>There is still a major annoyance though: we'd prefer to have the values for each possible "type instances" (here: the strings "free", "used", "cached" and so on) as different columns of a single row, instead of spread amongst several rows, so that we know at each point in time what the memory usage is like.  Since we seem to receive one report form collectd every 10 seconds or so, a simple way to achieve this would be to accumulate all such reports for 30 seconds and then output a single tuple every 30 seconds with one column per known "type instance".</p>

<p>For this, we need to "aggregate" several tuples together, using a <code>GROUP BY</code> clause. Try this:</p>

<pre>
DEFINE memory AS
  SELECT
    MIN start AS start,
    MAX start AS stop,
    host,
    COALESCE (type_instance, "") AS _type,
    AVG (IF _type = "free" THEN value) AS free,
    AVG (IF _type = "used" THEN value) AS used,
    AVG (IF _type = "cached" THEN value) AS cached,
    AVG (IF _type = "buffered" THEN value) AS buffered,
    AVG (IF _type LIKE "slab%" THEN value) AS slab
  FROM demo/collectd
  WHERE COALESCE (plugin = "memory", false)
  GROUP BY host, start // 30
  COMMIT AFTER in.start &gt; out.start + 30;
</pre>

<p>Let's quickly go through some bits of this example that might not be immediately clear.</p>

<ul>
<li>Most of the fields have been given a name explicitly with the <code>AS</code> keyword;</li>
<li>We compute both the start and stop of each event by taking the minimum and maximum of the timestamp present in collectd, because we need the events to have a duration (time series extraction should consider that default stop is the next start but that's still TBD);</li>
<li>Any field name starting with underscore, such as <code>_type</code> in this example, is private to the function and not part of the actual output;</li>
<li><code>//</code> is the integer division, this here we are grouping by host names and slices of 30 seconds;</li>
<li>The COMMIT clause tells ramen when to stop aggregating a group and output the result;</li>
<li>The field of new incoming tuples are accessible via the <code>in.*</code> record ; this is also the default so we do not have to write <code>in.value</code> but can merely write <code>value</code> instead. But in the COMMIT clause we need to refer to both the incoming tuple and the aggregated <code>out</code> tuple, so we make this explicit;</li>
<li>Beside <code>in</code> and <code>out</code> there are a few more rarely used records that are described in the <a href="language_reference.html#tuple-field-names">manual</a>;</li>
</ul>

<h2>Dashboarding</h2>

<p>Let's have a look at this using Grafana.</p>

<p>Run grafana:</p>

<pre>
$ docker run -d -p 3000:3000 grafana/grafana
</pre>

<p>Then connect to it (<a href="http://localhost:3000/">maybe?</a>), log in and go to the settings to add a data source. Select <em>Graphite</em>, and set the URL as <code>http://the.ip.of.the.machine:29380/</code>, selecting to access it from the browser (the simplest configuration). Click and voilà.</p>

<p>Now create a dashboard, add a graph, and you should be able to graph these tables as if they were from graphite (with the exception that functions are not supported yet. For instance, you could graph <code>hosts.memory.*</code>.</p>

<h2>Alerting On Low Memory</h2>

<p>Ramen only ways to notify the external world of some condition is the <code>NOTIFY</code> clause that takes an HTTP URL as a parameter and that will get (as in <code>HTTP GET</code>) that URL each time the function commits a tuple.</p>

<p>As a simple example, let's say we want to be alerted whenever the "used" memory grows beyond 50% of the total.</p>

<p>We can use the <code>NOTIFY</code> keyword to reach out to some imaginary alerting service. Let's add to <code>hosts.ramen</code> an function named <code>memory_alert</code>, defined like this:</p>

<pre>
DEFINE memory_alert AS
  FROM memory
  SELECT
    time, host,
    free + used + cached + buffered + slab AS total,
    free * 100 / total AS used_ratio
  GROUP BY host
  COMMIT AFTER used_ratio &gt; 50
  NOTIFY "http://imaginary-alerting.com/notify?title=RAM%20is%20low%20on%20${host}&time=${time}&text=Memory%20on%20${host}%20is%20filled%20up%20to%20${used_ratio}%25"
  EVENT STARTING AT time WITH DURATION 30;
</pre>

<p>Notice that we can reuse the field <code>total</code> after it has been defined in the select clause, which comes rather handy when building complex values as it allows to name intermediary result.</p>

<p>NOTE: Should you not want such an intermediary result to be actually part of the output tuple, you would have to prepend its name with an underscore ; as a corollary, any field which name starts with an underscore will not appear in the output. Those fields are called "private fields".</p>

<p>Notice the <code>NOTIFY</code> clause: it just needs an URL within which actual field values can be inserted.</p>

<p>Let's compile that new program.</p>

</pre>
In function memory_alert: comparison (>) must not be nullable
</pre>

<p>Wait, what? Now the compiler is complaining that <code>used_ratio</code> can be NULL?  Have you noticed that all of our memory values could be NULL? That's typically the kind of surprise Ramen type system is designed to catch early.</p>

<p>Of course, collectd "type_instance" field is nullable, so is the <code>IF type_instance = "whatever"</code> conditional, so are each of the averaged memory volumes. We could wrap each use of type_instance into a <code>COALESCE</code> function but that would be tedious. Rather, let's put in practice our new knowledge about private fields. Turn the memory function into:</p>

<pre>
DEFINE memory AS
  SELECT
    MIN time AS time,
    host,
    COALESCE (type_instance, "") AS _type,
    AVG (IF _type = "free" THEN value) AS free,
    AVG (IF _type = "used" THEN value) AS used,
    AVG (IF _type = "cached" THEN value) AS cached,
    AVG (IF _type = "buffered" THEN value) AS buffered,
    AVG (IF _type LIKE "slab%" THEN value) AS slab
  FROM demo/collectd
  WHERE COALESCE (plugin = "memory", false)
  GROUP BY host, time // 30
  COMMIT AFTER in.time &gt; out.time + 30
  EVENT STARTING AT time WITH DURATION 30;
</pre>

<p>...and then everything should compile and run.</p>

<p>What will happen whenever the memory usage ratio hit the threshold is that the imaginary alerting system will receive a notification from Ramen.  It would be nice if we could also tell it when the memory usage goes back below the threshold.  Let's add a boolean <code>firing</code> parameter for that purpose.</p>

<p>Edit the "memory alert" function into this:</p>

<pre>
DEFINE memory_alert AS
  FROM hosts/memory
  SELECT
    time, host,
    free + used + cached + buffered + slab AS total,
    free * 100 / total AS used_ratio,
    used_ratio &gt; 50 AS firing
  GROUP BY host
  COMMIT AND KEEP ALL WHEN COALESCE (out.firing <> previous.firing, false)
  NOTIFY "http://imaginary-alerting.com/notify?firing=${firing}&title=RAM%20is%20low%20on%20${host}&time=${time}&text=Memory%20on%20${host}%20is%20filled%20up%20to%20${used_ratio}%25"
  EVENT STARTING AT time WITH DURATION 30;
</pre>

<p>There should be little surprise but for the commit clause.</p>

<p>There we see the "previous" tuple for the first time. It's similar to the "out" tuple, but the "out" tuple refers to the tuple that's actualy in construction (the one that would be emitted right now if that commit clauses says so) whereas the "previous" tuple refers to the former value of that tuple (the one that would have been emitted the last time we added something to that aggregation group, should the commit clause said so). The out tuple always have a value, but the previous one not necessarily: indeed, when this group have just been created there is no "last time". In that case, all the previous tuple fields would be NULL (regardless of their normal nullability). Therefore the <code>COALESCE</code>.</p>

<p>What that <code>COMMIT AND KEEP ALL</code> does is to instruct Ramen not to delete the group when the tuple is output (the default behavior is to discard the group once it's been output).  <code>KEEP ALL</code> means that the group should stay untouched, as if it hasn't been output at all. Otherwise we would loose the memory of what was the last output tuple for this host (next time we hear about that host, a new group would be created and <code>previous.firing</code> would be NULL). In contrast, <code>KEEP ALL</code> will never delete the groups, so we will have as many groups as we have hosts to save their last firing state, which is reasonable.</p>

<p>So here we send a notification only when the value of <code>firing</code> changes.  Note that in production you likely want to use an hysteresis. There are several ways to do that, but let's leave it as an exercise.</p>

<h2>Monitoring Netflows</h2>

<p>Let's now turn into netflows.</p>

<p>Have a look at the output of the <code>demo/netflow</code> function and, armed with <a href="https://www.cisco.com/c/en/us/td/docs/net_mgmt/netflow_collection_engine/3-6/user/guide/format.html#wp1006186">netflow format reference</a>, see if you can make sense of that data.</p>

<p>If you are not already familiar with this, then you just have to know that netflows are bytes, packets and flag counts for each "flow" defined roughly as the IP socket pair (ip protocol, addresses and ports), and a "route" inside the switch from the inbound to outbound interface. Switches will send those records regularly every few minutes so that we know the volume of the traffic per socket, that we can aggregate per subnets or per switch interfaces, and so on.</p>

<p>What we are ultimately interested in, for monitoring purpose, will typically be:</p>

<ul>
<li>Is any switch interface close to saturation?</li>
<li>Is the total traffic from/to a given subnet within the expected range?</li>
<li>Is a link down?</li>
<li>Are there any traffic from a given subnet to another given subnet for a given port (for instance, from internal to external port 25)?</li>
<li>Is there some DDoS going on? Or some other malicious pattern?</li>
</ul>

<p>We will see how to compute some of those.</p>

<h3>Per interface traffic</h3>

<p>Let's start by aggregating all traffic per switch interfaces.</p>

<p>Netflow has 3 fields of interest here: "source", which is the IP address of the netflow emitter (say, a switch), and "in_iface" and "out_iface", which identifies the interfaces from which the flow entered and exited the switch.</p>

<p>To build a per interface aggregate view we therefore have to split each flow into two, saying that the traffic that have been received on interface X and emitted on interface Y count as traffic for interface X and traffic for interface Y, counting indifferently incoming and outgoing traffic.</p>

<p>Let's therefore create a new program file named "traffic.ramen", with two functions that we could name respectively "inbound" and "outbound":</p>

<pre>
DEFINE inbound AS
  SELECT source, first, last, bytes, packets, in_iface AS iface
  FROM demo/netflow;
</pre>

<p>...and...</p>

<pre>
DEFINE outbound AS
  SELECT source, first, last, bytes, packets, out_iface AS iface
  FROM demo/netflow;
</pre>

<p>Both will read the netflows and output flows with a single <code>iface</code> field for both incoming and outgoing traffic. We can then read from both those functions and have a single view of all traffic going through a given interface (in or out).</p>

<p>Let's jut do that. In an function named "total", grouping by interface (that is, by <code>source</code> and <code>iface</code>) and aggregating the traffic (<code>bytes</code> and <code>packets</code>), until enough time has passed (300 seconds in this example):</p>

<pre>
DEFINE total AS
  FROM inbound, outbound
  SELECT
    source, iface,
    min first AS first, max last AS last,
    sum bytes AS bytes, sum packets AS packets
  GROUP BY source, iface
  COMMIT AFTER out.last - out.first &gt; 300
  EVENT STARTING AT first AND STOPPING AT last;
</pre>

<p>It might be the first time you see a FROM clause with more that one function.  You are allowed to read from several functions as long as all these functions output (at least) all the fields that your function needs (with the same type).</p>

<p>You could plot the "bytes" or "packets" field of this function to get the total traffic reaching any interface.</p>

<p>For convenience let's rather compute the number of packets and bytes _per seconds_ instead:</p>

<pre>
DEFINE total AS
  FROM inbound, outbound
  SELECT
    source, iface,
    min first AS first, max last AS last,
    sum bytes / (out.last - out.first) AS bytes_per_secs,
    sum packets / (out.last - out.first) AS packets_per_secs
  GROUP BY source, iface
  COMMIT AFTER out.last - out.first &gt; 300
  EVENT STARTING AT first AND STOPPING AT last;
</pre>

<p>Notice the prefix in <code>out.first</code> and <code>out.last</code> to identify the computed <code>first</code> and <code>last</code> from the output tuple ; without the prefix Ramen would have used the <code>first</code> and <code>last</code> fields from the input tuple instead of the result of the <code>min</code>/<code>max</code> aggregation functions, as the input tuple (<code>in</code>) is the default when the same field can have several origins.</p>

<p>Now that we have the bandwidth per interface every 5 minutes, it is easy to signal when the traffic is outside the expected bounds for too long.  But we can do a bit better. Let's append this to <code>traffic.ramen</code>:</p>

<pre>
DEFINE traffic_alert AS
  FROM total
  SELECT
    source, iface,
    (last - first) / 2 AS time,
    bytes_per_secs,
    5-ma locally (bytes_per_secs &lt; 100 OR bytes_per_secs &gt; 8e3) &gt;= 4 AS firing
  GROUP BY source, iface
  COMMIT AND KEEP ALL WHEN COALESCE (out.firing <> previous.firing, false)
  NOTIFY "http://imaginary-alerting.com/notify?firing=${firing}&title=Traffic%20on%20${source}%2F${iface}&time=${time}";
</pre>

<p>Notice the definition of firing: instead of merely fire whenever the average traffic over 5 minutes is outside the range, we do this enigmatic "5-ma" dance. "5-ma" is actually a function that performs a moving average, ie. the average of the last 5 values. In order to average boolean values those will be converted into floats (1 for true and 0 for false as usual). So if the average of the last 5 values is above or equal to 4 that means at least 4 of the latests 5 values were true. Therefore, at the expense of a bit more latency, we can skip over a flapping metric.</p>

<p>NOTE: of course there are also functions for <code>2-ma</code>, <code>3-ma</code> and so on. This is just syntactic sugar.</p>

<p>The next enigmatic bit is the "locally" keyword. This is a function modifier that means that instead of storing it's temporary state globally the "5-ma" function should have one such state per group ; in other words, instead of computing the average over the last 5 incoming tuples regardless of their key, it should compute the average over the last 5 tuples aggregated into the same group.  Some functions default to having a global context while some default to have a local context. If unsure, add either <code>locally</code> or <code>globally</code> after any stateful function.</p>

<h3>Alerting on link down</h3>

<p>Alerting on link down might seems easy - isn't it a special case of the above, when we test for <code>bytes_per_secs = 0</code> ?  This won't work for a very interesting reason: When there is no traffic at all on an interface, switches will not send a netflow message with zero valued counters. Instead they will not send anything at all, thus stalling the stream processor. To detect link down, therefore, we need some timeout.</p>

<p>Assuming we will still receive some netflows even when one link is down, the easier solution seems to compare the last time of each group with the latest input tuple time, each time a tuple is received, like so:</p>

<pre>
DEFINE link_down_alert AS
  FROM traffic/total
  SELECT source, iface, max last
  GROUP BY source, iface
  COMMIT AND KEEP ALL WHEN in.last - max_last &gt; 300
  NOTIFY "http://imaginary-alerting.com/notify?text=link%20${source}%2F${iface}%20is%20down";
</pre>

<p>...and you have it!</p>

<p>If you have many interfaces, comparing each group with each incoming netflow might not be very efficient though. Maybe Ramen should provide a proper timeout feature, either based on actual wall clock time or on event time?</p>

<p>You should now be able to survive given only the <a href="language_reference.html">language reference manual</a>.</p>

<? include 'footer.php' ?>
