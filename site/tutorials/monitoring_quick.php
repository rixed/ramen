<? include 'header.php' ?>

<h1>Network Monitoring in 15 minutes</h1>

<p>I'm assuming you have a network with some hosts to monitor, and that you can capture the traffic on that network (or at some gateway) and run some standard open source probes on the hosts.</p>

<p>At worse, if you can do that at least on a single linux laptop then that should be enough to figure out what Ramen can do.</p>

<h2>A shared directory</h2>

<p>Ramen stores many things on disk: first and foremost of course the data, that are serialized on large sequential files with some accompanying index files. Also, each and every worker snapshots its internal state regularly. But more importantly for us now, Ramen read and writes many small human readable files for its configuration.</p>

<p>All of these files are stored below a single path, that is going to be <code>/ramen</code> in the docker image.</p>

<p>The simplest is to create an empty directory somewhere and mount it as <code>/ramen</code> in the image, so not only can we read and write any configuration file we please easily but also can we provision enough room for the archived data.</p>

<p>So let's create for instance <code>$HOME/ramen</code> on the host:</p>

<pre>
$ mkdir $HOME/ramen
</pre>

<p>We will store the source code for the Ramen operations in a subdirectory:</p>

<pre>
$ mkdir $HOME/ramen/src
</pre>

<p>We are also going to feed Ramen with many CSV files from <a href="https://github.com/rixed/junkie">a network sniffer called junkie</a> that we will make accessible in another subdirectory:</p>

<pre>
$ mkdir -p $HOME/ramen/junkie/csv
</pre>

<p>We still have to configure Ramen, but we can still launch it and let it run in the background:</p>

<pre>
$ docker run --name=ramen -v $HOME/ramen:/ramen -p 25826:25826/udp -p 29380:29380/tcp rixed/ramen
</pre>

<p>Note: the opened ports are for receiving collectd stats and an http server to retrieve data later.</p>

<h2>Network traffic analysis</h2>

<p>We are going to use <a href="https://github.com/rixed/junkie">the junkie DPI tool</a> to capture and analyze the traffic. Junkie is a beast of its own and we are not going to look too deep into it here. Suffice to say it uses libpcap to capture traffic and then performs a stateful inspection of it, and is highly configurable.<p>

<p>For this demo, we are going to asks junkie to dump all the transactions it can detect, in a timely fashion, into timestamped CSV files.</p>

<p>Download that configuration from <a href="https://raw.githubusercontent.com/rixed/junkie/master/examples/dump-transactions">the examples</a> and store it into <code>$HOME/ramen/junkie</code>:</p>

<pre>
$ wget -P $HOME/ramen/junkie \
       https://raw.githubusercontent.com/rixed/junkie/master/examples/dump-transactions
</pre>

<p>This configuration take a few parameters from environment variables:</p>

<ul>
<li><code>CSV_DIR</code>: where to write the CSV files - so for us it must be <code>$HOME/ramen/junkie/csv</code>;</li>
<li><code>CSV_DURATION</code>: the duration of each CSV file, in seconds - so let's say <code>60</code>;
<li><code>LOG_DIR</code>: where to write log file - let's say <code>/tmp</code>;</li>
<li><code>SNIFF_IFACES</code>: either the name of an interface (or a pattern matching several interface names, using the <a href="https://www.gnu.org/software/libc/manual/html_node/Regular-Expressions.html#Regular-Expressions">GNU libc standard regular expression syntax</a>) or the name of a pcap file;</li>
<li><code>SNIFF_CAPTURE_FILTER</code>: optionally, a BPF capture filter following <a href="https://www.tcpdump.org/manpages/pcap-filter.7.html">the standard pcap filter syntax</a>.</li>
</ul>

<p>So let's run junkie on this configuration file, with the proper parameters:</p>

<pre>
$ docker run --name=junkie --rm -v $HOME/ramen/junkie:/junkie --network host \
  -e CSV_DIR=/junkie/csv \
  -e LOG_DIR=/tmp \
  -e CSV_DURATION=60 \
  -e SNIFF_IFACES='v?eth.*' \
  rixed/junkie junkie -c /junkie/dump-transactions
</pre>

<p>You should then, after less than a few seconds, see a few CSV files created, and soon accumulating, in <code>$HOME/ramen/junkie/csv</code>. The more traffic you throw at it the better.</p>

<h2>Injecting the configuration</h2>

<p>So now we must instruct Ramen to read (and delete) all those CSV files that are now filling up the <code>csv</code> subdirectory.</p>

<p>Let's download again the configuration from <a href="https://raw.githubusercontent.com/rixed/junkie/master/examples/transactions.ramen">the same place</a>:</p>

<pre>
$ wget -P $HOME/ramen/src \
       https://raw.githubusercontent.com/rixed/junkie/master/examples/transactions.ramen
</pre>

<p>This configuration, which does little more than describing the schema of those CSV files, must still be compiled into a native code executable:</p>

<pre>
$ alias ramen="docker exec ramen ramen"
$ ramen compile -L src src/transactions.ramen
</pre>

<p>Which will produce <code>$HOME/ramen/src/transaction.x</code>.</p>

<p>If you look at the beginning of this <code>transactions.ramen</code> program you will see:</p>

<pre>
PARAMETER csv_dir DEFAULTS TO "/tmp";
</pre>

<p>Meaning that the program is parameterized with <code>csv_dir</code> which is supposed to give the location of the CSV files, for us <code>/ramen/junkie/csv</code>. So let's run that program with the proper <code>csv_dir</code>:</p>

<pre>
$ ramen run -p 'csv_dir="/ramen/junkie/csv"' src/transactions.x
</pre>

<p>(beware of the quotes: ramen must see that <code>/ramen/junkie/csv</code> is a string!)</p>

<p>Now we should see that this program is indeed running:</p>

<pre>
$ ramen ps --short --pretty
program      | parameters                  | #in  | #selected | #out  | #groups | CPU   | wait in      | wait out | heap     | max heap | volume in | volume out |
transactions | csv_dir="/ramen/junkie/csv" | 2018 |      2018 | 25163 |      12 | 1.516 | 60.180471817 |        0 | 28704768 | 35389440 |    231056 |     231056 |
</pre>

<p>...or for a detailed view per individual functions:</p>

<pre>
$ ramen ps --pretty
operation                     | #in  | #selected | #out  | #groups | last out            | min event time      | max event time      | CPU   | wait in       | wait out | heap    | max heap | volume in | volume out | startup time        | #parents | #children | signature                        |
transactions/dns              |  n/a |       n/a |     4 |     n/a | 2018-12-24T15:58:44 | 2018-12-24T15:46:53 | 2018-12-24T15:46:53 | 0.052 |  0            |        0 | 3932160 |  3932160 |         0 |          0 | 2018-12-24T15:58:44 |        0 |         0 | ec8864237bc33951888d392156793f66 |
transactions/flow             |  n/a |       n/a | 23019 |     n/a | 2018-12-24T15:58:45 | 2018-12-24T14:06:39 | 2018-12-24T15:51:41 | 1.176 |  0            |        0 |  589824 |  3932160 |         0 |          0 | 2018-12-24T15:58:44 |        0 |         0 | 31d574fd9d206c4808d7d986a3cb39ae |
transactions/labelled_traffic | 1009 |      1009 |  1009 |       0 | 2018-12-24T15:58:44 | 2018-12-24T14:06:37 | 2018-12-24T15:50:36 | 0.076 | 60.1013638382 |        0 | 3932160 |  3932160 |    100276 |     130780 | 2018-12-24T15:58:44 |        1 |         1 | 3c72eb6a9ae956ba1974392c0f495db0 |
transactions/tcp              |  n/a |       n/a |    14 |     n/a | 2018-12-24T15:58:44 | 2018-12-24T14:07:04 | 2018-12-24T15:51:38 | 0.044 |  0            |        0 | 3932160 |  3932160 |         0 |          0 | 2018-12-24T15:58:44 |        0 |         0 | e31025741efccfb30d11886cdb310767 |
transactions/tls              |  n/a |       n/a |     1 |     n/a | 2018-12-24T15:58:44 | 2018-12-24T15:46:53 | 2018-12-24T15:46:53 | 0.052 |  0            |        0 | 3932160 |  3932160 |         0 |          0 | 2018-12-24T15:58:44 |        0 |         0 | 4a4d66b576a0f2f57b06b853aee85c74 |
transactions/traffic          |  n/a |       n/a |  1009 |     n/a | 2018-12-24T15:58:44 | 2018-12-24T14:06:37 | 2018-12-24T15:50:36 | 0.104 |  0            |        0 |  589824 |  3932160 |         0 |     100276 | 2018-12-24T15:58:44 |        0 |         1 | bfd2b8ef3cd8f9ed2f89c001c906d0eb |
transactions/volumetry        | 1009 |      1009 |   119 |      12 | 2018-12-24T15:58:44 | 2018-12-24T14:06:00 | 2018-12-24T15:49:00 | 0.068 | 60.0791079788 |        0 | 3932160 |  3932160 |    130780 |          0 | 2018-12-24T15:58:44 |        1 |         0 | a3784c8998bb3f13c7e0473dc90f5a6d |
transactions/web              |  n/a |       n/a |     0 |     n/a | n/a                 | n/a                 | n/a                 | 0.052 |  0            |        0 | 3932160 |  3932160 |         0 |          0 | 2018-12-24T15:58:44 |        0 |         0 | 56d73b4f97e74ef326f6b64063cd0512 |
transactions/x509             |  n/a |       n/a |     2 |     n/a | 2018-12-24T15:58:44 | 2018-12-24T15:46:53 | 2018-12-24T15:46:53 | 0.056 |  0            |        0 | 3932160 |  3932160 |         0 |          0 | 2018-12-24T15:58:44 |        0 |         0 | 5eb919587dcf41a91ab0786dc443cbda |
</pre>

<p>...and indeed <code>ps</code> would confirm that there is one process per function.</p>

<p>Let's peek at some output, for instance:</p>

<pre>
$ ramen tail --header --follow transactions/labelled_traffic
vlan,stop,start,src_port,src_mac,src_ip,packets,ip_proto,ip_payload,eth_proto,eth_payload,eth_mtu,dst_port,dst_mac,dst_ip,device,label
n/a,1545668614.6,1545668614.6,51522,02:42:76:e3:93:43,172.17.0.1,1,"UDP",1312,"IPv4",1332,1332,25826,02:42:ac:11:00:02,172.17.0.2,2,"dev:2,mac:02:42:76:e3:93:43,UDP/ip:172.17.0.1 - dev:2,mac:02:42:ac:11:00:02,UDP/ip:172.17.0.2, port:25826"
n/a,1545668624.59,1545668614.6,46993,02:42:76:e3:93:43,172.17.0.1,2,"UDP",2646,"IPv4",2686,1362,25826,02:42:ac:11:00:02,172.17.0.2,2,"dev:2,mac:02:42:76:e3:93:43,UDP/ip:172.17.0.1 - dev:2,mac:02:42:ac:11:00:02,UDP/ip:172.17.0.2, port:25826"
n/a,1545668615.02,1545668615.02,13349,00:31:46:0d:22:dd,42.114.34.164,1,"TCP",32,"IPv4",52,52,445,00:26:88:75:c1:0f,46.4.136.34,0,"other"
n/a,1545668624.64,1545668615.64,50105,00:31:46:0d:22:dd,116.233.101.186,3,"TCP",92,"IPv4",152,52,445,14:da:e9:b3:96:4e,46.4.104.236,0,"other"
n/a,1545668615.81,1545668615.81,53375,00:31:46:0d:22:dd,180.97.4.18,1,"TCP",20,"IPv4",46,46,1433,6c:62:6d:d9:08:50,46.4.118.132,0,"other"
n/a,1545668616.53,1545668616.53,46229,00:31:46:0d:22:dd,109.248.9.14,1,"TCP",20,"IPv4",46,46,29405,6c:62:6d:46:aa:59,46.4.125.34,0,"other"
n/a,1545668618.17,1545668618.17,13747,00:31:46:0d:22:dd,42.114.34.164,1,"TCP",32,"IPv4",52,52,445,6c:62:6d:d9:0b:1e,46.4.136.37,0,"other"
n/a,1545668618.74,1545668618.74,53,00:31:46:0d:22:dd,120.52.19.143,1,"UDP",79,"IPv4",99,99,21404,6c:62:6d:d9:08:50,46.4.118.133,0,"dev:0,mac:00:31:46:0d:22:dd,UDP/ip:120.52.19.143 - dev:0,mac:6c:62:6d:d9:08:50,UDP/ip:46.4.118.133, port:53"
n/a,1545668618.76,1545668618.76,53,00:31:46:0d:22:dd,120.52.19.143,1,"UDP",79,"IPv4",99,99,39942,6c:62:6d:d9:08:50,46.4.118.133,0,"dev:0,mac:00:31:46:0d:22:dd,UDP/ip:120.52.19.143 - dev:0,mac:6c:62:6d:d9:08:50,UDP/ip:46.4.118.133, port:53"
n/a,1545668619.21,1545668619.21,9102,00:31:46:0d:22:dd,42.114.34.164,1,"TCP",32,"IPv4",52,52,445,6c:62:6d:d9:0b:1e,46.4.136.38,0,"other"
...
</pre>

<p>If all is good so far, why not mix in some host-centric data?</p>

<h2>Host stats collection with collectd</h2>

<p><a href="https://collectd.org/">Collectd</a> is a simple and fast statistics collector that can easily be installed on any Linux server regardless of the distribution, but docker is more convenient for this demo.</p>

<p>Ramen can listen for collectd protocol and turn all incoming collectd message into a tuple. Let's, for instance, do some simple memory monitoring with <a href="https://raw.githubusercontent.com/rixed/ramen/master/examples/programs/monitoring/hosts.ramen">this example configuration</a>, that we have to compile and run as before:</p>

<pre>
$ wget -P $HOME/ramen/src \
       https://raw.githubusercontent.com/rixed/ramen/master/examples/programs/monitoring/hosts.ramen
$ ramen compile -L src src/hosts.ramen
$ ramen run src/hosts.x
</pre>

<p>Ramen should now have a worker running, named <code>hosts/collectd</code>, that listen to port 25826 for incoming any collectd messages.</p>

<p>We are going to use <a href="https://github.com/MichielDeMey/docker-collectd">this</a> collectd docker image as it forward statistics using the collectd native format, which is what Ramen expect. So connect on some host (or your laptop) and run:</p>

<pre>
$ docker run --name collectd --rm -h $(hostname) -e COLLECTD_HOST=$IP -e COLLECTD_PORT=25826 michieldemey/collectd
</pre>

<p>Where $IP must be the IP address on which Ramen is listening on port 25826. 127.0.0.1 should do fine if you are running everything on your laptop.</p>

<p>If all goes well, you should not wait too long before seeing the first collectd messages:</p>

<pre>
$ ramen tail --header --follow hosts/collectd
"foobar.org",1545881860.12,"irq",n/a,"irq","97",735764078,n/a,n/a,n/a,n/a
"foobar.org",1545881860.12,"irq",n/a,"irq","94",0,n/a,n/a,n/a,n/a
"foobar.org",1545881860.12,"irq",n/a,"irq","NMI",0,n/a,n/a,n/a,n/a
"foobar.org",1545881860.12,"irq",n/a,"irq","99",0,n/a,n/a,n/a,n/a
"foobar.org",1545881860.12,"irq",n/a,"irq","LOC",0,n/a,n/a,n/a,n/a
...
</pre>

<h2>Dashboarding with Grafana</h2>

<p>Conveniently, Ramen can impersonate graphite (at least well enough to perform some basic timeseries representation with Grafana).</p>

<p>So now let's run Grafana and build a small dashboard:</p>

<pre>
$ docker run --name grafana -d -p 3000:3000 grafana/grafana
</pre>

<p>Login and add a graphite data source, pointing it at that same <code>http://$IP:29380</code> with the same <code>$IP</code> as before. No need to pick a version. Select to access it from the browser (the simplest configuration) or adjust the docker command line accordingly.</p>

<p>Set this data source as default.</p>

<p>Now create a new dashboard, with a new chart. You should be able to see all fields of all defined functions, and graph them. Compose a small and nice dashboard, such as this one:</p>

<img class="screenshot" src="tutorials/grafana.jpeg"/>

<h2>Alerting</h2>

<p>Dashboarding is pretty, but are only useful for two things:</p>

<ol>
<li>Setting up alerts;</li>
<li>Assessing a situation when an alert fires.</li>
</ol>

<p>So let's say we want to be alerted whenever the 95th percentile of the HTTP response time over the last 11 mins is above some threshold. First, add into your dashboard a chart of the number of http requests per status: <code>junkie.transactions.web.*.*.GET.200.resp_time</code>. Hopefully you have some data in there, or the rest of this demo is going to feel rather boring.</p>

<p>In theory, with a typical Graphite+Grafana based monitoring stack, you would configure Grafana so that it would request that percentile from Graphite every minute or so, and then configure an alert. So <em>every minute</em> Graphite would have to retrieve the past 5 minutes of that time series and compute the percentile.</p>

<p>That would not be a very wise thing to do if you had many such alerts.</p>

<p>With a stream processor things are different: whatever value you want to alert upon is computed on the fly, once. So let's add this function in Ramen:</p>

<pre>
$ cat &gt; $HOME/ramen/src/web_alerts.ramen &lt;&lt;EOF

DEFINE response_time AS
  FROM junkie/transactions/web
  SELECT
    time, resp_time, device, vlan,
    95th percentile (past 5min of resp_time) AS resp_time_95th;
EOF
$ ramen compile src/web_alerts.ramen
$ ramen run src/web_alerts.x
</pre>

<p>This would keep a sliding window of the last 5 minutes of the response times, and each time a new value gets in compute and output the percentile.</p>

<p>So now you can use Grafana threshold based alerting on <code>resp_time_95th</code>.</p>

<p>But that still requires that Grafana polls every minute, for every alert. Why can't Ramen directly push a notification as soon as, and only when, <code>reps_time_95th</code> is found to be above the threshold? This is doable, as Ramen comes with its own alerting service.</p>

<p>Let's edit <code>$HOME/ramen/src/web_alerts.ramen</code> and add this <code>alert_on_response_time</code> function:</p>

<pre>
$ cat &gt;&gt; $HOME/ramen/src/web_alerts.ramen &lt;&lt;EOF

DEFINE alert_on_response_time AS
  FROM response_time
  SELECT *,
    -- No resp_time =&gt; not firing:
    COALESCE(resp_time_95th &gt; 1s, false) AS firing
  NOTIFY "web resp time" AFTER firing &lt;&gt; COALESCE(previous.firing, false);
EOF
$ ramen compile src/web_alerts.ramen
$ ramen kill web_alerts
Killed 1 program
$ ramen run src/web_alerts.x
</pre>

<p>So now the Ramen <em>alerter</em> is going to be notified each time the metric goes above or below the threshold. What will happen next depends on its configuration. Let's for instance create this configuration in <code>$HOME/ramen/alerter.conf</code>:</p>

<pre>
{
    teams = [
      {
        contacts = [
           ViaSysLog "Alert ${name} firing=${firing}: ${resp_time_95th} seconds"
        ]
      }
    ]
}
</pre>

<p>Now the alerter will emit a log whenever the threshold is crossed; you could of course configure it to do more sophisticated things such as sending an email or contacting a paging service.</p>

<h2>Conclusion</h2>

<p>Of course nobody in his right mind believed the objective of this tutorial was to setup a proper monitoring stack in 15 minutes. But it is at least now hopefully clearer what Ramen has been designed to do and how it differs from other monitoring systems.</p>

<p>To sum it up:</p>

<p>Pros:</p>
<ul>
<ol>A single server running Ramen can process a large number of incoming messages in an efficient way;</ol>
<ol>Its data manipulation language is similar to SQL and offers all data types and functions required for monitoring;</ol>
<ol>It comes pre-equipped with a flexible alerting service;</ol>
</ul>

<p>Cons:</p>
<ul>
<ol>It is not designed to run at a scale where hardware cannot be relied upon any more;</ol>
<ol>It is evolving quickly and its API is changing on a weekly basis;</ol>
<ol>It is still in its very infancy so unfit for production;</ol>
</ul>

<? include 'footer.php' ?>
