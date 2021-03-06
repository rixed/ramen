<? include 'header.php' ?>

<h1>Benchmark against KafkaSQL</h1>
<p class="date">2019-02-10</p>

<h2>The Challenge</h2>

<p>Ramen is designed from scratch for small scale stream processing, because there is no such tool in that space. An obvious rebuttal that I've heard and formulated myself is: why not use a stream processor designed for large scale, and "scale it down" to just a few nodes? Surely what's optimised for large scale can perform acceptably good on small scale.</p>

<p>I know that it is not usually true, because scale change the design a lot. But at some point I needed to check this assertion and provide some data about the practical cost of using a bulldozer to crack a nut. So I went on the market for an off-the-shelf stream processor that could act as an easy replacement for Ramen.</p>

<p>Kafka is popular, can run on a single machine, supports stream processing for a good while and has a SQL-ish interface (KSQL) that one can use without resorting to programming at all; that ticks all the boxes. Popularity is hardly an indicator for quality, and there are certainly many better alternatives that even an outsider like me can spot easily, from Flink to SQLstream, but I wanted to start with the most obvious.</p>

<p>I therefore downloaded and installed confluent ksql <em>v5.1.0</em></p>

<p>The initial idea of the test was to read my usual testing dataset, representing 10 hours of network measurements in about 400k lines times about 80 columns of CSV and compute something short (so as not to spend too much time on this), easy to replicate with any stream processor, and not completely trivial like a word count but rather something we might actually want to do in reality.</p>

<p>For instance, computing the 95th percentile of the round-trip-time for the top 100 triplets (server ip * server port * client ip) by total traffic, per hour. Of course we must avoid grouping by triplets before computing the top, as there can be a very large number of triplets visible at the same time. So the first step is to compute the top 100 by traffic for every hour, and only then to compute the percentile of the RTT.</p>

<p>And to be fair, let’s skip over IPv6 addresses entirely, as Kafka cannot handle data size that big other than with strings; that would give Ramen (which is equipped with dedicated types for network addresses and such) an unfair advantage; the data model would have been different if we had designed it with Kafka in mind.</p>

<h2>Streaming processors without online algorithms?</h2>

<p>… Bummer! There is <a href="https://github.com/confluentinc/ksql/issues/403">no working TOP function in KSQL</a>!</p>

<p>I've always considered top-k to be the starting point of stream processing and the discovery that KSQL was actually not offering this function was a bit of a shock. Why a system lacking support for the most basic of <a href="https://en.wikipedia.org/wiki/Online_algorithm">online algorithm</a> can still be referenced to as a "stream processor"?</p>

<p>Kafka, like Flink, Storm, Spark and the whole family, are designed for distributed computing. Could it be that the well known online algorithms do not generalize easily, or at all, to a distributed world? That would lead to this paradox that large scale distributed stream processors can not process streams any more efficiently than any big distributed OLAP database can. The difference between a distributed OLAP DB (such as <a href="http://impala.apache.org">Impala</a> or <a href="https://hive.apache.org">Hive</a>) would be of little help as soon as anything more complex than projection and stateless functions are involved; ie pretty much right from the start, as in here. Sure, Kafka can act as an efficient cache for recent data, but probably so does <a href="http://kylin.apache.org">Kylin</a>.</p>

<p>For the record, I got curious and looked for the top operation in Flink. <a href="https://github.com/apache/flink/pull/1161/files">Here is one</a>, non exactly user friendly and not online.</p>

<p>But for now, the problem is: without a top it’s hard to perform any meaningful cardinality reduction.</p>

<p>So let’s devise a simpler challenge.  Let’s say we want to group by server port and aggregate per minute, and compute the sum of traffic and this 95th percentile of RTT. With maximum 65536 server ports we do not need a top operation any more.</p>

<p>… Bummer again! Probably for the same reason, there is no percentile function in KSQL!</p>

<p>Let’s compute (by hand) a mere average, maybe?</p>

<p>But even that turned out to be not so straightforward. Indeed, in the data the information about RTT come in 4 columns: the sum of all RTT measured from clients, the number of such measurements from clients, and all the same from the servers. The average is therefore:</p>

<ul>
<li><code>(rtt_client_sum + rtt_server_sum) / (rtt_client_count + rtt_server_count)</code>, when the count is not zero, and</li>
<li><code>NULL</code>, when the count is zero.</li>
</ul>

<p>Which translate into:</p>

<pre>
  CASE
    WHEN rtt_client_count + rtt_server_count > 0 THEN
      (rtt_client_sum + rtt_server_sum) /
      (rtt_client_count + rtt_server_count)
    ELSE NULL
  END
</pre>

<p>The <code>ELSE NULL</code> being of course optional as that's the default.</p>

<p>And we want this value, grouped by server port:</p>

<code>
  CASE
    WHEN SUM(rtt_client_count + rtt_server_count) > 0 THEN
      SUM(rtt_client_sum + rtt_server_sum) /
      SUM(rtt_client_count + rtt_server_count)
  END
</code>

<p>Ramen allows this more straightforward form:</p>

<code>
    SUM(rtt_count_client + rtt_count_server) AS rtt_count,
    CASE WHEN rtt_count > 0 THEN
           SUM(rtt_sum_client + rtt_sum_server) / rtt_count
    END
</code>

<p>To the best of my knowledge, most SQL databases will skip the inner division when the condition is false. Not KSQL, which will throw division by zero exceptions in the server log.</p>

<p>At this point I was happy with an ugly hack, so ended up adding a 1 to the denominator, and recording in an additional field that the average should indeed be NULL.</p>

<p>Here is what I ended up with:</p>

<pre>
CREATE TABLE top_tcp AS \
  SELECT \
    port_server, \
    SUM(traffic_bytes_client + traffic_bytes_server) AS traffic, \
    SUM(rtt_sum_client + rtt_sum_server) / \
    (1 + SUM(rtt_count_client + rtt_count_server)) AS avg_rtt, \
    SUM(rtt_count_client + rtt_count_server) = 0 AS was_zero \
  FROM tcp \
  WINDOW TUMBLING (SIZE 1 MINUTE) \
  WHERE ip4_client IS NOT NULL \
  GROUP BY port_server;
</pre>

<p>And finally I was able to throw those 400k tuples at it and measure the machine behavior with the excellent <a href="https://www.atoptool.nl/">atop tool</a>:

<script id="asciicast-WN0Nyy5UXBDiFUmAxxXeQUvB1" src="https://asciinema.org/a/WN0Nyy5UXBDiFUmAxxXeQUvB1.js" async></script>

<h2>Back into friendly territory</h2>

<p>The Ramen equivalent of the above:</p>

<pre>
DEFINE top_tcp AS
  SELECT
    min capture_begin AS capture_begin,
    port_server,
    SUM(traffic_bytes_client + traffic_bytes_server) AS traffic,
    SUM(rtt_count_client + rtt_count_server) AS rtt_count,
    CASE WHEN rtt_count > 0 THEN
           SUM(rtt_sum_client + rtt_sum_server) / rtt_count
    END AS avg_rtt
  FROM tcp
  WHERE ip4_client IS NOT NULL
  GROUP BY port_server, capture_begin // 60_000_000
  COMMIT AFTER
    in.capture_begin > out.capture_begin + 80_000_000;
</pre>

<p>Notice here windowing is part of the group-by. This is conceptually simpler and more flexible, but would greatly benefit from at least some syntactic sugar to automate basic cases of windowing like this.</p>

<p>Similarly, I run this and measured with atop:</p>

<script id="asciicast-qNuW69q7dyfP7wvImX5qD3f9R" src="https://asciinema.org/a/qNuW69q7dyfP7wvImX5qD3f9R.js" async></script>

<h2>Comparing user experience</h2>

<h3>Installation</h3>

<p>Ramen installation process is a joke. Even testing given the supplied <a href="https://cloud.docker.com/repository/docker/rixed/ramen">docker image</a> could occupy an engineer for an afternoon. Let's rather not talk about it.</p>

<p>Confluence installation process, though, works quite reliably thanks to Java. I was a bit annoyed at first by the download dialog that insisted to have an email address and than pushed the 500MiB tarball into my laptop from where I had to transfer it into the machine I intended to run the tests into.</p>

<p>But overall, java undeniably make installing software easier.</p>

<h3>Creating operations</h3>

<p>If it was not already obvious from the above, let me restate that KSQL was a huge disappointment with regard to data manipulation abilities. Ramen is comparatively much more useful. Of course this is due to the one-man-in-a-garage methodology that defeats any other methodology in use in the industry; for speed at least if not for robustness.</p>

<p>Despite this, Ramen language is clearly too complicated around window manipulation. Where the usual tumbling/hopping/sliding windows are easy to grasp, Ramen manual management of windowing is a pain. Honestly, it's been useful only in a couple of occasions where simpler windowing semantic would not do the job. I have ideas on how to improve things (further than just following the standard), but it's not quite ready yet.</p>

<h3>Inspection/Monitoring</h3>

<p>KSQL management and monitoring web GUI shines by it's clarity and simplicity. I resorted to the CLI though because the error reporting from the GUI was not sufficient (many times it would only report a timeout when an operation failed, for instance).</p>

<p>But then even in the CLI one has to tail the server logs, as the CLI report little more than cryptic error messages when defining operations, and nothing at all when the operation is running.</p>

<p>Ramen takes another approach to user interaction of course: there the CLI is supposed to be the main (if not the only) way for the user to interact with Ramen. A single command is used to compile, run and stop operations, and to gather statistics about running ones.</p>

<p>In case of errors happening within a running operation though, then it's like for KSQL: one has to look at the server (supervisor) logs to learn about it. It's just a bit simpler in my view because each operation being it's own unix process then one can use normal unix tools like ps, top, etc, to monitor the operations.</p>

<p>Regarding cryptic error messages, Ramen compiler probably wins hands down as it not only report cryptic but also misleading and often time plain bogus errors. All hope is not lost though, as I have plans to solve this that I believe are quite good; but unfortunately had no time to implement yet.</p>

<h2>Comparing resource consumption</h2>

<p>I have to start by admitting that I didn't take the time to isolate entirely those processes; but the machine was mostly idle during the ~30s that lasted the measurements but for a few cron jobs transferring data over the network. Also, I haven't tried to tune either Kafka nor Ramen for the test. Kafka because I wouldn't have been able to, and Ramen because there is not much one can tune anyway. So this comparison only holds for the most basic of single node installation.<p>

<p>The machine itself is a descent server with 8 cores (Xeon E3-1245 V2 at 3.4GHz) with 32GiB of RAM... that Kafka instantly claimed his even before being sent its first message.</p>

<p>The graphs below show the cumulated CPU usage, the Free memory (as defined by atop) and the cumulated number of sectors written to disk, comparing both KSQL and Ramen:</p>

<img src="blog/RamenVsKsql_cpu.svg">

<img src="blog/RamenVsKsql_ram.svg">

<img src="blog/RamenVsKsql_io.svg">

<p>The first thing to notice is that both tests lasted for about the same time, which is basically the time it took to read and uncompress the CSV files.</p>

<p>The other thing that's obvious, but that was obvious as soon as I ran "confluent start", is that Kafka uses <em>a lot</em> of memory. Even before I created a single byte of data, two java processes claimed 10GiB of resident memory each. This is why on the graph above the free memory starts already quite low for the KSQL test. Parsing the 400k incoming messages and bouncing them around the message queue did little to improve things.</p>

<p>But this is probably not as big of a deal as this chart makes it look. I guess this much memory is not really used and with better settings Kafka could leave some room to other processes without changing much to these results.</p>

<p>CPU consumption is more surprising. Of course I am expecting that using accurate static types and ringbuffers to directly pass around messages be more efficient than a message queue; but given Ramen is lacking many optimisations and Kafka has been used and optimised for so long, I was actually expecting it to be faster, instead of requiring that much more CPU.</p>

<p>What setting should be changed to make it 10 times faster? Sure we could use AVRO instead of CSV files, but Ramen also had to parse the CSV (and it is quite frankly slow to do so). Or could the delivery guarantees be alleviated somehow to simplify message management a bit?</p>

<p>The main explanation might reside in the third chart: Kafka materializes every message queues on disk. And to run a simple two-operations stream "graph" such as the one of this benchmark, KSQL will need at least 2 topics (one for the output of each operation). Plus the one for receiving the original CSV, plus maybe intermediary ones for the actual operations (I've seen simpler queries than this one requiring up to 5 or 6 topics). That's a lot of useless message bouncing and disk writes (at least from a single machine perspective; users of other use cases may object).</p>

<p>Message bouncing is made even worse by the fact that, as messages are untyped data blobs for Kafka, then the receiver receives all of it. In the case of our 80 columns messages, it means the operation that only makes use of 5 or 6 columns will still receive the full 80 columns, leading to more moving than necessary. Shall we have batched the messages in the producer, say by the default batch size of 200, then that's those 200 messages that the first consumer would receive. In contrast, in Ramen "producers" (aka parent functions) send only the requested fields to their "consumers" (aka child functions), including in deep compound data structures. I don't know how much of an issue this has been in this particular case though.</p>

<p>Regarding the disk IOs, the official solution for this seams to be to use an in-memory file-system. That will sure help reduce disk IOs, but I'm less optimistic about memory usage.</p>

<p>This is frankly a surprise and a disappointment. I had prepared a conclusion that went like this: "Sure, today Kafka is a bit more efficient than Ramen. But this is like benchmarking an early prototype of a car against the best racing horse; Sure the horse win, but the car prototype has such a large progression margin than it is going to blow the horse out of the water one day, eventually." But what I took for a race horse is actually more like a plain and boring beast of burden.</p>

<p>Well, I'm a bit disappointed also because I will now have to either learn to tune Kafka for this use case or find another contender.</p>

<p>Meanwhile, I now know better what's the cost of using a bulldozer to crack a nut.</p>

<p>atop result files:</p>
<ul>
<li><a href="blog/ksql.atop">ksql.atop</a></li>
<li><a href="blog/ramen.atop">ramen.atop</a></li>
</ul>

<? include 'footer.php' ?>
