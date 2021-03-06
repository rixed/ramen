<? include 'header.php' ?>
<h1>Data archival and retrieval</h1>

<h2>Requirements</h2>

<p>Ramen must be usable for alerting, dashboarding and troubleshooting. Alerting just requires to process the live flow of events with as little latency as possible. Dashboarding requires to be able to retrieve the recent values so that one can plot the recent past of some metric. Troubleshooting requires to be able to run new queries about the recent past as well as about the distant past, and is the most demanding feature.</p>

<p>There are clearly two kinds of queries:</p>

<ul>
<li><b>Continuous queries</b> which output must be computed once and only once each time the output changes regardless of how many times and how frequently we want to monitor it. Those are useful for alerting or dashboarding.</li>
<li><b>Transient queries</b> which are run once for as short a time as possible to get a given result about a given time range. This is what is needed for troubleshooting.</li>
</ul>

<p>Creating a new continuous query falls in between those two cases: we want for instance to monitor some new metric, so we write a new function, but we would like to see what this new function would have returned for the past few hours, in order to quickly check that it behaves as expected.</p>

<h2>Implementation</h2>

<h3>Overview</h3>

<p>It is clear that transient queries can only use past information if continuous queries store it somehow.</p>

<p>Given a total amount of disk space dedicated to data storage, and a sufficiently complex query tree, it is not easy for the user to properly assign disk space to each continuous query in order to optimize space and CPU requirements. That's a job for a constraint solver. Ideally, the user should specify nothing more than the amount of storage space, how long some continuous query output ought to be available (either because it's been archived or because it can be recomputed from other archived data), and how frequently this output is going to be requested. From there, and by monitoring the rate of output and CPU consumption of various functions, Ramen should be able to optimize storage all by itself.</p>

<p>Then, when asked for the output of a (possibly new) function over a given time range in the past, Ramen must somehow replay those archived output and inject them in the program tree until the actual function which output the user is interested in. Once the end of the time range is reached any temporary function that has been spawned to replay the past must be destroyed.</p>

<h3>From retention configuration to storage space allocation</h3>

<p>In order to allocate storage space, Ramen needs to keep an eye on several possibly changing data sources:</p>

<ul>
<li>The user configuration, that's a mere text file specifying the total available size, and what functions are likely to be queried in the future and how frequently;</li>
<li>Statistics about every running functions and their relationship, in order to estimate the cost of each function and their output volume;</li>
<li>What's the current state of archives to avoid costly plan changes.</li>
</ul>

<p>We therefore have a dedicated process for this. <code>ramen archivist</code> will monitor workers statistics and regularly read the user configuration and will turn this into an SMT2 file trying to minimize the future query times, and ask the <a href="https://github.com/Z3Prover/z3">Z3 constraint solver</a> to solve it. It will then turn the answer into a file mapping each function to the amount of storage space they should use, and reconfigure the workers so that those who must archive do so.</p>

<p><code>ramen gc</code> needs also to read this file in order to know how big of an history each workers are allowed to have.</p>

<h3>Archive format</h3>

<p>When archiving data there are two possibly conflicting requirements: data must be quick to write and later read from archives, and archives must take as little space as possible. In addition to this, archiving is an opportunity to exchange data with other systems (by archiving in an external database for instance).</p>

<p>Speed is not easy to predict, as various storage technologies have very different profiles in this regard. Roughly, old "tape like" kind of storage are more efficient the less is written, while newer "memory like" storage is more efficient the less (de)serialization takes place.</p>

<p>The decision to use one or the other format not only depends on the underlying storage technology, but also on the expected write and read frequencies and volumes. One might prefer a format that's more efficient for reads for frequently accessed archives and another format that's more compact for rarely retrieved data.</p>

<p>For now, Ramen knows of only one archival format, that is very simple: non-wrapping ringbuffers, that are exactly the same as the ringbuffers used to exchange data between workers but that are not wrapping. When full, such a buffer will be moved away in a subdirectory, under a name betraying the time-range that's covered in that file for faster scans.</p>

<p>It is planned to also support <a href="https://orc.apache.org/">ORC files</a>, which would be used for longer term storage with the additional benefit of being readily usable with <a href="https://hive.apache.org">hive</a> for instance.</p>

<p>Other databases could be easily supported as well, in write only mode, as a way to export data; but that would not be usable for querying the past.</p>

<h3>Transient queries</h3>

<p>Ramen implements both continuous and transient queries the same but for a few differences:</p>

<ul>
<li>transient queries must be given a time range (which can be in the future);</li>
<li>transient queries output is never archived;</li>
<li>transient queries are automatically destroyed after their result has been retrieved;</li>
</ul>

<h3>Spawning new workers or reusing the main ones?</h3>

<p>Suppose one wants to obtain the output of a given query Q1 for a given time range in the past; Q1 iutput is not archived, but it draws its input from, say, Q2 which itself selects from both Q3 and Q4, both of them archived.</p>

<p>First, Ramen has to find a path from Q3 up the query tree to all required ancestors with archives for the requested time range (here, Q3 and Q4).</p>

<p>From there, it could either run another instance of Q2 and another instance of Q1, read Q3 and Q4 archives and inject these archived tuples in the input of those new instances of Q2 and Q1. When the query is over, all those transient queries has to be destroyed.</p>

<p>Or it could inject those tuples in the input of the actual worker for Q2, which in turn will output the result into Q1 actual input, and wait for the answer at Q1 output. When the query is over, nohting needs to be destroyed so the bookkeeping is simpler. For this to work of course there must be a way to discriminate tuples that are being replayed from the past to answer this particular transient query from tuples that are coming from the live stream (and tuples processed to answer yet other transient queries).</p>

<p>Ramen does the later rather than the former, as it involves less bookkeeping, is believed to be more efficient and is compatible with asynchronous functions that I'd like to implement at a later stage and that will likely require local caching that I'd rather share and not duplicate for each transient query.</p>

<h3>Channels and replay workflow</h3>

<p>To follow up on the previous example, when a user ask for <code>ramen tail --since $SOME_TIME --until $SOME_LATER_TIME Q1</code> then here is what is happening:</p>

<ol>
<li>First, a new transient ringbuffer is created and a new channel identifier is chosen;</li>
<li>Then, Q1 is asked to output everything related to that channel into this ringbuffer;</li>
<li>Then, new workers for Q3 and Q4 are started to replay the archived tuples in the time range into the given channel;</li>
<li>When replaying the past, the workers will obey the normal output specifications, which in that case will instruct them to add their output into Q2 input ringbuffer;</li>
<li>When the last tuple is reached then the replay will end with a special message ending that channel, and then terminate;</li>
<li>Then Ramen merely prints what ends up in the transient ring buffer, waiting until it has received two end-of-replay notifications before exiting.</li>
</ol>

<p>With such a simple mechanism it is possible to see the recent past (and actually even the distant past) of any newly created function, which comes handy to make sure a new function returns the expected time series.</p>

<h3>Challenges</h3>

<h4>Cost of changing ones mind</h4>

<p>The SMT2 problem should take into account the current configuration and the value of the immobilised archives already stored.</p>

<p>Alternatively, it could take into account the cost of answering queries with archives spread in the function tree.</p>

<h4>Estimation of a program life expectancy</h4>

<p>All of the above assumes that any worker that's currently running will be running forever. This is obviously a dangerous assumption.</p>

<p>The SMT2 should vet this risk by estimating the life expectancy of functions considered for archival.</p>

<h4>Cost assessment</h4>

<p>Computation costs are bound to CPU and RAM. But we need to measure the cost of the live channel only. If the same Unix process is serving both live and other channels, though, then measuring the CPU and RAM costs of only the live channel is not easy (at best). Thus we have to also account for other channels in resource costs. But then we do not want a path to appear costly just because it is a very frequent pathway for replaying history. Should we artificially deflate the cost of CPU/RAM with the ratio of non-live tuples that go through a worker?</p>

<p>And then storage: assessing the storage cost ahead of time is also not as straightforward as looking for the recent past, especially when we have several level of compression as data ages. The current approach is to look at the aggregated total size written in all output ringbuffers, which is bogus because it does not distinguish between live channel and others, because we write only the fields required by children but we do not know what field will be asked in the future, and last but not least because we do not know how much disk space future tuples will occupy, especially by looking only at what we write in ringbuffers (we are not going to do long term storage in ringbuffers).</p>

<p>So it appears we have actually no reliable estimator for computing or storage costs (yet)!</p>

<? include 'footer.php' ?>
