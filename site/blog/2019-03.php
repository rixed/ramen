<? include 'header.php' ?>

<h1>One degree of scale</h1>
<p class="date">2019-04-05</p>

<p>This is a well known fact in engineering that the same problem have distinct solutions at different scales.</p>

<p>What is less often agreed upon is how many scales there are. Many software engineers would consider only two: small or big; not scalable or scalable; distributed or not distributed. But for many problems there are more meaningful scales than that.</p>

<p>The scales we are interested into are somewhere in between "a dedicated server can handle it forever" and "a handful of servers, to which we may add a few ones later". We are not interested in "so much hardware that a server or network link fails every two minutes" kind of scales. If you are then I'm sorry. How are things going at Google?</p>

<h2>Can't a single machine do more work?</h2>

<p>If a single server cannot handle your load, there are several things that can be tried before jumping to conclusions.</p>

<br/>

<p>First, operations running can be simplified/optimised, for instance with the help of <em>private fields</em>.</p>

<p>Private fields are any field which name starts with an underscore. Like any other named value, it can be used anywhere in an expression after it's been defined, but it remains invisible from other functions that are not allowed to select it. Indeed, private fields are not part of the output type of a tuple as one can check with <code>ramen info $SOME_PROGRAM</code>. This also makes sure that those temporary values are never archived, which would waste disk space.</p>

<p>So for instance, one could replace this:</p>

<pre>
  SELECT
    95th PERCENTILE OF (SAMPLE(1000, response_time)) AS resp_time_95th,
    99th PERCENTILE OF (SAMPLE(1000, response_time)) AS resp_time_99th,
    ...
</pre>

</p>by:</p>

<pre>
  SELECT
    SAMPLE(1000, response_time) AS _sample,
    95th PERCENTILE OF _sample AS resp_time_95th,
    99th PERCENTILE OF _sample AS resp_time_99th,
    ...
</pre>

<p>...thus performing only one <code>SAMPLE</code> operation instead of two.<p>

<br/>

<p>Another way to make Ramen faster, of course, is to optimise the functions themselves. Thankfully, there is plenty of room left for optimisation there!</p>

<p>Last month for instance the <code>PERCENTILE</code> operation has been optimised in two important ways.</p>

<p>Firstly, as it is often the case that one wants to extract several percentiles from the same value, the percentile function signature has been extended. It will now accept several percentiles and in that case will return a vector of the various results, like so:</p>

<pre>
  SELECT
    [95th; 99th] PERCENTILE (SAMPLE(1000, response_time)) AS _rt_perc,
    1st(_rt_perc) AS rt_95th,
    2nd(_rt_perc) AS rt_99th,
    ...
</pre>

<p>Then, the percentile computation itself has been improved to use a form of <a href="https://en.wikipedia.org/wiki/Quickselect">quick-select</a>; the idea is to neglect sorting subparts of the array where lie no percentile of interest.</p>

<p>After these two optimisations some of the tests ran 10 times faster.</p>

<br/>

<p>If that's not enough, then one can throw more CPUs at the problem. Indeed, as workers run concurrently then the more CPUs the less workers will step on each others toes.</p>

<p>But a single hardware machine does not scale linearly very far with the number of CPUs, so once you've spent a hundred dollars per month on a good server, then what?</p>

<h2>Let's grow beyond a single server</h2>

<p>We want to be able to handle more input tuples than a single machine can possibly read from its disks or its network interfaces. On top of that, we also want to process data that is produced in several geographic locations, and that we'd rather avoid to copy over to a single site.</p>

<p>Since Ramen is already a multi-program (ie. several independent processes running concurrently), what prevent the current implementation from spawning several machines?</p>

<p>First, there is <em>no shared hard disk</em> between different machines.</p>
<p>We use the local file system for storing a few configuration files, and to archive workers output. It is fine if archival stays on the machine where it is produced as it's also where it will be re-read by replay-workers. But we need a way to distribute the small set of configuration files.</p>

<p>Second, there are <em>no shared signals</em> and <em>no process management</em> between different machines.</p>
<p>I find actually quite surprising that this is not a Linux standard to share the list of pids of several kernels, forwarding signals and <code>execve</code> syscalls, to give the illusion of an operating system that goes beyond hardware and geographic boundaries. Of course for a fully fledged UNIX to run on multiple machines it would be a lot more involved than that, but as far as Ramen is concerned that would be enough. It seems a lot of effort have been spend recently with containers and such to scale Linux down rather than up.</p>

<p>Last but not least, there is <em>no shared memory</em> between different machines.</p>
<p>Ramen workers use memory-mapped ring-buffers to exchange data. Messages from one worker to another will have to go through the network, preferably direct.</p>

<p>The plan to scale Ramen by one order of magnitude is then straightforward, at least at the first approximation:</p>

<ul>
<li>it is needed that the configuration file listing which programs should run be distributed amongst all the sites;</li>
<li>it is needed that the supervisor process be made aware that there are other sites and that only a part of the configuration matters to him.</li>
<li>it is needed that workers be able to send their output into a socket, and read their input from a socket;</li>
</ul>

<p>...with plenty of details for the devil to hide.</p>

<p>Let's have a deeper look.</p>

<p>to begin with, a notion of <em>site</em> has to be added to the RC file.<p>
<p>I initially though a site would be a hostname, but I also wanted to test multi-site locally without messing my networking configuration; problem easily solved with a level of indirection: a site is now just a name, and there is a list of "services" that resolve into a hostname and a port. In other words, a service registry. That's going to come handy when port numbers can't be known in advance any longer.</p>

<p>So, now when starting a program with <code>ramen run</code>, one can specify on what site the worker is supposed to actually run: <code>ramen run --on-site 'dc13.cluster_a*'</code>. Similarly, when starting the supervisor, one can specify which site this is: <code>ramen supervisor --site dc13.cluster_b42</code>. This supervisor will then only start the workers supposed to be running on that site.</p>

<p>Easy enough, but it brakes an assumption: that function names are unique. Now the same function can be running in different places.</p>

<p>Then, workers have to be able to send tuples over the network.</p>
<p>We could merely connect each parent to each child with a socket, but that would not be efficient. Indeed, what if the child threw most of the input tuples away because of a picky WHERE clause? We'd rather have the tuples filtered before they board the plane.</p>

<p>So this is how it really works: for each individual parent and set of identical children, the children <em>top-half</em> is run in the same site than the parent. This top-half only executes the first stage of the WHERE clause, and then send the filtered tuples over the network to the actual children running remotely, which will treat those pre-filtered tuples are normal incoming tuples.</p>
<p>As an additional benefit, this frees normal workers to deal with sending packets to the network, as to them the top-half is just like a locally running child.</em></p>

<p>And to avoid that each child have to mess with the network (and the system administrator have to ensure many ports are reachable) we instead run a single <em>copy service</em> per site, that accept incoming connections and will forward incoming messages to the specified input ring-buffer. This is called the <em>tunneld</em> service, and is the first (and only) of the defined service in the service registry.</p>

<p>This is mostly it and is enough to have a bare minimal distributed version of Ramen running. But there are still some work required before the whole thing is running properly.</p>

<p>In particular, the statistics are not distributed, which means proper monitoring of Ramen itself is very hard, and more importantly the allocation of disk storage space is broken as it has to be done globally to be reliable, but can now work only on a per site basis (imagine if site A decided that it was better if site B's workers would archive their output, but site B decided that it's actually better the other way around!)</p>

<p>So, this one order of magnitude scaling up will likely requires that April be dedicated to it also.</p>

<? include 'footer.php' ?>
