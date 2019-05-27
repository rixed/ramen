<? include 'header.php' ?>

<h1>Who need two different message queues?</h1>
<p class="date">2019-05-25</p>

<p>The current state of Ramen in a distributed setting was that it kind of work as expected (ie. small scale etc), as long as the configuration files are synchronized <em>somehow</em> amongst all the sites.</p>

<p>The plan was to use a distributed file system for that, for instance ZooKeeper. But quite frankly, the complexity of maintaining ZooKeeper in exchange for a small scale deployment is not a great deal. I also played with the idea of replacing all the configration files by a single, network accessible key-value store. Actually, the first prototype of Ramen worked this way.</p>

<p>The advantages of a single key-value store are many:</p>
<ul>
<li>It solve the configuration synchronisation issue;</li>
<li>It's easier and quicker to notify clients of a single configuration change than for the clients to watch configuration files;</li>
</ul>

<p>But there are some disadvantages as well:</p>
<ul>
<li>Files gives us atomic modification of a set of values (almost) for free, while it's  harder to have transactions with a key-value store;</li>
<li>One cannot run the simplest command, such as <code>ramen ps</code>, when the key-value store service is not running.</li>
</ul>

<p>This last one especially killed the idea and that's why the current version of Ramen uses plain and simple configuration files.</p>

<p>And I would still favor files if not for some crazy ideas that have landed in my mind recently: if we had a single network accessible pub-sub kind of data store, we could use it as well to retrieve not only the configuration but also the time series out of Ramen's workers. So we could implement a graphical user interface that would also subscribe to it, and subscribe to some worker output, and update the charts as new points are produced.</p>

<p>That's streaming from end to end, something that I find very desirable and that I wanted to do at some point.</p>

<p>Picture this typical situation: The devops team at ACME hosting corp is monitoring its server farm with a dashboard displaying 50 time series. This dashboard is displayed in 20 different clients (some devops desktops and some screens on the wall), each refreshing the display every minute. For the server, that's going to be 1000 queries every minutes to retrieve the last 100 points for all these time series (99 of which are going to be the same as one minute before).</p>

<p>No amount of HTTP caches is going to convince me that it's not a stupid waste of resources. Stream values down to the clients and instead of all those queries you just have to push 1000 floating point numbers in total every minutes with no need for any IO at all on the server.</p>

<p>For the most extreme cases with even more clients you could even do multicast.</p>

<p>So it looks like this key-value service would kill two birds with one bow: synchronizing the configuration for multi-site deployments and end to end streaming of time series.</p>

<p>Therefore I took my shopping basket under my arm and went on the market looking for a key-value store that would fulfill these requirements:</p>

<ul>
<li>Must run as a library (for C or C++) so that Ramen is still easy to deploy;</li>
<li>Must be internet-worthy (authentication, crypto, etc);</li>
<li>Must have some authorization system to control who can read or write what keys;</li>
<li>Must support pub-sub like API;</li>
<li>Must have some form of transactions;</li>
<li>Must support (advisory) locking so that users can't step on each-others toes;</li>
<li>Should support better types for the values than mere strings;</li>
<li>Should be able to delegate user authentication to an external system (à la GSSAPI);</li>
<li>Should support some form of multicasting.</li>
</ul>

<p>That's a lot to ask for, but at the same time I can't think of sharing a configuration tree amongst several writers without those features. With the many key-value stores that have popped up those last years, surely there are still plenty fitting that description.</p>

<p>Well, I came back with an empty basket.</p>

<p>Some of the options I quickly envisaged and eventually turned away from:</p>
<ul>
<li><em>Redis</em>: it's not embeddable, it lacks proper authentication or authorization and is generally considered not internet-worthy (at least the open source version);</li>
<li><em>Etcd</em>: not embeddable neither, has a ton of dependencies, and like ZooKeeper does not look like a particularly good fit for small scale deployment;</li>
<li><em>Consul</em>: still not embeddable, still does not look appropriate for Ramen scale, and also apparently lacks a notification mechanism, and is bound to HTTP as a protocol (which is not a good start for anything lightweight);</li>
<li><em>Riak</em>: not embeddable, also no notifications it seems;</li>
</ul>

<p>At that point it started to feel like it would be quicker to just implement what I need than to keep looking for it. How hard could it be to slam together openssl, a small in memory hash table, and a custom pub-sub protocol?</p>

<p>But first, I had to come to term with another idea that was periodically resurfacing: There is already a message queue within Ramen: the ringbuffers, that can now work across server boundaries. Do Ramen really need two message queues? Can't ringbuffers be used for the configuration and the streaming down to the clients ?</p>

<p>Several reasons why they definitively can't:</p>
<ul>
<li>RingBuffers work in only one direction, but occasionally a client will want to write something in the configuration;</li>
<li>They would only address the low level transportation of messages part, but none of the actual key-value storage service requirements;</li>
<li>It would make writing clients quite more complicated than strictly necessary;</li>
<li>Last but not least, the networking part is certainly not internet-worthy!</li>
</ul>

<p>Also, I'd rather have the ringbuffer design be solely dictated by the need of the workers. The mechanism to pass data around from worker to worker has to be optimized for large and directed streams of arbitrary data; while distribution of the configuration parameters and even, to a lesser degree, of the last output value of a few leaf workers, represents only a small volume in comparison and does have completely different needs in terms of authentication etc.</p>

<p>Just another case of "one size does not fit all".</p>

<p>Then, for a reason I can't remember but maybe just because I've always had a soft spot in my mind for this library despite I've never used it, I came to think about <em>ZeroMq</em>.</p>

<p>Of course ZeroMQ is not a message queue (as the name implies). It is message agnostic; just a better socket library that does authentication, authorization (including the delegating part), crypto, and seams very internet worthy. On ACME LAN it would even do multicast. It would fit perfectly all "SHOULDs" but few of the "MUST".</p>

<p>Hopefully, the "MUST" are the easy parts to implement, also the parts that I'm happy to have 100% control over.</p>

<p>Next time I might write about how I went shopping again, this time for a GUI toolkit.</p>

<? include 'footer.php' ?>
