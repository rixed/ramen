<? include 'header.php' ?>

<h1>One-liners</h1>
<p class="date">2018-12-08</p>

<p>The two technological jewels I've seen during my short time at <a href="https://en.wikipedia.org/wiki/Booking.com">Booking.com</a> were:</p>

<ul>
<li>an experiment system that reliably measured the effects of any change while new versions were pushed in production several times a day;</li>
<li>an event system that collected and stored gazillions of metrics from all around the place, both for data mining and for troubleshooting operations.</li>
</ul>

<p>Those were the two legs on which their data-driven culture was standing.</p>

<p>One interesting feature of the event system that I've always kept in mind while designing Ramen was that it was possible from the shell to run a command line tool to see the live stream of events after some basic filtering (for instance, all events from the web servers of a particular cluster) or even to embed a Perl one-liner that would run on all events to answer any question you might have. To achieve this, that piece of code had to be sent up to the event database where it would run against a real-time copy of the event stream, without the user even noticing.</p>

<p>In the hands of experienced devops such a Swiss army knife was the quickest and surest diagnosing tools.</p>

<p>Of course Ramen has no ambition to address a data stream that big, by many orders of magnitude. But as far as convenience is concerned it may fare better. Indeed, Booking's event querying tool had a learning curve that was stepper than needed to be:</p>

<p><em>First</em>, to be able to make sense of the events you had to know their internal structure. You might think that's a necessary precondition, but those events being deep JSON objects didn't help discovering and retrieving the interesting bits.</p>
<p><em>Second</em>, to make the best use of the events querying tool you had to know the event system Perl API. If you didn't then in case of emergency you would have to resort solely on the few shortcuts offered by the tool to handle "common cases" (like "frequently asked questions", "common cases" seldom occurs in the real world). In practice only the members of the event-system team were familiar enough with that API.</p>

<p>Ramen improves on those two aspects. Regarding locating information, Ramen events use predominantly flat tuples rather than nested objects, and field names can be as long and descriptive as needed, since field name length incur no processing/saving costs contrary to JSON; and if that's not enough then fields can also be documented and assigned proper units.</p>

<p>Regarding data manipulation, Ramen uses a SQL-like language and compile it <em>before</em> starting to process the events.</p>

<p>So now is time for the three command line querying commands (<code>ramen tail</code>, <code>ramen timeseries</code> and <code>ramen replay</code> to display not only a given pre-existing function name, but to also accept any new function as a one-liner.</p>

<p>Example:</p>

<pre>
$ ramen tail -h -- select start, 5xx_errors from httpd/access where cluster="EU1"
#start,5xx_errors
2018-12-06 10:30:20,110
2018-12-06 10:30:30,71
2018-12-06 10:30:40,98
...
</pre>

<p>In practice, that function is compiled and run under the hood, and is destroyed once the command is over.</p>

<? include 'footer.php' ?>
