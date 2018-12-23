<? include 'header.php' ?>
<h1>Man pages</h1>

<p>Ramen consists of a single executable that can act as the supervisor daemon, the compiler, a data extractor, and so on.</p>

<p>It is self-documented so only a very rough summary of the main commands is given hereafter. You will find an actual copy of the embedded documentation at the end.</p>

<a name="compiling">
<h2>Compiling a program</h2>
</a>

<p>Programs are stored on mere text files which extension should be ".ramen".  To compile one, use the <code>ramen compile</code> command. This will parse, type-check and then generate the executable binary (which extension will be ".x").</p>

<p>For type-checking it is likely that the compiler will need to know the output types of referenced functions from other programs. It will then look for those programs in the path provided by wither the <code>RAMEN_ROOT</code>+ environment variable or the <code>--root</code> command line option. Those referenced programs must have been compiled already, which forces a given order when compiling multiple programs.</p>

<p>As an example, suppose this program:</p>

<pre>
SELECT hostname, last uptime
FROM monitoring/hosts
GROUP BY hostname
COMMIT WHEN in.time > group.first.time + 30
</pre>

<p>to know the types of <code>hostname</code>, <code>uptime</code> and <code>time</code> ramen must look at the <code>monitoring/hosts</code> operation output type. It will then look for the executable file <code>$RAMEN_ROOT/monitoring/hosts.x</code> which will then provide this type (as each executable worker can display its types and other information about itself). Therefore this program must have been compiled previously.</p>

<p>See <a href="man/compile.html">ramen compile --help</a> for details.</p>

<a name="running">
<h2>Running programs</h2>
</a>

<p>Contrary to normal executables, Ramen programs must be started by the <em>supervisor</em>, which will make sure all workers are connected properly.</p>

<h3>Starting the supervisor</h3>

<p><code>ramen supervisor</code> will start a daemon that will essentially check that the workers specified in the running-configuration are indeed working. When starting new worker it will first check that types are compatible with the other workers (or report an error), create the ring buffers and "connect" the parents to their children.</p>

<p>See <a href="man/supervisor.html">ramen supervisor --help</a> for more.</p>

<p>To stop the supervisor, merely sends it a TERM or INT signal. It will then stops all workers and exit. Workers themselves will save their state so that little is lost by stopping and restarting Ramen.</p>

<h3>Running a program</h3>

<p>To start a program use the <code>ramen run</code> command.  This command does not in itself start anything, though. Instead, it merely adds that program to the list of programs that the supervisor must keep running. Therefore the actual workers won't start until the process supervisor itself is running.</p>

<p>See <a href="man/run.html">ramen run --help</a> for details.

<h3>Stopping a program</h3>

<p>Similarly, <code>ramen kill</code> will remove a program from the running configuration of the supervisor.</p>

<p>See <a href="man/kill.html">ramen kill --help</a> for more.</p>

<h3>Listing running configuration</h3>

<p><code>ramen ps</code> will list the running programs and display some runtime statistics which makes it a `ps` as well as a `top` equivalent.</p>

<p>See <a href="man/ps.html">ramen ps --help</a> for more.</p>

<a name="querying">
<h2>Retrieving a worker output</h2>
</a>

<p><code>ramen tail</code> followed by the <a href="glossary.html#FQName">fully qualified name</a> of an operation will display the last output tuples by that operation.</p>

<p>If this is the first time in a while that this worker has been asked for its output then it may take some time to start receiving data as old tuples may not be archived (so <code>ramen tail</code> will wait for new tuples instead).</p>

<p>Refer to <a href="man/tail.html">ramen tail --help</a> for more information.</p>

<p>Similarly, <code>ramen timeseries</code> will extract a single field from an operation, indexed by time. Time points will be evenly spaced and events will be "bucketed" into the requested time scale.</p>

<p>See <a href="man/timeseries.html">ramen timeseries --help</a> for more details.</p>

<a name="alerting">
<h2>Alerting</h2>
</a>

<p>In addition to tuples, workers can emit <a href="glossary.html#Notification">notifications</a>. Those are received by a dedicated daemon: <code>ramen notifier</code> that will route them according to its configuration file.</p>

<p>See <a href="man/notifier.html">ramen notifier --help</a> for more details.</p>

<a name="maintenance">
<h2>Maintenance</h2>
</a>

<p>Other daemons are required in production, to clean the archived output, compute various statistics, and so on.</p>

<p>See <a href="man/gc.html">ramen gc --help</a> and <a href="man/archivist.html">ramen archivist --help</a> for more details.</p>

<p>Some commands are also available to help with diagnosing issues. See <a href="man/ramen.html">ramen --help</a> for an exhaustive list of commands.</p>

<a name="tests">
<h2>Testing configuration changes</h2>
</a>

<p>As Ramen is designed to connect to alerting systems, reliability has been an important design consideration. The main source of errors in production systems being configuration changes, it is therefore important to test any change in the configuration.</p>

<p>The <code>ramen test</code> command takes text files each describing a test and, independently of any already running instance of ramen, will run all specified programs, provide them with the test input, and check the output matches the ones described in the test file.</p>

<p>See for instance
<a href="https://github.com/rixed/ramen/blob/master/tests/basic_aggr.test">this test</a> from ramen own test suite for an example of a test specification.</p>

<p>Refer to <a href="man/test.html">ramen test --help</a> for details.</p>


<h2>Man pages of all ramen subcommands:</h2>

<? foreach ($info_pages['man.html']['sub_pages'] as $p) { ?>
<h2><?=$p['title']?></h2>
<ul>
  <? foreach ($p['man_pages'] as $page => $p) { ?>
<li><a href="man/<?=$page?>"><?=$p['title']?></a></li>
  <? } ?>
</ul>
<? } ?>
<? include 'footer.php' ?>
