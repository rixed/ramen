<? include "header.php" ?>

<h1 align=center>ramen-archivist</h1>



<a name="NAME"></a>
<h2>NAME</h2>



<p style="margin-left:11%; margin-top: 1em">Ramen-archivist
- Allocate disk for storage</p>

<a name="SYNOPSIS"></a>
<h2>SYNOPSIS</h2>


<p style="margin-left:11%; margin-top: 1em"><b>Ramen
archivist</b> [<i>OPTION</i>]...</p>

<a name="OPTIONS"></a>
<h2>OPTIONS</h2>



<p style="margin-left:11%; margin-top: 1em"><b>--daemonize</b>
(absent <b>RAMEN_DAEMONIZE</b> env)</p>

<p style="margin-left:17%;">Daemonize</p>

<p style="margin-left:11%;"><b>--help</b>[=<i>FMT</i>]
(default=auto)</p>

<p style="margin-left:17%;">Show this help in format
<i>FMT</i>. The value <i>FMT</i> must be one of
&lsquo;auto', &lsquo;pager', &lsquo;groff' or &lsquo;plain'.
With &lsquo;auto', the format is &lsquo;pager&lsquo; or
&lsquo;plain' whenever the <b>TERM</b> env var is
&lsquo;dumb' or undefined.</p>

<p style="margin-left:11%;"><b>--loop</b>[=<i>VAL</i>]
(default=) (absent=0.)</p>

<p style="margin-left:17%;">Do not return after the work is
over. Instead, wait for the specified amount of time and
restart</p>

<p style="margin-left:11%;"><b>--no-allocs</b></p>

<p style="margin-left:17%;">Do no attempt to update the
allocations file</p>

<p style="margin-left:11%;"><b>--no-reconf-workers</b></p>

<p style="margin-left:17%;">Do not change the workers
export configuration</p>

<p style="margin-left:11%;"><b>--no-stats</b></p>

<p style="margin-left:17%;">Do no attempt to update the
workers stats file</p>

<p style="margin-left:11%;"><b>--to-stdout</b>,
<b>--to-stderr</b>, <b>--stdout</b>, <b>--stderr</b> (absent
<b><br>
RAMEN_LOG_TO_STDERR</b> env)</p>

<p style="margin-left:17%;">Log onto stdout/stderr instead
of a file</p>

<p style="margin-left:11%;"><b>--to-syslog</b>,
<b>--syslog</b> (absent <b>RAMEN-LOG-SYSLOG</b> env)</p>

<p style="margin-left:17%;">log using syslog</p>

<p style="margin-left:11%;"><b>--version</b></p>

<p style="margin-left:17%;">Show version information.</p>

<a name="COMMON OPTIONS"></a>
<h2>COMMON OPTIONS</h2>


<p style="margin-left:11%; margin-top: 1em"><b>-d</b>,
<b>--debug</b> (absent <b>RAMEN_DEBUG</b> env)</p>

<p style="margin-left:17%;">Increase verbosity</p>


<p style="margin-left:11%;"><b>--initial-export-duration</b>=<i>VAL</i>
(absent=0. or <b>RAMEN_INITIAL_EXPORT</b> env)</p>

<p style="margin-left:17%;">How long to export a node
output after startup before a client asks for it</p>


<p style="margin-left:11%;"><b>--persist-dir</b>=<i>VAL</i>
(absent=/tmp/ramen or <b>RAMEN_DIR</b> env)</p>

<p style="margin-left:17%;">Directory where are stored data
persisted on disc</p>

<p style="margin-left:11%;"><b>-q</b>, <b>--quiet</b>
(absent <b>RAMEN_QUIET</b> env)</p>

<p style="margin-left:17%;">Decrease verbosity</p>

<p style="margin-left:11%;"><b>-S</b>,
<b>--keep-temp-files</b> (absent
<b>RAMEN_KEEP_TEMP_FILES</b> env)</p>

<p style="margin-left:17%;">Keep temporary files</p>

<p style="margin-left:11%;"><b>--seed</b>=<i>VAL</i>,
<b>--rand-seed</b>=<i>VAL</i> (absent
<b>RAMEN_RANDOM_SEED</b> env)</p>

<p style="margin-left:17%;">Seed to initialize the random
generator with (will use a random one if unset)</p>

<p style="margin-left:11%;"><b>--variant</b>=<i>VAL</i>
(absent <b>RAMEN_VARIANTS</b> env)</p>

<p style="margin-left:17%;">Force variants</p>

<a name="ENVIRONMENT"></a>
<h2>ENVIRONMENT</h2>


<p style="margin-left:11%; margin-top: 1em">These
environment variables affect the execution of
<b>archivist</b>: <b><br>
RAMEN-LOG-SYSLOG</b></p>

<p style="margin-left:17%;">See option
<b>--to-syslog</b>.</p>

<p style="margin-left:11%;"><b>RAMEN_DAEMONIZE</b></p>

<p style="margin-left:17%;">See option
<b>--daemonize</b>.</p>

<p style="margin-left:11%;"><b>RAMEN_DEBUG</b></p>

<p style="margin-left:17%;">See option <b>--debug</b>.</p>

<p style="margin-left:11%;"><b>RAMEN_DIR</b></p>

<p style="margin-left:17%;">See option
<b>--persist-dir</b>.</p>


<p style="margin-left:11%;"><b>RAMEN_INITIAL_EXPORT</b></p>

<p style="margin-left:17%;">See option
<b>--initial-export-duration</b>.</p>


<p style="margin-left:11%;"><b>RAMEN_KEEP_TEMP_FILES</b></p>

<p style="margin-left:17%;">See option
<b>--keep-temp-files</b>.</p>

<p style="margin-left:11%;"><b>RAMEN_LOG_TO_STDERR</b></p>

<p style="margin-left:17%;">See option
<b>--to-stderr</b>.</p>

<p style="margin-left:11%;"><b>RAMEN_QUIET</b></p>

<p style="margin-left:17%;">See option <b>--quiet</b>.</p>

<p style="margin-left:11%;"><b>RAMEN_RANDOM_SEED</b></p>

<p style="margin-left:17%;">See option <b>--seed</b>.</p>

<p style="margin-left:11%;"><b>RAMEN_VARIANTS</b></p>

<p style="margin-left:17%;">See option
<b>--variant</b>.</p>
<? include "footer.php" ?>
