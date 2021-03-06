<? include 'header.php' ?>

<h1>Functions, Programs, Workers and the supervisor</h1>

<h2>Requirements</h2>

<p>We want flexibility regarding parallelism, ideally the full spectrum of:</p>

<ul>
<li>The output of a function is sent to another function running in a separate machine;</li>
<li>The output of a function is sent to another function running in the same machine but another process;</li>
<li>The output of a function is sent to another function running in the same process but another thread;</li>
<li>The output of a function is sent to another function running in the same thread, asynchronously;</li>
<li>The output of a function is sent to another function running in the same thread, synchronously.</li>
</ul>

<p>In all these cases we want back-pressure in case of overcapacity.</p>

<p>The first case is required to spread Ramen over several machines. The last is the fastest one possible as we could go as far as directly calling the next operation as we call a local function.</p>

<p>Individual ring buffers per function has been chosen as a communication mechanism as it suits all of the above with little or no adjustment.</p>

<h2>Programs and Functions</h2>

<p>Although a stream processor usually consists of a <em>tree</em> of continuous queries, we wanted to be able to express local loops, despite it makes type checking and compilation harder.</p>

<p>So Ramen has both the notion of functions, that are individual operations turning a given stream of input tuples into a stream of output, and programs, which are a set of functions that are compiled, ran and stopped altogether. Within a program, loops are allowed and functions can be defined in any order. This is quite similar to compiling a normal program.</p>

<p>For a compiled program to be able to run though, all of its parents (programs which output are used anywhere in the new program being run) must be running already. This check is a bit similar to the linking stage of process that's being loaded.</p>

<p>To sum it up, users write Ramen <em>programs</em>, that are composed of several <em>functions</em>, and then run them. Each of the functions have its own input, output and name, which is the name of the program itself followed by a slash followed by the name of the function.</p>

<p>For instance, here is a short program demoing many of Ramen's features:</p>

<pre>
-- Single line comments start with a double dash as in SQL
PARAMETERS max_response_time DEFAULT 1.5s
       AND time_step DEFAULT 1m;
  -- Parameters can be overridden when starting a program.
  -- "1.5s" and "1m" mean "1.5 second" and "1 minute".

DEFINE web_resp_times AS
  SELECT
    min start AS start,
    max stop AS stop,
      -- By default, any field named start/stop are the start and stop unix
      -- timestamps of the described event.
    hostname,
    stop - start AS response_time
  FROM services/web/http_logs
    -- services/web/http_logs is the name of the function http_logs that's
    -- part of another program named services/web
  WHERE ip_server IN 192.168.42.0/24
  GROUP BY hostname
  COMMIT WHEN in.start > group.start + time_step + 15s;
    -- Assuming all events start timestamps are soft-ordered with max 15s
    -- of jitter, we can commit a group when we receive an input which
    -- start time is later than the end of the group by 15s.

DEFINE alert_on_slow_resp_times AS
  FROM web_resp_times
  WHERE response_time > max_response_time
  NOTIFY "Slow Response";
</pre>

<h2>Compilation</h2>

<p>To compile the above example program, Ramen needs to be given the name of the program (or it will have to infer it from the file name) and also where to find the binary of the program <code>services/web</code> where it will find the output type of the function <code>http_logs</code>, that it needs in order to type-check this program. It could either look for them in the running configuration or from a given path in the file-system.</p>

<p>The result of the compilation is going to be a single binary executable that embeds the code for all individual functions, as well as a few additional entry points to print various information such as the output type of the functions.</p>

<p>The running configuration is then nothing but a text file that specifies which program is supposed to be running (with what values for the parameters). <code>ramen run</code> merely edits this file. <code>ramen supervisor</code> is a daemon that will spawn one instance of the compiled binary for every of its functions, and connect them with the required ringbuffers.</p>

<p>Each instance of a compiled binary implementing a particular function is called a <em>worker</em>. So in the above example two workers will be run, one for each of the functions, each with its own input ringbuffer.<p>

<p>The supervisor daemon will also kill the workers that are no longer needed (again, <code>ramen kill</code> merely edits the running configuration).</p>

<? include 'footer.php' ?>
