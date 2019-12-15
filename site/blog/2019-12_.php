<? include 'header.php' ?>

<h1>Walk through the implementation of replays</h1>
<p class="date">2019-12-15</p>

<p>Another installment of Ramen implementation explanations.</p>

<p>Replays, in the context of Ramen, name the ability to inject in the stream processing some archived past data in such a way that it is possible to recompute the past values of any (possible more recent) function of these data.</p>

<p>This feature hinges on two achievements:</p>

<ol>
<li>First, Ramen must find out which function output must be archived and for how long, which is particularly tricky given the stream of processes is flexible;</li>
<li>Then, given there exist some past data that's been archived to answer the new query over the requested time range, how to compute the result from those data.</li>
</ol>

<p>In this post we are going to look only at this later point, leaving how and what to archive for a later installment.</p>

<h2>A picture is worth a thousand words...</h2>

<center><img src="blog/2019-12_.replay.svg"/></center>

<h2>But nothing beats a picture <em>and</em> a thousand words!</h2>

<p>We consider what happens when the user enter the command <code>ramen replay --since $some_date --until $some_other_date $target</code>. There are other variations similat to this one: one where the final answer is received via the configuration server rather than a ring-buffer, and one where the original replay request is a simplified one that needs to be processed by an additional actor not visible here: the replayer. But the most important concepts are already visible in this marginally simpler example.</p>

<p>To make things more interesting, we will consider that <code>$target</code>, the function we want to see the past values from, is a lazy function that is actually not running (and may not have been running during the requested time range either).</p>

<p>So, the first action performed by <code>ramen replay</code> is to go through the available archives and select what should be replayed in order to provide <code>$target</code> with the necessary data (this is the service that <code>ramen replayer</code> can perform on behalf of a "dumb" client). In this example, let's imagine that 3 data sets need from three different ancestors of <code>$target</code> need to be replayed.</p>

<p>The next thing <code>ramen replay</code> will do (❶) is to create a local ring-buffer in which to receive the result from <code>$target</code>. Notice that this limits this command usefulness to the same machine as the one where the target is (supposed to be) running. In practice this is not a problem because it's always possible to replay a local temporary function that <code>SELECT * FROM $real_target</code> instead.</p>

<p>It will then send the actual replay request to the configuration server (hereafter just "confserver"). This replay requests contains a random unique integer identifying this request, the target name, the requested time range and the name of the ancestors which archived output must be replayed, and this configuration message is immediately forwarded to all interested parties (namely, here, to the choreographer and to the supervisor.</p>

<p>On reception of that message, the supervisor does not start the replay immediately. Rather, it keeps track that those ancestors must be replayed and wait for 0.5s (<code>RamenConst.delay_before_replay</code>), in hope that other requests to replay the same data will arrive so that the archives have to be read only once. This is actually a very frequent occurrence when dashboards containing many related charts for a single time range are refreshed simultaneously.</p>

<p>In contrast, the choreographer reacts immediately on reception of the replay request. It quickly finds out that the replay <code>$target</code> is not running (it's worker's <code>in_use</code> flag is false), so it sets it to true (recursively also set it to true to all ancestors that may not also be running, but in this example let's consider all of the target ancestors were running). It thus immediately emits a new configuration message (❸) updating this worker's <code>in_use</code> flag to true.</p>

<p>This configuration message will be forwarded to the supervisor, which will therefore start a new worker process for the target (❹).</p>

<p>If this took longer than 0.5s than the replayers may have already started to write into that ring-buffer.

<p>Supervisor, after its half a second delay, will eventually spawn the three requested replayers (❺).</p>

<p>Replayers are special instances of a worker that will merely read its archives for a selected time range and inject the old tuples in the normal message streams of the normal workers, with just a specific header identifying the replay. Following workers will implicitly always group by the replay identifier so the same workers that handle normal "traffic" can also handle replayed past messages. Indeed, even if in the message sequence chart above a straight arrow (❻) connects the output of the replayers to the target input ring-buffer, in general there will be some intermediary workers in between.</p>

<p>Once a replayer has reached the end of the archives or the end of the time range, it enqueues a last message signifying that this replay is over and terminates. The supervisor will notice the exit status and perform some bookkeeping.</p>

<p>The target worker reads and process its input as usual (❼). When it started the replayers, the supervisor also had configured the target output to write messages for that replay channel into the replay final ring-buffer created by the <code>ramen replay</code> command, which therefore starts to receive the result.</p>

<p>This output configuration also specify that this replay channel is supposed to have three replayers, so that when and end-of-replay notification is received on the channel, the target worker can decrement the replayer count and eventually find out that a channel is exhausted.</p>

<p>When that happen, the worker send the configuration command to delete the replay, command which is forwarded to the client and choreographer.</p>

<p>When the replay is deleted the client knows that it is over and can dispose of its ring-buffer (❽).</p>

<p>On reception of this delete command the choreographer job is harder: given a replay is over, some lazy functions may have become useless, and it will thus reassess the whole workers graph, starting by locking all required keys in the configuration (❾).</p>

<p>After the new graph is computed, it's diffed with the current configuration, which is likely to result in the target worker becoming useless again. It will then emit a configuration message to update this worker's <code>in_use</code> flag back to false (❿).</p>

<p>This change will be picked up by the supervisor, which will therefore kill this worker process, and eventually update the statistics for this defunct worker instance.</p>

<? include 'footer.php' ?>
