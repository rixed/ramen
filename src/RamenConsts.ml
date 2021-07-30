module N = RamenName

(* Number of seconds we keep cached factors after new tuples have been
 * produced (seconds): *)
let cache_factors_ttl = 30.

(* Prefix to use before all parameter names in the environment: *)
let param_envvar_prefix = "param_"

(* Prefix to use before all experiment names in the environment: *)
let exp_envvar_prefix = "experiment_"

(* Lifespan of files caching each factor possible values: *)
let possible_values_lifespan = 24. *. 3600. (* FIXME: configurable? *)

(* Used to name factor possible value files in absence of event times: *)
let end_of_times = 5017590000.

(* From time to time every workers compute the size of a full out tuple,
 * to produce stats for the archivist. This is not necessary to do this
 * for every output tuple though, as all we need is a rough estimate of
 * produced output size. So do not do this measurement if one has been done
 * that recently: *)
let min_delay_between_full_out_measurement = 3.

(* Minimum duration between the publication of two tuples by a worker to the
 * conf-server: *)
let min_delay_between_publish = 1.

(* Max age of the archivist stat file before it's rebuilt: *)
let max_archivist_stat_file_age = 3. *. 60.

(* Number of fields in the special (hackish) fieldmask that's used to
 * mean "all fields": *)
let num_all_fields = 1000

(* Time to wait before a replay is created and the workers are actually
 * spawned. The longer and the more likely other replays can be served by
 * the same replayer. But that's also to be added to the response time of any
 * query. Also, beware that most web clients will wait when too many queries
 * are in flight so at some point there is little benefit to wait longer on
 * the server side. *)
let delay_before_replay = 0.5

(* Max number of last tuples kept for each worker in the config tree: *)
let max_last_tuples = 10

(* OCaml stdlib Random won't go further: *)
let max_int_for_random = 0x3FFFFFFF

(* Number of seconds to wait between the last conf update impacting the
 * allocations and their recomputation. Must be shorter than expected
 * update rate but above interval during burst. *)
let archivist_settle_delay = 1.

(* Do not attempt to realloc if last reallocation was less than that number
 * of seconds ago: *)
let min_duration_between_storage_alloc = 600.

(* Do realloc regardless of archivist_settle_delay if the last relloc is older
 * than that: *)
let max_duration_between_storage_alloc = 3600.

(* Similarly, do not attempt to reconfigure workers out-ref for archival
 * unless that number of seconds have passed: *)
let min_duration_between_archive_reconf = 60.

(* Size of the largest tuple that can be read from external source:
 * Note: Must be less than read_buffer_size. *)
let max_external_msg_size = 200_000

(* Size of the allocated circular buffer to read external data sources: *)
let read_buffer_size = 500_000

(* Timeout used when calling rd_kafka_consume_queue.
 * Must not be too high or those workers will take a long time to kill: *)
let kafka_consume_timeout = 0.3

(* We take a single list of options for both the consumer and the topic.
 * Like in kafkacat, options starting with "topic." are assumed to be for
 * the topic. *)
let kafka_topic_option_prefix = "topic."

(* Minimum number of seconds in between two attempt to synchronize running
 * workers with the shared configuration: *)
let delay_between_worker_syncs = 1.

(* How long to quarantine a poisonous key when synchronizing workers *)
let worker_quarantine_delay = 30.

(* Maximum duration to skip output to some ringbuf *)
let max_ringbuf_quarantine = 30.

(* Timeout any http command after that number of seconds: *)
let httpd_cmd_timeout = 300.

(* How frequently shall workers write their instrumentation in the dedicated
 * ring-buffer (seconds).
 * This is no longer configurable and is scheduled to be removed any time soon,
 * replaced by stats on the confserver: *)
let report_period_rb = 300.

(* Helpers.cached facility cache duration: *)
let cache_clean_after = 1200.

(* Replayer send tuples by batch. Max number of tuples in such a Value.Tuples
 * sync value: *)
let max_tuples_per_batch = 100

(* When no team name match a notification name, assign the notification
 * preferably to a team named: *)
let default_team_name = "default"

(* How often execompserver should check if the compiled binaries are still
 * present on disc (secs): *)
let check_binaries_on_disk_every = 10.
