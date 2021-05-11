module N = RamenName

(* Where to store all of daemons+workers state and logs: *)
let persist_dir = N.path "/tmp/ramen"

(* How frequently shall workers emit their instrumentation (seconds): *)
let report_period = 30.

(* Rate of fake errors in HTTP service: *)
let fault_injection_rate = 0.000_001

(* Max false-positive rate for notifications: *)
let max_fpr = 1. /. 600.

(* Every started program initially archive its output for that long: *)
let initial_export_duration = 0.

(* When asking for a time series or a tail, export for that long: *)
let export_duration = 600.

(* Special case for when that's the archivist asking: *)
let archivist_export_duration = 3600.

(* Minimum delay between two successive stats of the out-ref file: *)
let min_delay_restats = 0.1

(* How long to sleep between two GC passes: *)
let gc_loop = 180.

(* Do not delete all excess files at once but only that ratio: *)
let del_ratio = 0.3

(* Archive in ringbuf format older than that will be converted into ORC *)
let compress_older = 6. *. 3600.

(* How frequently should the archivist reallocate disk space and
 * reconfigure workers archival behavior: *)
let archivist_loop = 600.

(* Display headers every that many lines (only once on top by default;
 * 0 to disable headers) : *)
let header_every = max_int

(* Even when no sampling is specified, the `past` function still perform
 * sampling with this large reservoir size: (TODO: have a different
 * implementation of `past` with unlimited capacity): *)
let past_sample_size = 10_000

let csv_separator = ','
let csv_null = ""
let csv_true = "true"
let csv_false = "false"

(* Size (in 4-bytes words) or ringbuffer files. But see
 * https://github.com/rixed/ramen/issues/591 *)
let ringbuffer_word_length = 1_000_000

(* How long to keep retrying on NoMoreRoom before quarantining.
 * ramen test uses longer timeout. *)
let ringbuffer_timeout = 5.

(* When writing an ORC file, how many lines are buffered before we flush
 * to the file: *)
let orc_rows_per_batch = 1000
let orc_batches_per_file = 1000

(* Alerter: delay between first scheduling of a new alert: *)
let init_schedule_delay = 30.

(* Default port for the tuple forward service: *)
let tunneld_port = 29329

(* Default binding option for the secure config sync service: *)
let confserver_port_sec = 29341
let confserver_port_sec_str = string_of_int confserver_port_sec

(* Default binding option for the insecure (but faster) config sync service: *)
let confserver_port = 29340
let confserver_port_str = "127.0.0.1:" ^ string_of_int confserver_port

(* After that many seconds of inactivity, a remote user will be deleted: *)
let sync_sessions_timeout = 300.

(* Internal services uses a longer timeout: *)
let sync_long_sessions_timeout = 1200.

(* Number of seconds after which a replay channel will cease to conduct
 * tuples: *)
let replay_timeout = 300.

(* When some worker lacks stats it still needs to be allocated storage: *)
let compute_cost = 0.5 (* 0.5s of CPU for 1s of data *)
let recall_size = 100. (* 100 bytes of data every second *)

(* Default query period when unspecified in a retention configuration: *)
let query_period = 600.

(* Default lock timeout: *)
let sync_lock_timeout = 2.

(* Lock timeout used when compiling a program: *)
let sync_compile_timeo = 240.

(* Where site is not given, there is no HOSTNAME envvar and the hostname
 * command cannot be run, then we go by that modest name: *)
let site_name = N.site "master"

(* Default archive total size: *)
let archive_total_size = 1073741824

(* Default archive recall cost (vs. recomputing from source).
 * Should be << 1. 0 implies no recomputation will happen, archivist
 * then obeying blindly the persist clauses: *)
let archive_recall_cost = 0.

(* Max number of global variables in use in the whole Ramen instance.
 * Translate into LMDB maxdbs parameter. *)
let max_global_variables = 10

(* How frequently to send test notifications, when the actual period
 * is not provided (default is to never send those test notifications
 * if no option is passed. *)
let test_notifs_every = 3600.

(* By default we do not restore any other sites than the current one
 * (does _not_ suit multi-site settings obviously) *)
let oldest_restored_site = 0.

(* Idle kafka producers are deleted after this duration: *)
let timeout_idle_kafka_producers = 24. *. 3600.

(* Min debounce delay that the alerter will wait before scheduling the first
 * emission of a message after a notification is received: *)
let debounce_delay = 10.

(* How many past messages to keep in order to estimate the FPR: *)
let max_last_incidents_kept = 100

(* Incidents will be automatically cancelled after that duration: *)
let max_incident_age = 24. *. 3600.

(* Internal clock of the alerter to process the event heap. The longer the nicer
 * for resources, but alerter won't be snappier than this when scheduling
 * events: *)
let reschedule_clock = 15.

(* How many resolved past incidents to keep in the configuration tree: *)
let incidents_history_length = 500

(* How long must execompserer wait before retrying to compile a program after
 * a compilation error: *)
let execomp_quarantine = 300.
