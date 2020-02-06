module N = RamenName

module ContentTypes =
struct
  let json = "application/json"
  let dot = "text/vnd.graphviz"
  let mermaid = "text/x-mermaid"
  let text = "text/plain"
  let html = "text/html"
  let css = "text/css"
  let svg = "image/svg+xml"
  let js = "application/javascript"
  let ocaml_marshal_type = "application/marshaled.ocaml"
  let urlencoded = "application/x-www-form-urlencoded"
  let sqlite = "application/x-sqlite3"
end

module Metric =
struct
  module Names =
  struct
    (* Metrics reported by workers: *)
    let in_tuple_count = "in_tuple_count"
    let selected_tuple_count = "selected_tuple_count"
    let out_tuple_count = "out_tuple_count"
    let firing_notif_count = "firing_notif_count"
    let extinguished_notif_count = "extinguished_notif_count"
    let group_count = "group_count"
    let cpu_time = "cpu_time"
    let ram_usage = "ram_usage"
    let ram_usage = "ram_usage"
    let rb_wait_read = "in_sleep"
    let rb_wait_write = "out_sleep"
    let worker_read_bytes = "in_bytes"
    let worker_write_bytes = "out_bytes"
    let last_out = "last_out"
    let event_time = "event_time"
    let avg_full_out_bytes = "avg_full_out_bytes"
    let relocated_groups = "relocated_groups"
    let num_subscribers = "subscribers"
    let num_sync_msgs_in = "sync_msgs_in"
    let num_sync_msgs_out = "sync_msgs_out"
    let sync_resp_time = "sync_resp_time"
    let num_rate_limited_unpublished = "rate_limited_unpublished"
    let perf_per_tuple = "perf_per_tuple"
    let perf_where_fast = "perf_where_fast"
    let perf_find_group = "perf_find_group"
    let perf_where_slow = "perf_where_slow"
    let perf_update_group = "perf_update_group"
    let perf_commit_incoming = "perf_commit_incoming"
    let perf_select_others = "perf_select_others"
    let perf_finalize_others = "perf_finalize_others"
    let perf_commit_others = "perf_commit_others"
    let perf_flush_others = "perf_flush_others"

    (* Metrics reported by the supervisor: *)
    let worker_crashes = "workers_crashes"
    let worker_deadloopings = "workers_deadloopings"
    let worker_sigkills = "workers_sigkills"
    let ringbuf_repairs = "workers_ringbuf_repairs"
    let outref_repairs = "workers_outref_repairs"
    let replayer_crashes = "replayers_crashes"
    let replayer_sigkills = "replayers_sigkills"
    let chans_per_replayer = "channels_per_replayer"
    let worker_graph_build_time = "workers_graph_build_time"

    (* Metrics reported by the alerter: *)
    let messages_count = "messages_count"
    let messages_send_fails = "notifs_send_failures"
    let team_fallbacks = "notifs_team_fallbacks"
    let messages_cancelled = "messages_cancelled"

    (* Metrics reported by the HTTP server: *)
    let requests_count = "http_requests_count"
    let http_resp_time = "http_resp_time"

    (* Metrics reported by the compiler: *)
    let compiler_typing_time = "compiler_typing_time"
    let compiler_typing_count = "compiler_typing_count"

    (* Metrics reported by the copy service: *)
    let copy_server_accepts = "copy_server_accepts"
    let copy_server_tuples = "copy_server_tuples"
    let copy_client_connects = "copy_client_connects"
    let copy_client_tuples = "copy_client_tuples"

    (* Metrics reported by the confserver: *)
    let sync_session_count = "sync_sessions_count"
    let sync_user_count = "sync_users_count"
    let sync_subscription_count = "sync_subscriptions_count"
    let sync_sent_msgs = "sync_sent_msgs"
    let sync_sent_bytes = "sync_sent_bytes"
    let sync_recvd_msgs = "sync_recvd_msgs"
    let sync_recvd_bytes = "sync_recvd_bytes"
    let sync_bad_recvd_msgs = "sync_bad_recvd_msgs"
  end

  (* Only required when the doc is used in more than one place: *)
  module Docs =
  struct
    let in_tuple_count =
      "Number of received tuples since the worker has started (live channel \
       only)."
    let selected_tuple_count =
      "Number of received tuples that have passed the WHERE filter since \
       the worker has started (live channel only)."
    let out_tuple_count =
      "Number of tuples sent to each child of this worker since it \
       started (on live channel)."
    let firing_notif_count =
      "Number of notifications sent for a firing alert."
    let extinguished_notif_count =
      "Number of notifications sent for a no-longer firing alert."
    let group_count =
      "Number of groups currently maintained (for the live channel)."
    let cpu_time =
      "Total CPU time spent in this worker so far (regardless of the \
       channel)."
    let ram_usage =
      "Number of bytes currently allocated in the heap for that worker \
       (regardless of the channel)."
    let max_ram_usage =
      "Maximum observed value of ram_usage."
    let worker_read_bytes =
      "Number of bytes read from the input ring buffers so far (for the \
       live channel only)."
    let worker_write_bytes =
      "Number of bytes written in all output ring buffers or output socket \
       (for top-halves) since that worker has started (regardless of the \
       channel)."
    let rb_wait_read =
      "Total number of seconds spent waiting for the input to refill."
    let rb_wait_write =
      "Total number of seconds spent waiting for the output to empty."
    let last_out =
      "When was the last output emitted (for the live channel only)."
    let event_time =
      "Last, minimum and maximum (since startup) event time emitted \
       (for the live channel only)."
    let avg_full_out_bytes = "Average size of a fully-fledged out tuple."
    let relocated_groups =
      "How many times a group was moved in the commit precondition heap."
    let num_subscribers = "Number of tail-subscribers"
    let num_sync_msgs_in = "Number of received synchronisation messages"
    let num_sync_msgs_out = "Number of emitted synchronisation messages"
    let num_rate_limited_unpublished =
      "Number of tuples that were unpublished due to rate limit"
    let perf_per_tuple = "Average time spent processing an incoming tuples."
    let perf_where_fast =
      "Average time spent executing the (fast) WHERE clause for each \
       incoming tuple."
    let perf_find_group =
      "Average time spent looking for the group for each incoming tuple."
    let perf_where_slow =
      "Average time spent executing the (slow) WHERE clause for each \
       incoming tuple."
    let perf_update_group =
      "Average time spent updating the aggregate group for each incoming \
       tuple."
    let perf_commit_incoming =
      "Average time spent committing the incoming tuple."
    let perf_select_others =
      "Average time spent looking for other tuples to commit, for each \
       incoming one."
    let perf_finalize_others =
      "Average time spent finalizing the values to output for other tuples, \
       for each incoming one."
    let perf_commit_others =
      "Average time spent committing other tuples, for each \
       incoming one."
    let perf_flush_others =
      "Average time spent flushing other tuples, for each incoming one."
    let profile = "Performance measurements."
    let copy_client_connects = "Number of attempted connections."
    let copy_client_tuples = "Number of sent tuples."
  end
end

module CliInfo =
struct
  (* Commands *)
  let supervisor = "Start the processes supervisor."
  let httpd = "Start an HTTP server."
  let alerter = "Start the alerter."
  let tunneld = "Start the tuple forward service."
  let confserver = "Start the configuration synchronization service."
  let confclient = "Test client for the confserver."
  let compserver = "Service that (pre)compiles programs."
  let choreographer = "Service that decides where to run what."
  let replay_service =
    "Service to turn simple replay requests into query-plans \
     that can be understood by supervisor."
  let notify = "Send a notification."
  let compile = "Compile each given source file into an executable."
  let run = "Run one (or several) compiled program(s)."
  let info = "Print all meta information about a worker."
  let kill = "Stop a program."
  let tail = "Display the last outputs of an operation."
  let replay = "Rebuild the past output of the given operation."
  let timeseries = "Extract a time series from an operation."
  let ps = "Display info about running programs."
  let profile = "Display profiling information about running programs."
  let test = "Test a configuration against one or several tests."
  let dequeue = "Dequeue a message from a ringbuffer."
  let summary = "Display informations about a ring-buffer."
  let dump = "Dump some bytes from the ringbuffer."
  let repair = "Repair a ringbuf header, assuming no readers/writers."
  let links = "List all in use ring buffers with some statistics."
  let variants = "Display the experimenter identifier and variants."
  let autocomplete = "Autocomplete the given command."
  let gc = "Delete old or unused files."
  let stats = "Display internal statistics."
  let archivist = "Allocate disk for storage."
  let useradd =
    "Create a user that will be allowed to connect to the confserver and \
     generate his/her secret key."
  let userdel = "Delete a user."
  let usermod = "Change the roles assumed by a user."

  (* Options *)
  let help = "Show manual page."
  let debug = "Increase verbosity."
  let quiet = "Decrease verbosity."
  let version = "Show version number."
  let persist_dir = "Directory where are stored data persisted on disc."
  let initial_export_duration = "How long to export a node output after \
    startup before a client asks for it."
  let rand_seed =
    "Seed to initialize the random generator with \
     (will use a random one if unset)."
  let keep_temp_files = "Keep temporary files."
  let reuse_prev_files = "Reuse existing source files."
  let variant = "Force variants."
  let site = "The name of this site."
  let daemonize = "Daemonize."
  let to_stdout = "Log onto stdout/stderr instead of a file."
  let to_syslog = "log using syslog."
  let prefix_log_with_name =
    "Prefix every log lines with the service name. Comes handy when all logs \
     are scrambled together, as in the output of docker logs."
  let loop =
    "Do not return after the work is over. Instead, wait for the specified \
     amount of time and restart."
  let dry_run = "Just display what would be deleted."
  let del_ratio = "Only delete that ratio of in-excess archive files."
  let compress_older = "Convert to ORC archive files older than this."
  let report_period =
    "Number of seconds between two stats report from each worker."
  let rb_file = "File with the ring buffer."
  let num_tuples = "How many entries to dequeue."
  let rb_files = "The ring buffers to display information about."
  let external_compiler =
    "Call external compiler rather than using embedded one."
  let bundle_dir =
    "Directory where to find libraries for the embedded compiler."
  let max_simult_compilations =
    "Max number of compilations to perform simultaneously."
  let smt_solver =
    "Command to run the SMT solver (with %s in place of the SMT2 file name)."
  let fail_for_good = "For tests: do not restart after a crash."
  let kill_at_exit = "For tests: SIGKILL all workers at exit."
  let master  =
    "Indicates that Ramen must run in distributed mode and what sites play \
     the master role."
  let param = "Override parameter's P default value with V."
  let program_globs = "Program names."
  let lib_path = "Path where to find other programs."
  let src_files = "Source files to compile."
  let replace =
    "If a program with the same name is already running, replace it."
  let kill_if_disabled =
    "If the program is disabled by a run-if clause then kills it instead."
  let as_ = "Name under which to run this program."
  let src_file = "File from which the worker can be rebuilt."
  let on_site = "Sites where this worker has to run."
  let cwd = "Working directory for that worker."
  let bin_file = "Ramen worker executable file."
  let pretty = "Prettier output."
  let flush = "Flush each line to stdout."
  let with_header = "Output the header line in CSV."
  let with_units = "Add units in the header line."
  let csv_separator = "Field separator."
  let csv_null = "Representation of NULL values."
  let csv_raw = "Do not quote values."
  let last =
    "Read only the last N tuples. Applied *before* filtering."
  let next =
    "Read only up to the next N tuples. Applied *before* filtering."
  let follow = "Wait for more when end of file is reached."
  let where = "Output only tuples which given field match the given value."
  let factors =
    "Specify which fields to use as factors/categorical variables."
  let with_event_time = "Prepend tuples with their event time."
  let function_name = "Operation unique name."
  let timeout =
    "Operation will stop to archive its output after that duration if \
     nobody ask for it."
  let since = "Timestamp of the first point."
  let until = "Timestamp of the last point."
  let num_points = "Number of points returned."
  let time_step = "Duration between two points."
  let data_fields = "Fields to retrieve values from."
  let via =
    "Use either local file or remote confserver to retrieve the tuples."
  let func_name_or_code =
    "function fully qualified name and field names, or code statement."
  let consolidation = "Consolidation function."
  let bucket_time = "Selected bucket time."
  let short = "Display only a short summary."
  let sort_col =
    "Sort the operation list according to this column \
     (first column -name- is 1, then #in is 2 etc)."
  let top =
    "Truncate the list of operations after the first N entries."
  let all = "List all workers, including other sites."
  let pattern = "Display only those workers."
  let server_url = "URL to reach the HTTP service."
  let test_file = "Definition of a test to run."
  let command = "Ramen command line to be completed."
  let conffile = "Configuration file."
  let max_fpr = "Max global false-positive rate."
  let output_file = "Where to store the output."
  let program_name = "Resulting program name."
  let no_abbrev = "Do not abbreviate path names."
  let show_all_links =
    "Display information on every links including those that are OK."
  let show_all_workers =
    "List every workers mentioned in the configuration even those that are not \
     running yet."
  let as_tree = "Display links as a tree (imply --show-all)."
  let graphite = "Impersonate graphite for Grafana."
  let api = "Implement ramen API over http."
  let table_prefix = "Only consider tables under this prefix for the API."
  let fault_injection_rate = "Rate at which to generate fake errors."
  let purge = "Also remove the program from the configuration."
  let update_stats = "Update the workers stats."
  let update_allocs = "Update the allocations."
  let reconf_workers = "Change the workers export configuration."
  let max_bytes = "How many bytes to dump from the ringbuf messages."
  let start_word = "First word to dump."
  let stop_word = "First word to not dump."
  let tunneld_port = "Port number for the tuple forward service."
  let confserver_port =
    "Where to bind the configuration synchronisation service. It can be a \
     single port number, in which case all local addresses will be bound on \
     that port (equivalent of \"*:port\"), or an \"IP:port\" pair in which \
     case only that IP will be bound, or even \"itf:port\" where \"itf\" is \
     the network interface name."
  let confserver_port_sec =
    "Same as --port, but for the encrypted/authenticated variant of the \
     configuration synchronisation service. Notice that both can be run at \
     the same time, but not on the same address/port, obviously."
  let confserver_url = "host:port of Ramen confserver."
  let confserver_key = "File name where the confserver public key is stored."
  let client_pub_key = "File name where the client public key is stored."
  let client_priv_key =
    "File name where the client private key is stored. This file must not \
     be readable or writable by others."
  let server_pub_key = "File name where the server public key is stored."
  let server_priv_key =
    "File name where the server private key is stored. This file must not \
     be readable or writable by others."
  let no_source_examples =
    "Skip the insertion of source examples in the configuration tree."
  let username = "Login name to connect to the confserver."
  let role =
    "Role assumed by this user. Repeat for giving several roles to the \
     same user."
  let identity_file = "Location of the file storing user's identity"
  let rmadmin_config = "Path of RmAdmin configuration file."
  let colors = "Whether or not to use colors in terminal output."
  let default_archive_total_size =
    "How many bytes of archives to store by default."
  let default_archive_recall_cost =
    "Default cost to read archives vsrecomputing it."
end

module WorkerCommands =
struct
  let get_info = "1nf0"
  let wants_to_run = "r34dy?"
  let print_version = "version"
  let dump_archive = "dump"
  let convert_archive = "convert"
end

module ExitCodes =
struct
  (* These first 3 are not us to define: *)
  let terminated = 0
  let interrupted = 1
  let lwt_uncaught_exception = 2
  let cannot_parse_param = 3
  let watchdog = 4
  let uncaught_exception = 5
  let damaged_ringbuf = 6

  let string_of_code = function
    | 0 -> "terminated"
    | 1 -> "interrupted"
    | 2 -> "uncaught exception"
    | 3 -> "cannot parse a parameter"
    | 4 -> "killed by watchdog"
    | 5 -> "crashed"
    | 6 -> "suffered ringbuffer damage"
    | _ -> "unknown"
end

module FieldDocs =
struct
  let site = "Name of the Ramen instance that produced these statistics"
  let worker =
    "Fully qualified name of the Ramen function that these statistics are \
     about"
  let top_half =
    "True if this worker performs only the bottom half of a worker"
  let start = "When these statistics have been collected (wall clock time)"
end

module ServiceNames =
struct
  let supervisor = N.service "supervisor"
  let httpd = N.service "httpd"
  let tunneld = N.service "tunneld"
  let confserver = N.service "confserver"
  let alerter = N.service "alerter"
  let choreographer = N.service "choreographer"
  let replayer = N.service "replayer"
  let gc = N.service "gc"
  let archivist = N.service "archivist"
  let compserver = N.service "compserver"
end

module Default =
struct
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

  let csv_separator = ","
  let csv_null = ""

  (* Size (in 4-bytes words) or ringbuffer files. But see
   * https://github.com/rixed/ramen/issues/591 *)
  let ringbuffer_word_length = 1_000_000

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

  (* Default binding option for the insecure (but faster) config sync service: *)
  let confserver_port = 29340

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

  (* Lock timeout used when a human is editing the configuration: *)
  let sync_gui_lock_timeout = 600.

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
end

module SpecialFunctions =
struct
  let notifs = "notifs"
  let stats = "stats"
end

(* What we use as workers argv.(0) to make it easier to read ps/top
 * output: *)
module Worker_argv0 =
struct
  let full_worker = "ramen worker:"
  (* The top half of a worker executes the fast-where and then sends the
   * filtered tuples into the children. In the future, when we can transmit
   * aggregation state, it could go as far as aggregating.
   * The tunneld service then enqueue the tuples in the proper worker input
   * queue. *)
  let top_half = "ramen worker (top-half):"
  let replay = "ramen replay:"
end

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
let num_all_fields = 100

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

(* Size of the largest tuple that can be read from external source: *)
let max_external_msg_size = 60_000

(* Size of the allocated circular buffer to read external data sources: *)
let read_buffer_size = 200_000

(* Timeout used when calling rd_kafka_consume_queue: *)
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

(* Subdirectory name where out_ref files are stored: *)
let out_ref_subdir = N.path "workers/out_ref"

(* Timeout any http command after that number of seconds: *)
let httpd_cmd_timeout = 300.

(* Suffixes used to form the worker helper object file: *)
module ObjectSuffixes =
struct
  let orc_codec = N.path "orc_codec"
  let dessser_helper = N.path "dessser_helper"
end

(* Well known entry points in generated code: *)
module EntryPoints =
struct
  let worker = "worker"
  let top_half = "top_half"
  let replay = "replay"
  let convert = "convert"
end
