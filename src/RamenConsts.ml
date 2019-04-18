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
    let group_count = "group_count"
    let cpu_time = "cpu_time"
    let ram_usage = "ram_usage"
    let ram_usage = "ram_usage"
    let rb_wait_read = "in_sleep"
    let rb_wait_write = "out_sleep"
    let rb_read_bytes = "in_bytes"
    let rb_write_bytes = "out_bytes"
    let last_out = "last_out"
    let event_time = "event_time"
    let avg_full_out_bytes = "avg_full_out_bytes"
    let relocated_groups = "relocated_groups"
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
    let worker_count = "workers_count"
    let worker_running = "workers_running"
    let worker_sigkills = "workers_sigkills"
    let ringbuf_repairs = "workers_ringbuf_repairs"
    let outref_repairs = "workers_outref_repairs"
    let replayer_crashes = "replayers_crashes"
    let replayer_sigkills = "replayers_sigkills"
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
    let rb_read_bytes =
      "Number of bytes read from the input ring buffers so far (for the \
       live channel only)."
    let rb_write_bytes =
      "Number of bytes written in all output ring buffers since that worker \
       has started (regardless of the channel)."
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
  let supervisor = "Start the processes supervisor"
  let httpd = "Start an HTTP server"
  let alerter = "Start the alerter"
  let tunneld = "Start the tuple forward service"
  let notify = "Send a notification"
  let compile = "Compile each given source file into an executable"
  let run = "Run one (or several) compiled program(s)"
  let info = "Print all meta information about a worker"
  let kill = "Stop a program"
  let tail = "Display the last outputs of an operation"
  let replay = "Rebuild the past output of the given operation"
  let timeseries = "Extract a time series from an operation"
  let timerange =
    "Retrieve the available time range of an operation output"
  let ps = "Display info about running programs"
  let profile = "Display profiling information about running programs"
  let test = "Test a configuration against one or several tests"
  let dequeue = "Dequeue a message from a ringbuffer"
  let summary = "Dump info about a ring-buffer"
  let repair = "Repair a ringbuf header, assuming no readers/writers"
  let links = "List all in use ring buffers with some statistics"
  let variants = "Display the experimenter identifier and variants"
  let autocomplete = "Autocomplete the given command"
  let gc = "Delete old or unused files"
  let stats = "Display internal statistics"
  let archivist = "Allocate disk for storage"

  (* Options *)
  let help = "Show manual page"
  let debug = "Increase verbosity"
  let quiet = "Decrease verbosity"
  let version = "Show version number"
  let persist_dir = "Directory where are stored data persisted on disc"
  let initial_export_duration = "How long to export a node output after \
    startup before a client asks for it"
  let rand_seed =
    "Seed to initialize the random generator with \
     (will use a random one if unset)"
  let keep_temp_files = "Keep temporary files"
  let variant = "Force variants"
  let site = "The name of this site"
  let daemonize = "Daemonize"
  let to_stdout = "Log onto stdout/stderr instead of a file"
  let to_syslog = "log using syslog"
  let loop =
    "Do not return after the work is over. Instead, wait for the specified \
     amount of time and restart"
  let dry_run = "Just display what would be deleted"
  let del_ratio = "Only delete that ratio of in-excess archive files"
  let compress_older = "Convert to ORC archive files older than this"
  let autoreload =
    "Should workers be automatically reloaded when the \
     binary changes? And if so, how frequently to check"
  let report_period =
    "Number of seconds between two stats report from each worker"
  let rb_file = "File with the ring buffer"
  let num_tuples = "How many entries to dequeue"
  let rb_files = "The ring buffers to display information about"
  let external_compiler =
    "Call external compiler rather than using embedded one"
  let bundle_dir =
    "Directory where to find libraries for the embedded compiler"
  let max_simult_compilations =
    "Max number of compilations to perform simultaneously"
  let smt_solver =
    "Command to run the SMT solver (with %s in place of the SMT2 file name)"
  let fail_for_good = "For tests: do not restart after a crash"
  let master  =
    "Indicates that Ramen must run in distributed mode and what sites play \
     the master role"
  let param = "Override parameter's P default value with V"
  let program_globs = "Program names"
  let lib_path = "Path where to find other programs"
  let src_files = "Source files to compile"
  let replace =
    "If a program with the same name is already running, replace it"
  let kill_if_disabled =
    "If the program is disabled by a run-if clause then kills it instead"
  let as_ = "name under which to run this program"
  let src_file = "file from which the worker can be rebuilt"
  let on_site = "sites where this worker has to run"
  let bin_file = "Binary file to run"
  let pretty = "Prettier output"
  let flush = "Flush each line to stdout"
  let with_header = "Output the header line in CSV"
  let with_units = "Add units in the header line"
  let csv_separator = "Field separator"
  let csv_null = "Representation of NULL values"
  let csv_raw = "Do not quote values"
  let last =
    "Read only the last N tuples. Applied *before* filtering."
  let next =
    "Read only up to the next N tuples. Applied *before* filtering."
  let min_seq = "Output only tuples with greater sequence number"
  let max_seq = "Output only tuples with smaller sequence number"
  let follow = "Wait for more when end of file is reached"
  let where = "Output only tuples which given field match the given value"
  let factors =
    "specify which fields to use as factors/categorical variables"
  let with_event_time = "Prepend tuples with their event time"
  let function_name = "Operation unique name"
  let timeout =
    "Operation will stop archiving its output after that duration if \
     nobody ask for it"
  let since = "Timestamp of the first point"
  let until = "Timestamp of the last point"
  let num_points = "Number of points returned"
  let time_step = "Duration between two points"
  let data_fields = "Fields to retrieve values from"
  let func_name_or_code =
    "function fully qualified name and field names, or code statement"
  let consolidation = "Consolidation function"
  let bucket_time = "Selected bucket time"
  let short = "Display only a short summary"
  let sort_col =
    "Sort the operation list according to this column \
     (first column -name- is 1, then #in is 2...)"
  let top =
    "Truncate the list of operations after the first N entries"
  let all = "List all workers, including killed ones"
  let pattern = "Display only those workers"
  let server_url = "URL to reach the HTTP service"
  let test_file = "Definition of a test to run"
  let command = "Ramen command line to be completed"
  let conffile = "configuration file"
  let max_fpr = "max global false-positive rate"
  let output_file = "compiled file (with .x extension)"
  let program_name = "resulting program name"
  let no_abbrev = "do not abbreviate path names"
  let show_all = "display information on all links"
  let as_tree = "display links as a tree (imply --show-all)"
  let graphite = "Impersonate graphite for Grafana"
  let api = "Implement ramen API over http"
  let fault_injection_rate = "Rate at which to generate fake errors"
  let purge = "Also remove the program from the configuration"
  let update_stats = "update the workers stats file"
  let update_allocs = "update the allocations file"
  let reconf_workers = "change the workers export configuration"
  let max_bytes = "How many bytes to dump from the ringbuf messages"
  let tunneld_port = "Port number for the tuple forward service"
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
  let forking_server_uncaught_exception = 5

  let string_of_code = function
    | 0 -> "terminated"
    | 1 -> "interrupted"
    | 2 -> "uncaught exception in LWT scheduler"
    | 3 -> "cannot parse a parameter"
    | 4 -> "killed by watchdog"
    | 5 -> "forking server crashed"
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

  (* How frequently to run the GC: *)
  let gc_loop = 3600.

  (* Do not delete all excess files at once but only that ratio: *)
  let del_ratio = 0.3

  (* Archive in ringbuf format older than that will be converted into ORC *)
  let compress_older = 6. *. 3600.

  (* How frequently should the archivist reallocate disk space and
   * reconfigure workers archival behavior: *)
  let archivist_loop = 3600.

  (* Autoreload every that many seconds: *)
  let autoreload = 5.

  (* Display headers every that many lines (only once on top by default;
   * 0 to disable headers) : *)
  let header_every = max_int

  (* Even when no sampling is specified, the `past` function still perform
   * sampling with this large reservoir size: (TODO: have a different
   * implementation of `past` with unlimited capacity): *)
  let past_sample_size = 10_000

  let csv_separator = ","

  (* Size (in 4-bytes words) or ringbuffer files. But see
   * https://github.com/rixed/ramen/issues/591 *)
  let ringbuffer_word_length = 1_000_000

  (* When writing an ORC file, how many lines are buffered before we flush
   * to the file: *)
  let orc_rows_per_batch = 1000
  let orc_batches_per_file = 1000

  (* Alerter: delay between first scheduling of a new alert: *)
  let init_schedule_delay = 90.

  (* Default port for the tuple forward service: *)
  let tunneld_port = 29329

  (* Number of seconds after which a replay channel will cease to conduct
   * tuples: *)
  let replay_timeout = 300.

  (* When some worker lacks stats it still needs to be allocates storage: *)
  let compute_cost = 0.5 (* 0.5s of CPU for 1s of data *)
  let recall_size = 100. (* 100 bytes of data every second *)
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
  let replay = "ramen replay: "
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

(* Max age of the archivist stat file before it's rebuilt: *)
let max_archivist_stat_file_age = 3. *. 60.

(* Well known entry points in generated code: *)
module EntryPoints =
struct
  let worker = "worker"
  let top_half = "top_half"
  let replay = "replay"
  let convert = "convert"
end
