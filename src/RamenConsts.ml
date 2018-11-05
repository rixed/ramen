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

    (* Metrics reported by the supervisor: *)
    let worker_crashes = "workers_crashes"
    let worker_deadloopings = "workers_deadloopings"
    let worker_count = "workers_count"
    let worker_running = "workers_running"
    let worker_sigkills = "workers_sigkills"
    let ringbuf_repairs = "workers_ringbuf_repairs"
    let outref_repairs = "workers_outref_repairs"

    (* Metrics reported by the notifier: *)
    let notifs_count = "notifs_count"
    let notifs_send_fails = "notifs_send_failures"
    let team_fallbacks = "notifs_team_fallbacks"
    let notifs_cancelled = "notifs_cancelled"

    (* Metrics reported by the HTTP server: *)
    let requests_count = "http_requests_count"
    let http_resp_time = "http_resp_time"

    (* Metrics reported by the compiler: *)
    let compiler_typing_time = "compiler_typing_time"
    let compiler_typing_count = "compiler_typing_count"
  end

  (* Only required when the doc is used in more than one place: *)
  module Docs =
  struct
    let in_tuple_count =
      "Number of received tuples that have been processed since the \
       operation started."
    let selected_tuple_count =
      "Number of received tuples that have passed the WHERE filter since the \
       operation started."
    let out_tuple_count =
      "Number of tuples sent to each child of this operation since it \
       started."
    let group_count =
      "Number of groups currently maintained."
    let cpu_time =
      "Total CPU time spent in this operation so far."
    let ram_usage =
      "Number of bytes currently allocated in the heap."
    let max_ram_usage =
      "Maximum observed value of ram_usage."
    let rb_read_bytes =
      "Number of bytes read from the input ring buffers so far."
    let rb_write_bytes =
      "Number of bytes written in output ring buffers so far."
    let rb_wait_read =
      "Total number of seconds spent waiting for input to refill."
    let rb_wait_write =
      "Total number of seconds spent waiting for output to empty."
    let last_out =
      "When was the last output emitted."
    let event_time =
      "Current, minimum and maximum event start time emitted so far."
  end
end

module CliInfo =
struct
  (* Commands *)
  let supervisor = "Start the processes supervisor"
  let httpd = "Start an HTTP server"
  let notifier = "Start the notifier"
  let notify = "Send a notification"
  let compile = "Compile each given source file into an executable"
  let run = "Run one (or several) compiled program(s)"
  let kill = "Stop a program"
  let tail = "Display the last outputs of an operation"
  let timeseries = "Extract a timeseries from an operation"
  let timerange =
    "Retrieve the available time range of an operation output"
  let ps = "Display info about running programs"
  let test = "Test a configuration against one or several tests"
  let dequeue = "Dequeue a message from a ringbuffer"
  let summary = "Dump info about a ring-buffer"
  let repair = "Repair a ringbuf header, assuming no readers/writers"
  let links = "List all in use ring buffers with some statistics"
  let variants = "Display the experimenter identifier and variants"
  let autocomplete = "Autocomplete the given command"
  let gc = "Delete old or unused files"
  let stats = "Display internal statistics"

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
  let daemonize = "Daemonize"
  let to_stdout = "Log onto stdout/stderr instead of a file"
  let to_syslog = "log using syslog"
  let max_archives =
    "Max number of archive files to keep per operation; \
     0 would disable archiving altogether"
  let loop =
    "Do not return after the GC is over. Instead, wait for the specified \
     amount of time and restart"
  let dry_run = "Just display what would be deleted"
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
  let param = "Override parameter's P default value with V"
  let program_globs = "Program names"
  let root_path = "Path where to find other programs"
  let src_files = "Source files to compile"
  let replace =
    "If a program with the same name is already running, replace it"
  let as_ = "name under which to run this program"
  let src_file = "file from which the worker can be rebuilt"
  let bin_file = "Binary file to run"
  let pretty = "Prettier output"
  let with_header = "Output the header line in CSV"
  let csv_separator = "Field separator"
  let csv_null = "Representation of NULL values"
  let csv_raw = "Do not quote values"
  let last =
    "Output only the last N tuples (or only the next -N, if N is negative)"
  let min_seq = "Output only tuples with greater sequence number"
  let max_seq = "Output only tuples with smaller sequence number"
  let continuous = "Wait for more when end of file is reached"
  let where = "Output only tuples which given field match the given value"
  let factors =
    "specify which fields to use as factors/categorical variables"
  let with_seqnums = "Prepend tuples with their sequence number"
  let with_event_time = "Prepend tuples with their event time"
  let func_name = "Operation unique name"
  let duration =
    "Operation will stop archiving its output after that duration if \
     nobody ask for it"
  let since = "Timestamp of the first point"
  let until = "Timestamp of the last point"
  let num_points = "Number of points returned"
  let time_step = "Duration between two points"
  let data_fields = "Fields to retrieve values from"
  let consolidation = "Consolidation function"
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
  let program_name = "resulting program name"
  let parents_from_rc =
    "get parents from the running programs rather than from the file system."
  let no_abbrev = "do not abbreviate path names"
  let show_all = "display information on all links"
  let as_tree = "display links as a tree (imply --show-all)"
  let graphite = "Impersonate graphite for Grafana"
  let api = "Implement ramen API over http"
  let fault_injection_rate = "Rate at which to generate fake errors"
  let purge = "Also remove the program from the configuration"
end

module ExitCodes =
struct
  let terminated = 0
  let interrupted = 1
  let lwt_uncaught_exception = 2 (* This does not depend on us *)
  let cannot_parse_param = 3
  let watchdog = 4
end

module Default =
struct
  (* Where to store all of daemons+workers state and logs: *)
  let persist_dir = "/tmp/ramen"

  (* The default notifier configuration: *)
  let notif_conf_file = "/tmp/ramen/notifier.conf"

  (* How frequently shall workers emit their instrumentation (seconds): *)
  let report_period = 30.

  (* Rate of fake errors in HTTP service: *)
  let fault_injection_rate = 0.000_001

  (* Max false-positive rate for notifications: *)
  let max_fpr = 1. /. 600.

  (* Every started program initially archive its output for that long: *)
  let initial_export_duration = 600.

  (* When asking for a timeseries or any other export, export for that long: *)
  let export_duration = 600.
end

(* What we use as workers argv.(0) to make it easier to read ps/top
 * output: *)
let worker_argv0 = "ramen worker:"

(* Number of seconds we keep cached factors after new tuples have been
 * produced (seconds): *)
let cache_factors_ttl = 30.

(* Prefix to use before all parameter names in the environment: *)
let param_envvar_prefix = "param_"

(* Prefix to use before all experiment names in the environment: *)
let exp_envvar_prefix = "experiment_"
