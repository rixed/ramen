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

module MetricNames =
struct
  let in_tuple_count = "in_tuple_count"
  let selected_tuple_count = "selected_tuple_count"
  let out_tuple_count = "out_tuple_count"
  let group_count = "group_count"
  let cpu_time = "cpu_time"
  let ram_usage = "ram_usage"
  let rb_wait_read = "in_sleep"
  let rb_wait_write = "out_sleep"
  let rb_read_bytes = "in_bytes"
  let rb_write_bytes = "out_bytes"
end

module CliInfo =
struct
  (* Commands *)
  let supervisor = "Start the processes supervisor"
  let graphite = "Start a Graphite impersonator"
  let notifier = "Start the notifier"
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
  let autocomplete = "Autocomplete the given command"

  (* Options *)
  let help = "Show manual page"
  let debug = "Increase verbosity"
  let version = "Show version number"
  let persist_dir = "Directory where are stored data persisted on disc"
  let rand_seed =
    "Seed to initialize the random generator with. \
     (will use a random one if unset)"
  let keep_temp_files = "Keep temporary files"
  let daemonize = "Daemonize"
  let to_stdout = "Log onto stdout/stderr instead of a file"
  let max_archives =
    "Max number of archive files to keep per operation; \
     0 would disable archiving altogether"
  let autoreload =
    "Should workers be automatically reloaded when the \
     binary changes? And if so, how frequently to check"
  let report_period =
    "Number of seconds between two stats report from each worker"
  let rb_file = "File with the ring buffer"
  let nb_tuples = "How many entries to dequeue"
  let rb_files = "The ring buffers to display information about"
  let external_compiler =
    "Call external compiler rather than using embedded one"
  let bundle_dir =
    "Directory where to find libraries for the embedded compiler"
  let max_simult_compilations =
    "Max number of compilations to perform simultaneously"
  let param = "Override parameter's P default value with V"
  let program_name = "Program unique name"
  let root_path = "Path where to find other programs"
  let source_files = "Source files to compile"
  let bin_files = "Binary files to run"
  let with_header = "Output the header line in CSV"
  let csv_separator = "Field separator"
  let csv_null = "Representation of NULL values"
  let last =
    "Output only the last N tuples (or only the next -N, if N is negative)"
  let min_seq = "Output only tuples with greater sequence number"
  let max_seq = "Output only tuples with smaller sequence number"
  let continuous = "Wait for more when end of file is reached"
  let where = "Output only tuples which given field match the given value"
  let with_seqnums = "Prepend tuples with their sequence number"
  let func_name = "Operation unique name"
  let duration =
    "Operation will stop archiving its output after that duration if \
     nobody ask for it"
  let since = "Timestamp of the first point"
  let until = "Timestamp of the last point"
  let max_nb_points = "Max number of points returned"
  let data_field = "Field to retrieve values from"
  let consolidation = "Consolidation function"
  let short = "Display only a short summary"
  let sort_col =
    "Sort the operation list according to this column \
     (first column -name- is 1, then #in is 2...)"
  let top =
    "Truncate the list of operations after the first N entries"
  let port = "Port number where to listen to incoming HTTP connections"
  let test_files = "Definition of a test to run"
  let command = "Ramen command line to be completed"
end

module ExitCodes =
struct
  let lwt_uncaught_exception = 2
  let cannot_parse_param = 3
end

let default_persist_dir = "/tmp/ramen"
let default_report_period = 30.
let worker_argv0 = "ramen worker:"
