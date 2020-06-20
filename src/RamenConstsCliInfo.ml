(* Commands *)
let supervisor = "Start the processes supervisor."
let httpd = "Start an HTTP server."
let alerter = "Start the alerter."
let tunneld = "Start the tuple forward service."
let confserver = "Start the configuration synchronization service."
let start = "Start ramen with basic configuration"
let confclient = "Confserver client to dump/read/write configuration keys."
let precompserver = "Service that precompiles (aka parse+type-check) programs."
let execompserver =
  "Service that turns precompiled programs into local executables files."
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
  "If a program with the same name is already defined, replace it."
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
let test_notifs_every =
  "Approximate delay between test notifications (<=0 to disable)."
let is_test_alert =
  "Generate a testing alert (testing alerts disappear once acked)."
let oldest_restored_site =
  "Age of the last active site (before current one was last active) to \
   be restored from the configuration snapshot (in seconds!)."
let conf_key =
  "Configuration key to read or write. Can be a glob if for reading."
let conf_value =
  "Configuration value to write at the given key."
let timeout_idle_kafka_producers =
  "How long to keep idle kafka producers."
let debounce_delay =
  "Minimum delay between a notification and the first message is sent."
let max_last_sent_kept =
  "Maximum number of previous messages to keep in order to estimate the \
   false positive rate."
let max_incident_age =
  "Maximum age for an incident after which it is automatically cancelled."
