(* Options *)

(* From https://erratique.ch/software/cmdliner/doc/Cmdliner.Arg.html:
 *
 *   names defines the names under which an optional argument can be referred to.
 *   Strings of length 1 ("c") define short option names ("-c"), longer strings
 *   ("count") define long option names ("--count"). names must be empty for
 *   positional arguments.
 *
 *   env defines the name of an environment variable which is looked up for
 *   defining the argument if it is absent from the command line.
 *
 *   doc is the man page information of the argument.
 *
 *   docv is for positional and non-flag optional arguments. It is a variable
 *   name used in the man page to stand for their value.
 *)

let help = "Show manual page."
let version = "Show version number."

type opt_type = Flag | Scalar | List

type opt =
  { names : string list ;
    env : string ;
    doc : string ;
    docv : string ;
    typ : opt_type }

let persist_dir =
  { names = [ "persist-dir" ] ;
    env = "RAMEN_DIR" ;
    doc = "Directory where are stored data persisted on disc." ;
    docv = "" ;
    typ = Scalar }

let debug =
  { names = [ "debug" ; "d" ] ;
    env = "RAMEN_DEBUG" ;
    doc = "Increase verbosity." ;
    docv = "" ;
    typ = Flag }

let quiet =
  { names = [ "quiet" ; "q" ] ;
    env = "RAMEN_QUIET" ;
    doc = "Decrease verbosity." ;
    docv = "" ;
    typ = Flag }

let initial_export_duration =
  { names = [ "initial-export-duration" ] ;
    env = "RAMEN_INITIAL_EXPORT" ;
    doc = "How long to export a node output after \
           startup before a client asks for it." ;
    docv = "" ;
    typ = Scalar }

let rand_seed =
  { names = [ "rand-seed" ; "seed" ] ;
    env = "RAMEN_RANDOM_SEED" ;
    doc = "Seed to initialize the random generator with \
           (will use a random one if unset)." ;
    docv = "" ;
    typ = Scalar }

let keep_temp_files =
  { names = [ "keep-temp-files" ; "S" ] ;
    env = "RAMEN_KEEP_TEMP_FILES" ;
    doc = "Keep temporary files." ;
    docv = "" ;
    typ = Flag }

let reuse_prev_files =
  { names = [ "reuse-prev-files" ] ;
    env = "RAMEN_REUSE_PREV_FILES" ;
    doc = "Reuse existing source files." ;
    docv = "" ;
    typ = Flag }

let variant =
  { names = [ "variant" ] ;
    env = "RAMEN_VARIANTS" ;
    doc = "Force variants." ;
    docv = "" ;
    typ = List }

let site =
  { names = [ "site" ] ;
    env = "HOSTNAME" ;
    doc = "The name of this site." ;
    docv = "" ;
    typ = Scalar }

let bundle_dir =
  { names = [ "bundle-dir" ] ;
    env = "RAMEN_LIBS" ;
    doc = "Directory where to find libraries for the embedded compiler." ;
    docv = "" ;
    typ = Scalar }

let masters =
  { names = [ "masters" ] ;
    env = "RAMEN_MASTERS" ;
    doc = "Indicates that Ramen must run in distributed mode and what sites \
           play the master role." ;
    docv = "" ;
    typ = List }

let confserver_url =
  { names = [ "confserver" ] ;
    env = "RAMEN_CONFSERVER" ;
    doc = "host:port of Ramen confserver." ;
    docv = "" ;
    typ = Scalar }

let confserver_key =
  { names = [ "confserver-key" ] ;
    env = "RAMEN_CONFSERVER_KEY" ;
    doc = "File name where the confserver public key is stored." ;
    docv = "" ;
    typ = Scalar }

let username =
  { names = [ "username" ] ;
    env = "USER" ;
    doc = "Login name to connect to the confserver." ;
    docv = "" ;
    typ = Scalar }

let client_pub_key =
  { names = [ "pub-key" ] ;
    env = "RAMEN_CLIENT_PUB_KEY" ;
    doc = "File name where the client public key is stored." ;
    docv = "" ;
    typ = Scalar }

let client_priv_key =
  { names = [ "priv-key" ] ;
    env = "RAMEN_CLIENT_PRIV_KEY" ;
    doc = "File name where the client private key is stored. This file must \
           not be readable or writable by others." ;
    docv = "" ;
    typ = Scalar }

let identity_file =
  { names = [ "identity" ; "i" ] ;
    env = "RAMEN_CLIENT_IDENTITY" ;
    doc = "Location of the file storing user's identity" ;
    docv = "" ;
    typ = Scalar }

let colors =
  { names = [ "colors" ] ;
    env = "RAMEN_COLORS" ;
    doc = "Whether or not to use colors in terminal output." ;
    docv = "" ;
    typ = Scalar }

(* Color has a scalar value that's "never" or "always" and act like a boolean,
 * that is true by default: *)
let string_of_color = function
  | true -> "always"
  | false -> "never"

let daemonize =
  { names = [ "daemonize" ] ;
    env = "RAMEN_DAEMONIZE" ;
    doc = "Daemonize." ;
    docv = "" ;
    typ = Flag }

let to_stdout =
  { names = [ "to-stdout" ; "stdout" ; "to-stderr" ; "stderr" ] ;
    env = "RAMEN_LOG_TO_STDERR" ;
    doc = "Log onto stdout/stderr instead of a file." ;
    docv = "" ;
    typ = Flag }

let to_syslog =
  { names = [ "to-syslog" ; "syslog" ] ;
    env = "RAMEN_LOG_TO_SYSLOG" ;
    doc = "log using syslog." ;
    docv = "" ;
    typ = Flag }

let prefix_log_with_name =
  { names = [ "prefix-log-with-name" ] ;
    env = "RAMEN_PREFIX_LOG_WITH_NAME" ;
    doc = "Prefix every log lines with the service name. Comes handy when all \
           logs are scrambled together, as in the output of docker logs." ;
    docv = "" ;
    typ = Flag }

let external_compiler =
  { names = [ "external-compiler" ] ;
    env = "RAMEN_USE_EXTERNAL_COMPILER" ;
    doc = "Call external compiler rather than using embedded one." ;
    docv = "" ;
    typ = Flag }

let max_simult_compilations =
  { names = [ "max-simultaneous-compilations" ; "max-simult-compilations" ] ;
    env = "RAMEN_MAX_SIMULT_COMPILATIONS" ;
    doc = "Max number of compilations to perform simultaneously." ;
    docv = "" ;
    typ = Scalar }

let smt_solver =
  { names = [ "smt-solver" ; "solver" ] ;
    env = "RAMEN_SMT_SOLVER" ;
    doc = "Command to run the SMT solver (with %s in place of the SMT2 file \
           name)." ;
    docv = "" ;
    typ = Scalar }

let fail_for_good =
  { names = [ "fail-for-good" ] ;
    env = "" ;
    doc = "For tests: do not restart after a crash." ;
    docv = "" ;
    typ = Flag }

let kill_at_exit =
  { names = [ "kill-at-exit" ] ;
    env = "RAMEN_KILL_AT_EXIT" ;
    doc = "For tests: SIGKILL all workers at exit." ;
    docv = "" ;
    typ = Flag }

let test_notifs_every =
  { names = [ "test-notifs" ] ;
    env = "RAMEN_TEST_NOTIFS" ;
    doc = "Approximate delay between test notifications (<=0 to disable)." ;
    docv = "" ;
    typ = Scalar }

let loop =
  { names = [ "loop" ] ;
    env = "" ;
    doc = "Do not exit after work is over. Instead, wait for the specified \
           amount of time and restart." ;
    docv = "" ;
    typ = Scalar }

let dry_run =
  { names = [ "dry-run" ] ;
    env = "" ;
    doc = "Just display what would be deleted." ;
    docv = "" ;
    typ = Flag }

let del_ratio =
  { names = [ "del-ratio" ] ;
    env = "" ;
    doc = "Only delete that ratio of in-excess archive files." ;
    docv = "" ;
    typ = Scalar }

let compress_older =
  { names = [ "compress-older" ] ;
    env = "" ;
    doc = "Convert to ORC archive files older than this." ;
    docv = "" ;
    typ = Scalar }

let max_fpr =
  { names = [ "default-max-fpr" ; "max-fpr" ; "fpr" ] ;
    env = "ALERTER_MAX_FPR" ;
    doc = "Max global false-positive rate." ;
    docv = "" ;
    typ = Scalar }

let timeout_idle_kafka_producers =
  { names = [ "kafka-producers-timeout" ] ;
    env = "ALERTER_KAFKA_PRODUCERS_TIMEOUT" ;
    doc = "How long to keep idle kafka producers." ;
    docv = "" ;
    typ = Scalar }

let debounce_delay =
  { names = [ "debounce-delay" ] ;
    env = "ALERTER_DEBOUNCE_DELAY" ;
    doc = "Minimum delay between a notification and the first message is \
           sent." ;
    docv = "" ;
    typ = Scalar }

let max_last_sent_kept =
  { names = [ "max-last-sent-kept" ] ;
    env = "ALERTER_MAX_LAST_SENT_KEPT" ;
    doc = "Maximum number of previous messages to keep in order to estimate \
           the false positive rate." ;
    docv = "" ;
    typ = Scalar }

let max_incident_age =
  { names = [ "max-incident-age" ] ;
    env = "ALERTER_MAX_INCIDENT_AGE" ;
    doc = "Maximum age for an incident after which it is automatically \
           cancelled." ;
    docv = "" ;
    typ = Scalar }

let for_test =
  { names = [ "for-test" ] ;
    env = "" ;
    doc = "Internal special setup for running alerting tests." ;
    docv = "" ;
    typ = Flag }

let reschedule_clock =
  { names = [ "clock" ; "reschedule-clock" ] ;
    env = "ALERTER_CLOCK" ;
    doc = "Duration of the event loop." ;
    docv = "" ;
    typ = Scalar }

let parameter =
  { names = [ "parameter" ; "p" ] ;
    env = "" ;
    doc = "Override parameter's P default value with V." ;
    docv = "PARAM=VALUE" ;
    typ = List }

let is_test_alert =
  { names = [ "test" ] ;
    env = "" ;
    doc = "Generate a testing alert (testing alerts disappear once acked)." ;
    docv = "" ;
    typ = Flag }

let notif_name =
  { names = [] ;
    env = "" ;
    doc = "Notification name." ;
    docv = "NAME" ;
    typ = Scalar }

let tunneld_port =
  { names = [ "port" ; "p" ] ;
    env = "" ;
    doc = "Port number for the tuple forward service." ;
    docv = "" ;
    typ = Scalar }

let confserver_port =
  { names = [ "insecure" ; "p" ] ;
    env = "" ;
    doc = "Where to bind the configuration synchronisation service. It can \
           be a single port number, in which case all local addresses will \
           be bound on that port (equivalent of \"*:port\"), or an \
           \"IP:port\" pair in which case only that IP will be bound, or even \
           \"itf:port\" where \"itf\" is the network interface name." ;
    docv = "" ;
    typ = List }

let confserver_port_sec =
  { names = [ "secure" ; "P" ] ;
    env = "" ;
    doc = "Same as --port, but for the encrypted/authenticated variant of the \
           configuration synchronisation service. Notice that both can be run \
           at the same time, but not on the same address/port, obviously." ;
    docv = "" ;
    typ = List }

let server_priv_key =
  { names = [ "private-key" ; "K" ] ;
    env = "RAMEN_CONFSERVER_PRIV_KEY" ;
    doc = "File name where the server private key is stored. This file must \
           not be readable or writable by others." ;
    docv = "" ;
    typ = Scalar }

let server_pub_key =
  { names = [ "public-key" ; "k" ] ;
    env = "RAMEN_CONFSERVER_KEY" ;
    doc = "File name where the server public key is stored." ;
    docv = "" ;
    typ = Scalar }

let no_source_examples =
  { names = [ "no-source-examples" ; "no-examples" ] ;
    env = "RAMEN_NO_EXAMPLES" ;
    doc = "Skip the insertion of source examples in the configuration tree." ;
    docv = "" ;
    typ = Flag }

let default_archive_total_size =
  { names = [ "default-archive-total-size" ] ;
    env = "RAMEN_DEFAULT_ARCHIVE_SIZE" ;
    doc = "How many bytes of archives to store by default." ;
    docv = "" ;
    typ = Scalar }

let default_archive_recall_cost =
  { names = [ "default-archive-recall-cost" ] ;
    env = "RAMEN_DEFAULT_ARCHIVE_RECALL_COST" ;
    doc = "Default cost to read archives vsrecomputing it." ;
    docv = "" ;
    typ = Scalar }

let oldest_restored_site =
  { names = [ "oldest-restored-site" ] ;
    env = "RAMEN_OLDEST_RESTORED_SITE" ;
    doc = "Age of the last active site (before current one was last active) \
           to be restored from the configuration snapshot (in seconds!)." ;
    docv = "" ;
    typ = Scalar }

let conf_key =
  { names = [ "key" ; "k" ] ;
    env = "" ;
    doc = "Configuration key to read or write. Can be a glob if for reading." ;
    docv = "" ;
    typ = Scalar }

let conf_value =
  { names = [ "value" ; "v" ] ;
    env = "" ;
    doc = "Configuration value to write at the given key." ;
    docv = "" ;
    typ = Scalar }

let conf_delete =
  { names = [ "delete" ] ;
    env = "" ;
    doc = "Delete the configuration value identifier by the given --key." ;
    docv = "" ;
    typ = Flag }

let added_username =
  { names = [] ;
    env = "" ;
    doc = "Login name to connect to the confserver." ;
    docv = "USER" ;
    typ = Scalar }

let role =
  { names = [ "role" ; "r" ] ;
    env = "" ;
    doc = "Role assumed by this user. Repeat for giving several roles to the \
           same user." ;
    docv = "ROLE" ;
    typ = List }

let output_file =
  { names = [ "output" ; "o" ] ;
    env = "" ;
    doc = "Where to store the output." ;
    docv = "FILE" ;
    typ = Scalar }

let rb_file =
  { names = [] ;
    env = "" ;
    doc = "File with the ring buffer." ;
    docv = "FILE" ;
    typ = Scalar }

let num_entries =
  { names = [ "num-entries" ; "n" ] ;
    env = "" ;
    doc = "How many entries to dequeue." ;
    docv = "" ;
    typ = Scalar }

let max_bytes =
  { names = [ "max-bytes" ; "s" ] ;
    env = "" ;
    doc = "How many bytes to dump from the ringbuf messages." ;
    docv = "" ;
    typ = Scalar }

let rb_files =
  { names = [] ;
    env = "" ;
    doc = "The ring buffers to display information about." ;
    docv = "FILE" ;
    typ = Scalar }

let start_word =
  { names = [ "start" ; "f" ] ;
    env = "" ;
    doc = "First word to dump." ;
    docv = "" ;
    typ = Scalar }

let stop_word =
  { names = [ "stop" ; "t" ] ;
    env = "" ;
    doc = "First word to not dump." ;
    docv = "" ;
    typ = Scalar }

let pattern =
  { names = [] ;
    env = "" ;
    doc = "Display only those workers." ;
    docv = "PATTERN" ;
    typ = Scalar }

let no_abbrev =
  { names = [ "no-abbreviation" ] ;
    env = "RAMEN_NO_ABBREVIATION" ;
    doc = "Do not abbreviate path names." ;
    docv = "" ;
    typ = Flag }

let show_all_links =
  { names = [ "show-all" ; "all" ; "a" ] ;
    env = "RAMEN_SHOW_ALL" ;
    doc = "Display information on every links including those that are OK." ;
    docv = "" ;
    typ = Flag }

let show_all_workers =
  { names = [ "show-all" ; "all" ; "a" ] ;
    env = "RAMEN_SHOW_ALL" ;
    doc = "List every workers mentioned in the configuration even those that \
           are not running yet." ;
    docv = "" ;
    typ = Flag }

let as_tree =
  { names = [ "as-tree" ] ;
    env = "" ;
    doc = "Display links as a tree (imply --show-all)." ;
    docv = "" ;
    typ = Flag }

let pretty =
  { names = [ "pretty" ] ;
    env = "" ;
    doc = "Prettier output." ;
    docv = "" ;
    typ = Flag }

let with_header =
  { names = [ "with-header" ; "header" ; "h" ] ;
    env = "" ;
    doc = "Output the header line in CSV." ;
    docv = "" ;
    typ = Scalar }

let sort_col =
  { names = [ "sort" ; "s" ] ;
    env = "" ;
    doc = "Sort the operation list according to this column \
           (first column -name- is 1, then #in is 2 etc)." ;
    docv = "COL" ;
    typ = Scalar }

let top =
  { names = [ "top" ; "t" ] ;
    env = "" ;
    doc = "Truncate the list of operations after the first N entries." ;
    docv = "N" ;
    typ = Scalar }

let program_globs =
  { names = [] ;
    env = "" ;
    doc = "Program names." ;
    docv = "PROGRAM" ;
    typ = Scalar }

let lib_path =
  { names = [ "lib-path" ; "L" ] ;
    env = "RAMEN_PATH" ;
    doc = "Path where to find other programs." ;
    docv = "" ;
    typ = List }

let src_files =
  { names = [] ;
    env = "" ;
    doc = "Source files to compile." ;
    docv = "FILE" ;
    typ = Scalar }

let as_ =
  { names = [ "as" ] ;
    env = "" ;
    doc = "Name under which to run this program." ;
    docv = "NAME" ;
    typ = Scalar }

let replace =
  { names = [ "replace" ; "r" ] ;
    env = "" ;
    doc = "If a program with the same name is already defined, replace it." ;
    docv = "" ;
    typ = Flag }

let report_period =
  { names = [ "report-period" ] ;
    env = "RAMEN_REPORT_PERIOD" ;
    doc = "Number of seconds between two stats report from each worker." ;
    docv = "" ;
    typ = Scalar }

let src_file =
  { names = [] ;
    env = "" ;
    doc = "File from which the worker can be rebuilt." ;
    docv = "FILE" ;
    typ = Scalar }

let on_site =
  { names = [ "on-site" ] ;
    env = "" ;
    doc = "Sites where this worker has to run." ;
    docv = "" ;
    typ = Scalar }

let program_name =
  { names = [] ;
    env = "" ;
    doc = "Resulting program name." ;
    docv = "NAME" ;
    typ = Scalar }

let cwd =
  { names = [ "working-dir" ; "cwd" ] ;
    env = "" ;
    doc = "Working directory for that worker." ;
    docv = "" ;
    typ = Scalar }

let purge =
  { names = [ "purge" ] ;
    env = "" ;
    doc = "Also remove the program from the configuration." ;
    docv = "" ;
    typ = Flag }

let function_name =
  { names = [] ;
    env = "" ;
    doc = "Operation unique name." ;
    docv = "FUNCTION" ;
    typ = Scalar }

let bin_file =
  { names = [] ;
    env = "" ;
    doc = "Ramen worker executable file." ;
    docv = "FILE" ;
    typ = Scalar }

let csv_separator =
  { names = [ "separator" ] ;
    env = "RAMEN_CSV_SEPARATOR" ;
    doc = "Field separator." ;
    docv = "" ;
    typ = Scalar }

let csv_null =
  { names = [ "null" ] ;
    env = "RAMEN_CSV_NULL" ;
    doc = "Representation of NULL values." ;
    docv = "" ;
    typ = Scalar }

let csv_raw =
  { names = [ "raw" ] ;
    env = "" ;
    doc = "Do not quote values." ;
    docv = "" ;
    typ = Flag }

let last =
  { names = [ "last" ; "l" ] ;
    env = "" ;
    doc = "Read only the last N tuples. Applied *before* filtering." ;
    docv = "" ;
    typ = Scalar }

let next =
  { names = [ "next" ; "n" ] ;
    env = "" ;
    doc = "Read only up to the next N tuples. Applied *before* filtering." ;
    docv = "" ;
    typ = Scalar }

let follow =
  { names = [ "follow" ; "f" ] ;
    env = "" ;
    doc = "Wait for more when end of file is reached." ;
    docv = "" ;
    typ = Flag }

let where =
  { names = [ "where" ; "w" ] ;
    env = "" ;
    doc = "Output only tuples which given field match the given value." ;
    docv = "FIELD op VALUE" ;
    typ = List }

let since =
  { names = [ "since" ] ;
    env = "" ;
    doc = "Timestamp of the first point." ;
    docv = "SINCE" ;
    typ = Scalar }

let until =
  { names = [ "until" ] ;
    env = "" ;
    doc = "Timestamp of the last point." ;
    docv = "UNTIL" ;
    typ = Scalar }

let with_event_time =
  { names = [ "with-event-times" ; "with-times" ; "event-times" ; "t" ] ;
    env = "" ;
    doc = "Prepend tuples with their event time." ;
    docv = "" ;
    typ = Flag }

let timeout =
  { names = [ "timeout" ] ;
    env = "" ;
    doc = "Operation will stop to archive its output after that duration if \
           nobody ask for it." ;
    docv = "" ;
    typ = Scalar }

let with_units =
  { names = [ "with-units" ; "units" ; "u" ] ;
    env = "" ;
    doc = "Add units in the header line." ;
    docv = "" ;
    typ = Flag }

let flush =
  { names = [ "flush" ] ;
    env = "" ;
    doc = "Flush each line to stdout." ;
    docv = "" ;
    typ = Flag }

let func_name_or_code =
  { names = [] ;
    env = "" ;
    doc = "Function fully qualified name and field names, or code statement." ;
    docv = "FUNCTION" ;
    typ = Scalar }

let data_fields =
  { names = [] ;
    env = "" ;
    doc = "Fields to retrieve values from." ;
    docv = "FIELD" ;
    typ = Scalar }

let via =
  { names = [ "via" ] ;
    env = "" ;
    doc = "Use either local file or remote confserver to retrieve the tuples." ;
    docv = "file|confserver" ;
    typ = Scalar }

let num_points =
  { names = [ "num-points" ; "n" ] ;
    env = "" ;
    doc = "Number of points returned." ;
    docv = "POINTS" ;
    typ = Scalar }

let time_step =
  { names = [ "time-step" ] ;
    env = "" ;
    doc = "Duration between two points." ;
    docv = "DURATION" ;
    typ = Scalar }

let consolidation =
  { names = [ "consolidation" ] ;
    env = "" ;
    doc = "Consolidation function." ;
    docv = "min|max|avg|sum" ;
    typ = Scalar }

let bucket_time =
  { names = [ "bucket-time" ] ;
    env = "" ;
    doc = "Selected bucket time." ;
    docv = "begin|middle|end" ;
    typ = Scalar }

let factor =
  { names = [ "factor" ; "f" ] ;
    env = "" ;
    doc = "Specify which fields to use as factors/categorical variables." ;
    docv = "FIELD" ;
    typ = List }

let server_url =
  { names = [ "url" ] ;
    env = "RAMEN_URL" ;
    doc = "URL to reach the HTTP service." ;
    docv = "" ;
    typ = Scalar }

let graphite =
  { names = [ "graphite" ] ;
    env = "" ;
    doc = "Impersonate graphite for Grafana." ;
    docv = "" ;
    typ = Scalar }

let api =
  { names = [ "api-v1" ] ;
    env = "" ;
    doc = "Implement ramen API over http." ;
    docv = "" ;
    typ = Scalar }

let table_prefix =
  { names = [ "table-prefix" ] ;
    env = "" ;
    doc = "Only consider tables under this prefix for the API." ;
    docv = "STRING" ;
    typ = Scalar }

let fault_injection_rate =
  { names = [ "fault-injection-rate" ] ;
    env = "RAMEN_FAULT_INJECTION_RATE" ;
    doc = "Rate at which to generate fake errors." ;
    docv = "" ;
    typ = Scalar }

let for_render =
  { names = [ "for-render" ] ;
    env = "" ;
    doc = "Exact match as for the render query." ;
    docv = "" ;
    typ = Flag }

let graphite_query =
  { names = [] ;
    env = "" ;
    doc = "Test graphite query expansion." ;
    docv = "QUERY" ;
    typ = Scalar }

let test_file =
  { names = [] ;
    env = "" ;
    doc = "Definition of a test to run." ;
    docv = "file.test" ;
    typ = Scalar }

let update_stats =
  { names = [ "stats" ] ;
    env = "" ;
    doc = "Update the workers stats." ;
    docv = "" ;
    typ = Flag }

let update_allocs =
  { names = [ "allocs" ] ;
    env = "" ;
    doc = "Update the allocations." ;
    docv = "" ;
    typ = Flag }

let reconf_workers =
  { names = [ "reconf-workers" ] ;
    env = "" ;
    doc = "Change the workers export configuration." ;
    docv = "" ;
    typ = Flag }

let gc_loop =
  { names = [ "gc-loop" ] ;
    env = "" ;
    doc = "Do not exit after work is over. Instead, wait for the specified \
           amount of time and restart." ;
    docv = "" ;
    typ = Scalar }

let archivist_loop =
  { names = [ "archivist-loop" ] ;
    env = "" ;
    doc = "Do not exit after work is over. Instead, wait for the specified \
           amount of time and restart." ;
    docv = "" ;
    typ = Scalar }

let command =
  { names = [] ;
    env = "" ;
    doc = "Ramen command line to be completed." ;
    docv = "STRING" ;
    typ = Scalar }

(* Commands *)

type command =
  { name : string ;
    doc : string ;
    opts : opt list }

let copts =
  [ debug ; quiet ; rand_seed ; keep_temp_files ; reuse_prev_files ; variant ;
    initial_export_duration ; bundle_dir ; masters ; confserver_url ;
    confserver_key ; username ; client_pub_key ; client_priv_key ;
    identity_file ; colors ]

let httpd =
  { name = "httpd" ;
    doc = "Start an HTTP server." ;
    opts = [ daemonize ; to_stdout ; to_syslog ; prefix_log_with_name ;
             fault_injection_rate ; server_url ; api ; table_prefix ;
             graphite ; external_compiler ; max_simult_compilations ;
             smt_solver ] @ copts }

let expand =
  { name = "_expand" ;
    doc = "Test graphite query expansion." ;
    opts = [ for_render ; since ; until ; graphite_query ] @ copts }

let test =
  { name = "test" ;
    doc = "Test a configuration against one or several tests." ;
    opts = [ server_url ; api ; graphite ; external_compiler ;
             max_simult_compilations ; smt_solver ; test_file ] @ copts }

let archivist =
  { name = "archivist" ;
    doc = "Allocate disk for storage." ;
    opts = [ loop ; daemonize ; update_stats ; update_allocs ; reconf_workers ;
             to_stdout ; to_syslog ; prefix_log_with_name ; smt_solver ] @
           copts }

let start =
  { name = "start" ;
    doc = "Start ramen with basic configuration." ;
    opts = [ daemonize ; to_stdout ; to_syslog ; confserver_port ;
             confserver_port_sec ; smt_solver ; fail_for_good ; kill_at_exit ;
             test_notifs_every ; external_compiler ; max_simult_compilations ;
             server_pub_key ; server_priv_key ; no_source_examples ;
             default_archive_total_size ; default_archive_recall_cost ;
             oldest_restored_site ; gc_loop ; archivist_loop ; update_allocs ;
             reconf_workers ; del_ratio ; compress_older ; max_fpr ;
             timeout_idle_kafka_producers ; debounce_delay ;
             max_last_sent_kept ; max_incident_age ] @ copts }

let variants =
  { name = "variants" ;
    doc = "Display the experimenter identifier and variants." ;
    opts = copts }

let stats =
  { name = "stats" ;
    doc = "Display internal statistics." ;
    opts = copts }

let autocomplete =
  { name = "_completion" ;
    doc = "Autocomplete the given command." ;
    opts = [ command ] }

let supervisor =
  { name = "supervisor" ;
    doc = "Start the processes supervisor." ;
    opts = [ daemonize ; to_stdout ; to_syslog ; prefix_log_with_name ;
             external_compiler ; max_simult_compilations ; smt_solver ;
             fail_for_good ; kill_at_exit ; test_notifs_every ] @ copts }

let gc =
  { name = "gc" ;
    doc = "Delete old or unused files." ;
    opts = [ dry_run ; del_ratio ; compress_older ; loop ; daemonize ;
             to_stdout ; to_syslog ; prefix_log_with_name ] @ copts }

let alerter =
  { name = "alerter" ;
    doc = "Start the alerter." ;
    opts = [ max_fpr ; daemonize ; to_stdout ; to_syslog ;
             prefix_log_with_name ; timeout_idle_kafka_producers ;
             debounce_delay ; max_last_sent_kept ; max_incident_age ;
             for_test ; reschedule_clock ] @ copts }

let notify =
  { name = "notify" ;
    doc = "Send a notification." ;
    opts = [ parameter ; is_test_alert ; notif_name ] @ copts }

let test_alert =
  { name = "test-alert" ;
    doc = "Test alerting configuration." ;
    opts = [ test_file ] @ copts }

let tunneld =
  { name = "tunneld" ;
    doc = "Start the tuple forward service." ;
    opts = [ daemonize ; to_stdout ; to_syslog ; prefix_log_with_name ;
             tunneld_port ] @ copts }

let confserver =
  { name = "confserver" ;
    doc = "Start the configuration synchronization service." ;
    opts = [ daemonize ; to_stdout ; to_syslog ; prefix_log_with_name ;
             confserver_port ; confserver_port_sec ; server_pub_key ;
             server_priv_key ; no_source_examples ; default_archive_total_size ;
             default_archive_recall_cost ; oldest_restored_site ; persist_dir ]
             @ copts }

let confclient =
  { name = "confclient" ;
    doc = "Confserver client to dump/read/write configuration keys." ;
    opts = [ conf_key ; conf_value ; conf_delete ] @ copts }

let useradd =
  { name = "useradd" ;
    doc = "Create a user that will be allowed to connect to the confserver \
           and generate his/her secret key." ;
    opts = [ persist_dir ; output_file ; username ; role ;
             server_pub_key ] @ copts }
let userdel =
  { name = "userdel" ;
    doc = "Delete a user." ;
    opts = [ persist_dir ; username ] @ copts }

let usermod =
  { name = "usermod" ;
    doc = "Change the roles assumed by a user." ;
    opts = [ persist_dir ; username ; role ] @ copts }

let dequeue =
  { name = "dequeue" ;
    doc = "Dequeue a message from a ringbuffer." ;
    opts = [ rb_file ; num_entries ] @ copts }

let summary =
  { name = "ringbuf-summary" ;
    doc = "Display informations about a ring-buffer." ;
    opts = [ max_bytes ; rb_file ] @ copts }

let repair =
  { name = "repair-ringbuf" ;
    doc = "Repair a ringbuf header, assuming no readers/writers." ;
    opts = [ rb_files ] @ copts }

let dump_ringbuf =
  { name = "dump-ringbuf" ;
    doc = "Dump some bytes from the ringbuffer." ;
    opts = [ start_word ; stop_word ; rb_file ] @ copts }

let links =
  { name = "links" ;
    doc = "List all in use ring buffers with some statistics." ;
    opts = [ no_abbrev ; show_all_links ; as_tree ; pretty ; with_header ;
             sort_col ; top ; pattern ] @ copts }

let compile =
  { name = "compile" ;
    doc = "Compile each given source file into an executable." ;
    opts = [ lib_path ; external_compiler ; max_simult_compilations ;
             smt_solver ; src_files ; output_file ; as_ ; replace ] @ copts }

let precompserver =
  { name = "precompserver" ;
    doc = "Service that precompiles (aka parse+type-check) programs." ;
    opts = [ daemonize ; to_stdout ; to_syslog ; prefix_log_with_name ;
             smt_solver ] @ copts }

let execompserver =
  { name = "execompserver" ;
    doc = "Service that turns precompiled programs into local executables \
           files." ;
    opts = [ daemonize ; to_stdout ; to_syslog ; prefix_log_with_name ;
             external_compiler ; max_simult_compilations ] @ copts }

let run =
  { name = "run" ;
    doc = "Run one (or several) compiled program(s)." ;
    opts = [ parameter ; report_period ; as_ ; on_site ; cwd ;
             replace ] @ copts }

let kill =
  { name = "kill" ;
    doc = "Stop a program." ;
    opts = [ program_globs ; purge ] @ copts }

let info =
  { name = "info" ;
    doc = "Print all meta information about a worker." ;
    opts = [ parameter ; bin_file ; function_name ] @ copts }

let choreographer =
  { name = "choreographer" ;
    doc = "Service that decides where to run what." ;
    opts = [ daemonize ; to_stdout ; to_syslog ; prefix_log_with_name ] @
           copts }

let replayer =
  { name = "replayer" ;
    doc = "Service to turn simple replay requests into query-plans \
           that can be understood by supervisor." ;
    opts = [ daemonize ; to_stdout ; to_syslog ; prefix_log_with_name ] @
           copts }

let tail =
  { name = "tail" ;
    doc = "Display the last outputs of an operation." ;
    opts = [ func_name_or_code ; with_header ; with_units ; csv_separator ;
             csv_null ; csv_raw ; last ; next ; follow ; where ; since ;
             until ; with_event_time ; timeout ; pretty ; flush ;
             external_compiler ; max_simult_compilations ; smt_solver ] @
           copts }

let replay =
  { name = "replay" ;
    doc = "Rebuild the past output of the given operation." ;
    opts = [ func_name_or_code ; with_header ; with_units ; csv_separator ;
             csv_null ; csv_raw ; where ; since ; until ;
             with_event_time ; pretty ; flush ; external_compiler ;
             max_simult_compilations ; smt_solver ; via ] @ copts }

let timeseries =
  { name = "timeseries" ;
    doc = "Extract a time series from an operation." ;
    opts = [ func_name_or_code ; since ; until ; with_header ; where ;
             factor ; num_points ; time_step ; csv_separator ; csv_null ;
             consolidation ; bucket_time ; pretty ; external_compiler ;
             max_simult_compilations ; smt_solver ] @ copts }

let ps =
  { name = "ps" ;
    doc = "Display info about running programs." ;
    opts = [ pretty ; with_header ; sort_col ; top ; pattern ;
             show_all_workers ] @ copts }

let profile =
  { name = "_profile" ;
    doc = "Display profiling information about running programs." ;
    opts = [ pretty ; with_header ; sort_col ; top ; pattern ;
             show_all_workers ] @ copts }
