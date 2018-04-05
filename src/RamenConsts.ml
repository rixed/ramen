module HttpContentTypes =
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
  let start = "Start the processes orchestrator."
  let compile = "Compile each given source file into an executable."
  let run = "Run one (or several) compiled program(s)."
  let kill = "Stop a program."
  let tail = "Display the last outputs of an operation."
  let timeseries = "Extract a timeseries from an operation."
  let timerange =
    "Retrieve the available time range of an operation output."
  let ps = "Display info about running programs."
  let test = "Test a configuration against one or several tests"
end

let default_persist_dir = "/tmp/ramen"
