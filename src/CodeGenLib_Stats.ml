(* Health and Stats
 *
 * Each func has to periodically report to ramen http server its health and
 * some stats.
 * Could have been the other way around, and that would have made the
 * connection establishment easier possibly (since we already must be able to
 * ssh to other machines in order to start a func) but we already have an http
 * server on Ramen and probably want to avoid opening too many ports
 * everywhere, and forcing generated funcs to implement too many things.
 *
 * Stats must include:
 *
 * - total number of tuples input and output
 * - total CPU time consumed
 * - current RAM used
 * and others depending on the operation.
 *)
open Stdint
open RamenConsts
open RamenHelpers
open RamenLog

open Binocle

let in_tuple_count =
  IntCounter.make Metric.Names.in_tuple_count
    Metric.Docs.in_tuple_count

let selected_tuple_count =
  IntCounter.make Metric.Names.selected_tuple_count
    Metric.Docs.selected_tuple_count

let out_tuple_count =
  IntCounter.make Metric.Names.out_tuple_count
    Metric.Docs.out_tuple_count

let firing_notif_count =
  IntCounter.make Metric.Names.firing_notif_count
    Metric.Docs.firing_notif_count

let extinguished_notif_count =
  IntCounter.make Metric.Names.extinguished_notif_count
    Metric.Docs.extinguished_notif_count

let group_count =
  IntGauge.make Metric.Names.group_count
                Metric.Docs.group_count

let cpu =
  FloatCounter.make Metric.Names.cpu_time
    Metric.Docs.cpu_time

let ram =
  IntGauge.make Metric.Names.ram_usage
    Metric.Docs.ram_usage

let read_bytes =
  IntCounter.make Metric.Names.worker_read_bytes
    Metric.Docs.worker_read_bytes

let write_bytes =
  IntCounter.make Metric.Names.worker_write_bytes
    Metric.Docs.worker_write_bytes

let read_sleep_time =
  FloatCounter.make Metric.Names.rb_wait_read
    Metric.Docs.rb_wait_read

let sleep_in d =
  FloatCounter.add read_sleep_time d

let write_sleep_time =
  FloatCounter.make Metric.Names.rb_wait_write
    Metric.Docs.rb_wait_write

let sleep_out d =
  FloatCounter.add write_sleep_time d

let last_out =
  FloatGauge.make Metric.Names.last_out
    Metric.Docs.last_out

let num_subscribers =
  IntGauge.make Metric.Names.num_subscribers
    Metric.Docs.num_subscribers

let num_rate_limited_unpublished =
  IntCounter.make Metric.Names.num_rate_limited_unpublished
    Metric.Docs.num_rate_limited_unpublished

let event_time =
  FloatGauge.make Metric.Names.event_time
    Metric.Docs.event_time


(* From time to time we measure the full size of an output tuple and
 * update this. Have a single Gauge rather than two counters to avoid
 * race conditions between updates and send_stats: *)
let avg_full_out_bytes =
  IntGauge.make Metric.Names.avg_full_out_bytes
    Metric.Docs.avg_full_out_bytes

(* For confserver values, where no race conditions are possible: *)
let tot_full_bytes = ref Uint64.zero
let tot_full_bytes_samples = ref Uint64.zero

(* TODO: add in the instrumentation tuple? *)
let relocated_groups =
  IntCounter.make Metric.Names.relocated_groups
    Metric.Docs.relocated_groups

(* Perf counters: *)
let perf_per_tuple =
  Perf.make Metric.Names.perf_per_tuple Metric.Docs.perf_per_tuple

let perf_where_fast =
  Perf.make Metric.Names.perf_where_fast Metric.Docs.perf_where_fast

let perf_find_group =
  Perf.make Metric.Names.perf_find_group Metric.Docs.perf_find_group

let perf_where_slow =
  Perf.make Metric.Names.perf_where_slow Metric.Docs.perf_where_slow

let perf_update_group =
  Perf.make Metric.Names.perf_update_group Metric.Docs.perf_update_group

let perf_commit_incoming =
  Perf.make Metric.Names.perf_commit_incoming Metric.Docs.perf_commit_incoming

let perf_select_others =
  Perf.make Metric.Names.perf_select_others Metric.Docs.perf_select_others

let perf_finalize_others =
  Perf.make Metric.Names.perf_finalize_others
            Metric.Docs.perf_finalize_others

let perf_commit_others =
  Perf.make Metric.Names.perf_commit_others Metric.Docs.perf_commit_others

let perf_flush_others =
  Perf.make Metric.Names.perf_flush_others Metric.Docs.perf_flush_others

let measure_full_out sz =
  !logger.debug "Measured fully fledged out tuple of size %d" sz ;
  tot_full_bytes := Uint64.(add !tot_full_bytes (of_int sz)) ;
  tot_full_bytes_samples := Uint64.succ !tot_full_bytes_samples ;
  Uint64.to_float !tot_full_bytes /.
  Uint64.to_float !tot_full_bytes_samples |>
  round_to_int |>
  IntGauge.set avg_full_out_bytes

let tot_cpu_time () =
  let open Unix in
  let pt = times () in
  pt.tms_utime +. pt.tms_stime +. pt.tms_cutime +. pt.tms_cstime

let tot_ram_usage =
  let word_size = Sys.word_size / 8 in
  fun () ->
    let stat = Gc.quick_stat () in
    stat.Gc.heap_words * word_size

(* Unfortunately, we cannot distinguish that easily between CPU/RAM usage for
 * live channel and others: *)
let update () =
  FloatCounter.set cpu (tot_cpu_time ()) ;
  IntGauge.set ram (tot_ram_usage ())

let gauge_current (_mi, x, _ma) = x

let startup_time = Unix.gettimeofday ()

let first_output = ref None
let last_output = ref None
let update_output_times () =
  if !first_output = None then first_output := Some !CodeGenLib.now ;
  last_output := Some !CodeGenLib.now
