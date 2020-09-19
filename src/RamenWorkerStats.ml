(* Binocle is the instrumentation lib we use to monitor the workers.
 * Workers output a stream of statistics that are sent through a ringbuf
 * to ramen as they do with exported tuples.
 * Ramen then keep only the latest received info for each func (not in the
 * configuration persistent tree). This info is then added to the json
 * returning the current state of the configuration (/graph).
 *
 * Here we define the tuple type used to store these statistics and various
 * related functions. *)
open Batteries
open Stdint
open RamenLog
open RamenTuple
open RamenNullable
open RamenHelpers
module FieldDocs = RamenConstsFieldDocs
module Files = RamenFiles
module Metric = RamenConstsMetric
module N = RamenName
module T = RamenTypes
module DT = DessserTypes

let perf_kts =
  DT.[| "count",  DT.make (Mac TU32) ;
        "user",   DT.make (Mac TFloat) ;
        "system", DT.make (Mac TFloat) |]

let profile_typ =
  DT.make (TRec perf_kts)

let profile_fields =
  [| "tot_per_tuple", profile_typ ;
     "where_fast", profile_typ ;
     "find_group", profile_typ ;
     "where_slow", profile_typ ;
     "update_group", profile_typ ;
     "commit_incoming", profile_typ ;
     "select_others", profile_typ ;
     "finalize_others", profile_typ ;
     "commit_others", profile_typ ;
     "flush_others", profile_typ |]

(* <blink>DO NOT ALTER</blink> this record without also updating
 * (un)serialization functions! *)
let tuple_typ =
  [ { name = N.field "site" ;
      typ = DT.make (Mac TString) ;
      units = None ;
      doc = FieldDocs.site ;
      aggr = None } ;
    { name = N.field "worker" ;
      typ = DT.make (Mac TString) ;
      units = Some RamenUnits.processes ;
      doc = FieldDocs.worker ;
      aggr = None } ;
    { name = N.field "top-half" ;
      typ = DT.make (Mac TBool) ;
      units = None ;
      doc = FieldDocs.top_half ;
      aggr = None } ;
    { name = N.field "start" ;
      typ = DT.make (Mac TFloat) ;
      units = Some RamenUnits.seconds_since_epoch ;
      doc = FieldDocs.start ;
      aggr = None } ;
    { name = N.field "min_event_time" ;
      typ = DT.maken (Mac TFloat) ;
      units = Some RamenUnits.seconds_since_epoch ;
      doc = "Smallest event time emitted so far" ;
      aggr = None } ;
    { name = N.field "max_event_time" ;
      typ = DT.maken (Mac TFloat) ;
      units = Some RamenUnits.seconds_since_epoch ;
      doc = "Largest event time emitted so far" ;
      aggr = None } ;
    { name = N.field "tuples_in" ;
      typ = DT.maken (Mac TU64) ;
      units = Some RamenUnits.tuples ;
      doc = Metric.Docs.in_tuple_count ;
      aggr = None } ;
    { name = N.field "tuples_selected" ;
      typ = DT.maken (Mac TU64) ;
      units = Some RamenUnits.tuples ;
      doc = Metric.Docs.selected_tuple_count ;
      aggr = None } ;
    { name = N.field "tuples_out" ;
      typ = DT.maken (Mac TU64) ;
      units = Some RamenUnits.tuples ;
      doc = Metric.Docs.out_tuple_count ;
      aggr = None } ;
    { name = N.field "groups" ;
      typ = DT.maken (Mac TU64) ;
      units = Some RamenUnits.groups ;
      doc = Metric.Docs.group_count ;
      aggr = None } ;
    { name = N.field "cpu" ;
      typ = DT.make (Mac TFloat) ;
      units = Some RamenUnits.seconds ;
      doc = Metric.Docs.cpu_time ;
      aggr = None } ;
    { name = N.field "ram" ;
      typ = DT.make (Mac TU64) ;
      units = Some RamenUnits.bytes ;
      doc = Metric.Docs.ram_usage ;
      aggr = None } ;
    { name = N.field "max_ram" ;
      typ = DT.make (Mac TU64) ;
      units = Some RamenUnits.bytes ;
      doc = Metric.Docs.max_ram_usage ;
      aggr = None } ;
    { name = N.field "profile" ;
      typ = DT.make (TRec profile_fields) ;
      units = None ;
      doc = Metric.Docs.profile ;
      aggr = None } ;
    { name = N.field "wait_in" ;
      typ = DT.maken (Mac TFloat) ;
      units = Some RamenUnits.seconds ;
      doc = Metric.Docs.rb_wait_read ;
      aggr = None } ;
    { name = N.field "wait_out" ;
      typ = DT.maken (Mac TFloat) ;
      units = Some RamenUnits.seconds ;
      doc = Metric.Docs.rb_wait_write ;
      aggr = None } ;
    { name = N.field "bytes_in" ;
      typ = DT.maken (Mac TU64) ;
      units = Some RamenUnits.bytes ;
      doc = Metric.Docs.worker_read_bytes ;
      aggr = None } ;
    { name = N.field "bytes_out" ;
      typ = DT.maken (Mac TU64) ;
      units = Some RamenUnits.bytes ;
      doc = Metric.Docs.worker_write_bytes ;
      aggr = None } ;
    { name = N.field "avg_out_bytes" ;
      typ = DT.maken (Mac TU64) ;
      units = Some RamenUnits.bytes ;
      doc = "Average size of a full output tuple." ;
      aggr = None } ;
    { name = N.field "last_out" ;
      typ = DT.maken (Mac TFloat) ;
      units = Some RamenUnits.seconds_since_epoch ;
      doc = Metric.Docs.last_out ;
      aggr = None } ;
    { name = N.field "startup_time" ;
      typ = DT.make (Mac TFloat) ;
      units = Some RamenUnits.seconds_since_epoch ;
      doc = "When this worker started to run for the last time." ;
      aggr = None } ]

let event_time =
  let open RamenEventTime in
  Some ((N.field "start", ref OutputField, 1.),
        DurationConst 0.)

let factors = [ N.field "site" ; N.field "worker" ]

(* We want some metrics to save their values on disc, but since most metrics
 * are defined during module initialization and we do not know the RAMEN_DIR
 * yet, initialization of those metrics have to be delayed.
 * This is the purpose of [ensure_inited].
 * This function also save the metric in [all_saved_metrics] so that all of
 * them can be initialized at once with a simple call to
 * [initialize_all_saved_metrics], which is used by `ramen stats` to dump
 * all metrics.
 * Individual services usually just initialize their own metrics. *)

let all_saved_metrics = ref []

let ensure_inited f =
  let inited = ref None in
  let initer persist_dir =
    match !inited with
    | None ->
        let save_dir =
          N.path_cat [ persist_dir ; N.path "/binocle/" ;
                       N.path RamenVersions.binocle ] in
        Files.mkdir_all save_dir ;
        let m = f save_dir in
        inited := Some m ;
        m
    | Some m -> m
  in
  all_saved_metrics :=
    (fun persist_dir -> ignore (initer persist_dir)) :: !all_saved_metrics ;
  initer

let initialize_all_saved_metrics base_dir =
  List.iter (fun initer ->
    initer base_dir
  ) !all_saved_metrics
