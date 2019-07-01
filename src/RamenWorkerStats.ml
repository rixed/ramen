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
open RamenConsts
module T = RamenTypes
module N = RamenName
module Files = RamenFiles

let perf_kts =
  T.[| "count",  { nullable = false ; structure = TU32 } ;
       "user",   { nullable = false ; structure = TFloat } ;
       "system", { nullable = false ; structure = TFloat } |]

let profile_typ =
  T.{ nullable = false ; structure = TRecord perf_kts }

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
      typ = { structure = TString ; nullable = false } ;
      units = None ;
      doc = FieldDocs.site ;
      aggr = None } ;
    { name = N.field "worker" ;
      typ = { structure = TString ; nullable = false } ;
      units = Some RamenUnits.processes ;
      doc = FieldDocs.worker ;
      aggr = None } ;
    { name = N.field "top-half" ;
      typ = { structure = TBool ; nullable = false } ;
      units = None ;
      doc = FieldDocs.top_half ;
      aggr = None } ;
    { name = N.field "start" ;
      typ = { structure = TFloat ; nullable = false } ;
      units = Some RamenUnits.seconds_since_epoch ;
      doc = FieldDocs.start ;
      aggr = None } ;
    { name = N.field "min_event_time" ;
      typ = { structure = TFloat ; nullable = true } ;
      units = Some RamenUnits.seconds_since_epoch ;
      doc = "Smallest event time emitted so far" ;
      aggr = None } ;
    { name = N.field "max_event_time" ;
      typ = { structure = TFloat ; nullable = true } ;
      units = Some RamenUnits.seconds_since_epoch ;
      doc = "Largest event time emitted so far" ;
      aggr = None } ;
    { name = N.field "tuples_in" ;
      typ = { structure = TU64 ; nullable = true } ;
      units = Some RamenUnits.tuples ;
      doc = Metric.Docs.in_tuple_count ;
      aggr = None } ;
    { name = N.field "tuples_selected" ;
      typ = { structure = TU64 ; nullable = true } ;
      units = Some RamenUnits.tuples ;
      doc = Metric.Docs.selected_tuple_count ;
      aggr = None } ;
    { name = N.field "tuples_out" ;
      typ = { structure = TU64 ; nullable = true } ;
      units = Some RamenUnits.tuples ;
      doc = Metric.Docs.out_tuple_count ;
      aggr = None } ;
    { name = N.field "groups" ;
      typ = { structure = TU64 ; nullable = true } ;
      units = Some RamenUnits.groups ;
      doc = Metric.Docs.group_count ;
      aggr = None } ;
    { name = N.field "cpu" ;
      typ = { structure = TFloat ; nullable = false } ;
      units = Some RamenUnits.seconds ;
      doc = Metric.Docs.cpu_time ;
      aggr = None } ;
    { name = N.field "ram" ;
      typ = { structure = TU64 ; nullable = false } ;
      units = Some RamenUnits.bytes ;
      doc = Metric.Docs.ram_usage ;
      aggr = None } ;
    { name = N.field "max_ram" ;
      typ = { structure = TU64 ; nullable = false } ;
      units = Some RamenUnits.bytes ;
      doc = Metric.Docs.max_ram_usage ;
      aggr = None } ;
    { name = N.field "profile" ;
      typ = { nullable = false ; structure = TRecord profile_fields } ;
      units = None ;
      doc = Metric.Docs.profile ;
      aggr = None } ;
    { name = N.field "wait_in" ;
      typ = { structure = TFloat ; nullable = true } ;
      units = Some RamenUnits.seconds ;
      doc = Metric.Docs.rb_wait_read ;
      aggr = None } ;
    { name = N.field "wait_out" ;
      typ = { structure = TFloat ; nullable = true } ;
      units = Some RamenUnits.seconds ;
      doc = Metric.Docs.rb_wait_write ;
      aggr = None } ;
    { name = N.field "bytes_in" ;
      typ = { structure = TU64 ; nullable = true } ;
      units = Some RamenUnits.bytes ;
      doc = Metric.Docs.worker_read_bytes ;
      aggr = None } ;
    { name = N.field "bytes_out" ;
      typ = { structure = TU64 ; nullable = true } ;
      units = Some RamenUnits.bytes ;
      doc = Metric.Docs.worker_write_bytes ;
      aggr = None } ;
    { name = N.field "avg_out_bytes" ;
      typ = { structure = TU64 ; nullable = true } ;
      units = Some RamenUnits.bytes ;
      doc = "Average size of a full output tuple." ;
      aggr = None } ;
    { name = N.field "last_out" ;
      typ = { structure = TFloat ; nullable = true } ;
      units = Some RamenUnits.seconds_since_epoch ;
      doc = Metric.Docs.last_out ;
      aggr = None } ;
    { name = N.field "startup_time" ;
      typ = { structure = TFloat ; nullable = false } ;
      units = Some RamenUnits.seconds_since_epoch ;
      doc = "When this worker started to run for the last time." ;
      aggr = None } ]

let event_time =
  let open RamenEventTime in
  Some ((N.field "start", ref OutputField, 1.),
        DurationConst 0.)

let factors = [ N.field "site" ; N.field "worker" ]

(* Helper to initialize with the actual conf instrumentation metrics as
 * global parameters. We also register them all in a list so that
 * `ramen variant` can init and read them all. *)

let all_saved_metrics = ref []

let ensure_inited f =
  let inited = ref None in
  let initer persist_dir =
    match !inited with
    | None ->
        let save_dir =
          N.cat (N.cat persist_dir (N.path "/binocle/"))
                (N.path RamenVersions.binocle) in
        Files.mkdir_all save_dir ;
        let m = f save_dir in
        inited := Some m ;
        m
    | Some m -> m
  in
  all_saved_metrics :=
    (fun persist_dir -> ignore (initer persist_dir)) :: !all_saved_metrics ;
  initer
