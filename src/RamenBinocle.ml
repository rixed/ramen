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
module U = RamenUnits

(* <blink>DO NOT ALTER</blink> this record without also updating
 * (un)serialization functions! *)
let typ =
  T.(make (TRecord [|
    "worker",
      { structure = TString ; nullable = false ; units = Some U.processes ;
        doc = "" ; aggr = None } ;
    "start",
      { structure = TFloat ; nullable = false ;
        units = Some U.seconds_since_epoch ;
        doc = "When those statistics have been collected (wall clock \
               time)" ;
        aggr = None } ;
    "min_event_time",
      { structure = TFloat ; nullable = true ;
        units = Some U.seconds_since_epoch ;
        doc = "Smallest event time emitted so far" ; aggr = None } ;
    "max_event_time",
      { structure = TFloat ; nullable = true ;
        units = Some U.seconds_since_epoch ;
        doc = "Largest event time emitted so far" ; aggr = None } ;
    "tuples_in",
      { structure = TU64 ; nullable = true ; units = Some U.tuples ;
        doc = Metric.Docs.in_tuple_count ; aggr = None } ;
    "tuples_selected",
      { structure = TU64 ; nullable = true ; units = Some U.tuples ;
        doc = Metric.Docs.selected_tuple_count ; aggr = None } ;
    "tuples_out",
      { structure = TU64 ; nullable = true ; units = Some U.tuples ;
        doc = Metric.Docs.out_tuple_count ; aggr = None } ;
    "groups",
      { structure = TU64 ; nullable = true ; units = Some U.groups ;
        doc = Metric.Docs.group_count ; aggr = None } ;
    "cpu",
      { structure = TFloat ; nullable = false ; units = Some U.seconds ;
        doc = Metric.Docs.cpu_time ; aggr = None } ;
    "ram",
      { structure = TU64 ; nullable = false ; units = Some U.bytes ;
        doc = Metric.Docs.ram_usage ; aggr = None } ;
    "max_ram",
      { structure = TU64 ; nullable = false ; units = Some U.bytes ;
        doc = Metric.Docs.max_ram_usage ; aggr = None } ;
    "wait_in",
      { structure = TFloat ; nullable = true ; units = Some U.seconds ;
        doc = Metric.Docs.rb_wait_read ; aggr = None } ;
    "wait_out",
      { structure = TFloat ; nullable = true ; units = Some U.seconds ;
        doc = Metric.Docs.rb_wait_write ; aggr = None } ;
    "bytes_in",
      { structure = TU64 ; nullable = true ; units = Some U.bytes ;
        doc = Metric.Docs.rb_read_bytes ; aggr = None } ;
    "bytes_out",
      { structure = TU64 ; nullable = true ; units = Some U.bytes ;
        doc = Metric.Docs.rb_write_bytes ; aggr = None } ;
    "avg_out_bytes",
      { structure = TU64 ; nullable = true ; units = Some U.bytes ;
        doc = "Average size of a full output tuple." ; aggr = None } ;
    "last_out",
      { structure = TFloat ; nullable = true ;
        units = Some U.seconds_since_epoch ;
        doc = Metric.Docs.last_out ; aggr = None } ;
    "startup_time",
      { structure = TFloat ; nullable = false ;
        units = Some U.seconds_since_epoch ;
        doc = "When this worker started to run for the last time." ;
        aggr = None } |]))

let event_time =
  let open RamenEventTime in
  Some ((RamenName.field_of_string "start", ref OutputField, 1.),
        DurationConst 0.)

let factors = [ RamenName.field_of_string "worker" ]

open RingBuf
open RingBufLib

let nullmask_sz = nullmask_bytes_of_type typ

let fix_sz = fixsz_of_type typ

(* We will actually allocate that much on the RB since we know most of the
 * time the counters won't be NULL. *)
let max_sersize_of_tuple (worker, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =
  nullmask_sz + fix_sz + sersize_of_string worker

let serialize tx start_offs
              (worker, start, min_etime, max_etime, ic, sc, oc, gc, cpu,
               ram, max_ram, wi, wo, bi, bo, os, lo, stime) =
  zero_bytes tx start_offs nullmask_sz ; (* zero the nullmask *)
  let write_nullable_thing w sz offs null_i = function
    | None ->
      offs
    | Some v ->
      set_bit tx start_offs null_i ;
      w tx offs v ;
      offs + sz in
  let write_nullable_u64 =
    let sz = sersize_of_u64 in
    write_nullable_thing write_u64 sz
  and write_nullable_float =
    let sz = sersize_of_float in
    write_nullable_thing write_float sz in
  let offs = start_offs + nullmask_sz in
  let offs =
    write_string tx offs worker ;
    offs + sersize_of_string worker in
  let offs =
    write_float tx offs start ;
    offs + sersize_of_float in
  let offs = write_nullable_float offs 0 min_etime in
  let offs = write_nullable_float offs 1 max_etime in
  let offs = write_nullable_u64 offs 2 ic in
  let offs = write_nullable_u64 offs 3 sc in
  let offs = write_nullable_u64 offs 4 oc in
  let offs = write_nullable_u64 offs 5 gc in
  let offs =
    write_float tx offs cpu ;
    offs + sersize_of_float in
  let offs =
    write_u64 tx offs ram ;
    offs + sersize_of_u64 in
  let offs =
    write_u64 tx offs max_ram ;
    offs + sersize_of_u64 in
  let offs = write_nullable_float offs 6 wi in
  let offs = write_nullable_float offs 7 wo in
  let offs = write_nullable_u64 offs 8 bi in
  let offs = write_nullable_u64 offs 9 bo in
  let offs = write_nullable_u64 offs 10 os in
  let offs = write_nullable_float offs 11 lo in
  let offs =
    write_float tx offs stime ;
    offs + sersize_of_float in
  offs

let unserialize tx start_offs =
  let read_nullable_thing r sz null_i offs =
    if get_bit tx start_offs null_i then
      NotNull (r tx offs), offs + sz
    else
      Null, offs in
  let read_nullable_u64 =
    let sz = sersize_of_u64 in
    read_nullable_thing read_u64 sz
  and read_nullable_float =
    let sz = sersize_of_float in
    read_nullable_thing read_float sz in
  let offs = start_offs + nullmask_sz in
  let worker = read_string tx offs in
  let offs = offs + sersize_of_string worker in
  let start = read_float tx offs in
  let offs = offs + sersize_of_float in
  let min_etime, offs = read_nullable_float 0 offs in
  let max_etime, offs = read_nullable_float 1 offs in
  let ic, offs = read_nullable_u64 2 offs in
  let sc, offs = read_nullable_u64 3 offs in
  let oc, offs = read_nullable_u64 4 offs in
  let gc, offs = read_nullable_u64 5 offs in
  let cpu = read_float tx offs in
  let offs = offs + sersize_of_float in
  let ram = read_u64 tx offs in
  let offs = offs + sersize_of_u64 in
  let max_ram = read_u64 tx offs in
  let offs = offs + sersize_of_u64 in
  let wi, offs = read_nullable_float 6 offs in
  let wo, offs = read_nullable_float 7 offs in
  let bi, offs = read_nullable_u64 8 offs in
  let bo, offs = read_nullable_u64 9 offs in
  let os, offs = read_nullable_u64 10 offs in
  let lo, offs = read_nullable_float 11 offs in
  let stime = read_float tx offs in
  let offs = offs + sersize_of_float in
  let t =
    worker, start, min_etime, max_etime, ic, sc , oc, gc, cpu, ram, max_ram,
    wi, wo, bi, bo, os, lo, stime in
  assert (offs <= start_offs + max_sersize_of_tuple t) ;
  t

(* Helper to initialize with the actual conf instrumentation metrics as
 * global parameters. We also register them all in a list so that
 * `ramen variant` can init and read them all. *)

let all_saved_metrics = ref []

let ensure_inited f =
  let inited = ref None in
  let initer persist_dir =
    match !inited with
    | None ->
        let save_dir = persist_dir ^"/binocle/"^ RamenVersions.binocle in
        mkdir_all save_dir ;
        let m = f save_dir in
        inited := Some m ;
        m
    | Some m -> m
  in
  all_saved_metrics :=
    (fun persist_dir -> ignore (initer persist_dir)) :: !all_saved_metrics ;
  initer
