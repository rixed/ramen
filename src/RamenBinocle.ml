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

(* <blink>DO NOT ALTER</blink> this record without also updating
 * (un)serialization functions! *)
let tuple_typ =
  [ { typ_name = "worker" ; typ = { structure = TString ; nullable = false } ; units = Some RamenUnits.processes ; doc = "" ; aggr = None } ;
    { typ_name = "start" ; typ = { structure = TFloat ; nullable = false } ; units = Some RamenUnits.seconds_since_epoch ;
      doc = "When those statistics have been collected (wall clock time)" ; aggr = None } ;
    { typ_name = "min_event_time" ; typ = { structure = TFloat ; nullable = true } ; units = Some RamenUnits.seconds_since_epoch ;
      doc = "Smallest event time emitted so far" ; aggr = None } ;
    { typ_name = "max_event_time" ; typ = { structure = TFloat ; nullable = true } ; units = Some RamenUnits.seconds_since_epoch ;
      doc = "Largest event time emitted so far" ; aggr = None } ;
    { typ_name = "tuples_in" ; typ = { structure = TU64 ; nullable = true } ; units = Some RamenUnits.tuples ;
      doc = RamenConsts.Metric.Docs.in_tuple_count ; aggr = None } ;
    { typ_name = "tuples_selected" ; typ = { structure = TU64 ; nullable = true } ; units = Some RamenUnits.tuples ;
      doc = RamenConsts.Metric.Docs.selected_tuple_count ; aggr = None } ;
    { typ_name = "tuples_out" ; typ = { structure = TU64 ; nullable = true } ; units = Some RamenUnits.tuples ;
      doc = RamenConsts.Metric.Docs.out_tuple_count ; aggr = None } ;
    { typ_name = "groups" ; typ = { structure = TU64 ; nullable = true } ; units = Some RamenUnits.groups ;
      doc = RamenConsts.Metric.Docs.group_count ; aggr = None } ;
    { typ_name = "cpu" ; typ = { structure = TFloat ; nullable = false } ; units = Some RamenUnits.seconds ;
      doc = RamenConsts.Metric.Docs.cpu_time ; aggr = None } ;
    { typ_name = "ram" ; typ = { structure = TU64 ; nullable = false } ; units = Some RamenUnits.bytes ;
      doc = RamenConsts.Metric.Docs.ram_usage ; aggr = None } ;
    { typ_name = "max_ram" ; typ = { structure = TU64 ; nullable = false } ; units = Some RamenUnits.bytes ;
      doc = RamenConsts.Metric.Docs.max_ram_usage ; aggr = None } ;
    { typ_name = "wait_in" ; typ = { structure = TFloat ; nullable = true } ; units = Some RamenUnits.seconds ;
      doc = RamenConsts.Metric.Docs.rb_wait_read ; aggr = None } ;
    { typ_name = "wait_out" ; typ = { structure = TFloat ; nullable = true } ; units = Some RamenUnits.seconds ;
      doc = RamenConsts.Metric.Docs.rb_wait_write ; aggr = None } ;
    { typ_name = "bytes_in" ; typ = { structure = TU64 ; nullable = true } ; units = Some RamenUnits.bytes ;
      doc = RamenConsts.Metric.Docs.rb_read_bytes ; aggr = None } ;
    { typ_name = "bytes_out" ; typ = { structure = TU64 ; nullable = true } ; units = Some RamenUnits.bytes ;
      doc = RamenConsts.Metric.Docs.rb_write_bytes ; aggr = None } ;
    { typ_name = "last_out" ; typ = { structure = TFloat ; nullable = true } ; units = Some RamenUnits.seconds_since_epoch ;
      doc = RamenConsts.Metric.Docs.last_out ; aggr = None } ;
    { typ_name = "startup_time" ; typ = { structure = TFloat ; nullable = false } ; units = Some RamenUnits.seconds_since_epoch ;
      doc = "When this worker started to run for the last time." ; aggr = None } ]

let event_time =
  let open RamenEventTime in
  Some (("start", ref OutputField, 1.), DurationConst 0.)

let factors = [ "worker" ]

let nullmask_sz = RingBufLib.nullmask_bytes_of_tuple_type tuple_typ

let fix_sz = RingBufLib.tot_fixsz tuple_typ

(* We will actually allocate that much on the RB since we know most of the
 * time the counters won't be NULL. *)
let max_sersize_of_tuple (worker, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =
  let open RingBufLib in
  nullmask_sz + fix_sz + sersize_of_string worker

let serialize tx (worker, start, min_etime, max_etime, ic, sc, oc, gc, cpu,
                  ram, max_ram, wi, wo, bi, bo, lo, stime) =
  RingBuf.zero_bytes tx 0 nullmask_sz ; (* zero the nullmask *)
  let write_nullable_thing w sz offs null_i = function
    | None ->
      offs
    | Some v ->
      RingBuf.set_bit tx null_i ;
      w tx offs v ;
      offs + sz in
  let write_nullable_u64 =
    let sz = RingBufLib.sersize_of_u64 in
    write_nullable_thing RingBuf.write_u64 sz
  and write_nullable_float =
    let sz = RingBufLib.sersize_of_float in
    write_nullable_thing RingBuf.write_float sz in
  let offs = nullmask_sz in
  let offs =
    RingBuf.write_string tx offs worker ;
    offs + RingBufLib.sersize_of_string worker in
  let offs =
    RingBuf.write_float tx offs start ;
    offs + RingBufLib.sersize_of_float in
  let offs = write_nullable_float offs 0 min_etime in
  let offs = write_nullable_float offs 1 max_etime in
  let offs = write_nullable_u64 offs 2 ic in
  let offs = write_nullable_u64 offs 3 sc in
  let offs = write_nullable_u64 offs 4 oc in
  let offs = write_nullable_u64 offs 5 gc in
  let offs =
    RingBuf.write_float tx offs cpu ;
    offs + RingBufLib.sersize_of_float in
  let offs =
    RingBuf.write_u64 tx offs ram ;
    offs + RingBufLib.sersize_of_u64 in
  let offs =
    RingBuf.write_u64 tx offs max_ram ;
    offs + RingBufLib.sersize_of_u64 in
  let offs = write_nullable_float offs 6 wi in
  let offs = write_nullable_float offs 7 wo in
  let offs = write_nullable_u64 offs 8 bi in
  let offs = write_nullable_u64 offs 9 bo in
  let offs = write_nullable_float offs 10 lo in
  let offs =
    RingBuf.write_float tx offs stime ;
    offs + RingBufLib.sersize_of_float in
  offs

let unserialize tx =
  let read_nullable_thing r sz tx null_i offs =
    if RingBuf.get_bit tx null_i then
      NotNull (r tx offs), offs + sz
    else
      Null, offs in
  let read_nullable_u64 =
    let sz = RingBufLib.sersize_of_u64 in
    read_nullable_thing RingBuf.read_u64 sz
  and read_nullable_float =
    let sz = RingBufLib.sersize_of_float in
    read_nullable_thing RingBuf.read_float sz in
  let offs = nullmask_sz in
  let worker = RingBuf.read_string tx offs in
  let offs = offs + RingBufLib.sersize_of_string worker in
  let start = RingBuf.read_float tx offs in
  let offs = offs + RingBufLib.sersize_of_float in
  let min_etime, offs = read_nullable_float tx 0 offs in
  let max_etime, offs = read_nullable_float tx 1 offs in
  let ic, offs = read_nullable_u64 tx 2 offs in
  let sc, offs = read_nullable_u64 tx 3 offs in
  let oc, offs = read_nullable_u64 tx 4 offs in
  let gc, offs = read_nullable_u64 tx 5 offs in
  let cpu = RingBuf.read_float tx offs in
  let offs = offs + RingBufLib.sersize_of_float in
  let ram = RingBuf.read_u64 tx offs in
  let offs = offs + RingBufLib.sersize_of_u64 in
  let max_ram = RingBuf.read_u64 tx offs in
  let offs = offs + RingBufLib.sersize_of_u64 in
  let wi, offs = read_nullable_float tx 6 offs in
  let wo, offs = read_nullable_float tx 7 offs in
  let bi, offs = read_nullable_u64 tx 8 offs in
  let bo, offs = read_nullable_u64 tx 9 offs in
  let lo, offs = read_nullable_float tx 10 offs in
  let stime = RingBuf.read_float tx offs in
  let offs = offs + RingBufLib.sersize_of_float in
  let t =
    worker, start, min_etime, max_etime, ic, sc , oc, gc, cpu, ram, max_ram,
    wi, wo, bi, bo, lo, stime in
  assert (offs <= max_sersize_of_tuple t) ;
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
