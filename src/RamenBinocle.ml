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

let () =
  RamenHelpers.mkdir_all RamenConsts.binocle_save_dir

(* <blink>DO NOT ALTER</blink> this record without also updating
 * (un)serialization functions! *)
let tuple_typ =
  [ { typ_name = "worker" ; typ = { structure = TString ; nullable = false } ; units = Some RamenUnits.processes ; doc = "" } ;
    { typ_name = "time" ; typ = { structure = TFloat ; nullable = false } ; units = Some RamenUnits.seconds_since_epoch ;
      doc = "When those statistics have been collected (wall clock time)" } ;
    { typ_name = "max_event_time" ; typ = { structure = TFloat ; nullable = true } ; units = Some RamenUnits.seconds_since_epoch ;
      doc = "Largest event time encountered so far" } ;
    { typ_name = "tuples_in" ; typ = { structure = TU64 ; nullable = true } ; units = Some RamenUnits.tuples ;
      doc = RamenConsts.Metric.Docs.in_tuple_count } ;
    { typ_name = "tuples_selected" ; typ = { structure = TU64 ; nullable = true } ; units = Some RamenUnits.tuples ;
      doc = RamenConsts.Metric.Docs.selected_tuple_count } ;
    { typ_name = "tuples_out" ; typ = { structure = TU64 ; nullable = true } ; units = Some RamenUnits.tuples ;
      doc = RamenConsts.Metric.Docs.out_tuple_count } ;
    { typ_name = "groups" ; typ = { structure = TU64 ; nullable = true } ; units = Some RamenUnits.groups ;
      doc = RamenConsts.Metric.Docs.group_count } ;
    { typ_name = "cpu" ; typ = { structure = TFloat ; nullable = false } ; units = Some RamenUnits.seconds ;
      doc = RamenConsts.Metric.Docs.cpu_time } ;
    { typ_name = "ram" ; typ = { structure = TU64 ; nullable = false } ; units = Some RamenUnits.bytes ;
      doc = RamenConsts.Metric.Docs.ram_usage } ;
    { typ_name = "wait_in" ; typ = { structure = TFloat ; nullable = true } ; units = Some RamenUnits.seconds ;
      doc = RamenConsts.Metric.Docs.rb_wait_read } ;
    { typ_name = "wait_out" ; typ = { structure = TFloat ; nullable = true } ; units = Some RamenUnits.seconds ;
      doc = RamenConsts.Metric.Docs.rb_wait_write } ;
    { typ_name = "bytes_in" ; typ = { structure = TU64 ; nullable = true } ; units = Some RamenUnits.bytes ;
      doc = RamenConsts.Metric.Docs.rb_read_bytes } ;
    { typ_name = "bytes_out" ; typ = { structure = TU64 ; nullable = true } ; units = Some RamenUnits.bytes ;
      doc = RamenConsts.Metric.Docs.rb_write_bytes } ;
    { typ_name = "last_out" ; typ = { structure = TFloat ; nullable = true } ; units = Some RamenUnits.seconds_since_epoch ;
      doc = RamenConsts.Metric.Docs.last_out } ]

let event_time =
  let open RamenEventTime in
  Some (("time", ref OutputField, 1.), DurationConst 0.)

let factors = [ "worker" ]

let nullmask_sz = RingBufLib.nullmask_bytes_of_tuple_type tuple_typ

let fix_sz = RingBufLib.tot_fixsz tuple_typ

(* We will actually allocate that much on the RB since we know most of the
 * time the counters won't be NULL. *)
let max_sersize_of_tuple (worker, _, _, _, _, _, _, _, _, _, _, _, _, _) =
  let open RingBufLib in
  nullmask_sz + fix_sz + sersize_of_string worker

let serialize tx (worker, time, etime, ic, sc, oc, gc, cpu, ram, wi, wo,
                  bi, bo, lo) =
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
    RingBuf.write_float tx offs time ;
    offs + RingBufLib.sersize_of_float in
  let offs = write_nullable_float offs 0 etime in
  let offs = write_nullable_u64 offs 1 ic in
  let offs = write_nullable_u64 offs 2 sc in
  let offs = write_nullable_u64 offs 3 oc in
  let offs = write_nullable_u64 offs 4 gc in
  let offs =
    RingBuf.write_float tx offs cpu ;
    offs + RingBufLib.sersize_of_float in
  let offs =
    RingBuf.write_u64 tx offs ram ;
    offs + RingBufLib.sersize_of_u64 in
  let offs = write_nullable_float offs 5 wi in
  let offs = write_nullable_float offs 6 wo in
  let offs = write_nullable_u64 offs 7 bi in
  let offs = write_nullable_u64 offs 8 bo in
  let offs = write_nullable_float offs 9 lo in
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
  let time = RingBuf.read_float tx offs in
  let etime, offs = read_nullable_float tx 0 offs in
  let offs = offs + RingBufLib.sersize_of_float in
  let ic, offs = read_nullable_u64 tx 1 offs in
  let sc, offs = read_nullable_u64 tx 2 offs in
  let oc, offs = read_nullable_u64 tx 3 offs in
  let gc, offs = read_nullable_u64 tx 4 offs in
  let cpu = RingBuf.read_float tx offs in
  let offs = offs + RingBufLib.sersize_of_float in
  let ram = RingBuf.read_u64 tx offs in
  let offs = offs + RingBufLib.sersize_of_u64 in
  let wi, offs = read_nullable_float tx 5 offs in
  let wo, offs = read_nullable_float tx 6 offs in
  let bi, offs = read_nullable_u64 tx 7 offs in
  let bo, offs = read_nullable_u64 tx 8 offs in
  let lo, offs = read_nullable_float tx 9 offs in
  let t =
    worker, time, etime, ic, sc , oc, gc, cpu, ram, wi, wo, bi, bo, lo in
  assert (offs <= max_sersize_of_tuple t) ;
  t
