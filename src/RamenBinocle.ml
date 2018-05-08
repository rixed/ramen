(* Binocle is the instrumentation lib we use to monitor the workers.
 * Workers output a stream of statistics that are sent through a ringbuf
 * to ramen as they do with exported tuples.
 * Ramen then keep only the latest received info for each func (not in the
 * configuration persistent tree). This info is then added to the json
 * returning the current state of the configuration (/graph).
 *
 * Here we define the tuple type used to store these statistics and various
 * related functions. *)
open Stdint
open RamenLog
open RamenTuple

(* <blink>DO NOT ALTER</blink> this record without also updating
 * tuple_typ below and `ramen ps` implementation! *)
type tuple =
  string * float *
  Uint64.t option * Uint64.t option * Uint64.t option * Uint64.t option *
  float * Uint64.t * float option * float option * Uint64.t option *
  Uint64.t option * float option

let tuple_typ =
  [ { typ_name = "worker" ;          nullable = false ;  typ = TString } ;
    { typ_name = "time" ;            nullable = false ;  typ = TFloat } ;
    { typ_name = "tuples_in" ;       nullable = true ;   typ = TU64 } ;
    { typ_name = "tuples_selected" ; nullable = true ;   typ = TU64 } ;
    { typ_name = "tuples_out" ;      nullable = true ;   typ = TU64 } ;
    { typ_name = "groups" ;          nullable = true ;   typ = TU64 } ;
    { typ_name = "cpu" ;             nullable = false ;  typ = TFloat } ;
    { typ_name = "ram" ;             nullable = false ;  typ = TU64 } ;
    { typ_name = "wait_in" ;         nullable = true ;   typ = TFloat } ;
    { typ_name = "wait_out" ;        nullable = true ;   typ = TFloat } ;
    { typ_name = "bytes_in" ;        nullable = true ;   typ = TU64 } ;
    { typ_name = "bytes_out" ;       nullable = true ;   typ = TU64 } ;
    { typ_name = "last_out" ;        nullable = true ;   typ = TFloat } ]

let event_time = Some (("time", 1.), RamenEventTime.DurationConst 0.)

let factors = [ "worker" ]

let nb_nullables =
  List.fold_left (fun c t ->
      if t.RamenTuple.nullable then c+1 else c
    ) 0 tuple_typ

let nullmask_sz = RingBufLib.nullmask_bytes_of_tuple_type tuple_typ

let fix_sz =
  List.fold_left (fun c t ->
    if t.RamenTuple.typ = TString then c else
    c + RingBufLib.sersize_of_fixsz_typ t.typ) 0 tuple_typ

(* We will actually allocate that much on the RB since we know most of the
 * time the counters won't be NULL. *)
let max_sersize_of_tuple (worker, _, _, _, _, _, _, _, _, _, _, _, _) =
  let open RingBufLib in
  nullmask_sz + fix_sz + sersize_of_string worker

let check_tuple (worker, time, ic, sc, oc, gc, cpu, ram, wi, wo, bi, bo, lo) =
  let too_voluminous =
    let max_vol = Stdint.Uint64.of_string "1000000000000" in
    function
      | None -> false
      | Some f -> f > max_vol in
  if too_voluminous bi || too_voluminous bo then (
    let s = Uint64.to_string in
    let o = function None -> "None" | Some u -> s u in
    let f = function None -> "None" | Some f -> string_of_float f in
    !logger.error "volume stats incorrect: \
      %s, %f, %s, %s, %s, %s, %f, %s, %s, %s, %s, %s"
      worker time (o ic) (o sc) (o oc) (o gc) cpu (s ram) (f wi) (f wo)
      (o bi) (o bo)
  )

let serialize tx (worker, time, ic, sc, oc, gc, cpu, ram, wi, wo, bi, bo,
                  lo as t) =
  check_tuple t ;
  RingBuf.zero_bytes tx 0 nullmask_sz ; (* zero the nullmask *)
  let write_nullable_thing w sz offs null_i = function
    | None ->
      offs
    | Some v ->
      RingBuf.set_bit tx null_i ;
      w tx offs v ;
      offs + sz in
  let write_nullable_u64 =
    let sz = RingBufLib.sersize_of_fixsz_typ TU64 in
    write_nullable_thing RingBuf.write_u64 sz
  and write_nullable_float =
    let sz = RingBufLib.sersize_of_fixsz_typ TFloat in
    write_nullable_thing RingBuf.write_float sz in
  let offs = nullmask_sz in
  if worker = "" then
    !logger.error "empty worker while serializing binocle tuple" ;
  let offs =
    RingBuf.write_string tx offs worker ;
    offs + RingBufLib.sersize_of_string worker in
  let offs =
    RingBuf.write_float tx offs time ;
    offs + RingBufLib.sersize_of_fixsz_typ TFloat in
  if time > 2000000000. then
    !logger.error "wrong time (%f) while serializing binocle tuple" time ;
  let offs = write_nullable_u64 offs 0 ic in
  let offs = write_nullable_u64 offs 1 sc in
  let offs = write_nullable_u64 offs 2 oc in
  let offs = write_nullable_u64 offs 3 gc in
  let offs =
    RingBuf.write_float tx offs cpu ;
    offs + RingBufLib.sersize_of_fixsz_typ TFloat in
  let offs =
    RingBuf.write_u64 tx offs ram ;
    offs + RingBufLib.sersize_of_fixsz_typ TU64 in
  let offs = write_nullable_float offs 4 wi in
  let offs = write_nullable_float offs 5 wo in
  let offs = write_nullable_u64 offs 6 bi in
  let offs = write_nullable_u64 offs 7 bo in
  let offs = write_nullable_float offs 8 lo in
  offs

let unserialize tx =
  let read_nullable_thing r sz tx null_i offs =
    if RingBuf.get_bit tx null_i then
      Some (r tx offs), offs + sz
    else
      None, offs in
  let read_nullable_u64 =
    let sz = RingBufLib.sersize_of_fixsz_typ TU64 in
    read_nullable_thing RingBuf.read_u64 sz
  and read_nullable_float =
    let sz = RingBufLib.sersize_of_fixsz_typ TFloat in
    read_nullable_thing RingBuf.read_float sz in
  let offs = nullmask_sz in
  let worker = RingBuf.read_string tx offs in
  if worker = "" then
    !logger.error "empty worker while deserializing binocle tuple" ;
  let offs = offs + RingBufLib.sersize_of_string worker in
  let time = RingBuf.read_float tx offs in
  if time > 2000000000. then
    !logger.error "wrong time (%f) while deserializing binocle tuple" time ;
  let offs = offs + RingBufLib.sersize_of_fixsz_typ TFloat in
  let ic, offs = read_nullable_u64 tx 0 offs in
  let sc, offs = read_nullable_u64 tx 1 offs in
  let oc, offs = read_nullable_u64 tx 2 offs in
  let gc, offs = read_nullable_u64 tx 3 offs in
  let cpu = RingBuf.read_float tx offs in
  let offs = offs + RingBufLib.sersize_of_fixsz_typ TFloat in
  let ram = RingBuf.read_u64 tx offs in
  let offs = offs + RingBufLib.sersize_of_fixsz_typ TU64 in
  let wi, offs = read_nullable_float tx 4 offs in
  let wo, offs = read_nullable_float tx 5 offs in
  let bi, offs = read_nullable_u64 tx 6 offs in
  let bo, offs = read_nullable_u64 tx 7 offs in
  let lo, offs = read_nullable_float tx 8 offs in
  let t = worker, time, ic, sc , oc, gc, cpu, ram, wi, wo, bi, bo, lo in
  assert (offs <= max_sersize_of_tuple t) ;
  check_tuple t ;
  t
