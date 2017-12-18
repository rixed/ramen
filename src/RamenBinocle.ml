(* Binocle is the instrumentation lib we use to monitor the workers.
 * Workers output a stream of statistics that are sent through a ringbuf
 * to ramen as they do with exported tuples.
 * Ramen then keep only the latest received info for each node (not in the
 * configuration persistent tree). This info is then added to the json
 * returning the current state of the configuration (/graph).
 *
 * Here we define the tuple type used to store these statistics and various
 * related functions. *)
open Stdint

(* <blink>DO NOT ALTER</blink> this record without also updating
 * tuple_typ below! *)
type tuple =
  string * float *
  Uint64.t option * Uint64.t option * Uint64.t option * Uint64.t option *
  float * Uint64.t * float option * float option * Uint64.t option *
  Uint64.t option

(* Will be needed the day we want to turn ramen instrumentation into an
 * internal data source: *)
let tuple_typ =
  let open RamenSharedTypes in
  [ { typ_name = "worker" ;         nullable = false ;  typ = TString } ;
    { typ_name = "time" ;           nullable = false ;  typ = TFloat } ;
    { typ_name = "in_count" ;       nullable = true ;   typ = TU64 } ;
    { typ_name = "selected_count" ; nullable = true ;   typ = TU64 } ;
    { typ_name = "out_count" ;      nullable = true ;   typ = TU64 } ;
    { typ_name = "group_count" ;    nullable = true ;   typ = TU64 } ;
    { typ_name = "cpu" ;            nullable = false ;  typ = TFloat } ;
    { typ_name = "ram" ;            nullable = false ;  typ = TU64 } ;
    { typ_name = "wait_in" ;        nullable = true ;   typ = TFloat } ;
    { typ_name = "wait_out" ;       nullable = true ;   typ = TFloat } ;
    { typ_name = "bytes_in" ;       nullable = true ;   typ = TU64 } ;
    { typ_name = "bytes_out" ;      nullable = true ;   typ = TU64 } ]

let nb_nullables =
  List.fold_left (fun c t ->
      if t.RamenSharedTypes.nullable then c+1 else c
    ) 0 tuple_typ

let nullmask_sz = RingBufLib.nullmask_bytes_of_tuple_type tuple_typ

let fix_sz =
  List.fold_left (fun c t ->
    if t.RamenSharedTypes.typ = TString then c else
    c + RingBufLib.sersize_of_fixsz_typ t.typ) 0 tuple_typ

(* We will actually allocate that much on the RB since we know most of the
 * time the counters won't be NULL. *)
let max_sersize_of_tuple (worker, _, _, _, _, _, _, _, _, _, _, _) =
  let open RingBufLib in
  nullmask_sz + fix_sz + sersize_of_string worker

let serialize tx (worker, time, ic, sc, oc, gc, cpu, ram, wi, wo, bi, bo) =
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
  let offs =
    RingBuf.write_string tx offs worker ;
    offs + RingBufLib.sersize_of_string worker in
  let offs =
    RingBuf.write_float tx offs time ;
    offs + RingBufLib.sersize_of_fixsz_typ TFloat in
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
  let offs = offs + RingBufLib.sersize_of_string worker in
  let time = RingBuf.read_float tx offs in
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
  let t = worker, time, ic, sc , oc, gc, cpu, ram, wi, wo, bi, bo in
  assert (offs <= max_sersize_of_tuple t) ;
  t
