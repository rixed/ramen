(* Serialization part of RamenWorkerStats,
 * to reduce dependencies on ringbuf lib: *)
open RamenNullable
open RamenHelpers
open RingBuf
open RingBufLib
open RamenWorkerStats

let perf_nullmask_sz =
  RingBufLib.nullmask_sz_of_record perf_kts

let sersize_of_perf =
  perf_nullmask_sz +
  RingBufLib.(sersize_of_u32 + sersize_of_float + sersize_of_float)

let profile_nullmask_sz =
  RingBufLib.nullmask_sz_of_record profile_fields

let sersize_of_profile =
  profile_nullmask_sz + Array.length profile_fields * sersize_of_perf

let nullmask_sz = nullmask_bytes_of_tuple_type tuple_typ

let fix_sz =
  tot_fixsz tuple_typ +
  (* Records are not considered fixed-sized by tot_fixsz (TODO), but it is so
   * add it now. Note: All fields of a Tuples/records are considered
   * nullable, therefore the (useless) nullmask. Cf. isssue #712 *)
  (profile_nullmask_sz +
   Array.length profile_fields * sersize_of_perf)

(* We will actually allocate that much on the RB since we know most of the
 * time the counters won't be NULL. *)
let max_sersize_of_tuple (site, worker, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =
  nullmask_sz +
  fix_sz +
  sersize_of_string site +
  sersize_of_string worker

let serialize tx start_offs
              (site, worker, is_top_half, start, min_etime, max_etime,
               ic, sc, oc, gc, cpu, ram, max_ram, profile, wi, wo, bi, bo,
               os, lo, stime) =
  zero_bytes tx start_offs nullmask_sz ; (* zero the nullmask *)
  let null_i = ref 0 in
  let write_nullable_thing w sz offs t =
    let offs =
      match t with
      | Null ->
          offs
      | NotNull v ->
          set_bit tx start_offs !null_i ;
          w tx offs v ;
          offs + sz in
    incr null_i ;
    offs in
  let write_nullable_u64 =
    let sz = sersize_of_u64 in
    write_nullable_thing write_u64 sz
  and write_nullable_float =
    let sz = sersize_of_float in
    write_nullable_thing write_float sz
  and write_perf offs (count, system, user) =
    (* Cf issue #712: *)
    zero_bytes tx offs perf_nullmask_sz ;
    let offs = offs + perf_nullmask_sz in
    write_u32 tx offs count ;
    let offs = offs + sersize_of_u32 in
    write_float tx offs system ;
    let offs = offs + sersize_of_float in
    write_float tx offs user ;
    offs + sersize_of_float in
  let offs = start_offs + nullmask_sz in
  let offs =
    write_string tx offs site ;
    offs + sersize_of_string site in
  let offs =
    write_string tx offs worker ;
    offs + sersize_of_string worker in
  let offs =
    write_bool tx offs is_top_half ;
    offs + sersize_of_bool in
  let offs =
    write_float tx offs start ;
    offs + sersize_of_float in
  let offs = write_nullable_float offs min_etime in
  let offs = write_nullable_float offs max_etime in
  let offs = write_nullable_u64 offs ic in
  let offs = write_nullable_u64 offs sc in
  let offs = write_nullable_u64 offs oc in
  let offs = write_nullable_u64 offs gc in
  let offs =
    write_float tx offs cpu ;
    offs + sersize_of_float in
  let offs =
    write_u64 tx offs ram ;
    offs + sersize_of_u64 in
  let offs =
    write_u64 tx offs max_ram ;
    offs + sersize_of_u64 in
  let offs =
    (* Cf issue #712: *)
    zero_bytes tx offs profile_nullmask_sz ;
    let offs = offs + profile_nullmask_sz in
    let ci, co, fo, fg, fp, so, pt, ug, wf, ws = profile in
    let offs = write_perf offs ci in
    let offs = write_perf offs co in
    let offs = write_perf offs fo in
    let offs = write_perf offs fg in
    let offs = write_perf offs fp in
    let offs = write_perf offs so in
    let offs = write_perf offs pt in
    let offs = write_perf offs ug in
    let offs = write_perf offs wf in
    let offs = write_perf offs ws in
    offs in
  let offs = write_nullable_float offs wi in
  let offs = write_nullable_float offs wo in
  let offs = write_nullable_u64 offs bi in
  let offs = write_nullable_u64 offs bo in
  let offs = write_nullable_u64 offs os in
  let offs = write_nullable_float offs lo in
  let offs =
    write_float tx offs stime ;
    offs + sersize_of_float in
  offs

let unserialize tx start_offs =
  let null_i = ref 0 in
  let read_nullable_thing r sz offs =
    let res =
      if get_bit tx start_offs !null_i then
        NotNull (r tx offs), offs + sz
      else
        Null, offs in
    incr null_i ;
    res in
  let read_nullable_u64 =
    let sz = sersize_of_u64 in
    read_nullable_thing read_u64 sz
  and read_nullable_float =
    let sz = sersize_of_float in
    read_nullable_thing read_float sz
  and read_perf offs =
    (* Cf issue #712: *)
    let offs = offs + perf_nullmask_sz in
    let count = read_u32 tx offs in
    let offs = offs + sersize_of_u32 in
    let system = read_float tx offs in
    let offs = offs + sersize_of_float in
    let user = read_float tx offs in
    let offs = offs + sersize_of_float in
    (count, system, user), offs in
  let offs = start_offs + nullmask_sz in
  let site = read_string tx offs in
  let offs = offs + sersize_of_string site in
  let worker = read_string tx offs in
  let offs = offs + sersize_of_string worker in
  let is_top_half = read_bool tx offs in
  let offs = offs + sersize_of_bool in
  let start = read_float tx offs in
  let offs = offs + sersize_of_float in
  let min_etime, offs = read_nullable_float offs in
  let max_etime, offs = read_nullable_float offs in
  let ic, offs = read_nullable_u64 offs in
  let sc, offs = read_nullable_u64 offs in
  let oc, offs = read_nullable_u64 offs in
  let gc, offs = read_nullable_u64 offs in
  let cpu = read_float tx offs in
  let offs = offs + sersize_of_float in
  let ram = read_u64 tx offs in
  let offs = offs + sersize_of_u64 in
  let max_ram = read_u64 tx offs in
  let offs = offs + sersize_of_u64 in
  let profile, offs =
    (* Cf issue #712: *)
    let offs = offs + profile_nullmask_sz in
    let ci, offs = read_perf offs in
    let co, offs = read_perf offs in
    let fo, offs = read_perf offs in
    let fg, offs = read_perf offs in
    let fp, offs = read_perf offs in
    let so, offs = read_perf offs in
    let pt, offs = read_perf offs in
    let ug, offs = read_perf offs in
    let wf, offs = read_perf offs in
    let ws, offs = read_perf offs in
    (ci, co, fo, fg, fp, so, pt, ug, wf, ws), offs in
  let wi, offs = read_nullable_float offs in
  let wo, offs = read_nullable_float offs in
  let bi, offs = read_nullable_u64 offs in
  let bo, offs = read_nullable_u64 offs in
  let os, offs = read_nullable_u64 offs in
  let lo, offs = read_nullable_float offs in
  let stime = read_float tx offs in
  let offs = offs + sersize_of_float in
  let t =
    site, worker, is_top_half, start, min_etime, max_etime, ic, sc, oc, gc,
    cpu, ram, max_ram,
    profile, wi, wo, bi, bo, os, lo, stime in
  assert (offs <= start_offs + max_sersize_of_tuple t) ;
  t


