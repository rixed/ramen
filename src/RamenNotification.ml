(* Notifications are message send from workers to the alerter daemon via
 * some dedicated ringbuffers, a lot like instrumentation messages. *)
open Batteries
open Stdint
open RamenLog
open RamenTuple
open RamenNullable

(* <blink>DO NOT ALTER</blink> this record without also updating
 * the (un)serialization functions. *)
let tuple_typ =
  let open RamenTypes in
  [ { name = N.field "worker" ; typ = { structure = TString ; nullable = false } ; units = None ; doc = "" ; aggr = None } ;
    { name = N.field "start" ; typ = { structure = TFloat ; nullable = false } ; units = Some RamenUnits.seconds_since_epoch ;
      doc = "Time the notification was sent." ; aggr = None } ;
    { name = N.field "event_time" ; typ = { structure = TFloat ; nullable = true } ; units = Some RamenUnits.seconds_since_epoch ;
      doc = "Time the event occurred." ; aggr = None } ;
    { name = N.field "name" ; typ = { structure = TString ; nullable = false } ; units = None ; doc = "" ; aggr = None } ;
    { name = N.field "firing" ; typ = { structure = TBool ; nullable = true } ; units = None ; doc = "" ; aggr = None } ;
    { name = N.field "certainty" ; typ = { structure = TFloat ; nullable = false } ; units = Some RamenUnits.dimensionless ;
      doc = "How certain are we that there is a real problem." ; aggr = None } ;
    { name = N.field "parameters" ;
      typ = { structure = TList { structure = TTuple [|
                                    { structure = TString ;
                                      nullable = false } ;
                                    { structure = TString ;
                                      nullable = false } |] ;
                                  nullable = false } ;
              nullable = false } ; units = None ;
      doc = "List of arbitrary parameters associated with this notification." ; aggr = None } ]

(* Should the event time of a notification event the time it's been sent, or
 * the event time it refers to? It seems more logical to have it the time it's
 * sent... *)
let event_time =
  let open RamenEventTime in
  Some ((N.field "start", ref OutputField, 1.),
        DurationConst 0.)

(* We trust the user not to generate too many distinct names and use instead
 * parameters to store arbitrary values. *)
let factors =
  [ N.field "name" ;
    N.field "firing" ]

open RingBuf
open RingBufLib

let nullmask_sz =
  let sz = nullmask_bytes_of_tuple_type tuple_typ in
  assert (sz = notification_nullmask_sz) ; (* FIXME *)
  sz

let fix_sz =
  let sz = tot_fixsz tuple_typ in
  assert (sz = notification_fixsz) ; (* FIXME *)
  sz

let unserialize tx start_offs =
  let read_nullable_thing r sz null_i offs =
    if get_bit tx start_offs null_i then
      NotNull (r tx offs), offs + sz
    else
      Null, offs in
  let read_nullable_float =
    let sz = sersize_of_float in
    read_nullable_thing read_float sz in
  let read_nullable_bool =
    let sz = sersize_of_bool in
    read_nullable_thing read_bool sz in
  let offs = start_offs + nullmask_sz in
  let worker = read_string tx offs in
  let offs = offs + sersize_of_string worker in
  let start = read_float tx offs in
  let offs = offs + sersize_of_float in
  let event_time, offs = read_nullable_float 0 offs in
  let name = read_string tx offs in
  let offs = offs + sersize_of_string name in
  let firing, offs = read_nullable_bool 1 offs in
  let certainty = read_float tx offs in
  let offs = offs + sersize_of_float in
  let num_params = read_u32 tx offs |> Uint32.to_int in
  let offs = offs + sersize_of_u32 in
  (* We also have the vector internal nullmask, even though the parameters
   * cannot be NULL: *)
  let offs = offs + nullmask_sz_of_vector num_params in
  let offs = ref offs in
  let parameters =
    Array.init num_params (fun _ ->
      (* Also need to skip the tuple (pair) internal nullmask: *)
      offs := !offs + nullmask_sz_of_vector 2 ;
      let n = read_string tx !offs in
      offs := !offs + sersize_of_string n ;
      let v = read_string tx !offs in
      offs := !offs + sersize_of_string v ;
      n, v
    ) in
  let t =
    worker, start, event_time, name, firing, certainty, parameters in
  assert (!offs <= start_offs + max_sersize_of_notification t) ;
  t
