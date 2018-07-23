(* Notifications are message send from workers to the notifier daemon via
 * some dedicated ringbuffers, a lot like instrumentation messages. *)
open Batteries
open Stdint
open RamenLog
open RamenTuple

(* <blink>DO NOT ALTER</blink> this record without also updating
 * the (un)serialization functions. *)
let tuple_typ =
  let open RamenTypes in
  [ { typ_name = "worker" ; typ = { structure = TString ; nullable = Some false }} ;
    { typ_name = "sent_time" ; typ = { structure = TFloat ; nullable = Some false } } ;
    { typ_name = "event_time" ; typ = { structure = TFloat ; nullable = Some true } } ;
    { typ_name = "name" ; typ = { structure = TString ; nullable = Some false } } ;
    { typ_name = "firing" ; typ = { structure = TBool ; nullable = Some false } } ;
    { typ_name = "certainty" ; typ = { structure = TFloat ; nullable = Some false } } ;
    { typ_name = "parameters" ;
      typ = { structure = TList { structure = TTuple [|
                                    { structure = TString ;
                                      nullable = Some false } ;
                                    { structure = TString ;
                                      nullable = Some false } |] ;
                                  nullable = Some false } ;
              nullable = Some false } } ]

(* Should the event time of a notification event the time it's been sent, or
 * the event time it refers to? It seems more logical to have it the time it's
 * sent... *)
let event_time =
  let open RamenEventTime in
  Some (("sent_time", ref OutputField, 1.), DurationConst 0.)

(* We trust the user not to generate too many distinct names and use instead
 * parameters to store arbitrary values. *)
let factors = [ "name" ; "firing" ]

let nullmask_sz =
  let sz = RingBufLib.nullmask_bytes_of_tuple_type tuple_typ in
  assert (sz = RingBufLib.notification_nullmask_sz) ; (* FIXME *)
  sz

let fix_sz =
  let sz = RingBufLib.tot_fixsz tuple_typ in
  assert (sz = RingBufLib.notification_fixsz) ; (* FIXME *)
  sz

let unserialize tx =
  let read_nullable_thing r sz tx null_i offs =
    if RingBuf.get_bit tx null_i then
      Some (r tx offs), offs + sz
    else
      None, offs in
  let read_nullable_float =
    let sz = RingBufLib.sersize_of_fixsz_typ TFloat in
    read_nullable_thing RingBuf.read_float sz in
  let offs = nullmask_sz in
  let worker = RingBuf.read_string tx offs in
  let offs = offs + RingBufLib.sersize_of_string worker in
  let sent_time = RingBuf.read_float tx offs in
  let offs = offs + RingBufLib.sersize_of_float in
  let event_time, offs = read_nullable_float tx 0 offs in
  let name = RingBuf.read_string tx offs in
  let offs = offs + RingBufLib.sersize_of_string name in
  let firing = RingBuf.read_bool tx offs in
  let offs = offs + RingBufLib.sersize_of_bool in
  let certainty = RingBuf.read_float tx offs in
  let offs = offs + RingBufLib.sersize_of_float in
  let num_params = RingBuf.read_u32 tx offs |> Uint32.to_int in
  let offs = offs + RingBufLib.sersize_of_u32 in
  (* We also have the vector internal nullmask, even though the parameters
   * cannot be NULL: *)
  let offs = offs + RingBufLib.nullmask_sz_of_vector num_params in
  let offs = ref offs in
  let parameters =
    Array.init num_params (fun i ->
      (* Also need to skip the tuple (pair) internal nullmask: *)
      offs := !offs + RingBufLib.nullmask_sz_of_vector 2 ;
      let n = RingBuf.read_string tx !offs in
      offs := !offs + RingBufLib.sersize_of_string n ;
      let v = RingBuf.read_string tx !offs in
      offs := !offs + RingBufLib.sersize_of_string v ;
      n, v
    ) in
  let t =
    worker, sent_time, event_time, name, firing, certainty, parameters in
  assert (!offs <= RingBufLib.max_sersize_of_notification t) ;
  t
