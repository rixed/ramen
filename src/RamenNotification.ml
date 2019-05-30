(* Notifications are message send from workers to the alerter daemon via
 * some dedicated ringbuffers, a lot like instrumentation messages. *)
open Batteries
open Stdint
open RamenLog
open RamenTuple
open RamenNullable
open RamenConsts

(* <blink>DO NOT ALTER</blink> this record without also updating
 * the (un)serialization functions. *)
let tuple_typ =
  let open RamenTypes in
  [ { name = N.field "site" ;
      typ = { structure = TString ; nullable = false } ;
      units = None ;
      doc = FieldDocs.site ;
      aggr = None } ;
    { name = N.field "worker" ;
      typ = { structure = TString ; nullable = false } ;
      units = None ;
      doc = FieldDocs.worker ;
      aggr = None } ;
    { name = N.field "start" ;
      typ = { structure = TFloat ; nullable = false } ;
      units = Some RamenUnits.seconds_since_epoch ;
      doc = "Time the notification was sent." ;
      aggr = None } ;
    { name = N.field "event_time" ;
      typ = { structure = TFloat ; nullable = true } ;
      units = Some RamenUnits.seconds_since_epoch ;
      doc = "Time the event occurred." ;
      aggr = None } ;
    { name = N.field "name" ;
      typ = { structure = TString ; nullable = false } ;
      units = None ;
      doc = "" ;
      aggr = None } ;
    { name = N.field "firing" ;
      typ = { structure = TBool ; nullable = true } ;
      units = None ;
      doc = "" ;
      aggr = None } ;
    { name = N.field "certainty" ;
      typ = { structure = TFloat ; nullable = false } ;
      units = Some RamenUnits.dimensionless ;
      doc = "How certain are we that there is a real problem." ;
      aggr = None } ;
    { name = N.field "parameters" ;
      typ =
        { structure =
            TList
              { structure =
                  TTuple [| { structure = TString ; nullable = false } ;
                            { structure = TString ; nullable = false } |] ;
                nullable = false } ;
          nullable = false } ;
      units = None ;
      doc = "List of arbitrary parameters associated with this notification." ;
      aggr = None } ]

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
  [ N.field "site" ;
    N.field "name" ;
    N.field "firing" ]
