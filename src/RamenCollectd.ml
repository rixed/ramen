(* Collector for collectd binary protocol (as described in
 * https://collectd.org/wiki/index.php/Binary_protocol).
 *
 * Collectd sends one (or several) UDP datagrams every X seconds (usually
 * X=10s).  Messages contains individual metrics that are composed of a time, a
 * hostname, a metric name (composed of plugin name, instance name, etc) and
 * one (or sometime several) numerical values.  We map this into a tuple which
 * type is known. But since there is no boundary in between different
 * collections and no guarantee that the time of all closely related
 * measurements are strictly the same, what we do is accumulate every metric
 * until the time changes significantly (more than, say, 5s) or we encounter a
 * metric name we already have.  Only then do we output a tuple. *)
open Batteries

open RamenLog
open RamenHelpers
open RamenTuple
module DT = DessserTypes
module N = RamenName
module T = RamenTypes

let tuple_typ =
  [ { name = N.field "host" ;
      typ = DT.required (Base String) ;
      units = None ;
      doc = "" ;
      aggr = None } ;
    { name = N.field "start" ;
      typ = DT.required (Base Float) ;
      units = Some RamenUnits.seconds_since_epoch ;
      doc = "" ;
      aggr = None } ;
    { name = N.field "plugin" ;
      typ = DT.optional (Base String) ;
      units = None ;
      doc = "" ;
      aggr = None } ;
    { name = N.field "instance" ;
      typ = DT.optional (Base String) ;
      units = None ;
      doc = "" ;
      aggr = None } ;
    { name = N.field "type_name" ;
      typ = DT.optional (Base String) ;
      units = None ;
      doc = "" ;
      aggr = None } ;
    { name = N.field "type_instance" ;
      typ = DT.optional (Base String) ;
      units = None ;
      doc = "" ;
      aggr = None } ;
    { name = N.field "value" ;
      typ = DT.required (Base Float) ;
      units = None ;
      doc = "" ;
      aggr = None } ;
    { name = N.field "value2" ;
      typ = DT.optional (Base Float) ;
      units = None ;
      doc = "" ;
      aggr = None } ;
    { name = N.field "value3" ;
      typ = DT.optional (Base Float) ;
      units = None ;
      doc = "" ;
      aggr = None } ;
    { name = N.field "value4" ;
      typ = DT.optional (Base Float) ;
      units = None ;
      doc = "" ;
      aggr = None } ;
    { name = N.field "value5" ;
      typ = DT.optional (Base Float) ;
      units = None ;
      doc = "" ;
      aggr = None } ] |>
  RamenFieldOrder.order_tuple

let event_time =
  let open Event_time.DessserGen in
  let open Event_time_field.DessserGen in
  Some ((N.field "start", OutputField, 1.),
        DurationConst 0.)

let factors =
  [ N.field "plugin" ;
    N.field "type_instance" ;
    N.field "instance" ]
