(* Network Protocols for collecting metrics *)
open Raql_net_protocol.DessserGen
open Raql_ip_protocol.DessserGen
type t = Raql_net_protocol.DessserGen.t

let string_of_proto = function
  | Collectd -> "Collectd"
  | NetflowV5 -> "NetflowV5"
  | Graphite -> "Graphite"

let tuple_typ_of_proto = function
  | Collectd -> RamenCollectd.tuple_typ
  | NetflowV5 -> RamenNetflow.tuple_typ
  | Graphite -> RamenGraphiteSink.tuple_typ

let collector_of_proto = function
  | Collectd -> "RamenCollectdSerialization.collector"
  | NetflowV5 -> "RamenNetflowSerialization.collector"
  | Graphite -> "RamenGraphiteSink.collector"

let event_time_of_proto = function
  | Collectd -> RamenCollectd.event_time
  | NetflowV5 -> RamenNetflow.event_time
  | Graphite -> RamenGraphiteSink.event_time

let factors_of_proto = function
  | Collectd -> RamenCollectd.factors
  | NetflowV5 -> RamenNetflow.factors
  | Graphite -> RamenGraphiteSink.factors

let string_of_ip_proto = function
  | UDP -> "UDP"
  | TCP -> "TCP"
