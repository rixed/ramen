open Batteries
module T = RamenTypes

(* Network Protocols for collecting metrics *)
type net_protocol = Collectd | NetflowV5 | Graphite
  [@@ppp PPP_OCaml]

let string_of_proto = function
  | Collectd -> "Collectd"
  | NetflowV5 -> "NetflowV5"
  | Graphite -> "Graphite"

let typ_of_proto = function
  | Collectd -> RamenCollectd.typ
  | NetflowV5 -> RamenNetflow.typ
  | Graphite -> RamenGraphiteSink.typ

let fields_of_proto =
  T.fields_of_type % typ_of_proto

let collector_of_proto = function
  | Collectd -> "RamenCollectd.collector"
  | NetflowV5 -> "RamenNetflow.collector"
  | Graphite -> "RamenGraphiteSink.collector"

let event_time_of_proto = function
  | Collectd -> RamenCollectd.event_time
  | NetflowV5 -> RamenNetflow.event_time
  | Graphite -> RamenGraphiteSink.event_time

let factors_of_proto = function
  | Collectd -> RamenCollectd.factors
  | NetflowV5 -> RamenNetflow.factors
  | Graphite -> RamenGraphiteSink.factors
