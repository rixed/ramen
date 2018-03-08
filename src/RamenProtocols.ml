(* Network Protocols for collecting metrics *)
type net_protocol = Collectd | NetflowV5

let string_of_proto = function
  | Collectd -> "Collectd"
  | NetflowV5 -> "NetflowV5"

let tuple_typ_of_proto = function
  | Collectd -> RamenCollectd.tuple_typ
  | NetflowV5 -> RamenNetflow.tuple_typ

let collector_of_proto = function
  | Collectd -> "RamenCollectd.collector"
  | NetflowV5 -> "RamenNetflow.collector"
