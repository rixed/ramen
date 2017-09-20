(* Network Protocols for collecting metrics *)
type net_protocol = Collectd

let string_of_net_protocol = function
  | Collectd -> "Collectd"

let tuple_typ_of_proto = function
  | Collectd -> RamenCollectd.tuple_typ
