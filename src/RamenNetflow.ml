(* Collector for netflow v5.  *)
open Batteries
open Stdint

open RamenHelpers
open RamenLog
open RamenTuple
module DT = DessserTypes
module N = RamenName
module T = RamenTypes

(* Must be in serialization order, and corresponds to the type defined in
 * RamenNetflowSerialization. *)
let tuple_typ =
  [ { name = N.field "bytes" ;
      typ = DT.required TU32 ;
      units = Some RamenUnits.bytes ;
      doc = "Total number of Layer 3 bytes in the packets of the flow." ;
      aggr = None } ;
    { name = N.field "dst" ;
      typ = DT.required T.ipv4 ;
      units = None ;
      doc = "Destination IP address." ;
      aggr = None } ;
    { name = N.field "dst_as" ;
      typ = DT.required TU16 ;
      units = None ;
      doc = "Autonomous system number of the destination, either origin or peer." ;
      aggr = None } ;
    { name = N.field "dst_mask" ;
      typ = DT.required TU8 ;
      units = None ;
      doc = "Destination address prefix mask bits." ;
      aggr = None } ;
    { name = N.field "dst_port" ;
      typ = DT.required TU16 ;
      units = None ;
      doc = "TCP/UDP destination port number or equivalent." ;
      aggr = None } ;
    { name = N.field "engine_id" ;
      typ = DT.required TU8 ;
      units = None ;
      doc = "Slot number of the flow-switching engine." ;
      aggr = None } ;
    { name = N.field "engine_type" ;
      typ = DT.required TU8 ;
      units = None ;
      doc = "Type of flow-switching engine." ;
      aggr = None } ;
    { name = N.field "in_iface" ;
      typ = DT.required TU16 ;
      units = None ;
      doc = "SNMP index of input interface." ;
      aggr = None } ;
    { name = N.field "ip_proto" ;
      typ = DT.required TU8 ;
      units = None ;
      doc = "IP protocol type (for example, TCP = 6; UDP = 17)." ;
      aggr = None } ;
    { name = N.field "ip_tos" ;
      typ = DT.required TU8 ;
      units = None ;
      doc = "IP type of service (ToS)." ;
      aggr = None } ;
    { name = N.field "next_hop" ;
      typ = DT.required T.ipv4 ;
      units = None ;
      doc = "IP address of next hop router." ;
      aggr = None } ;
    { name = N.field "out_iface" ;
      typ = DT.required TU16 ;
      units = None ;
      doc = "SNMP index of output interface." ;
      aggr = None } ;
    { name = N.field "packets" ;
      typ = DT.required TU32 ;
      units = Some RamenUnits.packets ;
      doc = "Packets in the flow." ;
      aggr = None } ;
    { name = N.field "sampling_rate" ;
      typ = DT.required TU16 ;
      units = Some RamenUnits.dimensionless ;
      doc = "One one packet out of this number were sampled." ;
      aggr = None } ;
    { name = N.field "sampling_type" ;
      typ = DT.required TU8 ;
      units = None ;
      doc = "One of sampled, input or output." ;
      aggr = None } ;
    { name = N.field "seqnum" ;
      typ = DT.required TU32 ;
      units = None ;
      doc = "Sequence counter of total flows seen." ;
      aggr = None } ;
    { name = N.field "source" ;
      typ = DT.optional T.ip ;
      units = None ;
      doc = "IP address of the netflow source." ;
      aggr = None } ;
    { name = N.field "src" ;
      typ = DT.required T.ipv4 ;
      units = None ;
      doc = "Source IP address." ;
      aggr = None } ;
    { name = N.field "src_as" ;
      typ = DT.required TU16 ;
      units = None ;
      doc = "Autonomous system number of the source, either origin or peer." ;
      aggr = None } ;
    { name = N.field "src_mask" ;
      typ = DT.required TU8 ;
      units = None ;
      doc = "Source address prefix mask bits." ;
      aggr = None } ;
    { name = N.field "src_port" ;
      typ = DT.required TU16 ;
      units = None ;
      doc = "TCP/UDP source port number or equivalent" ;
      aggr = None } ;
    { name = N.field "start" ;
      typ = DT.required TFloat ;
      units = Some RamenUnits.seconds_since_epoch ;
      doc = "SysUptime at start of flow." ;
      aggr = None } ;
    { name = N.field "stop" ;
      typ = DT.required TFloat ;
      units = Some RamenUnits.seconds_since_epoch ;
      doc = "SysUptime at the time the last packet of the flow was received." ;
      aggr = None } ;
    { name = N.field "tcp_flags" ;
      typ = DT.required TU8 ;
      units = None ;
      doc = "Cumulative OR of TCP flags." ;
      aggr = None } ]

let event_time =
  let open Event_time.DessserGen in
  let open Event_time_field.DessserGen in
  Some ((N.field "start", OutputField, 1.),
        StopField (N.field "stop", OutputField, 1.))

let factors = [ N.field "source" ]
