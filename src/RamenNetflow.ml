(* Collector for netflow v5.  *)
open Batteries
open RamenLog
open RamenHelpers
open RamenNullable
open Stdint
open RamenTuple
module N = RamenName

(* <blink>DO NOT ALTER</blink> this record without also updating
 * wrap_netflow_decode in wrap_netflow.c and tuple_typ below! *)
type netflow_metric =
  RamenIp.t nullable * float * float *
  Uint32.t * Uint8.t * Uint8.t * Uint8.t * Uint16.t *
  Uint32.t * Uint32.t * Uint32.t * Uint16.t * Uint16.t *
  Uint16.t * Uint16.t * Uint32.t * Uint32.t * Uint8.t * Uint8.t *
  Uint8.t * Uint16.t * Uint16.t * Uint8.t * Uint8.t

let tuple_typ =
  [ { name = N.field "source" ;
      typ = { structure = TIp ; nullable = true } ;
      units = None ;
      doc = "IP address of the netflow source." ;
      aggr = None } ;
    { name = N.field "start" ;
      typ = { structure = TFloat ; nullable = false } ;
      units = Some RamenUnits.seconds_since_epoch ;
      doc = "SysUptime at start of flow." ;
      aggr = None } ;
    { name = N.field "stop" ;
      typ = { structure = TFloat ; nullable = false } ;
      units = Some RamenUnits.seconds_since_epoch ;
      doc = "SysUptime at the time the last packet of the flow was received." ;
      aggr = None } ;
    { name = N.field "seqnum" ;
      typ = { structure = TU32 ; nullable = false } ;
      units = None ;
      doc = "Sequence counter of total flows seen." ;
      aggr = None } ;
    { name = N.field "engine_type" ;
      typ = { structure = TU8 ; nullable = false } ;
      units = None ;
      doc = "Type of flow-switching engine." ;
      aggr = None } ;
    { name = N.field "engine_id" ;
      typ = { structure = TU8 ; nullable = false } ;
      units = None ;
      doc = "Slot number of the flow-switching engine." ;
      aggr = None } ;
    { name = N.field "sampling_type" ;
      typ = { structure = TU8 ; nullable = false } ;
      units = None ;
      doc = "One of sampled, input or output." ;
      aggr = None } ;
    { name = N.field "sampling_rate" ;
      typ = { structure = TU16 ; nullable = false } ;
      units = Some RamenUnits.dimensionless ;
      doc = "One one packet out of this number were sampled." ;
      aggr = None } ;
    { name = N.field "src" ;
      typ = { structure = TIpv4 ; nullable = false } ;
      units = None ;
      doc = "Source IP address." ;
      aggr = None } ;
    { name = N.field "dst" ;
      typ = { structure = TIpv4 ; nullable = false } ;
      units = None ;
      doc = "Destination IP address." ;
      aggr = None } ;
    { name = N.field "next_hop" ;
      typ = { structure = TIpv4 ; nullable = false } ;
      units = None ;
      doc = "IP address of next hop router." ;
      aggr = None } ;
    { name = N.field "src_port" ;
      typ = { structure = TU16 ; nullable = false } ;
      units = None ;
      doc = "TCP/UDP source port number or equivalent" ;
      aggr = None } ;
    { name = N.field "dst_port" ;
      typ = { structure = TU16 ; nullable = false } ;
      units = None ;
      doc = "TCP/UDP destination port number or equivalent." ;
      aggr = None } ;
    { name = N.field "in_iface" ;
      typ = { structure = TU16 ; nullable = false } ;
      units = None ;
      doc = "SNMP index of input interface." ;
      aggr = None } ;
    { name = N.field "out_iface" ;
      typ = { structure = TU16 ; nullable = false } ;
      units = None ;
      doc = "SNMP index of output interface." ;
      aggr = None } ;
    { name = N.field "packets" ;
      typ = { structure = TU32 ; nullable = false } ;
      units = Some RamenUnits.packets ;
      doc = "Packets in the flow." ;
      aggr = None } ;
    { name = N.field "bytes" ;
      typ = { structure = TU32 ; nullable = false } ;
      units = Some RamenUnits.bytes ;
      doc = "Total number of Layer 3 bytes in the packets of the flow." ;
      aggr = None } ;
    { name = N.field "tcp_flags" ;
      typ = { structure = TU8 ; nullable = false } ;
      units = None ;
      doc = "Cumulative OR of TCP flags." ;
      aggr = None } ;
    { name = N.field "ip_proto" ;
      typ = { structure = TU8 ; nullable = false } ;
      units = None ;
      doc = "IP protocol type (for example, TCP = 6; UDP = 17)." ;
      aggr = None } ;
    { name = N.field "ip_tos" ;
      typ = { structure = TU8 ; nullable = false } ;
      units = None ;
      doc = "IP type of service (ToS)." ;
      aggr = None } ;
    { name = N.field "src_as" ;
      typ = { structure = TU16 ; nullable = false } ;
      units = None ;
      doc = "Autonomous system number of the source, either origin or peer." ;
      aggr = None } ;
    { name = N.field "dst_as" ;
      typ = { structure = TU16 ; nullable = false } ;
      units = None ;
      doc = "Autonomous system number of the destination, either origin or peer." ;
      aggr = None } ;
    { name = N.field "src_mask" ;
      typ = { structure = TU8 ; nullable = false } ;
      units = None ;
      doc = "Source address prefix mask bits." ;
      aggr = None } ;
    { name = N.field "dst_mask" ;
      typ = { structure = TU8 ; nullable = false } ;
      units = None ;
      doc = "Destination address prefix mask bits." ;
      aggr = None } ]

let event_time =
  let open RamenEventTime in
  Some ((N.field "start", ref OutputField, 1.),
        StopField (N.field "stop", ref OutputField, 1.))

let factors = [ N.field "source" ]
