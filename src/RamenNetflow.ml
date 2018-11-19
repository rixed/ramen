(* Collector for netflow v5.  *)
open Batteries
open RamenLog
open RamenHelpers
open Stdint
open RamenTuple

(* <blink>DO NOT ALTER</blink> this record without also updating
 * wrap_netflow_decode in wrap_netflow.c and tuple_typ below! *)
type netflow_metric =
  string * float * float *
  Uint32.t * Uint8.t * Uint8.t * Uint8.t * Uint16.t *
  Uint32.t * Uint32.t * Uint32.t * Uint16.t * Uint16.t *
  Uint16.t * Uint16.t * Uint32.t * Uint32.t * Uint8.t * Uint8.t *
  Uint8.t * Uint16.t * Uint16.t * Uint8.t * Uint8.t

let tuple_typ =
  [ { name = RamenName.field_of_string "source" ; typ = { structure = TString ; nullable = false } ; units = None ;
      doc = "IP address of the netflow source." ; aggr = None } ;
    { name = RamenName.field_of_string "start" ; typ = { structure = TFloat ; nullable = false } ; units = Some RamenUnits.seconds_since_epoch ;
      doc = "SysUptime at start of flow." ; aggr = None } ;
    { name = RamenName.field_of_string "stop" ; typ = { structure = TFloat ; nullable = false } ; units = Some RamenUnits.seconds_since_epoch ;
      doc = "SysUptime at the time the last packet of the flow was received." ; aggr = None } ;
    { name = RamenName.field_of_string "seqnum" ; typ = { structure = TU32 ; nullable = false } ; units = None ;
      doc = "Sequence counter of total flows seen." ; aggr = None } ;
    { name = RamenName.field_of_string "engine_type" ; typ = { structure = TU8 ; nullable = false } ; units = None ;
      doc = "Type of flow-switching engine." ; aggr = None } ;
    { name = RamenName.field_of_string "engine_id" ; typ = { structure = TU8 ; nullable = false } ; units = None ;
      doc = "Slot number of the flow-switching engine." ; aggr = None } ;
    { name = RamenName.field_of_string "sampling_type" ; typ = { structure = TU8 ; nullable = false } ; units = None ;
      doc = "One of sampled, input or output." ; aggr = None } ;
    { name = RamenName.field_of_string "sampling_rate" ; typ = { structure = TU16 ; nullable = false } ; units = Some RamenUnits.dimensionless ;
      doc = "One one packet out of this number were sampled." ; aggr = None } ;
    { name = RamenName.field_of_string "src" ; typ = { structure = TIpv4 ; nullable = false } ; units = None ;
      doc = "Source IP address." ; aggr = None } ;
    { name = RamenName.field_of_string "dst" ; typ = { structure = TIpv4 ; nullable = false } ; units = None ;
      doc = "Destination IP address." ; aggr = None } ;
    { name = RamenName.field_of_string "next_hop" ; typ = { structure = TIpv4 ; nullable = false } ; units = None ;
      doc = "IP address of next hop router." ; aggr = None } ;
    { name = RamenName.field_of_string "src_port" ; typ = { structure = TU16 ; nullable = false } ; units = None ;
      doc = "TCP/UDP source port number or equivalent" ; aggr = None } ;
    { name = RamenName.field_of_string "dst_port" ; typ = { structure = TU16 ; nullable = false } ; units = None ;
      doc = "TCP/UDP destination port number or equivalent." ; aggr = None } ;
    { name = RamenName.field_of_string "in_iface" ; typ = { structure = TU16 ; nullable = false } ; units = None ;
      doc = "SNMP index of input interface." ; aggr = None } ;
    { name = RamenName.field_of_string "out_iface" ; typ = { structure = TU16 ; nullable = false } ; units = None ;
      doc = "SNMP index of output interface." ; aggr = None } ;
    { name = RamenName.field_of_string "packets" ; typ = { structure = TU32 ; nullable = false } ; units = Some RamenUnits.packets ;
      doc = "Packets in the flow." ; aggr = None } ;
    { name = RamenName.field_of_string "bytes" ; typ = { structure = TU32 ; nullable = false } ; units = Some RamenUnits.bytes ;
      doc = "Total number of Layer 3 bytes in the packets of the flow." ; aggr = None } ;
    { name = RamenName.field_of_string "tcp_flags" ; typ = { structure = TU8 ; nullable = false } ; units = None ;
      doc = "Cumulative OR of TCP flags." ; aggr = None } ;
    { name = RamenName.field_of_string "ip_proto" ; typ = { structure = TU8 ; nullable = false } ; units = None ;
      doc = "IP protocol type (for example, TCP = 6; UDP = 17)." ; aggr = None } ;
    { name = RamenName.field_of_string "ip_tos" ; typ = { structure = TU8 ; nullable = false } ; units = None ;
      doc = "IP type of service (ToS)." ; aggr = None } ;
    { name = RamenName.field_of_string "src_as" ; typ = { structure = TU16 ; nullable = false } ; units = None ;
      doc = "Autonomous system number of the source, either origin or peer." ; aggr = None } ;
    { name = RamenName.field_of_string "dst_as" ; typ = { structure = TU16 ; nullable = false } ; units = None ;
      doc = "Autonomous system number of the destination, either origin or peer." ; aggr = None } ;
    { name = RamenName.field_of_string "src_mask" ; typ = { structure = TU8 ; nullable = false } ; units = None ;
      doc = "Source address prefix mask bits." ; aggr = None } ;
    { name = RamenName.field_of_string "dst_mask" ; typ = { structure = TU8 ; nullable = false } ; units = None ;
      doc = "Destination address prefix mask bits." ; aggr = None } ]

let event_time =
  let open RamenEventTime in
  Some ((RamenName.field_of_string "start", ref OutputField, 1.),
        StopField (RamenName.field_of_string "stop", ref OutputField, 1.))

let factors = [ RamenName.field_of_string "source" ]

external decode :
  Bytes.t -> int -> string -> netflow_metric array =
  "wrap_netflow_v5_decode"

let collector ~inet_addr ~port ?while_ k =
  (* Listen to incoming UDP datagrams on given port: *)
  let serve sender buffer recv_len =
    !logger.debug "Received %d bytes from netflow source @ %s"
      recv_len sender ;
    decode buffer recv_len sender |>
    Array.iter k
  in
  udp_server ~inet_addr ~port ?while_ serve

let test ?(port=2055) () =
  init_logger Normal ;
  let display_tuple _t = () in
  collector ~inet_addr:Unix.inet_addr_any ~port display_tuple
