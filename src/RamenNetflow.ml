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
  [ { typ_name = "source" ; typ = { structure = TString ; nullable = false } ; units = None ;
      doc = "IP address of the netflow source." } ;
    { typ_name = "start" ; typ = { structure = TFloat ; nullable = false } ; units = Some RamenUnits.seconds_since_epoch ;
      doc = "SysUptime at start of flow." } ;
    { typ_name = "stop" ; typ = { structure = TFloat ; nullable = false } ; units = Some RamenUnits.seconds_since_epoch ;
      doc = "SysUptime at the time the last packet of the flow was received." } ;
    { typ_name = "seqnum" ; typ = { structure = TU32 ; nullable = false } ; units = None ;
      doc = "Sequence counter of total flows seen." } ;
    { typ_name = "engine_type" ; typ = { structure = TU8 ; nullable = false } ; units = None ;
      doc = "Type of flow-switching engine." } ;
    { typ_name = "engine_id" ; typ = { structure = TU8 ; nullable = false } ; units = None ;
      doc = "Slot number of the flow-switching engine." } ;
    { typ_name = "sampling_type" ; typ = { structure = TU8 ; nullable = false } ; units = None ;
      doc = "One of sampled, input or output." } ;
    { typ_name = "sampling_rate" ; typ = { structure = TU16 ; nullable = false } ; units = Some RamenUnits.dimensionless ;
      doc = "One one packet out of this number were sampled." } ;
    { typ_name = "src" ; typ = { structure = TIpv4 ; nullable = false } ; units = None ;
      doc = "Source IP address." } ;
    { typ_name = "dst" ; typ = { structure = TIpv4 ; nullable = false } ; units = None ;
      doc = "Destination IP address." } ;
    { typ_name = "next_hop" ; typ = { structure = TIpv4 ; nullable = false } ; units = None ;
      doc = "IP address of next hop router." } ;
    { typ_name = "src_port" ; typ = { structure = TU16 ; nullable = false } ; units = None ;
      doc = "TCP/UDP source port number or equivalent" } ;
    { typ_name = "dst_port" ; typ = { structure = TU16 ; nullable = false } ; units = None ;
      doc = "TCP/UDP destination port number or equivalent." } ;
    { typ_name = "in_iface" ; typ = { structure = TU16 ; nullable = false } ; units = None ;
      doc = "SNMP index of input interface." } ;
    { typ_name = "out_iface" ; typ = { structure = TU16 ; nullable = false } ; units = None ;
      doc = "SNMP index of output interface." } ;
    { typ_name = "packets" ; typ = { structure = TU32 ; nullable = false } ; units = Some RamenUnits.packets ;
      doc = "Packets in the flow." } ;
    { typ_name = "bytes" ; typ = { structure = TU32 ; nullable = false } ; units = Some RamenUnits.bytes ;
      doc = "Total number of Layer 3 bytes in the packets of the flow." } ;
    { typ_name = "tcp_flags" ; typ = { structure = TU8 ; nullable = false } ; units = None ;
      doc = "Cumulative OR of TCP flags." } ;
    { typ_name = "ip_proto" ; typ = { structure = TU8 ; nullable = false } ; units = None ;
      doc = "IP protocol type (for example, TCP = 6; UDP = 17)." } ;
    { typ_name = "ip_tos" ; typ = { structure = TU8 ; nullable = false } ; units = None ;
      doc = "IP type of service (ToS)." } ;
    { typ_name = "src_as" ; typ = { structure = TU16 ; nullable = false } ; units = None ;
      doc = "Autonomous system number of the source, either origin or peer." } ;
    { typ_name = "dst_as" ; typ = { structure = TU16 ; nullable = false } ; units = None ;
      doc = "Autonomous system number of the destination, either origin or peer." } ;
    { typ_name = "src_mask" ; typ = { structure = TU8 ; nullable = false } ; units = None ;
      doc = "Source address prefix mask bits." } ;
    { typ_name = "dst_mask" ; typ = { structure = TU8 ; nullable = false } ; units = None ;
      doc = "Destination address prefix mask bits." } ]

let event_time =
  let open RamenEventTime in
  Some (("start", ref OutputField, 1.),
        StopField ("stop", ref OutputField, 1.))

let factors = [ "source" ]

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
