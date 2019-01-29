(* Collector for netflow v5.  *)
open Batteries
open RamenLog
open RamenHelpers
open RamenNullable
open Stdint
open RamenTuple
module T = RamenTypes
module U = RamenUnits

(* <blink>DO NOT ALTER</blink> this record without also updating
 * wrap_netflow_decode in wrap_netflow.c and typ below! *)
type netflow_metric =
  RamenIp.t nullable * float * float *
  Uint32.t * Uint8.t * Uint8.t * Uint8.t * Uint16.t *
  Uint32.t * Uint32.t * Uint32.t * Uint16.t * Uint16.t *
  Uint16.t * Uint16.t * Uint32.t * Uint32.t * Uint8.t * Uint8.t *
  Uint8.t * Uint16.t * Uint16.t * Uint8.t * Uint8.t

let typ =
  T.(make (TRecord [|
    "source",
      { structure = TIp ; nullable = true ; units = None ;
        doc = "IP address of the netflow source." ; aggr = None } ;
    "start",
      { structure = TFloat ; nullable = false ;
        units = Some U.seconds_since_epoch ;
        doc = "SysUptime at start of flow." ; aggr = None } ;
    "stop",
      { structure = TFloat ; nullable = false ;
        units = Some U.seconds_since_epoch ;
        doc = "SysUptime at the time the last packet of the flow was \
               received." ;
        aggr = None } ;
    "seqnum",
      { structure = TU32 ; nullable = false ; units = None ;
        doc = "Sequence counter of total flows seen." ; aggr = None } ;
    "engine_type",
      { structure = TU8 ; nullable = false ; units = None ;
        doc = "Type of flow-switching engine." ; aggr = None } ;
    "engine_id",
      { structure = TU8 ; nullable = false ; units = None ;
        doc = "Slot number of the flow-switching engine." ; aggr = None } ;
    "sampling_type",
      { structure = TU8 ; nullable = false ; units = None ;
        doc = "One of sampled, input or output." ; aggr = None } ;
    "sampling_rate",
      { structure = TU16 ; nullable = false ; units = Some U.dimensionless ;
        doc = "One one packet out of this number were sampled." ;
        aggr = None } ;
    "src",
      { structure = TIpv4 ; nullable = false ; units = None ;
        doc = "Source IP address." ; aggr = None } ;
    "dst",
      { structure = TIpv4 ; nullable = false ; units = None ;
        doc = "Destination IP address." ; aggr = None } ;
    "next_hop",
      { structure = TIpv4 ; nullable = false ; units = None ;
        doc = "IP address of next hop router." ; aggr = None } ;
    "src_port",
      { structure = TU16 ; nullable = false ; units = None ;
        doc = "TCP/UDP source port number or equivalent" ; aggr = None } ;
    "dst_port",
      { structure = TU16 ; nullable = false ; units = None ;
        doc = "TCP/UDP destination port number or equivalent." ;
        aggr = None } ;
    "in_iface",
      { structure = TU16 ; nullable = false ; units = None ;
        doc = "SNMP index of input interface." ; aggr = None } ;
    "out_iface",
      { structure = TU16 ; nullable = false ; units = None ;
        doc = "SNMP index of output interface." ; aggr = None } ;
    "packets",
      { structure = TU32 ; nullable = false ; units = Some U.packets ;
        doc = "Packets in the flow." ; aggr = None } ;
    "bytes",
      { structure = TU32 ; nullable = false ; units = Some U.bytes ;
        doc = "Total number of Layer 3 bytes in the packets of the flow." ;
        aggr = None } ;
    "tcp_flags",
      { structure = TU8 ; nullable = false ; units = None ;
        doc = "Cumulative OR of TCP flags." ; aggr = None } ;
    "ip_proto",
      { structure = TU8 ; nullable = false ; units = None ;
        doc = "IP protocol type (for example, TCP = 6; UDP = 17)." ;
        aggr = None } ;
    "ip_tos",
      { structure = TU8 ; nullable = false ; units = None ;
        doc = "IP type of service (ToS)." ; aggr = None } ;
    "src_as",
      { structure = TU16 ; nullable = false ; units = None ;
        doc = "Autonomous system number of the source, either origin or \
               peer." ;
        aggr = None } ;
    "dst_as",
      { structure = TU16 ; nullable = false ; units = None ;
        doc = "Autonomous system number of the destination, either origin \
               or peer." ;
        aggr = None } ;
    "src_mask",
      { structure = TU8 ; nullable = false ; units = None ;
        doc = "Source address prefix mask bits." ; aggr = None } ;
    "dst_mask",
      { structure = TU8 ; nullable = false ; units = None ;
        doc = "Destination address prefix mask bits." ; aggr = None } |]))

let event_time =
  let open RamenEventTime in
  Some ((RamenName.field_of_string "start", ref OutputField, 1.),
        StopField (RamenName.field_of_string "stop", ref OutputField, 1.))

let factors = [ RamenName.field_of_string "source" ]

external decode :
  Bytes.t -> int -> RamenIp.t nullable -> netflow_metric array =
  "wrap_netflow_v5_decode"

let collector ~inet_addr ~port ?while_ k =
  (* Listen to incoming UDP datagrams on given port: *)
  let serve ?sender buffer recv_len =
    let sender = Option.map RamenIp.of_unix_addr sender in
    !logger.debug "Received %d bytes from netflow source @ %a"
      recv_len
      (Option.print RamenIp.print) sender ;
    decode buffer recv_len (nullable_of_option sender) |>
    Array.iter k
  in
  udp_server ~inet_addr ~port ?while_ serve

let test ?(port=2055) () =
  init_logger Normal ;
  let display_tuple _t = () in
  collector ~inet_addr:Unix.inet_addr_any ~port display_tuple
