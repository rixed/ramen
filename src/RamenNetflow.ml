(* Collector for netflow v5.  *)
open Batteries
open RamenLog
open Lwt
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
  [ { typ_name = "source" ; typ = { structure = TString ; nullable = Some false } ; units = None } ;
    { typ_name = "first" ; typ = { structure = TFloat ; nullable = Some false } ; units = Some RamenUnits.seconds_since_epoch } ;
    { typ_name = "last" ; typ = { structure = TFloat ; nullable = Some false } ; units = Some RamenUnits.seconds_since_epoch } ;
    { typ_name = "seqnum" ; typ = { structure = TU32 ; nullable = Some false } ; units = None } ;
    { typ_name = "engine_type" ; typ = { structure = TU8 ; nullable = Some false } ; units = None } ;
    { typ_name = "engine_id" ; typ = { structure = TU8 ; nullable = Some false } ; units = None } ;
    { typ_name = "sampling_type" ; typ = { structure = TU8 ; nullable = Some false } ; units = None } ;
    { typ_name = "sampling_rate" ; typ = { structure = TU16 ; nullable = Some false } ; units = Some RamenUnits.dimensionless } ;
    { typ_name = "src" ; typ = { structure = TIpv4 ; nullable = Some false } ; units = None } ;
    { typ_name = "dst" ; typ = { structure = TIpv4 ; nullable = Some false } ; units = None } ;
    { typ_name = "next_hop" ; typ = { structure = TIpv4 ; nullable = Some false } ; units = None } ;
    { typ_name = "src_port" ; typ = { structure = TU16 ; nullable = Some false } ; units = None } ;
    { typ_name = "dst_port" ; typ = { structure = TU16 ; nullable = Some false } ; units = None } ;
    { typ_name = "in_iface" ; typ = { structure = TU16 ; nullable = Some false } ; units = None } ;
    { typ_name = "out_iface" ; typ = { structure = TU16 ; nullable = Some false } ; units = None } ;
    { typ_name = "packets" ; typ = { structure = TU32 ; nullable = Some false } ; units = Some RamenUnits.packets } ;
    { typ_name = "bytes" ; typ = { structure = TU32 ; nullable = Some false } ; units = Some RamenUnits.bytes } ;
    { typ_name = "tcp_flags" ; typ = { structure = TU8 ; nullable = Some false } ; units = None } ;
    { typ_name = "ip_proto" ; typ = { structure = TU8 ; nullable = Some false } ; units = None } ;
    { typ_name = "ip_tos" ; typ = { structure = TU8 ; nullable = Some false } ; units = None } ;
    { typ_name = "src_as" ; typ = { structure = TU16 ; nullable = Some false } ; units = None } ;
    { typ_name = "dst_as" ; typ = { structure = TU16 ; nullable = Some false } ; units = None } ;
    { typ_name = "src_mask" ; typ = { structure = TU8 ; nullable = Some false } ; units = None } ;
    { typ_name = "dst_mask" ; typ = { structure = TU8 ; nullable = Some false } ; units = None } ]

let event_time =
  let open RamenEventTime in
  Some (("first", ref OutputField, 1.),
        StopField ("last", ref OutputField, 1.))

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
    Array.fold_left (fun th tuple -> th >>= fun () -> k tuple) return_unit
  in
  udp_server ~inet_addr ~port ?while_ serve

let test ?(port=2055) () =
  logger := make_logger true ;
  let display_tuple _t =
    return_unit in
  Lwt_main.run (collector ~inet_addr:Unix.inet_addr_any ~port display_tuple)
