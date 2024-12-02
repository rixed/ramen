(* Part of RamenNetflow requiring external libs: *)
open Batteries
open Stdint

open DessserOCamlBackEndHelpers
open RamenLog
open RamenHelpers
open RamenHelpersNoLog
open RamenNetflow

(* <blink>DO NOT ALTER</blink> this record without also updating
 * wrap_netflow_decode in wrap_netflow.c and tuple_typ below!
 * Must return a tuple in serialization order *)
type netflow_metric =
  Uint32.t (* bytes *) *
  Uint32.t (* dst *) *
  Uint16.t (* dst_as *) *
  Uint8.t (* dst_mask *) *
  Uint16.t (* dst_port *) *
  Uint8.t (* engine_id *) *
  Uint8.t (* engine_type *) *
  Uint16.t (* in_iface *) *
  Uint8.t (* ip_proto *) *
  Uint8.t (* ip_tos *) *
  Uint32.t (* next_hop *) *
  Uint16.t (* out_iface *) *
  Uint32.t (* packets *) *
  Uint16.t (* sampling_rate *) *
  Uint8.t (* sampling_type *) *
  Uint32.t (* seqnum *) *
  RamenIp.t option (* source *) *
  Uint32.t (* src *) *
  Uint16.t (* src_as *) *
  Uint8.t (* src_mask *) *
  Uint16.t (* src_port *) *
  float (* start *) *
  float (* stop *) *
  Uint8.t (* tcp_flags *)

external decode :
  Bytes.t -> int -> RamenIp.t option -> netflow_metric array =
  "wrap_netflow_v5_decode"

let collector ~inet_addr ~port ~ip_proto ?while_ k =
  (* Listen to incoming UDP datagrams on given port: *)
  let serve ?sender buffer start stop =
    if start <> 0 then todo "Netflow over TCP" ;
    let recv_len = stop - start in
    let sender = Option.map RamenIp.of_unix_addr sender in
    !logger.debug "Received %d bytes from netflow source @ %a"
      recv_len
      (Option.print RamenIp.print) sender ;
    decode buffer recv_len sender |>
    Array.iter k ;
    recv_len
  in
  ip_server ~ip_proto
    ~what:"netflow sink" ~inet_addr ~port ?while_ serve

let test ?(port=2055) () =
  init_logger ~with_time:false Normal ;
  let display_tuple _t = () in
  collector ~inet_addr:Unix.inet_addr_any ~port display_tuple
