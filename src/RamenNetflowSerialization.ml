(* Part of RamenNetflow requiring external libs: *)
open Batteries
open Stdint

open DessserOCamlBackEndHelpers
open RamenLog
open RamenHelpers
open RamenNetflow

(* <blink>DO NOT ALTER</blink> this record without also updating
 * wrap_netflow_decode in wrap_netflow.c and tuple_typ below! *)
(* FIXME: Broken: must now return a tuple in serialization order! *)
type netflow_metric =
  RamenIp.t nullable * float * float *
  Uint32.t * Uint8.t * Uint8.t * Uint8.t * Uint16.t *
  Uint32.t * Uint32.t * Uint32.t * Uint16.t * Uint16.t *
  Uint16.t * Uint16.t * Uint32.t * Uint32.t * Uint8.t * Uint8.t *
  Uint8.t * Uint16.t * Uint16.t * Uint8.t * Uint8.t

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
    decode buffer recv_len (Nullable.of_option sender) |>
    Array.iter k
  in
  udp_server
    ~what:"netflow sink" ~inet_addr ~port ?while_ serve

let test ?(port=2055) () =
  init_logger Normal ;
  let display_tuple _t = () in
  collector ~inet_addr:Unix.inet_addr_any ~port display_tuple
