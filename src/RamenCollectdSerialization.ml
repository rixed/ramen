(* Part of RamenCollecd requiring external libs *)
open Batteries
open RamenLog
open RamenHelpers
open RamenCollectd

(* <blink>DO NOT ALTER</blink> this record without also updating
 * wrap_collectd_decode in wrap_collectd.c and tuple_typ below! *)
(* FIXME: must now output a record in serialization order *)
type collectd_metric =
  string (* host *) * float (* start *) *
  string nullable (* plugin name *) * string nullable (* plugin instance *) *
  string nullable (* type name (whatever that means) *) *
  string nullable (* type instance *) *
  (* And the values (up to 5): *)
  float * float nullable * float nullable * float nullable * float nullable

external decode : Bytes.t -> int -> collectd_metric array = "wrap_collectd_decode"

let collector ~inet_addr ~port ?while_ k =
  (* Listen to incoming UDP datagrams on given port: *)
  let serve ?sender buffer recv_len =
    !logger.debug "Received %d bytes from collectd @ %s"
      recv_len
      (match sender with None -> "??" |
       Some ip -> Unix.string_of_inet_addr ip) ;
    decode buffer recv_len |>
    Array.iter k
  in
  (* collectd current network.c buffer is 1452 bytes: *)
  udp_server
    ~what:"collectd sink" ~buffer_size:1500 ~inet_addr ~port ?while_ serve

let test ?(port=25826) () =
  init_logger Normal ;
  let display_tuple _t = () in
  collector ~inet_addr:Unix.inet_addr_any ~port display_tuple
