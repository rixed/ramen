(* Part of RamenCollecd requiring external libs *)
open Batteries
open RamenLog
open RamenHelpers
open RamenCollectd

(* <blink>DO NOT ALTER</blink> this record without also updating
 * wrap_collectd_decode in wrap_collectd.c and tuple_typ below!
 * Must output the record in serialization order *)
type collectd_metric =
  string (* host *) *
  string option (* plugin instance *) *
  string option (* plugin name *) *
  float (* start *) *
  string option (* type instance *) *
  string option (* type name (whatever that means) *) *
  (* And the values (up to 5): *)
  float * float option * float option * float option * float option

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
