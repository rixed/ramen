(* Collector for collectd binary protocol (as described in
 * https://collectd.org/wiki/index.php/Binary_protocol).
 *
 * Collectd sends one (or several) UDP datagrams every X seconds (usually
 * X=10s).  Messages contains individual metrics that are composed of a time, a
 * hostname, a metric name (composed of plugin name, instance name, etc) and
 * one (or sometime several) numerical values.  We map this into a tuple which
 * type is known. But since there is no boundary in between different
 * collections and no guarantee that the time of all closely related
 * measurements are strictly the same, what we do is accumulate every metric
 * until the time changes significantly (more than, say, 5s) or we encounter a
 * metric name we already have.  Only then do we output a tuple. *)
open Batteries
open RamenLog
open Lwt
open Helpers
open RamenSharedTypes

(* <blink>DO NOT ALTER</blink> this record without also updating
 * wrap_collectd_decode in wrap_collectd.c and tuple_typ below! *)
type collectd_metric =
  string (* host *) * float (* time *) *
  string option (* plugin name *) * string option (* plugin instance *) *
  string option (* type name (whatever that means) *) *
  string option (* type instance *) *
  (* And the values (up to 5: *)
  float * float option * float option * float option * float option

let tuple_typ =
  [ { typ_name = "host" ; nullable = false ; typ = TString } ;
    { typ_name = "time" ; nullable = false ; typ = TFloat } ;
    { typ_name = "plugin" ; nullable = true ; typ = TString } ;
    { typ_name = "instance" ; nullable = true ; typ = TString } ;
    { typ_name = "type_name" ; nullable = true ; typ = TString } ;
    { typ_name = "type_instance" ; nullable = true ; typ = TString } ;
    { typ_name = "value" ; nullable = false ; typ = TFloat } ;
    { typ_name = "value2" ; nullable = true ; typ = TFloat } ;
    { typ_name = "value3" ; nullable = true ; typ = TFloat } ;
    { typ_name = "value4" ; nullable = true ; typ = TFloat } ;
    { typ_name = "value5" ; nullable = true ; typ = TFloat } ]

let event_time = Some (("time", 1.), DurationConst 0.)

external decode : Bytes.t -> int -> collectd_metric array = "wrap_collectd_decode"

let collector ~inet_addr ~port ?while_ k =
  (* Listen to incoming UDP datagrams on given port: *)
  let serve sender buffer recv_len =
    !logger.debug "Received %d bytes from collectd @ %s" recv_len sender ;
    decode buffer recv_len |>
    Array.fold_left (fun th tuple -> th >>= fun () -> k tuple) return_unit
  in
  (* collectd current network.c buffer is 1452 bytes: *)
  udp_server ~buffer_size:1500 ~inet_addr ~port ?while_ serve

let test ?(port=25826) () =
  logger := make_logger true ;
  let display_tuple _t =
    return_unit in
  Lwt_main.run (collector ~inet_addr:Unix.inet_addr_any ~port display_tuple)
