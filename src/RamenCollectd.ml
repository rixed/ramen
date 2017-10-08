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
  let open RamenSharedTypes in
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

external decode : Bytes.t -> int -> collectd_metric array = "wrap_collectd_decode"

let udp_server ~inet_addr ~port k =
  let open Lwt_unix in
  (* FIXME: it seems that binding that socket makes cohttp leack descriptors
   * when sending reports to ramen. Oh boy! *)
  let sock_of_domain domain =
    let sock = socket domain SOCK_DGRAM 0 in
    let%lwt () = bind sock (ADDR_INET (inet_addr, port)) in
    return sock in
  let%lwt sock =
    catch (fun () -> sock_of_domain PF_INET6)
          (fun _ -> sock_of_domain PF_INET) in
  !logger.debug "Listening for collectd datagrams on port %d" port ;
  (* collectd current network.c says 1452: *)
  let buffer = Bytes.create 1500 in
  let rec forever () =
    let%lwt recv_len, sockaddr = recvfrom sock buffer 0 (Bytes.length buffer) [] in
    let sender =
      match sockaddr with
      | ADDR_INET (addr, port) ->
        Printf.sprintf "%s:%d" (Unix.string_of_inet_addr addr) port
      | _ -> "??" in
    k sender buffer recv_len >>=
    forever
  in
  forever ()

let collectd_collector ~inet_addr ?(port=25826) k =
  (* Listen to incoming UDP datagrams on given port: *)
  let serve sender buffer recv_len =
    !logger.debug "Received %d bytes from collectd @ %s" recv_len sender ;
    decode buffer recv_len |>
    Array.fold_left (fun th tuple -> th >>= fun () -> k tuple) (return ())
  in
  udp_server ~inet_addr ~port serve

let test port () =
  logger := make_logger true ;
  let display_tuple _t =
    return_unit in
  Lwt_main.run (collectd_collector ~inet_addr:Unix.inet_addr_any ~port display_tuple)
