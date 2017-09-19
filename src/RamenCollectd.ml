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

external decode : Bytes.t -> int -> unit = "wrap_collectd_decode" (* TODO *)

let udp_server ?(ipv6=false) port k =
  let open Lwt_unix in
  let domain = if ipv6 then PF_INET6 else PF_INET in
  let sock = socket domain SOCK_DGRAM 0 in
  let addr = if ipv6 then Unix.inet6_addr_any else Unix.inet_addr_any in
  let%lwt () = bind sock (ADDR_INET (addr, port)) in
  !logger.info "Listening for collectd datagrams on port %d, ipv%d"
    port (if ipv6 then 6 else 4) ;
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

let collectd_collector ?(also_v6=false) ?(port=25826) _tuple_typ =
  (* Listen to incoming UDP datagrams on given port: *)
  let k sender buffer recv_len =
    !logger.debug "Received %d bytes from collectd @ %s" recv_len sender ;
    decode buffer recv_len ;
    return_unit
  in
  let th = [ udp_server ~ipv6:false port k ] in
  let th =
    if also_v6 then udp_server ~ipv6:true  port k :: th
    else th in
  join th

let test port () =
  logger := make_logger true ;
  Lwt_main.run (collectd_collector ~port ())
