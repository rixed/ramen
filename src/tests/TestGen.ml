open Batteries
open Stdint

type alerting_conf_entry =
  { zone_from : int ; zone_to : int ;
    avg_window : float ; obs_window  : float ; percentile : float ;
    min : int option ; max : int option ;
    relevancy : int option ; max_rtt : int option ; max_rr : int option }

(* FIXME: actually alerts are still an internal thing. We want to check
 * the oncallers notifications. *)
type expected_alert =
  { at : float ; name : string }

let alert ~at ~name = { at ; name }

module type TEST =
sig
  val alerting_config : unit -> alerting_conf_entry list
  val traffic : unit -> EventTypes.TCP_v29.csv list
  val expected_alerts : unit -> expected_alert list
end

let float_of_usec u = float_of_int u *. 0.000_001
let usec_of_float f = int_of_float (1_000_000.0 *. f)
let mins m = float_of_int (60 * m)
let hours h = float_of_int (3600 * h)
let kbs k = 1024 * k

let clock = ref 0 (* in usec, used to generate traffic. *)

(* Create a TCPv29 CSV from optional parameters: *)
let traffic
      ?(poller="9GJ3152") ?(start=0) ?(stop=1) ?itf_clt ?itf_srv
      ?vlan_clt ?vlan_srv ?mac_clt ?mac_srv
      ?(zone_clt=0) ?(zone_srv=0) ?ip4_clt ?ip6_clt
      ?ip4_srv ?ip6_srv ?ip4_external ?ip6_external
      ?(port_clt=49165) ?(port_srv=993)
      ?(diffserv_clt=0) ?(diffserv_srv=0) ?os_clt ?os_srv
      ?mtu_clt ?mtu_srv ?captured_pcap ?(application=91)
      ?protostack ?uuid
      ?(bytes_clt=0) ?(bytes_srv=0) ?(packets_clt=0) ?(packets_srv=0)
      ?(payload_bytes_clt=0) ?(payload_bytes_srv=0)
      ?(payload_packets_clt=0) ?(payload_packets_srv=0)
      ?retrans_bytes_clt ?retrans_bytes_srv
      ?retrans_payload_bytes_clt ?retrans_payload_bytes_srv
      ?syn_count_clt ?fin_count_clt ?fin_count_srv ?rst_count_clt
      ?rst_count_srv ?(timeout_count=0) ?close_count
      ?dupack_count_clt ?dupack_count_srv
      ?zero_window_count_clt ?zero_window_count_srv
      ?ct_count ?(ct_sum=0) ?(ct_square_sum=0)
      ?rt_count_srv ?(rt_sum_srv=0) ?(rt_square_sum_srv=0)
      ?rtt_count_clt ?(rtt_sum_clt=0) ?(rtt_square_sum_clt=0)
      ?rtt_count_srv ?(rtt_sum_srv=0) ?(rtt_square_sum_srv=0)
      ?rd_count_clt ?(rd_sum_clt=0) ?(rd_square_sum_clt=0)
      ?rd_count_srv ?(rd_sum_srv=0) ?(rd_square_sum_srv=0)
      ?dtt_count_clt ?(dtt_sum_clt=0) ?(dtt_square_sum_clt=0)
      ?dtt_count_srv ?(dtt_sum_srv=0) ?(dtt_square_sum_srv=0)
      ?dcerpc_uuid () =
  (* All IPvs cannot be NULL: *)
  let ip4_clt = if ip4_clt = None && ip6_clt = None
                then Some (Uint32.of_int 3232244565) else ip4_clt in
  let ip4_srv = if ip4_srv = None && ip6_srv = None
                then Some (Uint32.of_int 3232244656) else ip4_srv in
  poller, start, stop, itf_clt, itf_srv, vlan_clt, vlan_srv,
  mac_clt, mac_srv, zone_clt, zone_srv, ip4_clt, ip6_clt,
  ip4_srv, ip6_srv, ip4_external, ip6_external, port_clt, port_srv,
  diffserv_clt, diffserv_srv, os_clt, os_srv, mtu_clt, mtu_srv,
  captured_pcap, application, protostack, uuid,
  bytes_clt, bytes_srv, packets_clt, packets_srv,
  payload_bytes_clt, payload_bytes_srv,
  payload_packets_clt, payload_packets_srv,
  retrans_bytes_clt, retrans_bytes_srv,
  retrans_payload_bytes_clt, retrans_payload_bytes_srv,
  syn_count_clt, fin_count_clt, fin_count_srv, rst_count_clt,
  rst_count_srv, timeout_count, close_count,
  dupack_count_clt, dupack_count_srv,
  zero_window_count_clt, zero_window_count_srv,
  ct_count, ct_sum, ct_square_sum, rt_count_srv,
  rt_sum_srv, rt_square_sum_srv,
  rtt_count_clt, rtt_sum_clt, rtt_square_sum_clt,
  rtt_count_srv, rtt_sum_srv, rtt_square_sum_srv,
  rd_count_clt, rd_sum_clt, rd_square_sum_clt,
  rd_count_srv, rd_sum_srv, rd_square_sum_srv,
  dtt_count_clt, dtt_sum_clt, dtt_square_sum_clt,
  dtt_count_srv, dtt_sum_srv, dtt_square_sum_srv,
  dcerpc_uuid

(* Take a single TCPv29 CSV and slice it into 1min slices,
 * possibly randomizing some of the fields. *)
let slice ?(duration=(mins 1)) ?(randomize=[]) (
      poller, start, stop, itf_clt, itf_srv, vlan_clt, vlan_srv,
      mac_clt, mac_srv, zone_clt, zone_srv, ip4_clt, ip6_clt,
      ip4_srv, ip6_srv, ip4_external, ip6_external, port_clt, port_srv,
      diffserv_clt, diffserv_srv, os_clt, os_srv, mtu_clt, mtu_srv,
      captured_pcap, application, protostack, uuid,
      bytes_clt, bytes_srv, packets_clt, packets_srv,
      payload_bytes_clt, payload_bytes_srv,
      payload_packets_clt, payload_packets_srv,
      retrans_bytes_clt, retrans_bytes_srv,
      retrans_payload_bytes_clt, retrans_payload_bytes_srv,
      syn_count_clt, fin_count_clt, fin_count_srv, rst_count_clt,
      rst_count_srv, timeout_count, close_count,
      dupack_count_clt, dupack_count_srv,
      zero_window_count_clt, zero_window_count_srv,
      ct_count, ct_sum, ct_square_sum, rt_count_srv,
      rt_sum_srv, rt_square_sum_srv,
      rtt_count_clt, rtt_sum_clt, rtt_square_sum_clt,
      rtt_count_srv, rtt_sum_srv, rtt_square_sum_srv,
      rd_count_clt, rd_sum_clt, rd_square_sum_clt,
      rd_count_srv, rd_sum_srv, rd_square_sum_srv,
      dtt_count_clt, dtt_sum_clt, dtt_square_sum_clt,
      dtt_count_srv, dtt_sum_srv, dtt_square_sum_srv,
      dcerpc_uuid) =
  let rec loop prev orig =
    if orig >= stop then List.rev prev else
    let next_orig =
      min stop (orig + usec_of_float duration) in
    let frac =
      float_of_int (next_orig - orig) /. float_of_int (stop - start) in
    let slc x = int_of_float (Float.round (frac *. float_of_int x)) in
    let slc2 x = slc x (* FIXME *) in
    let slco = Option.map slc in
    loop ((
      poller, orig, next_orig, itf_clt, itf_srv, vlan_clt, vlan_srv,
      mac_clt, mac_srv, zone_clt, zone_srv, ip4_clt, ip6_clt,
      ip4_srv, ip6_srv, ip4_external, ip6_external, port_clt, port_srv,
      diffserv_clt, diffserv_srv, os_clt, os_srv, mtu_clt, mtu_srv,
      captured_pcap, application, protostack, uuid,
      slc bytes_clt, slc bytes_srv, slc packets_clt, slc packets_srv,
      slc payload_bytes_clt, slc payload_bytes_srv,
      slc payload_packets_clt, slc payload_packets_srv,
      slco retrans_bytes_clt, slco retrans_bytes_srv,
      slco retrans_payload_bytes_clt, slco retrans_payload_bytes_srv,
      slco syn_count_clt, slco fin_count_clt,
      slco fin_count_srv, slco rst_count_clt,
      slco rst_count_srv, slc timeout_count, slco close_count,
      slco dupack_count_clt, slco dupack_count_srv,
      slco zero_window_count_clt, slco zero_window_count_srv,
      slco ct_count, slc ct_sum, slc2 ct_square_sum,
      slco rt_count_srv, slc rt_sum_srv, slc2 rt_square_sum_srv,
      slco rtt_count_clt, slc rtt_sum_clt, slc2 rtt_square_sum_clt,
      slco rtt_count_srv, slc rtt_sum_srv, slc2 rtt_square_sum_srv,
      slco rd_count_clt, slc rd_sum_clt, slc2 rd_square_sum_clt,
      slco rd_count_srv, slc rd_sum_srv, slc2 rd_square_sum_srv,
      slco dtt_count_clt, slc dtt_sum_clt, slc2 dtt_square_sum_clt,
      slco dtt_count_srv, slc dtt_sum_srv, slc2 dtt_square_sum_srv,
      dcerpc_uuid) :: prev) next_orig
  in
  loop [] start

let traffic_between_zones z1 z2 ~bandw ~duration prev =
  let hbandw = bandw / 2 in
  let start = !clock in
  let stop = start + usec_of_float duration in
  clock := stop ;
  prev @ slice ~randomize:[`ip_clt; `ip_srv]
    (traffic ~start ~stop ~zone_clt:z1 ~zone_srv:z2
             ~bytes_clt:hbandw ~bytes_srv:hbandw ())


let all_tests : (string, (module TEST)) Hashtbl.t =
  Hashtbl.create 3

let register_test name m = Hashtbl.add all_tests name m
