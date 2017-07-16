open Batteries
open Log

type options = { debug : bool }

let options debug = { debug }

(* Build the node infos corresponding to the BCN configuration *)
let graph_info_of_bcns csv_dir bcns =
  let string_of_zone = function
    | Some z -> Printf.sprintf "%d" z
    | None -> Printf.sprintf "Any" in
  let all_nodes = ref [] in
  let make_node ?(parents=[]) name operation =
    let open RamenSharedTypes in
    let node = { empty_node_info with
                 name ; operation ;
                 parents = List.map (fun p -> p.name) parents } in
    List.iter (fun (p : RamenSharedTypes.node_info) ->
      p.children <- name :: p.children) parents ;
    all_nodes := node :: !all_nodes ;
    node
  in
  let top =
    let op =
      Printf.sprintf
        "READ AND DELETE CSV FILES \"%s/tcp_v29.*.csv\" SEPARATOR \"\\t\" NULL \"<NULL>\" (\n\
           poller string not null,\n\
           capture_begin u64 not null,\n\
           capture_end u64 not null,\n\
           device_client u8 null,\n\
           device_server u8 null,\n\
           vlan_client u32 null,\n\
           vlan_server u32 null,\n\
           mac_client u64 null,\n\
           mac_server u64 null,\n\
           zone_client u32 not null,\n\
           zone_server u32 not null,\n\
           ip4_client u32 null,\n\
           ip6_client i128 null,\n\
           ip4_server u32 null,\n\
           ip6_server i128 null,\n\
           ip4_external u32 null,\n\
           ip6_external i128 null,\n\
           port_client u16 not null,\n\
           port_server u16 not null,\n\
           diffserv_client u8 not null,\n\
           diffserv_server u8 not null,\n\
           os_client u8 null,\n\
           os_server u8 null,\n\
           mtu_client u16 null,\n\
           mtu_server u16 null,\n\
           captured_pcap string null,\n\
           application u32 not null,\n\
           protostack string null,\n\
           uuid string null,\n\
           traffic_bytes_client u64 not null,\n\
           traffic_bytes_server u64 not null,\n\
           traffic_packets_client u64 not null,\n\
           traffic_packets_server u64 not null,\n\
           payload_bytes_client u64 not null,\n\
           payload_bytes_server u64 not null,\n\
           payload_packets_client u64 not null,\n\
           payload_packets_server u64 not null,\n\
           retrans_traffic_bytes_client u64 null,\n\
           retrans_traffic_bytes_server u64 null,\n\
           retrans_payload_bytes_client u64 null,\n\
           retrans_payload_bytes_server u64 null,\n\
           syn_count_client u64 null,\n\
           fin_count_client u64 null,\n\
           fin_count_server u64 null,\n\
           rst_count_client u64 null,\n\
           rst_count_server u64 null,\n\
           timeout_count u64 not null,\n\
           close_count u64 null,\n\
           dupack_count_client u64 null,\n\
           dupack_count_server u64 null,\n\
           zero_window_count_client u64 null,\n\
           zero_window_count_server u64 null,\n\
           ct_count u64 null,\n\
           ct_sum u64 not null,\n\
           ct_square_sum u64 not null,\n\
           rt_count_server u64 null,\n\
           rt_sum_server u64 not null,\n\
           rt_square_sum_server u64 not null,\n\
           rtt_count_client u64 null,\n\
           rtt_sum_client u64 not null,\n\
           rtt_square_sum_client u64 not null,\n\
           rtt_count_server u64 null,\n\
           rtt_sum_server u64 not null,\n\
           rtt_square_sum_server u64 not null,\n\
           rd_count_client u64 null,\n\
           rd_sum_client u64 not null,\n\
           rd_square_sum_client u64 not null,\n\
           rd_count_server u64 null,\n\
           rd_sum_server u64 not null,\n\
           rd_square_sum_server u64 not null,\n\
           dtt_count_client u64 null,\n\
           dtt_sum_client u64 not null,\n\
           dtt_square_sum_client u64 not null,\n\
           dtt_count_server u64 null,\n\
           dtt_sum_server u64 not null,\n\
           dtt_square_sum_server u64 not null,\n\
           dcerpc_uuid string null\n\
         )" csv_dir in
    make_node "read csv" op in
  let to_unidir ~src ~dst =
    let op =
      (* Note: we keep the IPs etc although we do not use them later, for maybe this
       * could also provide a useful data stream to collect stats on individual hosts *)
      Printf.sprintf
        "SELECT capture_begin, capture_end,\n\
                device_%s AS device_src, device_%s AS device_dst,\n\
                vlan_%s AS vlan_src, vlan_%s AS vlan_dst,\n\
                mac_%s AS mac_src, mac_%s AS mac_dst,\n\
                zone_%s AS zone_src, zone_%s AS zone_dst,\n\
                ip4_%s AS ip4_src, ip4_%s AS ip4_dst,\n\
                ip6_%s AS ip6_src, ip6_%s AS ip6_dst,\n\
                port_%s AS port_src, port_%s AS port_dst,\n\
                traffic_packets_%s AS packets,\n\
                traffic_bytes_%s AS bytes\n\
         WHERE traffic_packets_%s > 0"
         src dst src dst src dst src dst src dst src dst src dst src dst src in
    let name = Printf.sprintf "to unidir %s to %s" src dst in
    make_node ~parents:[top] name op
  in
  let to_unidir_c2s = to_unidir ~src:"client" ~dst:"server"
  and to_unidir_s2c = to_unidir ~src:"server" ~dst:"client" in
  let alert_conf_of_bcn bcn =
    (* bcn.min_bps, bcn.max_bps, bcn.obs_window, bcn.avg_window, bcn.percentile, bcn.source bcn.dest *)
    let open Conf_of_sqlite in
    let name_prefix = Printf.sprintf "%s to %s"
      (string_of_zone bcn.source) (string_of_zone bcn.dest) in
    let avg_window = int_of_float (bcn.avg_window *. 1_000_000.0) in
    let obs_window = int_of_float (bcn.obs_window *. 1_000_000.0) in
    let avg_per_zones =
      let op =
        Printf.sprintf
          "SELECT min of capture_begin, max of capture_end,\n\
                  sum of packets / ((max_capture_end - min_capture_begin) / 1_000_000) as packets_per_secs,\n\
                  sum of bytes / ((max_capture_end - min_capture_begin) / 1_000_000) as bytes_per_secs,\n\
                  zone_src, zone_dst\n\
           WHERE %s AND %s\n\
           GROUP BY capture_begin // %d\n\
           COMMIT AND FLUSH WHEN all.capture_begin > min_capture_begin + 2 * %d"
          (match bcn.source with None -> "true" | Some z -> "zone_src = "^ string_of_int z)
          (match bcn.dest with None -> "true" | Some z -> "zone_dst = "^ string_of_int z)
          avg_window
          avg_window
          (* Note: Ideally we would want to compute the max of all.capture_begin *)
      and name =
        Printf.sprintf "%s: average traffic every %g seconds"
          name_prefix bcn.avg_window
      in
      make_node ~parents:[to_unidir_c2s;to_unidir_s2c] name op in
    let perc_per_obs_window =
      let op =
        Printf.sprintf
          "SELECT %gth percentile of bytes_per_secs AS bps,\n\
                  zone_src, zone_dst\n\
           GROUP BY min_capture_begin // %d\n\
           COMMIT AND SLIDE 1 WHEN in.#count > %d"
           bcn.percentile
           obs_window
           (Helpers.round_to_int (bcn.obs_window /. bcn.avg_window)) in
      let name =
        Printf.sprintf "%s: %gth percentile on last %g seconds"
          name_prefix bcn.percentile bcn.obs_window in
      make_node ~parents:[avg_per_zones] name op in
    Option.may (fun min_bps ->
        let subject = Printf.sprintf "Too little traffic from zone %s to %s"
                        (string_of_zone bcn.source) (string_of_zone bcn.dest)
        and text = Printf.sprintf
                     "The traffic from zone %s to %s has sunk below \
                      the configured minimum of %d \
                      for the last %g minutes.\n"
                      (string_of_zone bcn.source) (string_of_zone bcn.dest)
                      min_bps (bcn.obs_window /. 60.) in
        let ops = Printf.sprintf
          "ALERT \"low traffic\" WHEN bps < %d SUBJECT %S TEXT %S"
            min_bps subject text in
        let name = Printf.sprintf "%s: alert traffic too low" name_prefix in
        make_node ~parents:[perc_per_obs_window] name ops |>
        ignore
      ) bcn.min_bps ;
    Option.may (fun max_bps ->
        let subject = Printf.sprintf "Too much traffic from zone %s to %s"
                        (string_of_zone bcn.source) (string_of_zone bcn.dest)
        and text = Printf.sprintf
                     "The traffic from zones %s to %s has raised above \
                      the configured maximum of %d \
                      for the last %g minutes.\n"
                      (string_of_zone bcn.source) (string_of_zone bcn.dest)
                      max_bps (bcn.obs_window /. 60.) in
        let ops = Printf.sprintf
          "ALERT \"high traffic\" WHEN bps > %d SUBJECT %S TEXT %S"
            max_bps subject text in
        let name = Printf.sprintf "%s: alert traffic too high" name_prefix in
        make_node ~parents:[perc_per_obs_window] name ops |>
        ignore
      ) bcn.max_bps ;
  in
  List.iter alert_conf_of_bcn bcns ;
  RamenSharedTypes.{
    nodes = !all_nodes ; status = Edition ;
    last_started = None ; last_stopped = None }

let get_bcns_from_db db =
  let open Conf_of_sqlite in
  get_config db

(* Daemon *)

open Lwt
open Cohttp
open Cohttp_lwt_unix

let put_graph ramen_url graph =
  let graph_json = PPP.to_string RamenSharedTypes.graph_info_ppp graph in
  let url = ramen_url ^"/graph" in
  !logger.debug "Will send %S to %S" graph_json url ;
  let body = `String graph_json in
  let%lwt (resp, body) = Client.put ~body (Uri.of_string url) in
  let code = resp |> Response.status |> Code.code_of_status in
  !logger.debug "Response code: %d" code ;
  let%lwt body = Cohttp_lwt_body.to_string body in
  !logger.debug "Body: %S\n" body ;
  return_unit

let start conf ramen_url db_name csv_dir =
  logger := make_logger conf.debug ;
  let open Conf_of_sqlite in
  let db = get_db db_name in
  while%lwt true do
    check_config_changed db ;
    if must_reload db then (
      !logger.info "Must reload configuration" ;
      let bcns = get_bcns_from_db db in
      let graph = graph_info_of_bcns csv_dir bcns in
      put_graph ramen_url graph
    ) else (
      Lwt_unix.sleep 1.0
    )
  done

(* Args *)

open Cmdliner

let common_opts =
  let debug =
    Arg.(value (flag (info ~doc:"increase verbosity" ["d"; "debug"])))
  in
  Term.(const options $ debug)

let ramen_url =
  let i = Arg.info ~doc:"URL to reach ramen" [ "ramen-url" ] in
  Arg.(value (opt string "http://127.0.0.1:29380" i))

let db_name =
  let i = Arg.info ~doc:"Path of the SQLite file"
                   [ "db" ] in
  Arg.(required (opt (some string) None i))

let csv_dir =
  let i = Arg.info ~doc:"Path where the TCP_v29 CSV files are stored"
                   [ "csv-dir" ] in
  Arg.(required (opt (some string) None i))

let start_cmd =
  Term.(
    (const start
      $ common_opts
      $ ramen_url
      $ db_name
      $ csv_dir),
    info "configurator")

let () =
  match Term.eval start_cmd with
  | `Ok th -> Lwt_main.run th
  | x -> Term.exit x
