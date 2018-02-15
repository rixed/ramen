open Batteries
open RamenLog
module SN = RamenSharedTypes.Info.Func

type options = { debug : bool ; monitor : bool }

let options debug monitor = { debug ; monitor }

type export_what = ExportAll | ExportSome | ExportNone
let export_some = function ExportAll | ExportSome -> true | _ -> false
let export_all = function ExportAll -> true | _ -> false

let rebase dataset_name name = dataset_name ^"/"^ name

let enc = Uri.pct_encode

(* Get the operations to import the dataset and do basic transformations.
 * Named streams belonging to this base program:
 * - ${dataset_name}/tcp, etc: Raw imported tuples straight from the CSV;
 * - ${dataset_name}/c2s tcp: The first of a pair of streams of traffic info
 *                            (source to dest rather than client to server);
 * - ${dataset_name}/s2c tcp: The second stream of traffic info;
 *
 * If you wish to process traffic info you must feed on both c2s and s2c.
 *)

let make_func name operation = SN.{ name ; operation }

let program_of_funcs funcs =
  List.fold_left (fun s n ->
    s ^ "DEFINE '"^ n.SN.name ^"' AS "^ n.SN.operation ^";\n"
  ) "" funcs

let rep sub by str = String.nreplace ~str ~sub ~by

let print_squoted oc = Printf.fprintf oc "'%s'"

(* Aggregating TCP metrics for alert discovery: *)
let tcp_traffic_func ?where dataset_name name dt export =
  let dt_us = dt * 1_000_000 in
  let parents =
    List.map (rebase dataset_name) ["c2s tcp"; "s2c tcp"] |>
    IO.to_string (List.print ~first:"" ~last:"" ~sep:","
                    print_squoted) in
  let op =
    {|FROM $PARENTS$ SELECT
       (capture_begin // $DT_US$) * $DT$ AS start,
       min capture_begin, max capture_end,
       sum packets_src / $DT$ AS packets_per_secs,
       sum bytes_src / $DT$ AS bytes_per_secs,
       sum payload_src / $DT$ AS payload_per_secs,
       sum packets_with_payload_src / $DT$ AS packets_with_payload_per_secs,
       sum retrans_bytes_src / $DT$ AS retrans_bytes_per_secs,
       sum retrans_payload_src / $DT$ AS retrans_payload_per_secs,
       sum fins_src / $DT$ AS fins_per_secs,
       sum rsts_src / $DT$ AS rsts_per_secs,
       sum timeouts / $DT$ AS timeouts_per_secs,
       sum syns / $DT$ AS syns_per_secs,
       sum closes / $DT$ AS closes_per_secs,
       sum connections AS _sum_connections,
       _sum_connections / $DT$ AS connections_per_secs,
       sum dupacks_src / $DT$ AS dupacks_per_secs,
       sum zero_windows_src / $DT$ AS zero_windows_per_secs,
       sum rtt_count_src AS sum_rtt_count_src,
       sum rtt_sum_src AS _sum_rtt_sum_src,
       (_sum_rtt_sum_src / sum_rtt_count_src) / 1e6 AS rtt_avg,
       ((sum rtt_sum2_src - float(_sum_rtt_sum_src)^2 / sum_rtt_count_src) /
           sum_rtt_count_src) / 1e12 AS rtt_var,
       sum rd_count_src AS sum_rd_count_src,
       sum rd_sum_src AS _sum_rd_sum_src,
       (_sum_rd_sum_src / sum_rd_count_src) / 1e6 AS rd_avg,
       ((sum rd_sum2_src - float(_sum_rd_sum_src)^2 / sum_rd_count_src) /
           sum_rd_count_src) / 1e12 AS rd_var,
       sum dtt_count_src AS sum_dtt_count_src,
       sum dtt_sum_src AS _sum_dtt_sum_src,
       (_sum_dtt_sum_src / sum_dtt_count_src) / 1e6 AS dtt_avg,
       ((sum dtt_sum2_src - float(_sum_dtt_sum_src)^2 / sum_dtt_count_src) /
           sum_dtt_count_src) / 1e12 AS dtt_var,
       sum connections_time AS _sum_connections_time,
       (_sum_connections_time / _sum_connections) / 1e6 AS connection_time_avg,
       ((sum connections_time2 - float(_sum_connections_time)^2 / _sum_connections) /
           _sum_connections) / 1e12 AS connection_time_var
     GROUP BY capture_begin // $DT_US$
     COMMIT WHEN
       in.capture_begin > out.min_capture_begin + 2 * u64($DT_US$)|}^
     (if export_some export then {|
     EXPORT EVENT STARTING AT start
             WITH DURATION $DT$|} else "") |>
    rep "$DT$" (string_of_int dt) |>
    rep "$DT_US$" (string_of_int dt_us) |>
    rep "$PARENTS$" parents
  in
  let op =
    match where with None -> op
                   | Some w -> op ^"\nWHERE "^ w in
  make_func name op

(* Alerts:
 *
 * We want to direct all alerts into a custom SQLite database that other
 * programs will monitor.
 * This DB will have a single table named "alerts" with fields: "id", "name",
 * "started", "stopped", "title" and "text" (coming straight from the NOTIFY
 * parameters). The interesting field here is "text"; we will make it a JSON
 * string with more informations depending on the context: IPs, BCA/BCN, etc *)
let alert_text fields =
  "{"^ (List.map (fun (n, v) -> Printf.sprintf "%S:%S" n v) fields |>
        String.concat ", ") ^"}"

(* Anomaly Detection
 *
 * For any func which output interesting timeseries (interesting = that we hand
 * pick) we will add a func that compute all predictions we can compute for the
 * series. We will notify when x out of y predictions are "off". for perf
 * reasons we want the number of such funcs minimal, but as we have only one
 * possible notify per func we also can't have one func for different unrelated
 * things. A good trade-off is to have one func per BCN/BCA.
 * For each timeseries to predict, we also pass a list of other timeseries that
 * we think are good predictors. *)
let anomaly_detection_funcs avg_window from name timeseries alert_fields export =
  assert (timeseries <> []) ;
  let stand_alone_predictors = [ "smooth(" ; "fit(5, " ; "5-ma(" ; "lag(" ]
  and multi_predictors = [ "fit_multi(5, " ] in
  let predictor_name = from ^": "^ name ^" predictions" in
  let predictor_func =
    let predictions =
      List.fold_left (fun fields (ts, condition, nullable, preds) ->
          let preds_str = String.concat ", " preds in
          (* Add first the timeseries itself: *)
          let fields = ts :: fields in
          (* Then the predictors: *)
          let i, fields =
            List.fold_left (fun (i, fields) predictor ->
                i+1,
                (Printf.sprintf "%s%s) AS pred_%d_%s" predictor ts i ts) :: fields
              ) (0, fields) stand_alone_predictors in
          let nb_preds, fields =
            if preds <> [] then
              List.fold_left (fun (i, fields) multi_predictor ->
                  i + 1,
                  (Printf.sprintf "%s%s, %s) AS pred_%d_%s"
                     multi_predictor ts preds_str i ts) :: fields
                ) (i, fields) multi_predictors
            else i, fields in
          (* Then the "abnormality" of this timeseries: *)
          let abnormality =
            let rec loop sum i =
              if i >= nb_preds then sum else
              let pred = "pred_"^ string_of_int i ^"_"^ ts in
              let abno = "abs("^ pred ^" - "^ ts ^") /\n     \
                            max(abs "^ pred ^", abs "^ ts ^")" in
              let abno = if nullable then "coalesce("^ abno ^", 0)"
                         else abno in
              loop (abno :: sum) (i + 1) in
            loop [] 0 in
          let abnormality =
            "("^ String.concat " +\n   " abnormality ^") / "^
            string_of_int nb_preds in
          let abnormality =
            match condition with
            | "" -> abnormality
            | cond ->
              "IF "^ cond ^" THEN "^ abnormality ^" ELSE 0" in
          let abnormality = abnormality ^" AS abnormality_"^ ts in
          abnormality :: fields
        ) [] timeseries
    in
    let op =
      Printf.sprintf
        "SELECT\n  \
           start,\n  \
           %s\n\
         FROM '%s'"
        (String.concat ",\n  " (List.rev predictions))
        from in
  let op =
    if export_all export then
      op ^ Printf.sprintf "\n\
         EXPORT EVENT STARTING AT start\n        \
                 WITH DURATION %f"
        avg_window
    else op in
    make_func predictor_name op in
  let anomaly_func =
    let conditions =
      List.fold_left (fun cond (ts, _condition, _nullable, _preds) ->
          ("abnormality_"^ ts ^" > 0.75") :: cond
        ) [] timeseries in
    let condition = String.concat " OR\n     " conditions in
    let title = Printf.sprintf "%s is off" from
    and alert_name = from ^" "^ name ^" looks abnormal"
    and text = alert_text alert_fields in
    let op =
      Printf.sprintf
        {|FROM '%s'
          SELECT start,
          (%s) AS abnormality,
          hysteresis (5-ma abnormality, 3/5, 4/5) AS firing
          COMMIT AND KEEP ALL WHEN firing != COALESCE(previous.firing, false)
          NOTIFY "http://localhost:29380/notify?name=%s&firing=${firing}&time=${start}&title=%s&text=%s"|}
        predictor_name
        condition
        (enc alert_name) (enc title) (enc text) in
    let op =
      if export_all export then op ^ {|
        EXPORT EVENT STARTING AT start|}
      else op in
    make_func (from ^": "^ name ^" anomalies") op in
  predictor_func, anomaly_func

let base_program dataset_name delete uncompress csv_glob export =
  (* Outlines of CSV importers: *)
  let csv_import csv fields =
    "READ"^ (if delete then " AND DELETE" else "") ^
    " FILES \""^ (csv_glob |> rep "%" csv) ^"\""^
    (if uncompress then " PREPROCESS WITH \"lz4 -d -c\"" else "") ^
    " SEPARATOR \"\\t\" NULL \"<NULL>\" ("^ fields ^")"
  (* Helper to build dsr-dst view of clt-srv metrics, keeping only the fields
   * that are useful for traffic-related computations: *)
  and to_unidir csv non_cs_fields cs_fields ~src ~dst name =
    let cs_fields = cs_fields |>
      List.fold_left (fun s (field, alias) ->
        let alias = if alias <> "" then alias else field in
        s ^"    "^ field ^"_"^ src ^" AS "^ alias ^"_src, "
                 ^ field ^"_"^ dst ^" AS "^ alias ^"_dst,\n") ""
    in
    let op =
      "FROM '"^ rebase dataset_name csv ^"' SELECT\n"^
      cs_fields ^ non_cs_fields ^"\n"^
      "WHERE traffic_packets_"^ src ^" > 0"^
       (if export_all export then {|
         EXPORT EVENT STARTING AT capture_begin * 1e-6
                  AND STOPPING AT capture_end * 1e-6|}
        else "")
       in
    make_func name op in
  (* TCP CSV Importer: *)
  let tcp = csv_import "tcp" {|
      poller string not null,  -- 1
      capture_begin u64 not null,
      capture_end u64 not null,
      device_client u8 null,
      device_server u8 null,
      vlan_client u32 null,
      vlan_server u32 null,
      mac_client u64 null,
      mac_server u64 null,
      zone_client u32 not null,  -- 10
      zone_server u32 not null,
      ip4_client u32 null,
      ip6_client i128 null,
      ip4_server u32 null,
      ip6_server i128 null,
      ip4_external u32 null,
      ip6_external i128 null,
      port_client u16 not null,
      port_server u16 not null,
      diffserv_client u8 not null,  -- 20
      diffserv_server u8 not null,
      os_client u8 null,
      os_server u8 null,
      mtu_client u32 null,
      mtu_server u32 null,
      captured_pcap string null,
      application u32 not null,
      protostack string null,
      uuid string null,
      traffic_bytes_client u64 not null,  -- 30
      traffic_bytes_server u64 not null,
      traffic_packets_client u32 not null,
      traffic_packets_server u32 not null,
      payload_bytes_client u64 not null,
      payload_bytes_server u64 not null,
      payload_packets_client u32 not null,
      payload_packets_server u32 not null,
      retrans_traffic_bytes_client u64 not null,
      retrans_traffic_bytes_server u64 not null,
      retrans_payload_bytes_client u64 not null,
      retrans_payload_bytes_server u64 not null,
      syn_count_client u32 not null,
      fin_count_client u32 not null,
      fin_count_server u32 not null,
      rst_count_client u32 not null,
      rst_count_server u32 not null,
      timeout_count u32 not null,
      close_count u32 not null,
      dupack_count_client u32 not null,
      dupack_count_server u32 not null,
      zero_window_count_client u32 not null,
      zero_window_count_server u32 not null,
      ct_count u32 not null,
      ct_sum u64 not null,
      ct_square_sum u128 not null,
      rt_count_server u32 not null,
      rt_sum_server u64 not null,
      rt_square_sum_server u128 not null,
      rtt_count_client u32 not null,
      rtt_sum_client u64 not null,
      rtt_square_sum_client u128 not null,
      rtt_count_server u32 not null,
      rtt_sum_server u64 not null,
      rtt_square_sum_server u128 not null,
      rd_count_client u32 not null,
      rd_sum_client u64 not null,
      rd_square_sum_client u128 not null,
      rd_count_server u32 not null,
      rd_sum_server u64 not null,
      rd_square_sum_server u128 not null,
      dtt_count_client u32 not null,
      dtt_sum_client u64 not null,
      dtt_square_sum_client u128 not null,
      dtt_count_server u32 not null,
      dtt_sum_server u64 not null,
      dtt_square_sum_server u128 not null,
      dcerpc_uuid string null|} |>
    make_func "tcp"
  and tcp_to_unidir = to_unidir "tcp" {|
    poller, capture_begin, capture_end,
    ip4_external, ip6_external,
    captured_pcap, application, protostack, uuid,
    timeout_count AS timeouts, close_count AS closes,
    ct_count AS connections, ct_sum AS connections_time,
    ct_square_sum AS connections_time2, syn_count_client AS syns,
    dcerpc_uuid|} [
    "device", "" ; "vlan", "" ; "mac", "" ; "zone", "" ; "ip4", "" ;
    "ip6", "" ; "port", "" ; "diffserv", "" ; "os", "" ; "mtu", "" ;
    "traffic_packets", "packets" ; "traffic_bytes", "bytes" ;
    "payload_bytes", "payload" ;
    "payload_packets", "packets_with_payload" ;
    "retrans_traffic_bytes", "retrans_bytes" ;
    "retrans_payload_bytes", "retrans_payload" ;
    "fin_count", "fins" ; "rst_count", "rsts" ;
    "dupack_count", "dupacks" ; "zero_window_count", "zero_windows" ;
    "rtt_count", "" ; "rtt_sum", "" ; "rtt_square_sum", "rtt_sum2" ;
    "rd_count", "" ; "rd_sum", "" ; "rd_square_sum", "rd_sum2" ;
    "dtt_count", "" ; "dtt_sum", "" ; "dtt_square_sum", "dtt_sum2" ]
  (* UDP CSV Importer: *)
  and udp = csv_import "udp" {|
      poller string not null,  -- 1
      capture_begin u64 not null,
      capture_end u64 not null,
      device_client u8 null,
      device_server u8 null,
      vlan_client u32 null,
      vlan_server u32 null,
      mac_client u64 null,
      mac_server u64 null,
      zone_client u32 not null,  -- 10
      zone_server u32 not null,
      ip4_client u32 null,
      ip6_client i128 null,
      ip4_server u32 null,
      ip6_server i128 null,
      ip4_external u32 null,
      ip6_external i128 null,
      port_client u16 not null,
      port_server u16 not null,
      diffserv_client u8 not null,  -- 20
      diffserv_server u8 not null,
      mtu_client u32 null,
      mtu_server u32 null,
      application u32 not null,
      protostack string null,
      traffic_bytes_client u64 not null,
      traffic_bytes_server u64 not null,
      traffic_packets_client u32 not null,
      traffic_packets_server u32 not null,
      payload_bytes_client u64 not null,  -- 30
      payload_bytes_server u64 not null,
      dcerpc_uuid string null|} |>
    make_func "udp"
  and udp_to_unidir = to_unidir "udp" {|
    poller, capture_begin, capture_end,
    ip4_external, ip6_external,
    0u32 AS rtt_count_src, 0u64 AS rtt_sum_src,
    0u32 AS packets_with_payload_src, 0u32 AS rd_count_src,
    application, protostack,
    dcerpc_uuid|} [
    "device", "" ; "vlan", "" ; "mac", "" ; "zone", "" ; "ip4", "" ;
    "ip6", "" ; "port", "" ; "diffserv", "" ; "mtu", "" ;
    "traffic_packets", "packets" ; "traffic_bytes", "bytes" ;
    "payload_bytes", "payload" ]
  (* IP (non UDP/TCP) CSV Importer: *)
  and icmp = csv_import "icmp" {|
      poller string not null,
      capture_begin u64 not null,
      capture_end u64 not null,
      device_client u8 null,
      device_server u8 null,
      vlan_client u32 null,
      vlan_server u32 null,
      mac_client u64 null,
      mac_server u64 null,
      zone_client u32 not null,
      zone_server u32 not null,
      ip4_client u32 null,
      ip6_client i128 null,
      ip4_server u32 null,
      ip6_server i128 null,
      ip4_external u32 null,
      ip6_external i128 null,
      diffserv_client u8 not null,
      diffserv_server u8 not null,
      mtu_client u16 null,
      mtu_server u16 null,
      application u32 not null,
      protostack string null,
      traffic_bytes_client u64 not null,
      traffic_bytes_server u64 not null,
      traffic_packets_client u32 not null,
      traffic_packets_server u32 not null,
      icmp_type u8 not null,
      icmp_code u8 not null,
      error_ip4_client u32 null,
      error_ip6_client i128 null,
      error_ip4_server u32 null,
      error_ip6_server i128 null,
      error_port_client u16 null,
      error_port_server u16 null,
      error_ip_proto u8 null,
      error_zone_client u32 null,
      error_zone_server u32 null|} |>
    make_func "icmp"
  and icmp_to_unidir = to_unidir "icmp" {|
    poller, capture_begin, capture_end,
    ip4_external, ip6_external,
    0u32 AS rtt_count_src, 0u64 AS rtt_sum_src,
    0u32 AS packets_with_payload_src, 0u32 AS rd_count_src,
    application, protostack|} [
    "device", "" ; "vlan", "" ; "mac", "" ; "zone", "" ; "ip4", "" ;
    "ip6", "" ; "diffserv", "" ; "mtu", "" ;
    "traffic_packets", "packets" ; "traffic_bytes", "bytes" ]
  (* IP (non UDP/TCP) CSV Importer: *)
  and other_ip = csv_import "other_ip" {|
      poller string not null,
      capture_begin u64 not null,
      capture_end u64 not null,
      device_client u8 null,
      device_server u8 null,
      vlan_client u32 null,
      vlan_server u32 null,
      mac_client u64 null,
      mac_server u64 null,
      zone_client u32 not null,
      zone_server u32 not null,
      ip4_client u32 null,
      ip6_client i128 null,
      ip4_server u32 null,
      ip6_server i128 null,
      diffserv_client u8 not null,
      diffserv_server u8 not null,
      mtu_client u16 null,
      mtu_server u16 null,
      ip_protocol u8 not null,
      application u32 not null,
      protostack string null,
      traffic_bytes_client u64 not null,
      traffic_bytes_server u64 not null,
      traffic_packets_client u32 not null,
      traffic_packets_server u32 not null|} |>
    make_func "other-than-ip"
  and other_ip_to_unidir = to_unidir "other-than-ip" {|
    poller, capture_begin, capture_end,
    0u32 AS rtt_count_src, 0u64 AS rtt_sum_src,
    0u32 AS packets_with_payload_src, 0u32 AS rd_count_src,
    application, protostack|} [
    "device", "" ; "vlan", "" ; "mac", "" ; "zone", "" ; "ip4", "" ;
    "ip6", "" ; "diffserv", "" ; "mtu", "" ;
    "traffic_packets", "packets" ; "traffic_bytes", "bytes" ]
  (* non-IP CSV Importer: *)
  and non_ip = csv_import "non_ip" {|
      poller string not null,
      capture_begin u64 not null,
      capture_end u64 not null,
      device_client u8 null,
      device_server u8 null,
      vlan_client u32 null,
      vlan_server u32 null,
      mac_client u64 null,
      mac_server u64 null,
      zone_client u32 not null,
      zone_server u32 not null,
      mtu_client u16 null,
      mtu_server u16 null,
      eth_type u16 not null,
      application u32 not null,
      protostack string null,
      traffic_bytes_client u64 not null,
      traffic_bytes_server u64 not null,
      traffic_packets_client u32 not null,
      traffic_packets_server u32 not null|} |>
    make_func "non-ip"
  and non_ip_to_unidir = to_unidir "non-ip" {|
    poller, capture_begin, capture_end,
    0u32 AS rtt_count_src, 0u64 AS rtt_sum_src,
    0u32 AS packets_with_payload_src, 0u32 AS rd_count_src,
    application, protostack|} [
    "device", "" ; "vlan", "" ; "mac", "" ; "zone", "" ; "mtu", "" ;
    "traffic_packets", "packets" ; "traffic_bytes", "bytes" ]
  (* DNS CSV Importer: *)
  and dns = csv_import "dns" {|
      poller string not null,
      capture_begin u64 not null,
      capture_end u64 not null,
      device_client u8 null,
      device_server u8 null,
      vlan_client u32 null,
      vlan_server u32 null,
      mac_client u64 null,
      mac_server u64 null,
      zone_client u32 not null,
      zone_server u32 not null,
      ip4_client u32 null,
      ip6_client i128 null,
      ip4_server u32 null,
      ip6_server i128 null,
      query_name string not null,
      query_type u16 not null,
      query_class u16 not null,
      error_code u8 not null,
      error_count u32 not null,
      answer_type u16 not null,
      answer_class u16 not null,
      capture_file string null,
      connection_uuid string null,
      traffic_bytes_client u64 not null,
      traffic_bytes_server u64 not null,
      traffic_packets_client u32 not null,
      traffic_packets_server u32 not null,
      rt_count_server u32 not null,
      rt_sum_server u64 not null,
      rt_square_sum_server u128 not null|} |>
    make_func "dns"
  (* HTTP CSV Importer: *)
  and http = csv_import "http" {|
      poller string not null,
      capture_begin u64 not null,
      capture_end u64 not null,
      device_client u8 null,
      device_server u8 null,
      vlan_client u32 null,
      vlan_server u32 null,
      mac_client u64 null,
      mac_server u64 null,
      zone_client u32 not null, -- 10
      zone_server u32 not null,
      ip4_client u32 null,
      ip6_client i128 null,
      ip4_server u32 null,
      ip6_server i128 null,
      port_client u16 not null,
      port_server u16 not null,
      connection_uuid string null,
      id string not null,
      parent_id string not null,  -- 20
      referrer_id string null,
      deep_inspect bool not null,
      contributed bool not null,
      timeouted bool not null,
      host string null,
      user_agent string null,
      url string not null,
      server string null,
      compressed bool not null,
      chunked_encoding bool not null,  -- 30
      ajax bool not null,
      ip4_orig_client u32 null,
      ip6_orig_client i128 null,
      page_count u32 not null,
      hardcoded_one_facepalm bool not null,
      query_begin_ts u64 not null,
      query_end_ts u64 not null,
      query_method u8 not null,
      query_headers u32 not null,
      query_payload u32 not null,  -- 40
      query_pkts u32 not null,
      query_content string null,
      query_content_length u32 null,
      query_content_length_count u32 not null,
      query_mime_type string null,
      resp_begin_ts u64 null,
      resp_end_ts u64 null,
      resp_code u32 null,
      resp_headers u32 not null,
      resp_payload u32 not null,  -- 50
      resp_pkts u32 not null,
      resp_content string null,
      resp_content_length u32 null,
      resp_content_length_count u32 not null,
      resp_mime_type string null,
      tot_volume_query u32 null,
      tot_volume_response u32 null,
      tot_count u32 not null,
      tot_errors u16 not null,
      tot_timeouts u16 not null,  -- 60
      tot_begin_ts u64 not null,
      tot_end_ts u64 not null,
      tot_load_time u64 not null,
      tot_load_time_squared u128 not null,
      rt_count_server u32 not null,
      rt_sum_server u64 not null,
      rt_square_sum_server u128 not null,
      dtt_sum_client u64 not null,
      dtt_square_sum_client u128 not null,
      dtt_sum_server u64 not null,  -- 70
      dtt_square_sum_server u128 not null,
      application u32 not null|} |>
    make_func "http"
  (* Citrix CSV Importer: *)
  and citrix = csv_import "citrix" {|
      poller string not null,
      capture_begin u64 not null,
      capture_end u64 not null,
      device_client u8 null,
      device_server u8 null,
      vlan_client u32 null,
      vlan_server u32 null,
      mac_client u64 null,
      mac_server u64 null,
      zone_client u32 not null,
      zone_server u32 not null,
      ip4_client u32 null,
      ip6_client i128 null,
      ip4_server u32 null,
      ip6_server i128 null,
      port_client u16 not null,
      port_server u16 not null,
      application u32 not null,
      protostack string null,
      connection_uuid string null,
      channel_id u8 null,
      channel u8 null,
      pdus_client u32 not null,
      pdus_server u32 not null,
      nb_compressed_client u32 not null,
      nb_compressed_server u32 not null,
      payloads_client u32 not null,
      payloads_server u32 not null,
      rt_count_server u32 not null,
      rt_sum_server u64 not null,
      rt_square_sum_server u128 not null,
      dtt_count_client u32 not null,
      dtt_sum_client u64 not null,
      dtt_square_sum_client u128 not null,
      dtt_count_server u32 not null,
      dtt_sum_server u64 not null,
      dtt_square_sum_server u128 not null,
      username string null,
      domain string null,
      citrix_application string null|} |>
    make_func "citrix"
  (* Citrix (without channel) CSV Importer: *)
  and citrix_chanless = csv_import "citrix_chanless" {|
      poller string not null,
      capture_begin u64 not null,
      capture_end u64 not null,
      device_client u8 null,
      device_server u8 null,
      vlan_client u32 null,
      vlan_server u32 null,
      mac_client u64 null,
      mac_server u64 null,
      zone_client u32 not null,  -- 10
      zone_server u32 not null,
      ip4_client u32 null,
      ip6_client i128 null,
      ip4_server u32 null,
      ip6_server i128 null,
      port_client u16 not null,
      port_server u16 not null,
      application u32 not null,
      protostack string null,
      connection_uuid string null,  -- 20
      module_name string null,
      encrypt_type u8 not null,
      pdus_client u32 not null,
      pdus_server u32 not null,
      pdus_cgp_client u32 not null,
      pdus_cgp_server u32 not null,
      nb_keep_alives_client u32 not null,
      nb_keep_alives_server u32 not null,
      payloads_client u32 not null,
      payloads_server u32 not null,  -- 30
      rt_count_server u32 not null,
      rt_sum_server u64 not null,
      rt_square_sum_server u128 not null,
      dtt_count_client u32 not null,
      dtt_sum_client u64 not null,
      dtt_square_sum_client u128 not null,
      dtt_count_server u32 not null,
      dtt_sum_server u64 not null,
      dtt_square_sum_server u128 not null,
      login_time_count u32 not null,
      login_time_sum u64 not null,
      login_time_square_sum u128 not null,  -- 40
      launch_time_count u32 not null,
      launch_time_sum u64 not null,
      launch_time_square_sum u128 not null,
      nb_aborts u32 not null,
      nb_timeouts u32 not null,
      username string null,
      domain string null,
      citrix_application string null|} |>
    make_func "citrix_chanless"
  (* SMB CSV Importer: *)
  and smb = csv_import "smb" {|
      poller string not null,
      capture_begin u64 not null,
      capture_end u64 not null,
      device_client u8 null,
      device_server u8 null,
      vlan_client u32 null,
      vlan_server u32 null,
      mac_client u64 null,
      mac_server u64 null,
      zone_client u32 not null,  -- 10
      zone_server u32 not null,
      ip4_client u32 null,
      ip6_client i128 null,
      ip4_server u32 null,
      ip6_server i128 null,
      port_client u16 not null,
      port_server u16 not null,
      version u32 not null,
      protostack string null,
      user string null,  -- 20
      domain string null,
      file_id u128 null,
      path string null,
      tree_id u32 null,
      tree string null,
      status u32 null,
      command u32 not null,
      subcommand u32 null,
      timeouted bool not null,
      is_error bool not null,  -- 30
      is_warning bool not null,
      hardcoded_one_facepalm bool not null,
      connection_uuid string null,
      query_begin_ts u64 not null,
      query_end_ts u64 not null,
      query_payload u32 not null,
      query_pkts u32 not null,
      resp_begin_ts u64 null,
      resp_end_ts u64 null,
      resp_payload u32 not null,  -- 40
      resp_pkts u32 not null,
      meta_read_bytes u32 not null,
      meta_write_bytes u32 not null,
      query_write_bytes u32 not null,
      resp_read_bytes u32 not null,
      resp_write_bytes u32 not null,
      rt_count_server u32 not null,
      rt_sum_server u64 not null,
      rt_square_sum_server u128 not null,
      dtt_count_client u32 not null,  -- 50
      dtt_sum_client u64 not null,
      dtt_square_sum_client u128 not null,
      dtt_count_server u32 not null,
      dtt_sum_server u64 not null,
      dtt_square_sum_server u128 not null,
      application u32 not null|} |>
    make_func "smb"
  (* SQL CSV Importer: *)
  and sql = csv_import "sql" {|
      poller string not null,
      capture_begin u64 not null,
      capture_end u64 not null,
      device_client u8 null,
      device_server u8 null,
      vlan_client u32 null,
      vlan_server u32 null,
      mac_client u64 null,
      mac_server u64 null,
      zone_client u32 not null,  -- 10
      zone_server u32 not null,
      ip4_client u32 null,
      ip6_client i128 null,
      ip4_server u32 null,
      ip6_server i128 null,
      port_client u16 not null,
      port_server u16 not null,
      query string not null,
      timeouted bool not null,
      protostack string null,  -- 20
      user string null,
      dbname string null,
      error_sql_status string null,
      error_code string null,
      error_msg string null,
      is_error bool not null,
      hardcoded_one_facepalm bool not null,
      command u32 null,
      connection_uuid string null,
      query_begin_ts u64 not null,  -- 30
      query_end_ts u64 not null,
      query_payload u32 not null,
      query_pkts u32 not null,
      resp_begin_ts u64 null,
      resp_end_ts u64 null,
      resp_payload u32 not null,
      resp_pkts u32 not null,
      rt_count_server u32 not null,
      rt_sum_server u64 not null,
      rt_square_sum_server u128 not null,  -- 40
      dtt_count_client u32 not null,
      dtt_sum_client u64 not null,
      dtt_square_sum_client u128 not null,
      dtt_count_server u32 not null,
      dtt_sum_server u64 not null,
      dtt_square_sum_server u128 not null,
      application u32 not null|} |>
    make_func "sql"
  (* VoIP CSV Importer: *)
  and voip = csv_import "voip" {|
      poller string not null,
      capture_begin u64 not null,
      capture_end u64 not null,
      device_client u8 null,
      device_server u8 null,
      vlan_client u32 null,
      vlan_server u32 null,
      mac_client u64 null,
      mac_server u64 null,
      zone_client u32 not null,  -- 10
      zone_server u32 not null,
      ip4_client u32 null,
      ip6_client i128 null,
      ip4_server u32 null,
      ip6_server i128 null,
      port_client u16 not null,
      port_server u16 not null,
      capture_file string null,
      application u32 not null,
      protostack string null,
      connection_uuid string null,
      ip_protocol u8 not null,
      had_voice bool not null,
      call_direction_is_out bool null,
      last_call_state u8 not null,
      is_starting bool not null,
      is_finished bool not null,
      hardcoded_0 bool not null,
      last_error u32 not null,
      call_id string not null,
      rtp_duration u64 null,
      id_caller string not null,
      caller_mac u64 not null,
      ip4_caller u32 null,
      ip6_caller i128 null,
      zone_caller u32 not null,
      caller_codec string null,
      id_callee string not null,
      callee_mac u64 not null,
      ip4_callee u32 null,
      ip6_callee i128 null,
      zone_callee u32 not null,
      callee_codec string null,
      sign_bytes_client u32 not null,
      sign_bytes_server u32 not null,
      sign_count_client u32 not null,
      sign_count_server u32 not null,
      sign_payload_client u32 not null,
      sign_payload_server u32 not null,
      rtp_rtcp_bytes_client u32 not null,
      rtp_rtcp_bytes_server u32 not null,
      rtp_rtcp_count_client u32 not null,
      rtp_rtcp_count_server u32 not null,
      rtp_rtcp_payload_client u32 not null,
      rtp_rtcp_payload_server u32 not null,
      rt_count_server u32 not null,
      rt_sum_server u64 not null,
      rt_square_sum_server u128 not null,
      jitter_count_caller u32 not null,
      jitter_sum_caller u64 not null,
      jitter_square_sum_caller u128 not null,
      jitter_count_callee u32 not null,
      jitter_sum_callee u64 not null,
      jitter_square_sum_callee u128 not null,
      rtt_count_caller u32 not null,
      rtt_sum_caller u64 not null,
      rtt_square_sum_caller u128 not null,
      rtt_count_callee u32 not null,
      rtt_sum_callee u64 not null,
      rtt_square_sum_callee u128 not null,
      loss_callee2caller_alt_count u32 not null,
      loss_caller2callee_alt_count u32 not null,
      sign_rtt_count_client u32 not null,
      sign_rtt_sum_client u64 not null,
      sign_rtt_square_sum_client u128 not null,
      sign_rtt_count_server u32 not null,
      sign_rtt_sum_server u64 not null,
      sign_rtt_square_sum_server u128 not null,
      sign_rd_count_client u32 not null,
      sign_rd_sum_client u64 not null,
      sign_rd_square_sum_client u128 not null,
      sign_rd_count_server u32 not null,
      sign_rd_sum_server u64 not null,
      sign_rd_square_sum_server u128 not null|} |>
    make_func "voip"
  in
  RamenSharedTypes.{
    name = dataset_name ;
    ok_if_running = true ;
    start = true ;
    program = program_of_funcs [
      tcp ;
      tcp_to_unidir ~src:"client" ~dst:"server" "c2s tcp" ;
      tcp_to_unidir ~src:"server" ~dst:"client" "s2c tcp" ;
      udp ;
      udp_to_unidir ~src:"client" ~dst:"server" "c2s udp" ;
      udp_to_unidir ~src:"server" ~dst:"client" "s2c udp" ;
      icmp ;
      icmp_to_unidir ~src:"client" ~dst:"server" "c2s icmp" ;
      icmp_to_unidir ~src:"server" ~dst:"client" "s2c icmp" ;
      other_ip ;
      other_ip_to_unidir ~src:"client" ~dst:"server" "c2s other-than-ip" ;
      other_ip_to_unidir ~src:"server" ~dst:"client" "s2c other-than-ip" ;
      non_ip ;
      non_ip_to_unidir ~src:"client" ~dst:"server" "c2s non-ip" ;
      non_ip_to_unidir ~src:"server" ~dst:"client" "s2c non-ip" ;
      dns ; http ; citrix ; citrix_chanless ; smb ; sql ; voip ] }

(* Build the func infos corresponding to the BCN configuration *)
let program_of_bcns bcns dataset_name export =
  let program_name = rebase dataset_name "BCN" in
  let name_of_zones = function
    | [] -> "any"
    | main::_ -> string_of_int main in
  let all_funcs = ref [] in
  let make_func name operation =
    let func = make_func name operation in
    all_funcs := func :: !all_funcs
  in
  let conf_of_bcn bcn =
    (* bcn.min_bps, bcn.max_bps, bcn.obs_window, bcn.avg_window, bcn.percentile, bcn.source bcn.dest *)
    let open Conf_of_sqlite.BCN in
    let name_prefix = Printf.sprintf "%s to %s"
      (name_of_zones bcn.source) (name_of_zones bcn.dest) in
    let avg_window = int_of_float (bcn.avg_window *. 1_000_000.0) in
    let avg_per_zones_name =
      Printf.sprintf "%s: avg traffic every %gs"
        name_prefix bcn.avg_window in
    let in_zone what_zone = function
      | [] -> "true"
      | lst ->
        "("^ List.fold_left (fun s z ->
          s ^ (if s <> "" then " OR " else "") ^
          what_zone ^ " = " ^ string_of_int z) "" lst ^")" in
    let where =
      (in_zone "in.zone_src" bcn.source) ^" AND "^
      (in_zone "in.zone_dst" bcn.dest) in
    (* This operation differs from tcp_traffic_func:
     * - it adds zone_src and zone_dst names, which can be useful indeed;
     * - it lacks many of the TCP-only fields and so can apply on all traffic;
     * - it works for whatever avg_window not necessarily minutely. *)
    let op =
      Printf.sprintf {|
          FROM '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s'
          SELECT
            (capture_begin // %d) * %g AS start,
            min capture_begin, max capture_end,
            -- Traffic
            sum packets_src / %g AS packets_per_secs,
            sum bytes_src / %g AS bytes_per_secs,
            -- RTT (in seconds)
            sum rtt_count_src AS _sum_rtt_count_src,
            IF _sum_rtt_count_src = 0 THEN 0 ELSE
              (sum rtt_sum_src / _sum_rtt_count_src) / 1e6 AS avg_rtt,
            -- RD: percentage of retransmitted packets over packets with payload
            sum packets_with_payload_src AS _sum_packets_with_payload_src,
            IF _sum_packets_with_payload_src = 0 THEN 0 ELSE
              (sum rd_count_src / _sum_packets_with_payload_src) / 100 AS avg_rr,
            %S AS zone_src, %S AS zone_dst
          WHERE %s
          GROUP BY capture_begin // %d
          COMMIT WHEN
            in.capture_begin > out.min_capture_begin + 2 * u64(%d)|}
        (rebase dataset_name "c2s tcp") (rebase dataset_name "s2c tcp")
        (rebase dataset_name "c2s udp") (rebase dataset_name "s2c udp")
        (rebase dataset_name "c2s icmp") (rebase dataset_name "s2c icmp")
        (rebase dataset_name "c2s other-than-ip") (rebase dataset_name "s2c other-than-ip")
        (rebase dataset_name "c2s non-ip") (rebase dataset_name "s2c non-ip")
        avg_window bcn.avg_window
        bcn.avg_window bcn.avg_window
        (name_of_zones bcn.source)
        (name_of_zones bcn.dest)
        where
        avg_window
        avg_window in
    let op =
      if export_all export then op ^ Printf.sprintf {|
          EXPORT EVENT STARTING AT start
                 WITH DURATION %g|}
          bcn.avg_window
      else op
        (* Note: Ideally we would want to compute the max of all.capture_begin *)
    in
    make_func avg_per_zones_name op ;
    let perc_per_obs_window_name =
      Printf.sprintf "%s: %gth perc on last %gs"
        name_prefix bcn.percentile bcn.obs_window in
    let op =
      let nb_items_per_groups =
        Helpers.round_to_int (bcn.obs_window /. bcn.avg_window) in
      (* Note: The event start at the end of the observation window and lasts
       * for one avg window! *)
      Printf.sprintf
        {|FROM '%s' SELECT
           min start, max start,
           min min_capture_begin AS min_capture_begin,
           max max_capture_end AS max_capture_end,
           %gth percentile bytes_per_secs AS bytes_per_secs,
           %gth percentile avg_rtt AS rtt,
           %gth percentile avg_rr AS rr,
           zone_src, zone_dst
         COMMIT AND SLIDE 1 WHEN
           group.#count >= %d OR
           in.start > out.max_start + 5|}
         avg_per_zones_name
         bcn.percentile bcn.percentile bcn.percentile
         nb_items_per_groups in
    let op =
      if export_some export then
        op ^ Printf.sprintf {|
         EXPORT EVENT STARTING AT max_capture_end * 0.000001
                 WITH DURATION %g|}
           bcn.avg_window
      else op in
    make_func perc_per_obs_window_name op ;
    Option.may (fun min_bps ->
        let title = Printf.sprintf "Too little traffic from zone %s to %s"
                      (name_of_zones bcn.source) (name_of_zones bcn.dest)
        and text = alert_text [
          "descr", Printf.sprintf
                     "The traffic from zone %s to %s has sunk below \
                      the configured minimum of %d for the last %g minutes."
                      (name_of_zones bcn.source) (name_of_zones bcn.dest)
                      min_bps (bcn.obs_window /. 60.) ;
          "bcn_id", string_of_int bcn.id ] in
        let op = Printf.sprintf
          {|SELECT
              max_start,
              hysteresis (bytes_per_secs, %d, %d) AS firing
            FROM '%s'
            COMMIT AND KEEP ALL WHEN firing != COALESCE(previous.firing, false)
            NOTIFY "http://localhost:29380/notify?name=Low%%20traffic&firing=${firing}&time=${max_start}&title=%s&text=%s"|}
            (min_bps + min_bps/10) min_bps
            perc_per_obs_window_name
            (enc title) (enc text) in
        let op =
          if export_all export then op ^ {|
            EXPORT EVENT STARTING AT max_start|}
          else op in
        let name = Printf.sprintf "%s: alert traffic too low" name_prefix in
        make_func name op
      ) bcn.min_bps ;
    Option.may (fun max_bps ->
        let title = Printf.sprintf "Too much traffic from zone %s to %s"
                        (name_of_zones bcn.source) (name_of_zones bcn.dest)
        and text = alert_text [
          "descr", Printf.sprintf
                     "The traffic from zones %s to %s has raised above \
                      the configured maximum of %d for the last %g minutes."
                      (name_of_zones bcn.source) (name_of_zones bcn.dest)
                      max_bps (bcn.obs_window /. 60.) ;
          "bcn_id", string_of_int bcn.id ] in
        let op = Printf.sprintf
          {|SELECT
              max_start,
              hysteresis (bytes_per_secs, %d, %d) AS firing
            FROM '%s'
            COMMIT AND KEEP ALL WHEN firing != COALESCE(previous.firing, false)
            NOTIFY "http://localhost:29380/notify?name=High%%20traffic&firing=${firing}&time=${max_start}&title=%s&text=%s"|}
            (max_bps - max_bps/10) max_bps
            perc_per_obs_window_name
            (enc title) (enc text) in
        let op =
          if export_all export then op ^ {|
            EXPORT EVENT STARTING AT max_start|}
          else op in
        let name = Printf.sprintf "%s: alert traffic too high" name_prefix in
        make_func name op
      ) bcn.max_bps ;
    Option.may (fun max_rtt ->
        let title = Printf.sprintf "RTT too high from zone %s to %s"
                      (name_of_zones bcn.source) (name_of_zones bcn.dest)
        and text = alert_text [
          "descr", Printf.sprintf
                     "Traffic from zone %s to zone %s has an average RTT \
                      of _rtt_, greater than the configured maximum of %gs, \
                      for the last %g minutes."
                      (name_of_zones bcn.source) (name_of_zones bcn.dest)
                      max_rtt (bcn.obs_window /. 60.) ;
          "bcn_id", string_of_int bcn.id ] in
        let op = Printf.sprintf
          {|SELECT
              max_start,
              hysteresis (rtt, %f, %f) AS firing
            FROM '%s'
            COMMIT AND KEEP ALL WHEN firing != COALESCE(previous.firing, false)
            NOTIFY "http://localhost:29380/notify?name=High%%20RTT&firing=${firing}&time=${max_start}&title=%s&text=%s"|}
            (max_rtt -. max_rtt /. 10.) max_rtt
            perc_per_obs_window_name
            (enc title) (enc text |> rep "_rtt_" "${rtt}") in
        let op =
          if export_all export then op ^ {|
            EXPORT EVENT STARTING AT max_start|}
          else op in
        let name = Printf.sprintf "%s: alert RTT" name_prefix in
        make_func name op
      ) bcn.max_rtt ;
    Option.may (fun max_rr ->
        let title = Printf.sprintf "Too many retransmissions from zone %s to %s"
                      (name_of_zones bcn.source) (name_of_zones bcn.dest)
        and text = alert_text [
          "descr", Printf.sprintf
                     "Traffic from zone %s to zone %s has an average \
                      retransmission rate of _rr_%%, greater than the \
                      configured maximum of %gs, for the last %g minutes."
                      (name_of_zones bcn.source) (name_of_zones bcn.dest)
                      max_rr (bcn.obs_window /. 60.) ;
          "bcn_id", string_of_int bcn.id ] in
        let op = Printf.sprintf
          {|SELECT
              max_start,
              hysteresis (rr, %f, %f) AS firing
            FROM '%s'
            COMMIT AND KEEP ALL WHEN firing != COALESCE(previous.firing, false)
            NOTIFY "http://localhost:29380/notify?name=High%%20RR&firing=${firing}&time=${max_start}&title=%s&text=%s"|}
            (max_rr -. max_rr /. 10.) max_rr
            perc_per_obs_window_name
            (enc title) (enc text |> rep "_rr_" "${rr}") in
        let op =
          if export_all export then op ^ {|
            EXPORT EVENT STARTING AT max_start|}
          else op in
        let name = Printf.sprintf "%s: alert RR" name_prefix in
        make_func name op
      ) bcn.max_rr ;
    let minutely =
      tcp_traffic_func ~where dataset_name (name_prefix ^": TCP minutely traffic") 60 export in
    all_funcs := minutely :: !all_funcs ;
    let alert_fields = [
      "descr", "anomaly detected" ;
      "bcn_id", string_of_int bcn.id ] in
    let anom name timeseries =
      let alert_fields = ("metric", name) :: alert_fields in
      let pred, anom =
        anomaly_detection_funcs bcn.avg_window minutely.SN.name name timeseries alert_fields export in
      all_funcs := pred :: anom :: !all_funcs in
    (* TODO: a volume anomaly for other protocols as well *)
    anom "volume"
      [ "packets_per_secs", "packets_per_secs > 10", false,
          [ "bytes_per_secs" ; "packets_with_payload_per_secs" ] ;
        "bytes_per_secs", "bytes_per_secs > 1000", false,
          [ "packets_per_secs" ; "payload_per_secs" ] ;
        "payload_per_secs", "payload_per_secs > 1000", false,
          [ "bytes_per_secs" ] ;
        "packets_with_payload_per_secs",
          "packets_with_payload_per_secs > 10", false,
          [ "packets_per_secs" ] ] ;
    anom "retransmissions"
      [ "retrans_bytes_per_secs", "retrans_bytes_per_secs > 1000", false,
        [ "retrans_payload_per_secs" ] ;
        "dupacks_per_secs", "dupacks_per_secs > 1", false, [] ] ;
    anom "connections"
      [ "fins_per_secs", "fins_per_secs > 1", false, [ "packets_per_secs" ] ;
        "rsts_per_secs", "rsts_per_secs > 1", false, [ "packets_per_secs" ] ;
        "syns_per_secs", "syns_per_secs > 1", false, [ "packets_per_secs" ] ;
        "connections_per_secs", "connections_per_secs > 1", false, [] ;
        "connection_time_avg", "connections_per_secs > 1", false, [] ] ;
    anom "0-windows"
      [ "zero_windows_per_secs", "zero_windows_per_secs > 1", false, [] ] ;
    anom "RTT" [ "rtt_avg", "sum_rtt_count_src > 10", false, [] ] ;
    anom "RD" [ "rd_avg", "sum_rd_count_src > 10", false, [] ] ;
    anom "DTT" [ "dtt_avg", "sum_dtt_count_src > 10", false, [] ]
  in
  List.iter conf_of_bcn bcns ;
  RamenSharedTypes.{
    name = program_name ;
    ok_if_running = true ;
    start = true ;
    program = program_of_funcs !all_funcs }

(* Build the func infos corresponding to the BCA configuration *)
let program_of_bcas bcas dataset_name export =
  let program_name = rebase dataset_name "BCA" in
  let all_funcs = ref [] in
  let make_func name operation =
    let func = make_func name operation in
    all_funcs := func :: !all_funcs
  in
  let conf_of_bca bca =
    let open Conf_of_sqlite.BCA in
    let avg_window_int = int_of_float bca.avg_window in
    let avg_per_app = bca.name in
    let csv = rebase dataset_name "tcp" in
    let op =
      {|FROM '$CSV$' SELECT
          -- Key
          (capture_begin * 0.000001 // $AVG_INT$) * $AVG_INT$ AS start,
          -- Traffic
          sum traffic_bytes_client / $AVG$ AS c2s_bytes_per_secs,
          sum traffic_bytes_server / $AVG$ AS s2c_bytes_per_secs,
          sum traffic_packets_client / $AVG$ AS c2s_packets_per_secs,
          sum traffic_packets_server / $AVG$ AS s2c_packets_per_secs,
          -- Retransmissions
          sum retrans_traffic_bytes_client / $AVG$
            AS c2s_retrans_bytes_per_secs,
          sum retrans_traffic_bytes_server / $AVG$
            AS s2c_retrans_bytes_per_secs,
          -- TCP flags
          sum syn_count_client / $AVG$ AS c2s_syns_per_secs,
          sum fin_count_client / $AVG$ AS c2s_fins_per_secs,
          sum fin_count_server / $AVG$ AS s2c_fins_per_secs,
          sum rst_count_client / $AVG$ AS c2s_rsts_per_secs,
          sum rst_count_server / $AVG$ AS s2c_rsts_per_secs,
          sum close_count / $AVG$ AS close_per_secs,
          -- TCP issues
          sum dupack_count_client / $AVG$ AS c2s_dupacks_per_secs,
          sum dupack_count_server / $AVG$ AS s2c_dupacks_per_secs,
          sum zero_window_count_client / $AVG$ AS c2s_0wins_per_secs,
          sum zero_window_count_server / $AVG$ AS s2c_0wins_per_secs,
          -- Connection Time
          sum ct_count AS sum_ct_count,
          sum ct_sum AS _sum_ct_sum,
          sum_ct_count / $AVG$ AS ct_per_secs,
          IF sum_ct_count = 0 THEN 0 ELSE
            (_sum_ct_sum / sum_ct_count) / 1e6 AS ct_avg,
          IF sum_ct_count = 0 THEN 0 ELSE
            sqrt (((sum ct_square_sum - (_sum_ct_sum)^2) /
                   sum_ct_count) / 1e12) AS ct_stddev,
          -- Server Response Time
          sum rt_count_server AS sum_rt_count_server,
          sum rt_sum_server AS _sum_rt_sum_server,
          sum_rt_count_server / $AVG$ AS srt_per_secs,
          IF sum_rt_count_server = 0 THEN 0 ELSE
            (_sum_rt_sum_server / sum_rt_count_server) / 1e6 AS srt_avg,
          IF sum_rt_count_server = 0 THEN 0 ELSE
            sqrt (((sum rt_square_sum_server - (_sum_rt_sum_server)^2) /
                   sum_rt_count_server) / 1e12) AS srt_stddev,
          -- Round Trip Time CSC
          sum rtt_count_server AS sum_rtt_count_server,
          sum rtt_sum_server AS _sum_rtt_sum_server,
          sum_rtt_count_server / $AVG$ AS crtt_per_secs,
          IF sum_rtt_count_server = 0 THEN 0 ELSE
            (_sum_rtt_sum_server / sum_rtt_count_server) / 1e6 AS crtt_avg,
          IF sum_rtt_count_server = 0 THEN 0 ELSE
            sqrt (((sum rtt_square_sum_server - (_sum_rtt_sum_server)^2) /
                   sum_rtt_count_server) / 1e12) AS crtt_stddev,
          -- Round Trip Time SCS
          sum rtt_count_client AS sum_rtt_count_client,
          sum rtt_sum_client AS _sum_rtt_sum_client,
          sum_rtt_count_client / $AVG$ AS srtt_per_secs,
          IF sum_rtt_count_client = 0 THEN 0 ELSE
            (_sum_rtt_sum_client / sum_rtt_count_client) / 1e6 AS srtt_avg,
          IF sum_rtt_count_client = 0 THEN 0 ELSE
            sqrt (((sum rtt_square_sum_client - (_sum_rtt_sum_client)^2) /
                   sum_rtt_count_client) / 1e12) AS srtt_stddev,
          -- Retransmition Delay C2S
          sum rd_count_client AS sum_rd_count_client,
          sum rd_sum_client AS _sum_rd_sum_client,
          sum_rd_count_client / $AVG$ AS crd_per_secs,
          IF sum_rd_count_client = 0 THEN 0 ELSE
            (_sum_rd_sum_client / sum_rd_count_client) / 1e6 AS crd_avg,
          IF sum_rd_count_client = 0 THEN 0 ELSE
            sqrt (((sum rd_square_sum_client - (_sum_rd_sum_client)^2) /
                   sum_rd_count_client) / 1e12) AS crd_stddev,
          -- Retransmition Delay S2C
          sum rd_count_server AS sum_rd_count_server,
          sum rd_sum_server AS _sum_rd_sum_server,
          sum_rd_count_server / $AVG$ AS srd_per_secs,
          IF sum_rd_count_server = 0 THEN 0 ELSE
            (_sum_rd_sum_server / sum_rd_count_server) / 1e6 AS srd_avg,
          IF sum_rd_count_server = 0 THEN 0 ELSE
            sqrt (((sum rd_square_sum_server - (_sum_rd_sum_server)^2) /
                   sum_rd_count_server) / 1e12) AS srd_stddev,
          -- Data Transfer Time C2S
          sum dtt_count_client AS sum_dtt_count_client,
          sum dtt_sum_client AS _sum_dtt_sum_client,
          sum_dtt_count_client / $AVG$ AS cdtt_per_secs,
          IF sum_dtt_count_client = 0 THEN 0 ELSE
            (_sum_dtt_sum_client / sum_dtt_count_client) / 1e6 AS cdtt_avg,
          IF sum_dtt_count_client = 0 THEN 0 ELSE
            sqrt (((sum dtt_square_sum_client - (_sum_dtt_sum_client)^2) /
                   sum_dtt_count_client) / 1e12) AS cdtt_stddev,
          -- Data Transfer Time S2C
          sum dtt_count_server AS sum_dtt_count_server,
          sum dtt_sum_server AS _sum_dtt_sum_server,
          sum_dtt_count_server / $AVG$ AS sdtt_per_secs,
          IF sum_dtt_count_server = 0 THEN 0 ELSE
            (_sum_dtt_sum_server / sum_dtt_count_server) / 1e6 AS sdtt_avg,
          IF sum_dtt_count_server = 0 THEN 0 ELSE
            sqrt (((sum dtt_square_sum_server - (_sum_dtt_sum_server)^2) /
                   sum_dtt_count_server) / 1e12) AS sdtt_stddev
        WHERE application = $ID$
        GROUP BY capture_begin * 0.000001 // $AVG_INT$
        COMMIT WHEN
          in.capture_begin * 0.000001 > out.start + 2 * $AVG$|}^
        (if export_all export then {|
        EXPORT EVENT STARTING AT start
               WITH DURATION $AVG$|}
        else "") |>
      rep "$CSV$" csv |>
      rep "$ID$" (string_of_int bca.service_id) |>
      rep "$AVG_INT$" (string_of_int avg_window_int) |>
      rep "$AVG$" (string_of_float bca.avg_window)
    in
    make_func avg_per_app op ;
    let perc_per_obs_window_name =
      Printf.sprintf "%s: %gth perc on last %gs"
        bca.name bca.percentile bca.obs_window in
    let op =
      let nb_items_per_groups =
        Helpers.round_to_int (bca.obs_window /. bca.avg_window) in
      (* Note: The event start at the end of the observation window and lasts
       * for one avg window! *)
      (* EURT = RTTs + SRT + DTTs (DTT server to client being optional *)
      (* FIXME: sum of percentiles rather than percentiles of avg *)
      Printf.sprintf
        {|FROM '%s' SELECT
           min start, max start,
           srtt_avg, crtt_avg, srt_avg, cdtt_avg, sdtt_avg,
           %gth percentile (
            srtt_avg + crtt_avg + srt_avg + cdtt_avg + sdtt_avg) AS eurt
         COMMIT AND SLIDE 1 WHEN
           group.#count >= %d OR
           in.start > out.max_start + 5|}
         avg_per_app bca.percentile
         nb_items_per_groups in
    let op =
      if export_some export then op ^ Printf.sprintf {|
         EXPORT EVENT STARTING AT max_start WITH DURATION %g|}
         bca.obs_window
      else op in
    make_func perc_per_obs_window_name op ;
    let title =
      Printf.sprintf "EURT to %s is too large" bca.name
    and text = alert_text [
      "descr", Printf.sprintf
        "The average end user response time to application %s has raised \
         above the configured maximum of %gs for the last %g minutes."
         bca.name bca.max_eurt (bca.obs_window /. 60.) ;
      "bca_id", string_of_int bca.id ;
      "service_id", string_of_int bca.service_id ] in
    let op =
      Printf.sprintf
        {|SELECT
            max_start,
            hysteresis (eurt, %g, %g) AS firing
          FROM '%s'
          COMMIT AND KEEP ALL WHEN firing != COALESCE(previous.firing, false)
          NOTIFY "http://localhost:29380/notify?name=EURT%%20%s&firing=${firing}&time=${max_start}&title=%s&text=%s"|}
          (bca.max_eurt -. bca.max_eurt /. 10.) bca.max_eurt
          perc_per_obs_window_name
          (enc bca.name) (enc title) (enc text) in
    let op =
      if export_all export then op ^ {|
          EXPORT EVENT STARTING AT max_start|}
      else op in
    let name = bca.name ^": EURT too high" in
    make_func name op ;
    let alert_fields = [
      "descr", "anomaly detected" ;
      "bca_id", string_of_int bca.id ] in
    let anom name timeseries =
      let alert_fields = ("metric", name) :: alert_fields in
      let pred, anom =
        anomaly_detection_funcs bca.avg_window avg_per_app name timeseries alert_fields export in
      all_funcs := pred :: anom :: !all_funcs in
    anom "volume"
      [ "c2s_bytes_per_secs", "c2s_bytes_per_secs > 1000", false, [] ;
        "s2c_bytes_per_secs", "s2c_bytes_per_secs > 1000", false, [] ;
        "c2s_packets_per_secs", "c2s_packets_per_secs > 10", false, [] ;
        "s2c_packets_per_secs", "s2c_packets_per_secs > 10", false, [] ] ;
    anom "retransmissions"
      [ "c2s_retrans_bytes_per_secs", "c2s_retrans_bytes_per_secs > 1000", false, [] ;
        "s2c_retrans_bytes_per_secs", "s2c_retrans_bytes_per_secs > 1000", false, [] ;
        "c2s_dupacks_per_secs", "c2s_dupacks_per_secs > 10", false, [] ;
        "s2c_dupacks_per_secs", "s2c_dupacks_per_secs > 10", false, [] ] ;
    anom "connections"
      [ "c2s_syns_per_secs", "c2s_syns_per_secs > 1", false, [] ;
        "s2c_rsts_per_secs", "s2c_rsts_per_secs > 1", false, [] ;
        "close_per_secs", "close_per_secs > 1", false, [] ;
        "ct_avg", "sum_ct_count > 10", false, [] ] ;
    anom "0-windows"
      [ "c2s_0wins_per_secs", "c2s_0wins_per_secs > 1", false, [] ;
        "s2c_0wins_per_secs", "s2c_0wins_per_secs > 1", false, [] ] ;
    anom "SRT" [ "srt_avg", "sum_rt_count_server > 10", false, [] ] ;
    anom "RTT"
      [ "crtt_avg", "sum_rtt_count_server > 10", false, [] ;
        "srtt_avg", "sum_rtt_count_client > 10", false, [] ] ;
    anom "RD"
      [ "crd_avg", "sum_rd_count_client > 10", false, [] ;
        "srd_avg", "sum_rd_count_server > 10", false, [] ] ;
    anom "DTT"
      [ "cdtt_avg", "sum_dtt_count_client > 10", false, [] ;
        "sdtt_avg", "sum_dtt_count_server > 10", false, [] ]
  in
  List.iter conf_of_bca bcas ;
  RamenSharedTypes.{
    name = program_name ;
    ok_if_running = true ;
    start = true ;
    program = program_of_funcs !all_funcs }

let get_config_from_db db =
  Conf_of_sqlite.get_config db

let ddos_program dataset_name export =
  let program_name = rebase dataset_name "DDoS" in
  let avg_win = 60 and rem_win = 3600 in
  let op_new_peers =
    let avg_win_us = avg_win * 1_000_000 in
    {|FROM $CSVS$ SELECT
       (capture_begin // $AVG_WIN_US$) * $AVG_WIN$ AS start,
       min capture_begin, max capture_end,
       -- Traffic (of any kind) we haven't seen in the last $REM_WIN$ secs
       sum (0.9 * float(not remember (
              0.1, -- 10% of false positives
              capture_begin // 1_000_000, $REM_WIN$,
              (hash (coalesce (ip4_client, ip6_client, 0)) +
               hash (coalesce (ip4_server, ip6_server, 0)))))) /
         $AVG_WIN$
         AS nb_new_cnxs_per_secs,
       -- Clients we haven't seen in the last $REM_WIN$ secs
       sum (0.9 * float(not remember (
              0.1,
              capture_begin // 1_000_000, $REM_WIN$,
              hash (coalesce (ip4_client, ip6_client, 0))))) /
          $AVG_WIN$
          AS nb_new_clients_per_secs
     GROUP BY capture_begin // $AVG_WIN_US$
     COMMIT WHEN
       in.capture_begin > out.min_capture_begin + 2 * u64($AVG_WIN_US$)|}^
     (if export_some export then {|
     EXPORT EVENT STARTING AT start
                  WITH DURATION $AVG_WIN$|}
     else "") |>
    rep "$AVG_WIN_US$" (string_of_int avg_win_us) |>
    rep "$AVG_WIN$" (string_of_int avg_win) |>
    rep "$REM_WIN$" (string_of_int rem_win) |>
    rep "$CSVS$" (["tcp" ; "udp" ; "icmp" ; "other-than-ip"] |>
                  List.map (fun p -> "'"^ rebase dataset_name p ^"'") |>
                  String.join ",") in
  let global_new_peers =
    make_func "new peers" op_new_peers in
  let pred_func, anom_func =
    anomaly_detection_funcs
      (float_of_int avg_win) "new peers" "DDoS"
      [ "nb_new_cnxs_per_secs", "nb_new_cnxs_per_secs > 1", false, [] ;
        "nb_new_clients_per_secs", "nb_new_clients_per_secs > 1", false, [] ]
      [ "descr", "possible DDoS" ]
      export in
  RamenSharedTypes.{
    name = program_name ;
    ok_if_running = true ;
    start = true ;
    program = program_of_funcs [
      global_new_peers ; pred_func ; anom_func ] }

(* Daemon *)

open Lwt
open Cohttp
open Cohttp_lwt_unix

let put_program ramen_url program =
  let req = PPP.to_string RamenSharedTypes.put_program_req_ppp program in
  let url = ramen_url ^"/graph/" in
  !logger.debug "Will send %s to %S" req url ;
  let body = `String req in
  (* TODO: but also fix the server never timeouting! *)
  let headers = Header.init_with "Connection" "close" in
  let%lwt (resp, body) =
    Helpers.retry ~on:(fun _ -> return_true) ~min_delay:1.
      (Client.put ~headers ~body) (Uri.of_string url) in
  let code = resp |> Response.status |> Code.code_of_status in
  let%lwt body = Cohttp_lwt.Body.to_string body in
  if code <> 200 then (
    !logger.error "Error code %d: %S" code body ;
    exit 1) ;
  !logger.debug "Response code: %d" code ;
  !logger.debug "Body: %s\n" body ;
  return_unit

let start conf ramen_url db_name dataset_name delete uncompress
          csv_glob with_base with_bcns with_bcas with_ddos export_all =
  logger := make_logger conf.debug ;
  let open Conf_of_sqlite in
  let db = get_db db_name in
  let update () =
    (* TODO: The base program for this client *)
    let%lwt () = if with_base then (
        let base =
          base_program dataset_name delete uncompress csv_glob export_all in
        put_program ramen_url base
      ) else return_unit in
    let%lwt () = if with_bcns > 0 || with_bcas > 0 then (
        let bcns, bcas = get_config_from_db db in
        let bcns = List.take with_bcns bcns
        and bcas = List.take with_bcas bcas in
        let%lwt () = if bcns <> [] then (
            let bcns = program_of_bcns bcns dataset_name export_all in
            put_program ramen_url bcns
          ) else return_unit in
        if bcas <> [] then (
          let bcas = program_of_bcas bcas dataset_name export_all in
          put_program ramen_url bcas
        ) else return_unit
      ) else return_unit in
    if with_ddos then (
      (* Several DDoS detection approaches, regrouped in a "DDoS" program. *)
      let ddos = ddos_program dataset_name export_all in
      put_program ramen_url ddos
    ) else return_unit
  in
  let%lwt () = update () in
  if conf.monitor then
    while%lwt true do
      check_config_changed db ;
      if must_reload db then (
        !logger.info "Must reload configuration" ;
        update ()
      ) else (
        Lwt_unix.sleep 1.0
      )
    done
  else return_unit

(* Args *)

open Cmdliner

let common_opts =
  let debug =
    Arg.(value (flag (info ~doc:"increase verbosity" ["d"; "debug"])))
  and monitor =
    Arg.(value (flag (info ~doc:"keep running and update conf when DB changes"
                           ["m"; "monitor"])))
  in
  Term.(const options $ debug $ monitor)

let ramen_url =
  let env = Term.env_info "RAMEN_URL" in
  let i = Arg.info ~doc:"URL to reach ramen"
                   ~env [ "ramen-url" ] in
  Arg.(value (opt string "http://127.0.0.1:29380" i))

let db_name =
  let i = Arg.info ~doc:"Path of the SQLite file"
                   [ "db" ] in
  Arg.(required (opt (some string) None i))

let dataset_name =
  let i = Arg.info ~doc:"Name identifying this data set. Will be used to \
                         prefix any created programs."
                   [ "name" ; "dataset" ; "dataset-name" ] in
  Arg.(required (opt (some string) None i))

let delete_opt =
  let i = Arg.info ~doc:"Delete CSV files once injected"
                   [ "delete" ] in
  Arg.(value (flag i))

let uncompress_opt =
  let i = Arg.info ~doc:"CSV are compressed with lz4"
                   [ "uncompress" ; "uncompress-csv" ] in
  Arg.(value (flag i))

let csv_glob =
  let i = Arg.info ~doc:"File glob for the CSV files, where % will be \
                         replaced by the CSV type (\"tcp\", \"udp\"...)"
                   [ "csv" ] in
  Arg.(required (opt (some string) None i))

let with_base =
  let i = Arg.info ~doc:"Output the base program with CSV input and first \
                         operations"
                   [ "with-base" ; "base" ] in
  Arg.(value (flag i))

let with_bcns =
  let i = Arg.info ~doc:"Also output the program with BCN configuration"
                   [ "with-bcns" ; "with-bcn" ; "bcns" ; "bcn" ] in
  Arg.(value (opt ~vopt:10 int 0 i))

let with_bcas =
  let i = Arg.info ~doc:"Also output the program with BCA configuration"
                   [ "with-bcas" ; "with-bca" ; "bcas" ; "bca" ] in
  Arg.(value (opt ~vopt:10 int 0 i))

let with_ddos =
  let i = Arg.info ~doc:"Also output the program with DDoS detection"
                   [ "with-ddos" ; "with-dos" ; "ddos" ; "dos" ] in
  Arg.(value (flag i))

let exports =
  Arg.(value (vflag ExportNone [
    ExportNone, Arg.info ~doc:"No func will export data (the default)"
                         [ "export-none" ; "no-exports" ] ;
    ExportSome, Arg.info ~doc:"A few important funcs will export data"
                         [ "export-some" ; "exports" ] ;
    ExportAll,  Arg.info ~doc:"All func will export data (useful for \
                               debugging but expensive in CPU and IO)"
                         [ "export-all" ] ]))

let start_cmd =
  Term.(
    (const start
      $ common_opts
      $ ramen_url
      $ db_name
      $ dataset_name
      $ delete_opt
      $ uncompress_opt
      $ csv_glob
      $ with_base
      $ with_bcns
      $ with_bcas
      $ with_ddos
      $ exports),
    info "ramen_configurator")

let () =
  match Term.eval start_cmd with
  | `Ok th -> Lwt_main.run th
  | x -> Term.exit x
